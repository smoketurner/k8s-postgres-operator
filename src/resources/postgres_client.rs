//! PostgreSQL client for direct SQL connections
//!
//! Provides `PostgresConnection` which establishes a connection to PostgreSQL
//! via kube-rs port-forwarding. This enables direct SQL execution without
//! requiring pod exec.
//!
//! The connection includes RAII cleanup - when `PostgresConnection` is dropped,
//! both the database connection and port forward are automatically closed.

use crate::resources::port_forward::{PortForward, PortForwardError, PortForwardTarget};
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};
use tokio_postgres_rustls_improved::MakeRustlsConnect;

/// Errors that can occur during PostgreSQL operations
#[derive(Error, Debug)]
pub enum PostgresClientError {
    #[error("Connection failed: {0}")]
    Connection(#[from] tokio_postgres::Error),

    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Port forward error: {0}")]
    PortForward(#[from] PortForwardError),

    #[error("Credentials secret not found: {0}")]
    SecretNotFound(String),

    #[error("Secret missing required key: {0}")]
    SecretMissingKey(String),

    #[error("Invalid UTF-8 in secret data")]
    InvalidUtf8,

    #[error("Query failed: {0}")]
    Query(String),

    #[error("Connection timeout")]
    Timeout,

    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    #[error("Invalid certificate: {0}")]
    InvalidCertificate(String),
}

/// Result type for PostgreSQL client operations
pub type PostgresClientResult<T> = Result<T, PostgresClientError>;

/// Render a tokio_postgres error with its database-level detail.
///
/// `tokio_postgres::Error::to_string()` collapses to the generic "db error" for
/// server-side failures. The actionable text (SQLSTATE, message, hint, position)
/// lives in `as_db_error()`. This helper formats both into a single string so
/// CRD status and operator logs surface what actually went wrong.
fn format_pg_error(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut s = format!("[{}] {}", db.code().code(), db.message());
        if let Some(d) = db.detail() {
            s.push_str(" — ");
            s.push_str(d);
        }
        if let Some(h) = db.hint() {
            s.push_str(" (hint: ");
            s.push_str(h);
            s.push(')');
        }
        if let Some(p) = db.position() {
            use std::fmt::Write;
            let _ = write!(s, " @ {p:?}");
        }
        s
    } else {
        e.to_string()
    }
}

/// TLS mode for PostgreSQL connections
#[derive(Debug, Clone)]
pub enum TlsMode {
    /// No TLS - use for clusters with TLS disabled or internal connections
    Disabled,
    /// Require TLS but skip certificate verification (for self-signed certs via port-forward)
    RequireUnverified,
    /// Require TLS with CA certificate verification
    RequireVerified {
        /// PEM-encoded CA certificate
        ca_cert_pem: String,
    },
}

/// PostgreSQL connection credentials
#[derive(Debug, Clone)]
pub struct PostgresCredentials {
    pub username: String,
    pub password: String,
    pub database: String,
}

impl PostgresCredentials {
    /// Create credentials with explicit values
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            database: database.into(),
        }
    }

    /// Extract credentials from a cluster credentials Kubernetes Secret
    ///
    /// Expects the secret to contain `POSTGRES_PASSWORD` key.
    /// Uses default username "postgres" and database "postgres".
    pub fn from_cluster_secret(secret: &Secret) -> PostgresClientResult<Self> {
        let data = secret
            .data
            .as_ref()
            .ok_or_else(|| PostgresClientError::SecretMissingKey("no data in secret".into()))?;

        let password_bytes = data
            .get("POSTGRES_PASSWORD")
            .ok_or_else(|| PostgresClientError::SecretMissingKey("POSTGRES_PASSWORD".into()))?;

        let password = String::from_utf8(password_bytes.0.clone())
            .map_err(|_| PostgresClientError::InvalidUtf8)?;

        Ok(Self {
            username: "postgres".to_string(),
            password,
            database: "postgres".to_string(),
        })
    }

    /// Extract credentials from a PostgresDatabase role secret
    ///
    /// PostgresDatabase secrets have keys: username, password, database
    pub fn from_role_secret(secret: &Secret) -> PostgresClientResult<Self> {
        let data = secret
            .data
            .as_ref()
            .ok_or_else(|| PostgresClientError::SecretMissingKey("no data in secret".into()))?;

        let username = data
            .get("username")
            .ok_or_else(|| PostgresClientError::SecretMissingKey("username".into()))?;
        let password = data
            .get("password")
            .ok_or_else(|| PostgresClientError::SecretMissingKey("password".into()))?;
        let database = data
            .get("database")
            .ok_or_else(|| PostgresClientError::SecretMissingKey("database".into()))?;

        Ok(Self {
            username: String::from_utf8(username.0.clone())
                .map_err(|_| PostgresClientError::InvalidUtf8)?,
            password: String::from_utf8(password.0.clone())
                .map_err(|_| PostgresClientError::InvalidUtf8)?,
            database: String::from_utf8(database.0.clone())
                .map_err(|_| PostgresClientError::InvalidUtf8)?,
        })
    }
}

/// Fetch credentials from a cluster credentials Kubernetes secret
pub async fn fetch_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
) -> PostgresClientResult<PostgresCredentials> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

    let secret = secrets.get(secret_name).await.map_err(|e| match &e {
        kube::Error::Api(api_err) if api_err.code == 404 => {
            PostgresClientError::SecretNotFound(secret_name.to_string())
        }
        _ => PostgresClientError::Kube(e),
    })?;

    PostgresCredentials::from_cluster_secret(&secret)
}

/// Fetch credentials from a PostgresDatabase role secret
pub async fn fetch_role_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
) -> PostgresClientResult<PostgresCredentials> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

    let secret = secrets.get(secret_name).await.map_err(|e| match &e {
        kube::Error::Api(api_err) if api_err.code == 404 => {
            PostgresClientError::SecretNotFound(secret_name.to_string())
        }
        _ => PostgresClientError::Kube(e),
    })?;

    PostgresCredentials::from_role_secret(&secret)
}

/// Fetch CA certificate from a TLS secret
///
/// cert-manager stores certificates in secrets with key `ca.crt`
pub async fn fetch_ca_certificate(
    client: &Client,
    namespace: &str,
    secret_name: &str,
) -> PostgresClientResult<String> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

    let secret = secrets.get(secret_name).await.map_err(|e| match &e {
        kube::Error::Api(api_err) if api_err.code == 404 => {
            PostgresClientError::SecretNotFound(secret_name.to_string())
        }
        _ => PostgresClientError::Kube(e),
    })?;

    let data = secret
        .data
        .as_ref()
        .ok_or_else(|| PostgresClientError::SecretMissingKey("no data in secret".into()))?;

    let ca_bytes = data
        .get("ca.crt")
        .ok_or_else(|| PostgresClientError::SecretMissingKey("ca.crt".into()))?;

    String::from_utf8(ca_bytes.0.clone()).map_err(|_| PostgresClientError::InvalidUtf8)
}

/// PostgreSQL connection via kube-rs port-forward
///
/// Provides a connection to a PostgreSQL database through Kubernetes port-forwarding.
/// When dropped, both the database connection and port forward are automatically cleaned up.
pub struct PostgresConnection {
    client: tokio_postgres::Client,
    _port_forward: PortForward,
}

impl PostgresConnection {
    /// Connect to a PostgresCluster's primary (non-TLS)
    ///
    /// This establishes a port-forward to the primary pod and creates a database connection.
    /// Connects to the default "postgres" database.
    pub async fn connect_primary(
        kube_client: &Client,
        namespace: &str,
        cluster_name: &str,
    ) -> PostgresClientResult<Self> {
        let credentials = fetch_credentials(
            kube_client,
            namespace,
            &format!("{}-credentials", cluster_name),
        )
        .await?;

        let service_name = format!("{}-primary", cluster_name);
        Self::connect_service(
            kube_client,
            namespace,
            &service_name,
            5432,
            &credentials,
            TlsMode::Disabled,
        )
        .await
    }

    /// Connect to a specific database in a PostgresCluster's primary (non-TLS)
    ///
    /// Like `connect_primary`, but connects to the specified database instead of "postgres".
    pub async fn connect_database(
        kube_client: &Client,
        namespace: &str,
        cluster_name: &str,
        database: &str,
    ) -> PostgresClientResult<Self> {
        let mut credentials = fetch_credentials(
            kube_client,
            namespace,
            &format!("{}-credentials", cluster_name),
        )
        .await?;

        // Override the database name
        credentials.database = database.to_string();

        let service_name = format!("{}-primary", cluster_name);
        Self::connect_service(
            kube_client,
            namespace,
            &service_name,
            5432,
            &credentials,
            TlsMode::Disabled,
        )
        .await
    }

    /// Connect to a specific service with custom credentials and TLS mode
    pub async fn connect_service(
        kube_client: &Client,
        namespace: &str,
        service_name: &str,
        port: u16,
        credentials: &PostgresCredentials,
        tls_mode: TlsMode,
    ) -> PostgresClientResult<Self> {
        // Start port forward
        let pf = PortForward::start(
            kube_client.clone(),
            namespace,
            PortForwardTarget::service(service_name, port),
            None,
        )
        .await?;

        let local_port = pf.local_port();

        // Build connection string
        let config = format!(
            "host=127.0.0.1 port={} user={} password={} dbname={} connect_timeout=10",
            local_port, credentials.username, credentials.password, credentials.database
        );

        // Connect based on TLS mode
        let client = match &tls_mode {
            TlsMode::Disabled => {
                let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        tracing::trace!(error = %e, "PostgreSQL connection closed");
                    }
                });
                client
            }
            _ => {
                let tls = build_tls_connector(&tls_mode)?;
                let (client, connection) = tokio_postgres::connect(&config, tls).await?;
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        tracing::trace!(error = %e, "PostgreSQL TLS connection closed");
                    }
                });
                client
            }
        };

        tracing::debug!(
            service = service_name,
            local_port = local_port,
            database = &credentials.database,
            "PostgreSQL connection established"
        );

        Ok(Self {
            client,
            _port_forward: pf,
        })
    }

    /// Execute a query returning rows
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> PostgresClientResult<Vec<Row>> {
        self.client
            .query(sql, params)
            .await
            .map_err(|e| PostgresClientError::Query(format_pg_error(&e)))
    }

    /// Execute a query returning an optional single row
    pub async fn query_opt(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> PostgresClientResult<Option<Row>> {
        self.client
            .query_opt(sql, params)
            .await
            .map_err(|e| PostgresClientError::Query(format_pg_error(&e)))
    }

    /// Execute a query returning exactly one row
    pub async fn query_one(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> PostgresClientResult<Row> {
        self.client
            .query_one(sql, params)
            .await
            .map_err(|e| PostgresClientError::Query(format_pg_error(&e)))
    }

    /// Execute a statement returning the number of affected rows
    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> PostgresClientResult<u64> {
        self.client
            .execute(sql, params)
            .await
            .map_err(|e| PostgresClientError::Query(format_pg_error(&e)))
    }

    /// Execute multiple statements in a batch (no params support)
    ///
    /// Useful for DDL operations that don't support parameters
    pub async fn batch_execute(&self, sql: &str) -> PostgresClientResult<()> {
        self.client
            .batch_execute(sql)
            .await
            .map_err(|e| PostgresClientError::Query(format_pg_error(&e)))
    }

    /// Get the local port being used for the connection
    pub fn local_port(&self) -> u16 {
        self._port_forward.local_port()
    }
}

// =============================================================================
// TLS Configuration
// =============================================================================

/// Parse PEM-encoded certificates into DER format
fn parse_pem_certificates(pem_data: &str) -> PostgresClientResult<Vec<CertificateDer<'static>>> {
    let mut certs = Vec::new();

    for cert in CertificateDer::pem_slice_iter(pem_data.as_bytes()) {
        match cert {
            Ok(cert) => certs.push(cert),
            Err(e) => {
                return Err(PostgresClientError::InvalidCertificate(format!(
                    "Failed to parse certificate: {}",
                    e
                )));
            }
        }
    }

    if certs.is_empty() {
        return Err(PostgresClientError::InvalidCertificate(
            "No certificates found in PEM data".to_string(),
        ));
    }

    Ok(certs)
}

/// Build a rustls TLS connector for PostgreSQL
fn build_tls_connector(tls_mode: &TlsMode) -> PostgresClientResult<MakeRustlsConnect> {
    match tls_mode {
        TlsMode::Disabled => Err(PostgresClientError::TlsConfig(
            "Cannot build TLS connector for disabled TLS mode".to_string(),
        )),
        TlsMode::RequireUnverified => {
            // Create a config that doesn't verify certificates (for self-signed certs)
            let config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();

            Ok(MakeRustlsConnect::new(config))
        }
        TlsMode::RequireVerified { ca_cert_pem } => {
            // Parse CA certificate
            let certs = parse_pem_certificates(ca_cert_pem)?;

            // Build root cert store with CA
            let mut root_store = rustls::RootCertStore::empty();
            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    PostgresClientError::InvalidCertificate(format!("Failed to add CA cert: {}", e))
                })?;
            }

            let config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            Ok(MakeRustlsConnect::new(config))
        }
    }
}

/// Custom certificate verifier that accepts any certificate
/// Used for self-signed certificates when connecting via port-forward
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

// =============================================================================
// Test Helpers
// =============================================================================

/// Extended timeout for cluster readiness (includes PostgreSQL startup)
pub const POSTGRES_READY_TIMEOUT: Duration = Duration::from_secs(180);

/// Retry interval for connection attempts
pub const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum connection retries
pub const MAX_CONNECT_RETRIES: u32 = 15;

/// Environment variable name for test ClusterIssuer
pub const TEST_CLUSTER_ISSUER_ENV: &str = "TEST_CLUSTER_ISSUER";

/// Default ClusterIssuer name for TLS tests
pub const DEFAULT_CLUSTER_ISSUER: &str = "selfsigned-issuer";

/// Get the ClusterIssuer name for TLS tests
///
/// Returns the value of TEST_CLUSTER_ISSUER env var, or "selfsigned-issuer" as default.
pub fn get_test_cluster_issuer() -> String {
    std::env::var(TEST_CLUSTER_ISSUER_ENV).unwrap_or_else(|_| DEFAULT_CLUSTER_ISSUER.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::parse_pem_certificates;

    const CERT_A: &str = "\
-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUR75kqHNOemfd6MuPVhVGYP3jLIAwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJdGVzdC1jYS0xMB4XDTI2MDcyNTE5NTEwMloXDTI3MDcy
NTE5NTEwMlowFDESMBAGA1UEAwwJdGVzdC1jYS0xMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAqClV0XhN+meYeXqocKaYbWZx6hN91QXxiKNHkuM/wNR7
61f6ULUCvvlgffL+D0HSYM6LQ9RCqy8iuZctZbjgC4KvY2yF8tcjEAYtNR6IK5Bk
0C79+Mx6AKulCNnT9nCGZXRHNwWJD52JNUkqgD0i+ZJHP7a4/BCo0L+DQq5Y997K
up0E1JVh73Fxgi87hpmze3xqspYf6jLbvi1t/FrvUkE0iX4/McKJG04XlndW1IVa
VAME5/A3f+BkstAFiniodHU54HEBSm6IJKeV6UaMJJZcmjnk9Jj1BHQA8M1GbU5y
mPKUkUovCUtHz6zlV7Z6hY/FibTOj5Ep0gTEWbYpfQIDAQABo1MwUTAdBgNVHQ4E
FgQUIZvmpFI040oyfDt+BKw4KEuTvGswHwYDVR0jBBgwFoAUIZvmpFI040oyfDt+
BKw4KEuTvGswDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAk/4U
ouDHyeSuerbjnAf0/paI0p1PQdH44TXNUslfQT2uqFvCkeEdnlVONV/G7uh1W5u+
G1zjcNkVwEUM1AUMmPIlhiDy/xDXz1sMrYBHz4joEFAXrmXc3pvRHMCqh/J73KEM
YqAKvyiNoqoMN8Yr0B3CL2nHQ2WDT23812moJUV5oHw3UDmVQkEXcu5wzRWGa677
oK/Dy+YIgEv014ihPvxuXwu7CGhq8kQ/uAZNls2kS0ggAysOViR+J5kANhOX7fsJ
+jnDc7E6Wep2xQeXaYmQu4+kh0ccFEEG7eAowSUl5j/r1ipdsia1yxAoIATPsG0X
BZVTZBaptUrfElRk4w==
-----END CERTIFICATE-----
";

    const CERT_B: &str = "\
-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUdl9eK/c2Xz5TMUI6nc+w0z9mJ50wDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJdGVzdC1jYS0yMB4XDTI2MDcyNTE5NTEwMloXDTI3MDcy
NTE5NTEwMlowFDESMBAGA1UEAwwJdGVzdC1jYS0yMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA1NwqVz9mCV4wMTjNbVz/3gwOwgv5nhMG6ePHx1Wm8aTs
hafe4xhAcSG7hbJ2eniu2cC8QZhA8MJBEbC0Pku1dBM6AkV3rpozqNhLlMQMWcKh
9fV7VkV2h8B00aA41NUyHD0TbiGuhCZ3nSGbyru4mGQbR3SwmJcN3kE1EJ6A6X+J
wBLKCfREfqyeEMuLrbZ6mN1RPBsBjDmvWgN8UlIwmbWRgECXWiRZ/cexIpgOphfR
rj2icD1qRkAlUrYZ/pW0qbP5C8emFUDBGwfC31Sjc42zfCEKS5QYjPGDi/thDzhf
ljHosD9N2bFYIHPtJpePpFPHs7V2VaUNJC9vyTHSsQIDAQABo1MwUTAdBgNVHQ4E
FgQUdfYG/dYq/rALC5+1bG9BtuN+Pu0wHwYDVR0jBBgwFoAUdfYG/dYq/rALC5+1
bG9BtuN+Pu0wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAcRUV
8yF7xEdrMv1uEqF+EEryzi+ISniELXwvbZSjWkKh4ZncsLkouADogbHwrGjdqUYZ
qCIGPcxWSGM1bt/iy0331b3bY2UvbyXD9s0D0RSm/XDV4g4xhSzlVPAzaz0OhBhG
JueXihXcWfl5BEzRD8IklIDF6+way5fuQ8vL6O80nJifnsxn5rvSiJh8ILraGEBs
PpQbnHmCzNCPcBl7bO/wVs5O1mDEDQxMyKP7Y9C/Krn8RVIqH0ZEY8OdSUVg2zm9
461vCBy/OMfKFlKTdsK0gVVD+OcIimbBqIZ5VbG+uauVC5q3H+XFPlb59F2u7sxI
THo3aCm/GoIp54OATA==
-----END CERTIFICATE-----
";

    const PRIVATE_KEY: &str = "\
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCoKVXReE36Z5h5
eqhwpphtZnHqE33VBfGIo0eS4z/A1HvrV/pQtQK++WB98v4PQdJgzotD1EKrLyK5
ly1luOALgq9jbIXy1yMQBi01HogrkGTQLv34zHoAq6UI2dP2cIZldEc3BYkPnYk1
SSqAPSL5kkc/trj8EKjQv4NCrlj33sq6nQTUlWHvcXGCLzuGmbN7fGqylh/qMtu+
LW38Wu9SQTSJfj8xwokbTheWd1bUhVpUAwTn8Dd/4GSy0AWKeKh0dTngcQFKbogk
p5XpRowkllyaOeT0mPUEdADwzUZtTnKY8pSRSi8JS0fPrOVXtnqFj8WJtM6PkSnS
BMRZtil9AgMBAAECggEACcxCwk4yqOzhTu6tIscqKXGnIH7bPY63mID++hmAjPE0
eS1qmco6Kztnel8um1/37IkMRzr2WXgJG2wqCnu/nhwSsQXRNil/0v6xIp+xSmyC
2zhpttXfI+vcVUwv0/OReRbR0WxipITGylKFhexJ/eWefiFc3N7xnxwRf1CeQW7i
62c2n02+SAwz5PafoYCAAu2pdkoTxoCitqL6HUw8F4zXspo/RquIbQudXV+JGpQH
d5IOIw8PAnCyyOFQhOGDCxMZ7fnw8KOoYX1//CtpblkPhzXSN3HsgwNhzCws1PIs
4yamSMfBEnjwV8atX2nvHVdBAokgrx5XY4RundlmJwKBgQDmveRGQWyvto3ZXeCa
uq5ZKoQseF4UNbGw/mnXdmwnDb4tkRmTu98Di1XnXI4Pl6lqEP0C3U24aJkOcQH5
qmXJuq7Sg7OMmUsMdFpmbj1evAsRXrtUTT7m7ax6ajfdN/GQ4xZFqxTaXYE4HXmH
jlVtY6xLUvjueWyX/tkW/ZIrxwKBgQC6kb9Hgc3ZOJ83HvFMnetz1KDtnNXKkYO7
z04Uegr7GZNnAv1VJkFw8RO/qdg3tboc3yEc0TM8UTBku50UNXuyALq0Y2teU/om
gHmaO9J23ni7RmYZgI781HLMC23jr6WiKot1uKaTGwLkTjTDpqF6DqgFaDw2a419
a4kf8w0YmwKBgQDPMsGjnOheOQ3TnQstpmkdRKJ/1G6Ws0im6S5d/sdLonmeLWfM
U64FXr97DI+8zLGivzKTueoqqDKY1z2w1iSlK3AFNaKrpJPR0UHELUYKpc1CgdCx
+NN9RvvUyUD082GGe4TqdqA5HjIFE+KnqVZo7lIvKYjDjGHJc1252WXCzQKBgGLB
NyCgotdyU0SYCl3l0XXUfQKJW9kHwVUuXEQWfa2AUjfaq0HhKA6ibTOssZh7hvI1
YY+hZJ9u0lDfxjumO71zCWDmpzSc+vJaWwO62qK1C+8FSpIBLK7Dvagn/Jjipqf6
ISvE+9cuGw/CHcfacerryyBhlk2wDIrw2vqgarQ1AoGBAIt9AoGFPhA1XNXexIYk
+qstwkZdTJKoISViFesmfDtXUPZz7OAjQ9+sDm/Z/5zJ+qqGRWEXYwbz/LbcelS0
sAtLPdNGIBChXbaW531wpNVYlW5C87mxHlMJJNr9fo7mcE69tteg+tUmrGk67RZt
s1ufyiNAw0tbJIegkWF7X19Y
-----END PRIVATE KEY-----
";

    #[test]
    fn parses_single_certificate() {
        let certs = parse_pem_certificates(CERT_A).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn parses_certificate_chain() {
        let chain = format!("{CERT_A}{CERT_B}");
        let certs = parse_pem_certificates(&chain).unwrap();
        assert_eq!(certs.len(), 2);
    }

    #[test]
    fn skips_non_certificate_sections() {
        let bundle = format!("{PRIVATE_KEY}{CERT_A}");
        let certs = parse_pem_certificates(&bundle).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn rejects_pem_without_certificates() {
        for input in [PRIVATE_KEY, "", "not a pem at all\n"] {
            let err = parse_pem_certificates(input).unwrap_err();
            assert!(
                matches!(err, super::PostgresClientError::InvalidCertificate(ref m)
                    if m.contains("No certificates found")),
                "expected no-certificates error for {input:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_truncated_certificate() {
        // Header plus body, with the END marker cut off.
        let truncated: String = CERT_A.lines().take(3).map(|l| format!("{l}\n")).collect();
        let err = parse_pem_certificates(&truncated).unwrap_err();
        assert!(
            matches!(err, super::PostgresClientError::InvalidCertificate(ref m)
                if m.contains("Failed to parse certificate")),
            "expected parse failure, got {err:?}"
        );
    }
}
