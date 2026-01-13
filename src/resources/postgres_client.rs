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

    /// Connect with TLS to a specific service
    pub async fn connect_with_tls(
        kube_client: &Client,
        namespace: &str,
        service_name: &str,
        port: u16,
        credentials: &PostgresCredentials,
        tls_mode: TlsMode,
    ) -> PostgresClientResult<Self> {
        Self::connect_service(
            kube_client,
            namespace,
            service_name,
            port,
            credentials,
            tls_mode,
        )
        .await
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
            .map_err(|e| PostgresClientError::Query(e.to_string()))
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
            .map_err(|e| PostgresClientError::Query(e.to_string()))
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
            .map_err(|e| PostgresClientError::Query(e.to_string()))
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
            .map_err(|e| PostgresClientError::Query(e.to_string()))
    }

    /// Execute multiple statements in a batch (no params support)
    ///
    /// Useful for DDL operations that don't support parameters
    pub async fn batch_execute(&self, sql: &str) -> PostgresClientResult<()> {
        self.client
            .batch_execute(sql)
            .await
            .map_err(|e| PostgresClientError::Query(e.to_string()))
    }

    /// Get the local port being used for the connection
    pub fn local_port(&self) -> u16 {
        self._port_forward.local_port()
    }
}

/// Information about a PostgreSQL connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Full version string (e.g., "PostgreSQL 17.2 on x86_64-pc-linux-gnu")
    pub pg_version: String,
    /// Whether the connection is using TLS
    pub ssl_enabled: bool,
    /// TLS version if SSL is enabled (e.g., "TLSv1.3")
    pub ssl_version: Option<String>,
    /// Whether this is a replica (in recovery mode)
    pub is_replica: bool,
    /// Replication lag in seconds (only set for replicas)
    pub replication_lag_secs: Option<f64>,
}

impl ConnectionInfo {
    /// Query connection info from an established connection
    pub async fn from_connection(conn: &PostgresConnection) -> PostgresClientResult<Self> {
        let query = r#"
            SELECT
                version() as pg_version,
                COALESCE(pg_ssl.ssl, false) as ssl_enabled,
                pg_ssl.version as ssl_version,
                pg_is_in_recovery() as is_replica,
                CASE WHEN pg_is_in_recovery() THEN
                    extract(epoch from (now() - pg_last_xact_replay_timestamp()))::float8
                ELSE NULL END as replication_lag_seconds
            FROM pg_stat_activity pg_sa
            LEFT JOIN pg_stat_ssl pg_ssl ON pg_ssl.pid = pg_sa.pid
            WHERE pg_sa.pid = pg_backend_pid()
        "#;

        let row = conn.query_one(query, &[]).await?;

        Ok(Self {
            pg_version: row.get("pg_version"),
            ssl_enabled: row.get("ssl_enabled"),
            ssl_version: row.get("ssl_version"),
            is_replica: row.get("is_replica"),
            replication_lag_secs: row.get("replication_lag_seconds"),
        })
    }

    /// Extract major version number (e.g., 17 from "PostgreSQL 17.2 ...")
    pub fn major_version(&self) -> Option<u8> {
        self.pg_version
            .strip_prefix("PostgreSQL ")
            .and_then(|v| v.split('.').next())
            .and_then(|v| v.parse().ok())
    }
}

// =============================================================================
// TLS Configuration
// =============================================================================

/// Parse PEM-encoded certificates into DER format
fn parse_pem_certificates(pem_data: &str) -> PostgresClientResult<Vec<CertificateDer<'static>>> {
    let mut certs = Vec::new();
    let mut reader = std::io::BufReader::new(pem_data.as_bytes());

    for cert in rustls_pemfile::certs(&mut reader) {
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
