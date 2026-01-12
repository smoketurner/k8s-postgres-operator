//! PostgreSQL connection utilities for integration tests
//!
//! Provides utilities for connecting to PostgreSQL via port-forwarding
//! and verifying connection properties (version, TLS, replication status).

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use rustls::pki_types::{CertificateDer, ServerName};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Error, Debug)]
pub enum PostgresError {
    #[error("Connection failed: {0}")]
    Connection(#[from] tokio_postgres::Error),

    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

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

    #[error("Failed to parse version from: {0}")]
    VersionParse(String),

    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    #[error("Invalid certificate: {0}")]
    InvalidCertificate(String),
}

/// TLS mode for PostgreSQL connections
#[derive(Debug, Clone)]
pub enum TlsMode {
    /// No TLS - use for clusters with TLS disabled
    Disabled,
    /// Require TLS but skip certificate verification (for self-signed certs via port-forward)
    RequireUnverified,
    /// Require TLS with CA certificate verification
    RequireVerified {
        /// PEM-encoded CA certificate
        ca_cert_pem: String,
    },
}

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

/// PostgreSQL connection credentials
#[derive(Debug, Clone)]
pub struct PostgresCredentials {
    pub username: String,
    pub password: String,
    pub database: String,
}

impl PostgresCredentials {
    /// Extract credentials from a Kubernetes Secret
    ///
    /// Expects the secret to contain `POSTGRES_PASSWORD` key.
    /// Uses default username "postgres" and database "postgres".
    pub fn from_secret(secret: &Secret) -> Result<Self, PostgresError> {
        let data = secret
            .data
            .as_ref()
            .ok_or_else(|| PostgresError::SecretMissingKey("no data in secret".into()))?;

        let password_bytes = data
            .get("POSTGRES_PASSWORD")
            .ok_or_else(|| PostgresError::SecretMissingKey("POSTGRES_PASSWORD".into()))?;

        let password =
            String::from_utf8(password_bytes.0.clone()).map_err(|_| PostgresError::InvalidUtf8)?;

        Ok(Self {
            username: "postgres".to_string(),
            password,
            database: "postgres".to_string(),
        })
    }
}

/// Fetch credentials from a Kubernetes secret
pub async fn fetch_credentials(
    client: &Client,
    namespace: &str,
    secret_name: &str,
) -> Result<PostgresCredentials, PostgresError> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

    let secret = secrets.get(secret_name).await.map_err(|e| match &e {
        kube::Error::Api(api_err) if api_err.code == 404 => {
            PostgresError::SecretNotFound(secret_name.to_string())
        }
        _ => PostgresError::Kube(e),
    })?;

    PostgresCredentials::from_secret(&secret)
}

/// Fetch CA certificate from a TLS secret
///
/// cert-manager stores certificates in secrets with keys:
/// - `tls.crt`: Server certificate
/// - `tls.key`: Private key
/// - `ca.crt`: CA certificate
pub async fn fetch_ca_certificate(
    client: &Client,
    namespace: &str,
    secret_name: &str,
) -> Result<String, PostgresError> {
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

    let secret = secrets.get(secret_name).await.map_err(|e| match &e {
        kube::Error::Api(api_err) if api_err.code == 404 => {
            PostgresError::SecretNotFound(secret_name.to_string())
        }
        _ => PostgresError::Kube(e),
    })?;

    let data = secret
        .data
        .as_ref()
        .ok_or_else(|| PostgresError::SecretMissingKey("no data in secret".into()))?;

    let ca_bytes = data
        .get("ca.crt")
        .ok_or_else(|| PostgresError::SecretMissingKey("ca.crt".into()))?;

    String::from_utf8(ca_bytes.0.clone()).map_err(|_| PostgresError::InvalidUtf8)
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
    /// Extract major version number (e.g., 17 from "PostgreSQL 17.2 ...")
    pub fn major_version(&self) -> Result<u8, PostgresError> {
        // Format: "PostgreSQL X.Y on ..."
        let version_part = self
            .pg_version
            .strip_prefix("PostgreSQL ")
            .ok_or_else(|| PostgresError::VersionParse(self.pg_version.clone()))?;

        let major_str = version_part
            .split('.')
            .next()
            .ok_or_else(|| PostgresError::VersionParse(self.pg_version.clone()))?;

        major_str
            .parse()
            .map_err(|_| PostgresError::VersionParse(self.pg_version.clone()))
    }

    /// Assert that the major version matches expected
    pub fn assert_version(&self, expected: u8) {
        let actual = self
            .major_version()
            .unwrap_or_else(|e| panic!("Failed to parse version: {}", e));
        assert_eq!(
            actual, expected,
            "PostgreSQL version mismatch: expected {}, got {} (full: {})",
            expected, actual, self.pg_version
        );
    }

    /// Assert TLS status matches expected
    pub fn assert_tls(&self, expected: bool) {
        assert_eq!(
            self.ssl_enabled, expected,
            "TLS status mismatch: expected {}, got {} (ssl_version: {:?})",
            expected, self.ssl_enabled, self.ssl_version
        );
    }

    /// Assert this is a primary (not in recovery)
    pub fn assert_is_primary(&self) {
        assert!(
            !self.is_replica,
            "Expected primary but connected to replica (is_replica=true)"
        );
    }

    /// Assert this is a replica (in recovery)
    pub fn assert_is_replica(&self) {
        assert!(
            self.is_replica,
            "Expected replica but connected to primary (is_replica=false)"
        );
    }

    /// Assert replication lag is under the specified threshold
    pub fn assert_lag_under(&self, max_secs: f64) {
        if let Some(lag) = self.replication_lag_secs {
            assert!(
                lag < max_secs,
                "Replication lag {}s exceeds threshold {}s",
                lag,
                max_secs
            );
        }
        // If lag is None (primary), this passes
    }
}

/// Verify PostgreSQL connection and return connection info
///
/// This is a one-shot function that:
/// 1. Connects to PostgreSQL
/// 2. Executes a verification query
/// 3. Closes the connection
///
/// Returns information about:
/// - PostgreSQL version
/// - TLS status
/// - Replication role (primary vs replica)
/// - Replication lag (for replicas)
pub async fn verify_connection(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
) -> Result<ConnectionInfo, PostgresError> {
    let config = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=10",
        host, port, credentials.username, credentials.password, credentials.database
    );

    let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;

    // Spawn connection handler (will complete when client is dropped)
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::trace!(error = %e, "PostgreSQL connection closed");
        }
    });

    // Query connection info including SSL status via pg_stat_ssl joined with pg_stat_activity
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

    let row = client
        .query_one(query, &[])
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    let pg_version: String = row.get("pg_version");
    let ssl_enabled: bool = row.get("ssl_enabled");
    let ssl_version: Option<String> = row.get("ssl_version");
    let is_replica: bool = row.get("is_replica");
    let replication_lag_secs: Option<f64> = row.get("replication_lag_seconds");

    // Client is dropped here, connection closes automatically

    Ok(ConnectionInfo {
        pg_version,
        ssl_enabled,
        ssl_version,
        is_replica,
        replication_lag_secs,
    })
}

/// Verify connection with retry logic
pub async fn verify_connection_with_retry(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    max_retries: u32,
    retry_interval: Duration,
) -> Result<ConnectionInfo, PostgresError> {
    let mut last_error = None;

    for attempt in 1..=max_retries {
        match verify_connection(credentials, host, port).await {
            Ok(info) => {
                tracing::info!(attempt = attempt, "PostgreSQL verification successful");
                return Ok(info);
            }
            Err(e) => {
                tracing::debug!(
                    attempt = attempt,
                    max_retries = max_retries,
                    error = %e,
                    "Connection attempt failed, retrying..."
                );
                last_error = Some(e);
                if attempt < max_retries {
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or(PostgresError::Timeout))
}

/// Parse PEM-encoded certificates into DER format
fn parse_pem_certificates(pem_data: &str) -> Result<Vec<CertificateDer<'static>>, PostgresError> {
    let mut certs = Vec::new();
    let mut reader = std::io::BufReader::new(pem_data.as_bytes());

    for cert in rustls_pemfile::certs(&mut reader) {
        match cert {
            Ok(cert) => certs.push(cert),
            Err(e) => {
                return Err(PostgresError::InvalidCertificate(format!(
                    "Failed to parse certificate: {}",
                    e
                )));
            }
        }
    }

    if certs.is_empty() {
        return Err(PostgresError::InvalidCertificate(
            "No certificates found in PEM data".to_string(),
        ));
    }

    Ok(certs)
}

/// Build a rustls TLS connector for PostgreSQL
fn build_tls_connector(tls_mode: &TlsMode) -> Result<MakeRustlsConnect, PostgresError> {
    match tls_mode {
        TlsMode::Disabled => Err(PostgresError::TlsConfig(
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
                    PostgresError::InvalidCertificate(format!("Failed to add CA cert: {}", e))
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

/// Verify PostgreSQL connection with TLS and return connection info
///
/// Similar to `verify_connection` but with TLS support.
pub async fn verify_connection_tls(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    tls_mode: &TlsMode,
) -> Result<ConnectionInfo, PostgresError> {
    let config = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=10",
        host, port, credentials.username, credentials.password, credentials.database
    );

    // Query connection info - same for both TLS and non-TLS
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

    match tls_mode {
        TlsMode::Disabled => {
            // Non-TLS connection
            let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::trace!(error = %e, "PostgreSQL connection closed");
                }
            });

            let row = client
                .query_one(query, &[])
                .await
                .map_err(|e| PostgresError::Query(e.to_string()))?;

            Ok(ConnectionInfo {
                pg_version: row.get("pg_version"),
                ssl_enabled: row.get("ssl_enabled"),
                ssl_version: row.get("ssl_version"),
                is_replica: row.get("is_replica"),
                replication_lag_secs: row.get("replication_lag_seconds"),
            })
        }
        _ => {
            // TLS connection
            let tls = build_tls_connector(tls_mode)?;
            let (client, connection) = tokio_postgres::connect(&config, tls).await?;

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::trace!(error = %e, "PostgreSQL TLS connection closed");
                }
            });

            let row = client
                .query_one(query, &[])
                .await
                .map_err(|e| PostgresError::Query(e.to_string()))?;

            Ok(ConnectionInfo {
                pg_version: row.get("pg_version"),
                ssl_enabled: row.get("ssl_enabled"),
                ssl_version: row.get("ssl_version"),
                is_replica: row.get("is_replica"),
                replication_lag_secs: row.get("replication_lag_seconds"),
            })
        }
    }
}

/// Verify TLS connection with retry logic
pub async fn verify_connection_with_retry_tls(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    tls_mode: &TlsMode,
    max_retries: u32,
    retry_interval: Duration,
) -> Result<ConnectionInfo, PostgresError> {
    let mut last_error = None;

    for attempt in 1..=max_retries {
        match verify_connection_tls(credentials, host, port, tls_mode).await {
            Ok(info) => {
                tracing::info!(
                    attempt = attempt,
                    ssl_enabled = info.ssl_enabled,
                    ssl_version = ?info.ssl_version,
                    "PostgreSQL TLS verification successful"
                );
                return Ok(info);
            }
            Err(e) => {
                tracing::debug!(
                    attempt = attempt,
                    max_retries = max_retries,
                    error = %e,
                    "TLS connection attempt failed, retrying..."
                );
                last_error = Some(e);
                if attempt < max_retries {
                    tokio::time::sleep(retry_interval).await;
                }
            }
        }
    }

    Err(last_error.unwrap_or(PostgresError::Timeout))
}

/// Execute a simple SQL statement (one-shot, for write tests)
pub async fn execute_sql(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    sql: &str,
) -> Result<(), PostgresError> {
    let config = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=10",
        host, port, credentials.username, credentials.password, credentials.database
    );

    let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::trace!(error = %e, "PostgreSQL connection closed");
        }
    });

    client
        .execute(sql, &[])
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    Ok(())
}

/// Timeout for PostgreSQL connection attempts
pub const POSTGRES_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Extended timeout for cluster readiness (includes PostgreSQL startup)
pub const POSTGRES_READY_TIMEOUT: Duration = Duration::from_secs(180);

/// Retry interval for connection attempts
pub const CONNECT_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Maximum connection retries
pub const MAX_CONNECT_RETRIES: u32 = 15;
