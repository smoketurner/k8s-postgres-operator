//! PostgreSQL connection and verification utilities for integration tests.
//!
//! This module provides test-specific helpers for connecting to and verifying
//! PostgreSQL instances created by the operator.
//!
//! # Usage
//!
//! ```ignore
//! // Fetch credentials from a secret
//! let creds = fetch_credentials(client.clone(), namespace, "my-cluster-credentials").await?;
//!
//! // Verify connection (non-TLS)
//! let info = verify_connection(&creds, "my-cluster", 5432).await?;
//! info.assert_version(17);
//! info.assert_is_primary();
//!
//! // Verify connection with TLS
//! let info = verify_connection_tls(&creds, host, port, &TlsMode::RequireUnverified).await?;
//! info.assert_tls(true);
//!
//! // Verify connection with retry
//! let info = verify_connection_with_retry(&creds, host, port, 10, Duration::from_secs(2)).await?;
//!
//! // Execute SQL
//! execute_sql(&creds, host, port, "CREATE TABLE test (id int)").await?;
//!
//! // Query a single value
//! let count: Option<i64> = query_single_value(&creds, host, port, "SELECT count(*) FROM test").await?;
//! ```
//!
//! # Re-exports
//!
//! Core types are re-exported from the production module:
//! - [`PostgresCredentials`] - Database credentials
//! - [`TlsMode`] - TLS configuration options
//! - [`fetch_credentials`] - Fetch credentials from Kubernetes secret
//!
//! # Database Verification
//!
//! For testing PostgresDatabase provisioning:
//! - [`verify_database_exists`] - Check if a database was created
//! - [`verify_role_exists`] - Check if a role was created
//! - [`query_extensions`] - List installed extensions
//! - [`verify_schema_usage`] - Check role has schema privileges

use std::time::Duration;
use thiserror::Error;
use tokio_postgres::NoTls;

// Re-export core types needed by tests
pub use postgres_operator::resources::postgres_client::{
    CONNECT_RETRY_INTERVAL, DEFAULT_CLUSTER_ISSUER, MAX_CONNECT_RETRIES, POSTGRES_READY_TIMEOUT,
    PostgresClientError, PostgresCredentials, TEST_CLUSTER_ISSUER_ENV, TlsMode,
    fetch_ca_certificate, fetch_credentials, fetch_role_credentials, get_test_cluster_issuer,
};

/// Test-specific error type that wraps production errors
#[derive(Error, Debug)]
pub enum PostgresError {
    #[error("Connection failed: {0}")]
    Connection(#[from] tokio_postgres::Error),

    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Production error: {0}")]
    Client(#[from] PostgresClientError),

    #[error("Query failed: {0}")]
    Query(String),

    #[error("Connection timeout")]
    Timeout,

    #[error("Failed to parse version from: {0}")]
    VersionParse(String),
}

/// Information about a PostgreSQL connection obtained after verification.
///
/// Contains details about the PostgreSQL version, TLS status, and replication
/// state. Use the assertion methods to verify expected values in tests.
///
/// # Example
/// ```ignore
/// let info = verify_connection(&creds, host, port).await?;
/// info.assert_version(17);
/// info.assert_tls(true);
/// info.assert_is_primary();
/// ```
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
    }
}

/// Verify PostgreSQL connection and return connection info
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

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::trace!(error = %e, "PostgreSQL connection closed");
        }
    });

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

    Ok(ConnectionInfo {
        pg_version: row.get("pg_version"),
        ssl_enabled: row.get("ssl_enabled"),
        ssl_version: row.get("ssl_version"),
        is_replica: row.get("is_replica"),
        replication_lag_secs: row.get("replication_lag_seconds"),
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

/// Verify PostgreSQL connection with TLS and return connection info
pub async fn verify_connection_tls(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    tls_mode: &TlsMode,
) -> Result<ConnectionInfo, PostgresError> {
    use std::sync::Arc;
    use tokio_postgres_rustls_improved::MakeRustlsConnect;

    let config = format!(
        "host={} port={} user={} password={} dbname={} connect_timeout=10",
        host, port, credentials.username, credentials.password, credentials.database
    );

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
        TlsMode::RequireUnverified => {
            let tls_config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier))
                .with_no_client_auth();

            let tls = MakeRustlsConnect::new(tls_config);
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
        TlsMode::RequireVerified { ca_cert_pem } => {
            let certs = parse_pem_certificates(ca_cert_pem)?;
            let mut root_store = rustls::RootCertStore::empty();
            for cert in certs {
                root_store
                    .add(cert)
                    .map_err(|e| PostgresError::Query(format!("Failed to add CA cert: {}", e)))?;
            }

            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let tls = MakeRustlsConnect::new(tls_config);
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

/// Parse PEM-encoded certificates into DER format
fn parse_pem_certificates(
    pem_data: &str,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, PostgresError> {
    let mut certs = Vec::new();
    let mut reader = std::io::BufReader::new(pem_data.as_bytes());

    for cert in rustls_pemfile::certs(&mut reader) {
        match cert {
            Ok(cert) => certs.push(cert),
            Err(e) => {
                return Err(PostgresError::Query(format!(
                    "Failed to parse certificate: {}",
                    e
                )));
            }
        }
    }

    if certs.is_empty() {
        return Err(PostgresError::Query(
            "No certificates found in PEM data".to_string(),
        ));
    }

    Ok(certs)
}

/// Custom certificate verifier that accepts any certificate
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
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

// =============================================================================
// SQL Verification Helpers for PostgresDatabase Tests
// =============================================================================

/// Verify that a database exists in PostgreSQL
pub async fn verify_database_exists(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    database_name: &str,
) -> Result<bool, PostgresError> {
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

    let row = client
        .query_opt(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            &[&database_name],
        )
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    Ok(row.is_some())
}

/// Verify that a role exists in PostgreSQL
pub async fn verify_role_exists(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    role_name: &str,
) -> Result<bool, PostgresError> {
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

    let row = client
        .query_opt("SELECT 1 FROM pg_roles WHERE rolname = $1", &[&role_name])
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    Ok(row.is_some())
}

/// Query installed extensions in a database
pub async fn query_extensions(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
) -> Result<Vec<String>, PostgresError> {
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

    let rows = client
        .query(
            "SELECT extname FROM pg_extension WHERE extname NOT IN ('plpgsql')",
            &[],
        )
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    Ok(rows.iter().map(|r| r.get("extname")).collect())
}

/// Verify that a role has USAGE privilege on a schema
pub async fn verify_schema_usage(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    role_name: &str,
    schema_name: &str,
) -> Result<bool, PostgresError> {
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

    let row = client
        .query_one(
            "SELECT has_schema_privilege($1, $2, 'USAGE')",
            &[&role_name, &schema_name],
        )
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    Ok(row.get(0))
}

/// Decode a single value from secret data
pub fn decode_secret_value(
    data: &std::collections::BTreeMap<String, k8s_openapi::ByteString>,
    key: &str,
) -> Option<String> {
    data.get(key)
        .and_then(|v| String::from_utf8(v.0.clone()).ok())
}

/// Execute a query and return the first column of the first row
pub async fn query_single_value<T>(
    credentials: &PostgresCredentials,
    host: &str,
    port: u16,
    sql: &str,
) -> Result<Option<T>, PostgresError>
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
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

    let row = client
        .query_opt(sql, &[])
        .await
        .map_err(|e| PostgresError::Query(e.to_string()))?;

    Ok(row.map(|r| r.get(0)))
}
