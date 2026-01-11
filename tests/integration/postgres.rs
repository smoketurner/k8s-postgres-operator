//! PostgreSQL connection utilities for integration tests
//!
//! Provides utilities for connecting to PostgreSQL via port-forwarding
//! and verifying connection properties (version, TLS, replication status).

use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use std::time::Duration;
use thiserror::Error;
use tokio_postgres::NoTls;

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

    let secret = secrets
        .get(secret_name)
        .await
        .map_err(|e| match &e {
            kube::Error::Api(api_err) if api_err.code == 404 => {
                PostgresError::SecretNotFound(secret_name.to_string())
            }
            _ => PostgresError::Kube(e),
        })?;

    PostgresCredentials::from_secret(&secret)
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
