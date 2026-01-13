//! SQL execution for PostgreSQL operations
//!
//! This module provides functionality to execute SQL commands in PostgreSQL.
//! Uses direct PostgreSQL connections via tokio-postgres for most operations,
//! and pod exec for CLI tools like pg_dump.

// SQL provisioning functions naturally have many parameters (connection info + DDL options)
#![allow(clippy::too_many_arguments)]

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, AttachParams};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tracing::debug;

use super::postgres_client::{PostgresClientError, PostgresConnection};

/// Errors that can occur during SQL execution
#[derive(Error, Debug)]
pub enum SqlError {
    /// Failed to find primary pod
    #[error("No primary pod found for cluster {cluster} in namespace {namespace}")]
    NoPrimaryPod { cluster: String, namespace: String },

    /// Failed to execute command in pod
    #[error("Failed to execute command in pod: {0}")]
    ExecFailed(String),

    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    /// SQL execution returned an error
    #[error("SQL error: {0}")]
    SqlExecutionError(String),

    /// IO error during exec
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// PostgreSQL client error
    #[error("PostgreSQL client error: {0}")]
    PostgresClient(#[from] PostgresClientError),
}

/// Result type for SQL operations
pub(crate) type SqlResult<T> = std::result::Result<T, SqlError>;

// ============================================================================
// Pod Exec Functions (for CLI tools like pg_dump, psql)
// ============================================================================

/// Execute a command in a pod
async fn exec_command_in_pod(
    pods: &Api<Pod>,
    pod_name: &str,
    command: Vec<String>,
) -> SqlResult<String> {
    let attach_params = AttachParams {
        container: Some("postgres".to_string()),
        stdin: true,
        stdout: true,
        stderr: true,
        tty: false,
        ..Default::default()
    };

    let mut attached = pods.exec(pod_name, command, &attach_params).await?;

    // Close stdin to signal end of input
    if let Some(mut stdin) = attached.stdin() {
        stdin.shutdown().await?;
    }

    // Read stdout and stderr
    let stdout = attached
        .stdout()
        .ok_or_else(|| SqlError::ExecFailed("Failed to get stdout from exec".to_string()))?;

    let stderr = attached
        .stderr()
        .ok_or_else(|| SqlError::ExecFailed("Failed to get stderr from exec".to_string()))?;

    // Read output using tokio
    let stdout_output = read_stream(stdout).await?;
    let stderr_output = read_stream(stderr).await?;

    // Wait for the process to complete
    let status = attached
        .take_status()
        .ok_or_else(|| SqlError::ExecFailed("Failed to get status from exec".to_string()))?;

    if let Some(status) = status.await
        && status.status != Some("Success".to_string())
    {
        let error_msg = if stderr_output.is_empty() {
            format!("Command failed with status: {:?}", status)
        } else {
            stderr_output.clone()
        };
        return Err(SqlError::SqlExecutionError(error_msg));
    }

    // Check for SQL errors in stderr
    if !stderr_output.is_empty() && stderr_output.contains("ERROR") {
        return Err(SqlError::SqlExecutionError(stderr_output));
    }

    Ok(stdout_output)
}

/// Read all data from an async read stream
async fn read_stream<R: tokio::io::AsyncRead + Unpin>(mut reader: R) -> SqlResult<String> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Find the primary pod for a cluster
async fn find_primary_pod(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> SqlResult<(Api<Pod>, String)> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);

    let primary_selector = format!(
        "postgres-operator.smoketurner.com/cluster={},spilo-role=master",
        cluster_name
    );

    let pod_list = pods
        .list(&kube::api::ListParams::default().labels(&primary_selector))
        .await?;

    let primary_pod = pod_list
        .items
        .into_iter()
        .next()
        .ok_or_else(|| SqlError::NoPrimaryPod {
            cluster: cluster_name.to_string(),
            namespace: namespace.to_string(),
        })?;

    let pod_name = primary_pod
        .metadata
        .name
        .ok_or_else(|| SqlError::ExecFailed("Pod has no name".to_string()))?;

    Ok((pods, pod_name))
}

/// Dump schema from a PostgreSQL database using pg_dump
///
/// This exports the schema (tables, sequences, constraints, etc.) without data.
/// The output is SQL DDL statements that can be executed on another database.
///
/// Note: This uses pod exec because pg_dump is a CLI tool.
pub(crate) async fn dump_schema(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database: &str,
) -> SqlResult<String> {
    let (pods, pod_name) = find_primary_pod(client, namespace, cluster_name).await?;

    debug!(
        pod = %pod_name,
        namespace = %namespace,
        database = %database,
        "Dumping schema from primary pod"
    );

    // Run pg_dump with --schema-only to get just the DDL
    // Exclude pg_catalog and information_schema
    // Use --no-owner and --no-acl to make schema portable
    let command = vec![
        "pg_dump".to_string(),
        "-U".to_string(),
        "postgres".to_string(),
        "-d".to_string(),
        database.to_string(),
        "--schema-only".to_string(),
        "--no-owner".to_string(),
        "--no-acl".to_string(),
        // Exclude internal schemas
        "--exclude-schema=pg_catalog".to_string(),
        "--exclude-schema=information_schema".to_string(),
        // Exclude Spilo/Patroni-created schemas (already exist on target)
        "--exclude-schema=metric_helpers".to_string(),
        "--exclude-schema=admin".to_string(),
    ];

    exec_command_in_pod(&pods, &pod_name, command).await
}

/// Apply schema DDL to a PostgreSQL database using psql
///
/// This executes the DDL statements from a pg_dump output on the target database.
///
/// Note: This uses pod exec because psql handles multi-statement input better.
pub(crate) async fn apply_schema(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database: &str,
    schema_sql: &str,
) -> SqlResult<()> {
    let (pods, pod_name) = find_primary_pod(client, namespace, cluster_name).await?;

    debug!(
        pod = %pod_name,
        namespace = %namespace,
        database = %database,
        schema_len = schema_sql.len(),
        "Applying schema to primary pod"
    );

    // Use psql to execute the schema SQL
    // We pass the SQL via a shell command to handle multi-statement input
    // Don't use ON_ERROR_STOP since some objects might already exist (idempotent)
    // Pipe stderr through grep to filter out "already exists" errors
    let command = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!(
            "echo {} | psql -U postgres -d {} 2>&1 | grep -v 'already exists' | grep -v '^$'",
            shell_escape(schema_sql),
            database
        ),
    ];

    let output = exec_command_in_pod(&pods, &pod_name, command).await?;

    // Check if there are any real errors (not "already exists")
    if output.contains("ERROR:") {
        return Err(SqlError::SqlExecutionError(output));
    }

    Ok(())
}

/// Escape a string for use in a shell command
fn shell_escape(s: &str) -> String {
    // Use single quotes and escape any existing single quotes
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

// ============================================================================
// Direct SQL Functions (using PostgresConnection)
// ============================================================================

/// Check if a database exists
pub(crate) async fn database_exists(
    conn: &PostgresConnection,
    database_name: &str,
) -> SqlResult<bool> {
    let row = conn
        .query_opt(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            &[&database_name],
        )
        .await?;
    Ok(row.is_some())
}

/// Check if a role exists
pub(crate) async fn role_exists(conn: &PostgresConnection, role_name: &str) -> SqlResult<bool> {
    let row = conn
        .query_opt("SELECT 1 FROM pg_roles WHERE rolname = $1", &[&role_name])
        .await?;
    Ok(row.is_some())
}

/// Ensure a database exists (idempotent - creates if not exists)
///
/// Returns true if the database was created, false if it already existed.
pub(crate) async fn ensure_database(
    conn: &PostgresConnection,
    database_name: &str,
    owner: &str,
    encoding: Option<&str>,
    locale: Option<&str>,
    connection_limit: Option<i32>,
) -> SqlResult<bool> {
    if database_exists(conn, database_name).await? {
        debug!(database = %database_name, "Database already exists");
        return Ok(false);
    }

    create_database(
        conn,
        database_name,
        owner,
        encoding,
        locale,
        connection_limit,
    )
    .await?;
    Ok(true)
}

/// Create a database (not idempotent - will fail if database exists)
///
/// Prefer `ensure_database` for reconciliation loops.
///
/// Note: CREATE DATABASE doesn't support parameterized queries for identifiers,
/// so we use quote_identifier() to safely quote names.
pub(crate) async fn create_database(
    conn: &PostgresConnection,
    database_name: &str,
    owner: &str,
    encoding: Option<&str>,
    locale: Option<&str>,
    connection_limit: Option<i32>,
) -> SqlResult<()> {
    let mut sql = format!(
        "CREATE DATABASE {} OWNER {}",
        quote_identifier(database_name),
        quote_identifier(owner)
    );

    if let Some(enc) = encoding {
        sql.push_str(&format!(" ENCODING '{}'", escape_sql_string(enc)));
    }

    if let Some(loc) = locale {
        sql.push_str(&format!(" LOCALE '{}'", escape_sql_string(loc)));
    }

    if let Some(limit) = connection_limit {
        sql.push_str(&format!(" CONNECTION LIMIT {}", limit));
    }

    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Ensure a role exists (idempotent - creates if not exists)
///
/// Returns true if the role was created, false if it already existed.
/// If the role exists, the password is updated to match.
pub(crate) async fn ensure_role(
    conn: &PostgresConnection,
    role_name: &str,
    password: &str,
    privileges: &[String],
    connection_limit: Option<i32>,
    login: bool,
) -> SqlResult<bool> {
    if role_exists(conn, role_name).await? {
        debug!(role = %role_name, "Role already exists, updating password");
        // Update password to ensure it matches the secret
        update_role_password(conn, role_name, password).await?;
        return Ok(false);
    }

    create_role(
        conn,
        role_name,
        password,
        privileges,
        connection_limit,
        login,
    )
    .await?;
    Ok(true)
}

/// Create a role (not idempotent - will fail if role exists)
///
/// Prefer `ensure_role` for reconciliation loops.
///
/// Note: CREATE ROLE doesn't support parameterized queries for identifiers,
/// so we use quote_identifier() to safely quote names.
pub(crate) async fn create_role(
    conn: &PostgresConnection,
    role_name: &str,
    password: &str,
    privileges: &[String],
    connection_limit: Option<i32>,
    login: bool,
) -> SqlResult<()> {
    let mut sql = format!(
        "CREATE ROLE {} WITH PASSWORD '{}'",
        quote_identifier(role_name),
        escape_sql_string(password)
    );

    if login {
        sql.push_str(" LOGIN");
    }

    for privilege in privileges {
        sql.push_str(&format!(" {}", privilege));
    }

    if let Some(limit) = connection_limit {
        sql.push_str(&format!(" CONNECTION LIMIT {}", limit));
    }

    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Update a role's password
pub(crate) async fn update_role_password(
    conn: &PostgresConnection,
    role_name: &str,
    password: &str,
) -> SqlResult<()> {
    let sql = format!(
        "ALTER ROLE {} WITH PASSWORD '{}'",
        quote_identifier(role_name),
        escape_sql_string(password)
    );

    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Drop a role
pub(crate) async fn drop_role(conn: &PostgresConnection, role_name: &str) -> SqlResult<()> {
    let sql = format!("DROP ROLE IF EXISTS {}", quote_identifier(role_name));
    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Drop a database
pub(crate) async fn drop_database(conn: &PostgresConnection, database_name: &str) -> SqlResult<()> {
    let sql = format!(
        "DROP DATABASE IF EXISTS {}",
        quote_identifier(database_name)
    );
    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Grant table privileges
///
/// Note: GRANT doesn't support parameterized queries for identifiers.
pub(crate) async fn grant_privileges(
    conn: &PostgresConnection,
    role: &str,
    schema: &str,
    privileges: &[String],
    all_tables: bool,
) -> SqlResult<()> {
    if privileges.is_empty() {
        return Ok(());
    }

    let privs = privileges.join(", ");
    let target = if all_tables {
        format!("ALL TABLES IN SCHEMA {}", quote_identifier(schema))
    } else {
        format!("SCHEMA {}", quote_identifier(schema))
    };

    let sql = format!(
        "GRANT {} ON {} TO {}",
        privs,
        target,
        quote_identifier(role)
    );

    conn.batch_execute(&sql).await?;
    Ok(())
}

/// Create an extension
pub(crate) async fn create_extension(
    conn: &PostgresConnection,
    extension_name: &str,
) -> SqlResult<()> {
    let sql = format!(
        "CREATE EXTENSION IF NOT EXISTS {}",
        quote_identifier(extension_name)
    );
    conn.batch_execute(&sql).await?;
    Ok(())
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Quote a SQL identifier (table name, column name, etc.)
/// Uses PostgreSQL's standard double-quote escaping
///
/// This prevents SQL injection by ensuring special characters in identifiers
/// are properly escaped. For example:
/// - `my_table` -> `"my_table"`
/// - `table"name` -> `"table""name"`
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Public version of quote_identifier for use in other modules
pub fn quote_identifier_pub(name: &str) -> String {
    quote_identifier(name)
}

/// Escape a SQL string literal
/// Uses PostgreSQL's standard single-quote escaping
///
/// This prevents SQL injection in string values by doubling single quotes.
/// For example:
/// - `hello` -> `hello`
/// - `it's` -> `it''s`
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

/// Public version of escape_sql_string for use in other modules
pub fn escape_sql_string_pub(s: &str) -> String {
    escape_sql_string(s)
}

/// Validate that a name is safe for use as a PostgreSQL identifier
///
/// Returns true if the name matches the pattern: starts with letter or underscore,
/// followed by letters, digits, or underscores. Max length 63 characters.
pub fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }

    let mut chars = name.chars();

    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c == '_' => {}
        _ => return false,
    }

    // Rest must be letters, digits, or underscores
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Generate a secure random password
pub fn generate_password() -> String {
    use rand::Rng;
    const CHARSET: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";
    const PASSWORD_LEN: usize = 24;

    let mut rng = rand::rng();
    (0..PASSWORD_LEN)
        .filter_map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET.get(idx).map(|&c| c as char)
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "\"simple\"");
        assert_eq!(quote_identifier("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(quote_identifier("MixedCase"), "\"MixedCase\"");
    }

    #[test]
    fn test_escape_sql_string() {
        assert_eq!(escape_sql_string("simple"), "simple");
        assert_eq!(escape_sql_string("with'quote"), "with''quote");
        assert_eq!(
            escape_sql_string("multiple'single'quotes"),
            "multiple''single''quotes"
        );
    }

    #[test]
    fn test_generate_password() {
        let password = generate_password();
        assert_eq!(password.len(), 24);
        // Should be different each time
        let password2 = generate_password();
        assert_ne!(password, password2);
    }

    #[test]
    fn test_is_valid_identifier() {
        // Valid identifiers
        assert!(is_valid_identifier("mydb"));
        assert!(is_valid_identifier("my_database"));
        assert!(is_valid_identifier("db123"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("a"));

        // Invalid identifiers
        assert!(!is_valid_identifier("")); // Empty
        assert!(!is_valid_identifier("123db")); // Starts with number
        assert!(!is_valid_identifier("my-database")); // Contains hyphen
        assert!(!is_valid_identifier("my database")); // Contains space
        assert!(!is_valid_identifier("MyDatabase")); // Contains uppercase
        assert!(!is_valid_identifier("db;drop")); // Contains semicolon
        assert!(!is_valid_identifier("a".repeat(64).as_str())); // Too long (64 chars)

        // Edge case: exactly 63 characters is valid
        assert!(is_valid_identifier(&"a".repeat(63)));
    }

    #[test]
    fn test_sql_injection_prevention() {
        // These malicious inputs should be safely escaped
        assert_eq!(
            quote_identifier("users; DROP TABLE users;--"),
            "\"users; DROP TABLE users;--\""
        );
        assert_eq!(
            escape_sql_string("'; DROP TABLE users;--"),
            "''; DROP TABLE users;--"
        );
        assert_eq!(quote_identifier("test\"injection"), "\"test\"\"injection\"");
    }
}
