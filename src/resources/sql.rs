//! SQL execution via pod exec
//!
//! This module provides functionality to execute SQL commands in PostgreSQL pods
//! via the Kubernetes exec API. Used for database and role provisioning.

// SQL provisioning functions naturally have many parameters (connection info + DDL options)
#![allow(clippy::too_many_arguments)]

use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, AttachParams};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tracing::debug;

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
}

/// Result type for SQL operations
pub(crate) type SqlResult<T> = std::result::Result<T, SqlError>;

/// Execute SQL on the primary pod of a PostgresCluster
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `namespace` - Namespace of the cluster
/// * `cluster_name` - Name of the PostgresCluster
/// * `database` - Database to connect to (default: postgres)
/// * `sql` - SQL command to execute
///
/// # Returns
/// The output of the SQL command, or an error
pub(crate) async fn exec_sql(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database: &str,
    sql: &str,
) -> SqlResult<String> {
    // Find the primary pod
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);

    // Primary pods have the spilo-role=master label set by Patroni
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
        .as_ref()
        .ok_or_else(|| SqlError::ExecFailed("Pod has no name".to_string()))?;

    debug!(
        pod = %pod_name,
        namespace = %namespace,
        database = %database,
        "Executing SQL on primary pod"
    );

    // Execute psql command
    exec_psql(&pods, pod_name, database, sql).await
}

/// Execute psql command in a pod
async fn exec_psql(
    pods: &Api<Pod>,
    pod_name: &str,
    database: &str,
    sql: &str,
) -> SqlResult<String> {
    // Build the psql command
    // Use -A for unaligned output, -t for tuples only (no headers/footers)
    // -c executes the command
    let command = vec![
        "psql".to_string(),
        "-U".to_string(),
        "postgres".to_string(),
        "-d".to_string(),
        database.to_string(),
        "-A".to_string(),
        "-t".to_string(),
        "-c".to_string(),
        sql.to_string(),
    ];

    exec_command_in_pod(pods, pod_name, command).await
}

/// Execute an arbitrary command in a pod
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

/// Dump schema from a PostgreSQL database using pg_dump
///
/// This exports the schema (tables, sequences, constraints, etc.) without data.
/// The output is SQL DDL statements that can be executed on another database.
pub(crate) async fn dump_schema(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database: &str,
) -> SqlResult<String> {
    // Find the primary pod
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
        .as_ref()
        .ok_or_else(|| SqlError::ExecFailed("Pod has no name".to_string()))?;

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

    exec_command_in_pod(&pods, pod_name, command).await
}

/// Apply schema DDL to a PostgreSQL database using psql
///
/// This executes the DDL statements from a pg_dump output on the target database.
pub(crate) async fn apply_schema(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database: &str,
    schema_sql: &str,
) -> SqlResult<()> {
    // Find the primary pod
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
        .as_ref()
        .ok_or_else(|| SqlError::ExecFailed("Pod has no name".to_string()))?;

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

    let output = exec_command_in_pod(&pods, pod_name, command).await?;

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

/// Read all data from an async read stream
async fn read_stream<R: tokio::io::AsyncRead + Unpin>(mut reader: R) -> SqlResult<String> {
    use tokio::io::AsyncReadExt;

    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await?;
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Check if a database exists
pub(crate) async fn database_exists(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database_name: &str,
) -> SqlResult<bool> {
    let sql = format!(
        "SELECT 1 FROM pg_database WHERE datname = '{}'",
        escape_sql_string(database_name)
    );

    let result = exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(!result.trim().is_empty())
}

/// Check if a role exists
pub(crate) async fn role_exists(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    role_name: &str,
) -> SqlResult<bool> {
    let sql = format!(
        "SELECT 1 FROM pg_roles WHERE rolname = '{}'",
        escape_sql_string(role_name)
    );

    let result = exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(!result.trim().is_empty())
}

/// Ensure a database exists (idempotent - creates if not exists)
///
/// Returns true if the database was created, false if it already existed.
pub(crate) async fn ensure_database(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database_name: &str,
    owner: &str,
    encoding: Option<&str>,
    locale: Option<&str>,
    connection_limit: Option<i32>,
) -> SqlResult<bool> {
    if database_exists(client, namespace, cluster_name, database_name).await? {
        debug!(database = %database_name, "Database already exists");
        return Ok(false);
    }

    create_database(
        client,
        namespace,
        cluster_name,
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
pub(crate) async fn create_database(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
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

    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(())
}

/// Ensure a role exists (idempotent - creates if not exists)
///
/// Returns true if the role was created, false if it already existed.
/// If the role exists, the password is updated to match.
pub(crate) async fn ensure_role(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    role_name: &str,
    password: &str,
    privileges: &[String],
    connection_limit: Option<i32>,
    login: bool,
) -> SqlResult<bool> {
    if role_exists(client, namespace, cluster_name, role_name).await? {
        debug!(role = %role_name, "Role already exists, updating password");
        // Update password to ensure it matches the secret
        update_role_password(client, namespace, cluster_name, role_name, password).await?;
        return Ok(false);
    }

    create_role(
        client,
        namespace,
        cluster_name,
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
pub(crate) async fn create_role(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
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

    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(())
}

/// Update a role's password
pub(crate) async fn update_role_password(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    role_name: &str,
    password: &str,
) -> SqlResult<()> {
    let sql = format!(
        "ALTER ROLE {} WITH PASSWORD '{}'",
        quote_identifier(role_name),
        escape_sql_string(password)
    );

    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(())
}

/// Drop a role
pub(crate) async fn drop_role(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    role_name: &str,
) -> SqlResult<()> {
    let sql = format!("DROP ROLE IF EXISTS {}", quote_identifier(role_name));
    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(())
}

/// Drop a database
pub(crate) async fn drop_database(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database_name: &str,
) -> SqlResult<()> {
    let sql = format!(
        "DROP DATABASE IF EXISTS {}",
        quote_identifier(database_name)
    );
    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(())
}

/// Grant table privileges
pub(crate) async fn grant_privileges(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database_name: &str,
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

    exec_sql(client, namespace, cluster_name, database_name, &sql).await?;
    Ok(())
}

/// Create an extension
pub(crate) async fn create_extension(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    database_name: &str,
    extension_name: &str,
) -> SqlResult<()> {
    let sql = format!(
        "CREATE EXTENSION IF NOT EXISTS {}",
        quote_identifier(extension_name)
    );
    exec_sql(client, namespace, cluster_name, database_name, &sql).await?;
    Ok(())
}

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
