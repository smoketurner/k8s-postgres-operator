//! Logical replication management for PostgreSQL upgrades
//!
//! This module provides functionality to set up and manage PostgreSQL logical
//! replication between source and target clusters during blue-green upgrades.
//!
//! ## Overview
//!
//! Logical replication uses publications (source) and subscriptions (target) to
//! replicate data changes in real-time. This enables near-zero downtime upgrades
//! by keeping the target cluster in sync with the source.
//!
//! ## Key Operations
//!
//! - Publication management on source cluster
//! - Subscription management on target cluster
//! - Replication lag monitoring
//! - Row count verification
//! - Sequence synchronization

use kube::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, instrument, warn};

use crate::resources::postgres_client::{PostgresClientError, PostgresConnection};
use crate::resources::sql::{SqlError, apply_schema, dump_schema};

/// Errors that can occur during replication operations
#[derive(Error, Debug)]
pub enum ReplicationError {
    /// SQL execution failed
    #[error("SQL execution failed: {0}")]
    SqlError(#[from] SqlError),

    /// PostgreSQL client error
    #[error("PostgreSQL client error: {0}")]
    PostgresClient(#[from] PostgresClientError),

    /// Publication not found
    #[error("Publication not found: {0}")]
    PublicationNotFound(String),

    /// Subscription not found
    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(String),

    /// Subscription not ready
    #[error("Subscription not ready: {name}, state: {state}")]
    SubscriptionNotReady { name: String, state: String },

    /// Invalid LSN format
    #[error("Invalid LSN format: {0}")]
    InvalidLsn(String),

    /// Replication slot not found
    #[error("Replication slot not found: {0}")]
    ReplicationSlotNotFound(String),

    /// Parse error
    #[error("Failed to parse value: {0}")]
    ParseError(String),
}

/// Result type for replication operations
pub type ReplicationResult<T> = std::result::Result<T, ReplicationError>;

/// State of a PostgreSQL subscription
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscriptionState {
    /// Subscription is initializing
    Initializing,
    /// Initial data copy in progress
    CopyingData,
    /// Syncing - tables are being synchronized
    Syncing,
    /// Catching up with changes made during copy
    CatchingUp,
    /// Subscription is actively replicating (all tables synced, streaming changes)
    Active,
    /// Subscription is actively replicating (legacy alias for Active)
    Streaming,
    /// Subscription is inactive (enabled but worker not running)
    Inactive,
    /// Subscription is disabled
    Disabled,
    /// Unknown state
    Unknown(String),
}

impl From<&str> for SubscriptionState {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "i" | "initializing" => SubscriptionState::Initializing,
            "d" | "copyingdata" | "copying_data" => SubscriptionState::CopyingData,
            "syncing" => SubscriptionState::Syncing,
            "s" | "catchingup" | "catching_up" | "catchup" => SubscriptionState::CatchingUp,
            "r" | "streaming" | "ready" => SubscriptionState::Streaming,
            "active" => SubscriptionState::Active,
            "inactive" => SubscriptionState::Inactive,
            "" | "disabled" => SubscriptionState::Disabled,
            other => SubscriptionState::Unknown(other.to_string()),
        }
    }
}

impl SubscriptionState {
    /// Returns true if the subscription is actively replicating
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            SubscriptionState::Streaming | SubscriptionState::Active
        )
    }

    /// Returns true if the subscription is still syncing
    pub fn is_syncing(&self) -> bool {
        matches!(
            self,
            SubscriptionState::Initializing
                | SubscriptionState::CopyingData
                | SubscriptionState::Syncing
                | SubscriptionState::CatchingUp
        )
    }
}

/// Status of replication lag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LagStatus {
    /// Replication lag in bytes
    pub lag_bytes: i64,
    /// Estimated lag in seconds (based on write rate)
    pub lag_seconds: Option<i64>,
    /// Current LSN on source
    pub source_lsn: String,
    /// Current LSN on target
    pub target_lsn: String,
    /// Whether replication is in sync (lag is zero)
    pub in_sync: bool,
}

/// Status of LSN synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LsnSyncStatus {
    /// Source cluster's current write LSN
    pub source_lsn: String,
    /// Target cluster's received LSN
    pub target_lsn: String,
    /// Whether LSNs are in sync
    pub in_sync: bool,
    /// Lag in bytes between source and target
    pub lag_bytes: Option<i64>,
}

/// Result of row count verification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowCountVerification {
    /// Total number of tables checked
    pub tables_checked: i32,
    /// Number of tables with matching row counts
    pub tables_matched: i32,
    /// Number of tables with mismatched row counts
    pub tables_mismatched: i32,
    /// Details of mismatched tables
    pub mismatches: Vec<RowCountMismatch>,
}

/// Details of a row count mismatch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowCountMismatch {
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Row count in source
    pub source_count: i64,
    /// Row count in target
    pub target_count: i64,
    /// Difference (source - target)
    pub difference: i64,
}

/// Result of sequence synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceSyncResult {
    /// Total sequences processed
    pub total_sequences: i32,
    /// Sequences successfully synced
    pub synced_count: i32,
    /// Sequences that failed to sync
    pub failed_count: i32,
    /// Details of failed sequences
    pub failures: Vec<SequenceSyncFailure>,
}

/// Details of a sequence sync failure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceSyncFailure {
    /// Schema name
    pub schema: String,
    /// Sequence name
    pub sequence: String,
    /// Error message
    pub error: String,
}

// =============================================================================
// Publication Management (Source Cluster)
// =============================================================================

/// Create a publication on the source cluster for all tables
///
/// # Arguments
/// * `conn` - PostgreSQL connection to the source cluster
/// * `publication_name` - Name for the publication
///
/// # Returns
/// `true` if the publication was created, `false` if it already exists
#[instrument(skip(conn))]
pub async fn setup_publication(
    conn: &PostgresConnection,
    publication_name: &str,
) -> ReplicationResult<bool> {
    // Check if publication already exists
    let row = conn
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?;

    if row.is_some() {
        debug!(publication = %publication_name, "Publication already exists");
        return Ok(false);
    }

    // Create publication for all tables
    // Note: Publication names are identifiers - we use format! here because
    // CREATE PUBLICATION doesn't support parameterized identifiers
    let sql = format!(
        "CREATE PUBLICATION \"{}\" FOR ALL TABLES",
        publication_name.replace('"', "\"\"")
    );
    conn.batch_execute(&sql).await?;

    debug!(publication = %publication_name, "Publication created successfully");
    Ok(true)
}

/// Drop a publication from the source cluster
#[instrument(skip(conn))]
pub async fn drop_publication(
    conn: &PostgresConnection,
    publication_name: &str,
) -> ReplicationResult<()> {
    let sql = format!(
        "DROP PUBLICATION IF EXISTS \"{}\"",
        publication_name.replace('"', "\"\"")
    );
    conn.batch_execute(&sql).await?;

    debug!(publication = %publication_name, "Publication dropped");
    Ok(())
}

/// Check if a publication exists
pub async fn publication_exists(
    conn: &PostgresConnection,
    publication_name: &str,
) -> ReplicationResult<bool> {
    let row = conn
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await?;

    Ok(row.is_some())
}

// =============================================================================
// Subscription Management (Target Cluster)
// =============================================================================

/// Create a subscription on the target cluster
///
/// # Arguments
/// * `conn` - PostgreSQL connection to the target cluster
/// * `subscription_name` - Name for the subscription
/// * `source_host` - Hostname of the source cluster (typically the service name)
/// * `source_port` - Port of the source cluster
/// * `publication_name` - Name of the publication to subscribe to
/// * `source_password` - Password for the replication user
///
/// # Returns
/// `true` if the subscription was created, `false` if it already exists
#[allow(clippy::too_many_arguments)]
#[instrument(skip(conn, source_password))]
pub async fn setup_subscription(
    conn: &PostgresConnection,
    subscription_name: &str,
    source_host: &str,
    source_port: u16,
    publication_name: &str,
    source_password: &str,
) -> ReplicationResult<bool> {
    // Check if subscription already exists
    let row = conn
        .query_opt(
            "SELECT 1 FROM pg_subscription WHERE subname = $1",
            &[&subscription_name],
        )
        .await?;

    if row.is_some() {
        debug!(subscription = %subscription_name, "Subscription already exists");
        return Ok(false);
    }

    // Build connection string for the source
    let conn_string = format!(
        "host={} port={} dbname=postgres user=postgres password={}",
        source_host, source_port, source_password
    );

    // Create subscription
    // Note: Subscription names and connection strings need special handling
    let sql = format!(
        "CREATE SUBSCRIPTION \"{}\" CONNECTION '{}' PUBLICATION \"{}\" WITH (copy_data = true, create_slot = true)",
        subscription_name.replace('"', "\"\""),
        conn_string.replace('\'', "''"),
        publication_name.replace('"', "\"\"")
    );

    conn.batch_execute(&sql).await?;
    debug!(subscription = %subscription_name, "Subscription created successfully");

    Ok(true)
}

/// Drop a subscription from the target cluster
#[instrument(skip(conn))]
pub async fn drop_subscription(
    conn: &PostgresConnection,
    subscription_name: &str,
) -> ReplicationResult<()> {
    let escaped_name = subscription_name.replace('"', "\"\"");

    // First disable the subscription to release the replication slot
    let disable_sql = format!("ALTER SUBSCRIPTION \"{}\" DISABLE", escaped_name);
    if let Err(e) = conn.batch_execute(&disable_sql).await {
        debug!(subscription = %subscription_name, error = %e, "Failed to disable subscription (may not exist)");
    }

    // Drop the replication slot association
    let drop_slot_sql = format!(
        "ALTER SUBSCRIPTION \"{}\" SET (slot_name = NONE)",
        escaped_name
    );
    if let Err(e) = conn.batch_execute(&drop_slot_sql).await {
        debug!(subscription = %subscription_name, error = %e, "Failed to drop slot association");
    }

    // Drop the subscription
    let drop_sql = format!("DROP SUBSCRIPTION IF EXISTS \"{}\"", escaped_name);
    conn.batch_execute(&drop_sql).await?;

    debug!(subscription = %subscription_name, "Subscription dropped");
    Ok(())
}

/// Get the state of a subscription
#[instrument(skip(conn))]
pub async fn get_subscription_state(
    conn: &PostgresConnection,
    subscription_name: &str,
) -> ReplicationResult<SubscriptionState> {
    // Query subscription state using pg_subscription and pg_stat_subscription
    let row = conn
        .query_opt(
            r#"
            SELECT
                CASE
                    WHEN NOT EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $1) THEN 'not_found'
                    WHEN NOT (SELECT subenabled FROM pg_subscription WHERE subname = $1) THEN 'disabled'
                    WHEN (SELECT pid FROM pg_stat_subscription WHERE subname = $1 LIMIT 1) IS NULL THEN 'inactive'
                    WHEN EXISTS (
                        SELECT 1 FROM pg_subscription_rel sr
                        JOIN pg_subscription s ON s.oid = sr.srsubid
                        WHERE s.subname = $1 AND sr.srsubstate IN ('i', 'd')
                    ) THEN 'syncing'
                    WHEN EXISTS (
                        SELECT 1 FROM pg_subscription_rel sr
                        JOIN pg_subscription s ON s.oid = sr.srsubid
                        WHERE s.subname = $1 AND sr.srsubstate = 's'
                    ) THEN 'catchup'
                    ELSE 'active'
                END as state
            "#,
            &[&subscription_name],
        )
        .await?;

    let state_str: String = row
        .map(|r| r.get("state"))
        .unwrap_or_else(|| "not_found".to_string());

    match state_str.as_str() {
        "not_found" => Err(ReplicationError::SubscriptionNotFound(
            subscription_name.to_string(),
        )),
        "disabled" => Ok(SubscriptionState::Disabled),
        "inactive" => Ok(SubscriptionState::Inactive),
        "syncing" => Ok(SubscriptionState::Syncing),
        "catchup" => Ok(SubscriptionState::CatchingUp),
        _ => Ok(SubscriptionState::Active),
    }
}

/// Check if a subscription exists
pub async fn subscription_exists(
    conn: &PostgresConnection,
    subscription_name: &str,
) -> ReplicationResult<bool> {
    let row = conn
        .query_opt(
            "SELECT 1 FROM pg_subscription WHERE subname = $1",
            &[&subscription_name],
        )
        .await?;

    Ok(row.is_some())
}

// =============================================================================
// Schema Replication
// =============================================================================

/// Copy schema from source to target cluster
///
/// This is required before setting up logical replication because
/// PostgreSQL logical replication only replicates DML (data), not DDL (schema).
/// The tables, sequences, and other objects must exist on the target before
/// the subscription can sync data.
///
/// Note: This function uses pod exec for pg_dump since it's a CLI tool.
#[instrument(skip(client), fields(source_namespace = %source_namespace, target_namespace = %target_namespace))]
pub async fn copy_schema(
    client: &Client,
    source_namespace: &str,
    source_cluster_name: &str,
    target_namespace: &str,
    target_cluster_name: &str,
    database: &str,
) -> ReplicationResult<()> {
    debug!(
        source = %source_cluster_name,
        target = %target_cluster_name,
        database = %database,
        "Copying schema from source to target"
    );

    // Dump schema from source (uses pg_dump via pod exec)
    let schema_sql = dump_schema(client, source_namespace, source_cluster_name, database).await?;

    if schema_sql.trim().is_empty() {
        debug!("No user schema to copy (empty schema dump)");
        return Ok(());
    }

    debug!(
        schema_size = schema_sql.len(),
        "Schema dump completed, applying to target"
    );

    // Apply schema to target (uses psql via pod exec)
    apply_schema(
        client,
        target_namespace,
        target_cluster_name,
        database,
        &schema_sql,
    )
    .await?;

    debug!("Schema copy completed successfully");
    Ok(())
}

// =============================================================================
// Replication Monitoring
// =============================================================================

/// Get the current replication lag
#[instrument(skip(conn))]
pub async fn get_replication_lag(
    conn: &PostgresConnection,
    subscription_name: &str,
) -> ReplicationResult<LagStatus> {
    // Query the source cluster for replication slot lag
    let row = conn
        .query_opt(
            r#"
            SELECT
                pg_current_wal_lsn()::text as source_lsn,
                COALESCE(confirmed_flush_lsn::text, '0/0') as target_lsn,
                COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint as lag_bytes
            FROM pg_replication_slots
            WHERE slot_name = $1
            "#,
            &[&subscription_name],
        )
        .await?;

    let row = row
        .ok_or_else(|| ReplicationError::ReplicationSlotNotFound(subscription_name.to_string()))?;

    let source_lsn: String = row.get("source_lsn");
    let target_lsn: String = row.get("target_lsn");
    let lag_bytes: i64 = row.get("lag_bytes");

    // Estimate lag in seconds based on typical write rate (rough estimate)
    let lag_seconds = if lag_bytes > 0 {
        Some((lag_bytes / (1024 * 1024)).max(1))
    } else {
        Some(0)
    };

    Ok(LagStatus {
        lag_bytes,
        lag_seconds,
        source_lsn,
        target_lsn,
        in_sync: lag_bytes == 0,
    })
}

/// Check if source and target LSNs are in sync
#[instrument(skip(conn))]
pub async fn check_lsn_sync(
    conn: &PostgresConnection,
    subscription_name: &str,
) -> ReplicationResult<LsnSyncStatus> {
    let lag_status = get_replication_lag(conn, subscription_name).await?;

    Ok(LsnSyncStatus {
        source_lsn: lag_status.source_lsn,
        target_lsn: lag_status.target_lsn,
        in_sync: lag_status.in_sync,
        lag_bytes: Some(lag_status.lag_bytes),
    })
}

// =============================================================================
// Data Verification
// =============================================================================

/// Verify row counts between source and target clusters
///
/// This compares row counts for all user tables in both clusters.
#[instrument(skip(source_conn, target_conn))]
pub async fn verify_row_counts(
    source_conn: &PostgresConnection,
    target_conn: &PostgresConnection,
    tolerance: i64,
) -> ReplicationResult<RowCountVerification> {
    // Get table list and row counts from source
    let source_counts = get_table_row_counts(source_conn).await?;

    // Get table list and row counts from target
    let target_counts = get_table_row_counts(target_conn).await?;

    let mut tables_checked = 0;
    let mut tables_matched = 0;
    let mut mismatches = Vec::new();

    for (table_key, source_count) in &source_counts {
        tables_checked += 1;

        let target_count = target_counts.get(table_key).copied().unwrap_or(0);
        let difference = source_count - target_count;

        if difference.abs() <= tolerance {
            tables_matched += 1;
        } else {
            let parts: Vec<&str> = table_key.split('.').collect();
            let (schema, table) = if parts.len() == 2 {
                (
                    parts.first().unwrap_or(&"public").to_string(),
                    parts.get(1).unwrap_or(&"").to_string(),
                )
            } else {
                ("public".to_string(), table_key.clone())
            };

            mismatches.push(RowCountMismatch {
                schema,
                table,
                source_count: *source_count,
                target_count,
                difference,
            });
        }
    }

    // Check for tables in target that don't exist in source
    for (table_key, target_count) in &target_counts {
        if !source_counts.contains_key(table_key) {
            let parts: Vec<&str> = table_key.split('.').collect();
            let (schema, table) = if parts.len() == 2 {
                (
                    parts.first().unwrap_or(&"public").to_string(),
                    parts.get(1).unwrap_or(&"").to_string(),
                )
            } else {
                ("public".to_string(), table_key.clone())
            };

            if *target_count > tolerance {
                mismatches.push(RowCountMismatch {
                    schema,
                    table,
                    source_count: 0,
                    target_count: *target_count,
                    difference: -*target_count,
                });
            }
        }
    }

    Ok(RowCountVerification {
        tables_checked,
        tables_matched,
        tables_mismatched: mismatches.len() as i32,
        mismatches,
    })
}

/// Get row counts for all user tables in a cluster
async fn get_table_row_counts(
    conn: &PostgresConnection,
) -> ReplicationResult<std::collections::HashMap<String, i64>> {
    // Use pg_stat_user_tables for approximate counts (faster than COUNT(*))
    let rows = conn
        .query(
            r#"
            SELECT
                schemaname || '.' || relname as table_name,
                n_live_tup as row_count
            FROM pg_stat_user_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schemaname, relname
            "#,
            &[],
        )
        .await?;

    let mut counts = std::collections::HashMap::new();
    for row in rows {
        let table_name: String = row.get("table_name");
        let row_count: i64 = row.get("row_count");
        counts.insert(table_name, row_count);
    }

    Ok(counts)
}

// =============================================================================
// Sequence Synchronization
// =============================================================================

/// Synchronize sequences from source to target
///
/// This should be called after setting the source to read-only and before cutover.
/// It copies the current value of all sequences from source to target.
#[instrument(skip(source_conn, target_conn))]
pub async fn sync_sequences(
    source_conn: &PostgresConnection,
    target_conn: &PostgresConnection,
) -> ReplicationResult<SequenceSyncResult> {
    // Get all sequences and their current values from source
    let rows = source_conn
        .query(
            r#"
            SELECT
                schemaname || '.' || sequencename as seq_name,
                last_value
            FROM pg_sequences
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            "#,
            &[],
        )
        .await?;

    let mut total_sequences = 0;
    let mut synced_count = 0;
    let mut failures = Vec::new();

    for row in rows {
        let seq_name: String = row.get("seq_name");
        let last_value: Option<i64> = row.get("last_value");

        total_sequences += 1;

        // Parse schema and sequence name
        let name_parts: Vec<&str> = seq_name.split('.').collect();
        let (schema, sequence) = if name_parts.len() == 2 {
            (
                name_parts.first().copied().unwrap_or("public"),
                name_parts.get(1).copied().unwrap_or(&seq_name),
            )
        } else {
            ("public", seq_name.as_str())
        };

        // Set the sequence value on target
        // Add a buffer to avoid conflicts with in-flight transactions
        let buffer = 1000i64;
        let target_value: i64 = last_value.unwrap_or(0) + buffer;

        // setval requires the sequence name as a regclass, use format for the identifier
        let set_sql = format!(
            "SELECT setval('\"{}\".\"{}\"', $1, true)",
            schema.replace('"', "\"\""),
            sequence.replace('"', "\"\"")
        );

        match target_conn.execute(&set_sql, &[&target_value]).await {
            Ok(_) => {
                synced_count += 1;
                debug!(sequence = %seq_name, value = %target_value, "Sequence synchronized");
            }
            Err(e) => {
                warn!(sequence = %seq_name, error = %e, "Failed to sync sequence");
                failures.push(SequenceSyncFailure {
                    schema: schema.to_string(),
                    sequence: sequence.to_string(),
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(SequenceSyncResult {
        total_sequences,
        synced_count,
        failed_count: failures.len() as i32,
        failures,
    })
}

// =============================================================================
// Source Cluster Management
// =============================================================================

/// Set the source cluster to read-only mode
///
/// This should be called just before cutover to ensure no new writes
/// are made to the source cluster.
#[instrument(skip(conn))]
pub async fn set_source_readonly(conn: &PostgresConnection) -> ReplicationResult<()> {
    // Set default_transaction_read_only to prevent new writes
    conn.batch_execute("ALTER SYSTEM SET default_transaction_read_only = on")
        .await?;

    // Reload configuration
    conn.execute("SELECT pg_reload_conf()", &[]).await?;

    debug!("Source cluster set to read-only");
    Ok(())
}

/// Set the source cluster back to read-write mode (for rollback)
#[instrument(skip(conn))]
pub async fn set_source_readwrite(conn: &PostgresConnection) -> ReplicationResult<()> {
    conn.batch_execute("ALTER SYSTEM SET default_transaction_read_only = off")
        .await?;

    conn.execute("SELECT pg_reload_conf()", &[]).await?;

    debug!("Source cluster set to read-write");
    Ok(())
}

/// Get the count of active connections to the source cluster
#[instrument(skip(conn))]
pub async fn get_active_connections(conn: &PostgresConnection) -> ReplicationResult<i64> {
    let row = conn
        .query_one(
            r#"
            SELECT COUNT(*)::bigint as count
            FROM pg_stat_activity
            WHERE state = 'active'
            AND backend_type = 'client backend'
            AND pid != pg_backend_pid()
            "#,
            &[],
        )
        .await?;

    Ok(row.get("count"))
}

/// Wait for active connections to drain (with timeout)
#[instrument(skip(conn))]
pub async fn wait_for_connections_drain(
    conn: &PostgresConnection,
    timeout_seconds: u64,
    poll_interval_seconds: u64,
) -> ReplicationResult<bool> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_seconds);
    let poll_interval = std::time::Duration::from_secs(poll_interval_seconds);

    loop {
        let connections = get_active_connections(conn).await?;

        if connections == 0 {
            debug!("All connections drained");
            return Ok(true);
        }

        if start.elapsed() >= timeout {
            warn!(
                remaining_connections = %connections,
                "Timeout waiting for connections to drain"
            );
            return Ok(false);
        }

        debug!(
            connections = %connections,
            "Waiting for connections to drain"
        );

        tokio::time::sleep(poll_interval).await;
    }
}

// =============================================================================
// Cleanup Operations
// =============================================================================

/// Clean up replication slot on the source cluster
#[instrument(skip(conn))]
pub async fn drop_replication_slot(
    conn: &PostgresConnection,
    slot_name: &str,
) -> ReplicationResult<()> {
    // Check if slot exists before trying to drop
    let row = conn
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await?;

    if row.is_some() {
        // Use format! for the function call since pg_drop_replication_slot takes text
        conn.execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
            .await?;
        debug!(slot = %slot_name, "Replication slot dropped");
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_state_from_str() {
        assert_eq!(
            SubscriptionState::from("i"),
            SubscriptionState::Initializing
        );
        assert_eq!(SubscriptionState::from("d"), SubscriptionState::CopyingData);
        assert_eq!(SubscriptionState::from("s"), SubscriptionState::CatchingUp);
        assert_eq!(SubscriptionState::from("r"), SubscriptionState::Streaming);
        assert_eq!(SubscriptionState::from(""), SubscriptionState::Disabled);
        assert!(matches!(
            SubscriptionState::from("x"),
            SubscriptionState::Unknown(_)
        ));
    }

    #[test]
    fn test_subscription_state_is_active() {
        assert!(SubscriptionState::Streaming.is_active());
        assert!(!SubscriptionState::CopyingData.is_active());
        assert!(!SubscriptionState::Disabled.is_active());
    }

    #[test]
    fn test_subscription_state_is_syncing() {
        assert!(SubscriptionState::Initializing.is_syncing());
        assert!(SubscriptionState::CopyingData.is_syncing());
        assert!(SubscriptionState::CatchingUp.is_syncing());
        assert!(!SubscriptionState::Streaming.is_syncing());
        assert!(!SubscriptionState::Disabled.is_syncing());
    }

    #[test]
    fn test_row_count_verification_structure() {
        let verification = RowCountVerification {
            tables_checked: 10,
            tables_matched: 8,
            tables_mismatched: 2,
            mismatches: vec![
                RowCountMismatch {
                    schema: "public".to_string(),
                    table: "users".to_string(),
                    source_count: 1000,
                    target_count: 998,
                    difference: 2,
                },
                RowCountMismatch {
                    schema: "public".to_string(),
                    table: "orders".to_string(),
                    source_count: 5000,
                    target_count: 4990,
                    difference: 10,
                },
            ],
        };

        assert_eq!(verification.tables_checked, 10);
        assert_eq!(verification.tables_matched, 8);
        assert_eq!(verification.tables_mismatched, 2);
        assert_eq!(verification.mismatches.len(), 2);
    }

    #[test]
    fn test_lag_status_structure() {
        let lag_status = LagStatus {
            lag_bytes: 1024,
            lag_seconds: Some(1),
            source_lsn: "0/1000000".to_string(),
            target_lsn: "0/0FF0000".to_string(),
            in_sync: false,
        };

        assert_eq!(lag_status.lag_bytes, 1024);
        assert!(!lag_status.in_sync);

        let synced_status = LagStatus {
            lag_bytes: 0,
            lag_seconds: Some(0),
            source_lsn: "0/1000000".to_string(),
            target_lsn: "0/1000000".to_string(),
            in_sync: true,
        };

        assert!(synced_status.in_sync);
    }

    #[test]
    fn test_sequence_sync_result_structure() {
        let result = SequenceSyncResult {
            total_sequences: 5,
            synced_count: 4,
            failed_count: 1,
            failures: vec![SequenceSyncFailure {
                schema: "public".to_string(),
                sequence: "broken_seq".to_string(),
                error: "Permission denied".to_string(),
            }],
        };

        assert_eq!(result.total_sequences, 5);
        assert_eq!(result.synced_count, 4);
        assert_eq!(result.failed_count, 1);
    }
}
