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

use crate::resources::sql::{SqlError, escape_sql_string_pub, exec_sql, quote_identifier_pub};

/// Errors that can occur during replication operations
#[derive(Error, Debug)]
pub enum ReplicationError {
    /// SQL execution failed
    #[error("SQL execution failed: {0}")]
    SqlError(#[from] SqlError),

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
    /// Catching up with changes made during copy
    CatchingUp,
    /// Subscription is actively replicating
    Streaming,
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
            "s" | "catchingup" | "catching_up" => SubscriptionState::CatchingUp,
            "r" | "streaming" | "ready" => SubscriptionState::Streaming,
            "" | "disabled" => SubscriptionState::Disabled,
            other => SubscriptionState::Unknown(other.to_string()),
        }
    }
}

impl SubscriptionState {
    /// Returns true if the subscription is actively replicating
    pub fn is_active(&self) -> bool {
        matches!(self, SubscriptionState::Streaming)
    }

    /// Returns true if the subscription is still syncing
    pub fn is_syncing(&self) -> bool {
        matches!(
            self,
            SubscriptionState::Initializing
                | SubscriptionState::CopyingData
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
/// * `client` - Kubernetes client
/// * `namespace` - Namespace of the source cluster
/// * `cluster_name` - Name of the source PostgresCluster
/// * `publication_name` - Name for the publication
///
/// # Returns
/// `true` if the publication was created, `false` if it already exists
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn setup_publication(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    publication_name: &str,
) -> ReplicationResult<bool> {
    // Check if publication already exists
    let check_sql = format!(
        "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
        escape_sql_string_pub(publication_name)
    );

    let result = exec_sql(client, namespace, cluster_name, "postgres", &check_sql).await?;

    if !result.trim().is_empty() {
        debug!(publication = %publication_name, "Publication already exists");
        return Ok(false);
    }

    // Create publication for all tables
    let create_sql = format!(
        "CREATE PUBLICATION {} FOR ALL TABLES",
        quote_identifier_pub(publication_name)
    );

    exec_sql(client, namespace, cluster_name, "postgres", &create_sql).await?;
    debug!(publication = %publication_name, "Publication created successfully");

    Ok(true)
}

/// Drop a publication from the source cluster
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn drop_publication(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    publication_name: &str,
) -> ReplicationResult<()> {
    let sql = format!(
        "DROP PUBLICATION IF EXISTS {}",
        quote_identifier_pub(publication_name)
    );

    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    debug!(publication = %publication_name, "Publication dropped");

    Ok(())
}

/// Check if a publication exists
pub async fn publication_exists(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    publication_name: &str,
) -> ReplicationResult<bool> {
    let sql = format!(
        "SELECT 1 FROM pg_publication WHERE pubname = '{}'",
        escape_sql_string_pub(publication_name)
    );

    let result = exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(!result.trim().is_empty())
}

// =============================================================================
// Subscription Management (Target Cluster)
// =============================================================================

/// Create a subscription on the target cluster
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `target_namespace` - Namespace of the target cluster
/// * `target_cluster_name` - Name of the target PostgresCluster
/// * `subscription_name` - Name for the subscription
/// * `source_host` - Hostname of the source cluster (typically the service name)
/// * `source_port` - Port of the source cluster
/// * `publication_name` - Name of the publication to subscribe to
/// * `source_password` - Password for the replication user
///
/// # Returns
/// `true` if the subscription was created, `false` if it already exists
#[allow(clippy::too_many_arguments)]
#[instrument(skip(client, source_password), fields(target_namespace = %target_namespace, target_cluster = %target_cluster_name))]
pub async fn setup_subscription(
    client: &Client,
    target_namespace: &str,
    target_cluster_name: &str,
    subscription_name: &str,
    source_host: &str,
    source_port: u16,
    publication_name: &str,
    source_password: &str,
) -> ReplicationResult<bool> {
    // Check if subscription already exists
    let check_sql = format!(
        "SELECT 1 FROM pg_subscription WHERE subname = '{}'",
        escape_sql_string_pub(subscription_name)
    );

    let result = exec_sql(
        client,
        target_namespace,
        target_cluster_name,
        "postgres",
        &check_sql,
    )
    .await?;

    if !result.trim().is_empty() {
        debug!(subscription = %subscription_name, "Subscription already exists");
        return Ok(false);
    }

    // Build connection string for the source
    let conn_string = format!(
        "host={} port={} dbname=postgres user=postgres password={}",
        source_host, source_port, source_password
    );

    // Create subscription
    // copy_data = true: Copy existing data first
    // create_slot = true: Create a replication slot on the source
    let create_sql = format!(
        "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {} WITH (copy_data = true, create_slot = true)",
        quote_identifier_pub(subscription_name),
        escape_sql_string_pub(&conn_string),
        quote_identifier_pub(publication_name)
    );

    exec_sql(
        client,
        target_namespace,
        target_cluster_name,
        "postgres",
        &create_sql,
    )
    .await?;
    debug!(subscription = %subscription_name, "Subscription created successfully");

    Ok(true)
}

/// Drop a subscription from the target cluster
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn drop_subscription(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    subscription_name: &str,
) -> ReplicationResult<()> {
    // First disable the subscription to release the replication slot
    let disable_sql = format!(
        "ALTER SUBSCRIPTION {} DISABLE",
        quote_identifier_pub(subscription_name)
    );

    // Ignore errors if subscription doesn't exist
    if let Err(e) = exec_sql(client, namespace, cluster_name, "postgres", &disable_sql).await {
        debug!(subscription = %subscription_name, error = %e, "Failed to disable subscription (may not exist)");
    }

    // Drop the replication slot association
    let drop_slot_sql = format!(
        "ALTER SUBSCRIPTION {} SET (slot_name = NONE)",
        quote_identifier_pub(subscription_name)
    );

    if let Err(e) = exec_sql(client, namespace, cluster_name, "postgres", &drop_slot_sql).await {
        debug!(subscription = %subscription_name, error = %e, "Failed to drop slot association");
    }

    // Drop the subscription
    let drop_sql = format!(
        "DROP SUBSCRIPTION IF EXISTS {}",
        quote_identifier_pub(subscription_name)
    );

    exec_sql(client, namespace, cluster_name, "postgres", &drop_sql).await?;
    debug!(subscription = %subscription_name, "Subscription dropped");

    Ok(())
}

/// Get the state of a subscription
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn get_subscription_state(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    subscription_name: &str,
) -> ReplicationResult<SubscriptionState> {
    // Query pg_stat_subscription for the subscription state
    // The srsubstate column contains the state:
    // 'i' = initializing, 'd' = data copy, 's' = syncing, 'r' = ready
    let sql = format!(
        r#"
        SELECT COALESCE(
            (SELECT srsubstate FROM pg_stat_subscription_stats
             WHERE subname = '{}'),
            (SELECT
                CASE
                    WHEN subenabled THEN 'r'
                    ELSE ''
                END
             FROM pg_subscription
             WHERE subname = '{}')
        ) as state
        "#,
        escape_sql_string_pub(subscription_name),
        escape_sql_string_pub(subscription_name)
    );

    let result = exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    let state_str = result.trim();

    if state_str.is_empty() {
        // Check if subscription exists at all
        let exists_sql = format!(
            "SELECT 1 FROM pg_subscription WHERE subname = '{}'",
            escape_sql_string_pub(subscription_name)
        );
        let exists = exec_sql(client, namespace, cluster_name, "postgres", &exists_sql).await?;

        if exists.trim().is_empty() {
            return Err(ReplicationError::SubscriptionNotFound(
                subscription_name.to_string(),
            ));
        }

        // Subscription exists but no state info - might be disabled
        return Ok(SubscriptionState::Disabled);
    }

    Ok(SubscriptionState::from(state_str))
}

/// Check if a subscription exists
pub async fn subscription_exists(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    subscription_name: &str,
) -> ReplicationResult<bool> {
    let sql = format!(
        "SELECT 1 FROM pg_subscription WHERE subname = '{}'",
        escape_sql_string_pub(subscription_name)
    );

    let result = exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    Ok(!result.trim().is_empty())
}

// =============================================================================
// Replication Monitoring
// =============================================================================

/// Get the current replication lag
#[instrument(skip(client), fields(source_namespace = %source_namespace, source_cluster = %source_cluster_name))]
pub async fn get_replication_lag(
    client: &Client,
    source_namespace: &str,
    source_cluster_name: &str,
    subscription_name: &str,
) -> ReplicationResult<LagStatus> {
    // Query the source cluster for replication slot lag
    let sql = format!(
        r#"
        SELECT
            pg_current_wal_lsn() as source_lsn,
            COALESCE(confirmed_flush_lsn, '0/0') as target_lsn,
            COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint as lag_bytes
        FROM pg_replication_slots
        WHERE slot_name = '{}'
        "#,
        escape_sql_string_pub(subscription_name)
    );

    let result = exec_sql(
        client,
        source_namespace,
        source_cluster_name,
        "postgres",
        &sql,
    )
    .await?;

    let line = result.trim();
    if line.is_empty() {
        return Err(ReplicationError::ReplicationSlotNotFound(
            subscription_name.to_string(),
        ));
    }

    // Parse the result (format: source_lsn|target_lsn|lag_bytes)
    let parts: Vec<&str> = line.split('|').collect();
    if parts.len() < 3 {
        return Err(ReplicationError::ParseError(format!(
            "Unexpected result format: {}",
            line
        )));
    }

    let source_lsn = parts
        .first()
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let target_lsn = parts
        .get(1)
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let lag_str = parts.get(2).map(|s| s.trim()).unwrap_or("0");
    let lag_bytes: i64 = lag_str
        .parse()
        .map_err(|_| ReplicationError::ParseError(format!("Invalid lag_bytes: {}", lag_str)))?;

    // Estimate lag in seconds based on typical write rate (rough estimate)
    // Assuming ~1MB/s write rate, adjust based on your workload
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
#[instrument(skip(client), fields(source_namespace = %source_namespace))]
pub async fn check_lsn_sync(
    client: &Client,
    source_namespace: &str,
    source_cluster_name: &str,
    subscription_name: &str,
) -> ReplicationResult<LsnSyncStatus> {
    let lag_status = get_replication_lag(
        client,
        source_namespace,
        source_cluster_name,
        subscription_name,
    )
    .await?;

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
/// Only tables in the 'public' schema are compared by default.
#[instrument(skip(client), fields(source_namespace = %source_namespace, target_namespace = %target_namespace))]
pub async fn verify_row_counts(
    client: &Client,
    source_namespace: &str,
    source_cluster_name: &str,
    target_namespace: &str,
    target_cluster_name: &str,
    tolerance: i64,
) -> ReplicationResult<RowCountVerification> {
    // Get table list and row counts from source
    let source_counts = get_table_row_counts(client, source_namespace, source_cluster_name).await?;

    // Get table list and row counts from target
    let target_counts = get_table_row_counts(client, target_namespace, target_cluster_name).await?;

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
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> ReplicationResult<std::collections::HashMap<String, i64>> {
    // Use pg_stat_user_tables for approximate counts (faster than COUNT(*))
    let sql = r#"
        SELECT
            schemaname || '.' || relname as table_name,
            n_live_tup as row_count
        FROM pg_stat_user_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY schemaname, relname
    "#;

    let result = exec_sql(client, namespace, cluster_name, "postgres", sql).await?;

    let mut counts = std::collections::HashMap::new();

    for line in result.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split('|').collect();
        if let (Some(name), Some(count_str)) = (parts.first(), parts.get(1)) {
            let table_name = name.trim().to_string();
            let row_count: i64 = count_str.trim().parse().unwrap_or(0);
            counts.insert(table_name, row_count);
        }
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
#[instrument(skip(client), fields(source_namespace = %source_namespace, target_namespace = %target_namespace))]
pub async fn sync_sequences(
    client: &Client,
    source_namespace: &str,
    source_cluster_name: &str,
    target_namespace: &str,
    target_cluster_name: &str,
) -> ReplicationResult<SequenceSyncResult> {
    // Get all sequences and their current values from source
    let source_sql = r#"
        SELECT
            schemaname || '.' || sequencename as seq_name,
            last_value
        FROM pg_sequences
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    "#;

    let source_result = exec_sql(
        client,
        source_namespace,
        source_cluster_name,
        "postgres",
        source_sql,
    )
    .await?;

    let mut total_sequences = 0;
    let mut synced_count = 0;
    let mut failures = Vec::new();

    for line in source_result.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 2 {
            continue;
        }

        let Some(seq_name) = parts.first().map(|s| s.trim()) else {
            continue;
        };
        let Some(last_value) = parts.get(1).map(|s| s.trim()) else {
            continue;
        };

        total_sequences += 1;

        // Parse schema and sequence name
        let name_parts: Vec<&str> = seq_name.split('.').collect();
        let (schema, sequence) = if name_parts.len() == 2 {
            (
                name_parts.first().copied().unwrap_or("public"),
                name_parts.get(1).copied().unwrap_or(seq_name),
            )
        } else {
            ("public", seq_name)
        };

        // Set the sequence value on target
        // Add a buffer to avoid conflicts with in-flight transactions
        let buffer = 1000i64;
        let target_value: i64 = last_value.parse().unwrap_or(0) + buffer;

        let set_sql = format!(
            "SELECT setval('{}.{}', {}, true)",
            escape_sql_string_pub(schema),
            escape_sql_string_pub(sequence),
            target_value
        );

        match exec_sql(
            client,
            target_namespace,
            target_cluster_name,
            "postgres",
            &set_sql,
        )
        .await
        {
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
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn set_source_readonly(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> ReplicationResult<()> {
    // Set default_transaction_read_only to prevent new writes
    let sql = "ALTER SYSTEM SET default_transaction_read_only = on";
    exec_sql(client, namespace, cluster_name, "postgres", sql).await?;

    // Reload configuration
    let reload_sql = "SELECT pg_reload_conf()";
    exec_sql(client, namespace, cluster_name, "postgres", reload_sql).await?;

    debug!(cluster = %cluster_name, "Source cluster set to read-only");
    Ok(())
}

/// Set the source cluster back to read-write mode (for rollback)
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn set_source_readwrite(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> ReplicationResult<()> {
    let sql = "ALTER SYSTEM SET default_transaction_read_only = off";
    exec_sql(client, namespace, cluster_name, "postgres", sql).await?;

    let reload_sql = "SELECT pg_reload_conf()";
    exec_sql(client, namespace, cluster_name, "postgres", reload_sql).await?;

    debug!(cluster = %cluster_name, "Source cluster set to read-write");
    Ok(())
}

/// Get the count of active connections to the source cluster
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn get_active_connections(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> ReplicationResult<i64> {
    let sql = r#"
        SELECT COUNT(*)
        FROM pg_stat_activity
        WHERE state = 'active'
        AND backend_type = 'client backend'
        AND pid != pg_backend_pid()
    "#;

    let result = exec_sql(client, namespace, cluster_name, "postgres", sql).await?;
    let count: i64 = result
        .trim()
        .parse()
        .map_err(|_| ReplicationError::ParseError(format!("Invalid count: {}", result)))?;

    Ok(count)
}

/// Wait for active connections to drain (with timeout)
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn wait_for_connections_drain(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    timeout_seconds: u64,
    poll_interval_seconds: u64,
) -> ReplicationResult<bool> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_seconds);
    let poll_interval = std::time::Duration::from_secs(poll_interval_seconds);

    loop {
        let connections = get_active_connections(client, namespace, cluster_name).await?;

        if connections == 0 {
            debug!(cluster = %cluster_name, "All connections drained");
            return Ok(true);
        }

        if start.elapsed() >= timeout {
            warn!(
                cluster = %cluster_name,
                remaining_connections = %connections,
                "Timeout waiting for connections to drain"
            );
            return Ok(false);
        }

        debug!(
            cluster = %cluster_name,
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
#[instrument(skip(client), fields(namespace = %namespace, cluster = %cluster_name))]
pub async fn drop_replication_slot(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    slot_name: &str,
) -> ReplicationResult<()> {
    let sql = format!(
        "SELECT pg_drop_replication_slot('{}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
        escape_sql_string_pub(slot_name),
        escape_sql_string_pub(slot_name)
    );

    exec_sql(client, namespace, cluster_name, "postgres", &sql).await?;
    debug!(slot = %slot_name, "Replication slot dropped");

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
