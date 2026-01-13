//! PostgresUpgrade CRD for blue-green major version upgrades using logical replication.
//!
//! This CRD enables near-zero downtime PostgreSQL major version upgrades by:
//! 1. Creating a new cluster with the target version
//! 2. Setting up logical replication from source to target
//! 3. Verifying data integrity through row count comparison
//! 4. Performing an atomic cutover when replication is in sync
//!
//! # Safety First Design
//!
//! The upgrade mechanism follows safety-first principles:
//! - Zero replication lag required before cutover (by default)
//! - Row count verification enabled by default
//! - Backup required for automatic cutover mode
//! - Source cluster never auto-deleted (indefinite rollback window)
//! - Automated rollback via annotation support

use crate::crd::{Condition, PostgresVersion, ResourceRequirements};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// PostgresUpgrade is the Schema for managing PostgreSQL major version upgrades
/// using blue-green deployment with logical replication.
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "postgres-operator.smoketurner.com",
    version = "v1alpha1",
    kind = "PostgresUpgrade",
    plural = "postgresupgrades",
    shortname = "pgu",
    namespaced,
    status = "PostgresUpgradeStatus",
    printcolumn = r#"{"name":"Source", "type":"string", "jsonPath":".spec.sourceCluster.name"}"#,
    printcolumn = r#"{"name":"TargetVer", "type":"string", "jsonPath":".spec.targetVersion"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Lag", "type":"string", "jsonPath":".status.replication.lagSeconds"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresUpgradeSpec {
    /// Reference to the source PostgresCluster to upgrade from.
    /// The source cluster must be in Running phase.
    pub source_cluster: ClusterReference,

    /// Target PostgreSQL version (must be higher than source, Spilo-supported: 15, 16, 17).
    /// Downgrades are rejected during validation.
    pub target_version: PostgresVersion,

    /// Optional overrides for the target cluster configuration.
    /// If not specified, the target cluster inherits the source cluster's spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_cluster_overrides: Option<TargetClusterOverrides>,

    /// Upgrade strategy and behavior configuration.
    #[serde(default)]
    pub strategy: UpgradeStrategy,
}

/// Reference to a PostgresCluster resource
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ClusterReference {
    /// Name of the PostgresCluster
    pub name: String,

    /// Namespace of the PostgresCluster (defaults to upgrade's namespace if not specified)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Optional configuration overrides for the target cluster
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct TargetClusterOverrides {
    /// Number of replicas for the target cluster
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,

    /// Resource requirements override for the target cluster
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Additional labels for the target cluster
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// Upgrade strategy configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeStrategy {
    /// Type of upgrade strategy (currently only BlueGreen is supported)
    #[serde(default)]
    pub strategy_type: UpgradeStrategyType,

    /// Cutover configuration
    #[serde(default)]
    pub cutover: CutoverConfig,

    /// Pre-cutover safety checks
    #[serde(default)]
    pub pre_checks: PreChecksConfig,

    /// Timeouts for various upgrade phases
    #[serde(default)]
    pub timeouts: UpgradeTimeouts,

    /// Post-cutover behavior
    #[serde(default)]
    pub post_cutover: PostCutoverConfig,
}

/// Type of upgrade strategy
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, Default, PartialEq, Eq)]
pub enum UpgradeStrategyType {
    /// Blue-Green deployment using logical replication
    #[default]
    BlueGreen,
}

/// Cutover configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CutoverConfig {
    /// Cutover mode: Manual requires annotation, Automatic proceeds when checks pass
    #[serde(default)]
    pub mode: CutoverMode,

    /// Optional maintenance window for automatic cutover.
    /// If specified, automatic cutover only happens within this window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_window: Option<MaintenanceWindow>,
}

impl Default for CutoverConfig {
    fn default() -> Self {
        Self {
            mode: CutoverMode::Manual,
            allowed_window: None,
        }
    }
}

/// Cutover mode
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, Default, PartialEq, Eq)]
pub enum CutoverMode {
    /// Manual cutover requires `cutover: now` annotation
    #[default]
    Manual,
    /// Automatic cutover proceeds when all pre-checks pass
    Automatic,
}

/// Maintenance window for automatic cutover
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaintenanceWindow {
    /// Start time in 24h format (e.g., "02:00")
    pub start_time: String,

    /// End time in 24h format (e.g., "04:00")
    pub end_time: String,

    /// Timezone (e.g., "UTC", "America/New_York")
    #[serde(default = "default_timezone")]
    pub timezone: String,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

/// Pre-cutover safety checks configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreChecksConfig {
    /// Maximum replication lag in seconds before cutover is allowed.
    /// Default is 0 (strictest - no cutover until fully synced).
    #[serde(default)]
    pub max_replication_lag_seconds: i32,

    /// Whether to verify row counts match between source and target.
    /// Enabled by default for data integrity.
    #[serde(default = "default_true")]
    pub verify_row_counts: bool,

    /// Tolerance for row count differences (absolute number).
    /// Default is 0 (exact match required).
    #[serde(default)]
    pub row_count_tolerance: i64,

    /// Number of consecutive verification passes required.
    /// Default is 3 (row counts must match N times in a row).
    #[serde(default = "default_verification_passes")]
    pub min_verification_passes: i32,

    /// Interval between verification passes.
    /// Default is "1m".
    #[serde(default = "default_verification_interval")]
    pub verification_interval: String,

    /// Require a backup within this duration before cutover.
    /// REQUIRED for Automatic mode, recommended for Manual mode.
    /// Default is "1h".
    #[serde(default = "default_backup_within")]
    pub require_backup_within: String,

    /// Timeout for draining active connections before cutover.
    /// Default is "5m".
    #[serde(default = "default_drain_timeout")]
    pub drain_connections_timeout: String,
}

impl Default for PreChecksConfig {
    fn default() -> Self {
        Self {
            max_replication_lag_seconds: 0,
            verify_row_counts: true,
            row_count_tolerance: 0,
            min_verification_passes: 3,
            verification_interval: "1m".to_string(),
            require_backup_within: "1h".to_string(),
            drain_connections_timeout: "5m".to_string(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_verification_passes() -> i32 {
    3
}

fn default_verification_interval() -> String {
    "1m".to_string()
}

fn default_backup_within() -> String {
    "1h".to_string()
}

fn default_drain_timeout() -> String {
    "5m".to_string()
}

/// Timeouts for upgrade phases to prevent silent hangs
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeTimeouts {
    /// Timeout for target cluster to become ready.
    /// Default is "30m".
    #[serde(default = "default_target_ready_timeout")]
    pub target_cluster_ready: String,

    /// Timeout for initial data sync via logical replication.
    /// Default is "24h" (for large databases).
    #[serde(default = "default_initial_sync_timeout")]
    pub initial_sync: String,

    /// Timeout for replication to catch up after initial sync.
    /// Default is "1h".
    #[serde(default = "default_catchup_timeout")]
    pub replication_catchup: String,

    /// Timeout for verification to pass.
    /// Default is "30m".
    #[serde(default = "default_verification_timeout")]
    pub verification: String,
}

impl Default for UpgradeTimeouts {
    fn default() -> Self {
        Self {
            target_cluster_ready: "30m".to_string(),
            initial_sync: "24h".to_string(),
            replication_catchup: "1h".to_string(),
            verification: "30m".to_string(),
        }
    }
}

fn default_target_ready_timeout() -> String {
    "30m".to_string()
}

fn default_initial_sync_timeout() -> String {
    "24h".to_string()
}

fn default_catchup_timeout() -> String {
    "1h".to_string()
}

fn default_verification_timeout() -> String {
    "30m".to_string()
}

/// Post-cutover behavior configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PostCutoverConfig {
    /// Whether to keep the source cluster after cutover.
    /// Always true - source is never auto-deleted for rollback safety.
    #[serde(default = "default_true")]
    pub keep_source_cluster: bool,

    /// Minimum retention period before source cleanup is allowed.
    /// Default is "24h".
    #[serde(default = "default_retention")]
    pub min_retention_period: String,

    /// Interval for health checks after cutover.
    /// Default is "1m".
    #[serde(default = "default_health_check_interval")]
    pub health_check_interval: String,

    /// Duration for health checks after cutover.
    /// Default is "10m".
    #[serde(default = "default_health_check_duration")]
    pub health_check_duration: String,
}

impl Default for PostCutoverConfig {
    fn default() -> Self {
        Self {
            keep_source_cluster: true,
            min_retention_period: "24h".to_string(),
            health_check_interval: "1m".to_string(),
            health_check_duration: "10m".to_string(),
        }
    }
}

fn default_retention() -> String {
    "24h".to_string()
}

fn default_health_check_interval() -> String {
    "1m".to_string()
}

fn default_health_check_duration() -> String {
    "10m".to_string()
}

/// Status of the PostgresUpgrade
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresUpgradeStatus {
    /// Current phase of the upgrade lifecycle
    #[serde(default)]
    pub phase: UpgradePhase,

    /// Observed generation of the resource
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Timestamp when the upgrade started (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,

    /// Timestamp when the upgrade completed (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,

    /// Timestamp when the current phase started (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_started_at: Option<String>,

    /// Status message for the current phase
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Timestamp when cutover started (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cutover_started_at: Option<String>,

    /// Source cluster information
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cluster: Option<ClusterStatus>,

    /// Target cluster information
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_cluster: Option<ClusterStatus>,

    /// Replication status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<ReplicationStatus>,

    /// Row count verification status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<VerificationStatus>,

    /// Sequence synchronization status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequences: Option<SequenceSyncStatus>,

    /// Rollback feasibility status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback: Option<RollbackStatus>,

    /// Kubernetes-style conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Current retry count for exponential backoff
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i32>,

    /// Last error message encountered during reconciliation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    /// Timestamp of the last error
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_time: Option<String>,
}

/// Upgrade lifecycle phase
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, Default, PartialEq, Eq, Hash)]
pub enum UpgradePhase {
    /// Upgrade is pending validation
    #[default]
    Pending,
    /// Creating target cluster with new version
    CreatingTarget,
    /// Configuring logical replication (publication/subscription)
    ConfiguringReplication,
    /// Replication active, syncing data
    Replicating,
    /// Verifying row counts and data integrity
    Verifying,
    /// Synchronizing sequences (after source read-only)
    SyncingSequences,
    /// All checks passed, ready for cutover
    ReadyForCutover,
    /// Waiting for manual cutover annotation (Manual mode only)
    WaitingForManualCutover,
    /// Cutover in progress
    CuttingOver,
    /// Post-cutover health checking
    HealthChecking,
    /// Upgrade completed successfully
    Completed,
    /// Upgrade failed (see conditions for details)
    Failed,
    /// Rolled back to source cluster
    RolledBack,
}

impl std::fmt::Display for UpgradePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpgradePhase::Pending => write!(f, "Pending"),
            UpgradePhase::CreatingTarget => write!(f, "CreatingTarget"),
            UpgradePhase::ConfiguringReplication => write!(f, "ConfiguringReplication"),
            UpgradePhase::Replicating => write!(f, "Replicating"),
            UpgradePhase::Verifying => write!(f, "Verifying"),
            UpgradePhase::SyncingSequences => write!(f, "SyncingSequences"),
            UpgradePhase::ReadyForCutover => write!(f, "ReadyForCutover"),
            UpgradePhase::WaitingForManualCutover => write!(f, "WaitingForManualCutover"),
            UpgradePhase::CuttingOver => write!(f, "CuttingOver"),
            UpgradePhase::HealthChecking => write!(f, "HealthChecking"),
            UpgradePhase::Completed => write!(f, "Completed"),
            UpgradePhase::Failed => write!(f, "Failed"),
            UpgradePhase::RolledBack => write!(f, "RolledBack"),
        }
    }
}

impl UpgradePhase {
    /// Returns true if this is a terminal phase (no further transitions expected)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack
        )
    }

    /// Returns true if rollback is possible from this phase
    pub fn can_rollback(&self) -> bool {
        !matches!(self, UpgradePhase::Pending | UpgradePhase::RolledBack)
    }
}

/// Status information for a cluster (source or target)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatus {
    /// Name of the cluster
    pub name: String,

    /// PostgreSQL version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// Whether the cluster is ready
    #[serde(default)]
    pub ready: bool,

    /// Connection information
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_info: Option<ClusterConnectionInfo>,
}

/// Connection information for a cluster
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConnectionInfo {
    /// Primary service host
    pub host: String,

    /// PostgreSQL port
    #[serde(default = "default_port")]
    pub port: i32,
}

fn default_port() -> i32 {
    5432
}

/// Replication status between source and target
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationStatus {
    /// Current replication state
    #[serde(default)]
    pub status: ReplicationState,

    /// Replication lag in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_bytes: Option<i64>,

    /// Replication lag in seconds (estimated)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_seconds: Option<i64>,

    /// Last time replication was synced (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sync_time: Option<String>,

    /// Current WAL LSN on source
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_lsn: Option<String>,

    /// Received WAL LSN on target subscription
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_lsn: Option<String>,

    /// Whether LSN positions are in sync
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn_in_sync: Option<bool>,

    /// Name of the publication on source
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_name: Option<String>,

    /// Name of the subscription on target
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_name: Option<String>,
}

/// State of logical replication
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, Default, PartialEq, Eq)]
pub enum ReplicationState {
    /// Replication not yet configured
    #[default]
    NotConfigured,
    /// Initial data sync in progress
    Syncing,
    /// Replication active and streaming
    Active,
    /// Replication fully caught up
    Synced,
    /// Replication stopped (after cutover or error)
    Stopped,
    /// Replication error
    Error,
}

/// Row count verification status
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct VerificationStatus {
    /// Last time verification was performed (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_check_time: Option<String>,

    /// Number of tables verified
    #[serde(default)]
    pub tables_verified: i32,

    /// Number of tables with matching row counts
    #[serde(default)]
    pub tables_matched: i32,

    /// Number of tables with mismatched row counts
    #[serde(default)]
    pub tables_mismatched: i32,

    /// Number of consecutive successful verification passes
    #[serde(default)]
    pub consecutive_passes: i32,

    /// Details of tables with row count mismatches
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mismatched_tables: Vec<TableMismatch>,
}

/// Details of a table with row count mismatch
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TableMismatch {
    /// Schema name
    pub schema: String,

    /// Table name
    pub table: String,

    /// Row count on source
    pub source_count: i64,

    /// Row count on target
    pub target_count: i64,

    /// Difference (source - target)
    pub difference: i64,
}

/// Sequence synchronization status
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SequenceSyncStatus {
    /// Whether sequences have been synced
    #[serde(default)]
    pub synced: bool,

    /// Number of sequences successfully synced
    #[serde(default)]
    pub synced_count: i32,

    /// Number of sequences that failed to sync
    #[serde(default)]
    pub failed_count: i32,

    /// Names of sequences that failed to sync
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed_sequences: Vec<String>,

    /// Timestamp when sequences were synced (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_at: Option<String>,
}

/// Rollback feasibility status
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct RollbackStatus {
    /// Whether rollback is currently feasible
    #[serde(default)]
    pub feasible: bool,

    /// Reason for rollback feasibility/infeasibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Whether rollback would result in data loss
    /// True if writes have occurred to target since cutover
    #[serde(default)]
    pub data_loss_risk: bool,

    /// Last write timestamp on source (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_source_write: Option<String>,

    /// First write timestamp on target after cutover (RFC3339 format)
    /// If set, indicates writes occurred to target
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_target_write: Option<String>,
}

/// Condition types for PostgresUpgrade
pub mod condition_types {
    /// Source cluster is ready and valid
    pub const SOURCE_READY: &str = "SourceReady";
    /// Target cluster has been created and is ready
    pub const TARGET_READY: &str = "TargetReady";
    /// Logical replication is healthy
    pub const REPLICATION_HEALTHY: &str = "ReplicationHealthy";
    /// WAL LSN positions are in sync
    pub const LSN_IN_SYNC: &str = "LsnInSync";
    /// Row counts have been verified
    pub const ROW_COUNTS_VERIFIED: &str = "RowCountsVerified";
    /// Sequences have been synchronized
    pub const SEQUENCES_SYNCED: &str = "SequencesSynced";
    /// Recent backup exists
    pub const BACKUP_VERIFIED: &str = "BackupVerified";
    /// Active connections have been drained
    pub const CONNECTIONS_DRAINED: &str = "ConnectionsDrained";
    /// All pre-checks passed, ready for cutover
    pub const READY_FOR_CUTOVER: &str = "ReadyForCutover";
    /// Post-cutover health check passed
    pub const HEALTH_CHECK_PASSED: &str = "HealthCheckPassed";
    /// Cutover completed successfully
    pub const CUTOVER_COMPLETE: &str = "CutoverComplete";
}

/// Annotation keys for controlling upgrade behavior
pub mod annotations {
    /// Annotation to trigger manual cutover: set to "now"
    pub const CUTOVER: &str = "postgres-operator.smoketurner.com/cutover";
    /// Annotation to trigger rollback: set to "now"
    pub const ROLLBACK: &str = "postgres-operator.smoketurner.com/rollback";
    /// Annotation on source cluster indicating an upgrade is in progress
    pub const UPGRADE_IN_PROGRESS: &str = "postgres-operator.smoketurner.com/upgrade-in-progress";
    /// Annotation on source cluster after cutover indicating it was superseded
    pub const SUPERSEDED_BY: &str = "postgres-operator.smoketurner.com/superseded-by";
}

/// Label keys for upgrade resources
pub mod labels {
    /// Label linking resources to an upgrade
    pub const UPGRADE: &str = "postgres-operator.smoketurner.com/upgrade";
    /// Label indicating the role of a cluster in an upgrade (source/target)
    pub const UPGRADE_ROLE: &str = "postgres-operator.smoketurner.com/upgrade-role";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upgrade_phase_terminal() {
        assert!(UpgradePhase::Completed.is_terminal());
        assert!(UpgradePhase::Failed.is_terminal());
        assert!(UpgradePhase::RolledBack.is_terminal());
        assert!(!UpgradePhase::Pending.is_terminal());
        assert!(!UpgradePhase::Replicating.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_can_rollback() {
        assert!(!UpgradePhase::Pending.can_rollback());
        assert!(!UpgradePhase::RolledBack.can_rollback());
        assert!(UpgradePhase::CreatingTarget.can_rollback());
        assert!(UpgradePhase::Replicating.can_rollback());
        assert!(UpgradePhase::Completed.can_rollback());
    }

    #[test]
    fn test_default_pre_checks() {
        let config = PreChecksConfig::default();
        assert_eq!(config.max_replication_lag_seconds, 0);
        assert!(config.verify_row_counts);
        assert_eq!(config.row_count_tolerance, 0);
        assert_eq!(config.min_verification_passes, 3);
    }

    #[test]
    fn test_default_cutover_mode() {
        let config = CutoverConfig::default();
        assert_eq!(config.mode, CutoverMode::Manual);
    }
}
