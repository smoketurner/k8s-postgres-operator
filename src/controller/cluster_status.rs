//! Status and conditions management for PostgresCluster resources
//!
//! This module provides utilities for managing Kubernetes-style conditions
//! and updating the status subresource.

use jiff::Timestamp;
use kube::api::{Patch, PatchParams};
use kube::{Api, ResourceExt};

use crate::controller::Context;
use crate::controller::cluster_error::Result;
use crate::controller::conditions::{new_condition, set_status_condition, status as cond_status};
use crate::crd::{
    BackupStatus, ClusterPhase, Condition, ConnectionInfo, PostgresCluster, PostgresClusterStatus,
};
use crate::resources::pgbouncer;

/// Standard condition types following Kubernetes conventions
pub mod condition_types {
    /// Cluster is ready to accept connections
    pub const READY: &str = "Ready";
    /// Cluster is progressing towards a goal state
    pub const PROGRESSING: &str = "Progressing";
    /// Cluster is in a degraded state but still functional
    pub const DEGRADED: &str = "Degraded";
    /// Cluster configuration is valid
    pub const CONFIG_VALID: &str = "ConfigurationValid";
    /// Resource resize is in progress (Kubernetes 1.35+, KEP-1287)
    pub const RESOURCE_RESIZE_IN_PROGRESS: &str = "ResourceResizeInProgress";
    /// All pod specs have been applied by kubelet (Kubernetes 1.35+, KEP-5067)
    pub const POD_GENERATION_SYNCED: &str = "PodGenerationSynced";
}

/// Condition status values. Re-exported from [`crate::controller::conditions`]
/// to preserve the historical `cluster_status::condition_status::TRUE` path.
pub mod condition_status {
    pub use crate::controller::conditions::status::{FALSE, TRUE, UNKNOWN};
}

fn bool_status(value: bool) -> &'static str {
    if value {
        cond_status::TRUE
    } else {
        cond_status::FALSE
    }
}

/// Extract the Ready condition's `reason` and `message` so they can be
/// surfaced as top-level status fields. Keeps `status.reason` /
/// `status.message` in lockstep with the authoritative condition entry
/// instead of maintaining two sources of truth.
pub fn ready_summary(conditions: &[Condition]) -> (Option<String>, Option<String>) {
    conditions
        .iter()
        .find(|c| c.type_ == condition_types::READY)
        .map(|c| (Some(c.reason.clone()), Some(c.message.clone())))
        .unwrap_or_default()
}

/// Builder for creating and updating status conditions on a PostgresCluster.
///
/// Wraps [`set_status_condition`] so that callers get deduplication,
/// `lastTransitionTime` preservation across no-op updates, and
/// `observedGeneration` propagation for free.
pub struct ConditionBuilder {
    conditions: Vec<Condition>,
    generation: Option<i64>,
}

impl ConditionBuilder {
    /// Create a new condition builder
    pub fn new(generation: Option<i64>) -> Self {
        Self {
            conditions: Vec::new(),
            generation,
        }
    }

    /// Create from existing conditions
    pub fn from_existing(existing: Vec<Condition>, generation: Option<i64>) -> Self {
        Self {
            conditions: existing,
            generation,
        }
    }

    /// Set a condition, updating if it exists or adding if it doesn't
    pub fn set_condition(mut self, type_: &str, status: &str, reason: &str, message: &str) -> Self {
        set_status_condition(
            &mut self.conditions,
            new_condition(type_, status, reason, message, self.generation),
        );
        self
    }

    /// Set the Ready condition
    pub fn ready(self, is_ready: bool, reason: &str, message: &str) -> Self {
        self.set_condition(
            condition_types::READY,
            bool_status(is_ready),
            reason,
            message,
        )
    }

    /// Set the Progressing condition
    pub fn progressing(self, is_progressing: bool, reason: &str, message: &str) -> Self {
        self.set_condition(
            condition_types::PROGRESSING,
            bool_status(is_progressing),
            reason,
            message,
        )
    }

    /// Set the Degraded condition
    pub fn degraded(self, is_degraded: bool, reason: &str, message: &str) -> Self {
        self.set_condition(
            condition_types::DEGRADED,
            bool_status(is_degraded),
            reason,
            message,
        )
    }

    /// Set the ConfigurationValid condition
    pub fn config_valid(self, is_valid: bool, reason: &str, message: &str) -> Self {
        self.set_condition(
            condition_types::CONFIG_VALID,
            bool_status(is_valid),
            reason,
            message,
        )
    }

    /// Set the ResourceResizeInProgress condition (Kubernetes 1.35+, KEP-1287)
    pub fn resource_resize_in_progress(
        self,
        is_resizing: bool,
        reason: &str,
        message: &str,
    ) -> Self {
        self.set_condition(
            condition_types::RESOURCE_RESIZE_IN_PROGRESS,
            bool_status(is_resizing),
            reason,
            message,
        )
    }

    /// Set the PodGenerationSynced condition (Kubernetes 1.35+, KEP-5067)
    pub fn pod_generation_synced(self, is_synced: bool, reason: &str, message: &str) -> Self {
        self.set_condition(
            condition_types::POD_GENERATION_SYNCED,
            bool_status(is_synced),
            reason,
            message,
        )
    }

    /// Build the conditions list
    pub fn build(self) -> Vec<Condition> {
        self.conditions
    }
}

/// Progress signals that affect a Running cluster's Progressing condition.
///
/// A cluster that has all its replicas ready but is still applying a pod-level
/// change (in-place resize via KEP-1287 or a pod spec the kubelet has not yet
/// observed via KEP-5067) is not stable. Reporting `Progressing=False/Stable`
/// in that window contradicts the `resizeStatus` and `allPodsSynced` status
/// fields, so callers must thread this state through.
#[derive(Debug, Clone, Copy)]
pub struct RunningProgress {
    /// At least one pod has an in-place resize in flight (Kubernetes 1.35+).
    pub resize_in_progress: bool,
    /// Every pod's `observedGeneration` matches its `metadata.generation`
    /// (Kubernetes 1.35+). `true` when no pods are tracked yet.
    pub all_pods_synced: bool,
}

impl Default for RunningProgress {
    /// Default to the stable state: no resize active, all pods synced.
    /// Callers that have pod-tracking data should construct via [`Self::new`].
    fn default() -> Self {
        Self {
            resize_in_progress: false,
            all_pods_synced: true,
        }
    }
}

impl RunningProgress {
    /// Construct a [`RunningProgress`] from raw signals.
    pub fn new(resize_in_progress: bool, all_pods_synced: bool) -> Self {
        Self {
            resize_in_progress,
            all_pods_synced,
        }
    }

    /// `true` when no pod-level activity is preventing the Running phase from
    /// being reported as stable.
    pub fn is_stable(&self) -> bool {
        !self.resize_in_progress && self.all_pods_synced
    }
}

/// Build the Ready/Progressing/Degraded/ConfigurationValid condition set for a
/// cluster in the Running phase, reflecting pod-level resize and sync state.
fn build_running_conditions(
    existing: Vec<Condition>,
    generation: Option<i64>,
    progress: &RunningProgress,
) -> ConditionBuilder {
    let builder = ConditionBuilder::from_existing(existing, generation).ready(
        true,
        "ClusterReady",
        "All pods are ready and accepting connections",
    );

    let builder = if progress.resize_in_progress {
        builder
            .progressing(
                true,
                "ResizeInProgress",
                "In-place resource resize is being applied to one or more pods",
            )
            .resource_resize_in_progress(
                true,
                "ResizeInProgress",
                "In-place resource resize is being applied to one or more pods",
            )
    } else if !progress.all_pods_synced {
        builder
            .progressing(
                true,
                "SyncInProgress",
                "Waiting for kubelet to observe the latest pod spec",
            )
            .resource_resize_in_progress(false, "NoResize", "No in-place resize active")
    } else {
        builder
            .progressing(false, "Stable", "Cluster is stable")
            .resource_resize_in_progress(false, "NoResize", "No in-place resize active")
    };

    builder
        .degraded(false, "Healthy", "Cluster is healthy")
        .config_valid(true, "SpecValid", "Cluster specification is valid")
}

/// Status manager for PostgresCluster resources
pub(crate) struct StatusManager<'a> {
    cluster: &'a PostgresCluster,
    ctx: &'a Context,
    ns: &'a str,
}

impl<'a> StatusManager<'a> {
    /// Create a new status manager
    pub fn new(cluster: &'a PostgresCluster, ctx: &'a Context, ns: &'a str) -> Self {
        Self { cluster, ctx, ns }
    }

    /// Update the cluster status with full status object
    pub async fn update(&self, status: PostgresClusterStatus) -> Result<()> {
        let api: Api<PostgresCluster> = Api::namespaced(self.ctx.client.clone(), self.ns);
        let name = self.cluster.name_any();

        let patch = serde_json::json!({
            "status": status
        });

        api.patch_status(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        Ok(())
    }

    /// Stamp `status.lastFullReconcile` with the current time. Called at the end
    /// of a successful full reconcile so the periodic drift-repair timer resets.
    /// Uses a merge patch so it does not disturb other status fields.
    pub async fn stamp_full_reconcile(&self) -> Result<()> {
        let api: Api<PostgresCluster> = Api::namespaced(self.ctx.client.clone(), self.ns);
        let name = self.cluster.name_any();
        let patch = serde_json::json!({
            "status": {
                "lastFullReconcile": Timestamp::now().to_string(),
            }
        });
        api.patch_status(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;
        Ok(())
    }

    /// Patch only the observed-replica fields without touching `phase` or
    /// `conditions`. Used when the reconciler sees the live counts have
    /// changed but the state machine has not (or has refused to) transition
    /// — the status must still tell the truth about how many pods are
    /// Ready instead of leaving a stale snapshot.
    pub async fn patch_observed_counts(
        &self,
        ready_replicas: i32,
        primary_pod: Option<String>,
        replica_pods: Vec<String>,
    ) -> Result<()> {
        let api: Api<PostgresCluster> = Api::namespaced(self.ctx.client.clone(), self.ns);
        let name = self.cluster.name_any();
        let patch = serde_json::json!({
            "status": {
                "readyReplicas": ready_replicas,
                "primaryPod": primary_pod,
                "replicaPods": replica_pods,
            }
        });
        api.patch_status(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;
        Ok(())
    }

    /// Update status for a running cluster with all optional status fields
    ///
    /// This is the consolidated method that updates backup status and replication lag
    /// in a single atomic operation, avoiding race conditions from multiple status updates.
    #[allow(clippy::too_many_arguments)]
    pub async fn set_running_full(
        &self,
        ready_replicas: i32,
        total_replicas: i32,
        primary_pod: Option<String>,
        replica_pods: Vec<String>,
        version: &str,
        backup_status: Option<BackupStatus>,
        replication_lag_status: Option<
            &crate::controller::cluster_replication_lag::ReplicationLagStatus,
        >,
        progress: RunningProgress,
    ) -> Result<()> {
        let generation = self.cluster.metadata.generation;
        let existing_conditions = self
            .cluster
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();

        let conditions =
            build_running_conditions(existing_conditions, generation, &progress).build();

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Running);

        // Use provided backup status or fall back to existing/default
        let final_backup_status = backup_status.or_else(|| self.get_backup_status());

        // Use provided replication lag status or preserve existing
        let (replication_lag, max_replication_lag_bytes, replicas_lagging) =
            if let Some(lag_status) = replication_lag_status {
                (
                    lag_status.replicas.clone(),
                    lag_status.max_lag_bytes,
                    Some(lag_status.any_exceeds_threshold),
                )
            } else {
                // Preserve existing replication lag status
                (
                    self.cluster
                        .status
                        .as_ref()
                        .map(|s| s.replication_lag.clone())
                        .unwrap_or_default(),
                    self.cluster
                        .status
                        .as_ref()
                        .and_then(|s| s.max_replication_lag_bytes),
                    self.cluster
                        .status
                        .as_ref()
                        .and_then(|s| s.replicas_lagging),
                )
            };

        let (reason, message) = ready_summary(&conditions);
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods,
            backup: final_backup_status,
            observed_generation: generation,
            conditions,
            reason,
            message,
            // Clear error state on successful running
            retry_count: Some(0),
            last_error: None,
            last_error_time: None,
            previous_replicas: self.cluster.status.as_ref().map(|s| s.replicas),
            phase_started_at,
            // Set current version when cluster becomes running
            current_version: Some(version.to_string()),
            // TLS and PgBouncer status
            tls_enabled: Some(self.cluster.spec.tls.enabled),
            pgbouncer_enabled: self.cluster.spec.pgbouncer.as_ref().map(|p| p.enabled),
            pgbouncer_ready_replicas: None, // Updated by reconciler when checking deployment
            // Kubernetes 1.35+ pod tracking and resize status
            // These are populated by the reconciler's pod tracking functions
            pods: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.pods.clone())
                .unwrap_or_default(),
            resize_status: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.resize_status.clone())
                .unwrap_or_default(),
            all_pods_synced: self.cluster.status.as_ref().and_then(|s| s.all_pods_synced),
            // Preserve restore status
            restored_from: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.restored_from.clone()),
            // Replication lag tracking
            replication_lag,
            max_replication_lag_bytes,
            replicas_lagging,
            connection_info: self.get_connection_info(),
            // Preserve upgrade lineage (set by upgrade_reconciler)
            successor: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.successor.clone()),
            origin: self.cluster.status.as_ref().and_then(|s| s.origin.clone()),
            // Preserve the drift-repair timestamp across status-only updates.
            last_full_reconcile: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_full_reconcile.clone()),
        };

        self.update(status).await
    }

    /// Update status for a creating cluster
    pub async fn set_creating(
        &self,
        ready_replicas: i32,
        total_replicas: i32,
        primary_pod: Option<String>,
    ) -> Result<()> {
        let generation = self.cluster.metadata.generation;
        let existing_conditions = self
            .cluster
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();

        let conditions = ConditionBuilder::from_existing(existing_conditions, generation)
            .ready(false, "Creating", "Cluster is being created")
            .progressing(true, "CreatingResources", "Creating cluster resources")
            .degraded(false, "NotApplicable", "Cluster is being created")
            .config_valid(true, "SpecValid", "Cluster specification is valid")
            .build();

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Creating);

        let (reason, message) = ready_summary(&conditions);
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Creating,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods: vec![],
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
            reason,
            message,
            retry_count: None,
            last_error: None,
            last_error_time: None,
            previous_replicas: None,
            phase_started_at,
            // Preserve existing version during creation (usually None)
            current_version: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.current_version.clone()),
            // TLS and PgBouncer status
            tls_enabled: Some(self.cluster.spec.tls.enabled),
            pgbouncer_enabled: self.cluster.spec.pgbouncer.as_ref().map(|p| p.enabled),
            pgbouncer_ready_replicas: None,
            // Kubernetes 1.35+ pod tracking and resize status
            pods: vec![],
            resize_status: vec![],
            all_pods_synced: None,
            // Preserve restore status
            restored_from: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.restored_from.clone()),
            // Replication lag tracking (preserved from existing status)
            replication_lag: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.replication_lag.clone())
                .unwrap_or_default(),
            max_replication_lag_bytes: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.max_replication_lag_bytes),
            replicas_lagging: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.replicas_lagging),
            connection_info: self.get_connection_info(),
            // Preserve upgrade lineage (set by upgrade_reconciler)
            successor: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.successor.clone()),
            origin: self.cluster.status.as_ref().and_then(|s| s.origin.clone()),
            // Preserve the drift-repair timestamp across status-only updates.
            last_full_reconcile: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_full_reconcile.clone()),
        };

        self.update(status).await
    }

    /// Update status for an updating cluster
    pub async fn set_updating(
        &self,
        ready_replicas: i32,
        total_replicas: i32,
        primary_pod: Option<String>,
        replica_pods: Vec<String>,
    ) -> Result<()> {
        let generation = self.cluster.metadata.generation;
        let existing_conditions = self
            .cluster
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();

        let conditions = ConditionBuilder::from_existing(existing_conditions, generation)
            .ready(false, "Updating", "Cluster is being updated")
            .progressing(true, "RollingUpdate", "Performing rolling update")
            .degraded(false, "NotDegraded", "Cluster is updating normally")
            .config_valid(true, "SpecValid", "Cluster specification is valid")
            .build();

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Updating);

        let (reason, message) = ready_summary(&conditions);
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Updating,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods,
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
            reason,
            message,
            retry_count: self.cluster.status.as_ref().and_then(|s| s.retry_count),
            last_error: None,
            last_error_time: None,
            previous_replicas: self.cluster.status.as_ref().map(|s| s.replicas),
            phase_started_at,
            // Preserve existing version during updates
            current_version: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.current_version.clone()),
            // TLS and PgBouncer status
            tls_enabled: Some(self.cluster.spec.tls.enabled),
            pgbouncer_enabled: self.cluster.spec.pgbouncer.as_ref().map(|p| p.enabled),
            pgbouncer_ready_replicas: None,
            // Kubernetes 1.35+ pod tracking and resize status
            pods: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.pods.clone())
                .unwrap_or_default(),
            resize_status: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.resize_status.clone())
                .unwrap_or_default(),
            all_pods_synced: self.cluster.status.as_ref().and_then(|s| s.all_pods_synced),
            // Preserve restore status
            restored_from: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.restored_from.clone()),
            // Replication lag tracking (preserved from existing status)
            replication_lag: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.replication_lag.clone())
                .unwrap_or_default(),
            max_replication_lag_bytes: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.max_replication_lag_bytes),
            replicas_lagging: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.replicas_lagging),
            connection_info: self.get_connection_info(),
            // Preserve upgrade lineage (set by upgrade_reconciler)
            successor: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.successor.clone()),
            origin: self.cluster.status.as_ref().and_then(|s| s.origin.clone()),
            // Preserve the drift-repair timestamp across status-only updates.
            last_full_reconcile: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_full_reconcile.clone()),
        };

        self.update(status).await
    }

    /// Update status for a failed cluster
    pub async fn set_failed(&self, reason: &str, message: &str) -> Result<()> {
        let generation = self.cluster.metadata.generation;
        let existing_status = self.cluster.status.as_ref();
        let existing_conditions = existing_status
            .map(|s| s.conditions.clone())
            .unwrap_or_default();

        let conditions = ConditionBuilder::from_existing(existing_conditions, generation)
            .ready(false, reason, message)
            .progressing(false, "Failed", message)
            .degraded(true, reason, message)
            .build();

        // Increment retry count for exponential backoff
        let current_retry = existing_status.and_then(|s| s.retry_count).unwrap_or(0);

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Failed);

        let status = PostgresClusterStatus {
            phase: ClusterPhase::Failed,
            ready_replicas: existing_status.map(|s| s.ready_replicas).unwrap_or(0),
            replicas: self.cluster.spec.replicas,
            primary_pod: existing_status.and_then(|s| s.primary_pod.clone()),
            replica_pods: existing_status
                .map(|s| s.replica_pods.clone())
                .unwrap_or_default(),
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
            retry_count: Some(current_retry + 1),
            last_error: Some(message.to_string()),
            last_error_time: Some(Timestamp::now().to_string()),
            previous_replicas: existing_status.and_then(|s| s.previous_replicas),
            phase_started_at,
            // Preserve existing version when failed
            current_version: existing_status.and_then(|s| s.current_version.clone()),
            // TLS and PgBouncer status
            tls_enabled: Some(self.cluster.spec.tls.enabled),
            pgbouncer_enabled: self.cluster.spec.pgbouncer.as_ref().map(|p| p.enabled),
            pgbouncer_ready_replicas: existing_status.and_then(|s| s.pgbouncer_ready_replicas),
            // Kubernetes 1.35+ pod tracking and resize status
            pods: existing_status.map(|s| s.pods.clone()).unwrap_or_default(),
            resize_status: existing_status
                .map(|s| s.resize_status.clone())
                .unwrap_or_default(),
            all_pods_synced: existing_status.and_then(|s| s.all_pods_synced),
            // Preserve restore status
            restored_from: existing_status.and_then(|s| s.restored_from.clone()),
            // Replication lag tracking (preserved from existing status)
            replication_lag: existing_status
                .map(|s| s.replication_lag.clone())
                .unwrap_or_default(),
            max_replication_lag_bytes: existing_status.and_then(|s| s.max_replication_lag_bytes),
            replicas_lagging: existing_status.and_then(|s| s.replicas_lagging),
            connection_info: self.get_connection_info(),
            // Preserve upgrade lineage (set by upgrade_reconciler)
            successor: existing_status.and_then(|s| s.successor.clone()),
            origin: existing_status.and_then(|s| s.origin.clone()),
            // Preserve the drift-repair timestamp.
            last_full_reconcile: existing_status.and_then(|s| s.last_full_reconcile.clone()),
        };

        self.update(status).await
    }

    /// Update status for a deleting cluster
    pub async fn set_deleting(&self) -> Result<()> {
        let generation = self.cluster.metadata.generation;
        let existing_conditions = self
            .cluster
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();

        let conditions = ConditionBuilder::from_existing(existing_conditions, generation)
            .ready(false, "Deleting", "Cluster is being deleted")
            .progressing(
                true,
                "Terminating",
                "Cluster resources are being cleaned up",
            )
            .build();

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Deleting);

        let (reason, message) = ready_summary(&conditions);
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Deleting,
            ready_replicas: 0,
            replicas: 0,
            primary_pod: None,
            replica_pods: vec![],
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
            reason,
            message,
            retry_count: None,
            last_error: None,
            last_error_time: None,
            previous_replicas: None,
            phase_started_at,
            // Preserve existing version when deleting
            current_version: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.current_version.clone()),
            // TLS and PgBouncer status
            tls_enabled: None,
            pgbouncer_enabled: None,
            pgbouncer_ready_replicas: None,
            // Kubernetes 1.35+ pod tracking and resize status
            pods: vec![],
            resize_status: vec![],
            all_pods_synced: None,
            // Preserve restore status
            restored_from: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.restored_from.clone()),
            // Replication lag tracking (preserved from existing status)
            replication_lag: self
                .cluster
                .status
                .as_ref()
                .map(|s| s.replication_lag.clone())
                .unwrap_or_default(),
            max_replication_lag_bytes: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.max_replication_lag_bytes),
            replicas_lagging: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.replicas_lagging),
            // Connection info is cleared during deletion
            connection_info: None,
            // Preserve upgrade lineage (set by upgrade_reconciler)
            successor: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.successor.clone()),
            origin: self.cluster.status.as_ref().and_then(|s| s.origin.clone()),
            // Preserve the drift-repair timestamp across status-only updates.
            last_full_reconcile: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_full_reconcile.clone()),
        };

        self.update(status).await
    }

    /// Generate connection info for this cluster
    fn get_connection_info(&self) -> Option<ConnectionInfo> {
        Some(generate_connection_info(self.cluster, self.ns))
    }

    /// Get the backup status, preserving existing status or generating from spec
    fn get_backup_status(&self) -> Option<BackupStatus> {
        // If backup is configured in spec, generate initial status if not present
        if let Some(ref backup_spec) = self.cluster.spec.backup {
            // Try to preserve existing backup status
            if let Some(existing) = self.cluster.status.as_ref().and_then(|s| s.backup.clone()) {
                return Some(existing);
            }

            // Create initial backup status from spec
            Some(BackupStatus {
                enabled: true,
                destination_type: Some(backup_spec.destination.destination_type().to_string()),
                ..Default::default()
            })
        } else {
            // No backup configured, preserve any existing status (shouldn't happen normally)
            self.cluster.status.as_ref().and_then(|s| s.backup.clone())
        }
    }

    /// Get the timestamp when the current phase started
    /// If the phase is changing, returns a new timestamp
    /// If the phase is the same, returns the existing timestamp
    fn get_phase_started_at(&self, new_phase: ClusterPhase) -> Option<String> {
        let current_phase = self.cluster.status.as_ref().map(|s| s.phase);
        let existing_timestamp = self
            .cluster
            .status
            .as_ref()
            .and_then(|s| s.phase_started_at.clone());

        if current_phase == Some(new_phase) && existing_timestamp.is_some() {
            // Same phase, keep existing timestamp
            existing_timestamp
        } else {
            // New phase, set new timestamp
            Some(Timestamp::now().to_string())
        }
    }

    /// Update pod tracking status fields (Kubernetes 1.35+ features)
    ///
    /// This updates the pods, resize_status, and all_pods_synced fields
    /// which track per-pod generation and in-place resource resize status.
    pub async fn update_pod_tracking(
        &self,
        pods: Vec<crate::crd::PodInfo>,
        resize_status: Vec<crate::crd::PodResourceResizeStatus>,
    ) -> Result<()> {
        let api: Api<PostgresCluster> = Api::namespaced(self.ctx.client.clone(), self.ns);
        let name = self.cluster.name_any();

        // Calculate all_pods_synced from pod info
        let all_synced = if pods.is_empty() {
            None
        } else {
            Some(pods.iter().all(|p| p.spec_applied))
        };

        let patch = serde_json::json!({
            "status": {
                "pods": pods,
                "resize_status": resize_status,
                "all_pods_synced": all_synced
            }
        });

        api.patch_status(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        Ok(())
    }

    /// Update PgBouncer ready replicas status
    ///
    /// This updates the pgbouncerReadyReplicas field based on the actual
    /// Deployment readyReplicas count.
    pub async fn update_pgbouncer_status(&self, ready_replicas: Option<i32>) -> Result<()> {
        let api: Api<PostgresCluster> = Api::namespaced(self.ctx.client.clone(), self.ns);
        let name = self.cluster.name_any();

        // Note: Field name must be camelCase to match the serde(rename_all = "camelCase")
        // on PostgresClusterStatus
        let patch = serde_json::json!({
            "status": {
                "pgbouncerReadyReplicas": ready_replicas
            }
        });

        api.patch_status(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        Ok(())
    }
}

/// Check if the cluster spec has changed by comparing observed generation
pub fn spec_changed(cluster: &PostgresCluster) -> bool {
    let current_generation = cluster.metadata.generation;
    let observed_generation = cluster.status.as_ref().and_then(|s| s.observed_generation);

    match (current_generation, observed_generation) {
        (Some(current), Some(observed)) => current != observed,
        (Some(_), None) => true, // Never observed, needs reconciliation
        _ => true,               // No generation, always reconcile
    }
}

/// Generate connection info for a PostgresCluster
fn generate_connection_info(cluster: &PostgresCluster, namespace: &str) -> ConnectionInfo {
    let name = cluster.metadata.name.as_deref().unwrap_or("unknown");

    // Build service endpoints
    let primary = Some(format!("{}-primary.{}.svc:5432", name, namespace));
    let replicas = if cluster.spec.replicas > 1 {
        Some(format!("{}-repl.{}.svc:5432", name, namespace))
    } else {
        None
    };

    // PgBouncer endpoints (only if enabled)
    let (pooler, pooler_replicas) = if pgbouncer::is_pgbouncer_enabled(cluster) {
        let pooler_primary = Some(format!("{}-pooler.{}.svc:6432", name, namespace));
        let pooler_repl = if pgbouncer::is_replica_pooler_enabled(cluster) {
            Some(format!("{}-pooler-repl.{}.svc:6432", name, namespace))
        } else {
            None
        };
        (pooler_primary, pooler_repl)
    } else {
        (None, None)
    };

    ConnectionInfo {
        primary,
        replicas,
        pooler,
        pooler_replicas,
        credentials_secret: format!("{}-credentials", name),
        database: Some("postgres".to_string()),
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    fn find_condition<'a>(conditions: &'a [Condition], type_: &str) -> &'a Condition {
        conditions
            .iter()
            .find(|c| c.type_ == type_)
            .unwrap_or_else(|| panic!("expected {type_} condition"))
    }

    #[test]
    fn running_progress_default_is_stable() {
        let progress = RunningProgress::default();
        assert!(progress.is_stable());
        assert!(!progress.resize_in_progress);
        assert!(progress.all_pods_synced);
    }

    #[test]
    fn running_conditions_stable_when_no_pod_activity() {
        let progress = RunningProgress::new(false, true);
        let conditions = build_running_conditions(Vec::new(), Some(1), &progress).build();

        let progressing = find_condition(&conditions, condition_types::PROGRESSING);
        assert_eq!(progressing.status, condition_status::FALSE);
        assert_eq!(progressing.reason, "Stable");

        let ready = find_condition(&conditions, condition_types::READY);
        assert_eq!(ready.status, condition_status::TRUE);

        let resize = find_condition(&conditions, condition_types::RESOURCE_RESIZE_IN_PROGRESS);
        assert_eq!(resize.status, condition_status::FALSE);
    }

    #[test]
    fn running_conditions_progressing_during_resize() {
        let progress = RunningProgress::new(true, true);
        let conditions = build_running_conditions(Vec::new(), Some(2), &progress).build();

        let progressing = find_condition(&conditions, condition_types::PROGRESSING);
        assert_eq!(progressing.status, condition_status::TRUE);
        assert_eq!(progressing.reason, "ResizeInProgress");

        let resize = find_condition(&conditions, condition_types::RESOURCE_RESIZE_IN_PROGRESS);
        assert_eq!(resize.status, condition_status::TRUE);
        assert_eq!(resize.reason, "ResizeInProgress");
    }

    #[test]
    fn running_conditions_progressing_when_pods_not_synced() {
        let progress = RunningProgress::new(false, false);
        let conditions = build_running_conditions(Vec::new(), Some(3), &progress).build();

        let progressing = find_condition(&conditions, condition_types::PROGRESSING);
        assert_eq!(progressing.status, condition_status::TRUE);
        assert_eq!(progressing.reason, "SyncInProgress");

        // Resize is not what's blocking stability, so the resize condition is false.
        let resize = find_condition(&conditions, condition_types::RESOURCE_RESIZE_IN_PROGRESS);
        assert_eq!(resize.status, condition_status::FALSE);
    }

    #[test]
    fn running_conditions_resize_takes_priority_over_sync() {
        // When both signals are unhealthy, resize is the more specific
        // explanation and should win the Progressing reason.
        let progress = RunningProgress::new(true, false);
        let conditions = build_running_conditions(Vec::new(), Some(4), &progress).build();

        let progressing = find_condition(&conditions, condition_types::PROGRESSING);
        assert_eq!(progressing.reason, "ResizeInProgress");
    }

    #[test]
    fn running_conditions_observed_generation_is_propagated() {
        let progress = RunningProgress::new(false, true);
        let conditions = build_running_conditions(Vec::new(), Some(7), &progress).build();
        for condition in &conditions {
            assert_eq!(condition.observed_generation, Some(7));
        }
    }
}
