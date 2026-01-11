//! Status and conditions management for PostgresCluster resources
//!
//! This module provides utilities for managing Kubernetes-style conditions
//! and updating the status subresource.

use chrono::Utc;
use kube::api::{Patch, PatchParams};
use kube::{Api, ResourceExt};

use crate::controller::Context;
use crate::controller::error::Result;
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
    /// All replicas are synchronized
    pub const REPLICAS_READY: &str = "ReplicasReady";
    /// Resource resize is in progress (Kubernetes 1.35+, KEP-1287)
    pub const RESOURCE_RESIZE_IN_PROGRESS: &str = "ResourceResizeInProgress";
    /// All pod specs have been applied by kubelet (Kubernetes 1.35+, KEP-5067)
    pub const POD_GENERATION_SYNCED: &str = "PodGenerationSynced";
}

/// Condition status values
pub mod condition_status {
    pub const TRUE: &str = "True";
    pub const FALSE: &str = "False";
    pub const UNKNOWN: &str = "Unknown";
}

/// Builder for creating and updating status conditions
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
        let now = Utc::now().to_rfc3339();

        // Find existing condition of this type
        if let Some(existing) = self.conditions.iter_mut().find(|c| c.type_ == type_) {
            // Only update transition time if status changed
            if existing.status != status {
                existing.last_transition_time = now;
            }
            // Always update reason, message, and status
            existing.status = status.to_string();
            existing.reason = reason.to_string();
            existing.message = message.to_string();
            existing.observed_generation = self.generation;
        } else {
            // Add new condition
            self.conditions.push(Condition {
                type_: type_.to_string(),
                status: status.to_string(),
                reason: reason.to_string(),
                message: message.to_string(),
                last_transition_time: now,
                observed_generation: self.generation,
            });
        }
        self
    }

    /// Set the Ready condition
    pub fn ready(self, is_ready: bool, reason: &str, message: &str) -> Self {
        let status = if is_ready {
            condition_status::TRUE
        } else {
            condition_status::FALSE
        };
        self.set_condition(condition_types::READY, status, reason, message)
    }

    /// Set the Progressing condition
    pub fn progressing(self, is_progressing: bool, reason: &str, message: &str) -> Self {
        let status = if is_progressing {
            condition_status::TRUE
        } else {
            condition_status::FALSE
        };
        self.set_condition(condition_types::PROGRESSING, status, reason, message)
    }

    /// Set the Degraded condition
    pub fn degraded(self, is_degraded: bool, reason: &str, message: &str) -> Self {
        let status = if is_degraded {
            condition_status::TRUE
        } else {
            condition_status::FALSE
        };
        self.set_condition(condition_types::DEGRADED, status, reason, message)
    }

    /// Set the ResourceResizeInProgress condition (Kubernetes 1.35+, KEP-1287)
    pub fn resource_resize_in_progress(
        self,
        is_resizing: bool,
        reason: &str,
        message: &str,
    ) -> Self {
        let status = if is_resizing {
            condition_status::TRUE
        } else {
            condition_status::FALSE
        };
        self.set_condition(
            condition_types::RESOURCE_RESIZE_IN_PROGRESS,
            status,
            reason,
            message,
        )
    }

    /// Set the PodGenerationSynced condition (Kubernetes 1.35+, KEP-5067)
    pub fn pod_generation_synced(self, is_synced: bool, reason: &str, message: &str) -> Self {
        let status = if is_synced {
            condition_status::TRUE
        } else {
            condition_status::FALSE
        };
        self.set_condition(
            condition_types::POD_GENERATION_SYNCED,
            status,
            reason,
            message,
        )
    }

    /// Build the conditions list
    pub fn build(self) -> Vec<Condition> {
        self.conditions
    }
}

/// Status manager for PostgresCluster resources
pub struct StatusManager<'a> {
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

    /// Update status for a running cluster
    pub async fn set_running(
        &self,
        ready_replicas: i32,
        total_replicas: i32,
        primary_pod: Option<String>,
        replica_pods: Vec<String>,
        version: &str,
    ) -> Result<()> {
        self.set_running_with_backup(
            ready_replicas,
            total_replicas,
            primary_pod,
            replica_pods,
            version,
            None,
        )
        .await
    }

    /// Update status for a running cluster with optional backup status
    pub async fn set_running_with_backup(
        &self,
        ready_replicas: i32,
        total_replicas: i32,
        primary_pod: Option<String>,
        replica_pods: Vec<String>,
        version: &str,
        backup_status: Option<BackupStatus>,
    ) -> Result<()> {
        self.set_running_full(
            ready_replicas,
            total_replicas,
            primary_pod,
            replica_pods,
            version,
            backup_status,
            None,
        )
        .await
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
        replication_lag_status: Option<&crate::controller::replication_lag::ReplicationLagStatus>,
    ) -> Result<()> {
        let generation = self.cluster.metadata.generation;
        let existing_conditions = self
            .cluster
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();

        let conditions = ConditionBuilder::from_existing(existing_conditions, generation)
            .ready(
                true,
                "ClusterReady",
                "All pods are ready and accepting connections",
            )
            .progressing(false, "Stable", "Cluster is stable")
            .degraded(false, "Healthy", "Cluster is healthy")
            .build();

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

        let status = PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods,
            backup: final_backup_status,
            observed_generation: generation,
            conditions,
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
            .build();

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Creating);

        let status = PostgresClusterStatus {
            phase: ClusterPhase::Creating,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods: vec![],
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
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
            .build();

        // Track when we entered this phase
        let phase_started_at = self.get_phase_started_at(ClusterPhase::Updating);

        let status = PostgresClusterStatus {
            phase: ClusterPhase::Updating,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods,
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
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
            retry_count: Some(current_retry + 1),
            last_error: Some(message.to_string()),
            last_error_time: Some(chrono::Utc::now().to_rfc3339()),
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

        let status = PostgresClusterStatus {
            phase: ClusterPhase::Deleting,
            ready_replicas: 0,
            replicas: 0,
            primary_pod: None,
            replica_pods: vec![],
            backup: self.get_backup_status(),
            observed_generation: generation,
            conditions,
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
            Some(chrono::Utc::now().to_rfc3339())
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

/// Get the list of replica pod names from a StatefulSet
pub fn get_replica_pod_names(sts_name: &str, replica_count: i32) -> Vec<String> {
    (0..replica_count)
        .map(|i| format!("{}-{}", sts_name, i))
        .collect()
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
