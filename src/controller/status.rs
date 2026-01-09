//! Status and conditions management for PostgresCluster resources
//!
//! This module provides utilities for managing Kubernetes-style conditions
//! and updating the status subresource.

use chrono::Utc;
use kube::api::{Patch, PatchParams};
use kube::{Api, ResourceExt};

use crate::controller::Context;
use crate::controller::error::Result;
use crate::crd::{ClusterPhase, Condition, PostgresCluster, PostgresClusterStatus};

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
            // Only update if status changed
            if existing.status != status {
                existing.status = status.to_string();
                existing.reason = reason.to_string();
                existing.message = message.to_string();
                existing.last_transition_time = now;
                existing.observed_generation = self.generation;
            } else {
                // Update reason and message even if status unchanged
                existing.reason = reason.to_string();
                existing.message = message.to_string();
                existing.observed_generation = self.generation;
            }
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

        let status = PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas,
            replicas: total_replicas,
            primary_pod,
            replica_pods,
            last_backup: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_backup.clone()),
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
            last_backup: None,
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
            last_backup: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_backup.clone()),
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
            last_backup: existing_status.and_then(|s| s.last_backup.clone()),
            observed_generation: generation,
            conditions,
            retry_count: Some(current_retry + 1),
            last_error: Some(message.to_string()),
            last_error_time: Some(chrono::Utc::now().to_rfc3339()),
            previous_replicas: existing_status.and_then(|s| s.previous_replicas),
            phase_started_at,
            // Preserve existing version when failed
            current_version: existing_status.and_then(|s| s.current_version.clone()),
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
            last_backup: self
                .cluster
                .status
                .as_ref()
                .and_then(|s| s.last_backup.clone()),
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
        };

        self.update(status).await
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
