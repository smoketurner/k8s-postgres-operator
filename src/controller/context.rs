//! Shared context for the PostgresCluster controller
//!
//! This module provides the shared state and utilities used across
//! the reconciliation loop.

use std::sync::Arc;

use kube::Client;
use kube::runtime::events::{EventType, Reporter};

use crate::controller::events;
use crate::crd::PostgresCluster;
use crate::health::HealthState;

/// Default operator namespace when `POD_NAMESPACE` is not set.
pub const DEFAULT_OPERATOR_NAMESPACE: &str = "postgres-operator-system";

/// Shared context for the controller
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Event reporter identity
    reporter: Reporter,
    /// Health state for metrics (optional for tests)
    pub health_state: Option<Arc<HealthState>>,
    /// Namespace the operator pod is running in.
    ///
    /// Used to populate NetworkPolicy rules that allow operator-to-Patroni
    /// traffic (port 8008) regardless of where the operator is deployed.
    pub operator_namespace: String,
}

impl Context {
    /// Create a new context with the given Kubernetes client
    pub fn new(
        client: Client,
        health_state: Option<Arc<HealthState>>,
        operator_namespace: String,
    ) -> Self {
        Self {
            client,
            reporter: events::reporter(),
            health_state,
            operator_namespace,
        }
    }

    /// Record a successful reconciliation in metrics
    pub fn record_reconcile(&self, namespace: &str, name: &str, duration_secs: f64) {
        if let Some(ref state) = self.health_state {
            state
                .metrics
                .record_reconcile(namespace, name, duration_secs);
        }
    }

    /// Record a failed reconciliation in metrics
    pub fn record_error(&self, namespace: &str, name: &str) {
        if let Some(ref state) = self.health_state {
            state.metrics.record_error(namespace, name);
        }
    }

    /// Record cluster replica metrics
    pub fn record_cluster_replicas(&self, namespace: &str, name: &str, desired: i32, ready: i32) {
        if let Some(ref state) = self.health_state {
            state
                .metrics
                .set_cluster_replicas(namespace, name, desired as i64, ready as i64);
        }
    }

    /// Publish a normal event for a cluster
    pub async fn publish_normal_event(
        &self,
        cluster: &PostgresCluster,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        events::publish_event(
            &self.client,
            &self.reporter,
            cluster,
            EventType::Normal,
            reason,
            action,
            note,
        )
        .await;
    }

    /// Publish a warning event for a cluster
    pub async fn publish_warning_event(
        &self,
        cluster: &PostgresCluster,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        events::publish_event(
            &self.client,
            &self.reporter,
            cluster,
            EventType::Warning,
            reason,
            action,
            note,
        )
        .await;
    }
}
