//! Shared context for the PostgresCluster controller
//!
//! This module provides the shared state and utilities used across
//! the reconciliation loop.

use std::sync::Arc;

use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Client, Resource};

use crate::crd::PostgresCluster;
use crate::health::HealthState;

/// Field manager name for the operator
pub(crate) const FIELD_MANAGER: &str = "postgres-operator";

/// Shared context for the controller
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Event reporter identity
    reporter: Reporter,
    /// Health state for metrics (optional for tests)
    pub health_state: Option<Arc<HealthState>>,
}

impl Context {
    /// Create a new context with the given Kubernetes client
    pub fn new(client: Client, health_state: Option<Arc<HealthState>>) -> Self {
        Self {
            client,
            reporter: Reporter {
                controller: FIELD_MANAGER.into(),
                instance: std::env::var("POD_NAME").ok(),
            },
            health_state,
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

    /// Create an event recorder
    fn recorder(&self) -> Recorder {
        Recorder::new(self.client.clone(), self.reporter.clone())
    }

    /// Publish a normal event for a cluster
    pub async fn publish_normal_event(
        &self,
        cluster: &PostgresCluster,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        let recorder = self.recorder();
        let object_ref = cluster.object_ref(&());
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: EventType::Normal,
                    reason: reason.into(),
                    note,
                    action: action.into(),
                    secondary: None,
                },
                &object_ref,
            )
            .await
        {
            tracing::warn!("Failed to publish event: {}", e);
        }
    }

    /// Publish a warning event for a cluster
    pub async fn publish_warning_event(
        &self,
        cluster: &PostgresCluster,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        let recorder = self.recorder();
        let object_ref = cluster.object_ref(&());
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: EventType::Warning,
                    reason: reason.into(),
                    note,
                    action: action.into(),
                    secondary: None,
                },
                &object_ref,
            )
            .await
        {
            tracing::warn!("Failed to publish warning event: {}", e);
        }
    }
}
