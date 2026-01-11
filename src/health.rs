//! Health server for Kubernetes probes and Prometheus metrics
//!
//! Provides HTTP endpoints for:
//! - `/healthz` - Liveness probe (is the process alive?)
//! - `/readyz` - Readiness probe (is the operator ready to serve?)
//! - `/metrics` - Prometheus metrics

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

/// Labels for metrics
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ReconcileLabels {
    pub namespace: String,
    pub name: String,
}

impl prometheus_client::encoding::EncodeLabelSet for ReconcileLabels {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        use prometheus_client::encoding::EncodeLabel;
        ("namespace", self.namespace.as_str()).encode(encoder.encode_label())?;
        ("name", self.name.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

/// Labels for cluster phase metrics
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PhaseLabels {
    pub phase: String,
}

impl prometheus_client::encoding::EncodeLabelSet for PhaseLabels {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        use prometheus_client::encoding::EncodeLabel;
        ("phase", self.phase.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

/// Shared metrics state
pub struct Metrics {
    /// Total reconciliations counter
    pub reconciliations_total: Family<ReconcileLabels, Counter>,
    /// Failed reconciliations counter
    pub reconciliation_errors_total: Family<ReconcileLabels, Counter>,
    /// Reconciliation duration histogram
    pub reconcile_duration_seconds: Family<ReconcileLabels, Histogram>,

    // Fleet metrics
    /// Total clusters by phase
    pub clusters_total: Family<PhaseLabels, Gauge>,
    /// Desired replicas per cluster
    pub cluster_replicas_desired: Family<ReconcileLabels, Gauge>,
    /// Ready replicas per cluster
    pub cluster_replicas_ready: Family<ReconcileLabels, Gauge>,

    /// Prometheus registry
    registry: Registry,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let reconciliations_total = Family::<ReconcileLabels, Counter>::default();
        registry.register(
            "postgres_operator_reconciliations",
            "Total number of reconciliations",
            reconciliations_total.clone(),
        );

        let reconciliation_errors_total = Family::<ReconcileLabels, Counter>::default();
        registry.register(
            "postgres_operator_reconciliation_errors",
            "Total number of reconciliation errors",
            reconciliation_errors_total.clone(),
        );

        let reconcile_duration_seconds =
            Family::<ReconcileLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 15))
            });
        registry.register(
            "postgres_operator_reconcile_duration_seconds",
            "Duration of reconciliation in seconds",
            reconcile_duration_seconds.clone(),
        );

        // Fleet metrics
        let clusters_total = Family::<PhaseLabels, Gauge>::default();
        registry.register(
            "postgres_operator_clusters_total",
            "Total number of PostgreSQL clusters by phase",
            clusters_total.clone(),
        );

        let cluster_replicas_desired = Family::<ReconcileLabels, Gauge>::default();
        registry.register(
            "postgres_operator_cluster_replicas_desired",
            "Desired number of replicas for each cluster",
            cluster_replicas_desired.clone(),
        );

        let cluster_replicas_ready = Family::<ReconcileLabels, Gauge>::default();
        registry.register(
            "postgres_operator_cluster_replicas_ready",
            "Number of ready replicas for each cluster",
            cluster_replicas_ready.clone(),
        );

        Self {
            reconciliations_total,
            reconciliation_errors_total,
            reconcile_duration_seconds,
            clusters_total,
            cluster_replicas_desired,
            cluster_replicas_ready,
            registry,
        }
    }

    /// Record a successful reconciliation
    pub fn record_reconcile(&self, namespace: &str, name: &str, duration_secs: f64) {
        let labels = ReconcileLabels {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        self.reconciliations_total.get_or_create(&labels).inc();
        self.reconcile_duration_seconds
            .get_or_create(&labels)
            .observe(duration_secs);
    }

    /// Record a failed reconciliation
    pub fn record_error(&self, namespace: &str, name: &str) {
        let labels = ReconcileLabels {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        self.reconciliation_errors_total
            .get_or_create(&labels)
            .inc();
    }

    /// Update cluster phase count
    ///
    /// This should be called with the current count for each phase
    /// after listing all clusters.
    pub fn set_clusters_by_phase(&self, phase: &str, count: i64) {
        let labels = PhaseLabels {
            phase: phase.to_string(),
        };
        self.clusters_total.get_or_create(&labels).set(count);
    }

    /// Update cluster replica metrics
    pub fn set_cluster_replicas(&self, namespace: &str, name: &str, desired: i64, ready: i64) {
        let labels = ReconcileLabels {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        self.cluster_replicas_desired
            .get_or_create(&labels)
            .set(desired);
        self.cluster_replicas_ready
            .get_or_create(&labels)
            .set(ready);
    }

    /// Encode metrics to Prometheus text format
    ///
    /// Returns an empty string if encoding fails (should never happen with valid metrics).
    fn encode(&self) -> String {
        let mut buffer = String::new();
        if let Err(e) = encode(&mut buffer, &self.registry) {
            tracing::error!("Failed to encode metrics: {}", e);
            return String::new();
        }
        buffer
    }
}

/// Shared state for the health server
pub struct HealthState {
    /// Whether the operator is ready (connected to K8s API)
    pub ready: RwLock<bool>,
    /// Metrics registry
    pub metrics: Metrics,
    /// Last successful reconcile timestamp
    pub last_reconcile: AtomicU64,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    pub fn new() -> Self {
        Self {
            ready: RwLock::new(false),
            metrics: Metrics::new(),
            last_reconcile: AtomicU64::new(0),
        }
    }

    /// Mark the operator as ready
    pub async fn set_ready(&self, ready: bool) {
        *self.ready.write().await = ready;
    }

    /// Check if the operator is ready
    pub async fn is_ready(&self) -> bool {
        *self.ready.read().await
    }
}

/// Liveness probe handler
///
/// Returns 200 OK if the process is alive.
/// This is a simple check - if we can respond, we're alive.
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness probe handler
///
/// Returns 200 OK if the operator is ready to serve.
/// Returns 503 Service Unavailable if not ready.
async fn readyz(State(state): State<Arc<HealthState>>) -> Response {
    if state.is_ready().await {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

/// Metrics handler
///
/// Returns Prometheus-formatted metrics.
async fn metrics(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = state.metrics.encode();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Create the health server router
pub fn create_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Run the health server
///
/// Binds to 0.0.0.0:8080 and serves health/metrics endpoints.
pub async fn run_health_server(state: Arc<HealthState>) -> Result<(), std::io::Error> {
    let app = create_router(state);

    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    tracing::info!("Health server listening on 0.0.0.0:8080");

    axum::serve(listener, app).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();
        metrics.record_reconcile("default", "test-cluster", 0.5);
        metrics.record_error("default", "test-cluster");

        let encoded = metrics.encode();
        assert!(encoded.contains("postgres_operator_reconciliations"));
        assert!(encoded.contains("postgres_operator_reconciliation_errors"));
        assert!(encoded.contains("postgres_operator_reconcile_duration_seconds"));
    }

    #[test]
    fn test_fleet_metrics() {
        let metrics = Metrics::new();

        // Set cluster counts by phase
        metrics.set_clusters_by_phase("Running", 5);
        metrics.set_clusters_by_phase("Degraded", 1);
        metrics.set_clusters_by_phase("Creating", 2);

        // Set replica metrics for a cluster
        metrics.set_cluster_replicas("default", "prod-db", 3, 3);
        metrics.set_cluster_replicas("staging", "staging-db", 2, 1);

        let encoded = metrics.encode();
        assert!(encoded.contains("postgres_operator_clusters_total"));
        assert!(encoded.contains("postgres_operator_cluster_replicas_desired"));
        assert!(encoded.contains("postgres_operator_cluster_replicas_ready"));
    }

    #[tokio::test]
    async fn test_health_state() {
        let state = HealthState::new();
        assert!(!state.is_ready().await);

        state.set_ready(true).await;
        assert!(state.is_ready().await);
    }
}
