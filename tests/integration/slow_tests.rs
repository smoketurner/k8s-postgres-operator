//! Slow integration tests for postgres-operator
//!
//! These tests wait for pods to actually become ready, which requires:
//! - Pulling the Spilo container image
//! - Starting PostgreSQL
//! - Patroni leader election
//!
//! These tests take 2-5 minutes to run and are intended for nightly/release validation.
//!
//! Run with:
//! ```bash
//! cargo test --test integration slow_tests -- --ignored --test-threads=1
//! ```

use kube::Api;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use postgres_operator::crd::{ClusterPhase, PostgresCluster};
use std::time::Duration;

use crate::{
    PostgresClusterBuilder, ScopedOperator, SharedTestCluster, TestNamespace, ensure_crd_installed,
    ensure_operator_running, has_primary_pod, has_ready_replicas, is_phase,
};

/// Timeout for slow tests - 5 minutes to allow for image pull and startup
const SLOW_TIMEOUT: Duration = Duration::from_secs(300);

/// Test context that holds the operator for the test duration
struct TestContext {
    client: kube::Client,
    _operator: ScopedOperator,
    _cluster: std::sync::Arc<SharedTestCluster>,
}

/// Helper to set up test infrastructure
async fn setup() -> TestContext {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kube=warn")
        .with_test_writer()
        .try_init();

    let cluster = SharedTestCluster::get()
        .await
        .expect("Failed to get cluster");

    ensure_crd_installed(&cluster)
        .await
        .expect("Failed to install CRD");

    let client = cluster.new_client().await.expect("Failed to create client");
    let operator = ensure_operator_running(client.clone()).await;

    TestContext {
        client,
        _operator: operator,
        _cluster: cluster,
    }
}

/// Wait for a condition with timeout
async fn wait_for<C>(
    api: &Api<PostgresCluster>,
    name: &str,
    condition: C,
    timeout: Duration,
) -> Result<PostgresCluster, String>
where
    C: kube::runtime::wait::Condition<PostgresCluster>,
{
    use kube::runtime::wait::await_condition;

    let result = tokio::time::timeout(timeout, await_condition(api.clone(), name, condition)).await;

    match result {
        Ok(Ok(Some(cluster))) => Ok(cluster),
        Ok(Ok(None)) => Err("Resource not found".to_string()),
        Ok(Err(e)) => Err(format!("Watch error: {}", e)),
        Err(_) => Err("Timeout".to_string()),
    }
}

// =============================================================================
// SLOW TESTS - Wait for actual pod readiness
// =============================================================================

/// Test: Single replica cluster reaches Running phase
///
/// This test waits for the pod to actually become ready, which requires:
/// - Image pull (first run can take a while)
/// - PostgreSQL startup
/// - Patroni initialization
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_single_reaches_running_phase() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-run")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-slow-run", ns.name())
        .with_version("16")
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running phase - this is the key difference from fast tests
    let result = wait_for(
        &api,
        "test-slow-run",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await;

    match &result {
        Ok(cluster) => {
            let status = cluster.status.as_ref().expect("should have status");
            tracing::info!(
                "Cluster reached Running phase: ready_replicas={}, primary_pod={:?}",
                status.ready_replicas,
                status.primary_pod
            );
        }
        Err(e) => {
            // Log current state for debugging
            if let Ok(cluster) = api.get("test-slow-run").await {
                let phase = cluster.status.as_ref().map(|s| &s.phase);
                tracing::error!(
                    "Cluster did not reach Running phase: current_phase={:?}, error={}",
                    phase,
                    e
                );
            }
        }
    }

    result.expect("cluster should reach Running phase");

    api.delete("test-slow-run", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Single replica cluster has ready replicas
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_single_has_ready_replica() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-rdy")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-slow-rdy", ns.name())
        .with_version("16")
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for at least 1 ready replica
    let cluster = wait_for(&api, "test-slow-rdy", has_ready_replicas(1), SLOW_TIMEOUT)
        .await
        .expect("cluster should have ready replicas");

    let status = cluster.status.as_ref().expect("should have status");
    assert!(
        status.ready_replicas >= 1,
        "Should have at least 1 ready replica, got {}",
        status.ready_replicas
    );

    api.delete("test-slow-rdy", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: HA cluster (3 replicas) reaches Running phase
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_ha_reaches_running_phase() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-ha-run")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::ha("test-slow-ha", ns.name())
        .with_version("16")
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running phase
    let cluster = wait_for(
        &api,
        "test-slow-ha",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("HA cluster should reach Running phase");

    let status = cluster.status.as_ref().expect("should have status");
    tracing::info!(
        "HA cluster reached Running phase: ready_replicas={}, primary_pod={:?}",
        status.ready_replicas,
        status.primary_pod
    );

    api.delete("test-slow-ha", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: HA cluster has all replicas ready
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_ha_has_ready_replicas() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-ha-rdy")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::ha("test-ha-rdy", ns.name())
        .with_version("16")
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for all 3 replicas to be ready
    let cluster = wait_for(&api, "test-ha-rdy", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("HA cluster should have 3 ready replicas");

    let status = cluster.status.as_ref().expect("should have status");
    assert_eq!(
        status.ready_replicas, 3,
        "Should have exactly 3 ready replicas"
    );

    api.delete("test-ha-rdy", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: HA cluster elects a primary
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_ha_elects_primary() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-ha-pri")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::ha("test-ha-pri", ns.name())
        .with_version("16")
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for primary pod to be elected
    let cluster = wait_for(&api, "test-ha-pri", has_primary_pod(), SLOW_TIMEOUT)
        .await
        .expect("HA cluster should elect a primary");

    let status = cluster.status.as_ref().expect("should have status");
    let primary = status
        .primary_pod
        .as_ref()
        .expect("should have primary pod");
    tracing::info!("HA cluster elected primary: {}", primary);

    // Primary pod name should follow the pattern <cluster-name>-<number>
    assert!(
        primary.starts_with("test-ha-pri-"),
        "Primary pod should be named test-ha-pri-*, got {}",
        primary
    );

    api.delete("test-ha-pri", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Scaling updates ready replicas count
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_scaling_updates_ready_replicas() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-scale")
        .await
        .expect("create ns");

    // Start with 1 replica
    let pg = PostgresClusterBuilder::single("test-scale", ns.name())
        .with_version("16")
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for initial replica to be ready
    wait_for(&api, "test-scale", has_ready_replicas(1), SLOW_TIMEOUT)
        .await
        .expect("initial replica should be ready");

    // Scale up to 3 replicas
    let patch = serde_json::json!({
        "spec": { "replicas": 3 }
    });
    api.patch(
        "test-scale",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    // Wait for all 3 replicas to be ready
    let cluster = wait_for(&api, "test-scale", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("cluster should scale to 3 ready replicas");

    let status = cluster.status.as_ref().expect("should have status");
    assert_eq!(
        status.ready_replicas, 3,
        "Should have 3 ready replicas after scaling"
    );

    api.delete("test-scale", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}
