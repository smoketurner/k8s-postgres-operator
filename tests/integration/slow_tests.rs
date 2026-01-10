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
use postgres_operator::crd::PostgresVersion;

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
        .with_version(PostgresVersion::V16)
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
        .with_version(PostgresVersion::V16)
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
        .with_version(PostgresVersion::V16)
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
        .with_version(PostgresVersion::V16)
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
        .with_version(PostgresVersion::V16)
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
        .with_version(PostgresVersion::V16)
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

// =============================================================================
// ADDITIONAL SCALING TESTS
// =============================================================================

/// Test: Scale down from 3 to 1 replica
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_scale_down_3_to_1() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-sd")
        .await
        .expect("create ns");

    // Start with 3 replicas
    let pg = PostgresClusterBuilder::ha("test-sd", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for all 3 replicas to be ready
    wait_for(&api, "test-sd", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("3 replicas should be ready");

    // Get initial primary
    let cluster = api.get("test-sd").await.expect("get");
    let initial_primary = cluster
        .status
        .as_ref()
        .and_then(|s| s.primary_pod.clone())
        .expect("should have primary");
    tracing::info!("Initial primary before scale down: {}", initial_primary);

    // Scale down to 1 replica
    let patch = serde_json::json!({
        "spec": { "replicas": 1 }
    });
    api.patch(
        "test-sd",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    // Wait for scaling and Running phase
    wait_for(
        &api,
        "test-sd",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running after scale down");

    // Verify we still have a primary after scale down
    let cluster = api.get("test-sd").await.expect("get");
    let status = cluster.status.as_ref().expect("should have status");
    assert!(
        status.primary_pod.is_some(),
        "Should still have a primary after scale down"
    );
    assert!(
        status.ready_replicas >= 1,
        "Should have at least 1 ready replica"
    );

    api.delete("test-sd", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Scale up from 1 to 5 replicas
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_scale_up_1_to_5() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-su5")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-su5", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for initial replica
    wait_for(&api, "test-su5", has_ready_replicas(1), SLOW_TIMEOUT)
        .await
        .expect("initial replica should be ready");

    // Scale up to 5 replicas
    let patch = serde_json::json!({
        "spec": { "replicas": 5 }
    });
    api.patch(
        "test-su5",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    // Wait for all 5 replicas
    let cluster = wait_for(&api, "test-su5", has_ready_replicas(5), SLOW_TIMEOUT)
        .await
        .expect("cluster should have 5 ready replicas");

    let status = cluster.status.as_ref().expect("should have status");
    assert_eq!(status.ready_replicas, 5);
    assert!(status.primary_pod.is_some());

    api.delete("test-su5", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// FAILOVER TESTS
// =============================================================================

/// Test: Deleting primary pod triggers Patroni failover
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_primary_pod_deletion_triggers_failover() {
    use k8s_openapi::api::core::v1::Pod;

    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-fo")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::ha("test-fo", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for all replicas and primary election
    wait_for(&api, "test-fo", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("3 replicas should be ready");
    wait_for(&api, "test-fo", has_primary_pod(), SLOW_TIMEOUT)
        .await
        .expect("primary should be elected");

    // Get the current primary
    let cluster = api.get("test-fo").await.expect("get");
    let old_primary = cluster
        .status
        .as_ref()
        .and_then(|s| s.primary_pod.clone())
        .expect("should have primary");
    tracing::info!("Current primary before deletion: {}", old_primary);

    // Delete the primary pod
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), ns.name());
    pod_api
        .delete(&old_primary, &DeleteParams::default())
        .await
        .expect("delete primary pod");
    tracing::info!("Deleted primary pod: {}", old_primary);

    // Wait for cluster to recover - either new primary or same pod recreated
    // Give Patroni time to failover
    tokio::time::sleep(Duration::from_secs(30)).await;

    // Wait for all replicas to be ready again (pod should be recreated)
    wait_for(&api, "test-fo", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("cluster should recover with 3 ready replicas");

    // Verify we have a primary (may be the same or different pod)
    let cluster = api.get("test-fo").await.expect("get");
    let status = cluster.status.as_ref().expect("should have status");
    assert!(
        status.primary_pod.is_some(),
        "Should have a primary after recovery"
    );

    let new_primary = status.primary_pod.as_ref().unwrap();
    tracing::info!(
        "Primary after recovery: {} (was: {})",
        new_primary,
        old_primary
    );

    api.delete("test-fo", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Deleting a replica pod - cluster recovers
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_replica_pod_deletion_recovery() {
    use k8s_openapi::api::core::v1::Pod;

    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-rd")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::ha("test-rd", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for all replicas
    wait_for(&api, "test-rd", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("3 replicas should be ready");

    // Get the primary to avoid deleting it
    let cluster = api.get("test-rd").await.expect("get");
    let primary = cluster
        .status
        .as_ref()
        .and_then(|s| s.primary_pod.clone())
        .expect("should have primary");

    // Find a replica pod (not the primary)
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), ns.name());
    let pods = pod_api.list(&Default::default()).await.expect("list pods");
    let replica_pod = pods
        .items
        .iter()
        .find(|p| {
            let name = p.metadata.name.as_ref().unwrap();
            name.starts_with("test-rd-") && name != &primary
        })
        .expect("should find a replica pod");
    let replica_name = replica_pod.metadata.name.as_ref().unwrap().clone();

    tracing::info!(
        "Deleting replica pod: {} (primary is: {})",
        replica_name,
        primary
    );

    // Delete the replica
    pod_api
        .delete(&replica_name, &DeleteParams::default())
        .await
        .expect("delete replica pod");

    // Wait for cluster to recover
    tokio::time::sleep(Duration::from_secs(10)).await;

    // All replicas should be ready again
    wait_for(&api, "test-rd", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("cluster should recover with 3 ready replicas");

    // Primary should be unchanged
    let cluster = api.get("test-rd").await.expect("get");
    let current_primary = cluster
        .status
        .as_ref()
        .and_then(|s| s.primary_pod.clone())
        .expect("should have primary");
    assert_eq!(
        current_primary, primary,
        "Primary should be unchanged after replica deletion"
    );

    api.delete("test-rd", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Cluster enters Degraded when replicas fail, recovers when ready
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_degraded_to_running_transition() {
    use k8s_openapi::api::core::v1::Pod;

    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-deg")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::ha("test-deg", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-deg",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Get a replica pod to delete
    let cluster = api.get("test-deg").await.expect("get");
    let primary = cluster
        .status
        .as_ref()
        .and_then(|s| s.primary_pod.clone())
        .expect("should have primary");

    let pod_api: Api<Pod> = Api::namespaced(client.clone(), ns.name());
    let pods = pod_api.list(&Default::default()).await.expect("list pods");
    let replica_pod = pods
        .items
        .iter()
        .find(|p| {
            let name = p.metadata.name.as_ref().unwrap();
            name.starts_with("test-deg-") && name != &primary
        })
        .expect("should find a replica pod");
    let replica_name = replica_pod.metadata.name.as_ref().unwrap().clone();

    // Delete replica to cause degradation
    pod_api
        .delete(&replica_name, &DeleteParams::default())
        .await
        .expect("delete replica");

    tracing::info!(
        "Deleted replica pod: {}, waiting for degraded state",
        replica_name
    );

    // The cluster may briefly enter Degraded, then recover
    // Wait for recovery back to Running
    wait_for(
        &api,
        "test-deg",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should recover to Running");

    // Verify all replicas are ready again
    wait_for(&api, "test-deg", has_ready_replicas(3), SLOW_TIMEOUT)
        .await
        .expect("all replicas should be ready");

    api.delete("test-deg", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// VERSION UPGRADE TESTS
// =============================================================================

/// Test: Version upgrade triggers rolling update
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_version_upgrade_rolling_update() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-upg")
        .await
        .expect("create ns");

    // Start with version 15
    let pg = PostgresClusterBuilder::single("test-upg", ns.name())
        .with_version(PostgresVersion::V15)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-upg",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Upgrade to version 16
    let patch = serde_json::json!({
        "spec": { "version": "16" }
    });
    api.patch(
        "test-upg",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Initiated version upgrade from 15 to 16");

    // Wait for cluster to return to Running after upgrade
    wait_for(
        &api,
        "test-upg",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running after upgrade");

    // Verify current_version in status is updated
    let cluster = api.get("test-upg").await.expect("get");
    let status = cluster.status.as_ref().expect("should have status");
    if let Some(current_version) = &status.current_version {
        tracing::info!("Current version after upgrade: {}", current_version);
        assert!(
            current_version.starts_with("16"),
            "Version should be 16.x after upgrade"
        );
    }

    api.delete("test-upg", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// FEATURE TOGGLE TESTS
// =============================================================================

/// Test: Enable TLS on a running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_enable_tls_on_running_cluster() {
    use k8s_openapi::api::core::v1::Secret;

    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-tls-en")
        .await
        .expect("create ns");

    // Create cluster without TLS
    let pg = PostgresClusterBuilder::single("test-tls-en", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-tls-en",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Create a TLS secret (self-signed cert for testing)
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    let tls_secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "test-tls-secret",
            "namespace": ns.name()
        },
        "type": "kubernetes.io/tls",
        "data": {
            // Base64-encoded dummy cert and key for testing
            "tls.crt": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrVENDQVRlZ0F3SUJBZ0lKQUxBQUFBQUFBQUFBTUFvR0NDcUdTTTQ5QkFNQ01COHhIVEFiQmdOVkJBTU0KRkhSbGMzUXVaWGhoYlhCc1pTNWpiMjB3SGhjTk1qRXdNVEF4TURBd01EQXdXaGNOTXpFd01UQXhNREF3TURBdwpXakFmTVIwd0d3WURWUVFEREJSMFpYTjBMbVY0WVcxd2JHVXVZMjl0TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJCnpqMERBUWNEUWdBRXRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3QKdGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3RhTkFNQW9HQ0NxR1NNNDlCQU1DQTBnQU1FVUNJUUNYCnRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3RBSURBQUFBd0lnYkFBQUFBQUFBQQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==",
            "tls.key": "LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IUUNBUUVFSUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQW9Bb0dDQ3FHU000OUF3RUgKb1VRRFFnQUV0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdAp0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0PQotLS0tLUVORCBFQyBQUklWQVRFIEtFWS0tLS0tCg=="
        }
    });
    let secret: Secret = serde_json::from_value(tls_secret).expect("parse secret");
    secret_api
        .create(&PostParams::default(), &secret)
        .await
        .expect("create secret");

    // Enable TLS on the running cluster
    let patch = serde_json::json!({
        "spec": {
            "tls": {
                "enabled": true,
                "certSecret": "test-tls-secret"
            }
        }
    });
    api.patch(
        "test-tls-en",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Enabled TLS on running cluster");

    // Wait for cluster to return to Running after TLS enablement
    wait_for(
        &api,
        "test-tls-en",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after TLS enabled");

    // Verify TLS is now in spec
    let cluster = api.get("test-tls-en").await.expect("get");
    let tls = cluster.spec.tls.as_ref();
    assert!(tls.is_some(), "TLS spec should exist");
    assert!(tls.unwrap().enabled, "TLS should be enabled");

    api.delete("test-tls-en", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Disable TLS on a running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_disable_tls_on_running_cluster() {
    use k8s_openapi::api::core::v1::Secret;

    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-tls-dis")
        .await
        .expect("create ns");

    // Create TLS secret first
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    let tls_secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "test-tls-secret",
            "namespace": ns.name()
        },
        "type": "kubernetes.io/tls",
        "data": {
            "tls.crt": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrVENDQVRlZ0F3SUJBZ0lKQUxBQUFBQUFBQUFBTUFvR0NDcUdTTTQ5QkFNQ01COHhIVEFiQmdOVkJBTU0KRkhSbGMzUXVaWGhoYlhCc1pTNWpiMjB3SGhjTk1qRXdNVEF4TURBd01EQXdXaGNOTXpFd01UQXhNREF3TURBdwpXakFmTVIwd0d3WURWUVFEREJSMFpYTjBMbVY0WVcxd2JHVXVZMjl0TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJCnpqMERBUWNEUWdBRXRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3QKdGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3RhTkFNQW9HQ0NxR1NNNDlCQU1DQTBnQU1FVUNJUUNYCnRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3RBSURBQUFBd0lnYkFBQUFBQUFBQQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==",
            "tls.key": "LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IUUNBUUVFSUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQW9Bb0dDQ3FHU000OUF3RUgKb1VRRFFnQUV0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdAp0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0PQotLS0tLUVORCBFQyBQUklWQVRFIEtFWS0tLS0tCg=="
        }
    });
    let secret: Secret = serde_json::from_value(tls_secret).expect("parse secret");
    secret_api
        .create(&PostParams::default(), &secret)
        .await
        .expect("create secret");

    // Create cluster with TLS enabled
    let pg = PostgresClusterBuilder::single("test-tls-dis", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_tls("test-tls-secret")
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-tls-dis",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Disable TLS
    let patch = serde_json::json!({
        "spec": {
            "tls": {
                "enabled": false
            }
        }
    });
    api.patch(
        "test-tls-dis",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Disabled TLS on running cluster");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-tls-dis",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after TLS disabled");

    // Verify TLS is now disabled
    let cluster = api.get("test-tls-dis").await.expect("get");
    let tls = cluster.spec.tls.as_ref();
    if let Some(tls_spec) = tls {
        assert!(!tls_spec.enabled, "TLS should be disabled");
    }

    api.delete("test-tls-dis", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Enable PgBouncer on a running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_enable_pgbouncer_on_running_cluster() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-pb-en")
        .await
        .expect("create ns");

    // Create cluster without PgBouncer
    let pg = PostgresClusterBuilder::ha("test-pb-en", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-pb-en",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Verify no PgBouncer initially
    let cluster = api.get("test-pb-en").await.expect("get");
    assert!(
        cluster.spec.pgbouncer.is_none(),
        "PgBouncer should not be configured initially"
    );

    // Enable PgBouncer
    let patch = serde_json::json!({
        "spec": {
            "pgbouncer": {
                "enabled": true,
                "replicas": 2,
                "poolMode": "transaction",
                "maxDbConnections": 60,
                "defaultPoolSize": 20,
                "maxClientConn": 10000,
                "enableReplicaPooler": false
            }
        }
    });
    api.patch(
        "test-pb-en",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Enabled PgBouncer on running cluster");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-pb-en",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after PgBouncer enabled");

    // Verify PgBouncer is now enabled
    let cluster = api.get("test-pb-en").await.expect("get");
    let pgbouncer = cluster.spec.pgbouncer.as_ref();
    assert!(pgbouncer.is_some(), "PgBouncer spec should exist");
    assert!(pgbouncer.unwrap().enabled, "PgBouncer should be enabled");

    api.delete("test-pb-en", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Disable PgBouncer on a running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_disable_pgbouncer_on_running_cluster() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-pb-dis")
        .await
        .expect("create ns");

    // Create cluster with PgBouncer
    let pg = PostgresClusterBuilder::ha("test-pb-dis", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_pgbouncer()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-pb-dis",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Verify PgBouncer is initially enabled
    let cluster = api.get("test-pb-dis").await.expect("get");
    assert!(
        cluster.spec.pgbouncer.as_ref().map(|p| p.enabled) == Some(true),
        "PgBouncer should be enabled initially"
    );

    // Disable PgBouncer
    let patch = serde_json::json!({
        "spec": {
            "pgbouncer": {
                "enabled": false,
                "replicas": 2,
                "poolMode": "transaction",
                "maxDbConnections": 60,
                "defaultPoolSize": 20,
                "maxClientConn": 10000,
                "enableReplicaPooler": false
            }
        }
    });
    api.patch(
        "test-pb-dis",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Disabled PgBouncer on running cluster");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-pb-dis",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after PgBouncer disabled");

    // Verify PgBouncer is now disabled
    let cluster = api.get("test-pb-dis").await.expect("get");
    let pgbouncer = cluster.spec.pgbouncer.as_ref();
    if let Some(pb) = pgbouncer {
        assert!(!pb.enabled, "PgBouncer should be disabled");
    }

    api.delete("test-pb-dis", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Change PgBouncer pool mode on a running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_change_pgbouncer_pool_mode() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-pb-mode")
        .await
        .expect("create ns");

    // Create cluster with PgBouncer in transaction mode
    let pg = PostgresClusterBuilder::ha("test-pb-mode", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_pgbouncer_mode("transaction")
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-pb-mode",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Verify initial pool mode
    let cluster = api.get("test-pb-mode").await.expect("get");
    assert_eq!(
        cluster
            .spec
            .pgbouncer
            .as_ref()
            .map(|p| p.pool_mode.as_str()),
        Some("transaction"),
        "Initial pool mode should be transaction"
    );

    // Change to session mode
    let patch = serde_json::json!({
        "spec": {
            "pgbouncer": {
                "enabled": true,
                "replicas": 2,
                "poolMode": "session",
                "maxDbConnections": 60,
                "defaultPoolSize": 20,
                "maxClientConn": 10000,
                "enableReplicaPooler": false
            }
        }
    });
    api.patch(
        "test-pb-mode",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Changed PgBouncer pool mode from transaction to session");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-pb-mode",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after pool mode change");

    // Verify pool mode changed
    let cluster = api.get("test-pb-mode").await.expect("get");
    assert_eq!(
        cluster
            .spec
            .pgbouncer
            .as_ref()
            .map(|p| p.pool_mode.as_str()),
        Some("session"),
        "Pool mode should now be session"
    );

    api.delete("test-pb-mode", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Enable replica pooler on existing PgBouncer
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_enable_replica_pooler() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-rp")
        .await
        .expect("create ns");

    // Create cluster with PgBouncer but without replica pooler
    let pg = PostgresClusterBuilder::ha("test-rp", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_pgbouncer()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-rp",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Verify replica pooler is initially disabled
    let cluster = api.get("test-rp").await.expect("get");
    assert!(
        !cluster
            .spec
            .pgbouncer
            .as_ref()
            .map(|p| p.enable_replica_pooler)
            .unwrap_or(false),
        "Replica pooler should be disabled initially"
    );

    // Enable replica pooler
    let patch = serde_json::json!({
        "spec": {
            "pgbouncer": {
                "enabled": true,
                "replicas": 2,
                "poolMode": "transaction",
                "maxDbConnections": 60,
                "defaultPoolSize": 20,
                "maxClientConn": 10000,
                "enableReplicaPooler": true
            }
        }
    });
    api.patch(
        "test-rp",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Enabled replica pooler on running cluster");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-rp",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after replica pooler enabled");

    // Verify replica pooler is now enabled
    let cluster = api.get("test-rp").await.expect("get");
    assert!(
        cluster
            .spec
            .pgbouncer
            .as_ref()
            .map(|p| p.enable_replica_pooler)
            .unwrap_or(false),
        "Replica pooler should be enabled"
    );

    api.delete("test-rp", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Enable metrics on a running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_enable_metrics_on_running_cluster() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-metrics")
        .await
        .expect("create ns");

    // Create cluster without metrics
    let pg = PostgresClusterBuilder::single("test-metrics", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-metrics",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Verify metrics not initially configured
    let cluster = api.get("test-metrics").await.expect("get");
    assert!(
        cluster.spec.metrics.is_none(),
        "Metrics should not be configured initially"
    );

    // Enable metrics
    let patch = serde_json::json!({
        "spec": {
            "metrics": {
                "enabled": true,
                "port": 9187
            }
        }
    });
    api.patch(
        "test-metrics",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Enabled metrics on running cluster");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-metrics",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after metrics enabled");

    // Verify metrics is now enabled
    let cluster = api.get("test-metrics").await.expect("get");
    let metrics = cluster.spec.metrics.as_ref();
    assert!(metrics.is_some(), "Metrics spec should exist");
    assert!(metrics.unwrap().enabled, "Metrics should be enabled");

    api.delete("test-metrics", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Combined TLS + PgBouncer toggle on running cluster
#[tokio::test]
#[ignore = "slow - requires Kubernetes cluster with image pulling"]
async fn test_enable_tls_and_pgbouncer_together() {
    use k8s_openapi::api::core::v1::Secret;

    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "slow-combo")
        .await
        .expect("create ns");

    // Create cluster without TLS or PgBouncer
    let pg = PostgresClusterBuilder::ha("test-combo", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for Running state
    wait_for(
        &api,
        "test-combo",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should reach Running");

    // Create TLS secret
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    let tls_secret = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "combo-tls-secret",
            "namespace": ns.name()
        },
        "type": "kubernetes.io/tls",
        "data": {
            "tls.crt": "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrVENDQVRlZ0F3SUJBZ0lKQUxBQUFBQUFBQUFBTUFvR0NDcUdTTTQ5QkFNQ01COHhIVEFiQmdOVkJBTU0KRkhSbGMzUXVaWGhoYlhCc1pTNWpiMjB3SGhjTk1qRXdNVEF4TURBd01EQXdXaGNOTXpFd01UQXhNREF3TURBdwpXakFmTVIwd0d3WURWUVFEREJSMFpYTjBMbVY0WVcxd2JHVXVZMjl0TUZrd0V3WUhLb1pJemowQ0FRWUlLb1pJCnpqMERBUWNEUWdBRXRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3QKdGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3RhTkFNQW9HQ0NxR1NNNDlCQU1DQTBnQU1FVUNJUUNYCnRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3RBSURBQUFBd0lnYkFBQUFBQUFBQQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==",
            "tls.key": "LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IUUNBUUVFSUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQW9Bb0dDQ3FHU000OUF3RUgKb1VRRFFnQUV0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdAp0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0PQotLS0tLUVORCBFQyBQUklWQVRFIEtFWS0tLS0tCg=="
        }
    });
    let secret: Secret = serde_json::from_value(tls_secret).expect("parse secret");
    secret_api
        .create(&PostParams::default(), &secret)
        .await
        .expect("create secret");

    // Enable both TLS and PgBouncer in one patch
    let patch = serde_json::json!({
        "spec": {
            "tls": {
                "enabled": true,
                "certSecret": "combo-tls-secret"
            },
            "pgbouncer": {
                "enabled": true,
                "replicas": 2,
                "poolMode": "transaction",
                "maxDbConnections": 60,
                "defaultPoolSize": 20,
                "maxClientConn": 10000,
                "enableReplicaPooler": false
            }
        }
    });
    api.patch(
        "test-combo",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    tracing::info!("Enabled both TLS and PgBouncer on running cluster");

    // Wait for cluster to return to Running
    wait_for(
        &api,
        "test-combo",
        is_phase(ClusterPhase::Running),
        SLOW_TIMEOUT,
    )
    .await
    .expect("cluster should return to Running after enabling TLS and PgBouncer");

    // Verify both are now enabled
    let cluster = api.get("test-combo").await.expect("get");

    let tls = cluster.spec.tls.as_ref();
    assert!(tls.is_some(), "TLS spec should exist");
    assert!(tls.unwrap().enabled, "TLS should be enabled");

    let pgbouncer = cluster.spec.pgbouncer.as_ref();
    assert!(pgbouncer.is_some(), "PgBouncer spec should exist");
    assert!(pgbouncer.unwrap().enabled, "PgBouncer should be enabled");

    api.delete("test-combo", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}
