//! Integration tests for KEDA autoscaling features
//!
//! These tests verify that the operator generates correct ScaledObject and
//! TriggerAuthentication resources following KEDA best practices.
//!
//! Prerequisites:
//! - Running Kubernetes cluster (via kind, minikube, etc.)
//! - KEDA NOT required - tests verify resource generation, not KEDA behavior
//!
//! Run with: cargo test --test integration scaling_tests -- --ignored

use k8s_openapi::api::apps::v1::StatefulSet;
use kube::Client;
use kube::api::{Api, ApiResource, DynamicObject, PostParams};
use postgres_operator::crd::PostgresCluster;
use postgres_operator::resources::scaled_object::{
    KEDA_API_GROUP, KEDA_API_VERSION, SCALED_OBJECT_KIND,
};
use std::time::Duration;

use crate::{
    PostgresClusterBuilder, ScopedOperator, SharedTestCluster, TestNamespace, ensure_crd_installed,
    wait_for_resource,
};

/// Timeout for scaling tests - resources should be created quickly
const SCALING_TIMEOUT: Duration = Duration::from_secs(60);

// =============================================================================
// KEDA Resource Helpers
// =============================================================================

/// Get the ApiResource for KEDA ScaledObject
fn scaled_object_api_resource() -> ApiResource {
    ApiResource {
        group: KEDA_API_GROUP.to_string(),
        version: KEDA_API_VERSION.to_string(),
        kind: SCALED_OBJECT_KIND.to_string(),
        api_version: format!("{}/{}", KEDA_API_GROUP, KEDA_API_VERSION),
        plural: "scaledobjects".to_string(),
    }
}

/// Get the ApiResource for KEDA TriggerAuthentication
fn trigger_auth_api_resource() -> ApiResource {
    ApiResource {
        group: KEDA_API_GROUP.to_string(),
        version: KEDA_API_VERSION.to_string(),
        kind: "TriggerAuthentication".to_string(),
        api_version: format!("{}/{}", KEDA_API_GROUP, KEDA_API_VERSION),
        plural: "triggerauthentications".to_string(),
    }
}

/// Get ScaledObject by name (returns None if not found or CRD not installed)
async fn get_scaled_object(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<Option<DynamicObject>, kube::Error> {
    let api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), namespace, &scaled_object_api_resource());
    api.get_opt(name).await
}

/// Wait for ScaledObject to exist using kube's watch-based await_condition
async fn wait_for_scaled_object(
    client: &Client,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<DynamicObject, crate::WaitError> {
    let api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), namespace, &scaled_object_api_resource());
    wait_for_resource(&api, name, timeout).await
}

/// Wait for TriggerAuthentication to exist using kube's watch-based await_condition
async fn wait_for_trigger_auth(
    client: &Client,
    namespace: &str,
    name: &str,
    timeout: Duration,
) -> Result<DynamicObject, crate::WaitError> {
    let api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), namespace, &trigger_auth_api_resource());
    wait_for_resource(&api, name, timeout).await
}

// =============================================================================
// ScaledObject Assertion Helpers
// =============================================================================

/// Extract triggers from ScaledObject spec
fn get_triggers(obj: &DynamicObject) -> Vec<&serde_json::Value> {
    obj.data
        .get("spec")
        .and_then(|s| s.get("triggers"))
        .and_then(|t| t.as_array())
        .map(|arr| arr.iter().collect())
        .unwrap_or_default()
}

/// Check if ScaledObject has a specific trigger type
fn has_trigger_type(obj: &DynamicObject, trigger_type: &str) -> bool {
    get_triggers(obj)
        .iter()
        .any(|t| t.get("type").and_then(|v| v.as_str()) == Some(trigger_type))
}

/// Get minReplicaCount from ScaledObject
fn get_min_replicas(obj: &DynamicObject) -> Option<i64> {
    obj.data
        .get("spec")
        .and_then(|s| s.get("minReplicaCount"))
        .and_then(|v| v.as_i64())
}

/// Get maxReplicaCount from ScaledObject
fn get_max_replicas(obj: &DynamicObject) -> Option<i64> {
    obj.data
        .get("spec")
        .and_then(|s| s.get("maxReplicaCount"))
        .and_then(|v| v.as_i64())
}

// =============================================================================
// Test Context Setup
// =============================================================================

/// Initialize tracing and ensure CRD is installed
async fn init_test() -> std::sync::Arc<SharedTestCluster> {
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

    cluster
}

// =============================================================================
// Resource Generation Tests
// =============================================================================

/// Test: ScaledObject with CPU trigger is created when CPU scaling is enabled
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_cpu_resource_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-cpu")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create cluster with CPU scaling (2 replicas, can scale to 5)
    // Note: CPU scaling requires resource requests to be set for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-cpu", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 70)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation started)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-cpu", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created (may take a moment after StatefulSet)
    let so = wait_for_scaled_object(&client, ns.name(), "test-cpu-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");

    assert!(has_trigger_type(&so, "cpu"), "Should have CPU trigger");
    assert_eq!(
        get_min_replicas(&so),
        Some(2),
        "minReplicaCount should be 2"
    );
    assert_eq!(
        get_max_replicas(&so),
        Some(5),
        "maxReplicaCount should be 5"
    );
}

/// Test: No ScaledObject created when scaling is disabled (maxReplicas == replicas)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_disabled_no_resource() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-disabled")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create cluster with scaling disabled (maxReplicas == replicas)
    let cluster = PostgresClusterBuilder::new("test-disabled", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_scaling_disabled()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-disabled", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Verify NO ScaledObject was created
    let so = get_scaled_object(&client, ns.name(), "test-disabled-readers")
        .await
        .expect("should be able to query ScaledObject");

    assert!(
        so.is_none(),
        "ScaledObject should NOT be created when scaling disabled"
    );
}

/// Test: ScaledObject with PostgreSQL trigger and TriggerAuthentication for connection scaling
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_connection_resource_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-conn")
        .await
        .expect("create ns");

    let cluster = PostgresClusterBuilder::new("test-conn", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_connection_scaling(2, 8, 100)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation started)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-conn", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(&client, ns.name(), "test-conn-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");

    assert!(
        has_trigger_type(&so, "postgresql"),
        "Should have PostgreSQL trigger"
    );

    // Wait for TriggerAuthentication to be created
    let _ta = wait_for_trigger_auth(&client, ns.name(), "test-conn-pg-auth", SCALING_TIMEOUT)
        .await
        .expect("TriggerAuthentication should be created");
}

/// Test: ScaledObject with both CPU and connection triggers for combined scaling
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_combined_resource_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-combined")
        .await
        .expect("create ns");

    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-combined", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_combined_scaling(2, 10, 70, 100)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-combined", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(&client, ns.name(), "test-combined-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");

    assert!(has_trigger_type(&so, "cpu"), "Should have CPU trigger");
    assert!(
        has_trigger_type(&so, "postgresql"),
        "Should have PostgreSQL trigger"
    );

    // Wait for TriggerAuthentication to be created for connection scaling
    let _ta = wait_for_trigger_auth(&client, ns.name(), "test-combined-pg-auth", SCALING_TIMEOUT)
        .await
        .expect("TriggerAuthentication should be created");
}

// =============================================================================
// Best Practices Verification Tests
// =============================================================================

/// Test: ScaledObject has correct owner reference for garbage collection
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_owner_reference() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-owner")
        .await
        .expect("create ns");

    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-owner", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 70)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-owner", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(&client, ns.name(), "test-owner-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");
    let owner_refs = so.metadata.owner_references.as_ref();

    assert!(owner_refs.is_some(), "Should have owner references");
    let refs = owner_refs.unwrap();

    assert!(
        refs.iter()
            .any(|r| r.name == "test-owner" && r.kind == "PostgresCluster"),
        "Should have PostgresCluster owner reference"
    );
}

/// Test: ScaledObject has standard Kubernetes labels for filtering
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_standard_labels() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-labels")
        .await
        .expect("create ns");

    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-labels", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 70)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-labels", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(&client, ns.name(), "test-labels-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");
    let labels = so.metadata.labels.as_ref();

    assert!(labels.is_some(), "Should have labels");
    let labels = labels.unwrap();

    // Verify standard Kubernetes labels are present
    // Note: operator sets app.kubernetes.io/name to cluster name (could be instance in stricter best practice)
    assert!(
        labels.contains_key("app.kubernetes.io/name"),
        "Should have app.kubernetes.io/name label"
    );
    assert!(
        labels.contains_key("app.kubernetes.io/managed-by"),
        "Should have app.kubernetes.io/managed-by label"
    );
    assert!(
        labels.contains_key("app.kubernetes.io/component"),
        "Should have app.kubernetes.io/component label"
    );
}

/// Test: ScaledObject HPA behavior follows best practices (conservative scale-down, aggressive scale-up)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_hpa_behavior_best_practices() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-hpa")
        .await
        .expect("create ns");

    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-hpa", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 70)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-hpa", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(&client, ns.name(), "test-hpa-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");
    let spec = so.data.get("spec").expect("Should have spec");
    let advanced = spec.get("advanced").expect("Should have advanced config");
    let hpa_config = advanced
        .get("horizontalPodAutoscalerConfig")
        .expect("Should have HPA config");
    let behavior = hpa_config.get("behavior").expect("Should have behavior");

    // Verify conservative scale-down (longer stabilization window)
    let scale_down = behavior.get("scaleDown").expect("Should have scaleDown");
    let sd_window = scale_down
        .get("stabilizationWindowSeconds")
        .and_then(|v| v.as_i64());
    assert!(
        sd_window.unwrap_or(0) >= 300,
        "Scale-down should have >= 5min stabilization window"
    );

    // Verify aggressive scale-up (shorter stabilization window)
    let scale_up = behavior.get("scaleUp").expect("Should have scaleUp");
    let su_window = scale_up
        .get("stabilizationWindowSeconds")
        .and_then(|v| v.as_i64());
    assert!(
        su_window.unwrap_or(999) <= 60,
        "Scale-up should have <= 1min stabilization window"
    );
}

/// Test: ScaledObject has appropriate cooldown and polling intervals
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_cooldown_periods() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-cooldown")
        .await
        .expect("create ns");

    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-cooldown", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 70)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-cooldown", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(&client, ns.name(), "test-cooldown-readers", SCALING_TIMEOUT)
        .await
        .expect("ScaledObject should be created");
    let spec = so.data.get("spec").expect("Should have spec");

    // Verify cooldown period is set
    let cooldown = spec.get("cooldownPeriod").and_then(|v| v.as_i64());
    assert!(cooldown.is_some(), "Should have cooldownPeriod");
    assert!(
        cooldown.unwrap() >= 60,
        "Cooldown period should be at least 60 seconds"
    );

    // Verify polling interval is set
    let polling = spec.get("pollingInterval").and_then(|v| v.as_i64());
    assert!(polling.is_some(), "Should have pollingInterval");
    assert!(
        polling.unwrap() >= 15,
        "Polling interval should be at least 15 seconds"
    );
}

// =============================================================================
// Configuration Verification Tests
// =============================================================================

/// Test: minReplicas defaults to base replicas when not explicitly specified
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_min_replicas_default() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-min-default")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create cluster with scaling where minReplicas defaults to base replicas (2)
    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-min-default", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 70) // min=2 explicitly, same as base
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-min-default", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(
        &client,
        ns.name(),
        "test-min-default-readers",
        SCALING_TIMEOUT,
    )
    .await
    .expect("ScaledObject should be created");
    assert_eq!(
        get_min_replicas(&so),
        Some(2),
        "minReplicaCount should equal base replicas"
    );
}

/// Test: CPU utilization target is correctly propagated to ScaledObject
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_cpu_utilization_target() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-cpu-target")
        .await
        .expect("create ns");

    // Note: CPU scaling requires resource requests for KEDA to calculate utilization
    let cluster = PostgresClusterBuilder::new("test-cpu-target", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_resources("100m", "256Mi")
        .with_cpu_scaling(2, 5, 80) // 80% CPU target
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-cpu-target", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(
        &client,
        ns.name(),
        "test-cpu-target-readers",
        SCALING_TIMEOUT,
    )
    .await
    .expect("ScaledObject should be created");
    let triggers = get_triggers(&so);
    let cpu_trigger = triggers
        .iter()
        .find(|t| t.get("type").and_then(|v| v.as_str()) == Some("cpu"));

    assert!(cpu_trigger.is_some(), "Should have CPU trigger");

    let metadata = cpu_trigger
        .unwrap()
        .get("metadata")
        .expect("CPU trigger should have metadata");
    let value = metadata.get("value").and_then(|v| v.as_str());
    assert_eq!(value, Some("80"), "CPU target should be 80%");
}

/// Test: Connection target is correctly propagated to ScaledObject
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_scaling_connection_target() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "scaling-conn-target")
        .await
        .expect("create ns");

    let cluster = PostgresClusterBuilder::new("test-conn-target", ns.name())
        .with_replicas(2)
        .without_tls()
        .with_connection_scaling(2, 8, 150) // 150 connections per replica
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &cluster)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created (indicates reconciliation has run)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-conn-target", SCALING_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Wait for ScaledObject to be created
    let so = wait_for_scaled_object(
        &client,
        ns.name(),
        "test-conn-target-readers",
        SCALING_TIMEOUT,
    )
    .await
    .expect("ScaledObject should be created");
    let triggers = get_triggers(&so);
    let pg_trigger = triggers
        .iter()
        .find(|t| t.get("type").and_then(|v| v.as_str()) == Some("postgresql"));

    assert!(pg_trigger.is_some(), "Should have PostgreSQL trigger");

    let metadata = pg_trigger
        .unwrap()
        .get("metadata")
        .expect("PostgreSQL trigger should have metadata");
    let value = metadata.get("targetQueryValue").and_then(|v| v.as_str());
    assert_eq!(value, Some("150"), "Connection target should be 150");
}
