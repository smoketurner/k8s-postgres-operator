//! Fast integration tests for postgres-operator
//!
//! These tests focus on operator logic - verifying that the operator creates
//! the correct Kubernetes resources and manages lifecycle correctly.
//!
//! They do NOT wait for pods to become ready (which would require pulling
//! container images, starting PostgreSQL, etc.). This makes tests fast.
//!
//! Tests verify:
//! - Correct resources are created (StatefulSets, Services, ConfigMaps, Secrets, PDBs)
//! - Owner references are set correctly for garbage collection
//! - Finalizers are added and removed correctly
//! - Status is updated with correct phase transitions
//! - Scaling operations update StatefulSet replicas
//! - Config changes update ConfigMap and trigger hash changes
//!
//! All PostgreSQL clusters now use Patroni for consistent management.

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service, ServiceAccount};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::Api;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::runtime::wait::await_condition;
use postgres_operator::crd::{ClusterPhase, PostgresCluster};
use std::time::Duration;

use crate::{
    PostgresClusterBuilder, ScopedOperator, SharedTestCluster, TestNamespace, ensure_crd_installed,
    ensure_operator_running,
};
use postgres_operator::crd::PostgresVersion;

/// Short timeout - we're testing operator logic, not pod readiness
const FAST_TIMEOUT: Duration = Duration::from_secs(15);

/// Test context that holds the operator for the test duration
struct TestContext {
    client: kube::Client,
    _operator: ScopedOperator,
    _cluster: std::sync::Arc<SharedTestCluster>,
}

/// Helper to set up test infrastructure
/// Returns a TestContext that keeps the operator alive for the test duration
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
    let result = tokio::time::timeout(timeout, await_condition(api.clone(), name, condition)).await;

    match result {
        Ok(Ok(Some(cluster))) => Ok(cluster),
        Ok(Ok(None)) => Err("Resource not found".to_string()),
        Ok(Err(e)) => Err(format!("Watch error: {}", e)),
        Err(_) => Err("Timeout".to_string()),
    }
}

/// Wait for a resource to exist with retry
async fn wait_for_resource<T>(api: &Api<T>, name: &str, timeout: Duration) -> Result<T, String>
where
    T: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource,
{
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        match api.get(name).await {
            Ok(resource) => return Ok(resource),
            Err(kube::Error::Api(e)) if e.code == 404 => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(format!("Timeout waiting for {}", name));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(format!("Error getting {}: {:?}", name, e)),
        }
    }
}

/// Condition: cluster has finalizer
fn has_finalizer() -> impl kube::runtime::wait::Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.and_then(|c| c.metadata.finalizers.as_ref())
            .map(|f| f.contains(&"postgres-operator.smoketurner.com/finalizer".to_string()))
            .unwrap_or(false)
    }
}

/// Condition: cluster has observedGeneration set
fn has_observed_generation() -> impl kube::runtime::wait::Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.and_then(|c| c.status.as_ref())
            .and_then(|s| s.observed_generation)
            .is_some()
    }
}

// =============================================================================
// SINGLE REPLICA CLUSTER TESTS (using Patroni)
// =============================================================================

/// Test: Single replica cluster creates all required Patroni resources
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_single_creates_resources() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "sa-res")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-sa", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for operator to fully process (has observedGeneration = reconciled spec)
    wait_for(&api, "test-sa", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should set observedGeneration");

    // Verify Patroni resources exist (with retry in case reconciliation is still completing)
    // StatefulSet uses cluster name directly
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    let sts = wait_for_resource(&sts_api, "test-sa", FAST_TIMEOUT)
        .await
        .expect("StatefulSet should exist");
    assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(1));

    // Primary service uses cluster name
    let svc_api: Api<Service> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&svc_api, "test-sa", FAST_TIMEOUT)
        .await
        .expect("Primary service should exist");

    // Patroni config
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&cm_api, "test-sa-patroni-config", FAST_TIMEOUT)
        .await
        .expect("Patroni ConfigMap should exist");

    let secret_api: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&secret_api, "test-sa-credentials", FAST_TIMEOUT)
        .await
        .expect("Secret should exist");

    // PDB uses cluster name + -pdb
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&pdb_api, "test-sa-pdb", FAST_TIMEOUT)
        .await
        .expect("PDB should exist");

    // RBAC resources for Patroni
    let sa_api: Api<ServiceAccount> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sa_api, "test-sa-patroni", FAST_TIMEOUT)
        .await
        .expect("ServiceAccount should exist");

    let role_api: Api<Role> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&role_api, "test-sa-patroni", FAST_TIMEOUT)
        .await
        .expect("Role should exist");

    let rb_api: Api<RoleBinding> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&rb_api, "test-sa-patroni", FAST_TIMEOUT)
        .await
        .expect("RoleBinding should exist");

    // Cleanup
    api.delete("test-sa", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Finalizer is added to cluster
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_finalizer_added() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "fin")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-fin", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for finalizer
    wait_for(&api, "test-fin", has_finalizer(), FAST_TIMEOUT)
        .await
        .expect("finalizer should be added");

    let cluster = api.get("test-fin").await.expect("get");
    let finalizers = cluster.metadata.finalizers.as_ref().unwrap();
    assert!(finalizers.contains(&"postgres-operator.smoketurner.com/finalizer".to_string()));

    api.delete("test-fin", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Owner references are set on child resources
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_owner_references() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "owner")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-own", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    wait_for(&api, "test-own", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should process");

    // Check StatefulSet owner reference (with retry)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    let sts = wait_for_resource(&sts_api, "test-own", FAST_TIMEOUT)
        .await
        .expect("get sts");
    let owner_refs = sts
        .metadata
        .owner_references
        .as_ref()
        .expect("should have owner refs");
    let has_owner = owner_refs
        .iter()
        .any(|r| r.kind == "PostgresCluster" && r.name == "test-own" && r.controller == Some(true));
    assert!(
        has_owner,
        "StatefulSet should have PostgresCluster as controller owner"
    );

    // Check Service owner reference
    let svc_api: Api<Service> = Api::namespaced(client.clone(), ns.name());
    let svc = wait_for_resource(&svc_api, "test-own", FAST_TIMEOUT)
        .await
        .expect("get svc");
    assert!(
        svc.metadata.owner_references.is_some(),
        "Service should have owner refs"
    );

    // Check ConfigMap owner reference
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), ns.name());
    let cm = wait_for_resource(&cm_api, "test-own-patroni-config", FAST_TIMEOUT)
        .await
        .expect("get cm");
    assert!(
        cm.metadata.owner_references.is_some(),
        "ConfigMap should have owner refs"
    );

    api.delete("test-own", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Status is set with observed generation
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_status_observed_generation() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "gen")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-gen", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    wait_for(&api, "test-gen", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should process");

    let cluster = api.get("test-gen").await.expect("get");
    let status = cluster.status.as_ref().expect("should have status");

    // observedGeneration should match metadata.generation
    assert!(
        status.observed_generation.is_some(),
        "observedGeneration should be set"
    );
    assert_eq!(
        status.observed_generation, cluster.metadata.generation,
        "observedGeneration should match generation"
    );

    api.delete("test-gen", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// HA CLUSTER TESTS (3 replicas, using Patroni)
// =============================================================================

/// Test: HA cluster (3 replicas) creates all Patroni resources
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_ha_creates_patroni_resources() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "ha")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::new("test-ha", ns.name())
        .with_replicas(3)
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    wait_for(&api, "test-ha", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should process");

    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());

    // Single StatefulSet with all replicas (Patroni manages roles)
    let sts = wait_for_resource(&sts_api, "test-ha", FAST_TIMEOUT)
        .await
        .expect("StatefulSet");
    assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(3));

    // All services should exist
    let svc_api: Api<Service> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&svc_api, "test-ha", FAST_TIMEOUT)
        .await
        .expect("primary service");
    wait_for_resource(&svc_api, "test-ha-repl", FAST_TIMEOUT)
        .await
        .expect("replica service");
    wait_for_resource(&svc_api, "test-ha-headless", FAST_TIMEOUT)
        .await
        .expect("headless service");

    // PDB should exist
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&pdb_api, "test-ha-pdb", FAST_TIMEOUT)
        .await
        .expect("pdb");

    api.delete("test-ha", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// UPDATE/SCALING TESTS
// =============================================================================

/// Test: Scaling replicas updates StatefulSet
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_scale_updates_statefulset() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "scale")
        .await
        .expect("create ns");

    // Start with 2 replicas
    let pg = PostgresClusterBuilder::new("test-scale", ns.name())
        .with_replicas(2)
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    wait_for(&api, "test-scale", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should process");

    // Verify initial replica count
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    let sts = wait_for_resource(&sts_api, "test-scale", FAST_TIMEOUT)
        .await
        .expect("StatefulSet");
    assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(2));

    // Get current generation before patching
    let cluster = api.get("test-scale").await.expect("get cluster");
    let pre_patch_gen = cluster.metadata.generation;

    // Scale up to 4 replicas
    let patch = serde_json::json!({
        "spec": { "replicas": 4 }
    });
    api.patch(
        "test-scale",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    // Wait for new generation to be observed
    let new_gen_observed = |obj: Option<&PostgresCluster>| {
        obj.and_then(|c| c.status.as_ref())
            .and_then(|s| s.observed_generation)
            .map(|g| Some(g) > pre_patch_gen)
            .unwrap_or(false)
    };
    wait_for(&api, "test-scale", new_gen_observed, FAST_TIMEOUT)
        .await
        .expect("new generation observed");

    // Verify StatefulSet was updated
    let sts = sts_api.get("test-scale").await.expect("get sts");
    assert_eq!(
        sts.spec.as_ref().unwrap().replicas,
        Some(4),
        "StatefulSet should be scaled to 4"
    );

    api.delete("test-scale", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Config change updates ConfigMap content
///
/// Note: Patroni handles config changes via the ConfigMap directly,
/// not through pod annotations like the old non-Patroni implementation.
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_config_change_updates_configmap() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "cfg")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-cfg", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    wait_for(&api, "test-cfg", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should process");

    // Get initial ConfigMap content
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), ns.name());
    let initial_cm = wait_for_resource(&cm_api, "test-cfg-patroni-config", FAST_TIMEOUT)
        .await
        .expect("get configmap");
    let initial_content = initial_cm
        .data
        .as_ref()
        .and_then(|d| d.get("patroni.yml"))
        .cloned()
        .unwrap_or_default();

    // Get current generation
    let cluster = api.get("test-cfg").await.expect("get");
    let pre_patch_gen = cluster.metadata.generation;

    // Update PostgreSQL params
    let patch = serde_json::json!({
        "spec": {
            "postgresqlParams": {
                "max_connections": "500",
                "shared_buffers": "512MB"
            }
        }
    });
    api.patch(
        "test-cfg",
        &PatchParams::apply("test"),
        &Patch::Merge(&patch),
    )
    .await
    .expect("patch");

    // Wait for new generation to be observed
    let new_gen_observed = |obj: Option<&PostgresCluster>| {
        obj.and_then(|c| c.status.as_ref())
            .and_then(|s| s.observed_generation)
            .map(|g| Some(g) > pre_patch_gen)
            .unwrap_or(false)
    };
    wait_for(&api, "test-cfg", new_gen_observed, FAST_TIMEOUT)
        .await
        .expect("new generation observed");

    // Get new ConfigMap content
    let updated_cm = cm_api
        .get("test-cfg-patroni-config")
        .await
        .expect("get configmap");
    let updated_content = updated_cm
        .data
        .as_ref()
        .and_then(|d| d.get("patroni.yml"))
        .cloned()
        .unwrap_or_default();

    // Verify config changed and contains new values
    assert_ne!(
        initial_content, updated_content,
        "ConfigMap content should change"
    );
    assert!(
        updated_content.contains("max_connections"),
        "Should contain max_connections param"
    );

    api.delete("test-cfg", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// DELETION TESTS
// =============================================================================

/// Test: Deletion removes finalizer and allows cleanup
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_deletion_removes_finalizer() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "del")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-del", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for finalizer to be added
    wait_for(&api, "test-del", has_finalizer(), FAST_TIMEOUT)
        .await
        .expect("finalizer should be added");

    // Delete the cluster
    api.delete("test-del", &DeleteParams::default())
        .await
        .expect("delete");

    // Wait for deletion to complete
    let deleted = tokio::time::timeout(FAST_TIMEOUT, async {
        loop {
            match api.get("test-del").await {
                Err(kube::Error::Api(e)) if e.code == 404 => return true,
                Ok(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
    })
    .await;

    assert!(
        deleted.is_ok(),
        "Cluster should be deleted (finalizer removed)"
    );

    ns.cleanup().await.ok();
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

/// Test: Cluster with invalid storage class still creates resources
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_invalid_storage_class_still_creates_resources() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "bad-sc")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-bad", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", Some("nonexistent-storage-class"))
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Operator should still process and create resources
    wait_for(&api, "test-bad", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("operator should process");

    // StatefulSet should exist (even if pods won't schedule)
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-bad", FAST_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Status should show Creating (not crashed)
    let cluster = api.get("test-bad").await.expect("get");
    let phase = cluster.status.as_ref().map(|s| &s.phase);
    assert!(
        phase == Some(&ClusterPhase::Creating) || phase == Some(&ClusterPhase::Pending),
        "Phase should be Creating or Pending, got {:?}",
        phase
    );

    api.delete("test-bad", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

// =============================================================================
// CONCURRENT OPERATION TESTS
// =============================================================================

/// Test: Multiple clusters can be created in the same namespace
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_multiple_clusters_same_namespace() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "multi")
        .await
        .expect("create ns");

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());

    // Create 3 clusters concurrently
    let pg1 = PostgresClusterBuilder::single("test-m1", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();
    let pg2 = PostgresClusterBuilder::single("test-m2", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();
    let pg3 = PostgresClusterBuilder::single("test-m3", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    // Create all clusters
    api.create(&PostParams::default(), &pg1)
        .await
        .expect("create pg1");
    api.create(&PostParams::default(), &pg2)
        .await
        .expect("create pg2");
    api.create(&PostParams::default(), &pg3)
        .await
        .expect("create pg3");

    // Wait for all to be processed
    wait_for(&api, "test-m1", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("pg1 processed");
    wait_for(&api, "test-m2", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("pg2 processed");
    wait_for(&api, "test-m3", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("pg3 processed");

    // Verify each has its own resources
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-m1", FAST_TIMEOUT)
        .await
        .expect("sts1");
    wait_for_resource(&sts_api, "test-m2", FAST_TIMEOUT)
        .await
        .expect("sts2");
    wait_for_resource(&sts_api, "test-m3", FAST_TIMEOUT)
        .await
        .expect("sts3");

    // Cleanup
    api.delete("test-m1", &DeleteParams::default()).await.ok();
    api.delete("test-m2", &DeleteParams::default()).await.ok();
    api.delete("test-m3", &DeleteParams::default()).await.ok();
    ns.cleanup().await.ok();
}

/// Test: Deletion during creation still results in clean removal
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_deletion_during_creation() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "del-create")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::single("test-dc", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait just a moment for operator to start processing
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Delete immediately
    api.delete("test-dc", &DeleteParams::default())
        .await
        .expect("delete");

    // Wait for deletion to complete
    let deleted = tokio::time::timeout(FAST_TIMEOUT, async {
        loop {
            match api.get("test-dc").await {
                Err(kube::Error::Api(e)) if e.code == 404 => return true,
                Ok(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }
    })
    .await;

    assert!(deleted.is_ok(), "Cluster should be deleted cleanly");

    // Verify no orphaned resources
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    // Give GC time to clean up
    tokio::time::sleep(Duration::from_secs(2)).await;
    let sts_result = sts_api.get("test-dc").await;
    assert!(
        sts_result.is_err(),
        "StatefulSet should be deleted (orphan cleanup)"
    );

    ns.cleanup().await.ok();
}

/// Test: Rapid spec updates result in final correct state
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_rapid_spec_updates() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "rapid")
        .await
        .expect("create ns");

    let pg = PostgresClusterBuilder::new("test-rapid", ns.name())
        .with_replicas(1)
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create");

    // Wait for initial processing
    wait_for(&api, "test-rapid", has_observed_generation(), FAST_TIMEOUT)
        .await
        .expect("initial processing");

    // Apply rapid updates (1 -> 2 -> 3 -> 4 -> 5)
    for replicas in 2..=5 {
        let patch = serde_json::json!({
            "spec": { "replicas": replicas }
        });
        api.patch(
            "test-rapid",
            &PatchParams::apply("test"),
            &Patch::Merge(&patch),
        )
        .await
        .expect("patch");
        // Small delay but not waiting for processing
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Get final generation
    let cluster = api.get("test-rapid").await.expect("get");
    let final_gen = cluster.metadata.generation;

    // Wait for final generation to be observed
    let final_observed = |obj: Option<&PostgresCluster>| {
        obj.and_then(|c| c.status.as_ref())
            .and_then(|s| s.observed_generation)
            .map(|g| Some(g) == final_gen)
            .unwrap_or(false)
    };
    wait_for(&api, "test-rapid", final_observed, FAST_TIMEOUT)
        .await
        .expect("final generation observed");

    // Verify final state
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    let sts = sts_api.get("test-rapid").await.expect("get sts");
    assert_eq!(
        sts.spec.as_ref().unwrap().replicas,
        Some(5),
        "Final replica count should be 5"
    );

    api.delete("test-rapid", &DeleteParams::default())
        .await
        .ok();
    ns.cleanup().await.ok();
}

/// Test: Concurrent cluster deletion completes cleanly
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_concurrent_cluster_deletion() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "conc-del")
        .await
        .expect("create ns");

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());

    // Create 3 clusters
    for i in 1..=3 {
        let pg = PostgresClusterBuilder::single(&format!("test-cd{}", i), ns.name())
            .with_version(PostgresVersion::V16)
            .with_storage("1Gi", None)
            .build();
        api.create(&PostParams::default(), &pg)
            .await
            .expect("create");
    }

    // Wait for all to be processed
    for i in 1..=3 {
        wait_for(
            &api,
            &format!("test-cd{}", i),
            has_observed_generation(),
            FAST_TIMEOUT,
        )
        .await
        .expect("processed");
    }

    // Delete all concurrently
    for i in 1..=3 {
        api.delete(&format!("test-cd{}", i), &DeleteParams::default())
            .await
            .ok();
    }

    // Wait for all deletions to complete
    for i in 1..=3 {
        let name = format!("test-cd{}", i);
        let deleted = tokio::time::timeout(FAST_TIMEOUT, async {
            loop {
                match api.get(&name).await {
                    Err(kube::Error::Api(e)) if e.code == 404 => return true,
                    Ok(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                    Err(e) => panic!("Unexpected error for {}: {:?}", name, e),
                }
            }
        })
        .await;
        assert!(deleted.is_ok(), "Cluster {} should be deleted", name);
    }

    ns.cleanup().await.ok();
}
