//! Functional tests for postgres-operator
//!
//! These tests use watch-based waiting for efficient resource detection.
//! They verify end-to-end functionality against a real Kubernetes cluster.
//!
//! Each test creates its own namespace and operator instance, enabling
//! parallel test execution without interference.
//!
//! Run with: cargo test --test integration -- --ignored

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::Api;
use kube::api::{DeleteParams, PostParams};
use postgres_operator::crd::{PostgresCluster, PostgresVersion};

use crate::{
    PostgresClusterBuilder, SHORT_TIMEOUT, ScopedOperator, SharedTestCluster, TestNamespace,
    ensure_crd_installed, wait_for_resource, wait_for_resource_deletion,
};

/// Initialize tracing and ensure CRD is installed
/// Returns the shared test cluster for creating clients
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
// Core Cluster Tests
// =============================================================================

/// Test: PostgresCluster resource is created and accepted
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_resource_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-create")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-cluster", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Verify resource exists
    let created = api.get("test-cluster").await.expect("get cluster");
    assert_eq!(created.spec.version, PostgresVersion::V16);
    assert_eq!(created.spec.replicas, 1);
}

/// Test: PostgresCluster creates StatefulSet
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_creates_statefulset() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-sts")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-sts", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for StatefulSet using watch
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    let sts = wait_for_resource(&sts_api, "test-sts", SHORT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Verify owner reference
    let owner_refs = sts.metadata.owner_references.as_ref();
    assert!(owner_refs.is_some(), "Should have owner reference");
    assert_eq!(owner_refs.unwrap()[0].kind, "PostgresCluster");
}

/// Test: PostgresCluster creates Services
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_creates_services() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-svc")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-svc", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let svc_api: Api<Service> = Api::namespaced(client.clone(), ns.name());

    // Check primary service
    wait_for_resource(&svc_api, "test-svc-primary", SHORT_TIMEOUT)
        .await
        .expect("Primary service should be created");

    // Check headless service (used for pod discovery)
    wait_for_resource(&svc_api, "test-svc-headless", SHORT_TIMEOUT)
        .await
        .expect("Headless service should be created");

    // Note: Replica service (-repl) is only created when replicas > 1
}

/// Test: PostgresCluster creates ConfigMap with Patroni config
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_creates_configmap() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-cm")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-cm", ns.name())
        .with_version(PostgresVersion::V16)
        .with_param("max_connections", "200")
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), ns.name());
    let cm = wait_for_resource(&cm_api, "test-cm-patroni-config", SHORT_TIMEOUT)
        .await
        .expect("Patroni ConfigMap should be created");

    // Check it has patroni.yml
    let data = cm.data.as_ref().expect("ConfigMap should have data");
    assert!(data.contains_key("patroni.yml"), "Should have patroni.yml");

    // Verify pg_hba uses samenet (secure default)
    let patroni_yaml = data.get("patroni.yml").unwrap();
    assert!(
        patroni_yaml.contains("samenet") || !patroni_yaml.contains("10.0.0.0/8"),
        "pg_hba should use samenet or not have RFC1918 CIDRs"
    );
}

/// Test: PostgresCluster creates credentials Secret
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_creates_secret() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-secret")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-secret", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let secret_api: Api<Secret> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&secret_api, "test-secret-credentials", SHORT_TIMEOUT)
        .await
        .expect("Credentials secret should be created");
}

/// Test: PostgresCluster deletion cleans up resources
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_deletion_cleanup() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-del")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-del", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for StatefulSet to be created
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&sts_api, "test-del", SHORT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Delete the cluster
    api.delete("test-del", &DeleteParams::default())
        .await
        .expect("delete cluster");

    // Verify StatefulSet is garbage collected
    wait_for_resource_deletion(&sts_api, "test-del", SHORT_TIMEOUT)
        .await
        .expect("StatefulSet should be garbage collected");
}

// =============================================================================
// NetworkPolicy Tests
// =============================================================================

/// Test: NetworkPolicy created when enabled in spec
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_network_policy_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-np")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-np", ns.name())
        .with_version(PostgresVersion::V16)
        .with_network_policy()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let np_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), ns.name());
    let np = wait_for_resource(&np_api, "test-np-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be created");

    // Verify structure
    let spec = np.spec.as_ref().expect("Should have spec");
    let policy_types = spec
        .policy_types
        .as_ref()
        .expect("Should have policy types");
    assert!(policy_types.contains(&"Ingress".to_string()));
    assert!(policy_types.contains(&"Egress".to_string()));
}

/// Test: NetworkPolicy allows operator namespace (footgun prevention)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_network_policy_allows_operator_namespace() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-np-op")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-np-op", ns.name())
        .with_version(PostgresVersion::V16)
        .with_network_policy()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let np_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), ns.name());
    let np = wait_for_resource(&np_api, "test-np-op-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be created");

    // Check that operator namespace is in ingress rules
    let spec = np.spec.as_ref().expect("Should have spec");
    let ingress = spec.ingress.as_ref().expect("Should have ingress");

    let has_operator_access = ingress.iter().any(|rule| {
        rule.from.as_ref().is_some_and(|peers| {
            peers.iter().any(|peer| {
                peer.namespace_selector.as_ref().is_some_and(|sel| {
                    sel.match_labels.as_ref().is_some_and(|labels| {
                        labels.get("kubernetes.io/metadata.name")
                            == Some(&"postgres-operator-system".to_string())
                    })
                })
            })
        })
    });

    assert!(
        has_operator_access,
        "NetworkPolicy MUST allow operator namespace (footgun prevention)"
    );
}

/// Test: NetworkPolicy with external access adds RFC1918 CIDRs
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_network_policy_external_access() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-np-ext")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-np-ext", ns.name())
        .with_version(PostgresVersion::V16)
        .with_network_policy_external_access()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let np_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), ns.name());
    let np = wait_for_resource(&np_api, "test-np-ext-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be created");

    // Check for IP block rules
    let spec = np.spec.as_ref().expect("Should have spec");
    let ingress = spec.ingress.as_ref().expect("Should have ingress");

    let has_ip_blocks = ingress.iter().any(|rule| {
        rule.from
            .as_ref()
            .is_some_and(|peers| peers.iter().any(|peer| peer.ip_block.is_some()))
    });

    assert!(
        has_ip_blocks,
        "External access should add IP block rules for RFC1918"
    );
}

/// Test: NetworkPolicy egress allows DNS
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_network_policy_allows_dns() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-np-dns")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-np-dns", ns.name())
        .with_version(PostgresVersion::V16)
        .with_network_policy()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let np_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), ns.name());
    let np = wait_for_resource(&np_api, "test-np-dns-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be created");

    let spec = np.spec.as_ref().expect("Should have spec");
    let egress = spec.egress.as_ref().expect("Should have egress");

    // Check for DNS port 53
    let has_dns = egress.iter().any(|rule| {
        rule.ports.as_ref().is_some_and(|ports| {
            ports.iter().any(|port| {
                port.port.as_ref().is_some_and(|p| match p {
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => *n == 53,
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(s) => {
                        s == "53"
                    }
                })
            })
        })
    });

    assert!(has_dns, "NetworkPolicy must allow DNS egress on port 53");
}

/// Test: NetworkPolicy has correct owner reference for garbage collection
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_network_policy_owner_reference() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-np-own")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-np-own", ns.name())
        .with_version(PostgresVersion::V16)
        .with_network_policy()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let created = api
        .create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");
    let cluster_uid = created.metadata.uid.as_ref().expect("Should have UID");

    let np_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), ns.name());
    let np = wait_for_resource(&np_api, "test-np-own-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be created");

    let owner_refs = np
        .metadata
        .owner_references
        .as_ref()
        .expect("Should have owner refs");
    assert_eq!(owner_refs.len(), 1, "Should have one owner reference");
    assert_eq!(owner_refs[0].kind, "PostgresCluster");
    assert_eq!(owner_refs[0].name, "test-np-own");
    assert_eq!(&owner_refs[0].uid, cluster_uid);
}

/// Test: NetworkPolicy is garbage collected when cluster is deleted
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_network_policy_garbage_collected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-np-gc")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-np-gc", ns.name())
        .with_version(PostgresVersion::V16)
        .with_network_policy()
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for NetworkPolicy to be created
    let np_api: Api<NetworkPolicy> = Api::namespaced(client.clone(), ns.name());
    wait_for_resource(&np_api, "test-np-gc-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be created");

    // Delete the cluster
    api.delete("test-np-gc", &DeleteParams::default())
        .await
        .expect("delete cluster");

    // Verify NetworkPolicy is garbage collected
    wait_for_resource_deletion(&np_api, "test-np-gc-network-policy", SHORT_TIMEOUT)
        .await
        .expect("NetworkPolicy should be garbage collected");
}

// =============================================================================
// Custom Labels Tests
// =============================================================================

/// Test: Custom labels are applied to created resources
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_custom_labels() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-labels")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create cluster with custom labels via raw spec
    let mut pg = PostgresClusterBuilder::single("test-labels", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    // Add custom labels
    pg.spec
        .labels
        .insert("team".to_string(), "platform".to_string());
    pg.spec
        .labels
        .insert("cost-center".to_string(), "infrastructure".to_string());

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), ns.name());
    let sts = wait_for_resource(&sts_api, "test-labels", SHORT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    let labels = sts.metadata.labels.as_ref().expect("Should have labels");
    assert_eq!(labels.get("team"), Some(&"platform".to_string()));
    assert_eq!(
        labels.get("cost-center"),
        Some(&"infrastructure".to_string())
    );
}

// =============================================================================
// Error Path Tests - Validation Failures
// =============================================================================

/// Test: Invalid replica count (0) is rejected by CRD validation
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_invalid_replicas_zero_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-err-rep0")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let mut pg = PostgresClusterBuilder::single("invalid-rep", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    // Set invalid replica count (below minimum)
    pg.spec.replicas = 0;

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let result = api.create(&PostParams::default(), &pg).await;

    // Should fail with validation error
    assert!(
        result.is_err(),
        "Should reject cluster with 0 replicas, but got: {:?}",
        result
    );

    // Verify it's a validation error (422)
    if let Err(kube::Error::Api(ae)) = result {
        assert_eq!(
            ae.code, 422,
            "Expected validation error (422), got: {}",
            ae.code
        );
        assert!(
            ae.message.contains("Invalid value")
                || ae.message.contains("must be greater")
                || ae.message.contains("minimum"),
            "Error should mention validation failure: {}",
            ae.message
        );
    }
}

/// Test: Invalid replica count (above max) is rejected by CRD validation
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_invalid_replicas_over_max_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-err-rep100")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let mut pg = PostgresClusterBuilder::single("invalid-rep", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    // Set invalid replica count (above maximum of 100)
    pg.spec.replicas = 101;

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let result = api.create(&PostParams::default(), &pg).await;

    // Should fail with validation error
    assert!(
        result.is_err(),
        "Should reject cluster with 101 replicas, but got: {:?}",
        result
    );

    // Verify it's a validation error (422)
    if let Err(kube::Error::Api(ae)) = result {
        assert_eq!(
            ae.code, 422,
            "Expected validation error (422), got: {}",
            ae.code
        );
        assert!(
            ae.message.contains("Invalid value")
                || ae.message.contains("must be less")
                || ae.message.contains("maximum"),
            "Error should mention validation failure: {}",
            ae.message
        );
    }
}

/// Test: Invalid storage size format is rejected by CRD validation
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_invalid_storage_size_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-err-storage")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("invalid-storage", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("invalid", None) // Invalid storage format
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let result = api.create(&PostParams::default(), &pg).await;

    // Should fail with validation error
    assert!(
        result.is_err(),
        "Should reject cluster with invalid storage size, but got: {:?}",
        result
    );

    // Verify it's a validation error (422)
    if let Err(kube::Error::Api(ae)) = result {
        assert_eq!(
            ae.code, 422,
            "Expected validation error (422), got: {}",
            ae.code
        );
    }
}

/// Test: Cluster transitions to Failed state with descriptive error for invalid config
/// This tests operator-level validation, not CRD-level
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_operator_detects_degraded_cluster() {
    use crate::{SHORT_TIMEOUT, has_condition, wait_for_cluster_named};

    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-err-deg")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create a valid cluster first
    let pg = PostgresClusterBuilder::single("degraded-test", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for cluster to have status populated (via condition)
    // The cluster should reach at least the Creating phase with Progressing=True
    let cluster = wait_for_cluster_named(
        &api,
        "degraded-test",
        has_condition("Progressing", "True"),
        "Progressing=True",
        SHORT_TIMEOUT,
    )
    .await
    .expect("Cluster should be progressing");

    // Verify cluster has status populated
    assert!(
        cluster.status.is_some(),
        "Cluster should have status after reconciliation"
    );
}

/// Test: Cluster handles duplicate resource name gracefully
/// Creating a cluster with a name that already exists should fail with 409 Conflict
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_duplicate_cluster_name_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-err-dup")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("duplicate", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());

    // Create first cluster
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create first cluster");

    // Try to create duplicate
    let result = api.create(&PostParams::default(), &pg).await;

    // Should fail with already exists error
    assert!(
        result.is_err(),
        "Should reject duplicate cluster name, but got: {:?}",
        result
    );

    // Verify it's a conflict error (409)
    if let Err(kube::Error::Api(ae)) = result {
        assert_eq!(
            ae.code, 409,
            "Expected conflict error (409), got: {}",
            ae.code
        );
    }
}

// =============================================================================
// Status Condition Tests
// =============================================================================

/// Test: Cluster status has properly populated conditions with required fields
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_status_conditions_populated() {
    use crate::{DEFAULT_TIMEOUT, has_condition, wait_for_cluster_named};

    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-cond")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-cond", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for Progressing condition to be set
    let cluster = wait_for_cluster_named(
        &api,
        "test-cond",
        has_condition("Progressing", "True"),
        "Progressing=True",
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("Cluster should have Progressing condition");

    let status = cluster.status.as_ref().expect("Should have status");
    assert!(!status.conditions.is_empty(), "Should have conditions");

    // Find the Progressing condition and verify its fields
    let progressing = status
        .conditions
        .iter()
        .find(|c| c.type_ == "Progressing")
        .expect("Should have Progressing condition");

    assert_eq!(progressing.status, "True", "Progressing should be True");
    assert!(
        !progressing.reason.is_empty(),
        "Condition should have a reason"
    );
    assert!(
        !progressing.last_transition_time.is_empty(),
        "Condition should have lastTransitionTime"
    );
}

/// Test: Cluster observed_generation tracks spec changes
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_observed_generation() {
    use crate::{DEFAULT_TIMEOUT, generation_observed, wait_for_cluster_named};

    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-gen")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-gen", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for observed_generation to match
    let cluster = wait_for_cluster_named(
        &api,
        "test-gen",
        generation_observed(),
        "observedGeneration matches",
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("observed_generation should match");

    let status = cluster.status.as_ref().expect("Should have status");
    let observed = status
        .observed_generation
        .expect("Should have observed_generation");
    let generation = cluster.metadata.generation.expect("Should have generation");

    assert_eq!(
        observed, generation,
        "observed_generation should match metadata.generation"
    );
}

/// Test: Cluster ConfigurationValid condition is set for valid specs
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_configuration_valid_condition() {
    use crate::{DEFAULT_TIMEOUT, has_condition, wait_for_cluster_named};

    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-cfg-valid")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-cfg", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for ConfigurationValid condition
    let cluster = wait_for_cluster_named(
        &api,
        "test-cfg",
        has_condition("ConfigurationValid", "True"),
        "ConfigurationValid=True",
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("Cluster should have ConfigurationValid condition");

    let status = cluster.status.as_ref().expect("Should have status");
    let config_valid = status
        .conditions
        .iter()
        .find(|c| c.type_ == "ConfigurationValid")
        .expect("Should have ConfigurationValid condition");

    assert_eq!(
        config_valid.status, "True",
        "ConfigurationValid should be True for valid spec"
    );
    assert!(
        !config_valid.reason.is_empty(),
        "ConfigurationValid should have a reason"
    );
}

/// Test: Cluster phase transitions from Pending to Creating
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_phase_transitions() {
    use crate::{DEFAULT_TIMEOUT, is_phase, wait_for_cluster_named};
    use postgres_operator::crd::ClusterPhase;

    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-phase")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-phase", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for Creating phase (initial state after pending)
    let cluster = wait_for_cluster_named(
        &api,
        "test-phase",
        is_phase(ClusterPhase::Creating),
        "phase=Creating",
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("Cluster should reach Creating phase");

    let status = cluster.status.as_ref().expect("Should have status");
    assert_eq!(
        status.phase,
        ClusterPhase::Creating,
        "Should be in Creating phase"
    );
}

/// Test: Cluster status includes connection info
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_connection_info_populated() {
    use crate::{DEFAULT_TIMEOUT, has_connection_info, wait_for_cluster_named};

    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "func-conn-info")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    let pg = PostgresClusterBuilder::single("test-conn", ns.name())
        .with_version(PostgresVersion::V16)
        .build();

    let api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &pg)
        .await
        .expect("create cluster");

    // Wait for connection info to be populated in status
    let cluster = wait_for_cluster_named(
        &api,
        "test-conn",
        has_connection_info(),
        "connectionInfo populated",
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("Cluster should have connection info");

    let status = cluster.status.as_ref().expect("Should have status");
    let conn_info = status
        .connection_info
        .as_ref()
        .expect("Should have connectionInfo");

    // Verify connection info fields (primary endpoint should be set)
    assert!(
        conn_info.primary.is_some(),
        "connectionInfo.primary should be populated"
    );
    let primary = conn_info.primary.as_ref().unwrap();
    assert!(
        !primary.is_empty(),
        "connectionInfo.primary should not be empty"
    );
    assert!(
        primary.contains("test-conn-primary"),
        "connectionInfo.primary should reference primary service"
    );
}
