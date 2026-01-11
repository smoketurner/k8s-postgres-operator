//! Functional tests for postgres-operator
//!
//! These tests use watch-based waiting for efficient resource detection.
//! They verify end-to-end functionality against a real Kubernetes cluster.
//!
//! Run with: cargo test --test integration -- --ignored --test-threads=1

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use kube::Api;
use kube::api::{DeleteParams, PostParams};
use postgres_operator::crd::{PostgresCluster, PostgresVersion};

use crate::{
    PostgresClusterBuilder, SHORT_TIMEOUT, ScopedOperator, SharedTestCluster, TestNamespace,
    ensure_crd_installed, ensure_operator_running, wait_for_resource, wait_for_resource_deletion,
};

/// Test context with operator and cluster client
struct TestContext {
    client: kube::Client,
    _operator: ScopedOperator,
    _cluster: std::sync::Arc<SharedTestCluster>,
}

/// Setup test context - starts operator and returns client
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

    // Start the operator for this test
    let operator = ensure_operator_running(client.clone()).await;

    TestContext {
        client,
        _operator: operator,
        _cluster: cluster,
    }
}

// =============================================================================
// Core Cluster Tests
// =============================================================================

/// Test: PostgresCluster resource is created and accepted
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_resource_created() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-create")
        .await
        .expect("create ns");

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

    // Explicit cleanup to ensure it completes before test exits
}

/// Test: PostgresCluster creates StatefulSet
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_creates_statefulset() {
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-sts")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-svc")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-cm")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-secret")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-del")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-np")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-np-op")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-np-ext")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-np-dns")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-np-own")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-np-gc")
        .await
        .expect("create ns");

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
    let ctx = setup().await;
    let client = ctx.client.clone();
    let ns = TestNamespace::create(client.clone(), "func-labels")
        .await
        .expect("create ns");

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
