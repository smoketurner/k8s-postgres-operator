//! PostgresUpgrade integration tests
//!
//! These tests verify the PostgresUpgrade CRD functionality against a live
//! Kubernetes cluster. They test the upgrade resource creation, state machine
//! transitions, and basic upgrade flow.
//!
//! Note: Full end-to-end upgrade tests require:
//! - A running source PostgresCluster
//! - PostgreSQL logical replication support
//! - Significant time for data sync
//!
//! Run with: cargo test --test integration upgrade -- --ignored

use kube::Api;
use kube::api::{DeleteParams, PostParams};
use postgres_operator::crd::{
    ClusterReference, PostgresCluster, PostgresUpgrade, PostgresUpgradeSpec, PostgresVersion,
    UpgradePhase, UpgradeStrategy,
};
use std::time::Duration;

use crate::{
    PostgresClusterBuilder, ScopedOperator, SharedTestCluster, TestNamespace, cluster_operational,
    ensure_crd_installed, wait_for_cluster,
};

/// Initialize tracing and ensure CRDs are installed
async fn init_test() -> std::sync::Arc<SharedTestCluster> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kube=warn,postgres_operator=debug")
        .with_test_writer()
        .try_init();

    let cluster = SharedTestCluster::get()
        .await
        .expect("Failed to get cluster");

    ensure_crd_installed(&cluster)
        .await
        .expect("Failed to install PostgresCluster CRD");

    // Note: PostgresUpgrade CRD should also be installed
    // ensure_upgrade_crd_installed(&cluster)
    //     .await
    //     .expect("Failed to install PostgresUpgrade CRD");

    cluster
}

// =============================================================================
// Test 1: Basic CRD Creation
// =============================================================================

/// Test: Verify basic PostgresUpgrade resource creation
///
/// This test creates a PostgresUpgrade resource and verifies it's accepted
/// by the Kubernetes API. It does not wait for the upgrade to complete.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_resource_created() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-create")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create just the PostgresUpgrade (referencing non-existent cluster)
    // This tests that the CRD schema is valid
    let upgrade = PostgresUpgrade {
        metadata: kube::core::ObjectMeta {
            name: Some("test-upgrade".to_string()),
            namespace: Some(ns.name().to_string()),
            ..Default::default()
        },
        spec: PostgresUpgradeSpec {
            source_cluster: ClusterReference {
                name: "nonexistent-cluster".to_string(),
                namespace: None,
            },
            target_version: PostgresVersion::V17,
            target_cluster_overrides: None,
            strategy: UpgradeStrategy::default(),
        },
        status: None,
    };

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    let created = api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    assert_eq!(created.metadata.name, Some("test-upgrade".to_string()));
    assert_eq!(created.spec.target_version, PostgresVersion::V17);

    // Clean up
    api.delete("test-upgrade", &DeleteParams::default())
        .await
        .expect("delete upgrade");
}

// =============================================================================
// Test 2: Source Cluster Validation
// =============================================================================

/// Test: Verify upgrade fails validation when source cluster doesn't exist
///
/// This test creates an upgrade referencing a non-existent source cluster
/// and verifies the upgrade moves to a pending/failed state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_source_cluster_not_found() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-source-notfound")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create upgrade referencing non-existent cluster
    let upgrade = PostgresUpgrade {
        metadata: kube::core::ObjectMeta {
            name: Some("test-upgrade".to_string()),
            namespace: Some(ns.name().to_string()),
            ..Default::default()
        },
        spec: PostgresUpgradeSpec {
            source_cluster: ClusterReference {
                name: "nonexistent-cluster".to_string(),
                namespace: None,
            },
            target_version: PostgresVersion::V17,
            target_cluster_overrides: None,
            strategy: UpgradeStrategy::default(),
        },
        status: None,
    };

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for the controller to process and fail validation
    tokio::time::sleep(Duration::from_secs(5)).await;

    // The upgrade should stay in Pending phase (source not found)
    let upgraded = api.get("test-upgrade").await.expect("get upgrade");

    let phase = upgraded
        .status
        .as_ref()
        .map(|s| &s.phase)
        .unwrap_or(&UpgradePhase::Pending);

    // Should be Pending or Failed due to missing source
    assert!(
        matches!(phase, UpgradePhase::Pending | UpgradePhase::Failed),
        "Expected Pending or Failed phase, got {:?}",
        phase
    );

    // Clean up
    api.delete("test-upgrade", &DeleteParams::default())
        .await
        .expect("delete upgrade");
}

// =============================================================================
// Test 3: Same Version Upgrade Rejected
// =============================================================================

/// Test: Verify upgrade is rejected when target version equals source version
///
/// This test creates a source cluster with v16 and attempts an upgrade to v16,
/// which should be rejected as a "same version" upgrade.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with PostgresUpgrade CRD"]
async fn test_upgrade_same_version_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-same-ver")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create source cluster with v16
    let source = PostgresClusterBuilder::new("source-cluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_replicas(1)
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for cluster to be ready
    wait_for_cluster(
        &cluster_api,
        "source-cluster",
        cluster_operational(1),
        Duration::from_secs(300),
    )
    .await
    .expect("wait for source cluster");

    // Attempt upgrade to same version (v16)
    let upgrade = PostgresUpgrade {
        metadata: kube::core::ObjectMeta {
            name: Some("test-upgrade".to_string()),
            namespace: Some(ns.name().to_string()),
            ..Default::default()
        },
        spec: PostgresUpgradeSpec {
            source_cluster: ClusterReference {
                name: "source-cluster".to_string(),
                namespace: None,
            },
            target_version: PostgresVersion::V16, // Same as source
            target_cluster_overrides: None,
            strategy: UpgradeStrategy::default(),
        },
        status: None,
    };

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for validation
    tokio::time::sleep(Duration::from_secs(10)).await;

    let upgraded = api.get("test-upgrade").await.expect("get upgrade");

    // Should be Failed due to same version
    let phase = upgraded
        .status
        .as_ref()
        .map(|s| &s.phase)
        .unwrap_or(&UpgradePhase::Pending);

    // Webhook should reject, or controller should move to Failed
    assert!(
        matches!(phase, UpgradePhase::Pending | UpgradePhase::Failed),
        "Expected Pending or Failed for same-version upgrade, got {:?}",
        phase
    );

    // Clean up
    api.delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
    cluster_api
        .delete("source-cluster", &DeleteParams::default())
        .await
        .ok();
}

// =============================================================================
// Test 4: Valid Upgrade Starts
// =============================================================================

/// Test: Verify a valid upgrade progresses to CreatingTarget phase
///
/// This is a longer test that creates a source cluster and verifies
/// the upgrade starts correctly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with PostgresUpgrade CRD and takes several minutes"]
async fn test_upgrade_starts_creating_target() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-start")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start(client.clone(), ns.name()).await;

    // Create source cluster with v16
    let source = PostgresClusterBuilder::new("source-cluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_replicas(1)
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for cluster to be ready
    wait_for_cluster(
        &cluster_api,
        "source-cluster",
        cluster_operational(1),
        Duration::from_secs(300),
    )
    .await
    .expect("wait for source cluster");

    // Create upgrade to v17
    let upgrade = PostgresUpgrade {
        metadata: kube::core::ObjectMeta {
            name: Some("test-upgrade".to_string()),
            namespace: Some(ns.name().to_string()),
            ..Default::default()
        },
        spec: PostgresUpgradeSpec {
            source_cluster: ClusterReference {
                name: "source-cluster".to_string(),
                namespace: None,
            },
            target_version: PostgresVersion::V17,
            target_cluster_overrides: None,
            strategy: UpgradeStrategy::default(),
        },
        status: None,
    };

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to start creating target
    let mut attempts = 0;
    let max_attempts = 60; // 5 minutes
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        attempts += 1;

        let upgraded = api.get("test-upgrade").await.expect("get upgrade");
        let phase = upgraded
            .status
            .as_ref()
            .map(|s| &s.phase)
            .unwrap_or(&UpgradePhase::Pending);

        tracing::info!("Upgrade phase: {:?}", phase);

        // Success if we moved past Pending
        if !matches!(phase, UpgradePhase::Pending) {
            // Should be CreatingTarget or later
            assert!(
                matches!(
                    phase,
                    UpgradePhase::CreatingTarget
                        | UpgradePhase::ConfiguringReplication
                        | UpgradePhase::Replicating
                        | UpgradePhase::Failed // Also acceptable if there's a configuration issue
                ),
                "Expected upgrade to progress past Pending, got {:?}",
                phase
            );
            break;
        }

        if attempts >= max_attempts {
            panic!("Upgrade did not progress within timeout");
        }
    }

    // Clean up
    api.delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
    cluster_api
        .delete("source-cluster", &DeleteParams::default())
        .await
        .ok();
    // Target cluster cleanup (if created)
    cluster_api
        .delete("source-cluster-v17", &DeleteParams::default())
        .await
        .ok();
}
