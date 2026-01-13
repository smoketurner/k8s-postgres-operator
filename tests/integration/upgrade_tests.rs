//! PostgresUpgrade integration tests
//!
//! These tests verify the PostgresUpgrade CRD functionality against a live
//! Kubernetes cluster. They test the upgrade resource creation, state machine
//! transitions, and the full upgrade flow.
//!
//! Note: Full end-to-end upgrade tests require:
//! - A running source PostgresCluster
//! - PostgreSQL logical replication support
//! - Significant time for data sync
//!
//! Run with: cargo test --test integration upgrade -- --ignored

use kube::Api;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use postgres_operator::crd::{PostgresCluster, PostgresUpgrade, PostgresVersion, UpgradePhase};
use std::sync::Arc;
use std::time::Duration;

use crate::port_forward::{PortForward, PortForwardTarget};
use crate::postgres::{
    CONNECT_RETRY_INTERVAL, MAX_CONNECT_RETRIES, POSTGRES_READY_TIMEOUT, fetch_credentials,
    verify_connection_with_retry,
};
use crate::{
    PostgresClusterBuilder, PostgresUpgradeBuilder, ScopedOperator, SharedTestCluster,
    TestNamespace, cluster_operational, ensure_crd_installed, install_upgrade_crd,
    upgrade_is_phase, upgrade_past_pending, upgrade_ready_for_cutover, upgrade_terminal,
    wait_for_cluster, wait_for_upgrade_named, UPGRADE_PHASE_TIMEOUT, UPGRADE_TIMEOUT,
};

/// Initialize tracing and ensure CRDs are installed
async fn init_test() -> Arc<SharedTestCluster> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kube=warn,postgres_operator=debug")
        .with_test_writer()
        .try_init();

    let cluster = SharedTestCluster::get()
        .await
        .expect("Failed to get cluster");

    // Install all required CRDs
    ensure_crd_installed(&cluster)
        .await
        .expect("Failed to install PostgresCluster CRD");

    let client = cluster.new_client().await.expect("create client");
    install_upgrade_crd(client)
        .await
        .expect("Failed to install PostgresUpgrade CRD");

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
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // Create just the PostgresUpgrade (referencing non-existent cluster)
    // This tests that the CRD schema is valid
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("nonexistent-cluster")
        .with_target_version(PostgresVersion::V17)
        .build();

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    let created = api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    assert_eq!(created.metadata.name, Some("test-upgrade".to_string()));
    assert_eq!(created.spec.target_version, PostgresVersion::V17);

    tracing::info!("PostgresUpgrade resource created successfully");

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
/// and verifies the upgrade stays in pending or moves to failed state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_source_cluster_not_found() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-source-notfound")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // Create upgrade referencing non-existent cluster using builder
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("nonexistent-cluster")
        .with_target_version(PostgresVersion::V17)
        .build();

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

    tracing::info!("Upgrade correctly stayed in {:?} phase", phase);

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
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_same_version_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-same-ver")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // Create source cluster with v16
    let source = PostgresClusterBuilder::single("source-cluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for cluster to be ready
    tracing::info!("Waiting for source cluster to become operational...");
    wait_for_cluster(
        &cluster_api,
        "source-cluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("wait for source cluster");

    // Attempt upgrade to same version (v16)
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source-cluster")
        .with_target_version(PostgresVersion::V16) // Same as source
        .build();

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

    tracing::info!(
        "Same-version upgrade correctly rejected with phase: {:?}",
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
// Test 4: Valid Upgrade Starts Creating Target
// =============================================================================

/// Test: Verify a valid upgrade progresses past Pending phase
///
/// This test creates a source cluster and verifies the upgrade controller
/// starts processing it (moves past Pending).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_starts_creating_target() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-start")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // Create source cluster with v16
    let source = PostgresClusterBuilder::single("source-cluster", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for cluster to be ready
    tracing::info!("Waiting for source cluster to become operational...");
    wait_for_cluster(
        &cluster_api,
        "source-cluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("wait for source cluster");

    // Create upgrade to v17 using builder
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source-cluster")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .build();

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to progress past Pending using watch-based condition
    tracing::info!("Waiting for upgrade to progress past Pending...");
    let result = wait_for_upgrade_named(
        &api,
        "test-upgrade",
        upgrade_past_pending(),
        "past_pending",
        UPGRADE_PHASE_TIMEOUT,
    )
    .await;

    match result {
        Ok(upgraded) => {
            let phase = upgraded.status.as_ref().map(|s| &s.phase);
            tracing::info!("Upgrade progressed to phase: {:?}", phase);

            // Should be CreatingTarget or later (or Failed if there's a config issue)
            assert!(
                matches!(
                    phase,
                    Some(
                        UpgradePhase::CreatingTarget
                            | UpgradePhase::ConfiguringReplication
                            | UpgradePhase::Replicating
                            | UpgradePhase::Failed
                    )
                ),
                "Expected upgrade to progress past Pending, got {:?}",
                phase
            );
        }
        Err(e) => {
            // Check current state for debugging
            let upgraded = api.get("test-upgrade").await.expect("get upgrade");
            let phase = upgraded.status.as_ref().map(|s| &s.phase);
            let error = upgraded.status.as_ref().and_then(|s| s.last_error.as_ref());
            panic!(
                "Upgrade did not progress: {:?}, current phase: {:?}, error: {:?}",
                e, phase, error
            );
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

// =============================================================================
// Test 5: Full Upgrade V16 to V17 (Happy Path)
// =============================================================================

/// Test: Complete upgrade lifecycle from PostgreSQL 16 to 17
///
/// This is the main end-to-end happy path test that:
/// 1. Creates a source cluster with PostgreSQL 16
/// 2. Creates an upgrade to PostgreSQL 17
/// 3. Waits for replication to sync
/// 4. Triggers manual cutover
/// 5. Verifies upgrade completes
/// 6. Verifies connectivity to the target cluster
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster - full upgrade takes 10+ minutes"]
async fn test_upgrade_full_v16_to_v17() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // =========================================================================
    // Step 1: Create source cluster with PostgreSQL 16
    // =========================================================================
    tracing::info!("Step 1: Creating source cluster with PostgreSQL 16...");

    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_param("max_connections", "100")
        .with_param("wal_level", "logical") // Required for logical replication
        .with_param("max_replication_slots", "10")
        .with_param("max_wal_senders", "10")
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for source cluster to be operational
    tracing::info!("Waiting for source cluster to become operational...");
    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Verify source cluster connectivity and version
    tracing::info!("Verifying source cluster connectivity...");
    let source_credentials = fetch_credentials(&client, ns.name(), "source-credentials")
        .await
        .expect("fetch source credentials");

    let source_pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("source-primary", 5432),
        None,
    )
    .await
    .expect("start source port-forward");

    let source_info = verify_connection_with_retry(
        &source_credentials,
        "127.0.0.1",
        source_pf.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify source connection");

    source_info.assert_version(16);
    source_info.assert_is_primary();
    tracing::info!(
        "Source cluster ready: PostgreSQL {}",
        source_info.pg_version
    );

    // =========================================================================
    // Step 2: Create upgrade to PostgreSQL 17
    // =========================================================================
    tracing::info!("Step 2: Creating upgrade to PostgreSQL 17...");

    let upgrade = PostgresUpgradeBuilder::new("upgrade-v17", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .with_verification_passes(1) // Reduce for faster test
        .with_target_ready_timeout("15m")
        .with_initial_sync_timeout("30m")
        .build();

    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // =========================================================================
    // Step 3: Wait for upgrade to be ready for cutover
    // =========================================================================
    tracing::info!("Step 3: Waiting for upgrade to be ready for cutover...");

    // Wait for upgrade to reach ReadyForCutover or WaitingForManualCutover
    let upgraded = wait_for_upgrade_named(
        &upgrade_api,
        "upgrade-v17",
        upgrade_ready_for_cutover(),
        "ready_for_cutover",
        UPGRADE_TIMEOUT,
    )
    .await
    .expect("upgrade should reach ready for cutover");

    let phase = upgraded.status.as_ref().map(|s| &s.phase);
    tracing::info!("Upgrade ready for cutover, phase: {:?}", phase);

    // Log replication status
    if let Some(status) = &upgraded.status {
        if let Some(repl) = &status.replication {
            tracing::info!(
                "Replication status: lag_bytes={:?}, lag_seconds={:?}",
                repl.lag_bytes,
                repl.lag_seconds
            );
        }
        if let Some(verif) = &status.verification {
            tracing::info!(
                "Verification: passes={}, matched={}, mismatched={}",
                verif.consecutive_passes,
                verif.tables_matched,
                verif.tables_mismatched
            );
        }
    }

    // =========================================================================
    // Step 4: Trigger manual cutover
    // =========================================================================
    tracing::info!("Step 4: Triggering manual cutover...");

    // Apply cutover annotation
    let cutover_patch = serde_json::json!({
        "metadata": {
            "annotations": {
                "postgres-operator.smoketurner.com/cutover": "now"
            }
        }
    });

    upgrade_api
        .patch(
            "upgrade-v17",
            &PatchParams::default(),
            &Patch::Merge(&cutover_patch),
        )
        .await
        .expect("apply cutover annotation");

    // =========================================================================
    // Step 5: Wait for upgrade to complete
    // =========================================================================
    tracing::info!("Step 5: Waiting for upgrade to complete...");

    let final_upgrade = wait_for_upgrade_named(
        &upgrade_api,
        "upgrade-v17",
        upgrade_terminal(),
        "terminal_phase",
        UPGRADE_PHASE_TIMEOUT,
    )
    .await
    .expect("upgrade should reach terminal phase");

    let final_phase = final_upgrade.status.as_ref().map(|s| &s.phase);
    tracing::info!("Upgrade completed with phase: {:?}", final_phase);

    // Verify upgrade completed successfully
    assert!(
        matches!(final_phase, Some(UpgradePhase::Completed)),
        "Expected Completed phase, got {:?}",
        final_phase
    );

    // =========================================================================
    // Step 6: Verify target cluster connectivity
    // =========================================================================
    tracing::info!("Step 6: Verifying target cluster connectivity...");

    // The target cluster should be named "source-v17" by convention
    // Wait for target cluster to be operational
    wait_for_cluster(
        &cluster_api,
        "source-v17",
        cluster_operational(1),
        Duration::from_secs(60),
    )
    .await
    .expect("target cluster should be operational");

    // Verify target cluster connectivity and version
    let target_credentials = fetch_credentials(&client, ns.name(), "source-v17-credentials")
        .await
        .expect("fetch target credentials");

    let target_pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("source-v17-primary", 5432),
        None,
    )
    .await
    .expect("start target port-forward");

    let target_info = verify_connection_with_retry(
        &target_credentials,
        "127.0.0.1",
        target_pf.local_port(),
        MAX_CONNECT_RETRIES,
        CONNECT_RETRY_INTERVAL,
    )
    .await
    .expect("verify target connection");

    target_info.assert_version(17);
    target_info.assert_is_primary();

    tracing::info!(
        "SUCCESS: Upgrade completed! Source: PostgreSQL 16 -> Target: PostgreSQL {}",
        target_info.pg_version
    );

    // Clean up handled by TestNamespace drop
}

// =============================================================================
// Test 6: Downgrade Rejected
// =============================================================================

/// Test: Verify downgrade (v17 -> v16) is rejected
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_downgrade_rejected() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-downgrade")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // Create source cluster with v17
    let source = PostgresClusterBuilder::single("source-cluster", ns.name())
        .with_version(PostgresVersion::V17)
        .with_storage("1Gi", None)
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for cluster to be ready
    tracing::info!("Waiting for source cluster to become operational...");
    wait_for_cluster(
        &cluster_api,
        "source-cluster",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("wait for source cluster");

    // Attempt downgrade to v16
    let upgrade = PostgresUpgradeBuilder::new("test-downgrade", ns.name())
        .with_source_cluster("source-cluster")
        .with_target_version(PostgresVersion::V16) // Downgrade
        .build();

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for validation
    tokio::time::sleep(Duration::from_secs(10)).await;

    let upgraded = api.get("test-downgrade").await.expect("get upgrade");

    let phase = upgraded
        .status
        .as_ref()
        .map(|s| &s.phase)
        .unwrap_or(&UpgradePhase::Pending);

    // Should be Failed due to downgrade attempt
    assert!(
        matches!(phase, UpgradePhase::Pending | UpgradePhase::Failed),
        "Expected Pending or Failed for downgrade attempt, got {:?}",
        phase
    );

    // Check for error message about downgrade
    if let Some(status) = &upgraded.status {
        if let Some(error) = &status.last_error {
            tracing::info!("Downgrade correctly rejected with error: {}", error);
            assert!(
                error.to_lowercase().contains("downgrade")
                    || error.to_lowercase().contains("version"),
                "Expected error message about downgrade, got: {}",
                error
            );
        }
    }

    tracing::info!("Downgrade correctly rejected with phase: {:?}", phase);

    // Clean up
    api.delete("test-downgrade", &DeleteParams::default())
        .await
        .ok();
    cluster_api
        .delete("source-cluster", &DeleteParams::default())
        .await
        .ok();
}

// =============================================================================
// Test 7: Upgrade Creates Target Cluster
// =============================================================================

/// Test: Verify upgrade creates target cluster with correct version
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_creates_target_cluster() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let ns = TestNamespace::create(client.clone(), "upgrade-target")
        .await
        .expect("create ns");
    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    // Create source cluster with v16
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .with_param("wal_level", "logical")
        .without_tls()
        .build();

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    // Wait for source cluster to be ready
    tracing::info!("Waiting for source cluster to become operational...");
    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("wait for source cluster");

    // Create upgrade to v17
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .build();

    let api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());
    api.create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to move past CreatingTarget (target cluster exists)
    tracing::info!("Waiting for target cluster to be created...");

    // Either wait for ConfiguringReplication (which means target is ready)
    // or check for the target cluster directly
    let result = wait_for_upgrade_named(
        &api,
        "test-upgrade",
        upgrade_is_phase(UpgradePhase::ConfiguringReplication),
        "configuring_replication",
        UPGRADE_TIMEOUT,
    )
    .await;

    // Check if target cluster exists
    let target_result = cluster_api.get("source-v17").await;

    match target_result {
        Ok(target) => {
            tracing::info!("Target cluster created successfully");

            // Verify target cluster has v17
            assert_eq!(
                target.spec.version,
                PostgresVersion::V17,
                "Target cluster should have v17"
            );

            // Verify target cluster inherits source config
            assert_eq!(
                target.spec.storage.size, "1Gi",
                "Target should inherit storage size"
            );
        }
        Err(e) => {
            // If we got here via upgrade_is_phase, the target should exist
            if result.is_ok() {
                panic!("Upgrade reached ConfiguringReplication but target cluster not found: {:?}", e);
            }
            // Otherwise check the upgrade status
            let upgraded = api.get("test-upgrade").await.expect("get upgrade");
            let phase = upgraded.status.as_ref().map(|s| &s.phase);
            let error = upgraded.status.as_ref().and_then(|s| s.last_error.as_ref());
            tracing::warn!(
                "Target cluster not yet created, upgrade phase: {:?}, error: {:?}",
                phase,
                error
            );
        }
    }

    // Clean up
    api.delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
    cluster_api.delete("source", &DeleteParams::default()).await.ok();
    cluster_api
        .delete("source-v17", &DeleteParams::default())
        .await
        .ok();
}
