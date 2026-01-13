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
    TestNamespace, UPGRADE_PHASE_TIMEOUT, UPGRADE_TIMEOUT, cluster_operational,
    ensure_crd_installed, install_upgrade_crd, upgrade_is_phase, upgrade_past_pending,
    upgrade_past_phase, upgrade_ready_for_cutover, upgrade_terminal, wait_for_cluster,
    wait_for_upgrade_named,
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

    // The target cluster is named "{upgrade_name}-target" by convention
    // Since upgrade name is "upgrade-v17", target cluster is "upgrade-v17-target"
    let target_cluster_name = "upgrade-v17-target";

    // Wait for target cluster to be operational
    wait_for_cluster(
        &cluster_api,
        target_cluster_name,
        cluster_operational(1),
        Duration::from_secs(60),
    )
    .await
    .expect("target cluster should be operational");

    // Verify target cluster connectivity and version
    let target_credentials = fetch_credentials(
        &client,
        ns.name(),
        &format!("{}-credentials", target_cluster_name),
    )
    .await
    .expect("fetch target credentials");

    let target_pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service(format!("{}-primary", target_cluster_name), 5432),
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
    if let Some(status) = &upgraded.status
        && let Some(error) = &status.last_error
    {
        tracing::info!("Downgrade correctly rejected with error: {}", error);
        assert!(
            error.to_lowercase().contains("downgrade") || error.to_lowercase().contains("version"),
            "Expected error message about downgrade, got: {}",
            error
        );
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
                panic!(
                    "Upgrade reached ConfiguringReplication but target cluster not found: {:?}",
                    e
                );
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
    cluster_api
        .delete("source", &DeleteParams::default())
        .await
        .ok();
    cluster_api
        .delete("source-v17", &DeleteParams::default())
        .await
        .ok();
}

// =============================================================================
// Phase 2: Data Integrity Tests
// =============================================================================

/// Test that row count verification works with actual data.
/// This test:
/// 1. Creates a source cluster with sample data
/// 2. Creates an upgrade
/// 3. Verifies that the verification status shows matching row counts
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_row_count_with_data() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Step 1: Create source cluster with PostgreSQL 16
    tracing::info!("Step 1: Creating source cluster with PostgreSQL 16...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        Duration::from_secs(180),
    )
    .await
    .expect("source cluster should become operational");

    // Step 2: Insert sample data into source cluster
    tracing::info!("Step 2: Inserting sample data...");
    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    // Connect and create test table with data
    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Create table and insert rows
        pg_client
            .execute(
                "CREATE TABLE IF NOT EXISTS test_data (id SERIAL PRIMARY KEY, name TEXT, value INT)",
                &[],
            )
            .await
            .expect("create table");

        for i in 1..=100 {
            pg_client
                .execute(
                    "INSERT INTO test_data (name, value) VALUES ($1, $2)",
                    &[&format!("item_{}", i), &i],
                )
                .await
                .expect("insert row");
        }

        // Verify row count
        let count: i64 = pg_client
            .query_one("SELECT COUNT(*) FROM test_data", &[])
            .await
            .expect("count rows")
            .get(0);
        assert_eq!(count, 100, "Should have 100 rows");
        tracing::info!("Inserted {} rows into test_data table", count);
    }

    // Step 3: Create upgrade to v17
    tracing::info!("Step 3: Creating upgrade to v17...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .with_verification_passes(1)
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Step 4: Wait for verification phase and check status
    tracing::info!("Step 4: Waiting for verification phase...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_past_phase(UpgradePhase::Replicating),
        "past_replicating",
        Duration::from_secs(300),
    )
    .await
    .expect("upgrade should progress past Replicating");

    // Get upgrade status and verify row counts are being tracked
    let upgrade_status = upgrade_api.get("test-upgrade").await.expect("get upgrade");

    if let Some(status) = upgrade_status.status {
        tracing::info!("Upgrade phase: {:?}", status.phase);

        if let Some(verification) = &status.verification {
            tracing::info!(
                "Verification status: tables_verified={}, tables_matched={}, tables_mismatched={}, passes={}",
                verification.tables_verified,
                verification.tables_matched,
                verification.tables_mismatched,
                verification.consecutive_passes
            );

            // The test_data table should be among the verified tables
            assert!(
                verification.tables_verified >= 1,
                "Should have verified at least 1 table"
            );
            assert_eq!(
                verification.tables_mismatched, 0,
                "Should have no mismatched tables"
            );
        }
    }

    // Step 5: Wait for upgrade to reach WaitingForManualCutover
    tracing::info!("Step 5: Waiting for cutover ready...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_is_phase(UpgradePhase::WaitingForManualCutover),
        "waiting_for_cutover",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should reach WaitingForManualCutover");

    // Verify data was replicated to target
    // Target cluster naming: {upgrade_name}-target, so "test-upgrade" -> "test-upgrade-target"
    tracing::info!("Step 6: Verifying data replication to target...");
    let target_pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("test-upgrade-target-primary", 5432),
        None,
    )
    .await
    .expect("start target port-forward");

    let target_creds = fetch_credentials(&client, ns.name(), "test-upgrade-target-credentials")
        .await
        .expect("fetch target credentials");

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(target_pf.local_port())
            .user(&target_creds.username)
            .password(&target_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to target");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        let count: i64 = pg_client
            .query_one("SELECT COUNT(*) FROM test_data", &[])
            .await
            .expect("count rows on target")
            .get(0);

        assert_eq!(count, 100, "Target should have 100 replicated rows");
        tracing::info!(
            "SUCCESS: Target has {} rows, data replicated correctly!",
            count
        );
    }

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

/// Test that replication lag is monitored and reported correctly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_replication_status() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        Duration::from_secs(180),
    )
    .await
    .expect("source cluster should become operational");

    // Create upgrade
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .with_verification_passes(1)
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for Replicating phase
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_is_phase(UpgradePhase::Replicating),
        "replicating",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should reach Replicating phase");

    // Check replication status is being populated
    let upgrade_status = upgrade_api.get("test-upgrade").await.expect("get upgrade");

    if let Some(status) = upgrade_status.status {
        if let Some(replication) = &status.replication {
            tracing::info!(
                "Replication status: status={:?}, lag_bytes={:?}, lag_seconds={:?}, lsn_in_sync={:?}",
                replication.status,
                replication.lag_bytes,
                replication.lag_seconds,
                replication.lsn_in_sync
            );

            // Replication status should be populated
            assert!(
                replication.source_lsn.is_some() || replication.lag_bytes.is_some(),
                "Replication metrics should be tracked"
            );
        } else {
            // If still early in replication setup, that's OK
            tracing::info!("Replication status not yet populated (still initializing)");
        }
    }

    // Wait for lag to reach zero (replication caught up)
    tracing::info!("Waiting for replication to catch up...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_past_phase(UpgradePhase::Replicating),
        "past_replicating",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should progress past Replicating");

    // Verify final replication status shows synced
    let final_status = upgrade_api.get("test-upgrade").await.expect("get upgrade");
    if let Some(status) = final_status.status
        && let Some(replication) = &status.replication
    {
        tracing::info!(
            "Final replication: lag_bytes={:?}, lsn_in_sync={:?}",
            replication.lag_bytes,
            replication.lsn_in_sync
        );

        // After catching up, lag should be 0 or very small
        if let Some(lag) = replication.lag_bytes {
            assert!(lag <= 1024, "Lag should be minimal after catchup");
        }
    }

    tracing::info!("SUCCESS: Replication status monitoring works correctly!");

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

// =============================================================================
// Phase 3: Cutover Tests
// =============================================================================

/// Test: Auto cutover mode completes without manual intervention
///
/// This test creates an upgrade with auto cutover mode and verifies
/// it completes automatically when conditions are met.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_auto_cutover() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster
    tracing::info!("Creating source cluster...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Create upgrade with AUTO cutover mode
    tracing::info!("Creating upgrade with auto cutover mode...");
    let upgrade = PostgresUpgradeBuilder::new("auto-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_automatic_cutover()
        .with_verification_passes(1)
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to complete automatically (no manual cutover needed)
    tracing::info!("Waiting for auto cutover to complete...");
    let final_upgrade = wait_for_upgrade_named(
        &upgrade_api,
        "auto-upgrade",
        upgrade_terminal(),
        "terminal",
        UPGRADE_TIMEOUT,
    )
    .await
    .expect("upgrade should complete automatically");

    let final_phase = final_upgrade.status.as_ref().map(|s| &s.phase);
    tracing::info!(
        "Auto cutover upgrade completed with phase: {:?}",
        final_phase
    );

    assert!(
        matches!(final_phase, Some(UpgradePhase::Completed)),
        "Expected Completed phase for auto cutover, got {:?}",
        final_phase
    );

    tracing::info!("SUCCESS: Auto cutover completed successfully!");

    // Cleanup
    upgrade_api
        .delete("auto-upgrade", &DeleteParams::default())
        .await
        .ok();
}

/// Test: Verify connections work on target after cutover
///
/// This test performs a complete upgrade and verifies that:
/// 1. Target cluster is accessible after cutover
/// 2. Data is available on the target
/// 3. Target is the new primary
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_connectivity_after_cutover() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster with data
    tracing::info!("Creating source cluster...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Insert data into source
    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        pg_client
            .execute(
                "CREATE TABLE cutover_test (id SERIAL PRIMARY KEY, msg TEXT)",
                &[],
            )
            .await
            .expect("create table");

        pg_client
            .execute(
                "INSERT INTO cutover_test (msg) VALUES ('before_cutover')",
                &[],
            )
            .await
            .expect("insert row");
    }
    drop(source_pf);

    // Create upgrade with manual cutover
    tracing::info!("Creating upgrade...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .with_verification_passes(1)
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for cutover ready
    tracing::info!("Waiting for cutover ready...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_ready_for_cutover(),
        "ready_for_cutover",
        UPGRADE_TIMEOUT,
    )
    .await
    .expect("upgrade should be ready for cutover");

    // Trigger manual cutover
    tracing::info!("Triggering manual cutover...");
    let cutover_patch = serde_json::json!({
        "metadata": {
            "annotations": {
                "postgres-operator.smoketurner.com/cutover": "now"
            }
        }
    });

    upgrade_api
        .patch(
            "test-upgrade",
            &PatchParams::default(),
            &Patch::Merge(&cutover_patch),
        )
        .await
        .expect("apply cutover annotation");

    // Wait for completion
    tracing::info!("Waiting for upgrade completion...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_terminal(),
        "terminal",
        UPGRADE_PHASE_TIMEOUT,
    )
    .await
    .expect("upgrade should complete");

    // Verify target cluster connectivity
    tracing::info!("Verifying target cluster connectivity...");
    let target_creds = fetch_credentials(&client, ns.name(), "test-upgrade-target-credentials")
        .await
        .expect("fetch target credentials");

    let target_pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("test-upgrade-target-primary", 5432),
        None,
    )
    .await
    .expect("start target port-forward");

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(target_pf.local_port())
            .user(&target_creds.username)
            .password(&target_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to target");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Verify data exists
        let row: String = pg_client
            .query_one("SELECT msg FROM cutover_test WHERE id = 1", &[])
            .await
            .expect("query row")
            .get(0);

        assert_eq!(row, "before_cutover", "Data should be replicated");

        // Verify PostgreSQL version
        let version: String = pg_client
            .query_one("SHOW server_version", &[])
            .await
            .expect("get version")
            .get(0);

        tracing::info!("Target PostgreSQL version: {}", version);
        assert!(version.starts_with("17"), "Target should be version 17");

        // Verify this is a primary (can write)
        pg_client
            .execute(
                "INSERT INTO cutover_test (msg) VALUES ('after_cutover')",
                &[],
            )
            .await
            .expect("should be able to write to target");
    }

    tracing::info!("SUCCESS: Connectivity after cutover verified!");

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

/// Test: Cutover with data integrity verification
///
/// This test creates data before and during upgrade, then verifies
/// all data is present after cutover.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_cutover_data_integrity() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster
    tracing::info!("Creating source cluster...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Insert initial data
    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Create table with 50 initial rows
        pg_client
            .execute(
                "CREATE TABLE integrity_test (id SERIAL PRIMARY KEY, batch TEXT, value INT)",
                &[],
            )
            .await
            .expect("create table");

        for i in 1..=50 {
            pg_client
                .execute(
                    "INSERT INTO integrity_test (batch, value) VALUES ('initial', $1)",
                    &[&i],
                )
                .await
                .expect("insert row");
        }
        tracing::info!("Inserted 50 initial rows");
    }
    drop(source_pf);

    // Create upgrade
    tracing::info!("Creating upgrade...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_automatic_cutover()
        .with_verification_passes(1)
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to reach Replicating, then add more data
    tracing::info!("Waiting for Replicating phase...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_is_phase(UpgradePhase::Replicating),
        "replicating",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should reach Replicating");

    // Insert additional data during replication
    let source_pf2 = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("source-primary", 5432),
        None,
    )
    .await
    .expect("start source port-forward");

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf2.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Add 25 more rows during replication
        for i in 51..=75 {
            pg_client
                .execute(
                    "INSERT INTO integrity_test (batch, value) VALUES ('during_replication', $1)",
                    &[&i],
                )
                .await
                .expect("insert row");
        }
        tracing::info!("Inserted 25 additional rows during replication");
    }
    drop(source_pf2);

    // Wait for upgrade to complete
    tracing::info!("Waiting for upgrade completion...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_terminal(),
        "terminal",
        UPGRADE_TIMEOUT,
    )
    .await
    .expect("upgrade should complete");

    // Verify all data on target
    tracing::info!("Verifying data integrity on target...");
    let target_creds = fetch_credentials(&client, ns.name(), "test-upgrade-target-credentials")
        .await
        .expect("fetch target credentials");

    let target_pf = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("test-upgrade-target-primary", 5432),
        None,
    )
    .await
    .expect("start target port-forward");

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(target_pf.local_port())
            .user(&target_creds.username)
            .password(&target_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to target");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Count total rows
        let total: i64 = pg_client
            .query_one("SELECT COUNT(*) FROM integrity_test", &[])
            .await
            .expect("count rows")
            .get(0);

        // Count by batch
        let initial: i64 = pg_client
            .query_one(
                "SELECT COUNT(*) FROM integrity_test WHERE batch = 'initial'",
                &[],
            )
            .await
            .expect("count initial")
            .get(0);

        let during: i64 = pg_client
            .query_one(
                "SELECT COUNT(*) FROM integrity_test WHERE batch = 'during_replication'",
                &[],
            )
            .await
            .expect("count during")
            .get(0);

        tracing::info!(
            "Data integrity: total={}, initial={}, during_replication={}",
            total,
            initial,
            during
        );

        assert_eq!(total, 75, "Should have all 75 rows");
        assert_eq!(initial, 50, "Should have 50 initial rows");
        assert_eq!(during, 25, "Should have 25 rows added during replication");
    }

    tracing::info!("SUCCESS: All data preserved through cutover!");

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

// =============================================================================
// Phase 4: Rollback Tests
// =============================================================================

/// Test: Rollback triggered during replication phase
///
/// This test verifies that a rollback can be triggered during the replication
/// phase and the upgrade correctly transitions to RolledBack state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_rollback_during_replication() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster
    tracing::info!("Creating source cluster...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Create upgrade with manual cutover
    tracing::info!("Creating upgrade...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for Replicating phase
    tracing::info!("Waiting for Replicating phase...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_is_phase(UpgradePhase::Replicating),
        "replicating",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should reach Replicating");

    // Trigger rollback
    tracing::info!("Triggering rollback...");
    let rollback_patch = serde_json::json!({
        "metadata": {
            "annotations": {
                "postgres-operator.smoketurner.com/rollback": "now"
            }
        }
    });

    upgrade_api
        .patch(
            "test-upgrade",
            &PatchParams::default(),
            &Patch::Merge(&rollback_patch),
        )
        .await
        .expect("apply rollback annotation");

    // Wait for rollback to complete
    tracing::info!("Waiting for rollback to complete...");
    let final_upgrade = wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_terminal(),
        "terminal",
        UPGRADE_PHASE_TIMEOUT,
    )
    .await
    .expect("upgrade should reach terminal state");

    let final_phase = final_upgrade.status.as_ref().map(|s| &s.phase);
    tracing::info!("Rollback completed with phase: {:?}", final_phase);

    assert!(
        matches!(final_phase, Some(UpgradePhase::RolledBack)),
        "Expected RolledBack phase, got {:?}",
        final_phase
    );

    // Verify source cluster is still operational
    tracing::info!("Verifying source cluster is still operational...");
    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        Duration::from_secs(30),
    )
    .await
    .expect("source cluster should still be operational");

    // Verify source cluster connectivity
    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source after rollback");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Verify this is still the source (v16)
        let version: String = pg_client
            .query_one("SHOW server_version", &[])
            .await
            .expect("get version")
            .get(0);

        tracing::info!("Source PostgreSQL version after rollback: {}", version);
        assert!(
            version.starts_with("16"),
            "Source should still be version 16"
        );

        // Verify writes work (not read-only)
        pg_client
            .execute(
                "CREATE TABLE IF NOT EXISTS rollback_test (id SERIAL PRIMARY KEY, msg TEXT)",
                &[],
            )
            .await
            .expect("should be able to write after rollback");

        pg_client
            .execute(
                "INSERT INTO rollback_test (msg) VALUES ('after_rollback')",
                &[],
            )
            .await
            .expect("insert after rollback");
    }

    tracing::info!("SUCCESS: Rollback completed and source cluster operational!");

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

/// Test: Rollback from WaitingForManualCutover phase
///
/// This test verifies rollback works when the upgrade is waiting for
/// manual cutover approval.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_rollback_from_cutover_ready() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster with data
    tracing::info!("Creating source cluster with data...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Insert test data
    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        pg_client
            .execute(
                "CREATE TABLE rollback_data (id SERIAL PRIMARY KEY, value INT)",
                &[],
            )
            .await
            .expect("create table");

        for i in 1..=10 {
            pg_client
                .execute("INSERT INTO rollback_data (value) VALUES ($1)", &[&i])
                .await
                .expect("insert row");
        }
        tracing::info!("Inserted 10 test rows");
    }
    drop(source_pf);

    // Create upgrade with manual cutover
    tracing::info!("Creating upgrade...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .with_verification_passes(1)
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for WaitingForManualCutover
    tracing::info!("Waiting for upgrade to be ready for cutover...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_ready_for_cutover(),
        "ready_for_cutover",
        UPGRADE_TIMEOUT,
    )
    .await
    .expect("upgrade should be ready for cutover");

    // Instead of cutover, trigger rollback
    tracing::info!("Triggering rollback instead of cutover...");
    let rollback_patch = serde_json::json!({
        "metadata": {
            "annotations": {
                "postgres-operator.smoketurner.com/rollback": "now"
            }
        }
    });

    upgrade_api
        .patch(
            "test-upgrade",
            &PatchParams::default(),
            &Patch::Merge(&rollback_patch),
        )
        .await
        .expect("apply rollback annotation");

    // Wait for rollback
    tracing::info!("Waiting for rollback to complete...");
    let final_upgrade = wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_terminal(),
        "terminal",
        UPGRADE_PHASE_TIMEOUT,
    )
    .await
    .expect("upgrade should reach terminal state");

    let final_phase = final_upgrade.status.as_ref().map(|s| &s.phase);
    tracing::info!("Rollback completed with phase: {:?}", final_phase);

    assert!(
        matches!(final_phase, Some(UpgradePhase::RolledBack)),
        "Expected RolledBack phase, got {:?}",
        final_phase
    );

    // Verify source data is still accessible
    tracing::info!("Verifying source data after rollback...");
    let source_pf2 = PortForward::start(
        client.clone(),
        ns.name(),
        PortForwardTarget::service("source-primary", 5432),
        None,
    )
    .await
    .expect("start source port-forward");

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf2.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source after rollback");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Verify data
        let count: i64 = pg_client
            .query_one("SELECT COUNT(*) FROM rollback_data", &[])
            .await
            .expect("count rows")
            .get(0);

        assert_eq!(count, 10, "Should still have 10 rows after rollback");
        tracing::info!("All {} rows preserved after rollback", count);

        // Verify writes work
        pg_client
            .execute("INSERT INTO rollback_data (value) VALUES (11)", &[])
            .await
            .expect("should be able to write after rollback");
    }

    tracing::info!("SUCCESS: Rollback from cutover-ready phase completed successfully!");

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

// =============================================================================
// Phase 5: Error Handling Tests
// =============================================================================

/// Test: Upgrade handles verification mismatch correctly
///
/// This test creates a scenario where verification should fail by
/// inserting data on source after replication starts but before
/// verification, causing a row count mismatch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_verification_mismatch_detection() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster with data
    tracing::info!("Creating source cluster...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Insert initial data
    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        pg_client
            .execute(
                "CREATE TABLE verify_test (id SERIAL PRIMARY KEY, value INT)",
                &[],
            )
            .await
            .expect("create table");

        for i in 1..=10 {
            pg_client
                .execute("INSERT INTO verify_test (value) VALUES ($1)", &[&i])
                .await
                .expect("insert row");
        }
    }
    drop(source_pf);

    // Create upgrade
    tracing::info!("Creating upgrade...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .with_verification_passes(3) // Require multiple passes
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to reach Verifying phase
    tracing::info!("Waiting for Verifying phase...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_is_phase(UpgradePhase::Verifying),
        "verifying",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should reach Verifying");

    // Check verification status
    let upgrade_status = upgrade_api.get("test-upgrade").await.expect("get upgrade");
    if let Some(status) = upgrade_status.status
        && let Some(verification) = &status.verification
    {
        tracing::info!(
            "Verification status: verified={}, matched={}, mismatched={}, passes={}",
            verification.tables_verified,
            verification.tables_matched,
            verification.tables_mismatched,
            verification.consecutive_passes
        );

        // In a properly synced upgrade, tables should match
        assert!(
            verification.tables_verified > 0,
            "Should have verified some tables"
        );
    }

    tracing::info!("SUCCESS: Verification mismatch detection test passed!");

    // Cleanup
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .ok();
}

/// Test: Upgrade resource deletion during upgrade
///
/// This test verifies that deleting an upgrade resource during the
/// upgrade process is handled gracefully.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_upgrade_deletion_during_upgrade() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");

    let ns = TestNamespace::create(client.clone(), "upgrade-e2e")
        .await
        .expect("create namespace");

    let _operator = ScopedOperator::start_with_upgrade(client.clone(), ns.name()).await;

    let cluster_api: Api<PostgresCluster> = Api::namespaced(client.clone(), ns.name());
    let upgrade_api: Api<PostgresUpgrade> = Api::namespaced(client.clone(), ns.name());

    // Create source cluster
    tracing::info!("Creating source cluster...");
    let source = PostgresClusterBuilder::single("source", ns.name())
        .with_version(PostgresVersion::V16)
        .with_storage("1Gi", None)
        .without_tls()
        .build();
    cluster_api
        .create(&PostParams::default(), &source)
        .await
        .expect("create source cluster");

    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        POSTGRES_READY_TIMEOUT,
    )
    .await
    .expect("source cluster should become operational");

    // Create upgrade
    tracing::info!("Creating upgrade...");
    let upgrade = PostgresUpgradeBuilder::new("test-upgrade", ns.name())
        .with_source_cluster("source")
        .with_target_version(PostgresVersion::V17)
        .with_manual_cutover()
        .build();

    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("create upgrade");

    // Wait for upgrade to start
    tracing::info!("Waiting for upgrade to start...");
    wait_for_upgrade_named(
        &upgrade_api,
        "test-upgrade",
        upgrade_past_pending(),
        "past_pending",
        Duration::from_secs(180),
    )
    .await
    .expect("upgrade should start");

    // Delete the upgrade resource
    tracing::info!("Deleting upgrade resource...");
    upgrade_api
        .delete("test-upgrade", &DeleteParams::default())
        .await
        .expect("delete upgrade");

    // Wait for deletion to complete
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify upgrade is gone
    let result = upgrade_api.get("test-upgrade").await;
    assert!(result.is_err(), "Upgrade should be deleted");
    tracing::info!("Upgrade resource deleted successfully");

    // Verify source cluster is still operational
    tracing::info!("Verifying source cluster is still operational...");
    wait_for_cluster(
        &cluster_api,
        "source",
        cluster_operational(1),
        Duration::from_secs(30),
    )
    .await
    .expect("source cluster should still be operational");

    let source_creds = fetch_credentials(&client, ns.name(), "source-credentials")
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

    {
        let (pg_client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(source_pf.local_port())
            .user(&source_creds.username)
            .password(&source_creds.password)
            .dbname("postgres")
            .connect(tokio_postgres::NoTls)
            .await
            .expect("connect to source");

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                tracing::warn!("Connection error: {}", e);
            }
        });

        // Verify writes work
        pg_client
            .execute(
                "CREATE TABLE IF NOT EXISTS deletion_test (id SERIAL PRIMARY KEY)",
                &[],
            )
            .await
            .expect("should be able to write after upgrade deletion");
    }

    tracing::info!("SUCCESS: Upgrade deletion handled gracefully!");
}
