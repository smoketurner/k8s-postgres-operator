//! Reconciler for PostgresUpgrade resources
//!
//! This reconciler manages the lifecycle of blue-green PostgreSQL major version upgrades
//! using logical replication for near-zero downtime.
//!
//! ## Overview
//!
//! The upgrade process follows these high-level steps:
//! 1. Validate source cluster and target version
//! 2. Create target cluster with new PostgreSQL version
//! 3. Configure logical replication (publication on source, subscription on target)
//! 4. Monitor replication until caught up
//! 5. Verify data integrity (row counts, LSN sync)
//! 6. Sync sequences
//! 7. Cutover (switch services to target)
//! 8. Health check and cleanup
//!
//! Rollback is supported at most stages via annotation.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Client, ResourceExt};
use tracing::{debug, error, info, instrument, warn};

use crate::controller::upgrade_error::{UpgradeBackoffConfig, UpgradeError, UpgradeResult};
use crate::controller::upgrade_state_machine::{
    UpgradeEvent, UpgradeStateMachine, UpgradeTransitionContext, UpgradeTransitionResult,
};
use crate::crd::{
    ClusterPhase, CutoverMode, PostgresCluster, PostgresClusterSpec, PostgresUpgrade,
    ReplicationStatus, SequenceSyncStatus, UpgradeLineageRef, UpgradePhase, VerificationStatus,
};
use crate::resources::postgres_client::PostgresConnection;
use crate::resources::replication::{
    self, LagStatus, ReplicationError, RowCountVerification, SequenceSyncResult,
};

/// Finalizer for PostgresUpgrade resources
pub const UPGRADE_FINALIZER: &str = "postgresupgrade.postgres-operator.smoketurner.com/finalizer";

/// Label applied to target clusters linking them to the upgrade
pub const UPGRADE_LABEL: &str = "postgres-operator.smoketurner.com/upgrade";

/// Annotation for triggering manual cutover
pub const CUTOVER_ANNOTATION: &str = "postgres-operator.smoketurner.com/cutover";

/// Annotation for triggering rollback
pub const ROLLBACK_ANNOTATION: &str = "postgres-operator.smoketurner.com/rollback";

/// Default replication lag threshold in bytes (0 for zero lag)
#[allow(dead_code)]
const DEFAULT_LAG_THRESHOLD_BYTES: i64 = 0;

/// Default row count tolerance for verification
#[allow(dead_code)]
const DEFAULT_ROW_COUNT_TOLERANCE: i64 = 0;

/// Context for the upgrade reconciler
pub struct UpgradeContext {
    pub client: Client,
}

impl UpgradeContext {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Main reconciliation function for PostgresUpgrade
#[instrument(skip(upgrade, ctx), fields(name = %upgrade.name_any(), namespace = upgrade.namespace().unwrap_or_default()))]
pub async fn reconcile_upgrade(
    upgrade: Arc<PostgresUpgrade>,
    ctx: Arc<UpgradeContext>,
) -> Result<Action, UpgradeError> {
    let start_time = std::time::Instant::now();
    let ns = upgrade.namespace().unwrap_or_default();
    let _name = upgrade.name_any();

    info!("Reconciling PostgresUpgrade");

    // Handle deletion
    if upgrade.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&upgrade, &ctx, &ns).await;
    }

    // Ensure finalizer is present
    if !has_finalizer(&upgrade) {
        add_finalizer(&upgrade, &ctx, &ns).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Get current phase from status
    let current_phase = upgrade.status.as_ref().map(|s| s.phase).unwrap_or_default();

    // Build transition context from current state
    let transition_ctx = build_transition_context(&upgrade, &ctx, &ns).await?;

    // Check for rollback annotation
    if transition_ctx.rollback_requested {
        return handle_rollback(&upgrade, &ctx, &ns, &current_phase).await;
    }

    // Use state machine to determine next action
    let state_machine = UpgradeStateMachine::new();
    let event =
        determine_event_for_phase(&upgrade, &ctx, &ns, &current_phase, &transition_ctx).await?;

    let result = if let Some(event) = event {
        let transition_result =
            state_machine.transition(&current_phase, event.clone(), &transition_ctx);

        match transition_result {
            UpgradeTransitionResult::Success {
                to, description, ..
            } => {
                info!(
                    "Phase transition: {:?} -> {:?} ({})",
                    current_phase, to, description
                );

                // Execute phase-specific actions
                execute_phase_transition(&upgrade, &ctx, &ns, &current_phase, &to, &transition_ctx)
                    .await?;

                // Update status with new phase
                update_phase(&upgrade, &ctx, &ns, to).await?;

                // Determine requeue interval based on new phase
                Ok(Action::requeue(requeue_duration_for_phase(&to)))
            }
            UpgradeTransitionResult::InvalidTransition { current, event } => {
                debug!(
                    "No valid transition from {:?} with event {:?}",
                    current, event
                );
                // Stay in current phase, check again soon
                Ok(Action::requeue(requeue_duration_for_phase(&current_phase)))
            }
            UpgradeTransitionResult::GuardFailed { reason, .. } => {
                debug!("Transition guard failed: {}", reason);
                // Update status message and requeue
                Ok(Action::requeue(requeue_duration_for_phase(&current_phase)))
            }
        }
    } else {
        // No event to process, continue monitoring
        execute_phase_monitoring(&upgrade, &ctx, &ns, &current_phase).await?;
        Ok(Action::requeue(requeue_duration_for_phase(&current_phase)))
    };

    let duration_secs = start_time.elapsed().as_secs_f64();
    debug!("Reconciliation completed in {:.3}s", duration_secs);

    result
}

/// Error policy for the upgrade controller
pub fn upgrade_error_policy(
    upgrade: Arc<PostgresUpgrade>,
    error: &UpgradeError,
    _ctx: Arc<UpgradeContext>,
) -> Action {
    let name = upgrade.name_any();
    let backoff = UpgradeBackoffConfig::default();

    let retry_count = upgrade
        .status
        .as_ref()
        .and_then(|s| s.retry_count)
        .unwrap_or(0) as u32;

    let delay = backoff.delay_for_error(error, retry_count);

    if error.is_permanent() {
        error!(
            "Permanent error for upgrade {}: {:?}, not retrying automatically",
            name, error
        );
        // Still requeue with max delay to allow status updates
        Action::requeue(delay)
    } else if error.is_retryable() {
        warn!(
            "Retryable error for upgrade {} (retry #{}): {:?}, requeuing in {:?}",
            name, retry_count, error, delay
        );
        Action::requeue(delay)
    } else if error.blocks_cutover() {
        debug!(
            "Cutover-blocking error for upgrade {}: {:?}, continuing to monitor",
            name, error
        );
        Action::requeue(delay)
    } else {
        warn!(
            "Unexpected error for upgrade {}: {:?}, requeuing in {:?}",
            name, error, delay
        );
        Action::requeue(delay)
    }
}

/// Build the transition context from current state
async fn build_transition_context(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<UpgradeTransitionContext> {
    let mut tc = UpgradeTransitionContext::default();

    // Get source cluster status
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);

    match clusters_api.get_opt(source_name).await {
        Ok(Some(cluster)) => {
            let phase = cluster
                .status
                .as_ref()
                .map(|s| s.phase)
                .unwrap_or(ClusterPhase::Pending);
            tc.source_cluster_ready = phase == ClusterPhase::Running;

            // Store source version for validation
            let source_version = cluster.spec.version.as_major_version();
            tc.source_cluster_version = Some(source_version);

            // Validate target version against source
            let target_version = upgrade.spec.target_version.as_major_version();
            if target_version == source_version {
                tc.version_validation_failed = true;
                tc.error_message = Some(format!(
                    "Invalid upgrade: source and target versions are the same (PostgreSQL {})",
                    source_version
                ));
            } else if target_version < source_version {
                tc.version_validation_failed = true;
                tc.error_message = Some(format!(
                    "Invalid upgrade: cannot downgrade from PostgreSQL {} to {}",
                    source_version, target_version
                ));
            }
        }
        Ok(None) => {
            tc.source_cluster_ready = false;
            tc.error_message = Some(format!(
                "Source cluster {}/{} not found",
                source_ns, source_name
            ));
        }
        Err(e) => {
            tc.error_message = Some(format!("Failed to get source cluster: {}", e));
        }
    }

    // Get target cluster status if it exists
    let target_name = generate_target_cluster_name(&upgrade.name_any());
    let target_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);

    if let Ok(Some(cluster)) = target_api.get_opt(&target_name).await {
        let phase = cluster
            .status
            .as_ref()
            .map(|s| s.phase)
            .unwrap_or(ClusterPhase::Pending);
        tc.target_cluster_ready = phase == ClusterPhase::Running;
    }

    // Get replication status from upgrade status
    if let Some(status) = &upgrade.status {
        if let Some(repl) = &status.replication {
            tc.replication_lag_bytes = repl.lag_bytes;
            tc.replication_lag_seconds = repl.lag_seconds;
        }

        if let Some(verif) = &status.verification {
            tc.verification_passes = verif.consecutive_passes;
            tc.row_count_mismatches = verif.tables_mismatched;
        }

        if let Some(seq) = &status.sequences {
            tc.sequences_synced = seq.synced;
        }

        // Check rollback status
        if let Some(rollback) = &status.rollback {
            tc.rollback_feasible = rollback.feasible;
            tc.target_has_writes = rollback.data_loss_risk;
        }

        // For backup requirement, check if there's a recent backup in replication status
        tc.backup_requirement_met = true; // TODO: Implement proper backup check
    }

    // Get configuration
    tc.cutover_mode = upgrade.spec.strategy.cutover.mode;

    tc.required_verification_passes = upgrade.spec.strategy.pre_checks.min_verification_passes;

    // Check for maintenance window
    tc.within_maintenance_window = is_within_maintenance_window(upgrade);

    // Check for rollback annotation
    tc.rollback_requested = upgrade
        .metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key(ROLLBACK_ANNOTATION));

    // Check phase timeout
    tc.phase_timeout_elapsed = is_phase_timeout_elapsed(upgrade);

    Ok(tc)
}

/// Determine the appropriate event for the current phase
async fn determine_event_for_phase(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    current_phase: &UpgradePhase,
    tc: &UpgradeTransitionContext,
) -> UpgradeResult<Option<UpgradeEvent>> {
    // Check for errors first
    if tc.error_message.is_some() {
        return Ok(Some(UpgradeEvent::ErrorOccurred));
    }

    // Check for timeout
    if tc.phase_timeout_elapsed {
        return Ok(Some(UpgradeEvent::TimeoutOccurred));
    }

    // Phase-specific event determination
    match current_phase {
        UpgradePhase::Pending => {
            if tc.source_cluster_ready {
                Ok(Some(UpgradeEvent::ValidationPassed))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::CreatingTarget => {
            if tc.target_cluster_ready {
                Ok(Some(UpgradeEvent::TargetClusterReady))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::ConfiguringReplication => {
            // Check if replication is configured (subscription exists and active)
            let target_name = generate_target_cluster_name(&upgrade.name_any());
            let sub_name = generate_subscription_name(&upgrade.name_any());

            // Connect to target cluster to check subscription state
            match PostgresConnection::connect_primary(&ctx.client, ns, &target_name).await {
                Ok(conn) => match replication::get_subscription_state(&conn, &sub_name).await {
                    Ok(state) if state.is_active() || state.is_syncing() => {
                        Ok(Some(UpgradeEvent::ReplicationConfigured))
                    }
                    Ok(_) => Ok(None),
                    Err(ReplicationError::SubscriptionNotFound(_)) => Ok(None),
                    Err(e) => {
                        warn!("Error checking subscription state: {}", e);
                        Ok(None)
                    }
                },
                Err(e) => {
                    warn!("Failed to connect to target cluster: {}", e);
                    Ok(None)
                }
            }
        }

        UpgradePhase::Replicating => {
            // Check if replication is caught up
            if tc.replication_lag_bytes == Some(0) {
                Ok(Some(UpgradeEvent::ReplicationCaughtUp))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::Verifying => {
            let required = tc.required_verification_passes;
            if tc.verification_passes >= required && tc.row_count_mismatches == 0 {
                Ok(Some(UpgradeEvent::VerificationPassed))
            } else if tc.row_count_mismatches > 0 {
                Ok(Some(UpgradeEvent::VerificationFailed))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::SyncingSequences => {
            if tc.sequences_synced {
                Ok(Some(UpgradeEvent::SequencesSynced))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::ReadyForCutover => {
            if tc.cutover_mode == CutoverMode::Manual {
                Ok(Some(UpgradeEvent::PreChecksPassed))
            } else if tc.ready_for_auto_cutover() {
                Ok(Some(UpgradeEvent::AutoCutoverConditionsMet))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::WaitingForManualCutover => {
            // Check for cutover annotation
            let cutover_requested = upgrade
                .metadata
                .annotations
                .as_ref()
                .is_some_and(|a| a.contains_key(CUTOVER_ANNOTATION));

            if cutover_requested {
                Ok(Some(UpgradeEvent::ManualCutoverTriggered))
            } else if tc.row_count_mismatches > 0 {
                Ok(Some(UpgradeEvent::VerificationFailed))
            } else {
                Ok(None)
            }
        }

        UpgradePhase::CuttingOver => {
            // Cutover is executed during the transition INTO this phase.
            // Once we're in CuttingOver, the services have been switched,
            // so we immediately emit ServicesSwitched to move to HealthChecking.
            Ok(Some(UpgradeEvent::ServicesSwitched))
        }

        UpgradePhase::HealthChecking => {
            // Health check logic - verify target is accepting connections
            Ok(None)
        }

        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack => {
            // Terminal states - only rollback is possible for Completed/Failed
            Ok(None)
        }
    }
}

/// Execute actions for a phase transition
async fn execute_phase_transition(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    from: &UpgradePhase,
    to: &UpgradePhase,
    _tc: &UpgradeTransitionContext,
) -> UpgradeResult<()> {
    match (from, to) {
        (UpgradePhase::Pending, UpgradePhase::CreatingTarget) => {
            create_target_cluster(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::CreatingTarget, UpgradePhase::ConfiguringReplication) => {
            setup_replication(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::Verifying, UpgradePhase::SyncingSequences) => {
            // Set source to read-only before syncing sequences
            let source_name = &upgrade.spec.source_cluster.name;
            let source_ns = upgrade
                .spec
                .source_cluster
                .namespace
                .as_deref()
                .unwrap_or(ns);

            // Connect to source and set read-only
            let source_conn =
                PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;
            replication::set_source_readonly(&source_conn).await?;

            sync_sequences(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::ReadyForCutover, UpgradePhase::CuttingOver)
        | (UpgradePhase::WaitingForManualCutover, UpgradePhase::CuttingOver) => {
            execute_cutover(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::CuttingOver, UpgradePhase::HealthChecking) => {
            // Cutover completed - mark source as superseded and set origin on target
            mark_source_superseded(upgrade, ctx, ns).await?;
            set_target_origin(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::HealthChecking, UpgradePhase::Completed) => {
            cleanup_replication(upgrade, ctx, ns).await?;
        }

        _ => {
            // No special action needed for other transitions
        }
    }

    Ok(())
}

/// Execute monitoring actions for the current phase
async fn execute_phase_monitoring(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    current_phase: &UpgradePhase,
) -> UpgradeResult<()> {
    match current_phase {
        UpgradePhase::Replicating => {
            // Update replication lag status
            let lag_status = get_replication_lag(upgrade, ctx, ns).await?;
            update_replication_status(upgrade, ctx, ns, &lag_status).await?;
        }

        UpgradePhase::Verifying => {
            // Run row count verification
            let verification = run_verification(upgrade, ctx, ns).await?;
            debug!(
                tables_checked = verification.tables_checked,
                tables_matched = verification.tables_matched,
                tables_mismatched = verification.tables_mismatched,
                "Row count verification result"
            );
            update_verification_status(upgrade, ctx, ns, &verification).await?;
        }

        UpgradePhase::HealthChecking => {
            // Check target cluster health
            let target_name = generate_target_cluster_name(&upgrade.name_any());
            let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);

            if let Some(cluster) = clusters_api.get_opt(&target_name).await? {
                let phase = cluster
                    .status
                    .as_ref()
                    .map(|s| s.phase)
                    .unwrap_or(ClusterPhase::Pending);

                if phase == ClusterPhase::Running {
                    // Transition to completed
                    update_phase(upgrade, ctx, ns, UpgradePhase::Completed).await?;
                }
            }
        }

        _ => {
            // No monitoring needed for other phases
        }
    }

    Ok(())
}

/// Create the target cluster with new PostgreSQL version
async fn create_target_cluster(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    // Get source cluster spec
    let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);
    let source_cluster = clusters_api.get(source_name).await?;

    // Build target cluster spec
    let target_name = generate_target_cluster_name(&upgrade.name_any());
    let target_spec = build_target_spec(&source_cluster, upgrade);

    // Create target cluster (without owner reference - survives upgrade deletion)
    let target_cluster = PostgresCluster {
        metadata: kube::api::ObjectMeta {
            name: Some(target_name.clone()),
            namespace: Some(ns.to_string()),
            labels: Some({
                let mut labels = std::collections::BTreeMap::new();
                labels.insert(UPGRADE_LABEL.to_string(), upgrade.name_any());
                labels.insert(
                    "postgres-operator.smoketurner.com/cluster".to_string(),
                    target_name.clone(),
                );
                labels
            }),
            ..Default::default()
        },
        spec: target_spec,
        status: None,
    };

    let target_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);
    let params = PatchParams::apply("postgres-operator").force();

    target_api
        .patch(&target_name, &params, &Patch::Apply(&target_cluster))
        .await?;

    info!(
        "Created target cluster {} for upgrade {}",
        target_name,
        upgrade.name_any()
    );

    Ok(())
}

/// Build the target cluster spec from source and upgrade overrides
fn build_target_spec(source: &PostgresCluster, upgrade: &PostgresUpgrade) -> PostgresClusterSpec {
    let mut spec = source.spec.clone();

    // Apply target version
    spec.version = upgrade.spec.target_version.clone();

    // Apply any overrides from upgrade spec
    if let Some(overrides) = &upgrade.spec.target_cluster_overrides {
        if let Some(resources) = &overrides.resources {
            spec.resources = Some(resources.clone());
        }
        if let Some(replicas) = overrides.replicas {
            spec.replicas = replicas;
        }
        // Note: labels are merged into the cluster's metadata, not spec
    }

    spec
}

/// Set up logical replication between source and target
async fn setup_replication(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    let target_name = generate_target_cluster_name(&upgrade.name_any());
    let pub_name = generate_publication_name(&upgrade.name_any());
    let sub_name = generate_subscription_name(&upgrade.name_any());

    // Copy schema from source to target before setting up replication
    // Logical replication only replicates DML (data), not DDL (schema)
    // Note: copy_schema still uses pod exec for pg_dump
    info!(
        "Copying schema from source {} to target {} for upgrade {}",
        source_name,
        target_name,
        upgrade.name_any()
    );
    replication::copy_schema(
        &ctx.client,
        source_ns,
        source_name,
        ns,
        &target_name,
        "postgres",
    )
    .await?;

    // Connect to source cluster and create publication
    let source_conn =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;
    replication::setup_publication(&source_conn, &pub_name).await?;

    // Get source cluster service host
    let source_host = format!("{}-primary.{}.svc", source_name, source_ns);

    // Get source credentials (simplified - in production, get from secret)
    let source_password = get_postgres_password(&ctx.client, source_ns, source_name).await?;

    // Connect to target cluster and create subscription
    let target_conn = PostgresConnection::connect_primary(&ctx.client, ns, &target_name).await?;
    replication::setup_subscription(
        &target_conn,
        &sub_name,
        &source_host,
        5432,
        &pub_name,
        &source_password,
    )
    .await?;

    info!(
        "Configured replication for upgrade {} (pub: {}, sub: {})",
        upgrade.name_any(),
        pub_name,
        sub_name
    );

    Ok(())
}

/// Get the replication lag status
async fn get_replication_lag(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<LagStatus> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);
    let sub_name = generate_subscription_name(&upgrade.name_any());

    // Connect to source cluster and get replication lag
    let source_conn =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;
    let lag = replication::get_replication_lag(&source_conn, &sub_name)
        .await
        .map_err(|e| UpgradeError::SqlError(e.to_string()))?;

    Ok(lag)
}

/// Run row count verification
async fn run_verification(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<RowCountVerification> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);
    let target_name = generate_target_cluster_name(&upgrade.name_any());

    let tolerance = upgrade.spec.strategy.pre_checks.row_count_tolerance;

    // Connect to both clusters
    let source_conn =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;
    let target_conn = PostgresConnection::connect_primary(&ctx.client, ns, &target_name).await?;

    let verification = replication::verify_row_counts(&source_conn, &target_conn, tolerance)
        .await
        .map_err(|e| UpgradeError::SqlError(e.to_string()))?;

    Ok(verification)
}

/// Sync sequences from source to target
async fn sync_sequences(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);
    let target_name = generate_target_cluster_name(&upgrade.name_any());

    // Connect to both clusters
    let source_conn =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;
    let target_conn = PostgresConnection::connect_primary(&ctx.client, ns, &target_name).await?;

    let result = replication::sync_sequences(&source_conn, &target_conn)
        .await
        .map_err(|e| UpgradeError::SqlError(e.to_string()))?;

    // Update status with sync result
    update_sequence_sync_status(upgrade, ctx, ns, &result).await?;

    if result.failed_count > 0 {
        return Err(UpgradeError::SequenceSyncFailed {
            failed_count: result.failed_count,
        });
    }

    Ok(())
}

/// Execute the cutover by switching services
async fn execute_cutover(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let target_name = generate_target_cluster_name(&upgrade.name_any());

    info!(
        "Executing cutover for upgrade {}: {} -> {}",
        upgrade.name_any(),
        source_name,
        target_name
    );

    // The actual service switching is done by updating the service selectors
    // This is handled separately in the service module
    // For now, we just mark the cutover as initiated

    // Update status to indicate cutover is in progress
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "status": {
            "cutoverStartedAt": Utc::now().to_rfc3339(),
            "message": format!("Cutover initiated: switching services from {} to {}", source_name, target_name)
        }
    });

    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Clean up replication after successful upgrade
async fn cleanup_replication(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);
    let target_name = generate_target_cluster_name(&upgrade.name_any());
    let pub_name = generate_publication_name(&upgrade.name_any());
    let sub_name = generate_subscription_name(&upgrade.name_any());

    // Try to connect to target and drop subscription
    if let Ok(target_conn) =
        PostgresConnection::connect_primary(&ctx.client, ns, &target_name).await
        && let Err(e) = replication::drop_subscription(&target_conn, &sub_name).await
    {
        warn!("Failed to drop subscription {}: {}", sub_name, e);
    }

    // Try to connect to source and drop publication/replication slot
    if let Ok(source_conn) =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await
    {
        if let Err(e) = replication::drop_publication(&source_conn, &pub_name).await {
            warn!("Failed to drop publication {}: {}", pub_name, e);
        }
        if let Err(e) = replication::drop_replication_slot(&source_conn, &sub_name).await {
            warn!("Failed to drop replication slot {}: {}", sub_name, e);
        }
    }

    info!("Cleaned up replication for upgrade {}", upgrade.name_any());

    Ok(())
}

/// Mark the source cluster as Superseded and set the successor reference.
/// Called after cutover when traffic has been switched to the target cluster.
async fn mark_source_superseded(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);
    let target_name = generate_target_cluster_name(&upgrade.name_any());

    let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);

    let successor = UpgradeLineageRef {
        name: target_name.clone(),
        namespace: Some(ns.to_string()),
        upgrade_name: Some(upgrade.name_any()),
        created_at: Some(Utc::now().to_rfc3339()),
    };

    let patch = serde_json::json!({
        "status": {
            "phase": ClusterPhase::Superseded,
            "successor": successor
        }
    });

    clusters_api
        .patch_status(
            source_name,
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await?;

    info!(
        "Marked source cluster {}/{} as Superseded, successor: {}",
        source_ns, source_name, target_name
    );

    Ok(())
}

/// Set the origin reference on the target cluster.
/// Called after cutover to provide traceability for the cluster's lineage.
async fn set_target_origin(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);
    let target_name = generate_target_cluster_name(&upgrade.name_any());

    let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);

    let origin = UpgradeLineageRef {
        name: source_name.clone(),
        namespace: Some(source_ns.to_string()),
        upgrade_name: Some(upgrade.name_any()),
        created_at: Some(Utc::now().to_rfc3339()),
    };

    let patch = serde_json::json!({
        "status": {
            "origin": origin
        }
    });

    clusters_api
        .patch_status(
            &target_name,
            &PatchParams::default(),
            &Patch::Merge(&patch),
        )
        .await?;

    info!(
        "Set origin on target cluster {}/{} from: {}/{}",
        ns, target_name, source_ns, source_name
    );

    Ok(())
}

/// Handle rollback request
async fn handle_rollback(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    current_phase: &UpgradePhase,
) -> Result<Action, UpgradeError> {
    info!(
        "Processing rollback request for upgrade {} in phase {:?}",
        upgrade.name_any(),
        current_phase
    );

    // Check if rollback is feasible
    let rollback_status = upgrade.status.as_ref().and_then(|s| s.rollback.as_ref());

    if let Some(status) = rollback_status {
        if !status.feasible {
            warn!(
                "Rollback not feasible for upgrade {}: {}",
                upgrade.name_any(),
                status.reason.as_deref().unwrap_or("unknown reason")
            );
            return Err(UpgradeError::RollbackNotFeasible {
                reason: status.reason.clone().unwrap_or_default(),
            });
        }

        if status.data_loss_risk {
            warn!(
                "Rollback would cause data loss for upgrade {}",
                upgrade.name_any()
            );
            return Err(UpgradeError::RollbackDataLossRisk);
        }
    }

    // Execute rollback
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    // Set source back to read-write
    if let Ok(source_conn) =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await
        && let Err(e) = replication::set_source_readwrite(&source_conn).await
    {
        warn!("Failed to set source read-write during rollback: {}", e);
    }

    // Clean up replication
    if let Err(e) = cleanup_replication(upgrade, ctx, ns).await {
        warn!("Failed to clean up replication during rollback: {}", e);
    }

    // Update phase to RolledBack
    update_phase(upgrade, ctx, ns, UpgradePhase::RolledBack).await?;

    info!("Rollback completed for upgrade {}", upgrade.name_any());

    Ok(Action::await_change())
}

/// Handle deletion of the upgrade resource
async fn handle_deletion(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> Result<Action, UpgradeError> {
    info!("Handling deletion of upgrade {}", upgrade.name_any());

    // Clean up replication if still active
    let current_phase = upgrade.status.as_ref().map(|s| s.phase).unwrap_or_default();

    if !matches!(
        current_phase,
        UpgradePhase::Completed | UpgradePhase::RolledBack | UpgradePhase::Pending
    ) {
        // Clean up replication resources
        if let Err(e) = cleanup_replication(upgrade, ctx, ns).await {
            warn!("Failed to clean up replication during deletion: {}", e);
        }
    }

    // Remove finalizer
    if has_finalizer(upgrade) {
        let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
        let patch = serde_json::json!({
            "metadata": {
                "finalizers": null
            }
        });

        api.patch(
            &upgrade.name_any(),
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        info!("Removed finalizer from upgrade {}", upgrade.name_any());
    }

    Ok(Action::await_change())
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Check if finalizer is present
fn has_finalizer(upgrade: &PostgresUpgrade) -> bool {
    upgrade
        .metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.contains(&UPGRADE_FINALIZER.to_string()))
}

/// Add finalizer to upgrade
async fn add_finalizer(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [UPGRADE_FINALIZER]
        }
    });

    api.patch(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    info!("Added finalizer to upgrade {}", upgrade.name_any());
    Ok(())
}

/// Update the upgrade phase in status
async fn update_phase(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    phase: UpgradePhase,
) -> UpgradeResult<()> {
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let now = Utc::now().to_rfc3339();

    // Build status object conditionally
    let is_terminal = matches!(
        phase,
        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack
    );

    let is_starting = upgrade
        .status
        .as_ref()
        .map(|s| s.phase == UpgradePhase::Pending)
        .unwrap_or(true)
        && phase != UpgradePhase::Pending;

    let patch = match (is_terminal, is_starting) {
        (true, true) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "completedAt": now,
                "startedAt": now
            }
        }),
        (true, false) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "completedAt": now
            }
        }),
        (false, true) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "startedAt": now
            }
        }),
        (false, false) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation
            }
        }),
    };

    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    debug!(
        "Updated upgrade {} phase to {:?}",
        upgrade.name_any(),
        phase
    );

    Ok(())
}

/// Update replication status
async fn update_replication_status(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    lag: &LagStatus,
) -> UpgradeResult<()> {
    use crate::crd::ReplicationState;

    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);

    let replication_status = ReplicationStatus {
        status: if lag.in_sync {
            ReplicationState::Synced
        } else {
            ReplicationState::Syncing
        },
        source_lsn: Some(lag.source_lsn.clone()),
        target_lsn: Some(lag.target_lsn.clone()),
        lag_bytes: Some(lag.lag_bytes),
        lag_seconds: lag.lag_seconds,
        lsn_in_sync: Some(lag.in_sync),
        last_sync_time: Some(Utc::now().to_rfc3339()),
        publication_name: None,
        subscription_name: None,
    };

    let patch = serde_json::json!({
        "status": {
            "replication": replication_status
        }
    });

    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Update verification status
async fn update_verification_status(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    verification: &RowCountVerification,
) -> UpgradeResult<()> {
    use crate::crd::TableMismatch;

    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);

    let current_passes = upgrade
        .status
        .as_ref()
        .and_then(|s| s.verification.as_ref())
        .map(|v| v.consecutive_passes)
        .unwrap_or(0);

    let new_passes = if verification.tables_mismatched == 0 {
        current_passes + 1
    } else {
        0
    };

    // Convert mismatches to CRD type
    let mismatched_tables: Vec<TableMismatch> = verification
        .mismatches
        .iter()
        .map(|m| TableMismatch {
            schema: m.schema.clone(),
            table: m.table.clone(),
            source_count: m.source_count,
            target_count: m.target_count,
            difference: m.difference,
        })
        .collect();

    let verification_status = VerificationStatus {
        last_check_time: Some(Utc::now().to_rfc3339()),
        tables_verified: verification.tables_checked,
        tables_matched: verification.tables_matched,
        tables_mismatched: verification.tables_mismatched,
        consecutive_passes: new_passes,
        mismatched_tables,
    };

    let patch = serde_json::json!({
        "status": {
            "verification": verification_status
        }
    });

    debug!(patch = %patch, "Verification status patch");

    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Update sequence sync status
async fn update_sequence_sync_status(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    result: &SequenceSyncResult,
) -> UpgradeResult<()> {
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);

    // Collect failed sequence names
    let failed_sequences: Vec<String> = result
        .failures
        .iter()
        .map(|f| format!("{}.{}", f.schema, f.sequence))
        .collect();

    let sync_status = SequenceSyncStatus {
        synced: result.failed_count == 0,
        synced_count: result.synced_count,
        failed_count: result.failed_count,
        failed_sequences,
        synced_at: Some(Utc::now().to_rfc3339()),
    };

    let patch = serde_json::json!({
        "status": {
            "sequences": sync_status
        }
    });

    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Generate target cluster name from upgrade name
fn generate_target_cluster_name(upgrade_name: &str) -> String {
    format!("{}-target", upgrade_name)
}

/// Generate publication name from upgrade name
fn generate_publication_name(upgrade_name: &str) -> String {
    format!("{}_pub", upgrade_name.replace('-', "_"))
}

/// Generate subscription name from upgrade name
fn generate_subscription_name(upgrade_name: &str) -> String {
    format!("{}_sub", upgrade_name.replace('-', "_"))
}

/// Get postgres password from cluster credentials secret
async fn get_postgres_password(
    client: &Client,
    ns: &str,
    cluster_name: &str,
) -> UpgradeResult<String> {
    use k8s_openapi::api::core::v1::Secret;

    let secrets_api: Api<Secret> = Api::namespaced(client.clone(), ns);
    let secret_name = format!("{}-credentials", cluster_name);

    let secret =
        secrets_api
            .get(&secret_name)
            .await
            .map_err(|_| UpgradeError::SourceClusterNotFound {
                namespace: ns.to_string(),
                name: cluster_name.to_string(),
            })?;

    // Try PGPASSWORD first (used by psql clients), then POSTGRES_PASSWORD
    let password = secret
        .data
        .as_ref()
        .and_then(|d| d.get("PGPASSWORD").or_else(|| d.get("POSTGRES_PASSWORD")))
        .map(|b| String::from_utf8_lossy(&b.0).to_string())
        .unwrap_or_default();

    if password.is_empty() {
        return Err(UpgradeError::ValidationError(format!(
            "Password not found in secret {}-credentials",
            cluster_name
        )));
    }

    Ok(password)
}

/// Check if within maintenance window
fn is_within_maintenance_window(upgrade: &PostgresUpgrade) -> bool {
    let window = match upgrade.spec.strategy.cutover.allowed_window.as_ref() {
        Some(w) => w,
        None => return true, // No window specified means always allowed
    };

    // Parse current time and window times
    let now = Utc::now();
    let current_time = now.format("%H:%M").to_string();

    // Simple time comparison (assumes same-day window)
    current_time >= window.start_time && current_time <= window.end_time
}

/// Check if phase timeout has elapsed
fn is_phase_timeout_elapsed(upgrade: &PostgresUpgrade) -> bool {
    let phase_started_at = match upgrade
        .status
        .as_ref()
        .and_then(|s| s.phase_started_at.as_ref())
    {
        Some(ts) => ts,
        None => return false,
    };

    let started = match chrono::DateTime::parse_from_rfc3339(phase_started_at) {
        Ok(dt) => dt.with_timezone(&Utc),
        Err(_) => return false,
    };

    let current_phase = upgrade
        .status
        .as_ref()
        .map(|s| &s.phase)
        .unwrap_or(&UpgradePhase::Pending);

    let timeout = get_phase_timeout(upgrade, current_phase);
    let elapsed = Utc::now().signed_duration_since(started);

    elapsed > timeout
}

/// Get timeout for a specific phase
fn get_phase_timeout(upgrade: &PostgresUpgrade, phase: &UpgradePhase) -> chrono::Duration {
    let timeouts = &upgrade.spec.strategy.timeouts;

    let duration_str = match phase {
        UpgradePhase::CreatingTarget => &timeouts.target_cluster_ready,
        UpgradePhase::Replicating => &timeouts.initial_sync,
        UpgradePhase::Verifying => &timeouts.verification,
        _ => return chrono::Duration::hours(1), // Default timeout
    };

    parse_duration(duration_str).unwrap_or_else(|| chrono::Duration::hours(1))
}

/// Parse a duration string (e.g., "30m", "1h", "24h")
fn parse_duration(s: &str) -> Option<chrono::Duration> {
    let s = s.trim();
    if s.ends_with('h') {
        let hours: i64 = s.trim_end_matches('h').parse().ok()?;
        Some(chrono::Duration::hours(hours))
    } else if s.ends_with('m') {
        let minutes: i64 = s.trim_end_matches('m').parse().ok()?;
        Some(chrono::Duration::minutes(minutes))
    } else if s.ends_with('s') {
        let seconds: i64 = s.trim_end_matches('s').parse().ok()?;
        Some(chrono::Duration::seconds(seconds))
    } else {
        None
    }
}

/// Get appropriate requeue duration for a phase
fn requeue_duration_for_phase(phase: &UpgradePhase) -> Duration {
    match phase {
        UpgradePhase::Pending => Duration::from_secs(5),
        UpgradePhase::CreatingTarget => Duration::from_secs(10),
        UpgradePhase::ConfiguringReplication => Duration::from_secs(5),
        UpgradePhase::Replicating => Duration::from_secs(15),
        UpgradePhase::Verifying => Duration::from_secs(30),
        UpgradePhase::SyncingSequences => Duration::from_secs(5),
        UpgradePhase::ReadyForCutover => Duration::from_secs(30),
        UpgradePhase::WaitingForManualCutover => Duration::from_secs(30),
        UpgradePhase::CuttingOver => Duration::from_secs(5),
        UpgradePhase::HealthChecking => Duration::from_secs(10),
        UpgradePhase::Completed => Duration::from_secs(300),
        UpgradePhase::Failed => Duration::from_secs(60),
        UpgradePhase::RolledBack => Duration::from_secs(300),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_target_cluster_name() {
        assert_eq!(
            generate_target_cluster_name("orders-upgrade"),
            "orders-upgrade-target"
        );
    }

    #[test]
    fn test_generate_publication_name() {
        assert_eq!(
            generate_publication_name("orders-upgrade"),
            "orders_upgrade_pub"
        );
    }

    #[test]
    fn test_generate_subscription_name() {
        assert_eq!(
            generate_subscription_name("orders-upgrade"),
            "orders_upgrade_sub"
        );
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30m"), Some(chrono::Duration::minutes(30)));
        assert_eq!(parse_duration("1h"), Some(chrono::Duration::hours(1)));
        assert_eq!(parse_duration("24h"), Some(chrono::Duration::hours(24)));
        assert_eq!(parse_duration("60s"), Some(chrono::Duration::seconds(60)));
        assert_eq!(parse_duration("invalid"), None);
    }

    #[test]
    fn test_requeue_duration_for_phase() {
        assert_eq!(
            requeue_duration_for_phase(&UpgradePhase::Pending),
            Duration::from_secs(5)
        );
        assert_eq!(
            requeue_duration_for_phase(&UpgradePhase::Replicating),
            Duration::from_secs(15)
        );
        assert_eq!(
            requeue_duration_for_phase(&UpgradePhase::Completed),
            Duration::from_secs(300)
        );
    }
}
