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

use jiff::{SignedDuration, Timestamp};
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::events::{EventType, Reporter};
use kube::{Client, ResourceExt};
use tracing::{debug, error, info, instrument, warn};

use crate::controller::cleanup::{cleanup_stuck_resource, is_namespace_not_found_error};
use crate::controller::conditions::{new_condition, set_status_condition, status as cond_status};
use crate::controller::events;
use crate::controller::finalizer::remove_operator_finalizer;
use crate::controller::upgrade_error::{UpgradeBackoffConfig, UpgradeError, UpgradeResult};
use crate::controller::upgrade_preflight;
use crate::controller::upgrade_state_machine::{
    UpgradeEvent, UpgradeStateMachine, UpgradeTransitionContext, UpgradeTransitionResult,
};
use crate::crd::{
    ClusterPhase, Condition, CutoverMode, PostgresCluster, PostgresClusterSpec, PostgresUpgrade,
    ReplicationStatus, SequenceSyncStatus, UpgradeLineageRef, UpgradePhase, VerificationStatus,
    condition_types,
};
use crate::resources::ddl_audit;
use crate::resources::postgres_client::PostgresConnection;
use crate::resources::replication::{
    self, LagStatus, ReplicationError, RowCountVerification, SequenceSyncResult,
};
use crate::resources::service::{self, ServiceSwitchError};

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
    reporter: Reporter,
}

impl UpgradeContext {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            reporter: events::reporter(),
        }
    }

    /// Publish a Normal event attached to the PostgresUpgrade.
    pub async fn publish_normal_event(
        &self,
        upgrade: &PostgresUpgrade,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        events::publish_event(
            &self.client,
            &self.reporter,
            upgrade,
            EventType::Normal,
            reason,
            action,
            note,
        )
        .await;
    }

    /// Publish a Warning event attached to the PostgresUpgrade.
    pub async fn publish_warning_event(
        &self,
        upgrade: &PostgresUpgrade,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        events::publish_event(
            &self.client,
            &self.reporter,
            upgrade,
            EventType::Warning,
            reason,
            action,
            note,
        )
        .await;
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
    let name = upgrade.name_any();

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

    // Check if source cluster exists - delete orphaned upgrades
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(&ns);

    let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);
    let source_lookup = clusters_api.get_opt(source_name).await;

    // Check if source is gone (either not found, or namespace itself is gone)
    let source_is_gone = match &source_lookup {
        Ok(None) => true,
        Err(e) if is_namespace_not_found_error(e) => true,
        _ => false,
    };

    if source_is_gone {
        // Source cluster no longer exists
        // Completed upgrades are kept as historical records (source was intentionally superseded)
        if current_phase == UpgradePhase::Completed {
            // Keep completed upgrades for historical tracking
        } else if current_phase == UpgradePhase::Pending {
            // For Pending phase, the upgrade never started - move to Failed with an error
            // This gives users visibility into why the upgrade failed rather than silently deleting
            info!(
                "Source cluster {}/{} not found, failing upgrade {}",
                source_ns,
                source_name,
                upgrade.name_any()
            );

            let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), &ns);
            let now = Timestamp::now().to_string();
            let err_msg = format!(
                "Upgrade failed: source cluster {}/{} does not exist",
                source_ns, source_name
            );
            let conditions = conditions_for_phase(&upgrade, UpgradePhase::Failed, Some(&err_msg));
            let patch = serde_json::json!({
                "status": {
                    "phase": UpgradePhase::Failed,
                    "phaseStartedAt": now,
                    "completedAt": now,
                    "lastError": format!("Source cluster {}/{} not found", source_ns, source_name),
                    "reason": "UpgradeFailed",
                    "message": err_msg,
                    "conditions": conditions,
                }
            });

            api.patch_status(
                &upgrade.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await?;

            ctx.publish_warning_event(
                &upgrade,
                "SourceClusterMissing",
                "Upgrade",
                Some(err_msg.clone()),
            )
            .await;

            return Ok(Action::await_change());
        } else {
            // For in-progress upgrades (past Pending), the source was deleted mid-upgrade
            // This is an orphaned upgrade that should be cleaned up
            info!(
                "Source cluster {}/{} not found, deleting orphaned upgrade {} (was in {:?} phase)",
                source_ns,
                source_name,
                upgrade.name_any(),
                current_phase
            );

            // Remove finalizer first to allow deletion
            if has_finalizer(&upgrade) {
                let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), &ns);
                match remove_operator_finalizer(
                    &api,
                    &upgrade.name_any(),
                    upgrade.metadata.finalizers.as_ref(),
                    UPGRADE_FINALIZER,
                )
                .await
                {
                    Ok(()) => {}
                    Err(e) if is_namespace_not_found_error(&e) => {
                        // Namespace is gone - use special cleanup procedure
                        cleanup_stuck_resource::<PostgresUpgrade>(
                            ctx.client.clone(),
                            &upgrade.name_any(),
                            &ns,
                            UPGRADE_FINALIZER,
                        )
                        .await
                        .map_err(UpgradeError::KubeError)?;
                        return Ok(Action::await_change());
                    }
                    Err(e) => return Err(UpgradeError::KubeError(e)),
                }
            }

            // Delete the orphaned upgrade
            let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), &ns);
            match api
                .delete(&upgrade.name_any(), &DeleteParams::default())
                .await
            {
                Ok(_) => {}
                Err(e) if is_namespace_not_found_error(&e) => {
                    // Namespace is gone - use special cleanup procedure
                    cleanup_stuck_resource::<PostgresUpgrade>(
                        ctx.client.clone(),
                        &upgrade.name_any(),
                        &ns,
                        UPGRADE_FINALIZER,
                    )
                    .await
                    .map_err(UpgradeError::KubeError)?;
                }
                Err(e) => {
                    warn!(
                        "Failed to delete orphaned upgrade {}: {}",
                        upgrade.name_any(),
                        e
                    );
                }
            }

            return Ok(Action::await_change());
        }
    }

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

                // Emit a Kubernetes Event for every phase transition. Use
                // Warning for terminal-failure phases so it stands out in
                // `kubectl describe`.
                let event_type_warn = matches!(to, UpgradePhase::Failed | UpgradePhase::RolledBack);
                let note = Some(format!("{current_phase} -> {to}: {description}"));
                if event_type_warn {
                    ctx.publish_warning_event(&upgrade, "PhaseTransition", "Upgrade", note)
                        .await;
                } else {
                    ctx.publish_normal_event(&upgrade, "PhaseTransition", "Upgrade", note)
                        .await;
                }

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
    match &result {
        Ok(_) => {
            // Get the current phase after reconciliation (may have changed)
            let upgrades: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), &ns);
            let final_phase = upgrades
                .get_opt(&name)
                .await
                .ok()
                .flatten()
                .and_then(|u| u.status.map(|s| s.phase))
                .unwrap_or(current_phase);
            info!(
                "Reconciliation completed successfully in {:.3}s (phase: {:?})",
                duration_secs, final_phase
            );
        }
        Err(e) => error!("Reconciliation failed after {:.3}s: {}", duration_secs, e),
    }

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

            // Backup safety gate: auto-cutover requires a recent successful
            // backup on the source cluster. Manual cutover is permitted to
            // bypass this check (the user takes responsibility). The
            // requirement window is configured via
            // `spec.strategy.preChecks.requireBackupWithin` (default "1h").
            let last_backup_time = cluster
                .status
                .as_ref()
                .and_then(|s| s.backup.as_ref())
                .and_then(|b| b.last_backup_time.as_deref());
            let max_age = &upgrade.spec.strategy.pre_checks.require_backup_within;
            tc.backup_requirement_met =
                is_backup_recent_enough(last_backup_time, max_age, Timestamp::now());
            if !tc.backup_requirement_met {
                debug!(
                    "Source cluster {}/{} backup is stale or missing (last_backup_time={:?}, max_age={}); \
                     auto-cutover will be blocked",
                    source_ns, source_name, last_backup_time, max_age
                );
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
            tc.ddl_observed = repl.ddl_count.unwrap_or(0) > 0;
        }

        if let Some(verif) = &status.verification {
            tc.verification_passes = verif.consecutive_passes;
            tc.row_count_mismatches = verif.tables_mismatched;
        }

        if let Some(seq) = &status.sequences {
            tc.sequences_synced = seq.synced;
        }
    }

    // Get configuration
    tc.cutover_mode = upgrade.spec.strategy.cutover.mode;
    tc.ddl_acknowledged = upgrade.spec.strategy.acknowledge_ddl;
    tc.source_read_only = upgrade
        .status
        .as_ref()
        .and_then(|s| s.source_read_only_at.as_ref())
        .is_some();

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

/// Map an observed target cluster phase to the upgrade event for the
/// `HealthChecking` phase.
///
/// Returns `Some(HealthCheckPassed)` only when the target cluster has reached
/// `Running`. When the cluster is missing (`None`) or in any other phase, the
/// reconciler should keep polling, so this returns `None`.
///
/// Extracting this from [`determine_event_for_phase`] lets us unit-test the
/// decision without standing up a Kubernetes client.
fn health_check_event_for_target_phase(target_phase: Option<ClusterPhase>) -> Option<UpgradeEvent> {
    match target_phase {
        Some(ClusterPhase::Running) => Some(UpgradeEvent::HealthCheckPassed),
        _ => None,
    }
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
            // Emit HealthCheckPassed when the target cluster reaches Running so the
            // state machine drives the (HealthChecking -> Completed) transition,
            // which runs cleanup_replication. Keep polling otherwise.
            let target_name = generate_target_cluster_name(&upgrade.name_any());
            let clusters_api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);
            let target_phase = clusters_api.get_opt(&target_name).await?.map(|cluster| {
                cluster
                    .status
                    .as_ref()
                    .map(|s| s.phase)
                    .unwrap_or(ClusterPhase::Pending)
            });

            Ok(health_check_event_for_target_phase(target_phase))
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
    // Whenever we enter a terminal phase, release the in-progress lock on
    // the source cluster so the cluster reconciler resumes managing its
    // Services. We do this before the per-transition work so cleanup runs
    // even if the source happens to be inaccessible.
    if matches!(
        to,
        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack
    ) {
        clear_source_upgrade_in_progress(upgrade, ctx, ns).await;
        uninstall_source_ddl_audit(upgrade, ctx, ns).await;
    }

    match (from, to) {
        (UpgradePhase::Pending, UpgradePhase::CreatingTarget) => {
            // Replication-compatibility preflight runs *before* we touch
            // anything on the source cluster. A failure is permanent —
            // the FSM drives the upgrade to Failed; the user fixes the
            // source and creates a new PostgresUpgrade to retry.
            run_preflight_or_fail(upgrade, ctx, ns).await?;
            mark_source_upgrade_in_progress(upgrade, ctx, ns).await?;
            create_target_cluster(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::CreatingTarget, UpgradePhase::ConfiguringReplication) => {
            setup_replication(upgrade, ctx, ns).await?;
        }

        (UpgradePhase::Verifying, UpgradePhase::SyncingSequences) => {
            // Source is already read-only at this point — `take_source_read_only`
            // ran during Verifying monitoring once row counts converged and
            // LSN lag hit zero. `verification_complete()` (which gates this
            // transition) requires both, so re-asserting here would be
            // redundant.
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
            poll_ddl_audit_status(upgrade, ctx, ns).await?;
        }

        UpgradePhase::Verifying => {
            // Refresh replication lag so the LSN-distance gate decides
            // against fresh data, not whatever was last recorded while we
            // were still in Replicating.
            let lag_status = get_replication_lag(upgrade, ctx, ns).await?;
            update_replication_status(upgrade, ctx, ns, &lag_status).await?;

            // Run row count verification
            let verification = run_verification(upgrade, ctx, ns).await?;
            debug!(
                tables_checked = verification.tables_checked,
                tables_matched = verification.tables_matched,
                tables_mismatched = verification.tables_mismatched,
                "Row count verification result"
            );
            update_verification_status(upgrade, ctx, ns, &verification).await?;
            poll_ddl_audit_status(upgrade, ctx, ns).await?;

            // Once row counts have converged and replication lag is zero,
            // promote the source to read-only. This is what closes the
            // last-mile race: any writes that would have arrived between
            // here and `SyncingSequences` are now refused. The
            // `verification_complete()` guard on the FSM transition then
            // waits for a *re-checked* zero lag (the next monitoring tick
            // will refresh and confirm) before firing.
            promote_source_to_read_only_if_ready(upgrade, ctx, ns, &lag_status, &verification)
                .await?;
        }

        _ => {
            // No monitoring needed for other phases.
            // HealthChecking completion is routed through determine_event_for_phase,
            // which emits HealthCheckPassed when the target cluster reaches Running.
            // The (HealthChecking -> Completed) transition action runs
            // cleanup_replication, so completion must not be triggered here.
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

    // Purge idle-in-transaction sessions on the source so the slot
    // creation triggered by `CREATE SUBSCRIPTION` below can take a
    // consistent snapshot. Without this, slot creation can hang silently
    // on busy clusters — the most common opaque ConfiguringReplication
    // stall per Wiz's documented Aurora playbook.
    purge_source_idle_transactions(upgrade, ctx, source_ns, source_name, &source_conn).await?;

    // Install the DDL audit event trigger on the source so any DDL run
    // during the replication window is counted. The reconciler polls
    // `count_ddl_events` periodically and refuses to cut over if the
    // count is non-zero (unless `spec.strategy.acknowledgeDDL` is set).
    if let Err(e) = ddl_audit::install_ddl_audit(&source_conn).await {
        warn!(
            "Failed to install DDL audit on source {}/{}: {}; cutover will not be \
             protected against DDL drift",
            source_ns, source_name, e
        );
    } else {
        ctx.publish_normal_event(
            upgrade,
            "DDLAuditInstalled",
            "InstallDDLAudit",
            Some(format!(
                "Installed DDL audit on source {source_ns}/{source_name}; any DDL during \
                 the replication window will be counted and surfaced on \
                 status.replication.ddlCount"
            )),
        )
        .await;
    }

    // Get source cluster service host
    let source_host = format!("{}-primary.{}.svc", source_name, source_ns);

    // Get source credentials (simplified - in production, get from secret)
    let source_password = get_postgres_password(&ctx.client, source_ns, source_name).await?;

    // Connect to target cluster and create subscription
    let target_conn = PostgresConnection::connect_primary(&ctx.client, ns, &target_name).await?;
    setup_subscription_with_consistent_snapshot_retry(
        upgrade,
        ctx,
        source_ns,
        source_name,
        &source_conn,
        &target_conn,
        &sub_name,
        &source_host,
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

    // Refresh statistics on both clusters for accurate row counts
    // pg_stat_user_tables.n_live_tup is an estimate that needs ANALYZE to update
    replication::refresh_statistics(&source_conn)
        .await
        .map_err(|e| UpgradeError::SqlError(format!("Failed to refresh source stats: {}", e)))?;
    replication::refresh_statistics(&target_conn)
        .await
        .map_err(|e| UpgradeError::SqlError(format!("Failed to refresh target stats: {}", e)))?;

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

/// Drain active connections from the source primary before flipping
/// service selectors. The source is expected to already be read-only at
/// this point (set during the `Verifying` phase once row counts converged
/// and LSN lag hit zero), so this blocks until in-flight read transactions
/// complete or the configured `drain_connections_timeout` elapses.
///
/// On timeout, the source is restored to read-write and the cutover is
/// failed; otherwise a `ConnectionsDrained` condition and Normal Event are
/// recorded for operator visibility.
async fn drain_source_connections(
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

    let timeout_secs = parse_duration(&upgrade.spec.strategy.pre_checks.drain_connections_timeout)
        .map(|d| d.as_secs().max(0).try_into().unwrap_or(u64::MAX))
        .unwrap_or(300u64);

    let source_conn =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;

    info!(
        "Draining active connections on source {}/{} (timeout {}s)",
        source_ns, source_name, timeout_secs
    );

    let drained = replication::wait_for_connections_drain(&source_conn, timeout_secs, 2).await?;

    if !drained {
        warn!(
            "Connection drain timed out on source {}/{} after {}s; restoring source to read-write",
            source_ns, source_name, timeout_secs
        );

        if let Err(e) = replication::set_source_readwrite(&source_conn).await {
            // Log only — the cutover is going to fail regardless, and the
            // ConnectionDrainTimeout error carries the primary signal.
            error!(
                "Failed to restore source {}/{} to read-write after drain timeout: {}",
                source_ns, source_name, e
            );
        }

        ctx.publish_warning_event(
            upgrade,
            "ConnectionDrainTimeout",
            "DrainConnections",
            Some(format!(
                "Active connections remained on source {source_ns}/{source_name} after {timeout_secs}s; cutover aborted"
            )),
        )
        .await;

        return Err(UpgradeError::ConnectionDrainTimeout(format!(
            "{source_ns}/{source_name}: timed out after {timeout_secs}s"
        )));
    }

    record_connections_drained(upgrade, ctx, ns).await?;

    ctx.publish_normal_event(
        upgrade,
        "ConnectionsDrained",
        "DrainConnections",
        Some(format!(
            "All active connections drained from source {source_ns}/{source_name}; proceeding with service switch"
        )),
    )
    .await;

    Ok(())
}

/// Patch a `ConnectionsDrained=True` condition onto the upgrade status.
async fn record_connections_drained(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    let mut conditions = upgrade
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();

    set_status_condition(
        &mut conditions,
        new_condition(
            condition_types::CONNECTIONS_DRAINED,
            cond_status::TRUE,
            "ConnectionsDrained",
            "Active connections drained from source before service switch",
            upgrade.metadata.generation,
        ),
    );

    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "status": { "conditions": conditions }
    });
    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Execute the cutover by switching services from the source to the target cluster.
///
/// This atomically updates the primary and replica `Service` selectors to route
/// traffic to the target cluster. The target `PostgresCluster` is fetched first
/// so its `metadata` can be used to refresh the owner references on the
/// switched services.
///
/// On success, the upgrade status is patched with `cutoverStartedAt` and a
/// message reflecting the actual switch result. Status is only patched after
/// the switch succeeds so a failed switch does not falsely record a cutover
/// timestamp.
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

    // Fetch the target cluster so we can attach correct owner references when
    // patching the services. If it does not exist, the upgrade cannot proceed.
    let clusters: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);
    let target_cluster = clusters.get(&target_name).await.map_err(|e| match e {
        kube::Error::Api(ref api_err) if api_err.code == 404 => {
            UpgradeError::TargetClusterNotFound {
                namespace: ns.to_string(),
                name: target_name.clone(),
            }
        }
        other => UpgradeError::KubeError(other),
    })?;

    // Drain active connections on the source before flipping service
    // selectors. The source was already set read-only during the Verifying
    // phase (once row counts converged and LSN lag hit zero), so this only
    // blocks until in-flight read transactions complete. On timeout we
    // restore the source to read-write and fail the cutover; the FSM will
    // retry or move the upgrade to Failed.
    drain_source_connections(upgrade, ctx, ns).await?;

    // Perform the actual service switch.
    let switch_result = service::switch_services_to_target(
        &ctx.client,
        ns,
        source_name,
        &target_name,
        &target_cluster,
    )
    .await
    .map_err(map_service_switch_error)?;

    info!(
        "Service switch complete for upgrade {}: primary={} replica={} switched_at={}",
        upgrade.name_any(),
        switch_result.primary_service,
        switch_result.replica_service,
        switch_result.switched_at
    );

    // Patch status only after the switch succeeded, so the cutoverStartedAt
    // timestamp always corresponds to a real switch.
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "status": {
            "cutoverStartedAt": switch_result.switched_at.to_string(),
            "message": format!(
                "Services switched from {} to {} (primary={}, replica={})",
                source_name,
                target_name,
                switch_result.primary_service,
                switch_result.replica_service
            )
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

/// Map a `ServiceSwitchError` from the service module into an `UpgradeError`.
///
/// Patch failures bubble up the underlying `kube::Error` so existing retry
/// logic for transient API errors still applies. The remaining variants are
/// classified as transient service-switch failures.
fn map_service_switch_error(err: ServiceSwitchError) -> UpgradeError {
    match err {
        ServiceSwitchError::PatchFailed { name, source } => {
            // Preserve the kube error so transient API failures retry naturally,
            // while logging which service failed for operator visibility.
            warn!(
                "Failed to patch service {} during cutover: {}",
                name, source
            );
            UpgradeError::KubeError(source)
        }
        ServiceSwitchError::NotFound(name) => {
            UpgradeError::ServiceSwitchFailed(format!("service not found: {name}"))
        }
        ServiceSwitchError::InvalidConfig(msg) => {
            UpgradeError::ServiceSwitchFailed(format!("invalid service configuration: {msg}"))
        }
    }
}

/// Attempt `CREATE SUBSCRIPTION` with bounded retries when the publisher
/// fails to take a consistent snapshot. A new idle-in-transaction session
/// could appear in the window between our purge and the slot creation; if
/// it does, we re-purge and retry up to [`MAX_CONSISTENT_SNAPSHOT_RETRIES`]
/// times with bounded backoff.
#[allow(clippy::too_many_arguments)]
async fn setup_subscription_with_consistent_snapshot_retry(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    source_ns: &str,
    source_name: &str,
    source_conn: &PostgresConnection,
    target_conn: &PostgresConnection,
    sub_name: &str,
    source_host: &str,
    pub_name: &str,
    source_password: &str,
) -> UpgradeResult<()> {
    const MAX_CONSISTENT_SNAPSHOT_RETRIES: usize = 3;

    let mut attempt: usize = 0;
    loop {
        match replication::setup_subscription(
            target_conn,
            sub_name,
            source_host,
            5432,
            pub_name,
            source_password,
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                let retryable = msg.to_ascii_lowercase().contains("consistent snapshot");
                if !retryable || attempt >= MAX_CONSISTENT_SNAPSHOT_RETRIES {
                    return Err(UpgradeError::ReplicationError(e));
                }
                attempt += 1;
                let backoff = Duration::from_secs(2u64.saturating_pow(attempt as u32));
                warn!(
                    "CREATE SUBSCRIPTION failed to take a consistent snapshot (attempt {}/{}): {}; \
                     re-purging idle-in-transaction sessions and retrying after {:?}",
                    attempt, MAX_CONSISTENT_SNAPSHOT_RETRIES, msg, backoff
                );
                purge_source_idle_transactions(upgrade, ctx, source_ns, source_name, source_conn)
                    .await?;
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/// Purge long-running idle-in-transaction sessions on the source before
/// the publisher's slot gets created (via `CREATE SUBSCRIPTION` on the
/// target). Honours `spec.strategy.preChecks.terminate_idle_transactions`
/// — if the user opted out, we still surface the offending sessions so
/// they can clean them up before slot creation hangs.
async fn purge_source_idle_transactions(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    source_ns: &str,
    source_name: &str,
    source_conn: &PostgresConnection,
) -> UpgradeResult<()> {
    let pre_checks = &upgrade.spec.strategy.pre_checks;
    let threshold_secs = parse_duration(&pre_checks.idle_transaction_threshold)
        .map(|d| d.as_secs().max(0))
        .unwrap_or(300);

    let sessions = replication::find_idle_in_transaction(source_conn, threshold_secs).await?;

    if sessions.is_empty() {
        debug!(
            "No idle-in-transaction sessions older than {}s on source {}/{}",
            threshold_secs, source_ns, source_name
        );
        return Ok(());
    }

    let pids: Vec<i32> = sessions.iter().map(|s| s.pid).collect();
    let oldest_age = sessions
        .iter()
        .map(|s| s.state_change_age_secs)
        .max()
        .unwrap_or(0);

    if !pre_checks.terminate_idle_transactions {
        let summary = format!(
            "Found {} idle-in-transaction session(s) on source {}/{} older than {}s \
             (oldest {}s), but spec.strategy.preChecks.terminateIdleTransactions=false. \
             Slot creation may hang until these sessions are cleaned up manually.",
            pids.len(),
            source_ns,
            source_name,
            threshold_secs,
            oldest_age
        );
        warn!("{}", summary);
        ctx.publish_warning_event(
            upgrade,
            "IdleTransactionsNotPurged",
            "PurgeIdleTransactions",
            Some(summary),
        )
        .await;
        return Ok(());
    }

    info!(
        "Terminating {} idle-in-transaction session(s) on source {}/{} (oldest {}s) \
         before slot creation",
        pids.len(),
        source_ns,
        source_name,
        oldest_age
    );

    let terminated = replication::terminate_sessions(source_conn, &pids).await?;

    ctx.publish_normal_event(
        upgrade,
        "IdleTransactionsPurged",
        "PurgeIdleTransactions",
        Some(format!(
            "Terminated {terminated}/{} idle-in-transaction session(s) on source {source_ns}/{source_name} \
             (oldest was {oldest_age}s old) so the publication slot can take a consistent snapshot",
            pids.len()
        )),
    )
    .await;

    Ok(())
}

/// Read the storage size string that the target cluster will be created
/// with. Mirrors the storage portion of [`build_target_spec`]: today the
/// target inherits source storage verbatim because
/// `target_cluster_overrides` only touches `resources` and `replicas`.
///
/// Returns `None` only if the source cluster isn't reachable; callers
/// pass the empty string in that case so the parser surfaces a clear
/// preflight failure instead of skipping the check silently.
async fn fetch_target_storage_size(
    ctx: &UpgradeContext,
    source_ns: &str,
    source_name: &str,
) -> Option<String> {
    let clusters: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);
    match clusters.get_opt(source_name).await {
        Ok(Some(cluster)) => Some(cluster.spec.storage.size),
        Ok(None) => None,
        Err(e) => {
            debug!(
                "fetch_target_storage_size: failed to read source cluster {}/{}: {}",
                source_ns, source_name, e
            );
            None
        }
    }
}

/// If row counts have converged and replication lag is zero, take the
/// source primary read-only and record the timestamp on the upgrade
/// status. Idempotent — re-running once `source_read_only_at` is set is
/// a no-op (we don't want to flap the source between read-only and
/// read-write).
///
/// This closes the "last-mile" race: PostgreSQL logical replication's
/// `pg_current_wal_lsn() - confirmed_flush_lsn` only converges to zero
/// at a moment in time, not durably. Without freezing source writes at
/// that moment, by the time we begin sequence sync the source has
/// already advanced and the cutover would lose data.
async fn promote_source_to_read_only_if_ready(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    lag: &LagStatus,
    verification: &RowCountVerification,
) -> UpgradeResult<()> {
    // Already done — don't re-issue ALTER SYSTEM.
    if upgrade
        .status
        .as_ref()
        .and_then(|s| s.source_read_only_at.as_ref())
        .is_some()
    {
        return Ok(());
    }

    // `consecutive_passes` lives on the cumulative VerificationStatus
    // (incremented by `update_verification_status`), not on this single
    // run's result. Use the most recent status snapshot since it's been
    // patched just before this helper is called.
    let required_passes = upgrade.spec.strategy.pre_checks.min_verification_passes;
    let consecutive_passes = upgrade
        .status
        .as_ref()
        .and_then(|s| s.verification.as_ref())
        .map(|v| v.consecutive_passes)
        .unwrap_or(0);
    let row_counts_ok =
        consecutive_passes >= required_passes && verification.tables_mismatched == 0;
    if !row_counts_ok || !lag.in_sync {
        debug!(
            "Not ready to promote source to read-only yet: passes={} required={} \
             mismatches={} lag_bytes={} in_sync={}",
            consecutive_passes,
            required_passes,
            verification.tables_mismatched,
            lag.lag_bytes,
            lag.in_sync
        );
        return Ok(());
    }

    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    info!(
        "Verification complete and lag at zero; promoting source {}/{} to read-only \
         (source_lsn={}, target_lsn={})",
        source_ns, source_name, lag.source_lsn, lag.target_lsn
    );

    let source_conn =
        PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await?;
    replication::set_source_readonly(&source_conn).await?;

    let now = Timestamp::now().to_string();
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "status": { "sourceReadOnlyAt": now }
    });
    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    ctx.publish_normal_event(
        upgrade,
        "SourceReadOnly",
        "PromoteSourceReadOnly",
        Some(format!(
            "Source {source_ns}/{source_name} promoted to read-only; sequence sync will \
             proceed once the next monitoring tick re-confirms zero LSN distance"
        )),
    )
    .await;

    Ok(())
}

/// Poll the source's DDL audit table for the current event count, patch
/// the result onto `status.replication.ddlCount` and the `DDLObserved`
/// condition, and emit a Warning `DDLDetected` Event the first time the
/// count flips from 0 → non-zero.
///
/// Errors at every level are logged but not propagated: the audit is a
/// safety net, and we don't want a transient failure to read the count
/// to cascade into reconciliation failures that would themselves block
/// the upgrade.
async fn poll_ddl_audit_status(
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

    let source_conn =
        match PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await {
            Ok(c) => c,
            Err(e) => {
                debug!(
                    "DDL audit poll: source {}/{} unreachable, deferring to next reconcile: {}",
                    source_ns, source_name, e
                );
                return Ok(());
            }
        };

    let count = match ddl_audit::count_ddl_events(&source_conn).await {
        Ok(c) => c,
        Err(e) => {
            warn!(
                "DDL audit poll: failed to read count from {}/{}: {}",
                source_ns, source_name, e
            );
            return Ok(());
        }
    };

    let previous = upgrade
        .status
        .as_ref()
        .and_then(|s| s.replication.as_ref())
        .and_then(|r| r.ddl_count)
        .unwrap_or(0);

    // First detection: emit a Warning Event so it surfaces in the live
    // event stream as well as the condition message.
    if previous == 0 && count > 0 {
        let samples = ddl_audit::recent_ddl_samples(&source_conn, 5)
            .await
            .unwrap_or_default();
        let summary = ddl_audit::render_ddl_sample_message(count, &samples);
        ctx.publish_warning_event(upgrade, "DDLDetected", "PollDDLAudit", Some(summary))
            .await;
    }

    patch_ddl_audit_status(upgrade, ctx, ns, &source_conn, count).await
}

/// Patch `status.replication.ddlCount` and the `DDLObserved` condition.
async fn patch_ddl_audit_status(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    source_conn: &PostgresConnection,
    count: i64,
) -> UpgradeResult<()> {
    let samples = if count > 0 {
        ddl_audit::recent_ddl_samples(source_conn, 5)
            .await
            .unwrap_or_default()
    } else {
        Vec::new()
    };

    let mut conditions = upgrade
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();

    let (status_value, reason) = if count > 0 {
        (cond_status::TRUE, "DDLObserved")
    } else {
        (cond_status::FALSE, "NoDDLObserved")
    };
    let message = ddl_audit::render_ddl_sample_message(count, &samples);

    set_status_condition(
        &mut conditions,
        new_condition(
            condition_types::DDL_OBSERVED,
            status_value,
            reason,
            &message,
            upgrade.metadata.generation,
        ),
    );

    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "status": {
            "conditions": conditions,
            "replication": {
                "ddlCount": count,
            }
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

/// Drop the DDL audit objects from the source. Errors are logged only —
/// the user can clean up manually if needed (the SQL identifiers are
/// fixed and documented in `docs/upgrades.md`).
async fn uninstall_source_ddl_audit(upgrade: &PostgresUpgrade, ctx: &UpgradeContext, ns: &str) {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    let source_conn =
        match PostgresConnection::connect_primary(&ctx.client, source_ns, source_name).await {
            Ok(c) => c,
            Err(e) => {
                debug!(
                    "uninstall_source_ddl_audit: source {}/{} unreachable: {}",
                    source_ns, source_name, e
                );
                return;
            }
        };

    if let Err(e) = ddl_audit::uninstall_ddl_audit(&source_conn).await {
        warn!(
            "Failed to uninstall DDL audit from source {}/{}: {}; \
             you may need to manually `DROP EVENT TRIGGER {} CASCADE`",
            source_ns,
            source_name,
            e,
            ddl_audit::AUDIT_TRIGGER
        );
    }
}

/// Run replication-compatibility preflight checks against the source
/// cluster. On failure: patch a `PreflightPassed=False` condition with the
/// concrete failure messages, emit a Warning Event, and return a permanent
/// [`UpgradeError::PreflightCheckFailed`] which the reconciler will route
/// to the `Failed` phase. On success: patch `PreflightPassed=True` and
/// emit a Normal Event.
async fn run_preflight_or_fail(
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

    // Look up the source cluster's storage spec so we can compare it
    // against the source's actual data size (U13). Target inherits this
    // verbatim today (see `build_target_spec`); a future override would
    // be applied here. If the source is missing the field, fall back to
    // an empty string so the parser fails the check loudly rather than
    // silently skipping it.
    let target_storage_size = fetch_target_storage_size(ctx, source_ns, source_name)
        .await
        .unwrap_or_default();

    let outcome = match upgrade_preflight::run_preflight_checks(
        &ctx.client,
        source_ns,
        source_name,
        &target_storage_size,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(e) => {
            // Couldn't *run* the preflight (connectivity, query error).
            // Surface as transient — the FSM will retry rather than fail
            // the upgrade just because the source primary was momentarily
            // unreachable.
            warn!(
                "Preflight run failed against source {}/{}: {}; will retry",
                source_ns, source_name, e
            );
            return Err(UpgradeError::TransientError(format!(
                "preflight unavailable against {source_ns}/{source_name}: {e}"
            )));
        }
    };

    if outcome.passed() {
        record_preflight_passed(upgrade, ctx, ns).await?;
        ctx.publish_normal_event(
            upgrade,
            "PreflightPassed",
            "RunPreflight",
            Some(format!(
                "Replication-compatibility preflight passed against source {source_ns}/{source_name}"
            )),
        )
        .await;
        return Ok(());
    }

    let failures = outcome.failure_messages();
    let summary = outcome.summary();

    record_preflight_failed(upgrade, ctx, ns, &summary, &failures).await?;

    ctx.publish_warning_event(
        upgrade,
        "PreflightFailed",
        "RunPreflight",
        Some(format!("{summary}: {}", failures.join("; "))),
    )
    .await;

    Err(UpgradeError::PreflightCheckFailed { summary, failures })
}

/// Patch a `PreflightPassed=True` condition onto the upgrade status.
async fn record_preflight_passed(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) -> UpgradeResult<()> {
    patch_preflight_condition(
        upgrade,
        ctx,
        ns,
        cond_status::TRUE,
        "PreflightPassed",
        "Replication-compatibility preflight passed",
    )
    .await
}

/// Patch a `PreflightPassed=False` condition with the structured failure
/// messages. Surfaces the concrete failures so `kubectl describe pgu`
/// gives the user actionable text without making them grep logs.
async fn record_preflight_failed(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    summary: &str,
    failures: &[String],
) -> UpgradeResult<()> {
    // Truncate the joined message so we don't exceed Kubernetes' 32 KiB
    // condition-message limit on pathological inputs (thousands of bad
    // tables). 4 KiB is plenty for human consumption.
    const MAX_MESSAGE_BYTES: usize = 4096;
    let mut message = format!("{summary}: {}", failures.join("; "));
    if message.len() > MAX_MESSAGE_BYTES {
        message.truncate(MAX_MESSAGE_BYTES);
        message.push_str(" […truncated]");
    }

    patch_preflight_condition(
        upgrade,
        ctx,
        ns,
        cond_status::FALSE,
        "PreflightFailed",
        &message,
    )
    .await
}

async fn patch_preflight_condition(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    status: &str,
    reason: &str,
    message: &str,
) -> UpgradeResult<()> {
    let mut conditions = upgrade
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();

    set_status_condition(
        &mut conditions,
        new_condition(
            condition_types::PREFLIGHT_PASSED,
            status,
            reason,
            message,
            upgrade.metadata.generation,
        ),
    );

    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let patch = serde_json::json!({
        "status": { "conditions": conditions }
    });
    api.patch_status(
        &upgrade.name_any(),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;

    Ok(())
}

/// Set the `upgrade-in-progress` annotation on the source `PostgresCluster`.
///
/// The cluster reconciler observes this annotation and skips Service
/// reconciliation while it's present, preventing it from reverting the
/// service-selector flip performed by `execute_cutover`. Called on the
/// `Pending → CreatingTarget` transition.
async fn mark_source_upgrade_in_progress(
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

    let clusters: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                condition_annotation_key(): upgrade.name_any(),
            }
        }
    });

    match clusters
        .patch(source_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(_) => {
            info!(
                "Annotated source cluster {}/{} with upgrade-in-progress={}",
                source_ns,
                source_name,
                upgrade.name_any()
            );
            Ok(())
        }
        Err(kube::Error::Api(ref api_err)) if api_err.code == 404 => {
            // Source cluster missing — the upgrade will fail elsewhere with
            // a clearer error. Don't block on the annotation.
            warn!(
                "Source cluster {}/{} not found while annotating upgrade-in-progress",
                source_ns, source_name
            );
            Ok(())
        }
        Err(e) => Err(UpgradeError::KubeError(e)),
    }
}

/// Remove the `upgrade-in-progress` annotation from the source
/// `PostgresCluster`. Called when the upgrade reaches a terminal phase
/// (`Completed`, `Failed`, `RolledBack`) or is being deleted, so the
/// cluster reconciler resumes managing Services normally.
async fn clear_source_upgrade_in_progress(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
) {
    let source_name = &upgrade.spec.source_cluster.name;
    let source_ns = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(ns);

    let clusters: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), source_ns);
    let patch = serde_json::json!({
        "metadata": {
            "annotations": {
                condition_annotation_key(): serde_json::Value::Null,
            }
        }
    });

    match clusters
        .patch(source_name, &PatchParams::default(), &Patch::Merge(&patch))
        .await
    {
        Ok(_) => {
            debug!(
                "Cleared upgrade-in-progress annotation on source cluster {}/{}",
                source_ns, source_name
            );
        }
        Err(kube::Error::Api(ref api_err)) if api_err.code == 404 => {
            // Source already gone — nothing to clean up.
        }
        Err(e) => {
            warn!(
                "Failed to clear upgrade-in-progress annotation on source {}/{}: {}",
                source_ns, source_name, e
            );
        }
    }
}

/// The annotation key for marking an in-progress upgrade on the source
/// `PostgresCluster`. Wrapped in a function to keep the static borrow
/// inside the JSON literals above.
fn condition_annotation_key() -> &'static str {
    crate::crd::annotations::UPGRADE_IN_PROGRESS
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
        created_at: Some(Timestamp::now().to_string()),
    };

    let patch = serde_json::json!({
        "status": {
            "phase": ClusterPhase::Superseded,
            "successor": successor
        }
    });

    clusters_api
        .patch_status(source_name, &PatchParams::default(), &Patch::Merge(&patch))
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
        created_at: Some(Timestamp::now().to_string()),
    };

    let patch = serde_json::json!({
        "status": {
            "origin": origin
        }
    });

    clusters_api
        .patch_status(&target_name, &PatchParams::default(), &Patch::Merge(&patch))
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

    // Refuse rollback from cutover or post-cutover phases. Once the service
    // selectors have flipped to the target, the new primary may have
    // accepted writes that the source does not have. Rolling back at that
    // point would silently drop data; recovery requires PITR from a
    // pre-upgrade backup.
    if !current_phase.can_rollback() {
        warn!(
            "Rollback refused for upgrade {} in phase {:?}: rollback is not supported \
             after CuttingOver begins. See docs/upgrades.md for post-cutover recovery.",
            upgrade.name_any(),
            current_phase
        );

        ctx.publish_warning_event(
            upgrade,
            "RollbackNotAllowed",
            "Rollback",
            Some(format!(
                "Rollback is not supported in phase {current_phase:?}: cutover has begun. \
                 See docs/upgrades.md for post-cutover recovery."
            )),
        )
        .await;

        // Clear the rollback annotation so subsequent reconciles do not
        // re-enter this branch and emit duplicate events forever. Without
        // this clear, the rollback request would loop indefinitely because
        // returning an error never advances the FSM and the annotation
        // persists across reconciles.
        let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
        let patch = serde_json::json!({
            "metadata": {
                "annotations": {
                    ROLLBACK_ANNOTATION: Option::<String>::None,
                }
            }
        });
        if let Err(e) = api
            .patch(
                &upgrade.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
        {
            warn!(
                "Failed to clear rollback annotation on upgrade {}: {}",
                upgrade.name_any(),
                e
            );
        }

        return Ok(Action::requeue(Duration::from_secs(60)));
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

    // Release the in-progress lock on the source cluster before updating
    // phase, so the cluster reconciler resumes Service reconciliation as
    // soon as RolledBack is observed.
    clear_source_upgrade_in_progress(upgrade, ctx, ns).await;
    uninstall_source_ddl_audit(upgrade, ctx, ns).await;

    // Clear sourceReadOnlyAt so status reflects that the source is back to
    // read-write. The field is documented as a current-state signal: its
    // presence means the source is no longer accepting writes. Leaving it
    // set after rollback would lie to FSM guards and external consumers.
    {
        let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
        let patch = serde_json::json!({
            "status": { "sourceReadOnlyAt": Option::<String>::None }
        });
        if let Err(e) = api
            .patch_status(
                &upgrade.name_any(),
                &PatchParams::default(),
                &Patch::Merge(&patch),
            )
            .await
        {
            warn!(
                "Failed to clear sourceReadOnlyAt for upgrade {}: {}",
                upgrade.name_any(),
                e
            );
        }
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

    // Always clear the in-progress annotation on deletion, regardless of
    // phase, so the source cluster reconciler isn't left in suspended
    // Service-reconcile mode after the upgrade resource is gone.
    clear_source_upgrade_in_progress(upgrade, ctx, ns).await;
    uninstall_source_ddl_audit(upgrade, ctx, ns).await;

    // Remove finalizer
    if has_finalizer(upgrade) {
        let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
        match remove_operator_finalizer(
            &api,
            &upgrade.name_any(),
            upgrade.metadata.finalizers.as_ref(),
            UPGRADE_FINALIZER,
        )
        .await
        {
            Ok(()) => {}
            Err(e) if is_namespace_not_found_error(&e) => {
                // Namespace is gone - use special cleanup procedure
                cleanup_stuck_resource::<PostgresUpgrade>(
                    ctx.client.clone(),
                    &upgrade.name_any(),
                    ns,
                    UPGRADE_FINALIZER,
                )
                .await
                .map_err(UpgradeError::KubeError)?;
            }
            Err(e) => return Err(UpgradeError::KubeError(e)),
        }
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

/// Build the Ready / Progressing / Degraded conditions that describe an
/// upgrade in the given phase, merged into the upgrade's existing condition
/// list. Mirrors the convention used by valkey-operator and the cluster
/// controller in this repo: every status update emits the same three
/// top-level conditions so consumers can `kubectl wait --for=condition=Ready`.
fn conditions_for_phase(
    upgrade: &PostgresUpgrade,
    phase: UpgradePhase,
    error_message: Option<&str>,
) -> Vec<Condition> {
    let generation = upgrade.metadata.generation;
    let mut conditions = upgrade
        .status
        .as_ref()
        .map(|s| s.conditions.clone())
        .unwrap_or_default();

    let (ready, ready_reason, ready_msg): (bool, &str, &str) = match phase {
        UpgradePhase::Completed => (true, "UpgradeCompleted", "Major-version upgrade complete"),
        UpgradePhase::RolledBack => (false, "RolledBack", "Upgrade was rolled back to the source"),
        UpgradePhase::Failed => (
            false,
            "UpgradeFailed",
            error_message.unwrap_or("Upgrade failed"),
        ),
        _ => (false, "InProgress", "Upgrade is in progress"),
    };

    let progressing = !matches!(
        phase,
        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack
    );
    let progressing_reason = match phase {
        UpgradePhase::Pending => "Pending",
        UpgradePhase::CreatingTarget => "CreatingTarget",
        UpgradePhase::ConfiguringReplication => "ConfiguringReplication",
        UpgradePhase::Replicating => "Replicating",
        UpgradePhase::Verifying => "Verifying",
        UpgradePhase::SyncingSequences => "SyncingSequences",
        UpgradePhase::ReadyForCutover => "ReadyForCutover",
        UpgradePhase::WaitingForManualCutover => "WaitingForManualCutover",
        UpgradePhase::CuttingOver => "CuttingOver",
        UpgradePhase::HealthChecking => "HealthChecking",
        UpgradePhase::Completed => "UpgradeCompleted",
        UpgradePhase::Failed => "UpgradeFailed",
        UpgradePhase::RolledBack => "RolledBack",
    };

    let degraded = matches!(phase, UpgradePhase::Failed | UpgradePhase::RolledBack);

    set_status_condition(
        &mut conditions,
        new_condition(
            "Ready",
            if ready {
                cond_status::TRUE
            } else {
                cond_status::FALSE
            },
            ready_reason,
            ready_msg,
            generation,
        ),
    );
    set_status_condition(
        &mut conditions,
        new_condition(
            "Progressing",
            if progressing {
                cond_status::TRUE
            } else {
                cond_status::FALSE
            },
            progressing_reason,
            &format!("Phase: {}", phase),
            generation,
        ),
    );
    set_status_condition(
        &mut conditions,
        new_condition(
            "Degraded",
            if degraded {
                cond_status::TRUE
            } else {
                cond_status::FALSE
            },
            if degraded { "UpgradeFailed" } else { "Healthy" },
            error_message.unwrap_or(""),
            generation,
        ),
    );

    conditions
}

/// Update the upgrade phase in status
async fn update_phase(
    upgrade: &PostgresUpgrade,
    ctx: &UpgradeContext,
    ns: &str,
    phase: UpgradePhase,
) -> UpgradeResult<()> {
    let api: Api<PostgresUpgrade> = Api::namespaced(ctx.client.clone(), ns);
    let now = Timestamp::now().to_string();

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

    let conditions = conditions_for_phase(upgrade, phase, None);
    let (reason, message) = conditions
        .iter()
        .find(|c| c.type_ == "Ready")
        .map(|c| (c.reason.clone(), c.message.clone()))
        .unwrap_or_default();

    let patch = match (is_terminal, is_starting) {
        (true, true) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "completedAt": now,
                "startedAt": now,
                "conditions": conditions,
                "reason": reason,
                "message": message,
            }
        }),
        (true, false) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "completedAt": now,
                "conditions": conditions,
                "reason": reason,
                "message": message,
            }
        }),
        (false, true) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "startedAt": now,
                "conditions": conditions,
                "reason": reason,
                "message": message,
            }
        }),
        (false, false) => serde_json::json!({
            "status": {
                "phase": phase,
                "phaseStartedAt": now,
                "observedGeneration": upgrade.metadata.generation,
                "conditions": conditions,
                "reason": reason,
                "message": message,
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
        source_lsn: Some(lag.source_lsn),
        target_lsn: Some(lag.target_lsn),
        lag_bytes: Some(lag.lag_bytes),
        lag_seconds: lag.lag_seconds,
        lsn_in_sync: Some(lag.in_sync),
        last_sync_time: Some(Timestamp::now().to_string()),
        publication_name: None,
        subscription_name: None,
        // ddl_count is owned by the DDL audit path (`patch_ddl_count_status`);
        // don't clobber it from the lag-update path.
        ddl_count: upgrade
            .status
            .as_ref()
            .and_then(|s| s.replication.as_ref())
            .and_then(|r| r.ddl_count),
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
        last_check_time: Some(Timestamp::now().to_string()),
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
        synced_at: Some(Timestamp::now().to_string()),
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

/// Check if the current wall-clock time falls inside the configured
/// maintenance window.
///
/// Handles overnight windows where `end < start` (e.g. 23:00–03:00) by
/// matching either side of midnight. Compares times in the window's
/// declared timezone, not the operator's local timezone. Any parse failure
/// (invalid time format or unknown timezone) returns `false` — we refuse
/// to cutover rather than risk doing so at an unintended time.
fn is_within_maintenance_window(upgrade: &PostgresUpgrade) -> bool {
    let Some(window) = upgrade.spec.strategy.cutover.allowed_window.as_ref() else {
        // No window specified means always allowed.
        return true;
    };

    let tz = match jiff::tz::TimeZone::get(&window.timezone) {
        Ok(tz) => tz,
        Err(e) => {
            warn!(
                "maintenance window has invalid timezone {:?}: {}; refusing cutover",
                window.timezone, e
            );
            return false;
        }
    };

    let now_time = Timestamp::now().to_zoned(tz).time();
    is_time_within_window(&window.start_time, &window.end_time, now_time)
}

/// Pure-function core of [`is_within_maintenance_window`] for unit testing.
///
/// Returns `false` on any malformed time string. Treats `end < start` as
/// an overnight window that wraps midnight.
fn is_time_within_window(start_str: &str, end_str: &str, now: jiff::civil::Time) -> bool {
    let (Some(start), Some(end)) = (parse_window_time(start_str), parse_window_time(end_str))
    else {
        return false;
    };

    if start <= end {
        // Daytime window: [start, end] on the same calendar day.
        now >= start && now <= end
    } else {
        // Overnight window: matches either side of midnight.
        // [start, 23:59:59.999...] ∪ [00:00, end]
        now >= start || now <= end
    }
}

/// Parse an `HH:MM` window time, returning `None` on malformed input.
fn parse_window_time(s: &str) -> Option<jiff::civil::Time> {
    jiff::civil::Time::strptime("%H:%M", s.trim()).ok()
}

/// Decide whether the source cluster's most recent backup is recent enough
/// to allow auto-cutover.
///
/// Returns `true` only when:
/// - `last_backup_time` is present and parses as RFC 3339, AND
/// - `max_age` parses as a duration (e.g. `"1h"`, `"24h"`), AND
/// - `now - last_backup_time <= max_age`.
///
/// On any failure (missing backup, malformed timestamp, malformed duration)
/// returns `false`. This is the safe default: we never let auto-cutover
/// proceed without explicit confirmation that a recent backup exists, since
/// the upgrade is a one-way trip past the `CuttingOver` phase.
fn is_backup_recent_enough(last_backup_time: Option<&str>, max_age: &str, now: Timestamp) -> bool {
    let Some(last) = last_backup_time else {
        return false;
    };
    let Some(max_age) = parse_duration(max_age) else {
        return false;
    };
    let Ok(last_ts) = last.parse::<Timestamp>() else {
        return false;
    };

    let elapsed = now.as_second().saturating_sub(last_ts.as_second());
    elapsed >= 0 && elapsed <= max_age.as_secs()
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

    let started = match phase_started_at.parse::<Timestamp>() {
        Ok(ts) => ts,
        Err(_) => return false,
    };

    let current_phase = upgrade
        .status
        .as_ref()
        .map(|s| &s.phase)
        .unwrap_or(&UpgradePhase::Pending);

    let timeout = get_phase_timeout(upgrade, current_phase);
    let now_secs = Timestamp::now().as_second();
    let started_secs = started.as_second();
    let elapsed_secs = now_secs.saturating_sub(started_secs);

    elapsed_secs > timeout.as_secs()
}

/// Get timeout for a specific phase
fn get_phase_timeout(upgrade: &PostgresUpgrade, phase: &UpgradePhase) -> SignedDuration {
    let timeouts = &upgrade.spec.strategy.timeouts;

    let duration_str = match phase {
        UpgradePhase::CreatingTarget => &timeouts.target_cluster_ready,
        UpgradePhase::Replicating => &timeouts.initial_sync,
        UpgradePhase::Verifying => &timeouts.verification,
        _ => return SignedDuration::from_hours(1), // Default timeout
    };

    parse_duration(duration_str).unwrap_or_else(|| SignedDuration::from_hours(1))
}

/// Parse a duration string (e.g., "30m", "1h", "24h")
fn parse_duration(s: &str) -> Option<SignedDuration> {
    let s = s.trim();
    if s.ends_with('h') {
        let hours: i64 = s.trim_end_matches('h').parse().ok()?;
        Some(SignedDuration::from_hours(hours))
    } else if s.ends_with('m') {
        let minutes: i64 = s.trim_end_matches('m').parse().ok()?;
        Some(SignedDuration::from_mins(minutes))
    } else if s.ends_with('s') {
        let seconds: i64 = s.trim_end_matches('s').parse().ok()?;
        Some(SignedDuration::from_secs(seconds))
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
        assert_eq!(parse_duration("30m"), Some(SignedDuration::from_mins(30)));
        assert_eq!(parse_duration("1h"), Some(SignedDuration::from_hours(1)));
        assert_eq!(parse_duration("24h"), Some(SignedDuration::from_hours(24)));
        assert_eq!(parse_duration("60s"), Some(SignedDuration::from_secs(60)));
        assert_eq!(parse_duration("invalid"), None);
    }

    fn t(h: i8, m: i8) -> jiff::civil::Time {
        jiff::civil::Time::new(h, m, 0, 0).unwrap()
    }

    #[test]
    fn test_parse_window_time_valid() {
        assert_eq!(parse_window_time("00:00"), Some(t(0, 0)));
        assert_eq!(parse_window_time("23:59"), Some(t(23, 59)));
        assert_eq!(parse_window_time("02:30"), Some(t(2, 30)));
        // Leading/trailing whitespace tolerated.
        assert_eq!(parse_window_time("  02:30  "), Some(t(2, 30)));
    }

    #[test]
    fn test_parse_window_time_invalid() {
        assert_eq!(parse_window_time(""), None);
        assert_eq!(parse_window_time("not a time"), None);
        assert_eq!(parse_window_time("25:00"), None);
        assert_eq!(parse_window_time("12:60"), None);
        // No seconds permitted by the contract — keep the schema strict.
        assert_eq!(parse_window_time("02:30:00"), None);
    }

    #[test]
    fn test_window_daytime_inside() {
        // 02:00–04:00, current time 03:00.
        assert!(is_time_within_window("02:00", "04:00", t(3, 0)));
    }

    #[test]
    fn test_window_daytime_at_boundaries() {
        // Boundaries are inclusive on both ends.
        assert!(is_time_within_window("02:00", "04:00", t(2, 0)));
        assert!(is_time_within_window("02:00", "04:00", t(4, 0)));
    }

    #[test]
    fn test_window_daytime_outside() {
        assert!(!is_time_within_window("02:00", "04:00", t(1, 59)));
        assert!(!is_time_within_window("02:00", "04:00", t(4, 1)));
        assert!(!is_time_within_window("02:00", "04:00", t(12, 0)));
    }

    #[test]
    fn test_window_overnight_late_evening() {
        // 23:00–03:00 overnight. 23:30 is inside.
        assert!(is_time_within_window("23:00", "03:00", t(23, 30)));
    }

    #[test]
    fn test_window_overnight_early_morning() {
        // 23:00–03:00 overnight. 02:00 is inside.
        assert!(is_time_within_window("23:00", "03:00", t(2, 0)));
    }

    #[test]
    fn test_window_overnight_midnight() {
        // Midnight itself is on the "early morning" side of the wraparound.
        assert!(is_time_within_window("23:00", "03:00", t(0, 0)));
    }

    #[test]
    fn test_window_overnight_outside_midday() {
        // Outside the overnight window — well outside both halves.
        assert!(!is_time_within_window("23:00", "03:00", t(12, 0)));
        assert!(!is_time_within_window("23:00", "03:00", t(15, 30)));
        // Just before and after the boundaries.
        assert!(!is_time_within_window("23:00", "03:00", t(22, 59)));
        assert!(!is_time_within_window("23:00", "03:00", t(3, 1)));
    }

    #[test]
    fn test_window_zero_width() {
        // start == end: matches only at that exact minute.
        assert!(is_time_within_window("12:00", "12:00", t(12, 0)));
        assert!(!is_time_within_window("12:00", "12:00", t(11, 59)));
        assert!(!is_time_within_window("12:00", "12:00", t(12, 1)));
    }

    #[test]
    fn test_window_full_day() {
        // 00:00–23:59 matches essentially everything.
        assert!(is_time_within_window("00:00", "23:59", t(0, 0)));
        assert!(is_time_within_window("00:00", "23:59", t(12, 0)));
        assert!(is_time_within_window("00:00", "23:59", t(23, 59)));
    }

    #[test]
    fn test_window_invalid_input_refuses_cutover() {
        // Safe default: parse failure means "not in window" → cutover refused.
        assert!(!is_time_within_window("bogus", "04:00", t(3, 0)));
        assert!(!is_time_within_window("02:00", "bogus", t(3, 0)));
        assert!(!is_time_within_window("", "", t(12, 0)));
    }

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn test_backup_recent_within_max_age() {
        // Backup taken 30m ago, max age 1h → recent enough.
        let now = ts("2026-05-28T12:00:00Z");
        let last = "2026-05-28T11:30:00Z";
        assert!(is_backup_recent_enough(Some(last), "1h", now));
    }

    #[test]
    fn test_backup_exactly_at_max_age_boundary() {
        // Backup taken exactly 1h ago, max age 1h → still recent enough (≤).
        let now = ts("2026-05-28T12:00:00Z");
        let last = "2026-05-28T11:00:00Z";
        assert!(is_backup_recent_enough(Some(last), "1h", now));
    }

    #[test]
    fn test_backup_older_than_max_age() {
        // Backup taken 2h ago, max age 1h → too old.
        let now = ts("2026-05-28T12:00:00Z");
        let last = "2026-05-28T10:00:00Z";
        assert!(!is_backup_recent_enough(Some(last), "1h", now));
    }

    #[test]
    fn test_backup_missing_blocks_cutover() {
        // No backup recorded → block cutover.
        let now = ts("2026-05-28T12:00:00Z");
        assert!(!is_backup_recent_enough(None, "1h", now));
    }

    #[test]
    fn test_backup_unparseable_timestamp_blocks_cutover() {
        let now = ts("2026-05-28T12:00:00Z");
        assert!(!is_backup_recent_enough(Some("not a timestamp"), "1h", now));
        assert!(!is_backup_recent_enough(Some(""), "1h", now));
    }

    #[test]
    fn test_backup_unparseable_max_age_blocks_cutover() {
        // If the configured duration is malformed, default to "block" so we
        // surface the misconfiguration rather than silently allowing cutover.
        let now = ts("2026-05-28T12:00:00Z");
        let last = "2026-05-28T11:30:00Z";
        assert!(!is_backup_recent_enough(Some(last), "bogus", now));
        assert!(!is_backup_recent_enough(Some(last), "", now));
    }

    #[test]
    fn test_backup_future_timestamp_blocks_cutover() {
        // If the recorded backup time is in the future (clock skew, bad
        // status write), refuse to treat it as a valid backup.
        let now = ts("2026-05-28T12:00:00Z");
        let last = "2026-05-28T13:00:00Z";
        assert!(!is_backup_recent_enough(Some(last), "24h", now));
    }

    #[test]
    fn test_backup_long_max_age_window() {
        // 24h windows are common; verify nothing overflows or off-by-ones.
        let now = ts("2026-05-28T12:00:00Z");
        let last_23h_ago = "2026-05-27T13:00:00Z";
        assert!(is_backup_recent_enough(Some(last_23h_ago), "24h", now));
        let last_25h_ago = "2026-05-27T11:00:00Z";
        assert!(!is_backup_recent_enough(Some(last_25h_ago), "24h", now));
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

    #[test]
    fn test_map_service_switch_error_not_found() {
        let err =
            map_service_switch_error(ServiceSwitchError::NotFound("orders-primary".to_string()));
        match err {
            UpgradeError::ServiceSwitchFailed(msg) => {
                assert!(msg.contains("orders-primary"));
                assert!(msg.contains("service not found"));
            }
            other => panic!("expected ServiceSwitchFailed, got {other:?}"),
        }
    }

    #[test]
    fn test_map_service_switch_error_invalid_config() {
        let err = map_service_switch_error(ServiceSwitchError::InvalidConfig(
            "missing selector".to_string(),
        ));
        match err {
            UpgradeError::ServiceSwitchFailed(msg) => {
                assert!(msg.contains("invalid service configuration"));
                assert!(msg.contains("missing selector"));
            }
            other => panic!("expected ServiceSwitchFailed, got {other:?}"),
        }
    }

    #[test]
    fn test_map_service_switch_error_patch_failed_preserves_kube_error() {
        // PatchFailed wraps a kube::Error which should pass through so existing
        // retry classification (KubeError -> retryable) continues to apply.
        let kube_err = kube::Error::Api(Box::new(kube::core::Status {
            message: "internal".to_string(),
            reason: "InternalError".to_string(),
            code: 500,
            ..Default::default()
        }));
        let err = map_service_switch_error(ServiceSwitchError::PatchFailed {
            name: "orders-primary".to_string(),
            source: kube_err,
        });
        match err {
            UpgradeError::KubeError(_) => {}
            other => panic!("expected KubeError, got {other:?}"),
        }
    }

    #[test]
    fn test_service_switch_failed_is_retryable() {
        let err = UpgradeError::ServiceSwitchFailed("transient".to_string());
        assert!(err.is_retryable());
        assert!(!err.is_permanent());
        assert!(!err.blocks_cutover());
    }

    #[test]
    fn test_target_cluster_not_found_is_permanent() {
        let err = UpgradeError::TargetClusterNotFound {
            namespace: "default".to_string(),
            name: "orders-upgrade-target".to_string(),
        };
        assert!(err.is_permanent());
        assert!(!err.is_retryable());
        assert!(!err.blocks_cutover());
    }

    #[test]
    fn test_health_check_event_target_running() {
        // When target cluster is Running, emit HealthCheckPassed so the
        // (HealthChecking -> Completed) transition runs cleanup_replication.
        assert_eq!(
            health_check_event_for_target_phase(Some(ClusterPhase::Running)),
            Some(UpgradeEvent::HealthCheckPassed)
        );
    }

    #[test]
    fn test_health_check_event_target_missing() {
        // If the target cluster is not found, keep polling.
        assert_eq!(health_check_event_for_target_phase(None), None);
    }

    #[test]
    fn test_health_check_event_target_not_running() {
        // Any non-Running phase means the target isn't healthy yet; keep polling.
        for phase in [
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::Updating,
            ClusterPhase::Scaling,
            ClusterPhase::Degraded,
            ClusterPhase::Recovering,
            ClusterPhase::Failed,
            ClusterPhase::Deleting,
            ClusterPhase::Superseded,
        ] {
            assert_eq!(
                health_check_event_for_target_phase(Some(phase)),
                None,
                "expected no event when target phase is {:?}",
                phase
            );
        }
    }
}
