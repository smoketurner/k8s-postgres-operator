//! Formal finite state machine for PostgresUpgrade lifecycle management
//!
//! This module implements a proper FSM pattern with explicit state transitions,
//! guards, and actions. It ensures that only valid state transitions occur during
//! the blue-green upgrade process.
//!
//! ## Phase Flow
//!
//! ```text
//! Pending → CreatingTarget → ConfiguringReplication → Replicating →
//! Verifying → SyncingSequences → ReadyForCutover →
//! WaitingForManualCutover (Manual mode) | CuttingOver →
//! HealthChecking → Completed
//!                 ↓ (any phase)
//!               Failed | RolledBack
//! ```

use std::fmt;

use crate::crd::{CutoverMode, UpgradePhase};

/// Events that trigger state transitions in the upgrade lifecycle
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum UpgradeEvent {
    /// Validation passed, start creating target cluster
    ValidationPassed,
    /// Target cluster has been created and is ready
    TargetClusterReady,
    /// Publication and subscription have been configured
    ReplicationConfigured,
    /// Initial data sync completed, replication is active
    InitialSyncCompleted,
    /// Replication is caught up (lag within threshold)
    ReplicationCaughtUp,
    /// Row counts and data verification passed
    VerificationPassed,
    /// Sequences have been synchronized
    SequencesSynced,
    /// All pre-checks passed, ready for cutover
    PreChecksPassed,
    /// Manual cutover triggered (via annotation)
    ManualCutoverTriggered,
    /// Automatic cutover conditions met (within maintenance window)
    AutoCutoverConditionsMet,
    /// Service switching completed
    ServicesSwitched,
    /// Post-cutover health check passed
    HealthCheckPassed,
    /// Rollback requested (via annotation)
    RollbackRequested,
    /// Rollback completed successfully
    RollbackCompleted,
    /// An error occurred during the upgrade
    ErrorOccurred,
    /// Timeout occurred for current phase
    TimeoutOccurred,
    /// Verification failed (blocks cutover but continues monitoring)
    VerificationFailed,
}

impl fmt::Display for UpgradeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpgradeEvent::ValidationPassed => write!(f, "ValidationPassed"),
            UpgradeEvent::TargetClusterReady => write!(f, "TargetClusterReady"),
            UpgradeEvent::ReplicationConfigured => write!(f, "ReplicationConfigured"),
            UpgradeEvent::InitialSyncCompleted => write!(f, "InitialSyncCompleted"),
            UpgradeEvent::ReplicationCaughtUp => write!(f, "ReplicationCaughtUp"),
            UpgradeEvent::VerificationPassed => write!(f, "VerificationPassed"),
            UpgradeEvent::SequencesSynced => write!(f, "SequencesSynced"),
            UpgradeEvent::PreChecksPassed => write!(f, "PreChecksPassed"),
            UpgradeEvent::ManualCutoverTriggered => write!(f, "ManualCutoverTriggered"),
            UpgradeEvent::AutoCutoverConditionsMet => write!(f, "AutoCutoverConditionsMet"),
            UpgradeEvent::ServicesSwitched => write!(f, "ServicesSwitched"),
            UpgradeEvent::HealthCheckPassed => write!(f, "HealthCheckPassed"),
            UpgradeEvent::RollbackRequested => write!(f, "RollbackRequested"),
            UpgradeEvent::RollbackCompleted => write!(f, "RollbackCompleted"),
            UpgradeEvent::ErrorOccurred => write!(f, "ErrorOccurred"),
            UpgradeEvent::TimeoutOccurred => write!(f, "TimeoutOccurred"),
            UpgradeEvent::VerificationFailed => write!(f, "VerificationFailed"),
        }
    }
}

/// Context information available during upgrade state transitions
#[derive(Debug, Clone)]
pub struct UpgradeTransitionContext {
    /// Whether the source cluster is ready and running
    pub source_cluster_ready: bool,
    /// Whether the target cluster is ready and running
    pub target_cluster_ready: bool,
    /// Current replication lag in bytes (None if not replicating)
    pub replication_lag_bytes: Option<i64>,
    /// Current replication lag in seconds (None if not replicating)
    pub replication_lag_seconds: Option<i64>,
    /// Number of consecutive verification passes
    pub verification_passes: i32,
    /// Required number of verification passes
    pub required_verification_passes: i32,
    /// Whether sequences have been synchronized
    pub sequences_synced: bool,
    /// Whether backup requirement is met
    pub backup_requirement_met: bool,
    /// Cutover mode (Manual or Automatic)
    pub cutover_mode: CutoverMode,
    /// Whether within maintenance window (for Automatic mode)
    pub within_maintenance_window: bool,
    /// Whether rollback annotation is present
    pub rollback_requested: bool,
    /// Whether rollback is feasible
    pub rollback_feasible: bool,
    /// Whether target has received writes (blocks rollback)
    pub target_has_writes: bool,
    /// Error message if any
    pub error_message: Option<String>,
    /// Current phase timeout elapsed
    pub phase_timeout_elapsed: bool,
    /// Row count mismatch count (0 means all match)
    pub row_count_mismatches: i32,
}

impl Default for UpgradeTransitionContext {
    fn default() -> Self {
        Self {
            source_cluster_ready: false,
            target_cluster_ready: false,
            replication_lag_bytes: None,
            replication_lag_seconds: None,
            verification_passes: 0,
            required_verification_passes: 3,
            sequences_synced: false,
            backup_requirement_met: false,
            cutover_mode: CutoverMode::Manual,
            within_maintenance_window: false,
            rollback_requested: false,
            rollback_feasible: true,
            target_has_writes: false,
            error_message: None,
            phase_timeout_elapsed: false,
            row_count_mismatches: 0,
        }
    }
}

impl UpgradeTransitionContext {
    /// Check if replication is caught up (lag below threshold)
    pub fn replication_caught_up(&self, max_lag_bytes: i64) -> bool {
        matches!(self.replication_lag_bytes, Some(lag) if lag <= max_lag_bytes)
    }

    /// Check if verification has passed enough times
    pub fn verification_complete(&self) -> bool {
        self.verification_passes >= self.required_verification_passes
            && self.row_count_mismatches == 0
    }

    /// Check if ready for automatic cutover
    pub fn ready_for_auto_cutover(&self) -> bool {
        self.cutover_mode == CutoverMode::Automatic
            && self.within_maintenance_window
            && self.backup_requirement_met
    }

    /// Check if rollback is safe (no data loss risk)
    pub fn can_rollback_safely(&self) -> bool {
        self.rollback_feasible && !self.target_has_writes
    }
}

/// A state transition definition for upgrade phases
#[derive(Debug)]
pub struct UpgradeTransition {
    /// Source phase
    pub from: UpgradePhase,
    /// Target phase
    pub to: UpgradePhase,
    /// Event that triggers this transition
    pub event: UpgradeEvent,
    /// Human-readable description of this transition
    pub description: &'static str,
}

impl UpgradeTransition {
    /// Create a new transition
    const fn new(
        from: UpgradePhase,
        to: UpgradePhase,
        event: UpgradeEvent,
        description: &'static str,
    ) -> Self {
        Self {
            from,
            to,
            event,
            description,
        }
    }
}

/// Result of attempting an upgrade state transition
#[derive(Debug)]
pub enum UpgradeTransitionResult {
    /// Transition was successful
    Success {
        from: UpgradePhase,
        to: UpgradePhase,
        event: UpgradeEvent,
        description: &'static str,
    },
    /// Transition was not valid for current phase
    InvalidTransition {
        current: UpgradePhase,
        event: UpgradeEvent,
    },
    /// Guard condition prevented the transition
    GuardFailed {
        from: UpgradePhase,
        to: UpgradePhase,
        event: UpgradeEvent,
        reason: String,
    },
}

/// Formal state machine for PostgresUpgrade lifecycle
pub struct UpgradeStateMachine {
    transitions: Vec<UpgradeTransition>,
}

impl Default for UpgradeStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl UpgradeStateMachine {
    /// Create a new upgrade state machine with the defined transition table
    #[allow(clippy::too_many_lines)]
    pub fn new() -> Self {
        Self {
            transitions: vec![
                // === Pending state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::Pending,
                    UpgradePhase::CreatingTarget,
                    UpgradeEvent::ValidationPassed,
                    "Validation passed, creating target cluster",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Pending,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Validation failed",
                ),
                // === CreatingTarget state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::CreatingTarget,
                    UpgradePhase::ConfiguringReplication,
                    UpgradeEvent::TargetClusterReady,
                    "Target cluster ready, configuring replication",
                ),
                UpgradeTransition::new(
                    UpgradePhase::CreatingTarget,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Target cluster creation failed",
                ),
                UpgradeTransition::new(
                    UpgradePhase::CreatingTarget,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Target cluster creation timed out",
                ),
                UpgradeTransition::new(
                    UpgradePhase::CreatingTarget,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested during target creation",
                ),
                // === ConfiguringReplication state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::ConfiguringReplication,
                    UpgradePhase::Replicating,
                    UpgradeEvent::ReplicationConfigured,
                    "Replication configured, starting initial sync",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ConfiguringReplication,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Replication configuration failed",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ConfiguringReplication,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Replication configuration timed out",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ConfiguringReplication,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested during replication setup",
                ),
                // === Replicating state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::Replicating,
                    UpgradePhase::Verifying,
                    UpgradeEvent::ReplicationCaughtUp,
                    "Replication caught up, starting verification",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Replicating,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Replication error occurred",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Replicating,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Initial sync timed out",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Replicating,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested during replication",
                ),
                // === Verifying state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::Verifying,
                    UpgradePhase::SyncingSequences,
                    UpgradeEvent::VerificationPassed,
                    "Verification passed, syncing sequences",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Verifying,
                    UpgradePhase::Replicating,
                    UpgradeEvent::VerificationFailed,
                    "Verification failed, continuing replication monitoring",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Verifying,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Verification error occurred",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Verifying,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Verification timed out",
                ),
                UpgradeTransition::new(
                    UpgradePhase::Verifying,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested during verification",
                ),
                // === SyncingSequences state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::SyncingSequences,
                    UpgradePhase::ReadyForCutover,
                    UpgradeEvent::SequencesSynced,
                    "Sequences synced, ready for cutover",
                ),
                UpgradeTransition::new(
                    UpgradePhase::SyncingSequences,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Sequence sync failed",
                ),
                UpgradeTransition::new(
                    UpgradePhase::SyncingSequences,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Sequence sync timed out",
                ),
                UpgradeTransition::new(
                    UpgradePhase::SyncingSequences,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested during sequence sync",
                ),
                // === ReadyForCutover state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::ReadyForCutover,
                    UpgradePhase::WaitingForManualCutover,
                    UpgradeEvent::PreChecksPassed,
                    "Pre-checks passed, waiting for manual cutover (Manual mode)",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ReadyForCutover,
                    UpgradePhase::CuttingOver,
                    UpgradeEvent::AutoCutoverConditionsMet,
                    "Auto-cutover conditions met, starting cutover (Automatic mode)",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ReadyForCutover,
                    UpgradePhase::Verifying,
                    UpgradeEvent::VerificationFailed,
                    "Pre-checks failed, returning to verification",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ReadyForCutover,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Error while ready for cutover",
                ),
                UpgradeTransition::new(
                    UpgradePhase::ReadyForCutover,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested while ready for cutover",
                ),
                // === WaitingForManualCutover state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::WaitingForManualCutover,
                    UpgradePhase::CuttingOver,
                    UpgradeEvent::ManualCutoverTriggered,
                    "Manual cutover triggered, starting cutover",
                ),
                UpgradeTransition::new(
                    UpgradePhase::WaitingForManualCutover,
                    UpgradePhase::Verifying,
                    UpgradeEvent::VerificationFailed,
                    "Conditions changed, returning to verification",
                ),
                UpgradeTransition::new(
                    UpgradePhase::WaitingForManualCutover,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Error while waiting for manual cutover",
                ),
                UpgradeTransition::new(
                    UpgradePhase::WaitingForManualCutover,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackRequested,
                    "Rollback requested while waiting for manual cutover",
                ),
                // === CuttingOver state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::CuttingOver,
                    UpgradePhase::HealthChecking,
                    UpgradeEvent::ServicesSwitched,
                    "Services switched, performing health check",
                ),
                UpgradeTransition::new(
                    UpgradePhase::CuttingOver,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Cutover failed",
                ),
                UpgradeTransition::new(
                    UpgradePhase::CuttingOver,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Cutover timed out",
                ),
                // Note: No rollback from CuttingOver - too risky mid-transition
                // === HealthChecking state transitions ===
                UpgradeTransition::new(
                    UpgradePhase::HealthChecking,
                    UpgradePhase::Completed,
                    UpgradeEvent::HealthCheckPassed,
                    "Health check passed, upgrade completed",
                ),
                UpgradeTransition::new(
                    UpgradePhase::HealthChecking,
                    UpgradePhase::Failed,
                    UpgradeEvent::ErrorOccurred,
                    "Health check failed",
                ),
                UpgradeTransition::new(
                    UpgradePhase::HealthChecking,
                    UpgradePhase::Failed,
                    UpgradeEvent::TimeoutOccurred,
                    "Health check timed out",
                ),
                UpgradeTransition::new(
                    UpgradePhase::HealthChecking,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackCompleted,
                    "Rollback completed after health check failure",
                ),
                // === Completed state transitions ===
                // Completed can still rollback if needed (within rollback window)
                UpgradeTransition::new(
                    UpgradePhase::Completed,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackCompleted,
                    "Rollback completed from completed state",
                ),
                // === Failed state transitions ===
                // Failed can rollback to clean up
                UpgradeTransition::new(
                    UpgradePhase::Failed,
                    UpgradePhase::RolledBack,
                    UpgradeEvent::RollbackCompleted,
                    "Rollback completed from failed state",
                ),
                // === RolledBack state transitions ===
                // RolledBack is terminal - no transitions out
            ],
        }
    }

    /// Attempt to transition to a new phase based on an event
    pub fn transition(
        &self,
        current: &UpgradePhase,
        event: UpgradeEvent,
        ctx: &UpgradeTransitionContext,
    ) -> UpgradeTransitionResult {
        // Find a matching transition
        let transition = self
            .transitions
            .iter()
            .find(|t| t.from == *current && t.event == event);

        match transition {
            Some(t) => {
                // Apply guards based on the transition
                if let Some(reason) = self.check_guard(t, ctx) {
                    UpgradeTransitionResult::GuardFailed {
                        from: t.from,
                        to: t.to,
                        event,
                        reason,
                    }
                } else {
                    UpgradeTransitionResult::Success {
                        from: t.from,
                        to: t.to,
                        event,
                        description: t.description,
                    }
                }
            }
            None => UpgradeTransitionResult::InvalidTransition {
                current: *current,
                event,
            },
        }
    }

    /// Check if a transition is valid (ignoring guards)
    pub fn can_transition(&self, from: &UpgradePhase, event: &UpgradeEvent) -> bool {
        self.transitions
            .iter()
            .any(|t| t.from == *from && t.event == *event)
    }

    /// Get all valid events for a given phase
    pub fn valid_events(&self, phase: &UpgradePhase) -> Vec<&UpgradeEvent> {
        self.transitions
            .iter()
            .filter(|t| t.from == *phase)
            .map(|t| &t.event)
            .collect()
    }

    /// Check guard conditions for a transition
    fn check_guard(
        &self,
        transition: &UpgradeTransition,
        ctx: &UpgradeTransitionContext,
    ) -> Option<String> {
        match (&transition.from, &transition.to, &transition.event) {
            // Guard: TargetClusterReady requires target to be ready
            (
                UpgradePhase::CreatingTarget,
                UpgradePhase::ConfiguringReplication,
                UpgradeEvent::TargetClusterReady,
            ) => {
                if !ctx.target_cluster_ready {
                    Some("Target cluster is not ready".to_string())
                } else {
                    None
                }
            }

            // Guard: VerificationPassed requires enough passes and no mismatches
            (
                UpgradePhase::Verifying,
                UpgradePhase::SyncingSequences,
                UpgradeEvent::VerificationPassed,
            ) => {
                if !ctx.verification_complete() {
                    Some(format!(
                        "Verification incomplete: {}/{} passes, {} mismatches",
                        ctx.verification_passes,
                        ctx.required_verification_passes,
                        ctx.row_count_mismatches
                    ))
                } else {
                    None
                }
            }

            // Guard: AutoCutoverConditionsMet requires automatic mode and maintenance window
            (
                UpgradePhase::ReadyForCutover,
                UpgradePhase::CuttingOver,
                UpgradeEvent::AutoCutoverConditionsMet,
            ) => {
                if ctx.cutover_mode != CutoverMode::Automatic {
                    Some("Cutover mode is not Automatic".to_string())
                } else if !ctx.within_maintenance_window {
                    Some("Not within maintenance window".to_string())
                } else if !ctx.backup_requirement_met {
                    Some("Backup requirement not met".to_string())
                } else {
                    None
                }
            }

            // Guard: PreChecksPassed requires manual mode
            (
                UpgradePhase::ReadyForCutover,
                UpgradePhase::WaitingForManualCutover,
                UpgradeEvent::PreChecksPassed,
            ) => {
                if ctx.cutover_mode != CutoverMode::Manual {
                    Some("Cutover mode is not Manual".to_string())
                } else {
                    None
                }
            }

            // Guard: ManualCutoverTriggered requires rollback annotation not set
            (
                UpgradePhase::WaitingForManualCutover,
                UpgradePhase::CuttingOver,
                UpgradeEvent::ManualCutoverTriggered,
            ) => {
                if ctx.rollback_requested {
                    Some("Rollback is requested, cannot proceed with cutover".to_string())
                } else {
                    None
                }
            }

            // Guard: RollbackCompleted requires rollback to be feasible
            (_, UpgradePhase::RolledBack, UpgradeEvent::RollbackCompleted) => {
                if !ctx.rollback_feasible {
                    Some("Rollback is not feasible".to_string())
                } else if ctx.target_has_writes {
                    Some("Target has received writes, rollback would cause data loss".to_string())
                } else {
                    None
                }
            }

            // Guard: RollbackRequested during CuttingOver is not allowed
            (UpgradePhase::CuttingOver, _, UpgradeEvent::RollbackRequested) => {
                Some("Cannot rollback during cutover - operation must complete".to_string())
            }

            // No guard for other transitions
            _ => None,
        }
    }
}

/// Determine the appropriate event based on upgrade context
pub fn determine_upgrade_event(
    current_phase: &UpgradePhase,
    ctx: &UpgradeTransitionContext,
) -> Option<UpgradeEvent> {
    // Check for rollback request first (high priority)
    if ctx.rollback_requested && ctx.can_rollback_safely() {
        return Some(UpgradeEvent::RollbackRequested);
    }

    // Check for timeout
    if ctx.phase_timeout_elapsed {
        return Some(UpgradeEvent::TimeoutOccurred);
    }

    // Check for errors
    if ctx.error_message.is_some() {
        return Some(UpgradeEvent::ErrorOccurred);
    }

    // Phase-specific event determination
    match current_phase {
        UpgradePhase::Pending => {
            if ctx.source_cluster_ready {
                Some(UpgradeEvent::ValidationPassed)
            } else {
                None
            }
        }

        UpgradePhase::CreatingTarget => {
            if ctx.target_cluster_ready {
                Some(UpgradeEvent::TargetClusterReady)
            } else {
                None
            }
        }

        UpgradePhase::ConfiguringReplication => {
            // Check if replication is now active (indicated by lag being available)
            if ctx.replication_lag_bytes.is_some() {
                Some(UpgradeEvent::ReplicationConfigured)
            } else {
                None
            }
        }

        UpgradePhase::Replicating => {
            // Check if replication has caught up (lag below threshold)
            if ctx.replication_caught_up(0) {
                Some(UpgradeEvent::ReplicationCaughtUp)
            } else {
                None
            }
        }

        UpgradePhase::Verifying => {
            if ctx.verification_complete() {
                Some(UpgradeEvent::VerificationPassed)
            } else if ctx.row_count_mismatches > 0 {
                Some(UpgradeEvent::VerificationFailed)
            } else {
                None
            }
        }

        UpgradePhase::SyncingSequences => {
            if ctx.sequences_synced {
                Some(UpgradeEvent::SequencesSynced)
            } else {
                None
            }
        }

        UpgradePhase::ReadyForCutover => {
            if ctx.cutover_mode == CutoverMode::Manual {
                Some(UpgradeEvent::PreChecksPassed)
            } else if ctx.ready_for_auto_cutover() {
                Some(UpgradeEvent::AutoCutoverConditionsMet)
            } else if ctx.row_count_mismatches > 0 {
                Some(UpgradeEvent::VerificationFailed)
            } else {
                None
            }
        }

        UpgradePhase::WaitingForManualCutover => {
            // Manual cutover is triggered by annotation - handled by reconciler
            // Here we just check for verification regression
            if ctx.row_count_mismatches > 0 {
                Some(UpgradeEvent::VerificationFailed)
            } else {
                None
            }
        }

        UpgradePhase::CuttingOver => {
            // Service switching completion is determined by reconciler
            None
        }

        UpgradePhase::HealthChecking => {
            // Health check completion is determined by reconciler
            None
        }

        UpgradePhase::Completed | UpgradePhase::Failed => {
            // Terminal states - only rollback event is relevant
            if ctx.rollback_requested && ctx.can_rollback_safely() {
                Some(UpgradeEvent::RollbackRequested)
            } else {
                None
            }
        }

        UpgradePhase::RolledBack => {
            // Terminal state - no events
            None
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    fn default_ctx() -> UpgradeTransitionContext {
        UpgradeTransitionContext::default()
    }

    #[test]
    fn test_pending_to_creating_target() {
        let sm = UpgradeStateMachine::new();
        let ctx = default_ctx();

        let result = sm.transition(&UpgradePhase::Pending, UpgradeEvent::ValidationPassed, &ctx);

        match result {
            UpgradeTransitionResult::Success { from, to, .. } => {
                assert_eq!(from, UpgradePhase::Pending);
                assert_eq!(to, UpgradePhase::CreatingTarget);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_creating_target_to_configuring_replication_guard() {
        let sm = UpgradeStateMachine::new();

        // Should fail when target not ready
        let ctx = default_ctx();
        let result = sm.transition(
            &UpgradePhase::CreatingTarget,
            UpgradeEvent::TargetClusterReady,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should succeed when target is ready
        let mut ctx = default_ctx();
        ctx.target_cluster_ready = true;
        let result = sm.transition(
            &UpgradePhase::CreatingTarget,
            UpgradeEvent::TargetClusterReady,
            &ctx,
        );
        assert!(matches!(result, UpgradeTransitionResult::Success { .. }));
    }

    #[test]
    fn test_verification_guard() {
        let sm = UpgradeStateMachine::new();

        // Should fail with insufficient passes
        let mut ctx = default_ctx();
        ctx.verification_passes = 2;
        ctx.required_verification_passes = 3;
        let result = sm.transition(
            &UpgradePhase::Verifying,
            UpgradeEvent::VerificationPassed,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should fail with row count mismatches
        let mut ctx = default_ctx();
        ctx.verification_passes = 3;
        ctx.required_verification_passes = 3;
        ctx.row_count_mismatches = 1;
        let result = sm.transition(
            &UpgradePhase::Verifying,
            UpgradeEvent::VerificationPassed,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should succeed with enough passes and no mismatches
        let mut ctx = default_ctx();
        ctx.verification_passes = 3;
        ctx.required_verification_passes = 3;
        ctx.row_count_mismatches = 0;
        let result = sm.transition(
            &UpgradePhase::Verifying,
            UpgradeEvent::VerificationPassed,
            &ctx,
        );
        assert!(matches!(result, UpgradeTransitionResult::Success { .. }));
    }

    #[test]
    fn test_auto_cutover_guard() {
        let sm = UpgradeStateMachine::new();

        // Should fail in manual mode
        let mut ctx = default_ctx();
        ctx.cutover_mode = CutoverMode::Manual;
        let result = sm.transition(
            &UpgradePhase::ReadyForCutover,
            UpgradeEvent::AutoCutoverConditionsMet,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should fail outside maintenance window
        let mut ctx = default_ctx();
        ctx.cutover_mode = CutoverMode::Automatic;
        ctx.within_maintenance_window = false;
        let result = sm.transition(
            &UpgradePhase::ReadyForCutover,
            UpgradeEvent::AutoCutoverConditionsMet,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should fail without backup requirement met
        let mut ctx = default_ctx();
        ctx.cutover_mode = CutoverMode::Automatic;
        ctx.within_maintenance_window = true;
        ctx.backup_requirement_met = false;
        let result = sm.transition(
            &UpgradePhase::ReadyForCutover,
            UpgradeEvent::AutoCutoverConditionsMet,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should succeed with all conditions met
        let mut ctx = default_ctx();
        ctx.cutover_mode = CutoverMode::Automatic;
        ctx.within_maintenance_window = true;
        ctx.backup_requirement_met = true;
        let result = sm.transition(
            &UpgradePhase::ReadyForCutover,
            UpgradeEvent::AutoCutoverConditionsMet,
            &ctx,
        );
        assert!(matches!(result, UpgradeTransitionResult::Success { .. }));
    }

    #[test]
    fn test_rollback_guard() {
        let sm = UpgradeStateMachine::new();

        // Should fail when target has writes
        let mut ctx = default_ctx();
        ctx.rollback_feasible = true;
        ctx.target_has_writes = true;
        let result = sm.transition(
            &UpgradePhase::Completed,
            UpgradeEvent::RollbackCompleted,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should fail when rollback not feasible
        let mut ctx = default_ctx();
        ctx.rollback_feasible = false;
        ctx.target_has_writes = false;
        let result = sm.transition(
            &UpgradePhase::Completed,
            UpgradeEvent::RollbackCompleted,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::GuardFailed { .. }
        ));

        // Should succeed when rollback is safe
        let mut ctx = default_ctx();
        ctx.rollback_feasible = true;
        ctx.target_has_writes = false;
        let result = sm.transition(
            &UpgradePhase::Completed,
            UpgradeEvent::RollbackCompleted,
            &ctx,
        );
        assert!(matches!(result, UpgradeTransitionResult::Success { .. }));
    }

    #[test]
    fn test_rolled_back_is_terminal() {
        let sm = UpgradeStateMachine::new();

        // No valid events should transition out of RolledBack
        let valid_events = sm.valid_events(&UpgradePhase::RolledBack);
        assert!(valid_events.is_empty());
    }

    #[test]
    fn test_no_rollback_during_cutover() {
        let sm = UpgradeStateMachine::new();

        // Rollback should not be possible during cutover
        assert!(!sm.can_transition(&UpgradePhase::CuttingOver, &UpgradeEvent::RollbackRequested));
    }

    #[test]
    fn test_invalid_transition() {
        let sm = UpgradeStateMachine::new();
        let ctx = default_ctx();

        // Cannot go directly from Pending to Completed
        let result = sm.transition(
            &UpgradePhase::Pending,
            UpgradeEvent::HealthCheckPassed,
            &ctx,
        );
        assert!(matches!(
            result,
            UpgradeTransitionResult::InvalidTransition { .. }
        ));
    }

    #[test]
    fn test_determine_upgrade_event_pending() {
        let mut ctx = default_ctx();
        ctx.source_cluster_ready = true;

        let event = determine_upgrade_event(&UpgradePhase::Pending, &ctx);
        assert_eq!(event, Some(UpgradeEvent::ValidationPassed));
    }

    #[test]
    fn test_determine_upgrade_event_rollback_priority() {
        let mut ctx = default_ctx();
        ctx.source_cluster_ready = true;
        ctx.rollback_requested = true;
        ctx.rollback_feasible = true;
        ctx.target_has_writes = false;

        // Rollback should take priority
        let event = determine_upgrade_event(&UpgradePhase::Replicating, &ctx);
        assert_eq!(event, Some(UpgradeEvent::RollbackRequested));
    }

    #[test]
    fn test_determine_upgrade_event_error_priority() {
        let mut ctx = default_ctx();
        ctx.source_cluster_ready = true;
        ctx.error_message = Some("Something went wrong".to_string());

        let event = determine_upgrade_event(&UpgradePhase::Pending, &ctx);
        assert_eq!(event, Some(UpgradeEvent::ErrorOccurred));
    }

    #[test]
    fn test_verification_failed_returns_to_replicating() {
        let sm = UpgradeStateMachine::new();
        let ctx = default_ctx();

        let result = sm.transition(
            &UpgradePhase::Verifying,
            UpgradeEvent::VerificationFailed,
            &ctx,
        );

        match result {
            UpgradeTransitionResult::Success { from, to, .. } => {
                assert_eq!(from, UpgradePhase::Verifying);
                assert_eq!(to, UpgradePhase::Replicating);
            }
            _ => panic!("Expected successful transition back to Replicating"),
        }
    }
}
