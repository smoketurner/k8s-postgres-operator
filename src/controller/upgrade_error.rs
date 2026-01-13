//! Error types for the PostgresUpgrade controller
//!
//! Errors are classified into three categories:
//! - **Permanent**: Configuration/validation errors that won't resolve without user intervention
//! - **Transient**: Temporary errors that should be retried with backoff
//! - **Verification**: Errors that block cutover but don't prevent continued monitoring

use std::time::Duration;
use thiserror::Error;

/// Error variants for PostgresUpgrade reconciliation.
///
/// Each variant is classified as permanent, transient, or verification-blocking
/// to guide the retry behavior and status reporting.
#[derive(Error, Debug)]
pub enum UpgradeError {
    // ============================================
    // Permanent Errors (do not retry automatically)
    // ============================================
    /// Validation error - spec is invalid
    #[error("Validation failed: {0}")]
    ValidationError(String),

    /// Version downgrade attempted
    #[error("Version downgrade not allowed: {from_version} -> {to_version}")]
    DowngradeNotAllowed {
        from_version: String,
        to_version: String,
    },

    /// Unsupported PostgreSQL version
    #[error("Unsupported PostgreSQL version: {0}")]
    UnsupportedVersion(String),

    /// Source cluster not found
    #[error("Source cluster not found: {namespace}/{name}")]
    SourceClusterNotFound { namespace: String, name: String },

    /// Immutable field change attempted
    #[error("Immutable field cannot be changed: {field}")]
    ImmutableFieldChange { field: String },

    /// Concurrent upgrade exists
    #[error("Another upgrade is already in progress for source cluster: {0}")]
    ConcurrentUpgrade(String),

    // ============================================
    // Transient Errors (retry with backoff)
    // ============================================
    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    /// Replication error
    #[error("Replication error: {0}")]
    ReplicationError(#[from] crate::resources::replication::ReplicationError),

    /// PostgreSQL client error
    #[error("PostgreSQL client error: {0}")]
    PostgresClientError(#[from] crate::resources::postgres_client::PostgresClientError),

    /// Source cluster not ready
    #[error("Source cluster not ready: {0}")]
    SourceClusterNotReady(String),

    /// Target cluster not ready
    #[error("Target cluster not ready: {0}")]
    TargetClusterNotReady(String),

    /// Target cluster creation in progress
    #[error("Target cluster creation in progress")]
    TargetCreationInProgress,

    /// SQL execution error
    #[error("SQL execution failed: {0}")]
    SqlError(String),

    /// Replication setup error
    #[error("Replication setup failed: {0}")]
    ReplicationSetupError(String),

    /// Subscription not active
    #[error("Subscription not active: {0}")]
    SubscriptionNotActive(String),

    /// Connection draining timeout
    #[error("Connection draining timeout: {0}")]
    ConnectionDrainTimeout(String),

    /// Timeout error
    #[error("Phase timeout: {phase} exceeded {timeout}")]
    PhaseTimeout { phase: String, timeout: String },

    /// Generic transient error
    #[error("Transient error (will retry): {0}")]
    TransientError(String),

    // ============================================
    // Verification Errors (block cutover, continue monitoring)
    // ============================================
    /// Row count mismatch between source and target
    #[error("Row count mismatch: {mismatched_tables} tables differ")]
    RowCountMismatch { mismatched_tables: i32 },

    /// Replication lag too high for cutover
    #[error("Replication lag too high: {lag_bytes} bytes ({lag_seconds}s)")]
    ReplicationLagTooHigh { lag_bytes: i64, lag_seconds: i64 },

    /// LSN positions not in sync
    #[error("LSN positions not in sync: source={source_lsn}, target={target_lsn}")]
    LsnNotInSync {
        source_lsn: String,
        target_lsn: String,
    },

    /// Backup requirement not met
    #[error("No recent backup found within {required}")]
    BackupRequirementNotMet { required: String },

    /// Sequence sync failed
    #[error("Sequence synchronization failed: {failed_count} sequences failed")]
    SequenceSyncFailed { failed_count: i32 },

    /// Verification not passed enough times
    #[error("Verification passed {actual} times, need {required}")]
    InsufficientVerificationPasses { actual: i32, required: i32 },

    // ============================================
    // Rollback Errors
    // ============================================
    /// Rollback not feasible
    #[error("Rollback not feasible: {reason}")]
    RollbackNotFeasible { reason: String },

    /// Rollback would cause data loss
    #[error("Rollback would cause data loss: writes occurred to target after cutover")]
    RollbackDataLossRisk,
}

impl UpgradeError {
    /// Returns true if this error should trigger an automatic retry.
    ///
    /// Transient errors are retryable; permanent and verification errors are not.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            // Transient errors are retryable
            UpgradeError::KubeError(_)
                | UpgradeError::ReplicationError(_)
                | UpgradeError::PostgresClientError(_)
                | UpgradeError::SourceClusterNotReady(_)
                | UpgradeError::TargetClusterNotReady(_)
                | UpgradeError::TargetCreationInProgress
                | UpgradeError::SqlError(_)
                | UpgradeError::ReplicationSetupError(_)
                | UpgradeError::SubscriptionNotActive(_)
                | UpgradeError::ConnectionDrainTimeout(_)
                | UpgradeError::TransientError(_)
        )
    }

    /// Returns true if this error is permanent and requires user intervention.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            UpgradeError::ValidationError(_)
                | UpgradeError::DowngradeNotAllowed { .. }
                | UpgradeError::UnsupportedVersion(_)
                | UpgradeError::SourceClusterNotFound { .. }
                | UpgradeError::ImmutableFieldChange { .. }
                | UpgradeError::ConcurrentUpgrade(_)
        )
    }

    /// Returns true if this error blocks automatic cutover but doesn't prevent
    /// continued monitoring and progress toward cutover readiness.
    pub fn blocks_cutover(&self) -> bool {
        matches!(
            self,
            UpgradeError::RowCountMismatch { .. }
                | UpgradeError::ReplicationLagTooHigh { .. }
                | UpgradeError::LsnNotInSync { .. }
                | UpgradeError::BackupRequirementNotMet { .. }
                | UpgradeError::SequenceSyncFailed { .. }
                | UpgradeError::InsufficientVerificationPasses { .. }
        )
    }

    /// Returns true if this error indicates a timeout
    pub fn is_timeout(&self) -> bool {
        matches!(
            self,
            UpgradeError::PhaseTimeout { .. } | UpgradeError::ConnectionDrainTimeout(_)
        )
    }

    /// Returns true if this error is related to rollback
    pub fn is_rollback_error(&self) -> bool {
        matches!(
            self,
            UpgradeError::RollbackNotFeasible { .. } | UpgradeError::RollbackDataLossRisk
        )
    }
}

/// Result type for upgrade operations
pub type UpgradeResult<T> = std::result::Result<T, UpgradeError>;

/// Backoff configuration specific to upgrade operations
#[derive(Clone, Debug)]
pub struct UpgradeBackoffConfig {
    /// Initial delay for first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for each subsequent retry
    pub multiplier: f64,
    /// Random jitter factor (0.0 to 1.0)
    pub jitter: f64,
    /// Delay for verification errors (continue monitoring)
    pub verification_delay: Duration,
}

impl Default for UpgradeBackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(300), // 5 minutes
            multiplier: 2.0,
            jitter: 0.1,
            verification_delay: Duration::from_secs(30), // Check every 30s during verification
        }
    }
}

impl UpgradeBackoffConfig {
    /// Calculate the backoff delay for a given retry attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay_secs =
            self.initial_delay.as_secs_f64() * self.multiplier.powi(attempt as i32);

        // Apply jitter
        let jitter_range = base_delay_secs * self.jitter;
        let jitter = rand::random::<f64>() * jitter_range * 2.0 - jitter_range;
        let delay_with_jitter = (base_delay_secs + jitter).max(0.0);

        // Cap at max delay
        let capped_delay = delay_with_jitter.min(self.max_delay.as_secs_f64());

        Duration::from_secs_f64(capped_delay)
    }

    /// Get the appropriate delay for an error
    pub fn delay_for_error(&self, error: &UpgradeError, attempt: u32) -> Duration {
        if error.blocks_cutover() {
            // For verification errors, use a fixed monitoring interval
            self.verification_delay
        } else if error.is_retryable() {
            // For transient errors, use exponential backoff
            self.delay_for_attempt(attempt)
        } else {
            // For permanent errors, use max delay (allow manual intervention)
            self.max_delay
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification_retryable() {
        assert!(UpgradeError::SourceClusterNotReady("not ready".to_string()).is_retryable());
        assert!(UpgradeError::TargetCreationInProgress.is_retryable());
        assert!(UpgradeError::SqlError("connection refused".to_string()).is_retryable());
    }

    #[test]
    fn test_error_classification_permanent() {
        assert!(UpgradeError::ValidationError("invalid".to_string()).is_permanent());
        assert!(
            UpgradeError::DowngradeNotAllowed {
                from_version: "17".to_string(),
                to_version: "16".to_string(),
            }
            .is_permanent()
        );
        assert!(UpgradeError::ConcurrentUpgrade("other-upgrade".to_string()).is_permanent());
    }

    #[test]
    fn test_error_classification_blocks_cutover() {
        assert!(
            UpgradeError::RowCountMismatch {
                mismatched_tables: 5
            }
            .blocks_cutover()
        );
        assert!(
            UpgradeError::ReplicationLagTooHigh {
                lag_bytes: 1000,
                lag_seconds: 10,
            }
            .blocks_cutover()
        );
        assert!(
            UpgradeError::BackupRequirementNotMet {
                required: "1h".to_string()
            }
            .blocks_cutover()
        );
    }

    #[test]
    fn test_error_classification_mutually_exclusive() {
        // Verify that errors are only in one category
        let errors: Vec<UpgradeError> = vec![
            UpgradeError::ValidationError("test".to_string()),
            UpgradeError::SourceClusterNotReady("test".to_string()),
            UpgradeError::RowCountMismatch {
                mismatched_tables: 1,
            },
        ];

        for error in &errors {
            let categories = [
                error.is_permanent(),
                error.is_retryable(),
                error.blocks_cutover(),
            ];
            let count = categories.iter().filter(|&&x| x).count();

            // Each error should be in exactly one category
            // Note: Some errors might not be in any category (that's ok for some edge cases)
            assert!(
                count <= 1,
                "Error {:?} is in {} categories, should be at most 1",
                error,
                count
            );
        }
    }

    #[test]
    fn test_backoff_delay_for_verification_error() {
        let config = UpgradeBackoffConfig::default();
        let error = UpgradeError::RowCountMismatch {
            mismatched_tables: 5,
        };

        let delay = config.delay_for_error(&error, 0);
        assert_eq!(delay, config.verification_delay);
    }

    #[test]
    fn test_backoff_delay_for_transient_error() {
        let config = UpgradeBackoffConfig::default();
        let error = UpgradeError::SqlError("connection refused".to_string());

        let delay = config.delay_for_error(&error, 0);
        // Should be around initial_delay with some jitter
        assert!(delay >= Duration::from_secs(4));
        assert!(delay <= Duration::from_secs(6));
    }

    #[test]
    fn test_backoff_delay_for_permanent_error() {
        let config = UpgradeBackoffConfig::default();
        let error = UpgradeError::ValidationError("invalid".to_string());

        let delay = config.delay_for_error(&error, 0);
        assert_eq!(delay, config.max_delay);
    }
}
