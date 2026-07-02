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

    /// Source cluster not found
    #[error("Source cluster not found: {namespace}/{name}")]
    SourceClusterNotFound { namespace: String, name: String },

    /// Target cluster not found
    #[error("Target cluster not found: {namespace}/{name}")]
    TargetClusterNotFound { namespace: String, name: String },

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

    /// SQL execution error
    #[error("SQL execution failed: {0}")]
    SqlError(String),

    /// Connection draining timeout
    #[error("Connection draining timeout: {0}")]
    ConnectionDrainTimeout(String),

    /// Service switch failed during cutover
    #[error("Service switch failed: {0}")]
    ServiceSwitchFailed(String),

    /// Generic transient error
    #[error("Transient error (will retry): {0}")]
    TransientError(String),

    // ============================================
    // Verification Errors (block cutover, continue monitoring)
    // ============================================
    /// Sequence sync failed
    #[error("Sequence synchronization failed: {failed_count} sequences failed")]
    SequenceSyncFailed { failed_count: i32 },

    // ============================================
    // Preflight Errors
    // ============================================
    /// The source cluster failed one or more replication-compatibility
    /// preflight checks. The upgrade cannot proceed until the user resolves
    /// the listed conditions (e.g. add primary keys, disable `pg_cron`,
    /// drop large objects). This is a permanent error — the upgrade
    /// transitions to `Failed`; the user must fix the source and create a
    /// new `PostgresUpgrade` to retry.
    #[error("Preflight checks failed: {summary}")]
    PreflightCheckFailed {
        /// Short human-readable summary suitable for logs and the upgrade
        /// status `message` field.
        summary: String,
        /// One actionable string per failed check, suitable for surfacing
        /// in conditions and events.
        failures: Vec<String>,
    },
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
                | UpgradeError::SqlError(_)
                | UpgradeError::ConnectionDrainTimeout(_)
                | UpgradeError::ServiceSwitchFailed(_)
                | UpgradeError::TransientError(_)
        )
    }

    /// Returns true if this error is permanent and requires user intervention.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            UpgradeError::ValidationError(_)
                | UpgradeError::SourceClusterNotFound { .. }
                | UpgradeError::TargetClusterNotFound { .. }
                | UpgradeError::PreflightCheckFailed { .. }
        )
    }

    /// Returns true if this error blocks automatic cutover but doesn't prevent
    /// continued monitoring and progress toward cutover readiness.
    pub fn blocks_cutover(&self) -> bool {
        matches!(self, UpgradeError::SequenceSyncFailed { .. })
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
        assert!(UpgradeError::SqlError("connection refused".to_string()).is_retryable());
        assert!(UpgradeError::ConnectionDrainTimeout("timed out".to_string()).is_retryable());
        assert!(UpgradeError::TransientError("retry me".to_string()).is_retryable());
    }

    #[test]
    fn test_error_classification_permanent() {
        assert!(UpgradeError::ValidationError("invalid".to_string()).is_permanent());
        assert!(
            UpgradeError::SourceClusterNotFound {
                namespace: "default".to_string(),
                name: "my-cluster".to_string(),
            }
            .is_permanent()
        );
        assert!(
            UpgradeError::PreflightCheckFailed {
                summary: "1 preflight check failed".to_string(),
                failures: vec!["pg_cron is active".to_string()],
            }
            .is_permanent()
        );
    }

    #[test]
    fn test_preflight_check_failed_not_retryable() {
        // PreflightCheckFailed is permanent — the source must change, not
        // the operator's retry behaviour. Explicitly assert it's not in
        // the retryable set so a future maintainer doesn't accidentally
        // reclassify it.
        let err = UpgradeError::PreflightCheckFailed {
            summary: "2 preflight checks failed".to_string(),
            failures: vec![
                "pg_largeobject is non-empty".to_string(),
                "1 unlogged table(s) found".to_string(),
            ],
        };
        assert!(!err.is_retryable());
        assert!(err.is_permanent());
        assert!(!err.blocks_cutover());

        // Display surfaces the summary (machine-grepable) without
        // duplicating the full list (that lives in the structured field).
        let msg = err.to_string();
        assert!(
            msg.contains("Preflight checks failed: 2 preflight checks failed"),
            "got: {msg}"
        );
    }

    #[test]
    fn test_error_classification_blocks_cutover() {
        assert!(
            UpgradeError::SequenceSyncFailed {
                failed_count: 3
            }
            .blocks_cutover()
        );
        assert!(!UpgradeError::ValidationError("invalid".to_string()).blocks_cutover());
        assert!(!UpgradeError::SqlError("err".to_string()).blocks_cutover());
    }

    #[test]
    fn test_error_classification_mutually_exclusive() {
        // Verify that errors are only in one category
        let errors: Vec<UpgradeError> = vec![
            UpgradeError::ValidationError("test".to_string()),
            UpgradeError::SqlError("test".to_string()),
            UpgradeError::SequenceSyncFailed { failed_count: 1 },
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
        let error = UpgradeError::SequenceSyncFailed { failed_count: 5 };

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
