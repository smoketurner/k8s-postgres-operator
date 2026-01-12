//! Error types for the PostgresCluster controller

use std::time::Duration;

use thiserror::Error;

/// Error variants are named with the `Error` suffix for clarity (e.g., `KubeError`, `ValidationError`).
/// This is idiomatic for error enums and improves readability at call sites.
#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Missing object key: {0}")]
    MissingObjectKey(&'static str),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Finalizer error: {0}")]
    FinalizerError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Backup execution failed: {0}")]
    BackupExecFailed(String),

    #[error("Transient error (will retry): {0}")]
    TransientError(String),

    #[error("Permanent error (will not retry): {0}")]
    PermanentError(String),
}

impl Error {
    /// Check if this error indicates a resource was not found
    pub fn is_not_found(&self) -> bool {
        match self {
            Error::NotFound(_) => true,
            Error::KubeError(e) => matches!(e, kube::Error::Api(api_err) if api_err.code == 404),
            _ => false,
        }
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            // Kubernetes API errors are often retryable
            Error::KubeError(e) => {
                // Check for specific non-retryable HTTP codes
                match e {
                    kube::Error::Api(api_err) => {
                        // 4xx errors (except 409 Conflict, 429 TooManyRequests) are usually not retryable
                        let code = api_err.code;
                        if (400..500).contains(&code) {
                            return code == 409 || code == 429;
                        }
                        // 5xx errors are retryable
                        true
                    }
                    // Network and other errors are retryable
                    _ => true,
                }
            }
            // Transient errors are explicitly retryable
            Error::TransientError(_) => true,
            // Permanent errors should not be retried
            Error::PermanentError(_) => false,
            // Configuration and validation errors are permanent
            Error::InvalidConfig(_) => false,
            Error::ValidationError(_) => false,
            // Backup execution failures are retryable
            Error::BackupExecFailed(_) => true,
            // Other errors default to retryable
            Error::SerializationError(_) => false,
            Error::MissingObjectKey(_) => false,
            Error::NotFound(_) => true, // Resource might appear later
            Error::FinalizerError(_) => true,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Exponential backoff configuration
#[derive(Clone, Debug)]
pub struct BackoffConfig {
    /// Initial delay for first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for each subsequent retry
    pub multiplier: f64,
    /// Random jitter factor (0.0 to 1.0)
    pub jitter: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(5),
            max_delay: Duration::from_secs(300), // 5 minutes
            multiplier: 2.0,
            jitter: 0.1,
        }
    }
}

impl BackoffConfig {
    /// Calculate the backoff delay for a given retry attempt
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        // Calculate base delay with exponential backoff
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

    /// Get the delay for an error, with different handling for retryable vs non-retryable
    pub fn delay_for_error(&self, error: &Error, attempt: u32) -> Duration {
        if error.is_retryable() {
            self.delay_for_attempt(attempt)
        } else {
            // For non-retryable errors, use a longer fixed delay
            // This allows for manual intervention or eventual resolution
            self.max_delay
        }
    }
}
