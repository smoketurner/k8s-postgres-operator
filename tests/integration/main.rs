//! Integration tests for postgres-operator
//!
//! These tests require a running Kubernetes cluster accessible via kubeconfig.
//! Tests are marked with #[ignore] and must be run explicitly:
//!
//! ```bash
//! cargo test --test integration -- --ignored --test-threads=1
//! ```
//!
//! The tests use your existing kubeconfig (~/.kube/config or KUBECONFIG env var).
//! Note: Tests run sequentially to avoid conflicts.

mod assertions;
mod cluster;
mod crd;
mod fixtures;
mod namespace;
mod operator;
mod wait;

// Test modules
mod keda_scaling_tests;
mod slow_tests;
mod tests;

// Re-export common test utilities
pub use assertions::*;
pub use cluster::*;
pub use crd::*;
pub use fixtures::*;
pub use namespace::*;
pub use operator::*;
pub use wait::*;
