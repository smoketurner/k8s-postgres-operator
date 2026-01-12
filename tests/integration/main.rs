//! Functional tests for postgres-operator
//!
//! These tests require a running Kubernetes cluster accessible via kubeconfig.
//! Tests are marked with #[ignore] and must be run explicitly:
//!
//! ```bash
//! # Run all functional tests
//! cargo test --test integration -- --ignored --test-threads=1
//!
//! # Run specific test
//! cargo test --test integration test_cluster_creates_statefulset -- --ignored
//! ```
//!
//! The tests use your existing kubeconfig (~/.kube/config or KUBECONFIG env var).
//! Tests run sequentially to avoid conflicts.
//!
//! ## Design Principles
//!
//! - **RAII Cleanup**: TestNamespace implements Drop for automatic cleanup even on panic
//! - **Simple Polling**: Uses direct API polling instead of event watching for speed
//! - **Fast Execution**: Tests complete in seconds, not minutes
//! - **Isolation**: Each test gets its own namespace

// Test infrastructure modules
mod assertions;
mod cluster;
mod crd;
mod fixtures;
mod namespace;
mod operator;
mod port_forward;
mod postgres;
mod wait;

// Test modules
mod connectivity_tests;
mod functional_tests;
mod tls_tests;

// Re-export common test utilities
pub use assertions::*;
pub use cluster::*;
pub use crd::*;
pub use fixtures::*;
pub use namespace::*;
pub use operator::*;
pub use port_forward::*;
pub use postgres::*;
pub use wait::*;
