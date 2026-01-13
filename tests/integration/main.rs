// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

//! Integration tests for postgres-operator
//!
//! These tests require a running Kubernetes cluster accessible via kubeconfig.
//! Tests are marked with #[ignore] and must be run explicitly:
//!
//! ```bash
//! # Run all functional tests (parallel execution supported)
//! cargo test --test integration -- --ignored
//!
//! # Run specific test
//! cargo test --test integration test_cluster_creates_statefulset -- --ignored
//! ```
//!
//! The tests use your existing kubeconfig (~/.kube/config or KUBECONFIG env var).
//!
//! ## Design Principles
//!
//! - **Parallel Test Execution**: Each test creates its own namespace and scoped
//!   operator, enabling concurrent test runs without interference
//! - **RAII Cleanup**: TestNamespace implements Drop for automatic cleanup even on panic
//! - **Watch-Based Waiting**: Uses kube-rs watches for efficient resource detection
//! - **Fast Execution**: Tests complete in seconds, not minutes
//! - **Isolation**: Each test gets its own namespace and operator instance

// Shared test fixtures (used by unit, integration, and proptest)
#[path = "../common/mod.rs"]
mod common;

// Test infrastructure modules
mod assertions;
mod cluster;
mod crd;
mod namespace;
mod operator;
mod port_forward;
mod postgres;
mod wait;

// Test modules
mod connectivity_tests;
mod database_tests;
mod functional_tests;
mod scaling_tests;
mod tls_tests;

// Re-export common test utilities
pub use assertions::*;
pub use cluster::*;
pub use common::*;
pub use crd::*;
pub use namespace::*;
pub use operator::*;
pub use port_forward::*;
pub use postgres::*;
pub use wait::*;
