// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

//! Common test utilities and fixtures shared across all test targets
//!
//! This module provides reusable builders and helpers for creating test resources.
//! It is shared between unit tests, integration tests, and property tests.
//!
//! # Usage
//!
//! Include this module in your test file:
//! ```rust,ignore
//! #[path = "../common/mod.rs"]
//! mod common;
//! use common::*;
//! ```

mod fixtures;

pub use fixtures::*;
