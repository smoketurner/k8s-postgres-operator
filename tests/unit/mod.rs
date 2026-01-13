// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

//! Unit tests for the PostgreSQL Operator
//!
//! This module contains unit tests for:
//! - Resource generators (StatefulSet, Service, ConfigMap, Secret, PDB, NetworkPolicy)
//! - Validation logic
//! - Status management
//! - State machine transitions
//! - Error handling
//! - NetworkPolicy and pg_hba security
//! - PostgresDatabase CRD and provisioning
//! - Admission webhook policies

mod database;
mod network_policy;
mod resources;
mod state_machine;
mod status;
mod validation;
mod webhooks;
