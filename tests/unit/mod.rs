//! Unit tests for the PostgreSQL Operator
//!
//! This module contains unit tests for:
//! - Resource generators (StatefulSet, Service, ConfigMap, Secret, PDB)
//! - Validation logic
//! - Status management
//! - State machine transitions
//! - Error handling

mod resources;
mod state_machine;
mod status;
mod validation;
