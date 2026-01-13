pub(crate) mod backup_status;
pub mod context;
pub mod database_reconciler;
pub mod error;
pub mod reconciler;
pub(crate) mod replication_lag;
pub mod state_machine;
pub mod status;
pub mod validation;

// Public exports (used by main.rs, lib.rs, or integration tests)
pub use context::Context;
pub use database_reconciler::{
    DatabaseContext, DatabaseError, database_error_policy, reconcile_database,
};
pub use error::{BackoffConfig, Error, Result};
pub use reconciler::{FINALIZER, error_policy, reconcile};
// State machine types used by proptest
pub use state_machine::{ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult};
// Validation types used by proptest and unit tests
pub use validation::{
    MAX_REPLICAS, MIN_REPLICAS, SpecDiff, validate_spec, validate_spec_change,
    validate_version_upgrade,
};
// Status types used by unit tests
pub use status::{
    ConditionBuilder, condition_status, condition_types, get_replica_pod_names, spec_changed,
};
