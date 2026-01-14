pub(crate) mod backup_status;
pub mod cleanup;
pub mod context;
pub mod database_reconciler;
pub mod error;
pub mod reconciler;
pub(crate) mod replication_lag;
pub mod state_machine;
pub mod status;
pub mod upgrade_error;
pub mod upgrade_reconciler;
pub mod upgrade_state_machine;
pub mod validation;

// Public exports (used by main.rs, lib.rs, or integration tests)
pub use context::Context;
pub use database_reconciler::{
    DatabaseContext, DatabaseError, database_error_policy, reconcile_database,
};
pub use error::{BackoffConfig, Error, Result};
pub use reconciler::{FINALIZER, error_policy, reconcile};
pub use upgrade_error::{UpgradeBackoffConfig, UpgradeError, UpgradeResult};
pub use upgrade_reconciler::{
    UPGRADE_FINALIZER, UpgradeContext, reconcile_upgrade, upgrade_error_policy,
};
pub use upgrade_state_machine::{
    UpgradeEvent, UpgradeStateMachine, UpgradeTransitionContext, UpgradeTransitionResult,
    determine_upgrade_event,
};
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
