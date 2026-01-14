pub mod cleanup;
pub(crate) mod cluster_backup_status;
pub mod cluster_error;
pub mod cluster_reconciler;
pub(crate) mod cluster_replication_lag;
pub mod cluster_state_machine;
pub mod cluster_status;
pub mod cluster_validation;
pub mod context;
pub mod database_reconciler;
pub mod upgrade_error;
pub mod upgrade_reconciler;
pub mod upgrade_state_machine;

// Public exports (used by main.rs, lib.rs, or integration tests)
pub use cluster_error::{BackoffConfig, Error, Result};
pub use cluster_reconciler::{FINALIZER, error_policy, reconcile};
pub use context::Context;
pub use database_reconciler::{
    DatabaseContext, DatabaseError, database_error_policy, reconcile_database,
};
pub use upgrade_error::{UpgradeBackoffConfig, UpgradeError, UpgradeResult};
pub use upgrade_reconciler::{
    UPGRADE_FINALIZER, UpgradeContext, reconcile_upgrade, upgrade_error_policy,
};
pub use upgrade_state_machine::{
    UpgradeEvent, UpgradeStateMachine, UpgradeTransitionContext, UpgradeTransitionResult,
    determine_upgrade_event,
};
// State machine types used by proptest
pub use cluster_state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult,
};
// Validation types used by proptest and unit tests
pub use cluster_validation::{
    MAX_REPLICAS, MIN_REPLICAS, SpecDiff, validate_spec, validate_spec_change,
    validate_version_upgrade,
};
// Status types used by unit tests
pub use cluster_status::{
    ConditionBuilder, condition_status, condition_types, get_replica_pod_names, spec_changed,
};
