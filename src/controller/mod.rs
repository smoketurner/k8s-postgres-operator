pub mod cleanup;
pub(crate) mod cluster_backup_status;
pub mod cluster_error;
pub mod cluster_reconciler;
pub(crate) mod cluster_replication_lag;
pub mod cluster_state_machine;
pub mod cluster_status;
pub mod cluster_validation;
pub mod conditions;
pub mod context;
pub mod database_reconciler;
pub mod events;
pub mod finalizer;
pub mod upgrade_error;
pub mod upgrade_preflight;
pub mod upgrade_reconciler;
pub mod upgrade_state_machine;

// Public exports (used by main.rs, lib.rs, or integration tests)
pub use cluster_error::{Error, Result};
pub use cluster_reconciler::{error_policy, reconcile};
pub use context::Context;
pub use database_reconciler::{DatabaseContext, database_error_policy, reconcile_database};
pub use upgrade_reconciler::{UpgradeContext, reconcile_upgrade, upgrade_error_policy};
