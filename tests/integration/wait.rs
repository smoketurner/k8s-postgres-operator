//! Wait condition helpers for PostgresCluster, PostgresDatabase, and PostgresUpgrade resources

use kube::Api;
use kube::runtime::wait::{Condition, await_condition};
use postgres_operator::crd::{
    ClusterPhase, DatabaseConditionType, DatabasePhase, PostgresCluster, PostgresDatabase,
    PostgresUpgrade, UpgradePhase,
};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WaitError {
    #[error("Timeout waiting for condition: {condition}")]
    Timeout { condition: String },

    #[error("Watch error: {0}")]
    Watch(#[from] kube::runtime::wait::Error),

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Resource not found after wait")]
    ResourceNotFound,
}

impl WaitError {
    /// Create a timeout error with condition context
    pub fn timeout(condition: impl Into<String>) -> Self {
        Self::Timeout {
            condition: condition.into(),
        }
    }
}

/// Condition that checks if PostgresCluster is in a specific phase
pub fn is_phase(expected: ClusterPhase) -> impl Condition<PostgresCluster> {
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .map(|status| status.phase == expected)
            .unwrap_or(false)
    }
}

/// Condition that checks if readyReplicas >= expected
pub fn has_ready_replicas(expected: i32) -> impl Condition<PostgresCluster> {
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .map(|status| status.ready_replicas >= expected)
            .unwrap_or(false)
    }
}

/// Condition that checks if primaryPod is set
pub fn has_primary_pod() -> impl Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.primary_pod.as_ref())
            .is_some()
    }
}

/// Condition that checks if status contains a specific error message substring
pub fn has_error(expected_substring: &str) -> impl Condition<PostgresCluster> {
    let expected = expected_substring.to_string();
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.last_error.as_ref())
            .map(|err| err.contains(&expected))
            .unwrap_or(false)
    }
}

/// Condition that checks if a specific condition type has a given status
pub fn has_condition(type_: &str, expected_status: &str) -> impl Condition<PostgresCluster> {
    let cond_type = type_.to_string();
    let status = expected_status.to_string();
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .map(|s| {
                s.conditions
                    .iter()
                    .any(|c| c.type_ == cond_type && c.status == status)
            })
            .unwrap_or(false)
    }
}

/// Condition that checks if TLS is enabled in status
pub fn tls_enabled() -> impl Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.tls_enabled)
            .unwrap_or(false)
    }
}

/// Condition that checks if PgBouncer is enabled in status
pub fn pgbouncer_enabled() -> impl Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.pgbouncer_enabled)
            .unwrap_or(false)
    }
}

/// Condition that checks if PgBouncer has expected ready replicas
pub fn pgbouncer_ready(expected: i32) -> impl Condition<PostgresCluster> {
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.pgbouncer_ready_replicas)
            .map(|ready| ready >= expected)
            .unwrap_or(false)
    }
}

/// Condition that checks if replication lag is populated in status
pub fn has_replication_lag() -> impl Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .map(|status| !status.replication_lag.is_empty())
            .unwrap_or(false)
    }
}

/// Condition that checks if current_version matches expected
pub fn has_current_version(expected: &str) -> impl Condition<PostgresCluster> {
    let version = expected.to_string();
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.current_version.as_ref())
            .map(|v| v == &version)
            .unwrap_or(false)
    }
}

/// Condition that checks if observed_generation matches metadata.generation
pub fn generation_observed() -> impl Condition<PostgresCluster> {
    |obj: Option<&PostgresCluster>| {
        obj.map(|cluster| {
            let generation = cluster.metadata.generation;
            let observed = cluster.status.as_ref().and_then(|s| s.observed_generation);
            generation == observed
        })
        .unwrap_or(false)
    }
}

/// Condition that checks if primaryPod matches a specific pod name
pub fn has_primary_pod_named(expected: &str) -> impl Condition<PostgresCluster> {
    let name = expected.to_string();
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.primary_pod.as_ref())
            .map(|p| p == &name)
            .unwrap_or(false)
    }
}

/// Condition that checks if primaryPod is different from a specific pod name (failover detection)
pub fn primary_pod_changed_from(old_primary: &str) -> impl Condition<PostgresCluster> {
    let old = old_primary.to_string();
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .and_then(|status| status.primary_pod.as_ref())
            .map(|p| p != &old)
            .unwrap_or(false)
    }
}

/// Condition that checks if cluster is fully operational
///
/// This means:
/// - Phase is Running
/// - Ready replicas >= expected count
/// - Primary pod is set
///
/// Use this condition when you need to verify PostgreSQL is actually ready
/// to accept connections, not just that Kubernetes resources are created.
pub fn cluster_operational(expected_replicas: i32) -> impl Condition<PostgresCluster> {
    move |obj: Option<&PostgresCluster>| {
        obj.and_then(|cluster| cluster.status.as_ref())
            .map(|status| {
                status.phase == ClusterPhase::Running
                    && status.ready_replicas >= expected_replicas
                    && status.primary_pod.is_some()
            })
            .unwrap_or(false)
    }
}

/// Wait for a PostgresCluster to reach a condition with timeout
///
/// For better error messages, use `wait_for_cluster_named` which includes the condition name.
pub async fn wait_for_cluster<C>(
    api: &Api<PostgresCluster>,
    name: &str,
    condition: C,
    timeout: Duration,
) -> Result<PostgresCluster, WaitError>
where
    C: Condition<PostgresCluster>,
{
    wait_for_cluster_named(api, name, condition, "condition", timeout).await
}

/// Wait for a PostgresCluster to reach a condition with timeout and named condition
///
/// The `condition_name` parameter is used for error messages when the timeout occurs.
/// Example: `wait_for_cluster_named(&api, "my-cluster", is_phase(Running), "phase=Running", timeout)`
pub async fn wait_for_cluster_named<C>(
    api: &Api<PostgresCluster>,
    name: &str,
    condition: C,
    condition_name: &str,
    timeout: Duration,
) -> Result<PostgresCluster, WaitError>
where
    C: Condition<PostgresCluster>,
{
    let cond = await_condition(api.clone(), name, condition);
    let cond_desc = format!("{} for cluster '{}'", condition_name, name);

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::timeout(&cond_desc))?
        .map_err(WaitError::Watch)?;

    result.ok_or(WaitError::ResourceNotFound)
}

/// Wait for a PostgresCluster to be deleted
pub async fn wait_for_deletion(
    api: &Api<PostgresCluster>,
    name: &str,
    timeout: Duration,
) -> Result<(), WaitError> {
    use kube::runtime::wait::conditions;

    let cond = await_condition(api.clone(), name, conditions::is_deleted(""));
    let cond_desc = format!("deletion of cluster '{}'", name);

    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::timeout(&cond_desc))?
        .map_err(WaitError::Watch)?;

    Ok(())
}

/// Default timeout for wait operations (reduced for faster tests)
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(90);

/// Short timeout for quick checks
pub const SHORT_TIMEOUT: Duration = Duration::from_secs(30);

// =============================================================================
// Generic Resource Watchers
// =============================================================================

/// Condition that checks if a resource exists (is not None)
pub fn exists<T>() -> impl Condition<T>
where
    T: kube::Resource,
{
    |obj: Option<&T>| obj.is_some()
}

/// Wait for any resource to exist using watches
pub async fn wait_for_resource<T>(
    api: &Api<T>,
    name: &str,
    timeout: Duration,
) -> Result<T, WaitError>
where
    T: kube::Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    T: serde::de::DeserializeOwned,
{
    let cond = await_condition(api.clone(), name, exists::<T>());
    let cond_desc = format!("existence of resource '{}'", name);

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::timeout(&cond_desc))?
        .map_err(WaitError::Watch)?;

    result.ok_or(WaitError::ResourceNotFound)
}

/// Wait for any resource to be deleted using watches
pub async fn wait_for_resource_deletion<T>(
    api: &Api<T>,
    name: &str,
    timeout: Duration,
) -> Result<(), WaitError>
where
    T: kube::Resource + Clone + std::fmt::Debug + Send + Sync + 'static,
    T: serde::de::DeserializeOwned,
{
    use kube::runtime::wait::conditions;

    let cond = await_condition(api.clone(), name, conditions::is_deleted(""));
    let cond_desc = format!("deletion of resource '{}'", name);

    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::timeout(&cond_desc))?
        .map_err(WaitError::Watch)?;

    Ok(())
}

// =============================================================================
// PostgresDatabase Wait Conditions
// =============================================================================

/// Condition that checks if PostgresDatabase is in a specific phase
pub fn database_is_phase(expected: DatabasePhase) -> impl Condition<PostgresDatabase> {
    move |obj: Option<&PostgresDatabase>| {
        obj.and_then(|db| db.status.as_ref())
            .map(|status| status.phase == expected)
            .unwrap_or(false)
    }
}

/// Condition that checks if PostgresDatabase is in Ready phase
pub fn database_ready() -> impl Condition<PostgresDatabase> {
    database_is_phase(DatabasePhase::Ready)
}

/// Condition that checks if PostgresDatabase has a specific condition with given status
pub fn database_has_condition(
    condition_type: DatabaseConditionType,
    expected_status: &str,
) -> impl Condition<PostgresDatabase> {
    let status = expected_status.to_string();
    move |obj: Option<&PostgresDatabase>| {
        obj.and_then(|db| db.status.as_ref())
            .map(|s| {
                s.conditions
                    .iter()
                    .any(|c| c.condition_type == condition_type && c.status == status)
            })
            .unwrap_or(false)
    }
}

/// Condition that checks if PostgresDatabase has connection info populated
pub fn database_has_connection_info() -> impl Condition<PostgresDatabase> {
    |obj: Option<&PostgresDatabase>| {
        obj.and_then(|db| db.status.as_ref())
            .and_then(|status| status.connection_info.as_ref())
            .is_some()
    }
}

/// Condition that checks if PostgresDatabase has credential secrets
pub fn database_has_secrets(count: usize) -> impl Condition<PostgresDatabase> {
    move |obj: Option<&PostgresDatabase>| {
        obj.and_then(|db| db.status.as_ref())
            .map(|status| status.credential_secrets.len() >= count)
            .unwrap_or(false)
    }
}

/// Condition that checks if observed_generation matches metadata.generation
pub fn database_generation_observed() -> impl Condition<PostgresDatabase> {
    |obj: Option<&PostgresDatabase>| {
        obj.map(|db| {
            let generation = db.metadata.generation;
            let observed = db.status.as_ref().and_then(|s| s.observed_generation);
            generation == observed
        })
        .unwrap_or(false)
    }
}

/// Wait for a PostgresDatabase to reach a condition with timeout
///
/// For better error messages, use `wait_for_database_named` which includes the condition name.
pub async fn wait_for_database<C>(
    api: &Api<PostgresDatabase>,
    name: &str,
    condition: C,
    timeout: Duration,
) -> Result<PostgresDatabase, WaitError>
where
    C: Condition<PostgresDatabase>,
{
    wait_for_database_named(api, name, condition, "condition", timeout).await
}

/// Wait for a PostgresDatabase to reach a condition with timeout and named condition
///
/// The `condition_name` parameter is used for error messages when the timeout occurs.
/// Example: `wait_for_database_named(&api, "my-db", database_ready(), "phase=Ready", timeout)`
pub async fn wait_for_database_named<C>(
    api: &Api<PostgresDatabase>,
    name: &str,
    condition: C,
    condition_name: &str,
    timeout: Duration,
) -> Result<PostgresDatabase, WaitError>
where
    C: Condition<PostgresDatabase>,
{
    let cond = await_condition(api.clone(), name, condition);
    let cond_desc = format!("{} for database '{}'", condition_name, name);

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::timeout(&cond_desc))?
        .map_err(WaitError::Watch)?;

    result.ok_or(WaitError::ResourceNotFound)
}

/// Wait for a PostgresDatabase to be completely deleted (returns 404)
///
/// This polls until the resource is actually gone, not just marked for deletion.
/// This ensures finalizers have completed.
pub async fn wait_for_database_deletion(
    api: &Api<PostgresDatabase>,
    name: &str,
    timeout: Duration,
) -> Result<(), WaitError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(500);

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(WaitError::timeout(format!(
                "deletion of database '{}'",
                name
            )));
        }

        match api.get(name).await {
            Ok(_) => {
                // Resource still exists, wait and retry
                tokio::time::sleep(poll_interval).await;
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 404 => {
                // Resource is gone, success!
                return Ok(());
            }
            Err(e) => {
                // Unexpected error
                return Err(WaitError::KubeError(e));
            }
        }
    }
}

/// Timeout for database operations (shorter than cluster operations)
pub const DATABASE_TIMEOUT: Duration = Duration::from_secs(60);

// =============================================================================
// PostgresUpgrade Wait Conditions
// =============================================================================

/// Condition that checks if PostgresUpgrade is in a specific phase
pub fn upgrade_is_phase(expected: UpgradePhase) -> impl Condition<PostgresUpgrade> {
    move |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| status.phase == expected)
            .unwrap_or(false)
    }
}

/// Condition that checks if PostgresUpgrade is NOT in a specific phase
pub fn upgrade_not_phase(excluded: UpgradePhase) -> impl Condition<PostgresUpgrade> {
    move |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| status.phase != excluded)
            .unwrap_or(false)
    }
}

/// Condition that checks if PostgresUpgrade is in a terminal phase (Completed, Failed, or RolledBack)
pub fn upgrade_terminal() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| status.phase.is_terminal())
            .unwrap_or(false)
    }
}

/// Condition that checks if replication lag is zero (fully synced)
pub fn upgrade_replication_lag_zero() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.replication.as_ref())
            .and_then(|repl| repl.lag_bytes)
            .map(|lag| lag == 0)
            .unwrap_or(false)
    }
}

/// Condition that checks if replication lag is populated
pub fn upgrade_has_replication_status() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.replication.as_ref())
            .is_some()
    }
}

/// Condition that checks if verification has reached minimum passes
pub fn upgrade_verification_passes(min: i32) -> impl Condition<PostgresUpgrade> {
    move |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.verification.as_ref())
            .map(|verif| verif.consecutive_passes >= min)
            .unwrap_or(false)
    }
}

/// Condition that checks if row count verification has zero mismatches
pub fn upgrade_verification_no_mismatches() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.verification.as_ref())
            .map(|verif| verif.tables_mismatched == 0)
            .unwrap_or(false)
    }
}

/// Condition that checks if sequences have been synced
pub fn upgrade_sequences_synced() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.sequences.as_ref())
            .map(|seq| seq.synced)
            .unwrap_or(false)
    }
}

/// Condition that checks if target cluster is ready
pub fn upgrade_target_ready() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.target_cluster.as_ref())
            .map(|target| target.ready)
            .unwrap_or(false)
    }
}

/// Condition that checks if source cluster is ready
pub fn upgrade_source_ready() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.source_cluster.as_ref())
            .map(|source| source.ready)
            .unwrap_or(false)
    }
}

/// Condition that checks if upgrade has a specific condition type with given status
pub fn upgrade_has_condition(
    type_: &str,
    expected_status: &str,
) -> impl Condition<PostgresUpgrade> {
    let cond_type = type_.to_string();
    let status = expected_status.to_string();
    move |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|s| {
                s.conditions
                    .iter()
                    .any(|c| c.type_ == cond_type && c.status == status)
            })
            .unwrap_or(false)
    }
}

/// Condition that checks if observed_generation matches metadata.generation
pub fn upgrade_generation_observed() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.map(|upgrade| {
            let generation = upgrade.metadata.generation;
            let observed = upgrade.status.as_ref().and_then(|s| s.observed_generation);
            generation == observed
        })
        .unwrap_or(false)
    }
}

/// Condition that checks if upgrade has an error message
pub fn upgrade_has_error() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .and_then(|status| status.last_error.as_ref())
            .is_some()
    }
}

/// Composite condition: Upgrade is past Pending phase (validation passed)
pub fn upgrade_past_pending() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| status.phase != UpgradePhase::Pending)
            .unwrap_or(false)
    }
}

/// Composite condition: Upgrade is in a phase where replication is active
pub fn upgrade_replicating_or_later() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| {
                matches!(
                    status.phase,
                    UpgradePhase::Replicating
                        | UpgradePhase::Verifying
                        | UpgradePhase::SyncingSequences
                        | UpgradePhase::ReadyForCutover
                        | UpgradePhase::WaitingForManualCutover
                        | UpgradePhase::CuttingOver
                        | UpgradePhase::HealthChecking
                        | UpgradePhase::Completed
                )
            })
            .unwrap_or(false)
    }
}

/// Composite condition: Upgrade is ready for cutover (either ReadyForCutover or WaitingForManualCutover)
pub fn upgrade_ready_for_cutover() -> impl Condition<PostgresUpgrade> {
    |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| {
                matches!(
                    status.phase,
                    UpgradePhase::ReadyForCutover | UpgradePhase::WaitingForManualCutover
                )
            })
            .unwrap_or(false)
    }
}

/// Upgrade has progressed past a specific phase
pub fn upgrade_past_phase(target: UpgradePhase) -> impl Condition<PostgresUpgrade> {
    move |obj: Option<&PostgresUpgrade>| {
        obj.and_then(|upgrade| upgrade.status.as_ref())
            .map(|status| {
                // Define phase ordering for comparison
                let phase_order = |p: &UpgradePhase| match p {
                    UpgradePhase::Pending => 0,
                    UpgradePhase::CreatingTarget => 1,
                    UpgradePhase::ConfiguringReplication => 2,
                    UpgradePhase::Replicating => 3,
                    UpgradePhase::Verifying => 4,
                    UpgradePhase::SyncingSequences => 5,
                    UpgradePhase::ReadyForCutover => 6,
                    UpgradePhase::WaitingForManualCutover => 7,
                    UpgradePhase::CuttingOver => 8,
                    UpgradePhase::HealthChecking => 9,
                    UpgradePhase::Completed => 10,
                    UpgradePhase::Failed => 100,
                    UpgradePhase::RolledBack => 101,
                };
                phase_order(&status.phase) > phase_order(&target)
            })
            .unwrap_or(false)
    }
}

/// Wait for a PostgresUpgrade to reach a condition with timeout
pub async fn wait_for_upgrade<C>(
    api: &Api<PostgresUpgrade>,
    name: &str,
    condition: C,
    timeout: Duration,
) -> Result<PostgresUpgrade, WaitError>
where
    C: Condition<PostgresUpgrade>,
{
    wait_for_upgrade_named(api, name, condition, "condition", timeout).await
}

/// Wait for a PostgresUpgrade to reach a condition with timeout and named condition
pub async fn wait_for_upgrade_named<C>(
    api: &Api<PostgresUpgrade>,
    name: &str,
    condition: C,
    condition_name: &str,
    timeout: Duration,
) -> Result<PostgresUpgrade, WaitError>
where
    C: Condition<PostgresUpgrade>,
{
    let cond = await_condition(api.clone(), name, condition);
    let cond_desc = format!("{} for upgrade '{}'", condition_name, name);

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::timeout(&cond_desc))?
        .map_err(WaitError::Watch)?;

    result.ok_or(WaitError::ResourceNotFound)
}

/// Wait for a PostgresUpgrade to be completely deleted
pub async fn wait_for_upgrade_deletion(
    api: &Api<PostgresUpgrade>,
    name: &str,
    timeout: Duration,
) -> Result<(), WaitError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(500);

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(WaitError::timeout(format!(
                "deletion of upgrade '{}'",
                name
            )));
        }

        match api.get(name).await {
            Ok(_) => {
                // Resource still exists, wait and retry
                tokio::time::sleep(poll_interval).await;
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 404 => {
                // Resource is gone, success!
                return Ok(());
            }
            Err(e) => {
                // Unexpected error
                return Err(WaitError::KubeError(e));
            }
        }
    }
}

/// Timeout for upgrade operations (longer due to cluster creation)
pub const UPGRADE_TIMEOUT: Duration = Duration::from_secs(600);

/// Short timeout for upgrade phase transitions
pub const UPGRADE_PHASE_TIMEOUT: Duration = Duration::from_secs(300);
