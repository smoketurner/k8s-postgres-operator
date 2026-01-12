//! Wait condition helpers for PostgresCluster and PostgresDatabase resources

use kube::Api;
use kube::runtime::wait::{Condition, await_condition};
use postgres_operator::crd::{
    ClusterPhase, DatabaseConditionType, DatabasePhase, PostgresCluster, PostgresDatabase,
};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WaitError {
    #[error("Timeout waiting for condition")]
    Timeout,

    #[error("Watch error: {0}")]
    Watch(#[from] kube::runtime::wait::Error),

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Resource not found after wait")]
    ResourceNotFound,
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
pub async fn wait_for_cluster<C>(
    api: &Api<PostgresCluster>,
    name: &str,
    condition: C,
    timeout: Duration,
) -> Result<PostgresCluster, WaitError>
where
    C: Condition<PostgresCluster>,
{
    let cond = await_condition(api.clone(), name, condition);

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::Timeout)?
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

    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::Timeout)?
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

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::Timeout)?
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

    tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::Timeout)?
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
pub async fn wait_for_database<C>(
    api: &Api<PostgresDatabase>,
    name: &str,
    condition: C,
    timeout: Duration,
) -> Result<PostgresDatabase, WaitError>
where
    C: Condition<PostgresDatabase>,
{
    let cond = await_condition(api.clone(), name, condition);

    let result = tokio::time::timeout(timeout, cond)
        .await
        .map_err(|_| WaitError::Timeout)?
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
            return Err(WaitError::Timeout);
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
