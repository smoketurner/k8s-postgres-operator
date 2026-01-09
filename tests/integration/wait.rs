//! Wait condition helpers for PostgresCluster resources

use kube::Api;
use kube::runtime::wait::{Condition, await_condition};
use postgres_operator::crd::{ClusterPhase, PostgresCluster};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WaitError {
    #[error("Timeout waiting for condition")]
    Timeout,

    #[error("Watch error: {0}")]
    Watch(#[from] kube::runtime::wait::Error),

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
