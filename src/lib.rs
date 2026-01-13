pub mod controller;
pub mod crd;
pub mod health;
pub mod resources;
pub mod webhooks;

pub use controller::{
    BackoffConfig, Context, DatabaseContext, DatabaseError, Error, FINALIZER, Result,
    UPGRADE_FINALIZER, UpgradeBackoffConfig, UpgradeContext, UpgradeError, UpgradeResult,
    database_error_policy, error_policy, reconcile, reconcile_database, reconcile_upgrade,
    upgrade_error_policy,
};
pub use crd::{PostgresCluster, PostgresDatabase, PostgresUpgrade};
pub use health::{HealthState, Metrics};
pub use webhooks::{
    WEBHOOK_CERT_PATH, WEBHOOK_KEY_PATH, WEBHOOK_PORT, WebhookError, run_webhook_server,
};

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::runtime::Controller;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;

/// Helper to create a namespaced or cluster-wide API based on scope.
fn scoped_api<T>(client: Client, namespace: Option<&str>) -> Api<T>
where
    T: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <T as Resource>::DynamicType: Default,
    T: Clone + DeserializeOwned + std::fmt::Debug,
{
    match namespace {
        Some(ns) => Api::namespaced(client, ns),
        None => Api::all(client),
    }
}

/// Run the operator controller (cluster-wide).
///
/// This is the main controller loop that watches PostgresCluster resources
/// and reconciles them. It can be called from main.rs or spawned as a
/// background task during integration tests.
///
/// If health_state is provided, metrics will be recorded for reconciliations.
pub async fn run_controller(client: Client, health_state: Option<Arc<HealthState>>) {
    run_controller_scoped(client, health_state, None).await
}

/// Run the operator controller with optional namespace scoping.
///
/// When `namespace` is `Some(ns)`, only watches resources in that namespace.
/// When `namespace` is `None`, watches resources cluster-wide.
///
/// Use the scoped version for integration tests to enable parallel test execution.
pub async fn run_controller_scoped(
    client: Client,
    health_state: Option<Arc<HealthState>>,
    namespace: Option<&str>,
) {
    let scope_msg = namespace.unwrap_or("cluster-wide");
    tracing::info!(
        "Starting controller for PostgresCluster resources (scope: {})",
        scope_msg
    );

    // Mark as ready once we start the controller
    if let Some(ref state) = health_state {
        state.set_ready(true).await;
    }

    let ctx = Arc::new(Context::new(client.clone(), health_state));

    // Set up APIs for the controller (namespaced or cluster-wide)
    let clusters: Api<PostgresCluster> = scoped_api(client.clone(), namespace);
    let statefulsets: Api<StatefulSet> = scoped_api(client.clone(), namespace);
    let services: Api<Service> = scoped_api(client.clone(), namespace);
    let configmaps: Api<ConfigMap> = scoped_api(client.clone(), namespace);
    let secrets: Api<Secret> = scoped_api(client.clone(), namespace);
    let pdbs: Api<PodDisruptionBudget> = scoped_api(client.clone(), namespace);

    // Configure watcher to handle dynamic resource creation
    // Use any_semantic() for more reliable resource discovery in test environments
    let watcher_config = WatcherConfig::default().any_semantic();

    // Create and run the controller
    // Watch PostgresCluster and all owned resources to trigger reconciliation
    Controller::new(clusters, watcher_config.clone())
        .owns(statefulsets, watcher_config.clone())
        .owns(services, watcher_config.clone())
        .owns(configmaps, watcher_config.clone())
        .owns(secrets, watcher_config.clone())
        .owns(pdbs, watcher_config)
        .run(reconcile, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    tracing::debug!("Reconciled: {}", obj.name);
                }
                Err(e) => {
                    // ObjectNotFound/NotFound errors are expected after deletion when
                    // related watch events trigger reconciliation for a deleted object.
                    // Log these at debug level instead of error.
                    let is_not_found = matches!(
                        &e,
                        kube::runtime::controller::Error::ReconcilerFailed(err, _) if err.is_not_found()
                    );
                    if is_not_found {
                        tracing::debug!("Object no longer exists (likely deleted): {:?}", e);
                    } else {
                        tracing::error!("Reconciliation error: {:?}", e);
                    }
                }
            }
        })
        .await;

    // This should never complete in normal operation
    tracing::error!("Controller stream ended unexpectedly");
}

/// Run the database controller (cluster-wide).
///
/// This controller watches PostgresDatabase resources and provisions databases,
/// roles, and credentials within PostgresCluster instances.
pub async fn run_database_controller(client: Client) {
    run_database_controller_scoped(client, None).await
}

/// Run the database controller with optional namespace scoping.
///
/// When `namespace` is `Some(ns)`, only watches resources in that namespace.
/// When `namespace` is `None`, watches resources cluster-wide.
pub async fn run_database_controller_scoped(client: Client, namespace: Option<&str>) {
    let scope_msg = namespace.unwrap_or("cluster-wide");
    tracing::info!(
        "Starting controller for PostgresDatabase resources (scope: {})",
        scope_msg
    );

    let ctx = Arc::new(DatabaseContext::new(client.clone()));

    // Set up APIs for the controller (namespaced or cluster-wide)
    let databases: Api<PostgresDatabase> = scoped_api(client.clone(), namespace);
    let secrets: Api<Secret> = scoped_api(client.clone(), namespace);

    // Configure watcher
    let watcher_config = WatcherConfig::default().any_semantic();

    // Create and run the controller
    // Watch PostgresDatabase and owned secrets
    Controller::new(databases, watcher_config.clone())
        .owns(secrets, watcher_config)
        .run(reconcile_database, database_error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    tracing::debug!("Reconciled database: {}", obj.name);
                }
                Err(e) => {
                    let is_not_found = matches!(
                        &e,
                        kube::runtime::controller::Error::ReconcilerFailed(err, _)
                            if format!("{:?}", err).contains("NotFound")
                    );
                    if is_not_found {
                        tracing::debug!("Database object no longer exists: {:?}", e);
                    } else {
                        tracing::error!("Database reconciliation error: {:?}", e);
                    }
                }
            }
        })
        .await;

    tracing::error!("Database controller stream ended unexpectedly");
}

/// Run the upgrade controller (cluster-wide).
///
/// This controller watches PostgresUpgrade resources and manages blue-green
/// major version upgrades using logical replication.
pub async fn run_upgrade_controller(client: Client) {
    run_upgrade_controller_scoped(client, None).await
}

/// Run the upgrade controller with optional namespace scoping.
///
/// When `namespace` is `Some(ns)`, only watches resources in that namespace.
/// When `namespace` is `None`, watches resources cluster-wide.
pub async fn run_upgrade_controller_scoped(client: Client, namespace: Option<&str>) {
    let scope_msg = namespace.unwrap_or("cluster-wide");
    tracing::info!(
        "Starting controller for PostgresUpgrade resources (scope: {})",
        scope_msg
    );

    let ctx = Arc::new(UpgradeContext::new(client.clone()));

    // Set up APIs for the controller (namespaced or cluster-wide)
    let upgrades: Api<PostgresUpgrade> = scoped_api(client.clone(), namespace);

    // Configure watcher
    let watcher_config = WatcherConfig::default().any_semantic();

    // Create and run the controller
    // Watch PostgresUpgrade resources
    // Note: Target clusters are NOT owned by the upgrade resource (by design)
    // to ensure they survive upgrade deletion
    Controller::new(upgrades, watcher_config)
        .run(reconcile_upgrade, upgrade_error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    tracing::debug!("Reconciled upgrade: {}", obj.name);
                }
                Err(e) => {
                    let is_not_found = matches!(
                        &e,
                        kube::runtime::controller::Error::ReconcilerFailed(err, _)
                            if format!("{:?}", err).contains("NotFound")
                    );
                    if is_not_found {
                        tracing::debug!("Upgrade object no longer exists: {:?}", e);
                    } else {
                        tracing::error!("Upgrade reconciliation error: {:?}", e);
                    }
                }
            }
        })
        .await;

    tracing::error!("Upgrade controller stream ended unexpectedly");
}
