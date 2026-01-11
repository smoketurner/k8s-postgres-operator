pub mod controller;
pub mod crd;
pub mod health;
pub mod resources;
pub mod webhooks;

pub use controller::{
    BackoffConfig, Context, DatabaseContext, DatabaseError, Error, FINALIZER, Result,
    database_error_policy, error_policy, reconcile, reconcile_database,
};
pub use crd::{PostgresCluster, PostgresDatabase};
pub use health::{HealthState, Metrics};
pub use webhooks::{
    create_webhook_router, run_webhook_server, validate_postgres_cluster, WebhookError,
    WEBHOOK_CERT_PATH, WEBHOOK_KEY_PATH, WEBHOOK_PORT,
};

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::runtime::Controller;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::{Api, Client};

/// Run the operator controller
///
/// This is the main controller loop that watches PostgresCluster resources
/// and reconciles them. It can be called from main.rs or spawned as a
/// background task during integration tests.
///
/// If health_state is provided, metrics will be recorded for reconciliations.
pub async fn run_controller(client: Client, health_state: Option<Arc<HealthState>>) {
    tracing::info!("Starting controller for PostgresCluster resources");

    // Mark as ready once we start the controller
    if let Some(ref state) = health_state {
        state.set_ready(true).await;
    }

    let ctx = Arc::new(Context::new(client.clone(), health_state));

    // Set up APIs for the controller
    let clusters: Api<PostgresCluster> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());
    let configmaps: Api<ConfigMap> = Api::all(client.clone());
    let secrets: Api<Secret> = Api::all(client.clone());
    let pdbs: Api<PodDisruptionBudget> = Api::all(client.clone());

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

/// Run the database controller
///
/// This controller watches PostgresDatabase resources and provisions databases,
/// roles, and credentials within PostgresCluster instances.
pub async fn run_database_controller(client: Client) {
    tracing::info!("Starting controller for PostgresDatabase resources");

    let ctx = Arc::new(DatabaseContext::new(client.clone()));

    // Set up APIs for the controller
    let databases: Api<PostgresDatabase> = Api::all(client.clone());
    let secrets: Api<Secret> = Api::all(client.clone());

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
