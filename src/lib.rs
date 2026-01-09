pub mod controller;
pub mod crd;
pub mod postgres;
pub mod resources;

pub use controller::{error_policy, reconcile, BackoffConfig, Context, Error, Result, FINALIZER};
pub use crd::PostgresCluster;

use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::Controller;
use kube::{Api, Client};

/// Run the operator controller
///
/// This is the main controller loop that watches PostgresCluster resources
/// and reconciles them. It can be called from main.rs or spawned as a
/// background task during integration tests.
pub async fn run_controller(client: Client) {
    tracing::info!("Starting controller for PostgresCluster resources");

    let ctx = Arc::new(Context::new(client.clone()));

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
                    tracing::error!("Reconciliation error: {:?}", e);
                }
            }
        })
        .await;
    
    // This should never complete in normal operation
    tracing::error!("Controller stream ended unexpectedly");
}
