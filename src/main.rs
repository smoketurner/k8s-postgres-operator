use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::Controller;
use kube::{Api, Client};
use tracing::{error, info};

use postgres_operator::{error_policy, reconcile, Context, PostgresCluster};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging with JSON format for production
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("postgres_operator=info".parse()?)
                .add_directive("kube=info".parse()?),
        )
        .init();

    info!("Starting postgres-operator (Kubernetes 1.35)");

    // Create Kubernetes client
    let client = Client::try_default().await?;
    info!("Connected to Kubernetes cluster");

    // Create shared context
    let ctx = Arc::new(Context::new(client.clone()));

    // Set up APIs for the controller
    let clusters: Api<PostgresCluster> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());
    let configmaps: Api<ConfigMap> = Api::all(client.clone());
    let secrets: Api<Secret> = Api::all(client.clone());
    let pdbs: Api<PodDisruptionBudget> = Api::all(client.clone());

    // Print CRD info
    info!(
        "Watching PostgresCluster resources (apiVersion: postgres.example.com/v1alpha1)"
    );

    // Create and run the controller
    // Watches PostgresCluster CRD and all owned resources
    Controller::new(clusters, WatcherConfig::default())
        // Watch owned resources to trigger reconciliation when they change
        .owns(statefulsets, WatcherConfig::default())
        .owns(services, WatcherConfig::default())
        .owns(configmaps, WatcherConfig::default())
        .owns(secrets, WatcherConfig::default())
        .owns(pdbs, WatcherConfig::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!("Reconciled: {}", obj.name);
                }
                Err(e) => {
                    error!("Reconciliation error: {:?}", e);
                }
            }
        })
        .await;

    info!("Controller stopped");
    Ok(())
}
