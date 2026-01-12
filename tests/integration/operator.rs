//! Operator spawning utilities for integration tests
//!
//! Each test gets its own operator instance to avoid watch/state issues
//! between tests. The operator runs in the test's tokio runtime.

use kube::Client;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A scoped operator that runs for the duration of a test
pub struct ScopedOperator {
    handle: JoinHandle<()>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl ScopedOperator {
    /// Start a new operator instance
    ///
    /// The operator will watch PostgresCluster resources and reconcile them.
    /// It will be automatically stopped when dropped.
    pub async fn start(client: Client) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tracing::info!("Starting scoped operator controller...");

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_operator(client) => {
                    tracing::debug!("Operator exited normally");
                }
                _ = shutdown_rx => {
                    tracing::debug!("Operator received shutdown signal");
                }
            }
        });

        // Give the controller a moment to start watching
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// Start operators for both PostgresCluster and PostgresDatabase
    ///
    /// This runs both the cluster controller and the database controller
    /// concurrently. Use this for tests that need PostgresDatabase functionality.
    pub async fn start_with_database(client: Client) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        tracing::info!("Starting scoped operators (cluster + database)...");

        let cluster_client = client.clone();
        let database_client = client;

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_operator(cluster_client) => {
                    tracing::debug!("Cluster operator exited normally");
                }
                _ = run_database_operator(database_client) => {
                    tracing::debug!("Database operator exited normally");
                }
                _ = shutdown_rx => {
                    tracing::debug!("Operators received shutdown signal");
                }
            }
        });

        // Give the controllers a moment to start watching
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

impl Drop for ScopedOperator {
    fn drop(&mut self) {
        // Send shutdown signal (ignore error if receiver already dropped)
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Abort the task to ensure it stops
        self.handle.abort();
    }
}

/// Run the operator controller
async fn run_operator(client: Client) {
    use futures::StreamExt;
    use k8s_openapi::api::apps::v1::StatefulSet;
    use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
    use k8s_openapi::api::policy::v1::PodDisruptionBudget;
    use kube::Api;
    use kube::runtime::Controller;
    use kube::runtime::watcher::Config as WatcherConfig;
    use postgres_operator::crd::PostgresCluster;
    use postgres_operator::{Context, error_policy, reconcile};

    let ctx = Arc::new(Context::new(client.clone(), None));

    let clusters: Api<PostgresCluster> = Api::all(client.clone());
    let statefulsets: Api<StatefulSet> = Api::all(client.clone());
    let services: Api<Service> = Api::all(client.clone());
    let configmaps: Api<ConfigMap> = Api::all(client.clone());
    let secrets: Api<Secret> = Api::all(client.clone());
    let pdbs: Api<PodDisruptionBudget> = Api::all(client.clone());

    let watcher_config = WatcherConfig::default().any_semantic();

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
}

/// Helper function for backward compatibility
/// Starts a scoped operator that will be properly cleaned up
pub async fn ensure_operator_running(client: Client) -> ScopedOperator {
    ScopedOperator::start(client).await
}

/// Starts both cluster and database operators
/// Use this for PostgresDatabase integration tests
pub async fn ensure_operator_running_with_database(client: Client) -> ScopedOperator {
    ScopedOperator::start_with_database(client).await
}

/// Run the database operator controller
async fn run_database_operator(client: Client) {
    use futures::StreamExt;
    use k8s_openapi::api::core::v1::Secret;
    use kube::Api;
    use kube::runtime::Controller;
    use kube::runtime::watcher::Config as WatcherConfig;
    use postgres_operator::crd::PostgresDatabase;
    use postgres_operator::{DatabaseContext, database_error_policy, reconcile_database};

    let ctx = Arc::new(DatabaseContext::new(client.clone()));

    let databases: Api<PostgresDatabase> = Api::all(client.clone());
    let secrets: Api<Secret> = Api::all(client.clone());

    let watcher_config = WatcherConfig::default().any_semantic();

    Controller::new(databases, watcher_config.clone())
        .owns(secrets, watcher_config)
        .run(reconcile_database, database_error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    tracing::debug!("Reconciled database: {}", obj.name);
                }
                Err(e) => {
                    tracing::error!("Database reconciliation error: {:?}", e);
                }
            }
        })
        .await;
}
