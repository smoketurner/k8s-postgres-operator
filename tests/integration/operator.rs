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

        Self { handle, shutdown_tx: Some(shutdown_tx) }
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
    use kube::runtime::watcher::Config as WatcherConfig;
    use kube::runtime::Controller;
    use kube::Api;
    use postgres_operator::crd::PostgresCluster;
    use postgres_operator::{error_policy, reconcile, Context};

    let ctx = Arc::new(Context::new(client.clone()));

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
