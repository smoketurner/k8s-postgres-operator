//! Operator spawning utilities for integration tests
//!
//! Each test gets its own operator instance scoped to a specific namespace.
//! This enables test parallelization - multiple tests can run concurrently
//! without their operators interfering with each other.
//!
//! The operator runs in the test's tokio runtime and is automatically
//! stopped when dropped.
//!
//! ## Usage
//!
//! ```rust,ignore
//! let ns = TestNamespace::create(client.clone(), "my-test").await?;
//! let operator = ScopedOperator::start(client.clone(), ns.name()).await;
//!
//! // Run tests...
//! // Operator stops automatically when dropped
//! ```
//!
//! ## Design
//!
//! The scoped operator uses `Api::namespaced()` instead of `Api::all()` to
//! watch only resources in a specific namespace. This prevents tests from
//! interfering with each other when running in parallel.

use kube::Client;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A scoped operator that runs for the duration of a test
///
/// The operator only watches resources in its configured namespace,
/// enabling parallel test execution without interference.
pub struct ScopedOperator {
    handle: JoinHandle<()>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    namespace: String,
}

impl ScopedOperator {
    /// Start a new operator instance scoped to a specific namespace
    ///
    /// The operator will only watch and reconcile PostgresCluster resources
    /// in the specified namespace. It will be automatically stopped when dropped.
    pub async fn start(client: Client, namespace: &str) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let ns = namespace.to_string();
        let ns_clone = ns.clone();

        tracing::info!("Starting scoped operator controller in namespace: {}", ns);

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_operator(client, &ns_clone) => {
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
            namespace: ns,
        }
    }

    /// Start operators for both PostgresCluster and PostgresDatabase in a namespace
    ///
    /// This runs both the cluster controller and the database controller
    /// concurrently, scoped to the specified namespace.
    pub async fn start_with_database(client: Client, namespace: &str) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let ns = namespace.to_string();
        let ns_clone = ns.clone();

        tracing::info!(
            "Starting scoped operators (cluster + database) in namespace: {}",
            ns
        );

        let cluster_client = client.clone();
        let database_client = client;
        let ns_for_db = ns_clone.clone();

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_operator(cluster_client, &ns_clone) => {
                    tracing::debug!("Cluster operator exited normally");
                }
                _ = run_database_operator(database_client, &ns_for_db) => {
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
            namespace: ns,
        }
    }

    /// Start operators for PostgresCluster and PostgresUpgrade in a namespace
    ///
    /// This runs both the cluster controller and the upgrade controller
    /// concurrently, scoped to the specified namespace.
    pub async fn start_with_upgrade(client: Client, namespace: &str) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let ns = namespace.to_string();
        let ns_clone = ns.clone();

        tracing::info!(
            "Starting scoped operators (cluster + upgrade) in namespace: {}",
            ns
        );

        let cluster_client = client.clone();
        let upgrade_client = client;
        let ns_for_upgrade = ns_clone.clone();

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_operator(cluster_client, &ns_clone) => {
                    tracing::debug!("Cluster operator exited normally");
                }
                _ = run_upgrade_operator(upgrade_client, &ns_for_upgrade) => {
                    tracing::debug!("Upgrade operator exited normally");
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
            namespace: ns,
        }
    }

    /// Start all operators (cluster, database, and upgrade) in a namespace
    ///
    /// This runs all three controllers concurrently, scoped to the specified namespace.
    pub async fn start_all(client: Client, namespace: &str) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let ns = namespace.to_string();
        let ns_clone = ns.clone();

        tracing::info!(
            "Starting scoped operators (cluster + database + upgrade) in namespace: {}",
            ns
        );

        let cluster_client = client.clone();
        let database_client = client.clone();
        let upgrade_client = client;
        let ns_for_db = ns_clone.clone();
        let ns_for_upgrade = ns_clone.clone();

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = run_operator(cluster_client, &ns_clone) => {
                    tracing::debug!("Cluster operator exited normally");
                }
                _ = run_database_operator(database_client, &ns_for_db) => {
                    tracing::debug!("Database operator exited normally");
                }
                _ = run_upgrade_operator(upgrade_client, &ns_for_upgrade) => {
                    tracing::debug!("Upgrade operator exited normally");
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
            namespace: ns,
        }
    }

    /// Get the namespace this operator is scoped to
    pub fn namespace(&self) -> &str {
        &self.namespace
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

/// Run the operator controller scoped to a specific namespace
///
/// This enables parallel test execution by ensuring each operator only
/// watches resources in its designated namespace.
async fn run_operator(client: Client, namespace: &str) {
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

    // Use namespaced APIs for parallel test isolation
    let clusters: Api<PostgresCluster> = Api::namespaced(client.clone(), namespace);
    let statefulsets: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    let configmaps: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let pdbs: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), namespace);

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

/// Run the database operator controller scoped to a specific namespace
async fn run_database_operator(client: Client, namespace: &str) {
    use futures::StreamExt;
    use k8s_openapi::api::core::v1::Secret;
    use kube::Api;
    use kube::runtime::Controller;
    use kube::runtime::watcher::Config as WatcherConfig;
    use postgres_operator::crd::PostgresDatabase;
    use postgres_operator::{DatabaseContext, database_error_policy, reconcile_database};

    let ctx = Arc::new(DatabaseContext::new(client.clone()));

    // Use namespaced APIs for parallel test isolation
    let databases: Api<PostgresDatabase> = Api::namespaced(client.clone(), namespace);
    let secrets: Api<Secret> = Api::namespaced(client.clone(), namespace);

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

/// Run the upgrade operator controller scoped to a specific namespace
async fn run_upgrade_operator(client: Client, namespace: &str) {
    use futures::StreamExt;
    use kube::Api;
    use kube::runtime::Controller;
    use kube::runtime::watcher::Config as WatcherConfig;
    use postgres_operator::crd::PostgresUpgrade;
    use postgres_operator::{UpgradeContext, reconcile_upgrade, upgrade_error_policy};

    let ctx = Arc::new(UpgradeContext::new(client.clone()));

    // Use namespaced APIs for parallel test isolation
    let upgrades: Api<PostgresUpgrade> = Api::namespaced(client.clone(), namespace);

    let watcher_config = WatcherConfig::default().any_semantic();

    Controller::new(upgrades, watcher_config)
        .run(reconcile_upgrade, upgrade_error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    tracing::debug!("Reconciled upgrade: {}", obj.name);
                }
                Err(e) => {
                    tracing::error!("Upgrade reconciliation error: {:?}", e);
                }
            }
        })
        .await;
}
