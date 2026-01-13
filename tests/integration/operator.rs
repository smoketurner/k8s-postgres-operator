//! Operator spawning utilities for integration tests.
//!
//! Each test gets its own operator instance scoped to a specific namespace.
//! This enables test parallelization - multiple tests can run concurrently
//! without their operators interfering with each other.
//!
//! The operator runs in the test's tokio runtime and is automatically
//! stopped when dropped (RAII pattern).
//!
//! # Usage
//!
//! ```ignore
//! let ns = TestNamespace::create(client.clone(), "my-test").await?;
//!
//! // Start all controllers scoped to the namespace
//! let operator = ScopedOperator::start(client.clone(), ns.name()).await;
//!
//! // Run tests... operator stops automatically when dropped
//! ```
//!
//! # Design
//!
//! All controllers (cluster, database, upgrade) run together, matching
//! production behavior. Namespace scoping ensures each test's operator
//! only watches resources in its designated namespace, preventing
//! interference when running tests in parallel.

use kube::Client;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// A scoped operator that runs for the duration of a test.
///
/// Runs all controllers (cluster, database, upgrade) scoped to a single
/// namespace, matching production behavior while enabling test isolation.
/// The operator is automatically stopped when dropped (RAII pattern).
pub struct ScopedOperator {
    handle: JoinHandle<()>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    namespace: Arc<str>,
}

impl ScopedOperator {
    /// Start all operators scoped to a specific namespace.
    ///
    /// This starts the cluster, database, and upgrade controllers, all
    /// watching only the specified namespace. The operator will be
    /// automatically stopped when dropped.
    pub async fn start(client: Client, namespace: &str) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let ns: Arc<str> = namespace.into();

        tracing::info!("Starting scoped operator in namespace: {}", ns);

        let handle = Self::spawn_controllers(client, Arc::clone(&ns), shutdown_rx);

        // Give the controllers a moment to start watching
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            namespace: ns,
        }
    }

    fn spawn_controllers(
        client: Client,
        namespace: Arc<str>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            tokio::select! {
                _ = Self::run_controllers(client, &namespace) => {
                    tracing::debug!("Controllers exited normally");
                }
                _ = shutdown_rx => {
                    tracing::debug!("Controllers received shutdown signal");
                }
            }
        })
    }

    async fn run_controllers(client: Client, namespace: &str) {
        use postgres_operator::{
            run_controller_scoped, run_database_controller_scoped, run_upgrade_controller_scoped,
        };

        // Run all controllers concurrently using select! (matching production behavior).
        // If any controller exits unexpectedly, panic to fail the test immediately
        // rather than timing out with an unclear error.
        tokio::select! {
            _ = run_controller_scoped(client.clone(), None, Some(namespace)) => {
                panic!("Cluster controller exited unexpectedly - controllers should run indefinitely");
            }
            _ = run_database_controller_scoped(client.clone(), Some(namespace)) => {
                panic!("Database controller exited unexpectedly - controllers should run indefinitely");
            }
            _ = run_upgrade_controller_scoped(client.clone(), Some(namespace)) => {
                panic!("Upgrade controller exited unexpectedly - controllers should run indefinitely");
            }
        }
    }

    /// Get the namespace this operator is scoped to.
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
