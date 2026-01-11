//! Test namespace management for isolation
//!
//! Provides `TestNamespace` which implements RAII-style automatic cleanup.
//! When a `TestNamespace` goes out of scope (whether normally or due to panic),
//! it will automatically initiate namespace deletion.

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::core::ObjectMeta;
use kube::{Api, Client};
use postgres_operator::crd::PostgresCluster;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum NamespaceError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),
}

/// A test namespace that is automatically cleaned up via RAII.
///
/// When this struct is dropped (whether normally or due to panic),
/// it will spawn an async task to delete the namespace. This ensures
/// test resources are cleaned up even when tests fail.
pub struct TestNamespace {
    /// Name of the namespace
    pub name: String,
    /// Kubernetes client
    client: Client,
    /// Track if cleanup has already been initiated
    cleanup_initiated: AtomicBool,
}

impl TestNamespace {
    /// Create a new unique namespace for test isolation
    ///
    /// The namespace name is generated as `{prefix}-{uuid8}` to ensure
    /// uniqueness across test runs.
    pub async fn create(client: Client, prefix: &str) -> Result<Self, NamespaceError> {
        let suffix = &Uuid::new_v4().to_string()[..8];
        let name = format!("{}-{}", prefix, suffix);

        let labels = BTreeMap::from([
            ("postgres-operator.test".to_string(), "true".to_string()),
            ("test-prefix".to_string(), prefix.to_string()),
        ]);

        let ns = Namespace {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            ..Default::default()
        };

        let namespaces: Api<Namespace> = Api::all(client.clone());
        namespaces.create(&PostParams::default(), &ns).await?;

        tracing::info!("Created test namespace: {}", name);

        Ok(Self {
            name,
            client,
            cleanup_initiated: AtomicBool::new(false),
        })
    }

    /// Get the namespace name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get a clone of the client
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Cleanup the namespace and all resources within it
    ///
    /// This deletes PostgresCluster resources first to trigger operator cleanup,
    /// then removes any remaining finalizers, then deletes the namespace.
    ///
    /// Note: This is also called automatically by Drop, but you can call it
    /// explicitly if you want to handle errors.
    pub async fn cleanup(&self) -> Result<(), NamespaceError> {
        // Mark as initiated to avoid double-cleanup in Drop
        if self.cleanup_initiated.swap(true, Ordering::SeqCst) {
            // Already initiated
            return Ok(());
        }

        tracing::debug!("Cleaning up namespace: {}", self.name);

        // First, delete all PostgresCluster resources to trigger operator cleanup
        Self::delete_clusters(&self.client, &self.name).await;

        // Brief wait for operator to process deletions
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Remove any remaining finalizers (in case operator didn't clean up)
        Self::remove_finalizers(&self.client, &self.name).await;

        let namespaces: Api<Namespace> = Api::all(self.client.clone());

        // Delete namespace (cascades to all resources within)
        // Use background propagation to return immediately
        let dp = DeleteParams {
            propagation_policy: Some(kube::api::PropagationPolicy::Background),
            ..Default::default()
        };

        match namespaces.delete(&self.name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // Already deleted
                return Ok(());
            }
            Err(e) => return Err(NamespaceError::KubeError(e)),
        }

        tracing::debug!("Namespace {} deletion initiated", self.name);

        Ok(())
    }

    /// Delete all PostgresCluster resources in the namespace
    async fn delete_clusters(client: &Client, namespace: &str) {
        let clusters: Api<PostgresCluster> = Api::namespaced(client.clone(), namespace);

        // List all clusters in the namespace
        let cluster_list = match clusters.list(&Default::default()).await {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!("Failed to list clusters for deletion: {}", e);
                return;
            }
        };

        let dp = DeleteParams::default();
        for cluster in cluster_list.items {
            if let Some(name) = cluster.metadata.name {
                if let Err(e) = clusters.delete(&name, &dp).await {
                    tracing::debug!("Failed to delete cluster {}: {}", name, e);
                }
            }
        }
    }

    /// Remove finalizers from all PostgresCluster resources in the namespace
    async fn remove_finalizers(client: &Client, namespace: &str) {
        let clusters: Api<PostgresCluster> = Api::namespaced(client.clone(), namespace);

        // List all clusters in the namespace
        let cluster_list = match clusters.list(&Default::default()).await {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!("Failed to list clusters for cleanup: {}", e);
                return;
            }
        };

        // Remove finalizers using JSON patch - replace the array with empty
        let patch: Patch<serde_json::Value> = Patch::Json(
            serde_json::from_value(json!([
                { "op": "replace", "path": "/metadata/finalizers", "value": [] }
            ]))
            .expect("valid json patch"),
        );
        let patch_params = PatchParams::default();

        for cluster in cluster_list.items {
            if let Some(name) = cluster.metadata.name {
                // Skip if no finalizers
                if cluster
                    .metadata
                    .finalizers
                    .as_ref()
                    .is_none_or(|f| f.is_empty())
                {
                    continue;
                }
                if let Err(e) = clusters.patch(&name, &patch_params, &patch).await {
                    tracing::warn!("Failed to remove finalizer from {}: {}", name, e);
                } else {
                    tracing::debug!("Removed finalizer from cluster {}", name);
                }
            }
        }
    }

    /// Prevent automatic cleanup (use when you want to keep the namespace for debugging)
    pub fn keep(&self) {
        self.cleanup_initiated.store(true, Ordering::SeqCst);
        tracing::info!(
            "Namespace {} will NOT be cleaned up (keep() called)",
            self.name
        );
    }
}

/// Automatic cleanup on drop - synchronously deletes clusters, removes finalizers, and deletes namespace.
///
/// Uses tokio::task::block_in_place to allow blocking on async code from within
/// the tokio runtime. This requires the multi-threaded runtime (use `#[tokio::test(flavor = "multi_thread")]`).
impl Drop for TestNamespace {
    fn drop(&mut self) {
        // Check if cleanup was already initiated (via explicit cleanup() call)
        if self.cleanup_initiated.swap(true, Ordering::SeqCst) {
            return;
        }

        let name = self.name.clone();
        let client = self.client.clone();

        tracing::debug!("Drop: cleaning up namespace {}", name);

        // Use block_in_place to allow blocking from within the async runtime
        // This requires the multi-threaded runtime
        tokio::task::block_in_place(|| {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async {
                // First delete all PostgresCluster resources to trigger operator cleanup
                Self::delete_clusters(&client, &name).await;

                // Brief wait for operator to process deletions
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Remove any remaining finalizers
                Self::remove_finalizers(&client, &name).await;

                let namespaces: Api<Namespace> = Api::all(client);
                let dp = DeleteParams {
                    propagation_policy: Some(kube::api::PropagationPolicy::Background),
                    ..Default::default()
                };

                match namespaces.delete(&name, &dp).await {
                    Ok(_) => {
                        tracing::debug!("Drop: namespace {} deletion initiated", name);
                    }
                    Err(kube::Error::Api(e)) if e.code == 404 => {
                        tracing::debug!("Drop: namespace {} already deleted", name);
                    }
                    Err(e) => {
                        tracing::warn!("Drop: failed to delete namespace {}: {}", name, e);
                    }
                }
            });
        });
    }
}
