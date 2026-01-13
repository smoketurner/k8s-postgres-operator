//! Test namespace management for isolation
//!
//! Provides `TestNamespace` which implements RAII-style automatic cleanup.
//! When a `TestNamespace` goes out of scope (whether normally or due to panic),
//! it will automatically initiate namespace deletion.

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, Resource};
use postgres_operator::crd::{PostgresCluster, PostgresDatabase, PostgresUpgrade};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::collections::BTreeMap;
use std::fmt::Debug;
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
    /// This deletes PostgresDatabase, PostgresUpgrade, and PostgresCluster
    /// resources first to trigger operator cleanup, then removes any remaining
    /// finalizers, then deletes the namespace.
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

        // First, delete all PostgresDatabase resources (before clusters)
        Self::delete_resources::<PostgresDatabase>(&self.client, &self.name).await;

        // Delete all PostgresUpgrade resources (before clusters, since they reference clusters)
        Self::delete_resources::<PostgresUpgrade>(&self.client, &self.name).await;

        // Then delete all PostgresCluster resources to trigger operator cleanup
        Self::delete_resources::<PostgresCluster>(&self.client, &self.name).await;

        // Brief wait for operator to process deletions
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Remove any remaining finalizers (in case operator didn't clean up)
        Self::remove_resource_finalizers::<PostgresDatabase>(&self.client, &self.name).await;
        Self::remove_resource_finalizers::<PostgresUpgrade>(&self.client, &self.name).await;
        Self::remove_resource_finalizers::<PostgresCluster>(&self.client, &self.name).await;

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

    /// Delete all resources of type `T` in the namespace.
    ///
    /// This is a generic helper that works with any kube-rs custom resource type
    /// that implements the required traits for API operations.
    async fn delete_resources<T>(client: &Client, namespace: &str)
    where
        T: Resource<Scope = k8s_openapi::NamespaceResourceScope> + Clone + DeserializeOwned + Debug,
        <T as Resource>::DynamicType: Default,
    {
        let dt = T::DynamicType::default();
        let kind = T::kind(&dt);
        let api: Api<T> = Api::namespaced(client.clone(), namespace);

        let resource_list = match api.list(&Default::default()).await {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!("Failed to list {} resources for deletion: {}", kind, e);
                return;
            }
        };

        let dp = DeleteParams::default();
        for resource in resource_list.items {
            if let Some(name) = Resource::meta(&resource).name.as_ref()
                && let Err(e) = api.delete(name, &dp).await
            {
                tracing::debug!("Failed to delete {} {}: {}", kind, name, e);
            }
        }
    }

    /// Remove finalizers from all resources of type `T` in the namespace.
    ///
    /// This is a generic helper that patches resources to remove their finalizers,
    /// which is necessary for cleanup when the operator is not running.
    async fn remove_resource_finalizers<T>(client: &Client, namespace: &str)
    where
        T: Resource<Scope = k8s_openapi::NamespaceResourceScope>
            + Clone
            + DeserializeOwned
            + Serialize
            + Debug,
        <T as Resource>::DynamicType: Default,
    {
        let dt = T::DynamicType::default();
        let kind = T::kind(&dt);
        let api: Api<T> = Api::namespaced(client.clone(), namespace);

        let resource_list = match api.list(&Default::default()).await {
            Ok(list) => list,
            Err(e) => {
                tracing::debug!("Failed to list {} resources for cleanup: {}", kind, e);
                return;
            }
        };

        let patch: Patch<serde_json::Value> =
            Patch::Merge(json!({"metadata": {"finalizers": null}}));
        let patch_params = PatchParams::default();

        for resource in resource_list.items {
            let meta = Resource::meta(&resource);
            if let Some(name) = meta.name.as_ref() {
                // Skip if no finalizers
                if meta.finalizers.as_ref().is_none_or(|f| f.is_empty()) {
                    continue;
                }
                if let Err(e) = api.patch(name, &patch_params, &patch).await {
                    tracing::warn!("Failed to remove finalizer from {} {}: {}", kind, name, e);
                } else {
                    tracing::debug!("Removed finalizer from {} {}", kind, name);
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

/// Automatic cleanup on drop - synchronously deletes databases, upgrades, clusters, removes finalizers, and deletes namespace.
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
                // First delete all PostgresDatabase resources
                Self::delete_resources::<PostgresDatabase>(&client, &name).await;

                // Delete all PostgresUpgrade resources (before clusters)
                Self::delete_resources::<PostgresUpgrade>(&client, &name).await;

                // Then delete all PostgresCluster resources to trigger operator cleanup
                Self::delete_resources::<PostgresCluster>(&client, &name).await;

                // Brief wait for operator to process deletions
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                // Remove any remaining finalizers
                Self::remove_resource_finalizers::<PostgresDatabase>(&client, &name).await;
                Self::remove_resource_finalizers::<PostgresUpgrade>(&client, &name).await;
                Self::remove_resource_finalizers::<PostgresCluster>(&client, &name).await;

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
