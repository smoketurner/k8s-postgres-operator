//! Resource assertion helpers for integration tests.
//!
//! This module provides [`ResourceAssertions`], a helper struct for verifying
//! Kubernetes resources exist, are deleted, or have expected properties.
//!
//! # Usage
//!
//! ```ignore
//! use k8s_openapi::api::core::v1::{Secret, Service, ConfigMap};
//!
//! let assertions = ResourceAssertions::new(client.clone(), namespace);
//!
//! // Assert resources exist using the generic method
//! let sts = assertions.statefulset_exists("my-cluster", 3).await?;
//! let svc: Service = assertions.resource_exists("my-cluster").await?;
//! let secret: Secret = assertions.resource_exists("my-cluster-credentials").await?;
//! let cm: ConfigMap = assertions.resource_exists("my-cluster-patroni").await?;
//!
//! // Assert resources were deleted
//! assertions.resource_deleted::<StatefulSet>("my-cluster").await?;
//! assertions.resource_deleted::<Service>("my-cluster").await?;
//!
//! // Verify owner reference
//! assertions.has_owner_reference("my-cluster", "PostgresCluster").await?;
//!
//! // Verify ConfigMap content
//! assertions.configmap_contains("my-cluster-patroni", "patroni.yml", "scope:").await?;
//! ```

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AssertionError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Replica count mismatch for {resource}: expected {expected}, got {actual}")]
    ReplicaMismatch {
        resource: String,
        expected: i32,
        actual: i32,
    },

    #[error("Resource still exists: {0}")]
    ResourceStillExists(String),

    #[error("Missing owner reference on {resource}, expected owner: {expected_owner}")]
    MissingOwnerReference {
        resource: String,
        expected_owner: String,
    },

    #[error("ConfigMap missing expected content: {0}")]
    ConfigMapContentMissing(String),
}

/// Helper for asserting on Kubernetes resources in a specific namespace.
///
/// Provides convenient methods for verifying resource existence, deletion,
/// and properties. All methods return [`AssertionError`] on failure with
/// descriptive error messages.
pub struct ResourceAssertions {
    client: Client,
    namespace: String,
}

impl ResourceAssertions {
    /// Create a new assertions helper for the given namespace.
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
        }
    }

    /// Assert that a namespaced resource exists and return it.
    ///
    /// Works with any Kubernetes resource type (Pod, Job, ConfigMap, etc.).
    ///
    /// # Example
    /// ```ignore
    /// let job: Job = assertions.resource_exists("my-job").await?;
    /// let pvc: PersistentVolumeClaim = assertions.resource_exists("data-0").await?;
    /// ```
    pub async fn resource_exists<T>(&self, name: &str) -> Result<T, AssertionError>
    where
        T: Resource<Scope = k8s_openapi::NamespaceResourceScope> + Clone + DeserializeOwned + Debug,
        <T as Resource>::DynamicType: Default,
    {
        let api: Api<T> = Api::namespaced(self.client.clone(), &self.namespace);
        Ok(api.get(name).await?)
    }

    /// Assert that a namespaced resource does NOT exist (returns 404).
    ///
    /// Use this to verify cleanup after deletion.
    ///
    /// # Example
    /// ```ignore
    /// assertions.resource_deleted::<Job>("my-job").await?;
    /// assertions.resource_deleted::<Pod>("my-pod").await?;
    /// ```
    pub async fn resource_deleted<T>(&self, name: &str) -> Result<(), AssertionError>
    where
        T: Resource<Scope = k8s_openapi::NamespaceResourceScope> + Clone + DeserializeOwned + Debug,
        <T as Resource>::DynamicType: Default,
    {
        let api: Api<T> = Api::namespaced(self.client.clone(), &self.namespace);
        match api.get(name).await {
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Ok(_) => Err(AssertionError::ResourceStillExists(name.to_string())),
            Err(e) => Err(AssertionError::KubeError(e)),
        }
    }

    /// Assert that a StatefulSet exists with expected replicas
    pub async fn statefulset_exists(
        &self,
        name: &str,
        expected_replicas: i32,
    ) -> Result<StatefulSet, AssertionError> {
        let sts: StatefulSet = self.resource_exists(name).await?;

        let replicas = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);

        if replicas != expected_replicas {
            return Err(AssertionError::ReplicaMismatch {
                resource: name.to_string(),
                expected: expected_replicas,
                actual: replicas,
            });
        }

        Ok(sts)
    }

    /// Verify owner references are correctly set on a StatefulSet
    pub async fn has_owner_reference(
        &self,
        sts_name: &str,
        owner_name: &str,
    ) -> Result<(), AssertionError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let sts = api.get(sts_name).await?;

        let has_ref = sts
            .metadata
            .owner_references
            .as_ref()
            .map(|refs| {
                refs.iter()
                    .any(|r| r.name == owner_name && r.kind == "PostgresCluster")
            })
            .unwrap_or(false);

        if !has_ref {
            return Err(AssertionError::MissingOwnerReference {
                resource: sts_name.to_string(),
                expected_owner: owner_name.to_string(),
            });
        }

        Ok(())
    }

    /// Verify ConfigMap contains expected content
    pub async fn configmap_contains(
        &self,
        name: &str,
        key: &str,
        expected_content: &str,
    ) -> Result<(), AssertionError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        let cm = api.get(name).await?;

        let content = cm.data.as_ref().and_then(|d| d.get(key)).ok_or_else(|| {
            AssertionError::ConfigMapContentMissing(format!("Key '{}' not found", key))
        })?;

        if !content.contains(expected_content) {
            return Err(AssertionError::ConfigMapContentMissing(format!(
                "Expected '{}' in ConfigMap key '{}', got: {}",
                expected_content, key, content
            )));
        }

        Ok(())
    }
}
