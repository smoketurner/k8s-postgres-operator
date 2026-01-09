//! Resource assertion helpers for integration tests

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Secret, Service};
use kube::{Api, Client};
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

/// Helper for asserting on Kubernetes resources
pub struct ResourceAssertions {
    client: Client,
    namespace: String,
}

impl ResourceAssertions {
    pub fn new(client: Client, namespace: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
        }
    }

    /// Assert that a StatefulSet exists with expected replicas
    pub async fn statefulset_exists(
        &self,
        name: &str,
        expected_replicas: i32,
    ) -> Result<StatefulSet, AssertionError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        let sts = api.get(name).await?;

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

    /// Assert that a Service exists
    pub async fn service_exists(&self, name: &str) -> Result<Service, AssertionError> {
        let api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        Ok(api.get(name).await?)
    }

    /// Assert that a Secret exists
    pub async fn secret_exists(&self, name: &str) -> Result<Secret, AssertionError> {
        let api: Api<Secret> = Api::namespaced(self.client.clone(), &self.namespace);
        Ok(api.get(name).await?)
    }

    /// Assert that a ConfigMap exists
    pub async fn configmap_exists(&self, name: &str) -> Result<ConfigMap, AssertionError> {
        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), &self.namespace);
        Ok(api.get(name).await?)
    }

    /// Assert that a StatefulSet does NOT exist (for cleanup verification)
    pub async fn statefulset_deleted(&self, name: &str) -> Result<(), AssertionError> {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), &self.namespace);
        match api.get(name).await {
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Ok(_) => Err(AssertionError::ResourceStillExists(name.to_string())),
            Err(e) => Err(AssertionError::KubeError(e)),
        }
    }

    /// Assert that a Service does NOT exist
    pub async fn service_deleted(&self, name: &str) -> Result<(), AssertionError> {
        let api: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        match api.get(name).await {
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Ok(_) => Err(AssertionError::ResourceStillExists(name.to_string())),
            Err(e) => Err(AssertionError::KubeError(e)),
        }
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
