//! Test namespace management for isolation

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, PostParams};
use kube::core::ObjectMeta;
use kube::{Api, Client};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum NamespaceError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),
}

/// A test namespace that is automatically cleaned up
pub struct TestNamespace {
    /// Name of the namespace
    pub name: String,
    /// Kubernetes client
    client: Client,
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

        Ok(Self { name, client })
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
    /// This initiates deletion but doesn't wait for completion to keep tests fast.
    /// Kubernetes will garbage collect the namespace eventually.
    pub async fn cleanup(&self) -> Result<(), NamespaceError> {
        let namespaces: Api<Namespace> = Api::all(self.client.clone());

        tracing::debug!("Initiating deletion of namespace: {}", self.name);

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

        // Don't wait for deletion - let it happen in the background
        // This keeps tests fast and Kubernetes will clean up eventually
        tracing::debug!("Namespace {} deletion initiated", self.name);

        Ok(())
    }
}
