//! Cluster management for integration tests
//!
//! Uses existing kubeconfig (~/.kube/config or KUBECONFIG environment variable).

use kube::{Client, Config};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::OnceCell;

use crate::CrdError;

#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("Failed to create kube client: {0}")]
    ClientCreation(#[from] kube::Error),

    #[error("Failed to infer config: {0}")]
    InferConfig(#[from] kube::config::InferConfigError),
}

/// Global shared cluster instance
static SHARED_CLUSTER: OnceCell<Arc<SharedTestCluster>> = OnceCell::const_new();

/// CRD installation tracking (should only be done once)
static CRD_INSTALLED: OnceCell<()> = OnceCell::const_new();

/// A shared test cluster for all integration tests
///
/// Uses an existing Kubernetes cluster via kubeconfig.
pub struct SharedTestCluster {
    _marker: (),
}

impl SharedTestCluster {
    /// Get or initialize the shared cluster
    ///
    /// Uses the existing kubeconfig from ~/.kube/config or KUBECONFIG env var.
    pub async fn get() -> Result<Arc<SharedTestCluster>, ClusterError> {
        SHARED_CLUSTER
            .get_or_try_init(|| async {
                let cluster = Self::connect().await?;
                Ok(Arc::new(cluster))
            })
            .await
            .map(Arc::clone)
    }

    /// Create a new kube Client
    pub async fn new_client(&self) -> Result<Client, ClusterError> {
        let config = Config::infer().await?;
        Ok(Client::try_from(config)?)
    }

    /// Connect to the cluster using kubeconfig
    async fn connect() -> Result<Self, ClusterError> {
        // Verify we can connect
        let config = Config::infer().await?;
        let client = Client::try_from(config)?;

        // Quick health check
        let version = client.apiserver_version().await?;
        tracing::info!(
            "Connected to Kubernetes cluster: {} {}",
            version.platform,
            version.git_version
        );

        Ok(Self { _marker: () })
    }
}

/// Ensure CRD is installed (idempotent - only runs once per test run)
pub async fn ensure_crd_installed(cluster: &SharedTestCluster) -> Result<(), CrdError> {
    CRD_INSTALLED
        .get_or_try_init(|| async {
            let client = cluster.new_client().await.map_err(|e| {
                CrdError::KubeError(kube::Error::Service(
                    std::io::Error::other(e.to_string()).into(),
                ))
            })?;
            crate::install_crd(client).await
        })
        .await
        .map(|_| ())
}
