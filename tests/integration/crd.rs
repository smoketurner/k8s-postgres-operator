//! CRD installation helpers for integration tests

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{Patch, PatchParams};
use kube::runtime::wait::{await_condition, conditions};
use kube::{Api, Client};
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CrdError {
    #[error("Failed to parse CRD YAML: {0}")]
    ParseError(#[from] serde_yaml::Error),

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("CRD establishment timeout")]
    EstablishmentTimeout,

    #[error("Wait error: {0}")]
    WaitError(#[from] kube::runtime::wait::Error),
}

/// Install the PostgresCluster CRD into the cluster
pub async fn install_crd(client: Client) -> Result<(), CrdError> {
    let crd_yaml = include_str!("../../config/crd/postgres-cluster.yaml");
    let crd: CustomResourceDefinition = serde_yaml::from_str(crd_yaml)?;

    let crds: Api<CustomResourceDefinition> = Api::all(client);
    let params = PatchParams::apply("integration-test").force();

    tracing::info!("Installing PostgresCluster CRD...");

    crds.patch(
        "postgresclusters.postgres-operator.smoketurner.com",
        &params,
        &Patch::Apply(&crd),
    )
    .await?;

    // Wait for CRD to be established (up to 30 seconds)
    tracing::info!("Waiting for CRD to be established...");

    let establish = await_condition(
        crds,
        "postgresclusters.postgres-operator.smoketurner.com",
        conditions::is_crd_established(),
    );

    tokio::time::timeout(Duration::from_secs(30), establish)
        .await
        .map_err(|_| CrdError::EstablishmentTimeout)??;

    tracing::info!("CRD installed and established");

    Ok(())
}
