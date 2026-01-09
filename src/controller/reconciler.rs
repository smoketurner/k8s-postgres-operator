//! Reconciliation logic for PostgresCluster resources
//!
//! This module contains the main reconciliation loop that ensures
//! PostgresCluster resources are in their desired state.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{Pod, Secret};
use kube::api::{ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};
use serde::de::DeserializeOwned;
use tracing::{debug, error, info, instrument, warn};

use crate::controller::context::Context;
use crate::controller::error::{BackoffConfig, Error, Result};
use crate::controller::status::{spec_changed, StatusManager};
use crate::crd::{ClusterPhase, PostgresCluster};
use crate::resources::{patroni, pdb, secret, service};

/// Finalizer name for cleanup
pub const FINALIZER: &str = "postgres.example.com/finalizer";

/// Default backoff configuration for error handling
fn default_backoff() -> BackoffConfig {
    BackoffConfig::default()
}

/// Main reconciliation function
#[instrument(skip(cluster, ctx), fields(name = %cluster.name_any(), namespace = cluster.namespace().unwrap_or_default()))]
pub async fn reconcile(cluster: Arc<PostgresCluster>, ctx: Arc<Context>) -> Result<Action> {
    let ns = cluster.namespace().unwrap_or_default();
    let name = cluster.name_any();

    info!("Reconciling PostgresCluster");

    // Handle deletion
    if cluster.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&cluster, &ctx, &ns).await;
    }

    // Ensure finalizer is present
    if !has_finalizer(&cluster) {
        add_finalizer(&cluster, &ctx, &ns).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Check if spec has changed using observed generation
    // This optimization prevents unnecessary reconciliations when only status changed
    let is_spec_changed = spec_changed(&cluster);
    let current_phase = cluster
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or_default();

    // Skip full reconciliation if spec hasn't changed and cluster is running
    if !is_spec_changed && current_phase == ClusterPhase::Running {
        debug!(
            "Spec unchanged for {} (generation: {:?}, observed: {:?}), checking status only",
            name,
            cluster.metadata.generation,
            cluster.status.as_ref().and_then(|s| s.observed_generation)
        );
        // Still check and update status periodically
        return check_and_update_status(&cluster, &ctx, &ns).await;
    }

    if is_spec_changed {
        info!(
            "Spec changed for {} (generation: {:?} -> {:?}), performing full reconciliation",
            name,
            cluster.status.as_ref().and_then(|s| s.observed_generation),
            cluster.metadata.generation
        );
    }

    // All clusters use Patroni for consistent management
    let result = reconcile_cluster(&cluster, &ctx, &ns).await;

    match result {
        Ok(action) => {
            info!("Reconciliation completed successfully");
            Ok(action)
        }
        Err(e) => {
            error!("Reconciliation failed: {}", e);
            // Update status to failed with error details
            let status_manager = StatusManager::new(&cluster, &ctx, &ns);
            let _ = status_manager.set_failed("ReconciliationFailed", &e.to_string()).await;
            Err(e)
        }
    }
}

/// Error policy for the controller with exponential backoff
pub fn error_policy(cluster: Arc<PostgresCluster>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = cluster.name_any();
    let backoff = default_backoff();

    // Get retry count from status or default to 0
    // In a production system, you'd track this in a separate store or annotation
    let retry_count = 0u32; // Simplified - would need proper tracking

    let delay = backoff.delay_for_error(error, retry_count);

    if error.is_retryable() {
        warn!(
            "Retryable error for {}: {:?}, requeuing in {:?}",
            name, error, delay
        );
    } else {
        error!(
            "Non-retryable error for {}: {:?}, requeuing in {:?} for manual intervention",
            name, error, delay
        );
    }

    Action::requeue(delay)
}

/// Check and update status without full reconciliation
async fn check_and_update_status(
    cluster: &PostgresCluster,
    ctx: &Context,
    ns: &str,
) -> Result<Action> {
    let name = cluster.name_any();
    let status_manager = StatusManager::new(cluster, ctx, ns);

    // Check StatefulSet status (Patroni uses cluster name directly)
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);

    let (ready_replicas, _, _) = get_statefulset_status(&sts_api, &name).await;

    // Get pod names from Patroni labels (spilo-role)
    let primary_pod = get_patroni_leader(&pods_api, &name).await;
    let replica_pods = get_patroni_replicas(&pods_api, &name).await;

    if ready_replicas >= cluster.spec.replicas {
        status_manager
            .set_running(
                ready_replicas,
                cluster.spec.replicas,
                primary_pod,
                replica_pods,
            )
            .await?;
    }

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Reconcile a PostgreSQL cluster using Patroni
///
/// All clusters use Patroni for consistent management, regardless of size:
/// - 1 replica: single server managed by Patroni
/// - 2 replicas: primary + read replica with automatic failover
/// - 3+ replicas: highly available cluster
///
/// Patroni provides:
/// - Automatic leader election using Kubernetes native DCS (endpoints)
/// - Automatic failover when primary fails
/// - Built-in replica initialization via pg_basebackup
/// - Split-brain prevention
///
/// Reference: https://github.com/patroni/patroni
async fn reconcile_cluster(
    cluster: &PostgresCluster,
    ctx: &Context,
    ns: &str,
) -> Result<Action> {
    let name = cluster.name_any();
    let status_manager = StatusManager::new(cluster, ctx, ns);

    info!("Reconciling PostgreSQL cluster with Patroni ({} replicas)", cluster.spec.replicas);

    // Update status to Creating if pending
    let current_phase = cluster
        .status
        .as_ref()
        .map(|s| s.phase.clone())
        .unwrap_or_default();

    if current_phase == ClusterPhase::Pending {
        status_manager
            .set_creating(0, cluster.spec.replicas, None)
            .await?;
    } else if spec_changed(cluster) && current_phase == ClusterPhase::Running {
        status_manager
            .set_updating(0, cluster.spec.replicas, None, vec![])
            .await?;
    }

    // Ensure Secret exists (credentials)
    let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
    let secret_name = format!("{}-credentials", name);
    if secrets_api.get_opt(&secret_name).await?.is_none() {
        let secret = secret::generate_credentials_secret(cluster)?;
        apply_resource(ctx, ns, &secret).await?;
    }

    // Ensure Patroni ConfigMap exists
    let patroni_config = patroni::generate_patroni_config(cluster);
    apply_resource(ctx, ns, &patroni_config).await?;

    // Ensure ServiceAccount exists for Patroni
    let service_account = patroni::generate_service_account(cluster);
    apply_resource(ctx, ns, &service_account).await?;

    // Ensure Role exists for Patroni
    let role = patroni::generate_patroni_role(cluster);
    apply_role(ctx, ns, &role).await?;

    // Ensure RoleBinding exists for Patroni
    let role_binding = patroni::generate_patroni_role_binding(cluster);
    apply_role_binding(ctx, ns, &role_binding).await?;

    // Ensure PodDisruptionBudget exists
    let pdb = pdb::generate_pdb(cluster);
    apply_resource(ctx, ns, &pdb).await?;

    // Ensure Patroni StatefulSet exists (single StatefulSet for all members)
    let sts = patroni::generate_patroni_statefulset(cluster);
    apply_resource(ctx, ns, &sts).await?;

    // Ensure Patroni Services exist
    let primary_svc = service::generate_primary_service(cluster);
    apply_resource(ctx, ns, &primary_svc).await?;

    let replicas_svc = service::generate_replicas_service(cluster);
    apply_resource(ctx, ns, &replicas_svc).await?;

    let headless_svc = service::generate_headless_service(cluster);
    apply_resource(ctx, ns, &headless_svc).await?;

    // Check StatefulSet status
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);

    let (ready_replicas, _, _) = get_statefulset_status(&sts_api, &name).await;

    // Get pod names from Patroni labels (spilo-role)
    let primary_pod = get_patroni_leader(&pods_api, &name).await;
    let replica_pods = get_patroni_replicas(&pods_api, &name).await;

    if ready_replicas >= cluster.spec.replicas {
        status_manager
            .set_running(
                ready_replicas,
                cluster.spec.replicas,
                primary_pod,
                replica_pods,
            )
            .await?;
    } else {
        status_manager
            .set_creating(ready_replicas, cluster.spec.replicas, primary_pod)
            .await?;
    }

    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Get the Patroni leader pod name
async fn get_patroni_leader(api: &Api<Pod>, cluster_name: &str) -> Option<String> {
    let label_selector = format!(
        "postgres.example.com/cluster={},spilo-role=master",
        cluster_name
    );

    match api.list(&ListParams::default().labels(&label_selector)).await {
        Ok(pods) => pods
            .items
            .first()
            .and_then(|p| p.metadata.name.clone()),
        Err(_) => None,
    }
}

/// Get Patroni replica pod names
async fn get_patroni_replicas(api: &Api<Pod>, cluster_name: &str) -> Vec<String> {
    let label_selector = format!(
        "postgres.example.com/cluster={},spilo-role=replica",
        cluster_name
    );

    match api.list(&ListParams::default().labels(&label_selector)).await {
        Ok(pods) => pods
            .items
            .iter()
            .filter_map(|p| p.metadata.name.clone())
            .collect(),
        Err(_) => vec![],
    }
}

/// Apply a Role using server-side apply
async fn apply_role(ctx: &Context, ns: &str, resource: &k8s_openapi::api::rbac::v1::Role) -> Result<()> {
    let api: Api<k8s_openapi::api::rbac::v1::Role> = Api::namespaced(ctx.client.clone(), ns);
    let name = resource.metadata.name.as_ref().unwrap();

    let patch = Patch::Apply(resource);
    let params = PatchParams::apply("postgres-operator").force();

    api.patch(name, &params, &patch).await?;
    debug!("Applied Role: {}", name);

    Ok(())
}

/// Apply a RoleBinding using server-side apply
async fn apply_role_binding(ctx: &Context, ns: &str, resource: &k8s_openapi::api::rbac::v1::RoleBinding) -> Result<()> {
    let api: Api<k8s_openapi::api::rbac::v1::RoleBinding> = Api::namespaced(ctx.client.clone(), ns);
    let name = resource.metadata.name.as_ref().unwrap();

    let patch = Patch::Apply(resource);
    let params = PatchParams::apply("postgres-operator").force();

    api.patch(name, &params, &patch).await?;
    debug!("Applied RoleBinding: {}", name);

    Ok(())
}

/// Get StatefulSet status information
async fn get_statefulset_status(
    api: &Api<StatefulSet>,
    name: &str,
) -> (i32, i32, Option<String>) {
    match api.get(name).await {
        Ok(sts) => {
            let ready = sts
                .status
                .as_ref()
                .and_then(|s| s.ready_replicas)
                .unwrap_or(0);
            let desired = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
            let primary_pod = if ready > 0 {
                Some(format!("{}-0", name))
            } else {
                None
            };
            (ready, desired, primary_pod)
        }
        Err(_) => (0, 1, None),
    }
}

/// Apply a Kubernetes resource using server-side apply
async fn apply_resource<T>(ctx: &Context, ns: &str, resource: &T) -> Result<()>
where
    T: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>
        + serde::Serialize
        + DeserializeOwned
        + Clone
        + std::fmt::Debug,
    <T as kube::Resource>::DynamicType: Default,
{
    let api: Api<T> = Api::namespaced(ctx.client.clone(), ns);
    let name = resource.name_any();

    let patch = Patch::Apply(resource);
    // Use a proper field manager name for server-side apply
    let params = PatchParams::apply("postgres-operator").force();

    api.patch(&name, &params, &patch).await?;
    debug!("Applied resource: {}", name);

    Ok(())
}

/// Check if the finalizer is present
fn has_finalizer(cluster: &PostgresCluster) -> bool {
    cluster
        .metadata
        .finalizers
        .as_ref()
        .is_some_and(|f| f.contains(&FINALIZER.to_string()))
}

/// Add the finalizer to the resource
async fn add_finalizer(cluster: &PostgresCluster, ctx: &Context, ns: &str) -> Result<()> {
    let api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);
    let name = cluster.name_any();

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [FINALIZER]
        }
    });

    api.patch(
        &name,
        &PatchParams::apply("postgres-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!("Added finalizer to {}", name);
    Ok(())
}

/// Handle deletion of the PostgresCluster
async fn handle_deletion(cluster: &PostgresCluster, ctx: &Context, ns: &str) -> Result<Action> {
    let name = cluster.name_any();
    info!("Handling deletion of {}", name);

    // Update status to Deleting
    let status_manager = StatusManager::new(cluster, ctx, ns);
    let _ = status_manager.set_deleting().await;

    // Kubernetes will garbage collect owned resources via owner references
    // Just remove the finalizer to allow deletion to proceed

    if has_finalizer(cluster) {
        let api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);

        let patch = serde_json::json!({
            "metadata": {
                "finalizers": null
            }
        });

        api.patch(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        info!("Removed finalizer from {}", name);
    }

    Ok(Action::await_change())
}
