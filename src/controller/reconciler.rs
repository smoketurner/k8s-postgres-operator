//! Reconciliation logic for PostgresCluster resources
//!
//! This module contains the main reconciliation loop that ensures
//! PostgresCluster resources are in their desired state.
//!
//! The reconciler uses a formal finite state machine (FSM) to manage
//! cluster lifecycle transitions. See `state_machine.rs` for the
//! transition table and guards.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, PersistentVolumeClaim, Pod, Secret, Service, ServiceAccount,
};
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use kube::api::{DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};
use serde::de::DeserializeOwned;
use tracing::{debug, error, info, instrument, warn};

use crate::controller::backup_status::BackupStatusCollector;
use crate::controller::context::Context;
use crate::controller::error::{BackoffConfig, Error, Result};
use crate::controller::state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult, determine_event,
};
use crate::controller::status::{StatusManager, spec_changed};
use crate::crd::{ClusterPhase, PostgresCluster};
use crate::resources::{backup, patroni, pdb, pgbouncer, secret, service};

/// Finalizer name for cleanup
pub const FINALIZER: &str = "postgres.example.com/finalizer";

/// Tracks which critical resources are missing for a cluster
#[derive(Debug, Default)]
struct MissingResources {
    secret: bool,
    rbac: bool,
    configmap: bool,
    services: bool,
    statefulset: bool,
    pgbouncer: bool,
}

impl MissingResources {
    fn any_missing(&self) -> bool {
        self.secret
            || self.rbac
            || self.configmap
            || self.services
            || self.statefulset
            || self.pgbouncer
    }
}

impl std::fmt::Display for MissingResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "secret: {}, rbac: {}, configmap: {}, services: {}, statefulset: {}, pgbouncer: {}",
            self.secret, self.rbac, self.configmap, self.services, self.statefulset, self.pgbouncer
        )
    }
}

/// Check which critical resources are missing for a cluster
async fn check_missing_resources(
    cluster: &PostgresCluster,
    ctx: &Context,
    ns: &str,
) -> Result<MissingResources> {
    let name = cluster.name_any();

    // Check Secret
    let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
    let secret = secrets_api
        .get_opt(&format!("{}-credentials", name))
        .await?
        .is_none();

    // Check RBAC resources
    let sa_api: Api<ServiceAccount> = Api::namespaced(ctx.client.clone(), ns);
    let role_api: Api<Role> = Api::namespaced(ctx.client.clone(), ns);
    let rb_api: Api<RoleBinding> = Api::namespaced(ctx.client.clone(), ns);
    let rbac_name = format!("{}-patroni", name);
    let rbac = sa_api.get_opt(&rbac_name).await?.is_none()
        || role_api.get_opt(&rbac_name).await?.is_none()
        || rb_api.get_opt(&rbac_name).await?.is_none();

    // Check ConfigMap
    let cm_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), ns);
    let configmap = cm_api
        .get_opt(&format!("{}-patroni-config", name))
        .await?
        .is_none();

    // Check Services
    let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), ns);
    let services = svc_api
        .get_opt(&format!("{}-primary", name))
        .await?
        .is_none()
        || svc_api.get_opt(&format!("{}-repl", name)).await?.is_none()
        || svc_api
            .get_opt(&format!("{}-headless", name))
            .await?
            .is_none();

    // Check StatefulSet
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);
    let statefulset = sts_api.get_opt(&name).await?.is_none();

    // Check PgBouncer Deployments if enabled
    let deploy_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), ns);
    let pgbouncer = if pgbouncer::is_pgbouncer_enabled(cluster) {
        let pooler_missing = deploy_api
            .get_opt(&format!("{}-pooler", name))
            .await?
            .is_none();
        let replica_pooler_missing = if pgbouncer::is_replica_pooler_enabled(cluster) {
            deploy_api
                .get_opt(&format!("{}-pooler-repl", name))
                .await?
                .is_none()
        } else {
            false
        };
        pooler_missing || replica_pooler_missing
    } else {
        false
    };

    Ok(MissingResources {
        secret,
        rbac,
        configmap,
        services,
        statefulset,
        pgbouncer,
    })
}

/// Timeout for cluster stuck in Creating state (10 minutes)
/// After this duration, the cluster will transition to Failed state
const CREATING_TIMEOUT_SECS: i64 = 600;

/// Default backoff configuration for error handling
fn default_backoff() -> BackoffConfig {
    BackoffConfig::default()
}

/// Main reconciliation function
#[instrument(skip(cluster, ctx), fields(name = %cluster.name_any(), namespace = cluster.namespace().unwrap_or_default()))]
pub async fn reconcile(cluster: Arc<PostgresCluster>, ctx: Arc<Context>) -> Result<Action> {
    let start_time = std::time::Instant::now();
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
    let current_phase = cluster.status.as_ref().map(|s| s.phase).unwrap_or_default();

    // Check if critical owned resources exist
    // If they're missing, we need full reconciliation to recreate them
    let missing = check_missing_resources(&cluster, &ctx, &ns).await?;

    // Skip full reconciliation if spec hasn't changed, cluster is running, AND
    // all critical resources exist
    if !is_spec_changed && current_phase == ClusterPhase::Running && !missing.any_missing() {
        debug!(
            "Spec unchanged for {} (generation: {:?}, observed: {:?}), checking status only",
            name,
            cluster.metadata.generation,
            cluster.status.as_ref().and_then(|s| s.observed_generation)
        );
        // Still check and update status periodically
        return check_and_update_status(&cluster, &ctx, &ns).await;
    }

    // Log why we're doing full reconciliation
    if missing.any_missing() {
        info!(
            "Critical resource missing for {} ({}), performing full reconciliation",
            name, missing
        );
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

    // Record metrics
    let duration_secs = start_time.elapsed().as_secs_f64();

    match result {
        Ok(action) => {
            ctx.record_reconcile(&ns, &name, duration_secs);
            info!(
                "Reconciliation completed successfully in {:.3}s",
                duration_secs
            );
            Ok(action)
        }
        Err(e) => {
            ctx.record_error(&ns, &name);
            error!("Reconciliation failed after {:.3}s: {}", duration_secs, e);
            // Update status to failed with error details
            let status_manager = StatusManager::new(&cluster, &ctx, &ns);
            if let Err(status_err) = status_manager
                .set_failed("ReconciliationFailed", &e.to_string())
                .await
            {
                warn!(
                    "Failed to update status after reconciliation error: {}",
                    status_err
                );
            }
            // Emit warning event for the failure
            ctx.publish_warning_event(
                &cluster,
                "ReconciliationFailed",
                "Reconcile",
                Some(format!("Reconciliation failed: {}", e)),
            )
            .await;
            Err(e)
        }
    }
}

/// Error policy for the controller with exponential backoff
pub fn error_policy(cluster: Arc<PostgresCluster>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = cluster.name_any();
    let backoff = default_backoff();

    // Check if this is a NotFound error (object was deleted)
    // These are expected and should not be logged as errors
    if error.is_not_found() {
        debug!(
            "Object {} no longer exists (likely deleted), not requeuing",
            name
        );
        // Don't requeue - the object is gone
        return Action::await_change();
    }

    // Get retry count from status for proper exponential backoff
    let retry_count = cluster
        .status
        .as_ref()
        .and_then(|s| s.retry_count)
        .unwrap_or(0) as u32;

    let delay = backoff.delay_for_error(error, retry_count);

    if error.is_retryable() {
        warn!(
            "Retryable error for {} (retry #{}): {:?}, requeuing in {:?}",
            name, retry_count, error, delay
        );
    } else {
        error!(
            "Non-retryable error for {} (retry #{}): {:?}, requeuing in {:?} for manual intervention",
            name, retry_count, error, delay
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
    let state_machine = ClusterStateMachine::new();

    // Check StatefulSet status (Patroni uses cluster name directly)
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);

    let (ready_replicas, _, _) = get_statefulset_status(&sts_api, &name).await;

    // Get pod names from Patroni labels (spilo-role)
    let primary_pod = get_patroni_leader(&pods_api, &name).await;
    let replica_pods = get_patroni_replicas(&pods_api, &name).await;

    let current_phase = cluster.status.as_ref().map(|s| s.phase).unwrap_or_default();

    // Build transition context for potential state changes
    let transition_ctx = TransitionContext::new(ready_replicas, cluster.spec.replicas);

    // Collect backup status if backups are enabled and cluster is running
    let collected_backup_status = if current_phase == ClusterPhase::Running
        && backup::is_backup_enabled(cluster)
    {
        let collector = BackupStatusCollector::new(ctx.client.clone(), ns, &name);
        match collector.collect(cluster).await {
            Ok(backup_status) => {
                debug!(
                    cluster = %name,
                    backup_count = ?backup_status.backup_count,
                    wal_healthy = ?backup_status.wal_archiving_healthy,
                    "Collected backup status"
                );
                Some(backup_status)
            }
            Err(e) => {
                debug!(
                    cluster = %name,
                    error = %e,
                    "Failed to collect backup status (non-fatal)"
                );
                None
            }
        }
    } else {
        None
    };

    // Check if cluster has become degraded while running
    if current_phase == ClusterPhase::Running && transition_ctx.is_degraded() {
        let event = ClusterEvent::ReplicasDegraded;
        if let TransitionResult::Success {
            to, description, ..
        } = state_machine.transition(&current_phase, event, &transition_ctx)
        {
            info!("Cluster {} transitioned to {:?}: {}", name, to, description);
            // Update to degraded state
            status_manager
                .set_updating(
                    ready_replicas,
                    cluster.spec.replicas,
                    primary_pod,
                    replica_pods,
                )
                .await?;
        }
    } else if ready_replicas >= cluster.spec.replicas {
        status_manager
            .set_running_with_backup(
                ready_replicas,
                cluster.spec.replicas,
                primary_pod,
                replica_pods,
                &cluster.spec.version,
                collected_backup_status,
            )
            .await?;
    }

    // Requeue based on phase - running clusters need less frequent checks
    let requeue_duration = match current_phase {
        ClusterPhase::Running => Duration::from_secs(60),
        ClusterPhase::Degraded => Duration::from_secs(15),
        _ => Duration::from_secs(30),
    };

    Ok(Action::requeue(requeue_duration))
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
async fn reconcile_cluster(cluster: &PostgresCluster, ctx: &Context, ns: &str) -> Result<Action> {
    let name = cluster.name_any();
    let status_manager = StatusManager::new(cluster, ctx, ns);
    let state_machine = ClusterStateMachine::new();

    info!(
        "Reconciling PostgreSQL cluster with Patroni ({} replicas)",
        cluster.spec.replicas
    );

    // Get current phase and status info
    let current_phase = cluster.status.as_ref().map(|s| s.phase).unwrap_or_default();

    let previous_replicas = cluster.status.as_ref().and_then(|s| s.previous_replicas);
    let is_spec_changed = spec_changed(cluster);

    // Check for version downgrade before applying any resources
    // Version downgrades are not supported because PostgreSQL data format is not backwards compatible
    if is_spec_changed
        && let Some(status) = &cluster.status
        && let Some(current_version) = &status.current_version
        && let Err(e) = crate::controller::validation::validate_version_upgrade(
            current_version,
            &cluster.spec.version,
        )
    {
        warn!("Version change rejected for {}: {}", name, e);
        status_manager
            .set_failed("VersionDowngradeRejected", &e.to_string())
            .await?;
        ctx.publish_warning_event(
            cluster,
            "VersionDowngradeRejected",
            "Validation",
            Some(e.to_string()),
        )
        .await;
        // Don't requeue - requires user intervention to fix
        return Ok(Action::await_change());
    }

    // Validate TLS configuration if enabled
    if let Some(ref tls) = cluster.spec.tls
        && tls.enabled
    {
        // If TLS is enabled but no cert_secret is specified, that's a warning
        // (Spilo will generate self-signed certs)
        if tls.cert_secret.is_none() {
            info!(
                "TLS enabled for {} but no cert_secret specified - Spilo will use self-signed certificates",
                name
            );
        } else if let Some(ref cert_secret_name) = tls.cert_secret {
            // Validate that the cert secret exists
            let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
            if secrets_api.get_opt(cert_secret_name).await?.is_none() {
                warn!(
                    "TLS cert_secret '{}' not found for cluster {}",
                    cert_secret_name, name
                );
                status_manager
                    .set_failed(
                        "TLSSecretNotFound",
                        &format!("TLS certificate secret '{}' not found", cert_secret_name),
                    )
                    .await?;
                ctx.publish_warning_event(
                    cluster,
                    "TLSSecretNotFound",
                    "Validation",
                    Some(format!(
                        "TLS certificate secret '{}' not found",
                        cert_secret_name
                    )),
                )
                .await;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        }

        // Validate CA secret if specified
        if let Some(ref ca_secret_name) = tls.ca_secret {
            let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
            if secrets_api.get_opt(ca_secret_name).await?.is_none() {
                warn!(
                    "TLS ca_secret '{}' not found for cluster {}",
                    ca_secret_name, name
                );
                status_manager
                    .set_failed(
                        "TLSCASecretNotFound",
                        &format!("TLS CA certificate secret '{}' not found", ca_secret_name),
                    )
                    .await?;
                ctx.publish_warning_event(
                    cluster,
                    "TLSCASecretNotFound",
                    "Validation",
                    Some(format!(
                        "TLS CA certificate secret '{}' not found",
                        ca_secret_name
                    )),
                )
                .await;
                return Ok(Action::requeue(Duration::from_secs(30)));
            }
        }
    }

    // Validate backup configuration if enabled
    if let Some(ref backup_spec) = cluster.spec.backup {
        // Validate credentials secret exists
        let credentials_secret_name = backup_spec.credentials_secret_name();
        let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
        if secrets_api.get_opt(credentials_secret_name).await?.is_none() {
            let destination_type = backup_spec.destination.destination_type();
            warn!(
                "Backup credentials secret '{}' not found for {} destination on cluster {}",
                credentials_secret_name, destination_type, name
            );
            status_manager
                .set_failed(
                    "BackupCredentialsNotFound",
                    &format!(
                        "Backup credentials secret '{}' not found for {} destination",
                        credentials_secret_name, destination_type
                    ),
                )
                .await?;
            ctx.publish_warning_event(
                cluster,
                "BackupCredentialsNotFound",
                "Validation",
                Some(format!(
                    "Backup credentials secret '{}' not found for {} destination",
                    credentials_secret_name, destination_type
                )),
            )
            .await;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }

        // Validate encryption key secret if encryption is enabled
        if let Some(ref encryption) = backup_spec.encryption
            && encryption.enabled {
                if let Some(ref key_secret_name) = encryption.key_secret {
                    if secrets_api.get_opt(key_secret_name).await?.is_none() {
                        warn!(
                            "Backup encryption key secret '{}' not found for cluster {}",
                            key_secret_name, name
                        );
                        status_manager
                            .set_failed(
                                "EncryptionKeyNotFound",
                                &format!(
                                    "Backup encryption key secret '{}' not found",
                                    key_secret_name
                                ),
                            )
                            .await?;
                        ctx.publish_warning_event(
                            cluster,
                            "EncryptionKeyNotFound",
                            "Validation",
                            Some(format!(
                                "Backup encryption key secret '{}' not found",
                                key_secret_name
                            )),
                        )
                        .await;
                        return Ok(Action::requeue(Duration::from_secs(30)));
                    }
                } else {
                    // Encryption enabled but no key secret specified
                    warn!(
                        "Backup encryption enabled but no key secret specified for cluster {}",
                        name
                    );
                    status_manager
                        .set_failed(
                            "EncryptionKeyNotSpecified",
                            "Backup encryption is enabled but no key secret is specified",
                        )
                        .await?;
                    ctx.publish_warning_event(
                        cluster,
                        "EncryptionKeyNotSpecified",
                        "Validation",
                        Some("Backup encryption is enabled but no key secret is specified".to_string()),
                    )
                    .await;
                    return Ok(Action::requeue(Duration::from_secs(30)));
                }
            }

        info!(
            "Backup configured for cluster {}: {} destination, schedule '{}'",
            name,
            backup_spec.destination.destination_type(),
            backup_spec.schedule
        );
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

    // Apply PgBouncer resources if enabled
    if pgbouncer::is_pgbouncer_enabled(cluster) {
        info!("PgBouncer is enabled for cluster {}", name);

        // Apply PgBouncer ConfigMap
        let pgbouncer_config = pgbouncer::generate_pgbouncer_configmap(cluster);
        apply_resource(ctx, ns, &pgbouncer_config).await?;

        // Apply PgBouncer Deployment
        let pgbouncer_deployment = pgbouncer::generate_pgbouncer_deployment(cluster);
        apply_resource(ctx, ns, &pgbouncer_deployment).await?;

        // Apply PgBouncer Service
        let pgbouncer_svc = pgbouncer::generate_pgbouncer_service(cluster);
        apply_resource(ctx, ns, &pgbouncer_svc).await?;

        // Apply replica pooler if enabled
        if pgbouncer::is_replica_pooler_enabled(cluster) {
            info!("Replica PgBouncer pooler is enabled for cluster {}", name);

            let pgbouncer_replica_config = pgbouncer::generate_pgbouncer_replica_configmap(cluster);
            apply_resource(ctx, ns, &pgbouncer_replica_config).await?;

            let pgbouncer_replica_deployment =
                pgbouncer::generate_pgbouncer_replica_deployment(cluster);
            apply_resource(ctx, ns, &pgbouncer_replica_deployment).await?;

            let pgbouncer_replica_svc = pgbouncer::generate_pgbouncer_replica_service(cluster);
            apply_resource(ctx, ns, &pgbouncer_replica_svc).await?;
        }
    }

    // Check StatefulSet status
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), ns);

    let (ready_replicas, _, _) = get_statefulset_status(&sts_api, &name).await;

    // Get pod names from Patroni labels (spilo-role)
    let primary_pod = get_patroni_leader(&pods_api, &name).await;
    let replica_pods = get_patroni_replicas(&pods_api, &name).await;

    // Check for stuck Creating state with timeout
    if current_phase == ClusterPhase::Creating
        && let Some(stuck_reason) = check_creating_timeout(cluster, &pvc_api, &name).await
    {
        warn!("Cluster {} stuck in Creating state: {}", name, stuck_reason);
        status_manager
            .set_failed("CreatingTimeout", &stuck_reason)
            .await?;
        ctx.publish_warning_event(cluster, "CreatingTimeout", "Timeout", Some(stuck_reason))
            .await;
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    // Build transition context
    let mut transition_ctx = TransitionContext::new(ready_replicas, cluster.spec.replicas);
    transition_ctx.spec_changed = is_spec_changed;
    transition_ctx.retry_count = cluster
        .status
        .as_ref()
        .and_then(|s| s.retry_count)
        .unwrap_or(0);

    // Determine the event based on current state
    let event = determine_event(&current_phase, &transition_ctx, false, previous_replicas);

    // Attempt state transition using FSM
    let transition_result =
        state_machine.transition(&current_phase, event.clone(), &transition_ctx);

    // Handle the transition result
    match transition_result {
        TransitionResult::Success {
            from,
            to,
            description,
            ..
        } => {
            info!("State transition: {:?} -> {:?} ({})", from, to, description);

            // Emit Kubernetes event for the state transition
            emit_state_transition_event(cluster, ctx, from, to, description).await;

            // Update status based on new state
            match to {
                ClusterPhase::Creating => {
                    status_manager
                        .set_creating(ready_replicas, cluster.spec.replicas, primary_pod)
                        .await?;
                }
                ClusterPhase::Running => {
                    status_manager
                        .set_running(
                            ready_replicas,
                            cluster.spec.replicas,
                            primary_pod,
                            replica_pods,
                            &cluster.spec.version,
                        )
                        .await?;
                }
                ClusterPhase::Updating | ClusterPhase::Scaling => {
                    status_manager
                        .set_updating(
                            ready_replicas,
                            cluster.spec.replicas,
                            primary_pod,
                            replica_pods,
                        )
                        .await?;
                }
                ClusterPhase::Degraded | ClusterPhase::Recovering => {
                    // For degraded/recovering, still update with current replica info
                    status_manager
                        .set_updating(
                            ready_replicas,
                            cluster.spec.replicas,
                            primary_pod,
                            replica_pods,
                        )
                        .await?;
                }
                ClusterPhase::Failed => {
                    status_manager
                        .set_failed("ReconciliationFailed", "Cluster entered failed state")
                        .await?;
                }
                ClusterPhase::Pending | ClusterPhase::Deleting => {
                    // These are handled elsewhere (initial state / deletion handler)
                }
            }
        }
        TransitionResult::InvalidTransition { current, event } => {
            debug!(
                "No state transition needed: {:?} + {:?} event (staying in current state)",
                current, event
            );

            // Update status with current replica counts even if no state change
            match current_phase {
                ClusterPhase::Creating => {
                    status_manager
                        .set_creating(ready_replicas, cluster.spec.replicas, primary_pod)
                        .await?;
                }
                ClusterPhase::Running => {
                    status_manager
                        .set_running(
                            ready_replicas,
                            cluster.spec.replicas,
                            primary_pod,
                            replica_pods,
                            &cluster.spec.version,
                        )
                        .await?;
                }
                _ => {}
            }
        }
        TransitionResult::GuardFailed {
            from, to, reason, ..
        } => {
            debug!(
                "State transition {:?} -> {:?} blocked by guard: {}",
                from, to, reason
            );

            // Update status with current info
            if current_phase == ClusterPhase::Creating {
                status_manager
                    .set_creating(ready_replicas, cluster.spec.replicas, primary_pod)
                    .await?;
            }
        }
    }

    // Requeue interval based on current phase
    let requeue_duration = match current_phase {
        ClusterPhase::Creating | ClusterPhase::Recovering => Duration::from_secs(5),
        ClusterPhase::Updating | ClusterPhase::Scaling => Duration::from_secs(10),
        ClusterPhase::Degraded => Duration::from_secs(15),
        ClusterPhase::Running => Duration::from_secs(60),
        ClusterPhase::Failed => Duration::from_secs(30),
        ClusterPhase::Pending | ClusterPhase::Deleting => Duration::from_secs(5),
    };

    Ok(Action::requeue(requeue_duration))
}

/// Get the Patroni leader pod name
async fn get_patroni_leader(api: &Api<Pod>, cluster_name: &str) -> Option<String> {
    let label_selector = format!(
        "postgres.example.com/cluster={},spilo-role=master",
        cluster_name
    );

    match api
        .list(&ListParams::default().labels(&label_selector))
        .await
    {
        Ok(pods) => pods.items.first().and_then(|p| p.metadata.name.clone()),
        Err(_) => None,
    }
}

/// Get Patroni replica pod names
async fn get_patroni_replicas(api: &Api<Pod>, cluster_name: &str) -> Vec<String> {
    let label_selector = format!(
        "postgres.example.com/cluster={},spilo-role=replica",
        cluster_name
    );

    match api
        .list(&ListParams::default().labels(&label_selector))
        .await
    {
        Ok(pods) => pods
            .items
            .iter()
            .filter_map(|p| p.metadata.name.clone())
            .collect(),
        Err(_) => vec![],
    }
}

/// Apply a Role using server-side apply
async fn apply_role(
    ctx: &Context,
    ns: &str,
    resource: &k8s_openapi::api::rbac::v1::Role,
) -> Result<()> {
    let api: Api<k8s_openapi::api::rbac::v1::Role> = Api::namespaced(ctx.client.clone(), ns);
    let name = resource
        .metadata
        .name
        .as_ref()
        .ok_or(Error::MissingObjectKey("Role.metadata.name"))?;

    let patch = Patch::Apply(resource);
    let params = PatchParams::apply("postgres-operator").force();

    api.patch(name, &params, &patch).await?;
    debug!("Applied Role: {}", name);

    Ok(())
}

/// Apply a RoleBinding using server-side apply
async fn apply_role_binding(
    ctx: &Context,
    ns: &str,
    resource: &k8s_openapi::api::rbac::v1::RoleBinding,
) -> Result<()> {
    let api: Api<k8s_openapi::api::rbac::v1::RoleBinding> = Api::namespaced(ctx.client.clone(), ns);
    let name = resource
        .metadata
        .name
        .as_ref()
        .ok_or(Error::MissingObjectKey("RoleBinding.metadata.name"))?;

    let patch = Patch::Apply(resource);
    let params = PatchParams::apply("postgres-operator").force();

    api.patch(name, &params, &patch).await?;
    debug!("Applied RoleBinding: {}", name);

    Ok(())
}

/// Get StatefulSet status information
async fn get_statefulset_status(api: &Api<StatefulSet>, name: &str) -> (i32, i32, Option<String>) {
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
    if let Err(e) = status_manager.set_deleting().await {
        warn!("Failed to update status to Deleting: {}", e);
    }

    // Emit event for deletion
    ctx.publish_normal_event(
        cluster,
        "ClusterDeleting",
        "Delete",
        Some(format!("Cluster {} is being deleted", name)),
    )
    .await;

    // Delete Patroni DCS endpoints that are created by Patroni at runtime
    // These are NOT owned by the PostgresCluster and won't be garbage collected
    // If left behind, they cause new clusters with the same name to get stuck
    let endpoints_api: Api<Endpoints> = Api::namespaced(ctx.client.clone(), ns);
    for suffix in ["", "-config", "-sync"] {
        let endpoint_name = if suffix.is_empty() {
            name.clone()
        } else {
            format!("{}{}", name, suffix)
        };
        match endpoints_api
            .delete(&endpoint_name, &DeleteParams::default())
            .await
        {
            Ok(_) => {
                debug!("Deleted Patroni DCS endpoint: {}", endpoint_name);
            }
            Err(kube::Error::Api(err)) if err.code == 404 => {
                // Endpoint doesn't exist, that's fine
                debug!("Patroni endpoint {} not found, skipping", endpoint_name);
            }
            Err(e) => {
                warn!("Failed to delete Patroni endpoint {}: {}", endpoint_name, e);
            }
        }
    }

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

/// Check if a cluster has been stuck in Creating state for too long
/// Returns Some(reason) if stuck, None if still within timeout
async fn check_creating_timeout(
    cluster: &PostgresCluster,
    pvc_api: &Api<PersistentVolumeClaim>,
    cluster_name: &str,
) -> Option<String> {
    // Check if we have a phase_started_at timestamp
    let phase_started_at = cluster
        .status
        .as_ref()
        .and_then(|s| s.phase_started_at.as_ref())
        .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let Some(started_at) = phase_started_at else {
        // No timestamp yet, can't determine timeout
        return None;
    };

    let duration_in_creating = Utc::now().signed_duration_since(started_at);
    if duration_in_creating.num_seconds() < CREATING_TIMEOUT_SECS {
        // Still within timeout
        return None;
    }

    // Timeout exceeded, check for specific issues
    // Check if PVCs are stuck pending
    let label_selector = format!("postgres.example.com/cluster={}", cluster_name);
    if let Ok(pvcs) = pvc_api
        .list(&ListParams::default().labels(&label_selector))
        .await
    {
        for pvc in pvcs.items {
            let pvc_name = pvc.metadata.name.as_deref().unwrap_or("unknown");
            let phase = pvc
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown");

            if phase == "Pending" {
                // Check for storage class issues in events
                let storage_class = pvc
                    .spec
                    .as_ref()
                    .and_then(|s| s.storage_class_name.as_deref())
                    .unwrap_or("default");

                return Some(format!(
                    "Cluster stuck in Creating state for {} minutes. \
                     PVC '{}' is Pending - storage class '{}' may not exist or be provisioning. \
                     Fix the spec.storage.storageClass or delete the cluster.",
                    duration_in_creating.num_minutes(),
                    pvc_name,
                    storage_class
                ));
            }
        }
    }

    // Generic timeout message if no specific issue found
    Some(format!(
        "Cluster stuck in Creating state for {} minutes with 0 ready replicas. \
         Check pod events and logs for details.",
        duration_in_creating.num_minutes()
    ))
}

/// Emit a Kubernetes Event for a state transition
async fn emit_state_transition_event(
    cluster: &PostgresCluster,
    ctx: &Context,
    from: ClusterPhase,
    to: ClusterPhase,
    description: &str,
) {
    // Determine if this is a normal or warning event
    let is_warning = matches!(to, ClusterPhase::Failed | ClusterPhase::Degraded);

    // Create reason based on transition
    let reason = match to {
        ClusterPhase::Creating => "ClusterCreating",
        ClusterPhase::Running => "ClusterReady",
        ClusterPhase::Updating => "ClusterUpdating",
        ClusterPhase::Scaling => "ClusterScaling",
        ClusterPhase::Degraded => "ClusterDegraded",
        ClusterPhase::Recovering => "ClusterRecovering",
        ClusterPhase::Failed => "ClusterFailed",
        ClusterPhase::Deleting => "ClusterDeleting",
        ClusterPhase::Pending => "ClusterPending",
    };

    let note = Some(format!(
        "State changed from {:?} to {:?}: {}",
        from, to, description
    ));

    if is_warning {
        ctx.publish_warning_event(cluster, reason, "StateTransition", note)
            .await;
    } else {
        ctx.publish_normal_event(cluster, reason, "StateTransition", note)
            .await;
    }
}

/// Get pod generation and sync info for Kubernetes 1.35+ pod generation tracking (KEP-5067).
///
/// Returns a PodInfo struct with generation tracking data for each pod.
/// The `spec_applied` field indicates whether the kubelet has processed the latest pod spec.
#[instrument(skip(ctx))]
pub async fn get_pod_info(
    ctx: &Context,
    ns: &str,
    cluster_name: &str,
) -> Result<Vec<crate::crd::PodInfo>> {
    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);
    let label_selector = format!("postgres.example.com/cluster={}", cluster_name);
    let lp = ListParams::default().labels(&label_selector);

    let pod_list = pods.list(&lp).await?;
    let mut pod_infos = Vec::new();

    for pod in pod_list.items {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let generation = pod.metadata.generation;

        // Get observedGeneration from pod status (Kubernetes 1.35+ feature)
        // This field indicates when the kubelet has processed the pod spec
        let observed_generation = pod.status.as_ref().and_then(|s| s.observed_generation);

        // Pod spec is applied when observedGeneration matches generation
        let spec_applied = match (generation, observed_generation) {
            (Some(current_gen), Some(observed_gen)) => current_gen == observed_gen,
            // If either is missing, assume spec is applied (backwards compatibility)
            _ => true,
        };

        // Get role from spilo-role label
        let role = pod
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("spilo-role").cloned());

        // Check if pod is ready
        let ready = pod
            .status
            .as_ref()
            .and_then(|s| s.conditions.as_ref())
            .and_then(|conds| conds.iter().find(|c| c.type_ == "Ready"))
            .is_some_and(|c| c.status == "True");

        pod_infos.push(crate::crd::PodInfo {
            name,
            generation,
            observed_generation,
            spec_applied,
            role,
            ready,
        });
    }

    Ok(pod_infos)
}

/// Get resize status for pods (Kubernetes 1.35+ in-place resize, KEP-1287).
///
/// Returns the resize status for each pod, including allocated resources
/// and whether resize is in progress, deferred, or infeasible.
#[instrument(skip(ctx))]
pub async fn get_pod_resize_status(
    ctx: &Context,
    ns: &str,
    cluster_name: &str,
) -> Result<Vec<crate::crd::PodResourceResizeStatus>> {
    use crate::crd::{PodResizeStatus, PodResourceResizeStatus, ResourceList};

    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);
    let label_selector = format!("postgres.example.com/cluster={}", cluster_name);
    let lp = ListParams::default().labels(&label_selector);

    let pod_list = pods.list(&lp).await?;
    let mut resize_statuses = Vec::new();

    for pod in pod_list.items {
        let pod_name = pod.metadata.name.clone().unwrap_or_default();

        // Get resize status from pod conditions (Kubernetes 1.35+)
        // The resize field is in status.resize
        let resize_str = pod.status.as_ref().and_then(|s| {
            // The resize field is a string in the pod status
            // We access it via JSON since k8s-openapi v1_34 doesn't have it
            let status_json = serde_json::to_value(s).ok()?;
            status_json.get("resize")?.as_str().map(|s| s.to_string())
        });

        let status = match resize_str.as_deref() {
            Some("InProgress") => PodResizeStatus::InProgress,
            Some("Proposed") => PodResizeStatus::Proposed,
            Some("Deferred") => PodResizeStatus::Deferred,
            Some("Infeasible") => PodResizeStatus::Infeasible,
            _ => PodResizeStatus::NoResize,
        };

        // Get allocated resources from container status
        // This shows the actual resources assigned to the container after resize
        let allocated_resources = pod
            .status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|containers| containers.first())
            .and_then(|c| {
                // Access allocatedResources via JSON since k8s-openapi v1_34 doesn't have it
                let container_json = serde_json::to_value(c).ok()?;
                let allocated = container_json.get("allocatedResources")?;

                let cpu = allocated
                    .get("cpu")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let memory = allocated
                    .get("memory")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if cpu.is_some() || memory.is_some() {
                    Some(ResourceList { cpu, memory })
                } else {
                    None
                }
            });

        // Only include pods that have resize activity or allocated resources
        if status != PodResizeStatus::NoResize || allocated_resources.is_some() {
            resize_statuses.push(PodResourceResizeStatus {
                pod_name,
                status,
                allocated_resources,
                last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                message: None,
            });
        }
    }

    Ok(resize_statuses)
}

/// Check if all pods have synced (observedGeneration == generation).
/// Returns true if all pods are synced, false if any are pending.
pub fn all_pods_synced(pod_infos: &[crate::crd::PodInfo]) -> bool {
    pod_infos.iter().all(|p| p.spec_applied)
}

/// Check if any pod has a resize in progress.
pub fn any_resize_in_progress(resize_statuses: &[crate::crd::PodResourceResizeStatus]) -> bool {
    resize_statuses.iter().any(|s| {
        matches!(
            s.status,
            crate::crd::PodResizeStatus::InProgress | crate::crd::PodResizeStatus::Proposed
        )
    })
}
