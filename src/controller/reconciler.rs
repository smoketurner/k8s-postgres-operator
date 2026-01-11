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
use kube::api::{AttachParams, DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, ResourceExt};
use serde::de::DeserializeOwned;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, instrument, warn};

use crate::controller::backup_status::{BackupEvent, BackupStatusCollector, detect_backup_events};
use crate::controller::context::Context;
use crate::controller::error::{BackoffConfig, Error, Result};
use crate::controller::replication_lag::collect_replication_lag;
use crate::controller::state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult, determine_event,
};
use crate::controller::status::{StatusManager, spec_changed};
use crate::crd::{ClusterPhase, PostgresCluster};
use crate::resources::{
    backup, certificate, network_policy, patroni, pdb, pgbouncer, scaled_object, secret, service,
};

/// Finalizer name for cleanup
pub const FINALIZER: &str = "postgres-operator.smoketurner.com/finalizer";

/// Annotation to trigger a manual backup
/// Value should be a unique identifier (e.g., timestamp) to distinguish triggers
pub const BACKUP_TRIGGER_ANNOTATION: &str = "postgres-operator.smoketurner.com/trigger-backup";

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

    // Record cluster replica metrics for fleet observability
    ctx.record_cluster_replicas(ns, &name, cluster.spec.replicas, ready_replicas);

    // Get pod names from Patroni labels (spilo-role)
    let primary_pod = get_patroni_leader(&pods_api, &name).await;
    let replica_pods = get_patroni_replicas(&pods_api, &name).await;

    let current_phase = cluster.status.as_ref().map(|s| s.phase).unwrap_or_default();

    // Build transition context for potential state changes
    let transition_ctx = TransitionContext::new(ready_replicas, cluster.spec.replicas);

    // Collect backup status if backups are enabled and cluster is running
    let collected_backup_status =
        if current_phase == ClusterPhase::Running && backup::is_backup_enabled(cluster) {
            let collector = BackupStatusCollector::new(ctx.client.clone(), ns, &name);
            match collector.collect(cluster).await {
                Ok(backup_status) => {
                    debug!(
                        cluster = %name,
                        backup_count = ?backup_status.backup_count,
                        wal_healthy = ?backup_status.wal_archiving_healthy,
                        "Collected backup status"
                    );

                    // Detect and emit backup events
                    let previous_backup = cluster.status.as_ref().and_then(|s| s.backup.as_ref());
                    let events = detect_backup_events(previous_backup, &backup_status);
                    for event in events {
                        emit_backup_event(ctx, cluster, &event).await;
                    }

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

    // Collect replication lag status for running clusters with replicas
    // This is collected before the main status update to consolidate into a single update
    let collected_replication_lag =
        if current_phase == ClusterPhase::Running && cluster.spec.replicas > 1 {
            match collect_replication_lag(ctx.client.clone(), cluster).await {
                Ok(lag_status) => {
                    // Log if any replicas are lagging
                    if lag_status.any_exceeds_threshold {
                        warn!(
                            cluster = %name,
                            max_lag_bytes = ?lag_status.max_lag_bytes,
                            lagging_replicas = lag_status.replicas.iter()
                                .filter(|r| r.exceeds_threshold)
                                .map(|r| r.pod_name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                            "Replicas exceeding replication lag threshold"
                        );
                    } else {
                        debug!(
                            cluster = %name,
                            max_lag_bytes = ?lag_status.max_lag_bytes,
                            "Replication lag within threshold"
                        );
                    }
                    Some(lag_status)
                }
                Err(e) => {
                    debug!(
                        cluster = %name,
                        error = %e,
                        "Failed to collect replication lag (non-fatal)"
                    );
                    None
                }
            }
        } else {
            None
        };

    // Update KEDA pause state based on replication lag
    // This prevents KEDA from scaling up while replicas are struggling to catch up
    // Scale-down is still allowed since reducing load won't make lag worse
    if let Some(lag_status) = &collected_replication_lag
        && scaled_object::should_enable_reader_scaling(cluster)
    {
        match scaled_object::update_scaling_pause_state(
            ctx.client.clone(),
            cluster,
            lag_status.any_exceeds_threshold,
        )
        .await
        {
            Ok(Some(true)) => {
                // Scale-out was paused - emit event
                ctx.publish_warning_event(
                    cluster,
                    "ScaleOutPaused",
                    "UpdateScaling",
                    Some(
                        "Paused KEDA scale-out due to replication lag exceeding threshold (scale-down still allowed)"
                            .to_string(),
                    ),
                )
                .await;
            }
            Ok(Some(false)) => {
                // Scale-out was resumed - emit event
                ctx.publish_normal_event(
                    cluster,
                    "ScaleOutResumed",
                    "UpdateScaling",
                    Some("Resumed KEDA scale-out, replication lag within threshold".to_string()),
                )
                .await;
            }
            Ok(None) => {
                // No change needed
            }
            Err(e) => {
                // Non-fatal - log and continue
                warn!(
                    cluster = %name,
                    error = %e,
                    "Failed to update KEDA pause state"
                );
            }
        }
    }

    // Check for manual backup trigger annotation
    if current_phase == ClusterPhase::Running
        && backup::is_backup_enabled(cluster)
        && let Err(e) = check_and_trigger_manual_backup(cluster, ctx, ns).await
    {
        // Log but don't fail the reconciliation for backup trigger errors
        warn!(
            cluster = %name,
            error = %e,
            "Failed to process manual backup trigger"
        );
    }

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
        // Consolidated status update: backup status + replication lag in a single call
        // This avoids race conditions from multiple separate status updates
        status_manager
            .set_running_full(
                ready_replicas,
                cluster.spec.replicas,
                primary_pod,
                replica_pods,
                cluster.spec.version.as_str(),
                collected_backup_status,
                collected_replication_lag.as_ref(),
            )
            .await?;
    }

    // Apply KEDA scaling resources if configured (also needed in status-only path)
    // This ensures KEDA resources are created even for running clusters
    if let Err(e) = reconcile_keda_resources(cluster, ctx, ns).await {
        warn!(
            cluster = %name,
            error = %e,
            "Failed to reconcile KEDA resources (non-fatal)"
        );
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
            cluster.spec.version.as_str(),
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

    // Validate TLS configuration
    // TLS is enabled by default for security
    let tls = &cluster.spec.tls;
    if tls.enabled {
        // TLS enabled requires a cert-manager issuer reference
        if tls.issuer_ref.is_none() {
            warn!(
                "TLS enabled for {} but no issuerRef specified - cert-manager issuer is required",
                name
            );
            status_manager
                .set_failed(
                    "TLSIssuerNotConfigured",
                    "TLS is enabled but no cert-manager issuerRef is configured. \
                     Specify spec.tls.issuerRef with a valid Issuer or ClusterIssuer, \
                     or set spec.tls.enabled: false to disable TLS.",
                )
                .await?;
            ctx.publish_warning_event(
                cluster,
                "TLSIssuerNotConfigured",
                "Validation",
                Some(
                    "TLS is enabled but no cert-manager issuerRef is configured. \
                     Specify spec.tls.issuerRef or disable TLS."
                        .to_string(),
                ),
            )
            .await;
            // Don't requeue - requires user intervention to configure issuer
            return Ok(Action::await_change());
        } else {
            let issuer = tls.issuer_ref.as_ref().unwrap();
            info!(
                "TLS enabled for {} using cert-manager {} '{}'",
                name, issuer.kind, issuer.name
            );
        }
        // Apply cert-manager Certificate resource
        if let Some(cert) = certificate::generate_certificate(cluster) {
            apply_certificate(ctx, ns, &cert).await?;
            debug!("Applied cert-manager Certificate for cluster {}", name);
        }
    } else {
        // TLS explicitly disabled - warn about security implications
        warn!(
            "TLS is disabled for cluster {} - connections will be unencrypted",
            name
        );
        ctx.publish_warning_event(
            cluster,
            "TLSDisabled",
            "Security",
            Some("TLS is disabled - PostgreSQL connections will be unencrypted".to_string()),
        )
        .await;
    }

    // Validate backup configuration if enabled
    if let Some(ref backup_spec) = cluster.spec.backup {
        // Validate credentials secret exists
        let credentials_secret_name = backup_spec.credentials_secret_name();
        let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);
        if secrets_api
            .get_opt(credentials_secret_name)
            .await?
            .is_none()
        {
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

        // Require encryption for backups (security by design)
        // Backups contain sensitive data and must be encrypted at rest
        // The encryption section must be present - its presence enables encryption
        if backup_spec.encryption.is_none() {
            warn!(
                "Backup configured for cluster {} but encryption is not configured - backups must be encrypted",
                name
            );
            status_manager
                .set_failed(
                    "BackupEncryptionRequired",
                    "Backup encryption is required. Configure spec.backup.encryption with \
                     a keySecret to protect backup data at rest.",
                )
                .await?;
            ctx.publish_warning_event(
                cluster,
                "BackupEncryptionRequired",
                "Validation",
                Some(
                    "Backup encryption is required for security. \
                     Add spec.backup.encryption.keySecret with your encryption key secret name."
                        .to_string(),
                ),
            )
            .await;
            // Don't requeue - requires user intervention to configure encryption
            return Ok(Action::await_change());
        }

        // Validate encryption key secret exists
        if let Some(ref encryption) = backup_spec.encryption {
            let key_secret_name = &encryption.key_secret;
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

    // Ensure NetworkPolicy exists (always generated - primary security boundary)
    let np = network_policy::generate_network_policy(cluster);
    apply_resource(ctx, ns, &np).await?;

    // Ensure Patroni StatefulSet exists (single StatefulSet for all members)
    // Apply resize policy for Kubernetes 1.35+ in-place resource resizing
    let restart_on_resize = cluster
        .spec
        .resources
        .as_ref()
        .and_then(|r| r.restart_on_resize)
        .unwrap_or(false);
    // Check if KEDA is managing replicas - if so, don't set replica count
    // to avoid conflicts between the operator and KEDA's scaling decisions
    let keda_managed = scaled_object::is_keda_managing_replicas(cluster);
    let sts = patroni::generate_patroni_statefulset(cluster, keda_managed);
    let sts = patroni::add_resize_policy_to_statefulset(sts, restart_on_resize);
    apply_resource(ctx, ns, &sts).await?;

    // Ensure Patroni Services exist
    let primary_svc = service::generate_primary_service(cluster);
    apply_resource(ctx, ns, &primary_svc).await?;

    // Only create replicas service if replicas > 1 (production mode)
    // With replicas=1 (development mode), there are no read replicas
    if cluster.spec.replicas > 1 {
        let replicas_svc = service::generate_replicas_service(cluster);
        apply_resource(ctx, ns, &replicas_svc).await?;
    } else {
        // Clean up replicas service if it exists (cluster scaled down to 1 replica)
        let replicas_svc_name = format!("{}-repl", name);
        let svc_api: Api<Service> = Api::namespaced(ctx.client.clone(), ns);
        match svc_api
            .delete(&replicas_svc_name, &DeleteParams::default())
            .await
        {
            Ok(_) => {
                info!(
                    "Deleted replicas service {} (cluster scaled to single replica)",
                    replicas_svc_name
                );
            }
            Err(kube::Error::Api(err)) if err.code == 404 => {
                // Service doesn't exist, that's fine
            }
            Err(e) => {
                warn!(
                    "Failed to delete replicas service {}: {}",
                    replicas_svc_name, e
                );
            }
        }
    }

    let headless_svc = service::generate_headless_service(cluster);
    apply_resource(ctx, ns, &headless_svc).await?;

    // Apply PgBouncer resources if enabled
    if pgbouncer::is_pgbouncer_enabled(cluster) {
        info!("PgBouncer is enabled for cluster {}", name);

        // Apply PgBouncer ConfigMap
        let pgbouncer_config = pgbouncer::generate_pgbouncer_configmap(cluster);
        apply_resource(ctx, ns, &pgbouncer_config).await?;

        // Apply PgBouncer Deployment with resize policy for Kubernetes 1.35+
        let pgbouncer_restart_on_resize = cluster
            .spec
            .pgbouncer
            .as_ref()
            .and_then(|p| p.resources.as_ref())
            .and_then(|r| r.restart_on_resize)
            .unwrap_or(false);
        let pgbouncer_deployment = pgbouncer::generate_pgbouncer_deployment(cluster);
        let pgbouncer_deployment = pgbouncer::add_resize_policy_to_deployment(
            pgbouncer_deployment,
            pgbouncer_restart_on_resize,
        );
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
            let pgbouncer_replica_deployment = pgbouncer::add_resize_policy_to_deployment(
                pgbouncer_replica_deployment,
                pgbouncer_restart_on_resize,
            );
            apply_resource(ctx, ns, &pgbouncer_replica_deployment).await?;

            let pgbouncer_replica_svc = pgbouncer::generate_pgbouncer_replica_service(cluster);
            apply_resource(ctx, ns, &pgbouncer_replica_svc).await?;
        }
    }

    // Apply KEDA scaling resources if configured
    // This is a no-op if scaling is not configured or KEDA is not installed
    if let Err(e) = reconcile_keda_resources(cluster, ctx, ns).await {
        warn!(
            cluster = %name,
            error = %e,
            "Failed to reconcile KEDA resources (non-fatal)"
        );
        // Don't fail reconciliation if KEDA resources can't be applied
        // The cluster will still work, just without auto-scaling
    }

    // Check StatefulSet status
    let sts_api: Api<StatefulSet> = Api::namespaced(ctx.client.clone(), ns);
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), ns);

    let (ready_replicas, _, _) = get_statefulset_status(&sts_api, &name).await;

    // Record cluster replica metrics for fleet observability
    ctx.record_cluster_replicas(ns, &name, cluster.spec.replicas, ready_replicas);

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

    // Collect pod generation tracking info (Kubernetes 1.35+ feature)
    // This tracks whether kubelet has processed pod spec changes
    let pod_infos = get_pod_info(ctx, ns, &name).await.unwrap_or_default();
    transition_ctx.total_pods = i32::try_from(pod_infos.len()).unwrap_or(0);
    transition_ctx.synced_pods =
        i32::try_from(pod_infos.iter().filter(|p| p.spec_applied).count()).unwrap_or(0);

    // Check for in-place resize status (Kubernetes 1.35+ feature)
    let resize_statuses = get_pod_resize_status(ctx, ns, &name)
        .await
        .unwrap_or_default();
    transition_ctx.resize_in_progress = any_resize_in_progress(&resize_statuses);

    if !all_pods_synced(&pod_infos) {
        debug!(
            cluster = %name,
            synced = transition_ctx.synced_pods,
            total = transition_ctx.total_pods,
            "Waiting for pod specs to be applied by kubelet"
        );
    }

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
                            cluster.spec.version.as_str(),
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
                            cluster.spec.version.as_str(),
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

    // Update pod tracking status (Kubernetes 1.35+ features)
    // This updates pods, resize_status, and all_pods_synced fields
    if let Err(e) = status_manager
        .update_pod_tracking(pod_infos, resize_statuses)
        .await
    {
        // Log but don't fail the reconciliation - this is supplementary status data
        debug!(
            cluster = %name,
            error = %e,
            "Failed to update pod tracking status (non-fatal)"
        );
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
        "postgres-operator.smoketurner.com/cluster={},spilo-role=master",
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
        "postgres-operator.smoketurner.com/cluster={},spilo-role=replica",
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

/// Apply a cert-manager Certificate resource using server-side apply
///
/// This uses the dynamic API since cert-manager Certificate is a CRD,
/// not a built-in Kubernetes resource.
async fn apply_certificate(ctx: &Context, ns: &str, cert: &certificate::Certificate) -> Result<()> {
    use kube::api::{ApiResource, DynamicObject, GroupVersionKind};

    let name = cert
        .metadata
        .name
        .as_ref()
        .ok_or(Error::MissingObjectKey("Certificate.metadata.name"))?;

    // Define the cert-manager Certificate resource type
    let gvk = GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate");
    let ar = ApiResource::from_gvk(&gvk);

    // Convert our Certificate struct to a DynamicObject
    let cert_json = serde_json::to_value(cert)?;
    let mut dynamic_obj = DynamicObject::new(name, &ar);
    dynamic_obj.metadata = cert.metadata.clone();
    dynamic_obj.data = cert_json;

    let api: Api<DynamicObject> = Api::namespaced_with(ctx.client.clone(), ns, &ar);
    let params = PatchParams::apply("postgres-operator").force();

    api.patch(name, &params, &Patch::Apply(&dynamic_obj))
        .await?;
    debug!("Applied cert-manager Certificate: {}", name);

    Ok(())
}

/// Apply a KEDA resource (ScaledObject, HTTPScaledObject, TriggerAuthentication)
///
/// Uses the dynamic API since KEDA CRDs are not built-in Kubernetes resources.
/// Gracefully handles the case where KEDA is not installed.
async fn apply_keda_resource(
    ctx: &Context,
    ns: &str,
    obj: &kube::api::DynamicObject,
) -> Result<()> {
    use kube::api::ApiResource;

    let name = obj.name_any();
    let ar = ApiResource {
        group: obj
            .types
            .as_ref()
            .map(|t| t.api_version.split('/').next().unwrap_or(""))
            .unwrap_or("")
            .to_string(),
        version: obj
            .types
            .as_ref()
            .map(|t| {
                let parts: Vec<&str> = t.api_version.split('/').collect();
                parts.get(1).unwrap_or(&parts[0]).to_string()
            })
            .unwrap_or_default(),
        kind: obj
            .types
            .as_ref()
            .map(|t| t.kind.clone())
            .unwrap_or_default(),
        api_version: obj
            .types
            .as_ref()
            .map(|t| t.api_version.clone())
            .unwrap_or_default(),
        plural: format!(
            "{}s",
            obj.types
                .as_ref()
                .map(|t| t.kind.to_lowercase())
                .unwrap_or_default()
        ),
    };

    let api: Api<kube::api::DynamicObject> = Api::namespaced_with(ctx.client.clone(), ns, &ar);
    let params = PatchParams::apply("postgres-operator").force();

    match api.patch(&name, &params, &Patch::Apply(obj)).await {
        Ok(_) => {
            debug!("Applied KEDA resource {}: {}", ar.kind, name);
            Ok(())
        }
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            // CRD not found - KEDA is not installed
            warn!(
                "KEDA CRD {} not found - KEDA may not be installed. Skipping {}",
                ar.kind, name
            );
            Ok(())
        }
        Err(e) => Err(Error::KubeError(e)),
    }
}

/// Delete a KEDA resource if it exists
///
/// Used to clean up KEDA resources when scaling is disabled.
async fn delete_keda_resource(
    ctx: &Context,
    ns: &str,
    group: &str,
    version: &str,
    kind: &str,
    name: &str,
) -> Result<()> {
    use kube::api::ApiResource;

    let ar = ApiResource {
        group: group.to_string(),
        version: version.to_string(),
        kind: kind.to_string(),
        api_version: format!("{}/{}", group, version),
        plural: format!("{}s", kind.to_lowercase()),
    };

    let api: Api<kube::api::DynamicObject> = Api::namespaced_with(ctx.client.clone(), ns, &ar);

    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => {
            debug!("Deleted KEDA resource {}: {}", kind, name);
            Ok(())
        }
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            // Resource doesn't exist, nothing to delete
            Ok(())
        }
        Err(e) => Err(Error::KubeError(e)),
    }
}

/// Check if metrics-server is available in the cluster
///
/// Returns true if the metrics.k8s.io API group is available,
/// indicating metrics-server is installed and can provide pod metrics.
async fn is_metrics_server_available(ctx: &Context) -> bool {
    match ctx.client.list_api_groups().await {
        Ok(groups) => groups.groups.iter().any(|g| g.name == "metrics.k8s.io"),
        Err(e) => {
            debug!("Failed to list API groups: {}", e);
            false
        }
    }
}

/// Check if connection-based scaling is configured
fn has_connection_scaling(cluster: &PostgresCluster) -> bool {
    cluster
        .spec
        .scaling
        .as_ref()
        .and_then(|s| s.metrics.as_ref())
        .is_some_and(|m| m.connections.is_some())
}

/// Check if CPU-based scaling is configured (or will use default CPU trigger)
fn has_cpu_scaling(cluster: &PostgresCluster) -> bool {
    cluster.spec.scaling.as_ref().is_some_and(|scaling| {
        // CPU scaling is used if:
        // 1. Explicitly configured via metrics.cpu, OR
        // 2. No metrics configured at all (defaults to CPU at 70%)
        scaling
            .metrics
            .as_ref()
            .is_none_or(|m| m.cpu.is_some() || m.connections.is_none())
    })
}

/// Apply or clean up KEDA scaling resources based on cluster configuration
async fn reconcile_keda_resources(
    cluster: &PostgresCluster,
    ctx: &Context,
    ns: &str,
) -> Result<()> {
    let name = cluster.name_any();

    info!(cluster = %name, "Reconciling KEDA resources");

    // Reader auto-scaling (ScaledObject)
    if let Some(scaled_obj) = scaled_object::generate_scaled_object(cluster) {
        // Validate prerequisites before creating KEDA resources

        // Check metrics-server availability for CPU-based scaling
        if has_cpu_scaling(cluster) && !is_metrics_server_available(ctx).await {
            warn!(
                cluster = %name,
                "CPU-based scaling configured but metrics-server not available. \
                 KEDA ScaledObject will be created but CPU scaling may not work until \
                 metrics-server is installed."
            );
            // Continue creating ScaledObject - KEDA will handle fallback gracefully
        }

        // Validate connection-string secret for connection-based scaling
        if has_connection_scaling(cluster) {
            let secret_name = format!("{}-credentials", name);
            let secrets_api: Api<Secret> = Api::namespaced(ctx.client.clone(), ns);

            match secrets_api.get_opt(&secret_name).await? {
                Some(secret) => {
                    // Verify connection-string key exists
                    let has_connection_string = secret
                        .data
                        .as_ref()
                        .is_some_and(|d| d.contains_key("connection-string"));

                    if !has_connection_string {
                        warn!(
                            cluster = %name,
                            secret = %secret_name,
                            "Credentials secret exists but missing 'connection-string' key. \
                             Connection-based scaling requires this key. \
                             The secret should contain a PostgreSQL connection string."
                        );
                        // Don't fail - log warning and continue without TriggerAuthentication
                        // The ScaledObject will be created but connection scaling won't work
                    }
                }
                None => {
                    warn!(
                        cluster = %name,
                        secret = %secret_name,
                        "Credentials secret not found. Connection-based scaling requires \
                         the credentials secret to exist with a 'connection-string' key."
                    );
                    // Don't fail - the secret should be created by the reconciler
                    // Continue and let TriggerAuthentication reference it
                }
            }
        }

        info!(cluster = %name, "Applying KEDA ScaledObject for reader auto-scaling");
        apply_keda_resource(ctx, ns, &scaled_obj).await?;

        // Also create TriggerAuthentication if needed for connection-based scaling
        if let Some(trigger_auth) = scaled_object::generate_trigger_auth(cluster) {
            apply_keda_resource(ctx, ns, &trigger_auth).await?;
        }
    } else {
        // Clean up ScaledObject if scaling is disabled
        delete_keda_resource(
            ctx,
            ns,
            scaled_object::KEDA_API_GROUP,
            scaled_object::KEDA_API_VERSION,
            scaled_object::SCALED_OBJECT_KIND,
            &format!("{}-readers", name),
        )
        .await?;

        delete_keda_resource(
            ctx,
            ns,
            scaled_object::KEDA_API_GROUP,
            scaled_object::KEDA_API_VERSION,
            "TriggerAuthentication",
            &format!("{}-pg-auth", name),
        )
        .await?;
    }

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
    //
    // Patroni creates these endpoints for its Kubernetes DCS backend:
    // - <scope>        : Primary endpoint for leader election
    // - <scope>-config : Stores cluster configuration
    // - <scope>-leader : Stores the leader key
    // - <scope>-sync   : For synchronous replication coordination
    let endpoints_api: Api<Endpoints> = Api::namespaced(ctx.client.clone(), ns);
    for suffix in ["", "-config", "-leader", "-sync"] {
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
    let label_selector = format!("postgres-operator.smoketurner.com/cluster={}", cluster_name);
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

/// Emit a Kubernetes Event for a backup-related event
async fn emit_backup_event(ctx: &Context, cluster: &PostgresCluster, event: &BackupEvent) {
    match event {
        BackupEvent::BackupCompleted { name, size_bytes } => {
            let size_str = size_bytes
                .map(|s| format!(" ({} bytes)", s))
                .unwrap_or_default();
            ctx.publish_normal_event(
                cluster,
                "BackupCompleted",
                "Backup",
                Some(format!("Backup {} completed{}", name, size_str)),
            )
            .await;
        }
        BackupEvent::BackupFailed { error } => {
            ctx.publish_warning_event(
                cluster,
                "BackupFailed",
                "Backup",
                Some(format!("Backup failed: {}", error)),
            )
            .await;
        }
        BackupEvent::WALArchivingHealthy => {
            ctx.publish_normal_event(
                cluster,
                "WALArchivingHealthy",
                "WALArchive",
                Some("WAL archiving recovered and is healthy".to_string()),
            )
            .await;
        }
        BackupEvent::WALArchivingFailed => {
            ctx.publish_warning_event(
                cluster,
                "WALArchivingFailed",
                "WALArchive",
                Some("WAL archiving is not healthy".to_string()),
            )
            .await;
        }
    }
}

/// Check for and handle manual backup trigger annotation
///
/// Returns true if a backup was triggered or status was updated, false otherwise.
/// The backup is executed asynchronously in the pod.
///
/// This function also checks if a previously triggered "running" backup has completed
/// by comparing the current backup list with the trigger timestamp.
async fn check_and_trigger_manual_backup(
    cluster: &PostgresCluster,
    ctx: &Context,
    ns: &str,
) -> Result<bool> {
    let name = cluster.name_any();

    // First, check if there's a "running" backup that might have completed
    let current_status = cluster
        .status
        .as_ref()
        .and_then(|s| s.backup.as_ref())
        .and_then(|b| b.last_manual_backup_status.as_deref());

    let last_trigger = cluster
        .status
        .as_ref()
        .and_then(|s| s.backup.as_ref())
        .and_then(|b| b.last_manual_backup_trigger.clone());

    // Check if we need to poll for completion of a running backup
    if current_status == Some("running")
        && let Some(ref trigger_value) = last_trigger
        && let Some(backup_status) = cluster.status.as_ref().and_then(|s| s.backup.as_ref())
        && let Some(ref backup_time) = backup_status.last_backup_time
        && backup_time > trigger_value
    {
        info!(
            cluster = %name,
            trigger = %trigger_value,
            backup_time = %backup_time,
            "Manual backup completed"
        );
        update_manual_backup_status(cluster, ctx, ns, trigger_value, "completed").await?;
        ctx.publish_normal_event(
            cluster,
            "ManualBackupCompleted",
            "Backup",
            Some(format!(
                "Manual backup completed for trigger: {}",
                trigger_value
            )),
        )
        .await;
        return Ok(true);
    }

    // Check if backup trigger annotation is present
    let trigger_value = cluster
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(BACKUP_TRIGGER_ANNOTATION))
        .cloned();

    let Some(trigger_value) = trigger_value else {
        return Ok(false);
    };

    if last_trigger.as_ref() == Some(&trigger_value) {
        // Already processed this trigger (may still be running)
        debug!(
            cluster = %name,
            trigger = %trigger_value,
            status = ?current_status,
            "Manual backup trigger already processed"
        );
        return Ok(false);
    }

    // Check if backups are enabled
    if !backup::is_backup_enabled(cluster) {
        warn!(
            cluster = %name,
            "Manual backup triggered but backups are not configured"
        );
        ctx.publish_warning_event(
            cluster,
            "ManualBackupFailed",
            "Backup",
            Some("Manual backup failed: backups are not configured".to_string()),
        )
        .await;
        return Ok(false);
    }

    info!(
        cluster = %name,
        trigger = %trigger_value,
        "Processing manual backup trigger"
    );

    // Find the primary pod to run the backup on
    let pods_api: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);
    let primary_pod = get_patroni_leader(&pods_api, &name).await;

    let Some(primary_pod) = primary_pod else {
        warn!(
            cluster = %name,
            "Cannot trigger manual backup: no primary pod found"
        );
        ctx.publish_warning_event(
            cluster,
            "ManualBackupFailed",
            "Backup",
            Some("Manual backup failed: no primary pod available".to_string()),
        )
        .await;
        return Ok(false);
    };

    // Execute the backup command
    match exec_manual_backup(ctx, ns, &primary_pod).await {
        Ok(_) => {
            info!(
                cluster = %name,
                pod = %primary_pod,
                "Manual backup started successfully"
            );
            ctx.publish_normal_event(
                cluster,
                "ManualBackupStarted",
                "Backup",
                Some(format!(
                    "Manual backup triggered: {} (on pod {})",
                    trigger_value, primary_pod
                )),
            )
            .await;

            // Update status with the trigger value
            update_manual_backup_status(cluster, ctx, ns, &trigger_value, "running").await?;
        }
        Err(e) => {
            error!(
                cluster = %name,
                pod = %primary_pod,
                error = %e,
                "Manual backup failed to start"
            );
            ctx.publish_warning_event(
                cluster,
                "ManualBackupFailed",
                "Backup",
                Some(format!("Manual backup failed: {}", e)),
            )
            .await;

            // Update status with error
            update_manual_backup_status(cluster, ctx, ns, &trigger_value, "failed").await?;
        }
    }

    Ok(true)
}

/// Execute a manual backup on a pod using wal-g
async fn exec_manual_backup(ctx: &Context, ns: &str, pod_name: &str) -> Result<()> {
    use std::time::Duration;
    use tokio::time::timeout;

    const EXEC_TIMEOUT: Duration = Duration::from_secs(30);

    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), ns);

    let attach_params = AttachParams {
        container: Some("postgres".to_string()),
        stdin: false,
        stdout: true,
        stderr: true,
        tty: false,
        ..Default::default()
    };

    // Use Spilo's backup script which handles WAL-G environment properly
    // This runs the backup in the background within the container
    let command = vec![
        "su".to_string(),
        "postgres".to_string(),
        "-c".to_string(),
        // Run backup in background with nohup to prevent blocking
        "nohup /scripts/postgres_backup.sh > /tmp/manual_backup.log 2>&1 &".to_string(),
    ];

    let mut attached = timeout(EXEC_TIMEOUT, pods.exec(pod_name, command, &attach_params))
        .await
        .map_err(|_| {
            Error::BackupExecFailed(format!(
                "Backup exec timed out after {} seconds",
                EXEC_TIMEOUT.as_secs()
            ))
        })??;

    // Read any output (should be minimal since we're running in background)
    // Important: Don't ignore read errors - they could indicate command failure
    if let Some(mut stdout) = attached.stdout() {
        let mut output = Vec::new();
        stdout
            .read_to_end(&mut output)
            .await
            .map_err(|e| Error::BackupExecFailed(format!("Failed to read backup output: {}", e)))?;
        if !output.is_empty() {
            debug!(
                "Manual backup command output: {}",
                String::from_utf8_lossy(&output)
            );
        }
    }

    // Wait for the command to complete (just the nohup wrapper)
    if let Some(status_channel) = attached.take_status()
        && let Some(result) = status_channel.await
        && let Some(status) = result.status
        && status != "Success"
    {
        return Err(Error::BackupExecFailed(format!(
            "Backup command failed with status: {}",
            status
        )));
    }

    Ok(())
}

/// Update the manual backup status in the cluster status
async fn update_manual_backup_status(
    cluster: &PostgresCluster,
    ctx: &Context,
    ns: &str,
    trigger_value: &str,
    status: &str,
) -> Result<()> {
    let api: Api<PostgresCluster> = Api::namespaced(ctx.client.clone(), ns);
    let name = cluster.name_any();

    // Patch the backup status with the trigger value
    let patch = serde_json::json!({
        "status": {
            "backup": {
                "lastManualBackupTrigger": trigger_value,
                "lastManualBackupStatus": status
            }
        }
    });

    api.patch_status(
        &name,
        &PatchParams::apply("postgres-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    debug!(
        cluster = %name,
        trigger = %trigger_value,
        status = %status,
        "Updated manual backup status"
    );

    Ok(())
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
    let label_selector = format!("postgres-operator.smoketurner.com/cluster={}", cluster_name);
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
    let label_selector = format!("postgres-operator.smoketurner.com/cluster={}", cluster_name);
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
