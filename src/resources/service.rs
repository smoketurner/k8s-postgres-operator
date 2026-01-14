//! Service generation for Patroni-managed PostgreSQL clusters
//!
//! All PostgreSQL clusters use Patroni, so services use Patroni's label scheme
//! for routing traffic to the correct pods.
//!
//! ## Service Switching for Upgrades
//!
//! During blue-green upgrades, services can be switched from source to target
//! cluster using the [`switch_services_to_target`] function. This atomically
//! updates the service selectors to point to the new cluster.

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec as K8sServiceSpec};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Client, ResourceExt};
use serde_json::json;
use std::collections::BTreeMap;
use thiserror::Error;

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, standard_labels};

/// Generate the primary (read-write) service
///
/// This service routes traffic to the current Patroni leader.
/// Patroni updates the pod labels to indicate the current role (master/replica).
/// Note: We use "{cluster}-primary" to avoid conflicting with Patroni's DCS endpoint
/// which uses the scope name (cluster name) for leader election.
pub fn generate_primary_service(cluster: &PostgresCluster) -> Service {
    let name = format!("{}-primary", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    // Patroni sets this label on the leader pod
    let selector = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        (
            "postgres-operator.smoketurner.com/cluster".to_string(),
            cluster_name,
        ),
        ("spilo-role".to_string(), "master".to_string()), // Patroni/Spilo label
    ]);

    // Get service configuration from spec
    let service_config = cluster.spec.service.as_ref();
    let service_type = service_config.map(|s| s.type_.clone()).unwrap_or_default();

    // Merge annotations from service config with labels
    let mut annotations = BTreeMap::new();
    if let Some(config) = service_config {
        annotations.extend(config.annotations.clone());
    }

    // Build service spec
    let mut spec = K8sServiceSpec {
        selector: Some(selector),
        ports: Some(vec![ServicePort {
            port: 5432,
            target_port: Some(IntOrString::Int(5432)),
            name: Some("postgresql".to_string()),
            protocol: Some("TCP".to_string()),
            node_port: service_config.and_then(|s| s.node_port),
            ..Default::default()
        }]),
        type_: Some(service_type.to_string()),
        ..Default::default()
    };

    // Apply LoadBalancer-specific settings
    if let Some(config) = service_config {
        if !config.load_balancer_source_ranges.is_empty() {
            spec.load_balancer_source_ranges = Some(config.load_balancer_source_ranges.clone());
        }
        if let Some(ref policy) = config.external_traffic_policy {
            spec.external_traffic_policy = Some(policy.to_string());
        }
    }

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(spec),
        ..Default::default()
    }
}

/// Generate the replicas (read-only) service
///
/// This service routes read traffic to replica pods.
pub fn generate_replicas_service(cluster: &PostgresCluster) -> Service {
    let name = format!("{}-repl", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    // Patroni sets this label on replica pods
    let selector = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        (
            "postgres-operator.smoketurner.com/cluster".to_string(),
            cluster_name,
        ),
        ("spilo-role".to_string(), "replica".to_string()), // Patroni/Spilo label
    ]);

    // Get service configuration from spec
    let service_config = cluster.spec.service.as_ref();
    let service_type = service_config.map(|s| s.type_.clone()).unwrap_or_default();

    // Merge annotations from service config
    let mut annotations = BTreeMap::new();
    if let Some(config) = service_config {
        annotations.extend(config.annotations.clone());
    }

    // Build service spec
    let mut spec = K8sServiceSpec {
        selector: Some(selector),
        ports: Some(vec![ServicePort {
            port: 5432,
            target_port: Some(IntOrString::Int(5432)),
            name: Some("postgresql".to_string()),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
        type_: Some(service_type.to_string()),
        ..Default::default()
    };

    // Apply LoadBalancer-specific settings
    if let Some(config) = service_config {
        if !config.load_balancer_source_ranges.is_empty() {
            spec.load_balancer_source_ranges = Some(config.load_balancer_source_ranges.clone());
        }
        if let Some(ref policy) = config.external_traffic_policy {
            spec.external_traffic_policy = Some(policy.to_string());
        }
    }

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(spec),
        ..Default::default()
    }
}

/// Generate a headless service for pod discovery
///
/// This is used by Patroni for internal DNS-based discovery.
/// Note: Headless services always use ClusterIP type with clusterIP=None,
/// regardless of the service configuration, as they are for internal use only.
pub fn generate_headless_service(cluster: &PostgresCluster) -> Service {
    let name = format!("{}-headless", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    let selector = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        (
            "postgres-operator.smoketurner.com/cluster".to_string(),
            cluster_name,
        ),
    ]);

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(K8sServiceSpec {
            selector: Some(selector),
            ports: Some(vec![
                ServicePort {
                    port: 5432,
                    target_port: Some(IntOrString::Int(5432)),
                    name: Some("postgresql".to_string()),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                },
                ServicePort {
                    port: 8008,
                    target_port: Some(IntOrString::Int(8008)),
                    name: Some("patroni".to_string()),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                },
            ]),
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()), // Headless service
            publish_not_ready_addresses: Some(true), // Include unready pods
            ..Default::default()
        }),
        ..Default::default()
    }
}

// ============================================================================
// Service Switching for Blue-Green Upgrades
// ============================================================================

/// Errors that can occur during service switching
#[derive(Error, Debug)]
pub enum ServiceSwitchError {
    /// Failed to patch service
    #[error("Failed to patch service {name}: {source}")]
    PatchFailed {
        name: String,
        #[source]
        source: kube::Error,
    },

    /// Service not found
    #[error("Service not found: {0}")]
    NotFound(String),

    /// Invalid service configuration
    #[error("Invalid service configuration: {0}")]
    InvalidConfig(String),
}

/// Result of a service switch operation
#[derive(Debug, Clone)]
pub struct ServiceSwitchResult {
    /// Name of the primary service that was switched
    pub primary_service: String,
    /// Name of the replica service that was switched
    pub replica_service: String,
    /// Timestamp when the switch was completed
    pub switched_at: jiff::Timestamp,
    /// Previous cluster name (source)
    pub previous_cluster: String,
    /// New cluster name (target)
    pub new_cluster: String,
}

/// Switch services from source cluster to target cluster
///
/// This function atomically updates the service selectors to point to the
/// target cluster instead of the source cluster. Both primary and replica
/// services are updated in a single reconciliation.
///
/// # Arguments
///
/// * `client` - Kubernetes client
/// * `namespace` - Namespace of the services
/// * `source_name` - Name of the source cluster
/// * `target_name` - Name of the target cluster
/// * `target_cluster` - Reference to the target cluster for owner references
///
/// # Returns
///
/// Returns a `ServiceSwitchResult` on success, or a `ServiceSwitchError` on failure.
pub async fn switch_services_to_target(
    client: &Client,
    namespace: &str,
    source_name: &str,
    target_name: &str,
    target_cluster: &PostgresCluster,
) -> Result<ServiceSwitchResult, ServiceSwitchError> {
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);

    let primary_svc_name = format!("{}-primary", source_name);
    let replica_svc_name = format!("{}-repl", source_name);

    // Build owner reference for the target cluster
    let owner_ref = owner_reference(target_cluster);
    let owner_refs = vec![owner_ref];

    // Patch the primary service
    let primary_patch = json!({
        "metadata": {
            "ownerReferences": owner_refs,
            "annotations": {
                "postgres-operator.smoketurner.com/switched-from": source_name,
                "postgres-operator.smoketurner.com/switched-at": jiff::Timestamp::now().to_string()
            }
        },
        "spec": {
            "selector": {
                "app.kubernetes.io/name": target_name,
                "postgres-operator.smoketurner.com/cluster": target_name,
                "spilo-role": "master"
            }
        }
    });

    services
        .patch(
            &primary_svc_name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&primary_patch),
        )
        .await
        .map_err(|e| ServiceSwitchError::PatchFailed {
            name: primary_svc_name.clone(),
            source: e,
        })?;

    // Patch the replica service
    let replica_patch = json!({
        "metadata": {
            "ownerReferences": owner_refs,
            "annotations": {
                "postgres-operator.smoketurner.com/switched-from": source_name,
                "postgres-operator.smoketurner.com/switched-at": jiff::Timestamp::now().to_string()
            }
        },
        "spec": {
            "selector": {
                "app.kubernetes.io/name": target_name,
                "postgres-operator.smoketurner.com/cluster": target_name,
                "spilo-role": "replica"
            }
        }
    });

    services
        .patch(
            &replica_svc_name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&replica_patch),
        )
        .await
        .map_err(|e| ServiceSwitchError::PatchFailed {
            name: replica_svc_name.clone(),
            source: e,
        })?;

    Ok(ServiceSwitchResult {
        primary_service: primary_svc_name,
        replica_service: replica_svc_name,
        switched_at: jiff::Timestamp::now(),
        previous_cluster: source_name.to_string(),
        new_cluster: target_name.to_string(),
    })
}

/// Revert services back to the source cluster (for rollback)
///
/// This function reverses the service switch, pointing services back
/// to the original source cluster.
///
/// # Arguments
///
/// * `client` - Kubernetes client
/// * `namespace` - Namespace of the services
/// * `source_name` - Name of the source cluster (to switch back to)
/// * `target_name` - Name of the target cluster (current)
/// * `source_cluster` - Reference to the source cluster for owner references
pub async fn revert_services_to_source(
    client: &Client,
    namespace: &str,
    source_name: &str,
    target_name: &str,
    source_cluster: &PostgresCluster,
) -> Result<ServiceSwitchResult, ServiceSwitchError> {
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);

    let primary_svc_name = format!("{}-primary", source_name);
    let replica_svc_name = format!("{}-repl", source_name);

    // Build owner reference for the source cluster
    let owner_ref = owner_reference(source_cluster);
    let owner_refs = vec![owner_ref];

    // Patch the primary service back to source
    let primary_patch = json!({
        "metadata": {
            "ownerReferences": owner_refs,
            "annotations": {
                "postgres-operator.smoketurner.com/rolled-back-from": target_name,
                "postgres-operator.smoketurner.com/rolled-back-at": jiff::Timestamp::now().to_string()
            }
        },
        "spec": {
            "selector": {
                "app.kubernetes.io/name": source_name,
                "postgres-operator.smoketurner.com/cluster": source_name,
                "spilo-role": "master"
            }
        }
    });

    services
        .patch(
            &primary_svc_name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&primary_patch),
        )
        .await
        .map_err(|e| ServiceSwitchError::PatchFailed {
            name: primary_svc_name.clone(),
            source: e,
        })?;

    // Patch the replica service back to source
    let replica_patch = json!({
        "metadata": {
            "ownerReferences": owner_refs,
            "annotations": {
                "postgres-operator.smoketurner.com/rolled-back-from": target_name,
                "postgres-operator.smoketurner.com/rolled-back-at": jiff::Timestamp::now().to_string()
            }
        },
        "spec": {
            "selector": {
                "app.kubernetes.io/name": source_name,
                "postgres-operator.smoketurner.com/cluster": source_name,
                "spilo-role": "replica"
            }
        }
    });

    services
        .patch(
            &replica_svc_name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&replica_patch),
        )
        .await
        .map_err(|e| ServiceSwitchError::PatchFailed {
            name: replica_svc_name.clone(),
            source: e,
        })?;

    Ok(ServiceSwitchResult {
        primary_service: primary_svc_name,
        replica_service: replica_svc_name,
        switched_at: jiff::Timestamp::now(),
        previous_cluster: target_name.to_string(),
        new_cluster: source_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_switch_error_display() {
        let err = ServiceSwitchError::NotFound("my-service".to_string());
        assert!(err.to_string().contains("my-service"));

        let err = ServiceSwitchError::InvalidConfig("bad config".to_string());
        assert!(err.to_string().contains("bad config"));
    }

    #[test]
    fn test_service_names() {
        let source_name = "my-cluster";
        let primary_name = format!("{}-primary", source_name);
        let replica_name = format!("{}-repl", source_name);

        assert_eq!(primary_name, "my-cluster-primary");
        assert_eq!(replica_name, "my-cluster-repl");
    }
}
