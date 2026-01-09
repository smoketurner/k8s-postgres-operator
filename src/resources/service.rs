//! Service generation for Patroni-managed PostgreSQL clusters
//!
//! All PostgreSQL clusters use Patroni, so services use Patroni's label scheme
//! for routing traffic to the correct pods.

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec as K8sServiceSpec};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use std::collections::BTreeMap;

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
        ("postgres.example.com/cluster".to_string(), cluster_name),
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
        ("postgres.example.com/cluster".to_string(), cluster_name),
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
        ("postgres.example.com/cluster".to_string(), cluster_name),
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
