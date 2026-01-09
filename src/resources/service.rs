//! Service generation for Patroni-managed PostgreSQL clusters
//!
//! All PostgreSQL clusters use Patroni, so services use Patroni's label scheme
//! for routing traffic to the correct pods.

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
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
pub fn generate_primary_service(cluster: &PostgresCluster) -> Service {
    let name = cluster.name_any(); // Just the cluster name for primary
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    // Patroni sets this label on the leader pod
    let selector = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        ("postgres.example.com/cluster".to_string(), cluster_name),
        ("spilo-role".to_string(), "master".to_string()), // Patroni/Spilo label
    ]);

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                port: 5432,
                target_port: Some(IntOrString::Int(5432)),
                name: Some("postgresql".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            type_: Some("ClusterIP".to_string()),
            ..Default::default()
        }),
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

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(selector),
            ports: Some(vec![ServicePort {
                port: 5432,
                target_port: Some(IntOrString::Int(5432)),
                name: Some("postgresql".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            type_: Some("ClusterIP".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate a headless service for pod discovery
///
/// This is used by Patroni for internal DNS-based discovery.
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
        spec: Some(ServiceSpec {
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
