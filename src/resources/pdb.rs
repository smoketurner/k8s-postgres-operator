//! PodDisruptionBudget resource generation for PostgreSQL clusters
//!
//! Creates PDBs to protect cluster availability during node maintenance,
//! upgrades, and other disruptions.
//!
//! All clusters use Patroni, so PDBs are designed around Patroni's
//! dynamic role assignment using spilo-role labels.

use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::core::ObjectMeta;
use kube::ResourceExt;
use std::collections::BTreeMap;

use crate::crd::PostgresCluster;

/// Generate owner reference for the cluster
fn owner_reference(cluster: &PostgresCluster) -> OwnerReference {
    OwnerReference {
        api_version: "postgres.example.com/v1alpha1".to_string(),
        kind: "PostgresCluster".to_string(),
        name: cluster.name_any(),
        uid: cluster.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Generate a PodDisruptionBudget for the cluster
///
/// For Patroni-managed clusters, we create a single PDB that ensures
/// at least one pod can serve traffic during disruptions.
///
/// The PDB strategy depends on replica count:
/// - 1 replica: min_available = 0 (allow disruption, will cause downtime)
/// - 2 replicas: min_available = 1 (always keep one pod available)
/// - 3+ replicas: min_available = replicas - 1 (allow one disruption at a time)
pub fn generate_pdb(cluster: &PostgresCluster) -> PodDisruptionBudget {
    let name = format!("{}-pdb", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();
    let replicas = cluster.spec.replicas;

    let labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        ("app.kubernetes.io/component".to_string(), "postgresql".to_string()),
        ("app.kubernetes.io/managed-by".to_string(), "postgres-operator".to_string()),
        ("postgres.example.com/cluster".to_string(), cluster_name.clone()),
    ]);

    // Match all pods in the Patroni cluster (both master and replicas)
    let match_labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        ("postgres.example.com/cluster".to_string(), cluster_name),
    ]);

    // Calculate min_available based on replica count
    let min_available = match replicas {
        1 => 0,                    // Single instance - allow disruption (causes downtime)
        2 => 1,                    // Two instances - keep at least one running
        n => n - 1,                // 3+ instances - allow one disruption at a time
    };

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            min_available: Some(IntOrString::Int(min_available)),
            selector: Some(LabelSelector {
                match_labels: Some(match_labels),
                ..Default::default()
            }),
            // Use IfHealthyBudget to allow eviction if pod is unhealthy
            unhealthy_pod_eviction_policy: Some("IfHealthyBudget".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}
