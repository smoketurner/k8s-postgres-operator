//! Common utilities for Kubernetes resource generation
//!
//! This module provides shared functions and constants used across
//! all resource generators to ensure consistency and reduce duplication.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;

use crate::crd::PostgresCluster;

/// API version for PostgresCluster CRD
pub const API_VERSION: &str = "postgres.example.com/v1alpha1";

/// Kind for PostgresCluster CRD
pub const KIND: &str = "PostgresCluster";

/// Operator field manager name for server-side apply
pub const FIELD_MANAGER: &str = "postgres-operator";

/// Generate an owner reference for a PostgresCluster
///
/// This ensures that all child resources are properly owned by the cluster
/// and will be garbage collected when the cluster is deleted.
pub fn owner_reference(cluster: &PostgresCluster) -> OwnerReference {
    OwnerReference {
        api_version: API_VERSION.to_string(),
        kind: KIND.to_string(),
        name: cluster.name_any(),
        uid: cluster.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Generate standard labels for all resources belonging to a PostgresCluster
pub fn standard_labels(cluster_name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/name".to_string(),
            cluster_name.to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            "postgresql".to_string(),
        ),
        (
            "app.kubernetes.io/managed-by".to_string(),
            FIELD_MANAGER.to_string(),
        ),
        (
            "postgres.example.com/cluster".to_string(),
            cluster_name.to_string(),
        ),
    ])
}

/// Generate labels for Patroni-managed resources
pub fn patroni_labels(cluster_name: &str) -> BTreeMap<String, String> {
    let mut labels = standard_labels(cluster_name);
    labels.insert(
        "postgres.example.com/ha-mode".to_string(),
        "patroni".to_string(),
    );
    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_labels() {
        let labels = standard_labels("my-cluster");
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"postgresql".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"postgres-operator".to_string())
        );
        assert_eq!(
            labels.get("postgres.example.com/cluster"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_patroni_labels() {
        let labels = patroni_labels("my-cluster");
        assert_eq!(
            labels.get("postgres.example.com/ha-mode"),
            Some(&"patroni".to_string())
        );
        // Should also have standard labels
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
    }
}
