//! Common utilities for Kubernetes resource generation
//!
//! This module provides shared functions and constants used across
//! all resource generators to ensure consistency and reduce duplication.

use std::collections::BTreeMap;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;

use crate::crd::PostgresCluster;

/// API version for PostgresCluster CRD
pub const API_VERSION: &str = "postgres-operator.smoketurner.com/v1alpha1";

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
///
/// This includes base operator labels. For full labels including user-defined
/// cost allocation labels, use `cluster_labels` instead.
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
            "postgres-operator.smoketurner.com/cluster".to_string(),
            cluster_name.to_string(),
        ),
    ])
}

/// Generate labels for a PostgresCluster including user-defined cost allocation labels.
///
/// This merges standard operator labels with user-defined labels from the cluster spec.
/// User labels can override standard labels except for the cluster identifier.
///
/// Common user labels for cost allocation:
/// - `team`: Team that owns this cluster
/// - `cost-center`: Cost center for billing
/// - `project`: Project identifier
/// - `environment`: Environment type (production, staging, etc.)
pub fn cluster_labels(cluster: &PostgresCluster) -> BTreeMap<String, String> {
    let name = cluster.name_any();
    let mut labels = standard_labels(&name);

    // Merge user-defined labels from the cluster spec
    // User labels can override standard labels (except cluster identifier)
    for (key, value) in &cluster.spec.labels {
        // Don't allow overriding the cluster identifier
        if key != "postgres-operator.smoketurner.com/cluster" {
            labels.insert(key.clone(), value.clone());
        }
    }

    labels
}

/// Generate labels for Patroni-managed resources
///
/// This includes the `application: spilo` label which is required by
/// Patroni's Kubernetes DCS to discover cluster pods. The KUBERNETES_LABELS
/// env var tells Patroni to filter pods by this label.
pub fn patroni_labels(cluster_name: &str) -> BTreeMap<String, String> {
    let mut labels = standard_labels(cluster_name);
    labels.insert(
        "postgres-operator.smoketurner.com/ha-mode".to_string(),
        "patroni".to_string(),
    );
    // Required for Patroni's Kubernetes DCS pod discovery
    // Must match the KUBERNETES_LABELS env var
    labels.insert("application".to_string(), "spilo".to_string());
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
            labels.get("postgres-operator.smoketurner.com/cluster"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_patroni_labels() {
        let labels = patroni_labels("my-cluster");
        assert_eq!(
            labels.get("postgres-operator.smoketurner.com/ha-mode"),
            Some(&"patroni".to_string())
        );
        // Required for Patroni's Kubernetes DCS
        assert_eq!(labels.get("application"), Some(&"spilo".to_string()));
        // Should also have standard labels
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_cluster_labels_with_user_labels() {
        use crate::crd::{
            PostgresCluster, PostgresClusterSpec, PostgresVersion, StorageSpec, TLSSpec,
        };
        use kube::core::ObjectMeta;

        let mut user_labels = BTreeMap::new();
        user_labels.insert("team".to_string(), "platform".to_string());
        user_labels.insert("cost-center".to_string(), "eng-001".to_string());
        user_labels.insert("environment".to_string(), "production".to_string());

        let cluster = PostgresCluster {
            metadata: ObjectMeta {
                name: Some("my-cluster".to_string()),
                namespace: Some("test-ns".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 1,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                labels: user_labels,
                resources: None,
                postgresql_params: Default::default(),
                backup: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
            },
            status: None,
        };

        let labels = cluster_labels(&cluster);

        // Should have standard labels
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"postgres-operator".to_string())
        );

        // Should have user-defined cost allocation labels
        assert_eq!(labels.get("team"), Some(&"platform".to_string()));
        assert_eq!(labels.get("cost-center"), Some(&"eng-001".to_string()));
        assert_eq!(labels.get("environment"), Some(&"production".to_string()));

        // Cluster identifier should not be overridable
        assert_eq!(
            labels.get("postgres-operator.smoketurner.com/cluster"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_cluster_labels_cannot_override_cluster_identifier() {
        use crate::crd::{
            PostgresCluster, PostgresClusterSpec, PostgresVersion, StorageSpec, TLSSpec,
        };
        use kube::core::ObjectMeta;

        let mut user_labels = BTreeMap::new();
        // Try to override the cluster identifier
        user_labels.insert(
            "postgres-operator.smoketurner.com/cluster".to_string(),
            "hacked".to_string(),
        );

        let cluster = PostgresCluster {
            metadata: ObjectMeta {
                name: Some("real-cluster".to_string()),
                namespace: Some("test-ns".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 1,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                labels: user_labels,
                resources: None,
                postgresql_params: Default::default(),
                backup: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
            },
            status: None,
        };

        let labels = cluster_labels(&cluster);

        // Should not be able to override the cluster identifier
        assert_eq!(
            labels.get("postgres-operator.smoketurner.com/cluster"),
            Some(&"real-cluster".to_string())
        );
    }
}
