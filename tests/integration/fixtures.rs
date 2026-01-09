//! Test fixtures and builders for PostgresCluster resources
//!
//! All PostgreSQL clusters use Patroni for consistent management.
//! The replica count determines the cluster topology:
//! - 1 replica: single server
//! - 2 replicas: primary + 1 read replica
//! - 3+ replicas: highly available cluster

use kube::core::ObjectMeta;
use postgres_operator::crd::{
    PostgresCluster, PostgresClusterSpec, ResourceList, ResourceRequirements, StorageSpec,
};
use std::collections::BTreeMap;

/// Builder for PostgresCluster test fixtures
pub struct PostgresClusterBuilder {
    name: String,
    namespace: String,
    version: String,
    replicas: i32,
    storage_size: String,
    storage_class: Option<String>,
    postgresql_params: BTreeMap<String, String>,
    resources: Option<ResourceRequirements>,
}

impl PostgresClusterBuilder {
    /// Create a new builder with default values
    pub fn new(name: &str, namespace: &str) -> Self {
        Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            version: "16".to_string(),
            replicas: 1,
            storage_size: "1Gi".to_string(),
            storage_class: None,
            postgresql_params: BTreeMap::new(),
            resources: None,
        }
    }

    /// Create a builder configured for a single-replica cluster
    pub fn single(name: &str, namespace: &str) -> Self {
        Self::new(name, namespace).with_replicas(1)
    }

    /// Create a builder configured for a highly available cluster (3 replicas)
    pub fn ha(name: &str, namespace: &str) -> Self {
        Self::new(name, namespace).with_replicas(3)
    }

    /// Set the PostgreSQL version
    pub fn with_version(mut self, version: &str) -> Self {
        self.version = version.to_string();
        self
    }

    /// Set the number of replicas
    pub fn with_replicas(mut self, replicas: i32) -> Self {
        self.replicas = replicas;
        self
    }

    /// Set storage configuration
    pub fn with_storage(mut self, size: &str, class: Option<&str>) -> Self {
        self.storage_size = size.to_string();
        self.storage_class = class.map(String::from);
        self
    }

    /// Add a PostgreSQL parameter
    pub fn with_param(mut self, key: &str, value: &str) -> Self {
        self.postgresql_params
            .insert(key.to_string(), value.to_string());
        self
    }

    /// Set resource requirements
    pub fn with_resources(mut self, cpu: &str, memory: &str) -> Self {
        self.resources = Some(ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some(cpu.to_string()),
                memory: Some(memory.to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some(cpu.to_string()),
                memory: Some(memory.to_string()),
            }),
        });
        self
    }

    /// Build the PostgresCluster resource
    pub fn build(self) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(self.namespace),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: self.version,
                replicas: self.replicas,
                storage: StorageSpec {
                    size: self.storage_size,
                    storage_class: self.storage_class,
                },
                postgresql_params: self.postgresql_params,
                resources: self.resources,
                backup: None,
                pgbouncer: None,
                tls: None,
                metrics: None,
                service: None,
            },
            status: None,
        }
    }
}

/// Create a minimal single-replica PostgresCluster
pub fn minimal_single(name: &str, namespace: &str) -> PostgresCluster {
    PostgresClusterBuilder::single(name, namespace)
        .with_version("16")
        .with_storage("1Gi", None)
        .build()
}

/// Create a minimal HA PostgresCluster (3 replicas)
pub fn minimal_ha(name: &str, namespace: &str) -> PostgresCluster {
    PostgresClusterBuilder::ha(name, namespace)
        .with_version("16")
        .with_storage("1Gi", None)
        .build()
}

/// Create a PostgresCluster with an invalid version (for error testing)
pub fn invalid_version(name: &str, namespace: &str) -> PostgresCluster {
    PostgresClusterBuilder::single(name, namespace)
        .with_version("invalid-99.99")
        .build()
}

/// Create a PostgresCluster with a non-existent storage class (for error testing)
pub fn invalid_storage_class(name: &str, namespace: &str) -> PostgresCluster {
    PostgresClusterBuilder::single(name, namespace)
        .with_storage("10Gi", Some("nonexistent-storage-class"))
        .build()
}
