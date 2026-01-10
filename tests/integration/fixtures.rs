//! Test fixtures and builders for PostgresCluster resources
//!
//! All PostgreSQL clusters use Patroni for consistent management.
//! The replica count determines the cluster topology:
//! - 1 replica: single server
//! - 2 replicas: primary + 1 read replica
//! - 3+ replicas: highly available cluster

use kube::core::ObjectMeta;
use postgres_operator::crd::{
    ExternalTrafficPolicy, MetricsSpec, PgBouncerSpec, PostgresCluster, PostgresClusterSpec,
    ResourceList, ResourceRequirements, ServiceSpec, ServiceType, StorageSpec, TLSSpec,
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
    tls: Option<TLSSpec>,
    pgbouncer: Option<PgBouncerSpec>,
    metrics: Option<MetricsSpec>,
    service: Option<ServiceSpec>,
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
            tls: None,
            pgbouncer: None,
            metrics: None,
            service: None,
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
            restart_on_resize: None,
        });
        self
    }

    /// Set separate resource requests and limits
    pub fn with_resources_full(
        mut self,
        req_cpu: &str,
        req_memory: &str,
        limit_cpu: &str,
        limit_memory: &str,
    ) -> Self {
        self.resources = Some(ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some(req_cpu.to_string()),
                memory: Some(req_memory.to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some(limit_cpu.to_string()),
                memory: Some(limit_memory.to_string()),
            }),
            restart_on_resize: None,
        });
        self
    }

    /// Enable TLS with a certificate secret
    pub fn with_tls(mut self, cert_secret: &str) -> Self {
        self.tls = Some(TLSSpec {
            enabled: true,
            cert_secret: Some(cert_secret.to_string()),
            ca_secret: None,
            certificate_file: None,
            private_key_file: None,
            ca_file: None,
        });
        self
    }

    /// Enable TLS with certificate and CA secrets
    pub fn with_tls_and_ca(mut self, cert_secret: &str, ca_secret: &str) -> Self {
        self.tls = Some(TLSSpec {
            enabled: true,
            cert_secret: Some(cert_secret.to_string()),
            ca_secret: Some(ca_secret.to_string()),
            certificate_file: None,
            private_key_file: None,
            ca_file: None,
        });
        self
    }

    /// Enable TLS with custom filenames
    pub fn with_tls_custom(mut self, cert_secret: &str, cert_file: &str, key_file: &str) -> Self {
        self.tls = Some(TLSSpec {
            enabled: true,
            cert_secret: Some(cert_secret.to_string()),
            ca_secret: None,
            certificate_file: Some(cert_file.to_string()),
            private_key_file: Some(key_file.to_string()),
            ca_file: None,
        });
        self
    }

    /// Enable PgBouncer with default settings (transaction mode)
    pub fn with_pgbouncer(mut self) -> Self {
        self.pgbouncer = Some(PgBouncerSpec {
            enabled: true,
            replicas: 2,
            pool_mode: "transaction".to_string(),
            max_db_connections: 60,
            default_pool_size: 20,
            max_client_conn: 10000,
            image: None,
            resources: None,
            enable_replica_pooler: false,
        });
        self
    }

    /// Enable PgBouncer with a specific pool mode
    pub fn with_pgbouncer_mode(mut self, mode: &str) -> Self {
        self.pgbouncer = Some(PgBouncerSpec {
            enabled: true,
            replicas: 2,
            pool_mode: mode.to_string(),
            max_db_connections: 60,
            default_pool_size: 20,
            max_client_conn: 10000,
            image: None,
            resources: None,
            enable_replica_pooler: false,
        });
        self
    }

    /// Enable PgBouncer with replica pooler
    pub fn with_pgbouncer_replica(mut self) -> Self {
        self.pgbouncer = Some(PgBouncerSpec {
            enabled: true,
            replicas: 2,
            pool_mode: "transaction".to_string(),
            max_db_connections: 60,
            default_pool_size: 20,
            max_client_conn: 10000,
            image: None,
            resources: None,
            enable_replica_pooler: true,
        });
        self
    }

    /// Enable PgBouncer with custom settings
    pub fn with_pgbouncer_custom(
        mut self,
        replicas: i32,
        pool_mode: &str,
        max_db_connections: i32,
        default_pool_size: i32,
        max_client_conn: i32,
    ) -> Self {
        self.pgbouncer = Some(PgBouncerSpec {
            enabled: true,
            replicas,
            pool_mode: pool_mode.to_string(),
            max_db_connections,
            default_pool_size,
            max_client_conn,
            image: None,
            resources: None,
            enable_replica_pooler: false,
        });
        self
    }

    /// Enable metrics with default port (9187)
    pub fn with_metrics(mut self) -> Self {
        self.metrics = Some(MetricsSpec {
            enabled: true,
            port: 9187,
        });
        self
    }

    /// Enable metrics with custom port
    pub fn with_metrics_port(mut self, port: i32) -> Self {
        self.metrics = Some(MetricsSpec {
            enabled: true,
            port,
        });
        self
    }

    /// Set service type to ClusterIP (default)
    pub fn with_service_cluster_ip(mut self) -> Self {
        self.service = Some(ServiceSpec {
            type_: ServiceType::ClusterIP,
            annotations: BTreeMap::new(),
            load_balancer_source_ranges: vec![],
            external_traffic_policy: None,
            node_port: None,
        });
        self
    }

    /// Set service type to NodePort
    pub fn with_service_node_port(mut self, node_port: Option<i32>) -> Self {
        self.service = Some(ServiceSpec {
            type_: ServiceType::NodePort,
            annotations: BTreeMap::new(),
            load_balancer_source_ranges: vec![],
            external_traffic_policy: Some(ExternalTrafficPolicy::Cluster),
            node_port,
        });
        self
    }

    /// Set service type to LoadBalancer
    pub fn with_service_load_balancer(mut self, source_ranges: Vec<String>) -> Self {
        self.service = Some(ServiceSpec {
            type_: ServiceType::LoadBalancer,
            annotations: BTreeMap::new(),
            load_balancer_source_ranges: source_ranges,
            external_traffic_policy: Some(ExternalTrafficPolicy::Local),
            node_port: None,
        });
        self
    }

    /// Add service annotations
    pub fn with_service_annotations(mut self, annotations: BTreeMap<String, String>) -> Self {
        if let Some(ref mut svc) = self.service {
            svc.annotations = annotations;
        } else {
            self.service = Some(ServiceSpec {
                type_: ServiceType::ClusterIP,
                annotations,
                load_balancer_source_ranges: vec![],
                external_traffic_policy: None,
                node_port: None,
            });
        }
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
                pgbouncer: self.pgbouncer,
                tls: self.tls,
                metrics: self.metrics,
                service: self.service,
                restore: None,
            },
            status: None,
        }
    }
}

// =============================================================================
// Additional convenience constructors
// =============================================================================

/// Create a PostgresCluster with TLS enabled
pub fn with_tls(name: &str, namespace: &str, cert_secret: &str) -> PostgresCluster {
    PostgresClusterBuilder::single(name, namespace)
        .with_tls(cert_secret)
        .build()
}

/// Create a PostgresCluster with PgBouncer enabled
pub fn with_pgbouncer(name: &str, namespace: &str) -> PostgresCluster {
    PostgresClusterBuilder::ha(name, namespace)
        .with_pgbouncer()
        .build()
}

/// Create a PostgresCluster with TLS and PgBouncer
pub fn production_config(name: &str, namespace: &str) -> PostgresCluster {
    PostgresClusterBuilder::ha(name, namespace)
        .with_tls("tls-secret")
        .with_pgbouncer()
        .with_metrics()
        .with_resources("500m", "1Gi")
        .build()
}

/// Create a cluster with specific replica count for testing scaling
pub fn with_replicas(name: &str, namespace: &str, replicas: i32) -> PostgresCluster {
    PostgresClusterBuilder::new(name, namespace)
        .with_replicas(replicas)
        .build()
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
