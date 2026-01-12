//! Test fixtures and builders for PostgresCluster resources
//!
//! All PostgreSQL clusters use Patroni for consistent management.
//! The replica count determines the cluster topology:
//! - 1 replica: single server
//! - 2 replicas: primary + 1 read replica
//! - 3+ replicas: highly available cluster

use kube::core::ObjectMeta;
use postgres_operator::crd::{
    ConnectionScalingMetric, CpuScalingMetric, ExternalTrafficPolicy, IssuerKind, IssuerRef,
    MetricsSpec, PgBouncerSpec, PostgresCluster, PostgresClusterSpec, PostgresVersion,
    ResourceList, ResourceRequirements, ScalingMetrics, ScalingSpec, ServiceSpec, ServiceType,
    StorageSpec, TLSSpec,
};
use std::collections::BTreeMap;

/// Builder for PostgresCluster test fixtures
pub struct PostgresClusterBuilder {
    name: String,
    namespace: String,
    version: PostgresVersion,
    replicas: i32,
    storage_size: String,
    storage_class: Option<String>,
    postgresql_params: BTreeMap<String, String>,
    resources: Option<ResourceRequirements>,
    tls: TLSSpec,
    pgbouncer: Option<PgBouncerSpec>,
    metrics: Option<MetricsSpec>,
    service: Option<ServiceSpec>,
    scaling: Option<ScalingSpec>,
    network_policy: Option<postgres_operator::crd::NetworkPolicySpec>,
}

impl PostgresClusterBuilder {
    /// Create a new builder with default values
    /// Note: TLS is enabled by default but without an issuer (tests must provide one)
    pub fn new(name: &str, namespace: &str) -> Self {
        Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            version: PostgresVersion::V16,
            replicas: 1,
            storage_size: "1Gi".to_string(),
            storage_class: None,
            postgresql_params: BTreeMap::new(),
            resources: None,
            // TLS disabled by default for tests (no cert-manager in test environment)
            tls: TLSSpec {
                enabled: false,
                issuer_ref: None,
                additional_dns_names: vec![],
                duration: None,
                renew_before: None,
            },
            pgbouncer: None,
            metrics: None,
            service: None,
            scaling: None,
            network_policy: None,
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
    pub fn with_version(mut self, version: PostgresVersion) -> Self {
        self.version = version;
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

    /// Enable TLS with a cert-manager ClusterIssuer
    pub fn with_tls(mut self, issuer_name: &str) -> Self {
        self.tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: issuer_name.to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        self
    }

    /// Enable TLS with a namespace-scoped Issuer
    pub fn with_tls_issuer(mut self, issuer_name: &str) -> Self {
        self.tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: issuer_name.to_string(),
                kind: IssuerKind::Issuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        self
    }

    /// Disable TLS (for testing without cert-manager)
    pub fn without_tls(mut self) -> Self {
        self.tls = TLSSpec {
            enabled: false,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
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

    /// Enable CPU-based auto-scaling with KEDA
    pub fn with_cpu_scaling(
        mut self,
        min_replicas: i32,
        max_replicas: i32,
        target_cpu: i32,
    ) -> Self {
        self.scaling = Some(ScalingSpec {
            min_replicas: Some(min_replicas),
            max_replicas,
            metrics: Some(ScalingMetrics {
                cpu: Some(CpuScalingMetric {
                    target_utilization: target_cpu,
                }),
                connections: None,
            }),
            replication_lag_threshold: "30s".to_string(),
            ..Default::default()
        });
        self
    }

    /// Enable connection-based auto-scaling with KEDA
    pub fn with_connection_scaling(
        mut self,
        min_replicas: i32,
        max_replicas: i32,
        target_per_replica: i32,
    ) -> Self {
        self.scaling = Some(ScalingSpec {
            min_replicas: Some(min_replicas),
            max_replicas,
            metrics: Some(ScalingMetrics {
                cpu: None,
                connections: Some(ConnectionScalingMetric { target_per_replica }),
            }),
            replication_lag_threshold: "30s".to_string(),
            ..Default::default()
        });
        self
    }

    /// Enable combined CPU and connection-based auto-scaling with KEDA
    pub fn with_combined_scaling(
        mut self,
        min_replicas: i32,
        max_replicas: i32,
        target_cpu: i32,
        target_connections: i32,
    ) -> Self {
        self.scaling = Some(ScalingSpec {
            min_replicas: Some(min_replicas),
            max_replicas,
            metrics: Some(ScalingMetrics {
                cpu: Some(CpuScalingMetric {
                    target_utilization: target_cpu,
                }),
                connections: Some(ConnectionScalingMetric {
                    target_per_replica: target_connections,
                }),
            }),
            replication_lag_threshold: "30s".to_string(),
            ..Default::default()
        });
        self
    }

    /// Enable scaling with disabled headroom (max == min, no scaling)
    pub fn with_scaling_disabled(mut self) -> Self {
        let replicas = self.replicas;
        self.scaling = Some(ScalingSpec {
            min_replicas: Some(replicas),
            max_replicas: replicas,
            metrics: None,
            replication_lag_threshold: "30s".to_string(),
            ..Default::default()
        });
        self
    }

    /// Enable network policy with default settings (secure by default)
    pub fn with_network_policy(mut self) -> Self {
        self.network_policy = Some(postgres_operator::crd::NetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![],
        });
        self
    }

    /// Enable network policy with external access (for testing)
    pub fn with_network_policy_external_access(mut self) -> Self {
        self.network_policy = Some(postgres_operator::crd::NetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![],
        });
        self
    }

    /// Enable network policy with cross-namespace access
    pub fn with_network_policy_allow_from(
        mut self,
        peers: Vec<postgres_operator::crd::NetworkPolicyPeer>,
    ) -> Self {
        self.network_policy = Some(postgres_operator::crd::NetworkPolicySpec {
            allow_external_access: false,
            allow_from: peers,
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
                labels: Default::default(),
                resources: self.resources,
                backup: None,
                pgbouncer: self.pgbouncer,
                tls: self.tls,
                metrics: self.metrics,
                service: self.service,
                restore: None,
                scaling: self.scaling,
                network_policy: self.network_policy,
            },
            status: None,
        }
    }
}
