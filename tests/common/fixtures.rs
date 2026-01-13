//! Test fixtures and builders for PostgresCluster and PostgresDatabase resources
//!
//! All PostgreSQL clusters use Patroni for consistent management.
//! The replica count determines the cluster topology:
//! - 1 replica: single server
//! - 2 replicas: primary + 1 read replica
//! - 3+ replicas: highly available cluster
//!
//! # Quick Start
//!
//! For simple unit tests, use the convenience functions:
//! ```rust,ignore
//! let cluster = create_test_cluster("my-cluster", "default", 3);
//! let cluster_with_tls = create_test_cluster_with_tls("my-cluster", "default", 3);
//! ```
//!
//! For more complex configurations, use the builder pattern:
//! ```rust,ignore
//! let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
//!     .with_tls("letsencrypt-prod")
//!     .with_pgbouncer()
//!     .with_metrics()
//!     .build();
//! ```

use kube::core::ObjectMeta;
use postgres_operator::crd::{
    ClusterRef, ClusterReference, ConnectionScalingMetric, CpuScalingMetric, CutoverConfig,
    CutoverMode, DatabaseSpec, ExternalTrafficPolicy, GrantSpec, IssuerKind, IssuerRef,
    MetricsSpec, NetworkPolicySpec, PgBouncerSpec, PostgresCluster, PostgresClusterSpec,
    PostgresDatabase, PostgresDatabaseSpec, PostgresUpgrade, PostgresUpgradeSpec, PostgresVersion,
    PreChecksConfig, ResourceList, ResourceRequirements, RolePrivilege, RoleSpec, ScalingMetrics,
    ScalingSpec, ServiceSpec, ServiceType, StorageSpec, TLSSpec, TablePrivilege,
    TargetClusterOverrides, UpgradeStrategy, UpgradeTimeouts,
};
use std::collections::BTreeMap;

// =============================================================================
// Convenience Functions for Simple Test Cases
// =============================================================================

/// Create a basic test cluster with minimal configuration
///
/// This is the simplest way to create a cluster for unit tests.
/// TLS is disabled by default (no cert-manager in test environment).
///
/// # Example
/// ```rust,ignore
/// let cluster = create_test_cluster("my-cluster", "default", 1);
/// ```
pub fn create_test_cluster(name: &str, namespace: &str, replicas: i32) -> PostgresCluster {
    PostgresClusterBuilder::new(name, namespace)
        .with_replicas(replicas)
        .with_storage("10Gi", Some("standard"))
        .with_uid("test-uid-12345")
        .build()
}

/// Create a test cluster with TLS enabled (cert-manager ClusterIssuer)
///
/// # Example
/// ```rust,ignore
/// let cluster = create_test_cluster_with_tls("my-cluster", "default", 3);
/// ```
pub fn create_test_cluster_with_tls(name: &str, namespace: &str, replicas: i32) -> PostgresCluster {
    PostgresClusterBuilder::new(name, namespace)
        .with_replicas(replicas)
        .with_storage("10Gi", Some("standard"))
        .with_uid("test-uid-12345")
        .with_tls("letsencrypt-prod")
        .build()
}

/// Create a test cluster with PgBouncer enabled
///
/// # Example
/// ```rust,ignore
/// let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
/// ```
pub fn create_test_cluster_with_pgbouncer(
    name: &str,
    namespace: &str,
    replicas: i32,
) -> PostgresCluster {
    PostgresClusterBuilder::new(name, namespace)
        .with_replicas(replicas)
        .with_storage("10Gi", Some("standard"))
        .with_uid("test-uid-12345")
        .with_pgbouncer()
        .build()
}

/// Create a test cluster with PgBouncer and replica pooler enabled
///
/// # Example
/// ```rust,ignore
/// let cluster = create_test_cluster_with_pgbouncer_replica("my-cluster", "default", 3);
/// ```
pub fn create_test_cluster_with_pgbouncer_replica(
    name: &str,
    namespace: &str,
    replicas: i32,
) -> PostgresCluster {
    PostgresClusterBuilder::new(name, namespace)
        .with_replicas(replicas)
        .with_storage("10Gi", Some("standard"))
        .with_uid("test-uid-12345")
        .with_pgbouncer_replica()
        .build()
}

// =============================================================================
// PostgresCluster Builder
// =============================================================================

/// Builder for PostgresCluster test fixtures
///
/// Provides a fluent API for creating PostgresCluster resources with various
/// configurations. Use this for complex test scenarios.
///
/// # Example
/// ```rust,ignore
/// let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
///     .with_tls("letsencrypt-prod")
///     .with_pgbouncer()
///     .with_metrics()
///     .with_cpu_scaling(1, 5, 80)
///     .build();
/// ```
#[allow(dead_code)]
pub struct PostgresClusterBuilder {
    name: String,
    namespace: String,
    uid: Option<String>,
    generation: Option<i64>,
    version: PostgresVersion,
    replicas: i32,
    storage_size: String,
    storage_class: Option<String>,
    postgresql_params: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    resources: Option<ResourceRequirements>,
    tls: TLSSpec,
    pgbouncer: Option<PgBouncerSpec>,
    metrics: Option<MetricsSpec>,
    service: Option<ServiceSpec>,
    scaling: Option<ScalingSpec>,
    network_policy: Option<NetworkPolicySpec>,
}

#[allow(dead_code)]
impl PostgresClusterBuilder {
    /// Create a new builder with default values
    ///
    /// TLS is disabled by default for tests (no cert-manager in test environment).
    pub fn new(name: &str, namespace: &str) -> Self {
        Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            uid: None,
            generation: Some(1),
            version: PostgresVersion::V16,
            replicas: 1,
            storage_size: "1Gi".to_string(),
            storage_class: None,
            postgresql_params: BTreeMap::new(),
            labels: BTreeMap::new(),
            resources: None,
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

    /// Set the resource UID (for owner references)
    pub fn with_uid(mut self, uid: &str) -> Self {
        self.uid = Some(uid.to_string());
        self
    }

    /// Set the resource generation
    pub fn with_generation(mut self, generation: i64) -> Self {
        self.generation = Some(generation);
        self
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

    /// Add a user label
    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    /// Set resource requirements (same for requests and limits)
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

    /// Set resource requirements with restart_on_resize option
    pub fn with_resources_resize(
        mut self,
        req_cpu: &str,
        req_memory: &str,
        limit_cpu: &str,
        limit_memory: &str,
        restart_on_resize: bool,
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
            restart_on_resize: Some(restart_on_resize),
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

    /// Enable TLS with custom duration and renewal settings
    pub fn with_tls_full(
        mut self,
        issuer_name: &str,
        additional_dns_names: Vec<String>,
        duration: Option<&str>,
        renew_before: Option<&str>,
    ) -> Self {
        self.tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: issuer_name.to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names,
            duration: duration.map(String::from),
            renew_before: renew_before.map(String::from),
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
        self.network_policy = Some(NetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![],
        });
        self
    }

    /// Enable network policy with external access (for testing)
    pub fn with_network_policy_external_access(mut self) -> Self {
        self.network_policy = Some(NetworkPolicySpec {
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
        self.network_policy = Some(NetworkPolicySpec {
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
                uid: self.uid,
                generation: self.generation,
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
                labels: self.labels,
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

// =============================================================================
// PostgresDatabase Builder
// =============================================================================

/// Builder for PostgresDatabase test fixtures
///
/// Creates PostgresDatabase resources for integration testing.
/// Provides a fluent API to configure databases, roles, grants, and extensions.
///
/// # Example
/// ```rust,ignore
/// let db = PostgresDatabaseBuilder::new("myapp", "default", "my-cluster")
///     .with_database_name("myapp_production")
///     .with_role("app_user", "myapp-credentials")
///     .with_extension("uuid-ossp")
///     .build();
/// ```
#[allow(dead_code)]
pub struct PostgresDatabaseBuilder {
    name: String,
    namespace: String,
    cluster_ref: String,
    cluster_namespace: Option<String>,
    database_name: String,
    owner: String,
    encoding: Option<String>,
    locale: Option<String>,
    connection_limit: Option<i32>,
    roles: Vec<RoleSpec>,
    grants: Vec<GrantSpec>,
    extensions: Vec<String>,
}

#[allow(dead_code)]
impl PostgresDatabaseBuilder {
    /// Create a new builder with required fields
    ///
    /// # Arguments
    /// * `name` - Name of the PostgresDatabase resource
    /// * `namespace` - Namespace for the resource
    /// * `cluster_ref` - Name of the parent PostgresCluster
    pub fn new(name: &str, namespace: &str, cluster_ref: &str) -> Self {
        Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            cluster_ref: cluster_ref.to_string(),
            cluster_namespace: None,
            database_name: name.to_string(), // Default to resource name
            owner: format!("{}_owner", name),
            encoding: None,
            locale: None,
            connection_limit: None,
            roles: Vec::new(),
            grants: Vec::new(),
            extensions: Vec::new(),
        }
    }

    /// Set the database name (defaults to resource name)
    pub fn with_database_name(mut self, name: &str) -> Self {
        self.database_name = name.to_string();
        self
    }

    /// Set the database owner
    pub fn with_owner(mut self, owner: &str) -> Self {
        self.owner = owner.to_string();
        self
    }

    /// Set the cluster namespace for cross-namespace references
    pub fn with_cluster_namespace(mut self, namespace: &str) -> Self {
        self.cluster_namespace = Some(namespace.to_string());
        self
    }

    /// Set database encoding
    pub fn with_encoding(mut self, encoding: &str) -> Self {
        self.encoding = Some(encoding.to_string());
        self
    }

    /// Set database locale
    pub fn with_locale(mut self, locale: &str) -> Self {
        self.locale = Some(locale.to_string());
        self
    }

    /// Set database connection limit
    pub fn with_connection_limit(mut self, limit: i32) -> Self {
        self.connection_limit = Some(limit);
        self
    }

    /// Add a role with login capability
    ///
    /// This creates a role that can log in with a generated secret.
    pub fn with_role(mut self, name: &str, secret_name: &str) -> Self {
        self.roles.push(RoleSpec {
            name: name.to_string(),
            privileges: vec![],
            secret_name: secret_name.to_string(),
            connection_limit: None,
            login: true,
        });
        self
    }

    /// Add a role with specific privileges
    pub fn with_role_privileges(
        mut self,
        name: &str,
        secret_name: &str,
        privileges: Vec<RolePrivilege>,
    ) -> Self {
        self.roles.push(RoleSpec {
            name: name.to_string(),
            privileges,
            secret_name: secret_name.to_string(),
            connection_limit: None,
            login: true,
        });
        self
    }

    /// Add a role with full configuration
    pub fn with_role_full(
        mut self,
        name: &str,
        secret_name: &str,
        privileges: Vec<RolePrivilege>,
        connection_limit: Option<i32>,
        login: bool,
    ) -> Self {
        self.roles.push(RoleSpec {
            name: name.to_string(),
            privileges,
            secret_name: secret_name.to_string(),
            connection_limit,
            login,
        });
        self
    }

    /// Add a grant for table privileges
    pub fn with_grant(
        mut self,
        role: &str,
        schema: &str,
        privileges: Vec<TablePrivilege>,
        all_tables: bool,
    ) -> Self {
        self.grants.push(GrantSpec {
            role: role.to_string(),
            schema: schema.to_string(),
            privileges,
            all_tables,
            all_sequences: false,
            all_functions: false,
        });
        self
    }

    /// Add a grant with full configuration
    pub fn with_grant_full(
        mut self,
        role: &str,
        schema: &str,
        privileges: Vec<TablePrivilege>,
        all_tables: bool,
        all_sequences: bool,
        all_functions: bool,
    ) -> Self {
        self.grants.push(GrantSpec {
            role: role.to_string(),
            schema: schema.to_string(),
            privileges,
            all_tables,
            all_sequences,
            all_functions,
        });
        self
    }

    /// Add an extension to be created in the database
    pub fn with_extension(mut self, name: &str) -> Self {
        self.extensions.push(name.to_string());
        self
    }

    /// Add multiple extensions
    pub fn with_extensions(mut self, extensions: &[&str]) -> Self {
        for ext in extensions {
            self.extensions.push(ext.to_string());
        }
        self
    }

    /// Build the PostgresDatabase resource
    pub fn build(self) -> PostgresDatabase {
        PostgresDatabase {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(self.namespace),
                ..Default::default()
            },
            spec: PostgresDatabaseSpec {
                cluster_ref: ClusterRef {
                    name: self.cluster_ref,
                    namespace: self.cluster_namespace,
                },
                database: DatabaseSpec {
                    name: self.database_name,
                    owner: self.owner,
                    encoding: self.encoding,
                    locale: self.locale,
                    connection_limit: self.connection_limit,
                },
                roles: self.roles,
                grants: self.grants,
                extensions: self.extensions,
            },
            status: None,
        }
    }
}

// =============================================================================
// PostgresUpgrade Builder
// =============================================================================

/// Builder for PostgresUpgrade test fixtures
///
/// Creates PostgresUpgrade resources for integration testing.
/// Provides a fluent API to configure upgrade parameters.
///
/// # Example
/// ```rust,ignore
/// let upgrade = PostgresUpgradeBuilder::new("my-upgrade", "default")
///     .with_source_cluster("source-cluster")
///     .with_target_version(PostgresVersion::V17)
///     .with_manual_cutover()
///     .build();
/// ```
#[allow(dead_code)]
pub struct PostgresUpgradeBuilder {
    name: String,
    namespace: String,
    source_cluster: String,
    source_namespace: Option<String>,
    target_version: PostgresVersion,
    cutover_mode: CutoverMode,
    target_overrides: Option<TargetClusterOverrides>,
    max_replication_lag_seconds: i32,
    verify_row_counts: bool,
    row_count_tolerance: i64,
    min_verification_passes: i32,
    target_cluster_ready_timeout: String,
    initial_sync_timeout: String,
}

#[allow(dead_code)]
impl PostgresUpgradeBuilder {
    /// Create a new builder with required fields
    ///
    /// # Arguments
    /// * `name` - Name of the PostgresUpgrade resource
    /// * `namespace` - Namespace for the resource
    pub fn new(name: &str, namespace: &str) -> Self {
        Self {
            name: name.to_string(),
            namespace: namespace.to_string(),
            source_cluster: String::new(),
            source_namespace: None,
            target_version: PostgresVersion::V17,
            cutover_mode: CutoverMode::Manual,
            target_overrides: None,
            max_replication_lag_seconds: 0,
            verify_row_counts: true,
            row_count_tolerance: 0,
            min_verification_passes: 3,
            target_cluster_ready_timeout: "30m".to_string(),
            initial_sync_timeout: "24h".to_string(),
        }
    }

    /// Set the source cluster name (required)
    pub fn with_source_cluster(mut self, name: &str) -> Self {
        self.source_cluster = name.to_string();
        self
    }

    /// Set the source cluster namespace (for cross-namespace upgrades)
    pub fn with_source_namespace(mut self, namespace: &str) -> Self {
        self.source_namespace = Some(namespace.to_string());
        self
    }

    /// Set the target PostgreSQL version
    pub fn with_target_version(mut self, version: PostgresVersion) -> Self {
        self.target_version = version;
        self
    }

    /// Use manual cutover mode (default)
    pub fn with_manual_cutover(mut self) -> Self {
        self.cutover_mode = CutoverMode::Manual;
        self
    }

    /// Use automatic cutover mode
    pub fn with_automatic_cutover(mut self) -> Self {
        self.cutover_mode = CutoverMode::Automatic;
        self
    }

    /// Set target cluster overrides (replicas, resources, labels)
    pub fn with_target_overrides(mut self, overrides: TargetClusterOverrides) -> Self {
        self.target_overrides = Some(overrides);
        self
    }

    /// Override target cluster replicas
    pub fn with_target_replicas(mut self, replicas: i32) -> Self {
        let overrides = self.target_overrides.get_or_insert_with(Default::default);
        overrides.replicas = Some(replicas);
        self
    }

    /// Set maximum allowed replication lag in seconds
    pub fn with_max_replication_lag(mut self, seconds: i32) -> Self {
        self.max_replication_lag_seconds = seconds;
        self
    }

    /// Disable row count verification
    pub fn without_row_count_verification(mut self) -> Self {
        self.verify_row_counts = false;
        self
    }

    /// Set row count tolerance
    pub fn with_row_count_tolerance(mut self, tolerance: i64) -> Self {
        self.row_count_tolerance = tolerance;
        self
    }

    /// Set minimum verification passes required
    pub fn with_verification_passes(mut self, passes: i32) -> Self {
        self.min_verification_passes = passes;
        self
    }

    /// Set target cluster ready timeout
    pub fn with_target_ready_timeout(mut self, timeout: &str) -> Self {
        self.target_cluster_ready_timeout = timeout.to_string();
        self
    }

    /// Set initial sync timeout
    pub fn with_initial_sync_timeout(mut self, timeout: &str) -> Self {
        self.initial_sync_timeout = timeout.to_string();
        self
    }

    /// Build the PostgresUpgrade resource
    pub fn build(self) -> PostgresUpgrade {
        PostgresUpgrade {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: Some(self.namespace),
                ..Default::default()
            },
            spec: PostgresUpgradeSpec {
                source_cluster: ClusterReference {
                    name: self.source_cluster,
                    namespace: self.source_namespace,
                },
                target_version: self.target_version,
                target_cluster_overrides: self.target_overrides,
                strategy: UpgradeStrategy {
                    cutover: CutoverConfig {
                        mode: self.cutover_mode,
                        allowed_window: None,
                    },
                    pre_checks: PreChecksConfig {
                        max_replication_lag_seconds: self.max_replication_lag_seconds,
                        verify_row_counts: self.verify_row_counts,
                        row_count_tolerance: self.row_count_tolerance,
                        min_verification_passes: self.min_verification_passes,
                        ..Default::default()
                    },
                    timeouts: UpgradeTimeouts {
                        target_cluster_ready: self.target_cluster_ready_timeout,
                        initial_sync: self.initial_sync_timeout,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            status: None,
        }
    }
}
