//! PgBouncer connection pooler resources
//!
//! This module generates Kubernetes resources for running PgBouncer as a
//! separate Deployment (following Zalando's postgres-operator pattern).
//!
//! PgBouncer provides connection pooling to reduce the overhead of creating
//! new connections to PostgreSQL.
//!
//! Reference: https://www.pgbouncer.org/

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMap, Container, ContainerPort, EmptyDirVolumeSource, EnvVar, EnvVarSource,
    PodAffinityTerm, PodAntiAffinity, PodSpec, PodTemplateSpec, Probe, ResourceRequirements,
    SecretKeySelector, SecretVolumeSource, SecurityContext, Service, ServicePort, ServiceSpec,
    TCPSocketAction, Volume, VolumeMount, WeightedPodAffinityTerm,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use std::collections::BTreeMap;

use crate::crd::{PostgresCluster, ServiceType};
use crate::resources::common::{owner_reference, standard_labels};

/// Default PgBouncer image
const DEFAULT_PGBOUNCER_IMAGE: &str = "public.ecr.aws/bitnami/pgbouncer:latest";

/// PgBouncer listen port
const PGBOUNCER_PORT: i32 = 6432;

/// Generate labels for PgBouncer resources
fn pgbouncer_labels(cluster_name: &str) -> BTreeMap<String, String> {
    let mut labels = standard_labels(cluster_name);
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "pgbouncer".to_string(),
    );
    labels.insert(
        "postgres-operator.smoketurner.com/pooler".to_string(),
        "true".to_string(),
    );
    labels
}

/// Generate labels for replica PgBouncer resources
fn pgbouncer_replica_labels(cluster_name: &str) -> BTreeMap<String, String> {
    let mut labels = pgbouncer_labels(cluster_name);
    labels.insert(
        "postgres-operator.smoketurner.com/pooler-type".to_string(),
        "replica".to_string(),
    );
    labels
}

/// Generate the PgBouncer ConfigMap with pgbouncer.ini configuration
pub fn generate_pgbouncer_configmap(cluster: &PostgresCluster) -> ConfigMap {
    let name = format!("{}-pgbouncer-config", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let pgbouncer_spec = cluster.spec.pgbouncer.as_ref();
    let replicas = pgbouncer_spec.map(|s| s.replicas).unwrap_or(2);
    let pool_mode = pgbouncer_spec
        .map(|s| s.pool_mode.clone())
        .unwrap_or_else(|| "transaction".to_string());
    let max_db_connections = pgbouncer_spec.map(|s| s.max_db_connections).unwrap_or(60);
    let default_pool_size = pgbouncer_spec.map(|s| s.default_pool_size).unwrap_or(20);
    let max_client_conn = pgbouncer_spec.map(|s| s.max_client_conn).unwrap_or(10000);

    // Calculate per-instance max_db_connections
    let per_instance_max_db_conn = max_db_connections / replicas;

    // Generate pgbouncer.ini
    // Connect to the primary service for write operations
    let primary_service = format!("{}-primary", cluster_name);
    let pgbouncer_ini = format!(
        r#"[databases]
* = host={primary_service} port=5432

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = {port}
auth_type = scram-sha-256
auth_user = postgres
auth_query = SELECT usename, passwd FROM pg_catalog.pg_shadow WHERE usename=$1
pool_mode = {pool_mode}
max_client_conn = {max_client_conn}
default_pool_size = {default_pool_size}
max_db_connections = {per_instance_max_db_conn}
min_pool_size = 0
reserve_pool_size = 5
reserve_pool_timeout = 3
server_lifetime = 3600
server_idle_timeout = 600
server_connect_timeout = 15
server_login_retry = 15
query_timeout = 0
query_wait_timeout = 120
client_idle_timeout = 0
client_login_timeout = 60
autodb_idle_timeout = 3600
stats_period = 60
log_connections = 0
log_disconnections = 0
log_pooler_errors = 1
ignore_startup_parameters = extra_float_digits,search_path
"#,
        primary_service = primary_service,
        port = PGBOUNCER_PORT,
        pool_mode = pool_mode,
        max_client_conn = max_client_conn,
        default_pool_size = default_pool_size,
        per_instance_max_db_conn = per_instance_max_db_conn,
    );

    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(pgbouncer_labels(&cluster_name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "pgbouncer.ini".to_string(),
            pgbouncer_ini,
        )])),
        ..Default::default()
    }
}

/// Generate the PgBouncer ConfigMap for replica connections
pub fn generate_pgbouncer_replica_configmap(cluster: &PostgresCluster) -> ConfigMap {
    let name = format!("{}-pgbouncer-replica-config", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let pgbouncer_spec = cluster.spec.pgbouncer.as_ref();
    let replicas = pgbouncer_spec.map(|s| s.replicas).unwrap_or(2);
    let pool_mode = pgbouncer_spec
        .map(|s| s.pool_mode.clone())
        .unwrap_or_else(|| "transaction".to_string());
    let max_db_connections = pgbouncer_spec.map(|s| s.max_db_connections).unwrap_or(60);
    let default_pool_size = pgbouncer_spec.map(|s| s.default_pool_size).unwrap_or(20);
    let max_client_conn = pgbouncer_spec.map(|s| s.max_client_conn).unwrap_or(10000);

    let per_instance_max_db_conn = max_db_connections / replicas;

    // Connect to the replicas service for read operations
    let replica_service = format!("{}-repl", cluster_name);
    let pgbouncer_ini = format!(
        r#"[databases]
* = host={replica_service} port=5432

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = {port}
auth_type = scram-sha-256
auth_user = postgres
auth_query = SELECT usename, passwd FROM pg_catalog.pg_shadow WHERE usename=$1
pool_mode = {pool_mode}
max_client_conn = {max_client_conn}
default_pool_size = {default_pool_size}
max_db_connections = {per_instance_max_db_conn}
min_pool_size = 0
reserve_pool_size = 5
reserve_pool_timeout = 3
server_lifetime = 3600
server_idle_timeout = 600
server_connect_timeout = 15
server_login_retry = 15
query_timeout = 0
query_wait_timeout = 120
client_idle_timeout = 0
client_login_timeout = 60
autodb_idle_timeout = 3600
stats_period = 60
log_connections = 0
log_disconnections = 0
log_pooler_errors = 1
ignore_startup_parameters = extra_float_digits,search_path
"#,
        replica_service = replica_service,
        port = PGBOUNCER_PORT,
        pool_mode = pool_mode,
        max_client_conn = max_client_conn,
        default_pool_size = default_pool_size,
        per_instance_max_db_conn = per_instance_max_db_conn,
    );

    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(pgbouncer_replica_labels(&cluster_name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([(
            "pgbouncer.ini".to_string(),
            pgbouncer_ini,
        )])),
        ..Default::default()
    }
}

/// Generate anti-affinity for PgBouncer pods
fn generate_pgbouncer_anti_affinity(cluster_name: &str) -> Affinity {
    Affinity {
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_expressions: Some(vec![
                                LabelSelectorRequirement {
                                    key: "postgres-operator.smoketurner.com/cluster".to_string(),
                                    operator: "In".to_string(),
                                    values: Some(vec![cluster_name.to_string()]),
                                },
                                LabelSelectorRequirement {
                                    key: "postgres-operator.smoketurner.com/pooler".to_string(),
                                    operator: "In".to_string(),
                                    values: Some(vec!["true".to_string()]),
                                },
                            ]),
                            ..Default::default()
                        }),
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..Default::default()
                    },
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate the PgBouncer Deployment for primary connections
pub fn generate_pgbouncer_deployment(cluster: &PostgresCluster) -> Deployment {
    let name = format!("{}-pooler", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();
    let labels = pgbouncer_labels(&cluster_name);

    let pgbouncer_spec = cluster.spec.pgbouncer.as_ref();
    let replicas = pgbouncer_spec.map(|s| s.replicas).unwrap_or(2);
    let image = pgbouncer_spec
        .and_then(|s| s.image.clone())
        .unwrap_or_else(|| DEFAULT_PGBOUNCER_IMAGE.to_string());

    let secret_name = format!("{}-credentials", cluster_name);

    // Get pooler configuration
    let pool_mode = pgbouncer_spec
        .map(|s| s.pool_mode.clone())
        .unwrap_or_else(|| "transaction".to_string());
    let max_db_connections = pgbouncer_spec.map(|s| s.max_db_connections).unwrap_or(60);
    let default_pool_size = pgbouncer_spec.map(|s| s.default_pool_size).unwrap_or(20);
    let max_client_conn = pgbouncer_spec.map(|s| s.max_client_conn).unwrap_or(10000);
    let per_instance_max_db_conn = max_db_connections / replicas;

    // Primary service for write operations
    let primary_service = format!("{}-primary", cluster_name);

    // Build volumes
    // Bitnami PgBouncer image needs writable directories for initialization
    let mut volumes = vec![
        Volume {
            name: "pgbouncer-config".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
        Volume {
            name: "pgbouncer-tmp".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    ];

    let mut volume_mounts = vec![
        VolumeMount {
            name: "pgbouncer-config".to_string(),
            mount_path: "/opt/bitnami/pgbouncer/conf".to_string(),
            ..Default::default()
        },
        VolumeMount {
            name: "pgbouncer-tmp".to_string(),
            mount_path: "/opt/bitnami/pgbouncer/tmp".to_string(),
            ..Default::default()
        },
    ];

    // Add TLS volumes if TLS is enabled
    if let Some(ref tls) = cluster.spec.tls
        && tls.enabled
        && let Some(ref cert_secret) = tls.cert_secret
    {
        volumes.push(Volume {
            name: "tls-certs".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(cert_secret.clone()),
                default_mode: Some(0o640),
                ..Default::default()
            }),
            ..Default::default()
        });

        volume_mounts.push(VolumeMount {
            name: "tls-certs".to_string(),
            mount_path: "/tls".to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    // Environment variables for Bitnami PgBouncer image
    // Configure via environment variables instead of config file
    let mut env_vars = vec![
        EnvVar {
            name: "PGBOUNCER_DATABASE".to_string(),
            value: Some("*".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_HOST".to_string(),
            value: Some(primary_service),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_PORT".to_string(),
            value: Some("5432".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_USERNAME".to_string(),
            value: Some("postgres".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_PASSWORD".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name,
                    key: "POSTGRES_PASSWORD".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_AUTH_TYPE".to_string(),
            value: Some("scram-sha-256".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_POOL_MODE".to_string(),
            value: Some(pool_mode),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_MAX_CLIENT_CONN".to_string(),
            value: Some(max_client_conn.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_DEFAULT_POOL_SIZE".to_string(),
            value: Some(default_pool_size.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_MAX_DB_CONNECTIONS".to_string(),
            value: Some(per_instance_max_db_conn.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_IGNORE_STARTUP_PARAMETERS".to_string(),
            value: Some("extra_float_digits,search_path".to_string()),
            ..Default::default()
        },
        // Always use SSL when connecting to PostgreSQL backend (pg_hba.conf requires it)
        EnvVar {
            name: "PGBOUNCER_SERVER_TLS_SSLMODE".to_string(),
            value: Some("require".to_string()),
            ..Default::default()
        },
    ];

    // Add client TLS env vars if TLS is enabled
    if let Some(ref tls) = cluster.spec.tls
        && tls.enabled
        && tls.cert_secret.is_some()
    {
        let cert_file = tls
            .certificate_file
            .as_deref()
            .unwrap_or("tls.crt")
            .to_string();
        let key_file = tls
            .private_key_file
            .as_deref()
            .unwrap_or("tls.key")
            .to_string();

        env_vars.push(EnvVar {
            name: "PGBOUNCER_CLIENT_TLS_SSLMODE".to_string(),
            value: Some("require".to_string()),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "PGBOUNCER_CLIENT_TLS_CERT_FILE".to_string(),
            value: Some(format!("/tls/{}", cert_file)),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "PGBOUNCER_CLIENT_TLS_KEY_FILE".to_string(),
            value: Some(format!("/tls/{}", key_file)),
            ..Default::default()
        });
    }

    // Probes
    let readiness_probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(PGBOUNCER_PORT),
            ..Default::default()
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        success_threshold: Some(1),
        failure_threshold: Some(3),
        ..Default::default()
    };

    let liveness_probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(PGBOUNCER_PORT),
            ..Default::default()
        }),
        initial_delay_seconds: Some(30),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        success_threshold: Some(1),
        failure_threshold: Some(6),
        ..Default::default()
    };

    // Resource requirements
    let resources =
        pgbouncer_spec
            .and_then(|s| s.resources.as_ref())
            .map(|r| ResourceRequirements {
                limits: r.limits.as_ref().map(|l| {
                    let mut map = BTreeMap::new();
                    if let Some(cpu) = &l.cpu {
                        map.insert("cpu".to_string(), Quantity(cpu.clone()));
                    }
                    if let Some(memory) = &l.memory {
                        map.insert("memory".to_string(), Quantity(memory.clone()));
                    }
                    map
                }),
                requests: r.requests.as_ref().map(|req| {
                    let mut map = BTreeMap::new();
                    if let Some(cpu) = &req.cpu {
                        map.insert("cpu".to_string(), Quantity(cpu.clone()));
                    }
                    if let Some(memory) = &req.memory {
                        map.insert("memory".to_string(), Quantity(memory.clone()));
                    }
                    map
                }),
                ..Default::default()
            });

    // Note: resizePolicy is added via JSON patching in add_resize_policy_to_deployment()
    // since k8s-openapi v1_34 doesn't have the field yet

    let container = Container {
        name: "pgbouncer".to_string(),
        image: Some(image),
        image_pull_policy: Some("IfNotPresent".to_string()),
        ports: Some(vec![ContainerPort {
            container_port: PGBOUNCER_PORT,
            name: Some("pgbouncer".to_string()),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
        env: Some(env_vars),
        volume_mounts: Some(volume_mounts),
        resources,
        readiness_probe: Some(readiness_probe),
        liveness_probe: Some(liveness_probe),
        security_context: Some(SecurityContext {
            run_as_user: Some(1001), // bitnami pgbouncer user
            run_as_group: Some(1001),
            run_as_non_root: Some(true),
            allow_privilege_escalation: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };

    Deployment {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..Default::default()
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    volumes: Some(volumes),
                    termination_grace_period_seconds: Some(10),
                    affinity: Some(generate_pgbouncer_anti_affinity(&cluster_name)),
                    security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                        fs_group: Some(1001),
                        run_as_user: Some(1001),
                        run_as_group: Some(1001),
                        seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                            type_: "RuntimeDefault".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate the PgBouncer Deployment for replica connections
pub fn generate_pgbouncer_replica_deployment(cluster: &PostgresCluster) -> Deployment {
    let name = format!("{}-pooler-repl", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();
    let labels = pgbouncer_replica_labels(&cluster_name);

    let pgbouncer_spec = cluster.spec.pgbouncer.as_ref();
    let replicas = pgbouncer_spec.map(|s| s.replicas).unwrap_or(2);
    let image = pgbouncer_spec
        .and_then(|s| s.image.clone())
        .unwrap_or_else(|| DEFAULT_PGBOUNCER_IMAGE.to_string());

    let secret_name = format!("{}-credentials", cluster_name);

    // Get pooler configuration
    let pool_mode = pgbouncer_spec
        .map(|s| s.pool_mode.clone())
        .unwrap_or_else(|| "transaction".to_string());
    let max_db_connections = pgbouncer_spec.map(|s| s.max_db_connections).unwrap_or(60);
    let default_pool_size = pgbouncer_spec.map(|s| s.default_pool_size).unwrap_or(20);
    let max_client_conn = pgbouncer_spec.map(|s| s.max_client_conn).unwrap_or(10000);
    let per_instance_max_db_conn = max_db_connections / replicas;

    // Replica service for read operations
    let replica_service = format!("{}-repl", cluster_name);

    // Build volumes
    // Bitnami PgBouncer image needs writable directories for initialization
    let mut volumes = vec![
        Volume {
            name: "pgbouncer-config".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
        Volume {
            name: "pgbouncer-tmp".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        },
    ];

    let mut volume_mounts = vec![
        VolumeMount {
            name: "pgbouncer-config".to_string(),
            mount_path: "/opt/bitnami/pgbouncer/conf".to_string(),
            ..Default::default()
        },
        VolumeMount {
            name: "pgbouncer-tmp".to_string(),
            mount_path: "/opt/bitnami/pgbouncer/tmp".to_string(),
            ..Default::default()
        },
    ];

    // Add TLS volumes if TLS is enabled
    if let Some(ref tls) = cluster.spec.tls
        && tls.enabled
        && let Some(ref cert_secret) = tls.cert_secret
    {
        volumes.push(Volume {
            name: "tls-certs".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(cert_secret.clone()),
                default_mode: Some(0o640),
                ..Default::default()
            }),
            ..Default::default()
        });

        volume_mounts.push(VolumeMount {
            name: "tls-certs".to_string(),
            mount_path: "/tls".to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    // Environment variables for Bitnami PgBouncer image
    // Configure via environment variables instead of config file
    let mut env_vars = vec![
        EnvVar {
            name: "PGBOUNCER_DATABASE".to_string(),
            value: Some("*".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_HOST".to_string(),
            value: Some(replica_service),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_PORT".to_string(),
            value: Some("5432".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_USERNAME".to_string(),
            value: Some("postgres".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "POSTGRESQL_PASSWORD".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name,
                    key: "POSTGRES_PASSWORD".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_AUTH_TYPE".to_string(),
            value: Some("scram-sha-256".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_POOL_MODE".to_string(),
            value: Some(pool_mode),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_MAX_CLIENT_CONN".to_string(),
            value: Some(max_client_conn.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_DEFAULT_POOL_SIZE".to_string(),
            value: Some(default_pool_size.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_MAX_DB_CONNECTIONS".to_string(),
            value: Some(per_instance_max_db_conn.to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGBOUNCER_IGNORE_STARTUP_PARAMETERS".to_string(),
            value: Some("extra_float_digits,search_path".to_string()),
            ..Default::default()
        },
        // Always use SSL when connecting to PostgreSQL backend (pg_hba.conf requires it)
        EnvVar {
            name: "PGBOUNCER_SERVER_TLS_SSLMODE".to_string(),
            value: Some("require".to_string()),
            ..Default::default()
        },
    ];

    // Add client TLS env vars if TLS is enabled
    if let Some(ref tls) = cluster.spec.tls
        && tls.enabled
        && tls.cert_secret.is_some()
    {
        let cert_file = tls
            .certificate_file
            .as_deref()
            .unwrap_or("tls.crt")
            .to_string();
        let key_file = tls
            .private_key_file
            .as_deref()
            .unwrap_or("tls.key")
            .to_string();

        env_vars.push(EnvVar {
            name: "PGBOUNCER_CLIENT_TLS_SSLMODE".to_string(),
            value: Some("require".to_string()),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "PGBOUNCER_CLIENT_TLS_CERT_FILE".to_string(),
            value: Some(format!("/tls/{}", cert_file)),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "PGBOUNCER_CLIENT_TLS_KEY_FILE".to_string(),
            value: Some(format!("/tls/{}", key_file)),
            ..Default::default()
        });
    }

    // Probes
    let readiness_probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(PGBOUNCER_PORT),
            ..Default::default()
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        success_threshold: Some(1),
        failure_threshold: Some(3),
        ..Default::default()
    };

    let liveness_probe = Probe {
        tcp_socket: Some(TCPSocketAction {
            port: IntOrString::Int(PGBOUNCER_PORT),
            ..Default::default()
        }),
        initial_delay_seconds: Some(30),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        success_threshold: Some(1),
        failure_threshold: Some(6),
        ..Default::default()
    };

    // Resource requirements
    let resources =
        pgbouncer_spec
            .and_then(|s| s.resources.as_ref())
            .map(|r| ResourceRequirements {
                limits: r.limits.as_ref().map(|l| {
                    let mut map = BTreeMap::new();
                    if let Some(cpu) = &l.cpu {
                        map.insert("cpu".to_string(), Quantity(cpu.clone()));
                    }
                    if let Some(memory) = &l.memory {
                        map.insert("memory".to_string(), Quantity(memory.clone()));
                    }
                    map
                }),
                requests: r.requests.as_ref().map(|req| {
                    let mut map = BTreeMap::new();
                    if let Some(cpu) = &req.cpu {
                        map.insert("cpu".to_string(), Quantity(cpu.clone()));
                    }
                    if let Some(memory) = &req.memory {
                        map.insert("memory".to_string(), Quantity(memory.clone()));
                    }
                    map
                }),
                ..Default::default()
            });

    // Note: resizePolicy is added via JSON patching in add_resize_policy_to_deployment()
    // since k8s-openapi v1_34 doesn't have the field yet

    let container = Container {
        name: "pgbouncer".to_string(),
        image: Some(image),
        image_pull_policy: Some("IfNotPresent".to_string()),
        ports: Some(vec![ContainerPort {
            container_port: PGBOUNCER_PORT,
            name: Some("pgbouncer".to_string()),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
        env: Some(env_vars),
        volume_mounts: Some(volume_mounts),
        resources,
        readiness_probe: Some(readiness_probe),
        liveness_probe: Some(liveness_probe),
        security_context: Some(SecurityContext {
            run_as_user: Some(1001),
            run_as_group: Some(1001),
            run_as_non_root: Some(true),
            allow_privilege_escalation: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };

    Deployment {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_string()),
                ..Default::default()
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    volumes: Some(volumes),
                    termination_grace_period_seconds: Some(10),
                    affinity: Some(generate_pgbouncer_anti_affinity(&cluster_name)),
                    security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                        fs_group: Some(1001),
                        run_as_user: Some(1001),
                        run_as_group: Some(1001),
                        seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                            type_: "RuntimeDefault".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate the PgBouncer Service for primary connections
pub fn generate_pgbouncer_service(cluster: &PostgresCluster) -> Service {
    let name = format!("{}-pooler", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();
    let labels = pgbouncer_labels(&cluster_name);

    // Get service configuration from cluster spec
    let service_spec = cluster.spec.service.as_ref();
    let service_type = service_spec
        .map(|s| s.type_.clone())
        .unwrap_or(ServiceType::ClusterIP);
    let annotations = service_spec
        .map(|s| s.annotations.clone())
        .unwrap_or_default();
    let load_balancer_source_ranges = service_spec
        .map(|s| s.load_balancer_source_ranges.clone())
        .unwrap_or_default();

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels.clone()),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some(service_type.to_string()),
            selector: Some(labels),
            ports: Some(vec![ServicePort {
                name: Some("pgbouncer".to_string()),
                port: PGBOUNCER_PORT,
                target_port: Some(IntOrString::Int(PGBOUNCER_PORT)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            load_balancer_source_ranges: if load_balancer_source_ranges.is_empty() {
                None
            } else {
                Some(load_balancer_source_ranges)
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate the PgBouncer Service for replica connections
pub fn generate_pgbouncer_replica_service(cluster: &PostgresCluster) -> Service {
    let name = format!("{}-pooler-repl", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();
    let labels = pgbouncer_replica_labels(&cluster_name);

    // Get service configuration from cluster spec
    let service_spec = cluster.spec.service.as_ref();
    let service_type = service_spec
        .map(|s| s.type_.clone())
        .unwrap_or(ServiceType::ClusterIP);
    let annotations = service_spec
        .map(|s| s.annotations.clone())
        .unwrap_or_default();
    let load_balancer_source_ranges = service_spec
        .map(|s| s.load_balancer_source_ranges.clone())
        .unwrap_or_default();

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels.clone()),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some(service_type.to_string()),
            selector: Some(labels),
            ports: Some(vec![ServicePort {
                name: Some("pgbouncer".to_string()),
                port: PGBOUNCER_PORT,
                target_port: Some(IntOrString::Int(PGBOUNCER_PORT)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            load_balancer_source_ranges: if load_balancer_source_ranges.is_empty() {
                None
            } else {
                Some(load_balancer_source_ranges)
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Check if PgBouncer is enabled for a cluster
pub fn is_pgbouncer_enabled(cluster: &PostgresCluster) -> bool {
    cluster.spec.pgbouncer.as_ref().is_some_and(|s| s.enabled)
}

/// Check if replica pooler is enabled for a cluster
pub fn is_replica_pooler_enabled(cluster: &PostgresCluster) -> bool {
    cluster
        .spec
        .pgbouncer
        .as_ref()
        .is_some_and(|s| s.enabled && s.enable_replica_pooler)
}

/// Add Kubernetes 1.35+ resizePolicy to a Deployment's containers via JSON patching.
///
/// Since k8s-openapi v1_34 doesn't have the resizePolicy field, we add it by:
/// 1. Serializing the Deployment to JSON
/// 2. Adding resizePolicy to each container
/// 3. Deserializing back to Deployment
///
/// PgBouncer is stateless so we always use NotRequired (in-place resize).
///
/// TODO(k8s-openapi-upgrade): Remove this function when k8s-openapi supports v1_35.
/// Instead, add resizePolicy directly in generate_pgbouncer_deployment() using:
/// ```ignore
/// use k8s_openapi::api::core::v1::ContainerResizePolicy;
/// resize_policy: Some(vec![
///     ContainerResizePolicy {
///         resource_name: "cpu".to_string(),
///         restart_policy: "NotRequired".to_string(),
///     },
///     ContainerResizePolicy {
///         resource_name: "memory".to_string(),
///         restart_policy: "NotRequired".to_string(),
///     },
/// ]),
/// ```
pub fn add_resize_policy_to_deployment(deployment: Deployment, restart_on_resize: bool) -> Deployment {
    let policy = if restart_on_resize {
        "RestartContainer"
    } else {
        "NotRequired"
    };

    let resize_policy = serde_json::json!([
        {"resourceName": "cpu", "restartPolicy": policy},
        {"resourceName": "memory", "restartPolicy": policy}
    ]);

    // Serialize to JSON Value
    let mut deployment_json = match serde_json::to_value(&deployment) {
        Ok(v) => v,
        Err(_) => return deployment, // Return original on error
    };

    // Navigate to containers and add resizePolicy
    if let Some(spec) = deployment_json.get_mut("spec")
        && let Some(template) = spec.get_mut("template")
        && let Some(pod_spec) = template.get_mut("spec")
        && let Some(containers) = pod_spec.get_mut("containers")
        && let Some(containers_arr) = containers.as_array_mut()
    {
        for container in containers_arr {
            container["resizePolicy"] = resize_policy.clone();
        }
    }

    // Deserialize back to Deployment
    serde_json::from_value(deployment_json).unwrap_or(deployment)
}
