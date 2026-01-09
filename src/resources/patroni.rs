//! Patroni-based PostgreSQL HA resources
//!
//! This module generates Kubernetes resources for running PostgreSQL with Patroni
//! for automatic failover. Patroni handles:
//! - Leader election using Kubernetes native DCS (Endpoints/ConfigMaps)
//! - Automatic failover when primary fails
//! - Replica initialization via pg_basebackup
//! - Split-brain prevention
//!
//! Reference: https://github.com/patroni/patroni

use k8s_openapi::api::apps::v1::{
    RollingUpdateStatefulSetStrategy, StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy,
};
use k8s_openapi::api::core::v1::{
    Affinity, ConfigMap, Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction,
    PersistentVolumeClaim, PersistentVolumeClaimSpec, PodAffinityTerm, PodAntiAffinity, PodSpec,
    PodTemplateSpec, Probe, ResourceRequirements, SecretKeySelector, SecurityContext,
    ServiceAccount, Volume, VolumeMount, WeightedPodAffinityTerm,
};
use k8s_openapi::api::rbac::v1::{Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use std::collections::BTreeMap;

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, patroni_labels};

/// Default Patroni/Spilo image (Zalando's production-ready PostgreSQL + Patroni image)
/// This can be overridden in the CRD
const DEFAULT_SPILO_IMAGE: &str = "ghcr.io/zalando/spilo-16:3.3-p1";

/// Generate the Patroni configuration as a ConfigMap
///
/// This ConfigMap stores the effective Patroni configuration for auditing/debugging.
/// The actual config is passed to Spilo via SPILO_CONFIGURATION env var.
pub fn generate_patroni_config(cluster: &PostgresCluster) -> ConfigMap {
    let name = format!("{}-patroni-config", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    // Store both the Spilo config (what's actually used) and the full patroni.yml (for reference)
    let spilo_config = generate_spilo_config(cluster);
    let patroni_config = generate_patroni_yaml(cluster);

    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(patroni_labels(&cluster_name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        data: Some(BTreeMap::from([
            ("spilo-config.yml".to_string(), spilo_config),
            ("patroni.yml".to_string(), patroni_config),
        ])),
        ..Default::default()
    }
}

/// Generate Spilo configuration (YAML format)
///
/// This is passed via SPILO_CONFIGURATION env var and merged with Spilo's
/// auto-generated Patroni config. We use this to customize PostgreSQL parameters.
fn generate_spilo_config(cluster: &PostgresCluster) -> String {
    let mut params: BTreeMap<String, String> = BTreeMap::new();

    // Default parameters for HA operation
    params.insert("max_connections".to_string(), "100".to_string());
    params.insert("shared_buffers".to_string(), "128MB".to_string());
    params.insert("wal_level".to_string(), "replica".to_string());
    params.insert("hot_standby".to_string(), "on".to_string());
    params.insert("max_wal_senders".to_string(), "10".to_string());
    params.insert("max_replication_slots".to_string(), "10".to_string());
    params.insert("wal_keep_size".to_string(), "1GB".to_string());
    params.insert("hot_standby_feedback".to_string(), "on".to_string());

    // Override with user-defined parameters
    for (key, value) in &cluster.spec.postgresql_params {
        params.insert(key.clone(), value.clone());
    }

    // Format as YAML for SPILO_CONFIGURATION
    // Spilo expects this structure to be merged into its generated config
    let params_yaml: Vec<String> = params
        .iter()
        .map(|(k, v)| format!("    {}: {}", k, v))
        .collect();

    format!("postgresql:\n  parameters:\n{}", params_yaml.join("\n"))
}

/// Generate Patroni YAML configuration for ConfigMap
fn generate_patroni_yaml(cluster: &PostgresCluster) -> String {
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());

    let mut postgresql_params = vec![
        "max_connections: 100".to_string(),
        "shared_buffers: 128MB".to_string(),
        "wal_level: replica".to_string(),
        "hot_standby: 'on'".to_string(),
        "max_wal_senders: 10".to_string(),
        "max_replication_slots: 10".to_string(),
        "wal_keep_size: 1GB".to_string(),
        "hot_standby_feedback: 'on'".to_string(),
    ];

    // Add user-defined parameters
    for (key, value) in &cluster.spec.postgresql_params {
        postgresql_params.push(format!("{}: {}", key, value));
    }

    format!(
        r#"
scope: {cluster_name}
namespace: {ns}

kubernetes:
  use_endpoints: true
  scope_label: postgres.example.com/cluster
  role_label: postgres.example.com/role
  labels:
    app.kubernetes.io/name: {cluster_name}
    app.kubernetes.io/component: postgresql
    postgres.example.com/cluster: {cluster_name}

bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 33554432  # 32MB
    postgresql:
      use_pg_rewind: true
      use_slots: true
      parameters:
{params}

  initdb:
    - encoding: UTF8
    - data-checksums

  pg_hba:
    - local all all trust
    - host all all 127.0.0.1/32 scram-sha-256
    - host all all ::1/128 scram-sha-256
    - host all all 10.0.0.0/8 scram-sha-256
    - host all all 172.16.0.0/12 scram-sha-256
    - host all all 192.168.0.0/16 scram-sha-256
    - host replication replication 10.0.0.0/8 scram-sha-256
    - host replication replication 172.16.0.0/12 scram-sha-256
    - host replication replication 192.168.0.0/16 scram-sha-256

postgresql:
  listen: 0.0.0.0:5432
  connect_address: $(POD_IP):5432
  data_dir: /var/lib/postgresql/data/pgdata
  pgpass: /tmp/pgpass
  authentication:
    superuser:
      username: postgres
      password: $(POSTGRES_PASSWORD)
    replication:
      username: replication
      password: $(REPLICATION_PASSWORD)
  parameters:
{params}

restapi:
  listen: 0.0.0.0:8008
  connect_address: $(POD_IP):8008

tags:
  nofailover: false
  noloadbalance: false
  clonefrom: false
  nosync: false
"#,
        cluster_name = cluster_name,
        ns = ns,
        params = postgresql_params
            .iter()
            .map(|p| format!("        {}", p))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Generate a ServiceAccount for Patroni pods
///
/// Patroni needs permissions to read/write Endpoints and ConfigMaps for leader election.
pub fn generate_service_account(cluster: &PostgresCluster) -> ServiceAccount {
    let name = format!("{}-patroni", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    ServiceAccount {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(patroni_labels(&cluster_name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Generate a Role for Patroni with necessary permissions
pub fn generate_patroni_role(cluster: &PostgresCluster) -> Role {
    let name = format!("{}-patroni", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    Role {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(patroni_labels(&cluster_name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        rules: Some(vec![
            // Patroni needs to manage Endpoints for leader election
            k8s_openapi::api::rbac::v1::PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["endpoints".to_string()]),
                verbs: vec![
                    "get".to_string(),
                    "list".to_string(),
                    "watch".to_string(),
                    "create".to_string(),
                    "update".to_string(),
                    "patch".to_string(),
                    "delete".to_string(),
                ],
                ..Default::default()
            },
            // Patroni can also use ConfigMaps for DCS
            k8s_openapi::api::rbac::v1::PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["configmaps".to_string()]),
                verbs: vec![
                    "get".to_string(),
                    "list".to_string(),
                    "watch".to_string(),
                    "create".to_string(),
                    "update".to_string(),
                    "patch".to_string(),
                    "delete".to_string(),
                ],
                ..Default::default()
            },
            // Patroni needs to read pods for discovery
            k8s_openapi::api::rbac::v1::PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["pods".to_string()]),
                verbs: vec!["get".to_string(), "list".to_string(), "watch".to_string()],
                ..Default::default()
            },
            // Patroni needs to update its own pod labels
            k8s_openapi::api::rbac::v1::PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["pods".to_string()]),
                verbs: vec!["patch".to_string()],
                ..Default::default()
            },
        ]),
    }
}

/// Generate a RoleBinding for the Patroni ServiceAccount
pub fn generate_patroni_role_binding(cluster: &PostgresCluster) -> RoleBinding {
    let name = format!("{}-patroni", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    RoleBinding {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: ns.clone(),
            labels: Some(patroni_labels(&cluster_name)),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: name.clone(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name,
            namespace: ns,
            ..Default::default()
        }]),
    }
}

/// Generate pod anti-affinity for spreading across nodes
fn generate_anti_affinity(cluster_name: &str) -> Affinity {
    Affinity {
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_expressions: Some(vec![LabelSelectorRequirement {
                                key: "postgres.example.com/cluster".to_string(),
                                operator: "In".to_string(),
                                values: Some(vec![cluster_name.to_string()]),
                            }]),
                            ..Default::default()
                        }),
                        topology_key: "kubernetes.io/hostname".to_string(),
                        ..Default::default()
                    },
                },
                WeightedPodAffinityTerm {
                    weight: 50,
                    pod_affinity_term: PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_expressions: Some(vec![LabelSelectorRequirement {
                                key: "postgres.example.com/cluster".to_string(),
                                operator: "In".to_string(),
                                values: Some(vec![cluster_name.to_string()]),
                            }]),
                            ..Default::default()
                        }),
                        topology_key: "topology.kubernetes.io/zone".to_string(),
                        ..Default::default()
                    },
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate a StatefulSet for Patroni-managed PostgreSQL
///
/// Unlike the streaming replication mode which has separate primary/replica StatefulSets,
/// Patroni manages a single StatefulSet where any pod can become the primary.
pub fn generate_patroni_statefulset(cluster: &PostgresCluster) -> StatefulSet {
    let name = cluster.name_any();
    let ns = cluster.namespace();
    let labels = patroni_labels(&name);
    let replicas = cluster.spec.replicas;

    // Use Spilo image (Zalando's PostgreSQL + Patroni) or custom image
    let image = DEFAULT_SPILO_IMAGE.to_string();
    let secret_name = format!("{}-credentials", name);
    let sa_name = format!("{}-patroni", name);

    // Environment variables for Patroni
    let env_vars = vec![
        // Spilo scope (cluster name) - Spilo uses SCOPE env var
        EnvVar {
            name: "SCOPE".to_string(),
            value: Some(name.clone()),
            ..Default::default()
        },
        // Patroni scope (cluster name) - for compatibility
        EnvVar {
            name: "PATRONI_SCOPE".to_string(),
            value: Some(name.clone()),
            ..Default::default()
        },
        // Kubernetes namespace
        EnvVar {
            name: "PATRONI_KUBERNETES_NAMESPACE".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                    field_path: "metadata.namespace".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // Pod name for Patroni member name
        EnvVar {
            name: "PATRONI_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                    field_path: "metadata.name".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // Pod IP for connect addresses
        EnvVar {
            name: "POD_IP".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                    field_path: "status.podIP".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // PostgreSQL superuser password
        EnvVar {
            name: "POSTGRES_PASSWORD".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name.clone(),
                    key: "POSTGRES_PASSWORD".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // Replication password (Patroni native)
        EnvVar {
            name: "REPLICATION_PASSWORD".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name.clone(),
                    key: "REPLICATION_PASSWORD".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // Spilo-specific: replication password for standby user
        EnvVar {
            name: "PGPASSWORD_STANDBY".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name.clone(),
                    key: "REPLICATION_PASSWORD".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // Spilo configuration - pass entire Patroni config as YAML
        // This is the Spilo-native way to configure Patroni
        EnvVar {
            name: "SPILO_CONFIGURATION".to_string(),
            value: Some(generate_spilo_config(cluster)),
            ..Default::default()
        },
        // Data directory
        EnvVar {
            name: "PGDATA".to_string(),
            value: Some("/var/lib/postgresql/data/pgdata".to_string()),
            ..Default::default()
        },
        // Use Kubernetes for DCS - both Spilo and Patroni native settings
        EnvVar {
            name: "DCS_ENABLE_KUBERNETES_API".to_string(),
            value: Some("true".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PATRONI_KUBERNETES_USE_ENDPOINTS".to_string(),
            value: Some("true".to_string()),
            ..Default::default()
        },
        // Labels for Patroni to manage
        EnvVar {
            name: "PATRONI_KUBERNETES_LABELS".to_string(),
            value: Some(format!(
                "{{app.kubernetes.io/name: {}, postgres.example.com/cluster: {}}}",
                name, name
            )),
            ..Default::default()
        },
    ];

    // Volume mounts - only data volume needed, Spilo config is via env vars
    let volume_mounts = vec![VolumeMount {
        name: "data".to_string(),
        mount_path: "/var/lib/postgresql/data".to_string(),
        ..Default::default()
    }];

    // No additional volumes needed - Spilo generates its config from env vars
    let volumes: Vec<Volume> = vec![];

    // Startup probe - Patroni REST API
    let startup_probe = Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/readiness".to_string()),
            port: IntOrString::Int(8008),
            scheme: Some("HTTP".to_string()),
            ..Default::default()
        }),
        initial_delay_seconds: Some(10),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        failure_threshold: Some(30), // 5 minutes to start
        ..Default::default()
    };

    // Readiness probe - Patroni REST API
    let readiness_probe = Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/readiness".to_string()),
            port: IntOrString::Int(8008),
            scheme: Some("HTTP".to_string()),
            ..Default::default()
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        success_threshold: Some(1),
        failure_threshold: Some(3),
        ..Default::default()
    };

    // Liveness probe - Patroni REST API
    let liveness_probe = Probe {
        http_get: Some(HTTPGetAction {
            path: Some("/liveness".to_string()),
            port: IntOrString::Int(8008),
            scheme: Some("HTTP".to_string()),
            ..Default::default()
        }),
        initial_delay_seconds: Some(30),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        success_threshold: Some(1),
        failure_threshold: Some(6),
        ..Default::default()
    };

    // Container
    let container = Container {
        name: "postgres".to_string(),
        image: Some(image),
        image_pull_policy: Some("IfNotPresent".to_string()),
        ports: Some(vec![
            ContainerPort {
                container_port: 5432,
                name: Some("postgresql".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            },
            ContainerPort {
                container_port: 8008,
                name: Some("patroni".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            },
        ]),
        env: Some(env_vars),
        volume_mounts: Some(volume_mounts),
        resources: cluster
            .spec
            .resources
            .as_ref()
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
            }),
        startup_probe: Some(startup_probe),
        readiness_probe: Some(readiness_probe),
        liveness_probe: Some(liveness_probe),
        security_context: Some(SecurityContext {
            run_as_user: Some(101),  // Spilo uses uid 101
            run_as_group: Some(103), // postgres group
            allow_privilege_escalation: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };

    // PVC template
    let pvc_template = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("data".to_string()),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            storage_class_name: cluster.spec.storage.storage_class.clone(),
            resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                requests: Some(BTreeMap::from([(
                    "storage".to_string(),
                    Quantity(cluster.spec.storage.size.clone()),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Update strategy
    let update_strategy = StatefulSetUpdateStrategy {
        type_: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateStatefulSetStrategy {
            max_unavailable: Some(IntOrString::Int(1)),
            partition: Some(0),
        }),
    };

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: ns,
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            service_name: Some(name.clone()),
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            update_strategy: Some(update_strategy),
            pod_management_policy: Some("OrderedReady".to_string()),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(sa_name),
                    containers: vec![container],
                    volumes: Some(volumes),
                    termination_grace_period_seconds: Some(30),
                    affinity: Some(generate_anti_affinity(&name)),
                    security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                        fs_group: Some(103), // postgres group
                        run_as_user: Some(101),
                        run_as_group: Some(103),
                        seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
                            type_: "RuntimeDefault".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![pvc_template]),
            persistent_volume_claim_retention_policy: Some(
                k8s_openapi::api::apps::v1::StatefulSetPersistentVolumeClaimRetentionPolicy {
                    when_deleted: Some("Retain".to_string()),
                    when_scaled: Some("Retain".to_string()),
                },
            ),
            ..Default::default()
        }),
        ..Default::default()
    }
}
