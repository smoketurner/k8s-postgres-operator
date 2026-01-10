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
    PodTemplateSpec, Probe, ResourceRequirements, SecretKeySelector, SecretVolumeSource,
    SecurityContext, ServiceAccount, Volume, VolumeMount, WeightedPodAffinityTerm,
};
use k8s_openapi::api::rbac::v1::{Role, RoleBinding, RoleRef, Subject};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use std::collections::BTreeMap;

use crate::crd::PostgresCluster;
use crate::resources::backup;
use crate::resources::common::{owner_reference, patroni_labels};

/// Default PostgreSQL parameters for HA operation
const DEFAULT_POSTGRESQL_PARAMS: &[(&str, &str)] = &[
    ("max_connections", "100"),
    ("shared_buffers", "128MB"),
    ("wal_level", "replica"),
    ("hot_standby", "on"),
    ("max_wal_senders", "10"),
    ("max_replication_slots", "10"),
    ("wal_keep_size", "1GB"),
    ("hot_standby_feedback", "on"),
];

/// Get the Spilo image for a given PostgreSQL version
/// Spilo images are named: ghcr.io/zalando/spilo-{major_version}:{tag}
/// Different PostgreSQL versions have different available Spilo tags
/// Check https://github.com/orgs/zalando/packages?repo_name=spilo for available versions
fn get_spilo_image(version: &str) -> String {
    // Extract major version (e.g., "16" from "16.1" or just "16")
    let major_version = version.split('.').next().unwrap_or("16");

    // Map PostgreSQL major versions to their Spilo image tags
    // Updated tags available at: https://github.com/zalando/spilo/pkgs/container/
    let tag = match major_version {
        "17" => "4.0-p3", // spilo-17 latest
        "16" => "3.3-p3", // spilo-16 latest
        "15" => "3.2-p2", // spilo-15 latest (from https://github.com/zalando/spilo/pkgs/container/spilo-15)
        _ => "3.3-p3",    // Fallback to PostgreSQL 16 tag
    };

    format!("ghcr.io/zalando/spilo-{}:{}", major_version, tag)
}

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
    use std::fmt::Write;

    let mut params: BTreeMap<String, String> = BTreeMap::new();

    // Default parameters for HA operation
    for (key, value) in DEFAULT_POSTGRESQL_PARAMS {
        params.insert((*key).to_string(), (*value).to_string());
    }

    // Override with user-defined parameters
    for (key, value) in &cluster.spec.postgresql_params {
        params.insert(key.clone(), value.clone());
    }

    // Format as YAML for SPILO_CONFIGURATION
    // Spilo expects this structure to be merged into its generated config
    // Write directly to string buffer to avoid intermediate Vec allocation
    let mut result = String::from("postgresql:\n  parameters:\n");
    for (k, v) in &params {
        let _ = writeln!(result, "    {}: {}", k, v);
    }
    // Remove trailing newline to match original format
    result.pop();
    result
}

/// Generate Patroni YAML configuration for ConfigMap
fn generate_patroni_yaml(cluster: &PostgresCluster) -> String {
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());

    // Build parameter list from defaults
    let mut postgresql_params: Vec<String> = DEFAULT_POSTGRESQL_PARAMS
        .iter()
        .map(|(k, v)| {
            // Quote 'on' values for YAML compatibility
            if *v == "on" {
                format!("{}: 'on'", k)
            } else {
                format!("{}: {}", k, v)
            }
        })
        .collect();

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
  scope_label: postgres-operator.smoketurner.com/cluster
  role_label: spilo-role
  labels:
    application: spilo
    postgres-operator.smoketurner.com/cluster: {cluster_name}

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
      username: standby
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
                                key: "postgres-operator.smoketurner.com/cluster".to_string(),
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
                                key: "postgres-operator.smoketurner.com/cluster".to_string(),
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

    // Use Spilo image (Zalando's PostgreSQL + Patroni) based on requested version
    let image = get_spilo_image(&cluster.spec.version);
    let secret_name = format!("{}-credentials", name);
    let sa_name = format!("{}-patroni", name);

    // Environment variables for Spilo
    // Based on Zalando's postgres-operator configuration
    // See: https://github.com/zalando/postgres-operator/blob/master/pkg/cluster/k8sres.go
    let mut env_vars = vec![
        // Cluster scope name
        EnvVar {
            name: "SCOPE".to_string(),
            value: Some(name.clone()),
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
        // Pod namespace
        EnvVar {
            name: "POD_NAMESPACE".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                    field_path: "metadata.namespace".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        // PostgreSQL superuser credentials (Spilo-native)
        EnvVar {
            name: "PGUSER_SUPERUSER".to_string(),
            value: Some("postgres".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGPASSWORD_SUPERUSER".to_string(),
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
        // Replication/standby user credentials (Spilo-native)
        EnvVar {
            name: "PGUSER_STANDBY".to_string(),
            value: Some("standby".to_string()),
            ..Default::default()
        },
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
        // Data directory
        EnvVar {
            name: "PGROOT".to_string(),
            value: Some("/var/lib/postgresql".to_string()),
            ..Default::default()
        },
        // Enable Kubernetes API as DCS backend
        EnvVar {
            name: "DCS_ENABLE_KUBERNETES_API".to_string(),
            value: Some("true".to_string()),
            ..Default::default()
        },
        // Kubernetes labels for Spilo to manage pod roles
        EnvVar {
            name: "KUBERNETES_SCOPE_LABEL".to_string(),
            value: Some("postgres-operator.smoketurner.com/cluster".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERNETES_ROLE_LABEL".to_string(),
            value: Some("spilo-role".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "KUBERNETES_LABELS".to_string(),
            value: Some(format!(
                "{{\"application\":\"spilo\",\"postgres-operator.smoketurner.com/cluster\":\"{}\"}}",
                name
            )),
            ..Default::default()
        },
        // Spilo configuration - PostgreSQL parameters
        EnvVar {
            name: "SPILO_CONFIGURATION".to_string(),
            value: Some(generate_spilo_config(cluster)),
            ..Default::default()
        },
    ];

    // Volume mounts - data volume is always needed
    let mut volume_mounts = vec![VolumeMount {
        name: "data".to_string(),
        mount_path: "/var/lib/postgresql/data".to_string(),
        ..Default::default()
    }];

    // Volumes
    let mut volumes: Vec<Volume> = vec![];

    // TLS configuration
    if let Some(ref tls) = cluster.spec.tls
        && tls.enabled
    {
        // Get filenames with defaults
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

        // Add TLS certificate volume if cert_secret is specified
        if let Some(ref cert_secret) = tls.cert_secret {
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

            // Spilo TLS environment variables
            env_vars.push(EnvVar {
                name: "SSL_CERTIFICATE_FILE".to_string(),
                value: Some(format!("/tls/{}", cert_file)),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "SSL_PRIVATE_KEY_FILE".to_string(),
                value: Some(format!("/tls/{}", key_file)),
                ..Default::default()
            });
        }

        // Add separate CA secret volume if specified
        if let Some(ref ca_secret) = tls.ca_secret {
            let ca_file = tls.ca_file.as_deref().unwrap_or("ca.crt").to_string();

            volumes.push(Volume {
                name: "tls-ca".to_string(),
                secret: Some(SecretVolumeSource {
                    secret_name: Some(ca_secret.clone()),
                    default_mode: Some(0o640),
                    ..Default::default()
                }),
                ..Default::default()
            });

            volume_mounts.push(VolumeMount {
                name: "tls-ca".to_string(),
                mount_path: "/tlsca".to_string(),
                read_only: Some(true),
                ..Default::default()
            });

            env_vars.push(EnvVar {
                name: "SSL_CA_FILE".to_string(),
                value: Some(format!("/tlsca/{}", ca_file)),
                ..Default::default()
            });
        } else if tls.cert_secret.is_some() {
            // CA file in the same secret as cert/key
            if let Some(ref ca_file) = tls.ca_file {
                env_vars.push(EnvVar {
                    name: "SSL_CA_FILE".to_string(),
                    value: Some(format!("/tls/{}", ca_file)),
                    ..Default::default()
                });
            }
        }
    }

    // Backup configuration (WAL-G)
    // Add backup environment variables, volumes, and mounts
    if backup::is_backup_enabled(cluster) {
        // Add backup environment variables
        env_vars.extend(backup::generate_backup_env_vars(cluster));

        // Add backup volumes (e.g., GCS credentials, encryption keys)
        volumes.extend(backup::generate_backup_volumes(cluster));

        // Add backup volume mounts
        volume_mounts.extend(backup::generate_backup_volume_mounts(cluster));
    }

    // Restore configuration (WAL-G clone)
    // Add restore environment variables for bootstrapping from backup
    // Note: These are only used during initial cluster creation
    if backup::is_restore_configured(cluster) {
        // Add restore environment variables
        env_vars.extend(backup::generate_restore_env_vars(cluster));

        // Add restore volumes (e.g., GCS credentials)
        volumes.extend(backup::generate_restore_volumes(cluster));

        // Add restore volume mounts
        volume_mounts.extend(backup::generate_restore_volume_mounts(cluster));
    }

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
        // Note: resizePolicy is added via JSON patching in add_resize_policy_to_statefulset()
        // since k8s-openapi v1_34 doesn't have the field yet
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

    // Init container to fix permissions on the data directory
    // Spilo's entrypoint tries to chown/chmod the data directory, which fails
    // when running as non-root. This init container runs as root to set up
    // the correct permissions before the main container starts.
    let init_container = Container {
        name: "init-permissions".to_string(),
        image: Some("busybox:1.36".to_string()),
        image_pull_policy: Some("IfNotPresent".to_string()),
        command: Some(vec!["sh".to_string(), "-c".to_string()]),
        args: Some(vec![
            // Set ownership to postgres user (101) and group (103)
            // Create the pgdata subdirectory if it doesn't exist
            "chown -R 101:103 /var/lib/postgresql/data && chmod 700 /var/lib/postgresql/data"
                .to_string(),
        ]),
        volume_mounts: Some(vec![VolumeMount {
            name: "data".to_string(),
            mount_path: "/var/lib/postgresql/data".to_string(),
            ..Default::default()
        }]),
        security_context: Some(SecurityContext {
            run_as_user: Some(0), // Run as root to change ownership
            run_as_group: Some(0),
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

    // Update strategy - calculate maxUnavailable based on replica count
    // For single replica: 1 (minimum allowed by Kubernetes 1.35+)
    // For 2 replicas: 1 (one at a time)
    // For 3+ replicas: allow up to half to update in parallel while maintaining quorum
    let max_unavailable = match replicas {
        1 => 1,                             // Single replica - minimum allowed
        2 => 1,                             // Two replicas - one at a time
        n => std::cmp::max(1, (n - 1) / 2), // Keep quorum: at most (n-1)/2
    };

    let update_strategy = StatefulSetUpdateStrategy {
        type_: Some("RollingUpdate".to_string()),
        rolling_update: Some(RollingUpdateStatefulSetStrategy {
            max_unavailable: Some(IntOrString::Int(max_unavailable)),
            partition: Some(0),
        }),
    };

    // Headless service name must match serviceName for pod DNS to work
    let headless_service_name = format!("{}-headless", name);

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: ns,
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            service_name: Some(headless_service_name),
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
                    init_containers: Some(vec![init_container]),
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

/// Add Kubernetes 1.35+ resizePolicy to a StatefulSet's containers via JSON patching.
///
/// Since k8s-openapi v1_34 doesn't have the resizePolicy field, we add it by:
/// 1. Serializing the StatefulSet to JSON
/// 2. Adding resizePolicy to each container
/// 3. Deserializing back to StatefulSet
///
/// This allows in-place pod resource updates without container restarts (default)
/// or with restarts if restart_on_resize is true.
///
/// TODO(k8s-openapi-upgrade): Remove this function when k8s-openapi supports v1_35.
/// Instead, add resizePolicy directly in generate_patroni_statefulset() using:
/// ```ignore
/// use k8s_openapi::api::core::v1::ContainerResizePolicy;
/// resize_policy: Some(vec![
///     ContainerResizePolicy {
///         resource_name: "cpu".to_string(),
///         restart_policy: if restart_on_resize { "RestartContainer" } else { "NotRequired" }.to_string(),
///     },
///     ContainerResizePolicy {
///         resource_name: "memory".to_string(),
///         restart_policy: if restart_on_resize { "RestartContainer" } else { "NotRequired" }.to_string(),
///     },
/// ]),
/// ```
pub fn add_resize_policy_to_statefulset(sts: StatefulSet, restart_on_resize: bool) -> StatefulSet {
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
    let mut sts_json = match serde_json::to_value(&sts) {
        Ok(v) => v,
        Err(_) => return sts, // Return original on error
    };

    // Navigate to containers and add resizePolicy
    if let Some(spec) = sts_json.get_mut("spec")
        && let Some(template) = spec.get_mut("template")
        && let Some(pod_spec) = template.get_mut("spec")
        && let Some(containers) = pod_spec.get_mut("containers")
        && let Some(containers_arr) = containers.as_array_mut()
    {
        for container in containers_arr {
            container["resizePolicy"] = resize_policy.clone();
        }
    }

    // Deserialize back to StatefulSet
    serde_json::from_value(sts_json).unwrap_or(sts)
}
