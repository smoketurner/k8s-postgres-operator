//! Unit tests for resource generators
//!
//! Tests for Patroni StatefulSet, Service, Secret, and PDB generation.
//! All PostgreSQL clusters use Patroni for consistent management.

use postgres_operator::crd::{
    PgBouncerSpec, PostgresCluster, PostgresClusterSpec, StorageSpec, TLSSpec,
};
use postgres_operator::resources::{patroni, pdb, pgbouncer, secret, service};

/// Helper to create a test cluster
fn create_test_cluster(name: &str, namespace: &str, replicas: i32) -> PostgresCluster {
    PostgresCluster {
        metadata: kube::core::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some("test-uid-12345".to_string()),
            generation: Some(1),
            ..Default::default()
        },
        spec: PostgresClusterSpec {
            version: "16".to_string(),
            replicas,
            storage: StorageSpec {
                storage_class: Some("standard".to_string()),
                size: "10Gi".to_string(),
            },
            resources: None,
            postgresql_params: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: None,
            metrics: None,
            service: None,
        },
        status: None,
    }
}

/// Helper to create a test cluster with TLS enabled
fn create_test_cluster_with_tls(name: &str, namespace: &str, replicas: i32) -> PostgresCluster {
    let mut cluster = create_test_cluster(name, namespace, replicas);
    cluster.spec.tls = Some(TLSSpec {
        enabled: true,
        cert_secret: Some("my-tls-secret".to_string()),
        ca_secret: None,
        certificate_file: None,
        private_key_file: None,
        ca_file: None,
    });
    cluster
}

/// Helper to create a test cluster with TLS and custom filenames
fn create_test_cluster_with_tls_custom_files(
    name: &str,
    namespace: &str,
    replicas: i32,
) -> PostgresCluster {
    let mut cluster = create_test_cluster(name, namespace, replicas);
    cluster.spec.tls = Some(TLSSpec {
        enabled: true,
        cert_secret: Some("my-tls-secret".to_string()),
        ca_secret: Some("my-ca-secret".to_string()),
        certificate_file: Some("server.crt".to_string()),
        private_key_file: Some("server.key".to_string()),
        ca_file: Some("root.crt".to_string()),
    });
    cluster
}

/// Helper to create a test cluster with PgBouncer enabled
fn create_test_cluster_with_pgbouncer(
    name: &str,
    namespace: &str,
    replicas: i32,
) -> PostgresCluster {
    let mut cluster = create_test_cluster(name, namespace, replicas);
    cluster.spec.pgbouncer = Some(PgBouncerSpec {
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
    cluster
}

/// Helper to create a test cluster with PgBouncer and replica pooler
fn create_test_cluster_with_pgbouncer_replica(
    name: &str,
    namespace: &str,
    replicas: i32,
) -> PostgresCluster {
    let mut cluster = create_test_cluster(name, namespace, replicas);
    cluster.spec.pgbouncer = Some(PgBouncerSpec {
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
    cluster
}

mod patroni_statefulset_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_patroni_statefulset_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        // Patroni uses the cluster name directly
        assert_eq!(sts.name_any(), "my-cluster");
    }

    #[test]
    fn test_patroni_statefulset_single_replica() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(1));
    }

    #[test]
    fn test_patroni_statefulset_three_replicas() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(3));
    }

    #[test]
    fn test_patroni_statefulset_labels() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let labels = sts.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("postgres.example.com/ha-mode"),
            Some(&"patroni".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"postgres-operator".to_string())
        );
    }

    #[test]
    fn test_patroni_statefulset_owner_reference() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let owner_refs = sts.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
        assert_eq!(owner_refs[0].name, "my-cluster");
        assert!(owner_refs[0].controller.unwrap_or(false));
    }

    #[test]
    fn test_patroni_statefulset_update_strategy() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let spec = sts.spec.as_ref().unwrap();
        let strategy = spec.update_strategy.as_ref().unwrap();
        assert_eq!(strategy.type_, Some("RollingUpdate".to_string()));
    }

    #[test]
    fn test_patroni_statefulset_service_account() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let spec = sts.spec.as_ref().unwrap();
        let pod_spec = spec.template.spec.as_ref().unwrap();
        assert_eq!(
            pod_spec.service_account_name,
            Some("my-cluster-patroni".to_string())
        );
    }

    #[test]
    fn test_patroni_statefulset_ports() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let containers = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        let ports = containers[0].ports.as_ref().unwrap();

        // Should have PostgreSQL and Patroni ports
        let pg_port = ports.iter().find(|p| p.container_port == 5432);
        let patroni_port = ports.iter().find(|p| p.container_port == 8008);

        assert!(pg_port.is_some());
        assert!(patroni_port.is_some());
    }

    #[test]
    fn test_patroni_statefulset_probes() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let container = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        // Patroni uses HTTP probes against its REST API
        let readiness = container.readiness_probe.as_ref().unwrap();
        assert!(readiness.http_get.is_some());
        let http = readiness.http_get.as_ref().unwrap();
        assert_eq!(http.path, Some("/readiness".to_string()));

        let liveness = container.liveness_probe.as_ref().unwrap();
        assert!(liveness.http_get.is_some());
        let http = liveness.http_get.as_ref().unwrap();
        assert_eq!(http.path, Some("/liveness".to_string()));
    }
}

mod patroni_config_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_patroni_config_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let cm = patroni::generate_patroni_config(&cluster);

        assert_eq!(cm.name_any(), "my-cluster-patroni-config");
    }

    #[test]
    fn test_patroni_config_contains_yaml() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let cm = patroni::generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        assert!(data.contains_key("patroni.yml"));
    }
}

mod patroni_rbac_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_service_account_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sa = patroni::generate_service_account(&cluster);

        assert_eq!(sa.name_any(), "my-cluster-patroni");
    }

    #[test]
    fn test_role_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let role = patroni::generate_patroni_role(&cluster);

        assert_eq!(role.name_any(), "my-cluster-patroni");
    }

    #[test]
    fn test_role_permissions() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let role = patroni::generate_patroni_role(&cluster);

        let rules = role.rules.as_ref().unwrap();
        // Should have rules for endpoints, configmaps, and pods
        assert!(rules.len() >= 3);
    }

    #[test]
    fn test_role_binding_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let rb = patroni::generate_patroni_role_binding(&cluster);

        assert_eq!(rb.name_any(), "my-cluster-patroni");
    }
}

mod service_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_primary_service_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let svc = service::generate_primary_service(&cluster);

        // Primary service uses {cluster}-primary to avoid name conflicts
        assert_eq!(svc.name_any(), "my-cluster-primary");
    }

    #[test]
    fn test_primary_service_port() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let svc = service::generate_primary_service(&cluster);

        let ports = svc.spec.as_ref().unwrap().ports.as_ref().unwrap();
        assert_eq!(ports[0].port, 5432);
        assert_eq!(ports[0].name, Some("postgresql".to_string()));
    }

    #[test]
    fn test_primary_service_selector() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let svc = service::generate_primary_service(&cluster);

        let selector = svc.spec.as_ref().unwrap().selector.as_ref().unwrap();
        // Patroni uses spilo-role label
        assert_eq!(selector.get("spilo-role"), Some(&"master".to_string()));
        assert_eq!(
            selector.get("postgres.example.com/cluster"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_replicas_service() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let svc = service::generate_replicas_service(&cluster);

        // Service is named with -repl suffix
        assert_eq!(svc.name_any(), "my-cluster-repl");

        let selector = svc.spec.as_ref().unwrap().selector.as_ref().unwrap();
        assert_eq!(selector.get("spilo-role"), Some(&"replica".to_string()));
    }

    #[test]
    fn test_headless_service() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let svc = service::generate_headless_service(&cluster);

        assert_eq!(svc.name_any(), "my-cluster-headless");

        let spec = svc.spec.as_ref().unwrap();
        assert_eq!(spec.cluster_ip, Some("None".to_string()));

        // Should have both PostgreSQL and Patroni ports
        let ports = spec.ports.as_ref().unwrap();
        let pg_port = ports.iter().find(|p| p.port == 5432);
        let patroni_port = ports.iter().find(|p| p.port == 8008);
        assert!(pg_port.is_some());
        assert!(patroni_port.is_some());
    }
}

mod secret_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_secret_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster).unwrap();

        assert_eq!(secret.name_any(), "my-cluster-credentials");
    }

    #[test]
    fn test_secret_contains_passwords() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster).unwrap();

        let string_data = secret.string_data.as_ref().unwrap();
        assert!(string_data.contains_key("POSTGRES_PASSWORD"));
        // All clusters get replication password since they all use Patroni
        assert!(string_data.contains_key("REPLICATION_PASSWORD"));
    }

    #[test]
    fn test_secret_password_length() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster).unwrap();

        let string_data = secret.string_data.as_ref().unwrap();
        let password = string_data.get("POSTGRES_PASSWORD").unwrap();
        // Password should be 32 characters
        assert_eq!(password.len(), 32);
    }

    #[test]
    fn test_secret_owner_reference() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster).unwrap();

        let owner_refs = secret.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
    }
}

mod pdb_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_pdb_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let pdb_resource = pdb::generate_pdb(&cluster);

        assert_eq!(pdb_resource.name_any(), "my-cluster-pdb");
    }

    #[test]
    fn test_pdb_single_replica_min_available() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let pdb_resource = pdb::generate_pdb(&cluster);

        let spec = pdb_resource.spec.as_ref().unwrap();
        // Single replica: min_available = 0 (allow disruption)
        assert_eq!(
            spec.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(0))
        );
    }

    #[test]
    fn test_pdb_two_replica_min_available() {
        let cluster = create_test_cluster("my-cluster", "default", 2);
        let pdb_resource = pdb::generate_pdb(&cluster);

        let spec = pdb_resource.spec.as_ref().unwrap();
        // Two replicas: min_available = 1
        assert_eq!(
            spec.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(1))
        );
    }

    #[test]
    fn test_pdb_three_replica_min_available() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let pdb_resource = pdb::generate_pdb(&cluster);

        let spec = pdb_resource.spec.as_ref().unwrap();
        // Three replicas: min_available = 2 (n-1)
        assert_eq!(
            spec.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
        );
    }

    #[test]
    fn test_pdb_owner_reference() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let pdb_resource = pdb::generate_pdb(&cluster);

        let owner_refs = pdb_resource.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
    }
}

// =============================================================================
// TLS Tests
// =============================================================================

mod tls_statefulset_tests {
    use super::*;

    #[test]
    fn test_tls_disabled_no_volumes() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let volumes = pod_spec.volumes.as_ref();

        // Without TLS, should have no volumes (or empty)
        assert!(volumes.is_none() || volumes.unwrap().is_empty());
    }

    #[test]
    fn test_tls_enabled_adds_volume() {
        let cluster = create_test_cluster_with_tls("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let volumes = pod_spec.volumes.as_ref().unwrap();

        // Should have tls-certs volume
        let tls_volume = volumes.iter().find(|v| v.name == "tls-certs");
        assert!(tls_volume.is_some(), "TLS volume should exist");

        let secret_source = tls_volume.unwrap().secret.as_ref().unwrap();
        assert_eq!(
            secret_source.secret_name,
            Some("my-tls-secret".to_string())
        );
    }

    #[test]
    fn test_tls_enabled_adds_volume_mount() {
        let cluster = create_test_cluster_with_tls("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let containers = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        let volume_mounts = containers[0].volume_mounts.as_ref().unwrap();

        // Should have tls-certs mount at /tls
        let tls_mount = volume_mounts.iter().find(|m| m.name == "tls-certs");
        assert!(tls_mount.is_some(), "TLS volume mount should exist");
        assert_eq!(tls_mount.unwrap().mount_path, "/tls");
        assert_eq!(tls_mount.unwrap().read_only, Some(true));
    }

    #[test]
    fn test_tls_enabled_adds_env_vars() {
        let cluster = create_test_cluster_with_tls("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let containers = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        let env_vars = containers[0].env.as_ref().unwrap();

        // Should have SSL_CERTIFICATE_FILE and SSL_PRIVATE_KEY_FILE env vars
        let cert_env = env_vars.iter().find(|e| e.name == "SSL_CERTIFICATE_FILE");
        let key_env = env_vars.iter().find(|e| e.name == "SSL_PRIVATE_KEY_FILE");

        assert!(cert_env.is_some(), "SSL_CERTIFICATE_FILE should be set");
        assert!(key_env.is_some(), "SSL_PRIVATE_KEY_FILE should be set");

        assert_eq!(cert_env.unwrap().value, Some("/tls/tls.crt".to_string()));
        assert_eq!(key_env.unwrap().value, Some("/tls/tls.key".to_string()));
    }

    #[test]
    fn test_tls_custom_filenames() {
        let cluster = create_test_cluster_with_tls_custom_files("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let containers = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        let env_vars = containers[0].env.as_ref().unwrap();

        // Should use custom filenames
        let cert_env = env_vars.iter().find(|e| e.name == "SSL_CERTIFICATE_FILE");
        let key_env = env_vars.iter().find(|e| e.name == "SSL_PRIVATE_KEY_FILE");
        let ca_env = env_vars.iter().find(|e| e.name == "SSL_CA_FILE");

        assert_eq!(cert_env.unwrap().value, Some("/tls/server.crt".to_string()));
        assert_eq!(key_env.unwrap().value, Some("/tls/server.key".to_string()));
        assert!(ca_env.is_some(), "SSL_CA_FILE should be set");
        // CA is in separate secret, so mounted at /tlsca
        assert_eq!(ca_env.unwrap().value, Some("/tlsca/root.crt".to_string()));
    }

    #[test]
    fn test_tls_separate_ca_secret_adds_second_volume() {
        let cluster = create_test_cluster_with_tls_custom_files("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let volumes = pod_spec.volumes.as_ref().unwrap();

        // Should have both tls-certs and tls-ca volumes
        let tls_volume = volumes.iter().find(|v| v.name == "tls-certs");
        let ca_volume = volumes.iter().find(|v| v.name == "tls-ca");

        assert!(tls_volume.is_some(), "TLS certs volume should exist");
        assert!(ca_volume.is_some(), "TLS CA volume should exist");

        let ca_secret = ca_volume.unwrap().secret.as_ref().unwrap();
        assert_eq!(ca_secret.secret_name, Some("my-ca-secret".to_string()));
    }
}

// =============================================================================
// PgBouncer Tests
// =============================================================================

mod pgbouncer_deployment_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_pgbouncer_deployment_name() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        assert_eq!(deployment.name_any(), "my-cluster-pooler");
    }

    #[test]
    fn test_pgbouncer_deployment_replicas() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let spec = deployment.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(2));
    }

    #[test]
    fn test_pgbouncer_deployment_labels() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let labels = deployment.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"pgbouncer".to_string())
        );
        assert_eq!(
            labels.get("postgres.example.com/pooler"),
            Some(&"true".to_string())
        );
        assert_eq!(
            labels.get("postgres.example.com/cluster"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_pgbouncer_deployment_owner_reference() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let owner_refs = deployment.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
        assert_eq!(owner_refs[0].name, "my-cluster");
    }

    #[test]
    fn test_pgbouncer_deployment_port() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let containers = &deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        let ports = containers[0].ports.as_ref().unwrap();

        assert_eq!(ports[0].container_port, 6432);
        assert_eq!(ports[0].name, Some("pgbouncer".to_string()));
    }

    #[test]
    fn test_pgbouncer_deployment_has_probes() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let container = &deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        assert!(
            container.readiness_probe.is_some(),
            "Readiness probe should exist"
        );
        assert!(
            container.liveness_probe.is_some(),
            "Liveness probe should exist"
        );
    }

    #[test]
    fn test_pgbouncer_deployment_security_context() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let container = &deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        let security = container.security_context.as_ref().unwrap();
        assert_eq!(security.run_as_non_root, Some(true));
        assert_eq!(security.allow_privilege_escalation, Some(false));
    }
}

mod pgbouncer_configmap_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_pgbouncer_configmap_name() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        assert_eq!(cm.name_any(), "my-cluster-pgbouncer-config");
    }

    #[test]
    fn test_pgbouncer_configmap_contains_ini() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        assert!(data.contains_key("pgbouncer.ini"));
    }

    #[test]
    fn test_pgbouncer_configmap_pool_mode() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();

        assert!(ini.contains("pool_mode = transaction"));
    }

    #[test]
    fn test_pgbouncer_configmap_connects_to_primary() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();

        // Should connect to the primary service
        assert!(ini.contains("host=my-cluster-primary"));
    }

    #[test]
    fn test_pgbouncer_configmap_owner_reference() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let owner_refs = cm.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
    }
}

mod pgbouncer_service_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_pgbouncer_service_name() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let svc = pgbouncer::generate_pgbouncer_service(&cluster);

        assert_eq!(svc.name_any(), "my-cluster-pooler");
    }

    #[test]
    fn test_pgbouncer_service_port() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let svc = pgbouncer::generate_pgbouncer_service(&cluster);

        let ports = svc.spec.as_ref().unwrap().ports.as_ref().unwrap();
        assert_eq!(ports[0].port, 6432);
        assert_eq!(ports[0].name, Some("pgbouncer".to_string()));
    }

    #[test]
    fn test_pgbouncer_service_selector() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let svc = pgbouncer::generate_pgbouncer_service(&cluster);

        let selector = svc.spec.as_ref().unwrap().selector.as_ref().unwrap();
        assert_eq!(
            selector.get("postgres.example.com/pooler"),
            Some(&"true".to_string())
        );
    }
}

mod pgbouncer_replica_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_pgbouncer_replica_configmap_connects_to_replicas() {
        let cluster = create_test_cluster_with_pgbouncer_replica("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_replica_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();

        // Should connect to the replica service
        assert!(ini.contains("host=my-cluster-repl"));
    }

    #[test]
    fn test_pgbouncer_replica_deployment_name() {
        let cluster = create_test_cluster_with_pgbouncer_replica("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_replica_deployment(&cluster);

        assert_eq!(deployment.name_any(), "my-cluster-pooler-repl");
    }

    #[test]
    fn test_pgbouncer_replica_service_name() {
        let cluster = create_test_cluster_with_pgbouncer_replica("my-cluster", "default", 3);
        let svc = pgbouncer::generate_pgbouncer_replica_service(&cluster);

        assert_eq!(svc.name_any(), "my-cluster-pooler-repl");
    }
}

mod pgbouncer_helper_tests {
    use super::*;

    #[test]
    fn test_is_pgbouncer_enabled_true() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        assert!(pgbouncer::is_pgbouncer_enabled(&cluster));
    }

    #[test]
    fn test_is_pgbouncer_enabled_false() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        assert!(!pgbouncer::is_pgbouncer_enabled(&cluster));
    }

    #[test]
    fn test_is_replica_pooler_enabled_true() {
        let cluster = create_test_cluster_with_pgbouncer_replica("my-cluster", "default", 3);
        assert!(pgbouncer::is_replica_pooler_enabled(&cluster));
    }

    #[test]
    fn test_is_replica_pooler_enabled_false() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        assert!(!pgbouncer::is_replica_pooler_enabled(&cluster));
    }
}

// =============================================================================
// Combined TLS + PgBouncer Tests
// =============================================================================

mod tls_pgbouncer_integration_tests {
    use super::*;

    fn create_cluster_with_tls_and_pgbouncer(
        name: &str,
        namespace: &str,
        replicas: i32,
    ) -> PostgresCluster {
        let mut cluster = create_test_cluster_with_pgbouncer(name, namespace, replicas);
        cluster.spec.tls = Some(TLSSpec {
            enabled: true,
            cert_secret: Some("my-tls-secret".to_string()),
            ca_secret: None,
            certificate_file: None,
            private_key_file: None,
            ca_file: None,
        });
        cluster
    }

    #[test]
    fn test_pgbouncer_with_tls_has_tls_volume() {
        let cluster = create_cluster_with_tls_and_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let pod_spec = deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let volumes = pod_spec.volumes.as_ref().unwrap();

        // PgBouncer should also have TLS volume when TLS is enabled
        let tls_volume = volumes.iter().find(|v| v.name == "tls-certs");
        assert!(
            tls_volume.is_some(),
            "PgBouncer should have TLS volume when TLS is enabled"
        );
    }

    #[test]
    fn test_pgbouncer_with_tls_has_tls_env_vars() {
        let cluster = create_cluster_with_tls_and_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        let containers = &deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers;
        let env_vars = containers[0].env.as_ref().unwrap();

        // Should have TLS-related env vars
        let tls_mode = env_vars
            .iter()
            .find(|e| e.name == "PGBOUNCER_CLIENT_TLS_SSLMODE");
        assert!(
            tls_mode.is_some(),
            "PgBouncer should have TLS sslmode env var"
        );
    }
}
