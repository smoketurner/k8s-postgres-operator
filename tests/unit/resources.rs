//! Unit tests for resource generators
//!
//! Tests for Patroni StatefulSet, Service, Secret, and PDB generation.
//! All PostgreSQL clusters use Patroni for consistent management.

// Use shared test fixtures
#[path = "../common/mod.rs"]
mod common;

use common::{
    PostgresClusterBuilder, create_test_cluster, create_test_cluster_with_pgbouncer,
    create_test_cluster_with_pgbouncer_replica, create_test_cluster_with_tls,
};
use postgres_operator::crd::{PgBouncerSpec, TLSSpec};
use postgres_operator::resources::{patroni, pdb, pgbouncer, secret, service};

mod patroni_statefulset_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_patroni_statefulset_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        // Patroni uses the cluster name directly
        assert_eq!(sts.name_any(), "my-cluster");
    }

    #[test]
    fn test_patroni_statefulset_single_replica() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(1));
    }

    #[test]
    fn test_patroni_statefulset_three_replicas() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(3));
    }

    #[test]
    fn test_patroni_statefulset_labels() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let labels = sts.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("postgres-operator.smoketurner.com/ha-mode"),
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let owner_refs = sts.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
        assert_eq!(owner_refs[0].name, "my-cluster");
        assert!(owner_refs[0].controller.unwrap_or(false));
    }

    #[test]
    fn test_patroni_statefulset_update_strategy() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let spec = sts.spec.as_ref().unwrap();
        let strategy = spec.update_strategy.as_ref().unwrap();
        assert_eq!(strategy.type_, Some("RollingUpdate".to_string()));
    }

    #[test]
    fn test_patroni_statefulset_service_account() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

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
            selector.get("postgres-operator.smoketurner.com/cluster"),
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
    fn test_tls_disabled_no_tls_volume() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let volumes = pod_spec.volumes.as_ref().unwrap();

        // Without TLS, should have only the tmp volume (no tls-certs volume)
        assert!(
            !volumes.iter().any(|v| v.name == "tls-certs"),
            "TLS volume should not exist when TLS is disabled"
        );
        // Should have the tmp volume for Spilo
        assert!(
            volumes.iter().any(|v| v.name == "tmp"),
            "tmp volume should exist"
        );
    }

    #[test]
    fn test_tls_enabled_adds_volume() {
        let cluster = create_test_cluster_with_tls("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let volumes = pod_spec.volumes.as_ref().unwrap();

        // Should have tls-certs volume
        let tls_volume = volumes.iter().find(|v| v.name == "tls-certs");
        assert!(tls_volume.is_some(), "TLS volume should exist");

        // cert-manager creates a secret named {cluster-name}-tls
        let secret_source = tls_volume.unwrap().secret.as_ref().unwrap();
        assert_eq!(
            secret_source.secret_name,
            Some("my-cluster-tls".to_string())
        );
    }

    #[test]
    fn test_tls_enabled_adds_volume_mount() {
        let cluster = create_test_cluster_with_tls("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

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
    fn test_tls_includes_ca_file() {
        // With cert-manager, the CA is included in the same secret as tls.crt and tls.key
        let cluster = create_test_cluster_with_tls("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

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

        // cert-manager always provides ca.crt in the same secret
        let ca_env = env_vars.iter().find(|e| e.name == "SSL_CA_FILE");
        assert!(ca_env.is_some(), "SSL_CA_FILE should be set");
        assert_eq!(ca_env.unwrap().value, Some("/tls/ca.crt".to_string()));
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
            labels.get("postgres-operator.smoketurner.com/pooler"),
            Some(&"true".to_string())
        );
        assert_eq!(
            labels.get("postgres-operator.smoketurner.com/cluster"),
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
            selector.get("postgres-operator.smoketurner.com/pooler"),
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

    #[test]
    fn test_pgbouncer_with_tls_has_tls_volume() {
        let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
            .with_tls("letsencrypt-prod")
            .with_pgbouncer()
            .build();
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
        let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
            .with_tls("letsencrypt-prod")
            .with_pgbouncer()
            .build();
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

// =============================================================================
// Replica Count Configuration Tests
// =============================================================================

mod replica_count_tests {
    use super::*;

    #[test]
    fn test_single_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(1));
    }

    #[test]
    fn test_two_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 2);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(2));
    }

    #[test]
    fn test_five_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 5);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(5));
    }

    #[test]
    fn test_ten_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 10);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(10));
    }

    #[test]
    fn test_pdb_five_replicas() {
        let cluster = create_test_cluster("my-cluster", "default", 5);
        let pdb_resource = pdb::generate_pdb(&cluster);
        let spec = pdb_resource.spec.as_ref().unwrap();
        // Five replicas: min_available = 4 (n-1)
        assert_eq!(
            spec.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(4))
        );
    }

    #[test]
    fn test_pdb_ten_replicas() {
        let cluster = create_test_cluster("my-cluster", "default", 10);
        let pdb_resource = pdb::generate_pdb(&cluster);
        let spec = pdb_resource.spec.as_ref().unwrap();
        // Ten replicas: min_available = 9 (n-1)
        assert_eq!(
            spec.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(9))
        );
    }
}

// =============================================================================
// PgBouncer Pool Mode Tests
// =============================================================================

mod pgbouncer_pool_mode_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_session_pool_mode() {
        let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
            .with_pgbouncer_mode("session")
            .build();
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);
        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();
        assert!(ini.contains("pool_mode = session"));
    }

    #[test]
    fn test_transaction_pool_mode() {
        let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
            .with_pgbouncer_mode("transaction")
            .build();
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);
        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();
        assert!(ini.contains("pool_mode = transaction"));
    }

    #[test]
    fn test_statement_pool_mode() {
        let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
            .with_pgbouncer_mode("statement")
            .build();
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);
        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();
        assert!(ini.contains("pool_mode = statement"));
    }

    #[test]
    fn test_pgbouncer_configmap_has_correct_name() {
        let cluster = PostgresClusterBuilder::ha("my-cluster", "default")
            .with_pgbouncer_mode("transaction")
            .build();
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);
        assert_eq!(cm.name_any(), "my-cluster-pgbouncer-config");
    }
}

// =============================================================================
// Resource Configuration Tests
// =============================================================================

mod resource_configuration_tests {
    use super::*;

    #[test]
    fn test_low_resources() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_resources_full("100m", "128Mi", "500m", "512Mi")
            .build();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let container = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];
        let resources = container.resources.as_ref().unwrap();

        let requests = resources.requests.as_ref().unwrap();
        assert_eq!(requests.get("cpu").unwrap().0, "100m");
        assert_eq!(requests.get("memory").unwrap().0, "128Mi");

        let limits = resources.limits.as_ref().unwrap();
        assert_eq!(limits.get("cpu").unwrap().0, "500m");
        assert_eq!(limits.get("memory").unwrap().0, "512Mi");
    }

    #[test]
    fn test_high_resources() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_resources_full("2", "4Gi", "4", "8Gi")
            .build();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let container = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];
        let resources = container.resources.as_ref().unwrap();

        let requests = resources.requests.as_ref().unwrap();
        assert_eq!(requests.get("cpu").unwrap().0, "2");
        assert_eq!(requests.get("memory").unwrap().0, "4Gi");
    }

    #[test]
    fn test_no_resources() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let container = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];
        // Without resources, should either have None or empty resources
        // This test verifies no panic occurs when resources are not set
        let _ = container.resources.as_ref();
    }
}

// =============================================================================
// Storage Class Configuration Tests
// =============================================================================

mod storage_class_tests {
    use super::*;

    #[test]
    fn test_default_storage_class() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_storage("10Gi", None)
            .build();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let vct = &sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap()[0];
        // When no storage class is specified, storageClassName should be None (uses cluster default)
        assert!(vct.spec.as_ref().unwrap().storage_class_name.is_none());
    }

    #[test]
    fn test_custom_storage_class() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_storage("10Gi", Some("fast-ssd"))
            .build();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let vct = &sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap()[0];
        assert_eq!(
            vct.spec.as_ref().unwrap().storage_class_name,
            Some("fast-ssd".to_string())
        );
    }

    #[test]
    fn test_storage_size_in_volume_claim() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_storage("100Gi", None)
            .build();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let vct = &sts
            .spec
            .as_ref()
            .unwrap()
            .volume_claim_templates
            .as_ref()
            .unwrap()[0];
        let requests = vct
            .spec
            .as_ref()
            .unwrap()
            .resources
            .as_ref()
            .unwrap()
            .requests
            .as_ref()
            .unwrap();
        assert_eq!(requests.get("storage").unwrap().0, "100Gi");
    }
}

// =============================================================================
// Full Production Configuration Tests
// =============================================================================

mod production_configuration_tests {
    use super::*;
    use postgres_operator::crd::PostgresVersion;

    /// Create a production-like cluster using the builder pattern.
    /// This demonstrates how the builder simplifies complex cluster configurations.
    fn create_production_cluster() -> postgres_operator::crd::PostgresCluster {
        let mut cluster = PostgresClusterBuilder::ha("production-db", "databases")
            .with_version(PostgresVersion::V16)
            .with_storage("100Gi", Some("fast-ssd"))
            .with_resources_full("2", "4Gi", "4", "8Gi")
            .with_tls_full(
                "production-issuer",
                vec!["db.example.com".to_string()],
                Some("2160h"),
                Some("360h"),
            )
            .with_pgbouncer_custom(3, "transaction", 100, 25, 10000)
            .with_metrics()
            .with_param("max_connections", "500")
            .with_param("shared_buffers", "1GB")
            .build();

        // Enable replica pooler for production
        if let Some(ref mut pgbouncer) = cluster.spec.pgbouncer {
            pgbouncer.enable_replica_pooler = true;
        }

        cluster
    }

    #[test]
    fn test_production_statefulset_replicas() {
        let cluster = create_production_cluster();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[test]
    fn test_production_statefulset_has_tls_volumes() {
        let cluster = create_production_cluster();
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();
        let volumes = pod_spec.volumes.as_ref().unwrap();

        // With cert-manager, there's a single TLS secret containing cert, key, and CA
        let tls_volume = volumes.iter().find(|v| v.name == "tls-certs");

        assert!(
            tls_volume.is_some(),
            "Production cluster should have TLS cert volume"
        );

        // Verify the secret name follows cert-manager naming convention
        let secret_source = tls_volume.unwrap().secret.as_ref().unwrap();
        assert_eq!(
            secret_source.secret_name,
            Some("production-db-tls".to_string())
        );
    }

    #[test]
    fn test_production_pgbouncer_deployment() {
        let cluster = create_production_cluster();
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);
        assert_eq!(deployment.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[test]
    fn test_production_has_replica_pooler() {
        let cluster = create_production_cluster();
        assert!(pgbouncer::is_replica_pooler_enabled(&cluster));

        let repl_deployment = pgbouncer::generate_pgbouncer_replica_deployment(&cluster);
        assert!(repl_deployment.metadata.name.is_some());
    }

    #[test]
    fn test_production_pdb_min_available() {
        let cluster = create_production_cluster();
        let pdb_resource = pdb::generate_pdb(&cluster);
        let spec = pdb_resource.spec.as_ref().unwrap();
        // 3 replicas: min_available = 2
        assert_eq!(
            spec.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
        );
    }

    #[test]
    fn test_production_config_has_postgresql_params() {
        let cluster = create_production_cluster();
        let cm = patroni::generate_patroni_config(&cluster);
        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        // The PostgreSQL params should be included in the Patroni config
        assert!(yaml.contains("max_connections") || yaml.contains("parameters"));
    }
}

// =============================================================================
// Panic Prevention Tests for Resource Generation
// =============================================================================

mod resource_panic_prevention_tests {
    use super::*;
    use postgres_operator::crd::PostgresClusterStatus;

    #[test]
    fn test_generate_statefulset_with_nil_optional_fields() {
        let mut cluster = create_test_cluster("my-cluster", "default", 1);
        cluster.spec.resources = None;
        cluster.spec.postgresql_params = Default::default();
        cluster.spec.tls = TLSSpec::default(); // TLS with default values (enabled=true, no issuer)
        cluster.spec.pgbouncer = None;
        cluster.spec.metrics = None;
        cluster.spec.service = None;
        cluster.spec.backup = None;
        cluster.status = None;

        // Should not panic
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert!(sts.metadata.name.is_some());
    }

    #[test]
    fn test_generate_config_with_empty_params() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        assert!(cluster.spec.postgresql_params.is_empty());

        // Should not panic with empty params
        let cm = patroni::generate_patroni_config(&cluster);
        assert!(cm.data.is_some());
    }

    #[test]
    fn test_generate_services_with_nil_service_spec() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        assert!(cluster.spec.service.is_none());

        // Should not panic
        let primary = service::generate_primary_service(&cluster);
        let replicas = service::generate_replicas_service(&cluster);
        let headless = service::generate_headless_service(&cluster);

        assert!(primary.metadata.name.is_some());
        assert!(replicas.metadata.name.is_some());
        assert!(headless.metadata.name.is_some());
    }

    #[test]
    fn test_generate_pdb_with_status() {
        let mut cluster = create_test_cluster("my-cluster", "default", 3);
        cluster.status = Some(PostgresClusterStatus {
            phase: postgres_operator::crd::ClusterPhase::Running,
            ready_replicas: 3,
            replicas: 3,
            ..Default::default()
        });

        // Should not panic
        let pdb_resource = pdb::generate_pdb(&cluster);
        assert!(pdb_resource.metadata.name.is_some());
    }

    #[test]
    fn test_generate_secret_always_succeeds() {
        let cluster = create_test_cluster("my-cluster", "default", 1);

        // Secret generation should always succeed for valid cluster
        let result = secret::generate_credentials_secret(&cluster);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pgbouncer_not_generated_when_disabled() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        assert!(!pgbouncer::is_pgbouncer_enabled(&cluster));
    }

    #[test]
    fn test_tls_disabled_no_volumes() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();

        // Without TLS, should have no volumes or empty volumes
        if let Some(volumes) = &pod_spec.volumes {
            let tls_volume = volumes.iter().find(|v| v.name == "tls-certs");
            assert!(
                tls_volume.is_none(),
                "Should not have TLS volume when TLS is disabled"
            );
        }
    }
}

/// Tests for Kubernetes 1.35+ resizePolicy feature (KEP-1287)
mod resize_policy_tests {
    use super::*;
    use postgres_operator::crd::{ResourceList, ResourceRequirements};

    #[test]
    fn test_add_resize_policy_to_statefulset_in_place() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        // Apply resize policy for in-place resize (restart_on_resize = false)
        let sts_with_policy = patroni::add_resize_policy_to_statefulset(sts, false);

        // Verify the StatefulSet still has valid structure
        assert!(sts_with_policy.spec.is_some());
        let spec = sts_with_policy.spec.as_ref().unwrap();
        assert!(spec.template.spec.is_some());

        // Serialize to JSON and verify resizePolicy was added
        let sts_json = serde_json::to_value(&sts_with_policy).unwrap();
        let containers = sts_json
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .expect("Should have containers");

        // Check that resizePolicy was added to the first container
        let resize_policy = containers[0].get("resizePolicy");
        assert!(resize_policy.is_some(), "resizePolicy should be present");

        let policy_array = resize_policy.unwrap().as_array().unwrap();
        assert_eq!(policy_array.len(), 2);

        // Check CPU policy
        let cpu_policy = policy_array
            .iter()
            .find(|p| p.get("resourceName").and_then(|r| r.as_str()) == Some("cpu"))
            .expect("Should have CPU policy");
        assert_eq!(
            cpu_policy.get("restartPolicy").and_then(|r| r.as_str()),
            Some("NotRequired")
        );

        // Check memory policy
        let memory_policy = policy_array
            .iter()
            .find(|p| p.get("resourceName").and_then(|r| r.as_str()) == Some("memory"))
            .expect("Should have memory policy");
        assert_eq!(
            memory_policy.get("restartPolicy").and_then(|r| r.as_str()),
            Some("NotRequired")
        );
    }

    #[test]
    fn test_add_resize_policy_to_statefulset_restart() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        // Apply resize policy for restart on resize (restart_on_resize = true)
        let sts_with_policy = patroni::add_resize_policy_to_statefulset(sts, true);

        // Serialize to JSON and verify resizePolicy was added with RestartContainer
        let sts_json = serde_json::to_value(&sts_with_policy).unwrap();
        let containers = sts_json
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .expect("Should have containers");

        let resize_policy = containers[0]
            .get("resizePolicy")
            .and_then(|p| p.as_array())
            .expect("Should have resizePolicy");

        // Check CPU policy
        let cpu_policy = resize_policy
            .iter()
            .find(|p| p.get("resourceName").and_then(|r| r.as_str()) == Some("cpu"))
            .expect("Should have CPU policy");
        assert_eq!(
            cpu_policy.get("restartPolicy").and_then(|r| r.as_str()),
            Some("RestartContainer")
        );

        // Check memory policy
        let memory_policy = resize_policy
            .iter()
            .find(|p| p.get("resourceName").and_then(|r| r.as_str()) == Some("memory"))
            .expect("Should have memory policy");
        assert_eq!(
            memory_policy.get("restartPolicy").and_then(|r| r.as_str()),
            Some("RestartContainer")
        );
    }

    #[test]
    fn test_add_resize_policy_preserves_statefulset_fields() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        // Store original values
        let original_name = sts.metadata.name.clone();
        let original_replicas = sts.spec.as_ref().and_then(|s| s.replicas);

        // Apply resize policy
        let sts_with_policy = patroni::add_resize_policy_to_statefulset(sts, false);

        // Verify original fields are preserved
        assert_eq!(sts_with_policy.metadata.name, original_name);
        assert_eq!(
            sts_with_policy.spec.as_ref().and_then(|s| s.replicas),
            original_replicas
        );
    }

    #[test]
    fn test_add_resize_policy_to_deployment_always_in_place() {
        let mut cluster = create_test_cluster("my-cluster", "default", 3);
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

        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        // Apply resize policy - PgBouncer uses NotRequired by default (in-place resize)
        let deployment_with_policy = pgbouncer::add_resize_policy_to_deployment(deployment, false);

        // Serialize to JSON and verify resizePolicy was added
        let deployment_json = serde_json::to_value(&deployment_with_policy).unwrap();
        let containers = deployment_json
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .expect("Should have containers");

        let resize_policy = containers[0]
            .get("resizePolicy")
            .and_then(|p| p.as_array())
            .expect("Should have resizePolicy");

        // PgBouncer should always use NotRequired (in-place)
        for policy in resize_policy {
            assert_eq!(
                policy.get("restartPolicy").and_then(|r| r.as_str()),
                Some("NotRequired"),
                "PgBouncer should always use NotRequired policy"
            );
        }
    }

    #[test]
    fn test_add_resize_policy_preserves_deployment_fields() {
        let mut cluster = create_test_cluster("my-cluster", "default", 3);
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

        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster);

        // Store original values
        let original_name = deployment.metadata.name.clone();
        let original_replicas = deployment.spec.as_ref().and_then(|s| s.replicas);

        // Apply resize policy (with restart_on_resize=false for in-place resize)
        let deployment_with_policy = pgbouncer::add_resize_policy_to_deployment(deployment, false);

        // Verify original fields are preserved
        assert_eq!(deployment_with_policy.metadata.name, original_name);
        assert_eq!(
            deployment_with_policy
                .spec
                .as_ref()
                .and_then(|s| s.replicas),
            original_replicas
        );
    }

    #[test]
    fn test_resize_policy_with_resources() {
        let mut cluster = create_test_cluster("my-cluster", "default", 3);
        cluster.spec.resources = Some(ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("1Gi".to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some("2".to_string()),
                memory: Some("4Gi".to_string()),
            }),
            restart_on_resize: Some(false),
        });

        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let sts_with_policy = patroni::add_resize_policy_to_statefulset(
            sts,
            cluster
                .spec
                .resources
                .as_ref()
                .and_then(|r| r.restart_on_resize)
                .unwrap_or(false),
        );

        // Verify container still has resources
        let container = &sts_with_policy
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        assert!(container.resources.is_some());
        let resources = container.resources.as_ref().unwrap();
        assert!(resources.requests.is_some());
        assert!(resources.limits.is_some());
    }

    #[test]
    fn test_resize_policy_restart_on_resize_from_spec() {
        let mut cluster = create_test_cluster("my-cluster", "default", 3);
        cluster.spec.resources = Some(ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("1".to_string()),
                memory: Some("2Gi".to_string()),
            }),
            limits: None,
            restart_on_resize: Some(true), // Explicitly request restart on resize
        });

        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        let restart_on_resize = cluster
            .spec
            .resources
            .as_ref()
            .and_then(|r| r.restart_on_resize)
            .unwrap_or(false);

        let sts_with_policy = patroni::add_resize_policy_to_statefulset(sts, restart_on_resize);

        // Verify RestartContainer policy was applied
        let sts_json = serde_json::to_value(&sts_with_policy).unwrap();
        let resize_policy = sts_json
            .get("spec")
            .and_then(|s| s.get("template"))
            .and_then(|t| t.get("spec"))
            .and_then(|s| s.get("containers"))
            .and_then(|c| c.as_array())
            .and_then(|containers| containers.first())
            .and_then(|c| c.get("resizePolicy"))
            .and_then(|p| p.as_array())
            .expect("Should have resizePolicy");

        for policy in resize_policy {
            assert_eq!(
                policy.get("restartPolicy").and_then(|r| r.as_str()),
                Some("RestartContainer")
            );
        }
    }
}

// =============================================================================
// Spilo Config Tests (SPILO_CONFIGURATION env var content)
// =============================================================================

mod spilo_config_tests {
    use super::*;

    /// Helper to get the spilo-config.yml content from the ConfigMap
    fn get_spilo_config(cluster: &postgres_operator::crd::PostgresCluster) -> String {
        let cm = patroni::generate_patroni_config(cluster);
        let data = cm.data.as_ref().unwrap();
        data.get("spilo-config.yml").unwrap().clone()
    }

    /// Helper to convert a YAML value to a string, handling both string and numeric types
    fn yaml_value_to_string(value: Option<&serde_yaml::Value>) -> Option<String> {
        value.map(|v| match v {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            serde_yaml::Value::Bool(b) => b.to_string(),
            _ => format!("{:?}", v),
        })
    }

    #[test]
    fn test_spilo_config_is_valid_yaml() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        // Should parse as valid YAML
        let parsed: Result<serde_yaml::Value, _> = serde_yaml::from_str(&config);
        assert!(
            parsed.is_ok(),
            "Spilo config should be valid YAML: {}",
            config
        );
    }

    #[test]
    fn test_spilo_config_has_bootstrap_section() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        assert!(
            parsed.get("bootstrap").is_some(),
            "Should have bootstrap section"
        );
        assert!(
            parsed.get("bootstrap").unwrap().get("dcs").is_some(),
            "Should have bootstrap.dcs section"
        );
        assert!(
            parsed
                .get("bootstrap")
                .unwrap()
                .get("dcs")
                .unwrap()
                .get("postgresql")
                .is_some(),
            "Should have bootstrap.dcs.postgresql section"
        );
        assert!(
            parsed
                .get("bootstrap")
                .unwrap()
                .get("dcs")
                .unwrap()
                .get("postgresql")
                .unwrap()
                .get("parameters")
                .is_some(),
            "Should have bootstrap.dcs.postgresql.parameters section"
        );
    }

    #[test]
    fn test_spilo_config_has_postgresql_section() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        assert!(
            parsed.get("postgresql").is_some(),
            "Should have postgresql section"
        );
        assert!(
            parsed
                .get("postgresql")
                .unwrap()
                .get("parameters")
                .is_some(),
            "Should have postgresql.parameters section"
        );
    }

    #[test]
    fn test_spilo_config_has_default_wal_level_logical() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        let params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        assert_eq!(
            params.get("wal_level").and_then(|v| v.as_str()),
            Some("logical"),
            "wal_level should be logical by default"
        );
    }

    #[test]
    fn test_spilo_config_has_default_max_connections() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        let params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        assert_eq!(
            yaml_value_to_string(params.get("max_connections")),
            Some("100".to_string()),
            "max_connections should be 100 by default"
        );
    }

    #[test]
    fn test_spilo_config_has_default_shared_buffers() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        let params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        assert_eq!(
            params.get("shared_buffers").and_then(|v| v.as_str()),
            Some("128MB"),
            "shared_buffers should be 128MB by default"
        );
    }

    #[test]
    fn test_spilo_config_user_params_override_defaults() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_param("max_connections", "500")
            .with_param("shared_buffers", "1GB")
            .build();
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        let params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        assert_eq!(
            yaml_value_to_string(params.get("max_connections")),
            Some("500".to_string()),
            "max_connections should be overridden to 500"
        );
        assert_eq!(
            yaml_value_to_string(params.get("shared_buffers")),
            Some("1GB".to_string()),
            "shared_buffers should be overridden to 1GB"
        );
        // wal_level should still be logical (not overridden)
        assert_eq!(
            yaml_value_to_string(params.get("wal_level")),
            Some("logical".to_string()),
            "wal_level should still be logical"
        );
    }

    #[test]
    fn test_spilo_config_user_can_add_custom_params() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_param("work_mem", "256MB")
            .with_param("maintenance_work_mem", "512MB")
            .build();
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();
        let params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        assert_eq!(
            yaml_value_to_string(params.get("work_mem")),
            Some("256MB".to_string()),
            "work_mem should be set"
        );
        assert_eq!(
            yaml_value_to_string(params.get("maintenance_work_mem")),
            Some("512MB".to_string()),
            "maintenance_work_mem should be set"
        );
    }

    #[test]
    fn test_spilo_config_bootstrap_and_postgresql_params_match() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_param("max_connections", "200")
            .build();
        let config = get_spilo_config(&cluster);

        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();

        let bootstrap_params = parsed
            .get("bootstrap")
            .unwrap()
            .get("dcs")
            .unwrap()
            .get("postgresql")
            .unwrap()
            .get("parameters")
            .unwrap();

        let postgresql_params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        // Both sections should have the same max_connections value
        assert_eq!(
            yaml_value_to_string(bootstrap_params.get("max_connections")),
            Some("200".to_string()),
            "bootstrap params should have max_connections=200"
        );
        assert_eq!(
            yaml_value_to_string(postgresql_params.get("max_connections")),
            Some("200".to_string()),
            "postgresql params should have max_connections=200"
        );
    }

    #[test]
    fn test_spilo_config_all_default_params_have_correct_values() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        assert!(cluster.spec.postgresql_params.is_empty());

        let config = get_spilo_config(&cluster);
        let parsed: serde_yaml::Value = serde_yaml::from_str(&config).unwrap();

        let params = parsed.get("postgresql").unwrap().get("parameters").unwrap();

        // Verify all default parameter values match DEFAULT_POSTGRESQL_PARAMS
        assert_eq!(
            yaml_value_to_string(params.get("max_connections")),
            Some("100".to_string()),
            "max_connections default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("shared_buffers")),
            Some("128MB".to_string()),
            "shared_buffers default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("wal_level")),
            Some("logical".to_string()),
            "wal_level default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("hot_standby")),
            Some("on".to_string()),
            "hot_standby default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("max_wal_senders")),
            Some("10".to_string()),
            "max_wal_senders default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("max_replication_slots")),
            Some("10".to_string()),
            "max_replication_slots default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("wal_keep_size")),
            Some("1GB".to_string()),
            "wal_keep_size default"
        );
        assert_eq!(
            yaml_value_to_string(params.get("hot_standby_feedback")),
            Some("on".to_string()),
            "hot_standby_feedback default"
        );
    }
}
