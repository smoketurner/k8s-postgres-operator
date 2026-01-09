//! Unit tests for resource generators
//!
//! Tests for Patroni StatefulSet, Service, Secret, and PDB generation.
//! All PostgreSQL clusters use Patroni for consistent management.

use postgres_operator::crd::{PostgresCluster, PostgresClusterSpec, StorageSpec};
use postgres_operator::resources::{patroni, pdb, secret, service};

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
        },
        status: None,
    }
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

        // Patroni primary service uses just the cluster name
        assert_eq!(svc.name_any(), "my-cluster");
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
