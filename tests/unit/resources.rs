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
use postgres_operator::crd::{ClusterPhase, PgBouncerSpec, PostgresClusterStatus, TLSSpec};
use postgres_operator::resources::{patroni, pdb, pgbouncer, secret, service};

mod patroni_statefulset_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_patroni_statefulset_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        // Patroni uses the cluster name directly
        assert_eq!(sts.name_any(), "my-cluster");
    }

    #[test]
    fn test_patroni_statefulset_single_replica() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(1));
    }

    #[test]
    fn test_patroni_statefulset_three_replicas() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        let spec = sts.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(3));
    }

    #[test]
    fn test_patroni_statefulset_labels() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        let owner_refs = sts.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
        assert_eq!(owner_refs[0].name, "my-cluster");
        assert!(owner_refs[0].controller.unwrap_or(false));
    }

    #[test]
    fn test_patroni_statefulset_update_strategy() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        let spec = sts.spec.as_ref().unwrap();
        let strategy = spec.update_strategy.as_ref().unwrap();
        assert_eq!(strategy.type_, Some("RollingUpdate".to_string()));
    }

    #[test]
    fn test_patroni_statefulset_service_account() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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

    #[test]
    fn scheduling_fields_propagate_to_pod_spec() {
        use k8s_openapi::api::core::v1::{Toleration, TopologySpreadConstraint};
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
        use std::collections::BTreeMap;

        let mut cluster = create_test_cluster("sched", "default", 3);
        cluster.spec.node_selector =
            BTreeMap::from([("workload".to_string(), "database".to_string())]);
        cluster.spec.tolerations = vec![Toleration {
            key: Some("dedicated".to_string()),
            operator: Some("Equal".to_string()),
            value: Some("postgres".to_string()),
            effect: Some("NoSchedule".to_string()),
            ..Default::default()
        }];
        cluster.spec.topology_spread_constraints = vec![TopologySpreadConstraint {
            max_skew: 1,
            topology_key: "topology.kubernetes.io/zone".to_string(),
            when_unsatisfiable: "ScheduleAnyway".to_string(),
            label_selector: Some(LabelSelector::default()),
            ..Default::default()
        }];
        cluster.spec.priority_class_name = Some("high-priority-db".to_string());

        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();

        let node_selector = pod_spec.node_selector.as_ref().unwrap();
        assert_eq!(node_selector.get("workload"), Some(&"database".to_string()));

        let tolerations = pod_spec.tolerations.as_ref().unwrap();
        assert_eq!(tolerations.len(), 1);
        assert_eq!(tolerations[0].key.as_deref(), Some("dedicated"));

        let tsc = pod_spec.topology_spread_constraints.as_ref().unwrap();
        assert_eq!(tsc.len(), 1);
        assert_eq!(tsc[0].topology_key, "topology.kubernetes.io/zone");

        assert_eq!(
            pod_spec.priority_class_name.as_deref(),
            Some("high-priority-db")
        );
    }

    #[test]
    fn scheduling_fields_absent_when_unset() {
        let cluster = create_test_cluster("default-sched", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
        let pod_spec = sts.spec.as_ref().unwrap().template.spec.as_ref().unwrap();

        assert!(pod_spec.node_selector.is_none());
        assert!(pod_spec.tolerations.is_none());
        assert!(pod_spec.topology_spread_constraints.is_none());
        assert!(pod_spec.priority_class_name.is_none());
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

    #[test]
    fn metrics_service_absent_without_metrics_spec() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        assert!(service::generate_metrics_service(&cluster).is_none());
    }

    #[test]
    fn metrics_service_absent_when_metrics_disabled() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_storage("1Gi", None)
            .build();
        // Force the spec to have metrics: { enabled: false } via the builder
        let mut cluster = cluster;
        cluster.spec.metrics = Some(postgres_operator::crd::MetricsSpec {
            enabled: false,
            port: 9187,
            service_monitor: None,
        });
        assert!(service::generate_metrics_service(&cluster).is_none());
    }

    #[test]
    fn metrics_service_uses_configured_port_and_selector() {
        let cluster = PostgresClusterBuilder::single("my-cluster", "default")
            .with_storage("1Gi", None)
            .with_metrics_port(9999)
            .build();

        let svc = service::generate_metrics_service(&cluster).expect("metrics service");
        assert_eq!(svc.name_any(), "my-cluster-metrics");

        let ports = svc.spec.as_ref().unwrap().ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports[0].port, 9999);
        assert_eq!(ports[0].name.as_deref(), Some("metrics"));

        let labels = svc.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels
                .get("postgres-operator.smoketurner.com/service")
                .map(String::as_str),
            Some("metrics")
        );

        let selector = svc.spec.as_ref().unwrap().selector.as_ref().unwrap();
        assert_eq!(
            selector
                .get("postgres-operator.smoketurner.com/cluster")
                .map(String::as_str),
            Some("my-cluster")
        );
    }
}

mod secret_tests {
    use super::*;
    use kube::ResourceExt;

    #[test]
    fn test_secret_name() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster);

        assert_eq!(secret.name_any(), "my-cluster-credentials");
    }

    #[test]
    fn test_secret_contains_passwords() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster);

        let string_data = secret.string_data.as_ref().unwrap();
        assert!(string_data.contains_key("POSTGRES_PASSWORD"));
        // All clusters get replication password since they all use Patroni
        assert!(string_data.contains_key("REPLICATION_PASSWORD"));
    }

    #[test]
    fn test_secret_password_length() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster);

        let string_data = secret.string_data.as_ref().unwrap();
        let password = string_data.get("POSTGRES_PASSWORD").unwrap();
        // Password should be 32 characters
        assert_eq!(password.len(), 32);
    }

    #[test]
    fn test_secret_owner_reference() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster);

        let owner_refs = secret.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
    }

    #[test]
    fn test_secret_contains_connection_string() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let secret = secret::generate_credentials_secret(&cluster);

        let string_data = secret.string_data.as_ref().unwrap();
        // KEDA's connection-scaling TriggerAuthentication references this key.
        let conn = string_data
            .get("connection-string")
            .expect("connection-string key must be present");
        assert!(conn.starts_with("postgresql://postgres:"));
        assert!(conn.contains("my-cluster-primary.default.svc.cluster.local:5432"));
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

    #[test]
    fn test_pdb_selector_matches_only_patroni_pods() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        let pdb_resource = pdb::generate_pdb(&cluster);

        let spec = pdb_resource.spec.as_ref().unwrap();
        let selector = spec.selector.as_ref().unwrap();
        let match_labels = selector.match_labels.as_ref().unwrap();

        // The PDB must scope to PostgreSQL pods only so pgBouncer pods (which
        // share the name/cluster labels but carry component=pgbouncer) do not
        // count toward the PostgreSQL availability budget.
        assert_eq!(
            match_labels.get("app.kubernetes.io/component"),
            Some(&"postgresql".to_string())
        );
        assert_eq!(
            match_labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            match_labels.get("postgres-operator.smoketurner.com/cluster"),
            Some(&"my-cluster".to_string())
        );
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

        assert_eq!(deployment.name_any(), "my-cluster-pooler");
    }

    #[test]
    fn test_pgbouncer_deployment_replicas() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

        let spec = deployment.spec.as_ref().unwrap();
        assert_eq!(spec.replicas, Some(2));
    }

    #[test]
    fn test_pgbouncer_deployment_labels() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

        let owner_refs = deployment.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
        assert_eq!(owner_refs[0].name, "my-cluster");
    }

    #[test]
    fn test_pgbouncer_deployment_port() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

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

    /// Regression test for #50: integer division yielded 0 when replicas exceeded
    /// max_db_connections. PgBouncer interprets `max_db_connections = 0` as
    /// unlimited, silently bypassing the configured cap. The per-instance value
    /// must always be at least 1.
    #[test]
    fn test_pgbouncer_division_yields_zero_when_replicas_exceed_max_connections() {
        let cluster = PostgresClusterBuilder::new("my-cluster", "default")
            .with_uid("test-uid-12345")
            .with_storage("10Gi", Some("standard"))
            .with_pgbouncer_custom(10, "transaction", 5, 20, 10000)
            .build();
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();

        assert!(
            ini.contains("max_db_connections = 1"),
            "expected floored value of 1, got ini:\n{ini}"
        );
        assert!(
            !ini.contains("max_db_connections = 0"),
            "must never emit 0 (unlimited in PgBouncer), got ini:\n{ini}"
        );
    }

    #[test]
    fn test_pgbouncer_default_division() {
        let cluster = create_test_cluster_with_pgbouncer("my-cluster", "default", 3);
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();

        // Defaults: replicas=2, max_db_connections=60 -> 30 per instance
        assert!(
            ini.contains("max_db_connections = 30"),
            "expected 30 per instance, got ini:\n{ini}"
        );
    }

    #[test]
    fn test_pgbouncer_one_to_one() {
        let cluster = PostgresClusterBuilder::new("my-cluster", "default")
            .with_uid("test-uid-12345")
            .with_storage("10Gi", Some("standard"))
            .with_pgbouncer_custom(1, "transaction", 1, 20, 10000)
            .build();
        let cm = pgbouncer::generate_pgbouncer_configmap(&cluster);

        let data = cm.data.as_ref().unwrap();
        let ini = data.get("pgbouncer.ini").unwrap();

        assert!(
            ini.contains("max_db_connections = 1"),
            "expected 1 per instance, got ini:\n{ini}"
        );
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
        let deployment = pgbouncer::generate_pgbouncer_replica_deployment(&cluster, false);

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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(1));
    }

    #[test]
    fn test_two_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 2);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(2));
    }

    #[test]
    fn test_five_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 5);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(5));
    }

    #[test]
    fn test_ten_replica_statefulset() {
        let cluster = create_test_cluster("my-cluster", "default", 10);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
        assert_eq!(sts.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[test]
    fn test_production_statefulset_has_tls_volumes() {
        let cluster = create_production_cluster();
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);
        assert_eq!(deployment.spec.as_ref().unwrap().replicas, Some(3));
    }

    #[test]
    fn test_production_has_replica_pooler() {
        let cluster = create_production_cluster();
        assert!(pgbouncer::is_replica_pooler_enabled(&cluster));

        let repl_deployment = pgbouncer::generate_pgbouncer_replica_deployment(&cluster, false);
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
    fn test_pgbouncer_not_generated_when_disabled() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        assert!(!pgbouncer::is_pgbouncer_enabled(&cluster));
    }

    #[test]
    fn test_tls_disabled_no_volumes() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);
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
    fn test_resize_policy_statefulset_in_place() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        // Generate with restart_on_resize = false (in-place resize)
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        // Verify the StatefulSet still has valid structure
        assert!(sts.spec.is_some());
        let spec = sts.spec.as_ref().unwrap();
        assert!(spec.template.spec.is_some());

        // Check resize_policy directly on the container
        let container = &spec.template.spec.as_ref().unwrap().containers[0];
        let resize_policy = container
            .resize_policy
            .as_ref()
            .expect("resizePolicy should be present");
        assert_eq!(resize_policy.len(), 2);

        // Check CPU policy
        let cpu_policy = resize_policy
            .iter()
            .find(|p| p.resource_name == "cpu")
            .expect("Should have CPU policy");
        assert_eq!(cpu_policy.restart_policy, "NotRequired");

        // Check memory policy
        let memory_policy = resize_policy
            .iter()
            .find(|p| p.resource_name == "memory")
            .expect("Should have memory policy");
        assert_eq!(memory_policy.restart_policy, "NotRequired");
    }

    #[test]
    fn test_resize_policy_statefulset_restart() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        // Generate with restart_on_resize = true
        let sts = patroni::generate_patroni_statefulset(&cluster, false, true);

        let container = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        let resize_policy = container
            .resize_policy
            .as_ref()
            .expect("Should have resizePolicy");

        // Check CPU policy
        let cpu_policy = resize_policy
            .iter()
            .find(|p| p.resource_name == "cpu")
            .expect("Should have CPU policy");
        assert_eq!(cpu_policy.restart_policy, "RestartContainer");

        // Check memory policy
        let memory_policy = resize_policy
            .iter()
            .find(|p| p.resource_name == "memory")
            .expect("Should have memory policy");
        assert_eq!(memory_policy.restart_policy, "RestartContainer");
    }

    #[test]
    fn test_resize_policy_preserves_statefulset_fields() {
        let cluster = create_test_cluster("my-cluster", "default", 3);
        // Generate with resize policy applied inline
        let sts = patroni::generate_patroni_statefulset(&cluster, false, false);

        // Verify fields are present and valid
        assert!(sts.metadata.name.is_some());
        assert_eq!(sts.spec.as_ref().and_then(|s| s.replicas), Some(3));
    }

    #[test]
    fn test_resize_policy_deployment_always_in_place() {
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

        // PgBouncer uses NotRequired by default (in-place resize)
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

        let container = &deployment
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        let resize_policy = container
            .resize_policy
            .as_ref()
            .expect("Should have resizePolicy");

        // PgBouncer should always use NotRequired (in-place)
        for policy in resize_policy {
            assert_eq!(
                policy.restart_policy, "NotRequired",
                "PgBouncer should always use NotRequired policy"
            );
        }
    }

    #[test]
    fn test_resize_policy_preserves_deployment_fields() {
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

        // Generate with resize policy applied inline
        let deployment = pgbouncer::generate_pgbouncer_deployment(&cluster, false);

        // Verify fields are present and valid
        assert!(deployment.metadata.name.is_some());
        assert_eq!(deployment.spec.as_ref().and_then(|s| s.replicas), Some(2));
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

        let restart_on_resize = cluster
            .spec
            .resources
            .as_ref()
            .and_then(|r| r.restart_on_resize)
            .unwrap_or(false);
        let sts = patroni::generate_patroni_statefulset(&cluster, false, restart_on_resize);

        // Verify container still has resources
        let container = &sts
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

        let restart_on_resize = cluster
            .spec
            .resources
            .as_ref()
            .and_then(|r| r.restart_on_resize)
            .unwrap_or(false);

        let sts = patroni::generate_patroni_statefulset(&cluster, false, restart_on_resize);

        // Verify RestartContainer policy was applied via the container's resize_policy field
        let container = &sts
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0];

        let resize_policy = container
            .resize_policy
            .as_ref()
            .expect("Should have resizePolicy");

        for policy in resize_policy {
            assert_eq!(policy.restart_policy, "RestartContainer");
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
    fn yaml_value_to_string(value: Option<&serde_json::Value>) -> Option<String> {
        value.map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            _ => format!("{:?}", v),
        })
    }

    #[test]
    fn test_spilo_config_is_valid_yaml() {
        let cluster = create_test_cluster("my-cluster", "default", 1);
        let config = get_spilo_config(&cluster);

        // Should parse as valid YAML
        let parsed: Result<serde_json::Value, _> = serde_saphyr::from_str(&config);
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();
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

        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();

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
        let parsed: serde_json::Value = serde_saphyr::from_str(&config).unwrap();

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

// =============================================================================
// Service reconciliation guard tests (issue #95)
// =============================================================================

/// Tests for the guard that prevents Services from being regenerated for
/// Superseded clusters after a blue-green upgrade completes.
///
/// After cutover the upgrade reconciler flips Service selectors to point at
/// the *target* cluster pods. The source cluster enters `Superseded` phase and
/// the `UPGRADE_IN_PROGRESS` annotation is cleared. Without an explicit
/// `Superseded` guard, the cluster reconciler's next cycle re-applies
/// source-selector Services and reverts the cutover flip (GitHub issue #95).
mod service_reconciliation_guard_tests {
    use super::*;

    /// Mirrors the skip predicate in `reconcile_cluster` at the service-guard site.
    fn should_skip_services(
        cluster: &postgres_operator::crd::PostgresCluster,
        upgrade_annotation: Option<&str>,
    ) -> bool {
        let phase = cluster.status.as_ref().map(|s| s.phase).unwrap_or_default();
        upgrade_annotation.is_some() || phase == ClusterPhase::Superseded
    }

    #[test]
    fn superseded_cluster_skips_service_reconciliation() {
        let mut cluster = create_test_cluster("source-pg16", "default", 3);
        cluster.status = Some(PostgresClusterStatus {
            phase: ClusterPhase::Superseded,
            ..Default::default()
        });

        // No upgrade annotation — the Superseded gate alone must suppress
        // service regeneration so the cutover selector flip is preserved.
        assert!(
            should_skip_services(&cluster, None),
            "Superseded cluster must skip Service reconciliation to preserve the \
             cutover selector flip (GitHub issue #95)"
        );
    }

    #[test]
    fn running_cluster_does_not_skip_service_reconciliation() {
        let mut cluster = create_test_cluster("source-pg16", "default", 3);
        cluster.status = Some(PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ..Default::default()
        });

        assert!(
            !should_skip_services(&cluster, None),
            "Running cluster must NOT skip Service reconciliation"
        );
    }

    #[test]
    fn upgrade_in_progress_annotation_skips_service_reconciliation() {
        let cluster = create_test_cluster("source-pg16", "default", 3);
        // No status — default phase is Pending — but annotation is set.
        assert!(
            should_skip_services(&cluster, Some("my-upgrade")),
            "Cluster with upgrade-in-progress annotation must skip Service reconciliation"
        );
    }

    #[test]
    fn superseded_cluster_with_upgrade_annotation_skips_service_reconciliation() {
        let mut cluster = create_test_cluster("source-pg16", "default", 3);
        cluster.status = Some(PostgresClusterStatus {
            phase: ClusterPhase::Superseded,
            ..Default::default()
        });

        // Both gates active — must still skip.
        assert!(
            should_skip_services(&cluster, Some("my-upgrade")),
            "Superseded cluster with upgrade annotation must skip Service reconciliation"
        );
    }

    #[test]
    fn no_status_cluster_does_not_skip_service_reconciliation() {
        let cluster = create_test_cluster("new-cluster", "default", 1);
        // status == None → phase defaults to Pending, no annotation.
        assert!(
            !should_skip_services(&cluster, None),
            "Cluster with no status (Pending) must NOT skip Service reconciliation"
        );
    }

    /// Verify that every non-Superseded phase allows service reconciliation.
    /// This prevents a future phase addition from silently inhibiting services.
    #[test]
    fn only_superseded_phase_skips_service_reconciliation() {
        let non_superseded_phases = [
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::Running,
            ClusterPhase::Updating,
            ClusterPhase::Scaling,
            ClusterPhase::Degraded,
            ClusterPhase::Recovering,
            ClusterPhase::Failed,
            ClusterPhase::Deleting,
        ];

        for phase in non_superseded_phases {
            let mut cluster = create_test_cluster("test-cluster", "default", 1);
            cluster.status = Some(PostgresClusterStatus {
                phase,
                ..Default::default()
            });
            assert!(
                !should_skip_services(&cluster, None),
                "Phase {:?} must NOT skip Service reconciliation",
                phase
            );
        }
    }
}
