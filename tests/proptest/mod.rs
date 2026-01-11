//! Property-based tests for PostgresCluster validation and resource generation
//!
//! These tests use proptest to generate random configurations and verify that:
//! 1. Valid specs always generate valid resources without panicking
//! 2. Invalid specs are always rejected with errors, not panics
//! 3. The state machine never panics on any event sequence
//! 4. Validation is deterministic (same input = same output)

use proptest::prelude::*;

use postgres_operator::controller::state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext,
};
use postgres_operator::controller::validation::{validate_spec, validate_version_upgrade};
use postgres_operator::crd::{
    ClusterPhase, IssuerKind, IssuerRef, PgBouncerSpec, PostgresCluster, PostgresClusterSpec,
    PostgresClusterStatus, PostgresVersion, ResourceList, ResourceRequirements, StorageSpec,
    TLSSpec,
};
use postgres_operator::resources::{patroni, pdb, secret, service};

// =============================================================================
// Strategy generators for PostgreSQL cluster specs
// =============================================================================

/// Generate a valid PostgreSQL version (15, 16, 17 - supported by Spilo)
fn valid_version() -> impl Strategy<Value = PostgresVersion> {
    prop_oneof![
        Just(PostgresVersion::V15),
        Just(PostgresVersion::V16),
        Just(PostgresVersion::V17),
    ]
}

// Note: Invalid version tests are no longer needed as PostgresVersion enum
// enforces valid values at the CRD level

/// Generate a valid replica count (1-100)
fn valid_replicas() -> impl Strategy<Value = i32> {
    1..=100i32
}

/// Generate an invalid replica count
fn invalid_replicas() -> impl Strategy<Value = i32> {
    prop_oneof![
        Just(-10),
        Just(-1),
        Just(0),
        Just(101),
        Just(200),
        Just(1000),
    ]
}

/// Generate a valid storage size
fn valid_storage_size() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("1Gi".to_string()),
        Just("5Gi".to_string()),
        Just("10Gi".to_string()),
        Just("50Gi".to_string()),
        Just("100Gi".to_string()),
        Just("500Gi".to_string()),
        Just("1Ti".to_string()),
        Just("500Mi".to_string()),
    ]
}

/// Generate an invalid storage size
fn invalid_storage_size() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("".to_string()),
        Just("10".to_string()),
        Just("10GB".to_string()),
        Just("-10Gi".to_string()),
        Just("invalid".to_string()),
        Just("10 Gi".to_string()),
    ]
}

/// Generate an optional storage class
fn optional_storage_class() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        Just(None),
        Just(Some("standard".to_string())),
        Just(Some("fast-ssd".to_string())),
        Just(Some("slow-hdd".to_string())),
    ]
}

/// Generate TLS spec (cert-manager integration)
fn tls_spec() -> impl Strategy<Value = TLSSpec> {
    prop_oneof![
        // TLS disabled
        Just(TLSSpec {
            enabled: false,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        }),
        // TLS enabled with ClusterIssuer
        Just(TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "letsencrypt-prod".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        }),
        // TLS enabled with namespace Issuer
        Just(TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "my-issuer".to_string(),
                kind: IssuerKind::Issuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec!["extra.example.com".to_string()],
            duration: Some("2160h".to_string()),
            renew_before: Some("360h".to_string()),
        }),
    ]
}

/// Generate optional PgBouncer spec
fn optional_pgbouncer() -> impl Strategy<Value = Option<PgBouncerSpec>> {
    prop_oneof![
        Just(None),
        Just(Some(PgBouncerSpec {
            enabled: false,
            replicas: 2,
            pool_mode: "transaction".to_string(),
            max_db_connections: 60,
            default_pool_size: 20,
            max_client_conn: 10000,
            image: None,
            resources: None,
            enable_replica_pooler: false,
        })),
        Just(Some(PgBouncerSpec {
            enabled: true,
            replicas: 2,
            pool_mode: "transaction".to_string(),
            max_db_connections: 60,
            default_pool_size: 20,
            max_client_conn: 10000,
            image: None,
            resources: None,
            enable_replica_pooler: false,
        })),
        Just(Some(PgBouncerSpec {
            enabled: true,
            replicas: 3,
            pool_mode: "session".to_string(),
            max_db_connections: 100,
            default_pool_size: 25,
            max_client_conn: 5000,
            image: None,
            resources: None,
            enable_replica_pooler: true,
        })),
    ]
}

/// Generate optional resources
fn optional_resources() -> impl Strategy<Value = Option<ResourceRequirements>> {
    prop_oneof![
        Just(None),
        Just(Some(ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("100m".to_string()),
                memory: Some("128Mi".to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("512Mi".to_string()),
            }),
            restart_on_resize: None,
        })),
        Just(Some(ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("2".to_string()),
                memory: Some("4Gi".to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some("4".to_string()),
                memory: Some("8Gi".to_string()),
            }),
            restart_on_resize: None,
        })),
    ]
}

/// Generate a valid PostgresClusterSpec
fn valid_spec() -> impl Strategy<Value = PostgresClusterSpec> {
    (
        valid_version(),
        valid_replicas(),
        valid_storage_size(),
        optional_storage_class(),
        tls_spec(),
        optional_pgbouncer(),
        optional_resources(),
    )
        .prop_map(
            |(version, replicas, size, storage_class, tls, pgbouncer, resources)| {
                PostgresClusterSpec {
                    version,
                    replicas,
                    storage: StorageSpec {
                        size,
                        storage_class,
                    },
                    resources,
                    postgresql_params: Default::default(),
                    labels: Default::default(),
                    backup: None,
                    pgbouncer,
                    tls,
                    metrics: None,
                    service: None,
                    restore: None,
                    scaling: None,
                }
            },
        )
}

/// Generate a PostgresCluster from a spec
fn cluster_from_spec(spec: PostgresClusterSpec) -> PostgresCluster {
    PostgresCluster {
        metadata: kube::core::ObjectMeta {
            name: Some("test-cluster".to_string()),
            namespace: Some("default".to_string()),
            uid: Some("test-uid-12345".to_string()),
            generation: Some(1),
            ..Default::default()
        },
        spec,
        status: None,
    }
}

/// Generate a cluster phase
fn cluster_phase() -> impl Strategy<Value = ClusterPhase> {
    prop_oneof![
        Just(ClusterPhase::Pending),
        Just(ClusterPhase::Creating),
        Just(ClusterPhase::Running),
        Just(ClusterPhase::Updating),
        Just(ClusterPhase::Scaling),
        Just(ClusterPhase::Degraded),
        Just(ClusterPhase::Recovering),
        Just(ClusterPhase::Failed),
        Just(ClusterPhase::Deleting),
    ]
}

/// Generate a cluster event
fn cluster_event() -> impl Strategy<Value = ClusterEvent> {
    prop_oneof![
        Just(ClusterEvent::ResourcesApplied),
        Just(ClusterEvent::AllReplicasReady),
        Just(ClusterEvent::ReplicasDegraded),
        Just(ClusterEvent::SpecChanged),
        Just(ClusterEvent::ReplicaCountChanged),
        Just(ClusterEvent::ReconcileError),
        Just(ClusterEvent::DeletionRequested),
        Just(ClusterEvent::RecoveryInitiated),
        Just(ClusterEvent::RecoveryCompleted),
        Just(ClusterEvent::FullyRecovered),
    ]
}

// =============================================================================
// Property-based tests
// =============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    /// Property: Valid specs always pass validation
    #[test]
    fn prop_valid_spec_passes_validation(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        let result = validate_spec(&cluster);
        prop_assert!(result.is_ok(), "Valid spec should pass validation: {:?}", result);
    }

    /// Property: Valid specs always generate a StatefulSet without panicking
    #[test]
    fn prop_valid_spec_generates_statefulset(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        // This should not panic
        let sts = patroni::generate_patroni_statefulset(&cluster);
        prop_assert!(sts.metadata.name.is_some());
        prop_assert!(sts.spec.is_some());
    }

    /// Property: Valid specs always generate Patroni config without panicking
    #[test]
    fn prop_valid_spec_generates_patroni_config(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        let cm = patroni::generate_patroni_config(&cluster);
        prop_assert!(cm.metadata.name.is_some());
        prop_assert!(cm.data.is_some());
    }

    /// Property: Valid specs always generate services without panicking
    #[test]
    fn prop_valid_spec_generates_services(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        let primary = service::generate_primary_service(&cluster);
        let replicas = service::generate_replicas_service(&cluster);
        let headless = service::generate_headless_service(&cluster);

        prop_assert!(primary.metadata.name.is_some());
        prop_assert!(replicas.metadata.name.is_some());
        prop_assert!(headless.metadata.name.is_some());
    }

    /// Property: Valid specs always generate PDB without panicking
    #[test]
    fn prop_valid_spec_generates_pdb(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        let pdb_resource = pdb::generate_pdb(&cluster);
        prop_assert!(pdb_resource.metadata.name.is_some());
        prop_assert!(pdb_resource.spec.is_some());
    }

    /// Property: Valid specs always generate secrets without panicking
    #[test]
    fn prop_valid_spec_generates_secret(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        let result = secret::generate_credentials_secret(&cluster);
        prop_assert!(result.is_ok());
    }

    // Note: Invalid version tests are no longer needed because PostgresVersion enum
    // enforces valid values (15, 16, 17) at the CRD level. Invalid values cannot
    // be constructed at runtime.

    /// Property: Invalid replica counts are always rejected
    #[test]
    fn prop_invalid_replicas_rejected(replicas in invalid_replicas()) {
        let spec = PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas,
            storage: StorageSpec {
                size: "10Gi".to_string(),
                storage_class: None,
            },
            resources: None,
            postgresql_params: Default::default(),
            labels: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: TLSSpec::default(),
            metrics: None,
            service: None,
            restore: None,
                    scaling: None,
        };
        let cluster = cluster_from_spec(spec);
        let result = validate_spec(&cluster);
        prop_assert!(result.is_err(), "Invalid replicas should be rejected: {}", cluster.spec.replicas);
    }

    /// Property: Invalid storage sizes are always rejected
    #[test]
    fn prop_invalid_storage_rejected(size in invalid_storage_size()) {
        let spec = PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas: 1,
            storage: StorageSpec {
                size,
                storage_class: None,
            },
            resources: None,
            postgresql_params: Default::default(),
            labels: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: TLSSpec::default(),
            metrics: None,
            service: None,
            restore: None,
                    scaling: None,
        };
        let cluster = cluster_from_spec(spec);
        let result = validate_spec(&cluster);
        prop_assert!(result.is_err(), "Invalid storage should be rejected: {}", cluster.spec.storage.size);
    }

    /// Property: State machine never panics on any phase/event combination
    #[test]
    fn prop_state_machine_no_panic(phase in cluster_phase(), event in cluster_event()) {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext {
            ready_replicas: 3,
            desired_replicas: 3,
            spec_changed: false,
            error_message: None,
            retry_count: 0,
            synced_pods: 3,
            total_pods: 3,
            resize_in_progress: false,
        };

        // This should not panic, regardless of whether the transition is valid
        let _result = sm.transition(&phase, event, &ctx);
        // We don't assert on the result because some combinations are invalid,
        // but the important thing is that it doesn't panic
    }

    /// Property: Validation is deterministic
    #[test]
    fn prop_validation_deterministic(spec in valid_spec()) {
        let cluster = cluster_from_spec(spec);
        let result1 = validate_spec(&cluster);
        let result2 = validate_spec(&cluster);

        // Both should be Ok and equal
        prop_assert_eq!(result1.is_ok(), result2.is_ok());
    }

    /// Property: Version upgrades are allowed for valid paths
    #[test]
    fn prop_version_upgrade_valid(
        from_major in 10..=16i32,
        to_major in 10..=17i32
    ) {
        let from = from_major.to_string();
        let to = to_major.to_string();
        let result = validate_version_upgrade(&from, &to);

        if to_major >= from_major {
            // Upgrade or same version should be allowed
            prop_assert!(result.is_ok(), "Upgrade from {} to {} should be allowed", from, to);
        } else {
            // Downgrade should be rejected
            prop_assert!(result.is_err(), "Downgrade from {} to {} should be rejected", from, to);
        }
    }

    /// Property: PDB min_available is always <= replicas - 1 for replicas > 1
    #[test]
    fn prop_pdb_min_available_correct(replicas in 1..=20i32) {
        let spec = PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas,
            storage: StorageSpec {
                size: "10Gi".to_string(),
                storage_class: None,
            },
            resources: None,
            postgresql_params: Default::default(),
            labels: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: TLSSpec::default(),
            metrics: None,
            service: None,
            restore: None,
                    scaling: None,
        };
        let cluster = cluster_from_spec(spec);
        let pdb_resource = pdb::generate_pdb(&cluster);
        let min_available = pdb_resource.spec.as_ref().unwrap().min_available.as_ref().unwrap();

        match min_available {
            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n) => {
                if replicas == 1 {
                    prop_assert_eq!(*n, 0, "Single replica should have min_available=0");
                } else {
                    prop_assert!(*n < replicas, "min_available should be < replicas");
                    prop_assert!(*n >= 1, "min_available should be >= 1 for multi-replica");
                }
            }
            _ => prop_assert!(false, "Expected IntOrString::Int"),
        }
    }

    /// Property: StatefulSet replicas always matches spec
    #[test]
    fn prop_statefulset_replicas_match(replicas in 1..=50i32) {
        let spec = PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas,
            storage: StorageSpec {
                size: "10Gi".to_string(),
                storage_class: None,
            },
            resources: None,
            postgresql_params: Default::default(),
            labels: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: TLSSpec::default(),
            metrics: None,
            service: None,
            restore: None,
                    scaling: None,
        };
        let cluster = cluster_from_spec(spec);
        let sts = patroni::generate_patroni_statefulset(&cluster);

        prop_assert_eq!(
            sts.spec.as_ref().unwrap().replicas,
            Some(replicas),
            "StatefulSet replicas should match spec"
        );
    }
}

// =============================================================================
// Non-proptest unit tests for edge cases
// =============================================================================

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_cluster_with_all_optional_fields_none() {
        let spec = PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas: 1,
            storage: StorageSpec {
                size: "10Gi".to_string(),
                storage_class: None,
            },
            resources: None,
            postgresql_params: Default::default(),
            labels: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: TLSSpec::default(),
            metrics: None,
            service: None,
            restore: None,
            scaling: None,
        };
        let cluster = cluster_from_spec(spec);

        // All resource generation should succeed
        let sts = patroni::generate_patroni_statefulset(&cluster);
        let cm = patroni::generate_patroni_config(&cluster);
        let primary = service::generate_primary_service(&cluster);
        let pdb_resource = pdb::generate_pdb(&cluster);

        assert!(sts.metadata.name.is_some());
        assert!(cm.metadata.name.is_some());
        assert!(primary.metadata.name.is_some());
        assert!(pdb_resource.metadata.name.is_some());
    }

    #[test]
    fn test_cluster_with_status() {
        let mut cluster = cluster_from_spec(PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas: 3,
            storage: StorageSpec {
                size: "10Gi".to_string(),
                storage_class: None,
            },
            resources: None,
            postgresql_params: Default::default(),
            labels: Default::default(),
            backup: None,
            pgbouncer: None,
            tls: TLSSpec::default(),
            metrics: None,
            service: None,
            restore: None,
            scaling: None,
        });

        cluster.status = Some(PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas: 3,
            replicas: 3,
            ..Default::default()
        });

        // Resource generation should still work with status present
        let sts = patroni::generate_patroni_statefulset(&cluster);
        assert!(sts.metadata.name.is_some());
    }
}
