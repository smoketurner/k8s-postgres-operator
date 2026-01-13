// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

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
    ClusterPhase, ConnectionScalingMetric, CpuScalingMetric, IssuerKind, IssuerRef, PgBouncerSpec,
    PostgresCluster, PostgresClusterSpec, PostgresClusterStatus, PostgresVersion, ResourceList,
    ResourceRequirements, ScalingMetrics, ScalingSpec, StorageSpec, TLSSpec,
};
use postgres_operator::resources::{patroni, pdb, scaled_object, secret, service};

// =============================================================================
// Helper functions to reduce spec boilerplate
// =============================================================================

/// Create a minimal valid PostgresClusterSpec with sensible defaults.
/// Override fields as needed using struct update syntax: `..minimal_spec()`
fn minimal_spec() -> PostgresClusterSpec {
    PostgresClusterSpec {
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
        network_policy: None,
    }
}

/// Create a spec with the given replicas count
fn spec_with_replicas(replicas: i32) -> PostgresClusterSpec {
    PostgresClusterSpec {
        replicas,
        ..minimal_spec()
    }
}

/// Create a spec with the given storage size
fn spec_with_storage(size: String) -> PostgresClusterSpec {
    PostgresClusterSpec {
        storage: StorageSpec {
            size,
            storage_class: None,
        },
        ..minimal_spec()
    }
}

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

/// Generate an invalid replica count (shrinks toward boundary values)
fn invalid_replicas() -> impl Strategy<Value = i32> {
    prop_oneof![
        // Negative values - shrink toward -1
        (-100..=-1i32),
        // Zero
        Just(0),
        // Too large - shrink toward 101
        (101..=1000i32),
    ]
}

/// Generate a valid storage size (shrinks toward smaller values)
fn valid_storage_size() -> impl Strategy<Value = String> {
    prop_oneof![
        // Megabytes - shrink toward 100Mi
        (100..=999u32).prop_map(|n| format!("{}Mi", n)),
        // Gigabytes - shrink toward 1Gi
        (1..=100u32).prop_map(|n| format!("{}Gi", n)),
        // Terabytes - shrink toward 1Ti
        (1..=10u32).prop_map(|n| format!("{}Ti", n)),
    ]
}

/// Generate an invalid storage size (shrinks toward simpler invalid cases)
fn invalid_storage_size() -> impl Strategy<Value = String> {
    prop_oneof![
        // Empty string
        Just("".to_string()),
        // Missing unit - shrink toward "1"
        (1..=100u32).prop_map(|n| n.to_string()),
        // Wrong unit suffix - GB instead of Gi
        (1..=100u32).prop_map(|n| format!("{}GB", n)),
        // Negative values
        (1..=100u32).prop_map(|n| format!("-{}Gi", n)),
        // Space in middle
        (1..=100u32).prop_map(|n| format!("{} Gi", n)),
        // Invalid text
        "[a-z]{3,8}".prop_map(|s| s),
    ]
}

/// Generate an optional storage class (shrinks toward None or shorter names)
fn optional_storage_class() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        // None - simplest case
        2 => Just(None),
        // Generated storage class names - shrink toward shorter names
        1 => "[a-z]{3,10}(-[a-z]{2,5})?".prop_map(Some),
    ]
}

/// Generate an issuer kind
fn issuer_kind() -> impl Strategy<Value = IssuerKind> {
    prop_oneof![Just(IssuerKind::ClusterIssuer), Just(IssuerKind::Issuer)]
}

/// Generate an optional issuer reference (shrinks toward simpler issuer names)
fn optional_issuer_ref() -> impl Strategy<Value = Option<IssuerRef>> {
    prop_oneof![
        // No issuer - simplest
        2 => Just(None),
        // Generated issuer - shrinks toward shorter name
        1 => ("[a-z]{3,15}", issuer_kind()).prop_map(|(name, kind)| {
            Some(IssuerRef {
                name,
                kind,
                group: "cert-manager.io".to_string(),
            })
        }),
    ]
}

/// Generate additional DNS names (shrinks toward empty vec or fewer/shorter names)
fn additional_dns_names() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-z]{2,8}\\.[a-z]{2,4}", 0..3)
}

/// Generate optional duration string (shrinks toward None or smaller durations)
fn optional_duration() -> impl Strategy<Value = Option<String>> {
    prop_oneof![
        3 => Just(None),
        1 => (1..=8760u32).prop_map(|h| Some(format!("{}h", h))),
    ]
}

/// Generate TLS spec (cert-manager integration) with shrinkable components
fn tls_spec() -> impl Strategy<Value = TLSSpec> {
    prop_oneof![
        // TLS disabled - simplest case, most weight
        3 => Just(TLSSpec {
            enabled: false,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        }),
        // TLS enabled with generated issuer - components can shrink independently
        1 => (optional_issuer_ref(), additional_dns_names(), optional_duration(), optional_duration())
            .prop_map(|(issuer_ref, dns_names, duration, renew_before)| TLSSpec {
                enabled: true,
                issuer_ref,
                additional_dns_names: dns_names,
                duration,
                renew_before,
            }),
    ]
}

/// Generate a pool mode
fn pool_mode() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("transaction".to_string()),
        Just("session".to_string()),
        Just("statement".to_string()),
    ]
}

/// Generate optional PgBouncer spec with shrinkable numeric parameters
fn optional_pgbouncer() -> impl Strategy<Value = Option<PgBouncerSpec>> {
    prop_oneof![
        // None - simplest case, highest weight
        3 => Just(None),
        // Disabled PgBouncer
        1 => Just(Some(PgBouncerSpec {
            enabled: false,
            replicas: 1,
            pool_mode: "transaction".to_string(),
            max_db_connections: 60,
            default_pool_size: 20,
            max_client_conn: 10000,
            image: None,
            resources: None,
            enable_replica_pooler: false,
        })),
        // Enabled PgBouncer with shrinkable parameters
        1 => (
            1..=5i32,           // replicas - shrink toward 1
            pool_mode(),
            20..=200i32,        // max_db_connections - shrink toward 20
            5..=50i32,          // default_pool_size - shrink toward 5
            1000..=50000i32,    // max_client_conn - shrink toward 1000
            proptest::bool::ANY, // enable_replica_pooler
        ).prop_map(|(replicas, pool_mode, max_db, pool_size, max_client, replica_pooler)| {
            Some(PgBouncerSpec {
                enabled: true,
                replicas,
                pool_mode,
                max_db_connections: max_db,
                default_pool_size: pool_size,
                max_client_conn: max_client,
                image: None,
                resources: None,
                enable_replica_pooler: replica_pooler,
            })
        }),
    ]
}

/// Generate a CPU value string (shrinks toward smaller values)
fn cpu_value() -> impl Strategy<Value = String> {
    prop_oneof![
        // Millicores - shrink toward 100m
        (100..=4000u32).prop_map(|m| format!("{}m", m)),
        // Whole cores - shrink toward 1
        (1..=8u32).prop_map(|c| c.to_string()),
    ]
}

/// Generate a memory value string (shrinks toward smaller values)
fn memory_value() -> impl Strategy<Value = String> {
    prop_oneof![
        // Megabytes - shrink toward 128Mi
        (128..=4096u32).prop_map(|m| format!("{}Mi", m)),
        // Gigabytes - shrink toward 1Gi
        (1..=16u32).prop_map(|g| format!("{}Gi", g)),
    ]
}

/// Generate optional resources with shrinkable values
fn optional_resources() -> impl Strategy<Value = Option<ResourceRequirements>> {
    prop_oneof![
        // None - simplest case, highest weight
        3 => Just(None),
        // Generated resources with shrinkable numeric components
        1 => (cpu_value(), memory_value(), cpu_value(), memory_value())
            .prop_map(|(req_cpu, req_mem, lim_cpu, lim_mem)| {
                Some(ResourceRequirements {
                    requests: Some(ResourceList {
                        cpu: Some(req_cpu),
                        memory: Some(req_mem),
                    }),
                    limits: Some(ResourceList {
                        cpu: Some(lim_cpu),
                        memory: Some(lim_mem),
                    }),
                    restart_on_resize: None,
                })
            }),
    ]
}

/// Generate a valid CPU scaling metric
fn cpu_scaling_metric() -> impl Strategy<Value = CpuScalingMetric> {
    (10..=100i32).prop_map(|target| CpuScalingMetric {
        target_utilization: target,
    })
}

/// Generate a valid connection scaling metric
fn connection_scaling_metric() -> impl Strategy<Value = ConnectionScalingMetric> {
    (10..=500i32).prop_map(|target| ConnectionScalingMetric {
        target_per_replica: target,
    })
}

/// Generate optional scaling metrics
fn scaling_metrics() -> impl Strategy<Value = Option<ScalingMetrics>> {
    prop_oneof![
        // No metrics (will default to CPU 70%)
        Just(None),
        // CPU only
        cpu_scaling_metric().prop_map(|cpu| {
            Some(ScalingMetrics {
                cpu: Some(cpu),
                connections: None,
            })
        }),
        // Connections only
        connection_scaling_metric().prop_map(|conn| {
            Some(ScalingMetrics {
                cpu: None,
                connections: Some(conn),
            })
        }),
        // Both CPU and connections
        (cpu_scaling_metric(), connection_scaling_metric()).prop_map(|(cpu, conn)| {
            Some(ScalingMetrics {
                cpu: Some(cpu),
                connections: Some(conn),
            })
        }),
    ]
}

/// Generate a valid scaling spec with valid constraints (min <= base <= max)
fn valid_scaling_spec(base_replicas: i32) -> impl Strategy<Value = Option<ScalingSpec>> {
    prop_oneof![
        // No scaling
        Just(None),
        // Scaling disabled (max == base, no room to scale)
        Just(Some(ScalingSpec {
            min_replicas: Some(base_replicas),
            max_replicas: base_replicas,
            metrics: None,
            replication_lag_threshold: "30s".to_string(),
            ..Default::default()
        })),
        // Valid scaling range with headroom
        scaling_metrics().prop_map(move |metrics| {
            // max_replicas must be > base_replicas for scaling to be enabled
            Some(ScalingSpec {
                min_replicas: Some(base_replicas),
                max_replicas: base_replicas + 5, // Room to scale up
                metrics,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            })
        }),
        // Different min than base
        scaling_metrics().prop_map(move |metrics| {
            let min = std::cmp::max(1, base_replicas - 1);
            Some(ScalingSpec {
                min_replicas: Some(min),
                max_replicas: base_replicas + 10,
                metrics,
                replication_lag_threshold: "1m".to_string(),
                ..Default::default()
            })
        }),
    ]
}

/// Generate a valid PostgresClusterSpec
fn valid_spec() -> impl Strategy<Value = PostgresClusterSpec> {
    // First generate replicas, then use it for scaling spec
    valid_replicas().prop_flat_map(|replicas| {
        (
            valid_version(),
            Just(replicas),
            valid_storage_size(),
            optional_storage_class(),
            tls_spec(),
            optional_pgbouncer(),
            optional_resources(),
            valid_scaling_spec(replicas),
        )
            .prop_map(
                |(version, replicas, size, storage_class, tls, pgbouncer, resources, scaling)| {
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
                        scaling,
                        network_policy: None,
                    }
                },
            )
    })
}

/// Generate a valid PostgresClusterSpec with scaling enabled
fn valid_spec_with_scaling() -> impl Strategy<Value = PostgresClusterSpec> {
    // Generate replicas first, then scaling that enables KEDA
    (2..=10i32).prop_flat_map(|replicas| {
        (
            valid_version(),
            Just(replicas),
            valid_storage_size(),
            optional_storage_class(),
            tls_spec(),
            optional_pgbouncer(),
            optional_resources(),
            scaling_metrics(),
        )
            .prop_map(
                move |(
                    version,
                    replicas,
                    size,
                    storage_class,
                    tls,
                    pgbouncer,
                    resources,
                    metrics,
                )| {
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
                        scaling: Some(ScalingSpec {
                            min_replicas: Some(replicas),
                            max_replicas: replicas + 5,
                            metrics,
                            replication_lag_threshold: "30s".to_string(),
                            ..Default::default()
                        }),
                        network_policy: None,
                    }
                },
            )
    })
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
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
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
        let spec = spec_with_replicas(replicas);
        let cluster = cluster_from_spec(spec);
        let result = validate_spec(&cluster);
        prop_assert!(result.is_err(), "Invalid replicas should be rejected: {}", cluster.spec.replicas);
    }

    /// Property: Invalid storage sizes are always rejected
    #[test]
    fn prop_invalid_storage_rejected(size in invalid_storage_size()) {
        let spec = spec_with_storage(size);
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
        let spec = spec_with_replicas(replicas);
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
        let spec = spec_with_replicas(replicas);
        let cluster = cluster_from_spec(spec);
        let sts = patroni::generate_patroni_statefulset(&cluster, false);

        prop_assert_eq!(
            sts.spec.as_ref().unwrap().replicas,
            Some(replicas),
            "StatefulSet replicas should match spec"
        );
    }

    // =========================================================================
    // Scaling property tests
    // =========================================================================

    /// Property: ScaledObject is generated only when maxReplicas > replicas
    #[test]
    fn prop_scaledobject_generated_when_headroom_exists(spec in valid_spec_with_scaling()) {
        let cluster = cluster_from_spec(spec);
        let obj = scaled_object::generate_scaled_object(&cluster);

        // With scaling enabled and headroom, ScaledObject should be generated
        prop_assert!(obj.is_some(), "ScaledObject should be generated when scaling is enabled with headroom");

        let obj = obj.unwrap();
        prop_assert!(obj.metadata.name.is_some());
    }

    /// Property: ScaledObject is NOT generated when no scaling or no headroom
    #[test]
    fn prop_no_scaledobject_without_scaling(replicas in 1..=10i32) {
        let spec = spec_with_replicas(replicas);
        let cluster = cluster_from_spec(spec);
        let obj = scaled_object::generate_scaled_object(&cluster);

        prop_assert!(obj.is_none(), "ScaledObject should not be generated without scaling config");
    }

    /// Property: ScaledObject is NOT generated when max_replicas <= replicas (no headroom)
    #[test]
    fn prop_no_scaledobject_without_headroom(replicas in 1..=10i32) {
        let spec = PostgresClusterSpec {
            replicas,
            scaling: Some(ScalingSpec {
                min_replicas: Some(replicas),
                max_replicas: replicas, // No headroom
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
            ..minimal_spec()
        };
        let cluster = cluster_from_spec(spec);
        let obj = scaled_object::generate_scaled_object(&cluster);

        prop_assert!(obj.is_none(), "ScaledObject should not be generated without scaling headroom");
    }

    /// Property: TriggerAuthentication is generated only with connection-based scaling
    #[test]
    fn prop_trigger_auth_with_connection_scaling(
        replicas in 2..=10i32,
        target_connections in 10..=200i32
    ) {
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
            scaling: Some(ScalingSpec {
                min_replicas: Some(replicas),
                max_replicas: replicas + 5,
                metrics: Some(ScalingMetrics {
                    cpu: None,
                    connections: Some(ConnectionScalingMetric {
                        target_per_replica: target_connections,
                    }),
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
            network_policy: None,
        };
        let cluster = cluster_from_spec(spec);
        let obj = scaled_object::generate_trigger_auth(&cluster);

        prop_assert!(obj.is_some(), "TriggerAuthentication should be generated with connection scaling");
    }

    /// Property: TriggerAuthentication is NOT generated with CPU-only scaling
    #[test]
    fn prop_no_trigger_auth_with_cpu_only(
        replicas in 2..=10i32,
        target_cpu in 50..=90i32
    ) {
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
            scaling: Some(ScalingSpec {
                min_replicas: Some(replicas),
                max_replicas: replicas + 5,
                metrics: Some(ScalingMetrics {
                    cpu: Some(CpuScalingMetric {
                        target_utilization: target_cpu,
                    }),
                    connections: None,
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
            network_policy: None,
        };
        let cluster = cluster_from_spec(spec);
        let obj = scaled_object::generate_trigger_auth(&cluster);

        prop_assert!(obj.is_none(), "TriggerAuthentication should not be generated with CPU-only scaling");
    }

    /// Property: is_keda_managing_replicas returns true iff maxReplicas > replicas
    #[test]
    fn prop_keda_managing_replicas_correct(
        replicas in 1..=10i32,
        max_delta in 0..=10i32
    ) {
        let max_replicas = replicas + max_delta;
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
            scaling: Some(ScalingSpec {
                min_replicas: Some(replicas),
                max_replicas,
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
            network_policy: None,
        };
        let cluster = cluster_from_spec(spec);
        let is_managing = scaled_object::is_keda_managing_replicas(&cluster);

        if max_replicas > replicas {
            prop_assert!(is_managing, "KEDA should manage replicas when max > base");
        } else {
            prop_assert!(!is_managing, "KEDA should not manage when max <= base");
        }
    }

    /// Property: StatefulSet has no replicas field when KEDA is managing
    #[test]
    fn prop_statefulset_no_replicas_when_keda_manages(spec in valid_spec_with_scaling()) {
        let cluster = cluster_from_spec(spec.clone());

        // When KEDA manages replicas, StatefulSet should be generated with keda_manages=true
        if scaled_object::is_keda_managing_replicas(&cluster) {
            let sts = patroni::generate_patroni_statefulset(&cluster, true);
            prop_assert!(
                sts.spec.as_ref().unwrap().replicas.is_none(),
                "StatefulSet should have no replicas when KEDA manages"
            );
        }
    }

    /// Property: ScaledObject triggers count matches metrics configuration
    #[test]
    fn prop_scaledobject_trigger_count_matches_metrics(spec in valid_spec_with_scaling()) {
        let cluster = cluster_from_spec(spec.clone());
        let obj = scaled_object::generate_scaled_object(&cluster);

        if let Some(obj) = obj {
            let spec_json: serde_json::Value = obj.data["spec"].clone();
            let triggers = spec_json["triggers"].as_array().unwrap();

            let scaling = cluster.spec.scaling.as_ref().unwrap();
            let metrics = scaling.metrics.as_ref();

            let expected_triggers = match metrics {
                None => 1, // Default CPU trigger
                Some(m) => {
                    let cpu_count = if m.cpu.is_some() { 1 } else { 0 };
                    let conn_count = if m.connections.is_some() { 1 } else { 0 };
                    if cpu_count + conn_count == 0 { 1 } else { cpu_count + conn_count }
                }
            };

            prop_assert_eq!(
                triggers.len(),
                expected_triggers,
                "Trigger count should match metrics configuration"
            );
        }
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
            network_policy: None,
        };
        let cluster = cluster_from_spec(spec);

        // All resource generation should succeed
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
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
            network_policy: None,
        });

        cluster.status = Some(PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas: 3,
            replicas: 3,
            ..Default::default()
        });

        // Resource generation should still work with status present
        let sts = patroni::generate_patroni_statefulset(&cluster, false);
        assert!(sts.metadata.name.is_some());
    }
}

// =============================================================================
// NetworkPolicy Property Tests
// =============================================================================

mod network_policy_tests {
    use super::*;
    use postgres_operator::crd::{
        LabelSelectorConfig, LabelSelectorRequirement, NetworkPolicyPeer as CrdNetworkPolicyPeer,
        NetworkPolicySpec as CrdNetworkPolicySpec,
    };
    use postgres_operator::resources::network_policy;

    /// Generate an optional NetworkPolicySpec
    fn optional_network_policy() -> impl Strategy<Value = Option<CrdNetworkPolicySpec>> {
        prop_oneof![
            Just(None),
            // No external access, no allowFrom
            Just(Some(CrdNetworkPolicySpec {
                allow_external_access: false,
                allow_from: vec![],
            })),
            // External access enabled
            Just(Some(CrdNetworkPolicySpec {
                allow_external_access: true,
                allow_from: vec![],
            })),
            // With allowFrom peer (namespace selector)
            Just(Some(CrdNetworkPolicySpec {
                allow_external_access: false,
                allow_from: vec![CrdNetworkPolicyPeer {
                    namespace_selector: Some(LabelSelectorConfig {
                        match_labels: Some(
                            [("team".to_string(), "backend".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        match_expressions: vec![],
                    }),
                    pod_selector: None,
                }],
            })),
            // With allowFrom peer (pod selector)
            Just(Some(CrdNetworkPolicySpec {
                allow_external_access: false,
                allow_from: vec![CrdNetworkPolicyPeer {
                    namespace_selector: None,
                    pod_selector: Some(LabelSelectorConfig {
                        match_labels: Some(
                            [("app".to_string(), "api".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        match_expressions: vec![],
                    }),
                }],
            })),
            // With match expressions
            Just(Some(CrdNetworkPolicySpec {
                allow_external_access: false,
                allow_from: vec![CrdNetworkPolicyPeer {
                    namespace_selector: Some(LabelSelectorConfig {
                        match_labels: None,
                        match_expressions: vec![LabelSelectorRequirement {
                            key: "env".to_string(),
                            operator: "In".to_string(),
                            values: vec!["staging".to_string(), "production".to_string()],
                        }],
                    }),
                    pod_selector: None,
                }],
            })),
            // Multiple peers
            Just(Some(CrdNetworkPolicySpec {
                allow_external_access: true,
                allow_from: vec![
                    CrdNetworkPolicyPeer {
                        namespace_selector: Some(LabelSelectorConfig {
                            match_labels: Some(
                                [("team".to_string(), "frontend".to_string())]
                                    .into_iter()
                                    .collect(),
                            ),
                            match_expressions: vec![],
                        }),
                        pod_selector: None,
                    },
                    CrdNetworkPolicyPeer {
                        namespace_selector: Some(LabelSelectorConfig {
                            match_labels: Some(
                                [("team".to_string(), "backend".to_string())]
                                    .into_iter()
                                    .collect(),
                            ),
                            match_expressions: vec![],
                        }),
                        pod_selector: None,
                    },
                ],
            })),
        ]
    }

    fn cluster_with_network_policy(
        network_policy: Option<CrdNetworkPolicySpec>,
    ) -> PostgresCluster {
        let spec = PostgresClusterSpec {
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
            network_policy,
        };
        cluster_from_spec(spec)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// Property: NetworkPolicy generation never panics for any configuration
        #[test]
        fn prop_network_policy_no_panic(np_spec in optional_network_policy()) {
            let cluster = cluster_with_network_policy(np_spec);
            let np = network_policy::generate_network_policy(&cluster);

            // Should always generate valid NetworkPolicy
            prop_assert!(np.metadata.name.is_some());
            prop_assert!(np.spec.is_some());
        }

        /// Property: NetworkPolicy always has operator namespace access
        #[test]
        fn prop_network_policy_operator_access(np_spec in optional_network_policy()) {
            let cluster = cluster_with_network_policy(np_spec);
            let np = network_policy::generate_network_policy(&cluster);

            let spec = np.spec.unwrap();
            let ingress = spec.ingress.unwrap();

            // Find operator namespace rule
            let has_operator_rule = ingress.iter().any(|rule| {
                rule.from.as_ref().is_some_and(|peers| {
                    peers.iter().any(|peer| {
                        peer.namespace_selector.as_ref().is_some_and(|sel| {
                            sel.match_labels.as_ref().is_some_and(|labels| {
                                labels.get("kubernetes.io/metadata.name")
                                    == Some(&"postgres-operator-system".to_string())
                            })
                        })
                    })
                })
            });

            prop_assert!(has_operator_rule, "Operator namespace must always be allowed");
        }

        /// Property: NetworkPolicy always has egress rules for DNS and K8s API
        #[test]
        fn prop_network_policy_required_egress(np_spec in optional_network_policy()) {
            let cluster = cluster_with_network_policy(np_spec);
            let np = network_policy::generate_network_policy(&cluster);

            let spec = np.spec.unwrap();
            let egress = spec.egress.unwrap();

            // Should have at least 3 egress rules
            prop_assert!(egress.len() >= 3, "Should have egress rules for DNS, K8s API, and replication");
        }

        /// Property: External access adds RFC1918 CIDRs when enabled
        #[test]
        fn prop_external_access_adds_cidrs(allow_external in proptest::bool::ANY) {
            let np_spec = Some(CrdNetworkPolicySpec {
                allow_external_access: allow_external,
                allow_from: vec![],
            });
            let cluster = cluster_with_network_policy(np_spec);
            let np = network_policy::generate_network_policy(&cluster);

            let spec = np.spec.unwrap();
            let ingress = spec.ingress.unwrap();

            let has_ip_blocks = ingress.iter().any(|rule| {
                rule.from.as_ref().is_some_and(|peers| {
                    peers.iter().any(|peer| peer.ip_block.is_some())
                })
            });

            if allow_external {
                prop_assert!(has_ip_blocks, "External access should add IP blocks");
            } else {
                prop_assert!(!has_ip_blocks, "No external access should not add IP blocks");
            }
        }
    }
}

// =============================================================================
// SQL Security Property Tests
// =============================================================================

mod sql_security_tests {
    use super::*;
    use postgres_operator::resources::sql::{
        escape_sql_string_pub, is_valid_identifier, quote_identifier_pub,
    };

    /// Generate strings that might attempt SQL injection
    fn sql_injection_attempts() -> impl Strategy<Value = String> {
        prop_oneof![
            // Classic injection attempts
            Just("'; DROP TABLE users;--".to_string()),
            Just("' OR '1'='1".to_string()),
            Just("\"; DROP TABLE users;--".to_string()),
            Just("' UNION SELECT * FROM passwords --".to_string()),
            // Quote escaping attempts
            Just("it's".to_string()),
            Just("user\"name".to_string()),
            Just("test''injection".to_string()),
            Just("a'b'c".to_string()),
            // Unicode attempts
            Just("café".to_string()),
            Just("日本語".to_string()),
            Just("🐘postgres".to_string()),
            // Whitespace manipulation
            Just("  spaces  ".to_string()),
            Just("tab\there".to_string()),
            Just("new\nline".to_string()),
            // Empty and special
            Just("".to_string()),
            Just("a".to_string()),
            Just("_underscore".to_string()),
            // Long strings
            Just("a".repeat(100)),
            Just("'".repeat(50)),
        ]
    }

    /// Generate valid PostgreSQL identifier names (shrinks toward shorter names)
    /// Valid identifiers: start with letter or underscore, contain only lowercase letters,
    /// digits, underscores, and are max 63 chars
    fn valid_identifier_names() -> impl Strategy<Value = String> {
        prop_oneof![
            // Simple names - shrink toward shorter
            "[a-z][a-z0-9_]{0,10}".prop_map(|s| s),
            // Underscore-prefixed names
            "_[a-z][a-z0-9_]{0,8}".prop_map(|s| s),
            // Longer names (up to max length 63)
            "[a-z][a-z0-9_]{20,62}".prop_map(|s| s),
        ]
    }

    /// Generate invalid PostgreSQL identifier names (shrinks toward simpler violations)
    fn invalid_identifier_names() -> impl Strategy<Value = String> {
        prop_oneof![
            // Empty string
            Just("".to_string()),
            // Starts with digit - shrink toward "0a"
            "[0-9][a-z]{1,5}".prop_map(|s| s),
            // Contains uppercase - shrink toward "Aa"
            "[A-Z][a-z]{1,5}".prop_map(|s| s),
            // Contains hyphen - shrink toward "a-a"
            "[a-z]{1,3}-[a-z]{1,3}".prop_map(|s| s),
            // Contains space - shrink toward "a a"
            "[a-z]{1,3} [a-z]{1,3}".prop_map(|s| s),
            // Too long (>63 chars) - shrink toward exactly 64 chars
            "[a-z]{64,100}".prop_map(|s| s),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// Property: Quote identifier never panics and always produces quoted output
        #[test]
        fn prop_quote_identifier_never_panics(input in sql_injection_attempts()) {
            let result = quote_identifier_pub(&input);

            // Should always produce a string starting and ending with double quotes
            prop_assert!(result.starts_with('"'));
            prop_assert!(result.ends_with('"'));
        }

        /// Property: Escape SQL string never panics
        #[test]
        fn prop_escape_sql_string_never_panics(input in sql_injection_attempts()) {
            let result = escape_sql_string_pub(&input);

            // Should not contain unescaped single quotes (each ' becomes '')
            // Count single quotes in output vs input
            let input_quotes = input.matches('\'').count();
            let output_quotes = result.matches('\'').count();

            // Each single quote in input becomes two in output
            prop_assert_eq!(output_quotes, input_quotes * 2);
        }

        /// Property: Valid identifiers pass validation
        #[test]
        fn prop_valid_identifiers_pass(name in valid_identifier_names()) {
            prop_assert!(is_valid_identifier(&name), "Valid identifier should pass: {}", name);
        }

        /// Property: Invalid identifiers fail validation
        #[test]
        fn prop_invalid_identifiers_fail(name in invalid_identifier_names()) {
            prop_assert!(!is_valid_identifier(&name), "Invalid identifier should fail: {}", name);
        }

        /// Property: Quoted identifiers cannot break out of quotes
        #[test]
        fn prop_quoted_identifier_safe(input in sql_injection_attempts()) {
            let quoted = quote_identifier_pub(&input);

            // The result should be properly quoted - no unescaped double quotes inside
            let inner = &quoted[1..quoted.len()-1]; // Remove outer quotes

            // Count consecutive double quotes - they should all be pairs (escaped)
            let mut i = 0;
            let chars: Vec<char> = inner.chars().collect();
            while i < chars.len() {
                if chars[i] == '"' {
                    // Must be followed by another quote (escaped)
                    prop_assert!(i + 1 < chars.len() && chars[i + 1] == '"',
                        "Found unescaped double quote in: {}", quoted);
                    i += 2; // Skip the pair
                } else {
                    i += 1;
                }
            }
        }
    }
}

// =============================================================================
// PostgresDatabase Property Tests
// =============================================================================

mod database_crd_tests {
    use super::*;
    use postgres_operator::crd::{
        ClusterRef, DatabasePhase, DatabaseSpec, GrantSpec, PostgresDatabase, PostgresDatabaseSpec,
        RolePrivilege, RoleSpec, TablePrivilege,
    };

    /// Generate valid database names
    fn valid_database_name() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("mydb".to_string()),
            Just("orders".to_string()),
            Just("user_data".to_string()),
            Just("app123".to_string()),
            Just("_internal".to_string()),
        ]
    }

    /// Generate role specs
    fn role_spec() -> impl Strategy<Value = RoleSpec> {
        (
            valid_database_name(),
            prop::collection::vec(
                prop_oneof![
                    Just(RolePrivilege::Login),
                    Just(RolePrivilege::Createdb),
                    Just(RolePrivilege::Createrole),
                ],
                0..3,
            ),
        )
            .prop_map(|(name, privileges)| RoleSpec {
                name: name.clone(),
                privileges,
                secret_name: format!("{}-creds", name),
                connection_limit: None,
                login: true,
            })
    }

    /// Generate grant specs
    fn grant_spec() -> impl Strategy<Value = GrantSpec> {
        (
            valid_database_name(),
            prop::collection::vec(
                prop_oneof![
                    Just(TablePrivilege::Select),
                    Just(TablePrivilege::Insert),
                    Just(TablePrivilege::Update),
                    Just(TablePrivilege::Delete),
                ],
                1..4,
            ),
        )
            .prop_map(|(role, privileges)| GrantSpec {
                role,
                schema: "public".to_string(),
                privileges,
                all_tables: true,
                all_sequences: false,
                all_functions: false,
            })
    }

    /// Generate a PostgresDatabase spec
    fn postgres_database_spec() -> impl Strategy<Value = PostgresDatabaseSpec> {
        (
            valid_database_name(),
            valid_database_name(),
            prop::collection::vec(role_spec(), 0..3),
            prop::collection::vec(grant_spec(), 0..2),
            prop::collection::vec(
                prop_oneof![
                    Just("uuid-ossp".to_string()),
                    Just("pg_trgm".to_string()),
                    Just("hstore".to_string()),
                ],
                0..3,
            ),
        )
            .prop_map(
                |(db_name, owner, roles, grants, extensions)| PostgresDatabaseSpec {
                    cluster_ref: ClusterRef {
                        name: "test-cluster".to_string(),
                        namespace: Some("default".to_string()),
                    },
                    database: DatabaseSpec {
                        name: db_name,
                        owner,
                        encoding: None,
                        locale: None,
                        connection_limit: None,
                    },
                    roles,
                    grants,
                    extensions,
                },
            )
    }

    fn create_postgres_database(spec: PostgresDatabaseSpec) -> PostgresDatabase {
        PostgresDatabase {
            metadata: kube::core::ObjectMeta {
                name: Some("test-db".to_string()),
                namespace: Some("test-ns".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        /// Property: PostgresDatabase spec structure is always valid
        #[test]
        fn prop_database_spec_valid_structure(spec in postgres_database_spec()) {
            let db = create_postgres_database(spec);

            // Basic structure should be valid
            prop_assert!(db.metadata.name.is_some());
            prop_assert!(!db.spec.database.name.is_empty());
            prop_assert!(!db.spec.database.owner.is_empty());
        }

        /// Property: All roles have valid secret names
        #[test]
        fn prop_roles_have_secret_names(spec in postgres_database_spec()) {
            for role in &spec.roles {
                prop_assert!(!role.secret_name.is_empty(), "Role secret name should not be empty");
            }
        }

        /// Property: Default phase is Pending
        #[test]
        fn prop_default_phase_is_pending(_spec in postgres_database_spec()) {
            let phase = DatabasePhase::default();
            prop_assert_eq!(phase, DatabasePhase::Pending);
        }
    }
}

// =============================================================================
// Webhook Policy Property Tests
// =============================================================================

mod webhook_policy_tests {
    use super::*;
    use postgres_operator::crd::{
        BackupDestination, BackupSpec, EncryptionSpec, IssuerKind, IssuerRef, NetworkPolicySpec,
        ResourceList, ResourceRequirements, RetentionPolicy,
    };
    use postgres_operator::webhooks::policies::{
        ValidationContext, validate_all, validate_backup, validate_immutability, validate_tls,
    };
    use std::collections::BTreeMap;

    /// Generate backup specs with varying encryption
    fn backup_spec_with_encryption(has_encryption: bool) -> BackupSpec {
        BackupSpec {
            schedule: "0 2 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(7),
                max_age: None,
            },
            destination: BackupDestination::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                credentials_secret: "aws-creds".to_string(),
                path: None,
                force_path_style: false,
            },
            encryption: if has_encryption {
                Some(EncryptionSpec {
                    method: Default::default(),
                    key_secret: "encryption-key".to_string(),
                })
            } else {
                None
            },
            wal_archiving: None,
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        }
    }

    /// Create a test cluster from a spec
    fn test_cluster_from_spec(spec: PostgresClusterSpec) -> PostgresCluster {
        PostgresCluster {
            metadata: kube::core::ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        /// Property: Backup policy is deterministic
        #[test]
        fn prop_backup_policy_deterministic(has_encryption in proptest::bool::ANY) {
            let spec = PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                resources: None,
                postgresql_params: Default::default(),
                labels: Default::default(),
                backup: Some(backup_spec_with_encryption(has_encryption)),
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
            };

            let cluster = test_cluster_from_spec(spec);
            let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());

            let result1 = validate_backup(&ctx);
            let result2 = validate_backup(&ctx);

            prop_assert_eq!(result1.allowed, result2.allowed, "Backup policy should be deterministic");
        }

        /// Property: TLS policy requires issuer when enabled
        #[test]
        fn prop_tls_policy_requires_issuer(tls_enabled in proptest::bool::ANY, has_issuer in proptest::bool::ANY) {
            let spec = PostgresClusterSpec {
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
                tls: TLSSpec {
                    enabled: tls_enabled,
                    issuer_ref: if has_issuer {
                        Some(IssuerRef {
                            name: "test-issuer".to_string(),
                            kind: IssuerKind::ClusterIssuer,
                            group: "cert-manager.io".to_string(),
                        })
                    } else {
                        None
                    },
                    additional_dns_names: vec![],
                    duration: None,
                    renew_before: None,
                },
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
            };

            let cluster = test_cluster_from_spec(spec);
            let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
            let result = validate_tls(&ctx);

            if !tls_enabled {
                prop_assert!(result.allowed, "TLS disabled should always pass");
            } else if has_issuer {
                prop_assert!(result.allowed, "TLS enabled with issuer should pass");
            } else {
                prop_assert!(!result.allowed, "TLS enabled without issuer should fail");
            }
        }

        /// Property: Version downgrades are always rejected
        #[test]
        fn prop_version_downgrade_rejected(
            old_ver in prop_oneof![Just(PostgresVersion::V16), Just(PostgresVersion::V17)],
            new_ver in prop_oneof![Just(PostgresVersion::V15), Just(PostgresVersion::V16)]
        ) {
            let old_major = old_ver.as_major_version();
            let new_major = new_ver.as_major_version();

            let old_spec = PostgresClusterSpec {
                version: old_ver,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: Some("standard".to_string()),
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
                network_policy: None,
            };

            let new_spec = PostgresClusterSpec {
                version: new_ver,
                ..old_spec.clone()
            };

            let old_cluster = test_cluster_from_spec(old_spec);
            let new_cluster = test_cluster_from_spec(new_spec);
            let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
            let result = validate_immutability(&ctx);

            let is_downgrade = old_major > new_major;
            if is_downgrade {
                prop_assert!(!result.allowed, "Version downgrade should be rejected");
            }
        }

        /// Property: Production policy enforces replica requirements in production namespaces
        #[test]
        fn prop_production_requires_replicas(replicas in 1..=5i32, is_production in proptest::bool::ANY) {
            let spec = PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                resources: Some(ResourceRequirements {
                    requests: Some(ResourceList {
                        cpu: Some("1".to_string()),
                        memory: Some("2Gi".to_string()),
                    }),
                    limits: Some(ResourceList {
                        cpu: Some("2".to_string()),
                        memory: Some("4Gi".to_string()),
                    }),
                    restart_on_resize: None,
                }),
                postgresql_params: Default::default(),
                labels: Default::default(),
                backup: Some(backup_spec_with_encryption(true)),
                pgbouncer: None,
                // TLS disabled to avoid needing an issuer ref
                tls: TLSSpec {
                    enabled: false,
                    issuer_ref: None,
                    additional_dns_names: vec![],
                    duration: None,
                    renew_before: None,
                },
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: Some(NetworkPolicySpec {
                    allow_external_access: false,
                    allow_from: vec![],
                }),
            };

            let cluster = test_cluster_from_spec(spec);
            let namespace_labels = if is_production {
                BTreeMap::from([("env".to_string(), "production".to_string())])
            } else {
                BTreeMap::new()
            };
            let ctx = ValidationContext::new(&cluster, None, namespace_labels);

            // Use validate_all which checks is_production_namespace() before applying production rules
            let result = validate_all(&ctx);

            if !is_production {
                // Non-production clusters should pass (no production rules applied)
                // This assumes the cluster spec is otherwise valid
                prop_assert!(result.allowed, "Non-production should pass all validation");
            } else if replicas < 3 {
                // Production with less than 3 replicas should fail
                prop_assert!(!result.allowed, "Production with {} replicas should fail", replicas);
            } else {
                // Production with 3+ replicas and all other requirements met should pass
                prop_assert!(result.allowed, "Valid production cluster should pass");
            }
        }

        /// Property: validate_all is deterministic
        #[test]
        fn prop_validate_all_deterministic(spec in valid_spec()) {
            let cluster = cluster_from_spec(spec);
            let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());

            let result1 = validate_all(&ctx);
            let result2 = validate_all(&ctx);

            prop_assert_eq!(result1.allowed, result2.allowed, "validate_all should be deterministic");
            prop_assert_eq!(result1.reason, result2.reason, "validate_all reason should be deterministic");
        }
    }
}

// =============================================================================
// Webhook Contract Property Tests
// =============================================================================

mod webhook_contract_tests {
    use super::*;
    use serde_json::{Value, json};

    /// Generate a valid UID string (shrinks toward shorter UIDs)
    fn valid_uid() -> impl Strategy<Value = String> {
        "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}".prop_map(|s| s)
    }

    /// Generate an operation type
    fn operation() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("CREATE".to_string()),
            Just("UPDATE".to_string()),
            Just("DELETE".to_string()),
        ]
    }

    /// Build an AdmissionReview JSON request
    fn build_admission_review(
        uid: &str,
        operation: &str,
        cluster: &PostgresCluster,
        old_cluster: Option<&PostgresCluster>,
    ) -> Value {
        let object = serde_json::to_value(cluster).expect("serialize cluster");
        let old_object =
            old_cluster.map(|c| serde_json::to_value(c).expect("serialize old cluster"));

        let mut request = json!({
            "uid": uid,
            "kind": {
                "group": "postgres-operator.smoketurner.com",
                "version": "v1alpha1",
                "kind": "PostgresCluster"
            },
            "resource": {
                "group": "postgres-operator.smoketurner.com",
                "version": "v1alpha1",
                "resource": "postgresclusters"
            },
            "operation": operation,
            "namespace": cluster.metadata.namespace,
            "name": cluster.metadata.name,
            "object": object
        });

        if let Some(old) = old_object {
            request["oldObject"] = old;
        }

        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": request
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        /// Property: AdmissionReview request deserializes correctly with camelCase fields
        #[test]
        fn prop_admission_review_deserializes(
            uid in valid_uid(),
            op in operation(),
            spec in valid_spec()
        ) {
            use postgres_operator::webhooks::AdmissionReview;

            let cluster = cluster_from_spec(spec);
            let review_json = build_admission_review(&uid, &op, &cluster, None);

            // Should deserialize without error
            let review: Result<AdmissionReview, _> = serde_json::from_value(review_json);
            prop_assert!(review.is_ok(), "AdmissionReview should deserialize: {:?}", review.err());

            let review = review.unwrap();
            prop_assert_eq!(review.api_version, "admission.k8s.io/v1");
            prop_assert_eq!(review.kind, "AdmissionReview");
            prop_assert!(review.request.is_some(), "Request should be present");

            let request = review.request.unwrap();
            prop_assert_eq!(request.uid, uid);
            prop_assert_eq!(request.operation, op);
        }

        /// Property: AdmissionReview response serializes with correct camelCase fields
        #[test]
        fn prop_admission_review_response_serializes(
            uid in valid_uid(),
            allowed in proptest::bool::ANY,
            message in "[a-zA-Z ]{0,50}",
            reason in proptest::option::of("[A-Z][a-zA-Z]{3,15}")
        ) {
            use postgres_operator::webhooks::{AdmissionReviewResponse, AdmissionResponse, AdmissionStatus};

            let response = AdmissionReviewResponse {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                response: AdmissionResponse {
                    uid: uid.clone(),
                    allowed,
                    status: if allowed {
                        None
                    } else {
                        Some(AdmissionStatus {
                            code: 403,
                            message: message.clone(),
                            reason: reason.clone(),
                        })
                    },
                },
            };

            // Serialize to JSON
            let json_result = serde_json::to_value(&response);
            prop_assert!(json_result.is_ok(), "Response should serialize");

            let json = json_result.unwrap();

            // Verify camelCase field names (Kubernetes requires these)
            prop_assert_eq!(json["apiVersion"].as_str(), Some("admission.k8s.io/v1"));
            prop_assert_eq!(json["kind"].as_str(), Some("AdmissionReview"));
            prop_assert_eq!(json["response"]["uid"].as_str(), Some(uid.as_str()));
            prop_assert_eq!(json["response"]["allowed"].as_bool(), Some(allowed));

            // Verify status fields when denied
            if !allowed {
                prop_assert!(json["response"]["status"].is_object(), "Status should be present when denied");
                prop_assert_eq!(json["response"]["status"]["code"].as_i64(), Some(403));
                prop_assert_eq!(json["response"]["status"]["message"].as_str(), Some(message.as_str()));
                if let Some(ref r) = reason {
                    prop_assert_eq!(json["response"]["status"]["reason"].as_str(), Some(r.as_str()));
                }
            } else {
                // Status should be absent when allowed (skip_serializing_if works)
                prop_assert!(json["response"]["status"].is_null() || !json["response"].as_object().unwrap().contains_key("status"),
                    "Status should be absent when allowed");
            }
        }

        /// Property: UID is always echoed back in response
        #[test]
        fn prop_uid_echoed_in_response(uid in valid_uid()) {
            use postgres_operator::webhooks::{AdmissionReviewResponse, AdmissionResponse};

            let response = AdmissionReviewResponse {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                response: AdmissionResponse {
                    uid: uid.clone(),
                    allowed: true,
                    status: None,
                },
            };

            let json: Value = serde_json::to_value(&response).unwrap();
            prop_assert_eq!(json["response"]["uid"].as_str(), Some(uid.as_str()), "UID must be echoed exactly");
        }

        /// Property: UPDATE operation includes oldObject for immutability checks
        #[test]
        fn prop_update_includes_old_object(
            uid in valid_uid(),
            old_spec in valid_spec(),
            new_spec in valid_spec()
        ) {
            use postgres_operator::webhooks::AdmissionReview;

            let old_cluster = cluster_from_spec(old_spec);
            let new_cluster = cluster_from_spec(new_spec);
            let review_json = build_admission_review(&uid, "UPDATE", &new_cluster, Some(&old_cluster));

            let review: AdmissionReview = serde_json::from_value(review_json).unwrap();
            let request = review.request.unwrap();

            prop_assert!(request.old_object.is_some(), "UPDATE should have oldObject");
            prop_assert!(request.object.is_some(), "UPDATE should have object");
        }

        /// Property: Response status code is always 403 when denied
        #[test]
        fn prop_denied_status_code_403(
            uid in valid_uid(),
            message in "[a-zA-Z ]{1,30}"
        ) {
            use postgres_operator::webhooks::{AdmissionReviewResponse, AdmissionResponse, AdmissionStatus};

            let response = AdmissionReviewResponse {
                api_version: "admission.k8s.io/v1".to_string(),
                kind: "AdmissionReview".to_string(),
                response: AdmissionResponse {
                    uid,
                    allowed: false,
                    status: Some(AdmissionStatus {
                        code: 403,
                        message,
                        reason: Some("ValidationFailed".to_string()),
                    }),
                },
            };

            let json: Value = serde_json::to_value(&response).unwrap();
            prop_assert_eq!(json["response"]["status"]["code"].as_i64(), Some(403), "Denied responses should have 403 status code");
        }
    }

    /// Verify the exact field names match Kubernetes API expectations
    #[test]
    fn test_response_field_names_match_kubernetes_spec() {
        use postgres_operator::webhooks::{
            AdmissionResponse, AdmissionReviewResponse, AdmissionStatus,
        };

        let response = AdmissionReviewResponse {
            api_version: "admission.k8s.io/v1".to_string(),
            kind: "AdmissionReview".to_string(),
            response: AdmissionResponse {
                uid: "test-uid".to_string(),
                allowed: false,
                status: Some(AdmissionStatus {
                    code: 403,
                    message: "Test message".to_string(),
                    reason: Some("TestReason".to_string()),
                }),
            },
        };

        let json: Value = serde_json::to_value(&response).unwrap();

        // These exact field names are required by Kubernetes
        assert!(
            json.get("apiVersion").is_some(),
            "Must have apiVersion (camelCase)"
        );
        assert!(json.get("kind").is_some(), "Must have kind");
        assert!(json.get("response").is_some(), "Must have response");

        let resp = &json["response"];
        assert!(resp.get("uid").is_some(), "Response must have uid");
        assert!(resp.get("allowed").is_some(), "Response must have allowed");

        // These should NOT exist (snake_case would break the API)
        assert!(
            json.get("api_version").is_none(),
            "Must NOT have api_version (snake_case)"
        );
        assert!(
            resp.get("status").unwrap().get("code").is_some(),
            "Status must have code"
        );
        assert!(
            resp.get("status").unwrap().get("message").is_some(),
            "Status must have message"
        );
    }

    /// Verify AdmissionReview request can be round-tripped
    #[test]
    fn test_admission_review_round_trip() {
        use postgres_operator::webhooks::AdmissionReview;

        let cluster = cluster_from_spec(minimal_spec());
        let review_json = build_admission_review(
            "12345678-1234-1234-1234-123456789abc",
            "CREATE",
            &cluster,
            None,
        );

        // Deserialize
        let review: AdmissionReview =
            serde_json::from_value(review_json.clone()).expect("should deserialize");

        // Verify key fields
        assert_eq!(review.api_version, "admission.k8s.io/v1");
        assert_eq!(review.kind, "AdmissionReview");

        let request = review.request.expect("should have request");
        assert_eq!(request.uid, "12345678-1234-1234-1234-123456789abc");
        assert_eq!(request.operation, "CREATE");
        assert_eq!(request.kind.group, "postgres-operator.smoketurner.com");
        assert_eq!(request.kind.version, "v1alpha1");
        assert_eq!(request.kind.kind, "PostgresCluster");
    }
}
