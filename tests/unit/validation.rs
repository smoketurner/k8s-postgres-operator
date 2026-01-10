//! Unit tests for validation logic

use postgres_operator::controller::validation::{
    MAX_REPLICAS, MIN_REPLICAS, validate_spec, validate_spec_change, validate_version_upgrade,
};
use postgres_operator::crd::{
    PostgresCluster, PostgresClusterSpec, PostgresVersion, StorageSpec, TLSSpec,
};

/// Helper to create a test cluster
fn create_test_cluster(
    name: &str,
    namespace: &str,
    replicas: i32,
    version: PostgresVersion,
) -> PostgresCluster {
    PostgresCluster {
        metadata: kube::core::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            uid: Some("test-uid-12345".to_string()),
            generation: Some(1),
            ..Default::default()
        },
        spec: PostgresClusterSpec {
            version,
            replicas,
            storage: StorageSpec {
                storage_class: Some("standard".to_string()),
                size: "10Gi".to_string(),
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
        },
        status: None,
    }
}

mod replica_limits_tests {
    use super::*;

    #[test]
    fn test_min_replicas_constant() {
        assert_eq!(MIN_REPLICAS, 1);
    }

    #[test]
    fn test_max_replicas_constant() {
        assert_eq!(MAX_REPLICAS, 100);
    }
}

mod validate_spec_tests {
    use super::*;

    #[test]
    fn test_valid_single_replica_spec() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_valid_three_replica_spec() {
        let cluster = create_test_cluster("test", "default", 3, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_invalid_replicas_below_min() {
        let cluster = create_test_cluster("test", "default", 0, PostgresVersion::V16);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("below minimum"));
    }

    #[test]
    fn test_invalid_replicas_above_max() {
        let cluster = create_test_cluster("test", "default", MAX_REPLICAS + 1, PostgresVersion::V16);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    // Note: Invalid version tests are no longer needed because PostgresVersion enum
    // enforces valid values (15, 16, 17) at the CRD level

    #[test]
    fn test_valid_versions() {
        // All PostgresVersion enum variants should be valid
        for version in [
            PostgresVersion::V15,
            PostgresVersion::V16,
            PostgresVersion::V17,
        ] {
            let cluster = create_test_cluster("test", "default", 1, version.clone());
            assert!(
                validate_spec(&cluster).is_ok(),
                "Version {:?} should be valid",
                version
            );
        }
    }

    #[test]
    fn test_valid_storage_sizes() {
        for size in ["1Gi", "10Gi", "100Gi", "1Ti", "500Mi"] {
            let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
            cluster.spec.storage.size = size.to_string();
            assert!(
                validate_spec(&cluster).is_ok(),
                "Size {} should be valid",
                size
            );
        }
    }

    #[test]
    fn test_invalid_storage_size() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.spec.storage.size = "10GB".to_string(); // Wrong suffix
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }
}

mod spec_change_tests {
    use super::*;

    #[test]
    fn test_no_change() {
        let old = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 1, PostgresVersion::V16);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(!diff.has_changes());
    }

    #[test]
    fn test_replica_scale_up() {
        let old = create_test_cluster("test", "default", 2, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 4, PostgresVersion::V16);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, 2);
        assert!(diff.is_scale_only());
    }

    #[test]
    fn test_replica_scale_down() {
        let old = create_test_cluster("test", "default", 4, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 2, PostgresVersion::V16);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, -2);
    }

    #[test]
    fn test_storage_class_change_rejected() {
        let old = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        let mut new = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        new.spec.storage.storage_class = Some("fast".to_string());

        let result = validate_spec_change(&old, &new);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("storage class"));
    }

    #[test]
    fn test_version_change() {
        let old = create_test_cluster("test", "default", 1, PostgresVersion::V15);
        let new = create_test_cluster("test", "default", 1, PostgresVersion::V16);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.version_changed);
        assert!(diff.requires_rolling_update());
    }

    #[test]
    fn test_scale_down_to_zero_rejected() {
        let old = create_test_cluster("test", "default", 2, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 0, PostgresVersion::V16);

        let result = validate_spec_change(&old, &new);
        assert!(result.is_err());
    }
}

mod version_upgrade_tests {
    use super::*;

    #[test]
    fn test_valid_minor_upgrade() {
        assert!(validate_version_upgrade("16.1", "16.2").is_ok());
    }

    #[test]
    fn test_valid_major_upgrade() {
        assert!(validate_version_upgrade("15", "16").is_ok());
    }

    #[test]
    fn test_downgrade_rejected() {
        let result = validate_version_upgrade("16", "15");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("downgrade"));
    }

    #[test]
    fn test_same_version() {
        assert!(validate_version_upgrade("16", "16").is_ok());
    }

    #[test]
    fn test_multi_version_upgrade() {
        // This should work but log a warning
        assert!(validate_version_upgrade("14", "16").is_ok());
    }

    #[test]
    fn test_upgrade_14_to_15() {
        assert!(validate_version_upgrade("14", "15").is_ok());
    }

    #[test]
    fn test_upgrade_15_to_16() {
        assert!(validate_version_upgrade("15", "16").is_ok());
    }

    #[test]
    fn test_upgrade_16_to_17() {
        assert!(validate_version_upgrade("16", "17").is_ok());
    }

    #[test]
    fn test_downgrade_17_to_16_rejected() {
        let result = validate_version_upgrade("17", "16");
        assert!(result.is_err());
    }

    #[test]
    fn test_downgrade_16_to_14_rejected() {
        let result = validate_version_upgrade("16", "14");
        assert!(result.is_err());
    }

    #[test]
    fn test_minor_version_same_major() {
        assert!(validate_version_upgrade("16.0", "16.5").is_ok());
    }
}

// =============================================================================
// Edge Case and Panic Prevention Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_max_replicas_accepted() {
        let cluster = create_test_cluster("test", "default", MAX_REPLICAS, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_above_max_replicas_rejected() {
        let cluster = create_test_cluster("test", "default", MAX_REPLICAS + 1, PostgresVersion::V16);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_min_replicas_accepted() {
        let cluster = create_test_cluster("test", "default", MIN_REPLICAS, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_zero_replicas_rejected() {
        let cluster = create_test_cluster("test", "default", 0, PostgresVersion::V16);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }

    #[test]
    fn test_negative_replicas_rejected() {
        let cluster = create_test_cluster("test", "default", -1, PostgresVersion::V16);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }

    // Note: Version validation tests (test_version_9_rejected, test_version_18_rejected,
    // test_empty_version_rejected, test_invalid_version_string_rejected,
    // test_version_with_spaces_rejected) have been removed because PostgresVersion enum
    // now enforces valid values at the CRD level.

    #[test]
    fn test_empty_storage_size_rejected() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.spec.storage.size = "".to_string();
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_size_without_unit_rejected() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.spec.storage.size = "100".to_string();
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_size_with_invalid_unit_rejected() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.spec.storage.size = "100GB".to_string(); // Should be Gi, not GB
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_size_negative_rejected() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.spec.storage.size = "-10Gi".to_string();
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }

    #[test]
    fn test_two_replicas_accepted() {
        // 2 replicas is valid but not recommended for HA
        let cluster = create_test_cluster("test", "default", 2, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_five_replicas_accepted() {
        let cluster = create_test_cluster("test", "default", 5, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_ten_replicas_accepted() {
        let cluster = create_test_cluster("test", "default", 10, PostgresVersion::V16);
        assert!(validate_spec(&cluster).is_ok());
    }
}

mod panic_prevention_tests {
    use super::*;
    use postgres_operator::crd::PostgresClusterStatus;

    #[test]
    fn test_nil_status_validation_no_panic() {
        // Cluster with no status should still validate spec correctly
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.status = None;
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_default_status_validation_no_panic() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.status = Some(PostgresClusterStatus::default());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_empty_postgresql_params_accepted() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.postgresql_params.is_empty());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_nil_resources_accepted() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.resources.is_none());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_default_tls_accepted() {
        // TLS with default values (enabled=true, no issuer) should be accepted
        // The reconciler will validate issuer configuration separately
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.tls.enabled);
        assert!(cluster.spec.tls.issuer_ref.is_none());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_nil_pgbouncer_accepted() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.pgbouncer.is_none());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_nil_metrics_accepted() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.metrics.is_none());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_nil_service_accepted() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.service.is_none());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_nil_backup_accepted() {
        let cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        assert!(cluster.spec.backup.is_none());
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_nil_storage_class_accepted() {
        let mut cluster = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        cluster.spec.storage.storage_class = None;
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_spec_change_with_nil_status_no_panic() {
        let old = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        let mut new = create_test_cluster("test", "default", 3, PostgresVersion::V16);
        new.status = None;
        let result = validate_spec_change(&old, &new);
        assert!(result.is_ok());
    }

    #[test]
    fn test_version_upgrade_empty_strings_no_panic() {
        // Empty strings should return error, not panic
        let result = validate_version_upgrade("", "16");
        // Either error or ok is fine, just no panic
        let _ = result;
    }

    #[test]
    fn test_version_upgrade_both_empty_no_panic() {
        let result = validate_version_upgrade("", "");
        let _ = result;
    }
}

mod scale_validation_tests {
    use super::*;

    #[test]
    fn test_scale_up_1_to_3() {
        let old = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 3, PostgresVersion::V16);
        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, 2);
    }

    #[test]
    fn test_scale_up_3_to_5() {
        let old = create_test_cluster("test", "default", 3, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 5, PostgresVersion::V16);
        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, 2);
    }

    #[test]
    fn test_scale_down_5_to_3() {
        let old = create_test_cluster("test", "default", 5, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 3, PostgresVersion::V16);
        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, -2);
    }

    #[test]
    fn test_scale_down_3_to_1() {
        let old = create_test_cluster("test", "default", 3, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, -2);
    }

    #[test]
    fn test_scale_to_max_replicas() {
        let old = create_test_cluster("test", "default", 1, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", MAX_REPLICAS, PostgresVersion::V16);
        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, MAX_REPLICAS - 1);
    }

    #[test]
    fn test_scale_from_max_to_min() {
        let old = create_test_cluster("test", "default", MAX_REPLICAS, PostgresVersion::V16);
        let new = create_test_cluster("test", "default", MIN_REPLICAS, PostgresVersion::V16);
        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
    }
}
