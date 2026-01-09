//! Unit tests for validation logic

use postgres_operator::controller::validation::{
    validate_spec, validate_spec_change, validate_version_upgrade, MAX_REPLICAS, MIN_REPLICAS,
};
use postgres_operator::crd::{PostgresCluster, PostgresClusterSpec, StorageSpec};

/// Helper to create a test cluster
fn create_test_cluster(
    name: &str,
    namespace: &str,
    replicas: i32,
    version: &str,
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
            version: version.to_string(),
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
        let cluster = create_test_cluster("test", "default", 1, "16");
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_valid_three_replica_spec() {
        let cluster = create_test_cluster("test", "default", 3, "16");
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_invalid_replicas_below_min() {
        let cluster = create_test_cluster("test", "default", 0, "16");
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("below minimum"));
    }

    #[test]
    fn test_invalid_replicas_above_max() {
        let cluster = create_test_cluster("test", "default", MAX_REPLICAS + 1, "16");
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum"));
    }

    #[test]
    fn test_invalid_version() {
        let cluster = create_test_cluster("test", "default", 1, "8");
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not supported"));
    }

    #[test]
    fn test_valid_versions() {
        for version in ["10", "11", "12", "13", "14", "15", "16", "17"] {
            let cluster = create_test_cluster("test", "default", 1, version);
            assert!(validate_spec(&cluster).is_ok(), "Version {} should be valid", version);
        }
    }

    #[test]
    fn test_valid_storage_sizes() {
        for size in ["1Gi", "10Gi", "100Gi", "1Ti", "500Mi"] {
            let mut cluster = create_test_cluster("test", "default", 1, "16");
            cluster.spec.storage.size = size.to_string();
            assert!(validate_spec(&cluster).is_ok(), "Size {} should be valid", size);
        }
    }

    #[test]
    fn test_invalid_storage_size() {
        let mut cluster = create_test_cluster("test", "default", 1, "16");
        cluster.spec.storage.size = "10GB".to_string(); // Wrong suffix
        let result = validate_spec(&cluster);
        assert!(result.is_err());
    }
}

mod spec_change_tests {
    use super::*;

    #[test]
    fn test_no_change() {
        let old = create_test_cluster("test", "default", 1, "16");
        let new = create_test_cluster("test", "default", 1, "16");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(!diff.has_changes());
    }

    #[test]
    fn test_replica_scale_up() {
        let old = create_test_cluster("test", "default", 2, "16");
        let new = create_test_cluster("test", "default", 4, "16");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, 2);
        assert!(diff.is_scale_only());
    }

    #[test]
    fn test_replica_scale_down() {
        let old = create_test_cluster("test", "default", 4, "16");
        let new = create_test_cluster("test", "default", 2, "16");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, -2);
    }

    #[test]
    fn test_storage_class_change_rejected() {
        let old = create_test_cluster("test", "default", 1, "16");
        let mut new = create_test_cluster("test", "default", 1, "16");
        new.spec.storage.storage_class = Some("fast".to_string());

        let result = validate_spec_change(&old, &new);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("storage class"));
    }

    #[test]
    fn test_version_change() {
        let old = create_test_cluster("test", "default", 1, "15");
        let new = create_test_cluster("test", "default", 1, "16");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.version_changed);
        assert!(diff.requires_rolling_update());
    }

    #[test]
    fn test_scale_down_to_zero_rejected() {
        let old = create_test_cluster("test", "default", 2, "16");
        let new = create_test_cluster("test", "default", 0, "16");

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
}
