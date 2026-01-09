//! Unit tests for status management

use postgres_operator::controller::status::{
    ConditionBuilder, condition_status, condition_types, get_replica_pod_names, spec_changed,
};
use postgres_operator::crd::{
    ClusterPhase, Condition, PostgresCluster, PostgresClusterSpec, PostgresClusterStatus,
    StorageSpec,
};

/// Helper to create a test cluster with optional status
fn create_test_cluster_with_status(
    name: &str,
    generation: i64,
    status: Option<PostgresClusterStatus>,
) -> PostgresCluster {
    PostgresCluster {
        metadata: kube::core::ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some("default".to_string()),
            uid: Some("test-uid-12345".to_string()),
            generation: Some(generation),
            ..Default::default()
        },
        spec: PostgresClusterSpec {
            version: "16".to_string(),
            replicas: 3,
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
        status,
    }
}

mod condition_builder_tests {
    use super::*;

    #[test]
    fn test_new_condition_builder() {
        let builder = ConditionBuilder::new(Some(1));
        let conditions = builder.build();
        assert!(conditions.is_empty());
    }

    #[test]
    fn test_set_condition_adds_new() {
        let conditions = ConditionBuilder::new(Some(1))
            .set_condition("TestCondition", "True", "TestReason", "Test message")
            .build();

        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].type_, "TestCondition");
        assert_eq!(conditions[0].status, "True");
        assert_eq!(conditions[0].reason, "TestReason");
        assert_eq!(conditions[0].message, "Test message");
        assert_eq!(conditions[0].observed_generation, Some(1));
    }

    #[test]
    fn test_set_condition_updates_existing_same_status() {
        let existing = vec![Condition {
            type_: "TestCondition".to_string(),
            status: "True".to_string(),
            reason: "OldReason".to_string(),
            message: "Old message".to_string(),
            last_transition_time: "2024-01-01T00:00:00Z".to_string(),
            observed_generation: Some(1),
        }];

        let conditions = ConditionBuilder::from_existing(existing, Some(2))
            .set_condition("TestCondition", "True", "NewReason", "New message")
            .build();

        assert_eq!(conditions.len(), 1);
        // Status same, so transition time should NOT change
        assert_eq!(
            conditions[0].last_transition_time,
            "2024-01-01T00:00:00Z".to_string()
        );
        // But reason and message should update
        assert_eq!(conditions[0].reason, "NewReason");
        assert_eq!(conditions[0].message, "New message");
        assert_eq!(conditions[0].observed_generation, Some(2));
    }

    #[test]
    fn test_set_condition_updates_existing_different_status() {
        let existing = vec![Condition {
            type_: "TestCondition".to_string(),
            status: "True".to_string(),
            reason: "OldReason".to_string(),
            message: "Old message".to_string(),
            last_transition_time: "2024-01-01T00:00:00Z".to_string(),
            observed_generation: Some(1),
        }];

        let conditions = ConditionBuilder::from_existing(existing, Some(2))
            .set_condition("TestCondition", "False", "NewReason", "New message")
            .build();

        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].status, "False");
        // Status changed, so transition time SHOULD change
        assert_ne!(
            conditions[0].last_transition_time,
            "2024-01-01T00:00:00Z".to_string()
        );
    }

    #[test]
    fn test_ready_condition_helper() {
        let conditions = ConditionBuilder::new(Some(1))
            .ready(true, "ClusterReady", "All pods ready")
            .build();

        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].type_, condition_types::READY);
        assert_eq!(conditions[0].status, condition_status::TRUE);
    }

    #[test]
    fn test_ready_condition_false() {
        let conditions = ConditionBuilder::new(Some(1))
            .ready(false, "NotReady", "Pods pending")
            .build();

        assert_eq!(conditions[0].status, condition_status::FALSE);
    }

    #[test]
    fn test_progressing_condition_helper() {
        let conditions = ConditionBuilder::new(Some(1))
            .progressing(true, "Creating", "Creating resources")
            .build();

        assert_eq!(conditions[0].type_, condition_types::PROGRESSING);
        assert_eq!(conditions[0].status, condition_status::TRUE);
    }

    #[test]
    fn test_degraded_condition_helper() {
        let conditions = ConditionBuilder::new(Some(1))
            .degraded(true, "ReplicaDown", "One replica is down")
            .build();

        assert_eq!(conditions[0].type_, condition_types::DEGRADED);
        assert_eq!(conditions[0].status, condition_status::TRUE);
    }

    #[test]
    fn test_multiple_conditions() {
        let conditions = ConditionBuilder::new(Some(1))
            .ready(true, "Ready", "Ready")
            .progressing(false, "Stable", "Stable")
            .degraded(false, "Healthy", "Healthy")
            .build();

        assert_eq!(conditions.len(), 3);
    }

    #[test]
    fn test_condition_types_constants() {
        assert_eq!(condition_types::READY, "Ready");
        assert_eq!(condition_types::PROGRESSING, "Progressing");
        assert_eq!(condition_types::DEGRADED, "Degraded");
        assert_eq!(condition_types::CONFIG_VALID, "ConfigurationValid");
        assert_eq!(condition_types::REPLICAS_READY, "ReplicasReady");
    }

    #[test]
    fn test_condition_status_constants() {
        assert_eq!(condition_status::TRUE, "True");
        assert_eq!(condition_status::FALSE, "False");
        assert_eq!(condition_status::UNKNOWN, "Unknown");
    }
}

mod spec_changed_tests {
    use super::*;

    #[test]
    fn test_spec_changed_no_status() {
        let cluster = create_test_cluster_with_status("test", 1, None);
        // No status means never observed, needs reconciliation
        assert!(spec_changed(&cluster));
    }

    #[test]
    fn test_spec_changed_no_observed_generation() {
        let status = PostgresClusterStatus {
            observed_generation: None,
            ..Default::default()
        };
        let cluster = create_test_cluster_with_status("test", 1, Some(status));
        // No observed generation, needs reconciliation
        assert!(spec_changed(&cluster));
    }

    #[test]
    fn test_spec_changed_generations_match() {
        let status = PostgresClusterStatus {
            observed_generation: Some(5),
            ..Default::default()
        };
        let cluster = create_test_cluster_with_status("test", 5, Some(status));
        // Generations match, no change
        assert!(!spec_changed(&cluster));
    }

    #[test]
    fn test_spec_changed_generation_increased() {
        let status = PostgresClusterStatus {
            observed_generation: Some(5),
            ..Default::default()
        };
        let cluster = create_test_cluster_with_status("test", 6, Some(status));
        // Generation increased, spec changed
        assert!(spec_changed(&cluster));
    }
}

mod get_replica_pod_names_tests {
    use super::*;

    #[test]
    fn test_single_replica() {
        let names = get_replica_pod_names("my-cluster", 1);
        assert_eq!(names, vec!["my-cluster-0"]);
    }

    #[test]
    fn test_three_replicas() {
        let names = get_replica_pod_names("pg-test", 3);
        assert_eq!(names, vec!["pg-test-0", "pg-test-1", "pg-test-2"]);
    }

    #[test]
    fn test_zero_replicas() {
        let names = get_replica_pod_names("empty", 0);
        assert!(names.is_empty());
    }

    #[test]
    fn test_many_replicas() {
        let names = get_replica_pod_names("large", 5);
        assert_eq!(names.len(), 5);
        assert_eq!(names[4], "large-4");
    }
}

mod current_version_tests {
    use super::*;

    #[test]
    fn test_status_with_current_version() {
        let status = PostgresClusterStatus {
            current_version: Some("16".to_string()),
            phase: ClusterPhase::Running,
            ..Default::default()
        };

        assert_eq!(status.current_version, Some("16".to_string()));
    }

    #[test]
    fn test_status_without_current_version() {
        let status = PostgresClusterStatus::default();
        assert!(status.current_version.is_none());
    }

    #[test]
    fn test_status_preserves_version_through_serialization() {
        let status = PostgresClusterStatus {
            current_version: Some("17".to_string()),
            phase: ClusterPhase::Running,
            ready_replicas: 3,
            replicas: 3,
            ..Default::default()
        };

        let json = serde_json::to_string(&status).unwrap();
        let parsed: PostgresClusterStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.current_version, Some("17".to_string()));
    }
}

mod phase_tests {
    use super::*;

    #[test]
    fn test_phase_started_at_field() {
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Creating,
            phase_started_at: Some("2024-01-01T00:00:00Z".to_string()),
            ..Default::default()
        };

        assert_eq!(
            status.phase_started_at,
            Some("2024-01-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_all_phases_exist() {
        // Verify all phases can be created
        let phases = vec![
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

        for phase in phases {
            let status = PostgresClusterStatus {
                phase,
                ..Default::default()
            };
            // Verify phase can be set
            assert_eq!(status.phase, phase);
        }
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(format!("{}", ClusterPhase::Pending), "Pending");
        assert_eq!(format!("{}", ClusterPhase::Creating), "Creating");
        assert_eq!(format!("{}", ClusterPhase::Running), "Running");
        assert_eq!(format!("{}", ClusterPhase::Updating), "Updating");
        assert_eq!(format!("{}", ClusterPhase::Scaling), "Scaling");
        assert_eq!(format!("{}", ClusterPhase::Degraded), "Degraded");
        assert_eq!(format!("{}", ClusterPhase::Recovering), "Recovering");
        assert_eq!(format!("{}", ClusterPhase::Failed), "Failed");
        assert_eq!(format!("{}", ClusterPhase::Deleting), "Deleting");
    }
}

mod error_tracking_tests {
    use super::*;

    #[test]
    fn test_retry_count_tracking() {
        let status = PostgresClusterStatus {
            retry_count: Some(3),
            last_error: Some("Connection failed".to_string()),
            last_error_time: Some("2024-01-01T12:00:00Z".to_string()),
            ..Default::default()
        };

        assert_eq!(status.retry_count, Some(3));
        assert_eq!(status.last_error, Some("Connection failed".to_string()));
        assert!(status.last_error_time.is_some());
    }

    #[test]
    fn test_retry_count_default() {
        let status = PostgresClusterStatus::default();
        assert!(status.retry_count.is_none());
        assert!(status.last_error.is_none());
        assert!(status.last_error_time.is_none());
    }
}
