//! Unit tests for status management

use postgres_operator::controller::cluster_status::{
    ConditionBuilder, condition_status, condition_types, get_replica_pod_names, spec_changed,
};
use postgres_operator::crd::{
    ClusterPhase, Condition, PostgresCluster, PostgresClusterSpec, PostgresClusterStatus,
    PostgresVersion, StorageSpec, TLSSpec,
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
            version: PostgresVersion::V16,
            replicas: 3,
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
            scaling: None,
            network_policy: None,
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

/// Tests for Kubernetes 1.35+ condition helpers (KEP-1287, KEP-5067)
mod k8s_135_condition_tests {
    use super::*;

    #[test]
    fn test_resource_resize_in_progress_condition_type() {
        assert_eq!(
            condition_types::RESOURCE_RESIZE_IN_PROGRESS,
            "ResourceResizeInProgress"
        );
    }

    #[test]
    fn test_pod_generation_synced_condition_type() {
        assert_eq!(
            condition_types::POD_GENERATION_SYNCED,
            "PodGenerationSynced"
        );
    }

    #[test]
    fn test_resource_resize_in_progress_true() {
        let conditions = ConditionBuilder::new(Some(1))
            .resource_resize_in_progress(true, "Resizing", "CPU resize in progress")
            .build();

        assert_eq!(conditions.len(), 1);
        let condition = &conditions[0];
        assert_eq!(condition.type_, "ResourceResizeInProgress");
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "Resizing");
        assert_eq!(condition.message, "CPU resize in progress");
    }

    #[test]
    fn test_resource_resize_in_progress_false() {
        let conditions = ConditionBuilder::new(Some(1))
            .resource_resize_in_progress(false, "NoResize", "No resize operations pending")
            .build();

        assert_eq!(conditions.len(), 1);
        let condition = &conditions[0];
        assert_eq!(condition.type_, "ResourceResizeInProgress");
        assert_eq!(condition.status, "False");
        assert_eq!(condition.reason, "NoResize");
    }

    #[test]
    fn test_pod_generation_synced_true() {
        let conditions = ConditionBuilder::new(Some(1))
            .pod_generation_synced(true, "AllSynced", "All pods have applied the latest spec")
            .build();

        assert_eq!(conditions.len(), 1);
        let condition = &conditions[0];
        assert_eq!(condition.type_, "PodGenerationSynced");
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "AllSynced");
    }

    #[test]
    fn test_pod_generation_synced_false() {
        let conditions = ConditionBuilder::new(Some(1))
            .pod_generation_synced(false, "Pending", "2 of 3 pods synced")
            .build();

        assert_eq!(conditions.len(), 1);
        let condition = &conditions[0];
        assert_eq!(condition.type_, "PodGenerationSynced");
        assert_eq!(condition.status, "False");
        assert_eq!(condition.reason, "Pending");
        assert_eq!(condition.message, "2 of 3 pods synced");
    }

    #[test]
    fn test_combined_k8s_135_conditions() {
        let conditions = ConditionBuilder::new(Some(1))
            .ready(true, "ClusterReady", "Cluster is ready")
            .resource_resize_in_progress(false, "NoResize", "No resize pending")
            .pod_generation_synced(true, "AllSynced", "All pods synced")
            .build();

        assert_eq!(conditions.len(), 3);

        let ready = conditions.iter().find(|c| c.type_ == "Ready").unwrap();
        assert_eq!(ready.status, "True");

        let resize = conditions
            .iter()
            .find(|c| c.type_ == "ResourceResizeInProgress")
            .unwrap();
        assert_eq!(resize.status, "False");

        let synced = conditions
            .iter()
            .find(|c| c.type_ == "PodGenerationSynced")
            .unwrap();
        assert_eq!(synced.status, "True");
    }
}

/// Tests for Kubernetes 1.35+ status fields (pods, resize_status)
mod k8s_135_status_fields_tests {
    use super::*;
    use postgres_operator::crd::{PodInfo, PodResizeStatus, PodResourceResizeStatus, ResourceList};

    #[test]
    fn test_pod_info_creation() {
        let pod_info = PodInfo {
            name: "my-cluster-0".to_string(),
            generation: Some(5),
            observed_generation: Some(5),
            spec_applied: true,
            role: Some("master".to_string()),
            ready: true,
        };

        assert_eq!(pod_info.name, "my-cluster-0");
        assert_eq!(pod_info.generation, Some(5));
        assert_eq!(pod_info.observed_generation, Some(5));
        assert!(pod_info.spec_applied);
        assert_eq!(pod_info.role, Some("master".to_string()));
        assert!(pod_info.ready);
    }

    #[test]
    fn test_pod_info_not_synced() {
        let pod_info = PodInfo {
            name: "my-cluster-1".to_string(),
            generation: Some(6),
            observed_generation: Some(5),
            spec_applied: false,
            role: Some("replica".to_string()),
            ready: true,
        };

        assert!(!pod_info.spec_applied);
        assert_ne!(pod_info.generation, pod_info.observed_generation);
    }

    #[test]
    fn test_pod_resize_status_no_resize() {
        let status = PodResourceResizeStatus {
            pod_name: "my-cluster-0".to_string(),
            status: PodResizeStatus::NoResize,
            allocated_resources: None,
            last_transition_time: None,
            message: None,
        };

        assert_eq!(status.pod_name, "my-cluster-0");
        assert!(matches!(status.status, PodResizeStatus::NoResize));
    }

    #[test]
    fn test_pod_resize_status_in_progress() {
        let status = PodResourceResizeStatus {
            pod_name: "my-cluster-0".to_string(),
            status: PodResizeStatus::InProgress,
            allocated_resources: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("1Gi".to_string()),
            }),
            last_transition_time: Some("2024-01-01T12:00:00Z".to_string()),
            message: Some("Resizing CPU from 250m to 500m".to_string()),
        };

        assert!(matches!(status.status, PodResizeStatus::InProgress));
        assert!(status.allocated_resources.is_some());
        let resources = status.allocated_resources.unwrap();
        assert_eq!(resources.cpu, Some("500m".to_string()));
    }

    #[test]
    fn test_pod_resize_status_deferred() {
        let status = PodResourceResizeStatus {
            pod_name: "my-cluster-0".to_string(),
            status: PodResizeStatus::Deferred,
            allocated_resources: None,
            last_transition_time: None,
            message: Some("Resize deferred due to insufficient resources".to_string()),
        };

        assert!(matches!(status.status, PodResizeStatus::Deferred));
    }

    #[test]
    fn test_pod_resize_status_infeasible() {
        let status = PodResourceResizeStatus {
            pod_name: "my-cluster-0".to_string(),
            status: PodResizeStatus::Infeasible,
            allocated_resources: None,
            last_transition_time: None,
            message: Some("Requested resources exceed node capacity".to_string()),
        };

        assert!(matches!(status.status, PodResizeStatus::Infeasible));
    }

    #[test]
    fn test_status_with_pods_and_resize_status() {
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas: 3,
            replicas: 3,
            pods: vec![
                PodInfo {
                    name: "my-cluster-0".to_string(),
                    generation: Some(5),
                    observed_generation: Some(5),
                    spec_applied: true,
                    role: Some("master".to_string()),
                    ready: true,
                },
                PodInfo {
                    name: "my-cluster-1".to_string(),
                    generation: Some(5),
                    observed_generation: Some(5),
                    spec_applied: true,
                    role: Some("replica".to_string()),
                    ready: true,
                },
            ],
            resize_status: vec![],
            all_pods_synced: Some(true),
            ..Default::default()
        };

        assert_eq!(status.pods.len(), 2);
        assert!(status.resize_status.is_empty());
        assert_eq!(status.all_pods_synced, Some(true));
    }

    #[test]
    fn test_status_with_resize_in_progress() {
        let status = PostgresClusterStatus {
            phase: ClusterPhase::Running,
            ready_replicas: 3,
            replicas: 3,
            resize_status: vec![PodResourceResizeStatus {
                pod_name: "my-cluster-0".to_string(),
                status: PodResizeStatus::InProgress,
                allocated_resources: Some(ResourceList {
                    cpu: Some("1".to_string()),
                    memory: Some("2Gi".to_string()),
                }),
                last_transition_time: Some("2024-01-01T12:00:00Z".to_string()),
                message: None,
            }],
            all_pods_synced: Some(true),
            ..Default::default()
        };

        assert_eq!(status.resize_status.len(), 1);
        assert!(matches!(
            status.resize_status[0].status,
            PodResizeStatus::InProgress
        ));
    }
}

/// Tests for ResourceRequirements with restart_on_resize field
mod resource_requirements_tests {
    use postgres_operator::crd::{ResourceList, ResourceRequirements};

    #[test]
    fn test_resource_requirements_default_restart_on_resize() {
        let resources = ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("1Gi".to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some("2".to_string()),
                memory: Some("4Gi".to_string()),
            }),
            restart_on_resize: None,
        };

        // Default is None, which means in-place resize (no restart)
        assert!(resources.restart_on_resize.is_none());
    }

    #[test]
    fn test_resource_requirements_restart_on_resize_false() {
        let resources = ResourceRequirements {
            requests: None,
            limits: None,
            restart_on_resize: Some(false),
        };

        assert_eq!(resources.restart_on_resize, Some(false));
    }

    #[test]
    fn test_resource_requirements_restart_on_resize_true() {
        let resources = ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("1".to_string()),
                memory: Some("2Gi".to_string()),
            }),
            limits: None,
            restart_on_resize: Some(true),
        };

        assert_eq!(resources.restart_on_resize, Some(true));
    }

    #[test]
    fn test_resource_requirements_serialization() {
        let resources = ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("1Gi".to_string()),
            }),
            limits: None,
            restart_on_resize: Some(true),
        };

        let json = serde_json::to_string(&resources).unwrap();
        assert!(json.contains("restartOnResize"));
        assert!(json.contains("true"));
    }

    #[test]
    fn test_resource_requirements_deserialization() {
        let json = r#"{
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "restartOnResize": false
        }"#;

        let resources: ResourceRequirements = serde_json::from_str(json).unwrap();
        assert_eq!(resources.restart_on_resize, Some(false));
        assert!(resources.requests.is_some());
    }

    #[test]
    fn test_resource_requirements_skip_serializing_none() {
        let resources = ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: None,
            }),
            limits: None,
            restart_on_resize: None,
        };

        let json = serde_json::to_string(&resources).unwrap();
        // restartOnResize should not appear when None
        assert!(!json.contains("restartOnResize"));
    }
}
