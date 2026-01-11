//! Unit tests for admission webhook validation
//!
//! These tests use the public webhook API (ValidationContext and validate_all)
//! to verify policy enforcement from an external perspective.
//!
//! Note: Internal policy tests are in src/webhooks/policies/*.rs

use postgres_operator::crd::{
    BackupDestination, BackupSpec, EncryptionSpec, IssuerKind, IssuerRef, NetworkPolicySpec,
    PostgresCluster, PostgresClusterSpec, PostgresVersion, ResourceList, ResourceRequirements,
    RetentionPolicy, StorageSpec, TLSSpec,
};
use std::collections::BTreeMap;

// =============================================================================
// Helper Functions
// =============================================================================

/// Create a minimal test cluster
fn create_test_cluster() -> PostgresCluster {
    PostgresCluster {
        metadata: kube::core::ObjectMeta {
            name: Some("test-cluster".to_string()),
            namespace: Some("test-ns".to_string()),
            uid: Some("test-uid".to_string()),
            ..Default::default()
        },
        spec: PostgresClusterSpec {
            version: PostgresVersion::V16,
            replicas: 3,
            storage: StorageSpec {
                size: "10Gi".to_string(),
                storage_class: Some("standard".to_string()),
            },
            labels: Default::default(),
            resources: None,
            postgresql_params: Default::default(),
            backup: None,
            pgbouncer: None,
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
            network_policy: None,
        },
        status: None,
    }
}

/// Create a valid backup spec with encryption
fn create_valid_backup() -> BackupSpec {
    BackupSpec {
        schedule: "0 2 * * *".to_string(),
        retention: RetentionPolicy {
            count: Some(7),
            max_age: None,
        },
        destination: BackupDestination::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            credentials_secret: "aws-creds".to_string(),
            path: None,
            disable_sse: false,
            force_path_style: false,
        },
        encryption: Some(EncryptionSpec {
            method: Default::default(),
            key_secret: "encryption-key".to_string(),
        }),
        wal_archiving: None,
        compression: None,
        backup_from_replica: false,
        upload_concurrency: None,
        download_concurrency: None,
        enable_delta_backups: false,
        delta_max_steps: None,
    }
}

/// Create a backup spec without encryption
fn create_backup_without_encryption() -> BackupSpec {
    BackupSpec {
        schedule: "0 2 * * *".to_string(),
        retention: RetentionPolicy {
            count: Some(7),
            max_age: None,
        },
        destination: BackupDestination::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            credentials_secret: "aws-creds".to_string(),
            path: None,
            disable_sse: false,
            force_path_style: false,
        },
        encryption: None,
        wal_archiving: None,
        compression: None,
        backup_from_replica: false,
        upload_concurrency: None,
        download_concurrency: None,
        enable_delta_backups: false,
        delta_max_steps: None,
    }
}

/// Create valid resource limits
fn create_valid_resources() -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(ResourceList {
            cpu: Some("1".to_string()),
            memory: Some("2Gi".to_string()),
        }),
        limits: Some(ResourceList {
            cpu: Some("2".to_string()),
            memory: Some("4Gi".to_string()),
        }),
        restart_on_resize: None,
    }
}

/// Create a production-ready cluster
fn create_production_cluster() -> PostgresCluster {
    let mut cluster = create_test_cluster();
    cluster.spec.replicas = 3;
    cluster.spec.resources = Some(create_valid_resources());
    cluster.spec.backup = Some(create_valid_backup());
    cluster.spec.network_policy = Some(NetworkPolicySpec {
        allow_external_access: false,
        allow_from: vec![],
    });
    cluster
}

/// Create production namespace labels
fn production_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("env".to_string(), "production".to_string())])
}

/// Create non-production namespace labels
fn dev_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("env".to_string(), "development".to_string())])
}

// =============================================================================
// Backup Policy Integration Tests
// =============================================================================

mod backup_policy_integration_tests {
    use super::*;
    use postgres_operator::webhooks::policies::{ValidationContext, validate_all};

    #[test]
    fn test_no_backup_passes_validation() {
        let cluster = create_test_cluster();
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(
            result.allowed,
            "Cluster without backup should pass validation"
        );
    }

    #[test]
    fn test_backup_with_encryption_passes() {
        let mut cluster = create_test_cluster();
        cluster.spec.backup = Some(create_valid_backup());
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "Backup with encryption should pass");
    }

    #[test]
    fn test_backup_without_encryption_fails() {
        let mut cluster = create_test_cluster();
        cluster.spec.backup = Some(create_backup_without_encryption());
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(
            !result.allowed,
            "Backup without encryption should be denied"
        );
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("Encryption"))
                .unwrap_or(false),
            "Error reason should mention encryption"
        );
    }
}

// =============================================================================
// TLS Policy Integration Tests
// =============================================================================

mod tls_policy_integration_tests {
    use super::*;
    use postgres_operator::webhooks::policies::{ValidationContext, validate_all};

    #[test]
    fn test_tls_disabled_passes() {
        let cluster = create_test_cluster();
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "TLS disabled should pass");
    }

    #[test]
    fn test_tls_enabled_with_issuer_passes() {
        let mut cluster = create_test_cluster();
        cluster.spec.tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "letsencrypt-prod".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "TLS with issuer should pass");
    }

    #[test]
    fn test_tls_enabled_without_issuer_fails() {
        let mut cluster = create_test_cluster();
        cluster.spec.tls = TLSSpec {
            enabled: true,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(!result.allowed, "TLS without issuer should fail");
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("Issuer"))
                .unwrap_or(false),
            "Error should mention issuer"
        );
    }
}

// =============================================================================
// Immutability Policy Integration Tests
// =============================================================================

mod immutability_policy_integration_tests {
    use super::*;
    use postgres_operator::webhooks::policies::{ValidationContext, validate_all};

    #[test]
    fn test_create_always_allowed() {
        let cluster = create_test_cluster();
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(
            result.allowed,
            "Create should always be allowed (barring other violations)"
        );
    }

    #[test]
    fn test_version_upgrade_allowed() {
        let mut old_cluster = create_test_cluster();
        old_cluster.spec.version = PostgresVersion::V15;

        let mut new_cluster = create_test_cluster();
        new_cluster.spec.version = PostgresVersion::V16;

        let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "Version upgrade should be allowed");
    }

    #[test]
    fn test_version_downgrade_denied() {
        let mut old_cluster = create_test_cluster();
        old_cluster.spec.version = PostgresVersion::V16;

        let mut new_cluster = create_test_cluster();
        new_cluster.spec.version = PostgresVersion::V15;

        let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(!result.allowed, "Version downgrade should be denied");
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("Downgrade"))
                .unwrap_or(false),
            "Error should mention downgrade"
        );
    }

    #[test]
    fn test_storage_class_change_denied() {
        let old_cluster = create_test_cluster();

        let mut new_cluster = create_test_cluster();
        new_cluster.spec.storage.storage_class = Some("fast-ssd".to_string());

        let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(!result.allowed, "Storage class change should be denied");
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("StorageClass"))
                .unwrap_or(false),
            "Error should mention storage class"
        );
    }

    #[test]
    fn test_replica_change_allowed() {
        let old_cluster = create_test_cluster();

        let mut new_cluster = create_test_cluster();
        new_cluster.spec.replicas = 5;

        let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "Replica change should be allowed");
    }

    #[test]
    fn test_storage_size_change_allowed() {
        let old_cluster = create_test_cluster();

        let mut new_cluster = create_test_cluster();
        new_cluster.spec.storage.size = "100Gi".to_string();

        let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "Storage size change should be allowed");
    }
}

// =============================================================================
// Production Policy Integration Tests
// =============================================================================

mod production_policy_integration_tests {
    use super::*;
    use postgres_operator::webhooks::policies::{ValidationContext, validate_all};

    #[test]
    fn test_valid_production_cluster_passes() {
        let cluster = create_production_cluster();
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_all(&ctx);
        assert!(result.allowed, "Valid production cluster should pass");
    }

    #[test]
    fn test_non_production_skips_production_rules() {
        let mut cluster = create_test_cluster();
        cluster.spec.replicas = 1; // Would fail in production

        let ctx = ValidationContext::new(&cluster, None, dev_labels());
        let result = validate_all(&ctx);
        assert!(
            result.allowed,
            "Non-production namespace should skip production rules"
        );
    }

    #[test]
    fn test_production_replicas_too_low() {
        let mut cluster = create_production_cluster();
        cluster.spec.replicas = 1;

        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_all(&ctx);
        assert!(!result.allowed, "1 replica should fail in production");
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("HA"))
                .unwrap_or(false),
            "Error should mention HA"
        );
    }

    #[test]
    fn test_production_missing_backup() {
        let mut cluster = create_production_cluster();
        cluster.spec.backup = None;

        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_all(&ctx);
        assert!(!result.allowed, "Missing backup should fail in production");
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("Backup"))
                .unwrap_or(false),
            "Error should mention backup"
        );
    }

    #[test]
    fn test_production_missing_resources() {
        let mut cluster = create_production_cluster();
        cluster.spec.resources = None;

        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_all(&ctx);
        assert!(
            !result.allowed,
            "Missing resources should fail in production"
        );
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("Resource"))
                .unwrap_or(false),
            "Error should mention resources"
        );
    }

    #[test]
    fn test_production_external_access_denied() {
        let mut cluster = create_production_cluster();
        cluster.spec.network_policy = Some(NetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![],
        });

        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_all(&ctx);
        assert!(
            !result.allowed,
            "External access should be denied in production"
        );
        assert!(
            result
                .reason
                .as_ref()
                .map(|r| r.contains("External"))
                .unwrap_or(false),
            "Error should mention external access"
        );
    }
}

// =============================================================================
// Combined Validation Tests
// =============================================================================

mod combined_validation_tests {
    use super::*;
    use postgres_operator::webhooks::policies::{ValidationContext, validate_all};

    #[test]
    fn test_all_policies_pass_for_fully_configured_cluster() {
        let mut cluster = create_production_cluster();
        cluster.spec.tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "letsencrypt-prod".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };

        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_all(&ctx);
        assert!(
            result.allowed,
            "Fully configured cluster should pass all policies"
        );
    }

    #[test]
    fn test_first_failure_is_returned() {
        // Create a cluster with multiple policy violations
        let mut cluster = create_test_cluster();
        cluster.spec.backup = Some(create_backup_without_encryption()); // Fails backup policy
        cluster.spec.tls = TLSSpec {
            enabled: true,
            issuer_ref: None, // Would fail TLS policy
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };

        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);

        // Should fail on first tier-1 policy (backup)
        assert!(!result.allowed);
        // The first failure determines the reason (order depends on implementation)
        assert!(result.reason.is_some());
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;
    use postgres_operator::webhooks::policies::{ValidationContext, validate_all};

    #[test]
    fn test_empty_namespace_labels() {
        let cluster = create_test_cluster();
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        // Empty labels means not production, so production rules don't apply
        assert!(result.allowed);
    }

    #[test]
    fn test_issuer_with_whitespace_name() {
        let mut cluster = create_test_cluster();
        cluster.spec.tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "  letsencrypt  ".to_string(), // Has whitespace
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };

        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        // Whitespace-padded name is still non-empty
        assert!(result.allowed);
    }

    #[test]
    fn test_all_postgres_versions_valid_for_create() {
        for version in [
            PostgresVersion::V15,
            PostgresVersion::V16,
            PostgresVersion::V17,
        ] {
            let mut cluster = create_test_cluster();
            cluster.spec.version = version.clone();

            let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
            let result = validate_all(&ctx);
            assert!(
                result.allowed,
                "Version {:?} should be valid for create",
                version
            );
        }
    }

    #[test]
    fn test_v15_to_v17_upgrade_allowed() {
        let mut old_cluster = create_test_cluster();
        old_cluster.spec.version = PostgresVersion::V15;

        let mut new_cluster = create_test_cluster();
        new_cluster.spec.version = PostgresVersion::V17;

        let ctx = ValidationContext::new(&new_cluster, Some(&old_cluster), BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(result.allowed, "Multi-version upgrade should be allowed");
    }

    #[test]
    fn test_backup_with_empty_key_secret_denied() {
        let mut cluster = create_test_cluster();
        cluster.spec.backup = Some(BackupSpec {
            schedule: "0 2 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(7),
                max_age: None,
            },
            destination: BackupDestination::S3 {
                bucket: "my-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                credentials_secret: "aws-creds".to_string(),
                path: None,
                disable_sse: false,
                force_path_style: false,
            },
            encryption: Some(EncryptionSpec {
                method: Default::default(),
                key_secret: "".to_string(), // Empty!
            }),
            wal_archiving: None,
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        });

        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_all(&ctx);
        assert!(!result.allowed, "Empty key secret should be denied");
    }
}
