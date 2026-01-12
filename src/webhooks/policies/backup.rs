//! Backup encryption policy
//!
//! When backup is configured, encryption must be specified.
//! This ensures backups are never stored unencrypted.

use super::{ValidationContext, ValidationResult};

/// Validate backup encryption requirements
///
/// Rule: If `spec.backup` is configured, `spec.backup.encryption.keySecret` must be set.
pub fn validate_backup(ctx: &ValidationContext) -> ValidationResult {
    let spec = &ctx.cluster.spec;

    // If backup is not configured, nothing to validate
    let backup = match &spec.backup {
        Some(b) => b,
        None => return ValidationResult::allowed(),
    };

    // Check if encryption is configured
    match &backup.encryption {
        Some(enc) if !enc.key_secret.is_empty() => ValidationResult::allowed(),
        _ => ValidationResult::denied(
            "BackupEncryptionRequired",
            "Backup encryption is required. Set spec.backup.encryption.keySecret to a Secret containing the encryption key.",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        BackupDestination, BackupSpec, EncryptionSpec, PostgresCluster, PostgresClusterSpec,
        PostgresVersion, RetentionPolicy, StorageSpec, TLSSpec,
    };
    use kube::core::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_cluster(backup: Option<BackupSpec>) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                backup,
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                resources: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
            },
            status: None,
        }
    }

    fn valid_backup_with_encryption() -> BackupSpec {
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
            encryption: Some(EncryptionSpec {
                method: Default::default(),
                key_secret: "backup-key".to_string(),
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

    fn backup_without_encryption() -> BackupSpec {
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

    #[test]
    fn test_no_backup_allowed() {
        let cluster = create_cluster(None);
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_backup(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_backup_with_encryption_allowed() {
        let cluster = create_cluster(Some(valid_backup_with_encryption()));
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_backup(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_backup_without_encryption_denied() {
        let cluster = create_cluster(Some(backup_without_encryption()));
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_backup(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("BackupEncryptionRequired".to_string()));
    }

    #[test]
    fn test_backup_with_empty_key_secret_denied() {
        let mut backup = valid_backup_with_encryption();
        backup.encryption = Some(EncryptionSpec {
            method: Default::default(),
            key_secret: "".to_string(),
        });
        let cluster = create_cluster(Some(backup));
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_backup(&ctx);
        assert!(!result.allowed);
    }
}
