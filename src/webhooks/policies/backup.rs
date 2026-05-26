//! Backup encryption policy
//!
//! When backup is configured, encryption must be specified.
//! This ensures backups are never stored unencrypted.
//!
//! Additionally validates the backup cron schedule format. Invalid schedules
//! were previously passed through to Spilo's crontab, which would silently
//! reject the file and leave the cluster without scheduled backups.

use std::str::FromStr;

use super::{ValidationContext, ValidationResult};

/// vixie-cron compatible shortcut macros accepted in `BackupSpec.schedule`.
///
/// Spilo's crontab supports the standard set of `@`-specials. The upstream
/// `cron` crate only recognizes a subset (no `@annually`, `@midnight`, or
/// `@reboot`), so the webhook accepts them explicitly before falling back to
/// the parser for ordinary 5-field expressions.
const VIXIE_CRON_MACROS: &[&str] = &[
    "@yearly",
    "@annually",
    "@monthly",
    "@weekly",
    "@daily",
    "@midnight",
    "@hourly",
    "@reboot",
];

/// Validate backup configuration: encryption requirement and schedule format.
///
/// Rules:
/// - If `spec.backup` is configured, `spec.backup.encryption.keySecret` must be set.
/// - If `spec.backup` is configured, `spec.backup.schedule` must be a valid
///   cron expression: either a vixie-cron `@`-special or a standard 5-field
///   crontab expression (minute hour day-of-month month day-of-week).
pub fn validate_backup(ctx: &ValidationContext) -> ValidationResult {
    let spec = &ctx.cluster.spec;

    // If backup is not configured, nothing to validate
    let backup = match &spec.backup {
        Some(b) => b,
        None => return ValidationResult::allowed(),
    };

    // Check if encryption is configured
    match &backup.encryption {
        Some(enc) if !enc.key_secret.is_empty() => {}
        _ => {
            return ValidationResult::denied(
                "BackupEncryptionRequired",
                "Backup encryption is required. Set spec.backup.encryption.keySecret to a Secret containing the encryption key.",
            );
        }
    }

    validate_schedule(&backup.schedule)
}

/// Validate a cron schedule string.
///
/// Accepts vixie-cron `@`-specials (`@daily`, `@hourly`, `@reboot`, ...) and
/// standard 5-field crontab expressions. Returns a denial with an actionable
/// message when the schedule is empty or malformed.
fn validate_schedule(schedule: &str) -> ValidationResult {
    let trimmed = schedule.trim();
    if trimmed.is_empty() {
        return ValidationResult::denied(
            "BackupScheduleInvalid",
            "spec.backup.schedule must not be empty. Provide a 5-field cron expression (e.g., \"0 2 * * *\") or a shortcut like \"@daily\".",
        );
    }

    if trimmed.starts_with('@') {
        let lower = trimmed.to_ascii_lowercase();
        if VIXIE_CRON_MACROS.contains(&lower.as_str()) {
            return ValidationResult::allowed();
        }
        return ValidationResult::denied(
            "BackupScheduleInvalid",
            &format!(
                "spec.backup.schedule {schedule:?} is not a recognized cron shortcut. Supported shortcuts: {}.",
                VIXIE_CRON_MACROS.join(", "),
            ),
        );
    }

    let field_count = trimmed.split_whitespace().count();
    if field_count != 5 {
        return ValidationResult::denied(
            "BackupScheduleInvalid",
            &format!(
                "spec.backup.schedule {schedule:?} must have 5 fields (minute hour day-of-month month day-of-week); found {field_count}.",
            ),
        );
    }

    // The `cron` crate parses 6- or 7-field expressions (with seconds and an
    // optional year). Pad the standard 5-field crontab input with a leading
    // "0 " so the seconds field fires once per minute, matching crontab(5)
    // semantics.
    let padded = format!("0 {trimmed}");
    match cron::Schedule::from_str(&padded) {
        Ok(_) => ValidationResult::allowed(),
        Err(err) => ValidationResult::denied(
            "BackupScheduleInvalid",
            &format!("spec.backup.schedule {schedule:?} is not a valid cron expression: {err}"),
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
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

    fn backup_with_schedule(schedule: &str) -> BackupSpec {
        let mut backup = valid_backup_with_encryption();
        backup.schedule = schedule.to_string();
        backup
    }

    fn validate(backup: BackupSpec) -> ValidationResult {
        let cluster = create_cluster(Some(backup));
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        validate_backup(&ctx)
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

    #[test]
    fn test_schedule_valid_five_field() {
        let result = validate(backup_with_schedule("0 2 * * *"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_five_field_complex() {
        // 15-min interval on weekdays, 9am-5pm
        let result = validate(backup_with_schedule("*/15 9-17 * * 1-5"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_daily_macro() {
        let result = validate(backup_with_schedule("@daily"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_hourly_macro() {
        let result = validate(backup_with_schedule("@hourly"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_weekly_macro() {
        let result = validate(backup_with_schedule("@weekly"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_monthly_macro() {
        let result = validate(backup_with_schedule("@monthly"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_yearly_macro() {
        let result = validate(backup_with_schedule("@yearly"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_annually_macro() {
        let result = validate(backup_with_schedule("@annually"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_midnight_macro() {
        let result = validate(backup_with_schedule("@midnight"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_valid_reboot_macro() {
        let result = validate(backup_with_schedule("@reboot"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_macro_case_insensitive() {
        let result = validate(backup_with_schedule("@DAILY"));
        assert!(result.allowed, "expected allowed, got {result:?}");
    }

    #[test]
    fn test_schedule_empty_denied() {
        let result = validate(backup_with_schedule(""));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
        assert!(
            result
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("must not be empty"),
            "message was {:?}",
            result.message,
        );
    }

    #[test]
    fn test_schedule_whitespace_only_denied() {
        let result = validate(backup_with_schedule("   "));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
    }

    #[test]
    fn test_schedule_garbage_denied() {
        let result = validate(backup_with_schedule("not a cron"));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
    }

    #[test]
    fn test_schedule_invalid_minute_field_denied() {
        let result = validate(backup_with_schedule("60 2 * * *"));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
    }

    #[test]
    fn test_schedule_invalid_hour_field_denied() {
        let result = validate(backup_with_schedule("0 24 * * *"));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
    }

    #[test]
    fn test_schedule_too_few_fields_denied() {
        let result = validate(backup_with_schedule("0 2 *"));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
        assert!(
            result
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("5 fields"),
            "message was {:?}",
            result.message,
        );
    }

    #[test]
    fn test_schedule_too_many_fields_denied() {
        // 6-field with seconds is rejected — Spilo expects 5-field crontab.
        let result = validate(backup_with_schedule("0 0 2 * * *"));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
    }

    #[test]
    fn test_schedule_unknown_macro_denied() {
        let result = validate(backup_with_schedule("@bogus"));
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("BackupScheduleInvalid"));
        assert!(
            result
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("@daily"),
            "message was {:?}",
            result.message,
        );
    }

    #[test]
    fn test_schedule_error_message_includes_parse_error() {
        let result = validate(backup_with_schedule("60 2 * * *"));
        let message = result.message.unwrap_or_default();
        assert!(
            message.contains("60 2 * * *"),
            "expected message to echo schedule, got {message:?}",
        );
    }
}
