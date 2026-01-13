//! Backup configuration for PostgreSQL clusters
//!
//! This module generates backup-related configuration for Spilo/WAL-G:
//! - Environment variables for S3/S3-compatible storage credentials
//! - WAL-G configuration for continuous archiving
//! - Backup scheduling via Spilo's built-in cron
//!
//! # Architecture
//!
//! Spilo (the container image) includes WAL-G and handles:
//! - Continuous WAL archiving to S3-compatible storage
//! - Scheduled base backups via cron
//! - Automatic backup retention management
//!
//! We configure backups by setting environment variables on the StatefulSet pods.
//! No additional CronJobs or sidecars are needed.
//!
//! # Cloud Provider Configuration
//!
//! ## AWS S3 / S3-Compatible Storage
//! Environment variables:
//! - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` - credentials
//! - `AWS_REGION` - bucket region
//! - `WALE_S3_PREFIX` / `WALG_S3_PREFIX` - backup path
//! - `AWS_ENDPOINT` - custom endpoint (for S3-compatible storage like MinIO)
//!
//! # References
//! - WAL-G: https://github.com/wal-g/wal-g
//! - Spilo: https://github.com/zalando/spilo

use k8s_openapi::api::core::v1::{
    EnvVar, EnvVarSource, KeyToPath, SecretKeySelector, SecretVolumeSource, Volume, VolumeMount,
};

use crate::crd::{
    BackupDestination, EncryptionMethod, PostgresCluster, RecoveryTarget, RestoreSource,
};

/// Encryption key mount path
const ENCRYPTION_KEY_PATH: &str = "/var/secrets/backup-encryption";

/// Parse a duration string (e.g., "7d", "30d", "1w", "2m") to days.
///
/// Supported suffixes:
/// - `d` or `D`: days
/// - `w` or `W`: weeks (7 days)
/// - `m` or `M`: months (30 days, approximate)
///
/// Returns `None` if the format is invalid.
fn parse_duration_to_days(duration: &str) -> Option<i32> {
    let duration = duration.trim();
    if duration.is_empty() {
        return None;
    }

    let (num_str, suffix) = duration.split_at(duration.len() - 1);
    let num: i32 = num_str.parse().ok()?;

    match suffix.to_lowercase().as_str() {
        "d" => Some(num),
        "w" => Some(num * 7),
        "m" => Some(num * 30), // Approximate month
        _ => None,
    }
}

/// Generate backup-related environment variables for Spilo/WAL-G.
///
/// Returns a vector of environment variables to add to the StatefulSet pod spec.
/// These configure WAL-G for continuous archiving and scheduled base backups.
pub fn generate_backup_env_vars(cluster: &PostgresCluster) -> Vec<EnvVar> {
    let Some(ref backup) = cluster.spec.backup else {
        return Vec::new();
    };

    let cluster_name = cluster
        .metadata
        .name
        .as_deref()
        .unwrap_or("postgres-cluster");
    let namespace = cluster.metadata.namespace.as_deref().unwrap_or("default");

    let mut env_vars = Vec::new();

    // Enable WAL-G for backup and restore
    env_vars.push(EnvVar {
        name: "USE_WALG_BACKUP".to_string(),
        value: Some("true".to_string()),
        ..Default::default()
    });
    env_vars.push(EnvVar {
        name: "USE_WALG_RESTORE".to_string(),
        value: Some("true".to_string()),
        ..Default::default()
    });

    // Backup schedule
    env_vars.push(EnvVar {
        name: "BACKUP_SCHEDULE".to_string(),
        value: Some(backup.schedule.clone()),
        ..Default::default()
    });

    // Backup from replica if configured
    if backup.backup_from_replica {
        env_vars.push(EnvVar {
            name: "WALG_BACKUP_FROM_REPLICA".to_string(),
            value: Some("true".to_string()),
            ..Default::default()
        });
    }

    // Compression method
    env_vars.push(EnvVar {
        name: "WALG_COMPRESSION_METHOD".to_string(),
        value: Some(backup.compression_method().to_string()),
        ..Default::default()
    });

    // Concurrency settings
    env_vars.push(EnvVar {
        name: "WALG_UPLOAD_CONCURRENCY".to_string(),
        value: Some(backup.upload_concurrency().to_string()),
        ..Default::default()
    });
    env_vars.push(EnvVar {
        name: "WALG_DOWNLOAD_CONCURRENCY".to_string(),
        value: Some(backup.download_concurrency().to_string()),
        ..Default::default()
    });

    // Delta backups
    if backup.enable_delta_backups {
        env_vars.push(EnvVar {
            name: "WALG_DELTA_MAX_STEPS".to_string(),
            value: Some(backup.delta_max_steps().to_string()),
            ..Default::default()
        });
        env_vars.push(EnvVar {
            name: "WALG_DELTA_ORIGIN".to_string(),
            value: Some("LATEST_FULL".to_string()),
            ..Default::default()
        });
    }

    // WAL restore timeout
    if let Some(ref wal_archiving) = backup.wal_archiving
        && let Some(timeout) = wal_archiving.restore_timeout
        && timeout > 0
    {
        env_vars.push(EnvVar {
            name: "WAL_RESTORE_TIMEOUT".to_string(),
            value: Some(format!("{}s", timeout)),
            ..Default::default()
        });
    }

    // Retention settings via WAL-G
    // Spilo handles retention via BACKUP_NUM_TO_RETAIN
    // WAL-G also supports WALG_RETAIN_FULL_BACKUPS
    if let Some(count) = backup.retention.count {
        env_vars.push(EnvVar {
            name: "BACKUP_NUM_TO_RETAIN".to_string(),
            value: Some(count.to_string()),
            ..Default::default()
        });
        // WAL-G native retention setting
        env_vars.push(EnvVar {
            name: "WALG_RETAIN_FULL_BACKUPS".to_string(),
            value: Some(count.to_string()),
            ..Default::default()
        });
    }

    // Time-based retention
    // Note: Spilo's backup script uses BACKUP_NUM_TO_RETAIN primarily.
    // For time-based cleanup, users can run `wal-g delete retain FULL <count> --confirm`
    // or use WALG_BACKUP_RETENTION_DAYS with custom scripts
    if let Some(ref max_age) = backup.retention.max_age
        && let Some(days) = parse_duration_to_days(max_age)
    {
        env_vars.push(EnvVar {
            name: "WALG_BACKUP_RETENTION_DAYS".to_string(),
            value: Some(days.to_string()),
            ..Default::default()
        });
    }

    // Destination-specific configuration
    match &backup.destination {
        BackupDestination::S3 {
            bucket,
            region,
            endpoint,
            credentials_secret,
            path,
            force_path_style,
        } => {
            let prefix = backup.destination.wal_g_prefix(cluster_name, namespace);

            // WAL-G S3 prefix (primary)
            env_vars.push(EnvVar {
                name: "WALG_S3_PREFIX".to_string(),
                value: Some(prefix.clone()),
                ..Default::default()
            });

            // WAL-E compatibility (used by some Spilo scripts)
            env_vars.push(EnvVar {
                name: "WALE_S3_PREFIX".to_string(),
                value: Some(prefix),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "WAL_S3_BUCKET".to_string(),
                value: Some(bucket.clone()),
                ..Default::default()
            });

            // AWS region
            env_vars.push(EnvVar {
                name: "AWS_REGION".to_string(),
                value: Some(region.clone()),
                ..Default::default()
            });

            // Custom endpoint for S3-compatible storage
            if let Some(endpoint_url) = endpoint {
                env_vars.push(EnvVar {
                    name: "AWS_ENDPOINT".to_string(),
                    value: Some(endpoint_url.clone()),
                    ..Default::default()
                });
                env_vars.push(EnvVar {
                    name: "WALE_S3_ENDPOINT".to_string(),
                    value: Some(format!(
                        "{}+path://{}:{}",
                        if endpoint_url.starts_with("https") {
                            "https"
                        } else {
                            "http"
                        },
                        endpoint_url
                            .trim_start_matches("https://")
                            .trim_start_matches("http://")
                            .split(':')
                            .next()
                            .unwrap_or("localhost"),
                        endpoint_url.split(':').nth(2).unwrap_or("9000")
                    )),
                    ..Default::default()
                });
            }

            // Force path style (for MinIO and similar)
            if *force_path_style {
                env_vars.push(EnvVar {
                    name: "AWS_S3_FORCE_PATH_STYLE".to_string(),
                    value: Some("true".to_string()),
                    ..Default::default()
                });
            }

            // Store the path for reference
            if let Some(p) = path {
                env_vars.push(EnvVar {
                    name: "BACKUP_PATH".to_string(),
                    value: Some(p.clone()),
                    ..Default::default()
                });
            }

            // AWS credentials from secret
            env_vars.push(EnvVar {
                name: "AWS_ACCESS_KEY_ID".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AWS_ACCESS_KEY_ID".to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "AWS_SECRET_ACCESS_KEY".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AWS_SECRET_ACCESS_KEY".to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            // Optional session token for temporary credentials
            env_vars.push(EnvVar {
                name: "AWS_SESSION_TOKEN".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AWS_SESSION_TOKEN".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    // Encryption configuration
    // Presence of encryption section means encryption is enabled
    if let Some(ref encryption) = backup.encryption {
        match encryption.method {
            EncryptionMethod::Aes256 => {
                // libsodium key from secret (base64 encoded)
                // Using WALG_LIBSODIUM_KEY with secretKeyRef instead of KEY_PATH
                // because KEY_PATH doesn't work correctly in WAL-G v3.x
                env_vars.push(EnvVar {
                    name: "WALG_LIBSODIUM_KEY".to_string(),
                    value_from: Some(EnvVarSource {
                        secret_key_ref: Some(SecretKeySelector {
                            name: encryption.key_secret.clone(),
                            key: "encryption-key".to_string(),
                            optional: Some(false),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                // Tell WAL-G the key is base64 encoded
                env_vars.push(EnvVar {
                    name: "WALG_LIBSODIUM_KEY_TRANSFORM".to_string(),
                    value: Some("base64".to_string()),
                    ..Default::default()
                });
            }
            EncryptionMethod::Pgp => {
                // PGP key path
                env_vars.push(EnvVar {
                    name: "WALG_PGP_KEY_PATH".to_string(),
                    value: Some(format!("{}/pgp-key", ENCRYPTION_KEY_PATH)),
                    ..Default::default()
                });
            }
        }
    }

    env_vars
}

/// Generate restore-related environment variables for Spilo/WAL-G clone.
///
/// This configures Spilo's CLONE_WITH_WALG functionality to bootstrap
/// the cluster from an existing backup. These variables are only used
/// during initial cluster creation.
///
/// Returns a vector of environment variables to add to the StatefulSet pod spec.
pub fn generate_restore_env_vars(cluster: &PostgresCluster) -> Vec<EnvVar> {
    let Some(ref restore) = cluster.spec.restore else {
        return Vec::new();
    };

    let mut env_vars = Vec::new();

    // Enable WAL-G clone method
    env_vars.push(EnvVar {
        name: "CLONE_METHOD".to_string(),
        value: Some("CLONE_WITH_WALG".to_string()),
        ..Default::default()
    });
    env_vars.push(EnvVar {
        name: "CLONE_WITH_WALG".to_string(),
        value: Some("true".to_string()),
        ..Default::default()
    });

    // Configure source-specific variables
    match &restore.source {
        RestoreSource::S3 {
            prefix,
            region,
            credentials_secret,
            endpoint,
        } => {
            env_vars.push(EnvVar {
                name: "CLONE_WALG_S3_PREFIX".to_string(),
                value: Some(prefix.clone()),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "CLONE_AWS_REGION".to_string(),
                value: Some(region.clone()),
                ..Default::default()
            });

            // Custom endpoint for S3-compatible storage
            if let Some(endpoint_url) = endpoint {
                env_vars.push(EnvVar {
                    name: "CLONE_AWS_ENDPOINT".to_string(),
                    value: Some(endpoint_url.clone()),
                    ..Default::default()
                });
            }

            // AWS credentials from secret
            env_vars.push(EnvVar {
                name: "CLONE_AWS_ACCESS_KEY_ID".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AWS_ACCESS_KEY_ID".to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "CLONE_AWS_SECRET_ACCESS_KEY".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AWS_SECRET_ACCESS_KEY".to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    // Recovery target configuration
    if let Some(ref target) = restore.recovery_target {
        match target {
            RecoveryTarget::Time(time) => {
                env_vars.push(EnvVar {
                    name: "CLONE_TARGET_TIME".to_string(),
                    value: Some(time.clone()),
                    ..Default::default()
                });
            }
            RecoveryTarget::Backup(backup_name) => {
                // WAL-G uses BACKUP_NAME for specific backup restore
                env_vars.push(EnvVar {
                    name: "CLONE_TARGET_BACKUP".to_string(),
                    value: Some(backup_name.clone()),
                    ..Default::default()
                });
            }
            RecoveryTarget::Timeline(timeline) => {
                env_vars.push(EnvVar {
                    name: "CLONE_TARGET_TIMELINE".to_string(),
                    value: Some(timeline.to_string()),
                    ..Default::default()
                });
            }
        }
    }

    env_vars
}

/// Generate restore-related volumes for the StatefulSet.
///
/// S3 doesn't require additional volumes - credentials are passed via environment variables.
pub fn generate_restore_volumes(_cluster: &PostgresCluster) -> Vec<Volume> {
    // S3 restore uses environment variables for credentials, no volumes needed
    Vec::new()
}

/// Generate restore-related volume mounts for the container.
///
/// S3 doesn't require additional volume mounts - credentials are passed via environment variables.
pub fn generate_restore_volume_mounts(_cluster: &PostgresCluster) -> Vec<VolumeMount> {
    // S3 restore uses environment variables for credentials, no mounts needed
    Vec::new()
}

/// Check if restore is configured for a cluster.
pub fn is_restore_configured(cluster: &PostgresCluster) -> bool {
    cluster.spec.restore.is_some()
}

/// Generate backup-related volumes for the StatefulSet.
///
/// This creates volumes for encryption keys (PGP only - AES256/libsodium uses env vars).
/// S3 credentials are passed via environment variables, no volumes needed.
pub fn generate_backup_volumes(cluster: &PostgresCluster) -> Vec<Volume> {
    let Some(ref backup) = cluster.spec.backup else {
        return Vec::new();
    };

    let mut volumes = Vec::new();

    // Encryption key volume - only needed for PGP (AES256/libsodium uses env var)
    if let Some(ref encryption) = backup.encryption
        && matches!(encryption.method, EncryptionMethod::Pgp)
    {
        volumes.push(Volume {
            name: "backup-encryption-key".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(encryption.key_secret.clone()),
                items: Some(vec![KeyToPath {
                    key: "pgp-key".to_string(),
                    path: "pgp-key".to_string(),
                    mode: Some(0o400),
                }]),
                default_mode: Some(0o400),
                optional: Some(false),
            }),
            ..Default::default()
        });
    }

    volumes
}

/// Generate backup-related volume mounts for the container.
///
/// S3 credentials are passed via environment variables, no mounts needed.
/// Only PGP encryption requires a volume mount for the key file.
pub fn generate_backup_volume_mounts(cluster: &PostgresCluster) -> Vec<VolumeMount> {
    let Some(ref backup) = cluster.spec.backup else {
        return Vec::new();
    };

    let mut mounts = Vec::new();

    // Encryption key mount - only needed for PGP (AES256/libsodium uses env var)
    if let Some(ref encryption) = backup.encryption
        && matches!(encryption.method, EncryptionMethod::Pgp)
    {
        mounts.push(VolumeMount {
            name: "backup-encryption-key".to_string(),
            mount_path: ENCRYPTION_KEY_PATH.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    mounts
}

/// Check if backup is configured for a cluster.
pub fn is_backup_enabled(cluster: &PostgresCluster) -> bool {
    cluster.spec.backup.is_some()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::crd::{
        BackupDestination, BackupSpec, EncryptionMethod, EncryptionSpec, PostgresCluster,
        PostgresClusterSpec, PostgresVersion, RetentionPolicy, StorageSpec, TLSSpec,
    };
    use kube::core::ObjectMeta;

    fn create_test_cluster(backup: Option<BackupSpec>) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("test-ns".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                resources: None,
                postgresql_params: Default::default(),
                labels: Default::default(),
                backup,
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

    #[test]
    fn test_no_backup_returns_empty() {
        let cluster = create_test_cluster(None);
        let env_vars = generate_backup_env_vars(&cluster);
        assert!(env_vars.is_empty());
    }

    #[test]
    fn test_s3_backup_env_vars() {
        let backup = BackupSpec {
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
                force_path_style: false,
            },
            wal_archiving: None,
            encryption: None,
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        };

        let cluster = create_test_cluster(Some(backup));
        let env_vars = generate_backup_env_vars(&cluster);

        // Check that required env vars are present
        assert!(env_vars.iter().any(|e| e.name == "USE_WALG_BACKUP"));
        assert!(env_vars.iter().any(|e| e.name == "WALG_S3_PREFIX"));
        assert!(env_vars.iter().any(|e| e.name == "AWS_REGION"));
        assert!(env_vars.iter().any(|e| e.name == "AWS_ACCESS_KEY_ID"));
        assert!(env_vars.iter().any(|e| e.name == "BACKUP_SCHEDULE"));

        // Check S3 prefix format
        let prefix_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_S3_PREFIX")
            .expect("WALG_S3_PREFIX env var should be present for S3 backup");
        assert_eq!(
            prefix_var.value.as_deref(),
            Some("s3://my-bucket/test-ns/test-cluster")
        );
    }

    #[test]
    fn test_encryption_env_vars() {
        let backup = BackupSpec {
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
                force_path_style: false,
            },
            wal_archiving: None,
            encryption: Some(EncryptionSpec {
                method: EncryptionMethod::Aes256,
                key_secret: "encryption-key-secret".to_string(),
            }),
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        };

        let cluster = create_test_cluster(Some(backup));
        let env_vars = generate_backup_env_vars(&cluster);

        // AES256 encryption uses WALG_LIBSODIUM_KEY env var (not KEY_PATH) with base64 transform
        let key_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_LIBSODIUM_KEY")
            .expect("WALG_LIBSODIUM_KEY should be present");
        assert!(
            key_var.value_from.is_some(),
            "WALG_LIBSODIUM_KEY should use secretKeyRef"
        );
        let secret_ref = key_var
            .value_from
            .as_ref()
            .unwrap()
            .secret_key_ref
            .as_ref()
            .expect("Should have secretKeyRef");
        assert_eq!(secret_ref.name, "encryption-key-secret");
        assert_eq!(secret_ref.key, "encryption-key");

        let transform_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_LIBSODIUM_KEY_TRANSFORM")
            .expect("WALG_LIBSODIUM_KEY_TRANSFORM should be present");
        assert_eq!(transform_var.value.as_deref(), Some("base64"));
    }

    #[test]
    fn test_backup_destination_prefix() {
        let s3_dest = BackupDestination::S3 {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            credentials_secret: "creds".to_string(),
            path: None,
            force_path_style: false,
        };
        assert_eq!(
            s3_dest.wal_g_prefix("my-cluster", "my-ns"),
            "s3://bucket/my-ns/my-cluster"
        );

        let s3_with_path = BackupDestination::S3 {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            credentials_secret: "creds".to_string(),
            path: Some("custom/path".to_string()),
            force_path_style: false,
        };
        assert_eq!(
            s3_with_path.wal_g_prefix("my-cluster", "my-ns"),
            "s3://bucket/custom/path"
        );
    }

    #[test]
    fn test_is_backup_enabled() {
        let cluster_without_backup = create_test_cluster(None);
        assert!(!is_backup_enabled(&cluster_without_backup));

        let backup = BackupSpec {
            schedule: "0 2 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(7),
                max_age: None,
            },
            destination: BackupDestination::S3 {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                credentials_secret: "creds".to_string(),
                path: None,
                force_path_style: false,
            },
            wal_archiving: None,
            encryption: None,
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        };
        let cluster_with_backup = create_test_cluster(Some(backup));
        assert!(is_backup_enabled(&cluster_with_backup));
    }

    #[test]
    fn test_parse_duration_to_days() {
        // Days
        assert_eq!(parse_duration_to_days("7d"), Some(7));
        assert_eq!(parse_duration_to_days("30d"), Some(30));
        assert_eq!(parse_duration_to_days("1D"), Some(1));

        // Weeks
        assert_eq!(parse_duration_to_days("1w"), Some(7));
        assert_eq!(parse_duration_to_days("2W"), Some(14));
        assert_eq!(parse_duration_to_days("4w"), Some(28));

        // Months (approximate)
        assert_eq!(parse_duration_to_days("1m"), Some(30));
        assert_eq!(parse_duration_to_days("2M"), Some(60));

        // Invalid formats
        assert_eq!(parse_duration_to_days(""), None);
        assert_eq!(parse_duration_to_days("7"), None);
        assert_eq!(parse_duration_to_days("abc"), None);
        assert_eq!(parse_duration_to_days("7x"), None);
    }

    #[test]
    fn test_retention_max_age_env_var() {
        let backup = BackupSpec {
            schedule: "0 2 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(7),
                max_age: Some("30d".to_string()),
            },
            destination: BackupDestination::S3 {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                credentials_secret: "creds".to_string(),
                path: None,
                force_path_style: false,
            },
            wal_archiving: None,
            encryption: None,
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        };

        let cluster = create_test_cluster(Some(backup));
        let env_vars = generate_backup_env_vars(&cluster);

        // Check count-based retention
        assert!(env_vars.iter().any(|e| e.name == "BACKUP_NUM_TO_RETAIN"));
        assert!(
            env_vars
                .iter()
                .any(|e| e.name == "WALG_RETAIN_FULL_BACKUPS")
        );

        // Check time-based retention
        let days_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_BACKUP_RETENTION_DAYS")
            .expect(
                "WALG_BACKUP_RETENTION_DAYS env var should be present for time-based retention",
            );
        assert_eq!(days_var.value.as_deref(), Some("30"));
    }
}
