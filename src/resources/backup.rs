//! Backup configuration for PostgreSQL clusters
//!
//! This module generates backup-related configuration for Spilo/WAL-G:
//! - Environment variables for cloud storage credentials (S3, GCS, Azure)
//! - WAL-G configuration for continuous archiving
//! - Backup scheduling via Spilo's built-in cron
//!
//! # Architecture
//!
//! Spilo (the container image) includes WAL-G and handles:
//! - Continuous WAL archiving to cloud storage
//! - Scheduled base backups via cron
//! - Automatic backup retention management
//!
//! We configure backups by setting environment variables on the StatefulSet pods.
//! No additional CronJobs or sidecars are needed.
//!
//! # Cloud Provider Configuration
//!
//! ## AWS S3
//! Environment variables:
//! - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` - credentials
//! - `AWS_REGION` - bucket region
//! - `WALE_S3_PREFIX` / `WALG_S3_PREFIX` - backup path
//! - `AWS_ENDPOINT` - custom endpoint (for S3-compatible storage)
//!
//! ## Google Cloud Storage
//! Environment variables:
//! - `GOOGLE_APPLICATION_CREDENTIALS` - path to service account JSON
//! - `WALE_GS_PREFIX` / `WALG_GS_PREFIX` - backup path
//!
//! ## Azure Blob Storage
//! Environment variables:
//! - `AZURE_STORAGE_ACCOUNT` - storage account name
//! - `AZURE_STORAGE_ACCESS_KEY` or `AZURE_STORAGE_SAS_TOKEN` - credentials
//! - `WALG_AZ_PREFIX` - backup path
//!
//! # References
//! - WAL-G: https://github.com/wal-g/wal-g
//! - Spilo: https://github.com/zalando/spilo

use k8s_openapi::api::core::v1::{
    EnvVar, EnvVarSource, KeyToPath, SecretKeySelector, SecretVolumeSource, Volume, VolumeMount,
};

use crate::crd::{BackupDestination, EncryptionMethod, PostgresCluster};

/// GCS credentials mount path inside the container
const GCS_CREDENTIALS_PATH: &str = "/var/secrets/google";
/// GCS credentials filename
const GCS_CREDENTIALS_FILE: &str = "credentials.json";
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
            disable_sse,
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

            // Server-side encryption
            if *disable_sse {
                env_vars.push(EnvVar {
                    name: "WALG_DISABLE_S3_SSE".to_string(),
                    value: Some("true".to_string()),
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
        BackupDestination::GCS {
            bucket,
            credentials_secret: _,
            path,
        } => {
            let prefix = backup.destination.wal_g_prefix(cluster_name, namespace);

            // WAL-G GCS prefix
            env_vars.push(EnvVar {
                name: "WALG_GS_PREFIX".to_string(),
                value: Some(prefix.clone()),
                ..Default::default()
            });

            // WAL-E compatibility
            env_vars.push(EnvVar {
                name: "WALE_GS_PREFIX".to_string(),
                value: Some(prefix),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "WAL_GS_BUCKET".to_string(),
                value: Some(bucket.clone()),
                ..Default::default()
            });

            // GCS credentials path (mounted as volume)
            env_vars.push(EnvVar {
                name: "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                value: Some(format!("{}/{}", GCS_CREDENTIALS_PATH, GCS_CREDENTIALS_FILE)),
                ..Default::default()
            });

            if let Some(p) = path {
                env_vars.push(EnvVar {
                    name: "BACKUP_PATH".to_string(),
                    value: Some(p.clone()),
                    ..Default::default()
                });
            }
        }
        BackupDestination::Azure {
            container,
            storage_account,
            credentials_secret,
            path,
            environment,
        } => {
            let prefix = backup.destination.wal_g_prefix(cluster_name, namespace);

            // WAL-G Azure prefix
            env_vars.push(EnvVar {
                name: "WALG_AZ_PREFIX".to_string(),
                value: Some(prefix),
                ..Default::default()
            });

            // Azure storage account
            env_vars.push(EnvVar {
                name: "AZURE_STORAGE_ACCOUNT".to_string(),
                value: Some(storage_account.clone()),
                ..Default::default()
            });

            // Azure environment (optional)
            if let Some(env) = environment {
                env_vars.push(EnvVar {
                    name: "AZURE_ENVIRONMENT_NAME".to_string(),
                    value: Some(env.clone()),
                    ..Default::default()
                });
            }

            // Store container for reference
            env_vars.push(EnvVar {
                name: "AZURE_CONTAINER".to_string(),
                value: Some(container.clone()),
                ..Default::default()
            });

            if let Some(p) = path {
                env_vars.push(EnvVar {
                    name: "BACKUP_PATH".to_string(),
                    value: Some(p.clone()),
                    ..Default::default()
                });
            }

            // Azure credentials from secret
            // Try access key first
            env_vars.push(EnvVar {
                name: "AZURE_STORAGE_ACCESS_KEY".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AZURE_STORAGE_ACCESS_KEY".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            // Or SAS token
            env_vars.push(EnvVar {
                name: "AZURE_STORAGE_SAS_TOKEN".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AZURE_STORAGE_SAS_TOKEN".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            // Or service principal credentials
            env_vars.push(EnvVar {
                name: "AZURE_CLIENT_ID".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AZURE_CLIENT_ID".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "AZURE_CLIENT_SECRET".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AZURE_CLIENT_SECRET".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
            env_vars.push(EnvVar {
                name: "AZURE_TENANT_ID".to_string(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: credentials_secret.clone(),
                        key: "AZURE_TENANT_ID".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
    }

    // Encryption configuration
    if let Some(ref encryption) = backup.encryption
        && encryption.enabled
    {
        let method = encryption.method.as_ref().cloned().unwrap_or_default();
        match method {
            EncryptionMethod::Aes256 => {
                // libsodium key path
                env_vars.push(EnvVar {
                    name: "WALG_LIBSODIUM_KEY_PATH".to_string(),
                    value: Some(format!("{}/encryption-key", ENCRYPTION_KEY_PATH)),
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

/// Generate backup-related volumes for the StatefulSet.
///
/// This creates volumes for:
/// - GCS credentials (service account JSON file)
/// - Encryption keys
pub fn generate_backup_volumes(cluster: &PostgresCluster) -> Vec<Volume> {
    let Some(ref backup) = cluster.spec.backup else {
        return Vec::new();
    };

    let mut volumes = Vec::new();

    // GCS credentials volume
    if let BackupDestination::GCS {
        credentials_secret, ..
    } = &backup.destination
    {
        volumes.push(Volume {
            name: "gcs-credentials".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(credentials_secret.clone()),
                items: Some(vec![KeyToPath {
                    key: "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                    path: GCS_CREDENTIALS_FILE.to_string(),
                    mode: Some(0o400),
                }]),
                default_mode: Some(0o400),
                optional: Some(false),
            }),
            ..Default::default()
        });
    }

    // Encryption key volume
    if let Some(ref encryption) = backup.encryption
        && encryption.enabled
        && let Some(ref key_secret) = encryption.key_secret
    {
        let key_name = match encryption.method.as_ref().cloned().unwrap_or_default() {
            EncryptionMethod::Aes256 => "encryption-key",
            EncryptionMethod::Pgp => "pgp-key",
        };

        volumes.push(Volume {
            name: "backup-encryption-key".to_string(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(key_secret.clone()),
                items: Some(vec![KeyToPath {
                    key: key_name.to_string(),
                    path: key_name.to_string(),
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
pub fn generate_backup_volume_mounts(cluster: &PostgresCluster) -> Vec<VolumeMount> {
    let Some(ref backup) = cluster.spec.backup else {
        return Vec::new();
    };

    let mut mounts = Vec::new();

    // GCS credentials mount
    if matches!(backup.destination, BackupDestination::GCS { .. }) {
        mounts.push(VolumeMount {
            name: "gcs-credentials".to_string(),
            mount_path: GCS_CREDENTIALS_PATH.to_string(),
            read_only: Some(true),
            ..Default::default()
        });
    }

    // Encryption key mount
    if let Some(ref encryption) = backup.encryption
        && encryption.enabled
        && encryption.key_secret.is_some()
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

/// Get the backup credentials secret name, if backup is configured.
pub fn get_backup_credentials_secret(cluster: &PostgresCluster) -> Option<String> {
    cluster
        .spec
        .backup
        .as_ref()
        .map(|b| b.credentials_secret_name().to_string())
}

/// Get the encryption key secret name, if encryption is configured.
pub fn get_encryption_key_secret(cluster: &PostgresCluster) -> Option<String> {
    cluster
        .spec
        .backup
        .as_ref()
        .and_then(|b| b.encryption.as_ref())
        .filter(|e| e.enabled)
        .and_then(|e| e.key_secret.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        BackupDestination, BackupSpec, CompressionMethod, EncryptionMethod, EncryptionSpec,
        PostgresCluster, PostgresClusterSpec, RetentionPolicy, StorageSpec,
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
                version: "16".to_string(),
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                resources: None,
                postgresql_params: Default::default(),
                backup,
                pgbouncer: None,
                tls: None,
                metrics: None,
                service: None,
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
                disable_sse: false,
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
            .unwrap();
        assert_eq!(
            prefix_var.value.as_deref(),
            Some("s3://my-bucket/test-ns/test-cluster")
        );
    }

    #[test]
    fn test_gcs_backup_env_vars() {
        let backup = BackupSpec {
            schedule: "0 3 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(5),
                max_age: None,
            },
            destination: BackupDestination::GCS {
                bucket: "gcs-bucket".to_string(),
                credentials_secret: "gcs-creds".to_string(),
                path: Some("backups/prod".to_string()),
            },
            wal_archiving: None,
            encryption: None,
            compression: Some(CompressionMethod::Zstd),
            backup_from_replica: false,
            upload_concurrency: Some(8),
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        };

        let cluster = create_test_cluster(Some(backup));
        let env_vars = generate_backup_env_vars(&cluster);

        assert!(env_vars.iter().any(|e| e.name == "WALG_GS_PREFIX"));
        assert!(
            env_vars
                .iter()
                .any(|e| e.name == "GOOGLE_APPLICATION_CREDENTIALS")
        );

        // Check custom path
        let prefix_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_GS_PREFIX")
            .unwrap();
        assert_eq!(
            prefix_var.value.as_deref(),
            Some("gs://gcs-bucket/backups/prod")
        );

        // Check compression
        let compression_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_COMPRESSION_METHOD")
            .unwrap();
        assert_eq!(compression_var.value.as_deref(), Some("zstd"));

        // Check concurrency
        let upload_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_UPLOAD_CONCURRENCY")
            .unwrap();
        assert_eq!(upload_var.value.as_deref(), Some("8"));
    }

    #[test]
    fn test_azure_backup_env_vars() {
        let backup = BackupSpec {
            schedule: "0 4 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(10),
                max_age: None,
            },
            destination: BackupDestination::Azure {
                container: "backups".to_string(),
                storage_account: "myaccount".to_string(),
                credentials_secret: "azure-creds".to_string(),
                path: None,
                environment: Some("AzurePublicCloud".to_string()),
            },
            wal_archiving: None,
            encryption: None,
            compression: None,
            backup_from_replica: true,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: true,
            delta_max_steps: Some(5),
        };

        let cluster = create_test_cluster(Some(backup));
        let env_vars = generate_backup_env_vars(&cluster);

        assert!(env_vars.iter().any(|e| e.name == "WALG_AZ_PREFIX"));
        assert!(env_vars.iter().any(|e| e.name == "AZURE_STORAGE_ACCOUNT"));
        assert!(env_vars.iter().any(|e| e.name == "AZURE_ENVIRONMENT_NAME"));

        // Check backup from replica
        assert!(
            env_vars
                .iter()
                .any(|e| e.name == "WALG_BACKUP_FROM_REPLICA")
        );

        // Check delta backups
        let delta_var = env_vars
            .iter()
            .find(|e| e.name == "WALG_DELTA_MAX_STEPS")
            .unwrap();
        assert_eq!(delta_var.value.as_deref(), Some("5"));
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
                disable_sse: false,
                force_path_style: false,
            },
            wal_archiving: None,
            encryption: Some(EncryptionSpec {
                enabled: true,
                method: Some(EncryptionMethod::Aes256),
                key_secret: Some("encryption-key-secret".to_string()),
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

        assert!(env_vars.iter().any(|e| e.name == "WALG_LIBSODIUM_KEY_PATH"));
    }

    #[test]
    fn test_gcs_volumes() {
        let backup = BackupSpec {
            schedule: "0 2 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(7),
                max_age: None,
            },
            destination: BackupDestination::GCS {
                bucket: "gcs-bucket".to_string(),
                credentials_secret: "gcs-creds".to_string(),
                path: None,
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
        let volumes = generate_backup_volumes(&cluster);
        let mounts = generate_backup_volume_mounts(&cluster);

        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0].name, "gcs-credentials");

        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].name, "gcs-credentials");
        assert_eq!(mounts[0].mount_path, GCS_CREDENTIALS_PATH);
    }

    #[test]
    fn test_backup_destination_prefix() {
        let s3_dest = BackupDestination::S3 {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            credentials_secret: "creds".to_string(),
            path: None,
            disable_sse: false,
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
            disable_sse: false,
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
                disable_sse: false,
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
                disable_sse: false,
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
            .unwrap();
        assert_eq!(days_var.value.as_deref(), Some("30"));
    }
}
