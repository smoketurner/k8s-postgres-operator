//! Backup status monitoring for PostgresCluster resources
//!
//! This module provides functionality to collect backup status from running
//! PostgreSQL pods using WAL-G and pg_stat_archiver.

use k8s_openapi::api::core::v1::Pod;
use kube::api::AttachParams;
use kube::{Api, Client, ResourceExt};
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tracing::{debug, info, warn};

use crate::crd::{BackupStatus, PostgresCluster};

/// Result type for backup status operations
pub type Result<T> = std::result::Result<T, BackupStatusError>;

/// Errors that can occur during backup status collection
#[derive(Debug, thiserror::Error)]
pub enum BackupStatusError {
    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    /// JSON parsing error
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// No pods available for status collection
    #[error("No ready pods available")]
    NoPodsAvailable,

    /// Command execution failed
    #[error("Command execution failed: {0}")]
    ExecFailed(String),

    /// UTF-8 decoding error
    #[error("UTF-8 decoding error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

/// WAL-G backup list entry (from `wal-g backup-list --json`)
#[derive(Debug, Deserialize)]
struct WalGBackup {
    /// Backup name (e.g., "base_000000010000000000000010")
    backup_name: String,

    /// Backup time in RFC3339 format
    time: String,

    /// Backup method (wal-g or other)
    #[allow(dead_code)]
    wal_file_name: Option<String>,

    /// Start LSN
    #[allow(dead_code)]
    start_lsn: Option<u64>,

    /// Finish LSN
    #[allow(dead_code)]
    finish_lsn: Option<u64>,

    /// Compressed size in bytes
    compressed_size: Option<i64>,

    /// Uncompressed size in bytes
    #[allow(dead_code)]
    uncompressed_size: Option<i64>,
}

/// pg_stat_archiver output (from PostgreSQL)
#[derive(Debug, Deserialize)]
struct PgStatArchiver {
    /// Number of WAL files archived
    #[allow(dead_code)]
    archived_count: Option<i64>,

    /// Last archived WAL file
    last_archived_wal: Option<String>,

    /// Last archive time
    last_archived_time: Option<String>,

    /// Number of failed archives
    #[allow(dead_code)]
    failed_count: Option<i64>,

    /// Last failed WAL file
    #[allow(dead_code)]
    last_failed_wal: Option<String>,

    /// Last failed archive time
    last_failed_time: Option<String>,
}

/// Backup status collector
pub struct BackupStatusCollector {
    client: Client,
    namespace: String,
    cluster_name: String,
}

impl BackupStatusCollector {
    /// Create a new backup status collector
    pub fn new(client: Client, namespace: &str, cluster_name: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            cluster_name: cluster_name.to_string(),
        }
    }

    /// Collect backup status from the cluster's primary pod
    ///
    /// This executes commands in the pod to gather:
    /// - Backup list from `wal-g backup-list --json`
    /// - WAL archiving status from `pg_stat_archiver`
    pub async fn collect(&self, cluster: &PostgresCluster) -> Result<BackupStatus> {
        // Start with configuration-based status
        let mut status = self.get_config_status(cluster);

        // Find a ready pod to query
        let pod_name = match self.find_ready_pod().await {
            Ok(name) => name,
            Err(e) => {
                debug!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "No ready pods available for backup status collection"
                );
                // Return config-based status if no pods available
                return Ok(status);
            }
        };

        // Collect WAL-G backup list
        match self.get_walg_backup_list(&pod_name).await {
            Ok(backups) => {
                if !backups.is_empty() {
                    // Sort by time descending
                    let mut sorted_backups = backups;
                    sorted_backups.sort_by(|a, b| b.time.cmp(&a.time));

                    // Latest backup
                    if let Some(latest) = sorted_backups.first() {
                        status.last_backup_time = Some(latest.time.clone());
                        status.last_backup_name = Some(latest.backup_name.clone());
                        status.last_backup_size_bytes = latest.compressed_size;
                    }

                    // Oldest backup (for recovery window)
                    if let Some(oldest) = sorted_backups.last() {
                        status.oldest_backup_time = Some(oldest.time.clone());
                        status.recovery_window_start = Some(oldest.time.clone());
                    }

                    status.backup_count = Some(sorted_backups.len() as i32);

                    info!(
                        cluster = %self.cluster_name,
                        backup_count = sorted_backups.len(),
                        last_backup = ?status.last_backup_name,
                        "Collected backup status"
                    );
                }
            }
            Err(e) => {
                warn!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "Failed to get WAL-G backup list"
                );
                // Store error in status
                status.last_error = Some(format!("Failed to list backups: {}", e));
                status.last_error_time = Some(chrono::Utc::now().to_rfc3339());
            }
        }

        // Collect WAL archiving status
        match self.get_wal_archiving_status(&pod_name).await {
            Ok(archiver) => {
                status.last_wal_archived = archiver.last_archived_wal;
                status.last_wal_archive_time = archiver.last_archived_time.clone();

                // Check if archiving is healthy (archived recently and no recent failures)
                let is_healthy = archiver.last_archived_time.is_some()
                    && archiver.last_failed_time.is_none();
                status.wal_archiving_healthy = Some(is_healthy);

                // Update recovery window end to last archived WAL time
                if let Some(ref archive_time) = archiver.last_archived_time {
                    status.recovery_window_end = Some(archive_time.clone());
                }
            }
            Err(e) => {
                warn!(
                    cluster = %self.cluster_name,
                    error = %e,
                    "Failed to get WAL archiving status"
                );
                status.wal_archiving_healthy = Some(false);
            }
        }

        Ok(status)
    }

    /// Get configuration-based backup status from the cluster spec
    fn get_config_status(&self, cluster: &PostgresCluster) -> BackupStatus {
        if let Some(ref backup_spec) = cluster.spec.backup {
            BackupStatus {
                enabled: true,
                destination_type: Some(backup_spec.destination.destination_type().to_string()),
                ..Default::default()
            }
        } else {
            BackupStatus {
                enabled: false,
                ..Default::default()
            }
        }
    }

    /// Find a ready pod in the cluster to query
    async fn find_ready_pod(&self) -> Result<String> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

        let label_selector = format!(
            "postgres.example.com/cluster={},spilo-role=master",
            self.cluster_name
        );

        let pod_list = pods
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        // Find a ready pod
        for pod in pod_list.items {
            if let Some(ref status) = pod.status
                && let Some(ref conditions) = status.conditions {
                    let is_ready = conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True");
                    if is_ready {
                        return Ok(pod.name_any());
                    }
                }
        }

        // If no master found, try any cluster pod
        let label_selector = format!("postgres.example.com/cluster={}", self.cluster_name);
        let pod_list = pods
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        for pod in pod_list.items {
            if let Some(ref status) = pod.status
                && let Some(ref conditions) = status.conditions {
                    let is_ready = conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True");
                    if is_ready {
                        return Ok(pod.name_any());
                    }
                }
        }

        Err(BackupStatusError::NoPodsAvailable)
    }

    /// Get WAL-G backup list from a pod
    async fn get_walg_backup_list(&self, pod_name: &str) -> Result<Vec<WalGBackup>> {
        let output = self
            .exec_command(
                pod_name,
                &["su", "postgres", "-c", "wal-g backup-list --json 2>/dev/null || echo '[]'"],
            )
            .await?;

        // Parse JSON output
        let backups: Vec<WalGBackup> = serde_json::from_str(&output).unwrap_or_default();
        Ok(backups)
    }

    /// Get WAL archiving status from pg_stat_archiver
    async fn get_wal_archiving_status(&self, pod_name: &str) -> Result<PgStatArchiver> {
        let query = "SELECT \
            archived_count, \
            last_archived_wal, \
            to_char(last_archived_time, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as last_archived_time, \
            failed_count, \
            last_failed_wal, \
            to_char(last_failed_time, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') as last_failed_time \
            FROM pg_stat_archiver";

        let output = self
            .exec_command(
                pod_name,
                &[
                    "su",
                    "postgres",
                    "-c",
                    &format!("psql -U postgres -t -A -F',' -c \"{}\" 2>/dev/null || echo ''", query),
                ],
            )
            .await?;

        // Parse CSV output (field1,field2,...)
        let parts: Vec<&str> = output.trim().split(',').collect();
        if parts.len() >= 6 {
            Ok(PgStatArchiver {
                archived_count: parts[0].parse().ok(),
                last_archived_wal: if parts[1].is_empty() { None } else { Some(parts[1].to_string()) },
                last_archived_time: if parts[2].is_empty() { None } else { Some(parts[2].to_string()) },
                failed_count: parts[3].parse().ok(),
                last_failed_wal: if parts[4].is_empty() { None } else { Some(parts[4].to_string()) },
                last_failed_time: if parts[5].is_empty() { None } else { Some(parts[5].to_string()) },
            })
        } else {
            Ok(PgStatArchiver {
                archived_count: None,
                last_archived_wal: None,
                last_archived_time: None,
                failed_count: None,
                last_failed_wal: None,
                last_failed_time: None,
            })
        }
    }

    /// Execute a command in a pod and return the stdout
    async fn exec_command(&self, pod_name: &str, command: &[&str]) -> Result<String> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);

        let attach_params = AttachParams {
            container: Some("postgres".to_string()),
            stdin: false,
            stdout: true,
            stderr: false,
            tty: false,
            ..Default::default()
        };

        // Convert &[&str] to Vec<String> for kube-rs exec API
        let command_strings: Vec<String> = command.iter().map(|s| s.to_string()).collect();

        // Execute the command
        let mut attached = pods.exec(pod_name, command_strings, &attach_params).await?;

        // Read stdout
        let mut stdout_reader = attached
            .stdout()
            .ok_or_else(|| BackupStatusError::ExecFailed("No stdout available".to_string()))?;

        let mut output = Vec::new();
        stdout_reader.read_to_end(&mut output).await.map_err(|e| {
            BackupStatusError::ExecFailed(format!("Failed to read stdout: {}", e))
        })?;

        // Wait for the command to complete
        let status = attached.take_status();
        if let Some(status_channel) = status
            && let Some(result) = status_channel.await
                && let Some(status) = result.status
                    && status != "Success" {
                        debug!(
                            pod = pod_name,
                            status = status,
                            "Command exited with non-success status"
                        );
                    }

        String::from_utf8(output).map_err(BackupStatusError::from)
    }
}

/// Update the backup status in a PostgresCluster status object
pub fn update_backup_status(status: &mut BackupStatus, new_status: BackupStatus) {
    // Only update fields that have values in the new status
    status.enabled = new_status.enabled;

    if new_status.destination_type.is_some() {
        status.destination_type = new_status.destination_type;
    }
    if new_status.last_backup_time.is_some() {
        status.last_backup_time = new_status.last_backup_time;
    }
    if new_status.last_backup_size_bytes.is_some() {
        status.last_backup_size_bytes = new_status.last_backup_size_bytes;
    }
    if new_status.last_backup_name.is_some() {
        status.last_backup_name = new_status.last_backup_name;
    }
    if new_status.oldest_backup_time.is_some() {
        status.oldest_backup_time = new_status.oldest_backup_time;
    }
    if new_status.backup_count.is_some() {
        status.backup_count = new_status.backup_count;
    }
    if new_status.last_wal_archived.is_some() {
        status.last_wal_archived = new_status.last_wal_archived;
    }
    if new_status.last_wal_archive_time.is_some() {
        status.last_wal_archive_time = new_status.last_wal_archive_time;
    }
    if new_status.wal_archiving_healthy.is_some() {
        status.wal_archiving_healthy = new_status.wal_archiving_healthy;
    }
    if new_status.last_error.is_some() {
        status.last_error = new_status.last_error;
    }
    if new_status.last_error_time.is_some() {
        status.last_error_time = new_status.last_error_time;
    }
    if new_status.recovery_window_start.is_some() {
        status.recovery_window_start = new_status.recovery_window_start;
    }
    if new_status.recovery_window_end.is_some() {
        status.recovery_window_end = new_status.recovery_window_end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_backup_status_merges_fields() {
        let mut existing = BackupStatus {
            enabled: true,
            destination_type: Some("S3".to_string()),
            last_backup_time: Some("2025-01-10T00:00:00Z".to_string()),
            ..Default::default()
        };

        let new_status = BackupStatus {
            enabled: true,
            backup_count: Some(5),
            wal_archiving_healthy: Some(true),
            ..Default::default()
        };

        update_backup_status(&mut existing, new_status);

        assert_eq!(existing.destination_type, Some("S3".to_string()));
        assert_eq!(existing.last_backup_time, Some("2025-01-10T00:00:00Z".to_string()));
        assert_eq!(existing.backup_count, Some(5));
        assert_eq!(existing.wal_archiving_healthy, Some(true));
    }

    #[test]
    fn test_walg_backup_parse() {
        let json = r#"[
            {
                "backup_name": "base_000000010000000000000010",
                "time": "2025-01-10T02:00:00Z",
                "wal_file_name": "000000010000000000000010",
                "compressed_size": 1048576
            }
        ]"#;

        let backups: Vec<WalGBackup> = serde_json::from_str(json).unwrap();
        assert_eq!(backups.len(), 1);
        assert_eq!(backups[0].backup_name, "base_000000010000000000000010");
        assert_eq!(backups[0].compressed_size, Some(1048576));
    }
}
