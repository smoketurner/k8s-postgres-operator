//! Backup status monitoring for PostgresCluster resources
//!
//! This module provides functionality to collect backup status from running
//! PostgreSQL pods using WAL-G and pg_stat_archiver.
//!
//! # Events
//!
//! This module also detects status changes and returns events that should be
//! emitted by the reconciler:
//! - `BackupCompleted` - When a new backup is detected
//! - `BackupFailed` - When a backup error is detected
//! - `WALArchivingHealthy` - When WAL archiving recovers
//! - `WALArchivingFailed` - When WAL archiving becomes unhealthy

use k8s_openapi::api::core::v1::Pod;
use kube::api::AttachParams;
use kube::{Api, Client, ResourceExt};
use serde::Deserialize;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::time::timeout;
use tracing::{debug, info, warn};

/// Default timeout for pod exec commands
const EXEC_TIMEOUT: Duration = Duration::from_secs(30);

use crate::crd::{BackupStatus, PostgresCluster};

/// Result type for backup status operations
pub(crate) type Result<T> = std::result::Result<T, BackupStatusError>;

/// Errors that can occur during backup status collection
#[derive(Debug, thiserror::Error)]
pub(crate) enum BackupStatusError {
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

    /// Command timed out
    #[error("Command timed out after {0} seconds: {1}")]
    Timeout(u64, String),
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
pub(crate) struct BackupStatusCollector {
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
                    // Find latest backup using iterator max (more efficient than sorting)
                    if let Some(latest) = backups.iter().max_by_key(|b| &b.time) {
                        status.last_backup_time = Some(latest.time.clone());
                        status.last_backup_name = Some(latest.backup_name.clone());
                        status.last_backup_size_bytes = latest.compressed_size;
                    }

                    // Find oldest backup for recovery window (using iterator min)
                    if let Some(oldest) = backups.iter().min_by_key(|b| &b.time) {
                        status.oldest_backup_time = Some(oldest.time.clone());
                        status.recovery_window_start = Some(oldest.time.clone());
                    }

                    // Use i32::try_from to handle overflow safely
                    status.backup_count = i32::try_from(backups.len()).ok();

                    info!(
                        cluster = %self.cluster_name,
                        backup_count = backups.len(),
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
                let is_healthy =
                    archiver.last_archived_time.is_some() && archiver.last_failed_time.is_none();
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
            "postgres-operator.smoketurner.com/cluster={},spilo-role=master",
            self.cluster_name
        );

        let pod_list = pods
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        // Find a ready pod
        for pod in pod_list.items {
            if let Some(ref status) = pod.status
                && let Some(ref conditions) = status.conditions
            {
                let is_ready = conditions
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True");
                if is_ready {
                    return Ok(pod.name_any());
                }
            }
        }

        // If no master found, try any cluster pod
        let label_selector = format!(
            "postgres-operator.smoketurner.com/cluster={}",
            self.cluster_name
        );
        let pod_list = pods
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        for pod in pod_list.items {
            if let Some(ref status) = pod.status
                && let Some(ref conditions) = status.conditions
            {
                let is_ready = conditions
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True");
                if is_ready {
                    return Ok(pod.name_any());
                }
            }
        }

        Err(BackupStatusError::NoPodsAvailable)
    }

    /// Get WAL-G backup list from a pod
    async fn get_walg_backup_list(&self, pod_name: &str) -> Result<Vec<WalGBackup>> {
        let output = timeout(
            EXEC_TIMEOUT,
            self.exec_command(
                pod_name,
                &[
                    "su",
                    "postgres",
                    "-c",
                    "wal-g backup-list --json 2>/dev/null || echo '[]'",
                ],
            ),
        )
        .await
        .map_err(|_| {
            BackupStatusError::Timeout(EXEC_TIMEOUT.as_secs(), "wal-g backup-list".to_string())
        })??;

        // Parse JSON output - log errors instead of silently ignoring
        match serde_json::from_str(&output) {
            Ok(backups) => Ok(backups),
            Err(e) => {
                // Empty output or "[]" should not be logged as error
                if output.trim().is_empty() || output.trim() == "[]" {
                    Ok(Vec::new())
                } else {
                    warn!(
                        cluster = %self.cluster_name,
                        error = %e,
                        output = %output.chars().take(200).collect::<String>(),
                        "Failed to parse WAL-G backup list JSON"
                    );
                    Err(BackupStatusError::JsonError(e))
                }
            }
        }
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

        let output = timeout(
            EXEC_TIMEOUT,
            self.exec_command(
                pod_name,
                &[
                    "su",
                    "postgres",
                    "-c",
                    &format!(
                        "psql -U postgres -t -A -F',' -c \"{}\" 2>/dev/null || echo ''",
                        query
                    ),
                ],
            ),
        )
        .await
        .map_err(|_| {
            BackupStatusError::Timeout(EXEC_TIMEOUT.as_secs(), "pg_stat_archiver query".to_string())
        })??;

        // Parse CSV output using iterator pattern to avoid panic on malformed data
        let trimmed = output.trim();
        if trimmed.is_empty() {
            return Ok(PgStatArchiver {
                archived_count: None,
                last_archived_wal: None,
                last_archived_time: None,
                failed_count: None,
                last_failed_wal: None,
                last_failed_time: None,
            });
        }

        let mut parts = trimmed.split(',');

        // Helper to parse optional non-empty string
        let parse_optional_string = |part: Option<&str>| -> Option<String> {
            part.and_then(|s| {
                let s = s.trim();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            })
        };

        Ok(PgStatArchiver {
            archived_count: parts.next().and_then(|s| s.trim().parse().ok()),
            last_archived_wal: parse_optional_string(parts.next()),
            last_archived_time: parse_optional_string(parts.next()),
            failed_count: parts.next().and_then(|s| s.trim().parse().ok()),
            last_failed_wal: parse_optional_string(parts.next()),
            last_failed_time: parse_optional_string(parts.next()),
        })
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
        stdout_reader
            .read_to_end(&mut output)
            .await
            .map_err(|e| BackupStatusError::ExecFailed(format!("Failed to read stdout: {}", e)))?;

        // Wait for the command to complete
        let status = attached.take_status();
        if let Some(status_channel) = status
            && let Some(result) = status_channel.await
            && let Some(status) = result.status
            && status != "Success"
        {
            debug!(
                pod = pod_name,
                status = status,
                "Command exited with non-success status"
            );
        }

        String::from_utf8(output).map_err(BackupStatusError::from)
    }
}

/// Events that can be emitted based on backup status changes
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BackupEvent {
    /// A new backup was completed successfully
    BackupCompleted {
        name: String,
        size_bytes: Option<i64>,
    },
    /// A backup error was detected
    BackupFailed { error: String },
    /// WAL archiving recovered and is healthy again
    WALArchivingHealthy,
    /// WAL archiving became unhealthy
    WALArchivingFailed,
}

/// Detect backup events by comparing previous and current status
///
/// Returns a list of events that should be emitted by the reconciler.
pub(crate) fn detect_backup_events(
    previous: Option<&BackupStatus>,
    current: &BackupStatus,
) -> Vec<BackupEvent> {
    let mut events = Vec::new();

    // Detect new backup completion
    if let Some(ref current_backup) = current.last_backup_name {
        let prev_backup = previous.and_then(|p| p.last_backup_name.as_ref());
        if prev_backup != Some(current_backup) {
            // New backup detected
            events.push(BackupEvent::BackupCompleted {
                name: current_backup.clone(),
                size_bytes: current.last_backup_size_bytes,
            });
        }
    }

    // Detect backup errors (new error that wasn't there before)
    if let Some(ref error) = current.last_error {
        let prev_error = previous.and_then(|p| p.last_error.as_ref());
        let prev_error_time = previous.and_then(|p| p.last_error_time.as_ref());
        let current_error_time = current.last_error_time.as_ref();

        // Only emit if error is new (different error or different timestamp)
        if prev_error != Some(error) || prev_error_time != current_error_time {
            events.push(BackupEvent::BackupFailed {
                error: error.clone(),
            });
        }
    }

    // Detect WAL archiving status changes
    let prev_healthy = previous.and_then(|p| p.wal_archiving_healthy);
    let current_healthy = current.wal_archiving_healthy;

    match (prev_healthy, current_healthy) {
        // Was unhealthy (or unknown), now healthy
        (Some(false), Some(true)) | (None, Some(true)) => {
            // Only emit recovery event if there was a previous unhealthy state
            if prev_healthy == Some(false) {
                events.push(BackupEvent::WALArchivingHealthy);
            }
        }
        // Was healthy, now unhealthy
        (Some(true), Some(false)) | (None, Some(false)) => {
            // Only emit failure event if this is a change from healthy
            if prev_healthy == Some(true) {
                events.push(BackupEvent::WALArchivingFailed);
            }
        }
        _ => {}
    }

    events
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

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

    #[test]
    fn test_detect_new_backup_completed() {
        let previous = BackupStatus {
            enabled: true,
            last_backup_name: Some("base_old".to_string()),
            ..Default::default()
        };

        let current = BackupStatus {
            enabled: true,
            last_backup_name: Some("base_new".to_string()),
            last_backup_size_bytes: Some(1048576),
            ..Default::default()
        };

        let events = detect_backup_events(Some(&previous), &current);

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            BackupEvent::BackupCompleted { name, size_bytes }
            if name == "base_new" && *size_bytes == Some(1048576)
        ));
    }

    #[test]
    fn test_detect_no_event_when_same_backup() {
        let previous = BackupStatus {
            enabled: true,
            last_backup_name: Some("base_same".to_string()),
            ..Default::default()
        };

        let current = BackupStatus {
            enabled: true,
            last_backup_name: Some("base_same".to_string()),
            ..Default::default()
        };

        let events = detect_backup_events(Some(&previous), &current);
        assert!(events.is_empty());
    }

    #[test]
    fn test_detect_first_backup() {
        let previous = BackupStatus {
            enabled: true,
            last_backup_name: None,
            ..Default::default()
        };

        let current = BackupStatus {
            enabled: true,
            last_backup_name: Some("base_first".to_string()),
            ..Default::default()
        };

        let events = detect_backup_events(Some(&previous), &current);

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            BackupEvent::BackupCompleted { name, .. } if name == "base_first"
        ));
    }

    #[test]
    fn test_detect_backup_failed() {
        let previous = BackupStatus {
            enabled: true,
            ..Default::default()
        };

        let current = BackupStatus {
            enabled: true,
            last_error: Some("Connection failed".to_string()),
            last_error_time: Some("2025-01-10T12:00:00Z".to_string()),
            ..Default::default()
        };

        let events = detect_backup_events(Some(&previous), &current);

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            BackupEvent::BackupFailed { error } if error == "Connection failed"
        ));
    }

    #[test]
    fn test_detect_wal_archiving_failed() {
        let previous = BackupStatus {
            enabled: true,
            wal_archiving_healthy: Some(true),
            ..Default::default()
        };

        let current = BackupStatus {
            enabled: true,
            wal_archiving_healthy: Some(false),
            ..Default::default()
        };

        let events = detect_backup_events(Some(&previous), &current);

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], BackupEvent::WALArchivingFailed));
    }

    #[test]
    fn test_detect_wal_archiving_recovered() {
        let previous = BackupStatus {
            enabled: true,
            wal_archiving_healthy: Some(false),
            ..Default::default()
        };

        let current = BackupStatus {
            enabled: true,
            wal_archiving_healthy: Some(true),
            ..Default::default()
        };

        let events = detect_backup_events(Some(&previous), &current);

        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], BackupEvent::WALArchivingHealthy));
    }

    #[test]
    fn test_detect_no_wal_event_on_initial_healthy() {
        // When there's no previous status and WAL is healthy, don't emit event
        let current = BackupStatus {
            enabled: true,
            wal_archiving_healthy: Some(true),
            ..Default::default()
        };

        let events = detect_backup_events(None, &current);

        // Should not contain WALArchivingHealthy since this is initial state
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, BackupEvent::WALArchivingHealthy))
        );
    }
}
