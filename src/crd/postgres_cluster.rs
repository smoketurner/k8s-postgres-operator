use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Supported PostgreSQL versions (must match available Spilo images)
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub enum PostgresVersion {
    #[serde(rename = "15")]
    V15,
    #[serde(rename = "16")]
    V16,
    #[serde(rename = "17")]
    V17,
}

impl fmt::Display for PostgresVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresVersion::V15 => write!(f, "15"),
            PostgresVersion::V16 => write!(f, "16"),
            PostgresVersion::V17 => write!(f, "17"),
        }
    }
}

impl PostgresVersion {
    /// Returns the major version number as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            PostgresVersion::V15 => "15",
            PostgresVersion::V16 => "16",
            PostgresVersion::V17 => "17",
        }
    }

    /// Returns the Spilo image tag for this PostgreSQL version
    /// Updated tags available at: https://github.com/zalando/spilo/pkgs/container/
    pub fn spilo_tag(&self) -> &'static str {
        match self {
            PostgresVersion::V15 => "3.2-p2", // spilo-15 latest
            PostgresVersion::V16 => "3.3-p3", // spilo-16 latest
            PostgresVersion::V17 => "4.0-p3", // spilo-17 latest
        }
    }

    /// Returns the full Spilo image name for this PostgreSQL version
    /// Spilo images are named: ghcr.io/zalando/spilo-{major_version}:{tag}
    /// Check https://github.com/orgs/zalando/packages?repo_name=spilo for available versions
    pub fn spilo_image(&self) -> String {
        format!(
            "ghcr.io/zalando/spilo-{}:{}",
            self.as_str(),
            self.spilo_tag()
        )
    }
}

/// PostgresCluster is the Schema for the postgresclusters API
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "postgres-operator.smoketurner.com",
    version = "v1alpha1",
    kind = "PostgresCluster",
    plural = "postgresclusters",
    shortname = "pgc",
    namespaced,
    status = "PostgresClusterStatus",
    printcolumn = r#"{"name":"Version", "type":"string", "jsonPath":".spec.version"}"#,
    printcolumn = r#"{"name":"Replicas", "type":"integer", "jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Ready", "type":"integer", "jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PostgresClusterSpec {
    /// PostgreSQL version - must be a version supported by Spilo
    pub version: PostgresVersion,

    /// Number of replicas (total Patroni members)
    /// - 1 = single server (still managed by Patroni)
    /// - 2 = primary + one read replica
    /// - 3+ = primary + multiple read replicas for higher availability
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Storage configuration for PostgreSQL data
    pub storage: StorageSpec,

    /// Resource requirements for PostgreSQL pods
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// PostgreSQL configuration parameters (postgresql.conf)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub postgresql_params: BTreeMap<String, String>,

    /// Custom labels to apply to all resources created for this cluster.
    ///
    /// Use for cost allocation, team ownership, and organizational tracking.
    /// Common labels include:
    /// - `team`: Team that owns this cluster (e.g., "platform", "payments")
    /// - `cost-center`: Cost center for billing (e.g., "eng-001")
    /// - `project`: Project identifier (e.g., "checkout-service")
    /// - `environment`: Environment type (e.g., "production", "staging")
    ///
    /// These labels are propagated to StatefulSets, Services, ConfigMaps,
    /// Secrets, and other managed resources.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,

    /// Backup configuration (Phase 0.2)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupSpec>,

    /// Connection pooling configuration (Phase 0.3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgbouncer: Option<PgBouncerSpec>,

    /// TLS configuration for PostgreSQL connections.
    /// TLS is enabled by default for security. Set `enabled: false` to disable.
    #[serde(default = "default_tls")]
    pub tls: TLSSpec,

    /// Metrics configuration (Phase 0.3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsSpec>,

    /// Service configuration for external access
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<ServiceSpec>,

    /// Restore configuration for bootstrapping from an existing backup.
    /// When specified, the cluster will be initialized by restoring from the
    /// specified backup source instead of creating a fresh database.
    /// This is only used during initial cluster creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore: Option<RestoreSpec>,

    /// Auto-scaling configuration for the cluster.
    /// Requires KEDA to be installed in the cluster.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling: Option<ScalingSpec>,
}

fn default_replicas() -> i32 {
    1
}

fn default_tls() -> TLSSpec {
    TLSSpec {
        enabled: true,
        issuer_ref: None,
        additional_dns_names: vec![],
        duration: None,
        renew_before: None,
    }
}

/// Restore configuration for bootstrapping a cluster from an existing backup.
///
/// This uses Spilo's CLONE_WITH_WALG functionality to restore from a WAL-G backup.
/// The restore is only performed during initial cluster creation.
///
/// # Example
/// ```yaml
/// restore:
///   source:
///     s3:
///       prefix: s3://my-bucket/default/production-db
///       region: us-east-1
///       credentialsSecret: aws-backup-creds
///   recoveryTarget:
///     time: "2024-01-15T14:30:00Z"  # Point-in-time recovery
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSpec {
    /// Source backup location for the restore operation
    pub source: RestoreSource,

    /// Recovery target for point-in-time recovery (PITR).
    /// If not specified, restores to the latest available state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_target: Option<RecoveryTarget>,
}

/// Source location for restore operations.
///
/// Specifies where to find the backup to restore from.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RestoreSource {
    /// Restore from an S3-compatible backup
    #[serde(rename = "s3")]
    S3 {
        /// Full S3 prefix path (e.g., "s3://bucket/path/to/backup")
        prefix: String,
        /// AWS region
        region: String,
        /// Secret containing AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        credentials_secret: String,
        /// Custom endpoint for S3-compatible storage (e.g., MinIO)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },
    /// Restore from a Google Cloud Storage backup
    #[serde(rename = "gcs")]
    Gcs {
        /// Full GCS prefix path (e.g., "gs://bucket/path/to/backup")
        prefix: String,
        /// Secret containing GCS credentials (GOOGLE_APPLICATION_CREDENTIALS)
        credentials_secret: String,
    },
    /// Restore from an Azure Blob Storage backup
    #[serde(rename = "azure")]
    Azure {
        /// Full Azure prefix path (e.g., "azure://container/path/to/backup")
        prefix: String,
        /// Azure storage account name
        storage_account: String,
        /// Secret containing Azure credentials
        credentials_secret: String,
    },
}

/// Recovery target for point-in-time recovery (PITR).
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RecoveryTarget {
    /// Restore to a specific timestamp (ISO 8601 format).
    /// Example: "2024-01-15T14:30:00Z"
    #[serde(rename = "time")]
    Time(String),

    /// Restore to a specific backup by name.
    /// Example: "base_00000001000000000000000A"
    #[serde(rename = "backup")]
    Backup(String),

    /// Restore to a specific timeline.
    #[serde(rename = "timeline")]
    Timeline(i32),
}

/// Auto-scaling configuration for PostgreSQL clusters.
///
/// Requires KEDA (Kubernetes Event-driven Autoscaling) to be installed.
///
/// Scaling behavior is determined by `minReplicas` and `maxReplicas`:
/// - `minReplicas: 0` enables scale-to-zero (development clusters)
/// - `minReplicas > 0` with `maxReplicas > replicas` enables auto-scaling
///
/// The `replicas` field in the spec defines the desired/baseline replica count.
/// KEDA will scale between `minReplicas` and `maxReplicas` based on the
/// configured metrics.
///
/// # Example
/// ```yaml
/// # Development cluster with scale-to-zero
/// spec:
///   replicas: 1
///   scaling:
///     minReplicas: 0
///     maxReplicas: 1
///     idleTimeout: 30m
///     wakeupTimeout: 5m
///
/// # Production cluster with reader auto-scaling
/// spec:
///   replicas: 3
///   scaling:
///     minReplicas: 3
///     maxReplicas: 10
///     metrics:
///       cpu:
///         targetUtilization: 70
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalingSpec {
    /// Minimum number of replicas.
    /// Set to 0 to enable scale-to-zero for development clusters.
    /// For production, this should be >= 1 (typically >= spec.replicas).
    /// Default: spec.replicas (no scale-down below desired)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_replicas: Option<i32>,

    /// Maximum number of replicas.
    /// The cluster will scale up to this many replicas under load.
    /// Default: 10
    #[serde(default = "default_max_replicas")]
    pub max_replicas: i32,

    /// Time without connections before scaling to minReplicas.
    /// Particularly important when minReplicas=0 (scale-to-zero).
    /// Format: duration string (e.g., "30m", "1h").
    /// Default: 30m
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout: String,

    /// Maximum time to wait for cluster wake-up on connection attempt.
    /// Only applies when minReplicas=0 (scale-to-zero).
    /// If exceeded, the connection will fail.
    /// Format: duration string (e.g., "5m", "10m").
    /// Default: 5m
    #[serde(default = "default_wakeup_timeout")]
    pub wakeup_timeout: String,

    /// Scaling metrics configuration.
    /// Defines what triggers scaling (CPU, connections, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<ScalingMetrics>,

    /// Replication lag threshold for reader service routing.
    /// Replicas with lag exceeding this threshold are excluded from the reader service.
    /// Format: duration string (e.g., "30s", "1m").
    /// Default: 30s
    #[serde(default = "default_replication_lag_threshold")]
    pub replication_lag_threshold: String,
}

impl ScalingSpec {
    /// Returns the effective minimum replicas, defaulting to the cluster's desired replicas
    pub fn effective_min_replicas(&self, desired_replicas: i32) -> i32 {
        self.min_replicas.unwrap_or(desired_replicas)
    }

    /// Returns true if scale-to-zero is enabled (minReplicas = 0)
    pub fn is_scale_to_zero(&self) -> bool {
        self.min_replicas == Some(0)
    }

    /// Returns true if auto-scaling is effectively enabled
    /// (maxReplicas > minReplicas or scale-to-zero is enabled)
    pub fn is_scaling_enabled(&self, desired_replicas: i32) -> bool {
        let min = self.effective_min_replicas(desired_replicas);
        self.max_replicas > min || self.is_scale_to_zero()
    }
}

fn default_max_replicas() -> i32 {
    10
}

fn default_idle_timeout() -> String {
    "30m".to_string()
}

fn default_wakeup_timeout() -> String {
    "5m".to_string()
}

fn default_replication_lag_threshold() -> String {
    "30s".to_string()
}

/// Scaling metrics for KEDA triggers.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalingMetrics {
    /// CPU-based scaling configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<CpuScalingMetric>,

    /// Connection-based scaling configuration using PostgreSQL pg_stat_activity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connections: Option<ConnectionScalingMetric>,
}

/// CPU-based scaling metric.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CpuScalingMetric {
    /// Target CPU utilization percentage (0-100).
    /// Scale up when average CPU exceeds this threshold.
    /// Default: 70
    #[serde(default = "default_cpu_target")]
    pub target_utilization: i32,
}

fn default_cpu_target() -> i32 {
    70
}

/// Connection-based scaling metric using PostgreSQL pg_stat_activity.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionScalingMetric {
    /// Target active connections per replica.
    /// Scale up when connections per replica exceeds this threshold.
    /// Uses: SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
    /// Default: 100
    #[serde(default = "default_connections_target")]
    pub target_per_replica: i32,
}

fn default_connections_target() -> i32 {
    100
}

/// Storage configuration for PostgreSQL data volumes
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// Storage class name (uses default if not specified)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,

    /// Size of the persistent volume (e.g., "10Gi", "100Gi")
    pub size: String,
}

/// Resource requirements for PostgreSQL pods
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    /// CPU and memory limits
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<ResourceList>,

    /// CPU and memory requests
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<ResourceList>,

    /// Restart container when resources are resized (Kubernetes 1.35+)
    /// If false (default), resources are resized in-place without container restart.
    /// If true, container is restarted when resources change.
    /// Note: Some PostgreSQL settings like shared_buffers require a PostgreSQL
    /// restart to take effect even with in-place resize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_on_resize: Option<bool>,
}

/// Resource quantities for CPU and memory
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub struct ResourceList {
    /// CPU quantity (e.g., "500m", "2")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,

    /// Memory quantity (e.g., "512Mi", "2Gi")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

/// Backup configuration for continuous archiving and point-in-time recovery (PITR).
///
/// This integrates with WAL-G (default) or WAL-E for:
/// - Continuous WAL archiving to cloud storage (S3, GCS, Azure)
/// - Scheduled base backups (full physical backups)
/// - Point-in-time recovery from any moment between backups
///
/// # Example
/// ```yaml
/// backup:
///   schedule: "0 2 * * *"  # Daily at 2 AM
///   retention:
///     count: 7
///     maxAge: "30d"
///   destination:
///     type: S3
///     bucket: my-postgres-backups
///     region: us-east-1
///     credentialsSecret: aws-backup-credentials
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// Backup schedule in cron format (e.g., "0 2 * * *" for 2 AM daily).
    /// Base backups are created on this schedule.
    /// WAL archiving happens continuously regardless of this schedule.
    pub schedule: String,

    /// Retention policy for backups
    pub retention: RetentionPolicy,

    /// Backup destination (S3, GCS, or Azure)
    pub destination: BackupDestination,

    /// WAL archiving configuration.
    /// WAL archiving is enabled by default when backup is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_archiving: Option<WalArchivingSpec>,

    /// Encryption configuration for backups.
    /// When enabled, backups are encrypted at rest using the specified method.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<EncryptionSpec>,

    /// Compression method for backups.
    /// Default: lz4 (fast compression with good ratio)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<CompressionMethod>,

    /// Take backups from replica instead of primary.
    /// Reduces load on the primary server.
    /// Default: false
    #[serde(default)]
    pub backup_from_replica: bool,

    /// Number of concurrent upload streams for WAL-G.
    /// Higher values increase upload speed but use more resources.
    /// Default: 16
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_concurrency: Option<i32>,

    /// Number of concurrent download streams for restore.
    /// Default: 10
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_concurrency: Option<i32>,

    /// Enable delta backups (WAL-G feature).
    /// Delta backups only store pages changed since the last backup.
    /// Reduces backup size and time but increases restore complexity.
    /// Default: false
    #[serde(default)]
    pub enable_delta_backups: bool,

    /// Maximum number of delta backup steps before forcing a full backup.
    /// Only used when enable_delta_backups is true.
    /// Default: 3
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_max_steps: Option<i32>,
}

impl BackupSpec {
    /// Returns the compression method name for WAL-G configuration.
    pub fn compression_method(&self) -> &str {
        self.compression
            .as_ref()
            .map(CompressionMethod::as_str)
            .unwrap_or("lz4")
    }

    /// Returns the upload concurrency value.
    pub fn upload_concurrency(&self) -> i32 {
        self.upload_concurrency.unwrap_or(16)
    }

    /// Returns the download concurrency value.
    pub fn download_concurrency(&self) -> i32 {
        self.download_concurrency.unwrap_or(10)
    }

    /// Returns the delta max steps value.
    pub fn delta_max_steps(&self) -> i32 {
        self.delta_max_steps.unwrap_or(3)
    }

    /// Returns the credentials secret name for the backup destination.
    pub fn credentials_secret_name(&self) -> &str {
        match &self.destination {
            BackupDestination::S3 {
                credentials_secret, ..
            } => credentials_secret,
            BackupDestination::GCS {
                credentials_secret, ..
            } => credentials_secret,
            BackupDestination::Azure {
                credentials_secret, ..
            } => credentials_secret,
        }
    }
}

/// WAL archiving configuration for continuous backup
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WalArchivingSpec {
    /// Enable WAL archiving. Default: true when backup is configured.
    #[serde(default = "default_wal_archiving_enabled")]
    pub enabled: bool,

    /// Timeout for WAL restore operations in seconds.
    /// Set to 0 to disable timeout.
    /// Default: disabled (0)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_timeout: Option<i32>,
}

fn default_wal_archiving_enabled() -> bool {
    true
}

/// Encryption configuration for backups.
///
/// Encryption is REQUIRED when backups are configured. The presence of this
/// section enables encryption - there is no separate "enabled" flag.
///
/// All backups must be encrypted at rest to protect sensitive data.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct EncryptionSpec {
    /// Encryption method to use.
    /// Default: aes256 (AES-256-CTR using libsodium)
    #[serde(default)]
    pub method: EncryptionMethod,

    /// Secret containing encryption key (REQUIRED).
    /// For AES: expects a key named 'encryption-key' with the raw key bytes.
    /// For PGP: expects a key named 'pgp-key' with the PGP public key.
    pub key_secret: String,
}

/// Encryption method for backups
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum EncryptionMethod {
    /// AES-256-CTR encryption using libsodium (recommended)
    #[default]
    #[serde(rename = "aes256")]
    Aes256,
    /// PGP encryption
    #[serde(rename = "pgp")]
    Pgp,
}

/// Compression method for backups
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum CompressionMethod {
    /// LZ4 compression - fast with good compression ratio (recommended)
    #[default]
    Lz4,
    /// LZMA compression - slow but highest compression ratio
    Lzma,
    /// Brotli compression - good balance of speed and ratio
    Brotli,
    /// Zstandard compression - excellent compression with good speed
    Zstd,
    /// No compression
    None,
}

impl CompressionMethod {
    /// Returns the WAL-G compression method name.
    pub fn as_str(&self) -> &str {
        match self {
            CompressionMethod::Lz4 => "lz4",
            CompressionMethod::Lzma => "lzma",
            CompressionMethod::Brotli => "brotli",
            CompressionMethod::Zstd => "zstd",
            CompressionMethod::None => "none",
        }
    }
}

/// Retention policy for backups
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicy {
    /// Number of base backups to retain.
    /// Older backups and their associated WAL files are automatically deleted.
    /// Uses Spilo's BACKUP_NUM_TO_RETAIN environment variable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,

    /// Maximum age of backups (e.g., "7d", "30d", "1w").
    /// Backups older than this are automatically deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<String>,
}

/// Backup destination configuration
#[allow(clippy::upper_case_acronyms)] // GCS is a well-known acronym for Google Cloud Storage
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BackupDestination {
    /// Amazon S3 backup destination
    S3 {
        /// S3 bucket name
        bucket: String,
        /// AWS region (e.g., "us-east-1")
        region: String,
        /// Custom S3 endpoint URL (for S3-compatible storage like MinIO)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        /// Secret containing AWS credentials.
        /// Expected keys: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        /// Optional: AWS_SESSION_TOKEN for temporary credentials
        credentials_secret: String,
        /// Path prefix within the bucket (e.g., "backups/prod-cluster")
        /// Default: cluster name
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        /// Disable server-side encryption
        #[serde(default)]
        disable_sse: bool,
        /// Force path-style URLs (required for some S3-compatible storage)
        #[serde(default)]
        force_path_style: bool,
    },
    /// Google Cloud Storage backup destination
    GCS {
        /// GCS bucket name
        bucket: String,
        /// Secret containing GCS credentials.
        /// Expected key: GOOGLE_APPLICATION_CREDENTIALS (JSON service account key)
        credentials_secret: String,
        /// Path prefix within the bucket
        /// Default: cluster name
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
    },
    /// Azure Blob Storage backup destination
    Azure {
        /// Azure container name
        container: String,
        /// Azure storage account name
        storage_account: String,
        /// Secret containing Azure credentials.
        /// Expected keys: AZURE_STORAGE_ACCESS_KEY or AZURE_STORAGE_SAS_TOKEN
        /// For service principal: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
        credentials_secret: String,
        /// Path prefix within the container
        /// Default: cluster name
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<String>,
        /// Azure environment name (e.g., "AzurePublicCloud", "AzureUSGovernmentCloud")
        #[serde(default, skip_serializing_if = "Option::is_none")]
        environment: Option<String>,
    },
}

impl BackupDestination {
    /// Returns the WAL-G prefix URL for this destination.
    pub fn wal_g_prefix(&self, cluster_name: &str, namespace: &str) -> String {
        match self {
            BackupDestination::S3 { bucket, path, .. } => {
                let base_path = path
                    .clone()
                    .unwrap_or_else(|| format!("{}/{}", namespace, cluster_name));
                format!("s3://{}/{}", bucket, base_path)
            }
            BackupDestination::GCS { bucket, path, .. } => {
                let base_path = path
                    .clone()
                    .unwrap_or_else(|| format!("{}/{}", namespace, cluster_name));
                format!("gs://{}/{}", bucket, base_path)
            }
            BackupDestination::Azure {
                container,
                storage_account,
                path,
                ..
            } => {
                let base_path = path
                    .clone()
                    .unwrap_or_else(|| format!("{}/{}", namespace, cluster_name));
                // WAL-G Azure format: azure://container/path
                // Storage account is set via environment variable
                let _ = storage_account; // Used in env var, not in URL
                format!("azure://{}/{}", container, base_path)
            }
        }
    }

    /// Returns the destination type as a string.
    pub fn destination_type(&self) -> &'static str {
        match self {
            BackupDestination::S3 { .. } => "S3",
            BackupDestination::GCS { .. } => "GCS",
            BackupDestination::Azure { .. } => "Azure",
        }
    }
}

/// Backup status information
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct BackupStatus {
    /// Whether backups are currently configured and enabled
    #[serde(default)]
    pub enabled: bool,

    /// Backup destination type (S3, GCS, Azure)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_type: Option<String>,

    /// Timestamp of the last successful base backup (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup_time: Option<String>,

    /// Size of the last backup in bytes
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup_size_bytes: Option<i64>,

    /// Name/ID of the last backup
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup_name: Option<String>,

    /// Timestamp of the oldest available backup (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oldest_backup_time: Option<String>,

    /// Number of available base backups
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_count: Option<i32>,

    /// Last WAL file archived successfully
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_wal_archived: Option<String>,

    /// Time of last WAL archive (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_wal_archive_time: Option<String>,

    /// Whether WAL archiving is healthy (recent WAL files are being archived)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_archiving_healthy: Option<bool>,

    /// Last backup error message, if any
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    /// Time of last error (RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_time: Option<String>,

    /// Point-in-time recovery window start (oldest recoverable point, RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_window_start: Option<String>,

    /// Point-in-time recovery window end (most recent recoverable point, RFC3339)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_window_end: Option<String>,

    /// Timestamp of the last manually triggered backup request (from annotation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_manual_backup_trigger: Option<String>,

    /// Status of the last manual backup: pending, running, completed, failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_manual_backup_status: Option<String>,
}

/// PgBouncer connection pooling configuration (deployed as separate Deployment)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PgBouncerSpec {
    /// Enable PgBouncer connection pooler
    pub enabled: bool,

    /// Number of PgBouncer replicas (default: 2)
    #[serde(default = "default_pgbouncer_replicas")]
    pub replicas: i32,

    /// Pool mode: session, transaction, or statement (default: transaction)
    #[serde(default = "default_pool_mode")]
    pub pool_mode: String,

    /// Total max database connections distributed across all instances (default: 60)
    #[serde(default = "default_max_db_connections")]
    pub max_db_connections: i32,

    /// Default pool size per user/database pair (default: 20)
    #[serde(default = "default_pool_size")]
    pub default_pool_size: i32,

    /// Maximum client connections per PgBouncer instance (default: 10000)
    #[serde(default = "default_max_client_conn")]
    pub max_client_conn: i32,

    /// PgBouncer container image (default: bitnami/pgbouncer:latest)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Resource requirements for PgBouncer pods
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Enable connection pooler for replica connections (creates separate pooler)
    #[serde(default)]
    pub enable_replica_pooler: bool,
}

fn default_pgbouncer_replicas() -> i32 {
    2
}

fn default_pool_mode() -> String {
    "transaction".to_string()
}

fn default_max_db_connections() -> i32 {
    60
}

fn default_max_client_conn() -> i32 {
    10000
}

fn default_pool_size() -> i32 {
    20
}

/// TLS configuration for PostgreSQL connections.
/// TLS is enabled by default for security-by-design.
/// Requires cert-manager to be installed in the cluster.
///
/// The operator creates a Certificate resource which cert-manager uses to
/// generate and automatically rotate TLS certificates.
///
/// # Example
/// ```yaml
/// tls:
///   enabled: true  # default
///   issuerRef:
///     name: letsencrypt-prod
///     kind: ClusterIssuer
/// ```
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TLSSpec {
    /// Enable TLS for PostgreSQL connections.
    /// Default: true (secure by default)
    #[serde(default = "default_tls_enabled")]
    pub enabled: bool,

    /// Reference to a cert-manager Issuer or ClusterIssuer.
    /// Required when TLS is enabled.
    /// The operator creates a Certificate resource referencing this issuer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issuer_ref: Option<IssuerRef>,

    /// Additional DNS names to include in the certificate.
    /// The operator automatically includes:
    /// - {cluster-name}-primary.{namespace}.svc
    /// - {cluster-name}-repl.{namespace}.svc
    /// - {cluster-name}.{namespace}.svc
    /// - *.{cluster-name}.{namespace}.svc.cluster.local
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_dns_names: Vec<String>,

    /// Certificate duration (e.g., "2160h" for 90 days).
    /// Default: 2160h (90 days)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// Certificate renewal time before expiry (e.g., "360h" for 15 days).
    /// Default: 360h (15 days before expiry)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renew_before: Option<String>,
}

/// Reference to a cert-manager Issuer or ClusterIssuer
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IssuerRef {
    /// Name of the Issuer or ClusterIssuer
    pub name: String,

    /// Kind of the issuer: Issuer or ClusterIssuer
    /// Default: ClusterIssuer
    #[serde(default = "default_issuer_kind")]
    pub kind: IssuerKind,

    /// Group of the issuer resource.
    /// Default: cert-manager.io
    #[serde(default = "default_issuer_group")]
    pub group: String,
}

/// Kind of cert-manager issuer
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
pub enum IssuerKind {
    /// Namespace-scoped Issuer
    Issuer,
    /// Cluster-scoped ClusterIssuer
    #[default]
    ClusterIssuer,
}

impl std::fmt::Display for IssuerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IssuerKind::Issuer => write!(f, "Issuer"),
            IssuerKind::ClusterIssuer => write!(f, "ClusterIssuer"),
        }
    }
}

fn default_tls_enabled() -> bool {
    true
}

fn default_issuer_kind() -> IssuerKind {
    IssuerKind::ClusterIssuer
}

fn default_issuer_group() -> String {
    "cert-manager.io".to_string()
}

impl Default for TLSSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        }
    }
}

/// Metrics configuration (Phase 0.3)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSpec {
    /// Enable Prometheus metrics exporter
    pub enabled: bool,

    /// Port for metrics endpoint
    #[serde(default = "default_metrics_port")]
    pub port: i32,
}

fn default_metrics_port() -> i32 {
    9187
}

/// Service configuration for external access
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    /// Service type: ClusterIP, NodePort, or LoadBalancer
    #[serde(default = "default_service_type")]
    pub type_: ServiceType,

    /// Annotations to add to the services (e.g., for cloud provider load balancers)
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,

    /// Load balancer source ranges (CIDR notation) for restricting access
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub load_balancer_source_ranges: Vec<String>,

    /// External traffic policy: Cluster or Local
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_traffic_policy: Option<ExternalTrafficPolicy>,

    /// NodePort for the PostgreSQL port (only used when type is NodePort)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_port: Option<i32>,
}

/// Kubernetes Service type
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
pub enum ServiceType {
    /// ClusterIP (internal only, default)
    #[default]
    ClusterIP,
    /// NodePort (exposes on each node's IP)
    NodePort,
    /// LoadBalancer (provisions external load balancer)
    LoadBalancer,
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceType::ClusterIP => write!(f, "ClusterIP"),
            ServiceType::NodePort => write!(f, "NodePort"),
            ServiceType::LoadBalancer => write!(f, "LoadBalancer"),
        }
    }
}

/// External traffic policy for LoadBalancer/NodePort services
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq)]
pub enum ExternalTrafficPolicy {
    /// Route to any node (default, may lose client IP)
    Cluster,
    /// Route only to local node (preserves client IP)
    Local,
}

impl std::fmt::Display for ExternalTrafficPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExternalTrafficPolicy::Cluster => write!(f, "Cluster"),
            ExternalTrafficPolicy::Local => write!(f, "Local"),
        }
    }
}

fn default_service_type() -> ServiceType {
    ServiceType::ClusterIP
}

/// Status of the PostgresCluster
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct PostgresClusterStatus {
    /// Current phase of the cluster lifecycle
    #[serde(default)]
    pub phase: ClusterPhase,

    /// Number of ready replicas
    #[serde(default)]
    pub ready_replicas: i32,

    /// Total desired replicas
    #[serde(default)]
    pub replicas: i32,

    /// Name of the current primary pod
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_pod: Option<String>,

    /// Names of replica pods
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replica_pods: Vec<String>,

    /// Backup status information
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupStatus>,

    /// Observed generation of the resource
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Kubernetes-style conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Current retry count for exponential backoff
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_count: Option<i32>,

    /// Last error message encountered during reconciliation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,

    /// Timestamp of the last error
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_time: Option<String>,

    /// Previous replica count (used for tracking scaling operations)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_replicas: Option<i32>,

    /// Timestamp when the current phase started (RFC3339 format)
    /// Used to detect stuck states like Creating with invalid storage class
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_started_at: Option<String>,

    /// Current PostgreSQL version running in the cluster
    /// Used to prevent version downgrades which cause data incompatibility
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_version: Option<String>,

    /// Whether TLS is currently enabled on the cluster
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_enabled: Option<bool>,

    /// Whether PgBouncer pooler is currently enabled
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgbouncer_enabled: Option<bool>,

    /// Number of ready PgBouncer replicas
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgbouncer_ready_replicas: Option<i32>,

    /// Detailed pod information with generation tracking (Kubernetes 1.35+)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pods: Vec<PodInfo>,

    /// Resource resize status for each pod (Kubernetes 1.35+)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resize_status: Vec<PodResourceResizeStatus>,

    /// Whether all pods have applied their latest spec (observedGeneration == generation)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub all_pods_synced: Option<bool>,

    /// Source backup information if this cluster was restored from a backup.
    /// This is set after a successful restore and prevents re-restore on subsequent reconciles.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restored_from: Option<RestoredFromInfo>,

    /// Replication lag information for each replica pod.
    /// Used for auto-scaling decisions and monitoring.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replication_lag: Vec<ReplicaLagInfo>,

    /// Maximum replication lag across all replicas (in bytes).
    /// Useful for quick status checks and alerting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_replication_lag_bytes: Option<i64>,

    /// Whether any replica exceeds the configured lag threshold.
    /// When true, those replicas are excluded from reader service routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas_lagging: Option<bool>,

    /// Connection information for applications to connect to this cluster.
    /// Includes service endpoints for primary, replicas, and pooler (if enabled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_info: Option<ConnectionInfo>,
}

/// Connection information for connecting to the PostgreSQL cluster
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    /// Primary (read-write) service endpoint.
    /// Format: {cluster}-primary.{namespace}.svc:5432
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<String>,

    /// Replica (read-only) service endpoint.
    /// Format: {cluster}-repl.{namespace}.svc:5432
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<String>,

    /// PgBouncer pooler endpoint for primary connections.
    /// Only present when PgBouncer is enabled.
    /// Format: {cluster}-pooler.{namespace}.svc:6432
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pooler: Option<String>,

    /// PgBouncer pooler endpoint for replica connections.
    /// Only present when PgBouncer replica pooler is enabled.
    /// Format: {cluster}-pooler-repl.{namespace}.svc:6432
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pooler_replicas: Option<String>,

    /// Name of the Secret containing database credentials.
    /// The secret contains keys: username, password, database
    pub credentials_secret: String,

    /// Default database name
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
}

/// Replication lag information for a single replica
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaLagInfo {
    /// Name of the replica pod
    pub pod_name: String,

    /// Replication lag in bytes (from pg_stat_replication.replay_lag or similar)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_bytes: Option<i64>,

    /// Replication lag as a duration string (e.g., "1.5s", "30ms")
    /// Derived from replay_lag interval
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_time: Option<String>,

    /// Whether this replica exceeds the configured lag threshold
    #[serde(default)]
    pub exceeds_threshold: bool,

    /// Last time the lag was measured (RFC3339 format)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_measured: Option<String>,

    /// Replication state (streaming, catchup, etc.)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

/// Information about the backup a cluster was restored from
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct RestoredFromInfo {
    /// The prefix path of the backup source (e.g., "s3://bucket/path")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_prefix: Option<String>,

    /// The backup type (S3, GCS, Azure)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,

    /// The recovery target time if PITR was used
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_target_time: Option<String>,

    /// Timestamp when the restore was initiated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_started_at: Option<String>,

    /// Timestamp when the restore completed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_completed_at: Option<String>,
}

/// Cluster lifecycle phase
#[derive(Serialize, Deserialize, Clone, Copy, Debug, JsonSchema, Default, PartialEq, Eq, Hash)]
pub enum ClusterPhase {
    /// Cluster is waiting to be created
    #[default]
    Pending,
    /// Cluster resources are being created
    Creating,
    /// Cluster is running and healthy
    Running,
    /// Cluster is being updated (non-scaling spec change)
    Updating,
    /// Cluster is scaling (replica count change)
    Scaling,
    /// Cluster is running but in a degraded state (some replicas not ready)
    Degraded,
    /// Cluster is recovering from a failed state
    Recovering,
    /// Cluster is in a failed state
    Failed,
    /// Cluster is hibernating (scaled to zero, waiting for connections)
    /// Only applicable when scaling.minReplicas = 0
    Hibernating,
    /// Cluster is being deleted
    Deleting,
}

impl std::fmt::Display for ClusterPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterPhase::Pending => write!(f, "Pending"),
            ClusterPhase::Creating => write!(f, "Creating"),
            ClusterPhase::Running => write!(f, "Running"),
            ClusterPhase::Updating => write!(f, "Updating"),
            ClusterPhase::Scaling => write!(f, "Scaling"),
            ClusterPhase::Degraded => write!(f, "Degraded"),
            ClusterPhase::Recovering => write!(f, "Recovering"),
            ClusterPhase::Failed => write!(f, "Failed"),
            ClusterPhase::Hibernating => write!(f, "Hibernating"),
            ClusterPhase::Deleting => write!(f, "Deleting"),
        }
    }
}

/// Kubernetes-style condition
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition
    #[serde(rename = "type")]
    pub type_: String,

    /// Status of the condition: True, False, or Unknown
    pub status: String,

    /// Reason for the condition's last transition
    pub reason: String,

    /// Human-readable message
    pub message: String,

    /// Last time the condition transitioned
    pub last_transition_time: String,

    /// Generation observed when condition was set
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// Detailed pod information including generation tracking (Kubernetes 1.35+)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodInfo {
    /// Pod name
    pub name: String,

    /// Pod's metadata.generation
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,

    /// Pod's status.observedGeneration (from kubelet)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Whether kubelet has processed the latest pod spec
    #[serde(default)]
    pub spec_applied: bool,

    /// Pod role (master/replica from spilo-role label)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Ready status
    #[serde(default)]
    pub ready: bool,
}

/// Status of a pod resize operation (Kubernetes 1.35+)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, PartialEq, Default)]
pub enum PodResizeStatus {
    /// No resize in progress
    #[default]
    NoResize,
    /// Resize proposed but not yet actuated
    Proposed,
    /// Resize in progress
    InProgress,
    /// Resize deferred (waiting for resources)
    Deferred,
    /// Resize cannot be granted (infeasible)
    Infeasible,
}

/// Resource resize status for a single pod (Kubernetes 1.35+)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PodResourceResizeStatus {
    /// Pod name
    pub pod_name: String,

    /// Current resize status
    #[serde(default)]
    pub status: PodResizeStatus,

    /// Current allocated resources (from status.containerStatuses[].allocatedResources)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocated_resources: Option<ResourceList>,

    /// Last transition time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,

    /// Message about resize status
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}
