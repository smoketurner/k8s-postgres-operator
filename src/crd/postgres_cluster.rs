use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// PostgresCluster is the Schema for the postgresclusters API
#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "postgres.example.com",
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
    /// PostgreSQL version (e.g., "15", "16")
    pub version: String,

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

    /// Backup configuration (Phase 0.2)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup: Option<BackupSpec>,

    /// Connection pooling configuration (Phase 0.3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pgbouncer: Option<PgBouncerSpec>,

    /// TLS configuration (Phase 0.3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<TLSSpec>,

    /// Metrics configuration (Phase 0.3)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsSpec>,
}

fn default_replicas() -> i32 {
    1
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

/// Backup configuration (Phase 0.2)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    /// Backup schedule in cron format
    pub schedule: String,

    /// Retention policy for backups
    pub retention: RetentionPolicy,

    /// Backup destination
    pub destination: BackupDestination,
}

/// Retention policy for backups
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RetentionPolicy {
    /// Number of backups to retain
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<i32>,

    /// Maximum age of backups (e.g., "7d", "30d")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<String>,
}

/// Backup destination configuration
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BackupDestination {
    /// Amazon S3 backup destination
    S3 {
        bucket: String,
        region: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        credentials_secret: String,
    },
    /// Google Cloud Storage backup destination
    GCS {
        bucket: String,
        credentials_secret: String,
    },
    /// Azure Blob Storage backup destination
    Azure {
        container: String,
        storage_account: String,
        credentials_secret: String,
    },
}

/// PgBouncer connection pooling configuration (Phase 0.3)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PgBouncerSpec {
    /// Enable PgBouncer sidecar
    pub enabled: bool,

    /// Pool mode: session, transaction, or statement
    #[serde(default = "default_pool_mode")]
    pub pool_mode: String,

    /// Maximum client connections
    #[serde(default = "default_max_client_conn")]
    pub max_client_conn: i32,

    /// Default pool size per user/database pair
    #[serde(default = "default_pool_size")]
    pub default_pool_size: i32,
}

fn default_pool_mode() -> String {
    "transaction".to_string()
}

fn default_max_client_conn() -> i32 {
    100
}

fn default_pool_size() -> i32 {
    20
}

/// TLS configuration (Phase 0.3)
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TLSSpec {
    /// Enable TLS for PostgreSQL connections
    pub enabled: bool,

    /// Secret containing TLS certificate and key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_secret: Option<String>,

    /// Secret containing CA certificate
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_secret: Option<String>,
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

    /// Timestamp of the last successful backup
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup: Option<String>,

    /// Observed generation of the resource
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Kubernetes-style conditions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

/// Cluster lifecycle phase
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, PartialEq)]
pub enum ClusterPhase {
    /// Cluster is waiting to be created
    #[default]
    Pending,
    /// Cluster resources are being created
    Creating,
    /// Cluster is running and healthy
    Running,
    /// Cluster is being updated
    Updating,
    /// Cluster is in a failed state
    Failed,
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
            ClusterPhase::Failed => write!(f, "Failed"),
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
