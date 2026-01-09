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

    /// Service configuration for external access
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<ServiceSpec>,
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

    /// Timestamp of the last successful backup
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_backup: Option<String>,

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
