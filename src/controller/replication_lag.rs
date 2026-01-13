//! Replication lag monitoring for PostgresCluster resources
//!
//! This module provides functionality to collect replication lag status from running
//! PostgreSQL pods via the Patroni REST API.
//!
//! # Replication Lag Collection
//!
//! The collector queries the primary pod's Patroni API at port 8008 to get:
//! - Current replication lag in bytes for each replica
//! - Lag as a time interval (estimated based on throughput)
//!
//! This information is used for:
//! - Auto-scaling decisions (preventing scale-up when replicas are lagging)
//! - Monitoring and alerting
//! - Reader service endpoint selection (excluding lagging replicas)
//!
//! # Requirements
//!
//! **In-cluster deployment**: This module requires the operator to run inside the
//! Kubernetes cluster with network access to pod IPs. When running locally with
//! `make run`, replication lag collection will fail gracefully (non-fatal).
//!
//! **Network policies**: If NetworkPolicies restrict traffic to PostgreSQL pods,
//! ensure the operator namespace is allowed to connect to port 8008.
//!
//! # Security Notes
//!
//! - The Patroni API is accessed over plain HTTP (no TLS)
//! - Communication is within the cluster network
//! - No authentication is used for the Patroni REST API
//! - Consider using a service mesh for mTLS if required

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client, ResourceExt};
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, trace, warn};

use crate::crd::{PostgresCluster, ReplicaLagInfo};

/// Overall timeout for the entire HTTP operation (connect + request + response)
const OVERALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Patroni REST API port
const PATRONI_PORT: u16 = 8008;

/// Default replication lag threshold in seconds
const DEFAULT_LAG_THRESHOLD_SECS: f64 = 30.0;

/// Default throughput assumption in MB/s for lag time estimation
const DEFAULT_THROUGHPUT_MB_PER_SEC: i32 = 100;

/// Maximum valid length for Kubernetes label values (RFC 1123)
const MAX_LABEL_VALUE_LENGTH: usize = 63;

/// Result type for replication lag operations
pub(crate) type Result<T> = std::result::Result<T, ReplicationLagError>;

/// Errors that can occur during replication lag collection
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplicationLagError {
    /// Kubernetes API error (transient - may be retried)
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    /// JSON parsing error (permanent - response was malformed)
    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// No pods available for status collection (transient - cluster may be starting)
    #[error("No primary pod available")]
    NoPrimaryPod,

    /// Pod IP not available (transient - pod may be starting)
    #[error("Pod IP not available for {0}")]
    NoPodIp(String),

    /// HTTP request error (transient - network issue)
    #[error("HTTP request failed: {0}")]
    HttpError(String),

    /// Connection error (transient - network issue)
    #[error("Connection failed: {0}")]
    ConnectionError(String),

    /// Request timed out (transient)
    #[error("Request timed out")]
    Timeout,

    /// Invalid cluster name for label selector (permanent - configuration error)
    #[error("Invalid cluster name for label selector: {0}")]
    InvalidClusterName(String),
}

/// Patroni cluster member information (from /cluster endpoint)
#[derive(Debug, Deserialize)]
struct PatroniClusterResponse {
    /// List of cluster members
    members: Vec<PatroniMember>,
}

/// Individual Patroni cluster member
#[derive(Debug, Deserialize)]
struct PatroniMember {
    /// Member name (pod name)
    name: String,

    /// Member role: "leader", "replica", "sync_standby"
    role: String,

    /// Replication lag in bytes (for replicas)
    #[serde(default)]
    lag: Option<i64>,

    /// Member state: "running", "streaming", etc.
    #[serde(default)]
    state: Option<String>,
}

/// Replication lag information collected from the cluster
#[derive(Debug, Default)]
pub(crate) struct ReplicationLagStatus {
    /// Per-replica lag information
    pub replicas: Vec<ReplicaLagInfo>,

    /// Maximum lag across all replicas (in bytes)
    pub max_lag_bytes: Option<i64>,

    /// Whether any replica exceeds the configured threshold
    pub any_exceeds_threshold: bool,
}

/// Configuration for lag threshold calculation
#[derive(Debug, Clone)]
struct LagThresholdConfig {
    /// Threshold in bytes (if explicitly set)
    threshold_bytes: Option<i64>,
    /// Threshold in seconds (for time-based calculation)
    threshold_secs: f64,
    /// Throughput in MB/s for bytes-to-time conversion
    throughput_mb_per_sec: i32,
}

impl LagThresholdConfig {
    /// Create config from cluster spec
    fn from_cluster(cluster: &PostgresCluster) -> Self {
        let (threshold_bytes, threshold_secs, throughput_mb_per_sec) = cluster
            .spec
            .scaling
            .as_ref()
            .map(|s| {
                (
                    s.lag_threshold_bytes,
                    parse_duration_to_secs(&s.replication_lag_threshold),
                    s.estimated_throughput_mb_per_sec,
                )
            })
            .unwrap_or((
                None,
                DEFAULT_LAG_THRESHOLD_SECS,
                DEFAULT_THROUGHPUT_MB_PER_SEC,
            ));

        Self {
            threshold_bytes,
            threshold_secs,
            throughput_mb_per_sec,
        }
    }

    /// Get the effective threshold in bytes
    fn effective_threshold_bytes(&self) -> i64 {
        self.threshold_bytes.unwrap_or_else(|| {
            let throughput_bytes_per_sec = self.throughput_mb_per_sec as f64 * 1_000_000.0;
            (self.threshold_secs * throughput_bytes_per_sec) as i64
        })
    }

    /// Convert lag bytes to estimated time string
    fn lag_bytes_to_time_string(&self, bytes: i64) -> String {
        let throughput_bytes_per_sec = self.throughput_mb_per_sec as f64 * 1_000_000.0;
        let secs = bytes as f64 / throughput_bytes_per_sec;
        if secs < 1.0 {
            format!("{:.0}ms", secs * 1000.0)
        } else if secs < 60.0 {
            format!("{:.1}s", secs)
        } else {
            format!("{:.1}m", secs / 60.0)
        }
    }
}

/// Collector for replication lag status
pub(crate) struct ReplicationLagCollector {
    client: Client,
    namespace: String,
    cluster_name: String,
    lag_config: LagThresholdConfig,
}

impl ReplicationLagCollector {
    /// Create a new replication lag collector
    ///
    /// # Arguments
    /// * `client` - Kubernetes API client
    /// * `cluster` - The PostgresCluster to collect lag information for
    #[must_use]
    pub fn new(client: Client, cluster: &PostgresCluster) -> Self {
        let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
        let cluster_name = cluster.name_any();
        let lag_config = LagThresholdConfig::from_cluster(cluster);

        Self {
            client,
            namespace,
            cluster_name,
            lag_config,
        }
    }

    /// Collect replication lag status from the cluster
    ///
    /// This queries the Patroni API directly on the primary pod's IP.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The primary pod cannot be found
    /// - Network connectivity to the pod fails
    /// - The Patroni API returns an invalid response
    pub async fn collect(&self) -> Result<ReplicationLagStatus> {
        // Validate cluster name for label selector
        validate_label_value(&self.cluster_name)?;

        // Find the primary pod and get its IP
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let pod_ip = self.find_primary_pod_ip(&pods).await?;

        // Query Patroni cluster endpoint with overall timeout
        let cluster_info = timeout(OVERALL_TIMEOUT, self.query_patroni_cluster(&pod_ip))
            .await
            .map_err(|_| ReplicationLagError::Timeout)??;

        // Process the response into ReplicationLagStatus
        Ok(self.process_cluster_info(cluster_info))
    }

    /// Find the primary (leader) pod and return its IP
    async fn find_primary_pod_ip(&self, pods: &Api<Pod>) -> Result<String> {
        let label_selector = format!(
            "postgres-operator.smoketurner.com/cluster={},spilo-role=master",
            self.cluster_name
        );

        let pod_list = pods
            .list(&kube::api::ListParams::default().labels(&label_selector))
            .await?;

        let pod = pod_list
            .items
            .first()
            .ok_or(ReplicationLagError::NoPrimaryPod)?;

        let pod_name = pod.metadata.name.as_deref().unwrap_or("unknown");

        pod.status
            .as_ref()
            .and_then(|s| s.pod_ip.clone())
            .ok_or_else(|| ReplicationLagError::NoPodIp(pod_name.to_string()))
    }

    /// Query the Patroni /cluster endpoint via direct HTTP
    async fn query_patroni_cluster(&self, pod_ip: &str) -> Result<PatroniClusterResponse> {
        let addr: SocketAddr = format!("{}:{}", pod_ip, PATRONI_PORT)
            .parse()
            .map_err(|e| ReplicationLagError::ConnectionError(format!("Invalid address: {}", e)))?;

        // Connect to Patroni
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| ReplicationLagError::ConnectionError(e.to_string()))?;

        let io = TokioIo::new(stream);

        // Create HTTP/1.1 connection
        let (mut sender, conn) = http1::handshake(io)
            .await
            .map_err(|e| ReplicationLagError::HttpError(e.to_string()))?;

        // Build request
        let req = Request::builder()
            .method("GET")
            .uri("/cluster")
            .header("Host", format!("{}:{}", pod_ip, PATRONI_PORT))
            .body(Empty::<Bytes>::new())
            .map_err(|e| ReplicationLagError::HttpError(e.to_string()))?;

        // Use tokio::select! to properly handle the connection lifecycle
        // The connection task will be cancelled when we're done with the request
        let response = tokio::select! {
            // Drive the connection
            conn_result = conn => {
                if let Err(e) = conn_result {
                    debug!("Connection closed: {}", e);
                }
                return Err(ReplicationLagError::ConnectionError("Connection closed unexpectedly".to_string()));
            }
            // Send the request
            response = sender.send_request(req) => {
                response.map_err(|e| ReplicationLagError::HttpError(e.to_string()))?
            }
        };

        // Check status
        if !response.status().is_success() {
            return Err(ReplicationLagError::HttpError(format!(
                "HTTP {}: {}",
                response.status().as_u16(),
                response.status().canonical_reason().unwrap_or("Unknown")
            )));
        }

        // Read body
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|e| ReplicationLagError::HttpError(e.to_string()))?
            .to_bytes();

        let body_str = String::from_utf8_lossy(&body);

        trace!(
            pod_ip = pod_ip,
            response = %body_str,
            "Patroni cluster response"
        );

        // Parse JSON
        serde_json::from_slice(&body).map_err(|e| {
            debug!(
                pod_ip = pod_ip,
                response = %body_str,
                error = %e,
                "Failed to parse Patroni response"
            );
            ReplicationLagError::JsonError(e)
        })
    }

    /// Process Patroni cluster info into ReplicationLagStatus
    fn process_cluster_info(&self, info: PatroniClusterResponse) -> ReplicationLagStatus {
        process_patroni_cluster_info(info, &self.lag_config)
    }
}

/// Validate that a string is valid for use in a Kubernetes label selector.
///
/// Label values must:
/// - Be 63 characters or less
/// - Contain only alphanumeric characters, '-', '_', or '.'
/// - Begin and end with an alphanumeric character
fn validate_label_value(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(ReplicationLagError::InvalidClusterName(
            "Cluster name cannot be empty".to_string(),
        ));
    }

    if value.len() > MAX_LABEL_VALUE_LENGTH {
        return Err(ReplicationLagError::InvalidClusterName(format!(
            "Cluster name exceeds {} characters",
            MAX_LABEL_VALUE_LENGTH
        )));
    }

    let chars: Vec<char> = value.chars().collect();

    // Must start and end with alphanumeric
    if !chars.first().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return Err(ReplicationLagError::InvalidClusterName(
            "Cluster name must start with alphanumeric character".to_string(),
        ));
    }

    if !chars.last().is_some_and(|c| c.is_ascii_alphanumeric()) {
        return Err(ReplicationLagError::InvalidClusterName(
            "Cluster name must end with alphanumeric character".to_string(),
        ));
    }

    // All characters must be alphanumeric, '-', '_', or '.'
    for c in &chars {
        if !c.is_ascii_alphanumeric() && *c != '-' && *c != '_' && *c != '.' {
            return Err(ReplicationLagError::InvalidClusterName(format!(
                "Invalid character '{}' in cluster name",
                c
            )));
        }
    }

    Ok(())
}

/// Process Patroni cluster info into ReplicationLagStatus
///
/// Standalone function for easier testing.
fn process_patroni_cluster_info(
    info: PatroniClusterResponse,
    config: &LagThresholdConfig,
) -> ReplicationLagStatus {
    let mut status = ReplicationLagStatus::default();
    let lag_threshold_bytes = config.effective_threshold_bytes();

    for member in info.members {
        // Skip the leader
        if member.role == "leader" {
            continue;
        }

        let lag_bytes = member.lag;
        let exceeds_threshold = lag_bytes.is_some_and(|lag| lag > lag_threshold_bytes);

        // Convert lag bytes to estimated time string
        let lag_time = lag_bytes.map(|bytes| config.lag_bytes_to_time_string(bytes));

        status.replicas.push(ReplicaLagInfo {
            pod_name: member.name,
            lag_bytes,
            lag_time,
            exceeds_threshold,
            last_measured: Some(chrono::Utc::now().to_rfc3339()),
            state: member.state,
        });

        // Update max lag
        if let Some(lag) = lag_bytes {
            status.max_lag_bytes = Some(status.max_lag_bytes.map_or(lag, |max| max.max(lag)));
        }

        if exceeds_threshold {
            status.any_exceeds_threshold = true;
        }
    }

    status
}

/// Parse a duration string (e.g., "30s", "1m", "2m30s") to seconds
fn parse_duration_to_secs(duration_str: &str) -> f64 {
    let s = duration_str.trim().to_lowercase();

    // Try parsing as seconds only (e.g., "30s")
    if let Some(secs) = s.strip_suffix('s')
        && let Ok(n) = secs.parse::<f64>()
    {
        return n;
    }

    // Try parsing as minutes only (e.g., "1m")
    if let Some(mins) = s.strip_suffix('m')
        && !mins.contains('s')
        && let Ok(n) = mins.parse::<f64>()
    {
        return n * 60.0;
    }

    // Try parsing as minutes and seconds (e.g., "2m30s")
    if let Some(rest) = s.strip_suffix('s')
        && let Some((mins, secs)) = rest.rsplit_once('m')
        && let (Ok(m), Ok(sec)) = (mins.parse::<f64>(), secs.parse::<f64>())
    {
        return m * 60.0 + sec;
    }

    // Default fallback
    warn!(
        duration = duration_str,
        "Failed to parse duration, using default"
    );
    DEFAULT_LAG_THRESHOLD_SECS
}

/// Convenience function to collect replication lag for a cluster
///
/// # Arguments
/// * `client` - Kubernetes API client
/// * `cluster` - The PostgresCluster to collect lag information for
///
/// # Returns
/// Replication lag status for all replicas in the cluster
#[must_use = "This function returns the collected lag status which should be used"]
pub(crate) async fn collect_replication_lag(
    client: Client,
    cluster: &PostgresCluster,
) -> Result<ReplicationLagStatus> {
    let collector = ReplicationLagCollector::new(client, cluster);
    collector.collect().await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    // =========================================================================
    // Duration parsing tests
    // =========================================================================

    #[test]
    fn test_parse_duration_seconds() {
        assert_eq!(parse_duration_to_secs("30s"), 30.0);
        assert_eq!(parse_duration_to_secs("5s"), 5.0);
        assert_eq!(parse_duration_to_secs("0s"), 0.0);
    }

    #[test]
    fn test_parse_duration_minutes() {
        assert_eq!(parse_duration_to_secs("1m"), 60.0);
        assert_eq!(parse_duration_to_secs("2m"), 120.0);
        assert_eq!(parse_duration_to_secs("5m"), 300.0);
    }

    #[test]
    fn test_parse_duration_combined() {
        assert_eq!(parse_duration_to_secs("1m30s"), 90.0);
        assert_eq!(parse_duration_to_secs("2m30s"), 150.0);
        assert_eq!(parse_duration_to_secs("0m30s"), 30.0);
    }

    #[test]
    fn test_parse_duration_invalid_returns_default() {
        assert_eq!(
            parse_duration_to_secs("invalid"),
            DEFAULT_LAG_THRESHOLD_SECS
        );
        assert_eq!(parse_duration_to_secs(""), DEFAULT_LAG_THRESHOLD_SECS);
    }

    // =========================================================================
    // Lag threshold configuration tests
    // =========================================================================

    #[test]
    fn test_lag_threshold_bytes_calculation() {
        // 30s * 100MB/s = 3GB
        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };
        assert_eq!(config.effective_threshold_bytes(), 3_000_000_000);
    }

    #[test]
    fn test_lag_threshold_bytes_explicit() {
        // Explicit bytes takes precedence
        let config = LagThresholdConfig {
            threshold_bytes: Some(1_000_000_000),
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };
        assert_eq!(config.effective_threshold_bytes(), 1_000_000_000);
    }

    #[test]
    fn test_lag_threshold_custom_throughput() {
        // 30s * 50MB/s = 1.5GB
        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 50,
        };
        assert_eq!(config.effective_threshold_bytes(), 1_500_000_000);
    }

    #[test]
    fn test_lag_time_formatting() {
        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        // 50MB at 100MB/s = 0.5s = 500ms
        assert_eq!(config.lag_bytes_to_time_string(50_000_000), "500ms");

        // 500MB at 100MB/s = 5s
        assert_eq!(config.lag_bytes_to_time_string(500_000_000), "5.0s");

        // 12GB at 100MB/s = 120s = 2m
        assert_eq!(config.lag_bytes_to_time_string(12_000_000_000), "2.0m");
    }

    // =========================================================================
    // Label validation tests
    // =========================================================================

    #[test]
    fn test_validate_label_value_valid() {
        assert!(validate_label_value("my-cluster").is_ok());
        assert!(validate_label_value("test123").is_ok());
        assert!(validate_label_value("a").is_ok());
        assert!(validate_label_value("cluster-name-with.dots").is_ok());
        assert!(validate_label_value("cluster_with_underscores").is_ok());
    }

    #[test]
    fn test_validate_label_value_invalid_empty() {
        let result = validate_label_value("");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_label_value_invalid_start() {
        let result = validate_label_value("-invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_label_value_invalid_end() {
        let result = validate_label_value("invalid-");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_label_value_invalid_char() {
        let result = validate_label_value("invalid@name");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_label_value_too_long() {
        let long_name = "a".repeat(64);
        let result = validate_label_value(&long_name);
        assert!(result.is_err());
    }

    // =========================================================================
    // Cluster info processing tests
    // =========================================================================

    #[test]
    fn test_process_cluster_info() {
        let response = PatroniClusterResponse {
            members: vec![
                PatroniMember {
                    name: "test-0".to_string(),
                    role: "leader".to_string(),
                    lag: None,
                    state: Some("running".to_string()),
                },
                PatroniMember {
                    name: "test-1".to_string(),
                    role: "replica".to_string(),
                    lag: Some(1000),
                    state: Some("streaming".to_string()),
                },
                PatroniMember {
                    name: "test-2".to_string(),
                    role: "replica".to_string(),
                    lag: Some(5_000_000_000), // 5GB - exceeds 30s threshold at 100MB/s
                    state: Some("streaming".to_string()),
                },
            ],
        };

        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        // Should only have 2 replicas (leader excluded)
        assert_eq!(status.replicas.len(), 2);

        // Max lag should be 5GB
        assert_eq!(status.max_lag_bytes, Some(5_000_000_000));

        // One replica exceeds threshold
        assert!(status.any_exceeds_threshold);

        // Check individual replicas
        let replica1 = status
            .replicas
            .iter()
            .find(|r| r.pod_name == "test-1")
            .unwrap();
        assert_eq!(replica1.lag_bytes, Some(1000));
        assert!(!replica1.exceeds_threshold);

        let replica2 = status
            .replicas
            .iter()
            .find(|r| r.pod_name == "test-2")
            .unwrap();
        assert_eq!(replica2.lag_bytes, Some(5_000_000_000));
        assert!(replica2.exceeds_threshold);
    }

    #[test]
    fn test_process_cluster_info_no_replicas() {
        let response = PatroniClusterResponse {
            members: vec![PatroniMember {
                name: "test-0".to_string(),
                role: "leader".to_string(),
                lag: None,
                state: Some("running".to_string()),
            }],
        };

        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        assert!(status.replicas.is_empty());
        assert_eq!(status.max_lag_bytes, None);
        assert!(!status.any_exceeds_threshold);
    }

    #[test]
    fn test_process_cluster_info_empty_members() {
        let response = PatroniClusterResponse { members: vec![] };

        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        assert!(status.replicas.is_empty());
        assert_eq!(status.max_lag_bytes, None);
        assert!(!status.any_exceeds_threshold);
    }

    #[test]
    fn test_process_cluster_info_null_lag() {
        let response = PatroniClusterResponse {
            members: vec![PatroniMember {
                name: "replica-0".to_string(),
                role: "replica".to_string(),
                lag: None, // Null lag (replica might be catching up)
                state: Some("streaming".to_string()),
            }],
        };

        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        assert_eq!(status.replicas.len(), 1);
        assert_eq!(status.replicas[0].lag_bytes, None);
        assert_eq!(status.replicas[0].lag_time, None);
        assert!(!status.replicas[0].exceeds_threshold);
        assert_eq!(status.max_lag_bytes, None);
    }

    #[test]
    fn test_process_cluster_info_zero_lag() {
        let response = PatroniClusterResponse {
            members: vec![PatroniMember {
                name: "replica-0".to_string(),
                role: "replica".to_string(),
                lag: Some(0), // Zero lag - replica is fully caught up
                state: Some("streaming".to_string()),
            }],
        };

        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        assert_eq!(status.replicas.len(), 1);
        assert_eq!(status.replicas[0].lag_bytes, Some(0));
        assert_eq!(status.replicas[0].lag_time, Some("0ms".to_string()));
        assert!(!status.replicas[0].exceeds_threshold);
        assert_eq!(status.max_lag_bytes, Some(0));
    }

    #[test]
    fn test_process_cluster_info_with_custom_threshold_bytes() {
        let response = PatroniClusterResponse {
            members: vec![
                PatroniMember {
                    name: "replica-under".to_string(),
                    role: "replica".to_string(),
                    lag: Some(500_000_000), // 500MB - under 1GB threshold
                    state: None,
                },
                PatroniMember {
                    name: "replica-over".to_string(),
                    role: "replica".to_string(),
                    lag: Some(1_500_000_000), // 1.5GB - over 1GB threshold
                    state: None,
                },
            ],
        };

        // Use explicit byte threshold of 1GB
        let config = LagThresholdConfig {
            threshold_bytes: Some(1_000_000_000),
            threshold_secs: 30.0, // Ignored when threshold_bytes is set
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        let under = status
            .replicas
            .iter()
            .find(|r| r.pod_name == "replica-under")
            .unwrap();
        assert!(!under.exceeds_threshold);

        let over = status
            .replicas
            .iter()
            .find(|r| r.pod_name == "replica-over")
            .unwrap();
        assert!(over.exceeds_threshold);
    }

    #[test]
    fn test_process_cluster_info_sync_standby() {
        // sync_standby is also a replica type in Patroni
        let response = PatroniClusterResponse {
            members: vec![
                PatroniMember {
                    name: "test-0".to_string(),
                    role: "leader".to_string(),
                    lag: None,
                    state: Some("running".to_string()),
                },
                PatroniMember {
                    name: "test-1".to_string(),
                    role: "sync_standby".to_string(),
                    lag: Some(100),
                    state: Some("streaming".to_string()),
                },
            ],
        };

        let config = LagThresholdConfig {
            threshold_bytes: None,
            threshold_secs: 30.0,
            throughput_mb_per_sec: 100,
        };

        let status = process_patroni_cluster_info(response, &config);

        // sync_standby should be included (it's not "leader")
        assert_eq!(status.replicas.len(), 1);
        assert_eq!(status.replicas[0].pod_name, "test-1");
    }
}
