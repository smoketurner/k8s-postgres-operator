//! KEDA HTTP Add-on resource generation for PostgreSQL scale-to-zero
//!
//! Creates KEDA HTTPScaledObject resources to enable scale-to-zero functionality
//! for development PostgreSQL clusters (replicas=1).
//!
//! When enabled, the cluster scales to 0 pods after idle timeout. Connection
//! attempts wake up the cluster via KEDA HTTP Add-on interceptor.
//!
//! Requirements:
//! - KEDA must be installed with HTTP Add-on enabled
//! - The cluster must have replicas=1 (development mode)
//! - scaling.scaleToZero.enabled must be true
//! - PgBouncer should be enabled for connection queuing during wake-up

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt;
use kube::api::DynamicObject;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, standard_labels};

/// KEDA HTTP Add-on API group
pub const HTTP_API_GROUP: &str = "http.keda.sh";
/// KEDA HTTP Add-on API version
pub const HTTP_API_VERSION: &str = "v1alpha1";
/// HTTPScaledObject kind
pub const HTTP_SCALED_OBJECT_KIND: &str = "HTTPScaledObject";

/// KEDA HTTPScaledObject spec
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpScaledObjectSpec {
    /// Hosts that trigger scaling (DNS names)
    pub hosts: Vec<String>,
    /// Reference to the target deployment/statefulset
    pub scale_target_ref: HttpScaleTargetRef,
    /// Replicas configuration
    pub replicas: ReplicasConfig,
    /// Target pending requests that triggers scaling (per replica)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_pending_requests: Option<i32>,
    /// Scaling metric type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_metric: Option<ScalingMetricConfig>,
}

/// Reference to the target resource for HTTP scaling
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HttpScaleTargetRef {
    /// Name of the target deployment/statefulset
    pub name: String,
    /// Kind of the target (Deployment or StatefulSet)
    pub kind: String,
    /// API version of the target
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    /// Service name for routing
    pub service: String,
    /// Port on the service
    pub port: i32,
}

/// Replica count configuration for HTTP scaling
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ReplicasConfig {
    /// Minimum replicas (0 for scale-to-zero)
    pub min: i32,
    /// Maximum replicas
    pub max: i32,
}

/// Scaling metric configuration
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScalingMetricConfig {
    /// Concurrency target per replica
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<ConcurrencyConfig>,
    /// Rate target per replica
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate: Option<RateConfig>,
}

/// Concurrency-based scaling metric
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ConcurrencyConfig {
    /// Target concurrent requests per replica
    pub target_value: i32,
}

/// Rate-based scaling metric
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RateConfig {
    /// Target requests per second per replica
    pub target_value: i32,
    /// Time window for rate calculation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window: Option<String>,
}

/// Check if scale-to-zero should be enabled for a cluster
pub fn should_enable_scale_to_zero(cluster: &PostgresCluster) -> bool {
    // Check if scaling is configured with minReplicas = 0
    cluster
        .spec
        .scaling
        .as_ref()
        .is_some_and(|s| s.is_scale_to_zero())
}

/// Generate a KEDA HTTPScaledObject for scale-to-zero functionality
///
/// Creates an HTTPScaledObject that intercepts connections to the PostgreSQL
/// service and wakes up the cluster when connections arrive.
///
/// Returns None if scale-to-zero is not enabled or replicas != 1.
pub fn generate_http_scaled_object(cluster: &PostgresCluster) -> Option<DynamicObject> {
    if !should_enable_scale_to_zero(cluster) {
        return None;
    }

    let name = format!("{}-scale-to-zero", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());

    let labels = standard_labels(&cluster_name);

    // Determine the service name - prefer PgBouncer if enabled for connection queuing
    let (service_name, target_kind, target_name) =
        if cluster.spec.pgbouncer.as_ref().is_some_and(|p| p.enabled) {
            // Use PgBouncer deployment for scale-to-zero
            (
                format!("{}-pgbouncer", cluster_name),
                "Deployment".to_string(),
                format!("{}-pgbouncer", cluster_name),
            )
        } else {
            // Use StatefulSet directly (less ideal for connection handling)
            (
                format!("{}-primary", cluster_name),
                "StatefulSet".to_string(),
                cluster_name.clone(),
            )
        };

    // Build DNS names for the service
    let hosts = vec![
        format!("{}.{}.svc.cluster.local", service_name, ns),
        format!("{}.{}.svc", service_name, ns),
        format!("{}.{}", service_name, ns),
        service_name.clone(),
    ];

    let spec = HttpScaledObjectSpec {
        hosts,
        scale_target_ref: HttpScaleTargetRef {
            name: target_name,
            kind: target_kind,
            api_version: Some("apps/v1".to_string()),
            service: service_name,
            port: 5432, // PostgreSQL port
        },
        replicas: ReplicasConfig {
            min: 0, // Scale to zero
            max: 1, // Development cluster, single instance
        },
        target_pending_requests: Some(1), // Wake up on first connection
        scaling_metric: Some(ScalingMetricConfig {
            concurrency: Some(ConcurrencyConfig { target_value: 100 }),
            rate: None,
        }),
    };

    // Create as DynamicObject since HTTPScaledObject is a CRD
    let mut obj = DynamicObject::new(
        &name,
        &kube::api::ApiResource {
            group: HTTP_API_GROUP.to_string(),
            version: HTTP_API_VERSION.to_string(),
            kind: HTTP_SCALED_OBJECT_KIND.to_string(),
            api_version: format!("{}/{}", HTTP_API_GROUP, HTTP_API_VERSION),
            plural: "httpscaledobjects".to_string(),
        },
    );

    obj.metadata = ObjectMeta {
        name: Some(name),
        namespace: Some(ns),
        labels: Some(labels),
        owner_references: Some(vec![owner_reference(cluster)]),
        ..Default::default()
    };

    obj.data = json!({
        "spec": spec
    });

    Some(obj)
}

/// Generate a ScaledObject for the StatefulSet when using scale-to-zero with PgBouncer
///
/// When PgBouncer is enabled, we need a separate ScaledObject for the PostgreSQL
/// StatefulSet that scales based on the PgBouncer's connection state.
///
/// Returns None if scale-to-zero is not enabled or PgBouncer is not enabled.
pub fn generate_statefulset_scaled_object(cluster: &PostgresCluster) -> Option<DynamicObject> {
    if !should_enable_scale_to_zero(cluster) {
        return None;
    }

    // Only needed when PgBouncer is enabled
    if !cluster.spec.pgbouncer.as_ref().is_some_and(|p| p.enabled) {
        return None;
    }

    let scaling = cluster.spec.scaling.as_ref()?;

    let name = format!("{}-sts-scaler", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    // Parse idle timeout to seconds (default 30m = 1800s)
    let idle_timeout_secs = parse_duration_to_seconds(&scaling.idle_timeout).unwrap_or(1800);

    // Create as DynamicObject for KEDA ScaledObject
    let mut obj = DynamicObject::new(
        &name,
        &kube::api::ApiResource {
            group: "keda.sh".to_string(),
            version: "v1alpha1".to_string(),
            kind: "ScaledObject".to_string(),
            api_version: "keda.sh/v1alpha1".to_string(),
            plural: "scaledobjects".to_string(),
        },
    );

    obj.metadata = ObjectMeta {
        name: Some(name),
        namespace: ns,
        labels: Some(labels),
        owner_references: Some(vec![owner_reference(cluster)]),
        ..Default::default()
    };

    obj.data = json!({
        "spec": {
            "scaleTargetRef": {
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "name": cluster_name
            },
            "minReplicaCount": 0,
            "maxReplicaCount": 1,
            "cooldownPeriod": idle_timeout_secs,
            "pollingInterval": 30,
            "triggers": [
                {
                    "type": "prometheus",
                    "metadata": {
                        "serverAddress": "http://prometheus.monitoring.svc:9090",
                        "metricName": "pgbouncer_active_clients",
                        "query": format!(
                            "sum(pgbouncer_pools_client_active{{kubernetes_namespace=\"{}\", app_kubernetes_io_instance=\"{}\"}})",
                            cluster.namespace().unwrap_or_else(|| "default".to_string()),
                            cluster_name
                        ),
                        "threshold": "1"
                    }
                }
            ],
            "advanced": {
                "restoreToOriginalReplicaCount": true
            }
        }
    });

    Some(obj)
}

/// Parse a duration string like "30m", "1h", "300s" to seconds
fn parse_duration_to_seconds(duration: &str) -> Option<i64> {
    let duration = duration.trim();
    if duration.is_empty() {
        return None;
    }

    // Try to parse as a number with a suffix
    let (num_str, multiplier) = if duration.ends_with('s') {
        (&duration[..duration.len() - 1], 1)
    } else if duration.ends_with('m') {
        (&duration[..duration.len() - 1], 60)
    } else if duration.ends_with('h') {
        (&duration[..duration.len() - 1], 3600)
    } else if duration.ends_with('d') {
        (&duration[..duration.len() - 1], 86400)
    } else {
        // Assume seconds if no suffix
        (duration, 1)
    };

    num_str.parse::<i64>().ok().map(|n| n * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        PgBouncerSpec, PostgresClusterSpec, PostgresVersion, ScalingSpec, StorageSpec,
    };

    fn create_test_cluster(
        replicas: i32,
        scaling: Option<ScalingSpec>,
        pgbouncer: Option<PgBouncerSpec>,
    ) -> PostgresCluster {
        use crate::crd::TLSSpec;
        use std::collections::BTreeMap;

        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V17,
                replicas,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                scaling,
                pgbouncer,
                resources: None,
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                backup: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_should_enable_scale_to_zero_enabled() {
        let cluster = create_test_cluster(
            1,
            Some(ScalingSpec {
                min_replicas: Some(0),
                max_replicas: 1,
                idle_timeout: "30m".to_string(),
                wakeup_timeout: "5m".to_string(),
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
            }),
            None,
        );

        assert!(should_enable_scale_to_zero(&cluster));
    }

    #[test]
    fn test_should_enable_scale_to_zero_not_enabled() {
        // minReplicas = 1, not scale-to-zero
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(1),
                max_replicas: 10,
                idle_timeout: "30m".to_string(),
                wakeup_timeout: "5m".to_string(),
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
            }),
            None,
        );

        assert!(!should_enable_scale_to_zero(&cluster));
    }

    #[test]
    fn test_should_enable_scale_to_zero_no_scaling() {
        let cluster = create_test_cluster(1, None, None);

        assert!(!should_enable_scale_to_zero(&cluster));
    }

    #[test]
    fn test_generate_http_scaled_object_without_pgbouncer() {
        let cluster = create_test_cluster(
            1,
            Some(ScalingSpec {
                min_replicas: Some(0),
                max_replicas: 1,
                idle_timeout: "30m".to_string(),
                wakeup_timeout: "5m".to_string(),
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
            }),
            None,
        );

        let obj = generate_http_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        assert_eq!(
            obj.metadata.name,
            Some("test-cluster-scale-to-zero".to_string())
        );

        let spec: HttpScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");
        assert_eq!(spec.scale_target_ref.kind, "StatefulSet");
        assert_eq!(spec.replicas.min, 0);
        assert_eq!(spec.replicas.max, 1);
    }

    #[test]
    fn test_generate_http_scaled_object_with_pgbouncer() {
        let cluster = create_test_cluster(
            1,
            Some(ScalingSpec {
                min_replicas: Some(0),
                max_replicas: 1,
                idle_timeout: "30m".to_string(),
                wakeup_timeout: "5m".to_string(),
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
            }),
            Some(PgBouncerSpec {
                enabled: true,
                replicas: 2,
                pool_mode: "transaction".to_string(),
                max_db_connections: 60,
                default_pool_size: 20,
                max_client_conn: 10000,
                image: None,
                resources: None,
                enable_replica_pooler: false,
            }),
        );

        let obj = generate_http_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        let spec: HttpScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");
        assert_eq!(spec.scale_target_ref.kind, "Deployment");
        assert_eq!(spec.scale_target_ref.name, "test-cluster-pgbouncer");
    }

    #[test]
    fn test_generate_statefulset_scaled_object() {
        let cluster = create_test_cluster(
            1,
            Some(ScalingSpec {
                min_replicas: Some(0),
                max_replicas: 1,
                idle_timeout: "1h".to_string(),
                wakeup_timeout: "5m".to_string(),
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
            }),
            Some(PgBouncerSpec {
                enabled: true,
                replicas: 2,
                pool_mode: "transaction".to_string(),
                max_db_connections: 60,
                default_pool_size: 20,
                max_client_conn: 10000,
                image: None,
                resources: None,
                enable_replica_pooler: false,
            }),
        );

        let obj = generate_statefulset_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        assert_eq!(
            obj.metadata.name,
            Some("test-cluster-sts-scaler".to_string())
        );
    }

    #[test]
    fn test_parse_duration_to_seconds() {
        assert_eq!(parse_duration_to_seconds("30s"), Some(30));
        assert_eq!(parse_duration_to_seconds("30m"), Some(1800));
        assert_eq!(parse_duration_to_seconds("1h"), Some(3600));
        assert_eq!(parse_duration_to_seconds("1d"), Some(86400));
        assert_eq!(parse_duration_to_seconds("300"), Some(300));
        assert_eq!(parse_duration_to_seconds(""), None);
    }
}
