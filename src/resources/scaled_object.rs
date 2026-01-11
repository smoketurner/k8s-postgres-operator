//! KEDA ScaledObject resource generation for PostgreSQL reader auto-scaling
//!
//! Creates KEDA ScaledObjects to automatically scale PostgreSQL read replicas
//! based on CPU utilization and/or PostgreSQL connection count.
//!
//! This is used for production clusters (replicas >= 2) where we want to
//! auto-scale the number of read replicas based on load.
//!
//! Requirements:
//! - KEDA must be installed in the cluster
//! - The cluster must have replicas >= 2 (production mode)
//! - scaling.readers.enabled must be true

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, ApiResource, DynamicObject, Patch, PatchParams};
use kube::{Client, ResourceExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;
use tracing::{debug, info};

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, standard_labels};

/// KEDA annotation to pause scale-out (prevents adding replicas)
/// When replicas are lagging, we prevent scale-up but allow scale-down
const KEDA_PAUSED_SCALE_OUT: &str = "autoscaling.keda.sh/paused-scale-out";

/// KEDA API group
pub const KEDA_API_GROUP: &str = "keda.sh";
/// KEDA API version
pub const KEDA_API_VERSION: &str = "v1alpha1";
/// KEDA ScaledObject kind
pub const SCALED_OBJECT_KIND: &str = "ScaledObject";

/// KEDA ScaledObject spec
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScaledObjectSpec {
    /// Reference to the target resource to scale
    pub scale_target_ref: ScaleTargetRef,
    /// Minimum replica count (base replicas from spec)
    pub min_replica_count: i32,
    /// Maximum replica count
    pub max_replica_count: i32,
    /// Cooldown period after scale down (seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_period: Option<i32>,
    /// Polling interval for checking triggers (seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub polling_interval: Option<i32>,
    /// Advanced scaling configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advanced: Option<AdvancedConfig>,
    /// Scaling triggers
    pub triggers: Vec<ScaleTrigger>,
}

/// Reference to the target resource to scale
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScaleTargetRef {
    /// API version of the target (apps/v1 for StatefulSet)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    /// Kind of the target (StatefulSet)
    pub kind: String,
    /// Name of the target resource
    pub name: String,
}

/// Advanced KEDA scaling configuration
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AdvancedConfig {
    /// Horizontal pod autoscaler configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub horizontal_pod_autoscaler_config: Option<HpaConfig>,
    /// Restore to original replica count on delete
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_to_original_replica_count: Option<bool>,
}

/// HPA configuration for KEDA
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HpaConfig {
    /// Scaling behavior configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub behavior: Option<HpaBehavior>,
}

/// HPA scaling behavior
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct HpaBehavior {
    /// Scale down behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale_down: Option<ScalingRules>,
    /// Scale up behavior
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale_up: Option<ScalingRules>,
}

/// Scaling rules for HPA behavior
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScalingRules {
    /// Stabilization window seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stabilization_window_seconds: Option<i32>,
    /// Policies for scaling
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<Vec<ScalingPolicy>>,
    /// Select policy: Max, Min, or Disabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub select_policy: Option<String>,
}

/// Individual scaling policy
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScalingPolicy {
    /// Type of policy: Pods or Percent
    #[serde(rename = "type")]
    pub type_: String,
    /// Value for the policy
    pub value: i32,
    /// Period in seconds
    pub period_seconds: i32,
}

/// KEDA scaling trigger
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ScaleTrigger {
    /// Trigger type (cpu, postgres, prometheus, etc.)
    #[serde(rename = "type")]
    pub type_: String,
    /// Metric type for the trigger (Utilization, AverageValue, Value)
    /// For CPU/Memory scalers: Utilization or AverageValue
    /// For other scalers: AverageValue or Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_type: Option<String>,
    /// Trigger-specific metadata
    pub metadata: BTreeMap<String, String>,
    /// Optional authentication reference
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication_ref: Option<AuthenticationRef>,
}

/// Reference to KEDA TriggerAuthentication
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AuthenticationRef {
    /// Name of the TriggerAuthentication resource
    pub name: String,
}

/// Check if reader scaling should be enabled for a cluster
pub fn should_enable_reader_scaling(cluster: &PostgresCluster) -> bool {
    // Check if scaling is configured
    let Some(scaling) = &cluster.spec.scaling else {
        return false;
    };

    // Must have potential to scale up
    scaling.max_replicas > cluster.spec.replicas
}

/// Check if KEDA is managing replicas for this cluster
///
/// Returns true if reader auto-scaling is enabled (maxReplicas > replicas).
///
/// When KEDA manages replicas, the operator should not set the StatefulSet
/// replica count to avoid conflicts with KEDA's scaling decisions.
pub fn is_keda_managing_replicas(cluster: &PostgresCluster) -> bool {
    let Some(scaling) = &cluster.spec.scaling else {
        return false;
    };

    // Reader auto-scaling means KEDA manages replicas
    scaling.max_replicas > cluster.spec.replicas
}

/// Generate a KEDA ScaledObject for reader auto-scaling
///
/// Creates a ScaledObject that targets the PostgreSQL StatefulSet and
/// scales based on configured metrics (CPU and/or connections).
///
/// Returns None if reader scaling is not enabled.
pub fn generate_scaled_object(cluster: &PostgresCluster) -> Option<DynamicObject> {
    if !should_enable_reader_scaling(cluster) {
        return None;
    }

    let scaling = cluster.spec.scaling.as_ref()?;

    let name = format!("{}-readers", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    // Build triggers based on configured metrics
    let mut triggers = Vec::new();

    if let Some(metrics) = &scaling.metrics {
        // CPU-based scaling trigger
        if let Some(cpu) = &metrics.cpu {
            triggers.push(ScaleTrigger {
                type_: "cpu".to_string(),
                metric_type: Some("Utilization".to_string()),
                metadata: BTreeMap::from([(
                    "value".to_string(),
                    cpu.target_utilization.to_string(),
                )]),
                authentication_ref: None,
            });
        }

        // Connection-based scaling using PostgreSQL scaler
        if let Some(connections) = &metrics.connections {
            // PostgreSQL connection scaler requires auth configuration
            // which will be created separately via TriggerAuthentication
            triggers.push(ScaleTrigger {
                type_: "postgresql".to_string(),
                metric_type: None, // PostgreSQL scaler uses AverageValue by default
                metadata: BTreeMap::from([
                    (
                        "targetQueryValue".to_string(),
                        connections.target_per_replica.to_string(),
                    ),
                    (
                        "query".to_string(),
                        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'".to_string(),
                    ),
                ]),
                authentication_ref: Some(AuthenticationRef {
                    name: format!("{}-pg-auth", cluster_name),
                }),
            });
        }
    }

    // If no metrics configured, default to CPU at 70%
    if triggers.is_empty() {
        triggers.push(ScaleTrigger {
            type_: "cpu".to_string(),
            metric_type: Some("Utilization".to_string()),
            metadata: BTreeMap::from([("value".to_string(), "70".to_string())]),
            authentication_ref: None,
        });
    }

    let min_replicas = scaling.effective_min_replicas(cluster.spec.replicas);

    let spec = ScaledObjectSpec {
        scale_target_ref: ScaleTargetRef {
            api_version: Some("apps/v1".to_string()),
            kind: "StatefulSet".to_string(),
            name: cluster_name.clone(),
        },
        min_replica_count: min_replicas,
        max_replica_count: scaling.max_replicas,
        cooldown_period: Some(300), // 5 minutes cooldown
        polling_interval: Some(30), // Check every 30 seconds
        advanced: Some(AdvancedConfig {
            horizontal_pod_autoscaler_config: Some(HpaConfig {
                behavior: Some(HpaBehavior {
                    scale_down: Some(ScalingRules {
                        stabilization_window_seconds: Some(300),
                        policies: Some(vec![ScalingPolicy {
                            type_: "Pods".to_string(),
                            value: 1,
                            period_seconds: 60,
                        }]),
                        select_policy: Some("Min".to_string()),
                    }),
                    scale_up: Some(ScalingRules {
                        stabilization_window_seconds: Some(60),
                        policies: Some(vec![
                            ScalingPolicy {
                                type_: "Pods".to_string(),
                                value: 2,
                                period_seconds: 60,
                            },
                            ScalingPolicy {
                                type_: "Percent".to_string(),
                                value: 100,
                                period_seconds: 60,
                            },
                        ]),
                        select_policy: Some("Max".to_string()),
                    }),
                }),
            }),
            restore_to_original_replica_count: Some(true),
        }),
        triggers,
    };

    // Create as DynamicObject since ScaledObject is a CRD
    let mut obj = DynamicObject::new(
        &name,
        &kube::api::ApiResource {
            group: KEDA_API_GROUP.to_string(),
            version: KEDA_API_VERSION.to_string(),
            kind: SCALED_OBJECT_KIND.to_string(),
            api_version: format!("{}/{}", KEDA_API_GROUP, KEDA_API_VERSION),
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
        "spec": spec
    });

    Some(obj)
}

/// Generate a KEDA TriggerAuthentication for PostgreSQL connection scaling
///
/// Creates a TriggerAuthentication that references the cluster's credentials
/// secret for authenticating PostgreSQL queries.
///
/// Returns None if connection-based scaling is not configured.
pub fn generate_trigger_auth(cluster: &PostgresCluster) -> Option<DynamicObject> {
    // Check if connection-based scaling is enabled
    let has_connection_scaling = cluster
        .spec
        .scaling
        .as_ref()
        .and_then(|s| s.metrics.as_ref())
        .is_some_and(|m| m.connections.is_some());

    if !has_connection_scaling {
        return None;
    }

    let name = format!("{}-pg-auth", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();
    let secret_name = format!("{}-credentials", cluster_name);

    let labels = standard_labels(&cluster_name);

    let mut obj = DynamicObject::new(
        &name,
        &kube::api::ApiResource {
            group: KEDA_API_GROUP.to_string(),
            version: KEDA_API_VERSION.to_string(),
            kind: "TriggerAuthentication".to_string(),
            api_version: format!("{}/{}", KEDA_API_GROUP, KEDA_API_VERSION),
            plural: "triggerauthentications".to_string(),
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
            "secretTargetRef": [
                {
                    "parameter": "connection",
                    "name": secret_name,
                    "key": "connection-string"
                }
            ]
        }
    });

    Some(obj)
}

/// Update KEDA ScaledObject pause state based on replication lag
///
/// When replicas are lagging beyond the configured threshold, this function
/// adds the `autoscaling.keda.sh/paused-scale-out` annotation to prevent KEDA
/// from scaling up. Scale-down is still allowed, which is appropriate since
/// reducing load when replicas are struggling won't make things worse.
///
/// When replication catches up, the annotation is removed to resume normal
/// scaling behavior (both scale-up and scale-down).
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `cluster` - The PostgresCluster resource
/// * `replicas_lagging` - Whether any replica exceeds the lag threshold
///
/// # Returns
/// * `Ok(Some(true))` - Scale-out was paused
/// * `Ok(Some(false))` - Scale-out was resumed (unpaused)
/// * `Ok(None)` - No change was needed (ScaledObject doesn't exist or already in desired state)
/// * `Err(_)` - Failed to update the ScaledObject
pub async fn update_scaling_pause_state(
    client: Client,
    cluster: &PostgresCluster,
    replicas_lagging: bool,
) -> Result<Option<bool>, kube::Error> {
    // Only relevant if reader scaling is enabled
    if !should_enable_reader_scaling(cluster) {
        return Ok(None);
    }

    let name = format!("{}-readers", cluster.name_any());
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());

    let ar = ApiResource {
        group: KEDA_API_GROUP.to_string(),
        version: KEDA_API_VERSION.to_string(),
        kind: SCALED_OBJECT_KIND.to_string(),
        api_version: format!("{}/{}", KEDA_API_GROUP, KEDA_API_VERSION),
        plural: "scaledobjects".to_string(),
    };

    let api: Api<DynamicObject> = Api::namespaced_with(client, &ns, &ar);

    // Check if ScaledObject exists and get current annotation state
    let scaled_object = match api.get_opt(&name).await? {
        Some(obj) => obj,
        None => {
            debug!(
                cluster = %cluster.name_any(),
                "ScaledObject does not exist, skipping pause state update"
            );
            return Ok(None);
        }
    };

    let scale_out_paused = scaled_object
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(KEDA_PAUSED_SCALE_OUT))
        .is_some_and(|v| v == "true");

    // Determine if we need to make a change
    if replicas_lagging && !scale_out_paused {
        // Need to pause scale-out: add the annotation
        let patch = json!({
            "metadata": {
                "annotations": {
                    KEDA_PAUSED_SCALE_OUT: "true"
                }
            }
        });

        api.patch(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        info!(
            cluster = %cluster.name_any(),
            "Paused KEDA scale-out due to replication lag exceeding threshold"
        );

        Ok(Some(true))
    } else if !replicas_lagging && scale_out_paused {
        // Need to unpause: remove the annotation by setting it to null
        let patch = json!({
            "metadata": {
                "annotations": {
                    KEDA_PAUSED_SCALE_OUT: null
                }
            }
        });

        api.patch(
            &name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await?;

        info!(
            cluster = %cluster.name_any(),
            "Resumed KEDA scale-out, replication lag within threshold"
        );

        Ok(Some(false))
    } else {
        // No change needed
        debug!(
            cluster = %cluster.name_any(),
            replicas_lagging = replicas_lagging,
            scale_out_paused = scale_out_paused,
            "KEDA pause state already matches desired state"
        );
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ConnectionScalingMetric, CpuScalingMetric, PostgresClusterSpec, PostgresVersion,
        ScalingMetrics, ScalingSpec, StorageSpec,
    };

    fn create_test_cluster(replicas: i32, scaling: Option<ScalingSpec>) -> PostgresCluster {
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
                resources: None,
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                backup: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                network_policy: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_should_enable_reader_scaling_no_scaling() {
        let cluster = create_test_cluster(3, None);
        assert!(!should_enable_reader_scaling(&cluster));
    }

    #[test]
    fn test_should_enable_reader_scaling_with_headroom() {
        // Reader scaling enabled when maxReplicas > replicas
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        assert!(should_enable_reader_scaling(&cluster));
    }

    #[test]
    fn test_should_enable_reader_scaling_no_headroom() {
        // No reader scaling when maxReplicas == replicas
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 3,
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        assert!(!should_enable_reader_scaling(&cluster));
    }

    #[test]
    fn test_generate_scaled_object_with_cpu() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: Some(CpuScalingMetric {
                        target_utilization: 80,
                    }),
                    connections: None,
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        assert_eq!(obj.metadata.name, Some("test-cluster-readers".to_string()));

        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");
        assert_eq!(spec.min_replica_count, 3);
        assert_eq!(spec.max_replica_count, 10);
        assert_eq!(spec.triggers.len(), 1);
        assert_eq!(spec.triggers[0].type_, "cpu");
    }

    #[test]
    fn test_generate_scaled_object_with_connections() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: None,
                    connections: Some(ConnectionScalingMetric {
                        target_per_replica: 50,
                    }),
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");
        assert_eq!(spec.triggers.len(), 1);
        assert_eq!(spec.triggers[0].type_, "postgresql");
        assert!(spec.triggers[0].authentication_ref.is_some());
    }

    #[test]
    fn test_generate_trigger_auth() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: None,
                    connections: Some(ConnectionScalingMetric {
                        target_per_replica: 50,
                    }),
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_trigger_auth(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        assert_eq!(obj.metadata.name, Some("test-cluster-pg-auth".to_string()));
    }

    #[test]
    fn test_generate_trigger_auth_not_needed() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: Some(CpuScalingMetric {
                        target_utilization: 70,
                    }),
                    connections: None,
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_trigger_auth(&cluster);
        assert!(obj.is_none());
    }

    #[test]
    fn test_generate_scaled_object_with_combined_metrics() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: Some(CpuScalingMetric {
                        target_utilization: 75,
                    }),
                    connections: Some(ConnectionScalingMetric {
                        target_per_replica: 100,
                    }),
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");

        // Should have both CPU and PostgreSQL triggers
        assert_eq!(spec.triggers.len(), 2);

        let cpu_trigger = spec.triggers.iter().find(|t| t.type_ == "cpu");
        let pg_trigger = spec.triggers.iter().find(|t| t.type_ == "postgresql");

        assert!(cpu_trigger.is_some(), "Should have CPU trigger");
        assert!(pg_trigger.is_some(), "Should have PostgreSQL trigger");

        // Verify CPU trigger metadata
        let cpu = cpu_trigger.unwrap();
        assert_eq!(cpu.metadata.get("value"), Some(&"75".to_string()));

        // Verify PostgreSQL trigger has auth ref
        let pg = pg_trigger.unwrap();
        assert!(pg.authentication_ref.is_some());
        assert_eq!(
            pg.authentication_ref.as_ref().unwrap().name,
            "test-cluster-pg-auth"
        );
    }

    #[test]
    fn test_generate_scaled_object_default_cpu_when_no_metrics() {
        // When scaling is enabled but no metrics are specified, should default to CPU at 70%
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: None, // No metrics specified
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");

        // Should have exactly one trigger (default CPU)
        assert_eq!(spec.triggers.len(), 1);
        assert_eq!(spec.triggers[0].type_, "cpu");
        assert_eq!(
            spec.triggers[0].metadata.get("value"),
            Some(&"70".to_string())
        );
        assert!(spec.triggers[0].authentication_ref.is_none());
    }

    #[test]
    fn test_trigger_auth_references_correct_secret() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: None,
                    connections: Some(ConnectionScalingMetric {
                        target_per_replica: 50,
                    }),
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_trigger_auth(&cluster);
        assert!(obj.is_some());

        let obj = obj.unwrap();
        let spec = obj.data["spec"].clone();

        // Verify the secret reference
        let secret_refs = spec["secretTargetRef"].as_array().unwrap();
        assert_eq!(secret_refs.len(), 1);

        let secret_ref = &secret_refs[0];
        assert_eq!(secret_ref["parameter"], "connection");
        assert_eq!(secret_ref["name"], "test-cluster-credentials");
        assert_eq!(secret_ref["key"], "connection-string");
    }

    #[test]
    fn test_scaled_object_hpa_behavior_configuration() {
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(3),
                max_replicas: 10,
                metrics: Some(ScalingMetrics {
                    cpu: Some(CpuScalingMetric {
                        target_utilization: 70,
                    }),
                    connections: None,
                }),
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster).unwrap();
        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");

        // Verify HPA behavior is configured
        let advanced = spec.advanced.as_ref().unwrap();
        let hpa_config = advanced.horizontal_pod_autoscaler_config.as_ref().unwrap();
        let behavior = hpa_config.behavior.as_ref().unwrap();

        // Scale down behavior should be conservative
        let scale_down = behavior.scale_down.as_ref().unwrap();
        assert_eq!(scale_down.stabilization_window_seconds, Some(300));
        assert_eq!(scale_down.select_policy, Some("Min".to_string()));

        // Scale up behavior should be more aggressive
        let scale_up = behavior.scale_up.as_ref().unwrap();
        assert_eq!(scale_up.stabilization_window_seconds, Some(60));
        assert_eq!(scale_up.select_policy, Some("Max".to_string()));
    }

    #[test]
    fn test_scaled_object_min_replicas_uses_effective() {
        // Test that effective_min_replicas is used (defaults to base replicas if min not specified)
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: None, // Not specified, should default to base replicas
                max_replicas: 10,
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster).unwrap();
        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");

        // min_replica_count should be 3 (the base replica count)
        assert_eq!(spec.min_replica_count, 3);
    }

    #[test]
    fn test_scaled_object_min_replicas_respects_explicit_value() {
        // Test that explicit min_replicas is used when specified
        let cluster = create_test_cluster(
            3,
            Some(ScalingSpec {
                min_replicas: Some(2), // Explicit value lower than base
                max_replicas: 10,
                metrics: None,
                replication_lag_threshold: "30s".to_string(),
                ..Default::default()
            }),
        );

        let obj = generate_scaled_object(&cluster).unwrap();
        let spec: ScaledObjectSpec =
            serde_json::from_value(obj.data["spec"].clone()).expect("should parse spec");

        // min_replica_count should be 2 (the explicit value)
        assert_eq!(spec.min_replica_count, 2);
    }
}
