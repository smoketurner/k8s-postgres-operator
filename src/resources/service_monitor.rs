//! Prometheus Operator ServiceMonitor generation.
//!
//! Creates a `monitoring.coreos.com/v1.ServiceMonitor` selecting the
//! `<cluster>-metrics` Service. Returns `None` if metrics or
//! `serviceMonitor` are not enabled on the spec. The reconciler is
//! tolerant of the CRD not being installed — apply errors with code 404
//! are downgraded to a warning so the operator works against clusters
//! without Prometheus Operator.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt;
use kube::api::{ApiResource, DynamicObject};
use serde_json::json;

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, standard_labels};

/// API group for the Prometheus Operator CRDs.
pub const PROMETHEUS_API_GROUP: &str = "monitoring.coreos.com";
/// API version for the Prometheus Operator CRDs.
pub const PROMETHEUS_API_VERSION: &str = "v1";
/// `ServiceMonitor` kind.
pub const SERVICE_MONITOR_KIND: &str = "ServiceMonitor";

/// Generate a `ServiceMonitor` for the cluster's metrics Service, or `None`
/// if metrics + serviceMonitor are not enabled in the spec.
pub fn generate_service_monitor(cluster: &PostgresCluster) -> Option<DynamicObject> {
    let metrics = cluster.spec.metrics.as_ref()?;
    if !metrics.enabled {
        return None;
    }
    let sm = metrics.service_monitor.as_ref()?;
    if !sm.enabled {
        return None;
    }

    let cluster_name = cluster.name_any();
    let name = format!("{cluster_name}-metrics");
    let ns = cluster.namespace();

    // Standard cluster labels plus any user-supplied selector labels for the
    // Prometheus operator's serviceMonitorSelector.
    let mut labels = standard_labels(&cluster_name);
    for (k, v) in &sm.labels {
        labels.insert(k.clone(), v.clone());
    }

    let mut endpoint = serde_json::Map::new();
    endpoint.insert("port".to_string(), json!("metrics"));
    endpoint.insert(
        "path".to_string(),
        json!(sm.path.as_deref().unwrap_or("/metrics")),
    );
    if let Some(interval) = sm.interval.as_deref() {
        endpoint.insert("interval".to_string(), json!(interval));
    }
    if let Some(timeout) = sm.scrape_timeout.as_deref() {
        endpoint.insert("scrapeTimeout".to_string(), json!(timeout));
    }
    let endpoint = serde_json::Value::Object(endpoint);

    let spec = json!({
        "endpoints": [endpoint],
        "namespaceSelector": {
            "matchNames": [ns.clone().unwrap_or_default()],
        },
        "selector": {
            "matchLabels": {
                // Selects the dedicated metrics Service created by
                // service::generate_metrics_service.
                "postgres-operator.smoketurner.com/cluster": cluster_name.clone(),
                "postgres-operator.smoketurner.com/service": "metrics",
            },
        },
    });

    let api_resource = ApiResource {
        group: PROMETHEUS_API_GROUP.to_string(),
        version: PROMETHEUS_API_VERSION.to_string(),
        kind: SERVICE_MONITOR_KIND.to_string(),
        api_version: format!("{PROMETHEUS_API_GROUP}/{PROMETHEUS_API_VERSION}"),
        plural: "servicemonitors".to_string(),
    };

    let mut obj = DynamicObject::new(&name, &api_resource);
    obj.metadata = ObjectMeta {
        name: Some(name),
        namespace: ns,
        labels: Some(labels),
        owner_references: Some(vec![owner_reference(cluster)]),
        ..Default::default()
    };
    obj.data = json!({ "spec": spec });

    Some(obj)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;
    use crate::crd::{
        MetricsSpec, PostgresClusterSpec, PostgresVersion, ServiceMonitorSpec, StorageSpec, TLSSpec,
    };

    fn cluster_with_metrics(metrics: Option<MetricsSpec>) -> PostgresCluster {
        PostgresCluster {
            metadata: kube::core::ObjectMeta {
                name: Some("pg".to_string()),
                namespace: Some("apps".to_string()),
                uid: Some("uid".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 1,
                storage: StorageSpec {
                    storage_class: None,
                    size: "1Gi".to_string(),
                },
                resources: None,
                postgresql_params: Default::default(),
                labels: Default::default(),
                backup: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
                sidecars: vec![],
                node_selector: Default::default(),
                tolerations: vec![],
                topology_spread_constraints: vec![],
                priority_class_name: None,
            },
            status: None,
        }
    }

    #[test]
    fn returns_none_when_metrics_absent() {
        let cluster = cluster_with_metrics(None);
        assert!(generate_service_monitor(&cluster).is_none());
    }

    #[test]
    fn returns_none_when_metrics_disabled() {
        let cluster = cluster_with_metrics(Some(MetricsSpec {
            enabled: false,
            port: 9187,
            service_monitor: Some(ServiceMonitorSpec {
                enabled: true,
                ..Default::default()
            }),
        }));
        assert!(generate_service_monitor(&cluster).is_none());
    }

    #[test]
    fn returns_none_when_service_monitor_disabled() {
        let cluster = cluster_with_metrics(Some(MetricsSpec {
            enabled: true,
            port: 9187,
            service_monitor: Some(ServiceMonitorSpec {
                enabled: false,
                ..Default::default()
            }),
        }));
        assert!(generate_service_monitor(&cluster).is_none());
    }

    #[test]
    fn returns_none_when_service_monitor_unset() {
        let cluster = cluster_with_metrics(Some(MetricsSpec {
            enabled: true,
            port: 9187,
            service_monitor: None,
        }));
        assert!(generate_service_monitor(&cluster).is_none());
    }

    #[test]
    fn populates_expected_fields_when_enabled() {
        let mut sm_labels = std::collections::BTreeMap::new();
        sm_labels.insert("release".to_string(), "prom-stack".to_string());
        let cluster = cluster_with_metrics(Some(MetricsSpec {
            enabled: true,
            port: 9187,
            service_monitor: Some(ServiceMonitorSpec {
                enabled: true,
                interval: Some("15s".to_string()),
                scrape_timeout: Some("10s".to_string()),
                labels: sm_labels,
                path: Some("/probe".to_string()),
            }),
        }));

        let obj = generate_service_monitor(&cluster).expect("service monitor");
        assert_eq!(obj.name_any(), "pg-metrics");
        assert_eq!(obj.namespace().as_deref(), Some("apps"));

        let labels = obj.metadata.labels.as_ref().expect("labels");
        assert_eq!(
            labels.get("release").map(String::as_str),
            Some("prom-stack")
        );

        let spec = obj.data.get("spec").expect("spec");
        let endpoint = &spec["endpoints"][0];
        assert_eq!(endpoint["port"], "metrics");
        assert_eq!(endpoint["path"], "/probe");
        assert_eq!(endpoint["interval"], "15s");
        assert_eq!(endpoint["scrapeTimeout"], "10s");

        let ns_selector = &spec["namespaceSelector"]["matchNames"][0];
        assert_eq!(ns_selector, "apps");

        let match_labels = &spec["selector"]["matchLabels"];
        assert_eq!(
            match_labels["postgres-operator.smoketurner.com/cluster"],
            "pg"
        );
        assert_eq!(
            match_labels["postgres-operator.smoketurner.com/service"],
            "metrics"
        );
    }

    #[test]
    fn omits_interval_when_unset() {
        let cluster = cluster_with_metrics(Some(MetricsSpec {
            enabled: true,
            port: 9187,
            service_monitor: Some(ServiceMonitorSpec {
                enabled: true,
                ..Default::default()
            }),
        }));

        let obj = generate_service_monitor(&cluster).unwrap();
        let endpoint = &obj.data["spec"]["endpoints"][0];
        assert!(endpoint.get("interval").is_none());
        assert!(endpoint.get("scrapeTimeout").is_none());
        assert_eq!(endpoint["path"], "/metrics");
    }
}
