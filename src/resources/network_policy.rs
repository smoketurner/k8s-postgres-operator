//! NetworkPolicy resource generation for PostgreSQL clusters
//!
//! Creates NetworkPolicies to secure network access to PostgreSQL pods.
//! This is the primary security boundary - pg_hba provides defense in depth.
//!
//! Default behavior (secure by design):
//! - Same namespace can access PostgreSQL port (5432)
//! - Cluster pods can communicate for replication
//! - Operator namespace always has access (cannot be removed - prevents footguns)
//! - Patroni API (8008) only accessible within cluster

use k8s_openapi::api::networking::v1::{
    IPBlock, NetworkPolicy, NetworkPolicyEgressRule, NetworkPolicyIngressRule, NetworkPolicyPeer,
    NetworkPolicyPort, NetworkPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use std::collections::BTreeMap;

use crate::crd::{LabelSelectorConfig, PostgresCluster};
use crate::resources::common::{owner_reference, standard_labels};

/// Operator namespace - always allowed access to prevent footguns
const OPERATOR_NAMESPACE: &str = "postgres-operator-system";

/// Convert our LabelSelectorConfig to k8s_openapi's LabelSelector
fn convert_label_selector(config: &LabelSelectorConfig) -> LabelSelector {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    LabelSelector {
        match_labels: config.match_labels.clone(),
        match_expressions: if config.match_expressions.is_empty() {
            None
        } else {
            Some(
                config
                    .match_expressions
                    .iter()
                    .map(|req| LabelSelectorRequirement {
                        key: req.key.clone(),
                        operator: req.operator.clone(),
                        values: if req.values.is_empty() {
                            None
                        } else {
                            Some(req.values.clone())
                        },
                    })
                    .collect(),
            )
        },
    }
}

/// Generate a NetworkPolicy for the PostgreSQL cluster
///
/// This is always generated and cannot be disabled. The NetworkPolicy is the
/// primary security boundary for database access.
///
/// Access rules:
/// - Same namespace: Can access PostgreSQL (5432)
/// - Operator namespace: Always allowed (prevents lockout)
/// - Cluster pods: Can communicate internally (replication, Patroni API)
/// - External access: Only if `allowExternalAccess: true` (dev/test only)
pub fn generate_network_policy(cluster: &PostgresCluster) -> NetworkPolicy {
    let name = format!("{}-network-policy", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace().unwrap_or_else(|| "default".to_string());

    let labels = standard_labels(&cluster_name);

    // Match all pods in this PostgreSQL cluster
    let pod_selector = LabelSelector {
        match_labels: Some(BTreeMap::from([(
            "postgres-operator.smoketurner.com/cluster".to_string(),
            cluster_name.clone(),
        )])),
        ..Default::default()
    };

    // Build ingress rules
    let mut ingress_rules = Vec::new();

    // Rule 1: Allow PostgreSQL port from same namespace
    ingress_rules.push(NetworkPolicyIngressRule {
        from: Some(vec![NetworkPolicyPeer {
            namespace_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "kubernetes.io/metadata.name".to_string(),
                    ns.clone(),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }]),
        ports: Some(vec![NetworkPolicyPort {
            port: Some(IntOrString::Int(5432)),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
    });

    // Rule 2: Allow Patroni API from cluster pods only
    ingress_rules.push(NetworkPolicyIngressRule {
        from: Some(vec![NetworkPolicyPeer {
            pod_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "postgres-operator.smoketurner.com/cluster".to_string(),
                    cluster_name.clone(),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }]),
        ports: Some(vec![NetworkPolicyPort {
            port: Some(IntOrString::Int(8008)),
            protocol: Some("TCP".to_string()),
            ..Default::default()
        }]),
    });

    // Rule 3: ALWAYS allow operator namespace access (prevents footguns)
    // This cannot be disabled - users cannot lock themselves out
    ingress_rules.push(NetworkPolicyIngressRule {
        from: Some(vec![NetworkPolicyPeer {
            namespace_selector: Some(LabelSelector {
                match_labels: Some(BTreeMap::from([(
                    "kubernetes.io/metadata.name".to_string(),
                    OPERATOR_NAMESPACE.to_string(),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        }]),
        ports: Some(vec![
            NetworkPolicyPort {
                port: Some(IntOrString::Int(5432)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            },
            NetworkPolicyPort {
                port: Some(IntOrString::Int(8008)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            },
        ]),
    });

    // Rule 4: If allowExternalAccess is true, allow RFC1918 networks
    // This is for dev/test environments only
    if cluster
        .spec
        .network_policy
        .as_ref()
        .is_some_and(|np| np.allow_external_access)
    {
        ingress_rules.push(NetworkPolicyIngressRule {
            from: Some(vec![
                NetworkPolicyPeer {
                    ip_block: Some(IPBlock {
                        cidr: "10.0.0.0/8".to_string(),
                        except: None,
                    }),
                    ..Default::default()
                },
                NetworkPolicyPeer {
                    ip_block: Some(IPBlock {
                        cidr: "172.16.0.0/12".to_string(),
                        except: None,
                    }),
                    ..Default::default()
                },
                NetworkPolicyPeer {
                    ip_block: Some(IPBlock {
                        cidr: "192.168.0.0/16".to_string(),
                        except: None,
                    }),
                    ..Default::default()
                },
            ]),
            ports: Some(vec![NetworkPolicyPort {
                port: Some(IntOrString::Int(5432)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
        });
    }

    // Rule 5: Allow cross-namespace access if configured
    if let Some(np_spec) = &cluster.spec.network_policy {
        for peer in &np_spec.allow_from {
            ingress_rules.push(NetworkPolicyIngressRule {
                from: Some(vec![NetworkPolicyPeer {
                    namespace_selector: peer
                        .namespace_selector
                        .as_ref()
                        .map(convert_label_selector),
                    pod_selector: peer.pod_selector.as_ref().map(convert_label_selector),
                    ..Default::default()
                }]),
                ports: Some(vec![NetworkPolicyPort {
                    port: Some(IntOrString::Int(5432)),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                }]),
            });
        }
    }

    // Build egress rules
    let egress_rules = vec![
        // Allow replication to other cluster pods
        NetworkPolicyEgressRule {
            to: Some(vec![NetworkPolicyPeer {
                pod_selector: Some(LabelSelector {
                    match_labels: Some(BTreeMap::from([(
                        "postgres-operator.smoketurner.com/cluster".to_string(),
                        cluster_name.clone(),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
            ports: Some(vec![NetworkPolicyPort {
                port: Some(IntOrString::Int(5432)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
        },
        // Allow DNS (required for service discovery)
        NetworkPolicyEgressRule {
            to: None, // Allow to any destination
            ports: Some(vec![NetworkPolicyPort {
                port: Some(IntOrString::Int(53)),
                protocol: Some("UDP".to_string()),
                ..Default::default()
            }]),
        },
        // Allow Kubernetes API (required for Patroni DCS)
        NetworkPolicyEgressRule {
            to: None, // Allow to any destination
            ports: Some(vec![NetworkPolicyPort {
                port: Some(IntOrString::Int(443)),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
        },
    ];

    NetworkPolicy {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(ns),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: Some(NetworkPolicySpec {
            pod_selector: Some(pod_selector),
            policy_types: Some(vec!["Ingress".to_string(), "Egress".to_string()]),
            ingress: Some(ingress_rules),
            egress: Some(egress_rules),
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::crd::{
        NetworkPolicySpec as CrdNetworkPolicySpec, PostgresClusterSpec, PostgresVersion,
        StorageSpec, TLSSpec,
    };

    fn create_test_cluster(network_policy: Option<CrdNetworkPolicySpec>) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("test-ns".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                labels: Default::default(),
                resources: None,
                postgresql_params: Default::default(),
                backup: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy,
            },
            status: None,
        }
    }

    #[test]
    fn test_generate_network_policy_default() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        assert_eq!(
            np.metadata.name,
            Some("test-cluster-network-policy".to_string())
        );
        assert_eq!(np.metadata.namespace, Some("test-ns".to_string()));

        let spec = np.spec.unwrap();

        // Should have both ingress and egress policies
        assert_eq!(
            spec.policy_types,
            Some(vec!["Ingress".to_string(), "Egress".to_string()])
        );

        // Should have ingress rules for: namespace, patroni, operator
        let ingress = spec.ingress.unwrap();
        assert_eq!(ingress.len(), 3);
    }

    #[test]
    fn test_generate_network_policy_with_external_access() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have 4 rules: namespace, patroni, operator, RFC1918
        assert_eq!(ingress.len(), 4);

        // Last rule should have RFC1918 CIDR blocks
        let rfc1918_rule = &ingress[3];
        let from = rfc1918_rule.from.as_ref().unwrap();
        assert_eq!(from.len(), 3); // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
    }

    #[test]
    fn test_operator_namespace_always_allowed() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find the operator namespace rule
        let operator_rule = ingress.iter().find(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.namespace_selector.as_ref().is_some_and(|sel| {
                        sel.match_labels.as_ref().is_some_and(|labels| {
                            labels.get("kubernetes.io/metadata.name")
                                == Some(&OPERATOR_NAMESPACE.to_string())
                        })
                    })
                })
            })
        });

        assert!(
            operator_rule.is_some(),
            "Operator namespace must always be allowed"
        );
    }
}
