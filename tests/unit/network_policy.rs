//! Unit tests for NetworkPolicy generation
//!
//! Tests for the network security features including:
//! - Default secure configuration
//! - Operator namespace access (footgun prevention)
//! - External access for dev/test environments
//! - Cross-namespace access via allowFrom
//! - pg_hba configuration with samenet

use postgres_operator::crd::{
    LabelSelectorConfig, LabelSelectorRequirement, NetworkPolicyPeer as CrdNetworkPolicyPeer,
    NetworkPolicySpec as CrdNetworkPolicySpec, PostgresCluster, PostgresClusterSpec,
    PostgresVersion, StorageSpec, TLSSpec,
};
use postgres_operator::resources::network_policy::generate_network_policy;
use postgres_operator::resources::patroni::generate_patroni_config;

/// Helper to create a test cluster with optional network policy
fn create_test_cluster(network_policy: Option<CrdNetworkPolicySpec>) -> PostgresCluster {
    PostgresCluster {
        metadata: kube::core::ObjectMeta {
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

// =============================================================================
// NetworkPolicy Default Behavior Tests
// =============================================================================

mod default_behavior_tests {
    use super::*;

    #[test]
    fn test_network_policy_always_generated() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        assert!(np.metadata.name.is_some());
        assert!(np.spec.is_some());
    }

    #[test]
    fn test_network_policy_name_format() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        assert_eq!(
            np.metadata.name,
            Some("test-cluster-network-policy".to_string())
        );
    }

    #[test]
    fn test_network_policy_namespace() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        assert_eq!(np.metadata.namespace, Some("test-ns".to_string()));
    }

    #[test]
    fn test_network_policy_labels() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let labels = np.metadata.labels.as_ref().unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"test-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"postgres-operator".to_string())
        );
    }

    #[test]
    fn test_network_policy_owner_reference() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let owner_refs = np.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
        assert_eq!(owner_refs[0].name, "test-cluster");
    }

    #[test]
    fn test_network_policy_pod_selector() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let pod_selector = spec.pod_selector.unwrap();
        let match_labels = pod_selector.match_labels.unwrap();

        assert_eq!(
            match_labels.get("postgres-operator.smoketurner.com/cluster"),
            Some(&"test-cluster".to_string())
        );
    }

    #[test]
    fn test_network_policy_policy_types() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let policy_types = spec.policy_types.unwrap();

        assert!(policy_types.contains(&"Ingress".to_string()));
        assert!(policy_types.contains(&"Egress".to_string()));
    }
}

// =============================================================================
// Ingress Rules Tests
// =============================================================================

mod ingress_rules_tests {
    use super::*;

    #[test]
    fn test_same_namespace_access_allowed() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find rule that allows same namespace access
        let namespace_rule = ingress.iter().find(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.namespace_selector.as_ref().is_some_and(|sel| {
                        sel.match_labels.as_ref().is_some_and(|labels| {
                            labels.get("kubernetes.io/metadata.name")
                                == Some(&"test-ns".to_string())
                        })
                    })
                })
            })
        });

        assert!(
            namespace_rule.is_some(),
            "Same namespace should be allowed access"
        );

        // Verify it only allows PostgreSQL port
        let ports = namespace_rule.unwrap().ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(
            ports[0].port,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(5432))
        );
    }

    #[test]
    fn test_patroni_api_internal_only() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find rule for Patroni API (8008)
        let patroni_rule = ingress.iter().find(|rule| {
            rule.ports.as_ref().is_some_and(|ports| {
                ports.iter().any(|port| {
                    port.port
                        == Some(
                            k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8008),
                        )
                })
            }) && rule
                .from
                .as_ref()
                .is_some_and(|peers| peers.iter().any(|peer| peer.pod_selector.is_some()))
        });

        assert!(
            patroni_rule.is_some(),
            "Patroni API should be accessible within cluster"
        );

        // Verify it's only accessible to cluster pods
        let peer = &patroni_rule.unwrap().from.as_ref().unwrap()[0];
        let pod_selector = peer.pod_selector.as_ref().unwrap();
        let match_labels = pod_selector.match_labels.as_ref().unwrap();

        assert_eq!(
            match_labels.get("postgres-operator.smoketurner.com/cluster"),
            Some(&"test-cluster".to_string())
        );
    }

    #[test]
    fn test_operator_namespace_always_allowed() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find rule for operator namespace
        let operator_rule = ingress.iter().find(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.namespace_selector.as_ref().is_some_and(|sel| {
                        sel.match_labels.as_ref().is_some_and(|labels| {
                            labels.get("kubernetes.io/metadata.name")
                                == Some(&"postgres-operator-system".to_string())
                        })
                    })
                })
            })
        });

        assert!(
            operator_rule.is_some(),
            "Operator namespace must always be allowed (footgun prevention)"
        );

        // Verify operator can access both PostgreSQL and Patroni API
        let ports = operator_rule.unwrap().ports.as_ref().unwrap();
        let port_numbers: Vec<i32> = ports
            .iter()
            .filter_map(|p| {
                if let Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(n)) =
                    &p.port
                {
                    Some(*n)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            port_numbers.contains(&5432),
            "Operator should access PostgreSQL"
        );
        assert!(
            port_numbers.contains(&8008),
            "Operator should access Patroni API"
        );
    }

    #[test]
    fn test_default_no_external_access() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have exactly 3 rules: namespace, patroni, operator
        assert_eq!(
            ingress.len(),
            3,
            "Default should have only 3 ingress rules (no external access)"
        );

        // Verify no IP blocks are present
        let has_ip_block = ingress.iter().any(|rule| {
            rule.from
                .as_ref()
                .is_some_and(|peers| peers.iter().any(|peer| peer.ip_block.is_some()))
        });

        assert!(
            !has_ip_block,
            "Default should not have IP block rules (no external access)"
        );
    }
}

// =============================================================================
// External Access Tests
// =============================================================================

mod external_access_tests {
    use super::*;

    #[test]
    fn test_external_access_adds_rfc1918_cidrs() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have 4 rules: namespace, patroni, operator, RFC1918
        assert_eq!(ingress.len(), 4, "External access should add RFC1918 rule");

        // Find the RFC1918 rule
        let rfc1918_rule = ingress.iter().find(|rule| {
            rule.from
                .as_ref()
                .is_some_and(|peers| peers.iter().any(|peer| peer.ip_block.is_some()))
        });

        assert!(rfc1918_rule.is_some(), "Should have RFC1918 IP block rule");

        let peers = rfc1918_rule.unwrap().from.as_ref().unwrap();
        let cidrs: Vec<&str> = peers
            .iter()
            .filter_map(|p| p.ip_block.as_ref().map(|b| b.cidr.as_str()))
            .collect();

        assert!(cidrs.contains(&"10.0.0.0/8"));
        assert!(cidrs.contains(&"172.16.0.0/12"));
        assert!(cidrs.contains(&"192.168.0.0/16"));
    }

    #[test]
    fn test_external_access_only_allows_postgresql_port() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find the RFC1918 rule
        let rfc1918_rule = ingress
            .iter()
            .find(|rule| {
                rule.from
                    .as_ref()
                    .is_some_and(|peers| peers.iter().any(|peer| peer.ip_block.is_some()))
            })
            .unwrap();

        let ports = rfc1918_rule.ports.as_ref().unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(
            ports[0].port,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(5432))
        );
    }

    #[test]
    fn test_external_access_false_no_ip_blocks() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        let has_ip_block = ingress.iter().any(|rule| {
            rule.from
                .as_ref()
                .is_some_and(|peers| peers.iter().any(|peer| peer.ip_block.is_some()))
        });

        assert!(
            !has_ip_block,
            "allowExternalAccess=false should not add IP blocks"
        );
    }
}

// =============================================================================
// Cross-Namespace Access Tests
// =============================================================================

mod cross_namespace_access_tests {
    use super::*;

    #[test]
    fn test_allow_from_namespace_selector() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![CrdNetworkPolicyPeer {
                namespace_selector: Some(LabelSelectorConfig {
                    match_labels: Some(
                        [("postgres-access".to_string(), "allowed".to_string())]
                            .into_iter()
                            .collect(),
                    ),
                    match_expressions: vec![],
                }),
                pod_selector: None,
            }],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have 4 rules: namespace, patroni, operator, allowFrom
        assert_eq!(ingress.len(), 4);

        // Find the allowFrom rule
        let allow_from_rule = ingress.iter().find(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.namespace_selector.as_ref().is_some_and(|sel| {
                        sel.match_labels
                            .as_ref()
                            .is_some_and(|labels| labels.get("postgres-access").is_some())
                    })
                })
            })
        });

        assert!(allow_from_rule.is_some(), "Should have allowFrom rule");
    }

    #[test]
    fn test_allow_from_pod_selector() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![CrdNetworkPolicyPeer {
                namespace_selector: None,
                pod_selector: Some(LabelSelectorConfig {
                    match_labels: Some(
                        [("app".to_string(), "backend".to_string())]
                            .into_iter()
                            .collect(),
                    ),
                    match_expressions: vec![],
                }),
            }],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find rule with pod selector
        let pod_rule = ingress.iter().find(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.pod_selector.as_ref().is_some_and(|sel| {
                        sel.match_labels
                            .as_ref()
                            .is_some_and(|labels| labels.get("app") == Some(&"backend".to_string()))
                    })
                })
            })
        });

        assert!(pod_rule.is_some(), "Should have pod selector rule");
    }

    #[test]
    fn test_allow_from_with_match_expressions() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![CrdNetworkPolicyPeer {
                namespace_selector: Some(LabelSelectorConfig {
                    match_labels: None,
                    match_expressions: vec![LabelSelectorRequirement {
                        key: "env".to_string(),
                        operator: "In".to_string(),
                        values: vec!["staging".to_string(), "production".to_string()],
                    }],
                }),
                pod_selector: None,
            }],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Find rule with match expressions
        let expr_rule = ingress.iter().find(|rule| {
            rule.from.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.namespace_selector
                        .as_ref()
                        .is_some_and(|sel| sel.match_expressions.is_some())
                })
            })
        });

        assert!(expr_rule.is_some(), "Should have match expressions rule");
    }

    #[test]
    fn test_multiple_allow_from_peers() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![
                CrdNetworkPolicyPeer {
                    namespace_selector: Some(LabelSelectorConfig {
                        match_labels: Some(
                            [("team".to_string(), "frontend".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        match_expressions: vec![],
                    }),
                    pod_selector: None,
                },
                CrdNetworkPolicyPeer {
                    namespace_selector: Some(LabelSelectorConfig {
                        match_labels: Some(
                            [("team".to_string(), "backend".to_string())]
                                .into_iter()
                                .collect(),
                        ),
                        match_expressions: vec![],
                    }),
                    pod_selector: None,
                },
            ],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have 5 rules: namespace, patroni, operator, 2x allowFrom
        assert_eq!(ingress.len(), 5);
    }
}

// =============================================================================
// Egress Rules Tests
// =============================================================================

mod egress_rules_tests {
    use super::*;

    #[test]
    fn test_replication_egress_allowed() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let egress = spec.egress.unwrap();

        // Find replication rule (5432 to cluster pods)
        let replication_rule = egress.iter().find(|rule| {
            rule.to.as_ref().is_some_and(|peers| {
                peers.iter().any(|peer| {
                    peer.pod_selector.as_ref().is_some_and(|sel| {
                        sel.match_labels.as_ref().is_some_and(|labels| {
                            labels.get("postgres-operator.smoketurner.com/cluster")
                                == Some(&"test-cluster".to_string())
                        })
                    })
                })
            })
        });

        assert!(
            replication_rule.is_some(),
            "Replication egress should be allowed"
        );
    }

    #[test]
    fn test_dns_egress_allowed() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let egress = spec.egress.unwrap();

        // Find DNS rule (port 53 UDP)
        let dns_rule = egress.iter().find(|rule| {
            rule.ports.as_ref().is_some_and(|ports| {
                ports.iter().any(|port| {
                    port.port
                        == Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(53))
                        && port.protocol == Some("UDP".to_string())
                })
            })
        });

        assert!(dns_rule.is_some(), "DNS egress should be allowed");
    }

    #[test]
    fn test_kubernetes_api_egress_allowed() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let egress = spec.egress.unwrap();

        // Find K8s API rule (port 443 TCP)
        let k8s_api_rule = egress.iter().find(|rule| {
            rule.to.is_none()
                && rule.ports.as_ref().is_some_and(|ports| {
                    ports.iter().any(|port| {
                        port.port
                            == Some(
                                k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(443),
                            )
                            && port.protocol == Some("TCP".to_string())
                    })
                })
        });

        assert!(
            k8s_api_rule.is_some(),
            "Kubernetes API egress should be allowed (required for Patroni DCS)"
        );
    }

    #[test]
    fn test_egress_rule_count() {
        let cluster = create_test_cluster(None);
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let egress = spec.egress.unwrap();

        // Should have 3 egress rules: replication, DNS, K8s API
        assert_eq!(egress.len(), 3, "Should have exactly 3 egress rules");
    }
}

// =============================================================================
// pg_hba Configuration Tests
// =============================================================================

mod pg_hba_tests {
    use super::*;

    #[test]
    fn test_patroni_config_contains_pg_hba() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        assert!(yaml.contains("pg_hba:"), "Should contain pg_hba section");
    }

    #[test]
    fn test_pg_hba_local_trust() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        assert!(
            yaml.contains("local all all trust"),
            "Should allow local socket access with trust"
        );
    }

    #[test]
    fn test_pg_hba_loopback_scram() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        assert!(
            yaml.contains("host all all 127.0.0.1/32 scram-sha-256"),
            "Should allow IPv4 loopback with scram-sha-256"
        );
        assert!(
            yaml.contains("host all all ::1/128 scram-sha-256"),
            "Should allow IPv6 loopback with scram-sha-256"
        );
    }

    #[test]
    fn test_pg_hba_replication_samenet() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        assert!(
            yaml.contains("host replication standby samenet scram-sha-256"),
            "Should allow replication with samenet keyword"
        );
    }

    #[test]
    fn test_pg_hba_application_samenet() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        assert!(
            yaml.contains("host all all samenet scram-sha-256"),
            "Should allow application access with samenet keyword"
        );
    }

    #[test]
    fn test_pg_hba_no_rfc1918_cidrs() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        // Should NOT contain the old permissive RFC1918 CIDRs
        assert!(
            !yaml.contains("10.0.0.0/8"),
            "Should not contain RFC1918 10.0.0.0/8 (too permissive)"
        );
        assert!(
            !yaml.contains("172.16.0.0/12"),
            "Should not contain RFC1918 172.16.0.0/12 (too permissive)"
        );
        assert!(
            !yaml.contains("192.168.0.0/16"),
            "Should not contain RFC1918 192.168.0.0/16 (too permissive)"
        );
    }

    #[test]
    fn test_pg_hba_order() {
        let cluster = create_test_cluster(None);
        let cm = generate_patroni_config(&cluster);

        let data = cm.data.as_ref().unwrap();
        let yaml = data.get("patroni.yml").unwrap();

        // Find positions of key rules
        let local_pos = yaml.find("local all all trust");
        let loopback_pos = yaml.find("host all all 127.0.0.1/32");
        let replication_pos = yaml.find("host replication standby samenet");
        let application_pos = yaml.find("host all all samenet");

        // Verify all positions exist
        assert!(local_pos.is_some(), "local rule should exist");
        assert!(loopback_pos.is_some(), "loopback rule should exist");
        assert!(replication_pos.is_some(), "replication rule should exist");
        assert!(application_pos.is_some(), "application rule should exist");

        // Verify order: local < loopback < replication < application
        assert!(
            local_pos.unwrap() < loopback_pos.unwrap(),
            "local should come before loopback"
        );
        assert!(
            loopback_pos.unwrap() < replication_pos.unwrap(),
            "loopback should come before replication"
        );
        // Note: replication and application may be in either order as both use samenet
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

mod edge_case_tests {
    use super::*;

    #[test]
    fn test_empty_allow_from() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have only 3 rules (no allowFrom rules added)
        assert_eq!(ingress.len(), 3);
    }

    #[test]
    fn test_external_access_with_allow_from() {
        let cluster = create_test_cluster(Some(CrdNetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![CrdNetworkPolicyPeer {
                namespace_selector: Some(LabelSelectorConfig {
                    match_labels: Some(
                        [("team".to_string(), "backend".to_string())]
                            .into_iter()
                            .collect(),
                    ),
                    match_expressions: vec![],
                }),
                pod_selector: None,
            }],
        }));
        let np = generate_network_policy(&cluster);

        let spec = np.spec.unwrap();
        let ingress = spec.ingress.unwrap();

        // Should have 5 rules: namespace, patroni, operator, RFC1918, allowFrom
        assert_eq!(ingress.len(), 5);
    }

    #[test]
    fn test_different_namespaces() {
        for ns in [
            "default",
            "production",
            "dev-team-1",
            "my-namespace-with-dashes",
        ] {
            let mut cluster = create_test_cluster(None);
            cluster.metadata.namespace = Some(ns.to_string());

            let np = generate_network_policy(&cluster);
            assert_eq!(np.metadata.namespace, Some(ns.to_string()));

            // Verify same-namespace rule uses correct namespace
            let spec = np.spec.unwrap();
            let ingress = spec.ingress.unwrap();
            let namespace_rule = ingress.iter().find(|rule| {
                rule.from.as_ref().is_some_and(|peers| {
                    peers.iter().any(|peer| {
                        peer.namespace_selector.as_ref().is_some_and(|sel| {
                            sel.match_labels.as_ref().is_some_and(|labels| {
                                labels.get("kubernetes.io/metadata.name") == Some(&ns.to_string())
                            })
                        })
                    })
                })
            });
            assert!(
                namespace_rule.is_some(),
                "Should find namespace rule for {}",
                ns
            );
        }
    }

    #[test]
    fn test_different_cluster_names() {
        for name in ["db", "my-cluster", "prod-postgres-main", "test123"] {
            let mut cluster = create_test_cluster(None);
            cluster.metadata.name = Some(name.to_string());

            let np = generate_network_policy(&cluster);
            assert_eq!(np.metadata.name, Some(format!("{}-network-policy", name)));
        }
    }
}
