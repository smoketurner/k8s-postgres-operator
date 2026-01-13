//! TLS issuer policy
//!
//! When TLS is enabled, an issuer reference must be provided.
//! This ensures TLS certificates can be provisioned by cert-manager.

use super::{ValidationContext, ValidationResult};

/// Validate TLS issuer requirements
///
/// Rule: If `spec.tls.enabled=true`, `spec.tls.issuerRef` must be set.
pub fn validate_tls(ctx: &ValidationContext) -> ValidationResult {
    let tls = &ctx.cluster.spec.tls;

    // If TLS is disabled, nothing to validate
    if !tls.enabled {
        return ValidationResult::allowed();
    }

    // TLS is enabled, check for issuer reference
    match &tls.issuer_ref {
        Some(issuer) if !issuer.name.is_empty() => ValidationResult::allowed(),
        _ => ValidationResult::denied(
            "TLSIssuerRequired",
            "TLS is enabled but no issuer reference is configured. Set spec.tls.issuerRef to reference a cert-manager Issuer or ClusterIssuer.",
        ),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::crd::{
        IssuerKind, IssuerRef, PostgresCluster, PostgresClusterSpec, PostgresVersion, StorageSpec,
        TLSSpec,
    };
    use kube::core::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_cluster(tls: TLSSpec) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                tls,
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                resources: None,
                backup: None,
                pgbouncer: None,
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_tls_disabled_allowed() {
        let tls = TLSSpec {
            enabled: false,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        let cluster = create_cluster(tls);
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_tls(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_tls_enabled_with_issuer_allowed() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "letsencrypt-prod".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        let cluster = create_cluster(tls);
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_tls(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_tls_enabled_without_issuer_denied() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: None,
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        let cluster = create_cluster(tls);
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_tls(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("TLSIssuerRequired".to_string()));
    }

    #[test]
    fn test_tls_enabled_with_empty_issuer_name_denied() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec![],
            duration: None,
            renew_before: None,
        };
        let cluster = create_cluster(tls);
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_tls(&ctx);
        assert!(!result.allowed);
    }
}
