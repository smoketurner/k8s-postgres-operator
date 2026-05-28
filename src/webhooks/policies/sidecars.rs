//! Sidecar validation policy.
//!
//! Sidecars injected into the Spilo pod must have unique names that do not
//! collide with the operator-managed containers ("postgres" Spilo container
//! and "init-permissions" init container). Without this check, Kubernetes
//! would either reject the StatefulSet at apply time (poor UX, error buried
//! in operator logs) or silently overwrite operator state.

use super::{ValidationContext, ValidationResult};

/// Container names owned by the operator. Sidecars sharing these names are
/// rejected so user spec mistakes surface at admission instead of via a
/// StatefulSet apply failure during reconciliation.
const RESERVED_CONTAINER_NAMES: &[&str] = &["postgres", "init-permissions"];

pub fn validate_sidecars(ctx: &ValidationContext) -> ValidationResult {
    let sidecars = &ctx.cluster.spec.sidecars;
    if sidecars.is_empty() {
        return ValidationResult::allowed();
    }

    let mut seen = std::collections::BTreeSet::new();
    for sidecar in sidecars {
        if RESERVED_CONTAINER_NAMES.contains(&sidecar.name.as_str()) {
            return ValidationResult::denied(
                "SidecarNameReserved",
                &format!(
                    "sidecar name '{}' collides with an operator-managed container; rename the sidecar",
                    sidecar.name
                ),
            );
        }
        if !seen.insert(sidecar.name.clone()) {
            return ValidationResult::denied(
                "SidecarNameDuplicate",
                &format!(
                    "sidecar name '{}' is duplicated; sidecar names must be unique",
                    sidecar.name
                ),
            );
        }
    }

    ValidationResult::allowed()
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
    use crate::crd::{PostgresCluster, PostgresClusterSpec, PostgresVersion, StorageSpec, TLSSpec};
    use k8s_openapi::api::core::v1::Container;

    fn cluster_with_sidecars(sidecars: Vec<Container>) -> PostgresCluster {
        PostgresCluster {
            metadata: kube::core::ObjectMeta::default(),
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
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
                sidecars,
            },
            status: None,
        }
    }

    fn named(name: &str) -> Container {
        Container {
            name: name.to_string(),
            image: Some("nginx:1.27".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn empty_sidecars_pass() {
        let cluster = cluster_with_sidecars(vec![]);
        let ctx = ValidationContext::new(&cluster, None, Default::default());
        assert!(validate_sidecars(&ctx).allowed);
    }

    #[test]
    fn unique_named_sidecars_pass() {
        let cluster = cluster_with_sidecars(vec![named("exporter"), named("agent")]);
        let ctx = ValidationContext::new(&cluster, None, Default::default());
        assert!(validate_sidecars(&ctx).allowed);
    }

    #[test]
    fn sidecar_named_postgres_is_rejected() {
        let cluster = cluster_with_sidecars(vec![named("postgres")]);
        let ctx = ValidationContext::new(&cluster, None, Default::default());
        let result = validate_sidecars(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("SidecarNameReserved"));
    }

    #[test]
    fn sidecar_named_init_permissions_is_rejected() {
        let cluster = cluster_with_sidecars(vec![named("init-permissions")]);
        let ctx = ValidationContext::new(&cluster, None, Default::default());
        let result = validate_sidecars(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("SidecarNameReserved"));
    }

    #[test]
    fn duplicate_sidecar_names_rejected() {
        let cluster = cluster_with_sidecars(vec![named("exporter"), named("exporter")]);
        let ctx = ValidationContext::new(&cluster, None, Default::default());
        let result = validate_sidecars(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("SidecarNameDuplicate"));
    }
}
