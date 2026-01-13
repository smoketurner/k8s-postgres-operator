//! Immutability policies
//!
//! - Storage class cannot be changed after creation
//! - PostgreSQL version can only increase (no downgrades)

use super::{ValidationContext, ValidationResult};

/// Validate immutable fields and version upgrade rules
///
/// Rules:
/// - Storage class cannot be changed after creation
/// - PostgreSQL version can only increase (no downgrades)
pub fn validate_immutability(ctx: &ValidationContext) -> ValidationResult {
    // Only apply immutability checks on UPDATE, not CREATE
    let old_cluster = match ctx.old_cluster {
        Some(c) => c,
        None => return ValidationResult::allowed(),
    };

    // Check storage class immutability
    let old_storage_class = &old_cluster.spec.storage.storage_class;
    let new_storage_class = &ctx.cluster.spec.storage.storage_class;

    if old_storage_class != new_storage_class {
        return ValidationResult::denied(
            "StorageClassImmutable",
            "Storage class cannot be changed after creation. Delete and recreate the cluster to use a different storage class.",
        );
    }

    // Check version downgrade
    let old_version = old_cluster.spec.version.as_major_version();
    let new_version = ctx.cluster.spec.version.as_major_version();

    if new_version < old_version {
        return ValidationResult::denied(
            "VersionDowngradeNotAllowed",
            &format!(
                "PostgreSQL version downgrades are not allowed. Current version: {}, requested: {}",
                old_version, new_version
            ),
        );
    }

    ValidationResult::allowed()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::crd::{PostgresCluster, PostgresClusterSpec, PostgresVersion, StorageSpec, TLSSpec};
    use kube::core::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_cluster(version: PostgresVersion, storage_class: Option<String>) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class,
                },
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                resources: None,
                backup: None,
                pgbouncer: None,
                tls: TLSSpec::default(),
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
    fn test_create_allowed() {
        let cluster = create_cluster(PostgresVersion::V16, Some("fast-ssd".to_string()));
        let ctx = ValidationContext::new(&cluster, None, BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_no_change_allowed() {
        let old = create_cluster(PostgresVersion::V16, Some("fast-ssd".to_string()));
        let new = create_cluster(PostgresVersion::V16, Some("fast-ssd".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_version_upgrade_allowed() {
        let old = create_cluster(PostgresVersion::V15, Some("standard".to_string()));
        let new = create_cluster(PostgresVersion::V16, Some("standard".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_version_downgrade_denied() {
        let old = create_cluster(PostgresVersion::V16, Some("standard".to_string()));
        let new = create_cluster(PostgresVersion::V15, Some("standard".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(!result.allowed);
        assert_eq!(
            result.reason,
            Some("VersionDowngradeNotAllowed".to_string())
        );
    }

    #[test]
    fn test_storage_class_change_denied() {
        let old = create_cluster(PostgresVersion::V16, Some("standard".to_string()));
        let new = create_cluster(PostgresVersion::V16, Some("fast-ssd".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("StorageClassImmutable".to_string()));
    }

    #[test]
    fn test_storage_class_from_none_to_some_denied() {
        let old = create_cluster(PostgresVersion::V16, None);
        let new = create_cluster(PostgresVersion::V16, Some("fast-ssd".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(!result.allowed);
    }

    #[test]
    fn test_storage_class_from_some_to_none_denied() {
        let old = create_cluster(PostgresVersion::V16, Some("fast-ssd".to_string()));
        let new = create_cluster(PostgresVersion::V16, None);
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(!result.allowed);
    }
}
