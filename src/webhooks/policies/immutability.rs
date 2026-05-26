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

    // Check version downgrade against the actual running version, not the prior spec
    // value. Comparing against `spec.version` would block legitimate reverts of a
    // mistaken spec bump (e.g. user bumps 15 -> 16 then immediately reverts to 15
    // before any data migrates). When `status.current_version` is absent the cluster
    // has never reached Running and there is no real data to protect, so skip.
    if let Some(running_version) = old_cluster
        .status
        .as_ref()
        .and_then(|s| s.current_version.as_ref())
    {
        let running_major = parse_major_version(running_version);
        let new_major = ctx.cluster.spec.version.as_major_version();

        if new_major < running_major {
            return ValidationResult::denied(
                "VersionDowngradeNotAllowed",
                &format!(
                    "PostgreSQL version downgrades are not allowed. Current running version: {}, requested: {}",
                    running_major, new_major
                ),
            );
        }
    }

    ValidationResult::allowed()
}

/// Parse the major version component from a PostgreSQL version string.
///
/// Accepts values like "15", "16.2", or "17.0.1" and returns the leading integer
/// component. Returns 0 for unparseable values, which conservatively allows the
/// admission check to proceed without blocking on malformed status data.
fn parse_major_version(version: &str) -> i32 {
    version
        .split('.')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::crd::{
        PostgresCluster, PostgresClusterSpec, PostgresClusterStatus, PostgresVersion, StorageSpec,
        TLSSpec,
    };
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

    fn with_current_version(mut cluster: PostgresCluster, version: &str) -> PostgresCluster {
        cluster.status = Some(PostgresClusterStatus {
            current_version: Some(version.to_string()),
            ..Default::default()
        });
        cluster
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
        // spec V15 -> V16 with running version 15: legitimate upgrade, allowed.
        let old = with_current_version(
            create_cluster(PostgresVersion::V15, Some("standard".to_string())),
            "15",
        );
        let new = create_cluster(PostgresVersion::V16, Some("standard".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_version_downgrade_from_running_denied() {
        // spec V16 -> V15 with running version 16: real downgrade, denied.
        let old = with_current_version(
            create_cluster(PostgresVersion::V16, Some("standard".to_string())),
            "16",
        );
        let new = create_cluster(PostgresVersion::V15, Some("standard".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(!result.allowed);
        assert_eq!(
            result.reason,
            Some("VersionDowngradeNotAllowed".to_string())
        );
        let message = result.message.unwrap_or_default();
        assert!(
            message.contains("Current running version"),
            "expected message to mention 'Current running version', got: {message}"
        );
    }

    #[test]
    fn test_version_revert_after_mistake_allowed() {
        // User mistakenly bumped spec 15 -> 16, then reverts to 15 before any data
        // migrated. Running version is still 15, so the revert must be allowed.
        let old = with_current_version(
            create_cluster(PostgresVersion::V16, Some("standard".to_string())),
            "15",
        );
        let new = create_cluster(PostgresVersion::V15, Some("standard".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_no_current_version_skips_check() {
        // Cluster has no status.current_version (never reached Running). Downgrade
        // check is skipped because there is no real running data to protect.
        let old = create_cluster(PostgresVersion::V16, Some("standard".to_string()));
        let new = create_cluster(PostgresVersion::V15, Some("standard".to_string()));
        let ctx = ValidationContext::new(&new, Some(&old), BTreeMap::new());
        let result = validate_immutability(&ctx);
        assert!(result.allowed);
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
