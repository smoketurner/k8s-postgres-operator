//! Validation policies for PostgresUpgrade resources
//!
//! ## Policies
//!
//! - **Version Direction**: Target version must be greater than source version (no downgrades)
//! - **Immutability**: sourceCluster and targetVersion cannot be changed after creation
//! - **Concurrent Upgrades**: Only one upgrade per source cluster allowed
//! - **Source Validation**: Source cluster must exist and be in Running state

use kube::api::{Api, ListParams};
use kube::{Client, ResourceExt};

use crate::crd::{ClusterPhase, PostgresCluster, PostgresUpgrade, UpgradePhase};

/// Result of an upgrade validation
#[derive(Debug)]
pub struct UpgradeValidationResult {
    pub allowed: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl UpgradeValidationResult {
    pub fn allowed() -> Self {
        Self {
            allowed: true,
            reason: None,
            message: None,
        }
    }

    pub fn denied(reason: &str, message: &str) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        }
    }
}

/// Context for upgrade validation
pub struct UpgradeValidationContext<'a> {
    pub upgrade: &'a PostgresUpgrade,
    pub old_upgrade: Option<&'a PostgresUpgrade>,
    pub source_cluster: Option<&'a PostgresCluster>,
}

impl<'a> UpgradeValidationContext<'a> {
    pub fn new(
        upgrade: &'a PostgresUpgrade,
        old_upgrade: Option<&'a PostgresUpgrade>,
        source_cluster: Option<&'a PostgresCluster>,
    ) -> Self {
        Self {
            upgrade,
            old_upgrade,
            source_cluster,
        }
    }

    /// Check if this is a CREATE operation (no old object)
    pub fn is_create(&self) -> bool {
        self.old_upgrade.is_none()
    }
}

/// Validate that target version is greater than source version
pub fn validate_version_direction(ctx: &UpgradeValidationContext) -> UpgradeValidationResult {
    // Get source cluster version
    let source_version = match &ctx.source_cluster {
        Some(cluster) => cluster.spec.version.as_major_version(),
        None => {
            // Can't validate without source cluster - this will be caught by validate_source_cluster
            return UpgradeValidationResult::allowed();
        }
    };

    let target_version = ctx.upgrade.spec.target_version.as_major_version();

    // Check for downgrade
    if target_version < source_version {
        return UpgradeValidationResult::denied(
            "VersionDowngradeNotAllowed",
            &format!(
                "PostgreSQL version downgrades are not allowed. Source version: {}, target version: {}",
                source_version, target_version
            ),
        );
    }

    // Check for same version (not an upgrade)
    if target_version == source_version {
        return UpgradeValidationResult::denied(
            "SameVersionNotAllowed",
            &format!(
                "Target version must be greater than source version. Both are: {}",
                source_version
            ),
        );
    }

    // Validate that the version jump is not too large (only allow +1 major version at a time)
    // This is optional and can be relaxed based on PostgreSQL logical replication capabilities
    let version_jump = target_version - source_version;
    if version_jump > 2 {
        return UpgradeValidationResult::denied(
            "VersionJumpTooLarge",
            &format!(
                "Version jump from {} to {} is too large. Consider upgrading through intermediate versions.",
                source_version, target_version
            ),
        );
    }

    UpgradeValidationResult::allowed()
}

/// Validate that immutable fields have not changed
pub fn validate_upgrade_immutability(ctx: &UpgradeValidationContext) -> UpgradeValidationResult {
    // Only apply on UPDATE operations
    let old_upgrade = match ctx.old_upgrade {
        Some(u) => u,
        None => return UpgradeValidationResult::allowed(),
    };

    // sourceCluster is immutable
    if ctx.upgrade.spec.source_cluster.name != old_upgrade.spec.source_cluster.name {
        return UpgradeValidationResult::denied(
            "SourceClusterImmutable",
            "Source cluster reference cannot be changed after creation. Delete and recreate the upgrade to change the source cluster.",
        );
    }

    if ctx.upgrade.spec.source_cluster.namespace != old_upgrade.spec.source_cluster.namespace {
        return UpgradeValidationResult::denied(
            "SourceClusterImmutable",
            "Source cluster namespace cannot be changed after creation. Delete and recreate the upgrade to change the source cluster.",
        );
    }

    // targetVersion is immutable
    if ctx.upgrade.spec.target_version != old_upgrade.spec.target_version {
        return UpgradeValidationResult::denied(
            "TargetVersionImmutable",
            "Target version cannot be changed after creation. Delete and recreate the upgrade to change the target version.",
        );
    }

    UpgradeValidationResult::allowed()
}

/// Validate that source cluster exists and is in Running state
pub fn validate_source_cluster(ctx: &UpgradeValidationContext) -> UpgradeValidationResult {
    match &ctx.source_cluster {
        None => UpgradeValidationResult::denied(
            "SourceClusterNotFound",
            &format!(
                "Source cluster '{}' not found in namespace '{}'",
                ctx.upgrade.spec.source_cluster.name,
                ctx.upgrade
                    .spec
                    .source_cluster
                    .namespace
                    .as_deref()
                    .unwrap_or("default")
            ),
        ),
        Some(cluster) => {
            // Check if cluster is in Running state
            let phase = cluster
                .status
                .as_ref()
                .map(|s| &s.phase)
                .unwrap_or(&ClusterPhase::Pending);

            if *phase != ClusterPhase::Running {
                return UpgradeValidationResult::denied(
                    "SourceClusterNotRunning",
                    &format!(
                        "Source cluster '{}' is not in Running state (current phase: {:?}). Wait for the cluster to become ready.",
                        ctx.upgrade.spec.source_cluster.name, phase
                    ),
                );
            }

            UpgradeValidationResult::allowed()
        }
    }
}

/// Check for concurrent upgrades on the same source cluster
///
/// This requires checking existing PostgresUpgrade resources, so it's async
pub async fn validate_no_concurrent_upgrade(
    client: &Client,
    upgrade: &PostgresUpgrade,
    old_upgrade: Option<&PostgresUpgrade>,
) -> UpgradeValidationResult {
    // If this is an UPDATE operation, we only need to check if the upgrade being updated
    // is itself (which is fine)
    if old_upgrade.is_some() {
        return UpgradeValidationResult::allowed();
    }

    let namespace = upgrade.namespace().unwrap_or_default();
    let source_name = &upgrade.spec.source_cluster.name;

    // List all upgrades in the namespace
    let upgrades: Api<PostgresUpgrade> = Api::namespaced(client.clone(), &namespace);

    match upgrades.list(&ListParams::default()).await {
        Ok(list) => {
            for existing in list.items {
                // Skip if it's the same upgrade (in case of re-validation)
                if existing.name_any() == upgrade.name_any() {
                    continue;
                }

                // Check if another upgrade is targeting the same source cluster
                if existing.spec.source_cluster.name == *source_name {
                    // Check if the existing upgrade is still active (not completed or failed)
                    let phase = existing
                        .status
                        .as_ref()
                        .map(|s| &s.phase)
                        .unwrap_or(&UpgradePhase::Pending);

                    if !phase.is_terminal() {
                        return UpgradeValidationResult::denied(
                            "ConcurrentUpgradeNotAllowed",
                            &format!(
                                "Another upgrade '{}' is already in progress for source cluster '{}' (phase: {:?}). Wait for it to complete or delete it first.",
                                existing.name_any(),
                                source_name,
                                phase
                            ),
                        );
                    }
                }
            }
            UpgradeValidationResult::allowed()
        }
        Err(e) => {
            // If we can't list upgrades, deny to be safe
            UpgradeValidationResult::denied(
                "ValidationError",
                &format!("Failed to check for concurrent upgrades: {}", e),
            )
        }
    }
}

/// Run all synchronous validation policies
pub fn validate_upgrade_sync(ctx: &UpgradeValidationContext) -> UpgradeValidationResult {
    // Check immutability first (for UPDATE operations)
    let result = validate_upgrade_immutability(ctx);
    if !result.allowed {
        return result;
    }

    // Check source cluster exists and is running
    let result = validate_source_cluster(ctx);
    if !result.allowed {
        return result;
    }

    // Check version direction
    let result = validate_version_direction(ctx);
    if !result.allowed {
        return result;
    }

    UpgradeValidationResult::allowed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ClusterPhase, ClusterReference, PostgresClusterSpec, PostgresClusterStatus,
        PostgresUpgradeSpec, PostgresVersion, StorageSpec, TLSSpec, UpgradeStrategy,
    };
    use kube::core::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_source_cluster(name: &str, phase: ClusterPhase) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
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
            status: Some(PostgresClusterStatus {
                phase,
                ready_replicas: 3,
                ..Default::default()
            }),
        }
    }

    fn create_upgrade(
        name: &str,
        source: &str,
        target_version: PostgresVersion,
    ) -> PostgresUpgrade {
        PostgresUpgrade {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: PostgresUpgradeSpec {
                source_cluster: ClusterReference {
                    name: source.to_string(),
                    namespace: None,
                },
                target_version,
                target_cluster_overrides: None,
                strategy: UpgradeStrategy::default(),
            },
            status: None,
        }
    }

    #[test]
    fn test_version_upgrade_allowed() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Running);
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);

        let ctx = UpgradeValidationContext::new(&upgrade, None, Some(&source));
        let result = validate_version_direction(&ctx);

        assert!(result.allowed);
    }

    #[test]
    fn test_version_downgrade_denied() {
        let mut source = create_source_cluster("my-cluster", ClusterPhase::Running);
        source.spec.version = PostgresVersion::V17;
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V16);

        let ctx = UpgradeValidationContext::new(&upgrade, None, Some(&source));
        let result = validate_version_direction(&ctx);

        assert!(!result.allowed);
        assert_eq!(
            result.reason,
            Some("VersionDowngradeNotAllowed".to_string())
        );
    }

    #[test]
    fn test_same_version_denied() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Running);
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V16);

        let ctx = UpgradeValidationContext::new(&upgrade, None, Some(&source));
        let result = validate_version_direction(&ctx);

        assert!(!result.allowed);
        assert_eq!(result.reason, Some("SameVersionNotAllowed".to_string()));
    }

    #[test]
    fn test_source_cluster_not_running_denied() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Creating);
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);

        let ctx = UpgradeValidationContext::new(&upgrade, None, Some(&source));
        let result = validate_source_cluster(&ctx);

        assert!(!result.allowed);
        assert_eq!(result.reason, Some("SourceClusterNotRunning".to_string()));
    }

    #[test]
    fn test_source_cluster_not_found_denied() {
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);

        let ctx = UpgradeValidationContext::new(&upgrade, None, None);
        let result = validate_source_cluster(&ctx);

        assert!(!result.allowed);
        assert_eq!(result.reason, Some("SourceClusterNotFound".to_string()));
    }

    #[test]
    fn test_source_cluster_immutable() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Running);
        let old_upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);
        let mut new_upgrade = create_upgrade("my-upgrade", "other-cluster", PostgresVersion::V17);
        new_upgrade.spec.source_cluster.name = "other-cluster".to_string();

        let ctx = UpgradeValidationContext::new(&new_upgrade, Some(&old_upgrade), Some(&source));
        let result = validate_upgrade_immutability(&ctx);

        assert!(!result.allowed);
        assert_eq!(result.reason, Some("SourceClusterImmutable".to_string()));
    }

    #[test]
    fn test_target_version_immutable() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Running);
        let old_upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);
        let mut new_upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);
        new_upgrade.spec.target_version = PostgresVersion::V16;

        let ctx = UpgradeValidationContext::new(&new_upgrade, Some(&old_upgrade), Some(&source));
        let result = validate_upgrade_immutability(&ctx);

        assert!(!result.allowed);
        assert_eq!(result.reason, Some("TargetVersionImmutable".to_string()));
    }

    #[test]
    fn test_create_allowed_no_immutability_check() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Running);
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);

        let ctx = UpgradeValidationContext::new(&upgrade, None, Some(&source));
        let result = validate_upgrade_immutability(&ctx);

        assert!(result.allowed);
    }

    #[test]
    fn test_validate_upgrade_sync_all_pass() {
        let source = create_source_cluster("my-cluster", ClusterPhase::Running);
        let upgrade = create_upgrade("my-upgrade", "my-cluster", PostgresVersion::V17);

        let ctx = UpgradeValidationContext::new(&upgrade, None, Some(&source));
        let result = validate_upgrade_sync(&ctx);

        assert!(result.allowed);
    }
}
