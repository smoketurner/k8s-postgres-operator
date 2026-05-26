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

/// Compute the effective `(name, namespace)` of the source cluster a
/// PostgresUpgrade points at.
///
/// When `spec.sourceCluster.namespace` is omitted it defaults to the
/// upgrade resource's own namespace, matching the convention used in the
/// reconciler (see `controller::upgrade_reconciler`).
pub fn effective_source_ref(upgrade: &PostgresUpgrade) -> (&str, String) {
    let name = upgrade.spec.source_cluster.name.as_str();
    let ns = upgrade
        .spec
        .source_cluster
        .namespace
        .clone()
        .unwrap_or_else(|| upgrade.namespace().unwrap_or_default());
    (name, ns)
}

/// Pure helper that searches a slice of existing `PostgresUpgrade` objects
/// for a non-terminal upgrade that targets the same `(name, namespace)`
/// source cluster as `upgrade`, ignoring `upgrade` itself.
///
/// Extracted from `validate_no_concurrent_upgrade` so the comparison logic
/// can be unit-tested without a Kubernetes client.
pub fn find_conflicting_upgrade<'a>(
    upgrade: &PostgresUpgrade,
    existing: &'a [PostgresUpgrade],
) -> Option<&'a PostgresUpgrade> {
    let (source_name, source_ns) = effective_source_ref(upgrade);
    let upgrade_name = upgrade.name_any();

    existing.iter().find(|candidate| {
        // Skip the upgrade being validated (covers re-validation of an existing object).
        if candidate.name_any() == upgrade_name {
            return false;
        }

        let (candidate_name, candidate_ns) = effective_source_ref(candidate);
        if candidate_name != source_name || candidate_ns != source_ns {
            return false;
        }

        let phase = candidate
            .status
            .as_ref()
            .map(|s| &s.phase)
            .unwrap_or(&UpgradePhase::Pending);
        !phase.is_terminal()
    })
}

/// Check for concurrent upgrades on the same source cluster
///
/// Lists `PostgresUpgrade` objects across all namespaces because the source
/// cluster a given upgrade points at may live in a different namespace from
/// the upgrade resource itself. Comparison is on the effective
/// `(name, namespace)` tuple, not just the name.
///
/// Requires cluster-wide list permission on `postgresupgrades`; the
/// operator ClusterRole already grants this (see `config/rbac/role.yaml`).
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

    // List PostgresUpgrade resources across all namespaces. A cross-namespace
    // source reference would be invisible to a namespaced list.
    let upgrades: Api<PostgresUpgrade> = Api::all(client.clone());

    match upgrades.list(&ListParams::default()).await {
        Ok(list) => {
            if let Some(conflicting) = find_conflicting_upgrade(upgrade, &list.items) {
                let (source_name, source_ns) = effective_source_ref(upgrade);
                let phase = conflicting
                    .status
                    .as_ref()
                    .map(|s| &s.phase)
                    .unwrap_or(&UpgradePhase::Pending);
                let conflicting_ns = conflicting.namespace().unwrap_or_default();
                return UpgradeValidationResult::denied(
                    "ConcurrentUpgradeNotAllowed",
                    &format!(
                        "Another upgrade '{}/{}' is already in progress for source cluster '{}/{}' (phase: {:?}). Wait for it to complete or delete it first.",
                        conflicting_ns,
                        conflicting.name_any(),
                        source_ns,
                        source_name,
                        phase
                    ),
                );
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
        create_upgrade_full(name, "default", source, None, target_version, None)
    }

    fn create_upgrade_full(
        name: &str,
        namespace: &str,
        source_name: &str,
        source_namespace: Option<&str>,
        target_version: PostgresVersion,
        phase: Option<UpgradePhase>,
    ) -> PostgresUpgrade {
        PostgresUpgrade {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            spec: PostgresUpgradeSpec {
                source_cluster: ClusterReference {
                    name: source_name.to_string(),
                    namespace: source_namespace.map(String::from),
                },
                target_version,
                target_cluster_overrides: None,
                strategy: UpgradeStrategy::default(),
            },
            status: phase.map(|p| crate::crd::PostgresUpgradeStatus {
                phase: p,
                ..Default::default()
            }),
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

    // -- effective_source_ref ---------------------------------------------

    #[test]
    fn test_effective_source_ref_inherits_upgrade_namespace() {
        let upgrade = create_upgrade_full("u", "team-a", "src", None, PostgresVersion::V17, None);
        let (name, ns) = effective_source_ref(&upgrade);
        assert_eq!(name, "src");
        assert_eq!(ns, "team-a");
    }

    #[test]
    fn test_effective_source_ref_uses_explicit_namespace() {
        let upgrade = create_upgrade_full(
            "u",
            "team-a",
            "src",
            Some("shared"),
            PostgresVersion::V17,
            None,
        );
        let (name, ns) = effective_source_ref(&upgrade);
        assert_eq!(name, "src");
        assert_eq!(ns, "shared");
    }

    // -- find_conflicting_upgrade ----------------------------------------

    #[test]
    fn test_find_conflicting_upgrade_same_namespace_same_source_denied() {
        let new_upgrade =
            create_upgrade_full("new", "team-a", "src", None, PostgresVersion::V17, None);
        let existing = create_upgrade_full(
            "existing",
            "team-a",
            "src",
            None,
            PostgresVersion::V17,
            Some(UpgradePhase::Replicating),
        );

        let existing = vec![existing];
        let conflict = find_conflicting_upgrade(&new_upgrade, &existing);
        assert!(
            conflict.is_some(),
            "same-namespace conflict must be flagged"
        );
    }

    #[test]
    fn test_find_conflicting_upgrade_cross_namespace_same_source_denied() {
        // New upgrade lives in team-a but references a source in shared.
        let new_upgrade = create_upgrade_full(
            "new",
            "team-a",
            "src",
            Some("shared"),
            PostgresVersion::V17,
            None,
        );
        // Pre-existing upgrade in team-b also targets shared/src.
        let existing = create_upgrade_full(
            "existing",
            "team-b",
            "src",
            Some("shared"),
            PostgresVersion::V17,
            Some(UpgradePhase::Replicating),
        );

        let existing = vec![existing];
        let conflict = find_conflicting_upgrade(&new_upgrade, &existing);
        assert!(
            conflict.is_some(),
            "cross-namespace conflict on the same source must be flagged"
        );
    }

    #[test]
    fn test_find_conflicting_upgrade_same_source_name_different_namespace_allowed() {
        // Two clusters happen to share a name in different namespaces.
        // These are unrelated and should not conflict.
        let new_upgrade =
            create_upgrade_full("new", "team-a", "src", None, PostgresVersion::V17, None);
        let existing = create_upgrade_full(
            "existing",
            "team-b",
            "src",
            None,
            PostgresVersion::V17,
            Some(UpgradePhase::Replicating),
        );

        let existing = vec![existing];
        let conflict = find_conflicting_upgrade(&new_upgrade, &existing);
        assert!(
            conflict.is_none(),
            "same source name in different namespaces must not be flagged"
        );
    }

    #[test]
    fn test_find_conflicting_upgrade_no_match_allowed() {
        let new_upgrade =
            create_upgrade_full("new", "team-a", "src", None, PostgresVersion::V17, None);
        let existing = create_upgrade_full(
            "existing",
            "team-a",
            "other-src",
            None,
            PostgresVersion::V17,
            Some(UpgradePhase::Replicating),
        );

        let existing = vec![existing];
        let conflict = find_conflicting_upgrade(&new_upgrade, &existing);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_find_conflicting_upgrade_terminal_phase_allowed() {
        // A Completed upgrade against the same source should not block.
        let new_upgrade =
            create_upgrade_full("new", "team-a", "src", None, PostgresVersion::V17, None);
        let existing = create_upgrade_full(
            "existing",
            "team-a",
            "src",
            None,
            PostgresVersion::V17,
            Some(UpgradePhase::Completed),
        );

        let existing = vec![existing];
        let conflict = find_conflicting_upgrade(&new_upgrade, &existing);
        assert!(
            conflict.is_none(),
            "terminal-phase upgrades must not be flagged as conflicts"
        );
    }

    #[test]
    fn test_find_conflicting_upgrade_skips_self() {
        // Re-validating the same object against a list that contains it
        // (e.g., during a webhook retry) must not self-conflict.
        let new_upgrade =
            create_upgrade_full("same", "team-a", "src", None, PostgresVersion::V17, None);
        let existing = create_upgrade_full(
            "same",
            "team-a",
            "src",
            None,
            PostgresVersion::V17,
            Some(UpgradePhase::Replicating),
        );

        let existing = vec![existing];
        let conflict = find_conflicting_upgrade(&new_upgrade, &existing);
        assert!(conflict.is_none());
    }
}
