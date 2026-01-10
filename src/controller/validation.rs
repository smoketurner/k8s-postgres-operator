//! Validation logic for PostgresCluster spec changes
//!
//! This module provides validation for spec changes, including:
//! - Scale operations (up/down)
//! - Immutable field changes
//! - Version changes
//! - Storage changes

use crate::controller::error::{Error, Result};
use crate::crd::PostgresCluster;

/// Minimum number of replicas
/// With Patroni using Kubernetes DCS (endpoints), we can run with 1 replica
pub const MIN_REPLICAS: i32 = 1;

/// Maximum number of replicas (arbitrary limit for safety)
pub const MAX_REPLICAS: i32 = 100;

/// Validate the cluster spec
pub fn validate_spec(cluster: &PostgresCluster) -> Result<()> {
    validate_replicas(cluster)?;
    // Note: PostgreSQL version is validated by the CRD enum (PostgresVersion)
    validate_storage(cluster)?;
    Ok(())
}

/// Validate replica count
fn validate_replicas(cluster: &PostgresCluster) -> Result<()> {
    let replicas = cluster.spec.replicas;

    if replicas < MIN_REPLICAS {
        return Err(Error::ValidationError(format!(
            "replica count {} is below minimum {}",
            replicas, MIN_REPLICAS
        )));
    }

    if replicas > MAX_REPLICAS {
        return Err(Error::ValidationError(format!(
            "replica count {} exceeds maximum {}",
            replicas, MAX_REPLICAS
        )));
    }

    Ok(())
}

/// Validate storage configuration
fn validate_storage(cluster: &PostgresCluster) -> Result<()> {
    let size = &cluster.spec.storage.size;

    // Validate size format (e.g., "10Gi", "100Gi")
    if !size.ends_with("Gi") && !size.ends_with("Mi") && !size.ends_with("Ti") {
        return Err(Error::ValidationError(format!(
            "storage size must end with Gi, Mi, or Ti: {}",
            size
        )));
    }

    // Parse numeric value
    let num_str = size.trim_end_matches(char::is_alphabetic);
    let _num: u64 = num_str
        .parse()
        .map_err(|_| Error::ValidationError(format!("invalid storage size number: {}", size)))?;

    Ok(())
}

/// Result of comparing old and new spec
#[derive(Debug, Clone)]
pub struct SpecDiff {
    /// Number of replicas changed
    pub replicas_changed: bool,
    /// Scale direction (positive = up, negative = down)
    pub replica_delta: i32,
    /// Version changed
    pub version_changed: bool,
    /// Storage size changed
    pub storage_changed: bool,
    /// PostgreSQL parameters changed
    pub params_changed: bool,
    /// Resources changed
    pub resources_changed: bool,
}

impl SpecDiff {
    /// Check if any changes require a rolling update
    pub fn requires_rolling_update(&self) -> bool {
        self.version_changed || self.params_changed || self.resources_changed
    }

    /// Check if this is a scale-only operation
    pub fn is_scale_only(&self) -> bool {
        self.replicas_changed
            && !self.version_changed
            && !self.storage_changed
            && !self.params_changed
            && !self.resources_changed
    }

    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        self.replicas_changed
            || self.version_changed
            || self.storage_changed
            || self.params_changed
            || self.resources_changed
    }
}

/// Validate spec changes between old and new cluster specs
pub fn validate_spec_change(old: &PostgresCluster, new: &PostgresCluster) -> Result<SpecDiff> {
    let old_spec = &old.spec;
    let new_spec = &new.spec;

    // Check for immutable field changes
    if old_spec.storage.storage_class != new_spec.storage.storage_class {
        return Err(Error::ValidationError(
            "storage class cannot be changed after creation".to_string(),
        ));
    }

    // Calculate replica delta
    let replica_delta = new_spec.replicas - old_spec.replicas;

    // Validate scale down
    if replica_delta < 0 {
        validate_scale_down(old, new)?;
    }

    let diff = SpecDiff {
        replicas_changed: old_spec.replicas != new_spec.replicas,
        replica_delta,
        version_changed: old_spec.version != new_spec.version,
        storage_changed: old_spec.storage.size != new_spec.storage.size,
        params_changed: old_spec.postgresql_params != new_spec.postgresql_params,
        resources_changed: old_spec.resources != new_spec.resources,
    };

    Ok(diff)
}

/// Validate scale down operation
fn validate_scale_down(old: &PostgresCluster, new: &PostgresCluster) -> Result<()> {
    let new_replicas = new.spec.replicas;

    // Ensure we don't go below minimum
    if new_replicas < MIN_REPLICAS {
        return Err(Error::ValidationError(format!(
            "cannot scale down to {} replicas, minimum is {}",
            new_replicas, MIN_REPLICAS
        )));
    }

    // Check that we're not scaling down too aggressively (more than 50% at once)
    let old_replicas = old.spec.replicas;
    let delta = old_replicas - new_replicas;
    if old_replicas > 2 && delta > old_replicas / 2 {
        tracing::warn!(
            "Large scale down detected: {} -> {} ({}% reduction). Consider scaling down gradually.",
            old_replicas,
            new_replicas,
            (delta as f64 / old_replicas as f64 * 100.0) as i32
        );
    }

    Ok(())
}

/// Validate major version upgrade
pub fn validate_version_upgrade(old_version: &str, new_version: &str) -> Result<()> {
    let old_major: u32 = old_version
        .split('.')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let new_major: u32 = new_version
        .split('.')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // Downgrade is not allowed
    if new_major < old_major {
        return Err(Error::ValidationError(format!(
            "PostgreSQL downgrade from {} to {} is not supported",
            old_version, new_version
        )));
    }

    // Skip more than one major version is risky
    if new_major > old_major + 1 {
        tracing::warn!(
            "Upgrading more than one major version ({} -> {}). \
             Consider upgrading incrementally.",
            old_version,
            new_version
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replica_limits() {
        assert_eq!(MIN_REPLICAS, 1);
        assert_eq!(MAX_REPLICAS, 100);
    }

    // Note: PostgreSQL version validation is now handled by the CRD enum (PostgresVersion)
    // The enum only allows valid versions: "15", "16", "17"
}
