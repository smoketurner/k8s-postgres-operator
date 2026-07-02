//! Validation logic for PostgresCluster spec changes
//!
//! This module provides validation for spec changes, including:
//! - Scale operations (up/down)
//! - Immutable field changes
//! - Version changes
//! - Storage changes

use crate::controller::cluster_error::{Error, Result};

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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    // Note: PostgreSQL version validation is now handled by the CRD enum (PostgresVersion)
    // The enum only allows valid versions: "15", "16", "17"
}
