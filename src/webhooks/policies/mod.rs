//! Admission webhook policies
//!
//! Each policy module exports a `validate` function that checks specific rules.

pub mod backup;
pub mod immutability;
pub mod production;
pub mod tls;

pub use backup::validate_backup;
pub use immutability::validate_immutability;
pub use production::validate_production;
pub use tls::validate_tls;

use crate::crd::PostgresCluster;

/// Result of a policy validation
#[derive(Debug)]
pub struct ValidationResult {
    pub allowed: bool,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl ValidationResult {
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

/// Context for validation including old object for UPDATE operations
pub struct ValidationContext<'a> {
    pub cluster: &'a PostgresCluster,
    pub old_cluster: Option<&'a PostgresCluster>,
    pub namespace_labels: std::collections::BTreeMap<String, String>,
}

impl<'a> ValidationContext<'a> {
    pub fn new(
        cluster: &'a PostgresCluster,
        old_cluster: Option<&'a PostgresCluster>,
        namespace_labels: std::collections::BTreeMap<String, String>,
    ) -> Self {
        Self {
            cluster,
            old_cluster,
            namespace_labels,
        }
    }

    /// Check if this is a CREATE operation (no old object)
    #[allow(dead_code)]
    pub fn is_create(&self) -> bool {
        self.old_cluster.is_none()
    }

    /// Check if namespace is marked as production
    pub fn is_production_namespace(&self) -> bool {
        self.namespace_labels
            .get("env")
            .map(|v| v == "production")
            .unwrap_or(false)
    }
}

/// Run all validation policies and return the first failure
pub fn validate_all(ctx: &ValidationContext) -> ValidationResult {
    // Tier 1: Critical policies (always enforced)
    let tier1_policies: Vec<fn(&ValidationContext) -> ValidationResult> =
        vec![validate_backup, validate_tls, validate_immutability];

    for policy in tier1_policies {
        let result = policy(ctx);
        if !result.allowed {
            return result;
        }
    }

    // Tier 2: Production policies (only for production namespaces)
    if ctx.is_production_namespace() {
        let result = validate_production(ctx);
        if !result.allowed {
            return result;
        }
    }

    ValidationResult::allowed()
}
