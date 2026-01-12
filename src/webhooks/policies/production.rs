//! Production namespace policies
//!
//! These policies are only enforced when the namespace has the label `env=production`.
//!
//! Requirements for production namespaces:
//! - HA required (replicas >= 3)
//! - Backup must be configured
//! - Resource limits required
//! - External network access not allowed

use super::{ValidationContext, ValidationResult};

/// Validate production namespace requirements
///
/// Only called when namespace has `env=production` label.
pub fn validate_production(ctx: &ValidationContext) -> ValidationResult {
    let spec = &ctx.cluster.spec;

    // Rule 1: HA required (replicas >= 3)
    if spec.replicas < 3 {
        return ValidationResult::denied(
            "ProductionHARequired",
            &format!(
                "Production clusters must have at least 3 replicas for high availability. Current: {}",
                spec.replicas
            ),
        );
    }

    // Rule 2: Backup must be configured
    if spec.backup.is_none() {
        return ValidationResult::denied(
            "ProductionBackupRequired",
            "Production clusters must have backup configured. Set spec.backup with a valid backup destination.",
        );
    }

    // Rule 3: Resource limits required
    match &spec.resources {
        Some(resources) => {
            let has_limits = resources
                .limits
                .as_ref()
                .map(|l| l.cpu.is_some() && l.memory.is_some())
                .unwrap_or(false);
            if !has_limits {
                return ValidationResult::denied(
                    "ProductionResourceLimitsRequired",
                    "Production clusters must have CPU and memory limits configured. Set spec.resources.limits.cpu and spec.resources.limits.memory.",
                );
            }
        }
        None => {
            return ValidationResult::denied(
                "ProductionResourceLimitsRequired",
                "Production clusters must have resource limits configured. Set spec.resources.limits.",
            );
        }
    }

    // Rule 4: External network access not allowed in production
    if let Some(np) = &spec.network_policy
        && np.allow_external_access
    {
        return ValidationResult::denied(
            "ProductionExternalAccessNotAllowed",
            "Production clusters cannot enable external network access. Set spec.networkPolicy.allowExternalAccess to false or remove it.",
        );
    }

    ValidationResult::allowed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        BackupDestination, BackupSpec, EncryptionSpec, NetworkPolicySpec, PostgresCluster,
        PostgresClusterSpec, PostgresVersion, ResourceList, ResourceRequirements, RetentionPolicy,
        StorageSpec, TLSSpec,
    };
    use kube::core::ObjectMeta;
    use std::collections::BTreeMap;

    fn production_labels() -> BTreeMap<String, String> {
        BTreeMap::from([("env".to_string(), "production".to_string())])
    }

    fn valid_backup() -> BackupSpec {
        BackupSpec {
            schedule: "0 2 * * *".to_string(),
            retention: RetentionPolicy {
                count: Some(30),
                max_age: None,
            },
            destination: BackupDestination::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                credentials_secret: "aws-creds".to_string(),
                path: None,
                force_path_style: false,
            },
            encryption: Some(EncryptionSpec {
                method: Default::default(),
                key_secret: "backup-key".to_string(),
            }),
            wal_archiving: None,
            compression: None,
            backup_from_replica: false,
            upload_concurrency: None,
            download_concurrency: None,
            enable_delta_backups: false,
            delta_max_steps: None,
        }
    }

    fn valid_resources() -> ResourceRequirements {
        ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("1Gi".to_string()),
            }),
            limits: Some(ResourceList {
                cpu: Some("2".to_string()),
                memory: Some("4Gi".to_string()),
            }),
            restart_on_resize: None,
        }
    }

    fn create_cluster(
        replicas: i32,
        backup: Option<BackupSpec>,
        resources: Option<ResourceRequirements>,
        network_policy: Option<NetworkPolicySpec>,
    ) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("production".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas,
                storage: StorageSpec {
                    size: "100Gi".to_string(),
                    storage_class: Some("fast-ssd".to_string()),
                },
                backup,
                resources,
                network_policy,
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_valid_production_cluster() {
        let cluster = create_cluster(3, Some(valid_backup()), Some(valid_resources()), None);
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_replicas_too_low() {
        let cluster = create_cluster(1, Some(valid_backup()), Some(valid_resources()), None);
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("ProductionHARequired".to_string()));
    }

    #[test]
    fn test_two_replicas_denied() {
        let cluster = create_cluster(2, Some(valid_backup()), Some(valid_resources()), None);
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("ProductionHARequired".to_string()));
    }

    #[test]
    fn test_missing_backup() {
        let cluster = create_cluster(3, None, Some(valid_resources()), None);
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason, Some("ProductionBackupRequired".to_string()));
    }

    #[test]
    fn test_missing_resources() {
        let cluster = create_cluster(3, Some(valid_backup()), None, None);
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(!result.allowed);
        assert_eq!(
            result.reason,
            Some("ProductionResourceLimitsRequired".to_string())
        );
    }

    #[test]
    fn test_missing_cpu_limit() {
        let resources = ResourceRequirements {
            requests: Some(ResourceList {
                cpu: Some("500m".to_string()),
                memory: Some("1Gi".to_string()),
            }),
            limits: Some(ResourceList {
                cpu: None,
                memory: Some("4Gi".to_string()),
            }),
            restart_on_resize: None,
        };
        let cluster = create_cluster(3, Some(valid_backup()), Some(resources), None);
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(!result.allowed);
        assert_eq!(
            result.reason,
            Some("ProductionResourceLimitsRequired".to_string())
        );
    }

    #[test]
    fn test_external_access_denied() {
        let network_policy = NetworkPolicySpec {
            allow_external_access: true,
            allow_from: vec![],
        };
        let cluster = create_cluster(
            3,
            Some(valid_backup()),
            Some(valid_resources()),
            Some(network_policy),
        );
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(!result.allowed);
        assert_eq!(
            result.reason,
            Some("ProductionExternalAccessNotAllowed".to_string())
        );
    }

    #[test]
    fn test_external_access_false_allowed() {
        let network_policy = NetworkPolicySpec {
            allow_external_access: false,
            allow_from: vec![],
        };
        let cluster = create_cluster(
            3,
            Some(valid_backup()),
            Some(valid_resources()),
            Some(network_policy),
        );
        let ctx = ValidationContext::new(&cluster, None, production_labels());
        let result = validate_production(&ctx);
        assert!(result.allowed);
    }
}
