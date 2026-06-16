//! Logical backup CronJob generation.
//!
//! Builds a `batch/v1.CronJob` that runs `pg_dumpall` on a schedule and
//! uploads the gzipped output to the same S3 destination WAL-G uses. The
//! job is only generated when `spec.backup.logical.enabled` is true.
//!
//! The pod uses the cluster's Spilo image by default so the `pg_dumpall`
//! binary version matches the source PostgreSQL major version and the
//! `aws` CLI is available for the upload step. Users can override the
//! image via `spec.backup.logical.image` if their environment requires it.

use std::collections::BTreeMap;

use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobSpec, JobTemplateSpec};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, PodSpec, PodTemplateSpec, ResourceRequirements,
    SecretKeySelector,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt;

use crate::crd::{BackupDestination, PostgresCluster};
use crate::resources::common::{owner_reference, standard_labels};

/// Generate a `CronJob` that runs `pg_dumpall` to S3, or `None` if logical
/// backups are not enabled in the spec.
pub fn generate_logical_backup_cronjob(cluster: &PostgresCluster) -> Option<CronJob> {
    let backup = cluster.spec.backup.as_ref()?;
    let logical = backup.logical.as_ref()?;
    if !logical.enabled {
        return None;
    }

    let cluster_name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let name = format!("{cluster_name}-logical-backup");

    let BackupDestination::S3 {
        bucket,
        region,
        endpoint,
        credentials_secret,
        path,
        ..
    } = &backup.destination;

    let prefix = path
        .clone()
        .unwrap_or_else(|| format!("{namespace}/{cluster_name}"));
    let image = logical
        .image
        .clone()
        .unwrap_or_else(|| cluster.spec.version.spilo_image());

    let labels = standard_labels(&cluster_name);

    let mut env = vec![
        EnvVar {
            name: "PGHOST".to_string(),
            value: Some(format!("{cluster_name}-primary.{namespace}.svc")),
            ..Default::default()
        },
        EnvVar {
            name: "PGUSER".to_string(),
            value: Some("postgres".to_string()),
            ..Default::default()
        },
        EnvVar {
            name: "PGPASSWORD".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: format!("{cluster_name}-credentials"),
                    key: "PGPASSWORD".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "AWS_ACCESS_KEY_ID".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: credentials_secret.clone(),
                    key: "AWS_ACCESS_KEY_ID".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "AWS_SECRET_ACCESS_KEY".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: credentials_secret.clone(),
                    key: "AWS_SECRET_ACCESS_KEY".to_string(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "AWS_SESSION_TOKEN".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: credentials_secret.clone(),
                    key: "AWS_SESSION_TOKEN".to_string(),
                    optional: Some(true),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "AWS_DEFAULT_REGION".to_string(),
            value: Some(region.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "S3_BUCKET".to_string(),
            value: Some(bucket.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "S3_PREFIX".to_string(),
            value: Some(prefix),
            ..Default::default()
        },
    ];
    if let Some(ep) = endpoint.as_ref() {
        env.push(EnvVar {
            name: "AWS_ENDPOINT_URL".to_string(),
            value: Some(ep.clone()),
            ..Default::default()
        });
    }

    // The script streams pg_dumpall through gzip and pipes it to aws s3 cp.
    // `set -euo pipefail` propagates failures from any pipeline stage; without
    // pipefail a failed pg_dumpall would silently succeed because the aws CLI
    // would still exit 0 after uploading an empty object.
    let script = r#"set -euo pipefail
TS=$(date -u +%Y-%m-%dT%H-%M-%SZ)
DEST="s3://${S3_BUCKET}/${S3_PREFIX}/logical/${TS}.sql.gz"
echo "Streaming pg_dumpall to ${DEST}"
ENDPOINT_FLAG=""
if [ -n "${AWS_ENDPOINT_URL:-}" ]; then
  ENDPOINT_FLAG="--endpoint-url ${AWS_ENDPOINT_URL}"
fi
pg_dumpall --clean --if-exists --no-password \
  | gzip -c \
  | aws s3 cp ${ENDPOINT_FLAG} - "${DEST}"
echo "Logical backup complete: ${DEST}"
"#;

    let container = Container {
        name: "pg-dumpall".to_string(),
        image: Some(image),
        command: Some(vec![
            "/bin/bash".to_string(),
            "-c".to_string(),
            script.to_string(),
        ]),
        env: Some(env),
        resources: logical.resources.as_ref().map(to_k8s_resources),
        ..Default::default()
    };

    let cronjob_spec = CronJobSpec {
        schedule: logical.schedule.clone(),
        // Don't pile up dumps if a run is slow; skip the next tick instead.
        concurrency_policy: Some("Forbid".to_string()),
        successful_jobs_history_limit: Some(logical.successful_jobs_history_limit.unwrap_or(3)),
        failed_jobs_history_limit: Some(logical.failed_jobs_history_limit.unwrap_or(3)),
        job_template: JobTemplateSpec {
            spec: Some(JobSpec {
                backoff_limit: Some(2),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels.clone()),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        restart_policy: Some("OnFailure".to_string()),
                        containers: vec![container],
                        ..Default::default()
                    }),
                },
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };

    Some(CronJob {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        spec: cronjob_spec,
        ..Default::default()
    })
}

/// Translate our [`crate::crd::ResourceRequirements`] into the upstream
/// k8s_openapi shape used by `PodSpec.containers[].resources`.
fn to_k8s_resources(spec: &crate::crd::ResourceRequirements) -> ResourceRequirements {
    let to_map = |list: &crate::crd::ResourceList| -> BTreeMap<String, Quantity> {
        let mut map = BTreeMap::new();
        if let Some(cpu) = list.cpu.as_ref() {
            map.insert("cpu".to_string(), Quantity(cpu.clone()));
        }
        if let Some(mem) = list.memory.as_ref() {
            map.insert("memory".to_string(), Quantity(mem.clone()));
        }
        map
    };
    ResourceRequirements {
        requests: spec.requests.as_ref().map(to_map),
        limits: spec.limits.as_ref().map(to_map),
        ..Default::default()
    }
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
    use crate::crd::{
        BackupSpec, EncryptionMethod, EncryptionSpec, LogicalBackupSpec, PostgresClusterSpec,
        PostgresVersion, RetentionPolicy, StorageSpec, TLSSpec,
    };

    fn cluster_with_logical(logical: Option<LogicalBackupSpec>) -> PostgresCluster {
        PostgresCluster {
            metadata: kube::core::ObjectMeta {
                name: Some("pg".to_string()),
                namespace: Some("apps".to_string()),
                uid: Some("uid".to_string()),
                ..Default::default()
            },
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
                backup: Some(BackupSpec {
                    schedule: "0 2 * * *".to_string(),
                    retention: RetentionPolicy {
                        count: Some(7),
                        max_age: None,
                    },
                    destination: BackupDestination::S3 {
                        bucket: "my-bucket".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: None,
                        credentials_secret: "aws-creds".to_string(),
                        path: None,
                        force_path_style: false,
                    },
                    wal_archiving: None,
                    encryption: Some(EncryptionSpec {
                        method: EncryptionMethod::Aes256,
                        key_secret: "enc-key".to_string(),
                    }),
                    compression: None,
                    backup_from_replica: false,
                    upload_concurrency: None,
                    download_concurrency: None,
                    enable_delta_backups: false,
                    delta_max_steps: None,
                    logical,
                }),
                pgbouncer: None,
                tls: TLSSpec::default(),
                metrics: None,
                service: None,
                restore: None,
                scaling: None,
                network_policy: None,
                sidecars: vec![],
                node_selector: Default::default(),
                tolerations: vec![],
                topology_spread_constraints: vec![],
                priority_class_name: None,
            },
            status: None,
        }
    }

    #[test]
    fn returns_none_when_logical_absent() {
        let cluster = cluster_with_logical(None);
        assert!(generate_logical_backup_cronjob(&cluster).is_none());
    }

    #[test]
    fn returns_none_when_disabled() {
        let cluster = cluster_with_logical(Some(LogicalBackupSpec {
            enabled: false,
            schedule: "0 3 * * *".to_string(),
            image: None,
            resources: None,
            successful_jobs_history_limit: None,
            failed_jobs_history_limit: None,
        }));
        assert!(generate_logical_backup_cronjob(&cluster).is_none());
    }

    #[test]
    fn populates_schedule_and_history_limits() {
        let cluster = cluster_with_logical(Some(LogicalBackupSpec {
            enabled: true,
            schedule: "0 3 * * *".to_string(),
            image: None,
            resources: None,
            successful_jobs_history_limit: Some(5),
            failed_jobs_history_limit: Some(1),
        }));

        let cj = generate_logical_backup_cronjob(&cluster).expect("cronjob");
        let spec = &cj.spec;
        assert_eq!(spec.schedule, "0 3 * * *");
        assert_eq!(spec.successful_jobs_history_limit, Some(5));
        assert_eq!(spec.failed_jobs_history_limit, Some(1));
        assert_eq!(spec.concurrency_policy.as_deref(), Some("Forbid"));
        assert_eq!(cj.name_any(), "pg-logical-backup");
        assert_eq!(cj.namespace().as_deref(), Some("apps"));
    }

    #[test]
    fn defaults_image_to_cluster_spilo_image() {
        let cluster = cluster_with_logical(Some(LogicalBackupSpec {
            enabled: true,
            schedule: "0 3 * * *".to_string(),
            image: None,
            resources: None,
            successful_jobs_history_limit: None,
            failed_jobs_history_limit: None,
        }));
        let cj = generate_logical_backup_cronjob(&cluster).unwrap();
        let pod_spec = cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap();
        let container = &pod_spec.containers[0];
        assert_eq!(
            container.image.as_deref(),
            Some(PostgresVersion::V16.spilo_image().as_str()),
        );
    }

    #[test]
    fn injects_pg_and_aws_env_from_secrets() {
        let cluster = cluster_with_logical(Some(LogicalBackupSpec {
            enabled: true,
            schedule: "0 3 * * *".to_string(),
            image: None,
            resources: None,
            successful_jobs_history_limit: None,
            failed_jobs_history_limit: None,
        }));
        let cj = generate_logical_backup_cronjob(&cluster).unwrap();
        let env = cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .expect("env");

        let by_name: std::collections::BTreeMap<_, _> =
            env.iter().map(|e| (e.name.as_str(), e)).collect();

        assert_eq!(
            by_name["PGHOST"].value.as_deref(),
            Some("pg-primary.apps.svc")
        );
        assert_eq!(by_name["PGUSER"].value.as_deref(), Some("postgres"));

        let pgpass = by_name["PGPASSWORD"];
        let secret_ref = pgpass
            .value_from
            .as_ref()
            .and_then(|s| s.secret_key_ref.as_ref())
            .unwrap();
        assert_eq!(secret_ref.name, "pg-credentials");

        let aws_key = by_name["AWS_ACCESS_KEY_ID"];
        let aws_key_ref = aws_key
            .value_from
            .as_ref()
            .and_then(|s| s.secret_key_ref.as_ref())
            .unwrap();
        assert_eq!(aws_key_ref.name, "aws-creds");
        assert_eq!(aws_key_ref.key, "AWS_ACCESS_KEY_ID");

        // Session token must be optional (assume-role flows often omit it).
        let token = by_name["AWS_SESSION_TOKEN"];
        let token_ref = token
            .value_from
            .as_ref()
            .and_then(|s| s.secret_key_ref.as_ref())
            .unwrap();
        assert_eq!(token_ref.optional, Some(true));

        // AWS_ENDPOINT_URL only present when destination specifies one.
        assert!(!by_name.contains_key("AWS_ENDPOINT_URL"));

        assert_eq!(by_name["S3_BUCKET"].value.as_deref(), Some("my-bucket"));
        assert_eq!(by_name["S3_PREFIX"].value.as_deref(), Some("apps/pg"));
    }

    #[test]
    fn includes_endpoint_url_when_destination_has_one() {
        let mut cluster = cluster_with_logical(Some(LogicalBackupSpec {
            enabled: true,
            schedule: "0 3 * * *".to_string(),
            image: None,
            resources: None,
            successful_jobs_history_limit: None,
            failed_jobs_history_limit: None,
        }));
        if let Some(backup) = cluster.spec.backup.as_mut() {
            let BackupDestination::S3 { endpoint, .. } = &mut backup.destination;
            *endpoint = Some("https://minio.example.com".to_string());
        }

        let cj = generate_logical_backup_cronjob(&cluster).unwrap();
        let env = cj
            .spec
            .job_template
            .spec
            .as_ref()
            .unwrap()
            .template
            .spec
            .as_ref()
            .unwrap()
            .containers[0]
            .env
            .as_ref()
            .unwrap();
        let endpoint = env
            .iter()
            .find(|e| e.name == "AWS_ENDPOINT_URL")
            .expect("endpoint env var");
        assert_eq!(endpoint.value.as_deref(), Some("https://minio.example.com"));
    }
}
