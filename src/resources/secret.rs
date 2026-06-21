use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use rand::RngExt;

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, standard_labels};

/// Generate a secure random password
fn generate_password(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..len)
        .filter_map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET.get(idx).map(|&c| c as char)
        })
        .collect()
}

/// Generate the credentials Secret
pub fn generate_credentials_secret(cluster: &PostgresCluster) -> Secret {
    let name = format!("{}-credentials", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = standard_labels(&cluster_name);

    // Generate passwords
    let superuser_password = generate_password(32);
    let replication_password = generate_password(32);

    // PostgreSQL connection string for the primary service. The KEDA
    // TriggerAuthentication for connection-based autoscaling references the
    // `connection-string` key, so it must exist in this secret or the resulting
    // ScaledObject's PostgreSQL trigger fails to evaluate (blocking all scaling).
    let namespace = ns.clone().unwrap_or_else(|| "default".to_string());
    let connection_string = format!(
        "postgresql://postgres:{}@{}-primary.{}.svc.cluster.local:5432/postgres?sslmode=require",
        superuser_password, cluster_name, namespace
    );

    let string_data = BTreeMap::from([
        ("POSTGRES_PASSWORD".to_string(), superuser_password.clone()),
        ("REPLICATION_PASSWORD".to_string(), replication_password),
        ("PGPASSWORD".to_string(), superuser_password),
        ("connection-string".to_string(), connection_string),
    ]);

    Secret {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: ns,
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            ..Default::default()
        },
        type_: Some("Opaque".to_string()),
        string_data: Some(string_data),
        ..Default::default()
    }
}
