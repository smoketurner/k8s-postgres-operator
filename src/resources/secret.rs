use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::core::ObjectMeta;
use kube::ResourceExt;
use rand::Rng;
use std::collections::BTreeMap;

use crate::controller::error::Result;
use crate::crd::PostgresCluster;

/// Generate owner reference for the cluster
fn owner_reference(cluster: &PostgresCluster) -> OwnerReference {
    OwnerReference {
        api_version: "postgres.example.com/v1alpha1".to_string(),
        kind: "PostgresCluster".to_string(),
        name: cluster.name_any(),
        uid: cluster.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Generate a secure random password
fn generate_password(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

/// Generate the credentials Secret
pub fn generate_credentials_secret(cluster: &PostgresCluster) -> Result<Secret> {
    let name = format!("{}-credentials", cluster.name_any());
    let cluster_name = cluster.name_any();
    let ns = cluster.namespace();

    let labels = BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), cluster_name.clone()),
        ("app.kubernetes.io/component".to_string(), "postgresql".to_string()),
        ("app.kubernetes.io/managed-by".to_string(), "postgres-operator".to_string()),
        ("postgres.example.com/cluster".to_string(), cluster_name),
    ]);

    // Generate passwords
    let superuser_password = generate_password(32);
    let replication_password = generate_password(32);

    let string_data = BTreeMap::from([
        ("POSTGRES_PASSWORD".to_string(), superuser_password.clone()),
        ("REPLICATION_PASSWORD".to_string(), replication_password),
        ("PGPASSWORD".to_string(), superuser_password),
    ]);

    Ok(Secret {
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
    })
}
