use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use kube::ResourceExt;
use kube::core::ObjectMeta;
use rand::RngExt;

use crate::crd::PostgresCluster;
use crate::resources::common::{owner_reference, standard_labels};

/// Generate a secure random password.
///
/// The charset is deliberately alphanumeric. Generated passwords are embedded
/// in libpq URIs, JDBC query strings, and libpq keyword/value conninfo, and
/// characters with reserved meaning in those formats (`@`, `&`, `%`, `#`,
/// whitespace) corrupt the result. URI builders percent-encode defensively, but
/// keeping the charset unambiguous removes the hazard at the source.
pub fn generate_password(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..len)
        .filter_map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET.get(idx).map(|&c| c as char)
        })
        .collect()
}

/// Secret key holding the PostgreSQL connection string used by the KEDA
/// connection-based autoscaling TriggerAuthentication.
pub const CONNECTION_STRING_KEY: &str = "connection-string";

/// Build the PostgreSQL connection string for the primary service. The KEDA
/// TriggerAuthentication for connection-based autoscaling references the
/// `connection-string` key, so it must exist in the credentials secret or the
/// resulting ScaledObject's PostgreSQL trigger fails to evaluate (blocking all
/// scaling).
///
/// `tls_enabled` must reflect `spec.tls.enabled`: when TLS is disabled the
/// operator does not configure PostgreSQL for SSL, so a `sslmode=require`
/// client (libpq/KEDA) would be rejected and scaling would silently fail.
pub fn build_connection_string(
    cluster_name: &str,
    namespace: &str,
    superuser_password: &str,
    tls_enabled: bool,
) -> String {
    let sslmode = if tls_enabled { "require" } else { "disable" };
    format!(
        "postgresql://postgres:{}@{}-primary.{}.svc.cluster.local:5432/postgres?sslmode={}",
        superuser_password, cluster_name, namespace, sslmode
    )
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

    let namespace = ns.clone().unwrap_or_else(|| "default".to_string());
    let connection_string = build_connection_string(
        &cluster_name,
        &namespace,
        &superuser_password,
        cluster.spec.tls.enabled,
    );

    let string_data = BTreeMap::from([
        ("POSTGRES_PASSWORD".to_string(), superuser_password.clone()),
        ("REPLICATION_PASSWORD".to_string(), replication_password),
        ("PGPASSWORD".to_string(), superuser_password),
        (CONNECTION_STRING_KEY.to_string(), connection_string),
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
