//! cert-manager Certificate resource generation
//!
//! This module generates cert-manager Certificate custom resources for PostgresCluster
//! TLS configuration. The Certificate CR tells cert-manager to provision a TLS
//! certificate and store it in a Kubernetes Secret.
//!
//! Reference: https://cert-manager.io/docs/concepts/certificate/

use std::collections::BTreeMap;

use kube::api::ObjectMeta;
use serde::{Deserialize, Serialize};

use crate::crd::PostgresCluster;
use crate::resources::common::{FIELD_MANAGER, owner_reference, standard_labels};

/// cert-manager Certificate resource
///
/// This is a simplified representation of cert-manager's Certificate CRD
/// for generating TLS certificates.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Certificate {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: CertificateSpec,
}

/// Certificate spec for cert-manager
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateSpec {
    /// Name of the Secret to store the certificate
    pub secret_name: String,

    /// Reference to the issuer (Issuer or ClusterIssuer)
    pub issuer_ref: CertIssuerRef,

    /// DNS names to include in the certificate
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dns_names: Vec<String>,

    /// Certificate validity duration (e.g., "2160h" for 90 days)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// Time before expiry to renew (e.g., "360h" for 15 days)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub renew_before: Option<String>,

    /// Secret template for labels/annotations on the generated Secret
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_template: Option<SecretTemplate>,

    /// Private key configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<PrivateKeySpec>,

    /// Usages for the certificate
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub usages: Vec<String>,
}

/// Reference to a cert-manager issuer
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CertIssuerRef {
    pub name: String,
    pub kind: String,
    pub group: String,
}

/// Secret template for the generated certificate secret
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretTemplate {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// Private key configuration
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivateKeySpec {
    /// Key algorithm (RSA, ECDSA, Ed25519)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<String>,
    /// Key size (for RSA: 2048, 4096; for ECDSA: 256, 384, 521)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i32>,
    /// Encoding format (PKCS1, PKCS8)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    /// Rotation policy (Always, Never)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_policy: Option<String>,
}

/// Generate a cert-manager Certificate resource for a PostgresCluster
///
/// Returns None if TLS is disabled or no issuerRef is configured.
pub fn generate_certificate(cluster: &PostgresCluster) -> Option<Certificate> {
    let tls = &cluster.spec.tls;

    // Only generate if TLS is enabled and issuerRef is configured
    if !tls.enabled {
        return None;
    }

    let issuer_ref = tls.issuer_ref.as_ref()?;

    let name = cluster.metadata.name.as_ref()?;
    let namespace = cluster.metadata.namespace.as_ref()?;

    let secret_name = format!("{}-tls", name);

    // Build DNS names for the certificate
    let mut dns_names = vec![
        // Primary service
        format!("{}-primary", name),
        format!("{}-primary.{}", name, namespace),
        format!("{}-primary.{}.svc", name, namespace),
        format!("{}-primary.{}.svc.cluster.local", name, namespace),
        // Replica service
        format!("{}-repl", name),
        format!("{}-repl.{}", name, namespace),
        format!("{}-repl.{}.svc", name, namespace),
        format!("{}-repl.{}.svc.cluster.local", name, namespace),
        // Headless service (for individual pod DNS)
        format!("{}-headless", name),
        format!("{}-headless.{}", name, namespace),
        format!("{}-headless.{}.svc", name, namespace),
        format!("{}-headless.{}.svc.cluster.local", name, namespace),
        // Pod DNS names (StatefulSet pods)
        format!("*.{}-headless.{}.svc.cluster.local", name, namespace),
    ];

    // Add any additional DNS names from the spec
    dns_names.extend(tls.additional_dns_names.iter().cloned());

    // Build labels for the secret
    let mut labels = standard_labels(name);
    labels.insert("app.kubernetes.io/component".to_string(), "tls".to_string());

    let cert_issuer_ref = CertIssuerRef {
        name: issuer_ref.name.clone(),
        kind: issuer_ref.kind.to_string(),
        group: issuer_ref.group.clone(),
    };

    let spec = CertificateSpec {
        secret_name,
        issuer_ref: cert_issuer_ref,
        dns_names,
        duration: tls.duration.clone(),
        renew_before: tls.renew_before.clone(),
        secret_template: Some(SecretTemplate {
            labels: labels.clone(),
            annotations: BTreeMap::new(),
        }),
        private_key: Some(PrivateKeySpec {
            algorithm: Some("RSA".to_string()),
            size: Some(4096),
            encoding: Some("PKCS8".to_string()),
            rotation_policy: Some("Always".to_string()),
        }),
        usages: vec![
            "server auth".to_string(),
            "client auth".to_string(),
            "digital signature".to_string(),
            "key encipherment".to_string(),
        ],
    };

    Some(Certificate {
        api_version: "cert-manager.io/v1".to_string(),
        kind: "Certificate".to_string(),
        metadata: ObjectMeta {
            name: Some(format!("{}-tls", name)),
            namespace: Some(namespace.clone()),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(cluster)]),
            annotations: Some(BTreeMap::from([(
                "postgres-operator.smoketurner.com/managed-by".to_string(),
                FIELD_MANAGER.to_string(),
            )])),
            ..Default::default()
        },
        spec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        IssuerKind, IssuerRef, PostgresClusterSpec, PostgresVersion, StorageSpec, TLSSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn create_test_cluster(tls: TLSSpec) -> PostgresCluster {
        PostgresCluster {
            metadata: ObjectMeta {
                name: Some("test-cluster".to_string()),
                namespace: Some("test-ns".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: PostgresClusterSpec {
                version: PostgresVersion::V16,
                replicas: 3,
                storage: StorageSpec {
                    size: "10Gi".to_string(),
                    storage_class: None,
                },
                tls,
                resources: None,
                postgresql_params: BTreeMap::new(),
                labels: BTreeMap::new(),
                backup: None,
                restore: None,
                pgbouncer: None,
                metrics: None,
                service: None,
            },
            status: None,
        }
    }

    #[test]
    fn test_generate_certificate_with_issuer() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "letsencrypt-prod".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            additional_dns_names: vec!["custom.example.com".to_string()],
            duration: Some("2160h".to_string()),
            renew_before: Some("360h".to_string()),
        };

        let cluster = create_test_cluster(tls);
        let cert = generate_certificate(&cluster);

        assert!(cert.is_some());
        let cert = cert.unwrap();

        assert_eq!(cert.api_version, "cert-manager.io/v1");
        assert_eq!(cert.kind, "Certificate");
        assert_eq!(cert.metadata.name.as_deref(), Some("test-cluster-tls"));
        assert_eq!(cert.metadata.namespace.as_deref(), Some("test-ns"));

        assert_eq!(cert.spec.secret_name, "test-cluster-tls");
        assert_eq!(cert.spec.issuer_ref.name, "letsencrypt-prod");
        assert_eq!(cert.spec.issuer_ref.kind, "ClusterIssuer");

        // Check DNS names
        assert!(
            cert.spec
                .dns_names
                .contains(&"test-cluster-primary".to_string())
        );
        assert!(
            cert.spec
                .dns_names
                .contains(&"test-cluster-repl".to_string())
        );
        assert!(
            cert.spec
                .dns_names
                .contains(&"custom.example.com".to_string())
        );

        // Check duration settings
        assert_eq!(cert.spec.duration, Some("2160h".to_string()));
        assert_eq!(cert.spec.renew_before, Some("360h".to_string()));
    }

    #[test]
    fn test_generate_certificate_disabled() {
        let tls = TLSSpec {
            enabled: false,
            issuer_ref: Some(IssuerRef {
                name: "letsencrypt-prod".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            ..Default::default()
        };

        let cluster = create_test_cluster(tls);
        let cert = generate_certificate(&cluster);

        assert!(cert.is_none());
    }

    #[test]
    fn test_generate_certificate_no_issuer() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: None,
            ..Default::default()
        };

        let cluster = create_test_cluster(tls);
        let cert = generate_certificate(&cluster);

        assert!(cert.is_none());
    }

    #[test]
    fn test_certificate_has_owner_reference() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "test-issuer".to_string(),
                kind: IssuerKind::Issuer,
                group: "cert-manager.io".to_string(),
            }),
            ..Default::default()
        };

        let cluster = create_test_cluster(tls);
        let cert = generate_certificate(&cluster).unwrap();

        let owner_refs = cert.metadata.owner_references.as_ref().unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs[0].name, "test-cluster");
        assert_eq!(owner_refs[0].kind, "PostgresCluster");
    }

    #[test]
    fn test_certificate_dns_names() {
        let tls = TLSSpec {
            enabled: true,
            issuer_ref: Some(IssuerRef {
                name: "test-issuer".to_string(),
                kind: IssuerKind::ClusterIssuer,
                group: "cert-manager.io".to_string(),
            }),
            ..Default::default()
        };

        let cluster = create_test_cluster(tls);
        let cert = generate_certificate(&cluster).unwrap();

        // Should include all service DNS names
        let dns_names = &cert.spec.dns_names;

        // Primary service variations
        assert!(dns_names.contains(&"test-cluster-primary.test-ns.svc.cluster.local".to_string()));

        // Replica service variations
        assert!(dns_names.contains(&"test-cluster-repl.test-ns.svc.cluster.local".to_string()));

        // Headless service with wildcard for pods
        assert!(
            dns_names.contains(&"*.test-cluster-headless.test-ns.svc.cluster.local".to_string())
        );
    }
}
