//! Webhook HTTP server handlers
//!
//! Implements the ValidatingAdmissionWebhook HTTP endpoints for both
//! PostgresCluster and PostgresUpgrade resources.

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use k8s_openapi::api::core::v1::Namespace;
use kube::api::ListParams;
use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview, Operation};
use kube::{Api, Client, Resource, ResourceExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::policies::{
    UpgradeValidationContext, ValidationContext, validate_all, validate_no_concurrent_upgrade,
    validate_upgrade_sync,
};
use crate::crd::{ClusterPhase, PostgresCluster, PostgresUpgrade, UpgradePhase};

/// Shared state for webhook handlers
pub(crate) struct WebhookState {
    pub client: Client,
}

impl WebhookState {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Create the webhook router
pub(crate) fn create_webhook_router(state: Arc<WebhookState>) -> Router {
    Router::new()
        .route("/validate", post(validate_postgres_cluster))
        .route("/validate-upgrade", post(validate_postgres_upgrade))
        .with_state(state)
}

/// Validate PostgresCluster admission webhook handler
pub(crate) async fn validate_postgres_cluster(
    State(state): State<Arc<WebhookState>>,
    Json(review): Json<AdmissionReview<PostgresCluster>>,
) -> impl IntoResponse {
    let request: AdmissionRequest<PostgresCluster> = match review.try_into() {
        Ok(req) => req,
        Err(e) => {
            error!(error = %e, "Admission review missing or invalid request");
            return (
                StatusCode::BAD_REQUEST,
                Json(
                    AdmissionResponse::invalid(format!(
                        "Missing request in AdmissionReview: {}",
                        e
                    ))
                    .into_review(),
                ),
            );
        }
    };

    let uid = &request.uid;
    info!(
        uid = %uid,
        operation = ?request.operation,
        namespace = ?request.namespace,
        name = ?request.name,
        "Processing admission request"
    );

    // DELETE operations are always allowed
    if request.operation == Operation::Delete {
        info!(uid = %uid, "DELETE operation allowed");
        return (
            StatusCode::OK,
            Json(AdmissionResponse::from(&request).into_review()),
        );
    }

    // Get the new object (typed as PostgresCluster)
    let cluster: PostgresCluster = match &request.object {
        Some(obj) => obj.clone(),
        None => {
            return (
                StatusCode::OK,
                Json(deny_with_reason(
                    &request,
                    "Missing object in request",
                    "InvalidRequest",
                )),
            );
        }
    };

    // Get the old object for UPDATE operations (already typed)
    let old_cluster: Option<&PostgresCluster> = request.old_object.as_ref();

    // Get namespace labels for production policy
    let namespace_labels = match &request.namespace {
        Some(ns) => get_namespace_labels(&state.client, ns).await,
        None => BTreeMap::new(),
    };

    // Create validation context
    let ctx = ValidationContext::new(&cluster, old_cluster, namespace_labels);

    // Run all validations
    let result = validate_all(&ctx);

    if !result.allowed {
        let reason = result
            .reason
            .unwrap_or_else(|| "ValidationFailed".to_string());
        let message = result
            .message
            .unwrap_or_else(|| "Validation failed".to_string());
        warn!(uid = %uid, reason = %reason, message = %message, "Admission request denied");
        return (
            StatusCode::OK,
            Json(deny_with_reason(&request, &message, &reason)),
        );
    }

    // Check if cluster has an active upgrade in progress (block modifications during upgrade)
    if request.operation == Operation::Update {
        if let Some(ns) = &request.namespace
            && let Some((reason, message)) =
                check_cluster_upgrade_in_progress(&state.client, ns, &request.name).await
        {
            warn!(uid = %uid, reason = %reason, message = %message, "Admission request denied - upgrade in progress");
            return (
                StatusCode::OK,
                Json(deny_with_reason(&request, &message, &reason)),
            );
        }

        // Check if cluster has been superseded (permanently block modifications)
        if let Some((reason, message)) = check_cluster_superseded(&cluster) {
            warn!(uid = %uid, reason = %reason, message = %message, "Admission request denied - cluster superseded");
            return (
                StatusCode::OK,
                Json(deny_with_reason(&request, &message, &reason)),
            );
        }
    }

    info!(uid = %uid, "Admission request allowed");
    (
        StatusCode::OK,
        Json(AdmissionResponse::from(&request).into_review()),
    )
}

/// Get namespace labels for policy decisions
async fn get_namespace_labels(client: &Client, namespace: &str) -> BTreeMap<String, String> {
    let ns_api: Api<Namespace> = Api::all(client.clone());

    match ns_api.get(namespace).await {
        Ok(ns) => ns.metadata.labels.unwrap_or_default(),
        Err(e) => {
            warn!(namespace = %namespace, error = %e, "Failed to get namespace, using empty labels");
            BTreeMap::new()
        }
    }
}

/// Check if a cluster has an active upgrade in progress
///
/// Returns Some((reason, message)) if an upgrade is blocking modifications, None otherwise.
async fn check_cluster_upgrade_in_progress(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
) -> Option<(String, String)> {
    let upgrades: Api<PostgresUpgrade> = Api::namespaced(client.clone(), namespace);

    match upgrades.list(&ListParams::default()).await {
        Ok(list) => {
            for upgrade in list.items {
                // Check if this upgrade targets the cluster being modified
                if upgrade.spec.source_cluster.name == cluster_name {
                    let phase = upgrade
                        .status
                        .as_ref()
                        .map(|s| &s.phase)
                        .unwrap_or(&UpgradePhase::Pending);

                    // Block modifications if upgrade is active (not terminal)
                    if !phase.is_terminal() {
                        return Some((
                            "UpgradeInProgress".to_string(),
                            format!(
                                "Cluster '{}' has an active upgrade '{}' in phase {:?}. \
                                 Modifications are blocked during upgrades. Wait for the upgrade \
                                 to complete or delete it first.",
                                cluster_name,
                                upgrade.name_any(),
                                phase
                            ),
                        ));
                    }
                }
            }
            None
        }
        Err(e) => {
            // If we can't check, log but allow the operation
            // (fail-open to avoid blocking normal operations if CRD isn't installed)
            warn!(
                namespace = %namespace,
                cluster = %cluster_name,
                error = %e,
                "Failed to check for active upgrades, allowing operation"
            );
            None
        }
    }
}

/// Check if a cluster has been superseded by an upgrade.
/// Superseded clusters should not be modified - users should manage the successor instead.
///
/// Returns Some((reason, message)) if the cluster is superseded, None otherwise.
fn check_cluster_superseded(cluster: &PostgresCluster) -> Option<(String, String)> {
    if let Some(status) = &cluster.status
        && status.phase == ClusterPhase::Superseded
    {
        if let Some(successor) = &status.successor {
            let successor_display = if let Some(ns) = &successor.namespace {
                format!("{}/{}", ns, successor.name)
            } else {
                successor.name.clone()
            };

            return Some((
                "ClusterSuperseded".to_string(),
                format!(
                    "Cluster '{}' has been superseded by '{}' via upgrade{}. \
                     Modifications are blocked. To manage the upgraded cluster, \
                     modify '{}' instead.",
                    cluster.name_any(),
                    successor_display,
                    successor
                        .upgrade_name
                        .as_ref()
                        .map(|u| format!(" '{}'", u))
                        .unwrap_or_default(),
                    successor.name
                ),
            ));
        }
        // Superseded but no successor reference - shouldn't happen but handle gracefully
        return Some((
            "ClusterSuperseded".to_string(),
            format!(
                "Cluster '{}' is in Superseded phase. \
                 Modifications are blocked.",
                cluster.name_any()
            ),
        ));
    }
    None
}

/// Validate PostgresUpgrade admission webhook handler
pub(crate) async fn validate_postgres_upgrade(
    State(state): State<Arc<WebhookState>>,
    Json(review): Json<AdmissionReview<PostgresUpgrade>>,
) -> impl IntoResponse {
    let request: AdmissionRequest<PostgresUpgrade> = match review.try_into() {
        Ok(req) => req,
        Err(e) => {
            error!(error = %e, "Admission review missing or invalid request");
            return (
                StatusCode::BAD_REQUEST,
                Json(
                    AdmissionResponse::invalid(format!(
                        "Missing request in AdmissionReview: {}",
                        e
                    ))
                    .into_review(),
                ),
            );
        }
    };

    let uid = &request.uid;
    info!(
        uid = %uid,
        operation = ?request.operation,
        namespace = ?request.namespace,
        name = ?request.name,
        "Processing PostgresUpgrade admission request"
    );

    // DELETE operations are always allowed
    if request.operation == Operation::Delete {
        info!(uid = %uid, "DELETE operation allowed");
        return (
            StatusCode::OK,
            Json(AdmissionResponse::from(&request).into_review()),
        );
    }

    // Get the new object (typed as PostgresUpgrade)
    let upgrade: PostgresUpgrade = match &request.object {
        Some(obj) => obj.clone(),
        None => {
            return (
                StatusCode::OK,
                Json(deny_with_reason(
                    &request,
                    "Missing object in request",
                    "InvalidRequest",
                )),
            );
        }
    };

    // Get the old object for UPDATE operations (already typed)
    let old_upgrade: Option<&PostgresUpgrade> = request.old_object.as_ref();

    // Fetch the source cluster for validation
    let namespace = upgrade.namespace().unwrap_or_else(|| "default".to_string());
    let source_namespace = upgrade
        .spec
        .source_cluster
        .namespace
        .as_deref()
        .unwrap_or(&namespace);
    let source_cluster = get_source_cluster(
        &state.client,
        source_namespace,
        &upgrade.spec.source_cluster.name,
    )
    .await;

    // Create validation context
    let ctx = UpgradeValidationContext::new(&upgrade, old_upgrade, source_cluster.as_ref());

    // Run synchronous validations
    let result = validate_upgrade_sync(&ctx);

    if !result.allowed {
        let reason = result
            .reason
            .unwrap_or_else(|| "ValidationFailed".to_string());
        let message = result
            .message
            .unwrap_or_else(|| "Validation failed".to_string());
        warn!(uid = %uid, reason = %reason, message = %message, "PostgresUpgrade admission denied");
        return (
            StatusCode::OK,
            Json(deny_with_reason(&request, &message, &reason)),
        );
    }

    // Run async validation for concurrent upgrades (only on CREATE)
    if request.operation == Operation::Create {
        let concurrent_result =
            validate_no_concurrent_upgrade(&state.client, &upgrade, old_upgrade).await;

        if !concurrent_result.allowed {
            let reason = concurrent_result
                .reason
                .unwrap_or_else(|| "ValidationFailed".to_string());
            let message = concurrent_result
                .message
                .unwrap_or_else(|| "Validation failed".to_string());
            warn!(uid = %uid, reason = %reason, message = %message, "PostgresUpgrade admission denied - concurrent upgrade");
            return (
                StatusCode::OK,
                Json(deny_with_reason(&request, &message, &reason)),
            );
        }
    }

    info!(uid = %uid, "PostgresUpgrade admission request allowed");
    (
        StatusCode::OK,
        Json(AdmissionResponse::from(&request).into_review()),
    )
}

/// Fetch the source cluster for upgrade validation
async fn get_source_cluster(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Option<PostgresCluster> {
    let clusters: Api<PostgresCluster> = Api::namespaced(client.clone(), namespace);

    match clusters.get(name).await {
        Ok(cluster) => Some(cluster),
        Err(e) => {
            warn!(
                namespace = %namespace,
                name = %name,
                error = %e,
                "Failed to get source cluster for validation"
            );
            None
        }
    }
}

/// Create a denied AdmissionResponse with both reason and message
///
/// The kube-rs `AdmissionResponse::deny()` only takes a message, but Kubernetes
/// admission responses support both a machine-readable `reason` and a human-readable
/// `message`. This helper provides both for better observability by including
/// the reason in the message.
fn deny_with_reason<T: Resource<DynamicType = ()>>(
    request: &AdmissionRequest<T>,
    message: &str,
    reason: &str,
) -> AdmissionReview<kube::core::DynamicObject> {
    // Include reason in the message for visibility since kube-rs deny() doesn't have a separate reason field
    let full_message = format!("[{}] {}", reason, message);
    AdmissionResponse::from(request)
        .deny(full_message)
        .into_review()
}

/// Default path to webhook TLS certificate
pub const WEBHOOK_CERT_PATH: &str = "/etc/webhook/certs/tls.crt";
/// Default path to webhook TLS private key
pub const WEBHOOK_KEY_PATH: &str = "/etc/webhook/certs/tls.key";
/// Default webhook server port
pub const WEBHOOK_PORT: u16 = 8443;

/// Run the webhook server with TLS
///
/// Binds to 0.0.0.0:8443 and serves the /validate endpoint.
/// TLS certificates are loaded from the paths specified.
///
/// # Arguments
/// * `client` - Kubernetes client for looking up namespace labels
/// * `cert_path` - Path to TLS certificate file (PEM format)
/// * `key_path` - Path to TLS private key file (PEM format)
pub async fn run_webhook_server(
    client: Client,
    cert_path: &str,
    key_path: &str,
) -> Result<(), WebhookError> {
    use axum_server::tls_rustls::RustlsConfig;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    let state = Arc::new(WebhookState::new(client));
    let app = create_webhook_router(state);

    let config = RustlsConfig::from_pem_file(PathBuf::from(cert_path), PathBuf::from(key_path))
        .await
        .map_err(|e| WebhookError::TlsConfig(e.to_string()))?;

    let addr = SocketAddr::from(([0, 0, 0, 0], WEBHOOK_PORT));
    info!("Webhook server listening on {} with TLS", addr);

    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .map_err(|e| WebhookError::Server(e.to_string()))?;

    Ok(())
}

/// Errors that can occur when running the webhook server
#[derive(Debug)]
pub enum WebhookError {
    /// TLS configuration error
    TlsConfig(String),
    /// Server error
    Server(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::TlsConfig(msg) => write!(f, "TLS configuration error: {}", msg),
            WebhookError::Server(msg) => write!(f, "Webhook server error: {}", msg),
        }
    }
}

impl std::error::Error for WebhookError {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use kube::core::admission::Operation;
    use serde_json::json;

    /// Helper to create a minimal AdmissionRequest for testing
    fn create_test_request() -> AdmissionRequest<PostgresCluster> {
        let review_json = json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "test-uid",
                "kind": {
                    "group": "postgres-operator.smoketurner.com",
                    "version": "v1alpha1",
                    "kind": "PostgresCluster"
                },
                "resource": {
                    "group": "postgres-operator.smoketurner.com",
                    "version": "v1alpha1",
                    "resource": "postgresclusters"
                },
                "requestKind": {
                    "group": "postgres-operator.smoketurner.com",
                    "version": "v1alpha1",
                    "kind": "PostgresCluster"
                },
                "requestResource": {
                    "group": "postgres-operator.smoketurner.com",
                    "version": "v1alpha1",
                    "resource": "postgresclusters"
                },
                "operation": "CREATE",
                "namespace": "default",
                "name": "test-cluster",
                "object": null,
                "oldObject": null,
                "dryRun": false,
                "userInfo": {
                    "username": "test-user"
                }
            }
        });

        let review: AdmissionReview<PostgresCluster> = serde_json::from_value(review_json).unwrap();
        review.try_into().unwrap()
    }

    #[test]
    fn test_allowed_response() {
        let request = create_test_request();
        let response = AdmissionResponse::from(&request);

        assert_eq!(response.uid, "test-uid");
        assert!(response.allowed);
    }

    #[test]
    fn test_denied_response_with_reason() {
        let request = create_test_request();
        let review = deny_with_reason(&request, "Test error", "TestReason");

        // Verify the JSON serialization has correct structure
        let json = serde_json::to_value(&review).unwrap();

        assert_eq!(json["response"]["uid"].as_str(), Some("test-uid"));
        assert_eq!(json["response"]["allowed"].as_bool(), Some(false));
        // kube-rs uses "status" for the result field in JSON
        // The status field structure depends on kube-rs serialization
        assert!(
            json["response"]["status"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("TestReason") && m.contains("Test error")),
            "Message should contain reason and error text"
        );
    }

    #[test]
    fn test_operation_enum_comparison() {
        // Verify operation enum works as expected
        assert_eq!(Operation::Create, Operation::Create);
        assert_eq!(Operation::Update, Operation::Update);
        assert_eq!(Operation::Delete, Operation::Delete);
        assert_ne!(Operation::Create, Operation::Delete);
    }
}
