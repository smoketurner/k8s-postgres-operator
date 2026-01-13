//! Webhook HTTP server handlers
//!
//! Implements the ValidatingAdmissionWebhook HTTP endpoint.

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use k8s_openapi::api::core::v1::Namespace;
use kube::{Api, Client};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::policies::{ValidationContext, validate_all};
use crate::crd::PostgresCluster;

/// Kubernetes AdmissionReview request
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReview {
    pub api_version: String,
    pub kind: String,
    pub request: Option<AdmissionRequest>,
}

/// AdmissionRequest contains the details of the admission request
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionRequest {
    pub uid: String,
    pub kind: GroupVersionKind,
    pub resource: GroupVersionResource,
    pub operation: String,
    pub namespace: Option<String>,
    pub name: Option<String>,
    pub object: Option<serde_json::Value>,
    pub old_object: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionKind {
    pub group: String,
    pub version: String,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupVersionResource {
    pub group: String,
    pub version: String,
    pub resource: String,
}

/// AdmissionReview response
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionReviewResponse {
    pub api_version: String,
    pub kind: String,
    pub response: AdmissionResponse,
}

/// AdmissionResponse contains the result
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AdmissionStatus>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmissionStatus {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

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
        .with_state(state)
}

/// Validate PostgresCluster admission webhook handler
pub(crate) async fn validate_postgres_cluster(
    State(state): State<Arc<WebhookState>>,
    Json(review): Json<AdmissionReview>,
) -> impl IntoResponse {
    let request = match review.request {
        Some(req) => req,
        None => {
            error!("Admission review missing request");
            return (
                StatusCode::BAD_REQUEST,
                Json(create_response(
                    "",
                    false,
                    "Missing request in AdmissionReview",
                    None,
                )),
            );
        }
    };

    let uid = request.uid.clone();
    info!(
        uid = %uid,
        operation = %request.operation,
        namespace = ?request.namespace,
        name = ?request.name,
        "Processing admission request"
    );

    // Parse the new object
    let cluster: PostgresCluster = match request.object {
        Some(obj) => match serde_json::from_value(obj) {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to parse PostgresCluster");
                return (
                    StatusCode::OK,
                    Json(create_response(
                        &uid,
                        false,
                        &format!("Failed to parse object: {}", e),
                        None,
                    )),
                );
            }
        },
        None => {
            // DELETE operations may not have object
            if request.operation == "DELETE" {
                return (StatusCode::OK, Json(create_response(&uid, true, "", None)));
            }
            return (
                StatusCode::OK,
                Json(create_response(
                    &uid,
                    false,
                    "Missing object in request",
                    None,
                )),
            );
        }
    };

    // Parse the old object for UPDATE operations
    let old_cluster: Option<PostgresCluster> = match &request.old_object {
        Some(obj) => match serde_json::from_value(obj.clone()) {
            Ok(c) => Some(c),
            Err(e) => {
                warn!(error = %e, "Failed to parse old PostgresCluster, treating as CREATE");
                None
            }
        },
        None => None,
    };

    // Get namespace labels for production policy
    let namespace_labels = match &request.namespace {
        Some(ns) => get_namespace_labels(&state.client, ns).await,
        None => BTreeMap::new(),
    };

    // Create validation context
    let ctx = ValidationContext::new(&cluster, old_cluster.as_ref(), namespace_labels);

    // Run all validations
    let result = validate_all(&ctx);

    if result.allowed {
        info!(uid = %uid, "Admission request allowed");
        (StatusCode::OK, Json(create_response(&uid, true, "", None)))
    } else {
        let reason = result
            .reason
            .unwrap_or_else(|| "ValidationFailed".to_string());
        let message = result
            .message
            .unwrap_or_else(|| "Validation failed".to_string());
        warn!(uid = %uid, reason = %reason, message = %message, "Admission request denied");
        (
            StatusCode::OK,
            Json(create_response(&uid, false, &message, Some(&reason))),
        )
    }
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

/// Create an AdmissionReview response
fn create_response(
    uid: &str,
    allowed: bool,
    message: &str,
    reason: Option<&str>,
) -> AdmissionReviewResponse {
    AdmissionReviewResponse {
        api_version: "admission.k8s.io/v1".to_string(),
        kind: "AdmissionReview".to_string(),
        response: AdmissionResponse {
            uid: uid.to_string(),
            allowed,
            status: if allowed {
                None
            } else {
                Some(AdmissionStatus {
                    code: 403,
                    message: message.to_string(),
                    reason: reason.map(String::from),
                })
            },
        },
    }
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

    #[test]
    fn test_create_allowed_response() {
        let resp = create_response("test-uid", true, "", None);
        assert_eq!(resp.response.uid, "test-uid");
        assert!(resp.response.allowed);
        assert!(resp.response.status.is_none());
    }

    #[test]
    fn test_create_denied_response() {
        let resp = create_response("test-uid", false, "Test error", Some("TestReason"));
        assert_eq!(resp.response.uid, "test-uid");
        assert!(!resp.response.allowed);
        let status = resp.response.status.unwrap();
        assert_eq!(status.code, 403);
        assert_eq!(status.message, "Test error");
        assert_eq!(status.reason, Some("TestReason".to_string()));
    }
}
