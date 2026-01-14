//! Cleanup utilities for handling orphaned Kubernetes resources
//!
//! This module provides utilities for cleaning up resources that become stuck
//! when their namespace is deleted while they still have finalizers.

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{Api, DeleteParams, ObjectMeta, Patch, PatchParams, PostParams};
use kube::{Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use tracing::{debug, info, warn};

/// Check if a kube error indicates the namespace was not found.
///
/// This is used to detect when a resource's namespace has been deleted,
/// leaving the resource in an orphaned state where it can be read but not modified.
pub fn is_namespace_not_found_error(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(resp) if resp.code == 404 && resp.message.contains("namespace"))
}

/// Clean up a stuck resource whose namespace has been deleted.
///
/// This handles an edge case where a namespace is deleted while resources with
/// finalizers still exist. The resources become orphaned - readable but not
/// modifiable through normal API calls.
///
/// The cleanup process:
/// 1. Temporarily recreates the namespace
/// 2. Removes the finalizer from the stuck resource
/// 3. Deletes the namespace again to allow Kubernetes garbage collection
///
/// # Type Parameters
///
/// * `K` - The Kubernetes resource type (must implement Resource, Clone, DeserializeOwned, Serialize, Debug)
///
/// # Arguments
///
/// * `client` - Kubernetes client
/// * `resource_name` - Name of the stuck resource
/// * `ns` - Namespace the resource belongs to
///
/// # Example
///
/// ```ignore
/// use postgres_operator::controller::cleanup::cleanup_stuck_resource;
/// use postgres_operator::crd::PostgresCluster;
///
/// if is_namespace_not_found_error(&error) {
///     cleanup_stuck_resource::<PostgresCluster>(client, "my-cluster", "my-namespace").await?;
/// }
/// ```
pub async fn cleanup_stuck_resource<K>(
    client: Client,
    resource_name: &str,
    ns: &str,
) -> Result<(), kube::Error>
where
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>
        + Clone
        + DeserializeOwned
        + Serialize
        + Debug,
    <K as Resource>::DynamicType: Default,
{
    let dt = K::DynamicType::default();
    let kind = K::kind(&dt);
    info!(
        "Attempting to clean up stuck {} {}/{} in deleted namespace",
        kind, ns, resource_name
    );

    // Create a minimal namespace to allow the patch operation
    let ns_api: Api<Namespace> = Api::all(client.clone());
    let temp_ns = Namespace {
        metadata: ObjectMeta {
            name: Some(ns.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };

    // Try to create the namespace - it may already exist in terminating state
    match ns_api.create(&PostParams::default(), &temp_ns).await {
        Ok(_) => {
            info!("Temporarily recreated namespace {} for cleanup", ns);
        }
        Err(kube::Error::Api(resp)) if resp.code == 409 => {
            // Namespace already exists (possibly in Terminating state), that's ok
            debug!("Namespace {} already exists, proceeding with cleanup", ns);
        }
        Err(e) => {
            warn!("Failed to recreate namespace {} for cleanup: {}", ns, e);
            return Err(e);
        }
    }

    // Now remove the finalizer from the stuck resource
    let api: Api<K> = Api::namespaced(client.clone(), ns);
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": null
        }
    });

    if let Err(e) = api
        .patch(
            resource_name,
            &PatchParams::apply("postgres-operator"),
            &Patch::Merge(&patch),
        )
        .await
    {
        warn!(
            "Failed to remove finalizer from stuck {} {}/{}: {}",
            kind, ns, resource_name, e
        );
        // Don't return error - the namespace recreation may help on next reconcile
    } else {
        info!(
            "Removed finalizer from stuck {} {}/{}",
            kind, ns, resource_name
        );
    }

    // Delete the temporary namespace (non-blocking)
    if let Err(e) = ns_api.delete(ns, &DeleteParams::default()).await {
        // This is expected if namespace was already terminating
        debug!("Namespace {} deletion: {}", ns, e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_namespace_not_found_error_true() {
        let error = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "namespaces \"test-ns\" not found".to_string(),
            reason: "NotFound".to_string(),
            code: 404,
        });
        assert!(is_namespace_not_found_error(&error));
    }

    #[test]
    fn test_is_namespace_not_found_error_false_wrong_code() {
        let error = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "namespaces \"test-ns\" not found".to_string(),
            reason: "Forbidden".to_string(),
            code: 403,
        });
        assert!(!is_namespace_not_found_error(&error));
    }

    #[test]
    fn test_is_namespace_not_found_error_false_wrong_message() {
        let error = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "resource not found".to_string(),
            reason: "NotFound".to_string(),
            code: 404,
        });
        assert!(!is_namespace_not_found_error(&error));
    }
}
