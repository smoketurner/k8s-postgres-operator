//! Shared helpers for adding and removing the operator's finalizer without
//! disturbing Kubernetes system finalizers.
//!
//! Earlier versions of the reconcilers cleared the entire `metadata.finalizers`
//! array by patching it to `null`. Under JSON Merge Patch semantics, a `null`
//! value removes the field entirely, which also wipes system finalizers such
//! as `foregroundDeletion` and `orphan`. Removing those finalizers prevents
//! Kubernetes garbage collection from observing the cascade mode the user
//! requested via `kubectl delete --cascade=foreground|orphan`.
//!
//! The helpers in this module filter the operator's finalizer out of the
//! existing list and patch the remainder. JSON Merge Patch replaces array
//! fields wholesale, so the resulting patch always contains the full set of
//! finalizers that should remain on the object. An empty array is a valid,
//! distinct value from `null`: it explicitly removes the operator finalizer
//! without disturbing anything that was not already present.

use std::fmt::Debug;

use kube::Resource;
use kube::api::{Api, Patch, PatchParams};
use serde::de::DeserializeOwned;
use tracing::info;

/// Return a new vector containing every finalizer except `finalizer_to_remove`.
///
/// Pure helper so the filtering logic can be unit tested without a Kubernetes
/// client.
#[must_use]
pub fn filter_finalizers(current: &[String], finalizer_to_remove: &str) -> Vec<String> {
    current
        .iter()
        .filter(|f| f.as_str() != finalizer_to_remove)
        .cloned()
        .collect()
}

/// Return a new vector containing every existing finalizer plus
/// `finalizer_to_add` appended, or `None` when it is already present.
///
/// Pure helper so the append logic can be unit tested without a Kubernetes
/// client.
#[must_use]
pub fn append_finalizer(current: &[String], finalizer_to_add: &str) -> Option<Vec<String>> {
    if current.iter().any(|f| f == finalizer_to_add) {
        return None;
    }
    let mut finalizers = current.to_vec();
    finalizers.push(finalizer_to_add.to_string());
    Some(finalizers)
}

/// Add the operator's finalizer to a Kubernetes resource while preserving any
/// other finalizers (user-added, system finalizers like `foregroundDeletion`,
/// or those owned by other controllers).
///
/// Returns `Ok(())` without contacting the API server when the finalizer is
/// already present.
///
/// The patch is sent with JSON Merge Patch semantics. Because Merge Patch
/// replaces array fields wholesale, the full list (existing finalizers plus
/// ours) is written back — patching `[finalizer]` alone would silently clobber
/// every other finalizer on the object.
///
/// # Errors
///
/// Returns any error from `Api::patch`.
pub async fn add_operator_finalizer<K>(
    api: &Api<K>,
    name: &str,
    current_finalizers: Option<&Vec<String>>,
    finalizer_to_add: &str,
) -> Result<(), kube::Error>
where
    K: Resource + Clone + DeserializeOwned + Debug,
{
    let current = current_finalizers.map(Vec::as_slice).unwrap_or_default();
    let Some(finalizers) = append_finalizer(current, finalizer_to_add) else {
        return Ok(());
    };

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": finalizers
        }
    });

    api.patch(
        name,
        &PatchParams::apply("postgres-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!(
        "Added finalizer {} to {} (full set: {:?})",
        finalizer_to_add, name, finalizers
    );
    Ok(())
}

/// Remove the operator's finalizer from a Kubernetes resource while preserving
/// any other finalizers (including system finalizers like `foregroundDeletion`
/// or `orphan`).
///
/// Returns `Ok(())` without contacting the API server when the operator
/// finalizer is not present.
///
/// The patch is sent with JSON Merge Patch semantics. Because Merge Patch
/// replaces array fields wholesale, the entire filtered list is written back.
/// An empty array is a legitimate value and means "the operator was the only
/// finalizer"; it is distinct from `null`, which would remove the field and
/// any system finalizers along with it.
///
/// # Errors
///
/// Returns any error from `Api::patch`.
pub async fn remove_operator_finalizer<K>(
    api: &Api<K>,
    name: &str,
    current_finalizers: Option<&Vec<String>>,
    finalizer_to_remove: &str,
) -> Result<(), kube::Error>
where
    K: Resource + Clone + DeserializeOwned + Debug,
{
    let Some(current) = current_finalizers else {
        return Ok(());
    };

    if !current.iter().any(|f| f == finalizer_to_remove) {
        return Ok(());
    }

    let remaining = filter_finalizers(current, finalizer_to_remove);

    let patch = serde_json::json!({
        "metadata": {
            "finalizers": remaining
        }
    });

    api.patch(
        name,
        &PatchParams::apply("postgres-operator"),
        &Patch::Merge(&patch),
    )
    .await?;

    info!(
        "Removed finalizer {} from {} (remaining: {:?})",
        finalizer_to_remove, name, remaining
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPERATOR: &str = "postgres-operator.smoketurner.com/finalizer";
    const FOREGROUND: &str = "foregroundDeletion";
    const ORPHAN: &str = "orphan";
    const CUSTOM: &str = "example.com/custom";

    #[test]
    fn filter_only_operator_finalizer_yields_empty() {
        let current = vec![OPERATOR.to_string()];
        let filtered = filter_finalizers(&current, OPERATOR);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_preserves_foreground_deletion() {
        let current = vec![OPERATOR.to_string(), FOREGROUND.to_string()];
        let filtered = filter_finalizers(&current, OPERATOR);
        assert_eq!(filtered, vec![FOREGROUND.to_string()]);
    }

    #[test]
    fn filter_preserves_orphan_and_custom() {
        let current = vec![ORPHAN.to_string(), OPERATOR.to_string(), CUSTOM.to_string()];
        let filtered = filter_finalizers(&current, OPERATOR);
        assert_eq!(filtered, vec![ORPHAN.to_string(), CUSTOM.to_string()]);
    }

    #[test]
    fn filter_returns_unchanged_when_operator_absent() {
        let current = vec![FOREGROUND.to_string(), CUSTOM.to_string()];
        let filtered = filter_finalizers(&current, OPERATOR);
        assert_eq!(filtered, current);
    }

    #[test]
    fn filter_empty_input_yields_empty() {
        let current: Vec<String> = Vec::new();
        let filtered = filter_finalizers(&current, OPERATOR);
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_removes_all_duplicate_occurrences() {
        let current = vec![
            OPERATOR.to_string(),
            FOREGROUND.to_string(),
            OPERATOR.to_string(),
        ];
        let filtered = filter_finalizers(&current, OPERATOR);
        assert_eq!(filtered, vec![FOREGROUND.to_string()]);
    }

    #[test]
    fn append_to_empty_yields_single_finalizer() {
        let current: Vec<String> = Vec::new();
        let appended = append_finalizer(&current, OPERATOR);
        assert_eq!(appended, Some(vec![OPERATOR.to_string()]));
    }

    #[test]
    fn append_preserves_existing_finalizers() {
        let current = vec![FOREGROUND.to_string(), CUSTOM.to_string()];
        let appended = append_finalizer(&current, OPERATOR);
        assert_eq!(
            appended,
            Some(vec![
                FOREGROUND.to_string(),
                CUSTOM.to_string(),
                OPERATOR.to_string(),
            ])
        );
    }

    #[test]
    fn append_is_noop_when_already_present() {
        let current = vec![FOREGROUND.to_string(), OPERATOR.to_string()];
        assert_eq!(append_finalizer(&current, OPERATOR), None);
    }
}
