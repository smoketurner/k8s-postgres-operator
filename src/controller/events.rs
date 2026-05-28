//! Shared helper for emitting Kubernetes Events from any controller in
//! this operator.
//!
//! Each controller's context holds a [`Reporter`] identifying the operator
//! and (optionally) the pod instance. [`publish_event`] is generic over the
//! owning resource type so the same call site shape works for
//! `PostgresCluster`, `PostgresDatabase`, and `PostgresUpgrade`.

use kube::Client;
use kube::Resource;
use kube::runtime::events::{Event, EventType, Recorder, Reporter};

/// Default operator identity used by all three controllers when constructing a
/// [`Reporter`]. The `instance` field is populated from `POD_NAME` at runtime.
pub const FIELD_MANAGER: &str = "postgres-operator";

/// Build a [`Reporter`] identifying this operator instance.
pub fn reporter() -> Reporter {
    Reporter {
        controller: FIELD_MANAGER.into(),
        instance: std::env::var("POD_NAME").ok(),
    }
}

/// Publish a Kubernetes Event attached to `obj`. Logs (does not propagate)
/// errors — event publication is best-effort.
pub async fn publish_event<R>(
    client: &Client,
    reporter: &Reporter,
    obj: &R,
    type_: EventType,
    reason: &str,
    action: &str,
    note: Option<String>,
) where
    R: Resource<DynamicType = ()>,
{
    let recorder = Recorder::new(client.clone(), reporter.clone());
    let object_ref = obj.object_ref(&());
    if let Err(e) = recorder
        .publish(
            &Event {
                type_,
                reason: reason.into(),
                note,
                action: action.into(),
                secondary: None,
            },
            &object_ref,
        )
        .await
    {
        tracing::warn!("Failed to publish event: {}", e);
    }
}
