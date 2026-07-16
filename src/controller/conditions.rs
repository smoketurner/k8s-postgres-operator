//! Shared helpers for Kubernetes-style status conditions.
//!
//! Mirrors the semantics of `apimachinery/pkg/api/meta.SetStatusCondition`
//! used by upstream controllers (and by reference operators like
//! valkey-io/valkey-operator). All CRDs in this operator store conditions as
//! `k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition` and use
//! [`set_status_condition`] to merge them, so the field semantics
//! (`lastTransitionTime` only advances on status change, `observedGeneration`
//! is carried through, no duplicate types) match what kubectl and dashboards
//! expect.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};

/// Condition status values, named to match the upstream `metav1.ConditionStatus` constants.
pub mod status {
    pub const TRUE: &str = "True";
    pub const FALSE: &str = "False";
    pub const UNKNOWN: &str = "Unknown";
}

/// Construct a [`Condition`] with `lastTransitionTime` set to now.
///
/// Use this when seeding a brand-new condition. To merge a condition into an
/// existing list (with dedup and transition-time preservation), call
/// [`set_status_condition`] instead.
pub fn new_condition(
    type_: &str,
    status: &str,
    reason: &str,
    message: &str,
    observed_generation: Option<i64>,
) -> Condition {
    Condition {
        type_: type_.to_string(),
        status: status.to_string(),
        reason: reason.to_string(),
        message: message.to_string(),
        last_transition_time: Time(jiff::Timestamp::now()),
        observed_generation,
    }
}

/// Merge a condition into a list, mirroring `meta.SetStatusCondition`.
///
/// * If no condition of the same `type_` exists, the new condition is pushed.
/// * If one exists and the `status` differs, the existing condition is
///   replaced with the new one (its `lastTransitionTime` is taken from `new`).
/// * If one exists and the `status` matches, the reason/message/observed
///   generation are updated in place but `lastTransitionTime` is preserved.
pub fn set_status_condition(conditions: &mut Vec<Condition>, new: Condition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == new.type_) {
        if existing.status == new.status {
            existing.reason = new.reason;
            existing.message = new.message;
            existing.observed_generation = new.observed_generation;
        } else {
            *existing = new;
        }
    } else {
        conditions.push(new);
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

    fn cond(type_: &str, status: &str, ts: &str, generation: Option<i64>) -> Condition {
        Condition {
            type_: type_.to_string(),
            status: status.to_string(),
            reason: "Seed".to_string(),
            message: String::new(),
            last_transition_time: Time(ts.parse().unwrap()),
            observed_generation: generation,
        }
    }

    #[test]
    fn inserts_when_absent() {
        let mut conditions = Vec::new();
        set_status_condition(
            &mut conditions,
            new_condition("Ready", status::TRUE, "Ok", "all good", Some(3)),
        );
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].type_, "Ready");
        assert_eq!(conditions[0].observed_generation, Some(3));
    }

    #[test]
    fn preserves_transition_time_when_status_unchanged() {
        let mut conditions = vec![cond("Ready", status::TRUE, "2024-01-01T00:00:00Z", Some(1))];
        let original_ts = conditions[0].last_transition_time.clone();

        set_status_condition(
            &mut conditions,
            new_condition("Ready", status::TRUE, "NewReason", "new message", Some(2)),
        );

        assert_eq!(conditions[0].last_transition_time, original_ts);
        assert_eq!(conditions[0].reason, "NewReason");
        assert_eq!(conditions[0].message, "new message");
        assert_eq!(conditions[0].observed_generation, Some(2));
    }

    #[test]
    fn advances_transition_time_when_status_changes() {
        let mut conditions = vec![cond("Ready", status::TRUE, "2024-01-01T00:00:00Z", Some(1))];
        let original_ts = conditions[0].last_transition_time.clone();

        set_status_condition(
            &mut conditions,
            new_condition("Ready", status::FALSE, "Down", "lost primary", Some(2)),
        );

        assert_eq!(conditions[0].status, status::FALSE);
        assert_ne!(conditions[0].last_transition_time, original_ts);
    }
}
