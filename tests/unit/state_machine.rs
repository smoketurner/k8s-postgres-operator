//! Additional unit tests for state machine transitions

use postgres_operator::controller::state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult, determine_event,
};
use postgres_operator::crd::ClusterPhase;

mod transition_context_tests {
    use super::*;

    #[test]
    fn test_new_context() {
        let ctx = TransitionContext::new(2, 3);
        assert_eq!(ctx.ready_replicas, 2);
        assert_eq!(ctx.desired_replicas, 3);
        assert!(!ctx.spec_changed);
        assert!(ctx.error_message.is_none());
        assert_eq!(ctx.retry_count, 0);
    }

    #[test]
    fn test_all_replicas_ready_true() {
        let ctx = TransitionContext::new(3, 3);
        assert!(ctx.all_replicas_ready());
    }

    #[test]
    fn test_all_replicas_ready_more_than_desired() {
        // Edge case: more ready than desired (shouldn't happen but should handle)
        let ctx = TransitionContext::new(4, 3);
        assert!(ctx.all_replicas_ready());
    }

    #[test]
    fn test_all_replicas_ready_false() {
        let ctx = TransitionContext::new(2, 3);
        assert!(!ctx.all_replicas_ready());
    }

    #[test]
    fn test_is_degraded_true() {
        let ctx = TransitionContext::new(2, 3);
        assert!(ctx.is_degraded());
    }

    #[test]
    fn test_is_degraded_false_all_ready() {
        let ctx = TransitionContext::new(3, 3);
        assert!(!ctx.is_degraded());
    }

    #[test]
    fn test_is_degraded_false_none_ready() {
        let ctx = TransitionContext::new(0, 3);
        assert!(!ctx.is_degraded());
    }

    #[test]
    fn test_no_replicas_ready_true() {
        let ctx = TransitionContext::new(0, 3);
        assert!(ctx.no_replicas_ready());
    }

    #[test]
    fn test_no_replicas_ready_false() {
        let ctx = TransitionContext::new(1, 3);
        assert!(!ctx.no_replicas_ready());
    }

    #[test]
    fn test_single_replica_edge_cases() {
        // Single replica cluster
        let ctx = TransitionContext::new(1, 1);
        assert!(ctx.all_replicas_ready());
        assert!(!ctx.is_degraded());
        assert!(!ctx.no_replicas_ready());
    }

    // Kubernetes 1.35+ pod sync and resize tests (KEP-1287, KEP-5067)

    #[test]
    fn test_new_context_has_k8s_135_defaults() {
        let ctx = TransitionContext::new(3, 3);
        assert_eq!(ctx.synced_pods, 0);
        assert_eq!(ctx.total_pods, 0);
        assert!(!ctx.resize_in_progress);
    }

    #[test]
    fn test_all_pods_synced_when_no_pods() {
        let ctx = TransitionContext::new(3, 3);
        // When total_pods is 0, all_pods_synced should return true
        assert!(ctx.all_pods_synced());
    }

    #[test]
    fn test_all_pods_synced_true() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.synced_pods = 3;
        ctx.total_pods = 3;
        assert!(ctx.all_pods_synced());
    }

    #[test]
    fn test_all_pods_synced_false() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.synced_pods = 2;
        ctx.total_pods = 3;
        assert!(!ctx.all_pods_synced());
    }

    #[test]
    fn test_all_pods_synced_more_synced_than_total() {
        // Edge case: more synced than total (shouldn't happen but should handle)
        let mut ctx = TransitionContext::new(3, 3);
        ctx.synced_pods = 4;
        ctx.total_pods = 3;
        assert!(ctx.all_pods_synced());
    }

    #[test]
    fn test_is_resizing_false() {
        let ctx = TransitionContext::new(3, 3);
        assert!(!ctx.is_resizing());
    }

    #[test]
    fn test_is_resizing_true() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.resize_in_progress = true;
        assert!(ctx.is_resizing());
    }

    #[test]
    fn test_ready_for_running_all_conditions_met() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.synced_pods = 3;
        ctx.total_pods = 3;
        ctx.resize_in_progress = false;
        assert!(ctx.ready_for_running());
    }

    #[test]
    fn test_ready_for_running_replicas_not_ready() {
        let mut ctx = TransitionContext::new(2, 3);
        ctx.synced_pods = 3;
        ctx.total_pods = 3;
        ctx.resize_in_progress = false;
        assert!(!ctx.ready_for_running());
    }

    #[test]
    fn test_ready_for_running_pods_not_synced() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.synced_pods = 2;
        ctx.total_pods = 3;
        ctx.resize_in_progress = false;
        assert!(!ctx.ready_for_running());
    }

    #[test]
    fn test_ready_for_running_resize_in_progress() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.synced_pods = 3;
        ctx.total_pods = 3;
        ctx.resize_in_progress = true;
        assert!(!ctx.ready_for_running());
    }

    #[test]
    fn test_ready_for_running_multiple_failures() {
        let mut ctx = TransitionContext::new(2, 3);
        ctx.synced_pods = 1;
        ctx.total_pods = 3;
        ctx.resize_in_progress = true;
        // All three conditions fail
        assert!(!ctx.ready_for_running());
    }

    #[test]
    fn test_context_with_full_k8s_135_state() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.spec_changed = true;
        ctx.error_message = None;
        ctx.retry_count = 0;
        ctx.synced_pods = 3;
        ctx.total_pods = 3;
        ctx.resize_in_progress = false;

        // Verify all methods work together
        assert!(ctx.all_replicas_ready());
        assert!(!ctx.is_degraded());
        assert!(!ctx.no_replicas_ready());
        assert!(ctx.all_pods_synced());
        assert!(!ctx.is_resizing());
        assert!(ctx.ready_for_running());
    }
}

mod updating_transitions {
    use super::*;

    #[test]
    fn test_updating_to_running() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(
            &ClusterPhase::Updating,
            ClusterEvent::AllReplicasReady,
            &ctx,
        );

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Updating);
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_updating_to_degraded() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(2, 3);

        let result = sm.transition(
            &ClusterPhase::Updating,
            ClusterEvent::ReplicasDegraded,
            &ctx,
        );

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Updating);
                assert_eq!(to, ClusterPhase::Degraded);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_updating_to_failed() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(1, 3);

        let result = sm.transition(&ClusterPhase::Updating, ClusterEvent::ReconcileError, &ctx);

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Updating);
                assert_eq!(to, ClusterPhase::Failed);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_updating_to_deleting() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(2, 3);

        let result = sm.transition(
            &ClusterPhase::Updating,
            ClusterEvent::DeletionRequested,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Deleting);
            }
            _ => panic!("Expected successful transition"),
        }
    }
}

mod scaling_transitions {
    use super::*;

    #[test]
    fn test_scaling_to_running() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(5, 5);

        let result = sm.transition(&ClusterPhase::Scaling, ClusterEvent::AllReplicasReady, &ctx);

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Scaling);
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_scaling_to_degraded() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 5);

        let result = sm.transition(&ClusterPhase::Scaling, ClusterEvent::ReplicasDegraded, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Degraded);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_scaling_to_failed() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(0, 5);

        let result = sm.transition(&ClusterPhase::Scaling, ClusterEvent::ReconcileError, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Failed);
            }
            _ => panic!("Expected successful transition"),
        }
    }
}

mod degraded_transitions {
    use super::*;

    #[test]
    fn test_degraded_to_running_all_ready() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(
            &ClusterPhase::Degraded,
            ClusterEvent::AllReplicasReady,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_degraded_to_running_fully_recovered() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::FullyRecovered, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_degraded_to_running_guard_fails() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(2, 3);

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::FullyRecovered, &ctx);

        assert!(matches!(result, TransitionResult::GuardFailed { .. }));
    }

    #[test]
    fn test_degraded_to_updating() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(2, 3);

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::SpecChanged, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Updating);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_degraded_to_failed() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(1, 3);

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::ReconcileError, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Failed);
            }
            _ => panic!("Expected successful transition"),
        }
    }
}

mod recovery_transitions {
    use super::*;

    #[test]
    fn test_failed_to_recovering() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(&ClusterPhase::Failed, ClusterEvent::RecoveryInitiated, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Recovering);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_recovering_to_running_completed() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(
            &ClusterPhase::Recovering,
            ClusterEvent::RecoveryCompleted,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_recovering_to_running_all_ready() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(
            &ClusterPhase::Recovering,
            ClusterEvent::AllReplicasReady,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_recovering_to_degraded() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(2, 3);

        let result = sm.transition(
            &ClusterPhase::Recovering,
            ClusterEvent::ReplicasDegraded,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Degraded);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_recovering_to_failed() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(0, 3);

        let result = sm.transition(
            &ClusterPhase::Recovering,
            ClusterEvent::ReconcileError,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Failed);
            }
            _ => panic!("Expected successful transition"),
        }
    }
}

mod determine_event_tests {
    use super::*;

    #[test]
    fn test_determine_event_spec_changed_from_running() {
        let mut ctx = TransitionContext::new(3, 3);
        ctx.spec_changed = true;
        let event = determine_event(&ClusterPhase::Running, &ctx, false, None);
        assert_eq!(event, ClusterEvent::SpecChanged);
    }

    #[test]
    fn test_determine_event_spec_changed_from_degraded() {
        let mut ctx = TransitionContext::new(2, 3);
        ctx.spec_changed = true;
        let event = determine_event(&ClusterPhase::Degraded, &ctx, false, None);
        assert_eq!(event, ClusterEvent::SpecChanged);
    }

    #[test]
    fn test_determine_event_all_replicas_ready_from_creating() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&ClusterPhase::Creating, &ctx, false, None);
        assert_eq!(event, ClusterEvent::AllReplicasReady);
    }

    #[test]
    fn test_determine_event_degraded_from_running() {
        let ctx = TransitionContext::new(2, 3);
        let event = determine_event(&ClusterPhase::Running, &ctx, false, None);
        assert_eq!(event, ClusterEvent::ReplicasDegraded);
    }

    #[test]
    fn test_determine_event_recovery_initiated_from_failed() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&ClusterPhase::Failed, &ctx, false, None);
        assert_eq!(event, ClusterEvent::RecoveryInitiated);
    }

    #[test]
    fn test_determine_event_recovery_completed() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&ClusterPhase::Recovering, &ctx, false, None);
        assert_eq!(event, ClusterEvent::RecoveryCompleted);
    }

    #[test]
    fn test_determine_event_fully_recovered() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&ClusterPhase::Degraded, &ctx, false, None);
        assert_eq!(event, ClusterEvent::FullyRecovered);
    }

    #[test]
    fn test_determine_event_resources_applied_from_pending() {
        let ctx = TransitionContext::new(0, 3);
        let event = determine_event(&ClusterPhase::Pending, &ctx, false, None);
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }

    #[test]
    fn test_determine_event_no_ready_during_creating() {
        let ctx = TransitionContext::new(0, 3);
        let event = determine_event(&ClusterPhase::Creating, &ctx, false, None);
        // During creating with 0 ready, we wait (InvalidTransition keeps us in Creating)
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }

    #[test]
    fn test_determine_event_no_ready_during_updating() {
        let ctx = TransitionContext::new(0, 3);
        let event = determine_event(&ClusterPhase::Updating, &ctx, false, None);
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }

    #[test]
    fn test_determine_event_no_ready_during_scaling() {
        let ctx = TransitionContext::new(0, 5);
        let event = determine_event(&ClusterPhase::Scaling, &ctx, false, None);
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }
}

mod valid_events_tests {
    use super::*;

    #[test]
    fn test_valid_events_from_pending() {
        let sm = ClusterStateMachine::new();
        let events = sm.valid_events(&ClusterPhase::Pending);

        assert!(events.contains(&&ClusterEvent::ResourcesApplied));
        assert!(events.contains(&&ClusterEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_running() {
        let sm = ClusterStateMachine::new();
        let events = sm.valid_events(&ClusterPhase::Running);

        assert!(events.contains(&&ClusterEvent::SpecChanged));
        assert!(events.contains(&&ClusterEvent::ReplicaCountChanged));
        assert!(events.contains(&&ClusterEvent::ReplicasDegraded));
        assert!(events.contains(&&ClusterEvent::ReconcileError));
        assert!(events.contains(&&ClusterEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_failed() {
        let sm = ClusterStateMachine::new();
        let events = sm.valid_events(&ClusterPhase::Failed);

        assert!(events.contains(&&ClusterEvent::RecoveryInitiated));
        assert!(events.contains(&&ClusterEvent::DeletionRequested));
    }
}

mod event_display_tests {
    use super::*;

    #[test]
    fn test_all_events_have_display() {
        let events = vec![
            ClusterEvent::ResourcesApplied,
            ClusterEvent::AllReplicasReady,
            ClusterEvent::ReplicasDegraded,
            ClusterEvent::SpecChanged,
            ClusterEvent::ReplicaCountChanged,
            ClusterEvent::ReconcileError,
            ClusterEvent::DeletionRequested,
            ClusterEvent::RecoveryInitiated,
            ClusterEvent::RecoveryCompleted,
            ClusterEvent::FullyRecovered,
        ];

        for event in events {
            let display = format!("{}", event);
            assert!(!display.is_empty());
        }
    }
}
