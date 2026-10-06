use super::claims::{ClaimPolicy, ClaimService};
use super::domain::{
    ActivityCategory, ActivityEventPayload, Claim, ClaimOutcome, ClaimOutcomeKind, ClaimScope,
    ClaimState, CoordinationActor, CoordinationEvent, CoordinationEventPayload, EventSeverity,
    Worker, WorkerId, WorkerState,
};
use super::store::{ClaimRepository, EventRepository, SqliteCoordinationStore, WorkerRepository};
use crate::models::Status;
use crate::tracker::Tracker;
use chrono::{DateTime, Duration, Utc};
use proptest::prelude::*;
use std::sync::Arc;

fn fixed_time() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

fn worker_state(index: u8) -> WorkerState {
    match index % 10 {
        0 => WorkerState::Created,
        1 => WorkerState::Starting,
        2 => WorkerState::Active,
        3 => WorkerState::Waiting,
        4 => WorkerState::Paused,
        5 => WorkerState::Blocked,
        6 => WorkerState::Recovering,
        7 => WorkerState::Completed,
        8 => WorkerState::Failed,
        _ => WorkerState::Cancelled,
    }
}

fn terminal_claim(index: u8, at: DateTime<Utc>) -> (ClaimState, ClaimOutcome) {
    let (state, kind) = match index % 5 {
        0 => (ClaimState::Released, ClaimOutcomeKind::Released),
        1 => (ClaimState::Completed, ClaimOutcomeKind::Completed),
        2 => (ClaimState::Blocked, ClaimOutcomeKind::Blocked),
        3 => (ClaimState::Expired, ClaimOutcomeKind::Expired),
        _ => (ClaimState::Revoked, ClaimOutcomeKind::Revoked),
    };
    (
        state,
        ClaimOutcome {
            kind,
            reason: "property terminal transition".into(),
            artifact_id: None,
            evidence_refs: vec!["property:model".into()],
            escalation_id: None,
            at,
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn worker_terminal_states_are_absorbing(path in prop::collection::vec(0u8..10, 0..40)) {
        let now = fixed_time();
        let mut worker = Worker::new("goal-property", now);
        for (index, requested) in path.into_iter().enumerate() {
            let before = worker.state;
            let next = worker_state(requested);
            let result = worker.transition(
                next,
                now + Duration::seconds(index as i64 + 1),
                Some("property transition".into()),
            );
            if before.is_terminal() {
                prop_assert!(result.is_err());
                prop_assert_eq!(worker.state, before);
            } else if result.is_ok() {
                prop_assert_eq!(worker.state, next);
                prop_assert_eq!(worker.termination.is_some(), next.is_terminal());
            } else {
                prop_assert_eq!(worker.state, before);
            }
        }
        if !worker.state.is_terminal() {
            worker
                .transition(WorkerState::Cancelled, now + Duration::hours(1), Some("property terminal".into()))
                .unwrap();
        }
        let terminal = worker.state;
        for index in 0..10 {
            prop_assert!(
                worker
                    .transition(worker_state(index), now + Duration::hours(2), None)
                    .is_err()
            );
            prop_assert_eq!(worker.state, terminal);
        }
    }

    #[test]
    fn claim_leases_and_terminal_transitions_preserve_invariants(
        offsets in prop::collection::vec(1i64..600, 1..30),
        terminal_index in 0u8..5,
    ) {
        let now = fixed_time();
        let mut claim = Claim::new(
            "goal-property",
            ClaimScope::Feature { feature_id: "feature-a".into() },
            WorkerId::new(),
            "base-a",
            now,
            now + Duration::minutes(5),
        ).unwrap();
        let mut heartbeat = now;
        for offset in offsets {
            heartbeat += Duration::seconds(offset);
            let expiry = heartbeat + Duration::minutes(5);
            claim.renew(claim.lease_generation, heartbeat, expiry).unwrap();
            prop_assert_eq!(claim.heartbeat_at, heartbeat);
            prop_assert!(claim.lease_expires_at > claim.heartbeat_at);
            prop_assert!(claim.renew(claim.lease_generation + 1, heartbeat, expiry).is_err());
        }
        let (terminal, outcome) = terminal_claim(terminal_index, heartbeat + Duration::seconds(1));
        claim.transition(terminal, outcome.clone()).unwrap();
        prop_assert_eq!(claim.state, terminal);
        prop_assert_eq!(claim.outcome.as_ref(), Some(&outcome));
        prop_assert!(
            claim
                .renew(
                    claim.lease_generation,
                    heartbeat + Duration::seconds(2),
                    heartbeat + Duration::minutes(5),
                )
                .is_err()
        );
        let (_, second_outcome) = terminal_claim((terminal_index + 1) % 5, heartbeat + Duration::seconds(2));
        prop_assert!(claim.transition(terminal, second_outcome).is_err());
        prop_assert_eq!(claim.outcome.as_ref(), Some(&outcome));
    }

    #[test]
    fn event_replay_is_strictly_ordered_and_lossless(count in 1usize..64) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordination.sqlite");
        let store = SqliteCoordinationStore::open(&path).unwrap();
        let now = fixed_time();
        for index in 0..count {
            let event = CoordinationEvent::from_typed_payload(
                "goal-property",
                EventSeverity::Info,
                CoordinationActor::System,
                "property-ordering",
                CoordinationEventPayload::Activity(ActivityEventPayload {
                    worker_id: WorkerId::new(),
                    claim_id: super::domain::ClaimId::new(),
                    category: ActivityCategory::Progress,
                    summary: format!("property event {index}"),
                    progress_percent: Some((index % 101) as u8),
                    changed_scope: vec![format!("scope-{index}")],
                    artifact_id: None,
                    evidence_refs: vec![format!("property:event:{index}")],
                    validation_succeeded: None,
                }),
                now + Duration::milliseconds(index as i64),
            ).unwrap();
            store.append_event(&event).unwrap();
        }
        let first = store.events_for_goal("goal-property", 0, None, 100).unwrap();
        prop_assert_eq!(first.len(), count);
        prop_assert!(first.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
        let reopened = SqliteCoordinationStore::open(&path).unwrap();
        let replay = reopened.events_for_goal("goal-property", 0, None, 100).unwrap();
        prop_assert_eq!(replay, first);
    }

    #[test]
    fn repeated_idempotent_claim_requests_have_one_owner_and_one_event(retries in 1usize..32) {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-property", "Property Goal", "").unwrap();
        tracker
            .add_feature("goal-property", "feature-a", "Feature A", "", Status::Planned)
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = fixed_time();
        let mut worker = Worker::new("goal-property", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        store.upsert_worker(&worker).unwrap();
        let service = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        let first = service
            .claim_ready_unit(
                "goal-property",
                &worker.id,
                None,
                "base-a",
                "property-idempotency-key",
                now,
            )
            .unwrap();
        for _ in 0..retries {
            let replay = service
                .claim_ready_unit(
                    "goal-property",
                    &worker.id,
                    None,
                    "base-a",
                    "property-idempotency-key",
                    now,
                )
                .unwrap();
            prop_assert_eq!(replay.id, first.id.clone());
        }
        let active = store
            .claims_for_goal("goal-property", Some(ClaimState::Active))
            .unwrap();
        prop_assert_eq!(active.len(), 1);
        prop_assert_eq!(active[0].id.clone(), first.id);
        let events = store.events_for_goal("goal-property", 0, None, 100).unwrap();
        prop_assert_eq!(events.len(), 1);
    }
}
