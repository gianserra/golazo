use super::claims::{ClaimPolicy, ClaimService};
use super::domain::{
    ActivityCategory, ActivityEventPayload, Claim, ClaimScope, ClaimState, CoordinationActor,
    CoordinationEvent, CoordinationEventPayload, EventSeverity, IntegrationArtifact,
    IntegrationArtifactId, IntegrationDiffSummary, IntegrationJobState,
    IntegrationWorkspaceSnapshot, NotificationState, RecordMetadata, Worker, WorkerId, WorkerState,
};
use super::integration::IntegrationQueueService;
use super::notifications::{
    NotificationCandidate, NotificationEnqueueOutcome, NotificationNoisePolicy,
    NotificationQueueService,
};
use super::store::{
    ClaimRepository, EventRepository, IntegrationArtifactRepository, IntegrationJobRepository,
    NotificationRepository, SqliteCoordinationStore, WorkerRepository,
};
use crate::models::Status;
use crate::tracker::Tracker;
use chrono::{Duration, Utc};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn simultaneous_coordination_mutations_remain_exclusive_ordered_and_restart_safe() {
    let directory = tempfile::tempdir().unwrap();
    let tracker_root = directory.path().join("goals");
    let tracker = Tracker::new(&tracker_root);
    tracker
        .create_goal("goal-stress", "Concurrency Stress", "")
        .unwrap();
    for feature in ["feature-a", "feature-b"] {
        tracker
            .add_feature(
                "goal-stress",
                feature,
                feature,
                "stress fixture",
                Status::Planned,
            )
            .unwrap();
    }
    let database = directory.path().join("coordination.sqlite");
    let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
    let now = Utc::now();

    let claim_barrier = Arc::new(Barrier::new(17));
    let mut claim_threads = Vec::new();
    for index in 0..16 {
        let database = database.clone();
        let barrier = Arc::clone(&claim_barrier);
        claim_threads.push(thread::spawn(move || {
            let connection = SqliteCoordinationStore::open(database).unwrap();
            let claim = Claim::new(
                "goal-stress",
                ClaimScope::Feature {
                    feature_id: "feature-a".into(),
                },
                WorkerId::parse(format!("worker-stress-{index}")).unwrap(),
                "base-a",
                now,
                now + Duration::minutes(5),
            )
            .unwrap();
            barrier.wait();
            let result = connection.insert_claim(&claim);
            (claim, result)
        }));
    }
    claim_barrier.wait();
    let claim_results = claim_threads
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let winners = claim_results
        .into_iter()
        .filter_map(|(claim, result)| result.ok().map(|_| claim))
        .collect::<Vec<_>>();
    assert_eq!(winners.len(), 1, "one transaction must own the scope");
    let parent = winners.into_iter().next().unwrap();

    let mut owner = Worker::new("goal-stress", now);
    owner.id = parent.owner.clone();
    owner.transition(WorkerState::Starting, now, None).unwrap();
    owner.transition(WorkerState::Active, now, None).unwrap();
    owner.active_claims.push(parent.id.clone());
    store.upsert_worker(&owner).unwrap();

    let heartbeat_at = now + Duration::minutes(1);
    let heartbeat_barrier = Arc::new(Barrier::new(13));
    let mut heartbeat_threads = Vec::new();
    for _ in 0..12 {
        let database = database.clone();
        let tracker_root = tracker_root.clone();
        let barrier = Arc::clone(&heartbeat_barrier);
        let claim_id = parent.id.clone();
        let worker_id = owner.id.clone();
        heartbeat_threads.push(thread::spawn(move || {
            let connection = Arc::new(SqliteCoordinationStore::open(database).unwrap());
            let service = ClaimService::new(
                Tracker::new(tracker_root),
                connection,
                ClaimPolicy::default(),
            );
            barrier.wait();
            service.heartbeat(&claim_id, &worker_id, 1, heartbeat_at)
        }));
    }
    heartbeat_barrier.wait();
    assert!(
        heartbeat_threads
            .into_iter()
            .all(|handle| handle.join().unwrap().is_ok())
    );
    let heartbeated = store.claim(&parent.id).unwrap().unwrap();
    assert_eq!(heartbeated.heartbeat_at, heartbeat_at);
    assert_eq!(
        heartbeated.lease_expires_at,
        heartbeat_at + Duration::minutes(5)
    );

    let expansion_barrier = Arc::new(Barrier::new(13));
    let mut expansion_threads = Vec::new();
    for index in 0..12 {
        let database = database.clone();
        let tracker_root = tracker_root.clone();
        let barrier = Arc::clone(&expansion_barrier);
        let claim_id = parent.id.clone();
        let worker_id = owner.id.clone();
        expansion_threads.push(thread::spawn(move || {
            let connection = Arc::new(SqliteCoordinationStore::open(database).unwrap());
            let service = ClaimService::new(
                Tracker::new(tracker_root),
                connection,
                ClaimPolicy::default(),
            );
            barrier.wait();
            service.expand_claim(
                &claim_id,
                &worker_id,
                ClaimScope::Feature {
                    feature_id: "feature-b".into(),
                },
                "stress expansion",
                "base-a",
                &format!("stress-expand-{index}"),
                heartbeat_at + Duration::seconds(1),
            )
        }));
    }
    expansion_barrier.wait();
    let expansion_successes = expansion_threads
        .into_iter()
        .map(|handle| handle.join().unwrap().is_ok())
        .filter(|succeeded| *succeeded)
        .count();
    assert_eq!(expansion_successes, 1);
    assert_eq!(
        store
            .claims_for_goal("goal-stress", Some(ClaimState::Active))
            .unwrap()
            .len(),
        2
    );

    let event_barrier = Arc::new(Barrier::new(33));
    let mut event_threads = Vec::new();
    for index in 0..32 {
        let database = database.clone();
        let barrier = Arc::clone(&event_barrier);
        let claim_id = parent.id.clone();
        let worker_id = owner.id.clone();
        event_threads.push(thread::spawn(move || {
            let connection = SqliteCoordinationStore::open(database).unwrap();
            let event = activity_event(index, &worker_id, &claim_id, now);
            barrier.wait();
            connection
                .append_event(&event)
                .map(|sequence| (event, sequence))
        }));
    }
    event_barrier.wait();
    let source_events = event_threads
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap().0)
        .collect::<Vec<_>>();
    let durable_events = store.events_for_goal("goal-stress", 0, None, 100).unwrap();
    assert!(
        durable_events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    assert_eq!(
        durable_events
            .iter()
            .map(|event| event.sequence.unwrap())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        durable_events.len()
    );

    let notification_barrier = Arc::new(Barrier::new(17));
    let mut notification_threads = Vec::new();
    for (index, source) in source_events.into_iter().take(16).enumerate() {
        let database = database.clone();
        let barrier = Arc::clone(&notification_barrier);
        let target = owner.id.clone();
        notification_threads.push(thread::spawn(move || {
            let connection = Arc::new(SqliteCoordinationStore::open(database).unwrap());
            let queue =
                NotificationQueueService::new(connection, NotificationNoisePolicy::default())
                    .unwrap();
            barrier.wait();
            queue.enqueue(
                NotificationCandidate {
                    goal_id: "goal-stress".into(),
                    target_worker: target,
                    source_event_id: source.id,
                    purpose: "stress coordination".into(),
                    summary: format!("notification {index}"),
                    severity: EventSeverity::Info,
                    evidence_refs: vec![format!("stress:{index}")],
                    recommended_action: Some("refresh state".into()),
                    required_acknowledgement: false,
                    expires_at: None,
                },
                now + Duration::minutes(2),
            )
        }));
    }
    notification_barrier.wait();
    let notification_outcomes = notification_threads
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert!(
        notification_outcomes.iter().all(|outcome| matches!(
            outcome,
            Ok(NotificationEnqueueOutcome::Queued(_))
                | Ok(NotificationEnqueueOutcome::Coalesced { .. })
        )),
        "unexpected concurrent notification outcomes: {notification_outcomes:?}"
    );
    let notifications = store.notifications_for_worker(&owner.id, None).unwrap();
    assert_eq!(notifications.len(), 16);
    assert!(notifications.iter().all(|notification| matches!(
        notification.state,
        NotificationState::Queued | NotificationState::Superseded
    )));
    assert!(
        notifications
            .iter()
            .any(|notification| notification.state == NotificationState::Queued)
    );

    let mut artifacts = Vec::new();
    for index in 0..8 {
        let artifact = integration_artifact(index, &parent, &owner, now);
        store.insert_integration_artifact(&artifact).unwrap();
        artifacts.push(artifact);
    }
    let enqueue_barrier = Arc::new(Barrier::new(9));
    let mut enqueue_threads = Vec::new();
    for (index, artifact) in artifacts.into_iter().enumerate() {
        let database = database.clone();
        let barrier = Arc::clone(&enqueue_barrier);
        enqueue_threads.push(thread::spawn(move || {
            let connection = Arc::new(SqliteCoordinationStore::open(database).unwrap());
            let queue = IntegrationQueueService::new(connection);
            barrier.wait();
            queue.enqueue(&artifact.id, index as i32, now + Duration::minutes(3))
        }));
    }
    enqueue_barrier.wait();
    assert!(
        enqueue_threads
            .into_iter()
            .all(|handle| handle.join().unwrap().is_ok())
    );

    let acquire_barrier = Arc::new(Barrier::new(9));
    let mut acquire_threads = Vec::new();
    for _ in 0..8 {
        let database = database.clone();
        let barrier = Arc::clone(&acquire_barrier);
        acquire_threads.push(thread::spawn(move || {
            let connection = Arc::new(SqliteCoordinationStore::open(database).unwrap());
            let queue = IntegrationQueueService::new(connection);
            barrier.wait();
            queue.acquire_next("repo-stress", now + Duration::minutes(4))
        }));
    }
    acquire_barrier.wait();
    let acquired = acquire_threads
        .into_iter()
        .filter_map(|handle| handle.join().unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(acquired.len(), 1);

    let reopened = SqliteCoordinationStore::open(&database).unwrap();
    reopened.integrity_check().unwrap();
    let recovered = reopened
        .recover_running_integration_jobs(now + Duration::minutes(5))
        .unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].id, acquired[0].id);
    assert_eq!(recovered[0].state, IntegrationJobState::Queued);
    assert!(
        reopened
            .integration_jobs_for_goal("goal-stress", None)
            .unwrap()
            .iter()
            .all(|job| job.state == IntegrationJobState::Queued)
    );
}

fn activity_event(
    index: usize,
    worker_id: &WorkerId,
    claim_id: &super::domain::ClaimId,
    now: chrono::DateTime<Utc>,
) -> CoordinationEvent {
    CoordinationEvent::from_typed_payload(
        "goal-stress",
        EventSeverity::Info,
        CoordinationActor::Worker {
            worker_id: worker_id.clone(),
        },
        "stress-events",
        CoordinationEventPayload::Activity(ActivityEventPayload {
            worker_id: worker_id.clone(),
            claim_id: claim_id.clone(),
            category: ActivityCategory::Progress,
            summary: format!("stress event {index}"),
            progress_percent: Some((index % 101) as u8),
            changed_scope: vec![format!("path-{index}")],
            artifact_id: None,
            evidence_refs: vec![format!("stress:event:{index}")],
            validation_succeeded: None,
        }),
        now + Duration::seconds(index as i64),
    )
    .unwrap()
}

fn integration_artifact(
    index: usize,
    claim: &Claim,
    worker: &Worker,
    now: chrono::DateTime<Utc>,
) -> IntegrationArtifact {
    IntegrationArtifact {
        metadata: RecordMetadata::new(now),
        id: IntegrationArtifactId::new(),
        goal_id: "goal-stress".into(),
        claim_id: claim.id.clone(),
        claim_generation: claim.lease_generation,
        worker_id: worker.id.clone(),
        repository_id: "repo-stress".into(),
        base_revision: "base-a".into(),
        head_revision: format!("head-{index}"),
        commits: vec![],
        diff_summary: IntegrationDiffSummary {
            files_changed: 0,
            insertions: 0,
            deletions: 0,
            changed_paths: vec![],
            summary: format!("artifact {index}"),
        },
        changed_contracts: vec![],
        migrations: vec![],
        validations: vec![],
        evidence_refs: vec![format!("stress:artifact:{index}")],
        known_risks: vec![],
        workspace: IntegrationWorkspaceSnapshot {
            branch: format!("codex/stress-{index}"),
            staged_paths: vec![],
            unstaged_paths: vec![],
            untracked_paths: vec![],
            conflicted_paths: vec![],
        },
    }
}
