use super::contracts::{ContractRegistration, ContractRegistry, ContractRegistryError};
use super::domain::{
    ActivityCategory, ActivityEventPayload, ContractKind, CoordinationActor, CoordinationEvent,
    CoordinationEventKind, CoordinationEventPayload, EventRetentionPolicy, EventSeverity, WorkerId,
};
use super::store::{ContractRepository, EventRepository, SqliteCoordinationStore, StoreError};
use chrono::{Duration, Utc};
use rusqlite::{Connection, params};
use std::sync::Arc;

#[test]
fn schema_v4_events_upgrade_through_replay_restart_deduplication_and_retention() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("coordination.sqlite");
    let now = Utc::now();
    let occurred_at = now - Duration::hours(2);
    let mut legacy = CoordinationEvent::new(
        "goal-a",
        CoordinationEventKind::ActivityPublished,
        EventSeverity::Info,
        CoordinationActor::System,
        "claim-a",
        serde_json::json!({"summary": "legacy event"}),
        occurred_at,
    );
    legacy.sequence = Some(1);
    let mut legacy_json = serde_json::to_value(&legacy).unwrap();
    legacy_json.as_object_mut().unwrap().remove("payloadSchema");

    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                PRAGMA user_version = 4;
                CREATE TABLE coordination_schema (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                INSERT INTO coordination_schema(version, applied_at)
                    VALUES (4, '2026-01-01T00:00:00Z');
                CREATE TABLE coordination_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    id TEXT NOT NULL UNIQUE,
                    goal_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    data TEXT NOT NULL
                );
                CREATE INDEX events_goal_sequence
                    ON coordination_events(goal_id, sequence);
                CREATE INDEX events_goal_correlation
                    ON coordination_events(goal_id, correlation_id, sequence);
                CREATE TABLE event_publications (
                    publication_key TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    sequence INTEGER NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(sequence) REFERENCES coordination_events(sequence) ON DELETE CASCADE
                );
                "#,
            )
            .unwrap();
        connection
            .execute(
                r#"INSERT INTO coordination_events(
                     sequence, id, goal_id, kind, correlation_id, occurred_at, data
                   ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
                params![
                    1_i64,
                    legacy.id.as_str(),
                    "goal-a",
                    "activity_published",
                    "claim-a",
                    occurred_at.to_rfc3339(),
                    serde_json::to_string(&legacy_json).unwrap(),
                ],
            )
            .unwrap();
    }

    let first_activity = typed_activity("first typed event", occurred_at);
    let retry_activity = typed_activity("retry must not replace first", occurred_at);
    {
        let store = SqliteCoordinationStore::open(&path).unwrap();
        let migrated = store.events_for_goal("goal-a", 0, None, 10).unwrap();
        assert_eq!(migrated.len(), 1);
        assert_eq!(migrated[0].payload["summary"], "legacy event");
        assert_eq!(migrated[0].typed_payload().unwrap(), None);

        let first_sequence = store
            .append_event_once("activity:request-1", &first_activity)
            .unwrap();
        let retry_sequence = store
            .append_event_once("activity:request-1", &retry_activity)
            .unwrap();
        assert_eq!(first_sequence, 2);
        assert_eq!(retry_sequence, first_sequence);

        let first_delivery = store
            .replay_events_for_consumer("supervisor", "goal-a", 10, now)
            .unwrap();
        let retry_delivery = store
            .replay_events_for_consumer("supervisor", "goal-a", 10, now)
            .unwrap();
        assert_eq!(first_delivery.events, retry_delivery.events);
        assert_eq!(
            first_delivery
                .events
                .iter()
                .filter_map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        store
            .acknowledge_event_sequence("supervisor", "goal-a", 2, now)
            .unwrap();
    }

    let reopened = SqliteCoordinationStore::open(&path).unwrap();
    assert_eq!(
        reopened
            .event_replay_cursor("supervisor", "goal-a")
            .unwrap()
            .unwrap()
            .acknowledged_sequence,
        2
    );
    assert_eq!(
        reopened
            .append_event_once("activity:request-1", &retry_activity)
            .unwrap(),
        2
    );
    let final_activity = typed_activity("event after restart", occurred_at);
    assert_eq!(reopened.append_event(&final_activity).unwrap(), 3);
    let remaining = reopened
        .replay_events_for_consumer("supervisor", "goal-a", 10, now)
        .unwrap();
    assert_eq!(remaining.events.len(), 1);
    assert_eq!(remaining.events[0].sequence, Some(3));
    reopened
        .acknowledge_event_sequence("supervisor", "goal-a", 3, now)
        .unwrap();

    let compacted = reopened
        .compact_events(
            "goal-a",
            &EventRetentionPolicy {
                hot_retention_seconds: 60 * 60,
                retain_latest: 1,
                max_compaction_batch: 10,
            },
            now,
        )
        .unwrap();
    assert_eq!(compacted.compacted_sequences, vec![1, 2]);
    let archive = reopened.archived_events_for_goal("goal-a", 0, 10).unwrap();
    assert_eq!(archive.len(), 2);
    assert_eq!(archive[0].event.typed_payload().unwrap(), None);
    assert!(matches!(
        archive[1].event.typed_payload().unwrap(),
        Some(CoordinationEventPayload::Activity(_))
    ));
    let hot = reopened.events_for_goal("goal-a", 2, None, 10).unwrap();
    assert_eq!(hot.len(), 1);
    assert!(matches!(
        hot[0].typed_payload().unwrap(),
        Some(CoordinationEventPayload::Activity(_))
    ));
}

#[test]
fn contract_changes_are_atomic_attributed_and_revision_safe() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
    );
    let registry = ContractRegistry::new(Arc::clone(&store));
    let now = Utc::now();
    let contract = registry
        .register(
            ContractRegistration {
                goal_id: "goal-a".into(),
                stable_key: "api.worker.v1".into(),
                title: "Worker API".into(),
                kind: ContractKind::Api,
                producer: None,
                compatibility_notes: "Initial contract".into(),
            },
            CoordinationActor::System,
            None,
            now,
        )
        .unwrap();
    let registration_event = store.events_for_goal("goal-a", 0, None, 10).unwrap()[0].clone();

    let worker_id = WorkerId::new();
    let mut failed_revision = contract.clone();
    failed_revision.revise(worker_id.clone(), "Would add a field", now);
    let failed = store.revise_contract_with_event(
        &failed_revision,
        1,
        &registration_event,
        "contract.revise:forced-failure",
    );
    assert!(matches!(failed, Err(StoreError::Database(_))));
    assert_eq!(store.contract(&contract.id).unwrap().unwrap().revision, 1);
    assert_eq!(
        store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
        1
    );

    let revised = registry
        .revise(
            &contract.id,
            1,
            worker_id.clone(),
            "Adds an optional field".into(),
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            now,
        )
        .unwrap();
    assert_eq!(revised.revision, 2);
    let events = store.events_for_goal("goal-a", 0, None, 10).unwrap();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[1].typed_payload().unwrap(),
        Some(CoordinationEventPayload::Contract(ref payload))
            if payload.previous_revision == 1
                && payload.revision == 2
                && payload.producer_worker_id.as_ref() == Some(&worker_id)
    ));

    assert!(matches!(
        registry.revise(
            &contract.id,
            1,
            worker_id.clone(),
            "Stale retry".into(),
            CoordinationActor::Worker { worker_id },
            now,
        ),
        Err(ContractRegistryError::RevisionMismatch {
            expected: 1,
            current: 2
        })
    ));
    assert_eq!(store.contract(&contract.id).unwrap().unwrap().revision, 2);
    assert_eq!(
        store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
        2
    );
}

fn typed_activity(summary: &str, occurred_at: chrono::DateTime<Utc>) -> CoordinationEvent {
    CoordinationEvent::from_typed_payload(
        "goal-a",
        EventSeverity::Info,
        CoordinationActor::System,
        "claim-a",
        CoordinationEventPayload::Activity(ActivityEventPayload {
            worker_id: WorkerId::new(),
            claim_id: super::domain::ClaimId::new(),
            category: ActivityCategory::Progress,
            summary: summary.into(),
            progress_percent: Some(50),
            changed_scope: vec!["rust-backend/coordination".into()],
            artifact_id: None,
            evidence_refs: vec!["test:coordination-conformance".into()],
            validation_succeeded: None,
        }),
        occurred_at,
    )
    .unwrap()
}
