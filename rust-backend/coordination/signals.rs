use super::domain::{
    ClaimId, ClaimState, CoordinationEvent, CoordinationEventPayload, CoordinationSignal,
    DomainError, EventId, EventSeverity, SignalId, SignalKind, WatchdogActionKind, WorkerId,
};
use super::store::{SignalMergeResult, SqliteCoordinationStore, StoreError};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalProcessingPolicy {
    pub time_to_live_seconds: u64,
    pub minimum_confidence_percent: u8,
}

impl Default for SignalProcessingPolicy {
    fn default() -> Self {
        Self {
            time_to_live_seconds: 15 * 60,
            minimum_confidence_percent: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalProcessingReport {
    pub goal_id: String,
    pub processed_at: DateTime<Utc>,
    pub created_signal_ids: Vec<SignalId>,
    pub updated_signal_ids: Vec<SignalId>,
    pub duplicate_event_ids: Vec<EventId>,
    pub ignored_event_ids: Vec<EventId>,
    pub expired_signal_ids: Vec<SignalId>,
}

#[derive(Debug, Error)]
pub enum SignalProcessorError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("invalid signal processing policy: {0}")]
    InvalidPolicy(String),
    #[error("event {event_id} belongs to goal {event_goal}, not {requested_goal}")]
    GoalMismatch {
        event_id: String,
        event_goal: String,
        requested_goal: String,
    },
}

#[derive(Debug, Clone)]
pub struct SignalProcessor {
    store: Arc<SqliteCoordinationStore>,
    policy: SignalProcessingPolicy,
}

impl SignalProcessor {
    pub fn new(
        store: Arc<SqliteCoordinationStore>,
        policy: SignalProcessingPolicy,
    ) -> Result<Self, SignalProcessorError> {
        if policy.time_to_live_seconds == 0
            || policy.time_to_live_seconds > i64::MAX as u64
            || policy.minimum_confidence_percent > 100
        {
            return Err(SignalProcessorError::InvalidPolicy(
                "signal TTL must be positive and confidence must be between 0 and 100".into(),
            ));
        }
        Ok(Self { store, policy })
    }

    pub fn process(
        &self,
        goal_id: &str,
        events: &[CoordinationEvent],
        now: DateTime<Utc>,
    ) -> Result<SignalProcessingReport, SignalProcessorError> {
        let expired = self.store.expire_signals(goal_id, now)?;
        let mut report = SignalProcessingReport {
            goal_id: goal_id.into(),
            processed_at: now,
            created_signal_ids: Vec::new(),
            updated_signal_ids: Vec::new(),
            duplicate_event_ids: Vec::new(),
            ignored_event_ids: Vec::new(),
            expired_signal_ids: expired.into_iter().map(|signal| signal.id).collect(),
        };
        let mut ordered = events.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            left.sequence
                .cmp(&right.sequence)
                .then_with(|| left.occurred_at.cmp(&right.occurred_at))
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        let ttl = Duration::seconds(self.policy.time_to_live_seconds as i64);
        for event in ordered {
            if event.goal_id != goal_id {
                return Err(SignalProcessorError::GoalMismatch {
                    event_id: event.id.as_str().into(),
                    event_goal: event.goal_id.clone(),
                    requested_goal: goal_id.into(),
                });
            }
            if let Some(CoordinationEventPayload::Claim(payload)) = event.typed_payload()?
                && payload.to_state.is_terminal()
            {
                report.expired_signal_ids.extend(
                    self.store
                        .expire_signals_for_claims(goal_id, &[payload.claim_id], now)?
                        .into_iter()
                        .map(|signal| signal.id),
                );
            }
            let Some(scored) = score_event(event)? else {
                report.ignored_event_ids.push(event.id.clone());
                continue;
            };
            let expires_at = event.occurred_at + ttl;
            if scored.confidence_percent < self.policy.minimum_confidence_percent
                || expires_at <= now
            {
                report.ignored_event_ids.push(event.id.clone());
                continue;
            }
            let observation = CoordinationSignal::new(
                goal_id,
                scored.correlation_key,
                scored.kind,
                scored.confidence_percent,
                scored.severity,
                event.id.clone(),
                scored.worker_ids,
                scored.claim_ids,
                event.occurred_at,
                expires_at,
            )?;
            match self.store.merge_signal_observation(&observation, now)? {
                SignalMergeResult {
                    signal,
                    created: true,
                    ..
                } => report.created_signal_ids.push(signal.id),
                SignalMergeResult {
                    signal,
                    changed: true,
                    ..
                } => report.updated_signal_ids.push(signal.id),
                SignalMergeResult { .. } => report.duplicate_event_ids.push(event.id.clone()),
            }
        }
        canonicalize_report(&mut report);
        Ok(report)
    }
}

#[derive(Debug)]
struct ScoredSignal {
    correlation_key: String,
    kind: SignalKind,
    confidence_percent: u8,
    severity: EventSeverity,
    worker_ids: Vec<WorkerId>,
    claim_ids: Vec<ClaimId>,
}

fn score_event(event: &CoordinationEvent) -> Result<Option<ScoredSignal>, DomainError> {
    let Some(payload) = event.typed_payload()? else {
        return Ok(None);
    };
    let scored = match payload {
        CoordinationEventPayload::FileOverlap(payload) => {
            let high_risk = payload
                .paths
                .iter()
                .any(|path| path.migration || path.dependency_file);
            ScoredSignal {
                correlation_key: overlap_key(&payload.left_claim_id, &payload.right_claim_id),
                kind: SignalKind::FileOverlap,
                confidence_percent: if high_risk { 85 } else { 60 },
                severity: severity_max(
                    event.severity,
                    if high_risk {
                        EventSeverity::Warning
                    } else {
                        EventSeverity::Info
                    },
                ),
                worker_ids: vec![payload.left_worker_id, payload.right_worker_id],
                claim_ids: vec![payload.left_claim_id, payload.right_claim_id],
            }
        }
        CoordinationEventPayload::SymbolOverlap(payload) => {
            let exact = payload
                .overlaps
                .iter()
                .any(|overlap| overlap.match_kind == "symbol");
            ScoredSignal {
                correlation_key: overlap_key(&payload.left_claim_id, &payload.right_claim_id),
                kind: SignalKind::SymbolOverlap,
                confidence_percent: if exact { 80 } else { 55 },
                severity: severity_max(
                    event.severity,
                    if exact {
                        EventSeverity::Warning
                    } else {
                        EventSeverity::Info
                    },
                ),
                worker_ids: vec![payload.left_worker_id, payload.right_worker_id],
                claim_ids: vec![payload.left_claim_id, payload.right_claim_id],
            }
        }
        CoordinationEventPayload::Dependency(payload) => ScoredSignal {
            correlation_key: format!(
                "dependency:{}:{}",
                payload.work_package_id.as_str(),
                payload.dependency_key
            ),
            kind: SignalKind::DependencyChanged,
            confidence_percent: 90,
            severity: severity_max(event.severity, EventSeverity::Warning),
            worker_ids: payload.affected_worker_ids,
            claim_ids: vec![],
        },
        CoordinationEventPayload::ContractMismatch(payload) => ScoredSignal {
            correlation_key: format!(
                "contract:{}:{}",
                payload.claim_id.as_str(),
                payload.contract_id.as_str()
            ),
            kind: SignalKind::ContractMismatch,
            confidence_percent: 95,
            severity: severity_max(event.severity, EventSeverity::Error),
            worker_ids: vec![payload.worker_id],
            claim_ids: vec![payload.claim_id],
        },
        CoordinationEventPayload::MigrationOverlap(payload) => ScoredSignal {
            correlation_key: overlap_key(&payload.left_claim_id, &payload.right_claim_id),
            kind: SignalKind::MigrationOverlap,
            confidence_percent: 95,
            severity: severity_max(event.severity, EventSeverity::Error),
            worker_ids: vec![payload.left_worker_id, payload.right_worker_id],
            claim_ids: vec![payload.left_claim_id, payload.right_claim_id],
        },
        CoordinationEventPayload::MergeConflict(payload) => ScoredSignal {
            correlation_key: overlap_key(&payload.left_claim_id, &payload.right_claim_id),
            kind: SignalKind::MergeConflict,
            confidence_percent: 100,
            severity: EventSeverity::Critical,
            worker_ids: vec![payload.left_worker_id, payload.right_worker_id],
            claim_ids: vec![payload.left_claim_id, payload.right_claim_id],
        },
        CoordinationEventPayload::WorkerStalled(payload) => ScoredSignal {
            correlation_key: health_key(&payload.worker_id),
            kind: SignalKind::WorkerStalled,
            confidence_percent: 80,
            severity: severity_max(event.severity, EventSeverity::Warning),
            worker_ids: vec![payload.worker_id],
            claim_ids: vec![payload.claim_id],
        },
        CoordinationEventPayload::RepeatedValidationFailure(payload) => ScoredSignal {
            correlation_key: health_key(&payload.worker_id),
            kind: SignalKind::RepeatedValidationFailure,
            confidence_percent: 90,
            severity: severity_max(event.severity, EventSeverity::Error),
            worker_ids: vec![payload.worker_id],
            claim_ids: vec![payload.claim_id],
        },
        CoordinationEventPayload::WatchdogAction(payload) => {
            let severity = match payload.action {
                WatchdogActionKind::PermissionViolation => EventSeverity::Critical,
                WatchdogActionKind::EnvironmentRecovery
                | WatchdogActionKind::ExecutionLimit
                | WatchdogActionKind::ResourceQuota
                | WatchdogActionKind::FailureLoopCutoff => EventSeverity::Error,
            };
            ScoredSignal {
                correlation_key: health_key(&payload.worker_id),
                kind: SignalKind::WatchdogAction,
                confidence_percent: 100,
                severity: severity_max(event.severity, severity),
                worker_ids: vec![payload.worker_id],
                claim_ids: payload.claim_ids,
            }
        }
        CoordinationEventPayload::Claim(payload) if payload.to_state == ClaimState::Expired => {
            ScoredSignal {
                correlation_key: health_key(&payload.worker_id),
                kind: SignalKind::LeaseExpired,
                confidence_percent: 100,
                severity: severity_max(event.severity, EventSeverity::Error),
                worker_ids: vec![payload.worker_id],
                claim_ids: vec![payload.claim_id],
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(scored))
}

fn overlap_key(left: &ClaimId, right: &ClaimId) -> String {
    let (left, right) = if left.as_str() <= right.as_str() {
        (left, right)
    } else {
        (right, left)
    };
    format!("overlap:{}:{}", left.as_str(), right.as_str())
}

fn health_key(worker_id: &WorkerId) -> String {
    format!("health:{}", worker_id.as_str())
}

fn severity_max(left: EventSeverity, right: EventSeverity) -> EventSeverity {
    if left.rank() >= right.rank() {
        left
    } else {
        right
    }
}

fn canonicalize_report(report: &mut SignalProcessingReport) {
    let mut ids = BTreeMap::<String, SignalId>::new();
    for id in report.created_signal_ids.drain(..) {
        ids.entry(id.as_str().into()).or_insert(id);
    }
    report.created_signal_ids = ids.into_values().collect();
    let mut ids = BTreeMap::<String, SignalId>::new();
    for id in report.updated_signal_ids.drain(..) {
        ids.entry(id.as_str().into()).or_insert(id);
    }
    report.updated_signal_ids = ids.into_values().collect();
    report
        .duplicate_event_ids
        .sort_by(|left, right| left.as_str().cmp(right.as_str()));
    report.duplicate_event_ids.dedup();
    report
        .ignored_event_ids
        .sort_by(|left, right| left.as_str().cmp(right.as_str()));
    report.ignored_event_ids.dedup();
    report
        .expired_signal_ids
        .sort_by(|left, right| left.as_str().cmp(right.as_str()));
    report.expired_signal_ids.dedup();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        ClaimEventPayload, CoordinationActor, FileOverlapEventPayload, FileOverlapInput,
        MergeConflictEventPayload, MigrationOverlapEventPayload, MigrationOverlapInput,
        SymbolOverlapEventPayload, SymbolOverlapInput,
    };
    use crate::coordination::store::SignalRepository;

    #[test]
    fn correlates_scores_deduplicates_expires_and_reopens_signals() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&path).unwrap());
        let processor = SignalProcessor::new(
            Arc::clone(&store),
            SignalProcessingPolicy {
                time_to_live_seconds: 60,
                minimum_confidence_percent: 50,
            },
        )
        .unwrap();
        let now = Utc::now();
        let left_worker = WorkerId::new();
        let right_worker = WorkerId::new();
        let left_claim = ClaimId::new();
        let right_claim = ClaimId::new();
        let events = overlap_events(&left_worker, &left_claim, &right_worker, &right_claim, now);
        let mut with_duplicate = events.clone();
        with_duplicate.push(events[0].clone());

        let first = processor.process("goal-a", &with_duplicate, now).unwrap();
        assert_eq!(first.created_signal_ids.len(), 1);
        assert_eq!(first.updated_signal_ids.len(), 1);
        assert_eq!(first.duplicate_event_ids, vec![events[0].id.clone()]);
        let active = store
            .signals_for_goal("goal-a", Some(super::super::domain::SignalState::Active))
            .unwrap();
        assert_eq!(active.len(), 1);
        let signal = &active[0];
        assert_eq!(
            signal.correlation_key,
            overlap_key(&left_claim, &right_claim)
        );
        assert_eq!(signal.occurrence_count, 4);
        assert_eq!(signal.source_event_ids.len(), 4);
        assert_eq!(signal.kinds.len(), 4);
        assert_eq!(signal.primary_kind, SignalKind::MergeConflict);
        assert_eq!(signal.confidence_percent, 100);
        assert_eq!(signal.severity, EventSeverity::Critical);
        assert_eq!(signal.affected_worker_ids.len(), 2);
        assert_eq!(signal.affected_claim_ids.len(), 2);

        let replay = processor.process("goal-a", &events, now).unwrap();
        assert!(replay.created_signal_ids.is_empty());
        assert!(replay.updated_signal_ids.is_empty());
        assert_eq!(replay.duplicate_event_ids.len(), 4);
        let expired = processor
            .process("goal-a", &[], now + Duration::seconds(61))
            .unwrap();
        assert_eq!(expired.expired_signal_ids, vec![signal.id.clone()]);
        assert!(
            store
                .signals_for_goal("goal-a", Some(super::super::domain::SignalState::Active))
                .unwrap()
                .is_empty()
        );

        let later_event = overlap_events(
            &left_worker,
            &left_claim,
            &right_worker,
            &right_claim,
            now + Duration::seconds(62),
        )
        .remove(0);
        let reopened = processor
            .process("goal-a", &[later_event], now + Duration::seconds(62))
            .unwrap();
        assert_eq!(reopened.created_signal_ids.len(), 1);
        assert_ne!(reopened.created_signal_ids[0], signal.id);
        let completed = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::System,
            left_claim.as_str(),
            CoordinationEventPayload::Claim(ClaimEventPayload {
                claim_id: left_claim.clone(),
                worker_id: left_worker.clone(),
                from_state: Some(ClaimState::Active),
                to_state: ClaimState::Completed,
                reason: "integrated".into(),
                artifact_id: Some("artifact-a".into()),
                evidence_refs: vec!["test:passed".into()],
            }),
            now + Duration::seconds(63),
        )
        .unwrap();
        let obsolete = processor
            .process("goal-a", &[completed], now + Duration::seconds(63))
            .unwrap();
        assert_eq!(obsolete.expired_signal_ids, reopened.created_signal_ids);
        assert!(
            store
                .signals_for_goal("goal-a", Some(super::super::domain::SignalState::Active))
                .unwrap()
                .is_empty()
        );
        drop(processor);
        drop(store);

        let reopened_store = SqliteCoordinationStore::open(&path).unwrap();
        assert_eq!(
            reopened_store
                .signals_for_goal("goal-a", None)
                .unwrap()
                .len(),
            2
        );
    }

    fn overlap_events(
        left_worker: &WorkerId,
        left_claim: &ClaimId,
        right_worker: &WorkerId,
        right_claim: &ClaimId,
        now: DateTime<Utc>,
    ) -> Vec<CoordinationEvent> {
        let actor = CoordinationActor::System;
        let file = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            actor.clone(),
            "file-overlap",
            CoordinationEventPayload::FileOverlap(FileOverlapEventPayload {
                left_worker_id: left_worker.clone(),
                left_claim_id: left_claim.clone(),
                left_base_revision: "base".into(),
                left_head_revision: "left".into(),
                right_worker_id: right_worker.clone(),
                right_claim_id: right_claim.clone(),
                right_base_revision: "base".into(),
                right_head_revision: "right".into(),
                paths: vec![FileOverlapInput {
                    path: "src/lib.rs".into(),
                    left_status: "modified".into(),
                    right_status: "modified".into(),
                    migration: false,
                    dependency_file: false,
                    generated_artifact: false,
                }],
            }),
            now,
        )
        .unwrap();
        let symbol = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Warning,
            actor.clone(),
            "symbol-overlap",
            CoordinationEventPayload::SymbolOverlap(SymbolOverlapEventPayload {
                left_worker_id: left_worker.clone(),
                left_claim_id: left_claim.clone(),
                left_base_revision: "base".into(),
                left_head_revision: "left".into(),
                right_worker_id: right_worker.clone(),
                right_claim_id: right_claim.clone(),
                right_base_revision: "base".into(),
                right_head_revision: "right".into(),
                overlaps: vec![SymbolOverlapInput {
                    identity: "fn:shared".into(),
                    match_kind: "symbol".into(),
                    left_path: "src/lib.rs".into(),
                    right_path: "src/lib.rs".into(),
                }],
            }),
            now + Duration::milliseconds(1),
        )
        .unwrap();
        let migration = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Warning,
            actor.clone(),
            "migration-overlap",
            CoordinationEventPayload::MigrationOverlap(MigrationOverlapEventPayload {
                left_worker_id: left_worker.clone(),
                left_claim_id: left_claim.clone(),
                left_base_revision: "base".into(),
                left_head_revision: "left".into(),
                right_worker_id: right_worker.clone(),
                right_claim_id: right_claim.clone(),
                right_base_revision: "base".into(),
                right_head_revision: "right".into(),
                overlaps: vec![MigrationOverlapInput {
                    classification: "database".into(),
                    left_paths: vec!["migrations/001.sql".into()],
                    right_paths: vec!["migrations/002.sql".into()],
                    shared_paths: vec![],
                }],
            }),
            now + Duration::milliseconds(2),
        )
        .unwrap();
        let merge = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Error,
            actor,
            "merge-conflict",
            CoordinationEventPayload::MergeConflict(MergeConflictEventPayload {
                left_worker_id: left_worker.clone(),
                left_claim_id: left_claim.clone(),
                left_head_revision: "left".into(),
                right_worker_id: right_worker.clone(),
                right_claim_id: right_claim.clone(),
                right_head_revision: "right".into(),
                merge_base_revision: "base".into(),
                conflicting_paths: vec!["src/lib.rs".into()],
                diagnostics: "CONFLICT".into(),
                diagnostics_truncated: false,
            }),
            now + Duration::milliseconds(3),
        )
        .unwrap();
        vec![file, symbol, migration, merge]
    }
}
