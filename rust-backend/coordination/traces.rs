use super::domain::{
    Claim, ClaimId, ClaimState, CoordinationEvent, CoordinationEventKind, CoordinationEventPayload,
    DomainError, IntegrationJobState, NotificationState,
};
use super::store::{
    ClaimRepository, EventRepository, IntegrationArtifactRepository, IntegrationJobRepository,
    NotificationRepository, SqliteCoordinationStore, StoreError, ValidationReportRepository,
    WorkerRepository,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

pub const CLAIM_TRACE_SCHEMA: &str = "golazo.claim-trace.v1";

#[derive(Debug, Error)]
pub enum TraceError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("claim {0} was not found")]
    ClaimNotFound(String),
    #[error("claim {claim_id} belongs to goal {actual_goal}, not {requested_goal}")]
    GoalMismatch {
        claim_id: String,
        requested_goal: String,
        actual_goal: String,
    },
    #[error("worker {0} referenced by the claim was not found")]
    WorkerNotFound(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimTraceStage {
    Acquisition,
    WorkerTurn,
    Notification,
    Validation,
    Integration,
    Completion,
}

impl ClaimTraceStage {
    const REQUIRED: [Self; 6] = [
        Self::Acquisition,
        Self::WorkerTurn,
        Self::Notification,
        Self::Validation,
        Self::Integration,
        Self::Completion,
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceSpanStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl TraceSpanStatus {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    fn is_failed(self) -> bool {
        self == Self::Failed
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TraceLinks {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notification_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_report_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_artifact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integration_job_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimTraceSpan {
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub stage: ClaimTraceStage,
    pub operation: String,
    pub status: TraceSpanStatus,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub source_kind: String,
    pub source_id: String,
    pub links: TraceLinks,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TracePhaseSummary {
    pub stage: ClaimTraceStage,
    pub observed_spans: usize,
    pub terminal_spans: usize,
    pub failed_spans: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimTrace {
    pub schema: String,
    pub trace_id: String,
    pub goal_id: String,
    pub claim_id: String,
    pub worker_id: String,
    pub claim_state: ClaimState,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub collected_at: DateTime<Utc>,
    pub phases: Vec<TracePhaseSummary>,
    pub spans: Vec<ClaimTraceSpan>,
}

pub struct ClaimTraceService {
    store: Arc<SqliteCoordinationStore>,
}

impl ClaimTraceService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn collect(
        &self,
        goal_id: &str,
        claim_id: &ClaimId,
        collected_at: DateTime<Utc>,
    ) -> Result<ClaimTrace, TraceError> {
        let claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| TraceError::ClaimNotFound(claim_id.as_str().into()))?;
        if claim.goal_id != goal_id {
            return Err(TraceError::GoalMismatch {
                claim_id: claim.id.as_str().into(),
                requested_goal: goal_id.into(),
                actual_goal: claim.goal_id,
            });
        }
        let worker = self
            .store
            .worker(&claim.owner)?
            .ok_or_else(|| TraceError::WorkerNotFound(claim.owner.as_str().into()))?;
        let trace_id = format!("goal:{goal_id}:claim:{}", claim.id.as_str());
        let root_span_id = format!("{trace_id}:acquisition");
        let completed_at = claim.outcome.as_ref().map(|outcome| outcome.at);
        let events = all_events_for_goal(self.store.as_ref(), goal_id)?;
        let related_events = events
            .iter()
            .filter_map(|event| {
                event_references_claim(event, claim_id)
                    .transpose()
                    .map(|result| result.map(|_| event))
            })
            .collect::<Result<Vec<_>, DomainError>>()?;
        let related_event_ids = related_events
            .iter()
            .map(|event| event.id.as_str().to_string())
            .collect::<BTreeSet<_>>();

        let mut spans = Vec::new();
        spans.push(acquisition_span(
            &claim,
            &root_span_id,
            related_events.as_slice(),
        ));

        let trace_end = completed_at.unwrap_or(collected_at);
        for turn in worker.turn_history.iter().filter(|turn| {
            turn.started_at <= trace_end
                && turn.completed_at.unwrap_or(collected_at) >= claim.metadata.created_at
        }) {
            spans.push(ClaimTraceSpan {
                span_id: format!("{trace_id}:turn:{}", turn.sequence),
                parent_span_id: Some(root_span_id.clone()),
                stage: ClaimTraceStage::WorkerTurn,
                operation: "worker.turn".into(),
                status: if turn.completed_at.is_some() {
                    TraceSpanStatus::Succeeded
                } else {
                    TraceSpanStatus::Running
                },
                started_at: turn.started_at,
                completed_at: turn.completed_at,
                source_kind: "worker_turn".into(),
                source_id: turn.sequence.to_string(),
                links: TraceLinks {
                    run_id: Some(turn.run_id.clone()),
                    thread_id: Some(turn.thread_id.clone()),
                    evidence_refs: turn.context_transfer_artifact_id.iter().cloned().collect(),
                    ..TraceLinks::default()
                },
            });
        }

        for notification in self
            .store
            .notifications_for_goal(goal_id, None)?
            .into_iter()
            .filter(|notification| {
                notification.target_worker == claim.owner
                    && (related_event_ids.contains(notification.source_event_id.as_str())
                        || notification
                            .coalesced_source_event_ids
                            .iter()
                            .any(|event_id| related_event_ids.contains(event_id.as_str())))
            })
        {
            spans.push(ClaimTraceSpan {
                span_id: format!("{trace_id}:notification:{}", notification.id.as_str()),
                parent_span_id: Some(root_span_id.clone()),
                stage: ClaimTraceStage::Notification,
                operation: format!("notification.{}", notification.purpose),
                status: notification_status(notification.state),
                started_at: notification.metadata.created_at,
                completed_at: notification_span_completed_at(&notification),
                source_kind: "worker_notification".into(),
                source_id: notification.id.as_str().into(),
                links: TraceLinks {
                    notification_id: Some(notification.id.as_str().into()),
                    event_ids: std::iter::once(notification.source_event_id.as_str().to_string())
                        .chain(
                            notification
                                .coalesced_source_event_ids
                                .iter()
                                .map(|event_id| event_id.as_str().to_string()),
                        )
                        .collect(),
                    evidence_refs: notification.evidence_refs.clone(),
                    ..TraceLinks::default()
                },
            });
        }

        let artifacts = self
            .store
            .integration_artifacts_for_goal(goal_id)?
            .into_iter()
            .filter(|artifact| artifact.claim_id == *claim_id)
            .collect::<Vec<_>>();
        let artifact_ids = artifacts
            .iter()
            .map(|artifact| artifact.id.as_str().to_string())
            .collect::<BTreeSet<_>>();

        for artifact in &artifacts {
            let reports = self.store.validation_reports_for_artifact(&artifact.id)?;
            if reports.is_empty() {
                for (index, validation) in artifact.validations.iter().enumerate() {
                    spans.push(ClaimTraceSpan {
                        span_id: format!("{trace_id}:validation:{}:{index}", artifact.id.as_str()),
                        parent_span_id: Some(root_span_id.clone()),
                        stage: ClaimTraceStage::Validation,
                        operation: "validation.evidence".into(),
                        status: if validation.succeeded {
                            TraceSpanStatus::Succeeded
                        } else {
                            TraceSpanStatus::Failed
                        },
                        started_at: validation.completed_at,
                        completed_at: Some(validation.completed_at),
                        source_kind: "integration_validation_evidence".into(),
                        source_id: format!("{}:{index}", artifact.id.as_str()),
                        links: TraceLinks {
                            integration_artifact_id: Some(artifact.id.as_str().into()),
                            evidence_refs: validation.evidence_refs.clone(),
                            ..TraceLinks::default()
                        },
                    });
                }
            } else {
                for report in reports {
                    spans.push(ClaimTraceSpan {
                        span_id: format!("{trace_id}:validation:{}", report.id.as_str()),
                        parent_span_id: Some(root_span_id.clone()),
                        stage: ClaimTraceStage::Validation,
                        operation: "validation.report".into(),
                        status: if report.passed {
                            TraceSpanStatus::Succeeded
                        } else {
                            TraceSpanStatus::Failed
                        },
                        started_at: report.metadata.created_at,
                        completed_at: Some(report.metadata.updated_at),
                        source_kind: "integration_validation_report".into(),
                        source_id: report.id.as_str().into(),
                        links: TraceLinks {
                            validation_report_id: Some(report.id.as_str().into()),
                            integration_artifact_id: Some(artifact.id.as_str().into()),
                            evidence_refs: report
                                .results
                                .iter()
                                .flat_map(|result| {
                                    [
                                        (!result.stdout_excerpt.is_empty()).then(|| {
                                            format!("validation:{}:stdout", result.gate_id)
                                        }),
                                        (!result.stderr_excerpt.is_empty()).then(|| {
                                            format!("validation:{}:stderr", result.gate_id)
                                        }),
                                    ]
                                    .into_iter()
                                    .flatten()
                                })
                                .collect(),
                            ..TraceLinks::default()
                        },
                    });
                }
            }

            spans.push(ClaimTraceSpan {
                span_id: format!("{trace_id}:artifact:{}", artifact.id.as_str()),
                parent_span_id: Some(root_span_id.clone()),
                stage: ClaimTraceStage::Integration,
                operation: "integration.artifact".into(),
                status: TraceSpanStatus::Succeeded,
                started_at: artifact.metadata.created_at,
                completed_at: Some(artifact.metadata.updated_at),
                source_kind: "integration_artifact".into(),
                source_id: artifact.id.as_str().into(),
                links: TraceLinks {
                    integration_artifact_id: Some(artifact.id.as_str().into()),
                    evidence_refs: artifact.evidence_refs.clone(),
                    ..TraceLinks::default()
                },
            });
        }

        for job in self
            .store
            .integration_jobs_for_goal(goal_id, None)?
            .into_iter()
            .filter(|job| artifact_ids.contains(job.artifact_id.as_str()))
        {
            spans.push(ClaimTraceSpan {
                span_id: format!("{trace_id}:integration-job:{}", job.id.as_str()),
                parent_span_id: Some(root_span_id.clone()),
                stage: ClaimTraceStage::Integration,
                operation: "integration.job".into(),
                status: integration_job_status(job.state),
                started_at: job.started_at.unwrap_or(job.enqueued_at),
                completed_at: job.completed_at,
                source_kind: "integration_job".into(),
                source_id: job.id.as_str().into(),
                links: TraceLinks {
                    integration_artifact_id: Some(job.artifact_id.as_str().into()),
                    integration_job_id: Some(job.id.as_str().into()),
                    ..TraceLinks::default()
                },
            });
        }

        if claim.state.is_terminal()
            || related_events
                .iter()
                .any(|event| event.kind == CoordinationEventKind::CompletionPublished)
        {
            let completion_events = related_events
                .iter()
                .filter(|event| event.kind == CoordinationEventKind::CompletionPublished)
                .copied()
                .collect::<Vec<_>>();
            let completion_at = completion_events
                .iter()
                .map(|event| event.occurred_at)
                .max()
                .or(completed_at)
                .unwrap_or(claim.metadata.updated_at);
            spans.push(ClaimTraceSpan {
                span_id: format!("{trace_id}:completion"),
                parent_span_id: Some(root_span_id.clone()),
                stage: ClaimTraceStage::Completion,
                operation: "claim.completion".into(),
                status: terminal_claim_status(claim.state),
                started_at: completion_at,
                completed_at: Some(completion_at),
                source_kind: if completion_events.is_empty() {
                    "claim_outcome".into()
                } else {
                    "coordination_event".into()
                },
                source_id: completion_events
                    .first()
                    .map(|event| event.id.as_str().to_string())
                    .unwrap_or_else(|| claim.id.as_str().into()),
                links: TraceLinks {
                    event_ids: completion_events
                        .iter()
                        .map(|event| event.id.as_str().to_string())
                        .collect(),
                    evidence_refs: claim
                        .outcome
                        .as_ref()
                        .map(|outcome| outcome.evidence_refs.clone())
                        .unwrap_or_default(),
                    integration_artifact_id: claim
                        .outcome
                        .as_ref()
                        .and_then(|outcome| outcome.artifact_id.clone()),
                    ..TraceLinks::default()
                },
            });
        }

        spans.sort_by(|left, right| {
            left.started_at
                .cmp(&right.started_at)
                .then_with(|| left.stage.cmp(&right.stage))
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
        let phases = phase_summaries(&spans);

        Ok(ClaimTrace {
            schema: CLAIM_TRACE_SCHEMA.into(),
            trace_id,
            goal_id: goal_id.into(),
            claim_id: claim.id.as_str().into(),
            worker_id: claim.owner.as_str().into(),
            claim_state: claim.state,
            started_at: claim.metadata.created_at,
            completed_at,
            collected_at,
            phases,
            spans,
        })
    }
}

fn acquisition_span(
    claim: &Claim,
    root_span_id: &str,
    related_events: &[&CoordinationEvent],
) -> ClaimTraceSpan {
    let acquisition_events = related_events
        .iter()
        .filter(|event| event.kind == CoordinationEventKind::ClaimLifecycleChanged)
        .filter(|event| {
            event
                .typed_payload()
                .ok()
                .flatten()
                .is_some_and(|payload| {
                    matches!(payload, CoordinationEventPayload::Claim(payload) if payload.to_state == ClaimState::Active)
                })
        })
        .collect::<Vec<_>>();
    ClaimTraceSpan {
        span_id: root_span_id.into(),
        parent_span_id: None,
        stage: ClaimTraceStage::Acquisition,
        operation: "claim.acquire".into(),
        status: TraceSpanStatus::Succeeded,
        started_at: claim.metadata.created_at,
        completed_at: Some(claim.metadata.created_at),
        source_kind: "claim".into(),
        source_id: claim.id.as_str().into(),
        links: TraceLinks {
            event_ids: acquisition_events
                .iter()
                .map(|event| event.id.as_str().to_string())
                .collect(),
            ..TraceLinks::default()
        },
    }
}

fn notification_span_completed_at(
    notification: &super::domain::WorkerNotification,
) -> Option<DateTime<Utc>> {
    matches!(
        notification.state,
        NotificationState::ActedOn
            | NotificationState::Expired
            | NotificationState::Superseded
            | NotificationState::Failed
    )
    .then_some(notification.metadata.updated_at)
}

fn notification_status(state: NotificationState) -> TraceSpanStatus {
    match state {
        NotificationState::Queued => TraceSpanStatus::Pending,
        NotificationState::Delivered | NotificationState::Acknowledged => TraceSpanStatus::Running,
        NotificationState::ActedOn => TraceSpanStatus::Succeeded,
        NotificationState::Failed => TraceSpanStatus::Failed,
        NotificationState::Expired | NotificationState::Superseded => TraceSpanStatus::Cancelled,
    }
}

fn integration_job_status(state: IntegrationJobState) -> TraceSpanStatus {
    match state {
        IntegrationJobState::Queued => TraceSpanStatus::Pending,
        IntegrationJobState::Running => TraceSpanStatus::Running,
        IntegrationJobState::Succeeded => TraceSpanStatus::Succeeded,
        IntegrationJobState::Failed => TraceSpanStatus::Failed,
        IntegrationJobState::Cancelled => TraceSpanStatus::Cancelled,
    }
}

fn terminal_claim_status(state: ClaimState) -> TraceSpanStatus {
    match state {
        ClaimState::Completed => TraceSpanStatus::Succeeded,
        ClaimState::Blocked | ClaimState::Expired => TraceSpanStatus::Failed,
        ClaimState::Released | ClaimState::Revoked => TraceSpanStatus::Cancelled,
        ClaimState::Active => TraceSpanStatus::Running,
    }
}

fn phase_summaries(spans: &[ClaimTraceSpan]) -> Vec<TracePhaseSummary> {
    let mut summaries = BTreeMap::new();
    for stage in ClaimTraceStage::REQUIRED {
        summaries.insert(
            stage,
            TracePhaseSummary {
                stage,
                observed_spans: 0,
                terminal_spans: 0,
                failed_spans: 0,
            },
        );
    }
    for span in spans {
        let summary = summaries
            .get_mut(&span.stage)
            .expect("required trace stage");
        summary.observed_spans += 1;
        summary.terminal_spans += usize::from(span.status.is_terminal());
        summary.failed_spans += usize::from(span.status.is_failed());
    }
    summaries.into_values().collect()
}

fn event_references_claim(
    event: &CoordinationEvent,
    claim_id: &ClaimId,
) -> Result<Option<()>, DomainError> {
    if event.correlation_id == claim_id.as_str() {
        return Ok(Some(()));
    }
    let Some(payload) = event.typed_payload()? else {
        return Ok(None);
    };
    let matches = match payload {
        CoordinationEventPayload::Activity(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::Claim(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::ContractMismatch(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::FileOverlap(payload) => {
            payload.left_claim_id == *claim_id || payload.right_claim_id == *claim_id
        }
        CoordinationEventPayload::SymbolOverlap(payload) => {
            payload.left_claim_id == *claim_id || payload.right_claim_id == *claim_id
        }
        CoordinationEventPayload::MigrationOverlap(payload) => {
            payload.left_claim_id == *claim_id || payload.right_claim_id == *claim_id
        }
        CoordinationEventPayload::MergeConflict(payload) => {
            payload.left_claim_id == *claim_id || payload.right_claim_id == *claim_id
        }
        CoordinationEventPayload::WorkerStalled(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::RepeatedValidationFailure(payload) => {
            payload.claim_id == *claim_id
        }
        CoordinationEventPayload::WatchdogAction(payload) => {
            payload.claim_ids.iter().any(|id| id == claim_id)
        }
        CoordinationEventPayload::Blocker(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::CoordinationRequest(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::Completion(payload) => payload.claim_id == *claim_id,
        CoordinationEventPayload::Contract(_)
        | CoordinationEventPayload::Dependency(_)
        | CoordinationEventPayload::HumanDecision(_)
        | CoordinationEventPayload::PrivilegedAction(_) => false,
    };
    Ok(matches.then_some(()))
}

fn all_events_for_goal(
    store: &SqliteCoordinationStore,
    goal_id: &str,
) -> Result<Vec<CoordinationEvent>, StoreError> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let page = store.events_for_goal(goal_id, after, None, 1_000)?;
        if page.is_empty() {
            break;
        }
        after = page
            .last()
            .and_then(|event| event.sequence)
            .unwrap_or(after);
        let complete = page.len() < 1_000;
        events.extend(page);
        if complete {
            break;
        }
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        ActivityCategory, ActivityEventPayload, ClaimOutcome, ClaimOutcomeKind, ClaimScope,
        CompletionEventPayload, CoordinationActor, EventSeverity, IntegrationArtifact,
        IntegrationArtifactId, IntegrationDiffSummary, IntegrationValidationReport,
        IntegrationWorkspaceSnapshot, RecordMetadata, ValidationGateKind, ValidationGateResult,
        Worker, WorkerNotification, WorkerState, WorkerTurnBinding,
    };
    use crate::coordination::integration::IntegrationQueueService;
    use chrono::{Duration, TimeZone};

    #[test]
    fn traces_every_required_claim_phase_from_durable_records() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let started_at = Utc.with_ymd_and_hms(2026, 10, 6, 12, 0, 0).unwrap();
        let completed_at = started_at + Duration::minutes(8);
        let mut worker = Worker::new("goal-a", started_at);
        worker
            .transition(WorkerState::Starting, started_at, None)
            .unwrap();
        worker
            .transition(WorkerState::Active, started_at, None)
            .unwrap();
        worker.turn_history.push(WorkerTurnBinding {
            sequence: 1,
            run_id: "run-a".into(),
            thread_id: "thread-a".into(),
            started_at: started_at + Duration::minutes(1),
            completed_at: Some(started_at + Duration::minutes(2)),
            continuation_of_thread_id: None,
            context_transfer_artifact_id: None,
        });
        store.upsert_worker(&worker).unwrap();

        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "delivery".into(),
            },
            worker.id.clone(),
            "base",
            started_at,
            started_at + Duration::minutes(30),
        )
        .unwrap();
        let artifact_id = IntegrationArtifactId::new();
        claim
            .transition(
                ClaimState::Completed,
                ClaimOutcome {
                    kind: ClaimOutcomeKind::Completed,
                    reason: "integrated".into(),
                    artifact_id: Some(artifact_id.as_str().into()),
                    evidence_refs: vec!["test:passed".into()],
                    escalation_id: None,
                    at: completed_at,
                },
            )
            .unwrap();
        store.insert_claim(&claim).unwrap();

        let acquisition = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker.id.clone(),
            },
            claim.id.as_str(),
            CoordinationEventPayload::Claim(super::super::domain::ClaimEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker.id.clone(),
                from_state: None,
                to_state: ClaimState::Active,
                reason: "claimed ready feature".into(),
                artifact_id: None,
                evidence_refs: vec![],
            }),
            started_at,
        )
        .unwrap();
        store.append_event(&acquisition).unwrap();

        let activity = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker.id.clone(),
            },
            claim.id.as_str(),
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker.id.clone(),
                claim_id: claim.id.clone(),
                category: ActivityCategory::Progress,
                summary: "implementation ready for review".into(),
                progress_percent: Some(75),
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec![],
                validation_succeeded: None,
            }),
            started_at + Duration::minutes(3),
        )
        .unwrap();
        store.append_event(&activity).unwrap();
        let mut notification = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            activity.id.clone(),
            "review_progress",
            "Review the new implementation evidence.",
            EventSeverity::Info,
            vec!["event:activity".into()],
            None,
            true,
            None,
            started_at + Duration::minutes(3),
        )
        .unwrap();
        notification
            .transition(
                NotificationState::Delivered,
                started_at + Duration::minutes(4),
            )
            .unwrap();
        notification
            .transition(
                NotificationState::ActedOn,
                started_at + Duration::minutes(5),
            )
            .unwrap();
        store.upsert_notification(&notification).unwrap();

        let artifact = IntegrationArtifact {
            metadata: RecordMetadata::new(started_at + Duration::minutes(5)),
            id: artifact_id,
            goal_id: "goal-a".into(),
            claim_id: claim.id.clone(),
            claim_generation: 1,
            worker_id: worker.id.clone(),
            repository_id: "repo-a".into(),
            base_revision: "base".into(),
            head_revision: "head".into(),
            commits: vec![],
            diff_summary: IntegrationDiffSummary {
                files_changed: 1,
                insertions: 10,
                deletions: 0,
                changed_paths: vec!["src/lib.rs".into()],
                summary: "one file changed".into(),
            },
            changed_contracts: vec![],
            migrations: vec![],
            validations: vec![],
            evidence_refs: vec!["commit:head".into()],
            known_risks: vec![],
            workspace: IntegrationWorkspaceSnapshot {
                branch: "codex/worker".into(),
                staged_paths: vec![],
                unstaged_paths: vec![],
                untracked_paths: vec![],
                conflicted_paths: vec![],
            },
        };
        store.insert_integration_artifact(&artifact).unwrap();
        let report = IntegrationValidationReport {
            metadata: RecordMetadata::new(started_at + Duration::minutes(6)),
            id: super::super::domain::ValidationReportId::new(),
            artifact_id: artifact.id.clone(),
            goal_id: "goal-a".into(),
            worker_id: worker.id.clone(),
            passed: true,
            results: vec![ValidationGateResult {
                gate_id: "unit".into(),
                kind: ValidationGateKind::Unit,
                command: "cargo test".into(),
                required: true,
                succeeded: true,
                exit_code: Some(0),
                stdout_excerpt: "ok".into(),
                stderr_excerpt: String::new(),
                started_at: started_at + Duration::minutes(5),
                completed_at: started_at + Duration::minutes(6),
            }],
        };
        store.insert_validation_report(&report).unwrap();
        let queue = IntegrationQueueService::new(Arc::clone(&store));
        let queued_job = queue
            .enqueue(&artifact.id, 0, started_at + Duration::minutes(6))
            .unwrap();
        let job = queue
            .acquire_next("repo-a", started_at + Duration::minutes(7))
            .unwrap()
            .unwrap();
        assert_eq!(job.id, queued_job.id);
        queue
            .finish(
                &job.id,
                true,
                "integrated",
                started_at + Duration::minutes(8),
            )
            .unwrap();

        let completion = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::System,
            claim.id.as_str(),
            CoordinationEventPayload::Completion(CompletionEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker.id.clone(),
                artifact_id: artifact.id.as_str().into(),
                evidence_refs: vec!["test:passed".into()],
                integration_revision: "head".into(),
            }),
            completed_at,
        )
        .unwrap();
        store.append_event(&completion).unwrap();

        let trace = ClaimTraceService::new(store)
            .collect("goal-a", &claim.id, completed_at + Duration::minutes(1))
            .unwrap();
        assert_eq!(trace.schema, CLAIM_TRACE_SCHEMA);
        assert_eq!(trace.completed_at, Some(completed_at));
        assert_eq!(trace.phases.len(), 6);
        assert!(trace.phases.iter().all(|phase| phase.observed_spans > 0));
        assert_eq!(
            trace
                .spans
                .iter()
                .find(|span| span.stage == ClaimTraceStage::WorkerTurn)
                .unwrap()
                .links
                .thread_id
                .as_deref(),
            Some("thread-a")
        );
        assert!(trace.spans.iter().any(|span| {
            span.stage == ClaimTraceStage::Validation
                && span.links.validation_report_id.as_deref() == Some(report.id.as_str())
        }));
        assert!(trace.spans.iter().any(|span| {
            span.stage == ClaimTraceStage::Integration
                && span.links.integration_job_id.as_deref() == Some(job.id.as_str())
        }));
        assert_eq!(
            trace.spans.last().map(|span| span.stage),
            Some(ClaimTraceStage::Completion)
        );
    }
}
