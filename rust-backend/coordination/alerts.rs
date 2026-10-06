use super::claims::{ClaimPolicy, ClaimService, ClaimServiceError};
use super::domain::{
    ClaimState, CoordinationEventKind, EventSeverity, IntegrationJobState, WorkerState,
};
use super::recovery::StartupRecoveryReport;
use super::store::{
    ClaimRepository, EventRepository, IntegrationJobRepository, SqliteCoordinationStore,
    StoreError, WorkerRepository,
};
use crate::observability::CorrelationIds;
use crate::redaction::redact_sensitive_text;
use crate::tracker::{Tracker, TrackerError};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

pub const OPERATIONAL_ALERT_SCHEMA: &str = "golazo.operational-alerts.v1";

#[derive(Debug, Error)]
pub enum AlertError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error(transparent)]
    Claims(#[from] ClaimServiceError),
    #[error("operational alert serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("operational alert I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("operational alert snapshot could not be persisted: {0}")]
    Persist(#[from] tempfile::PersistError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalAlertKind {
    StuckClaim,
    HeartbeatLoss,
    RepeatedFailure,
    StoreError,
    OrphanedWorkspace,
    QueueBacklog,
    IntegrationFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalAlertState {
    Active,
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationalAlert {
    pub id: String,
    pub goal_id: String,
    pub correlation_key: String,
    pub kind: OperationalAlertKind,
    pub state: OperationalAlertState,
    pub severity: EventSeverity,
    pub summary: String,
    pub recommended_action: String,
    pub entity_kind: Option<String>,
    pub entity_id: Option<String>,
    pub evidence_refs: Vec<String>,
    pub occurrence_count: u64,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationalAlertSnapshot {
    pub schema: String,
    pub goal_id: String,
    pub evaluated_at: DateTime<Utc>,
    pub active_alerts: Vec<OperationalAlert>,
    pub resolved_alerts: Vec<OperationalAlert>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AlertPolicy {
    pub stuck_claim_after_seconds: u64,
    pub heartbeat_lost_after_seconds: u64,
    pub repeated_failure_window_seconds: u64,
    pub ready_queue_threshold: usize,
    pub integration_queue_threshold: usize,
    pub integration_queue_stale_after_seconds: u64,
    pub resolved_history_limit: usize,
}

impl Default for AlertPolicy {
    fn default() -> Self {
        Self {
            stuck_claim_after_seconds: 5 * 60,
            heartbeat_lost_after_seconds: 2 * 60,
            repeated_failure_window_seconds: 60 * 60,
            ready_queue_threshold: 10,
            integration_queue_threshold: 5,
            integration_queue_stale_after_seconds: 15 * 60,
            resolved_history_limit: 500,
        }
    }
}

#[derive(Debug, Clone)]
struct AlertCandidate {
    correlation_key: String,
    kind: OperationalAlertKind,
    severity: EventSeverity,
    summary: String,
    recommended_action: String,
    entity_kind: Option<String>,
    entity_id: Option<String>,
    evidence_refs: Vec<String>,
}

pub struct OperationalAlertService {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    policy: AlertPolicy,
}

impl OperationalAlertService {
    pub fn new(tracker: Tracker, store: Arc<SqliteCoordinationStore>, policy: AlertPolicy) -> Self {
        Self {
            tracker,
            store,
            policy,
        }
    }

    pub fn evaluate_goal(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<OperationalAlertSnapshot, AlertError> {
        self.tracker.get_goal(goal_id)?;
        self.store.integrity_check()?;
        let path = alert_snapshot_path(&self.tracker.root, goal_id);
        with_alert_lock(&path, || {
            let previous = read_snapshot(&path, goal_id)?;
            let candidates = self.collect_candidates(goal_id, now)?;
            let snapshot = merge_snapshot(previous, candidates, now, &self.policy);
            write_snapshot(&path, &snapshot)?;
            Ok(snapshot)
        })
    }

    pub fn read_goal_snapshot(
        tracker_root: &Path,
        goal_id: &str,
    ) -> Result<OperationalAlertSnapshot, AlertError> {
        read_snapshot(&alert_snapshot_path(tracker_root, goal_id), goal_id)
    }

    pub fn record_store_error_for_all(
        tracker_root: &Path,
        error: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AlertError> {
        let tracker = Tracker::new(tracker_root);
        let goals = match tracker.list_goals() {
            Ok(goals) => goals,
            Err(_) => return Ok(()),
        };
        for goal_id in goals.into_iter().filter_map(|goal| {
            goal.get("goal_id")
                .and_then(|value| value.as_str())
                .map(str::to_string)
        }) {
            let path = alert_snapshot_path(tracker_root, &goal_id);
            with_alert_lock(&path, || {
                let mut snapshot = read_snapshot(&path, &goal_id)?;
                upsert_candidate(
                    &mut snapshot,
                    AlertCandidate {
                        correlation_key: "store:error".into(),
                        kind: OperationalAlertKind::StoreError,
                        severity: EventSeverity::Critical,
                        summary: "The coordination store is unavailable or failed an integrity operation."
                            .into(),
                        recommended_action:
                            "Stop autonomous execution, preserve the database, and follow the store recovery runbook before resuming."
                                .into(),
                        entity_kind: Some("coordination_store".into()),
                        entity_id: None,
                        evidence_refs: vec![redact_sensitive_text(error)],
                    },
                    now,
                );
                snapshot.evaluated_at = now;
                canonicalize_snapshot(&mut snapshot, 500);
                write_snapshot(&path, &snapshot)
            })?;
        }
        Ok(())
    }

    fn collect_candidates(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<AlertCandidate>, AlertError> {
        let workers = self.store.workers_for_goal(goal_id, None)?;
        let claims = self.store.claims_for_goal(goal_id, None)?;
        let jobs = self.store.integration_jobs_for_goal(goal_id, None)?;
        let events = all_events_for_goal(self.store.as_ref(), goal_id)?;
        let mut candidates = Vec::new();

        for worker in workers.iter().filter(|worker| {
            matches!(
                worker.state,
                WorkerState::Starting
                    | WorkerState::Active
                    | WorkerState::Waiting
                    | WorkerState::Recovering
            )
        }) {
            let heartbeat = worker
                .last_heartbeat_at
                .unwrap_or(worker.metadata.updated_at);
            if age_seconds(heartbeat, now) >= self.policy.heartbeat_lost_after_seconds {
                candidates.push(AlertCandidate {
                    correlation_key: format!("heartbeat:{}", worker.id.as_str()),
                    kind: OperationalAlertKind::HeartbeatLoss,
                    severity: EventSeverity::Error,
                    summary: format!("Worker {} stopped heartbeating.", worker.id.as_str()),
                    recommended_action:
                        "Inspect the worker process, run, thread, claim, and workspace; recover or quarantine before replacement."
                            .into(),
                    entity_kind: Some("worker".into()),
                    entity_id: Some(worker.id.as_str().into()),
                    evidence_refs: vec![format!(
                        "last_heartbeat_at:{}",
                        heartbeat.to_rfc3339()
                    )],
                });
            }
        }

        for claim in claims
            .iter()
            .filter(|claim| claim.state == ClaimState::Active)
        {
            let latest_event_at = events
                .iter()
                .filter(|event| event.correlation_id == claim.id.as_str())
                .map(|event| event.occurred_at)
                .max();
            let last_progress = latest_event_at
                .unwrap_or(claim.heartbeat_at)
                .max(claim.heartbeat_at);
            if age_seconds(last_progress, now) >= self.policy.stuck_claim_after_seconds {
                candidates.push(AlertCandidate {
                    correlation_key: format!("stuck-claim:{}", claim.id.as_str()),
                    kind: OperationalAlertKind::StuckClaim,
                    severity: EventSeverity::Error,
                    summary: format!(
                        "Claim {} has made no durable progress.",
                        claim.id.as_str()
                    ),
                    recommended_action:
                        "Inspect the claim trace and worker health; renew only with proof of liveness, otherwise preserve and reclaim the work."
                            .into(),
                    entity_kind: Some("claim".into()),
                    entity_id: Some(claim.id.as_str().into()),
                    evidence_refs: vec![format!(
                        "last_progress_at:{}",
                        last_progress.to_rfc3339()
                    )],
                });
            }
        }

        let failure_cutoff = now
            - Duration::seconds(
                self.policy
                    .repeated_failure_window_seconds
                    .min(i64::MAX as u64) as i64,
            );
        for event in events.iter().filter(|event| {
            event.occurred_at >= failure_cutoff
                && matches!(
                    event.kind,
                    CoordinationEventKind::RepeatedValidationFailure
                        | CoordinationEventKind::WatchdogActionApplied
                )
        }) {
            candidates.push(AlertCandidate {
                correlation_key: format!("repeated-failure:{}", event.correlation_id),
                kind: OperationalAlertKind::RepeatedFailure,
                severity: EventSeverity::Error,
                summary: "A worker entered a repeated failure or watchdog cutoff condition."
                    .into(),
                recommended_action:
                    "Inspect the correlated event sequence and stop automatic retries until the underlying failure changes."
                        .into(),
                entity_kind: Some("correlation".into()),
                entity_id: Some(event.correlation_id.clone()),
                evidence_refs: vec![format!("event:{}", event.id.as_str())],
            });
        }

        let ready_count = ClaimService::new(
            self.tracker.clone(),
            Arc::clone(&self.store),
            ClaimPolicy::default(),
        )
        .ready_unclaimed_scopes(goal_id)?
        .len();
        let queued_jobs = jobs
            .iter()
            .filter(|job| job.state == IntegrationJobState::Queued)
            .collect::<Vec<_>>();
        let stale_queue = queued_jobs.iter().any(|job| {
            age_seconds(job.enqueued_at, now) >= self.policy.integration_queue_stale_after_seconds
        });
        if ready_count >= self.policy.ready_queue_threshold
            || queued_jobs.len() >= self.policy.integration_queue_threshold
            || stale_queue
        {
            candidates.push(AlertCandidate {
                correlation_key: "queue:backlog".into(),
                kind: OperationalAlertKind::QueueBacklog,
                severity: EventSeverity::Warning,
                summary: format!(
                    "Work backlog exceeded policy: {ready_count} ready unit(s), {} queued integration job(s).",
                    queued_jobs.len()
                ),
                recommended_action:
                    "Inspect worker capacity, blocked dependencies, and integration-lane throughput before increasing concurrency."
                        .into(),
                entity_kind: Some("goal".into()),
                entity_id: Some(goal_id.into()),
                evidence_refs: vec![
                    format!("ready_units:{ready_count}"),
                    format!("queued_integrations:{}", queued_jobs.len()),
                ],
            });
        }

        let succeeded_artifacts = jobs
            .iter()
            .filter(|job| job.state == IntegrationJobState::Succeeded)
            .map(|job| job.artifact_id.as_str())
            .collect::<BTreeSet<_>>();
        for job in jobs.iter().filter(|job| {
            job.state == IntegrationJobState::Failed
                && !succeeded_artifacts.contains(job.artifact_id.as_str())
        }) {
            candidates.push(AlertCandidate {
                correlation_key: format!("integration-failed:{}", job.artifact_id.as_str()),
                kind: OperationalAlertKind::IntegrationFailure,
                severity: EventSeverity::Error,
                summary: format!(
                    "Integration job {} failed without a successful retry.",
                    job.id.as_str()
                ),
                recommended_action:
                    "Inspect validation, reconciliation, and preserved workspace evidence; retry or escalate the integration lane."
                        .into(),
                entity_kind: Some("integration_job".into()),
                entity_id: Some(job.id.as_str().into()),
                evidence_refs: vec![format!("artifact:{}", job.artifact_id.as_str())],
            });
        }

        let recovery_path = self.tracker.root.join("startup-recovery.json");
        if let Ok(bytes) = std::fs::read(&recovery_path)
            && let Ok(report) = serde_json::from_slice::<StartupRecoveryReport>(&bytes)
            && report
                .pool
                .recovered_goal_ids
                .iter()
                .any(|id| id == goal_id)
            && let Some(reconciliation) = report.workspaces.reconciliation
        {
            for entry in reconciliation.orphaned_worktrees {
                candidates.push(AlertCandidate {
                    correlation_key: format!("orphaned-workspace:{}", entry.path),
                    kind: OperationalAlertKind::OrphanedWorkspace,
                    severity: EventSeverity::Error,
                    summary: "Startup reconciliation found an orphaned managed worktree."
                        .into(),
                    recommended_action:
                        "Review the quarantine record and preserve or integrate the branch before any cleanup."
                            .into(),
                    entity_kind: Some("workspace".into()),
                    entity_id: Some(entry.path.clone()),
                    evidence_refs: vec![format!(
                        "branch:{}",
                        entry.branch.unwrap_or_default()
                    )],
                });
            }
            for path in reconciliation.unregistered_directories {
                candidates.push(AlertCandidate {
                    correlation_key: format!("unregistered-workspace:{path}"),
                    kind: OperationalAlertKind::OrphanedWorkspace,
                    severity: EventSeverity::Warning,
                    summary: "Startup reconciliation found an unregistered workspace directory."
                        .into(),
                    recommended_action:
                        "Inspect the directory and attach or quarantine it before cleanup.".into(),
                    entity_kind: Some("workspace".into()),
                    entity_id: Some(path),
                    evidence_refs: vec![],
                });
            }
        }
        candidates.sort_by(|left, right| left.correlation_key.cmp(&right.correlation_key));
        candidates.dedup_by(|left, right| left.correlation_key == right.correlation_key);
        Ok(candidates)
    }
}

fn merge_snapshot(
    previous: OperationalAlertSnapshot,
    candidates: Vec<AlertCandidate>,
    now: DateTime<Utc>,
    policy: &AlertPolicy,
) -> OperationalAlertSnapshot {
    let mut active = previous
        .active_alerts
        .into_iter()
        .map(|alert| (alert.correlation_key.clone(), alert))
        .collect::<BTreeMap<_, _>>();
    let observed = candidates
        .iter()
        .map(|candidate| candidate.correlation_key.clone())
        .collect::<BTreeSet<_>>();
    let mut resolved = previous.resolved_alerts;
    for key in active.keys().cloned().collect::<Vec<_>>() {
        if !observed.contains(&key) {
            let mut alert = active.remove(&key).expect("active alert key");
            alert.state = OperationalAlertState::Resolved;
            alert.resolved_at = Some(now);
            alert.last_seen_at = now;
            resolved.push(alert);
        }
    }
    let mut snapshot = OperationalAlertSnapshot {
        schema: OPERATIONAL_ALERT_SCHEMA.into(),
        goal_id: previous.goal_id,
        evaluated_at: now,
        active_alerts: active.into_values().collect(),
        resolved_alerts: resolved,
    };
    for candidate in candidates {
        upsert_candidate(&mut snapshot, candidate, now);
    }
    canonicalize_snapshot(&mut snapshot, policy.resolved_history_limit);
    snapshot
}

fn upsert_candidate(
    snapshot: &mut OperationalAlertSnapshot,
    candidate: AlertCandidate,
    now: DateTime<Utc>,
) {
    if let Some(existing) = snapshot
        .active_alerts
        .iter_mut()
        .find(|alert| alert.correlation_key == candidate.correlation_key)
    {
        existing.kind = candidate.kind;
        existing.severity = candidate.severity;
        existing.summary = candidate.summary;
        existing.recommended_action = candidate.recommended_action;
        existing.entity_kind = candidate.entity_kind;
        existing.entity_id = candidate.entity_id;
        existing.evidence_refs = candidate.evidence_refs;
        existing.last_seen_at = now;
        existing.occurrence_count = existing.occurrence_count.saturating_add(1);
        return;
    }
    CorrelationIds::for_goal(&snapshot.goal_id).emit_warn("operational.alert", "active");
    snapshot.active_alerts.push(OperationalAlert {
        id: stable_alert_id(&snapshot.goal_id, &candidate.correlation_key),
        goal_id: snapshot.goal_id.clone(),
        correlation_key: candidate.correlation_key,
        kind: candidate.kind,
        state: OperationalAlertState::Active,
        severity: candidate.severity,
        summary: candidate.summary,
        recommended_action: candidate.recommended_action,
        entity_kind: candidate.entity_kind,
        entity_id: candidate.entity_id,
        evidence_refs: candidate.evidence_refs,
        occurrence_count: 1,
        first_seen_at: now,
        last_seen_at: now,
        resolved_at: None,
    });
}

fn canonicalize_snapshot(snapshot: &mut OperationalAlertSnapshot, resolved_limit: usize) {
    snapshot.active_alerts.sort_by(|left, right| {
        right
            .severity
            .rank()
            .cmp(&left.severity.rank())
            .then_with(|| left.correlation_key.cmp(&right.correlation_key))
    });
    snapshot.resolved_alerts.sort_by(|left, right| {
        right
            .resolved_at
            .cmp(&left.resolved_at)
            .then_with(|| left.correlation_key.cmp(&right.correlation_key))
    });
    snapshot.resolved_alerts.truncate(resolved_limit);
}

fn stable_alert_id(goal_id: &str, correlation_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(goal_id.as_bytes());
    hasher.update([0]);
    hasher.update(correlation_key.as_bytes());
    format!("alert-{:x}", hasher.finalize())[..22].into()
}

fn alert_snapshot_path(tracker_root: &Path, goal_id: &str) -> PathBuf {
    tracker_root.join(goal_id).join("operational-alerts.json")
}

fn read_snapshot(path: &Path, goal_id: &str) -> Result<OperationalAlertSnapshot, AlertError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(OperationalAlertSnapshot {
                schema: OPERATIONAL_ALERT_SCHEMA.into(),
                goal_id: goal_id.into(),
                evaluated_at: Utc::now(),
                active_alerts: vec![],
                resolved_alerts: vec![],
            })
        }
        Err(error) => Err(error.into()),
    }
}

fn write_snapshot(path: &Path, snapshot: &OperationalAlertSnapshot) -> Result<(), AlertError> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "alert path has no parent")
    })?;
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, snapshot)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}

fn with_alert_lock<T>(
    snapshot_path: &Path,
    operation: impl FnOnce() -> Result<T, AlertError>,
) -> Result<T, AlertError> {
    let parent = snapshot_path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "alert path has no parent")
    })?;
    std::fs::create_dir_all(parent)?;
    let lock = OpenOptions::new()
        .create(true)
        .append(true)
        .open(parent.join(".operational-alerts.lock"))?;
    lock.lock_exclusive()?;
    let result = operation();
    let _ = lock.unlock();
    result
}

fn all_events_for_goal(
    store: &SqliteCoordinationStore,
    goal_id: &str,
) -> Result<Vec<super::domain::CoordinationEvent>, StoreError> {
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

fn age_seconds(start: DateTime<Utc>, end: DateTime<Utc>) -> u64 {
    (end - start).num_seconds().max(0) as u64
}

pub fn spawn_operational_alert_monitor(tracker_root: Arc<RwLock<PathBuf>>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let root = tracker_root.read().await.clone();
            let tracker = Tracker::new(&root);
            let goals = match tracker.list_goals() {
                Ok(goals) => goals,
                Err(error) => {
                    tracing::warn!(
                        target: "golazo_backend::operation",
                        operation = "operational.alert.scan",
                        outcome = "tracker_unavailable",
                        error = %error,
                    );
                    continue;
                }
            };
            let store = match SqliteCoordinationStore::open(root.join("coordination.sqlite")) {
                Ok(store) => Arc::new(store),
                Err(error) => {
                    let _ = OperationalAlertService::record_store_error_for_all(
                        &root,
                        &error.to_string(),
                        Utc::now(),
                    );
                    continue;
                }
            };
            let service = OperationalAlertService::new(tracker, store, AlertPolicy::default());
            for goal_id in goals.into_iter().filter_map(|goal| {
                goal.get("goal_id")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            }) {
                if let Err(error) = service.evaluate_goal(&goal_id, Utc::now())
                    && matches!(error, AlertError::Store(_))
                {
                    let _ = OperationalAlertService::record_store_error_for_all(
                        &root,
                        &error.to_string(),
                        Utc::now(),
                    );
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        Claim, ClaimScope, CoordinationActor, CoordinationEvent, CoordinationEventPayload, EventId,
        IntegrationArtifact, IntegrationArtifactId, IntegrationDiffSummary,
        IntegrationWorkspaceSnapshot, RecordMetadata, RepeatedValidationFailureEventPayload,
        Worker,
    };
    use crate::coordination::integration::IntegrationQueueService;
    use crate::coordination::store::IntegrationArtifactRepository;
    use crate::models::Status;

    #[test]
    fn evaluates_deduplicates_and_resolves_required_alert_families() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "alerts").unwrap();
        for index in 0..11 {
            tracker
                .add_feature(
                    "goal-a",
                    &format!("feature-{index}"),
                    &format!("Feature {index}"),
                    "alerts",
                    Status::Planned,
                )
                .unwrap();
        }
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("goals/coordination.sqlite"))
                .unwrap(),
        );
        let now = Utc::now();
        let stale_at = now - Duration::minutes(20);
        let mut worker = Worker::new("goal-a", stale_at);
        worker
            .transition(WorkerState::Starting, stale_at, None)
            .unwrap();
        worker
            .transition(WorkerState::Active, stale_at, None)
            .unwrap();
        worker.last_heartbeat_at = Some(stale_at);
        store.upsert_worker(&worker).unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "feature-0".into(),
            },
            worker.id.clone(),
            "base",
            stale_at,
            now + Duration::minutes(5),
        )
        .unwrap();
        store.insert_claim(&claim).unwrap();
        let failure = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Error,
            CoordinationActor::System,
            claim.id.as_str(),
            CoordinationEventPayload::RepeatedValidationFailure(
                RepeatedValidationFailureEventPayload {
                    worker_id: worker.id.clone(),
                    claim_id: claim.id.clone(),
                    failure_event_ids: vec![EventId::new(), EventId::new()],
                    last_failure_at: stale_at,
                    window_seconds: 300,
                    threshold: 2,
                },
            ),
            stale_at,
        )
        .unwrap();
        store.append_event(&failure).unwrap();

        let artifact = IntegrationArtifact {
            metadata: RecordMetadata::new(stale_at),
            id: IntegrationArtifactId::new(),
            goal_id: "goal-a".into(),
            claim_id: claim.id.clone(),
            claim_generation: 1,
            worker_id: worker.id.clone(),
            repository_id: "repo-a".into(),
            base_revision: "base".into(),
            head_revision: "head".into(),
            commits: vec![],
            diff_summary: IntegrationDiffSummary {
                files_changed: 0,
                insertions: 0,
                deletions: 0,
                changed_paths: vec![],
                summary: "no changes".into(),
            },
            changed_contracts: vec![],
            migrations: vec![],
            validations: vec![],
            evidence_refs: vec![],
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
        let queue = IntegrationQueueService::new(Arc::clone(&store));
        let failed_job = queue.enqueue(&artifact.id, 0, stale_at).unwrap();
        let running = queue.acquire_next("repo-a", stale_at).unwrap().unwrap();
        queue
            .finish(&running.id, false, "failed", stale_at)
            .unwrap();
        std::fs::write(
            tracker.root.join("startup-recovery.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "schema": "golazo.startup-recovery.v1",
                "startedAt": stale_at,
                "completedAt": stale_at,
                "tracker": {
                    "goalIds": ["goal-a"],
                    "validatedGoalIds": ["goal-a"],
                    "invalidGoals": []
                },
                "runner": {
                    "interruptedRunIds": [],
                    "liveProcessRunIds": [],
                    "localThreadIds": [],
                    "nativeThreadIds": [],
                    "threadInventoryError": null
                },
                "workspaces": {
                    "provider": "git_worktrees",
                    "reconciliation": {
                        "attachedWorkerIds": [],
                        "missingWorkerIds": [],
                        "orphanedWorktrees": [{
                            "path": "/tmp/orphaned-worker",
                            "headRevision": "head",
                            "branch": "codex/orphaned-worker",
                            "prunable": false
                        }],
                        "unregisteredDirectories": []
                    },
                    "quarantineRecords": [],
                    "existingWorkspacePaths": [],
                    "error": null
                },
                "pool": {
                    "recoveredGoalIds": ["goal-a"],
                    "quarantinedWorkerIds": [],
                    "removedStaleClaimBindings": 0
                },
                "expiredClaimIds": [],
                "requeuedIntegrationJobIds": [],
                "warnings": []
            }))
            .unwrap(),
        )
        .unwrap();

        let service = OperationalAlertService::new(
            tracker.clone(),
            Arc::clone(&store),
            AlertPolicy::default(),
        );
        let first = service.evaluate_goal("goal-a", now).unwrap();
        for kind in [
            OperationalAlertKind::StuckClaim,
            OperationalAlertKind::HeartbeatLoss,
            OperationalAlertKind::RepeatedFailure,
            OperationalAlertKind::OrphanedWorkspace,
            OperationalAlertKind::QueueBacklog,
            OperationalAlertKind::IntegrationFailure,
        ] {
            assert!(
                first.active_alerts.iter().any(|alert| alert.kind == kind),
                "missing {kind:?}; active kinds: {:?}",
                first
                    .active_alerts
                    .iter()
                    .map(|alert| alert.kind)
                    .collect::<Vec<_>>()
            );
        }
        let second = service
            .evaluate_goal("goal-a", now + Duration::seconds(1))
            .unwrap();
        assert_eq!(first.active_alerts.len(), second.active_alerts.len());
        assert!(
            second
                .active_alerts
                .iter()
                .all(|alert| alert.occurrence_count == 2)
        );

        OperationalAlertService::record_store_error_for_all(
            &tracker.root,
            "database unavailable",
            now,
        )
        .unwrap();
        let with_store =
            OperationalAlertService::read_goal_snapshot(&tracker.root, "goal-a").unwrap();
        assert!(with_store.active_alerts.iter().any(|alert| {
            alert.kind == OperationalAlertKind::StoreError && !alert.recommended_action.is_empty()
        }));

        let mut finished = store.claim(&claim.id).unwrap().unwrap();
        finished.state = ClaimState::Completed;
        finished.metadata.updated_at = now + Duration::minutes(1);
        store.upsert_claim(&finished).unwrap();
        let retry =
            super::super::domain::IntegrationJob::new(&artifact, 1, now + Duration::minutes(1));
        // The active-entry constraint is released after the failed job.
        store.enqueue_integration_job(&retry).unwrap();
        let retry = queue
            .acquire_next("repo-a", now + Duration::minutes(1))
            .unwrap()
            .unwrap();
        queue
            .finish(&retry.id, true, "recovered", now + Duration::minutes(2))
            .unwrap();
        assert_eq!(failed_job.id, running.id);
        let resolved = service
            .evaluate_goal("goal-a", now + Duration::minutes(3))
            .unwrap();
        assert!(resolved.resolved_alerts.iter().any(|alert| {
            alert.kind == OperationalAlertKind::StoreError
                || alert.kind == OperationalAlertKind::IntegrationFailure
        }));
        assert!(
            resolved
                .active_alerts
                .iter()
                .all(|alert| alert.id.starts_with("alert-"))
        );
    }
}
