use super::domain::*;
use super::store::{
    ClaimRepository, ContractRepository, EscalationRepository, EventRepository,
    IntegrationArtifactRepository, IntegrationFinalizationRepository, IntegrationJobRepository,
    IntegrationMaintenanceRepository, NotificationRepository, ReconciliationRepository,
    SignalRepository, SqliteCoordinationStore, StoreError, ValidationReportRepository,
    WorkerRepository,
};
use super::workspace::{CleanupAuthorization, WorkspaceError, WorktreeManager};
use crate::models::Status;
use crate::tracker::Tracker;
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct CaptureIntegrationArtifactRequest {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub changed_contracts: Vec<IntegrationContractChange>,
    pub migrations: Vec<IntegrationMigration>,
    pub validations: Vec<IntegrationValidationEvidence>,
    pub evidence_refs: Vec<String>,
    pub known_risks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SynchronizationPhase {
    Implementation,
    Validation,
    Integration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SynchronizationDisposition {
    Required,
    Deferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SynchronizationCauseKind {
    IntegrationBoundary,
    DependencyChanged,
    ContractInvalidated,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SynchronizationCause {
    pub kind: SynchronizationCauseKind,
    pub reference: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SynchronizationDecision {
    pub phase: SynchronizationPhase,
    pub disposition: SynchronizationDisposition,
    pub causes: Vec<SynchronizationCause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreflightCheckKind {
    ClaimOwnership,
    ClaimLease,
    RepositoryRevision,
    RepositoryState,
    ContractRevisions,
    OverlapSignals,
    PredictedConflicts,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreflightCheck {
    pub kind: PreflightCheckKind,
    pub passed: bool,
    pub summary: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationPreflight {
    pub artifact_id: IntegrationArtifactId,
    pub passed: bool,
    pub checks: Vec<PreflightCheck>,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationGateSpec {
    pub id: String,
    pub kind: ValidationGateKind,
    pub program: String,
    pub args: Vec<String>,
    pub required: bool,
    pub max_output_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegenerationCommand {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationRequest {
    pub artifact_id: IntegrationArtifactId,
    pub strategy: ReconciliationStrategy,
    pub target_revision: String,
    pub commit_revisions: Vec<String>,
    pub regeneration: Option<RegenerationCommand>,
    pub manual_instructions: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IntegrationFinalizationRequest {
    pub job_id: IntegrationJobId,
    pub validation_report_id: ValidationReportId,
    pub tracker_feature_id: String,
    pub tracker_step_ids: Vec<String>,
    pub tracker_summary: String,
    pub tracker_evidence: Vec<String>,
    pub tracker_status: Status,
    pub integration_revision: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationFailureKind {
    Preflight,
    Validation,
    Reconciliation,
    Integration,
    Infrastructure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationFailureDisposition {
    ReturnToWorker,
    Escalate,
}

#[derive(Debug, Clone)]
pub struct IntegrationFailureRequest {
    pub job_id: IntegrationJobId,
    pub kind: IntegrationFailureKind,
    pub disposition: IntegrationFailureDisposition,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub recommended_action: String,
}

#[derive(Debug, Clone)]
pub struct IntegrationFailureOutcome {
    pub job: IntegrationJob,
    pub worker: Worker,
    pub claim: Claim,
    pub notification: Option<WorkerNotification>,
    pub escalation: Option<HumanEscalation>,
    pub event: CoordinationEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationPolicy {
    pub allowed_strategies: Vec<ReconciliationStrategy>,
    pub regeneration_programs: Vec<String>,
    pub max_output_bytes: usize,
}

impl Default for ReconciliationPolicy {
    fn default() -> Self {
        Self {
            allowed_strategies: vec![
                ReconciliationStrategy::Rebase,
                ReconciliationStrategy::Merge,
                ReconciliationStrategy::CherryPick,
                ReconciliationStrategy::Manual,
            ],
            regeneration_programs: Vec::new(),
            max_output_bytes: 64 * 1_024,
        }
    }
}

#[derive(Debug, Error)]
pub enum IntegrationArtifactError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("claim was not found: {0}")]
    ClaimNotFound(String),
    #[error("integration artifact was not found: {0}")]
    ArtifactNotFound(String),
    #[error("worker was not found: {0}")]
    WorkerNotFound(String),
    #[error("claim is not active")]
    ClaimNotActive,
    #[error("worker does not own the claim")]
    ClaimOwnerMismatch,
    #[error("worker and claim belong to different goals")]
    GoalMismatch,
    #[error("worker has no isolated workspace")]
    WorkspaceMissing,
    #[error("claim and workspace base revisions differ")]
    BaseRevisionMismatch,
    #[error("Git command failed: {command}: {message}")]
    GitFailed { command: String, message: String },
    #[error("integration preflight did not pass")]
    PreflightFailed,
    #[error("invalid validation gate configuration: {0}")]
    InvalidValidationGate(String),
    #[error("reconciliation strategy is not allowed by repository policy")]
    ReconciliationNotAllowed,
    #[error("invalid reconciliation request: {0}")]
    InvalidReconciliation(String),
    #[error("tracker finalization failed: {0}")]
    Tracker(String),
}

#[derive(Clone)]
pub struct IntegrationArtifactService {
    store: Arc<SqliteCoordinationStore>,
}

#[derive(Clone)]
pub struct SynchronizationPolicyService {
    store: Arc<SqliteCoordinationStore>,
}

#[derive(Clone)]
pub struct IntegrationPreflightService {
    store: Arc<SqliteCoordinationStore>,
}

#[derive(Clone)]
pub struct ValidationGateRunner {
    store: Arc<SqliteCoordinationStore>,
}

#[derive(Clone)]
pub struct ReconciliationService {
    store: Arc<SqliteCoordinationStore>,
    policy: ReconciliationPolicy,
}

#[derive(Clone)]
pub struct IntegrationQueueService {
    store: Arc<SqliteCoordinationStore>,
}

#[derive(Clone)]
pub struct IntegrationFinalizationService {
    store: Arc<SqliteCoordinationStore>,
    tracker: Tracker,
}

#[derive(Clone)]
pub struct IntegrationFailureService {
    store: Arc<SqliteCoordinationStore>,
}

#[derive(Clone)]
pub struct IntegrationMaintenanceService {
    store: Arc<SqliteCoordinationStore>,
}

impl IntegrationMaintenanceService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn rollback(
        &self,
        finalization_id: &IntegrationFinalizationId,
        requested_by: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<IntegrationMaintenanceRecord, IntegrationArtifactError> {
        validate_maintenance_request(requested_by, reason)?;
        let (finalization, artifact, worker) = self.maintenance_context(finalization_id)?;
        if let Some(existing) = self.existing_record(
            &artifact.id,
            &finalization.id,
            IntegrationMaintenanceKind::Rollback,
        )? {
            return Ok(existing);
        }
        let binding = worker
            .workspace
            .as_ref()
            .ok_or(IntegrationArtifactError::WorkspaceMissing)?;
        let repository = Path::new(&binding.canonical_repository_path);
        let previous_head = git_output(repository, &["rev-parse", "HEAD"])?
            .trim()
            .to_string();
        if previous_head != finalization.integration_revision {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "rollback requires repository HEAD to match the finalized integration revision"
                    .into(),
            ));
        }
        let id = IntegrationMaintenanceId::new();
        let backup_ref = format!("refs/golazo-backups/rollback/{}", id.as_str());
        git_output(repository, &["update-ref", &backup_ref, &previous_head])?;
        let output = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(["revert", "--no-edit", &finalization.integration_revision])
            .output()?;
        let error = (!output.status.success()).then(|| bounded_output(&output.stderr, 16_384));
        if error.is_some() {
            let _ = Command::new("git")
                .arg("-C")
                .arg(repository)
                .args(["revert", "--abort"])
                .output();
        }
        let resulting_head = git_output(repository, &["rev-parse", "HEAD"])?
            .trim()
            .to_string();
        let record = IntegrationMaintenanceRecord {
            metadata: RecordMetadata::new(now),
            id,
            finalization_id: finalization.id,
            artifact_id: artifact.id,
            goal_id: artifact.goal_id,
            worker_id: worker.id,
            kind: IntegrationMaintenanceKind::Rollback,
            state: if error.is_none() {
                IntegrationMaintenanceState::Succeeded
            } else {
                IntegrationMaintenanceState::Failed
            },
            requested_by: requested_by.into(),
            reason: reason.into(),
            previous_repository_head: previous_head,
            resulting_repository_head: resulting_head,
            backup_ref: Some(backup_ref),
            retained_branch: binding.branch.clone(),
            cleanup_record_path: None,
            artifact_preserved: true,
            error,
        };
        self.store.insert_integration_maintenance(&record)?;
        Ok(record)
    }

    pub fn cleanup_after_integration(
        &self,
        finalization_id: &IntegrationFinalizationId,
        now: DateTime<Utc>,
    ) -> Result<IntegrationMaintenanceRecord, IntegrationArtifactError> {
        let (finalization, artifact, worker) = self.maintenance_context(finalization_id)?;
        if let Some(existing) = self.existing_record(
            &artifact.id,
            &finalization.id,
            IntegrationMaintenanceKind::Cleanup,
        )? {
            return Ok(existing);
        }
        let binding = worker
            .workspace
            .as_ref()
            .ok_or(IntegrationArtifactError::WorkspaceMissing)?;
        let manager = WorktreeManager::open(&binding.canonical_repository_path)?;
        let repository_head = git_output(manager.repository_root(), &["rev-parse", "HEAD"])?
            .trim()
            .to_string();
        let cleanup = manager.cleanup(
            binding,
            CleanupAuthorization::Integrated {
                integration_artifact_id: artifact.id.as_str().into(),
                expected_head_revision: artifact.head_revision.clone(),
            },
            now,
        )?;
        let record = IntegrationMaintenanceRecord {
            metadata: RecordMetadata::new(now),
            id: IntegrationMaintenanceId::new(),
            finalization_id: finalization.id,
            artifact_id: artifact.id,
            goal_id: artifact.goal_id,
            worker_id: worker.id,
            kind: IntegrationMaintenanceKind::Cleanup,
            state: IntegrationMaintenanceState::Succeeded,
            requested_by: "system".into(),
            reason: "validated integration workspace cleanup".into(),
            previous_repository_head: repository_head.clone(),
            resulting_repository_head: repository_head,
            backup_ref: None,
            retained_branch: cleanup.retained_branch,
            cleanup_record_path: Some(cleanup.record_path),
            artifact_preserved: true,
            error: None,
        };
        self.store.insert_integration_maintenance(&record)?;
        Ok(record)
    }

    fn maintenance_context(
        &self,
        finalization_id: &IntegrationFinalizationId,
    ) -> Result<(IntegrationFinalization, IntegrationArtifact, Worker), IntegrationArtifactError>
    {
        let finalization = self
            .store
            .integration_finalization(finalization_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(finalization_id.as_str().into())
            })?;
        if finalization.state != IntegrationFinalizationState::Completed {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "rollback and cleanup require a completed finalization".into(),
            ));
        }
        let artifact = self
            .store
            .integration_artifact(&finalization.artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(finalization.artifact_id.as_str().into())
            })?;
        let worker = self.store.worker(&finalization.worker_id)?.ok_or_else(|| {
            IntegrationArtifactError::WorkerNotFound(finalization.worker_id.as_str().into())
        })?;
        Ok((finalization, artifact, worker))
    }

    fn existing_record(
        &self,
        artifact_id: &IntegrationArtifactId,
        finalization_id: &IntegrationFinalizationId,
        kind: IntegrationMaintenanceKind,
    ) -> Result<Option<IntegrationMaintenanceRecord>, IntegrationArtifactError> {
        Ok(self
            .store
            .integration_maintenance_for_artifact(artifact_id)?
            .into_iter()
            .find(|record| record.finalization_id == *finalization_id && record.kind == kind))
    }
}

impl IntegrationFailureService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn handle(
        &self,
        request: IntegrationFailureRequest,
        now: DateTime<Utc>,
    ) -> Result<IntegrationFailureOutcome, IntegrationArtifactError> {
        if request.summary.trim().is_empty()
            || request.summary.chars().count() > 2_000
            || request.recommended_action.trim().is_empty()
            || request.recommended_action.chars().count() > 2_000
            || request.evidence_refs.len() > 32
            || request
                .evidence_refs
                .iter()
                .any(|reference| reference.trim().is_empty() || reference.chars().count() > 1_000)
        {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "integration failures require bounded summary, evidence, and an actionable next step"
                    .into(),
            ));
        }
        let job = self
            .store
            .integration_job(&request.job_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(request.job_id.as_str().into())
            })?;
        if job.state != IntegrationJobState::Running {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "only a running integration job can fail".into(),
            ));
        }
        let artifact = self
            .store
            .integration_artifact(&job.artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(job.artifact_id.as_str().into())
            })?;
        let mut escalation = None;
        if request.disposition == IntegrationFailureDisposition::Escalate {
            let mut record = HumanEscalation::new(
                artifact.goal_id.clone(),
                EscalationKind::HardBlocker,
                EscalationSeverity::High,
                EscalationScope::IntegrationLane {
                    repository_id: artifact.repository_id.clone(),
                },
                request.summary.clone(),
                now,
            );
            record.evidence_refs = request.evidence_refs.clone();
            record.options = vec![
                RemediationOption {
                    id: "retry-after-remediation".into(),
                    action: RemediationAction::ReconcileNow,
                    label: "Remediate and retry".into(),
                    description: request.recommended_action.clone(),
                    consequences: vec!["The preserved worker workspace will be reused.".into()],
                    recommended: true,
                },
                RemediationOption {
                    id: "pause-integration-lane".into(),
                    action: RemediationAction::PauseScope,
                    label: "Pause integration".into(),
                    description: "Keep the integration lane blocked for manual inspection.".into(),
                    consequences: vec![
                        "No work is deleted and the original evidence remains available.".into(),
                    ],
                    recommended: false,
                },
            ];
            escalation = Some(record);
        }
        let event = CoordinationEvent::from_typed_payload(
            artifact.goal_id.clone(),
            if escalation.is_some() {
                EventSeverity::Error
            } else {
                EventSeverity::Warning
            },
            CoordinationActor::System,
            artifact.claim_id.as_str(),
            CoordinationEventPayload::Blocker(BlockerEventPayload {
                claim_id: artifact.claim_id.clone(),
                worker_id: artifact.worker_id.clone(),
                summary: request.summary.clone(),
                evidence_refs: request.evidence_refs.clone(),
                escalation_id: escalation.as_ref().map(|record| record.id.clone()),
            }),
            now,
        )?;
        let notification = if escalation.is_none() {
            Some(WorkerNotification::new(
                artifact.goal_id.clone(),
                artifact.worker_id.clone(),
                event.id.clone(),
                format!("integration_{:?}_failed", request.kind).to_lowercase(),
                request.summary.clone(),
                EventSeverity::Warning,
                request.evidence_refs.clone(),
                Some(request.recommended_action.clone()),
                true,
                None,
                now,
            )?)
        } else {
            None
        };
        let stored = self.store.record_integration_failure(
            &job.id,
            &request.summary,
            notification.as_ref(),
            escalation.as_ref(),
            &event,
            now,
        )?;
        Ok(IntegrationFailureOutcome {
            job: stored.job,
            worker: stored.worker,
            claim: stored.claim,
            notification,
            escalation,
            event,
        })
    }
}

impl IntegrationFinalizationService {
    pub fn new(store: Arc<SqliteCoordinationStore>, tracker: Tracker) -> Self {
        Self { store, tracker }
    }

    pub fn finalize(
        &self,
        request: IntegrationFinalizationRequest,
        now: DateTime<Utc>,
    ) -> Result<IntegrationFinalization, IntegrationArtifactError> {
        let job = self
            .store
            .integration_job(&request.job_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(request.job_id.as_str().into())
            })?;
        if job.state != IntegrationJobState::Running {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "only a running integration job can finalize".into(),
            ));
        }
        let artifact = self
            .store
            .integration_artifact(&job.artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(job.artifact_id.as_str().into())
            })?;
        let report = self
            .store
            .validation_report(&request.validation_report_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::InvalidValidationGate(
                    "validation report was not found".into(),
                )
            })?;
        if !report.passed || report.artifact_id != artifact.id {
            return Err(IntegrationArtifactError::InvalidValidationGate(
                "a passing validation report for the queued artifact is required".into(),
            ));
        }
        if request.tracker_summary.trim().is_empty()
            || request.integration_revision.trim().is_empty()
        {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "tracker summary and integration revision are required".into(),
            ));
        }
        if request.tracker_status == Status::Done && request.tracker_step_ids.is_empty() {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "a completed tracker step is required when finalizing as Done".into(),
            ));
        }
        let status = match request.tracker_status {
            Status::Partial => "Partial",
            Status::Done => "Done",
            _ => {
                return Err(IntegrationArtifactError::InvalidReconciliation(
                    "finalization tracker status must be Partial or Done".into(),
                ));
            }
        };
        let finalization = IntegrationFinalization {
            metadata: RecordMetadata::new(now),
            id: IntegrationFinalizationId::new(),
            job_id: job.id,
            artifact_id: artifact.id,
            validation_report_id: report.id,
            goal_id: artifact.goal_id,
            worker_id: artifact.worker_id,
            claim_id: artifact.claim_id,
            tracker_feature_id: request.tracker_feature_id,
            tracker_step_ids: request.tracker_step_ids,
            tracker_summary: request.tracker_summary,
            tracker_evidence: request.tracker_evidence,
            tracker_status: status.into(),
            integration_revision: request.integration_revision,
            completion_event_id: EventId::new(),
            state: IntegrationFinalizationState::Prepared,
            completed_at: None,
        };
        self.store.insert_integration_finalization(&finalization)?;
        self.apply_prepared(finalization, now)
    }

    pub fn recover_prepared(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<IntegrationFinalization>, IntegrationArtifactError> {
        self.store
            .prepared_integration_finalizations()?
            .into_iter()
            .map(|finalization| self.apply_prepared(finalization, now))
            .collect()
    }

    fn apply_prepared(
        &self,
        finalization: IntegrationFinalization,
        now: DateTime<Utc>,
    ) -> Result<IntegrationFinalization, IntegrationArtifactError> {
        let status = match finalization.tracker_status.as_str() {
            "Partial" => Status::Partial,
            "Done" => Status::Done,
            _ => {
                return Err(IntegrationArtifactError::InvalidReconciliation(
                    "prepared finalization contains an invalid tracker status".into(),
                ));
            }
        };
        self.tracker
            .apply_integration_finalization(
                &finalization.goal_id,
                &finalization.tracker_feature_id,
                &finalization.tracker_step_ids,
                &finalization.tracker_summary,
                status,
                finalization.tracker_evidence.clone(),
                finalization.id.as_str(),
            )
            .map_err(|error| IntegrationArtifactError::Tracker(error.to_string()))?;
        let event = completion_event(&finalization)?;
        Ok(self
            .store
            .commit_integration_finalization(&finalization.id, &event, now)?)
    }
}

impl IntegrationQueueService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn enqueue(
        &self,
        artifact_id: &IntegrationArtifactId,
        priority: i32,
        now: DateTime<Utc>,
    ) -> Result<IntegrationJob, IntegrationArtifactError> {
        let artifact = self
            .store
            .integration_artifact(artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(artifact_id.as_str().into())
            })?;
        let job = IntegrationJob::new(&artifact, priority, now);
        self.store.enqueue_integration_job(&job)?;
        Ok(job)
    }

    pub fn acquire_next(
        &self,
        repository_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<IntegrationJob>, IntegrationArtifactError> {
        if repository_id.trim().is_empty() {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "repository identity is required for integration acquisition".into(),
            ));
        }
        Ok(self
            .store
            .acquire_next_integration_job(repository_id, now)?)
    }

    pub fn acquire_next_for_goal(
        &self,
        repository_id: &str,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<IntegrationJob>, IntegrationArtifactError> {
        if repository_id.trim().is_empty() || goal_id.trim().is_empty() {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "repository and goal identities are required for integration acquisition".into(),
            ));
        }
        Ok(self
            .store
            .acquire_next_integration_job_for_goal(repository_id, Some(goal_id), now)?)
    }

    pub fn finish(
        &self,
        job_id: &IntegrationJobId,
        succeeded: bool,
        summary: &str,
        now: DateTime<Utc>,
    ) -> Result<IntegrationJob, IntegrationArtifactError> {
        Ok(self
            .store
            .finish_integration_job(job_id, succeeded, summary, now)?)
    }

    pub fn jobs(
        &self,
        repository_id: &str,
        state: Option<IntegrationJobState>,
    ) -> Result<Vec<IntegrationJob>, IntegrationArtifactError> {
        Ok(self
            .store
            .integration_jobs_for_repository(repository_id, state)?)
    }
}

impl ReconciliationService {
    pub fn new(store: Arc<SqliteCoordinationStore>, policy: ReconciliationPolicy) -> Self {
        Self { store, policy }
    }

    pub fn reconcile(
        &self,
        request: ReconciliationRequest,
        now: DateTime<Utc>,
    ) -> Result<ReconciliationRecord, IntegrationArtifactError> {
        validate_reconciliation_request(&request, &self.policy)?;
        let artifact = self
            .store
            .integration_artifact(&request.artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(request.artifact_id.as_str().into())
            })?;
        let worker = self.store.worker(&artifact.worker_id)?.ok_or_else(|| {
            IntegrationArtifactError::WorkerNotFound(artifact.worker_id.as_str().into())
        })?;
        let binding = worker
            .workspace
            .as_ref()
            .ok_or(IntegrationArtifactError::WorkspaceMissing)?;
        let worktree = Path::new(&binding.worktree_path);
        let previous_head = git_output(worktree, &["rev-parse", "HEAD"])?
            .trim()
            .to_string();
        let reconciliation_id = ReconciliationId::new();

        if request.strategy == ReconciliationStrategy::Manual {
            let record = ReconciliationRecord {
                metadata: RecordMetadata::new(now),
                id: reconciliation_id,
                artifact_id: artifact.id,
                goal_id: artifact.goal_id,
                worker_id: artifact.worker_id,
                strategy: request.strategy,
                state: ReconciliationState::ManualRequired,
                target_revision: request.target_revision,
                commit_revisions: request.commit_revisions,
                previous_head: previous_head.clone(),
                resulting_head: previous_head,
                backup_ref: String::new(),
                command: String::new(),
                stdout_excerpt: String::new(),
                stderr_excerpt: String::new(),
                abort_succeeded: None,
                workspace_preserved: true,
                manual_instructions: request.manual_instructions,
            };
            self.store.insert_reconciliation(&record)?;
            return Ok(record);
        }

        let preflight =
            IntegrationPreflightService::new(self.store.clone()).run(&request.artifact_id, now)?;
        if preflight.checks.iter().any(|check| {
            matches!(
                check.kind,
                PreflightCheckKind::ClaimOwnership
                    | PreflightCheckKind::ClaimLease
                    | PreflightCheckKind::RepositoryRevision
                    | PreflightCheckKind::RepositoryState
            ) && !check.passed
        }) {
            return Err(IntegrationArtifactError::PreflightFailed);
        }

        let target_revision = if request.target_revision.trim().is_empty() {
            previous_head.clone()
        } else {
            resolve_revision(worktree, &request.target_revision)?
        };
        let commit_revisions = request
            .commit_revisions
            .iter()
            .map(|revision| resolve_revision(worktree, revision))
            .collect::<Result<Vec<_>, _>>()?;
        let backup_ref = format!(
            "refs/golazo/reconciliation-backups/{}",
            reconciliation_id.as_str()
        );
        git_output(worktree, &["update-ref", &backup_ref, &previous_head])?;

        let (program, arguments, display) =
            reconciliation_command(&request, &target_revision, &commit_revisions)?;
        let output = Command::new(&program)
            .args(&arguments)
            .current_dir(worktree)
            .output()?;
        let succeeded = output.status.success();
        let abort_succeeded = if succeeded {
            None
        } else {
            abort_reconciliation(worktree, request.strategy)
        };
        let resulting_head = git_output(worktree, &["rev-parse", "HEAD"])?
            .trim()
            .to_string();
        let record = ReconciliationRecord {
            metadata: RecordMetadata::new(now),
            id: reconciliation_id,
            artifact_id: artifact.id,
            goal_id: artifact.goal_id,
            worker_id: artifact.worker_id,
            strategy: request.strategy,
            state: if succeeded {
                ReconciliationState::Succeeded
            } else {
                ReconciliationState::Failed
            },
            target_revision,
            commit_revisions,
            previous_head,
            resulting_head,
            backup_ref,
            command: display,
            stdout_excerpt: bounded_output(&output.stdout, self.policy.max_output_bytes),
            stderr_excerpt: bounded_output(&output.stderr, self.policy.max_output_bytes),
            abort_succeeded,
            workspace_preserved: true,
            manual_instructions: request.manual_instructions,
        };
        self.store.insert_reconciliation(&record)?;
        Ok(record)
    }
}

impl ValidationGateRunner {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn run(
        &self,
        artifact_id: &IntegrationArtifactId,
        gates: &[ValidationGateSpec],
        now: DateTime<Utc>,
    ) -> Result<IntegrationValidationReport, IntegrationArtifactError> {
        validate_gate_specs(gates)?;
        let preflight =
            IntegrationPreflightService::new(self.store.clone()).run(artifact_id, now)?;
        if !preflight.passed {
            return Err(IntegrationArtifactError::PreflightFailed);
        }
        let artifact = self
            .store
            .integration_artifact(artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(artifact_id.as_str().into())
            })?;
        let worker = self.store.worker(&artifact.worker_id)?.ok_or_else(|| {
            IntegrationArtifactError::WorkerNotFound(artifact.worker_id.as_str().into())
        })?;
        let binding = worker
            .workspace
            .as_ref()
            .ok_or(IntegrationArtifactError::WorkspaceMissing)?;
        let worktree = Path::new(&binding.worktree_path);
        let mut results = Vec::with_capacity(gates.len());
        for gate in gates {
            let started_at = Utc::now();
            let output = Command::new(&gate.program)
                .args(&gate.args)
                .current_dir(worktree)
                .output();
            let completed_at = Utc::now();
            let command = std::iter::once(gate.program.as_str())
                .chain(gate.args.iter().map(String::as_str))
                .collect::<Vec<_>>()
                .join(" ");
            let result = match output {
                Ok(output) => ValidationGateResult {
                    gate_id: gate.id.clone(),
                    kind: gate.kind,
                    command,
                    required: gate.required,
                    succeeded: output.status.success(),
                    exit_code: output.status.code(),
                    stdout_excerpt: bounded_output(&output.stdout, gate.max_output_bytes),
                    stderr_excerpt: bounded_output(&output.stderr, gate.max_output_bytes),
                    started_at,
                    completed_at,
                },
                Err(error) => ValidationGateResult {
                    gate_id: gate.id.clone(),
                    kind: gate.kind,
                    command,
                    required: gate.required,
                    succeeded: false,
                    exit_code: None,
                    stdout_excerpt: String::new(),
                    stderr_excerpt: bounded_output(
                        error.to_string().as_bytes(),
                        gate.max_output_bytes,
                    ),
                    started_at,
                    completed_at,
                },
            };
            results.push(result);
        }
        let passed = results
            .iter()
            .all(|result| !result.required || result.succeeded);
        let report = IntegrationValidationReport {
            metadata: RecordMetadata::new(now),
            id: ValidationReportId::new(),
            artifact_id: artifact.id,
            goal_id: artifact.goal_id,
            worker_id: artifact.worker_id,
            passed,
            results,
        };
        self.store.insert_validation_report(&report)?;
        Ok(report)
    }
}

impl IntegrationPreflightService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn run(
        &self,
        artifact_id: &IntegrationArtifactId,
        now: DateTime<Utc>,
    ) -> Result<IntegrationPreflight, IntegrationArtifactError> {
        let artifact = self
            .store
            .integration_artifact(artifact_id)?
            .ok_or_else(|| {
                IntegrationArtifactError::ArtifactNotFound(artifact_id.as_str().into())
            })?;
        let claim = self.store.claim(&artifact.claim_id)?.ok_or_else(|| {
            IntegrationArtifactError::ClaimNotFound(artifact.claim_id.as_str().into())
        })?;
        let worker = self.store.worker(&artifact.worker_id)?.ok_or_else(|| {
            IntegrationArtifactError::WorkerNotFound(artifact.worker_id.as_str().into())
        })?;
        let mut checks = Vec::new();

        let ownership_passed = claim.state == ClaimState::Active
            && claim.owner == artifact.worker_id
            && worker.id == artifact.worker_id
            && worker.goal_id == artifact.goal_id
            && claim.goal_id == artifact.goal_id
            && claim.lease_generation == artifact.claim_generation;
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::ClaimOwnership,
            passed: ownership_passed,
            summary: if ownership_passed {
                "claim ownership and generation match the captured artifact".into()
            } else {
                "claim ownership, lifecycle, goal, or generation changed after capture".into()
            },
            evidence_refs: vec![
                format!("claim:{}", claim.id.as_str()),
                format!("worker:{}", worker.id.as_str()),
                format!("claim-generation:{}", claim.lease_generation),
            ],
        });

        let lease_passed = claim.state == ClaimState::Active
            && claim.lease_expires_at > now
            && claim.heartbeat_at <= now;
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::ClaimLease,
            passed: lease_passed,
            summary: if lease_passed {
                "claim lease is current".into()
            } else {
                "claim lease is expired, inactive, or has a future heartbeat".into()
            },
            evidence_refs: vec![format!("lease-expires-at:{}", claim.lease_expires_at)],
        });

        let (revision_passed, state_passed, repository_evidence) = match worker.workspace.as_ref() {
            Some(binding)
                if binding.repository_id == artifact.repository_id
                    && binding.base_revision == artifact.base_revision =>
            {
                let manager = WorktreeManager::open(&binding.canonical_repository_path)?;
                let state = manager.inspect(binding)?;
                (
                    state.head_revision == artifact.head_revision,
                    !state.has_dirty_files(),
                    vec![
                        format!("repository:{}", binding.repository_id),
                        format!("head:{}", state.head_revision),
                        format!("branch:{}", state.branch),
                    ],
                )
            }
            _ => (
                false,
                false,
                vec!["workspace binding is missing or no longer matches the artifact".into()],
            ),
        };
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::RepositoryRevision,
            passed: revision_passed,
            summary: if revision_passed {
                "repository identity, base, and HEAD match the artifact".into()
            } else {
                "repository identity, base, or HEAD changed after capture".into()
            },
            evidence_refs: repository_evidence.clone(),
        });
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::RepositoryState,
            passed: state_passed,
            summary: if state_passed {
                "workspace has no staged, unstaged, untracked, or conflicted paths".into()
            } else {
                "workspace contains dirty or ambiguous state".into()
            },
            evidence_refs: repository_evidence,
        });

        let mut contract_failures = Vec::new();
        for expectation in &claim.consumes_contracts {
            let contract_id = ContractId::parse(&expectation.contract_id)?;
            match self.store.contract(&contract_id)? {
                Some(contract) if contract.revision == expectation.expected_revision => {}
                Some(contract) => contract_failures.push(format!(
                    "{} expected {} observed {}",
                    contract.stable_key, expectation.expected_revision, contract.revision
                )),
                None => contract_failures.push(format!("missing {}", expectation.contract_id)),
            }
        }
        for change in &artifact.changed_contracts {
            match self.store.contract(&change.contract_id)? {
                Some(contract) if contract.revision == change.revision => {}
                Some(contract) => contract_failures.push(format!(
                    "{} artifact {} observed {}",
                    change.stable_key, change.revision, contract.revision
                )),
                None => contract_failures.push(format!("missing {}", change.stable_key)),
            }
        }
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::ContractRevisions,
            passed: contract_failures.is_empty(),
            summary: if contract_failures.is_empty() {
                "consumed and changed contract revisions are current".into()
            } else {
                format!(
                    "contract revisions changed: {}",
                    contract_failures.join("; ")
                )
            },
            evidence_refs: contract_failures,
        });

        let relevant_signals = self
            .store
            .signals_for_goal(&artifact.goal_id, Some(SignalState::Active))?
            .into_iter()
            .filter(|signal| {
                signal.expires_at > now
                    && (signal.affected_worker_ids.contains(&artifact.worker_id)
                        || signal.affected_claim_ids.contains(&artifact.claim_id))
            })
            .collect::<Vec<_>>();
        let overlaps = relevant_signals
            .iter()
            .filter(|signal| {
                signal.kinds.iter().any(|kind| {
                    matches!(
                        kind,
                        SignalKind::FileOverlap
                            | SignalKind::SymbolOverlap
                            | SignalKind::MigrationOverlap
                    )
                })
            })
            .map(|signal| signal.id.as_str().to_string())
            .collect::<Vec<_>>();
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::OverlapSignals,
            passed: overlaps.is_empty(),
            summary: if overlaps.is_empty() {
                "no unresolved affected overlap signals".into()
            } else {
                "unresolved affected overlap signals require reconciliation".into()
            },
            evidence_refs: overlaps,
        });
        let conflicts = relevant_signals
            .iter()
            .filter(|signal| signal.kinds.contains(&SignalKind::MergeConflict))
            .map(|signal| signal.id.as_str().to_string())
            .collect::<Vec<_>>();
        checks.push(PreflightCheck {
            kind: PreflightCheckKind::PredictedConflicts,
            passed: conflicts.is_empty(),
            summary: if conflicts.is_empty() {
                "no affected merge conflict is predicted".into()
            } else {
                "predicted merge conflicts require reconciliation".into()
            },
            evidence_refs: conflicts,
        });

        let passed = checks.iter().all(|check| check.passed);
        Ok(IntegrationPreflight {
            artifact_id: artifact.id,
            passed,
            checks,
            evaluated_at: now,
        })
    }
}

impl SynchronizationPolicyService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn evaluate(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        phase: SynchronizationPhase,
        now: DateTime<Utc>,
    ) -> Result<SynchronizationDecision, IntegrationArtifactError> {
        let claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| IntegrationArtifactError::ClaimNotFound(claim_id.as_str().into()))?;
        let worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| IntegrationArtifactError::WorkerNotFound(worker_id.as_str().into()))?;
        ensure_active_owner(&claim, &worker)?;

        let mut causes = Vec::new();
        if phase == SynchronizationPhase::Integration {
            causes.push(SynchronizationCause {
                kind: SynchronizationCauseKind::IntegrationBoundary,
                reference: claim.id.as_str().into(),
                summary: "synchronize at the controlled integration boundary".into(),
            });
        } else {
            for expectation in &claim.consumes_contracts {
                let contract_id = ContractId::parse(&expectation.contract_id)?;
                match self.store.contract(&contract_id)? {
                    Some(contract) if contract.revision != expectation.expected_revision => {
                        causes.push(SynchronizationCause {
                            kind: SynchronizationCauseKind::ContractInvalidated,
                            reference: contract.id.as_str().into(),
                            summary: format!(
                                "contract {} expected revision {} but is now {}",
                                contract.stable_key,
                                expectation.expected_revision,
                                contract.revision
                            ),
                        });
                    }
                    None => causes.push(SynchronizationCause {
                        kind: SynchronizationCauseKind::ContractInvalidated,
                        reference: expectation.contract_id.clone(),
                        summary: "an expected shared contract is no longer available".into(),
                    }),
                    _ => {}
                }
            }
            for signal in self
                .store
                .signals_for_goal(&claim.goal_id, Some(SignalState::Active))?
            {
                if signal.expires_at <= now
                    || (!signal.affected_worker_ids.contains(worker_id)
                        && !signal.affected_claim_ids.contains(claim_id))
                {
                    continue;
                }
                if signal.kinds.contains(&SignalKind::ContractMismatch) {
                    causes.push(SynchronizationCause {
                        kind: SynchronizationCauseKind::ContractInvalidated,
                        reference: signal.id.as_str().into(),
                        summary: "an active contract mismatch invalidates worker assumptions"
                            .into(),
                    });
                } else if signal.kinds.contains(&SignalKind::DependencyChanged) {
                    causes.push(SynchronizationCause {
                        kind: SynchronizationCauseKind::DependencyChanged,
                        reference: signal.id.as_str().into(),
                        summary:
                            "an active material dependency change invalidates worker assumptions"
                                .into(),
                    });
                }
            }
        }
        causes.sort_by(|left, right| {
            synchronization_cause_rank(left.kind)
                .cmp(&synchronization_cause_rank(right.kind))
                .then_with(|| left.reference.cmp(&right.reference))
        });
        causes.dedup_by(|left, right| left.kind == right.kind && left.reference == right.reference);
        Ok(SynchronizationDecision {
            phase,
            disposition: if causes.is_empty() {
                SynchronizationDisposition::Deferred
            } else {
                SynchronizationDisposition::Required
            },
            causes,
        })
    }
}

impl IntegrationArtifactService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn capture(
        &self,
        request: CaptureIntegrationArtifactRequest,
        now: DateTime<Utc>,
    ) -> Result<IntegrationArtifact, IntegrationArtifactError> {
        let claim = self.store.claim(&request.claim_id)?.ok_or_else(|| {
            IntegrationArtifactError::ClaimNotFound(request.claim_id.as_str().into())
        })?;
        let worker = self.store.worker(&request.worker_id)?.ok_or_else(|| {
            IntegrationArtifactError::WorkerNotFound(request.worker_id.as_str().into())
        })?;
        ensure_active_owner(&claim, &worker)?;
        let binding = worker
            .workspace
            .as_ref()
            .ok_or(IntegrationArtifactError::WorkspaceMissing)?;
        if claim.base_revision != binding.base_revision {
            return Err(IntegrationArtifactError::BaseRevisionMismatch);
        }

        let manager = WorktreeManager::open(&binding.canonical_repository_path)?;
        let state = manager.inspect(binding)?;
        let worktree = Path::new(&binding.worktree_path);
        let commits = capture_commits(worktree, &claim.base_revision, &state.head_revision)?;
        let diff_summary =
            capture_diff_summary(worktree, &claim.base_revision, &state.head_revision)?;
        let artifact = IntegrationArtifact {
            metadata: RecordMetadata::new(now),
            id: IntegrationArtifactId::new(),
            goal_id: claim.goal_id,
            claim_id: claim.id,
            claim_generation: claim.lease_generation,
            worker_id: worker.id,
            repository_id: binding.repository_id.clone(),
            base_revision: claim.base_revision,
            head_revision: state.head_revision,
            commits,
            diff_summary,
            changed_contracts: request.changed_contracts,
            migrations: request.migrations,
            validations: request.validations,
            evidence_refs: request.evidence_refs,
            known_risks: request.known_risks,
            workspace: IntegrationWorkspaceSnapshot {
                branch: state.branch,
                staged_paths: state.staged_paths,
                unstaged_paths: state.unstaged_paths,
                untracked_paths: state.untracked_paths,
                conflicted_paths: state.conflicted_paths,
            },
        };
        artifact.validate()?;
        self.store.insert_integration_artifact(&artifact)?;
        Ok(artifact)
    }
}

fn ensure_active_owner(claim: &Claim, worker: &Worker) -> Result<(), IntegrationArtifactError> {
    if claim.state != ClaimState::Active {
        return Err(IntegrationArtifactError::ClaimNotActive);
    }
    if claim.owner != worker.id {
        return Err(IntegrationArtifactError::ClaimOwnerMismatch);
    }
    if claim.goal_id != worker.goal_id {
        return Err(IntegrationArtifactError::GoalMismatch);
    }
    Ok(())
}

fn synchronization_cause_rank(kind: SynchronizationCauseKind) -> u8 {
    match kind {
        SynchronizationCauseKind::IntegrationBoundary => 0,
        SynchronizationCauseKind::ContractInvalidated => 1,
        SynchronizationCauseKind::DependencyChanged => 2,
    }
}

fn completion_event(
    finalization: &IntegrationFinalization,
) -> Result<CoordinationEvent, IntegrationArtifactError> {
    let mut event = CoordinationEvent::from_typed_payload(
        &finalization.goal_id,
        EventSeverity::Info,
        CoordinationActor::System,
        finalization.claim_id.as_str(),
        CoordinationEventPayload::Completion(CompletionEventPayload {
            claim_id: finalization.claim_id.clone(),
            worker_id: finalization.worker_id.clone(),
            artifact_id: finalization.artifact_id.as_str().into(),
            evidence_refs: finalization.tracker_evidence.clone(),
            integration_revision: finalization.integration_revision.clone(),
        }),
        finalization.metadata.created_at,
    )?;
    event.id = finalization.completion_event_id.clone();
    Ok(event)
}

fn validate_maintenance_request(
    requested_by: &str,
    reason: &str,
) -> Result<(), IntegrationArtifactError> {
    if requested_by.trim().is_empty()
        || requested_by.chars().count() > 200
        || reason.trim().is_empty()
        || reason.chars().count() > 2_000
    {
        return Err(IntegrationArtifactError::InvalidReconciliation(
            "rollback requires a bounded attributed actor and reason".into(),
        ));
    }
    Ok(())
}

fn validate_reconciliation_request(
    request: &ReconciliationRequest,
    policy: &ReconciliationPolicy,
) -> Result<(), IntegrationArtifactError> {
    if !policy.allowed_strategies.contains(&request.strategy) {
        return Err(IntegrationArtifactError::ReconciliationNotAllowed);
    }
    if !(1..=262_144).contains(&policy.max_output_bytes) {
        return Err(IntegrationArtifactError::InvalidReconciliation(
            "output bound must be between 1 and 262144 bytes".into(),
        ));
    }
    if request.commit_revisions.len() > 1_000
        || request
            .manual_instructions
            .as_ref()
            .is_some_and(|instructions| {
                instructions.trim().is_empty() || instructions.chars().count() > 4_000
            })
    {
        return Err(IntegrationArtifactError::InvalidReconciliation(
            "commit and manual-instruction bounds were exceeded".into(),
        ));
    }
    match request.strategy {
        ReconciliationStrategy::Rebase | ReconciliationStrategy::Merge => {
            if request.target_revision.trim().is_empty()
                || !request.commit_revisions.is_empty()
                || request.regeneration.is_some()
            {
                return Err(IntegrationArtifactError::InvalidReconciliation(
                    "rebase and merge require only a target revision".into(),
                ));
            }
        }
        ReconciliationStrategy::CherryPick => {
            if request.commit_revisions.is_empty() || request.regeneration.is_some() {
                return Err(IntegrationArtifactError::InvalidReconciliation(
                    "cherry-pick requires one or more commit revisions".into(),
                ));
            }
        }
        ReconciliationStrategy::Regenerate => {
            let regeneration = request.regeneration.as_ref().ok_or_else(|| {
                IntegrationArtifactError::InvalidReconciliation(
                    "regeneration requires a shell-free command".into(),
                )
            })?;
            if regeneration.program.trim().is_empty()
                || regeneration.args.len() > 128
                || !policy.regeneration_programs.contains(&regeneration.program)
            {
                return Err(IntegrationArtifactError::ReconciliationNotAllowed);
            }
        }
        ReconciliationStrategy::Manual => {
            if request.manual_instructions.is_none() || request.regeneration.is_some() {
                return Err(IntegrationArtifactError::InvalidReconciliation(
                    "manual reconciliation requires explicit instructions".into(),
                ));
            }
        }
    }
    Ok(())
}

fn reconciliation_command(
    request: &ReconciliationRequest,
    target_revision: &str,
    commit_revisions: &[String],
) -> Result<(String, Vec<String>, String), IntegrationArtifactError> {
    let (program, arguments) = match request.strategy {
        ReconciliationStrategy::Rebase => (
            "git".to_string(),
            vec!["rebase".into(), target_revision.into()],
        ),
        ReconciliationStrategy::Merge => (
            "git".to_string(),
            vec![
                "merge".into(),
                "--no-edit".into(),
                "--no-ff".into(),
                target_revision.into(),
            ],
        ),
        ReconciliationStrategy::CherryPick => {
            let mut arguments = vec!["cherry-pick".into()];
            arguments.extend(commit_revisions.iter().cloned());
            ("git".to_string(), arguments)
        }
        ReconciliationStrategy::Regenerate => {
            let command = request.regeneration.as_ref().ok_or_else(|| {
                IntegrationArtifactError::InvalidReconciliation(
                    "regeneration command is missing".into(),
                )
            })?;
            (command.program.clone(), command.args.clone())
        }
        ReconciliationStrategy::Manual => {
            return Err(IntegrationArtifactError::InvalidReconciliation(
                "manual reconciliation does not execute a command".into(),
            ));
        }
    };
    let display = std::iter::once(program.as_str())
        .chain(arguments.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    Ok((program, arguments, display))
}

fn resolve_revision(worktree: &Path, revision: &str) -> Result<String, IntegrationArtifactError> {
    if revision.trim().is_empty() {
        return Err(IntegrationArtifactError::InvalidReconciliation(
            "revision must not be empty".into(),
        ));
    }
    let commit = format!("{revision}^{{commit}}");
    Ok(git_output(worktree, &["rev-parse", "--verify", &commit])?
        .trim()
        .to_string())
}

fn abort_reconciliation(worktree: &Path, strategy: ReconciliationStrategy) -> Option<bool> {
    let arguments = match strategy {
        ReconciliationStrategy::Rebase => ["rebase", "--abort"],
        ReconciliationStrategy::Merge => ["merge", "--abort"],
        ReconciliationStrategy::CherryPick => ["cherry-pick", "--abort"],
        ReconciliationStrategy::Regenerate | ReconciliationStrategy::Manual => return None,
    };
    Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(arguments)
        .output()
        .ok()
        .map(|output| output.status.success())
}

fn validate_gate_specs(gates: &[ValidationGateSpec]) -> Result<(), IntegrationArtifactError> {
    if gates.is_empty() {
        return Err(IntegrationArtifactError::InvalidValidationGate(
            "at least one gate is required".into(),
        ));
    }
    let mut ids = HashSet::new();
    for gate in gates {
        if gate.id.trim().is_empty()
            || gate.program.trim().is_empty()
            || !ids.insert(gate.id.as_str())
            || gate.args.len() > 128
            || !(1..=262_144).contains(&gate.max_output_bytes)
        {
            return Err(IntegrationArtifactError::InvalidValidationGate(
                "gate IDs and programs must be non-empty and unique, arguments bounded, and output limited to 1-262144 bytes"
                    .into(),
            ));
        }
    }
    Ok(())
}

fn bounded_output(bytes: &[u8], maximum: usize) -> String {
    let start = bytes.len().saturating_sub(maximum);
    String::from_utf8_lossy(&bytes[start..]).to_string()
}

fn capture_commits(
    worktree: &Path,
    base_revision: &str,
    head_revision: &str,
) -> Result<Vec<IntegrationCommit>, IntegrationArtifactError> {
    let range = format!("{base_revision}..{head_revision}");
    let output = git_output(worktree, &["log", "--reverse", "--format=%H%x09%s", &range])?;
    Ok(output
        .lines()
        .filter_map(|line| {
            let (revision, subject) = line.split_once('\t')?;
            Some(IntegrationCommit {
                revision: revision.into(),
                subject: subject.into(),
            })
        })
        .collect())
}

fn capture_diff_summary(
    worktree: &Path,
    base_revision: &str,
    head_revision: &str,
) -> Result<IntegrationDiffSummary, IntegrationArtifactError> {
    let range = format!("{base_revision}..{head_revision}");
    let output = git_output(worktree, &["diff", "--numstat", &range])?;
    let mut insertions = 0_u64;
    let mut deletions = 0_u64;
    let mut changed_paths = Vec::new();
    for line in output.lines() {
        let mut fields = line.splitn(3, '\t');
        let added = fields.next().unwrap_or_default();
        let removed = fields.next().unwrap_or_default();
        let path = fields.next().unwrap_or_default();
        insertions += added.parse::<u64>().unwrap_or(0);
        deletions += removed.parse::<u64>().unwrap_or(0);
        if !path.is_empty() {
            changed_paths.push(path.to_string());
        }
    }
    changed_paths.sort();
    changed_paths.dedup();
    let files_changed = changed_paths.len() as u64;
    Ok(IntegrationDiffSummary {
        files_changed,
        insertions,
        deletions,
        changed_paths,
        summary: format!(
            "{files_changed} files changed, {insertions} insertions(+), {deletions} deletions(-)"
        ),
    })
}

fn git_output(worktree: &Path, args: &[&str]) -> Result<String, IntegrationArtifactError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(git_error(&format!("git {}", args.join(" ")), output))
    }
}

fn git_error(command: &str, output: Output) -> IntegrationArtifactError {
    let message = String::from_utf8_lossy(&output.stderr).trim().to_string();
    IntegrationArtifactError::GitFailed {
        command: command.into(),
        message: if message.is_empty() {
            format!("exit status {}", output.status)
        } else {
            message
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::fs;
    use std::sync::Barrier;
    use std::thread;
    use tempfile::TempDir;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn queued_artifact(repository_id: &str, now: DateTime<Utc>) -> IntegrationArtifact {
        IntegrationArtifact {
            metadata: RecordMetadata::new(now),
            id: IntegrationArtifactId::new(),
            goal_id: "goal-a".into(),
            claim_id: ClaimId::new(),
            claim_generation: 1,
            worker_id: WorkerId::new(),
            repository_id: repository_id.into(),
            base_revision: "base".into(),
            head_revision: "head".into(),
            commits: Vec::new(),
            diff_summary: IntegrationDiffSummary {
                files_changed: 0,
                insertions: 0,
                deletions: 0,
                changed_paths: Vec::new(),
                summary: "0 files changed, 0 insertions(+), 0 deletions(-)".into(),
            },
            changed_contracts: Vec::new(),
            migrations: Vec::new(),
            validations: Vec::new(),
            evidence_refs: Vec::new(),
            known_risks: Vec::new(),
            workspace: IntegrationWorkspaceSnapshot {
                branch: "codex/worker".into(),
                staged_paths: Vec::new(),
                unstaged_paths: Vec::new(),
                untracked_paths: Vec::new(),
                conflicted_paths: Vec::new(),
            },
        }
    }

    fn active_failure_fixture(
        directory: &TempDir,
        now: DateTime<Utc>,
    ) -> (
        Arc<SqliteCoordinationStore>,
        Worker,
        Claim,
        IntegrationArtifact,
        IntegrationJob,
        IntegrationValidationReport,
        ReconciliationRecord,
    ) {
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.workspace = Some(WorkspaceBinding {
            repository_id: "repo-a".into(),
            canonical_repository_path: directory.path().display().to_string(),
            worktree_path: directory
                .path()
                .join("preserved-worktree")
                .display()
                .to_string(),
            branch: "codex/worker".into(),
            base_revision: "base".into(),
            created_at: Some(now),
            creation_evidence: vec!["workspace-created".into()],
        });
        fs::create_dir(directory.path().join("preserved-worktree")).unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "delivery".into(),
            },
            worker.id.clone(),
            "base",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let mut artifact = queued_artifact("repo-a", now);
        artifact.claim_id = claim.id.clone();
        artifact.claim_generation = claim.lease_generation;
        artifact.worker_id = worker.id.clone();
        artifact.evidence_refs = vec!["artifact-log".into()];
        store.insert_integration_artifact(&artifact).unwrap();
        let report = IntegrationValidationReport {
            metadata: RecordMetadata::new(now),
            id: ValidationReportId::new(),
            artifact_id: artifact.id.clone(),
            goal_id: artifact.goal_id.clone(),
            worker_id: worker.id.clone(),
            passed: false,
            results: vec![],
        };
        store.insert_validation_report(&report).unwrap();
        let reconciliation = ReconciliationRecord {
            metadata: RecordMetadata::new(now),
            id: ReconciliationId::new(),
            artifact_id: artifact.id.clone(),
            goal_id: artifact.goal_id.clone(),
            worker_id: worker.id.clone(),
            strategy: ReconciliationStrategy::Manual,
            state: ReconciliationState::ManualRequired,
            target_revision: "target".into(),
            commit_revisions: vec![],
            previous_head: "head".into(),
            resulting_head: "head".into(),
            backup_ref: "refs/golazo-backups/test".into(),
            command: "manual".into(),
            stdout_excerpt: String::new(),
            stderr_excerpt: "conflict details".into(),
            abort_succeeded: None,
            workspace_preserved: true,
            manual_instructions: Some("Resolve the conflicting contract.".into()),
        };
        store.insert_reconciliation(&reconciliation).unwrap();
        let queue = IntegrationQueueService::new(store.clone());
        queue.enqueue(&artifact.id, 0, now).unwrap();
        let job = queue.acquire_next("repo-a", now).unwrap().unwrap();
        (store, worker, claim, artifact, job, report, reconciliation)
    }

    fn completed_maintenance_fixture(
        directory: &TempDir,
        now: DateTime<Utc>,
    ) -> (
        Arc<SqliteCoordinationStore>,
        IntegrationFinalization,
        IntegrationArtifact,
        WorkspaceBinding,
    ) {
        let repository = directory.path().join("repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &["config", "user.email", "golazo@example.test"],
        );
        git(&repository, &["config", "user.name", "Golazo Tests"]);
        fs::write(repository.join("README.md"), "initial\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-q", "-m", "initial"]);
        let base = git(&repository, &["rev-parse", "HEAD"]);
        let manager = WorktreeManager::open(&repository).unwrap();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker.id, "HEAD", now)
            .unwrap();
        fs::write(
            Path::new(&binding.worktree_path).join("README.md"),
            "integrated\n",
        )
        .unwrap();
        git(Path::new(&binding.worktree_path), &["add", "README.md"]);
        git(
            Path::new(&binding.worktree_path),
            &["commit", "-q", "-m", "integrated change"],
        );
        let integrated = git(Path::new(&binding.worktree_path), &["rev-parse", "HEAD"]);
        git(&repository, &["merge", "--ff-only", &binding.branch]);
        worker.workspace = Some(binding.clone());
        worker
            .transition(WorkerState::Completed, now, Some("integrated".into()))
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        store.upsert_worker(&worker).unwrap();
        let mut artifact = queued_artifact(&binding.repository_id, now);
        artifact.worker_id = worker.id.clone();
        artifact.base_revision = base;
        artifact.head_revision = integrated.clone();
        artifact.workspace.branch = binding.branch.clone();
        artifact.diff_summary = IntegrationDiffSummary {
            files_changed: 1,
            insertions: 1,
            deletions: 1,
            changed_paths: vec!["README.md".into()],
            summary: "1 file changed".into(),
        };
        store.insert_integration_artifact(&artifact).unwrap();
        let finalization = IntegrationFinalization {
            metadata: RecordMetadata::new(now),
            id: IntegrationFinalizationId::new(),
            job_id: IntegrationJobId::new(),
            artifact_id: artifact.id.clone(),
            validation_report_id: ValidationReportId::new(),
            goal_id: artifact.goal_id.clone(),
            worker_id: worker.id,
            claim_id: artifact.claim_id.clone(),
            tracker_feature_id: "delivery".into(),
            tracker_step_ids: vec!["integrate".into()],
            tracker_summary: "Integrated".into(),
            tracker_evidence: vec!["validation:passed".into()],
            tracker_status: "Done".into(),
            integration_revision: integrated,
            completion_event_id: EventId::new(),
            state: IntegrationFinalizationState::Completed,
            completed_at: Some(now),
        };
        store
            .insert_integration_finalization(&finalization)
            .unwrap();
        (store, finalization, artifact, binding)
    }

    #[test]
    fn serializes_integration_per_repository_with_deterministic_queue_order() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&path).unwrap());
        let now = Utc::now();
        let repository_id = "repo-a";
        let first = queued_artifact(repository_id, now);
        let priority = queued_artifact(repository_id, now);
        let second = queued_artifact(repository_id, now);
        for artifact in [&first, &priority, &second] {
            store.insert_integration_artifact(artifact).unwrap();
        }
        let queue = IntegrationQueueService::new(store.clone());
        let first_job = queue.enqueue(&first.id, 0, now).unwrap();
        let priority_job = queue.enqueue(&priority.id, 10, now).unwrap();
        let second_job = queue.enqueue(&second.id, 0, now).unwrap();
        assert!(matches!(
            queue.enqueue(&priority.id, 10, now),
            Err(IntegrationArtifactError::Store(
                StoreError::IntegrationArtifactAlreadyQueued(_)
            ))
        ));

        let barrier = Arc::new(Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let queue = queue.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    queue.acquire_next(repository_id, now).unwrap()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let acquired = handles
            .into_iter()
            .filter_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(acquired.len(), 1);
        assert_eq!(acquired[0].id, priority_job.id);
        assert_eq!(acquired[0].attempt, 1);
        assert!(queue.acquire_next(repository_id, now).unwrap().is_none());
        queue
            .finish(
                &priority_job.id,
                true,
                "priority integration completed",
                now,
            )
            .unwrap();

        let acquired_first = queue.acquire_next(repository_id, now).unwrap().unwrap();
        assert_eq!(acquired_first.id, first_job.id);
        queue
            .finish(&first_job.id, false, "validation failed", now)
            .unwrap();
        let acquired_second = queue.acquire_next(repository_id, now).unwrap().unwrap();
        assert_eq!(acquired_second.id, second_job.id);
        queue
            .finish(&second_job.id, true, "integration completed", now)
            .unwrap();

        let reopened = SqliteCoordinationStore::open(&path).unwrap();
        let jobs = reopened
            .integration_jobs_for_repository(repository_id, None)
            .unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].state, IntegrationJobState::Failed);
        assert_eq!(jobs[1].state, IntegrationJobState::Succeeded);
        assert_eq!(jobs[2].state, IntegrationJobState::Succeeded);
        assert!(jobs.iter().all(|job| job.state.is_terminal()));
    }

    #[test]
    fn finalizes_tracker_and_coordination_state_as_one_recoverable_outcome() {
        let directory = TempDir::new().unwrap();
        let tracker = Tracker::new(directory.path().join("trackers"));
        tracker
            .create_goal_with_features(
                "goal-a",
                "Goal A",
                "integration finalization",
                vec![crate::models::Feature {
                    id: "delivery".into(),
                    title: "Delivery".into(),
                    description: String::new(),
                    status: Status::Partial,
                    steps: vec![crate::models::Step {
                        id: "integrate".into(),
                        title: "Integrate".into(),
                        done: false,
                        next: true,
                    }],
                }],
            )
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "delivery".into(),
            },
            worker.id.clone(),
            "base",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let mut artifact = queued_artifact("repo-a", now);
        artifact.goal_id = "goal-a".into();
        artifact.claim_id = claim.id.clone();
        artifact.claim_generation = claim.lease_generation;
        artifact.worker_id = worker.id.clone();
        store.insert_integration_artifact(&artifact).unwrap();
        let report = IntegrationValidationReport {
            metadata: RecordMetadata::new(now),
            id: ValidationReportId::new(),
            artifact_id: artifact.id.clone(),
            goal_id: "goal-a".into(),
            worker_id: worker.id.clone(),
            passed: true,
            results: Vec::new(),
        };
        store.insert_validation_report(&report).unwrap();
        let queue = IntegrationQueueService::new(store.clone());
        let queued = queue.enqueue(&artifact.id, 0, now).unwrap();
        let running = queue.acquire_next("repo-a", now).unwrap().unwrap();
        assert_eq!(queued.id, running.id);

        let finalizer = IntegrationFinalizationService::new(store.clone(), tracker.clone());
        let completed = finalizer
            .finalize(
                IntegrationFinalizationRequest {
                    job_id: running.id.clone(),
                    validation_report_id: report.id.clone(),
                    tracker_feature_id: "delivery".into(),
                    tracker_step_ids: vec!["integrate".into()],
                    tracker_summary: "Integrated validated delivery".into(),
                    tracker_evidence: vec!["validation:passed".into()],
                    tracker_status: Status::Done,
                    integration_revision: artifact.head_revision.clone(),
                },
                now,
            )
            .unwrap();
        assert_eq!(completed.state, IntegrationFinalizationState::Completed);
        assert_eq!(
            store.integration_job(&running.id).unwrap().unwrap().state,
            IntegrationJobState::Succeeded
        );
        assert_eq!(
            store.claim(&claim.id).unwrap().unwrap().state,
            ClaimState::Completed
        );
        assert_eq!(
            store.worker(&worker.id).unwrap().unwrap().state,
            WorkerState::Completed
        );
        assert_eq!(
            store
                .event(&completed.completion_event_id)
                .unwrap()
                .unwrap()
                .kind,
            CoordinationEventKind::CompletionPublished
        );
        let goal = tracker.get_goal("goal-a").unwrap();
        assert_eq!(goal["features"][0]["steps"][0]["done"], true);
        assert_eq!(goal["features"][0]["status"], "Done");
        let slices = goal["slices"].as_array().unwrap();
        assert_eq!(slices.len(), 1);
        assert!(
            slices[0]["evidence"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value
                    .as_str()
                    .unwrap()
                    .starts_with("integration-finalization:"))
        );
        assert!(finalizer.recover_prepared(now).unwrap().is_empty());
    }

    #[test]
    fn finalizes_verified_partial_progress_without_completing_a_tracker_step() {
        let directory = TempDir::new().unwrap();
        let tracker = Tracker::new(directory.path().join("trackers"));
        tracker
            .create_goal_with_features(
                "goal-a",
                "Goal A",
                "partial integration finalization",
                vec![crate::models::Feature {
                    id: "delivery".into(),
                    title: "Delivery".into(),
                    description: String::new(),
                    status: Status::Partial,
                    steps: vec![crate::models::Step {
                        id: "integrate".into(),
                        title: "Integrate".into(),
                        done: false,
                        next: true,
                    }],
                }],
            )
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "delivery".into(),
            },
            worker.id.clone(),
            "base",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let mut artifact = queued_artifact("repo-a", now);
        artifact.goal_id = "goal-a".into();
        artifact.claim_id = claim.id.clone();
        artifact.claim_generation = claim.lease_generation;
        artifact.worker_id = worker.id.clone();
        store.insert_integration_artifact(&artifact).unwrap();
        let report = IntegrationValidationReport {
            metadata: RecordMetadata::new(now),
            id: ValidationReportId::new(),
            artifact_id: artifact.id.clone(),
            goal_id: "goal-a".into(),
            worker_id: worker.id.clone(),
            passed: true,
            results: Vec::new(),
        };
        store.insert_validation_report(&report).unwrap();
        let queue = IntegrationQueueService::new(store.clone());
        queue.enqueue(&artifact.id, 0, now).unwrap();
        let running = queue.acquire_next("repo-a", now).unwrap().unwrap();

        let completed = IntegrationFinalizationService::new(store.clone(), tracker.clone())
            .finalize(
                IntegrationFinalizationRequest {
                    job_id: running.id,
                    validation_report_id: report.id,
                    tracker_feature_id: "delivery".into(),
                    tracker_step_ids: vec![],
                    tracker_summary: "Integrated a verified portion of delivery".into(),
                    tracker_evidence: vec!["validation:passed".into()],
                    tracker_status: Status::Partial,
                    integration_revision: artifact.head_revision,
                },
                now,
            )
            .unwrap();

        assert_eq!(completed.state, IntegrationFinalizationState::Completed);
        let goal = tracker.get_goal("goal-a").unwrap();
        assert_eq!(goal["features"][0]["steps"][0]["done"], false);
        assert_eq!(goal["features"][0]["steps"][0]["next"], true);
        assert_eq!(goal["features"][0]["status"], "Partial");
        assert_eq!(goal["slices"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn returns_actionable_integration_failures_or_escalates_without_losing_evidence() {
        let now = Utc::now();
        let retry_directory = TempDir::new().unwrap();
        let (store, worker, _claim, artifact, job, report, reconciliation) =
            active_failure_fixture(&retry_directory, now);
        let outcome = IntegrationFailureService::new(store.clone())
            .handle(
                IntegrationFailureRequest {
                    job_id: job.id.clone(),
                    kind: IntegrationFailureKind::Validation,
                    disposition: IntegrationFailureDisposition::ReturnToWorker,
                    summary: "Required validation failed.".into(),
                    evidence_refs: vec![
                        format!("validation-report:{}", report.id.as_str()),
                        format!("reconciliation:{}", reconciliation.id.as_str()),
                    ],
                    recommended_action: "Fix the failing validation and submit a new artifact."
                        .into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(outcome.job.state, IntegrationJobState::Failed);
        assert_eq!(outcome.worker.state, WorkerState::Waiting);
        assert_eq!(outcome.claim.state, ClaimState::Active);
        let notification = outcome.notification.unwrap();
        assert_eq!(notification.target_worker, worker.id);
        assert_eq!(notification.state, NotificationState::Queued);
        assert!(notification.recommended_action.is_some());
        assert!(outcome.escalation.is_none());
        assert!(retry_directory.path().join("preserved-worktree").is_dir());
        assert!(store.integration_artifact(&artifact.id).unwrap().is_some());
        assert!(store.validation_report(&report.id).unwrap().is_some());
        assert!(store.reconciliation(&reconciliation.id).unwrap().is_some());
        assert!(store.notification(&notification.id).unwrap().is_some());
        assert_eq!(
            store.event(&outcome.event.id).unwrap().unwrap().kind,
            CoordinationEventKind::BlockerRaised
        );

        let escalation_directory = TempDir::new().unwrap();
        let (store, worker, claim, artifact, job, report, reconciliation) =
            active_failure_fixture(&escalation_directory, now);
        let outcome = IntegrationFailureService::new(store.clone())
            .handle(
                IntegrationFailureRequest {
                    job_id: job.id,
                    kind: IntegrationFailureKind::Reconciliation,
                    disposition: IntegrationFailureDisposition::Escalate,
                    summary: "Integration requires a consequential manual choice.".into(),
                    evidence_refs: vec![
                        format!("validation-report:{}", report.id.as_str()),
                        format!("reconciliation:{}", reconciliation.id.as_str()),
                    ],
                    recommended_action: "Choose the contract direction before retrying.".into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap();
        let escalation = outcome.escalation.unwrap();
        assert_eq!(outcome.job.state, IntegrationJobState::Failed);
        assert_eq!(outcome.worker.state, WorkerState::Blocked);
        assert_eq!(outcome.claim.state, ClaimState::Blocked);
        assert_eq!(
            outcome
                .claim
                .outcome
                .as_ref()
                .unwrap()
                .artifact_id
                .as_deref(),
            Some(artifact.id.as_str())
        );
        assert!(outcome.notification.is_none());
        assert_eq!(escalation.state, EscalationState::Open);
        assert!(store.escalation(&escalation.id).unwrap().is_some());
        assert!(
            escalation_directory
                .path()
                .join("preserved-worktree")
                .is_dir()
        );
        assert!(store.integration_artifact(&artifact.id).unwrap().is_some());
        assert!(store.validation_report(&report.id).unwrap().is_some());
        assert!(store.reconciliation(&reconciliation.id).unwrap().is_some());
        assert!(
            !store
                .worker(&worker.id)
                .unwrap()
                .unwrap()
                .active_claims
                .contains(&claim.id)
        );
    }

    #[test]
    fn retries_with_a_new_artifact_after_failure_and_store_restart() {
        let directory = TempDir::new().unwrap();
        let now = Utc::now();
        let (store, _worker, _claim, artifact, failed_job, report, reconciliation) =
            active_failure_fixture(&directory, now);
        let database_path = store.path().to_path_buf();
        IntegrationFailureService::new(store.clone())
            .handle(
                IntegrationFailureRequest {
                    job_id: failed_job.id.clone(),
                    kind: IntegrationFailureKind::Validation,
                    disposition: IntegrationFailureDisposition::ReturnToWorker,
                    summary: "Required validation failed.".into(),
                    evidence_refs: vec![
                        format!("validation-report:{}", report.id.as_str()),
                        format!("reconciliation:{}", reconciliation.id.as_str()),
                    ],
                    recommended_action: "Correct the failure and capture a replacement artifact."
                        .into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap();
        drop(store);

        let reopened = Arc::new(SqliteCoordinationStore::open(&database_path).unwrap());
        assert_eq!(
            reopened
                .integration_job(&failed_job.id)
                .unwrap()
                .unwrap()
                .state,
            IntegrationJobState::Failed
        );
        let mut replacement = artifact.clone();
        replacement.metadata = RecordMetadata::new(now + Duration::seconds(2));
        replacement.id = IntegrationArtifactId::new();
        replacement.head_revision = "head-after-fix".into();
        replacement.evidence_refs.push("retry:1".into());
        reopened.insert_integration_artifact(&replacement).unwrap();
        let queue = IntegrationQueueService::new(reopened.clone());
        let queued = queue
            .enqueue(
                &replacement.id,
                failed_job.priority,
                now + Duration::seconds(2),
            )
            .unwrap();
        let retry = queue
            .acquire_next(&replacement.repository_id, now + Duration::seconds(3))
            .unwrap()
            .unwrap();
        assert_eq!(retry.id, queued.id);
        assert_ne!(retry.id, failed_job.id);
        assert_eq!(retry.attempt, 1);
        assert_eq!(retry.state, IntegrationJobState::Running);
        assert!(
            reopened
                .integration_artifact(&artifact.id)
                .unwrap()
                .is_some()
        );
        assert!(
            reopened
                .integration_artifact(&replacement.id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn rolls_back_with_a_backup_and_cleans_up_only_verified_integrated_workspaces() {
        let now = Utc::now();
        let rollback_directory = TempDir::new().unwrap();
        let (store, finalization, artifact, binding) =
            completed_maintenance_fixture(&rollback_directory, now);
        let service = IntegrationMaintenanceService::new(store.clone());
        let rollback = service
            .rollback(
                &finalization.id,
                "operator@example.test",
                "Production verification found a regression.",
                now + Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(rollback.state, IntegrationMaintenanceState::Succeeded);
        assert_ne!(
            rollback.resulting_repository_head,
            rollback.previous_repository_head
        );
        assert_eq!(
            fs::read_to_string(Path::new(&binding.canonical_repository_path).join("README.md"))
                .unwrap(),
            "initial\n"
        );
        let backup_ref = rollback.backup_ref.as_ref().unwrap();
        assert_eq!(
            git(
                Path::new(&binding.canonical_repository_path),
                &["rev-parse", backup_ref]
            ),
            finalization.integration_revision
        );
        assert!(Path::new(&binding.worktree_path).is_dir());
        assert_eq!(
            git(
                Path::new(&binding.canonical_repository_path),
                &["rev-parse", &binding.branch]
            ),
            artifact.head_revision
        );
        assert!(store.integration_artifact(&artifact.id).unwrap().is_some());
        assert!(
            store
                .integration_maintenance(&rollback.id)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            service
                .rollback(
                    &finalization.id,
                    "operator@example.test",
                    "Production verification found a regression.",
                    now + Duration::seconds(2),
                )
                .unwrap()
                .id,
            rollback.id
        );

        let cleanup_directory = TempDir::new().unwrap();
        let (store, finalization, artifact, binding) =
            completed_maintenance_fixture(&cleanup_directory, now);
        let cleanup = IntegrationMaintenanceService::new(store.clone())
            .cleanup_after_integration(&finalization.id, now + Duration::seconds(1))
            .unwrap();
        assert_eq!(cleanup.state, IntegrationMaintenanceState::Succeeded);
        assert!(!Path::new(&binding.worktree_path).exists());
        assert!(
            cleanup
                .cleanup_record_path
                .as_ref()
                .unwrap()
                .ends_with(".json")
        );
        assert_eq!(
            git(
                Path::new(&binding.canonical_repository_path),
                &["rev-parse", &binding.branch]
            ),
            artifact.head_revision
        );
        assert!(store.integration_artifact(&artifact.id).unwrap().is_some());
        assert!(
            store
                .integration_maintenance(&cleanup.id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn captures_and_persists_complete_integration_evidence() {
        let directory = TempDir::new().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &["config", "user.email", "golazo@example.test"],
        );
        git(&repository, &["config", "user.name", "Golazo Tests"]);
        fs::write(repository.join("README.md"), "initial\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-q", "-m", "initial"]);

        let now = Utc::now();
        let manager = WorktreeManager::open(&repository).unwrap();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker.id, "HEAD", now)
            .unwrap();
        let base_revision = binding.base_revision.clone();
        worker.workspace = Some(binding.clone());
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "integration".into(),
            },
            worker.id.clone(),
            &base_revision,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        let database_path = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database_path).unwrap());
        let mut changed_contract = SharedContract::new(
            "goal-a",
            "database-schema",
            "Database schema",
            ContractKind::PersistedSchema,
            now,
        );
        changed_contract.revise(worker.id.clone(), "adds an optional table", now);
        store.upsert_contract(&changed_contract).unwrap();
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();

        let worktree = Path::new(&binding.worktree_path);
        fs::write(worktree.join("README.md"), "initial\nworker change\n").unwrap();
        fs::write(
            worktree.join("migration.sql"),
            "CREATE TABLE example(id);\n",
        )
        .unwrap();
        git(worktree, &["add", "README.md", "migration.sql"]);
        git(
            worktree,
            &["commit", "-q", "-m", "add integration evidence"],
        );

        let service = IntegrationArtifactService::new(store.clone());
        let artifact = service
            .capture(
                CaptureIntegrationArtifactRequest {
                    claim_id: claim.id.clone(),
                    worker_id: worker.id.clone(),
                    changed_contracts: vec![IntegrationContractChange {
                        contract_id: changed_contract.id.clone(),
                        stable_key: "database-schema".into(),
                        previous_revision: 1,
                        revision: 2,
                        compatibility_notes: "adds an optional table".into(),
                    }],
                    migrations: vec![IntegrationMigration {
                        path: "migration.sql".into(),
                        description: "create the example table".into(),
                        reversible: false,
                    }],
                    validations: vec![IntegrationValidationEvidence {
                        command: "cargo test".into(),
                        succeeded: true,
                        evidence_refs: vec!["test-run:1".into()],
                        completed_at: now,
                    }],
                    evidence_refs: vec!["slice-1".into()],
                    known_risks: vec!["migration is forward-only".into()],
                },
                now,
            )
            .unwrap();

        assert_eq!(artifact.base_revision, base_revision);
        assert_eq!(
            artifact.head_revision,
            git(worktree, &["rev-parse", "HEAD"])
        );
        assert_eq!(artifact.commits.len(), 1);
        assert_eq!(artifact.commits[0].subject, "add integration evidence");
        assert_eq!(artifact.diff_summary.files_changed, 2);
        assert_eq!(artifact.diff_summary.insertions, 2);
        assert_eq!(
            artifact.diff_summary.changed_paths,
            vec!["README.md", "migration.sql"]
        );
        assert_eq!(artifact.changed_contracts.len(), 1);
        assert_eq!(artifact.migrations.len(), 1);
        assert_eq!(artifact.validations.len(), 1);
        assert_eq!(artifact.evidence_refs, vec!["slice-1"]);
        assert_eq!(artifact.known_risks, vec!["migration is forward-only"]);

        let preflight = IntegrationPreflightService::new(store.clone());
        let clean = preflight.run(&artifact.id, now).unwrap();
        assert!(clean.passed);
        assert!(clean.checks.iter().all(|check| check.passed));

        let runner = ValidationGateRunner::new(store.clone());
        let passing_gates = [
            ValidationGateKind::Format,
            ValidationGateKind::TypeCheck,
            ValidationGateKind::Unit,
            ValidationGateKind::Integration,
            ValidationGateKind::Security,
            ValidationGateKind::Policy,
            ValidationGateKind::Repository,
        ]
        .into_iter()
        .enumerate()
        .map(|(index, kind)| ValidationGateSpec {
            id: format!("gate-{index}"),
            kind,
            program: "git".into(),
            args: vec!["diff".into(), "--check".into()],
            required: true,
            max_output_bytes: 4_096,
        })
        .collect::<Vec<_>>();
        let passing_report = runner.run(&artifact.id, &passing_gates, now).unwrap();
        assert!(passing_report.passed);
        assert_eq!(passing_report.results.len(), 7);
        assert!(passing_report.results.iter().all(|result| result.succeeded));

        let failing_report = runner
            .run(
                &artifact.id,
                &[ValidationGateSpec {
                    id: "required-failure".into(),
                    kind: ValidationGateKind::Repository,
                    program: "git".into(),
                    args: vec![
                        "rev-parse".into(),
                        "--verify".into(),
                        "missing-revision".into(),
                    ],
                    required: true,
                    max_output_bytes: 128,
                }],
                now,
            )
            .unwrap();
        assert!(!failing_report.passed);
        assert!(!failing_report.results[0].succeeded);
        assert!(failing_report.results[0].stderr_excerpt.len() <= 128);

        fs::write(worktree.join("unexpected.txt"), "not captured\n").unwrap();
        changed_contract.revise(worker.id.clone(), "adds a required column", now);
        store.upsert_contract(&changed_contract).unwrap();
        for (kind, key) in [
            (SignalKind::FileOverlap, "overlap:readme"),
            (SignalKind::MergeConflict, "conflict:readme"),
        ] {
            let signal = CoordinationSignal::new(
                "goal-a",
                key,
                kind,
                100,
                EventSeverity::Warning,
                EventId::new(),
                vec![worker.id.clone()],
                vec![claim.id.clone()],
                now,
                now + Duration::minutes(5),
            )
            .unwrap();
            store.merge_signal_observation(&signal, now).unwrap();
        }
        let rejected = preflight.run(&artifact.id, now).unwrap();
        assert!(!rejected.passed);
        for kind in [
            PreflightCheckKind::RepositoryState,
            PreflightCheckKind::ContractRevisions,
            PreflightCheckKind::OverlapSignals,
            PreflightCheckKind::PredictedConflicts,
        ] {
            assert!(
                rejected
                    .checks
                    .iter()
                    .any(|check| check.kind == kind && !check.passed)
            );
        }

        drop(preflight);
        drop(runner);
        drop(service);
        drop(store);
        let reopened = SqliteCoordinationStore::open(&database_path).unwrap();
        assert_eq!(
            reopened.integration_artifact(&artifact.id).unwrap(),
            Some(artifact.clone())
        );
        assert_eq!(
            reopened.integration_artifacts_for_goal("goal-a").unwrap(),
            vec![artifact]
        );
        assert_eq!(
            reopened
                .validation_reports_for_artifact(&passing_report.artifact_id)
                .unwrap(),
            vec![passing_report, failing_report]
        );
    }

    #[test]
    fn synchronizes_near_integration_or_on_material_invalidation_only() {
        let directory = TempDir::new().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        let mut contract =
            SharedContract::new("goal-a", "public-api", "Public API", ContractKind::Api, now);
        store.upsert_contract(&contract).unwrap();
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "consumer".into(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        claim.consumes_contracts.push(ContractExpectation {
            contract_id: contract.id.as_str().into(),
            expected_revision: 1,
        });
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let policy = SynchronizationPolicyService::new(store.clone());

        let routine = policy
            .evaluate(
                &worker.id,
                &claim.id,
                SynchronizationPhase::Implementation,
                now,
            )
            .unwrap();
        assert_eq!(routine.disposition, SynchronizationDisposition::Deferred);
        assert!(routine.causes.is_empty());

        let integration = policy
            .evaluate(
                &worker.id,
                &claim.id,
                SynchronizationPhase::Integration,
                now,
            )
            .unwrap();
        assert_eq!(
            integration.disposition,
            SynchronizationDisposition::Required
        );
        assert_eq!(
            integration.causes[0].kind,
            SynchronizationCauseKind::IntegrationBoundary
        );

        contract.revise(worker.id.clone(), "breaking response shape", now);
        store.upsert_contract(&contract).unwrap();
        let invalidated = policy
            .evaluate(&worker.id, &claim.id, SynchronizationPhase::Validation, now)
            .unwrap();
        assert_eq!(
            invalidated.disposition,
            SynchronizationDisposition::Required
        );
        assert_eq!(
            invalidated.causes[0].kind,
            SynchronizationCauseKind::ContractInvalidated
        );

        let dependency_signal = CoordinationSignal::new(
            "goal-a",
            "dependency:consumer",
            SignalKind::DependencyChanged,
            100,
            EventSeverity::Warning,
            EventId::new(),
            vec![worker.id.clone()],
            vec![claim.id.clone()],
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        store
            .merge_signal_observation(&dependency_signal, now)
            .unwrap();
        let with_dependency = policy
            .evaluate(
                &worker.id,
                &claim.id,
                SynchronizationPhase::Implementation,
                now,
            )
            .unwrap();
        assert!(
            with_dependency
                .causes
                .iter()
                .any(|cause| cause.kind == SynchronizationCauseKind::DependencyChanged)
        );
    }

    #[test]
    fn reconciles_with_backup_refs_policy_and_durable_manual_fallback() {
        let directory = TempDir::new().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &["config", "user.email", "golazo@example.test"],
        );
        git(&repository, &["config", "user.name", "Golazo Tests"]);
        fs::write(repository.join("README.md"), "initial\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-q", "-m", "initial"]);

        let now = Utc::now();
        let manager = WorktreeManager::open(&repository).unwrap();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker.id, "HEAD", now)
            .unwrap();
        worker.workspace = Some(binding.clone());
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "reconciliation".into(),
            },
            worker.id.clone(),
            &binding.base_revision,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let worktree = Path::new(&binding.worktree_path);
        fs::write(worktree.join("worker.txt"), "worker\n").unwrap();
        git(worktree, &["add", "worker.txt"]);
        git(worktree, &["commit", "-q", "-m", "worker change"]);
        let artifact = IntegrationArtifactService::new(store.clone())
            .capture(
                CaptureIntegrationArtifactRequest {
                    claim_id: claim.id.clone(),
                    worker_id: worker.id.clone(),
                    changed_contracts: Vec::new(),
                    migrations: Vec::new(),
                    validations: Vec::new(),
                    evidence_refs: vec!["worker-change".into()],
                    known_risks: Vec::new(),
                },
                now,
            )
            .unwrap();

        let policy = ReconciliationPolicy {
            allowed_strategies: vec![
                ReconciliationStrategy::Rebase,
                ReconciliationStrategy::Merge,
                ReconciliationStrategy::CherryPick,
                ReconciliationStrategy::Regenerate,
                ReconciliationStrategy::Manual,
            ],
            regeneration_programs: vec!["git".into()],
            max_output_bytes: 4_096,
        };
        let service = ReconciliationService::new(store.clone(), policy);
        let manual = service
            .reconcile(
                ReconciliationRequest {
                    artifact_id: artifact.id.clone(),
                    strategy: ReconciliationStrategy::Manual,
                    target_revision: String::new(),
                    commit_revisions: Vec::new(),
                    regeneration: None,
                    manual_instructions: Some(
                        "review the semantic conflict and record the selected resolution".into(),
                    ),
                },
                now,
            )
            .unwrap();
        assert_eq!(manual.state, ReconciliationState::ManualRequired);
        assert!(manual.workspace_preserved);

        let regeneration = service
            .reconcile(
                ReconciliationRequest {
                    artifact_id: artifact.id.clone(),
                    strategy: ReconciliationStrategy::Regenerate,
                    target_revision: String::new(),
                    commit_revisions: Vec::new(),
                    regeneration: Some(RegenerationCommand {
                        program: "git".into(),
                        args: vec!["status".into(), "--short".into()],
                    }),
                    manual_instructions: None,
                },
                now,
            )
            .unwrap();
        assert_eq!(regeneration.state, ReconciliationState::Succeeded);
        assert!(regeneration.backup_ref.starts_with("refs/golazo/"));

        fs::write(repository.join("main.txt"), "main\n").unwrap();
        git(&repository, &["add", "main.txt"]);
        git(&repository, &["commit", "-q", "-m", "main change"]);
        let target = git(&repository, &["rev-parse", "HEAD"]);
        let rebased = service
            .reconcile(
                ReconciliationRequest {
                    artifact_id: artifact.id.clone(),
                    strategy: ReconciliationStrategy::Rebase,
                    target_revision: target,
                    commit_revisions: Vec::new(),
                    regeneration: None,
                    manual_instructions: None,
                },
                now,
            )
            .unwrap();
        assert_eq!(rebased.state, ReconciliationState::Succeeded);
        assert_eq!(rebased.previous_head, artifact.head_revision);
        assert_ne!(rebased.resulting_head, rebased.previous_head);
        assert_eq!(
            git(worktree, &["rev-parse", &rebased.backup_ref]),
            rebased.previous_head
        );
        assert_eq!(
            store.reconciliation(&rebased.id).unwrap(),
            Some(rebased.clone())
        );
        assert_eq!(
            store.reconciliations_for_artifact(&artifact.id).unwrap(),
            vec![manual, regeneration, rebased.clone()]
        );

        fs::write(worktree.join("README.md"), "worker conflict\n").unwrap();
        git(worktree, &["add", "README.md"]);
        git(worktree, &["commit", "-q", "-m", "worker conflict"]);
        let conflict_artifact = IntegrationArtifactService::new(store.clone())
            .capture(
                CaptureIntegrationArtifactRequest {
                    claim_id: claim.id.clone(),
                    worker_id: worker.id.clone(),
                    changed_contracts: Vec::new(),
                    migrations: Vec::new(),
                    validations: Vec::new(),
                    evidence_refs: vec!["conflict-candidate".into()],
                    known_risks: vec!["README may conflict".into()],
                },
                now,
            )
            .unwrap();
        fs::write(repository.join("README.md"), "main conflict\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-q", "-m", "main conflict"]);
        let conflict_target = git(&repository, &["rev-parse", "HEAD"]);
        let failed = service
            .reconcile(
                ReconciliationRequest {
                    artifact_id: conflict_artifact.id.clone(),
                    strategy: ReconciliationStrategy::Rebase,
                    target_revision: conflict_target,
                    commit_revisions: Vec::new(),
                    regeneration: None,
                    manual_instructions: Some("resolve README semantics manually".into()),
                },
                now,
            )
            .unwrap();
        assert_eq!(failed.state, ReconciliationState::Failed);
        assert_eq!(failed.abort_succeeded, Some(true));
        assert_eq!(failed.resulting_head, failed.previous_head);
        assert_eq!(
            git(worktree, &["rev-parse", &failed.backup_ref]),
            failed.previous_head
        );
        assert_eq!(
            store
                .reconciliations_for_artifact(&conflict_artifact.id)
                .unwrap(),
            vec![failed]
        );

        let merge_request = ReconciliationRequest {
            artifact_id: artifact.id,
            strategy: ReconciliationStrategy::Merge,
            target_revision: "target".into(),
            commit_revisions: Vec::new(),
            regeneration: None,
            manual_instructions: None,
        };
        assert_eq!(
            reconciliation_command(&merge_request, "resolved-target", &[])
                .unwrap()
                .1,
            vec!["merge", "--no-edit", "--no-ff", "resolved-target"]
        );
        let mut cherry_request = merge_request;
        cherry_request.strategy = ReconciliationStrategy::CherryPick;
        cherry_request.commit_revisions = vec!["commit-a".into()];
        assert_eq!(
            reconciliation_command(&cherry_request, "", &["resolved-a".into()])
                .unwrap()
                .1,
            vec!["cherry-pick", "resolved-a"]
        );
    }
}
