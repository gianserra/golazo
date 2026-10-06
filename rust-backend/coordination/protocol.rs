use super::claims::{
    BlockEvidence, CancelEvidence, ClaimPolicy, ClaimService, ClaimServiceError,
    CompletionEvidence, ReleaseEvidence,
};
use super::contracts::{ContractRegistry, ContractRegistryError};
use super::domain::{
    ActivityCategory, ActivityEventPayload, BlockerEventPayload, Claim, ClaimId, ClaimScope,
    ClaimState, CompletionEventPayload, ContractEventPayload, ContractExpectation, ContractId,
    CoordinationActor, CoordinationEvent, CoordinationEventKind, CoordinationEventPayload,
    CoordinationExchange, CoordinationRequestEventPayload, DomainError, EscalationId, EventId,
    EventSeverity, ExchangeId, NotificationId, NotificationState, SharedContract, Worker, WorkerId,
    WorkerNotification, WorkerPermissionProfile, WorkerState, WorkerTurnBinding, WorkspaceBinding,
};
use super::notifications::{
    CoordinationExchangeError, CoordinationExchangeService, CoordinationRequestError,
    CoordinationRequestService, NotificationLifecycleError, NotificationLifecycleService,
};
use super::store::{
    ClaimRepository, ContractRepository, EventRepository, NotificationRepository,
    SqliteCoordinationStore, StoreError, WorkPackageRepository, WorkerRepository,
};
use crate::models::{Feature, GoalRecord, Status, TrackerWorkPackage};
use crate::tracker::{Tracker, TrackerError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;

pub const WORKER_CONTEXT_PACKET_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerContextLimits {
    pub max_encoded_bytes: usize,
    pub max_goal_description_chars: usize,
    pub max_feature_description_chars: usize,
    pub max_text_chars: usize,
    pub max_features: usize,
    pub max_steps_per_feature: usize,
    pub max_dependencies: usize,
    pub max_contracts: usize,
    pub max_events: usize,
    pub max_notifications: usize,
    pub max_event_payload_chars: usize,
    pub max_prior_turns: usize,
}

impl Default for WorkerContextLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 48 * 1024,
            max_goal_description_chars: 2_000,
            max_feature_description_chars: 1_200,
            max_text_chars: 300,
            max_features: 8,
            max_steps_per_feature: 24,
            max_dependencies: 8,
            max_contracts: 16,
            max_events: 20,
            max_notifications: 8,
            max_event_payload_chars: 1_000,
            max_prior_turns: 8,
        }
    }
}

#[derive(Debug, Error)]
pub enum WorkerContextError {
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("worker was not found: {0}")]
    WorkerNotFound(String),
    #[error("claim was not found: {0}")]
    ClaimNotFound(String),
    #[error("worker {worker_id} does not own claim {claim_id}")]
    ClaimOwnerMismatch { worker_id: String, claim_id: String },
    #[error("worker and claim belong to different goals")]
    GoalMismatch,
    #[error("worker has no isolated workspace")]
    MissingWorkspace,
    #[error("worker has no permission profile")]
    MissingPermissionProfile,
    #[error("claimed tracker scope is missing: {0}")]
    MissingTrackerScope(String),
    #[error("worker context identity exceeds {maximum} characters: {label}")]
    IdentityTooLong { label: &'static str, maximum: usize },
    #[error("worker context packet is {actual} bytes; maximum is {maximum}")]
    PacketTooLarge { actual: usize, maximum: usize },
}

#[derive(Debug, Error)]
pub enum WorkerToolError {
    #[error(transparent)]
    Context(#[from] WorkerContextError),
    #[error(transparent)]
    Claim(#[from] ClaimServiceError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    ContractRegistry(#[from] ContractRegistryError),
    #[error(transparent)]
    Notification(#[from] NotificationLifecycleError),
    #[error(transparent)]
    CoordinationRequest(#[from] CoordinationRequestError),
    #[error(transparent)]
    CoordinationExchange(#[from] CoordinationExchangeError),
    #[error("worker operation requires an active owned claim")]
    InactiveOrUnownedClaim,
    #[error("shared contract was not found: {0}")]
    ContractNotFound(String),
    #[error("shared contract belongs to a different goal")]
    ContractGoalMismatch,
    #[error("claim does not authorize changes to shared contract {0}")]
    ContractChangeNotAuthorized(String),
    #[error("shared contract revision changed: expected {expected}, current {current}")]
    ContractRevisionMismatch { expected: u64, current: u64 },
    #[error("worker tool payload exceeds its bounded input policy: {0}")]
    PayloadTooLarge(&'static str),
    #[error("worker evidence must include at least one non-empty reference")]
    EmptyEvidence,
    #[error("worker activity is invalid: {0}")]
    InvalidActivity(&'static str),
    #[error("claim completion prerequisites are not satisfied: {0}")]
    CompletionGateFailed(String),
    #[error("worker has no active run and thread to roll over")]
    NoActiveTurn,
    #[error("worker already has an active run")]
    ActiveRunExists,
    #[error("context transfer artifact was not found: {0}")]
    ContextTransferNotFound(String),
    #[error("context transfer artifact does not belong to this worker and active claim")]
    ContextTransferMismatch,
    #[error("context transfer artifact was already used")]
    ContextTransferAlreadyUsed,
    #[error("context continuation must use a new bounded run and thread identity")]
    InvalidContinuation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerContextPacket {
    pub version: u16,
    pub assembled_at: DateTime<Utc>,
    pub goal: GoalContext,
    pub worker: WorkerContext,
    pub assignment: AssignmentContext,
    pub repository: WorkspaceBinding,
    pub policy: WorkerPolicyContext,
    pub prior_state: PriorStateContext,
    pub truncation: TruncationContext,
}

impl WorkerContextPacket {
    pub fn execution_instructions(&self) -> Result<String, serde_json::Error> {
        Ok(crate::prompts::autonomous_worker(
            &serde_json::to_string_pretty(self)?,
        ))
    }

    pub fn revision(&self) -> Result<WorkerContextRevision, serde_json::Error> {
        let mut contract_revisions = self
            .assignment
            .contracts
            .iter()
            .map(|contract| ContractRevision {
                id: contract.id.clone(),
                revision: contract.revision,
            })
            .collect::<Vec<_>>();
        contract_revisions.sort_by(|left, right| left.id.cmp(&right.id));
        let latest_event_sequence = self
            .prior_state
            .events
            .iter()
            .filter_map(|event| event.sequence)
            .max();
        let scope_fingerprint = hash_json(&serde_json::json!({
            "package": self.assignment.package,
            "features": self.assignment.features,
        }))?;
        let dependency_fingerprint = hash_json(&serde_json::json!({
            "dependencies": self.assignment.dependencies,
        }))?;
        let material = serde_json::json!({
            "goalId": self.goal.id,
            "workerId": self.worker.id,
            "scopeFingerprint": scope_fingerprint,
            "dependencyFingerprint": dependency_fingerprint,
            "claimId": self.assignment.claim.id,
            "leaseGeneration": self.assignment.claim.lease_generation,
            "heartbeatAt": self.assignment.claim.heartbeat_at,
            "contracts": contract_revisions,
            "latestEventSequence": latest_event_sequence,
            "notifications": self.prior_state.notifications.iter().map(|notification| serde_json::json!({
                "id": notification.id,
                "state": notification.state,
            })).collect::<Vec<_>>(),
            "repositoryId": self.repository.repository_id,
            "baseRevision": self.repository.base_revision,
        });
        let bytes = serde_json::to_vec(&material)?;
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Ok(WorkerContextRevision {
            tracker_updated_at: self.goal.tracker_updated_at.clone(),
            claim_id: self.assignment.claim.id.clone(),
            lease_generation: self.assignment.claim.lease_generation,
            heartbeat_at: self.assignment.claim.heartbeat_at,
            scope_fingerprint,
            dependency_fingerprint,
            contract_revisions,
            latest_event_sequence,
            repository_id: self.repository.repository_id.clone(),
            base_revision: self.repository.base_revision.clone(),
            fingerprint: format!("sha256:{:x}", hasher.finalize()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerBoundary {
    MajorEdit,
    Validation,
    Synchronization,
    Integration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRecoveryKind {
    WorkerFailure,
    ThreadFailure,
    BackendRestart,
    WorkspaceRecovery,
    ContextRollover,
}

impl WorkerRecoveryKind {
    fn label(self) -> &'static str {
        match self {
            Self::WorkerFailure => "worker failure",
            Self::ThreadFailure => "thread failure",
            Self::BackendRestart => "backend restart",
            Self::WorkspaceRecovery => "workspace recovery",
            Self::ContextRollover => "context rollover",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractRevision {
    pub id: String,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerContextRevision {
    pub tracker_updated_at: String,
    pub claim_id: String,
    pub lease_generation: u64,
    pub heartbeat_at: DateTime<Utc>,
    pub scope_fingerprint: String,
    pub dependency_fingerprint: String,
    pub contract_revisions: Vec<ContractRevision>,
    pub latest_event_sequence: Option<u64>,
    pub repository_id: String,
    pub base_revision: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssumptionDisposition {
    Current,
    RefreshRequired,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloverReason {
    TokenLimit,
    ContextLimit,
    TimeLimit,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextTransferArtifact {
    pub id: String,
    pub goal_id: String,
    pub worker_id: String,
    pub claim_id: String,
    pub reason: RolloverReason,
    pub from_run_id: String,
    pub from_thread_id: String,
    pub summary: String,
    pub completed_evidence: Vec<String>,
    pub unresolved_assumptions: Vec<String>,
    pub next_safe_action: String,
    pub repository: WorkspaceBinding,
    pub context_revision: WorkerContextRevision,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerPublishableEventKind {
    Activity,
    Validation,
    CoordinationRequest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerActivityKind {
    Progress,
    ScopeChanged,
    Validation,
    Artifact,
    Blocker,
    Completion,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerActivity {
    pub kind: WorkerActivityKind,
    pub summary: String,
    #[serde(default)]
    pub progress_percent: Option<u8>,
    #[serde(default)]
    pub changed_scope: Vec<String>,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub validation_succeeded: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum WorkerToolRequest {
    RefreshContext {
        claim_id: String,
    },
    RefreshAtBoundary {
        claim_id: String,
        boundary: WorkerBoundary,
    },
    CheckAssumptions {
        claim_id: String,
        boundary: WorkerBoundary,
        expected_revision: WorkerContextRevision,
    },
    PrepareRollover {
        claim_id: String,
        reason: RolloverReason,
        summary: String,
        completed_evidence: Vec<String>,
        unresolved_assumptions: Vec<String>,
        next_safe_action: String,
    },
    ContinueFromTransfer {
        claim_id: String,
        artifact_id: String,
        run_id: String,
        thread_id: String,
    },
    RecoverContext {
        claim_id: String,
        recovery_kind: WorkerRecoveryKind,
    },
    Heartbeat {
        claim_id: String,
        expected_generation: u64,
    },
    ExpandClaim {
        claim_id: String,
        scope: ClaimScope,
        rationale: String,
        base_revision: String,
        idempotency_key: String,
    },
    PublishEvent {
        claim_id: String,
        kind: WorkerPublishableEventKind,
        severity: EventSeverity,
        payload: serde_json::Value,
        idempotency_key: String,
    },
    PublishActivity {
        claim_id: String,
        activity: WorkerActivity,
        idempotency_key: String,
    },
    ReviseContract {
        claim_id: String,
        contract_id: String,
        expected_revision: u64,
        compatibility_notes: String,
    },
    RecordEvidence {
        claim_id: String,
        summary: String,
        evidence_refs: Vec<String>,
        idempotency_key: String,
    },
    RequestCoordination {
        claim_id: String,
        target_scope: String,
        desired_outcome: String,
        evidence_refs: Vec<String>,
        severity: EventSeverity,
        idempotency_key: String,
    },
    OpenCoordinationExchange {
        claim_id: String,
        request_event_id: String,
        target_worker_id: String,
        insufficiency_reason: String,
        ttl_seconds: u64,
    },
    PostCoordinationExchange {
        claim_id: String,
        exchange_id: String,
        body: String,
        evidence_refs: Vec<String>,
    },
    CompleteCoordinationExchange {
        claim_id: String,
        exchange_id: String,
        shared_note: String,
    },
    AcknowledgeNotification {
        claim_id: String,
        notification_id: String,
    },
    ActOnNotification {
        claim_id: String,
        notification_id: String,
        outcome: String,
    },
    Release {
        claim_id: String,
        expected_generation: u64,
        reason: String,
        artifact_id: Option<String>,
        evidence_refs: Vec<String>,
        idempotency_key: String,
    },
    Block {
        claim_id: String,
        expected_generation: u64,
        reason: String,
        evidence_refs: Vec<String>,
        escalation_id: Option<String>,
        idempotency_key: String,
    },
    Cancel {
        claim_id: String,
        expected_generation: u64,
        reason: String,
        evidence_refs: Vec<String>,
        idempotency_key: String,
    },
    Complete {
        claim_id: String,
        expected_generation: u64,
        reason: String,
        artifact_id: String,
        integration_revision: String,
        evidence_refs: Vec<String>,
        validation_event_sequence: u64,
        audit_slice_id: String,
        context_revision: WorkerContextRevision,
        integration_boundary_satisfied: bool,
        idempotency_key: String,
    },
}

impl WorkerToolRequest {
    fn claim_id(&self) -> &str {
        match self {
            Self::RefreshContext { claim_id }
            | Self::RefreshAtBoundary { claim_id, .. }
            | Self::CheckAssumptions { claim_id, .. }
            | Self::PrepareRollover { claim_id, .. }
            | Self::ContinueFromTransfer { claim_id, .. }
            | Self::RecoverContext { claim_id, .. }
            | Self::Heartbeat { claim_id, .. }
            | Self::ExpandClaim { claim_id, .. }
            | Self::PublishEvent { claim_id, .. }
            | Self::PublishActivity { claim_id, .. }
            | Self::ReviseContract { claim_id, .. }
            | Self::RecordEvidence { claim_id, .. }
            | Self::RequestCoordination { claim_id, .. }
            | Self::OpenCoordinationExchange { claim_id, .. }
            | Self::PostCoordinationExchange { claim_id, .. }
            | Self::CompleteCoordinationExchange { claim_id, .. }
            | Self::AcknowledgeNotification { claim_id, .. }
            | Self::ActOnNotification { claim_id, .. }
            | Self::Release { claim_id, .. }
            | Self::Block { claim_id, .. }
            | Self::Cancel { claim_id, .. }
            | Self::Complete { claim_id, .. } => claim_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum WorkerToolResponse {
    Context {
        packet: Box<WorkerContextPacket>,
    },
    BoundaryContext {
        boundary: WorkerBoundary,
        revision: WorkerContextRevision,
        packet: Box<WorkerContextPacket>,
    },
    AssumptionStatus {
        boundary: WorkerBoundary,
        disposition: AssumptionDisposition,
        changes: Vec<String>,
        revision: WorkerContextRevision,
        packet: Box<WorkerContextPacket>,
    },
    TransferPrepared {
        artifact: Box<ContextTransferArtifact>,
        worker: Worker,
    },
    ContinuationStarted {
        artifact: Box<ContextTransferArtifact>,
        worker: Worker,
        recovery_context: Box<WorkerContextPacket>,
        replayed_notification_ids: Vec<String>,
    },
    RecoveryContext {
        recovery_kind: WorkerRecoveryKind,
        revision: WorkerContextRevision,
        packet: Box<WorkerContextPacket>,
        replayed_notification_ids: Vec<String>,
    },
    Claim {
        claim: Claim,
    },
    Event {
        sequence: u64,
    },
    CoordinationRequested {
        event_id: String,
        sequence: u64,
        target_worker_ids: Vec<String>,
        notification_ids: Vec<String>,
    },
    Contract {
        contract: SharedContract,
    },
    Notification {
        notification: WorkerNotification,
    },
    CoordinationExchange {
        exchange: CoordinationExchange,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalContext {
    pub id: String,
    pub title: String,
    pub description: String,
    pub tracker_updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerContext {
    pub id: String,
    pub state: WorkerState,
    pub current_run_id: Option<String>,
    pub current_thread_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentContext {
    pub claim: ClaimContext,
    pub package: Option<PackageContext>,
    pub features: Vec<FeatureContext>,
    pub dependencies: Vec<DependencyContext>,
    pub contracts: Vec<ContractContext>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimContext {
    pub id: String,
    pub scope: ClaimScope,
    pub base_revision: String,
    pub lease_generation: u64,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub expansion_count: usize,
    pub overlap_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PackageContext {
    pub id: String,
    pub title: String,
    pub description: String,
    pub priority: i32,
    pub status: Status,
    pub integration_scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeatureContext {
    pub id: String,
    pub title: String,
    pub description: String,
    pub status: Status,
    pub steps: Vec<StepContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepContext {
    pub id: String,
    pub title: String,
    pub done: bool,
    pub next: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyContext {
    pub id: String,
    pub title: String,
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractContext {
    pub id: String,
    pub stable_key: String,
    pub title: String,
    pub revision: u64,
    pub expected_revision: Option<u64>,
    pub compatibility_notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPolicyContext {
    pub permissions: WorkerPermissionProfile,
    pub workspace_isolation_required: bool,
    pub refresh_before: Vec<String>,
    pub completion_requires: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PriorStateContext {
    pub turns: Vec<TurnContext>,
    pub events: Vec<EventContext>,
    #[serde(default)]
    pub notifications: Vec<NotificationContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnContext {
    pub sequence: u64,
    pub run_id: String,
    pub thread_id: String,
    pub completed_at: Option<DateTime<Utc>>,
    pub context_transfer_artifact_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventContext {
    pub sequence: Option<u64>,
    pub kind: String,
    pub severity: String,
    pub occurred_at: DateTime<Utc>,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationContext {
    pub id: String,
    pub source_event_id: String,
    pub state: NotificationState,
    pub purpose: String,
    pub summary: String,
    pub severity: String,
    pub evidence_refs: Vec<String>,
    pub recommended_action: Option<String>,
    pub required_acknowledgement: bool,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TruncationContext {
    pub omitted_features: usize,
    pub omitted_dependencies: usize,
    pub omitted_contracts: usize,
    pub omitted_turns: usize,
    #[serde(default)]
    pub omitted_notifications: usize,
}

#[derive(Debug, Clone)]
pub struct WorkerContextAssembler {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    limits: WorkerContextLimits,
}

impl WorkerContextAssembler {
    pub fn new(
        tracker: Tracker,
        store: Arc<SqliteCoordinationStore>,
        limits: WorkerContextLimits,
    ) -> Self {
        Self {
            tracker,
            store,
            limits,
        }
    }

    pub fn assemble(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<WorkerContextPacket, WorkerContextError> {
        self.assemble_with_pending_notifications(worker_id, claim_id, now, false)
    }

    pub fn assemble_recovery(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<WorkerContextPacket, WorkerContextError> {
        self.assemble_with_pending_notifications(worker_id, claim_id, now, true)
    }

    fn assemble_with_pending_notifications(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        now: DateTime<Utc>,
        replay_pending: bool,
    ) -> Result<WorkerContextPacket, WorkerContextError> {
        let worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| WorkerContextError::WorkerNotFound(worker_id.as_str().into()))?;
        let claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| WorkerContextError::ClaimNotFound(claim_id.as_str().into()))?;
        if claim.owner != worker.id {
            return Err(WorkerContextError::ClaimOwnerMismatch {
                worker_id: worker.id.as_str().into(),
                claim_id: claim.id.as_str().into(),
            });
        }
        if claim.goal_id != worker.goal_id {
            return Err(WorkerContextError::GoalMismatch);
        }
        check_identity("goal id", &worker.goal_id, self.limits.max_text_chars)?;
        check_identity("worker id", worker.id.as_str(), self.limits.max_text_chars)?;
        check_identity("claim id", claim.id.as_str(), self.limits.max_text_chars)?;

        let goal_value = self.tracker.get_goal(&worker.goal_id)?;
        let goal: GoalRecord = serde_json::from_value(goal_value)?;
        let (tracker_package, feature_ids) = tracker_scope(&goal, &claim)?;
        let features = feature_contexts(&goal, &feature_ids, &self.limits);
        let omitted_features = feature_ids.len().saturating_sub(features.len());
        let (dependencies, omitted_dependencies) =
            dependency_contexts(&goal, tracker_package, &self.limits);
        let package_record = match &claim.scope {
            ClaimScope::WorkPackage { work_package_id } => {
                self.store.work_package(work_package_id)?
            }
            ClaimScope::Feature { .. } => None,
        };
        let all_contracts = self.store.contracts_for_goal(&worker.goal_id)?;
        let (contracts, omitted_contracts) =
            contract_contexts(all_contracts, package_record.as_ref(), &claim, &self.limits);
        let events = self.store.latest_events_for_goal(
            &worker.goal_id,
            Some(claim.id.as_str()),
            self.limits.max_events,
        )?;
        let queued_notifications = self
            .store
            .notifications_for_worker(&worker.id, None)?
            .into_iter()
            .filter(|notification| {
                notification.goal_id == worker.goal_id
                    && (notification.state == NotificationState::Queued
                        || (replay_pending
                            && matches!(
                                notification.state,
                                NotificationState::Delivered | NotificationState::Acknowledged
                            )))
            })
            .collect();
        let (notifications, omitted_notifications) =
            notification_contexts(queued_notifications, now, &self.limits);
        let (turns, omitted_turns) = turn_contexts(&worker.turn_history, &self.limits);
        let workspace = worker
            .workspace
            .clone()
            .ok_or(WorkerContextError::MissingWorkspace)?;
        let permissions = worker
            .permission_profile
            .clone()
            .ok_or(WorkerContextError::MissingPermissionProfile)?;
        let packet = WorkerContextPacket {
            version: WORKER_CONTEXT_PACKET_VERSION,
            assembled_at: now,
            goal: GoalContext {
                id: goal.goal_id.clone(),
                title: bounded(&goal.title, self.limits.max_text_chars),
                description: bounded(&goal.description, self.limits.max_goal_description_chars),
                tracker_updated_at: goal.updated_at.clone(),
            },
            worker: WorkerContext {
                id: worker.id.as_str().into(),
                state: worker.state,
                current_run_id: worker.current_run_id,
                current_thread_id: worker.current_thread_id,
            },
            assignment: AssignmentContext {
                claim: claim_context(&claim),
                package: tracker_package.map(|package| PackageContext {
                    id: package.id.clone(),
                    title: bounded(&package.title, self.limits.max_text_chars),
                    description: bounded(
                        &package.description,
                        self.limits.max_feature_description_chars,
                    ),
                    priority: package.priority,
                    status: package.status,
                    integration_scope: enum_string(&package.integration_scope),
                }),
                features,
                dependencies,
                contracts,
            },
            repository: workspace,
            policy: WorkerPolicyContext {
                permissions,
                workspace_isolation_required: true,
                refresh_before: vec![
                    "major_edits".into(),
                    "validation".into(),
                    "synchronization".into(),
                    "integration".into(),
                ],
                completion_requires: vec![
                    "validation_evidence".into(),
                    "tracker_update".into(),
                    "audit_slice".into(),
                    "contract_check".into(),
                    "integration_boundary".into(),
                ],
            },
            prior_state: PriorStateContext {
                turns,
                events: event_contexts(events, &self.limits),
                notifications,
            },
            truncation: TruncationContext {
                omitted_features,
                omitted_dependencies,
                omitted_contracts,
                omitted_turns,
                omitted_notifications,
            },
        };
        let actual = serde_json::to_vec(&packet)?.len();
        if actual > self.limits.max_encoded_bytes {
            return Err(WorkerContextError::PacketTooLarge {
                actual,
                maximum: self.limits.max_encoded_bytes,
            });
        }
        Ok(packet)
    }
}

#[derive(Debug, Clone)]
pub struct WorkerProtocolService {
    assembler: WorkerContextAssembler,
    claims: ClaimService,
    contracts: ContractRegistry,
    notifications: NotificationLifecycleService,
    coordination_requests: CoordinationRequestService,
    exchanges: CoordinationExchangeService,
    store: Arc<SqliteCoordinationStore>,
    tracker: Tracker,
}

impl WorkerProtocolService {
    pub fn new(
        tracker: Tracker,
        store: Arc<SqliteCoordinationStore>,
        claim_policy: ClaimPolicy,
        context_limits: WorkerContextLimits,
    ) -> Self {
        Self {
            assembler: WorkerContextAssembler::new(
                tracker.clone(),
                Arc::clone(&store),
                context_limits,
            ),
            claims: ClaimService::new(tracker.clone(), Arc::clone(&store), claim_policy),
            contracts: ContractRegistry::new(Arc::clone(&store)),
            notifications: NotificationLifecycleService::new(Arc::clone(&store)),
            coordination_requests: CoordinationRequestService::new(Arc::clone(&store)),
            exchanges: CoordinationExchangeService::new(Arc::clone(&store)),
            store,
            tracker,
        }
    }

    pub fn execute(
        &self,
        worker_id: &WorkerId,
        request: WorkerToolRequest,
        now: DateTime<Utc>,
    ) -> Result<WorkerToolResponse, WorkerToolError> {
        let authorized_claim_id = ClaimId::parse(request.claim_id())?;
        self.active_owned_claim(worker_id, &authorized_claim_id)?;
        match request {
            WorkerToolRequest::RefreshContext { claim_id } => {
                let claim_id = ClaimId::parse(claim_id)?;
                let mut packet = self.assembler.assemble(worker_id, &claim_id, now)?;
                self.mark_context_notifications_delivered(&mut packet, now)?;
                Ok(WorkerToolResponse::Context {
                    packet: Box::new(packet),
                })
            }
            WorkerToolRequest::RefreshAtBoundary { claim_id, boundary } => {
                let claim_id = ClaimId::parse(claim_id)?;
                let mut packet = self.assembler.assemble(worker_id, &claim_id, now)?;
                self.mark_context_notifications_delivered(&mut packet, now)?;
                let revision = packet.revision().map_err(WorkerContextError::from)?;
                Ok(WorkerToolResponse::BoundaryContext {
                    boundary,
                    revision,
                    packet: Box::new(packet),
                })
            }
            WorkerToolRequest::CheckAssumptions {
                claim_id,
                boundary,
                expected_revision,
            } => self.check_assumptions(
                worker_id,
                &ClaimId::parse(claim_id)?,
                boundary,
                expected_revision,
                now,
            ),
            WorkerToolRequest::PrepareRollover {
                claim_id,
                reason,
                summary,
                completed_evidence,
                unresolved_assumptions,
                next_safe_action,
            } => self.prepare_rollover(
                worker_id,
                &ClaimId::parse(claim_id)?,
                reason,
                summary,
                completed_evidence,
                unresolved_assumptions,
                next_safe_action,
                now,
            ),
            WorkerToolRequest::ContinueFromTransfer {
                claim_id,
                artifact_id,
                run_id,
                thread_id,
            } => self.continue_from_transfer(
                worker_id,
                &ClaimId::parse(claim_id)?,
                &artifact_id,
                run_id,
                thread_id,
                now,
            ),
            WorkerToolRequest::RecoverContext {
                claim_id,
                recovery_kind,
            } => {
                let claim_id = ClaimId::parse(claim_id)?;
                self.active_owned_claim(worker_id, &claim_id)?;
                let (packet, replayed_notification_ids) =
                    self.assemble_recovery_context(worker_id, &claim_id, recovery_kind, now)?;
                let revision = packet.revision().map_err(WorkerContextError::from)?;
                Ok(WorkerToolResponse::RecoveryContext {
                    recovery_kind,
                    revision,
                    packet: Box::new(packet),
                    replayed_notification_ids,
                })
            }
            WorkerToolRequest::Heartbeat {
                claim_id,
                expected_generation,
            } => Ok(WorkerToolResponse::Claim {
                claim: self.claims.heartbeat(
                    &ClaimId::parse(claim_id)?,
                    worker_id,
                    expected_generation,
                    now,
                )?,
            }),
            WorkerToolRequest::ExpandClaim {
                claim_id,
                scope,
                rationale,
                base_revision,
                idempotency_key,
            } => {
                validate_text(&rationale, "claim expansion rationale")?;
                Ok(WorkerToolResponse::Claim {
                    claim: self.claims.expand_claim(
                        &ClaimId::parse(claim_id)?,
                        worker_id,
                        scope,
                        &rationale,
                        &base_revision,
                        &idempotency_key,
                        now,
                    )?,
                })
            }
            WorkerToolRequest::PublishEvent {
                claim_id,
                kind,
                severity,
                payload,
                idempotency_key,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                validate_payload(&payload)?;
                validate_text(&idempotency_key, "event idempotency key")?;
                let typed_payload: CoordinationEventPayload = serde_json::from_value(payload)
                    .map_err(|error| DomainError::InvalidEventPayload(error.to_string()))?;
                let authorized = match (&kind, &typed_payload) {
                    (
                        WorkerPublishableEventKind::Activity,
                        CoordinationEventPayload::Activity(activity),
                    ) => {
                        activity.category != ActivityCategory::Validation
                            && activity.worker_id == *worker_id
                            && activity.claim_id == claim.id
                    }
                    (
                        WorkerPublishableEventKind::Validation,
                        CoordinationEventPayload::Activity(activity),
                    ) => {
                        activity.category == ActivityCategory::Validation
                            && activity.worker_id == *worker_id
                            && activity.claim_id == claim.id
                    }
                    _ => false,
                };
                if !authorized {
                    return Err(WorkerToolError::InactiveOrUnownedClaim);
                }
                let event = CoordinationEvent::from_typed_payload(
                    &claim.goal_id,
                    severity,
                    CoordinationActor::Worker {
                        worker_id: worker_id.clone(),
                    },
                    claim.id.as_str(),
                    typed_payload,
                    now,
                )?;
                Ok(WorkerToolResponse::Event {
                    sequence: self.store.append_event_once(
                        &format!("worker.event:{}:{idempotency_key}", claim.id.as_str()),
                        &event,
                    )?,
                })
            }
            WorkerToolRequest::PublishActivity {
                claim_id,
                activity,
                idempotency_key,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                Ok(WorkerToolResponse::Event {
                    sequence: self.publish_worker_activity(
                        worker_id,
                        &claim,
                        activity,
                        &idempotency_key,
                        now,
                    )?,
                })
            }
            WorkerToolRequest::ReviseContract {
                claim_id,
                contract_id,
                expected_revision,
                compatibility_notes,
            } => self.revise_contract(
                worker_id,
                &ClaimId::parse(claim_id)?,
                &ContractId::parse(contract_id)?,
                expected_revision,
                compatibility_notes,
                now,
            ),
            WorkerToolRequest::RecordEvidence {
                claim_id,
                summary,
                evidence_refs,
                idempotency_key,
            } => {
                validate_text(&summary, "evidence summary")?;
                validate_evidence(&evidence_refs)?;
                validate_text(&idempotency_key, "evidence idempotency key")?;
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                let event = CoordinationEvent::from_typed_payload(
                    &claim.goal_id,
                    EventSeverity::Info,
                    CoordinationActor::Worker {
                        worker_id: worker_id.clone(),
                    },
                    claim.id.as_str(),
                    CoordinationEventPayload::Activity(ActivityEventPayload {
                        worker_id: worker_id.clone(),
                        claim_id: claim.id.clone(),
                        category: ActivityCategory::Artifact,
                        summary,
                        progress_percent: None,
                        changed_scope: vec![],
                        artifact_id: None,
                        evidence_refs,
                        validation_succeeded: None,
                    }),
                    now,
                )?;
                Ok(WorkerToolResponse::Event {
                    sequence: self.store.append_event_once(
                        &format!("worker.evidence:{}:{idempotency_key}", claim.id.as_str()),
                        &event,
                    )?,
                })
            }
            WorkerToolRequest::RequestCoordination {
                claim_id,
                target_scope,
                desired_outcome,
                evidence_refs,
                severity,
                idempotency_key,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                let event = CoordinationEvent::from_typed_payload(
                    &claim.goal_id,
                    severity,
                    CoordinationActor::Worker {
                        worker_id: worker_id.clone(),
                    },
                    claim.id.as_str(),
                    CoordinationEventPayload::CoordinationRequest(
                        CoordinationRequestEventPayload {
                            claim_id: claim.id.clone(),
                            requester_worker_id: worker_id.clone(),
                            target_scope,
                            desired_outcome,
                            evidence_refs,
                        },
                    ),
                    now,
                )?;
                let dispatch =
                    self.coordination_requests
                        .dispatch(&event, &idempotency_key, now)?;
                Ok(WorkerToolResponse::CoordinationRequested {
                    event_id: dispatch.event_id.as_str().into(),
                    sequence: dispatch.event_sequence,
                    target_worker_ids: dispatch
                        .deliveries
                        .iter()
                        .map(|delivery| delivery.worker_id.as_str().into())
                        .collect(),
                    notification_ids: dispatch
                        .deliveries
                        .iter()
                        .map(|delivery| delivery.notification_id.as_str().into())
                        .collect(),
                })
            }
            WorkerToolRequest::OpenCoordinationExchange {
                claim_id,
                request_event_id,
                target_worker_id,
                insufficiency_reason,
                ttl_seconds,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                Ok(WorkerToolResponse::CoordinationExchange {
                    exchange: self.exchanges.open(
                        &claim.goal_id,
                        &EventId::parse(request_event_id)?,
                        worker_id,
                        &WorkerId::parse(target_worker_id)?,
                        insufficiency_reason,
                        chrono::Duration::seconds(ttl_seconds as i64),
                        now,
                    )?,
                })
            }
            WorkerToolRequest::PostCoordinationExchange {
                claim_id,
                exchange_id,
                body,
                evidence_refs,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                Ok(WorkerToolResponse::CoordinationExchange {
                    exchange: self.exchanges.post_message(
                        &claim.goal_id,
                        &ExchangeId::parse(exchange_id)?,
                        worker_id,
                        body,
                        evidence_refs,
                        now,
                    )?,
                })
            }
            WorkerToolRequest::CompleteCoordinationExchange {
                claim_id,
                exchange_id,
                shared_note,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                Ok(WorkerToolResponse::CoordinationExchange {
                    exchange: self.exchanges.complete(
                        &claim.goal_id,
                        &ExchangeId::parse(exchange_id)?,
                        worker_id,
                        shared_note,
                        now,
                    )?,
                })
            }
            WorkerToolRequest::AcknowledgeNotification {
                claim_id,
                notification_id,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                let notification_id = NotificationId::parse(notification_id)?;
                let current = self.store.notification(&notification_id)?.ok_or_else(|| {
                    NotificationLifecycleError::NotFound(notification_id.as_str().into())
                })?;
                let transition_at =
                    next_notification_revision_time(now, current.metadata.updated_at);
                Ok(WorkerToolResponse::Notification {
                    notification: self.notifications.acknowledge(
                        &claim.goal_id,
                        &notification_id,
                        current.metadata.updated_at,
                        worker_id,
                        transition_at,
                    )?,
                })
            }
            WorkerToolRequest::ActOnNotification {
                claim_id,
                notification_id,
                outcome,
            } => {
                let claim = self.active_owned_claim(worker_id, &ClaimId::parse(claim_id)?)?;
                let notification_id = NotificationId::parse(notification_id)?;
                let current = self.store.notification(&notification_id)?.ok_or_else(|| {
                    NotificationLifecycleError::NotFound(notification_id.as_str().into())
                })?;
                let transition_at =
                    next_notification_revision_time(now, current.metadata.updated_at);
                Ok(WorkerToolResponse::Notification {
                    notification: self.notifications.mark_acted_on(
                        &claim.goal_id,
                        &notification_id,
                        current.metadata.updated_at,
                        worker_id,
                        outcome,
                        transition_at,
                    )?,
                })
            }
            WorkerToolRequest::Release {
                claim_id,
                expected_generation,
                reason,
                artifact_id,
                evidence_refs,
                idempotency_key,
            } => {
                let activity = WorkerActivity {
                    kind: WorkerActivityKind::Progress,
                    summary: reason.clone(),
                    progress_percent: None,
                    changed_scope: vec![],
                    artifact_id: artifact_id.clone(),
                    evidence_refs: evidence_refs.clone(),
                    validation_succeeded: None,
                };
                validate_activity(&activity)?;
                let claim = self.claims.release_claim(
                    &ClaimId::parse(claim_id)?,
                    worker_id,
                    expected_generation,
                    ReleaseEvidence {
                        reason: reason.clone(),
                        artifact_id: artifact_id.clone(),
                        evidence_refs: evidence_refs.clone(),
                    },
                    &idempotency_key,
                    now,
                )?;
                Ok(WorkerToolResponse::Claim { claim })
            }
            WorkerToolRequest::Block {
                claim_id,
                expected_generation,
                reason,
                evidence_refs,
                escalation_id,
                idempotency_key,
            } => {
                let activity = WorkerActivity {
                    kind: WorkerActivityKind::Blocker,
                    summary: reason.clone(),
                    progress_percent: None,
                    changed_scope: vec![],
                    artifact_id: None,
                    evidence_refs: evidence_refs.clone(),
                    validation_succeeded: None,
                };
                validate_activity(&activity)?;
                let claim = self.claims.block_claim(
                    &ClaimId::parse(claim_id)?,
                    worker_id,
                    expected_generation,
                    BlockEvidence {
                        reason: reason.clone(),
                        evidence_refs: evidence_refs.clone(),
                        escalation_id: escalation_id.map(EscalationId::parse).transpose()?,
                    },
                    &idempotency_key,
                    now,
                )?;
                Ok(WorkerToolResponse::Claim { claim })
            }
            WorkerToolRequest::Cancel {
                claim_id,
                expected_generation,
                reason,
                evidence_refs,
                idempotency_key,
            } => {
                validate_text(&reason, "cancellation reason")?;
                validate_evidence(&evidence_refs)?;
                let claim = self.claims.cancel_claim(
                    &ClaimId::parse(claim_id)?,
                    worker_id,
                    expected_generation,
                    CancelEvidence {
                        reason: reason.clone(),
                        evidence_refs: evidence_refs.clone(),
                    },
                    &idempotency_key,
                    now,
                )?;
                let mut worker = self
                    .store
                    .worker(worker_id)?
                    .ok_or_else(|| WorkerContextError::WorkerNotFound(worker_id.as_str().into()))?;
                if !worker.state.is_terminal() {
                    worker.transition(WorkerState::Cancelled, now, Some(reason.clone()))?;
                    self.store.upsert_worker(&worker)?;
                }
                Ok(WorkerToolResponse::Claim { claim })
            }
            WorkerToolRequest::Complete {
                claim_id,
                expected_generation,
                reason,
                artifact_id,
                integration_revision,
                evidence_refs,
                validation_event_sequence,
                audit_slice_id,
                context_revision,
                integration_boundary_satisfied,
                idempotency_key,
            } => {
                let activity = WorkerActivity {
                    kind: WorkerActivityKind::Completion,
                    summary: reason.clone(),
                    progress_percent: Some(100),
                    changed_scope: vec![],
                    artifact_id: Some(artifact_id.clone()),
                    evidence_refs: evidence_refs.clone(),
                    validation_succeeded: None,
                };
                validate_activity(&activity)?;
                let claim_id = ClaimId::parse(claim_id)?;
                self.verify_completion(
                    worker_id,
                    &claim_id,
                    validation_event_sequence,
                    &audit_slice_id,
                    &context_revision,
                    now,
                )?;
                let claim = self.claims.complete_claim(
                    &claim_id,
                    worker_id,
                    expected_generation,
                    CompletionEvidence {
                        reason: reason.clone(),
                        artifact_id: artifact_id.clone(),
                        integration_revision,
                        evidence_refs: evidence_refs.clone(),
                        integration_boundary_satisfied,
                    },
                    &idempotency_key,
                    now,
                )?;
                Ok(WorkerToolResponse::Claim { claim })
            }
        }
    }

    fn active_owned_claim(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
    ) -> Result<Claim, WorkerToolError> {
        let claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(claim_id.as_str().into()))?;
        let Some(worker) = self.store.worker(worker_id)? else {
            return Err(WorkerToolError::InactiveOrUnownedClaim);
        };
        if claim.owner != *worker_id
            || claim.state != ClaimState::Active
            || claim.goal_id != worker.goal_id
        {
            return Err(WorkerToolError::InactiveOrUnownedClaim);
        }
        Ok(claim)
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_rollover(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        reason: RolloverReason,
        summary: String,
        completed_evidence: Vec<String>,
        unresolved_assumptions: Vec<String>,
        next_safe_action: String,
        now: DateTime<Utc>,
    ) -> Result<WorkerToolResponse, WorkerToolError> {
        validate_text(&summary, "rollover summary")?;
        validate_text(&next_safe_action, "next safe action")?;
        validate_string_list(&completed_evidence, "completed evidence", 32, 500)?;
        validate_string_list(&unresolved_assumptions, "unresolved assumptions", 32, 500)?;
        let claim = self.active_owned_claim(worker_id, claim_id)?;
        let mut worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| WorkerContextError::WorkerNotFound(worker_id.as_str().into()))?;
        let from_run_id = worker
            .current_run_id
            .clone()
            .ok_or(WorkerToolError::NoActiveTurn)?;
        let from_thread_id = worker
            .current_thread_id
            .clone()
            .ok_or(WorkerToolError::NoActiveTurn)?;
        let packet = self.assembler.assemble(worker_id, claim_id, now)?;
        let artifact = ContextTransferArtifact {
            id: format!("transfer-{}", uuid::Uuid::new_v4()),
            goal_id: claim.goal_id.clone(),
            worker_id: worker_id.as_str().into(),
            claim_id: claim.id.as_str().into(),
            reason,
            from_run_id,
            from_thread_id,
            summary,
            completed_evidence,
            unresolved_assumptions,
            next_safe_action,
            repository: packet.repository.clone(),
            context_revision: packet.revision().map_err(WorkerContextError::from)?,
            created_at: now,
        };
        self.store.put_context_transfer(
            &artifact.id,
            &artifact.goal_id,
            worker_id,
            claim_id,
            now,
            &artifact,
        )?;
        let turn = worker
            .turn_history
            .last_mut()
            .filter(|turn| turn.run_id == artifact.from_run_id)
            .ok_or(WorkerToolError::NoActiveTurn)?;
        turn.completed_at = Some(now);
        worker.current_run_id = None;
        worker.last_heartbeat_at = Some(now);
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        let event = CoordinationEvent::new(
            &claim.goal_id,
            CoordinationEventKind::WorkerLifecycleChanged,
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim.id.as_str(),
            serde_json::json!({
                "category": "context_transfer_prepared",
                "artifactId": artifact.id.clone(),
                "reason": artifact.reason,
                "fromRunId": artifact.from_run_id.clone(),
                "fromThreadId": artifact.from_thread_id.clone(),
            }),
            now,
        );
        self.store.append_event(&event)?;
        Ok(WorkerToolResponse::TransferPrepared {
            artifact: Box::new(artifact),
            worker,
        })
    }

    fn continue_from_transfer(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        artifact_id: &str,
        run_id: String,
        thread_id: String,
        now: DateTime<Utc>,
    ) -> Result<WorkerToolResponse, WorkerToolError> {
        validate_text(artifact_id, "context transfer artifact id")?;
        validate_text(&run_id, "continuation run id")?;
        validate_text(&thread_id, "continuation thread id")?;
        let claim = self.active_owned_claim(worker_id, claim_id)?;
        let artifact: ContextTransferArtifact = self
            .store
            .context_transfer(artifact_id)?
            .ok_or_else(|| WorkerToolError::ContextTransferNotFound(artifact_id.into()))?;
        if artifact.worker_id != worker_id.as_str()
            || artifact.claim_id != claim.id.as_str()
            || artifact.goal_id != claim.goal_id
        {
            return Err(WorkerToolError::ContextTransferMismatch);
        }
        let mut worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| WorkerContextError::WorkerNotFound(worker_id.as_str().into()))?;
        if worker.current_run_id.is_some() {
            return Err(WorkerToolError::ActiveRunExists);
        }
        if worker
            .turn_history
            .iter()
            .any(|turn| turn.context_transfer_artifact_id.as_deref() == Some(artifact_id))
        {
            return Err(WorkerToolError::ContextTransferAlreadyUsed);
        }
        if worker.current_thread_id.as_deref() != Some(artifact.from_thread_id.as_str())
            || thread_id == artifact.from_thread_id
            || run_id == artifact.from_run_id
        {
            return Err(WorkerToolError::InvalidContinuation);
        }
        worker.turn_history.push(WorkerTurnBinding {
            sequence: worker.turn_history.len() as u64 + 1,
            run_id: run_id.clone(),
            thread_id: thread_id.clone(),
            started_at: now,
            completed_at: None,
            continuation_of_thread_id: Some(artifact.from_thread_id.clone()),
            context_transfer_artifact_id: Some(artifact.id.clone()),
        });
        worker.current_run_id = Some(run_id.clone());
        worker.current_thread_id = Some(thread_id.clone());
        worker.last_heartbeat_at = Some(now);
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        let event = CoordinationEvent::new(
            &claim.goal_id,
            CoordinationEventKind::WorkerLifecycleChanged,
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim.id.as_str(),
            serde_json::json!({
                "category": "context_transfer_continued",
                "artifactId": artifact.id.clone(),
                "runId": run_id,
                "threadId": thread_id,
                "workerId": worker.id.as_str(),
                "claimId": claim.id.as_str(),
            }),
            now,
        );
        self.store.append_event(&event)?;
        let (recovery_context, replayed_notification_ids) = self.assemble_recovery_context(
            worker_id,
            claim_id,
            WorkerRecoveryKind::ContextRollover,
            now,
        )?;
        Ok(WorkerToolResponse::ContinuationStarted {
            artifact: Box::new(artifact),
            worker,
            recovery_context: Box::new(recovery_context),
            replayed_notification_ids,
        })
    }

    fn check_assumptions(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        boundary: WorkerBoundary,
        expected: WorkerContextRevision,
        now: DateTime<Utc>,
    ) -> Result<WorkerToolResponse, WorkerToolError> {
        let claim = self.active_owned_claim(worker_id, claim_id)?;
        let current_packet = self.assembler.assemble(worker_id, claim_id, now)?;
        let current = current_packet
            .revision()
            .map_err(WorkerContextError::from)?;
        let mut critical = Vec::new();
        let mut refresh = Vec::new();
        if expected.claim_id != current.claim_id
            || expected.lease_generation != current.lease_generation
        {
            critical.push("claim ownership or lease generation changed".to_string());
        }
        if expected.repository_id != current.repository_id
            || expected.base_revision != current.base_revision
        {
            critical.push("repository binding or base revision changed".to_string());
        }
        if expected.contract_revisions != current.contract_revisions {
            critical.push("shared contract revisions changed".to_string());
        }
        for mismatch in self.contracts.revision_mismatches_for_claim(claim_id)? {
            critical.push(format!(
                "contract {} expected revision {} but current revision is {}",
                mismatch.stable_key, mismatch.expected_revision, mismatch.current_revision
            ));
        }
        if expected.dependency_fingerprint != current.dependency_fingerprint {
            critical.push("work-package dependency state changed".to_string());
        }
        if expected.scope_fingerprint != current.scope_fingerprint {
            refresh.push("claimed tracker scope changed".to_string());
        }
        if expected.latest_event_sequence != current.latest_event_sequence {
            refresh.push("new relevant coordination events are available".to_string());
        }
        if expected.heartbeat_at != current.heartbeat_at {
            refresh.push("claim heartbeat changed".to_string());
        }
        if expected.fingerprint != current.fingerprint && critical.is_empty() && refresh.is_empty()
        {
            refresh.push("context revision changed".to_string());
        }
        let disposition = if !critical.is_empty() {
            AssumptionDisposition::Paused
        } else if !refresh.is_empty() {
            AssumptionDisposition::RefreshRequired
        } else {
            AssumptionDisposition::Current
        };
        let mut changes = critical;
        changes.extend(refresh);

        if disposition == AssumptionDisposition::Paused {
            let mut worker = self
                .store
                .worker(worker_id)?
                .ok_or_else(|| WorkerContextError::WorkerNotFound(worker_id.as_str().into()))?;
            if matches!(
                worker.state,
                WorkerState::Active | WorkerState::Waiting | WorkerState::Blocked
            ) {
                worker.transition(
                    WorkerState::Paused,
                    now,
                    Some("worker assumptions became stale".into()),
                )?;
                self.store.upsert_worker(&worker)?;
            }
        }
        if disposition != AssumptionDisposition::Current {
            let event = CoordinationEvent::new(
                &claim.goal_id,
                if disposition == AssumptionDisposition::Paused {
                    CoordinationEventKind::DependencyChanged
                } else {
                    CoordinationEventKind::ActivityPublished
                },
                if disposition == AssumptionDisposition::Paused {
                    EventSeverity::Warning
                } else {
                    EventSeverity::Info
                },
                CoordinationActor::Worker {
                    worker_id: worker_id.clone(),
                },
                claim.id.as_str(),
                serde_json::json!({
                    "category": "stale_assumptions",
                    "boundary": boundary,
                    "disposition": disposition,
                    "changes": changes.clone(),
                    "expectedFingerprint": expected.fingerprint.clone(),
                    "observedFingerprint": current.fingerprint.clone(),
                }),
                now,
            );
            self.store.append_event(&event)?;
        }
        let mut packet = self.assembler.assemble(worker_id, claim_id, now)?;
        self.mark_context_notifications_delivered(&mut packet, now)?;
        let revision = packet.revision().map_err(WorkerContextError::from)?;
        Ok(WorkerToolResponse::AssumptionStatus {
            boundary,
            disposition,
            changes,
            revision,
            packet: Box::new(packet),
        })
    }

    fn mark_context_notifications_delivered(
        &self,
        packet: &mut WorkerContextPacket,
        now: DateTime<Utc>,
    ) -> Result<(), WorkerToolError> {
        for context in &mut packet.prior_state.notifications {
            let notification_id = NotificationId::parse(&context.id)?;
            let Some(notification) = self.store.notification(&notification_id)? else {
                return Err(NotificationLifecycleError::NotFound(context.id.clone()).into());
            };
            if notification.state != NotificationState::Queued {
                continue;
            }
            let delivered = self.notifications.mark_delivered(
                &packet.goal.id,
                &notification_id,
                notification.metadata.updated_at,
                CoordinationActor::System,
                next_notification_revision_time(now, notification.metadata.updated_at),
            )?;
            context.state = delivered.state;
        }
        Ok(())
    }

    fn assemble_recovery_context(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        recovery_kind: WorkerRecoveryKind,
        now: DateTime<Utc>,
    ) -> Result<(WorkerContextPacket, Vec<String>), WorkerToolError> {
        for notification in self.store.notifications_for_worker(worker_id, None)? {
            if matches!(
                notification.state,
                NotificationState::Queued
                    | NotificationState::Delivered
                    | NotificationState::Acknowledged
            ) && notification
                .expires_at
                .is_some_and(|expires_at| expires_at <= now)
            {
                self.notifications.mark_expired(
                    &notification.goal_id,
                    &notification.id,
                    notification.metadata.updated_at,
                    CoordinationActor::System,
                    next_notification_revision_time(now, notification.metadata.updated_at),
                )?;
            }
        }
        let mut packet = self.assembler.assemble_recovery(worker_id, claim_id, now)?;
        let mut replayed_notification_ids = Vec::new();
        for context in &mut packet.prior_state.notifications {
            let notification_id = NotificationId::parse(&context.id)?;
            let notification = self
                .store
                .notification(&notification_id)?
                .ok_or_else(|| NotificationLifecycleError::NotFound(context.id.clone()))?;
            let replayed = match notification.state {
                NotificationState::Queued => self.notifications.mark_delivered(
                    &packet.goal.id,
                    &notification_id,
                    notification.metadata.updated_at,
                    CoordinationActor::System,
                    next_notification_revision_time(now, notification.metadata.updated_at),
                )?,
                NotificationState::Delivered | NotificationState::Acknowledged => {
                    self.notifications.record_replayed(
                        &packet.goal.id,
                        &notification_id,
                        notification.metadata.updated_at,
                        CoordinationActor::System,
                        recovery_kind.label(),
                        next_notification_revision_time(now, notification.metadata.updated_at),
                    )?
                }
                _ => continue,
            };
            context.state = replayed.state;
            replayed_notification_ids.push(context.id.clone());
        }
        Ok((packet, replayed_notification_ids))
    }

    fn verify_completion(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        validation_event_sequence: u64,
        audit_slice_id: &str,
        expected_revision: &WorkerContextRevision,
        now: DateTime<Utc>,
    ) -> Result<(), WorkerToolError> {
        validate_text(audit_slice_id, "audit slice id")?;
        let claim = self.active_owned_claim(worker_id, claim_id)?;
        let validation = self
            .store
            .events_for_goal(
                &claim.goal_id,
                validation_event_sequence.saturating_sub(1),
                Some(claim.id.as_str()),
                1,
            )?
            .into_iter()
            .next();
        let validation_matches = validation.as_ref().is_some_and(|event| {
            event.sequence == Some(validation_event_sequence)
                && event.kind == CoordinationEventKind::ValidationCompleted
                && matches!(
                    &event.producer,
                    CoordinationActor::Worker { worker_id: producer } if producer == worker_id
                )
        });
        if !validation_matches {
            return Err(WorkerToolError::CompletionGateFailed(
                "validation event is missing, stale, or belongs to another worker".into(),
            ));
        }

        let goal = self
            .tracker
            .get_goal(&claim.goal_id)
            .map_err(WorkerContextError::from)?;
        let feature_ids = completed_scope_feature_ids(&goal, &claim.scope)?;
        let tracker_complete = feature_ids.iter().all(|feature_id| {
            goal["features"].as_array().is_some_and(|features| {
                features.iter().any(|feature| {
                    feature["id"] == *feature_id
                        && feature["status"] == "Done"
                        && feature["steps"]
                            .as_array()
                            .is_some_and(|steps| steps.iter().all(|step| step["done"] == true))
                })
            })
        });
        if !tracker_complete {
            return Err(WorkerToolError::CompletionGateFailed(
                "claimed tracker scope is not Done with all steps complete".into(),
            ));
        }
        let slice_matches = goal["slices"].as_array().is_some_and(|slices| {
            slices.iter().any(|slice| {
                slice["id"] == audit_slice_id
                    && slice["status"] == "Done"
                    && slice["evidence"]
                        .as_array()
                        .is_some_and(|evidence| !evidence.is_empty())
                    && slice["feature_id"]
                        .as_str()
                        .is_some_and(|feature_id| feature_ids.iter().any(|id| id == feature_id))
            })
        });
        if !slice_matches {
            return Err(WorkerToolError::CompletionGateFailed(
                "matching Done audit slice with evidence was not found".into(),
            ));
        }

        let current_revision = self
            .assembler
            .assemble(worker_id, claim_id, now)?
            .revision()
            .map_err(WorkerContextError::from)?;
        if &current_revision != expected_revision {
            return Err(WorkerToolError::CompletionGateFailed(
                "tracker, claim, contract, event, or repository assumptions changed after the integration refresh"
                    .into(),
            ));
        }
        Ok(())
    }

    fn publish_worker_activity(
        &self,
        worker_id: &WorkerId,
        claim: &Claim,
        activity: WorkerActivity,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<u64, WorkerToolError> {
        validate_activity(&activity)?;
        validate_text(idempotency_key, "activity idempotency key")?;
        let severity = if activity.kind == WorkerActivityKind::Blocker
            || (activity.kind == WorkerActivityKind::Validation
                && activity.validation_succeeded == Some(false))
        {
            EventSeverity::Warning
        } else {
            EventSeverity::Info
        };
        let payload = match activity.kind {
            WorkerActivityKind::Progress
            | WorkerActivityKind::ScopeChanged
            | WorkerActivityKind::Validation
            | WorkerActivityKind::Artifact => {
                let category = match activity.kind {
                    WorkerActivityKind::Progress => ActivityCategory::Progress,
                    WorkerActivityKind::ScopeChanged => ActivityCategory::ScopeChanged,
                    WorkerActivityKind::Validation => ActivityCategory::Validation,
                    WorkerActivityKind::Artifact => ActivityCategory::Artifact,
                    _ => unreachable!(),
                };
                CoordinationEventPayload::Activity(ActivityEventPayload {
                    worker_id: worker_id.clone(),
                    claim_id: claim.id.clone(),
                    category,
                    summary: activity.summary,
                    progress_percent: activity.progress_percent,
                    changed_scope: activity.changed_scope,
                    artifact_id: activity.artifact_id,
                    evidence_refs: activity.evidence_refs,
                    validation_succeeded: activity.validation_succeeded,
                })
            }
            WorkerActivityKind::Blocker => CoordinationEventPayload::Blocker(BlockerEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker_id.clone(),
                summary: activity.summary,
                evidence_refs: activity.evidence_refs,
                escalation_id: None,
            }),
            WorkerActivityKind::Completion => {
                return Err(WorkerToolError::InvalidActivity(
                    "completion activity must use the complete operation",
                ));
            }
        };
        let event = CoordinationEvent::from_typed_payload(
            &claim.goal_id,
            severity,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim.id.as_str(),
            payload,
            now,
        )?;
        Ok(self.store.append_event_once(
            &format!("worker.activity:{}:{idempotency_key}", claim.id.as_str()),
            &event,
        )?)
    }

    fn revise_contract(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        contract_id: &ContractId,
        expected_revision: u64,
        compatibility_notes: String,
        now: DateTime<Utc>,
    ) -> Result<WorkerToolResponse, WorkerToolError> {
        validate_text(&compatibility_notes, "contract compatibility notes")?;
        let claim = self.active_owned_claim(worker_id, claim_id)?;
        let mut contract = self
            .store
            .contract(contract_id)?
            .ok_or_else(|| WorkerToolError::ContractNotFound(contract_id.as_str().into()))?;
        if contract.goal_id != claim.goal_id {
            return Err(WorkerToolError::ContractGoalMismatch);
        }
        let ClaimScope::WorkPackage { work_package_id } = &claim.scope else {
            return Err(WorkerToolError::ContractChangeNotAuthorized(
                contract.id.as_str().into(),
            ));
        };
        let package = self.store.work_package(work_package_id)?.ok_or_else(|| {
            WorkerToolError::ContractChangeNotAuthorized(contract.id.as_str().into())
        })?;
        if !package
            .produces_contracts
            .iter()
            .any(|produced| produced == contract.id.as_str() || produced == &contract.stable_key)
        {
            return Err(WorkerToolError::ContractChangeNotAuthorized(
                contract.id.as_str().into(),
            ));
        }
        if contract.revision != expected_revision {
            return Err(WorkerToolError::ContractRevisionMismatch {
                expected: expected_revision,
                current: contract.revision,
            });
        }
        let previous_revision = contract.revision;
        contract.revise(worker_id.clone(), compatibility_notes.clone(), now);
        let event = CoordinationEvent::from_typed_payload(
            &claim.goal_id,
            EventSeverity::Warning,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim.id.as_str(),
            CoordinationEventPayload::Contract(ContractEventPayload {
                contract_id: contract.id.clone(),
                stable_key: contract.stable_key.clone(),
                previous_revision,
                revision: contract.revision,
                producer_worker_id: Some(worker_id.clone()),
                compatibility_notes,
            }),
            now,
        )?;
        if self
            .store
            .revise_contract_with_event(
                &contract,
                expected_revision,
                &event,
                &format!(
                    "contract.revise:{}:{}",
                    contract.id.as_str(),
                    contract.revision
                ),
            )?
            .is_none()
        {
            let current = self
                .store
                .contract(contract_id)?
                .map(|current| current.revision)
                .unwrap_or_default();
            return Err(WorkerToolError::ContractRevisionMismatch {
                expected: expected_revision,
                current,
            });
        }
        Ok(WorkerToolResponse::Contract { contract })
    }
}

fn tracker_scope<'a>(
    goal: &'a GoalRecord,
    claim: &Claim,
) -> Result<(Option<&'a TrackerWorkPackage>, Vec<String>), WorkerContextError> {
    match &claim.scope {
        ClaimScope::Feature { feature_id } => {
            if goal
                .features
                .iter()
                .any(|feature| feature.id == *feature_id)
            {
                Ok((None, vec![feature_id.clone()]))
            } else {
                Err(WorkerContextError::MissingTrackerScope(feature_id.clone()))
            }
        }
        ClaimScope::WorkPackage { work_package_id } => {
            let package = goal
                .work_packages
                .iter()
                .find(|package| package.id == work_package_id.as_str())
                .ok_or_else(|| {
                    WorkerContextError::MissingTrackerScope(work_package_id.as_str().into())
                })?;
            Ok((Some(package), package.feature_ids.clone()))
        }
    }
}

fn completed_scope_feature_ids(
    goal: &serde_json::Value,
    scope: &ClaimScope,
) -> Result<Vec<String>, WorkerToolError> {
    match scope {
        ClaimScope::Feature { feature_id } => Ok(vec![feature_id.clone()]),
        ClaimScope::WorkPackage { work_package_id } => {
            let package = goal["work_packages"]
                .as_array()
                .and_then(|packages| {
                    packages
                        .iter()
                        .find(|package| package["id"] == work_package_id.as_str())
                })
                .ok_or_else(|| {
                    WorkerToolError::CompletionGateFailed(
                        "claimed work package is missing from the tracker".into(),
                    )
                })?;
            if package["status"] != "Done" {
                return Err(WorkerToolError::CompletionGateFailed(
                    "claimed work package is not Done".into(),
                ));
            }
            let feature_ids = package["feature_ids"]
                .as_array()
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_str().map(str::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if feature_ids.is_empty() {
                return Err(WorkerToolError::CompletionGateFailed(
                    "claimed work package has no tracker features".into(),
                ));
            }
            Ok(feature_ids)
        }
    }
}

fn feature_contexts(
    goal: &GoalRecord,
    feature_ids: &[String],
    limits: &WorkerContextLimits,
) -> Vec<FeatureContext> {
    feature_ids
        .iter()
        .take(limits.max_features)
        .filter_map(|id| goal.features.iter().find(|feature| feature.id == *id))
        .map(|feature| feature_context(feature, limits))
        .collect()
}

fn feature_context(feature: &Feature, limits: &WorkerContextLimits) -> FeatureContext {
    FeatureContext {
        id: feature.id.clone(),
        title: bounded(&feature.title, limits.max_text_chars),
        description: bounded(&feature.description, limits.max_feature_description_chars),
        status: feature.status,
        steps: feature
            .steps
            .iter()
            .take(limits.max_steps_per_feature)
            .map(|step| StepContext {
                id: step.id.clone(),
                title: bounded(&step.title, limits.max_text_chars),
                done: step.done,
                next: step.next,
            })
            .collect(),
    }
}

fn dependency_contexts(
    goal: &GoalRecord,
    package: Option<&TrackerWorkPackage>,
    limits: &WorkerContextLimits,
) -> (Vec<DependencyContext>, usize) {
    let Some(package) = package else {
        return (Vec::new(), 0);
    };
    let dependencies = package
        .depends_on
        .iter()
        .filter_map(|id| {
            goal.work_packages
                .iter()
                .find(|candidate| candidate.id == *id)
        })
        .collect::<Vec<_>>();
    let omitted = dependencies.len().saturating_sub(limits.max_dependencies);
    (
        dependencies
            .into_iter()
            .take(limits.max_dependencies)
            .map(|dependency| DependencyContext {
                id: dependency.id.clone(),
                title: bounded(&dependency.title, limits.max_text_chars),
                status: dependency.status,
            })
            .collect(),
        omitted,
    )
}

fn contract_contexts(
    mut contracts: Vec<SharedContract>,
    package: Option<&super::domain::WorkPackage>,
    claim: &Claim,
    limits: &WorkerContextLimits,
) -> (Vec<ContractContext>, usize) {
    contracts.sort_by(|left, right| left.stable_key.cmp(&right.stable_key));
    let mut expectations: HashMap<&str, u64> = package
        .map(|package| {
            package
                .consumes_contracts
                .iter()
                .map(|expectation| {
                    (
                        expectation.contract_id.as_str(),
                        expectation.expected_revision,
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    for expectation in &claim.consumes_contracts {
        expectations.insert(
            expectation.contract_id.as_str(),
            expectation.expected_revision,
        );
    }
    let mut relevant: HashSet<&str> = package
        .map(|package| {
            package
                .produces_contracts
                .iter()
                .map(String::as_str)
                .chain(expectations.keys().copied())
                .collect()
        })
        .unwrap_or_default();
    relevant.extend(claim.produces_contracts.iter().map(String::as_str));
    relevant.extend(expectations.keys().copied());
    if !relevant.is_empty() {
        contracts.retain(|contract| {
            relevant.contains(contract.id.as_str())
                || relevant.contains(contract.stable_key.as_str())
        });
    }
    let omitted = contracts.len().saturating_sub(limits.max_contracts);
    (
        contracts
            .into_iter()
            .take(limits.max_contracts)
            .map(|contract| ContractContext {
                expected_revision: expectations.get(contract.id.as_str()).copied(),
                id: contract.id.as_str().into(),
                stable_key: bounded(&contract.stable_key, limits.max_text_chars),
                title: bounded(&contract.title, limits.max_text_chars),
                revision: contract.revision,
                compatibility_notes: bounded(
                    &contract.compatibility_notes,
                    limits.max_feature_description_chars,
                ),
            })
            .collect(),
        omitted,
    )
}

fn turn_contexts(
    turns: &[WorkerTurnBinding],
    limits: &WorkerContextLimits,
) -> (Vec<TurnContext>, usize) {
    let omitted = turns.len().saturating_sub(limits.max_prior_turns);
    (
        turns
            .iter()
            .skip(omitted)
            .map(|turn| TurnContext {
                sequence: turn.sequence,
                run_id: turn.run_id.clone(),
                thread_id: turn.thread_id.clone(),
                completed_at: turn.completed_at,
                context_transfer_artifact_id: turn.context_transfer_artifact_id.clone(),
            })
            .collect(),
        omitted,
    )
}

fn event_contexts(
    events: Vec<CoordinationEvent>,
    limits: &WorkerContextLimits,
) -> Vec<EventContext> {
    events
        .into_iter()
        .map(|event| EventContext {
            sequence: event.sequence,
            kind: enum_string(&event.kind),
            severity: enum_string(&event.severity),
            occurred_at: event.occurred_at,
            payload: bounded(
                &serde_json::to_string(&event.payload).unwrap_or_else(|_| "null".into()),
                limits.max_event_payload_chars,
            ),
        })
        .collect()
}

fn notification_contexts(
    mut notifications: Vec<WorkerNotification>,
    now: DateTime<Utc>,
    limits: &WorkerContextLimits,
) -> (Vec<NotificationContext>, usize) {
    notifications.retain(|notification| {
        notification
            .expires_at
            .is_none_or(|expires_at| expires_at > now)
    });
    notifications.sort_by(|left, right| {
        right
            .required_acknowledgement
            .cmp(&left.required_acknowledgement)
            .then_with(|| right.severity.rank().cmp(&left.severity.rank()))
            .then_with(|| left.metadata.updated_at.cmp(&right.metadata.updated_at))
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
    let omitted = notifications.len().saturating_sub(limits.max_notifications);
    let contexts = notifications
        .into_iter()
        .take(limits.max_notifications)
        .map(|notification| NotificationContext {
            id: notification.id.as_str().into(),
            source_event_id: notification.source_event_id.as_str().into(),
            state: notification.state,
            purpose: bounded(&notification.purpose, limits.max_text_chars),
            summary: bounded(&notification.summary, limits.max_feature_description_chars),
            severity: enum_string(&notification.severity),
            evidence_refs: notification
                .evidence_refs
                .into_iter()
                .take(16)
                .map(|reference| bounded(&reference, limits.max_text_chars))
                .collect(),
            recommended_action: notification
                .recommended_action
                .map(|action| bounded(&action, limits.max_feature_description_chars)),
            required_acknowledgement: notification.required_acknowledgement,
            expires_at: notification.expires_at,
        })
        .collect();
    (contexts, omitted)
}

fn next_notification_revision_time(
    requested: DateTime<Utc>,
    current: DateTime<Utc>,
) -> DateTime<Utc> {
    if requested <= current {
        current + chrono::Duration::microseconds(1)
    } else {
        requested
    }
}

fn claim_context(claim: &Claim) -> ClaimContext {
    ClaimContext {
        id: claim.id.as_str().into(),
        scope: claim.scope.clone(),
        base_revision: claim.base_revision.clone(),
        lease_generation: claim.lease_generation,
        heartbeat_at: claim.heartbeat_at,
        lease_expires_at: claim.lease_expires_at,
        expansion_count: claim.expansion_history.len(),
        overlap_count: claim.overlaps.len(),
    }
}

fn validate_text(value: &str, label: &'static str) -> Result<(), WorkerToolError> {
    if value.trim().is_empty() || value.chars().count() > 2_000 {
        Err(WorkerToolError::PayloadTooLarge(label))
    } else {
        Ok(())
    }
}

fn validate_payload(payload: &serde_json::Value) -> Result<(), WorkerToolError> {
    if serde_json::to_vec(payload)
        .map(|value| value.len() <= 8 * 1024)
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(WorkerToolError::PayloadTooLarge("event payload"))
    }
}

fn validate_evidence(evidence_refs: &[String]) -> Result<(), WorkerToolError> {
    if evidence_refs.is_empty() || evidence_refs.iter().any(|value| value.trim().is_empty()) {
        return Err(WorkerToolError::EmptyEvidence);
    }
    if evidence_refs.len() > 32
        || evidence_refs
            .iter()
            .any(|value| value.chars().count() > 500)
    {
        return Err(WorkerToolError::PayloadTooLarge("evidence references"));
    }
    Ok(())
}

fn validate_string_list(
    values: &[String],
    label: &'static str,
    max_items: usize,
    max_chars: usize,
) -> Result<(), WorkerToolError> {
    if values.len() > max_items
        || values
            .iter()
            .any(|value| value.trim().is_empty() || value.chars().count() > max_chars)
    {
        Err(WorkerToolError::PayloadTooLarge(label))
    } else {
        Ok(())
    }
}

fn validate_activity(activity: &WorkerActivity) -> Result<(), WorkerToolError> {
    validate_text(&activity.summary, "activity summary")?;
    if activity.progress_percent.is_some_and(|value| value > 100) {
        return Err(WorkerToolError::InvalidActivity(
            "progress_percent must be between 0 and 100",
        ));
    }
    if activity.kind != WorkerActivityKind::Validation && activity.validation_succeeded.is_some() {
        return Err(WorkerToolError::InvalidActivity(
            "validation_succeeded is only valid for validation activity",
        ));
    }
    if activity.changed_scope.len() > 32
        || activity
            .changed_scope
            .iter()
            .any(|value| value.trim().is_empty() || value.chars().count() > 300)
    {
        return Err(WorkerToolError::PayloadTooLarge("changed scope"));
    }
    if activity
        .artifact_id
        .as_ref()
        .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 300)
    {
        return Err(WorkerToolError::PayloadTooLarge("artifact id"));
    }
    if !activity.evidence_refs.is_empty() {
        validate_evidence(&activity.evidence_refs)?;
    }
    match activity.kind {
        WorkerActivityKind::ScopeChanged if activity.changed_scope.is_empty() => {
            return Err(WorkerToolError::InvalidActivity(
                "scope changes require changed_scope",
            ));
        }
        WorkerActivityKind::Validation | WorkerActivityKind::Blocker
            if activity.evidence_refs.is_empty() =>
        {
            return Err(WorkerToolError::InvalidActivity(
                "validation and blocker activity require evidence",
            ));
        }
        WorkerActivityKind::Artifact | WorkerActivityKind::Completion
            if activity.artifact_id.is_none() || activity.evidence_refs.is_empty() =>
        {
            return Err(WorkerToolError::InvalidActivity(
                "artifact and completion activity require an artifact and evidence",
            ));
        }
        _ => {}
    }
    validate_payload(
        &serde_json::to_value(activity)
            .map_err(|_| WorkerToolError::PayloadTooLarge("activity serialization"))?,
    )
}

fn bounded(value: &str, maximum: usize) -> String {
    let mut chars = value.chars();
    let mut result = chars.by_ref().take(maximum).collect::<String>();
    if chars.next().is_some() {
        result.push('…');
    }
    result
}

fn check_identity(
    label: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), WorkerContextError> {
    if value.chars().count() > maximum {
        Err(WorkerContextError::IdentityTooLong { label, maximum })
    } else {
        Ok(())
    }
}

fn enum_string(value: &impl Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

fn hash_json(value: &serde_json::Value) -> Result<String, serde_json::Error> {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value)?);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        Claim, CoordinationActor, CoordinationEventKind, EventId, EventSeverity, Worker,
        WorkerState,
    };
    use crate::coordination::store::{
        ClaimRepository, EventRepository, ExchangeRepository, NotificationRepository,
        WorkerRepository,
    };
    use crate::models::Status;
    use chrono::Duration;

    fn fixture() -> (
        tempfile::TempDir,
        Arc<SqliteCoordinationStore>,
        Tracker,
        Worker,
        Claim,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker
            .create_goal("goal-a", "Goal A", &"goal context ".repeat(500))
            .unwrap();
        tracker
            .add_feature(
                "goal-a",
                "feature-a",
                "Feature A",
                &"feature context ".repeat(300),
                Status::Planned,
            )
            .unwrap();
        for index in 0..30 {
            tracker
                .add_step(
                    "goal-a",
                    "feature-a",
                    &format!("step-{index}"),
                    &format!("Step {index}"),
                    false,
                    index == 0,
                )
                .unwrap();
        }
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.permission_profile = Some(WorkerPermissionProfile::default());
        worker.workspace = Some(WorkspaceBinding {
            repository_id: "repo-a".into(),
            canonical_repository_path: directory.path().to_string_lossy().into(),
            worktree_path: directory.path().join("worktree").to_string_lossy().into(),
            branch: "codex/golazo/goal-a/worker-a".into(),
            base_revision: "base-a".into(),
            created_at: Some(now),
            creation_evidence: vec!["fixture".into()],
        });
        for index in 0..12 {
            worker.turn_history.push(WorkerTurnBinding {
                sequence: index + 1,
                run_id: format!("run-{index}"),
                thread_id: format!("thread-{index}"),
                started_at: now,
                completed_at: Some(now),
                continuation_of_thread_id: None,
                context_transfer_artifact_id: None,
            });
        }
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "feature-a".into(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        (directory, store, tracker, worker, claim)
    }

    #[test]
    fn assembles_a_bounded_packet_with_recent_durable_state() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let now = Utc::now();
        for index in 0..25 {
            let event = CoordinationEvent::new(
                "goal-a",
                CoordinationEventKind::ActivityPublished,
                EventSeverity::Info,
                CoordinationActor::Worker {
                    worker_id: worker.id.clone(),
                },
                claim.id.as_str(),
                serde_json::json!({"index": index, "detail": "x".repeat(2_000)}),
                now,
            );
            store.append_event(&event).unwrap();
        }
        let source_event = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::DependencyChanged,
            EventSeverity::Warning,
            CoordinationActor::System,
            claim.id.as_str(),
            serde_json::json!({"summary": "dependency changed"}),
            now,
        );
        store.append_event(&source_event).unwrap();
        let notification_created_at = now - Duration::minutes(2);
        for index in 0..10 {
            let notification = WorkerNotification::new(
                "goal-a",
                worker.id.clone(),
                source_event.id.clone(),
                "dependency_changed",
                format!("Dependency notification {index}"),
                if index == 0 {
                    EventSeverity::Critical
                } else {
                    EventSeverity::Info
                },
                vec![format!("event:{}", source_event.id.as_str())],
                Some("Refresh dependency state before editing.".into()),
                index == 0,
                (index == 9).then_some(notification_created_at + Duration::seconds(30)),
                notification_created_at,
            )
            .unwrap();
            store.upsert_notification(&notification).unwrap();
        }
        let other_notification = WorkerNotification::new(
            "goal-a",
            WorkerId::new(),
            source_event.id.clone(),
            "dependency_changed",
            "Notification for another worker",
            EventSeverity::Critical,
            vec![format!("event:{}", source_event.id.as_str())],
            None,
            true,
            None,
            notification_created_at,
        )
        .unwrap();
        store.upsert_notification(&other_notification).unwrap();
        let foreign_goal_notification = WorkerNotification::new(
            "goal-b",
            worker.id.clone(),
            source_event.id.clone(),
            "foreign_goal",
            "Notification from another goal",
            EventSeverity::Critical,
            vec!["goal:goal-b".into()],
            None,
            true,
            None,
            notification_created_at,
        )
        .unwrap();
        store
            .upsert_notification(&foreign_goal_notification)
            .unwrap();
        let limits = WorkerContextLimits::default();
        let packet = WorkerContextAssembler::new(tracker, store, limits.clone())
            .assemble(&worker.id, &claim.id, now)
            .unwrap();

        assert_eq!(packet.assignment.features.len(), 1);
        assert_eq!(packet.assignment.features[0].steps.len(), 24);
        assert_eq!(packet.prior_state.turns.len(), 8);
        assert_eq!(packet.prior_state.turns[0].sequence, 5);
        assert_eq!(packet.prior_state.events.len(), 20);
        assert_eq!(packet.prior_state.events[0].sequence, Some(7));
        assert_eq!(packet.prior_state.notifications.len(), 8);
        assert_eq!(
            packet.prior_state.notifications[0].summary,
            "Dependency notification 0"
        );
        assert!(
            packet
                .prior_state
                .notifications
                .iter()
                .all(
                    |notification| notification.summary != "Notification for another worker"
                        && notification.summary != "Notification from another goal"
                )
        );
        assert_eq!(packet.truncation.omitted_turns, 4);
        assert_eq!(packet.truncation.omitted_notifications, 1);
        assert!(packet.goal.description.ends_with('…'));
        assert!(serde_json::to_vec(&packet).unwrap().len() <= limits.max_encoded_bytes);
        let instructions = packet.execution_instructions().unwrap();
        assert!(instructions.starts_with("# Golazo Autonomous Worker Protocol"));
        assert!(instructions.contains("durable coordination context"));
        assert!(instructions.contains("Dependency notification 0"));
        assert!(instructions.contains(&format!("\"id\": \"{}\"", claim.id.as_str())));
    }

    #[test]
    fn delivers_and_accepts_typed_notification_acknowledgement_and_outcome() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );
        let now = Utc::now();
        let notification = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            EventId::new(),
            "dependency_changed",
            "A required dependency changed.",
            EventSeverity::Warning,
            vec!["package:producer".into()],
            Some("Refresh assumptions before continuing.".into()),
            true,
            None,
            now - Duration::seconds(1),
        )
        .unwrap();
        store.upsert_notification(&notification).unwrap();

        let delivered = service
            .execute(
                &worker.id,
                WorkerToolRequest::RefreshAtBoundary {
                    claim_id: claim.id.as_str().into(),
                    boundary: WorkerBoundary::MajorEdit,
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            delivered,
            WorkerToolResponse::BoundaryContext { ref packet, .. }
                if packet.prior_state.notifications.iter().any(|context| context.id == notification.id.as_str())
        ));
        let delivered_record = store.notification(&notification.id).unwrap().unwrap();
        assert_eq!(delivered_record.state, NotificationState::Delivered);
        assert_eq!(delivered_record.delivery_attempts, 1);

        let acknowledged = service
            .execute(
                &worker.id,
                WorkerToolRequest::AcknowledgeNotification {
                    claim_id: claim.id.as_str().into(),
                    notification_id: notification.id.as_str().into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap();
        assert!(matches!(
            acknowledged,
            WorkerToolResponse::Notification { ref notification }
                if notification.state == NotificationState::Acknowledged
        ));

        let acted_on = service
            .execute(
                &worker.id,
                WorkerToolRequest::ActOnNotification {
                    claim_id: claim.id.as_str().into(),
                    notification_id: notification.id.as_str().into(),
                    outcome: "Refreshed assumptions and reran validation.".into(),
                },
                now + Duration::seconds(2),
            )
            .unwrap();
        assert!(matches!(
            acted_on,
            WorkerToolResponse::Notification { ref notification }
                if notification.state == NotificationState::ActedOn
                    && notification.audit_history.len() == 4
        ));
    }

    #[test]
    fn replays_only_pending_notifications_after_each_recovery_boundary() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let tracker_after_restart = tracker.clone();
        let now = Utc::now();
        let created_at = now - Duration::seconds(10);
        let mut delivered = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            EventId::new(),
            "dependency_changed",
            "Delivered context still needs acknowledgement.",
            EventSeverity::Warning,
            vec!["package:producer".into()],
            Some("Acknowledge after reviewing the dependency.".into()),
            true,
            None,
            created_at,
        )
        .unwrap();
        delivered
            .transition_attributed(
                NotificationState::Delivered,
                CoordinationActor::System,
                Some("initial delivery".into()),
                created_at + Duration::seconds(1),
            )
            .unwrap();
        let mut acknowledged = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            EventId::new(),
            "contract_changed",
            "Acknowledged context still needs an outcome.",
            EventSeverity::Info,
            vec!["contract:shared-api".into()],
            Some("Record the outcome after adapting the consumer.".into()),
            true,
            None,
            created_at,
        )
        .unwrap();
        acknowledged
            .transition_attributed(
                NotificationState::Delivered,
                CoordinationActor::System,
                Some("initial delivery".into()),
                created_at + Duration::seconds(1),
            )
            .unwrap();
        acknowledged
            .transition_attributed(
                NotificationState::Acknowledged,
                CoordinationActor::Worker {
                    worker_id: worker.id.clone(),
                },
                Some("review started".into()),
                created_at + Duration::seconds(2),
            )
            .unwrap();
        let mut acted_on = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            EventId::new(),
            "completed_context",
            "This notification already has a terminal outcome.",
            EventSeverity::Info,
            vec![],
            None,
            false,
            None,
            created_at,
        )
        .unwrap();
        acted_on
            .transition(
                NotificationState::Delivered,
                created_at + Duration::seconds(1),
            )
            .unwrap();
        acted_on
            .transition_attributed(
                NotificationState::ActedOn,
                CoordinationActor::Worker {
                    worker_id: worker.id.clone(),
                },
                Some("completed before failure".into()),
                created_at + Duration::seconds(2),
            )
            .unwrap();
        let expired = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            EventId::new(),
            "stale_context",
            "This notification expired while the worker was unavailable.",
            EventSeverity::Warning,
            vec![],
            None,
            false,
            Some(created_at + Duration::seconds(5)),
            created_at,
        )
        .unwrap();
        for notification in [&delivered, &acknowledged, &acted_on, &expired] {
            store.upsert_notification(notification).unwrap();
        }

        let restarted_store = Arc::new(SqliteCoordinationStore::open(store.path()).unwrap());
        let service = WorkerProtocolService::new(
            tracker_after_restart,
            Arc::clone(&restarted_store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );
        let causes = [
            WorkerRecoveryKind::BackendRestart,
            WorkerRecoveryKind::WorkerFailure,
            WorkerRecoveryKind::ThreadFailure,
            WorkerRecoveryKind::WorkspaceRecovery,
        ];
        for (index, recovery_kind) in causes.into_iter().enumerate() {
            let response = service
                .execute(
                    &worker.id,
                    WorkerToolRequest::RecoverContext {
                        claim_id: claim.id.as_str().into(),
                        recovery_kind,
                    },
                    now + Duration::seconds(index as i64),
                )
                .unwrap();
            match response {
                WorkerToolResponse::RecoveryContext {
                    recovery_kind: observed,
                    packet,
                    replayed_notification_ids,
                    ..
                } => {
                    assert_eq!(observed, recovery_kind);
                    assert_eq!(replayed_notification_ids.len(), 2);
                    assert!(replayed_notification_ids.contains(&delivered.id.as_str().into()));
                    assert!(replayed_notification_ids.contains(&acknowledged.id.as_str().into()));
                    assert_eq!(packet.prior_state.notifications.len(), 2);
                    assert!(packet.prior_state.notifications.iter().any(|context| {
                        context.id == delivered.id.as_str()
                            && context.state == NotificationState::Delivered
                    }));
                    assert!(packet.prior_state.notifications.iter().any(|context| {
                        context.id == acknowledged.id.as_str()
                            && context.state == NotificationState::Acknowledged
                    }));
                }
                other => panic!("unexpected recovery response: {other:?}"),
            }
        }

        assert_eq!(
            restarted_store
                .notification(&delivered.id)
                .unwrap()
                .unwrap()
                .delivery_attempts,
            5
        );
        assert_eq!(
            restarted_store
                .notification(&acknowledged.id)
                .unwrap()
                .unwrap()
                .delivery_attempts,
            5
        );
        assert_eq!(
            restarted_store
                .notification(&expired.id)
                .unwrap()
                .unwrap()
                .state,
            NotificationState::Expired
        );
        assert_eq!(
            restarted_store
                .notification(&acted_on.id)
                .unwrap()
                .unwrap()
                .delivery_attempts,
            1
        );
    }

    #[test]
    fn worker_requests_exceptional_coordination_through_a_scoped_durable_notification() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let now = Utc::now();
        let mut target = Worker::new("goal-a", now);
        target.transition(WorkerState::Starting, now, None).unwrap();
        target.transition(WorkerState::Active, now, None).unwrap();
        store.upsert_worker(&target).unwrap();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );
        let request = WorkerToolRequest::RequestCoordination {
            claim_id: claim.id.as_str().into(),
            target_scope: format!("worker:{}", target.id.as_str()),
            desired_outcome: "Confirm which shared response field the target worker will consume."
                .into(),
            evidence_refs: vec!["contract:shared-response".into()],
            severity: EventSeverity::Warning,
            idempotency_key: "shared-response-boundary".into(),
        };
        let first = service.execute(&worker.id, request.clone(), now).unwrap();
        let (event_id, sequence, notification_id) = match first {
            WorkerToolResponse::CoordinationRequested {
                event_id,
                sequence,
                target_worker_ids,
                notification_ids,
            } => {
                assert_eq!(target_worker_ids, vec![target.id.as_str().to_string()]);
                assert_eq!(notification_ids.len(), 1);
                (event_id, sequence, notification_ids[0].clone())
            }
            other => panic!("unexpected coordination response: {other:?}"),
        };
        let notification = store
            .notification(&NotificationId::parse(&notification_id).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(notification.target_worker, target.id);
        assert_eq!(notification.state, NotificationState::Queued);
        assert!(notification.required_acknowledgement);
        assert!(notification.summary.contains("requests coordination"));
        let event = store
            .event(&EventId::parse(&event_id).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(event.sequence, Some(sequence));
        assert!(matches!(
            event.typed_payload().unwrap(),
            Some(CoordinationEventPayload::CoordinationRequest(ref payload))
                if payload.requester_worker_id == worker.id
                    && payload.target_scope == format!("worker:{}", target.id.as_str())
        ));

        let retried = service
            .execute(&worker.id, request, now + Duration::seconds(1))
            .unwrap();
        assert!(matches!(
            retried,
            WorkerToolResponse::CoordinationRequested {
                ref event_id,
                sequence: retried_sequence,
                ref notification_ids,
                ..
            } if event_id == event.id.as_str()
                && retried_sequence == sequence
                && notification_ids == &[notification.id.as_str().to_string()]
        ));
        assert_eq!(
            store
                .notifications_for_worker(&target.id, Some(NotificationState::Queued))
                .unwrap()
                .len(),
            1
        );

        let self_target = service
            .execute(
                &worker.id,
                WorkerToolRequest::RequestCoordination {
                    claim_id: claim.id.as_str().into(),
                    target_scope: format!("worker:{}", worker.id.as_str()),
                    desired_outcome: "Coordinate with myself.".into(),
                    evidence_refs: vec![],
                    severity: EventSeverity::Info,
                    idempotency_key: "self-target".into(),
                },
                now + Duration::seconds(2),
            )
            .unwrap_err();
        assert!(matches!(
            self_target,
            WorkerToolError::CoordinationRequest(CoordinationRequestError::NoRecipients)
        ));
    }

    #[test]
    fn mediated_exchange_requires_attempted_notification_and_persists_a_shared_note() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let now = Utc::now();
        let mut target = Worker::new("goal-a", now);
        target.transition(WorkerState::Starting, now, None).unwrap();
        target.transition(WorkerState::Active, now, None).unwrap();
        let target_claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "target-feature".into(),
            },
            target.id.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        target.active_claims.push(target_claim.id.clone());
        store.upsert_worker(&target).unwrap();
        store.insert_claim(&target_claim).unwrap();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );

        let requested = service
            .execute(
                &worker.id,
                WorkerToolRequest::RequestCoordination {
                    claim_id: claim.id.as_str().into(),
                    target_scope: format!("worker:{}", target.id.as_str()),
                    desired_outcome: "Agree on the shared response field name.".into(),
                    evidence_refs: vec!["contract:response".into()],
                    severity: EventSeverity::Warning,
                    idempotency_key: "response-field-exchange".into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap();
        let (request_event_id, notification_id) = match requested {
            WorkerToolResponse::CoordinationRequested {
                event_id,
                notification_ids,
                ..
            } => (
                EventId::parse(event_id).unwrap(),
                NotificationId::parse(&notification_ids[0]).unwrap(),
            ),
            other => panic!("unexpected request response: {other:?}"),
        };
        let before_delivery = service
            .execute(
                &worker.id,
                WorkerToolRequest::OpenCoordinationExchange {
                    claim_id: claim.id.as_str().into(),
                    request_event_id: request_event_id.as_str().into(),
                    target_worker_id: target.id.as_str().into(),
                    insufficiency_reason: "The field name still needs a synchronous answer.".into(),
                    ttl_seconds: 300,
                },
                now + Duration::seconds(2),
            )
            .unwrap_err();
        assert!(matches!(
            before_delivery,
            WorkerToolError::CoordinationExchange(
                CoordinationExchangeError::NotificationNotAttempted
            )
        ));
        let notification = store.notification(&notification_id).unwrap().unwrap();
        NotificationLifecycleService::new(Arc::clone(&store))
            .mark_delivered(
                "goal-a",
                &notification_id,
                notification.metadata.updated_at,
                CoordinationActor::System,
                now + Duration::seconds(2),
            )
            .unwrap();

        let opened = service
            .execute(
                &worker.id,
                WorkerToolRequest::OpenCoordinationExchange {
                    claim_id: claim.id.as_str().into(),
                    request_event_id: request_event_id.as_str().into(),
                    target_worker_id: target.id.as_str().into(),
                    insufficiency_reason: "The field name still needs a synchronous answer.".into(),
                    ttl_seconds: 300,
                },
                now + Duration::seconds(3),
            )
            .unwrap();
        let exchange = match opened {
            WorkerToolResponse::CoordinationExchange { exchange } => exchange,
            other => panic!("unexpected exchange response: {other:?}"),
        };
        assert_eq!(
            exchange.state,
            super::super::domain::CoordinationExchangeState::Open
        );
        assert_eq!(exchange.max_messages, 4);

        let requester_message = service
            .execute(
                &worker.id,
                WorkerToolRequest::PostCoordinationExchange {
                    claim_id: claim.id.as_str().into(),
                    exchange_id: exchange.id.as_str().into(),
                    body: "Can the consumer use response.value?".into(),
                    evidence_refs: vec!["contract:response".into()],
                },
                now + Duration::seconds(4),
            )
            .unwrap();
        assert!(matches!(
            requester_message,
            WorkerToolResponse::CoordinationExchange { ref exchange }
                if exchange.messages.len() == 1
        ));
        let repeated_author = service
            .execute(
                &worker.id,
                WorkerToolRequest::PostCoordinationExchange {
                    claim_id: claim.id.as_str().into(),
                    exchange_id: exchange.id.as_str().into(),
                    body: "A second unmediated instruction.".into(),
                    evidence_refs: vec![],
                },
                now + Duration::seconds(5),
            )
            .unwrap_err();
        assert!(matches!(
            repeated_author,
            WorkerToolError::CoordinationExchange(CoordinationExchangeError::Domain(_))
        ));

        service
            .execute(
                &target.id,
                WorkerToolRequest::PostCoordinationExchange {
                    claim_id: target_claim.id.as_str().into(),
                    exchange_id: exchange.id.as_str().into(),
                    body: "Yes. I will consume response.value.".into(),
                    evidence_refs: vec!["test:consumer".into()],
                },
                now + Duration::seconds(5),
            )
            .unwrap();
        let completed = service
            .execute(
                &target.id,
                WorkerToolRequest::CompleteCoordinationExchange {
                    claim_id: target_claim.id.as_str().into(),
                    exchange_id: exchange.id.as_str().into(),
                    shared_note:
                        "Producer and consumer agreed on response.value; target owns consumer validation."
                            .into(),
                },
                now + Duration::seconds(6),
            )
            .unwrap();
        let completed = match completed {
            WorkerToolResponse::CoordinationExchange { exchange } => exchange,
            other => panic!("unexpected completed exchange: {other:?}"),
        };
        assert_eq!(
            completed.state,
            super::super::domain::CoordinationExchangeState::Completed
        );
        assert_eq!(completed.messages.len(), 2);
        assert!(
            completed
                .shared_note
                .as_deref()
                .unwrap()
                .contains("response.value")
        );
        assert_eq!(store.exchange(&exchange.id).unwrap(), Some(completed));
        let exchange_events = store
            .events_for_goal("goal-a", 0, Some(exchange.id.as_str()), 20)
            .unwrap();
        assert_eq!(exchange_events.len(), 4);
        assert!(
            exchange_events
                .iter()
                .all(|event| { event.kind == CoordinationEventKind::CoordinationExchangeChanged })
        );
    }

    #[test]
    fn rejects_a_claim_owned_by_another_worker() {
        let (_directory, store, tracker, _worker, claim) = fixture();
        let mut other = Worker::new("goal-a", Utc::now());
        other.permission_profile = Some(WorkerPermissionProfile::default());
        other.workspace = Some(WorkspaceBinding {
            repository_id: "repo-a".into(),
            canonical_repository_path: "/tmp/repo".into(),
            worktree_path: "/tmp/worktree".into(),
            branch: "branch".into(),
            base_revision: "base-a".into(),
            created_at: None,
            creation_evidence: vec![],
        });
        store.upsert_worker(&other).unwrap();

        let error = WorkerContextAssembler::new(tracker, store, WorkerContextLimits::default())
            .assemble(&other.id, &claim.id, Utc::now())
            .unwrap_err();
        assert!(matches!(
            error,
            WorkerContextError::ClaimOwnerMismatch { .. }
        ));
    }

    #[test]
    fn typed_worker_tools_enforce_ownership_and_persist_evidence() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let tracker_updates = tracker.clone();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );
        let now = Utc::now();

        let heartbeat = service
            .execute(
                &worker.id,
                WorkerToolRequest::Heartbeat {
                    claim_id: claim.id.as_str().into(),
                    expected_generation: claim.lease_generation,
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            heartbeat,
            WorkerToolResponse::Claim { claim: ref renewed }
                if renewed.heartbeat_at == now && renewed.state == ClaimState::Active
        ));
        let before_evidence = service
            .execute(
                &worker.id,
                WorkerToolRequest::RefreshAtBoundary {
                    claim_id: claim.id.as_str().into(),
                    boundary: WorkerBoundary::Validation,
                },
                now,
            )
            .unwrap();
        let before_revision = match before_evidence {
            WorkerToolResponse::BoundaryContext {
                boundary: WorkerBoundary::Validation,
                revision,
                ..
            } => revision,
            other => panic!("unexpected boundary response: {other:?}"),
        };

        let evidence = service
            .execute(
                &worker.id,
                WorkerToolRequest::RecordEvidence {
                    claim_id: claim.id.as_str().into(),
                    summary: "Focused tests passed".into(),
                    evidence_refs: vec!["cargo test protocol".into()],
                    idempotency_key: "evidence-1".into(),
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            evidence,
            WorkerToolResponse::Event { sequence: 1 }
        ));
        let evidence_retry = service
            .execute(
                &worker.id,
                WorkerToolRequest::RecordEvidence {
                    claim_id: claim.id.as_str().into(),
                    summary: "Focused tests passed".into(),
                    evidence_refs: vec!["cargo test protocol".into()],
                    idempotency_key: "evidence-1".into(),
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            evidence_retry,
            WorkerToolResponse::Event { sequence: 1 }
        ));
        let events = store
            .latest_events_for_goal("goal-a", Some(claim.id.as_str()), 10)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload_schema, "coordination.activity.v1");
        assert!(matches!(
            events[0].typed_payload().unwrap(),
            Some(CoordinationEventPayload::Activity(ActivityEventPayload {
                category: ActivityCategory::Artifact,
                ..
            }))
        ));
        let after_evidence = service
            .execute(
                &worker.id,
                WorkerToolRequest::RefreshAtBoundary {
                    claim_id: claim.id.as_str().into(),
                    boundary: WorkerBoundary::Integration,
                },
                now,
            )
            .unwrap();
        let after_revision = match after_evidence {
            WorkerToolResponse::BoundaryContext {
                boundary: WorkerBoundary::Integration,
                revision,
                ..
            } => revision,
            other => panic!("unexpected boundary response: {other:?}"),
        };
        assert_ne!(before_revision.fingerprint, after_revision.fingerprint);

        let validation = service
            .execute(
                &worker.id,
                WorkerToolRequest::PublishActivity {
                    claim_id: claim.id.as_str().into(),
                    idempotency_key: "validation-1".into(),
                    activity: WorkerActivity {
                        kind: WorkerActivityKind::Validation,
                        summary: "Protocol tests passed".into(),
                        progress_percent: Some(60),
                        changed_scope: vec![],
                        artifact_id: None,
                        evidence_refs: vec!["cargo test coordination::protocol".into()],
                        validation_succeeded: Some(true),
                    },
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            validation,
            WorkerToolResponse::Event { sequence: 2 }
        ));
        let validation_events = store
            .latest_events_for_goal("goal-a", Some(claim.id.as_str()), 10)
            .unwrap();
        assert_eq!(
            validation_events[1].kind,
            CoordinationEventKind::ValidationCompleted
        );

        let invalid_scope = service
            .execute(
                &worker.id,
                WorkerToolRequest::PublishActivity {
                    claim_id: claim.id.as_str().into(),
                    idempotency_key: "invalid-scope-1".into(),
                    activity: WorkerActivity {
                        kind: WorkerActivityKind::ScopeChanged,
                        summary: "Scope changed".into(),
                        progress_percent: None,
                        changed_scope: vec![],
                        artifact_id: None,
                        evidence_refs: vec![],
                        validation_succeeded: None,
                    },
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            invalid_scope,
            WorkerToolError::InvalidActivity("scope changes require changed_scope")
        ));

        let oversized = service
            .execute(
                &worker.id,
                WorkerToolRequest::PublishEvent {
                    claim_id: claim.id.as_str().into(),
                    kind: WorkerPublishableEventKind::Activity,
                    severity: EventSeverity::Info,
                    payload: serde_json::json!({"detail": "x".repeat(9_000)}),
                    idempotency_key: "oversized-1".into(),
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            oversized,
            WorkerToolError::PayloadTooLarge("event payload")
        ));

        for index in 0..30 {
            tracker_updates
                .set_step("goal-a", "feature-a", &format!("step-{index}"), true)
                .unwrap();
        }
        tracker_updates
            .add_slice(
                "goal-a",
                "feature-a",
                "Completed and verified the claimed feature",
                Some(Status::Done),
                vec!["cargo test coordination::protocol".into()],
            )
            .unwrap();
        let completion_context = service
            .execute(
                &worker.id,
                WorkerToolRequest::RefreshAtBoundary {
                    claim_id: claim.id.as_str().into(),
                    boundary: WorkerBoundary::Integration,
                },
                now,
            )
            .unwrap();
        let completion_revision = match completion_context {
            WorkerToolResponse::BoundaryContext { revision, .. } => revision,
            other => panic!("unexpected completion context: {other:?}"),
        };

        let stale_completion = service
            .execute(
                &worker.id,
                WorkerToolRequest::Complete {
                    claim_id: claim.id.as_str().into(),
                    expected_generation: claim.lease_generation,
                    reason: "Verified and integrated".into(),
                    artifact_id: "artifact-protocol".into(),
                    integration_revision: "commit-protocol".into(),
                    evidence_refs: vec!["cargo test coordination::protocol".into()],
                    validation_event_sequence: 2,
                    audit_slice_id: "slice-000001".into(),
                    context_revision: after_revision,
                    integration_boundary_satisfied: true,
                    idempotency_key: "stale-complete-protocol".into(),
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            stale_completion,
            WorkerToolError::CompletionGateFailed(_)
        ));

        let completed = service
            .execute(
                &worker.id,
                WorkerToolRequest::Complete {
                    claim_id: claim.id.as_str().into(),
                    expected_generation: claim.lease_generation,
                    reason: "Verified and integrated".into(),
                    artifact_id: "artifact-protocol".into(),
                    integration_revision: "commit-protocol".into(),
                    evidence_refs: vec!["cargo test coordination::protocol".into()],
                    validation_event_sequence: 2,
                    audit_slice_id: "slice-000001".into(),
                    context_revision: completion_revision,
                    integration_boundary_satisfied: true,
                    idempotency_key: "complete-protocol".into(),
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            completed,
            WorkerToolResponse::Claim { claim: ref finished }
                if finished.state == ClaimState::Completed
        ));
        let terminal_events = store
            .latest_events_for_goal("goal-a", Some(claim.id.as_str()), 10)
            .unwrap();
        let terminal_event = terminal_events.last().unwrap();
        assert_eq!(
            terminal_event.kind,
            CoordinationEventKind::CompletionPublished
        );
        assert!(matches!(
            terminal_event.typed_payload().unwrap(),
            Some(CoordinationEventPayload::Completion(CompletionEventPayload {
                ref integration_revision,
                ..
            })) if integration_revision == "commit-protocol"
        ));
    }

    #[test]
    fn publication_authorization_rejects_cross_worker_scope_and_identity_spoofing() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );
        let stranger = WorkerId::new();
        let now = Utc::now();

        let cross_worker = service
            .execute(
                &stranger,
                WorkerToolRequest::PublishActivity {
                    claim_id: claim.id.as_str().into(),
                    activity: WorkerActivity {
                        kind: WorkerActivityKind::Progress,
                        summary: "Spoofed progress".into(),
                        progress_percent: Some(10),
                        changed_scope: vec![],
                        artifact_id: None,
                        evidence_refs: vec![],
                        validation_succeeded: None,
                    },
                    idempotency_key: "cross-worker".into(),
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            cross_worker,
            WorkerToolError::InactiveOrUnownedClaim
        ));

        let spoofed_payload =
            serde_json::to_value(CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: stranger,
                claim_id: claim.id.clone(),
                category: ActivityCategory::Progress,
                summary: "Spoofed identity".into(),
                progress_percent: Some(10),
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec![],
                validation_succeeded: None,
            }))
            .unwrap();
        let spoofed_identity = service
            .execute(
                &worker.id,
                WorkerToolRequest::PublishEvent {
                    claim_id: claim.id.as_str().into(),
                    kind: WorkerPublishableEventKind::Activity,
                    severity: EventSeverity::Info,
                    payload: spoofed_payload,
                    idempotency_key: "spoofed-identity".into(),
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            spoofed_identity,
            WorkerToolError::InactiveOrUnownedClaim
        ));

        let contract = SharedContract::new(
            "goal-a",
            "api.unauthorized",
            "Unauthorized API",
            crate::coordination::domain::ContractKind::Api,
            now,
        );
        store.upsert_contract(&contract).unwrap();
        let unauthorized_contract = service
            .execute(
                &worker.id,
                WorkerToolRequest::ReviseContract {
                    claim_id: claim.id.as_str().into(),
                    contract_id: contract.id.as_str().into(),
                    expected_revision: 1,
                    compatibility_notes: "Attempted change".into(),
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            unauthorized_contract,
            WorkerToolError::ContractChangeNotAuthorized(_)
        ));
        assert!(
            store
                .events_for_goal("goal-a", 0, Some(claim.id.as_str()), 100)
                .unwrap()
                .is_empty()
        );

        let mut terminal_claim = store.claim(&claim.id).unwrap().unwrap();
        terminal_claim
            .transition(
                ClaimState::Released,
                super::super::domain::ClaimOutcome {
                    kind: super::super::domain::ClaimOutcomeKind::Released,
                    reason: "authorization test complete".into(),
                    artifact_id: None,
                    evidence_refs: vec!["test:authorization".into()],
                    escalation_id: None,
                    at: now,
                },
            )
            .unwrap();
        store.upsert_claim(&terminal_claim).unwrap();
        let terminal_read = service
            .execute(
                &worker.id,
                WorkerToolRequest::RefreshContext {
                    claim_id: claim.id.as_str().into(),
                },
                now,
            )
            .unwrap_err();
        assert!(matches!(
            terminal_read,
            WorkerToolError::InactiveOrUnownedClaim
        ));
    }

    #[test]
    fn stale_contract_revision_pauses_the_worker() {
        let (_directory, store, tracker, worker, claim) = fixture();
        let now = Utc::now();
        let mut contract = SharedContract::new(
            "goal-a",
            "api.v1",
            "API v1",
            crate::coordination::domain::ContractKind::Api,
            now,
        );
        store.upsert_contract(&contract).unwrap();
        let mut declared_claim = store.claim(&claim.id).unwrap().unwrap();
        declared_claim.consumes_contracts.push(ContractExpectation {
            contract_id: contract.id.as_str().into(),
            expected_revision: 1,
        });
        store.upsert_claim(&declared_claim).unwrap();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );
        contract.revise(WorkerId::new(), "response shape changed", now);
        store.upsert_contract(&contract).unwrap();
        let refreshed = service
            .execute(
                &worker.id,
                WorkerToolRequest::RefreshAtBoundary {
                    claim_id: claim.id.as_str().into(),
                    boundary: WorkerBoundary::MajorEdit,
                },
                now,
            )
            .unwrap();
        let expected_revision = match refreshed {
            WorkerToolResponse::BoundaryContext { revision, .. } => revision,
            other => panic!("unexpected refreshed context: {other:?}"),
        };
        let result = service
            .execute(
                &worker.id,
                WorkerToolRequest::CheckAssumptions {
                    claim_id: claim.id.as_str().into(),
                    boundary: WorkerBoundary::MajorEdit,
                    expected_revision,
                },
                now,
            )
            .unwrap();
        assert!(matches!(
            result,
            WorkerToolResponse::AssumptionStatus {
                disposition: AssumptionDisposition::Paused,
                ref changes,
                ..
            } if changes.iter().any(|change| change.contains("contract"))
        ));
        assert_eq!(
            store.worker(&worker.id).unwrap().unwrap().state,
            WorkerState::Paused
        );
        let events = store
            .latest_events_for_goal("goal-a", Some(claim.id.as_str()), 10)
            .unwrap();
        assert_eq!(
            events.last().unwrap().kind,
            CoordinationEventKind::DependencyChanged
        );
    }

    #[test]
    fn rollover_persists_transfer_and_preserves_worker_and_claim_identity() {
        let (_directory, store, tracker, mut worker, claim) = fixture();
        let tracker_after_restart = tracker.clone();
        let now = Utc::now();
        worker.current_run_id = Some("run-current".into());
        worker.current_thread_id = Some("thread-current".into());
        worker.turn_history.push(WorkerTurnBinding {
            sequence: worker.turn_history.len() as u64 + 1,
            run_id: "run-current".into(),
            thread_id: "thread-current".into(),
            started_at: now,
            completed_at: None,
            continuation_of_thread_id: None,
            context_transfer_artifact_id: None,
        });
        store.upsert_worker(&worker).unwrap();
        let service = WorkerProtocolService::new(
            tracker,
            Arc::clone(&store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );

        let prepared = service
            .execute(
                &worker.id,
                WorkerToolRequest::PrepareRollover {
                    claim_id: claim.id.as_str().into(),
                    reason: RolloverReason::ContextLimit,
                    summary: "Implemented the protocol core".into(),
                    completed_evidence: vec!["focused tests passed".into()],
                    unresolved_assumptions: vec!["integration API remains".into()],
                    next_safe_action: "Continue with the integration API".into(),
                },
                now,
            )
            .unwrap();
        let artifact = match prepared {
            WorkerToolResponse::TransferPrepared { artifact, worker } => {
                assert!(worker.current_run_id.is_none());
                assert_eq!(worker.id, claim.owner);
                *artifact
            }
            other => panic!("unexpected transfer response: {other:?}"),
        };
        let persisted: ContextTransferArtifact =
            store.context_transfer(&artifact.id).unwrap().unwrap();
        assert_eq!(persisted.worker_id, worker.id.as_str());
        assert_eq!(persisted.claim_id, claim.id.as_str());
        let mut pending_notification = WorkerNotification::new(
            "goal-a",
            worker.id.clone(),
            EventId::new(),
            "rollover_context",
            "Pending coordination context must survive the thread rollover.",
            EventSeverity::Warning,
            vec!["transfer:pending".into()],
            Some("Review this context in the continuation thread.".into()),
            true,
            None,
            now - Duration::seconds(2),
        )
        .unwrap();
        pending_notification
            .transition(NotificationState::Delivered, now - Duration::seconds(1))
            .unwrap();
        store.upsert_notification(&pending_notification).unwrap();

        let restarted_store = Arc::new(SqliteCoordinationStore::open(store.path()).unwrap());
        let restarted_service = WorkerProtocolService::new(
            tracker_after_restart,
            Arc::clone(&restarted_store),
            ClaimPolicy::default(),
            WorkerContextLimits::default(),
        );

        let continued = restarted_service
            .execute(
                &worker.id,
                WorkerToolRequest::ContinueFromTransfer {
                    claim_id: claim.id.as_str().into(),
                    artifact_id: artifact.id.clone(),
                    run_id: "run-next".into(),
                    thread_id: "thread-next".into(),
                },
                now + Duration::seconds(1),
            )
            .unwrap();
        assert!(matches!(
            continued,
            WorkerToolResponse::ContinuationStarted {
                worker: ref continued,
                ref recovery_context,
                ref replayed_notification_ids,
                ..
            }
                if continued.id == worker.id
                    && continued.active_claims.contains(&claim.id)
                    && continued.current_run_id.as_deref() == Some("run-next")
                    && continued.current_thread_id.as_deref() == Some("thread-next")
                    && continued.turn_history.last().unwrap().context_transfer_artifact_id.as_deref()
                        == Some(artifact.id.as_str())
                    && replayed_notification_ids == &[pending_notification.id.as_str().to_string()]
                    && recovery_context.prior_state.notifications.iter().any(|context|
                        context.id == pending_notification.id.as_str()
                            && context.state == NotificationState::Delivered)
        ));
    }

    #[test]
    fn release_block_and_cancel_flows_preserve_durable_outcomes() {
        {
            let (_directory, store, tracker, worker, claim) = fixture();
            let service = WorkerProtocolService::new(
                tracker,
                Arc::clone(&store),
                ClaimPolicy::default(),
                WorkerContextLimits::default(),
            );
            let released = service
                .execute(
                    &worker.id,
                    WorkerToolRequest::Release {
                        claim_id: claim.id.as_str().into(),
                        expected_generation: claim.lease_generation,
                        reason: "Partial implementation is reviewable".into(),
                        artifact_id: Some("artifact-partial".into()),
                        evidence_refs: vec!["partial tests passed".into()],
                        idempotency_key: "release-partial".into(),
                    },
                    Utc::now(),
                )
                .unwrap();
            assert!(matches!(
                released,
                WorkerToolResponse::Claim { claim: ref released }
                    if released.state == ClaimState::Released
                        && released.outcome.as_ref().unwrap().artifact_id.as_deref()
                            == Some("artifact-partial")
            ));
        }
        {
            let (_directory, store, tracker, worker, claim) = fixture();
            let service = WorkerProtocolService::new(
                tracker,
                Arc::clone(&store),
                ClaimPolicy::default(),
                WorkerContextLimits::default(),
            );
            let blocked = service
                .execute(
                    &worker.id,
                    WorkerToolRequest::Block {
                        claim_id: claim.id.as_str().into(),
                        expected_generation: claim.lease_generation,
                        reason: "Contract decision is required".into(),
                        evidence_refs: vec!["contract mismatch".into()],
                        escalation_id: None,
                        idempotency_key: "block-contract".into(),
                    },
                    Utc::now(),
                )
                .unwrap();
            assert!(matches!(
                blocked,
                WorkerToolResponse::Claim { claim: ref blocked }
                    if blocked.state == ClaimState::Blocked
            ));
            let events = store
                .latest_events_for_goal("goal-a", Some(claim.id.as_str()), 10)
                .unwrap();
            assert_eq!(
                events.last().unwrap().kind,
                CoordinationEventKind::BlockerRaised
            );
        }
        {
            let (_directory, store, tracker, worker, claim) = fixture();
            let service = WorkerProtocolService::new(
                tracker,
                Arc::clone(&store),
                ClaimPolicy::default(),
                WorkerContextLimits::default(),
            );
            let cancelled = service
                .execute(
                    &worker.id,
                    WorkerToolRequest::Cancel {
                        claim_id: claim.id.as_str().into(),
                        expected_generation: claim.lease_generation,
                        reason: "User cancelled this worker".into(),
                        evidence_refs: vec!["user-decision:cancel".into()],
                        idempotency_key: "cancel-worker".into(),
                    },
                    Utc::now(),
                )
                .unwrap();
            assert!(matches!(
                cancelled,
                WorkerToolResponse::Claim { claim: ref cancelled }
                    if cancelled.state == ClaimState::Revoked
            ));
            assert_eq!(
                store.worker(&worker.id).unwrap().unwrap().state,
                WorkerState::Cancelled
            );
        }
    }
}
