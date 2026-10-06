use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

pub const COORDINATION_SCHEMA_VERSION: u16 = 1;

macro_rules! durable_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self {
                Self(format!("{}-{}", $prefix, Uuid::new_v4()))
            }

            pub fn parse(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(DomainError::InvalidIdentity(stringify!($name)));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

durable_id!(WorkerId, "worker");
durable_id!(WorkPackageId, "package");
durable_id!(ClaimId, "claim");
durable_id!(ContractId, "contract");
durable_id!(EventId, "event");
durable_id!(SignalId, "signal");
durable_id!(InterventionId, "intervention");
durable_id!(EscalationId, "escalation");
durable_id!(NotificationId, "notification");
durable_id!(ExchangeId, "exchange");
durable_id!(IntegrationArtifactId, "integration-artifact");
durable_id!(ValidationReportId, "validation-report");
durable_id!(ReconciliationId, "reconciliation");
durable_id!(IntegrationJobId, "integration-job");
durable_id!(IntegrationFinalizationId, "integration-finalization");
durable_id!(IntegrationMaintenanceId, "integration-maintenance");

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("invalid empty identity for {0}")]
    InvalidIdentity(&'static str),
    #[error("invalid {entity} transition from {from} to {to}")]
    InvalidTransition {
        entity: &'static str,
        from: &'static str,
        to: &'static str,
    },
    #[error("a work package must contain at least one feature")]
    EmptyWorkPackage,
    #[error("claim lease generation must be greater than zero")]
    InvalidLeaseGeneration,
    #[error("stale claim lease generation: expected {expected}, received {received}")]
    StaleLeaseGeneration { expected: u64, received: u64 },
    #[error("claim lease expiry must be after its heartbeat")]
    InvalidLeaseWindow,
    #[error("a terminal claim requires an outcome")]
    MissingClaimOutcome,
    #[error("unsupported coordination schema version {0}")]
    UnsupportedSchemaVersion(u16),
    #[error("invalid coordination event payload: {0}")]
    InvalidEventPayload(String),
    #[error("invalid coordination signal: {0}")]
    InvalidSignal(String),
    #[error("invalid integration artifact: {0}")]
    InvalidIntegrationArtifact(String),
}

pub fn validate_schema_version(version: u16) -> Result<(), DomainError> {
    if version == 0 || version > COORDINATION_SCHEMA_VERSION {
        Err(DomainError::UnsupportedSchemaVersion(version))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordMetadata {
    pub schema_version: u16,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl RecordMetadata {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            schema_version: COORDINATION_SCHEMA_VERSION,
            created_at: now,
            updated_at: now,
        }
    }

    pub(crate) fn touch(&mut self, now: DateTime<Utc>) {
        self.updated_at = now;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Created,
    Starting,
    Active,
    Waiting,
    Paused,
    Blocked,
    Recovering,
    Completed,
    Failed,
    Cancelled,
}

impl WorkerState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Active => "active",
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Blocked => "blocked",
            Self::Recovering => "recovering",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn can_transition_to(self, next: Self) -> bool {
        use WorkerState::*;
        matches!(
            (self, next),
            (Created, Starting | Cancelled)
                | (Starting, Active | Recovering | Failed | Cancelled)
                | (
                    Active,
                    Waiting | Paused | Blocked | Recovering | Completed | Failed | Cancelled
                )
                | (
                    Waiting,
                    Active | Paused | Blocked | Recovering | Completed | Failed | Cancelled
                )
                | (Paused, Active | Recovering | Cancelled)
                | (Blocked, Active | Paused | Recovering | Failed | Cancelled)
                | (
                    Recovering,
                    Active | Waiting | Paused | Blocked | Failed | Cancelled
                )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerTerminationKind {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerTermination {
    pub kind: WorkerTerminationKind,
    pub reason: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceBinding {
    pub repository_id: String,
    pub canonical_repository_path: String,
    pub worktree_path: String,
    pub branch: String,
    pub base_revision: String,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub creation_evidence: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerTurnBinding {
    pub sequence: u64,
    pub run_id: String,
    pub thread_id: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub continuation_of_thread_id: Option<String>,
    pub context_transfer_artifact_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerToolCapability {
    ReadFiles,
    WriteFiles,
    RunCommands,
    Git,
    Network,
}

fn default_worker_tool_capabilities() -> Vec<WorkerToolCapability> {
    vec![
        WorkerToolCapability::ReadFiles,
        WorkerToolCapability::WriteFiles,
        WorkerToolCapability::RunCommands,
        WorkerToolCapability::Git,
    ]
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPermissionPolicy {
    pub sandbox: String,
    pub approval_policy: String,
    pub approvals_reviewer: String,
    pub network_access: bool,
    #[serde(default = "default_worker_tool_capabilities")]
    pub tool_capabilities: Vec<WorkerToolCapability>,
}

impl WorkerPermissionPolicy {
    pub fn conservative() -> Self {
        Self {
            sandbox: "workspace-write".into(),
            approval_policy: "on-request".into(),
            approvals_reviewer: "user".into(),
            network_access: false,
            tool_capabilities: default_worker_tool_capabilities(),
        }
    }
}

impl Default for WorkerPermissionPolicy {
    fn default() -> Self {
        Self::conservative()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPermissionDerivation {
    pub policy_version: u32,
    pub user_policy: WorkerPermissionPolicy,
    pub goal_policy: WorkerPermissionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPermissionProfile {
    pub sandbox: String,
    pub approval_policy: String,
    pub approvals_reviewer: String,
    pub network_access: bool,
    #[serde(default = "default_worker_tool_capabilities")]
    pub tool_capabilities: Vec<WorkerToolCapability>,
    #[serde(default)]
    pub derived_from: Option<WorkerPermissionDerivation>,
}

impl Default for WorkerPermissionProfile {
    fn default() -> Self {
        Self {
            sandbox: "workspace-write".into(),
            approval_policy: "on-request".into(),
            approvals_reviewer: "user".into(),
            network_access: false,
            tool_capabilities: WorkerPermissionPolicy::conservative().tool_capabilities,
            derived_from: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Worker {
    pub metadata: RecordMetadata,
    pub id: WorkerId,
    pub goal_id: String,
    pub state: WorkerState,
    pub active_claims: Vec<ClaimId>,
    pub current_run_id: Option<String>,
    pub current_thread_id: Option<String>,
    pub workspace: Option<WorkspaceBinding>,
    #[serde(default)]
    pub permission_profile: Option<WorkerPermissionProfile>,
    #[serde(default)]
    pub turn_history: Vec<WorkerTurnBinding>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub termination: Option<WorkerTermination>,
}

impl Worker {
    pub fn new(goal_id: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            id: WorkerId::new(),
            goal_id: goal_id.into(),
            state: WorkerState::Created,
            active_claims: Vec::new(),
            current_run_id: None,
            current_thread_id: None,
            workspace: None,
            permission_profile: None,
            turn_history: Vec::new(),
            last_heartbeat_at: None,
            termination: None,
        }
    }

    pub fn transition(
        &mut self,
        next: WorkerState,
        now: DateTime<Utc>,
        reason: Option<String>,
    ) -> Result<(), DomainError> {
        if !self.state.can_transition_to(next) {
            return Err(DomainError::InvalidTransition {
                entity: "worker",
                from: self.state.name(),
                to: next.name(),
            });
        }
        self.state = next;
        self.metadata.touch(now);
        self.termination = match next {
            WorkerState::Completed => Some(WorkerTermination {
                kind: WorkerTerminationKind::Completed,
                reason: reason.unwrap_or_else(|| "worker completed".into()),
                at: now,
            }),
            WorkerState::Failed => Some(WorkerTermination {
                kind: WorkerTerminationKind::Failed,
                reason: reason.unwrap_or_else(|| "worker failed".into()),
                at: now,
            }),
            WorkerState::Cancelled => Some(WorkerTermination {
                kind: WorkerTerminationKind::Cancelled,
                reason: reason.unwrap_or_else(|| "worker cancelled".into()),
                at: now,
            }),
            _ => None,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkPackageState {
    Draft,
    Planned,
    Ready,
    Claimed,
    Blocked,
    Integrating,
    Done,
    Cancelled,
}

impl WorkPackageState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Planned => "planned",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Blocked => "blocked",
            Self::Integrating => "integrating",
            Self::Done => "done",
            Self::Cancelled => "cancelled",
        }
    }

    fn can_transition_to(self, next: Self) -> bool {
        use WorkPackageState::*;
        matches!(
            (self, next),
            (Draft, Planned | Cancelled)
                | (Planned, Draft | Ready | Blocked | Cancelled)
                | (Ready, Planned | Claimed | Blocked | Cancelled)
                | (Claimed, Ready | Blocked | Integrating | Cancelled)
                | (Blocked, Planned | Ready | Claimed | Cancelled)
                | (Integrating, Claimed | Blocked | Done | Cancelled)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationBoundary {
    Feature,
    WorkPackage,
    Repository,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractExpectation {
    pub contract_id: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractConsumer {
    pub work_package_id: WorkPackageId,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractKind {
    Api,
    PersistedSchema,
    Migration,
    DomainModel,
    Configuration,
    GeneratedInterface,
    Serialization,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedContract {
    pub metadata: RecordMetadata,
    pub id: ContractId,
    pub goal_id: String,
    pub stable_key: String,
    pub title: String,
    pub kind: ContractKind,
    pub revision: u64,
    pub producer: Option<WorkPackageId>,
    pub dependents: Vec<ContractConsumer>,
    pub compatibility_notes: String,
    pub last_changed_by: Option<WorkerId>,
}

impl SharedContract {
    pub fn new(
        goal_id: impl Into<String>,
        stable_key: impl Into<String>,
        title: impl Into<String>,
        kind: ContractKind,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            id: ContractId::new(),
            goal_id: goal_id.into(),
            stable_key: stable_key.into(),
            title: title.into(),
            kind,
            revision: 1,
            producer: None,
            dependents: Vec::new(),
            compatibility_notes: String::new(),
            last_changed_by: None,
        }
    }

    pub fn revise(
        &mut self,
        changed_by: WorkerId,
        compatibility_notes: impl Into<String>,
        now: DateTime<Utc>,
    ) {
        self.revision += 1;
        self.last_changed_by = Some(changed_by);
        self.compatibility_notes = compatibility_notes.into();
        self.metadata.touch(now);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkPackage {
    pub metadata: RecordMetadata,
    pub id: WorkPackageId,
    pub goal_id: String,
    pub title: String,
    pub feature_ids: Vec<String>,
    pub depends_on: Vec<WorkPackageId>,
    pub priority: i32,
    pub state: WorkPackageState,
    pub integration_boundary: IntegrationBoundary,
    pub produces_contracts: Vec<String>,
    pub consumes_contracts: Vec<ContractExpectation>,
    pub ready_at: Option<DateTime<Utc>>,
}

impl WorkPackage {
    pub fn new(
        goal_id: impl Into<String>,
        title: impl Into<String>,
        feature_ids: Vec<String>,
        now: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        if feature_ids.is_empty() {
            return Err(DomainError::EmptyWorkPackage);
        }
        Ok(Self {
            metadata: RecordMetadata::new(now),
            id: WorkPackageId::new(),
            goal_id: goal_id.into(),
            title: title.into(),
            feature_ids,
            depends_on: Vec::new(),
            priority: 0,
            state: WorkPackageState::Draft,
            integration_boundary: IntegrationBoundary::WorkPackage,
            produces_contracts: Vec::new(),
            consumes_contracts: Vec::new(),
            ready_at: None,
        })
    }

    pub fn transition(
        &mut self,
        next: WorkPackageState,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        if !self.state.can_transition_to(next) {
            return Err(DomainError::InvalidTransition {
                entity: "work_package",
                from: self.state.name(),
                to: next.name(),
            });
        }
        self.state = next;
        self.metadata.touch(now);
        self.ready_at = if next == WorkPackageState::Ready {
            Some(now)
        } else if self.ready_at.is_some() {
            self.ready_at
        } else {
            None
        };
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ClaimScope {
    Feature { feature_id: String },
    WorkPackage { work_package_id: WorkPackageId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimState {
    Active,
    Released,
    Completed,
    Blocked,
    Expired,
    Revoked,
}

impl ClaimState {
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Active)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Released => "released",
            Self::Completed => "completed",
            Self::Blocked => "blocked",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlapKind {
    Benign,
    ReconciliationRequired,
    SemanticConflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedOverlap {
    pub other_claim_id: ClaimId,
    pub kind: OverlapKind,
    pub paths: Vec<String>,
    pub rationale: String,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimExpansion {
    pub added_scope: ClaimScope,
    pub rationale: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimOutcomeKind {
    Released,
    Completed,
    Blocked,
    Expired,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimOutcome {
    pub kind: ClaimOutcomeKind,
    pub reason: String,
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub escalation_id: Option<EscalationId>,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    pub metadata: RecordMetadata,
    pub id: ClaimId,
    pub goal_id: String,
    pub scope: ClaimScope,
    pub owner: WorkerId,
    #[serde(default)]
    pub parent_claim_id: Option<ClaimId>,
    pub state: ClaimState,
    pub base_revision: String,
    pub lease_generation: u64,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub expansion_history: Vec<ClaimExpansion>,
    pub overlaps: Vec<ManagedOverlap>,
    #[serde(default)]
    pub produces_contracts: Vec<String>,
    #[serde(default)]
    pub consumes_contracts: Vec<ContractExpectation>,
    pub outcome: Option<ClaimOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationCommit {
    pub revision: String,
    pub subject: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationDiffSummary {
    pub files_changed: u64,
    pub insertions: u64,
    pub deletions: u64,
    pub changed_paths: Vec<String>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationContractChange {
    pub contract_id: ContractId,
    pub stable_key: String,
    pub previous_revision: u64,
    pub revision: u64,
    pub compatibility_notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationMigration {
    pub path: String,
    pub description: String,
    pub reversible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationValidationEvidence {
    pub command: String,
    pub succeeded: bool,
    pub evidence_refs: Vec<String>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationWorkspaceSnapshot {
    pub branch: String,
    pub staged_paths: Vec<String>,
    pub unstaged_paths: Vec<String>,
    pub untracked_paths: Vec<String>,
    pub conflicted_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationArtifact {
    pub metadata: RecordMetadata,
    pub id: IntegrationArtifactId,
    pub goal_id: String,
    pub claim_id: ClaimId,
    pub claim_generation: u64,
    pub worker_id: WorkerId,
    pub repository_id: String,
    pub base_revision: String,
    pub head_revision: String,
    pub commits: Vec<IntegrationCommit>,
    pub diff_summary: IntegrationDiffSummary,
    pub changed_contracts: Vec<IntegrationContractChange>,
    pub migrations: Vec<IntegrationMigration>,
    pub validations: Vec<IntegrationValidationEvidence>,
    pub evidence_refs: Vec<String>,
    pub known_risks: Vec<String>,
    pub workspace: IntegrationWorkspaceSnapshot,
}

impl IntegrationArtifact {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.goal_id.trim().is_empty()
            || self.repository_id.trim().is_empty()
            || self.base_revision.trim().is_empty()
            || self.head_revision.trim().is_empty()
            || self.claim_generation == 0
        {
            return Err(DomainError::InvalidIntegrationArtifact(
                "goal, repository, base revision, and head revision are required".into(),
            ));
        }
        if self.commits.len() > 1_000
            || self.diff_summary.changed_paths.len() > 10_000
            || self.changed_contracts.len() > 1_000
            || self.migrations.len() > 1_000
            || self.validations.len() > 1_000
            || self.evidence_refs.len() > 1_000
            || self.known_risks.len() > 1_000
        {
            return Err(DomainError::InvalidIntegrationArtifact(
                "artifact collections exceed durable bounds".into(),
            ));
        }
        if self.changed_contracts.iter().any(|change| {
            change.stable_key.trim().is_empty() || change.revision < change.previous_revision
        }) {
            return Err(DomainError::InvalidIntegrationArtifact(
                "contract changes require a stable key and monotonic revision".into(),
            ));
        }
        if self.migrations.iter().any(|migration| {
            migration.path.trim().is_empty() || migration.description.trim().is_empty()
        }) {
            return Err(DomainError::InvalidIntegrationArtifact(
                "migrations require a path and description".into(),
            ));
        }
        if self.validations.iter().any(|validation| {
            validation.command.trim().is_empty() || validation.evidence_refs.is_empty()
        }) {
            return Err(DomainError::InvalidIntegrationArtifact(
                "validation evidence requires a command and evidence reference".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationGateKind {
    Format,
    TypeCheck,
    Unit,
    Integration,
    Security,
    Policy,
    Repository,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationGateResult {
    pub gate_id: String,
    pub kind: ValidationGateKind,
    pub command: String,
    pub required: bool,
    pub succeeded: bool,
    pub exit_code: Option<i32>,
    pub stdout_excerpt: String,
    pub stderr_excerpt: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationValidationReport {
    pub metadata: RecordMetadata,
    pub id: ValidationReportId,
    pub artifact_id: IntegrationArtifactId,
    pub goal_id: String,
    pub worker_id: WorkerId,
    pub passed: bool,
    pub results: Vec<ValidationGateResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationStrategy {
    Rebase,
    Merge,
    CherryPick,
    Regenerate,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationState {
    Succeeded,
    Failed,
    ManualRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReconciliationRecord {
    pub metadata: RecordMetadata,
    pub id: ReconciliationId,
    pub artifact_id: IntegrationArtifactId,
    pub goal_id: String,
    pub worker_id: WorkerId,
    pub strategy: ReconciliationStrategy,
    pub state: ReconciliationState,
    pub target_revision: String,
    pub commit_revisions: Vec<String>,
    pub previous_head: String,
    pub resulting_head: String,
    pub backup_ref: String,
    pub command: String,
    pub stdout_excerpt: String,
    pub stderr_excerpt: String,
    pub abort_succeeded: Option<bool>,
    pub workspace_preserved: bool,
    pub manual_instructions: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationJobState {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl IntegrationJobState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationJobOutcome {
    pub summary: String,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationJob {
    pub metadata: RecordMetadata,
    pub id: IntegrationJobId,
    pub artifact_id: IntegrationArtifactId,
    pub goal_id: String,
    pub repository_id: String,
    pub priority: i32,
    pub state: IntegrationJobState,
    pub attempt: u32,
    pub enqueued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub outcome: Option<IntegrationJobOutcome>,
}

impl IntegrationJob {
    pub fn new(artifact: &IntegrationArtifact, priority: i32, now: DateTime<Utc>) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            id: IntegrationJobId::new(),
            artifact_id: artifact.id.clone(),
            goal_id: artifact.goal_id.clone(),
            repository_id: artifact.repository_id.clone(),
            priority,
            state: IntegrationJobState::Queued,
            attempt: 0,
            enqueued_at: now,
            started_at: None,
            completed_at: None,
            outcome: None,
        }
    }

    pub fn start(&mut self, now: DateTime<Utc>) -> Result<(), DomainError> {
        if self.state != IntegrationJobState::Queued {
            return Err(DomainError::InvalidTransition {
                entity: "integration_job",
                from: integration_job_state_name(self.state),
                to: "running",
            });
        }
        self.state = IntegrationJobState::Running;
        self.attempt += 1;
        self.started_at = Some(now);
        self.metadata.touch(now);
        Ok(())
    }

    pub fn finish(
        &mut self,
        succeeded: bool,
        summary: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        if self.state != IntegrationJobState::Running {
            return Err(DomainError::InvalidTransition {
                entity: "integration_job",
                from: integration_job_state_name(self.state),
                to: if succeeded { "succeeded" } else { "failed" },
            });
        }
        let summary = summary.into();
        if summary.trim().is_empty() || summary.chars().count() > 4_000 {
            return Err(DomainError::InvalidIntegrationArtifact(
                "integration job outcome must be bounded and non-empty".into(),
            ));
        }
        self.state = if succeeded {
            IntegrationJobState::Succeeded
        } else {
            IntegrationJobState::Failed
        };
        self.completed_at = Some(now);
        self.outcome = Some(IntegrationJobOutcome { summary, at: now });
        self.metadata.touch(now);
        Ok(())
    }

    pub fn recover_after_restart(&mut self, now: DateTime<Utc>) -> Result<(), DomainError> {
        if self.state != IntegrationJobState::Running || now < self.metadata.updated_at {
            return Err(DomainError::InvalidTransition {
                entity: "integration_job",
                from: integration_job_state_name(self.state),
                to: "queued",
            });
        }
        self.state = IntegrationJobState::Queued;
        self.started_at = None;
        self.completed_at = None;
        self.outcome = None;
        self.metadata.touch(now);
        Ok(())
    }
}

fn integration_job_state_name(state: IntegrationJobState) -> &'static str {
    match state {
        IntegrationJobState::Queued => "queued",
        IntegrationJobState::Running => "running",
        IntegrationJobState::Succeeded => "succeeded",
        IntegrationJobState::Failed => "failed",
        IntegrationJobState::Cancelled => "cancelled",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationFinalizationState {
    Prepared,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationFinalization {
    pub metadata: RecordMetadata,
    pub id: IntegrationFinalizationId,
    pub job_id: IntegrationJobId,
    pub artifact_id: IntegrationArtifactId,
    pub validation_report_id: ValidationReportId,
    pub goal_id: String,
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub tracker_feature_id: String,
    pub tracker_step_ids: Vec<String>,
    pub tracker_summary: String,
    pub tracker_evidence: Vec<String>,
    pub tracker_status: String,
    pub integration_revision: String,
    pub completion_event_id: EventId,
    pub state: IntegrationFinalizationState,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationMaintenanceKind {
    Rollback,
    Cleanup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationMaintenanceState {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationMaintenanceRecord {
    pub metadata: RecordMetadata,
    pub id: IntegrationMaintenanceId,
    pub finalization_id: IntegrationFinalizationId,
    pub artifact_id: IntegrationArtifactId,
    pub goal_id: String,
    pub worker_id: WorkerId,
    pub kind: IntegrationMaintenanceKind,
    pub state: IntegrationMaintenanceState,
    pub requested_by: String,
    pub reason: String,
    pub previous_repository_head: String,
    pub resulting_repository_head: String,
    pub backup_ref: Option<String>,
    pub retained_branch: String,
    pub cleanup_record_path: Option<String>,
    pub artifact_preserved: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSeverity {
    Debug,
    Info,
    Warning,
    Error,
    Critical,
}

impl EventSeverity {
    pub fn rank(self) -> u8 {
        match self {
            Self::Debug => 0,
            Self::Info => 1,
            Self::Warning => 2,
            Self::Error => 3,
            Self::Critical => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoordinationActor {
    System,
    User { user_id: String },
    Worker { worker_id: WorkerId },
    Supervisor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationEventKind {
    WorkerLifecycleChanged,
    ClaimLifecycleChanged,
    ContractChanged,
    DependencyChanged,
    FileOverlapDetected,
    SymbolOverlapDetected,
    MigrationOverlapDetected,
    MergeConflictPredicted,
    WorkerStalled,
    RepeatedValidationFailure,
    WatchdogActionApplied,
    ActivityPublished,
    ValidationCompleted,
    BlockerRaised,
    CoordinationRequested,
    CoordinationExchangeChanged,
    InterventionCreated,
    HumanDecisionRecorded,
    PrivilegedActionRecorded,
    IntegrationChanged,
    CompletionPublished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityCategory {
    Progress,
    ScopeChanged,
    Validation,
    Artifact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityEventPayload {
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub category: ActivityCategory,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimEventPayload {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub from_state: Option<ClaimState>,
    pub to_state: ClaimState,
    pub reason: String,
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractEventPayload {
    pub contract_id: ContractId,
    pub stable_key: String,
    pub previous_revision: u64,
    pub revision: u64,
    pub producer_worker_id: Option<WorkerId>,
    pub compatibility_notes: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyEventPayload {
    pub work_package_id: WorkPackageId,
    pub dependency_key: String,
    pub previous_revision: String,
    pub revision: String,
    #[serde(default)]
    pub affected_worker_ids: Vec<WorkerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractMismatchEventPayload {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub contract_id: ContractId,
    pub stable_key: String,
    pub expected_revision: u64,
    pub current_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileOverlapInput {
    pub path: String,
    pub left_status: String,
    pub right_status: String,
    pub migration: bool,
    pub dependency_file: bool,
    pub generated_artifact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileOverlapEventPayload {
    pub left_worker_id: WorkerId,
    pub left_claim_id: ClaimId,
    pub left_base_revision: String,
    pub left_head_revision: String,
    pub right_worker_id: WorkerId,
    pub right_claim_id: ClaimId,
    pub right_base_revision: String,
    pub right_head_revision: String,
    pub paths: Vec<FileOverlapInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolOverlapInput {
    pub identity: String,
    pub match_kind: String,
    pub left_path: String,
    pub right_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolOverlapEventPayload {
    pub left_worker_id: WorkerId,
    pub left_claim_id: ClaimId,
    pub left_base_revision: String,
    pub left_head_revision: String,
    pub right_worker_id: WorkerId,
    pub right_claim_id: ClaimId,
    pub right_base_revision: String,
    pub right_head_revision: String,
    pub overlaps: Vec<SymbolOverlapInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationOverlapInput {
    pub classification: String,
    pub left_paths: Vec<String>,
    pub right_paths: Vec<String>,
    pub shared_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrationOverlapEventPayload {
    pub left_worker_id: WorkerId,
    pub left_claim_id: ClaimId,
    pub left_base_revision: String,
    pub left_head_revision: String,
    pub right_worker_id: WorkerId,
    pub right_claim_id: ClaimId,
    pub right_base_revision: String,
    pub right_head_revision: String,
    pub overlaps: Vec<MigrationOverlapInput>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeConflictEventPayload {
    pub left_worker_id: WorkerId,
    pub left_claim_id: ClaimId,
    pub left_head_revision: String,
    pub right_worker_id: WorkerId,
    pub right_claim_id: ClaimId,
    pub right_head_revision: String,
    pub merge_base_revision: String,
    pub conflicting_paths: Vec<String>,
    pub diagnostics: String,
    pub diagnostics_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStalledEventPayload {
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub last_heartbeat_at: DateTime<Utc>,
    pub last_activity_at: Option<DateTime<Utc>>,
    pub detected_at: DateTime<Utc>,
    pub heartbeat_age_seconds: u64,
    pub activity_age_seconds: Option<u64>,
    pub heartbeat_threshold_seconds: u64,
    pub activity_threshold_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RepeatedValidationFailureEventPayload {
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub failure_event_ids: Vec<EventId>,
    pub last_failure_at: DateTime<Utc>,
    pub window_seconds: u64,
    pub threshold: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogActionKind {
    EnvironmentRecovery,
    PermissionViolation,
    ExecutionLimit,
    ResourceQuota,
    FailureLoopCutoff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchdogActionEventPayload {
    pub worker_id: WorkerId,
    pub claim_ids: Vec<ClaimId>,
    pub action: WatchdogActionKind,
    pub reason: String,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockerEventPayload {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub summary: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub escalation_id: Option<EscalationId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationRequestEventPayload {
    pub claim_id: ClaimId,
    pub requester_worker_id: WorkerId,
    pub target_scope: String,
    pub desired_outcome: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HumanDecisionEventPayload {
    pub escalation_id: EscalationId,
    pub decided_by: String,
    pub option_id: String,
    #[serde(default)]
    pub action: RemediationAction,
    pub accepted_risk: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivilegedActionEventPayload {
    pub actor: String,
    pub authority: String,
    pub action: String,
    pub target: String,
    pub decision: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionEventPayload {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub artifact_id: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub integration_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum CoordinationEventPayload {
    Activity(ActivityEventPayload),
    Claim(ClaimEventPayload),
    Contract(ContractEventPayload),
    Dependency(DependencyEventPayload),
    ContractMismatch(ContractMismatchEventPayload),
    FileOverlap(FileOverlapEventPayload),
    SymbolOverlap(SymbolOverlapEventPayload),
    MigrationOverlap(MigrationOverlapEventPayload),
    MergeConflict(MergeConflictEventPayload),
    WorkerStalled(WorkerStalledEventPayload),
    RepeatedValidationFailure(RepeatedValidationFailureEventPayload),
    WatchdogAction(WatchdogActionEventPayload),
    Blocker(BlockerEventPayload),
    CoordinationRequest(CoordinationRequestEventPayload),
    HumanDecision(HumanDecisionEventPayload),
    PrivilegedAction(PrivilegedActionEventPayload),
    Completion(CompletionEventPayload),
}

impl CoordinationEventPayload {
    pub fn schema_key(&self) -> &'static str {
        match self {
            Self::Activity(_) => "coordination.activity.v1",
            Self::Claim(_) => "coordination.claim.v1",
            Self::Contract(_) => "coordination.contract.v1",
            Self::Dependency(_) => "coordination.dependency.v1",
            Self::ContractMismatch(_) => "coordination.contract_mismatch.v1",
            Self::FileOverlap(_) => "coordination.file_overlap.v1",
            Self::SymbolOverlap(_) => "coordination.symbol_overlap.v1",
            Self::MigrationOverlap(_) => "coordination.migration_overlap.v1",
            Self::MergeConflict(_) => "coordination.merge_conflict.v1",
            Self::WorkerStalled(_) => "coordination.worker_stalled.v1",
            Self::RepeatedValidationFailure(_) => "coordination.repeated_validation_failure.v1",
            Self::WatchdogAction(_) => "coordination.watchdog_action.v1",
            Self::Blocker(_) => "coordination.blocker.v1",
            Self::CoordinationRequest(_) => "coordination.request.v1",
            Self::HumanDecision(_) => "coordination.human_decision.v1",
            Self::PrivilegedAction(_) => "coordination.privileged_action.v1",
            Self::Completion(_) => "coordination.completion.v1",
        }
    }

    pub fn event_kind(&self) -> CoordinationEventKind {
        match self {
            Self::Activity(payload) => match payload.category {
                ActivityCategory::Validation => CoordinationEventKind::ValidationCompleted,
                _ => CoordinationEventKind::ActivityPublished,
            },
            Self::Claim(_) => CoordinationEventKind::ClaimLifecycleChanged,
            Self::Contract(_) => CoordinationEventKind::ContractChanged,
            Self::Dependency(_) => CoordinationEventKind::DependencyChanged,
            Self::ContractMismatch(_) => CoordinationEventKind::DependencyChanged,
            Self::FileOverlap(_) => CoordinationEventKind::FileOverlapDetected,
            Self::SymbolOverlap(_) => CoordinationEventKind::SymbolOverlapDetected,
            Self::MigrationOverlap(_) => CoordinationEventKind::MigrationOverlapDetected,
            Self::MergeConflict(_) => CoordinationEventKind::MergeConflictPredicted,
            Self::WorkerStalled(_) => CoordinationEventKind::WorkerStalled,
            Self::RepeatedValidationFailure(_) => CoordinationEventKind::RepeatedValidationFailure,
            Self::WatchdogAction(_) => CoordinationEventKind::WatchdogActionApplied,
            Self::Blocker(_) => CoordinationEventKind::BlockerRaised,
            Self::CoordinationRequest(_) => CoordinationEventKind::CoordinationRequested,
            Self::HumanDecision(_) => CoordinationEventKind::HumanDecisionRecorded,
            Self::PrivilegedAction(_) => CoordinationEventKind::PrivilegedActionRecorded,
            Self::Completion(_) => CoordinationEventKind::CompletionPublished,
        }
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        let (summary, evidence_len): (&str, usize) = match self {
            Self::Activity(payload) => {
                if payload.progress_percent.is_some_and(|value| value > 100) {
                    return Err(DomainError::InvalidEventPayload(
                        "activity progress must be between 0 and 100".into(),
                    ));
                }
                if payload.category != ActivityCategory::Validation
                    && payload.validation_succeeded.is_some()
                {
                    return Err(DomainError::InvalidEventPayload(
                        "validation outcome is only valid for validation activity".into(),
                    ));
                }
                (&payload.summary, payload.evidence_refs.len())
            }
            Self::Claim(payload) => (&payload.reason, payload.evidence_refs.len()),
            Self::Contract(payload) => {
                if payload.revision <= payload.previous_revision {
                    return Err(DomainError::InvalidEventPayload(
                        "contract revision must increase".into(),
                    ));
                }
                (&payload.compatibility_notes, 0)
            }
            Self::Dependency(payload) => {
                if payload.dependency_key.trim().is_empty()
                    || payload.revision == payload.previous_revision
                {
                    return Err(DomainError::InvalidEventPayload(
                        "dependency key and changed revision are required".into(),
                    ));
                }
                (&payload.dependency_key, 0)
            }
            Self::ContractMismatch(payload) => {
                if payload.stable_key.trim().is_empty()
                    || payload.expected_revision == 0
                    || payload.current_revision == 0
                    || payload.expected_revision == payload.current_revision
                {
                    return Err(DomainError::InvalidEventPayload(
                        "contract mismatch requires a stable key and distinct positive revisions"
                            .into(),
                    ));
                }
                (&payload.stable_key, 0)
            }
            Self::FileOverlap(payload) => {
                if payload.left_worker_id == payload.right_worker_id
                    || payload.left_claim_id == payload.right_claim_id
                    || payload.left_base_revision.trim().is_empty()
                    || payload.left_head_revision.trim().is_empty()
                    || payload.right_base_revision.trim().is_empty()
                    || payload.right_head_revision.trim().is_empty()
                    || payload.paths.is_empty()
                    || payload.paths.len() > 512
                    || payload.paths.iter().any(|path| {
                        path.path.trim().is_empty()
                            || path.left_status.trim().is_empty()
                            || path.right_status.trim().is_empty()
                    })
                {
                    return Err(DomainError::InvalidEventPayload(
                        "file overlap requires distinct workers and claims, revisions, and 1-512 classified paths"
                            .into(),
                    ));
                }
                (&payload.paths[0].path, 0)
            }
            Self::SymbolOverlap(payload) => {
                if payload.left_worker_id == payload.right_worker_id
                    || payload.left_claim_id == payload.right_claim_id
                    || payload.left_base_revision.trim().is_empty()
                    || payload.left_head_revision.trim().is_empty()
                    || payload.right_base_revision.trim().is_empty()
                    || payload.right_head_revision.trim().is_empty()
                    || payload.overlaps.is_empty()
                    || payload.overlaps.len() > 512
                    || payload.overlaps.iter().any(|overlap| {
                        overlap.identity.trim().is_empty()
                            || !matches!(overlap.match_kind.as_str(), "symbol" | "subsystem")
                            || overlap.left_path.trim().is_empty()
                            || overlap.right_path.trim().is_empty()
                    })
                {
                    return Err(DomainError::InvalidEventPayload(
                        "symbol overlap requires distinct workers and claims, revisions, and 1-512 symbol or subsystem matches"
                            .into(),
                    ));
                }
                (&payload.overlaps[0].identity, 0)
            }
            Self::MigrationOverlap(payload) => {
                if payload.left_worker_id == payload.right_worker_id
                    || payload.left_claim_id == payload.right_claim_id
                    || payload.left_base_revision.trim().is_empty()
                    || payload.left_head_revision.trim().is_empty()
                    || payload.right_base_revision.trim().is_empty()
                    || payload.right_head_revision.trim().is_empty()
                    || payload.overlaps.is_empty()
                    || payload.overlaps.len() > 32
                    || payload.overlaps.iter().any(|overlap| {
                        !matches!(
                            overlap.classification.as_str(),
                            "database" | "data" | "configuration" | "serialization"
                        ) || overlap.left_paths.is_empty()
                            || overlap.right_paths.is_empty()
                            || overlap.left_paths.len() > 512
                            || overlap.right_paths.len() > 512
                    })
                {
                    return Err(DomainError::InvalidEventPayload(
                        "migration overlap requires distinct workers and claims, revisions, and bounded classified paths"
                            .into(),
                    ));
                }
                (&payload.overlaps[0].classification, 0)
            }
            Self::MergeConflict(payload) => {
                if payload.left_worker_id == payload.right_worker_id
                    || payload.left_claim_id == payload.right_claim_id
                    || payload.left_head_revision.trim().is_empty()
                    || payload.right_head_revision.trim().is_empty()
                    || payload.merge_base_revision.trim().is_empty()
                    || payload.conflicting_paths.is_empty()
                    || payload.conflicting_paths.len() > 512
                    || payload
                        .conflicting_paths
                        .iter()
                        .any(|path| path.trim().is_empty())
                    || payload.diagnostics.chars().count() > 16_384
                {
                    return Err(DomainError::InvalidEventPayload(
                        "merge conflict prediction requires distinct workers and claims, revisions, bounded paths, and bounded diagnostics"
                            .into(),
                    ));
                }
                (&payload.conflicting_paths[0], 0)
            }
            Self::WorkerStalled(payload) => {
                if payload.heartbeat_threshold_seconds == 0
                    || payload.activity_threshold_seconds == 0
                    || payload.heartbeat_age_seconds < payload.heartbeat_threshold_seconds
                    || payload
                        .activity_age_seconds
                        .is_some_and(|age| age < payload.activity_threshold_seconds)
                {
                    return Err(DomainError::InvalidEventPayload(
                        "worker stall evidence must exceed positive heartbeat and activity thresholds"
                            .into(),
                    ));
                }
                (payload.claim_id.as_str(), 0)
            }
            Self::RepeatedValidationFailure(payload) => {
                if payload.threshold < 2
                    || payload.window_seconds == 0
                    || payload.failure_event_ids.len() < payload.threshold
                    || payload.failure_event_ids.len() > 100
                {
                    return Err(DomainError::InvalidEventPayload(
                        "repeated validation failure requires a bounded threshold and evidence window"
                            .into(),
                    ));
                }
                (payload.claim_id.as_str(), payload.failure_event_ids.len())
            }
            Self::WatchdogAction(payload) => {
                if payload.reason.trim().is_empty()
                    || payload.reason.chars().count() > 2_000
                    || payload.claim_ids.len() > 32
                {
                    return Err(DomainError::InvalidEventPayload(
                        "watchdog actions require a bounded reason and claim scope".into(),
                    ));
                }
                (&payload.reason, 0)
            }
            Self::Blocker(payload) => (&payload.summary, payload.evidence_refs.len()),
            Self::CoordinationRequest(payload) => {
                let valid_scope =
                    payload
                        .target_scope
                        .split_once(':')
                        .is_some_and(|(kind, identity)| {
                            matches!(kind, "worker" | "claim" | "package" | "contract")
                                && !identity.trim().is_empty()
                                && identity.chars().count() <= 200
                                && identity == identity.trim()
                        });
                if !valid_scope || payload.target_scope.chars().count() > 220 {
                    return Err(DomainError::InvalidEventPayload(
                        "coordination request target scope must be worker, claim, package, or contract plus a bounded identity"
                            .into(),
                    ));
                }
                (&payload.desired_outcome, payload.evidence_refs.len())
            }
            Self::HumanDecision(payload) => {
                if payload.decided_by.trim().is_empty() {
                    return Err(DomainError::InvalidEventPayload(
                        "human decision author is required".into(),
                    ));
                }
                (&payload.option_id, 0)
            }
            Self::PrivilegedAction(payload) => {
                if payload.actor.trim().is_empty()
                    || payload.authority.trim().is_empty()
                    || payload.action.trim().is_empty()
                    || payload.target.trim().is_empty()
                    || payload.outcome.trim().is_empty()
                    || payload.actor.chars().count() > 200
                    || payload.authority.chars().count() > 200
                    || payload.action.chars().count() > 200
                    || payload.target.chars().count() > 500
                    || payload.outcome.chars().count() > 2_000
                {
                    return Err(DomainError::InvalidEventPayload(
                        "privileged action audit requires bounded actor, authority, action, target, decision, evidence, and outcome"
                            .into(),
                    ));
                }
                (&payload.decision, payload.evidence_refs.len())
            }
            Self::Completion(payload) => {
                if payload.artifact_id.trim().is_empty()
                    || payload.integration_revision.trim().is_empty()
                {
                    return Err(DomainError::InvalidEventPayload(
                        "completion artifact and integration revision are required".into(),
                    ));
                }
                (&payload.artifact_id, payload.evidence_refs.len())
            }
        };
        if summary.trim().is_empty() || summary.chars().count() > 2_000 {
            return Err(DomainError::InvalidEventPayload(
                "required event text must contain 1-2000 characters".into(),
            ));
        }
        if evidence_len > 32 {
            return Err(DomainError::InvalidEventPayload(
                "event evidence is limited to 32 references".into(),
            ));
        }
        Ok(())
    }
}

fn legacy_payload_schema() -> String {
    "coordination.legacy_untyped.v1".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationEvent {
    pub metadata: RecordMetadata,
    pub id: EventId,
    pub goal_id: String,
    pub sequence: Option<u64>,
    pub kind: CoordinationEventKind,
    pub severity: EventSeverity,
    pub producer: CoordinationActor,
    pub causation_id: Option<EventId>,
    pub correlation_id: String,
    pub payload_version: u16,
    #[serde(default = "legacy_payload_schema")]
    pub payload_schema: String,
    pub payload: serde_json::Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventReplayCursor {
    pub consumer_id: String,
    pub goal_id: String,
    pub acknowledged_sequence: u64,
    pub last_delivered_sequence: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventReplayBatch {
    pub cursor: EventReplayCursor,
    pub events: Vec<CoordinationEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventRetentionPolicy {
    pub hot_retention_seconds: u64,
    pub retain_latest: usize,
    pub max_compaction_batch: usize,
}

impl Default for EventRetentionPolicy {
    fn default() -> Self {
        Self {
            hot_retention_seconds: 30 * 24 * 60 * 60,
            retain_latest: 1_000,
            max_compaction_batch: 500,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArchivedCoordinationEvent {
    pub original_sequence: u64,
    pub archived_at: DateTime<Utc>,
    pub artifact_refs: Vec<String>,
    pub event: CoordinationEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventCompactionReport {
    pub goal_id: String,
    pub acknowledged_through: u64,
    pub compacted_sequences: Vec<u64>,
}

impl CoordinationEvent {
    pub fn new(
        goal_id: impl Into<String>,
        kind: CoordinationEventKind,
        severity: EventSeverity,
        producer: CoordinationActor,
        correlation_id: impl Into<String>,
        payload: serde_json::Value,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            id: EventId::new(),
            goal_id: goal_id.into(),
            sequence: None,
            kind,
            severity,
            producer,
            causation_id: None,
            correlation_id: correlation_id.into(),
            payload_version: 1,
            payload_schema: legacy_payload_schema(),
            payload,
            occurred_at: now,
        }
    }

    pub fn from_typed_payload(
        goal_id: impl Into<String>,
        severity: EventSeverity,
        producer: CoordinationActor,
        correlation_id: impl Into<String>,
        payload: CoordinationEventPayload,
        now: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        payload.validate()?;
        let kind = payload.event_kind();
        let payload_schema = payload.schema_key().to_string();
        let payload = serde_json::to_value(payload)
            .map_err(|error| DomainError::InvalidEventPayload(error.to_string()))?;
        Ok(Self {
            metadata: RecordMetadata::new(now),
            id: EventId::new(),
            goal_id: goal_id.into(),
            sequence: None,
            kind,
            severity,
            producer,
            causation_id: None,
            correlation_id: correlation_id.into(),
            payload_version: 1,
            payload_schema,
            payload,
            occurred_at: now,
        })
    }

    pub fn typed_payload(&self) -> Result<Option<CoordinationEventPayload>, DomainError> {
        if self.payload_schema == legacy_payload_schema()
            || self.payload_schema == "coordination.compacted.v1"
        {
            return Ok(None);
        }
        let payload: CoordinationEventPayload = serde_json::from_value(self.payload.clone())
            .map_err(|error| DomainError::InvalidEventPayload(error.to_string()))?;
        if payload.schema_key() != self.payload_schema
            || payload.event_kind() != self.kind
            || self.payload_version != 1
        {
            return Err(DomainError::InvalidEventPayload(
                "event kind, schema, or version does not match its typed payload".into(),
            ));
        }
        payload.validate()?;
        Ok(Some(payload))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalState {
    Active,
    Expired,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    FileOverlap,
    SymbolOverlap,
    DependencyChanged,
    ContractMismatch,
    MigrationOverlap,
    MergeConflict,
    WorkerStalled,
    RepeatedValidationFailure,
    LeaseExpired,
    WatchdogAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationSignal {
    pub metadata: RecordMetadata,
    pub id: SignalId,
    pub goal_id: String,
    pub correlation_key: String,
    pub state: SignalState,
    pub kinds: Vec<SignalKind>,
    pub primary_kind: SignalKind,
    pub confidence_percent: u8,
    pub severity: EventSeverity,
    pub source_event_ids: Vec<EventId>,
    pub affected_worker_ids: Vec<WorkerId>,
    pub affected_claim_ids: Vec<ClaimId>,
    pub occurrence_count: u64,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub superseded_by: Option<SignalId>,
}

impl CoordinationSignal {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        goal_id: impl Into<String>,
        correlation_key: impl Into<String>,
        kind: SignalKind,
        confidence_percent: u8,
        severity: EventSeverity,
        source_event_id: EventId,
        affected_worker_ids: Vec<WorkerId>,
        affected_claim_ids: Vec<ClaimId>,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        let goal_id = goal_id.into();
        let correlation_key = correlation_key.into();
        if goal_id.trim().is_empty()
            || correlation_key.trim().is_empty()
            || correlation_key.chars().count() > 512
            || confidence_percent > 100
            || expires_at <= now
        {
            return Err(DomainError::InvalidSignal(
                "signals require a goal, bounded correlation key, confidence from 0 to 100, and a future expiry"
                    .into(),
            ));
        }
        let mut signal = Self {
            metadata: RecordMetadata::new(now),
            id: SignalId::new(),
            goal_id,
            correlation_key,
            state: SignalState::Active,
            kinds: vec![kind],
            primary_kind: kind,
            confidence_percent,
            severity,
            source_event_ids: vec![source_event_id],
            affected_worker_ids,
            affected_claim_ids,
            occurrence_count: 1,
            first_seen_at: now,
            last_seen_at: now,
            expires_at,
            superseded_by: None,
        };
        signal.canonicalize();
        Ok(signal)
    }

    pub fn merge_observation(
        &mut self,
        observation: &Self,
        now: DateTime<Utc>,
    ) -> Result<bool, DomainError> {
        if self.state != SignalState::Active
            || observation.state != SignalState::Active
            || self.goal_id != observation.goal_id
            || self.correlation_key != observation.correlation_key
        {
            return Err(DomainError::InvalidSignal(
                "only active observations with the same goal and correlation key can merge".into(),
            ));
        }
        let mut changed = false;
        for event_id in &observation.source_event_ids {
            if !self.source_event_ids.contains(event_id) {
                self.source_event_ids.push(event_id.clone());
                changed = true;
            }
        }
        if !changed {
            return Ok(false);
        }
        for kind in &observation.kinds {
            if !self.kinds.contains(kind) {
                self.kinds.push(*kind);
            }
        }
        self.affected_worker_ids
            .extend(observation.affected_worker_ids.iter().cloned());
        self.affected_claim_ids
            .extend(observation.affected_claim_ids.iter().cloned());
        if observation.confidence_percent > self.confidence_percent
            || (observation.confidence_percent == self.confidence_percent
                && signal_kind_rank(observation.primary_kind) > signal_kind_rank(self.primary_kind))
        {
            self.primary_kind = observation.primary_kind;
        }
        self.confidence_percent = self.confidence_percent.max(observation.confidence_percent);
        if observation.severity.rank() > self.severity.rank() {
            self.severity = observation.severity;
        }
        self.last_seen_at = self.last_seen_at.max(observation.last_seen_at);
        self.expires_at = self.expires_at.max(observation.expires_at);
        self.metadata.touch(now);
        self.canonicalize();
        self.occurrence_count = self.source_event_ids.len() as u64;
        Ok(true)
    }

    pub fn expire(&mut self, now: DateTime<Utc>) -> bool {
        if self.state != SignalState::Active || self.expires_at > now {
            return false;
        }
        self.expire_obsolete(now)
    }

    pub fn expire_obsolete(&mut self, now: DateTime<Utc>) -> bool {
        if self.state != SignalState::Active {
            return false;
        }
        self.state = SignalState::Expired;
        self.metadata.touch(now);
        true
    }

    fn canonicalize(&mut self) {
        self.kinds.sort_by_key(|kind| signal_kind_rank(*kind));
        self.kinds.dedup();
        self.affected_worker_ids
            .sort_by(|left, right| left.as_str().cmp(right.as_str()));
        self.affected_worker_ids.dedup();
        self.affected_claim_ids
            .sort_by(|left, right| left.as_str().cmp(right.as_str()));
        self.affected_claim_ids.dedup();
        self.source_event_ids
            .sort_by(|left, right| left.as_str().cmp(right.as_str()));
        self.source_event_ids.dedup();
    }
}

fn signal_kind_rank(kind: SignalKind) -> u8 {
    match kind {
        SignalKind::FileOverlap => 0,
        SignalKind::SymbolOverlap => 1,
        SignalKind::WorkerStalled => 2,
        SignalKind::DependencyChanged => 3,
        SignalKind::ContractMismatch => 4,
        SignalKind::RepeatedValidationFailure => 5,
        SignalKind::MigrationOverlap => 6,
        SignalKind::LeaseExpired => 7,
        SignalKind::WatchdogAction => 8,
        SignalKind::MergeConflict => 9,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionLevel {
    Observe,
    Inform,
    Recommend,
    Coordinate,
    Block,
    Escalate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterventionState {
    Proposed,
    Delivered,
    Acknowledged,
    Applied,
    Dismissed,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorBudgetLedger {
    pub metadata: RecordMetadata,
    pub goal_id: String,
    pub window_started_at: DateTime<Utc>,
    pub evaluation_count: u32,
    pub consumed_tokens: u64,
    pub consecutive_failures: u32,
    pub circuit_open_until: Option<DateTime<Utc>>,
    pub attempts_by_trigger: BTreeMap<String, u32>,
    #[serde(default)]
    pub completed_trigger_keys: Vec<String>,
}

impl SupervisorBudgetLedger {
    pub fn new(goal_id: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            goal_id: goal_id.into(),
            window_started_at: now,
            evaluation_count: 0,
            consumed_tokens: 0,
            consecutive_failures: 0,
            circuit_open_until: None,
            attempts_by_trigger: BTreeMap::new(),
            completed_trigger_keys: Vec::new(),
        }
    }

    pub fn touch(&mut self, now: DateTime<Utc>) {
        self.metadata.touch(now);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterventionAuditEntry {
    pub state: InterventionState,
    pub at: DateTime<Utc>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorIntervention {
    pub metadata: RecordMetadata,
    pub id: InterventionId,
    pub goal_id: String,
    pub level: InterventionLevel,
    pub state: InterventionState,
    pub signal_event_ids: Vec<EventId>,
    pub evidence_refs: Vec<String>,
    pub target_workers: Vec<WorkerId>,
    pub recommendation: String,
    pub requested_action: Option<String>,
    pub outcome: Option<String>,
    pub supersedes: Option<InterventionId>,
    #[serde(default)]
    pub supersession_history: Vec<InterventionId>,
    #[serde(default)]
    pub trigger_key: Option<String>,
    #[serde(default)]
    pub reasoning_input_json: Option<String>,
    #[serde(default)]
    pub decision_summary: String,
    #[serde(default)]
    pub confidence_percent: Option<u8>,
    #[serde(default)]
    pub audit_history: Vec<InterventionAuditEntry>,
}

impl SupervisorIntervention {
    pub fn new(
        goal_id: impl Into<String>,
        level: InterventionLevel,
        signal_event_ids: Vec<EventId>,
        target_workers: Vec<WorkerId>,
        recommendation: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            id: InterventionId::new(),
            goal_id: goal_id.into(),
            level,
            state: InterventionState::Proposed,
            signal_event_ids,
            evidence_refs: Vec::new(),
            target_workers,
            recommendation: recommendation.into(),
            requested_action: None,
            outcome: None,
            supersedes: None,
            supersession_history: Vec::new(),
            trigger_key: None,
            reasoning_input_json: None,
            decision_summary: String::new(),
            confidence_percent: None,
            audit_history: vec![InterventionAuditEntry {
                state: InterventionState::Proposed,
                at: now,
                outcome: None,
            }],
        }
    }

    pub fn transition(
        &mut self,
        next: InterventionState,
        now: DateTime<Utc>,
        outcome: Option<String>,
    ) -> Result<(), DomainError> {
        use InterventionState::*;
        let allowed = matches!(
            (self.state, next),
            (Proposed, Delivered | Applied | Dismissed | Superseded)
                | (Delivered, Acknowledged | Applied | Dismissed | Superseded)
                | (Acknowledged, Applied | Dismissed | Superseded)
        );
        if !allowed {
            return Err(DomainError::InvalidTransition {
                entity: "intervention",
                from: intervention_state_name(self.state),
                to: intervention_state_name(next),
            });
        }
        self.state = next;
        self.outcome = outcome.clone();
        self.audit_history.push(InterventionAuditEntry {
            state: next,
            at: now,
            outcome,
        });
        self.metadata.touch(now);
        Ok(())
    }
}

fn intervention_state_name(state: InterventionState) -> &'static str {
    match state {
        InterventionState::Proposed => "proposed",
        InterventionState::Delivered => "delivered",
        InterventionState::Acknowledged => "acknowledged",
        InterventionState::Applied => "applied",
        InterventionState::Dismissed => "dismissed",
        InterventionState::Superseded => "superseded",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationKind {
    HardBlocker,
    DecisionPoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationContinuationMode {
    ScopeBlocked,
    ContinueAtAcceptedRisk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationImpact {
    pub continuation_mode: EscalationContinuationMode,
    pub blocks_affected_scope: bool,
    pub may_continue_before_resolution: bool,
    pub accepted_risk_required_for_override: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationSeverity {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EscalationScope {
    Claim { claim_id: ClaimId },
    WorkPackage { work_package_id: WorkPackageId },
    Contract { contract_id: ContractId },
    IntegrationLane { repository_id: String },
    Goal { goal_id: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationState {
    Open,
    Acknowledged,
    Resolved,
    Overridden,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemediationAction {
    ReconcileNow,
    Wait,
    SplitPackage,
    PauseScope,
    CancelScope,
    ChooseDirection,
    OverrideRisk,
}

impl Default for RemediationAction {
    fn default() -> Self {
        Self::ChooseDirection
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemediationOption {
    pub id: String,
    #[serde(default)]
    pub action: RemediationAction,
    pub label: String,
    pub description: String,
    pub consequences: Vec<String>,
    pub recommended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationDecision {
    pub option_id: String,
    pub decided_by: String,
    pub accepted_risk: Option<String>,
    pub decided_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationOutcomeKind {
    DecisionApplied,
    Waiting,
    ScopePaused,
    ScopeCancelled,
    WorkResumed,
    WorkerReplaced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationDownstreamOutcome {
    pub kind: EscalationOutcomeKind,
    pub recorded_at: DateTime<Utc>,
    pub summary: String,
    #[serde(default)]
    pub claim_ids: Vec<ClaimId>,
    #[serde(default)]
    pub worker_ids: Vec<WorkerId>,
    #[serde(default)]
    pub work_package_ids: Vec<WorkPackageId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationDecisionAudit {
    pub decided_by: String,
    pub decided_at: DateTime<Utc>,
    pub selected_option: RemediationOption,
    #[serde(default)]
    pub alternatives: Vec<RemediationOption>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub accepted_risk: Option<String>,
    #[serde(default)]
    pub downstream_outcomes: Vec<EscalationDownstreamOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationAuditEntry {
    pub state: EscalationState,
    pub at: DateTime<Utc>,
    pub actor: String,
    pub reason: Option<String>,
    pub decision: Option<EscalationDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationClaimRevision {
    pub claim_id: ClaimId,
    pub scope: ClaimScope,
    pub owner: WorkerId,
    pub state: ClaimState,
    pub base_revision: String,
    pub lease_generation: u64,
    pub produces_contracts: Vec<String>,
    pub consumes_contracts: Vec<ContractExpectation>,
    pub outcome: Option<ClaimOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationWorkerRevision {
    pub worker_id: WorkerId,
    pub state: WorkerState,
    pub active_claims: Vec<ClaimId>,
    pub current_run_id: Option<String>,
    pub current_thread_id: Option<String>,
    pub workspace_repository_id: Option<String>,
    pub workspace_branch: Option<String>,
    pub workspace_base_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationPackageRevision {
    pub work_package_id: WorkPackageId,
    pub state: WorkPackageState,
    pub feature_ids: Vec<String>,
    pub depends_on: Vec<WorkPackageId>,
    pub priority: i32,
    pub produces_contracts: Vec<String>,
    pub consumes_contracts: Vec<ContractExpectation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationContractRevision {
    pub contract_id: ContractId,
    pub stable_key: String,
    pub revision: u64,
    pub producer: Option<WorkPackageId>,
    pub dependents: Vec<ContractConsumer>,
    pub last_changed_by: Option<WorkerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationStalenessSnapshot {
    pub captured_at: DateTime<Utc>,
    pub scope: EscalationScope,
    pub evidence_refs: Vec<String>,
    pub options: Vec<RemediationOption>,
    pub claims: Vec<EscalationClaimRevision>,
    pub workers: Vec<EscalationWorkerRevision>,
    pub work_packages: Vec<EscalationPackageRevision>,
    pub contracts: Vec<EscalationContractRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HumanEscalation {
    pub metadata: RecordMetadata,
    pub id: EscalationId,
    pub goal_id: String,
    pub kind: EscalationKind,
    pub severity: EscalationSeverity,
    pub state: EscalationState,
    pub scope: EscalationScope,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub options: Vec<RemediationOption>,
    pub stale_after_revisions: Vec<String>,
    #[serde(default)]
    pub staleness_snapshot: Option<EscalationStalenessSnapshot>,
    pub decision: Option<EscalationDecision>,
    #[serde(default)]
    pub decision_audit: Option<EscalationDecisionAudit>,
    #[serde(default)]
    pub audit_history: Vec<EscalationAuditEntry>,
}

impl HumanEscalation {
    pub fn new(
        goal_id: impl Into<String>,
        kind: EscalationKind,
        severity: EscalationSeverity,
        scope: EscalationScope,
        summary: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            metadata: RecordMetadata::new(now),
            id: EscalationId::new(),
            goal_id: goal_id.into(),
            kind,
            severity,
            state: EscalationState::Open,
            scope,
            summary: summary.into(),
            evidence_refs: Vec::new(),
            options: Vec::new(),
            stale_after_revisions: Vec::new(),
            staleness_snapshot: None,
            decision: None,
            decision_audit: None,
            audit_history: vec![EscalationAuditEntry {
                state: EscalationState::Open,
                at: now,
                actor: "system".into(),
                reason: Some("escalation created".into()),
                decision: None,
            }],
        }
    }

    pub fn transition(
        &mut self,
        next: EscalationState,
        now: DateTime<Utc>,
        decision: Option<EscalationDecision>,
    ) -> Result<(), DomainError> {
        self.transition_attributed(next, now, "system", None, decision)
    }

    pub fn impact(&self) -> EscalationImpact {
        match self.kind {
            EscalationKind::HardBlocker => EscalationImpact {
                continuation_mode: EscalationContinuationMode::ScopeBlocked,
                blocks_affected_scope: true,
                may_continue_before_resolution: false,
                accepted_risk_required_for_override: true,
            },
            EscalationKind::DecisionPoint => EscalationImpact {
                continuation_mode: EscalationContinuationMode::ContinueAtAcceptedRisk,
                blocks_affected_scope: false,
                may_continue_before_resolution: true,
                accepted_risk_required_for_override: true,
            },
        }
    }

    pub fn transition_attributed(
        &mut self,
        next: EscalationState,
        now: DateTime<Utc>,
        actor: impl Into<String>,
        reason: Option<String>,
        decision: Option<EscalationDecision>,
    ) -> Result<(), DomainError> {
        use EscalationState::*;
        let actor = actor.into();
        if actor.trim().is_empty() || actor.chars().count() > 200 {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        if now < self.metadata.updated_at
            || reason
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 2_000)
        {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        let allowed = matches!(
            (self.state, next),
            (
                Open,
                Acknowledged | Resolved | Overridden | Expired | Cancelled
            ) | (Acknowledged, Resolved | Overridden | Expired | Cancelled)
        );
        if !allowed {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        if matches!(next, Resolved | Overridden) && decision.is_none() {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        if matches!(next, Acknowledged | Expired) && decision.is_some() {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        if let Some(decision) = &decision
            && (decision.option_id.trim().is_empty()
                || decision.option_id.chars().count() > 200
                || decision.decided_by.trim().is_empty()
                || decision.decided_by.chars().count() > 200
                || decision.decided_at != now
                || decision
                    .accepted_risk
                    .as_ref()
                    .is_some_and(|risk| risk.trim().is_empty() || risk.chars().count() > 2_000))
        {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        if next == Overridden
            && decision
                .as_ref()
                .and_then(|decision| decision.accepted_risk.as_deref())
                .is_none_or(|risk| risk.trim().is_empty())
        {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: escalation_state_name(next),
            });
        }
        self.state = next;
        self.decision = decision.clone();
        self.audit_history.push(EscalationAuditEntry {
            state: next,
            at: now,
            actor,
            reason,
            decision,
        });
        self.metadata.touch(now);
        Ok(())
    }

    pub fn record_acknowledged_decision(
        &mut self,
        now: DateTime<Utc>,
        actor: impl Into<String>,
        reason: Option<String>,
        decision: EscalationDecision,
    ) -> Result<(), DomainError> {
        let actor = actor.into();
        if !matches!(
            self.state,
            EscalationState::Open | EscalationState::Acknowledged
        ) || actor.trim().is_empty()
            || actor.chars().count() > 200
            || now < self.metadata.updated_at
            || decision.option_id.trim().is_empty()
            || decision.option_id.chars().count() > 200
            || decision.decided_by.trim().is_empty()
            || decision.decided_by.chars().count() > 200
            || decision.decided_by != actor
            || decision.decided_at != now
            || reason
                .as_ref()
                .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 2_000)
        {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: escalation_state_name(self.state),
                to: "acknowledged",
            });
        }
        self.state = EscalationState::Acknowledged;
        self.decision = Some(decision.clone());
        self.audit_history.push(EscalationAuditEntry {
            state: EscalationState::Acknowledged,
            at: now,
            actor,
            reason,
            decision: Some(decision),
        });
        self.metadata.touch(now);
        Ok(())
    }
}

fn escalation_state_name(state: EscalationState) -> &'static str {
    match state {
        EscalationState::Open => "open",
        EscalationState::Acknowledged => "acknowledged",
        EscalationState::Resolved => "resolved",
        EscalationState::Overridden => "overridden",
        EscalationState::Expired => "expired",
        EscalationState::Cancelled => "cancelled",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationState {
    Queued,
    Delivered,
    Acknowledged,
    ActedOn,
    Expired,
    Superseded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NotificationAuditEntry {
    pub state: NotificationState,
    pub at: DateTime<Utc>,
    pub actor: CoordinationActor,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerNotification {
    pub metadata: RecordMetadata,
    pub id: NotificationId,
    pub goal_id: String,
    pub target_worker: WorkerId,
    pub source_event_id: EventId,
    pub state: NotificationState,
    pub purpose: String,
    #[serde(default = "legacy_notification_summary")]
    pub summary: String,
    #[serde(default = "default_notification_severity")]
    pub severity: EventSeverity,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub recommended_action: Option<String>,
    pub required_acknowledgement: bool,
    pub expires_at: Option<DateTime<Utc>>,
    pub superseded_by: Option<NotificationId>,
    #[serde(default)]
    pub content_fingerprint: Option<String>,
    #[serde(default)]
    pub coalesced_source_event_ids: Vec<EventId>,
    #[serde(default)]
    pub delivery_attempts: u32,
    #[serde(default)]
    pub audit_history: Vec<NotificationAuditEntry>,
}

fn legacy_notification_summary() -> String {
    "Legacy notification".into()
}

fn default_notification_severity() -> EventSeverity {
    EventSeverity::Info
}

impl WorkerNotification {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        goal_id: impl Into<String>,
        target_worker: WorkerId,
        source_event_id: EventId,
        purpose: impl Into<String>,
        summary: impl Into<String>,
        severity: EventSeverity,
        evidence_refs: Vec<String>,
        recommended_action: Option<String>,
        required_acknowledgement: bool,
        expires_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        let notification = Self {
            metadata: RecordMetadata::new(now),
            id: NotificationId::new(),
            goal_id: goal_id.into(),
            target_worker,
            source_event_id,
            state: NotificationState::Queued,
            purpose: purpose.into(),
            summary: summary.into(),
            severity,
            evidence_refs,
            recommended_action,
            required_acknowledgement,
            expires_at,
            superseded_by: None,
            content_fingerprint: None,
            coalesced_source_event_ids: vec![],
            delivery_attempts: 0,
            audit_history: vec![NotificationAuditEntry {
                state: NotificationState::Queued,
                at: now,
                actor: CoordinationActor::System,
                detail: Some("notification queued".into()),
            }],
        };
        notification.validate()?;
        Ok(notification)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.goal_id.trim().is_empty()
            || self.goal_id.chars().count() > 200
            || self.purpose.trim().is_empty()
            || self.purpose.chars().count() > 100
            || self.summary.trim().is_empty()
            || self.summary.chars().count() > 2_000
            || self.evidence_refs.len() > 32
            || self
                .evidence_refs
                .iter()
                .any(|reference| reference.trim().is_empty() || reference.chars().count() > 1_000)
            || self
                .recommended_action
                .as_ref()
                .is_some_and(|action| action.trim().is_empty() || action.chars().count() > 2_000)
            || self
                .expires_at
                .is_some_and(|expires_at| expires_at <= self.metadata.created_at)
            || (self.state == NotificationState::Superseded && self.superseded_by.is_none())
            || (self.state != NotificationState::Superseded && self.superseded_by.is_some())
            || self
                .content_fingerprint
                .as_ref()
                .is_some_and(|fingerprint| {
                    fingerprint.trim().is_empty() || fingerprint.chars().count() > 100
                })
            || self.coalesced_source_event_ids.len() > 32
            || self.delivery_attempts > 10_000
            || self.audit_history.len() > 256
            || self.audit_history.iter().any(|entry| {
                entry.at < self.metadata.created_at
                    || entry.at > self.metadata.updated_at
                    || entry.detail.as_ref().is_some_and(|detail| {
                        detail.trim().is_empty() || detail.chars().count() > 2_000
                    })
            })
            || self
                .audit_history
                .windows(2)
                .any(|entries| entries[1].at < entries[0].at)
            || self.audit_history.first().is_some_and(|entry| {
                entry.state != NotificationState::Queued || entry.at != self.metadata.created_at
            })
            || self
                .audit_history
                .last()
                .is_some_and(|entry| entry.state != self.state)
        {
            return Err(DomainError::InvalidEventPayload(
                "notification requires bounded purpose, summary, evidence, action, expiry, attempts, audit, and consistent supersession"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn transition(
        &mut self,
        next: NotificationState,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        self.transition_attributed(next, CoordinationActor::System, None, now)
    }

    pub fn transition_attributed(
        &mut self,
        next: NotificationState,
        actor: CoordinationActor,
        detail: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        use NotificationState::*;
        let allowed = matches!(
            (self.state, next),
            (Queued, Delivered | Expired | Superseded | Failed)
                | (
                    Delivered,
                    Acknowledged | ActedOn | Expired | Superseded | Failed
                )
                | (Acknowledged, ActedOn | Expired | Superseded | Failed)
        );
        if !allowed || now < self.metadata.updated_at {
            return Err(DomainError::InvalidTransition {
                entity: "notification",
                from: notification_state_name(self.state),
                to: notification_state_name(next),
            });
        }
        if detail
            .as_ref()
            .is_some_and(|detail| detail.trim().is_empty() || detail.chars().count() > 2_000)
        {
            return Err(DomainError::InvalidEventPayload(
                "notification transition detail must be non-empty and at most 2,000 characters"
                    .into(),
            ));
        }
        if next == Delivered || (self.state == Queued && next == Failed) {
            self.delivery_attempts = self.delivery_attempts.saturating_add(1);
        }
        self.state = next;
        self.metadata.touch(now);
        self.audit_history.push(NotificationAuditEntry {
            state: next,
            at: now,
            actor,
            detail,
        });
        self.validate()
    }

    pub fn supersede(
        &mut self,
        replacement_id: NotificationId,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        self.supersede_attributed(replacement_id, CoordinationActor::System, None, now)
    }

    pub fn supersede_attributed(
        &mut self,
        replacement_id: NotificationId,
        actor: CoordinationActor,
        detail: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        self.superseded_by = Some(replacement_id);
        if let Err(error) =
            self.transition_attributed(NotificationState::Superseded, actor, detail, now)
        {
            self.superseded_by = None;
            return Err(error);
        }
        Ok(())
    }

    pub fn record_replay_attributed(
        &mut self,
        actor: CoordinationActor,
        detail: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        if !matches!(
            self.state,
            NotificationState::Delivered | NotificationState::Acknowledged
        ) || now < self.metadata.updated_at
            || detail
                .as_ref()
                .is_some_and(|detail| detail.trim().is_empty() || detail.chars().count() > 2_000)
        {
            return Err(DomainError::InvalidTransition {
                entity: "notification",
                from: notification_state_name(self.state),
                to: notification_state_name(self.state),
            });
        }
        self.delivery_attempts = self.delivery_attempts.saturating_add(1);
        self.metadata.touch(now);
        self.audit_history.push(NotificationAuditEntry {
            state: self.state,
            at: now,
            actor,
            detail,
        });
        self.validate()
    }
}

fn notification_state_name(state: NotificationState) -> &'static str {
    match state {
        NotificationState::Queued => "queued",
        NotificationState::Delivered => "delivered",
        NotificationState::Acknowledged => "acknowledged",
        NotificationState::ActedOn => "acted_on",
        NotificationState::Expired => "expired",
        NotificationState::Superseded => "superseded",
        NotificationState::Failed => "failed",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationExchangeState {
    Open,
    Completed,
    Expired,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationExchangeMessage {
    pub sequence: u8,
    pub author_worker_id: WorkerId,
    pub at: DateTime<Utc>,
    pub body: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationExchangeAuditEntry {
    pub state: CoordinationExchangeState,
    pub at: DateTime<Utc>,
    pub actor: CoordinationActor,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CoordinationExchange {
    pub metadata: RecordMetadata,
    pub id: ExchangeId,
    pub goal_id: String,
    pub request_event_id: EventId,
    pub target_scope: String,
    pub desired_outcome: String,
    pub requester_worker_id: WorkerId,
    pub target_worker_id: WorkerId,
    pub insufficiency_reason: String,
    pub state: CoordinationExchangeState,
    pub expires_at: DateTime<Utc>,
    pub max_messages: u8,
    #[serde(default)]
    pub messages: Vec<CoordinationExchangeMessage>,
    pub shared_note: Option<String>,
    #[serde(default)]
    pub audit_history: Vec<CoordinationExchangeAuditEntry>,
}

impl CoordinationExchange {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        goal_id: impl Into<String>,
        request_event_id: EventId,
        target_scope: impl Into<String>,
        desired_outcome: impl Into<String>,
        requester_worker_id: WorkerId,
        target_worker_id: WorkerId,
        insufficiency_reason: impl Into<String>,
        expires_at: DateTime<Utc>,
        max_messages: u8,
        now: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        let exchange = Self {
            metadata: RecordMetadata::new(now),
            id: ExchangeId::new(),
            goal_id: goal_id.into(),
            request_event_id,
            target_scope: target_scope.into(),
            desired_outcome: desired_outcome.into(),
            requester_worker_id: requester_worker_id.clone(),
            target_worker_id,
            insufficiency_reason: insufficiency_reason.into(),
            state: CoordinationExchangeState::Open,
            expires_at,
            max_messages,
            messages: vec![],
            shared_note: None,
            audit_history: vec![CoordinationExchangeAuditEntry {
                state: CoordinationExchangeState::Open,
                at: now,
                actor: CoordinationActor::Worker {
                    worker_id: requester_worker_id,
                },
                reason: "exceptional mediated exchange opened".into(),
            }],
        };
        exchange.validate()?;
        Ok(exchange)
    }

    pub fn validate(&self) -> Result<(), DomainError> {
        if self.goal_id.trim().is_empty()
            || self.goal_id.chars().count() > 200
            || self.requester_worker_id == self.target_worker_id
            || self.target_scope.trim().is_empty()
            || self.target_scope.chars().count() > 220
            || self.desired_outcome.trim().is_empty()
            || self.desired_outcome.chars().count() > 2_000
            || self.insufficiency_reason.trim().is_empty()
            || self.insufficiency_reason.chars().count() > 2_000
            || self.expires_at <= self.metadata.created_at
            || self.expires_at - self.metadata.created_at > chrono::Duration::minutes(15)
            || !(2..=6).contains(&self.max_messages)
            || self.messages.len() > self.max_messages as usize
            || self.messages.iter().enumerate().any(|(index, message)| {
                message.sequence as usize != index + 1
                    || !self.is_participant(&message.author_worker_id)
                    || message.at < self.metadata.created_at
                    || message.at > self.metadata.updated_at
                    || message.body.trim().is_empty()
                    || message.body.chars().count() > 2_000
                    || message.evidence_refs.len() > 16
                    || message.evidence_refs.iter().any(|reference| {
                        reference.trim().is_empty() || reference.chars().count() > 1_000
                    })
            })
            || self.messages.windows(2).any(|messages| {
                messages[0].author_worker_id == messages[1].author_worker_id
                    || messages[1].at < messages[0].at
            })
            || (self.state == CoordinationExchangeState::Completed
                && self
                    .shared_note
                    .as_ref()
                    .is_none_or(|note| note.trim().is_empty()))
            || (self.state != CoordinationExchangeState::Completed && self.shared_note.is_some())
            || self
                .shared_note
                .as_ref()
                .is_some_and(|note| note.chars().count() > 2_000)
            || self.audit_history.is_empty()
            || self.audit_history.len() > 16
            || self.audit_history.first().is_some_and(|entry| {
                entry.state != CoordinationExchangeState::Open
                    || entry.at != self.metadata.created_at
            })
            || self
                .audit_history
                .last()
                .is_some_and(|entry| entry.state != self.state)
            || self.audit_history.iter().any(|entry| {
                entry.at < self.metadata.created_at
                    || entry.at > self.metadata.updated_at
                    || entry.reason.trim().is_empty()
                    || entry.reason.chars().count() > 2_000
            })
        {
            return Err(DomainError::InvalidEventPayload(
                "coordination exchange must be short-lived, bounded, alternating, attributed, and observable"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn is_participant(&self, worker_id: &WorkerId) -> bool {
        self.requester_worker_id == *worker_id || self.target_worker_id == *worker_id
    }

    pub fn add_message(
        &mut self,
        author_worker_id: WorkerId,
        body: impl Into<String>,
        evidence_refs: Vec<String>,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        if self.state != CoordinationExchangeState::Open
            || now < self.metadata.updated_at
            || now >= self.expires_at
            || !self.is_participant(&author_worker_id)
            || self.messages.len() >= self.max_messages as usize
            || self
                .messages
                .last()
                .is_some_and(|message| message.author_worker_id == author_worker_id)
        {
            return Err(DomainError::InvalidTransition {
                entity: "coordination_exchange",
                from: coordination_exchange_state_name(self.state),
                to: "message_added",
            });
        }
        self.messages.push(CoordinationExchangeMessage {
            sequence: self.messages.len() as u8 + 1,
            author_worker_id,
            at: now,
            body: body.into(),
            evidence_refs,
        });
        self.metadata.touch(now);
        self.validate()
    }

    pub fn complete(
        &mut self,
        actor_worker_id: WorkerId,
        shared_note: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        if self.state != CoordinationExchangeState::Open
            || self.messages.is_empty()
            || !self.is_participant(&actor_worker_id)
            || now < self.metadata.updated_at
            || now >= self.expires_at
        {
            return Err(DomainError::InvalidTransition {
                entity: "coordination_exchange",
                from: coordination_exchange_state_name(self.state),
                to: "completed",
            });
        }
        self.state = CoordinationExchangeState::Completed;
        self.shared_note = Some(shared_note.into());
        self.metadata.touch(now);
        self.audit_history.push(CoordinationExchangeAuditEntry {
            state: CoordinationExchangeState::Completed,
            at: now,
            actor: CoordinationActor::Worker {
                worker_id: actor_worker_id,
            },
            reason: "shared coordination note recorded".into(),
        });
        self.validate()
    }

    pub fn expire(&mut self, now: DateTime<Utc>) -> Result<(), DomainError> {
        if self.state != CoordinationExchangeState::Open || now < self.expires_at {
            return Err(DomainError::InvalidTransition {
                entity: "coordination_exchange",
                from: coordination_exchange_state_name(self.state),
                to: "expired",
            });
        }
        self.state = CoordinationExchangeState::Expired;
        self.metadata.touch(now);
        self.audit_history.push(CoordinationExchangeAuditEntry {
            state: CoordinationExchangeState::Expired,
            at: now,
            actor: CoordinationActor::System,
            reason: "short-lived exchange deadline elapsed".into(),
        });
        self.validate()
    }
}

fn coordination_exchange_state_name(state: CoordinationExchangeState) -> &'static str {
    match state {
        CoordinationExchangeState::Open => "open",
        CoordinationExchangeState::Completed => "completed",
        CoordinationExchangeState::Expired => "expired",
        CoordinationExchangeState::Cancelled => "cancelled",
    }
}

impl Claim {
    pub fn new(
        goal_id: impl Into<String>,
        scope: ClaimScope,
        owner: WorkerId,
        base_revision: impl Into<String>,
        heartbeat_at: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Self, DomainError> {
        if lease_expires_at <= heartbeat_at {
            return Err(DomainError::InvalidLeaseWindow);
        }
        Ok(Self {
            metadata: RecordMetadata::new(heartbeat_at),
            id: ClaimId::new(),
            goal_id: goal_id.into(),
            scope,
            owner,
            parent_claim_id: None,
            state: ClaimState::Active,
            base_revision: base_revision.into(),
            lease_generation: 1,
            heartbeat_at,
            lease_expires_at,
            expansion_history: Vec::new(),
            overlaps: Vec::new(),
            produces_contracts: Vec::new(),
            consumes_contracts: Vec::new(),
            outcome: None,
        })
    }

    pub fn transition(
        &mut self,
        next: ClaimState,
        outcome: ClaimOutcome,
    ) -> Result<(), DomainError> {
        if self.state != ClaimState::Active || !next.is_terminal() {
            return Err(DomainError::InvalidTransition {
                entity: "claim",
                from: self.state.name(),
                to: next.name(),
            });
        }
        let outcome_matches = matches!(
            (next, &outcome.kind),
            (ClaimState::Released, ClaimOutcomeKind::Released)
                | (ClaimState::Completed, ClaimOutcomeKind::Completed)
                | (ClaimState::Blocked, ClaimOutcomeKind::Blocked)
                | (ClaimState::Expired, ClaimOutcomeKind::Expired)
                | (ClaimState::Revoked, ClaimOutcomeKind::Revoked)
        );
        if !outcome_matches {
            return Err(DomainError::MissingClaimOutcome);
        }
        self.state = next;
        self.metadata.touch(outcome.at);
        self.outcome = Some(outcome);
        Ok(())
    }

    pub fn renew(
        &mut self,
        expected_generation: u64,
        heartbeat_at: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<(), DomainError> {
        if self.state != ClaimState::Active {
            return Err(DomainError::InvalidTransition {
                entity: "claim",
                from: self.state.name(),
                to: "active",
            });
        }
        if expected_generation == 0 {
            return Err(DomainError::InvalidLeaseGeneration);
        }
        if self.lease_generation != expected_generation {
            return Err(DomainError::StaleLeaseGeneration {
                expected: self.lease_generation,
                received: expected_generation,
            });
        }
        if lease_expires_at <= heartbeat_at {
            return Err(DomainError::InvalidLeaseWindow);
        }
        self.heartbeat_at = heartbeat_at;
        self.lease_expires_at = lease_expires_at;
        self.metadata.touch(heartbeat_at);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn worker_identity_is_not_a_runtime_identity() {
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.current_thread_id = Some("thread-1".into());
        worker.current_run_id = Some("run-1".into());

        assert!(worker.id.as_str().starts_with("worker-"));
        assert_ne!(worker.id.as_str(), "thread-1");
        assert_ne!(worker.id.as_str(), "run-1");
    }

    #[test]
    fn worker_rejects_transition_out_of_terminal_state() {
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker
            .transition(WorkerState::Cancelled, now, Some("user stopped".into()))
            .unwrap();

        assert_eq!(
            worker.transition(WorkerState::Starting, now, None),
            Err(DomainError::InvalidTransition {
                entity: "worker",
                from: "cancelled",
                to: "starting",
            })
        );
    }

    #[test]
    fn work_package_requires_coherent_feature_membership() {
        assert_eq!(
            WorkPackage::new("goal-a", "Empty", vec![], Utc::now()),
            Err(DomainError::EmptyWorkPackage)
        );
    }

    #[test]
    fn work_package_records_when_it_becomes_ready() {
        let now = Utc::now();
        let mut package =
            WorkPackage::new("goal-a", "Foundation", vec!["storage".into()], now).unwrap();
        package.transition(WorkPackageState::Planned, now).unwrap();
        package.transition(WorkPackageState::Ready, now).unwrap();

        assert_eq!(package.ready_at, Some(now));
    }

    #[test]
    fn claim_rejects_an_invalid_lease_window() {
        let now = Utc::now();
        assert_eq!(
            Claim::new(
                "goal-a",
                ClaimScope::Feature {
                    feature_id: "storage".into(),
                },
                WorkerId::new(),
                "abc123",
                now,
                now,
            ),
            Err(DomainError::InvalidLeaseWindow)
        );
    }

    #[test]
    fn claim_has_one_terminal_transition_with_matching_outcome() {
        let now = Utc::now();
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "storage".into(),
            },
            WorkerId::new(),
            "abc123",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        claim
            .transition(
                ClaimState::Completed,
                ClaimOutcome {
                    kind: ClaimOutcomeKind::Completed,
                    reason: "validated and integrated".into(),
                    artifact_id: Some("artifact-1".into()),
                    evidence_refs: vec!["test:passed".into()],
                    escalation_id: None,
                    at: now + Duration::minutes(1),
                },
            )
            .unwrap();

        assert_eq!(claim.state, ClaimState::Completed);
        assert!(claim.outcome.is_some());
        assert!(claim.state.is_terminal());
    }

    #[test]
    fn claim_renewal_rejects_a_stale_generation() {
        let now = Utc::now();
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "storage".into(),
            },
            WorkerId::new(),
            "abc123",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();

        assert_eq!(
            claim.renew(2, now, now + Duration::minutes(5)),
            Err(DomainError::StaleLeaseGeneration {
                expected: 1,
                received: 2,
            })
        );
    }

    #[test]
    fn coordination_records_round_trip_with_schema_version() {
        let now = Utc::now();
        let worker = Worker::new("goal-a", now);
        let json = serde_json::to_string(&worker).unwrap();
        let restored: Worker = serde_json::from_str(&json).unwrap();

        assert_eq!(restored, worker);
        assert_eq!(
            restored.metadata.schema_version,
            COORDINATION_SCHEMA_VERSION
        );
    }

    #[test]
    fn contract_revisions_are_monotonic_and_attributed() {
        let now = Utc::now();
        let worker = WorkerId::new();
        let mut contract =
            SharedContract::new("goal-a", "api.worker", "Worker API", ContractKind::Api, now);
        contract.revise(worker.clone(), "adds an optional field", now);

        assert_eq!(contract.revision, 2);
        assert_eq!(contract.last_changed_by, Some(worker));
    }

    #[test]
    fn events_keep_causation_and_correlation_separate() {
        let now = Utc::now();
        let cause = EventId::new();
        let mut event = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ContractChanged,
            EventSeverity::Warning,
            CoordinationActor::System,
            "contract-api.worker",
            serde_json::json!({"revision": 2}),
            now,
        );
        event.causation_id = Some(cause.clone());

        assert_eq!(event.causation_id, Some(cause));
        assert_eq!(event.correlation_id, "contract-api.worker");
        assert_eq!(event.payload_version, 1);
    }

    #[test]
    fn typed_event_catalog_round_trips_every_schema() {
        let now = Utc::now();
        let worker_id = WorkerId::new();
        let claim_id = ClaimId::new();
        let payloads = vec![
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker_id.clone(),
                claim_id: claim_id.clone(),
                category: ActivityCategory::Progress,
                summary: "Implemented the requested boundary".into(),
                progress_percent: Some(50),
                changed_scope: vec!["src/api.rs".into()],
                artifact_id: None,
                evidence_refs: vec!["test:activity".into()],
                validation_succeeded: None,
            }),
            CoordinationEventPayload::Claim(ClaimEventPayload {
                claim_id: claim_id.clone(),
                worker_id: worker_id.clone(),
                from_state: Some(ClaimState::Active),
                to_state: ClaimState::Completed,
                reason: "Validated and ready for integration".into(),
                artifact_id: Some("artifact-1".into()),
                evidence_refs: vec!["test:claim".into()],
            }),
            CoordinationEventPayload::Contract(ContractEventPayload {
                contract_id: ContractId::new(),
                stable_key: "api.worker".into(),
                previous_revision: 1,
                revision: 2,
                producer_worker_id: Some(worker_id.clone()),
                compatibility_notes: "Adds an optional response field".into(),
            }),
            CoordinationEventPayload::Dependency(DependencyEventPayload {
                work_package_id: WorkPackageId::new(),
                dependency_key: "package:foundation".into(),
                previous_revision: "rev-1".into(),
                revision: "rev-2".into(),
                affected_worker_ids: vec![worker_id.clone()],
            }),
            CoordinationEventPayload::ContractMismatch(ContractMismatchEventPayload {
                claim_id: claim_id.clone(),
                worker_id: worker_id.clone(),
                contract_id: ContractId::new(),
                stable_key: "api.worker".into(),
                expected_revision: 1,
                current_revision: 2,
            }),
            CoordinationEventPayload::FileOverlap(FileOverlapEventPayload {
                left_worker_id: worker_id.clone(),
                left_claim_id: claim_id.clone(),
                left_base_revision: "base-a".into(),
                left_head_revision: "head-a".into(),
                right_worker_id: WorkerId::new(),
                right_claim_id: ClaimId::new(),
                right_base_revision: "base-a".into(),
                right_head_revision: "head-b".into(),
                paths: vec![FileOverlapInput {
                    path: "src/api.rs".into(),
                    left_status: "modified".into(),
                    right_status: "modified".into(),
                    migration: false,
                    dependency_file: false,
                    generated_artifact: false,
                }],
            }),
            CoordinationEventPayload::SymbolOverlap(SymbolOverlapEventPayload {
                left_worker_id: worker_id.clone(),
                left_claim_id: claim_id.clone(),
                left_base_revision: "base-a".into(),
                left_head_revision: "head-a".into(),
                right_worker_id: WorkerId::new(),
                right_claim_id: ClaimId::new(),
                right_base_revision: "base-a".into(),
                right_head_revision: "head-b".into(),
                overlaps: vec![SymbolOverlapInput {
                    identity: "fn:shared".into(),
                    match_kind: "symbol".into(),
                    left_path: "src/api.rs".into(),
                    right_path: "src/api.rs".into(),
                }],
            }),
            CoordinationEventPayload::MigrationOverlap(MigrationOverlapEventPayload {
                left_worker_id: worker_id.clone(),
                left_claim_id: claim_id.clone(),
                left_base_revision: "base-a".into(),
                left_head_revision: "head-a".into(),
                right_worker_id: WorkerId::new(),
                right_claim_id: ClaimId::new(),
                right_base_revision: "base-a".into(),
                right_head_revision: "head-b".into(),
                overlaps: vec![MigrationOverlapInput {
                    classification: "database".into(),
                    left_paths: vec!["migrations/001_left.sql".into()],
                    right_paths: vec!["migrations/002_right.sql".into()],
                    shared_paths: vec![],
                }],
            }),
            CoordinationEventPayload::MergeConflict(MergeConflictEventPayload {
                left_worker_id: worker_id.clone(),
                left_claim_id: claim_id.clone(),
                left_head_revision: "head-a".into(),
                right_worker_id: WorkerId::new(),
                right_claim_id: ClaimId::new(),
                right_head_revision: "head-b".into(),
                merge_base_revision: "base-a".into(),
                conflicting_paths: vec!["src/api.rs".into()],
                diagnostics: "CONFLICT (content): Merge conflict in src/api.rs".into(),
                diagnostics_truncated: false,
            }),
            CoordinationEventPayload::WorkerStalled(WorkerStalledEventPayload {
                worker_id: worker_id.clone(),
                claim_id: claim_id.clone(),
                last_heartbeat_at: now - Duration::minutes(5),
                last_activity_at: Some(now - Duration::minutes(4)),
                detected_at: now,
                heartbeat_age_seconds: 300,
                activity_age_seconds: Some(240),
                heartbeat_threshold_seconds: 120,
                activity_threshold_seconds: 180,
            }),
            CoordinationEventPayload::RepeatedValidationFailure(
                RepeatedValidationFailureEventPayload {
                    worker_id: worker_id.clone(),
                    claim_id: claim_id.clone(),
                    failure_event_ids: vec![EventId::new(), EventId::new(), EventId::new()],
                    last_failure_at: now,
                    window_seconds: 900,
                    threshold: 3,
                },
            ),
            CoordinationEventPayload::WatchdogAction(WatchdogActionEventPayload {
                worker_id: worker_id.clone(),
                claim_ids: vec![claim_id.clone()],
                action: WatchdogActionKind::EnvironmentRecovery,
                reason: "worker process exited while the claim remained active".into(),
                observed_at: now,
            }),
            CoordinationEventPayload::Blocker(BlockerEventPayload {
                claim_id: claim_id.clone(),
                worker_id: worker_id.clone(),
                summary: "Required migration decision is unresolved".into(),
                evidence_refs: vec!["decision:migration".into()],
                escalation_id: Some(EscalationId::new()),
            }),
            CoordinationEventPayload::CoordinationRequest(CoordinationRequestEventPayload {
                claim_id: claim_id.clone(),
                requester_worker_id: worker_id.clone(),
                target_scope: "contract:api.worker".into(),
                desired_outcome: "Confirm the response compatibility boundary".into(),
                evidence_refs: vec!["diff:api".into()],
            }),
            CoordinationEventPayload::HumanDecision(HumanDecisionEventPayload {
                escalation_id: EscalationId::new(),
                decided_by: "user-1".into(),
                option_id: "online-migration".into(),
                action: RemediationAction::ChooseDirection,
                accepted_risk: Some("Temporary mixed-version reads".into()),
            }),
            CoordinationEventPayload::PrivilegedAction(PrivilegedActionEventPayload {
                actor: "user-1".into(),
                authority: "explicit_user_confirmation".into(),
                action: "pool.stop".into(),
                target: "goal-a".into(),
                decision: "Stop the autonomous worker pool".into(),
                evidence_refs: vec!["idempotency:stop-1".into()],
                outcome: "succeeded".into(),
            }),
            CoordinationEventPayload::Completion(CompletionEventPayload {
                claim_id,
                worker_id,
                artifact_id: "artifact-2".into(),
                evidence_refs: vec!["test:completion".into()],
                integration_revision: "commit-abc".into(),
            }),
        ];

        for payload in payloads {
            let expected_schema = payload.schema_key();
            let expected_kind = payload.event_kind();
            let event = CoordinationEvent::from_typed_payload(
                "goal-a",
                EventSeverity::Info,
                CoordinationActor::System,
                "catalog-test",
                payload.clone(),
                now,
            )
            .unwrap();

            assert_eq!(event.payload_schema, expected_schema);
            assert_eq!(event.payload_version, 1);
            assert_eq!(event.kind, expected_kind);
            assert_eq!(event.typed_payload().unwrap(), Some(payload));
        }
    }

    #[test]
    fn typed_events_reject_schema_kind_version_and_payload_mismatches() {
        let now = Utc::now();
        let payload = CoordinationEventPayload::Activity(ActivityEventPayload {
            worker_id: WorkerId::new(),
            claim_id: ClaimId::new(),
            category: ActivityCategory::Progress,
            summary: "Working".into(),
            progress_percent: Some(10),
            changed_scope: vec![],
            artifact_id: None,
            evidence_refs: vec![],
            validation_succeeded: None,
        });
        let event = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::System,
            "catalog-test",
            payload,
            now,
        )
        .unwrap();

        let mut wrong_schema = event;
        wrong_schema.payload_schema = "coordination.claim.v1".into();
        assert!(matches!(
            wrong_schema.typed_payload(),
            Err(DomainError::InvalidEventPayload(_))
        ));

        let mut wrong_kind = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::System,
            "catalog-test",
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: WorkerId::new(),
                claim_id: ClaimId::new(),
                category: ActivityCategory::Progress,
                summary: "Working".into(),
                progress_percent: Some(10),
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec![],
                validation_succeeded: None,
            }),
            now,
        )
        .unwrap();
        wrong_kind.kind = CoordinationEventKind::ClaimLifecycleChanged;
        assert!(wrong_kind.typed_payload().is_err());

        let mut wrong_version = wrong_kind;
        wrong_version.kind = CoordinationEventKind::ActivityPublished;
        wrong_version.payload_version = 2;
        assert!(wrong_version.typed_payload().is_err());
    }

    #[test]
    fn typed_event_validation_rejects_invalid_values_and_preserves_legacy_events() {
        let now = Utc::now();
        let invalid_contract = CoordinationEventPayload::Contract(ContractEventPayload {
            contract_id: ContractId::new(),
            stable_key: "api.worker".into(),
            previous_revision: 2,
            revision: 2,
            producer_worker_id: Some(WorkerId::new()),
            compatibility_notes: "No revision change".into(),
        });
        assert!(matches!(
            CoordinationEvent::from_typed_payload(
                "goal-a",
                EventSeverity::Warning,
                CoordinationActor::System,
                "contract-test",
                invalid_contract,
                now,
            ),
            Err(DomainError::InvalidEventPayload(_))
        ));

        let legacy = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Info,
            CoordinationActor::System,
            "legacy-test",
            serde_json::json!({"summary": "old event"}),
            now,
        );
        assert_eq!(legacy.typed_payload().unwrap(), None);
    }

    #[test]
    fn supervisor_intervention_targets_only_affected_workers() {
        let now = Utc::now();
        let target = WorkerId::new();
        let other = WorkerId::new();
        let intervention = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Inform,
            vec![EventId::new()],
            vec![target.clone()],
            "Refresh the shared API contract before continuing",
            now,
        );

        assert!(intervention.target_workers.contains(&target));
        assert!(!intervention.target_workers.contains(&other));
        assert_eq!(intervention.state, InterventionState::Proposed);
    }

    #[test]
    fn escalation_resolution_requires_an_attributed_decision() {
        let now = Utc::now();
        let mut escalation = HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Goal {
                goal_id: "goal-a".into(),
            },
            "Choose a migration strategy",
            now,
        );
        assert!(
            escalation
                .transition(EscalationState::Resolved, now, None)
                .is_err()
        );
        escalation
            .transition(
                EscalationState::Resolved,
                now,
                Some(EscalationDecision {
                    option_id: "online-migration".into(),
                    decided_by: "user-1".into(),
                    accepted_risk: None,
                    decided_at: now,
                }),
            )
            .unwrap();

        assert_eq!(escalation.state, EscalationState::Resolved);
    }

    #[test]
    fn terminal_notification_cannot_be_redelivered() {
        let now = Utc::now();
        let mut notification = WorkerNotification::new(
            "goal-a",
            WorkerId::new(),
            EventId::new(),
            "contract_changed",
            "A consumed contract changed revision.",
            EventSeverity::Warning,
            vec!["contract:api.shared".into()],
            Some("Refresh context before continuing.".into()),
            true,
            None,
            now,
        )
        .unwrap();
        notification
            .transition(NotificationState::Expired, now)
            .unwrap();

        assert!(
            notification
                .transition(NotificationState::Delivered, now)
                .is_err()
        );
    }

    #[test]
    fn notification_schema_requires_bounded_actionable_context() {
        let now = Utc::now();
        let notification = WorkerNotification::new(
            "goal-a",
            WorkerId::new(),
            EventId::new(),
            "dependency_changed",
            "A required work package changed.",
            EventSeverity::Warning,
            vec!["package:producer".into(), "event:dependency".into()],
            Some("Review the new package revision before the next turn.".into()),
            true,
            Some(now + chrono::Duration::minutes(15)),
            now,
        )
        .unwrap();
        assert_eq!(notification.state, NotificationState::Queued);
        assert_eq!(notification.evidence_refs.len(), 2);
        let mut legacy = serde_json::to_value(&notification).unwrap();
        legacy.as_object_mut().unwrap().remove("summary");
        legacy.as_object_mut().unwrap().remove("severity");
        legacy.as_object_mut().unwrap().remove("evidenceRefs");
        legacy.as_object_mut().unwrap().remove("recommendedAction");
        legacy.as_object_mut().unwrap().remove("deliveryAttempts");
        legacy.as_object_mut().unwrap().remove("auditHistory");
        legacy.as_object_mut().unwrap().remove("contentFingerprint");
        legacy
            .as_object_mut()
            .unwrap()
            .remove("coalescedSourceEventIds");
        let legacy: WorkerNotification = serde_json::from_value(legacy).unwrap();
        legacy.validate().unwrap();
        assert_eq!(legacy.summary, "Legacy notification");
        assert_eq!(legacy.severity, EventSeverity::Info);
        assert_eq!(legacy.delivery_attempts, 0);
        assert!(legacy.audit_history.is_empty());
        assert!(legacy.content_fingerprint.is_none());
        assert!(legacy.coalesced_source_event_ids.is_empty());
        assert!(
            WorkerNotification::new(
                "goal-a",
                WorkerId::new(),
                EventId::new(),
                "",
                "A required work package changed.",
                EventSeverity::Warning,
                vec![],
                None,
                false,
                None,
                now,
            )
            .is_err()
        );
        assert!(
            WorkerNotification::new(
                "goal-a",
                WorkerId::new(),
                EventId::new(),
                "dependency_changed",
                "A required work package changed.",
                EventSeverity::Warning,
                vec![],
                None,
                false,
                Some(now),
                now,
            )
            .is_err()
        );
    }

    #[test]
    fn direct_coordination_exchange_is_short_lived_bounded_and_alternating() {
        let now = Utc::now();
        let requester = WorkerId::new();
        let target = WorkerId::new();
        assert!(
            CoordinationExchange::new(
                "goal-a",
                EventId::new(),
                format!("worker:{}", target.as_str()),
                "Agree on one response field",
                requester.clone(),
                target.clone(),
                "The durable notification did not resolve the ambiguity",
                now + chrono::Duration::minutes(16),
                4,
                now,
            )
            .is_err()
        );
        let mut exchange = CoordinationExchange::new(
            "goal-a",
            EventId::new(),
            format!("worker:{}", target.as_str()),
            "Agree on one response field",
            requester.clone(),
            target.clone(),
            "The durable notification did not resolve the ambiguity",
            now + chrono::Duration::minutes(10),
            4,
            now,
        )
        .unwrap();
        exchange
            .add_message(
                requester.clone(),
                "Can you consume response.value?",
                vec!["contract:response".into()],
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        assert!(
            exchange
                .add_message(
                    requester.clone(),
                    "Repeated hidden instruction",
                    vec![],
                    now + chrono::Duration::seconds(2),
                )
                .is_err()
        );
        exchange
            .add_message(
                target.clone(),
                "Yes, response.value is the agreed boundary.",
                vec!["test:consumer".into()],
                now + chrono::Duration::seconds(2),
            )
            .unwrap();
        exchange
            .complete(
                target,
                "Both workers will use response.value; consumer validation is test:consumer.",
                now + chrono::Duration::seconds(3),
            )
            .unwrap();
        assert_eq!(exchange.state, CoordinationExchangeState::Completed);
        assert_eq!(exchange.messages.len(), 2);
        assert!(exchange.shared_note.is_some());
        exchange.validate().unwrap();
    }

    #[test]
    fn current_schema_accepts_unknown_future_fields_but_rejects_future_versions() {
        let now = Utc::now();
        let worker = Worker::new("goal-a", now);
        let mut value = serde_json::to_value(&worker).unwrap();
        value["futureField"] = serde_json::json!({"safe": true});
        let restored: Worker = serde_json::from_value(value).unwrap();

        assert_eq!(restored.id, worker.id);
        assert_eq!(validate_schema_version(COORDINATION_SCHEMA_VERSION), Ok(()));
        assert_eq!(
            validate_schema_version(COORDINATION_SCHEMA_VERSION + 1),
            Err(DomainError::UnsupportedSchemaVersion(
                COORDINATION_SCHEMA_VERSION + 1
            ))
        );
    }
}
