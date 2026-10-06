use super::detectors::ChangeInventoryCollector;
use super::domain::{
    ClaimId, ClaimScope, ClaimState, ContractId, ContractKind, CoordinationEvent,
    CoordinationEventKind, CoordinationEventPayload, DomainError, EscalationId, EscalationKind,
    EscalationScope, EscalationSeverity, EscalationState, EventId, EventSeverity, InterventionId,
    InterventionLevel, InterventionState, SignalId, SignalState, SupervisorBudgetLedger,
    SupervisorIntervention, WorkerId, WorkerState,
};
use super::store::{
    ClaimRepository, ContractRepository, EscalationRepository, EventRepository,
    InterventionRepository, SignalRepository, SqliteCoordinationStore, StoreError,
    SupervisorBudgetRepository, WorkerRepository,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

const MAX_TRIGGER_EVENT_SCAN: usize = 1_000;
const MAX_SUPERVISOR_CONTEXT_BYTES: usize = 128 * 1024;
const MAX_CONTEXT_WORKERS: usize = 16;
const MAX_CONTEXT_CLAIMS: usize = 32;
const MAX_CONTEXT_CHANGES: usize = 8;
const MAX_CONTEXT_CONTRACTS: usize = 64;
const MAX_CONTEXT_FAILURES: usize = 64;
const MAX_CONTEXT_INTERVENTIONS: usize = 16;
const MAX_CONTEXT_ESCALATIONS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorTriggerKind {
    Signal,
    CoordinationRequest,
    UnresolvedEscalation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTrigger {
    pub key: String,
    pub goal_id: String,
    pub kind: SupervisorTriggerKind,
    pub severity: EventSeverity,
    pub confidence_percent: u8,
    pub occurred_at: DateTime<Utc>,
    pub signal_id: Option<SignalId>,
    pub request_event_id: Option<EventId>,
    pub escalation_id: Option<EscalationId>,
    pub evidence_event_ids: Vec<EventId>,
    pub affected_worker_ids: Vec<WorkerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTriggerPolicy {
    pub minimum_signal_confidence_percent: u8,
    pub minimum_signal_severity: EventSeverity,
    pub budget_window_seconds: u64,
    pub max_evaluations_per_window: u32,
    pub max_tokens_per_window: u64,
    pub max_tokens_per_evaluation: u64,
    pub max_retries_per_trigger: u32,
    pub consecutive_failure_limit: u32,
    pub circuit_breaker_seconds: u64,
}

impl Default for SupervisorTriggerPolicy {
    fn default() -> Self {
        Self {
            minimum_signal_confidence_percent: 50,
            minimum_signal_severity: EventSeverity::Warning,
            budget_window_seconds: 60,
            max_evaluations_per_window: 20,
            max_tokens_per_window: 100_000,
            max_tokens_per_evaluation: 16_000,
            max_retries_per_trigger: 2,
            consecutive_failure_limit: 3,
            circuit_breaker_seconds: 300,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTriggerBatch {
    pub goal_id: String,
    pub after_event_sequence: u64,
    pub last_event_sequence: u64,
    pub triggers: Vec<SupervisorTrigger>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorContextPacket {
    pub schema_version: u16,
    pub goal_id: String,
    pub generated_at: DateTime<Utc>,
    pub trigger: SupervisorTrigger,
    pub workers: Vec<SupervisorWorkerContext>,
    pub claims: Vec<SupervisorClaimContext>,
    pub repository_changes: Vec<SupervisorRepositoryChangeContext>,
    pub contracts: Vec<SupervisorContractContext>,
    pub failure_events: Vec<SupervisorEventContext>,
    pub prior_interventions: Vec<SupervisorInterventionContext>,
    pub open_escalations: Vec<SupervisorEscalationContext>,
    pub truncated: bool,
}

impl SupervisorContextPacket {
    pub fn evaluation_instructions(&self) -> Result<String, serde_json::Error> {
        Ok(crate::prompts::supervisor(&serde_json::to_string_pretty(
            self,
        )?))
    }

    pub fn parse_decision(
        &self,
        response: &str,
    ) -> Result<SupervisorDecision, SupervisorDecisionError> {
        let decision: SupervisorDecision = serde_json::from_str(response)?;
        validate_decision(self, &decision)?;
        Ok(decision)
    }
}

fn validate_decision(
    context: &SupervisorContextPacket,
    decision: &SupervisorDecision,
) -> Result<(), SupervisorDecisionError> {
    if decision.schema_version != 1 {
        return Err(SupervisorDecisionError::Invalid(
            "schema_version must be 1".into(),
        ));
    }
    if decision.confidence_percent > 100 {
        return Err(SupervisorDecisionError::Invalid(
            "confidence_percent must be between 0 and 100".into(),
        ));
    }
    if decision.trigger_key != context.trigger.key {
        return Err(SupervisorDecisionError::Invalid(
            "trigger_key does not match the evaluated trigger".into(),
        ));
    }
    validate_required_text(&decision.summary, "summary", 2_000)?;
    validate_optional_text(&decision.recommendation, "recommendation", 2_000)?;
    if let Some(action) = &decision.requested_action {
        validate_required_text(action, "requested_action", 2_000)?;
    }
    if decision.evidence_event_ids.len() > 64
        || decision.evidence_refs.len() > 64
        || decision.target_worker_ids.len() > MAX_CONTEXT_WORKERS
    {
        return Err(SupervisorDecisionError::Invalid(
            "evidence and target collections exceed their bounds".into(),
        ));
    }
    reject_duplicate_ids(
        decision.evidence_event_ids.iter().map(|id| id.as_str()),
        "evidence_event_ids",
    )?;
    reject_duplicate_ids(
        decision.evidence_refs.iter().map(String::as_str),
        "evidence_refs",
    )?;
    reject_duplicate_ids(
        decision.target_worker_ids.iter().map(|id| id.as_str()),
        "target_worker_ids",
    )?;
    let allowed_events = context
        .trigger
        .evidence_event_ids
        .iter()
        .chain(context.failure_events.iter().map(|event| &event.event_id))
        .map(|id| id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(unknown) = decision
        .evidence_event_ids
        .iter()
        .find(|id| id.as_str().is_empty() || !allowed_events.contains(id.as_str()))
    {
        return Err(SupervisorDecisionError::Invalid(format!(
            "evidence event {} was not supplied in context",
            unknown.as_str()
        )));
    }
    let allowed_refs = context
        .prior_interventions
        .iter()
        .flat_map(|intervention| intervention.evidence_refs.iter())
        .chain(
            context
                .open_escalations
                .iter()
                .flat_map(|escalation| escalation.evidence_refs.iter()),
        )
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if let Some(unknown) = decision
        .evidence_refs
        .iter()
        .find(|reference| reference.trim().is_empty() || !allowed_refs.contains(reference.as_str()))
    {
        return Err(SupervisorDecisionError::Invalid(format!(
            "evidence reference {unknown} was not supplied in context"
        )));
    }
    let allowed_workers = context
        .workers
        .iter()
        .map(|worker| worker.worker_id.as_str())
        .chain(
            context
                .trigger
                .affected_worker_ids
                .iter()
                .map(|id| id.as_str()),
        )
        .collect::<BTreeSet<_>>();
    if let Some(unknown) = decision
        .target_worker_ids
        .iter()
        .find(|id| id.as_str().is_empty() || !allowed_workers.contains(id.as_str()))
    {
        return Err(SupervisorDecisionError::Invalid(format!(
            "target worker {} was not supplied in context",
            unknown.as_str()
        )));
    }
    let has_evidence =
        !decision.evidence_event_ids.is_empty() || !decision.evidence_refs.is_empty();
    if decision.decision != InterventionLevel::Observe && !has_evidence {
        return Err(SupervisorDecisionError::Invalid(
            "non-observe decisions require supplied durable evidence".into(),
        ));
    }
    match decision.decision {
        InterventionLevel::Observe => {
            if !decision.target_worker_ids.is_empty()
                || !decision.recommendation.trim().is_empty()
                || decision.requested_action.is_some()
                || decision.requires_human_decision
                || decision.escalation.is_some()
            {
                return Err(SupervisorDecisionError::Invalid(
                    "observe cannot target workers, request action, recommend work, or escalate"
                        .into(),
                ));
            }
        }
        InterventionLevel::Inform => {
            require_targets(decision)?;
            if decision.requested_action.is_some()
                || decision.requires_human_decision
                || decision.escalation.is_some()
            {
                return Err(SupervisorDecisionError::Invalid(
                    "inform cannot request action or a human decision".into(),
                ));
            }
        }
        InterventionLevel::Recommend => {
            require_targets(decision)?;
            validate_required_text(&decision.recommendation, "recommendation", 2_000)?;
            if decision.requires_human_decision || decision.escalation.is_some() {
                return Err(SupervisorDecisionError::Invalid(
                    "recommend cannot contain a human escalation".into(),
                ));
            }
        }
        InterventionLevel::Coordinate => {
            require_targets(decision)?;
            validate_required_text(&decision.recommendation, "recommendation", 2_000)?;
            if decision.requested_action.is_none()
                || decision.requires_human_decision
                || decision.escalation.is_some()
            {
                return Err(SupervisorDecisionError::Invalid(
                    "coordinate requires a requested action and cannot contain a human escalation"
                        .into(),
                ));
            }
        }
        InterventionLevel::Block => {
            require_targets(decision)?;
            if decision.confidence_percent < 90
                || decision.requested_action.is_none()
                || decision.requires_human_decision
                || decision.escalation.is_some()
            {
                return Err(SupervisorDecisionError::Invalid(
                    "block requires at least 90% confidence, affected targets, and a requested action"
                        .into(),
                ));
            }
        }
        InterventionLevel::Escalate => {
            if !decision.requires_human_decision || decision.requested_action.is_some() {
                return Err(SupervisorDecisionError::Invalid(
                    "escalate requires a human decision and cannot request an automatic action"
                        .into(),
                ));
            }
            validate_escalation(decision.escalation.as_ref().ok_or_else(|| {
                SupervisorDecisionError::Invalid("escalate requires an escalation payload".into())
            })?)?;
        }
    }
    if decision.decision != InterventionLevel::Escalate
        && (decision.requires_human_decision || decision.escalation.is_some())
    {
        return Err(SupervisorDecisionError::Invalid(
            "only escalate may include a human decision payload".into(),
        ));
    }
    Ok(())
}

fn validate_escalation(
    escalation: &SupervisorEscalationProposal,
) -> Result<(), SupervisorDecisionError> {
    validate_required_text(&escalation.scope, "escalation.scope", 500)?;
    validate_required_text(&escalation.question, "escalation.question", 2_000)?;
    if !(2..=6).contains(&escalation.options.len()) {
        return Err(SupervisorDecisionError::Invalid(
            "escalation requires two to six remediation options".into(),
        ));
    }
    reject_duplicate_ids(
        escalation.options.iter().map(|option| option.id.as_str()),
        "escalation option ids",
    )?;
    if escalation
        .options
        .iter()
        .filter(|option| option.recommended)
        .count()
        > 1
    {
        return Err(SupervisorDecisionError::Invalid(
            "at most one escalation option may be recommended".into(),
        ));
    }
    for option in &escalation.options {
        validate_required_text(&option.id, "option.id", 100)?;
        validate_required_text(&option.label, "option.label", 200)?;
        validate_required_text(&option.description, "option.description", 1_000)?;
        if option.consequences.is_empty() || option.consequences.len() > 16 {
            return Err(SupervisorDecisionError::Invalid(
                "every escalation option requires one to sixteen consequences".into(),
            ));
        }
        for consequence in &option.consequences {
            validate_required_text(consequence, "option consequence", 1_000)?;
        }
    }
    Ok(())
}

fn require_targets(decision: &SupervisorDecision) -> Result<(), SupervisorDecisionError> {
    if decision.target_worker_ids.is_empty() {
        Err(SupervisorDecisionError::Invalid(
            "this decision requires at least one affected target worker".into(),
        ))
    } else {
        Ok(())
    }
}

fn validate_required_text(
    value: &str,
    field: &str,
    max_chars: usize,
) -> Result<(), SupervisorDecisionError> {
    if value.trim().is_empty() || value.chars().count() > max_chars {
        Err(SupervisorDecisionError::Invalid(format!(
            "{field} must contain 1-{max_chars} characters"
        )))
    } else {
        Ok(())
    }
}

fn validate_optional_text(
    value: &str,
    field: &str,
    max_chars: usize,
) -> Result<(), SupervisorDecisionError> {
    if value.chars().count() > max_chars {
        Err(SupervisorDecisionError::Invalid(format!(
            "{field} must contain at most {max_chars} characters"
        )))
    } else {
        Ok(())
    }
}

fn reject_duplicate_ids<'a>(
    values: impl Iterator<Item = &'a str>,
    field: &str,
) -> Result<(), SupervisorDecisionError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(SupervisorDecisionError::Invalid(format!(
                "{field} contains a duplicate value"
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorWorkerContext {
    pub worker_id: WorkerId,
    pub state: WorkerState,
    pub updated_at: DateTime<Utc>,
    pub current_run_id: Option<String>,
    pub current_thread_id: Option<String>,
    pub active_claim_ids: Vec<ClaimId>,
    pub repository_id: Option<String>,
    pub worktree_path: Option<String>,
    pub base_revision: Option<String>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorClaimContext {
    pub claim_id: ClaimId,
    pub owner_worker_id: WorkerId,
    pub state: ClaimState,
    pub scope: ClaimScope,
    pub updated_at: DateTime<Utc>,
    pub base_revision: String,
    pub lease_generation: u64,
    pub lease_expires_at: DateTime<Utc>,
    pub produced_contract_keys: Vec<String>,
    pub expected_contract_revisions: Vec<SupervisorExpectedContractRevision>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorExpectedContractRevision {
    pub contract_id_or_key: String,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRepositoryChangeContext {
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub repository_id: Option<String>,
    pub base_revision: String,
    pub head_revision: Option<String>,
    pub patch_sha256: Option<String>,
    pub changed_paths: Vec<String>,
    pub changed_symbols: Vec<String>,
    pub migrations: Vec<String>,
    pub validation_summaries: Vec<String>,
    pub unavailable_reason: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorContractContext {
    pub contract_id: ContractId,
    pub stable_key: String,
    pub title: String,
    pub kind: ContractKind,
    pub current_revision: u64,
    pub expected_revisions: Vec<SupervisorContractExpectation>,
    pub compatibility_notes: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorContractExpectation {
    pub claim_id: ClaimId,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorEventContext {
    pub event_id: EventId,
    pub kind: CoordinationEventKind,
    pub severity: EventSeverity,
    pub occurred_at: DateTime<Utc>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorInterventionContext {
    pub intervention_id: InterventionId,
    pub level: InterventionLevel,
    pub state: InterventionState,
    pub updated_at: DateTime<Utc>,
    pub target_worker_ids: Vec<WorkerId>,
    pub evidence_refs: Vec<String>,
    pub recommendation: String,
    pub requested_action: Option<String>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorEscalationContext {
    pub escalation_id: EscalationId,
    pub kind: EscalationKind,
    pub severity: EscalationSeverity,
    pub state: EscalationState,
    pub scope: EscalationScope,
    pub updated_at: DateTime<Utc>,
    pub summary: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorDecision {
    pub schema_version: u16,
    pub trigger_key: String,
    pub decision: InterventionLevel,
    pub confidence_percent: u8,
    pub summary: String,
    pub evidence_event_ids: Vec<EventId>,
    pub evidence_refs: Vec<String>,
    pub target_worker_ids: Vec<WorkerId>,
    pub recommendation: String,
    pub requested_action: Option<String>,
    pub requires_human_decision: bool,
    pub escalation: Option<SupervisorEscalationProposal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorEscalationProposal {
    pub kind: EscalationKind,
    pub severity: EscalationSeverity,
    pub scope: String,
    pub question: String,
    pub options: Vec<SupervisorRemediationProposal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupervisorRemediationProposal {
    pub id: String,
    pub label: String,
    pub description: String,
    pub consequences: Vec<String>,
    pub recommended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorDispatchFailure {
    pub trigger_key: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorDispatchSkipped {
    pub trigger_key: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorEvaluationBudget {
    pub max_total_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorEvaluationUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

impl SupervisorEvaluationUsage {
    fn total(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorDispatchReport {
    pub batch: SupervisorTriggerBatch,
    pub dispatched_trigger_keys: Vec<String>,
    pub failures: Vec<SupervisorDispatchFailure>,
    pub skipped: Vec<SupervisorDispatchSkipped>,
    pub budget: SupervisorBudgetLedger,
}

pub trait SupervisorEvaluationSink: Send + Sync {
    fn evaluate(
        &self,
        trigger: &SupervisorTrigger,
        budget: SupervisorEvaluationBudget,
    ) -> Result<SupervisorEvaluationUsage, String>;
}

#[derive(Debug, Error)]
pub enum SupervisorTriggerError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("invalid supervisor trigger policy: {0}")]
    InvalidPolicy(String),
}

#[derive(Debug, Error)]
pub enum SupervisorContextError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("supervisor trigger belongs to goal {trigger_goal}, not {requested_goal}")]
    GoalMismatch {
        trigger_goal: String,
        requested_goal: String,
    },
    #[error(
        "supervisor context remains larger than {MAX_SUPERVISOR_CONTEXT_BYTES} bytes after deterministic truncation"
    )]
    ContextTooLarge,
}

#[derive(Debug, Error)]
pub enum SupervisorDecisionError {
    #[error("Supervisor response is not valid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid Supervisor decision: {0}")]
    Invalid(String),
}

#[derive(Debug, Error)]
pub enum SupervisorPersistenceError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Decision(#[from] SupervisorDecisionError),
    #[error(transparent)]
    Domain(#[from] DomainError),
}

#[derive(Debug, Clone)]
pub struct SupervisorInterventionService {
    store: Arc<SqliteCoordinationStore>,
}

impl SupervisorInterventionService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn persist_decision(
        &self,
        context: &SupervisorContextPacket,
        decision: &SupervisorDecision,
        now: DateTime<Utc>,
    ) -> Result<SupervisorIntervention, SupervisorPersistenceError> {
        validate_decision(context, decision)?;
        let reasoning_input_json =
            serde_json::to_string(context).map_err(SupervisorDecisionError::Json)?;
        let existing = self.store.interventions_for_goal(&context.goal_id, None)?;
        if let Some(same) = existing.iter().rev().find(|intervention| {
            intervention.trigger_key.as_deref() == Some(decision.trigger_key.as_str())
                && intervention.reasoning_input_json.as_deref()
                    == Some(reasoning_input_json.as_str())
                && intervention.level == decision.decision
                && intervention.signal_event_ids == decision.evidence_event_ids
                && intervention.evidence_refs == decision.evidence_refs
                && intervention.target_workers == decision.target_worker_ids
                && intervention.recommendation == decision.recommendation
                && intervention.requested_action == decision.requested_action
                && intervention.decision_summary == decision.summary
                && intervention.confidence_percent == Some(decision.confidence_percent)
        }) {
            return Ok(same.clone());
        }

        let mut replacement = SupervisorIntervention::new(
            &context.goal_id,
            decision.decision,
            decision.evidence_event_ids.clone(),
            decision.target_worker_ids.clone(),
            &decision.recommendation,
            now,
        );
        replacement.evidence_refs = decision.evidence_refs.clone();
        replacement.requested_action = decision.requested_action.clone();
        replacement.trigger_key = Some(decision.trigger_key.clone());
        replacement.reasoning_input_json = Some(reasoning_input_json);
        replacement.decision_summary = decision.summary.clone();
        replacement.confidence_percent = Some(decision.confidence_percent);

        let mut superseded = existing.into_iter().rev().find(|intervention| {
            intervention.trigger_key.as_deref() == Some(decision.trigger_key.as_str())
                && matches!(
                    intervention.state,
                    InterventionState::Proposed
                        | InterventionState::Delivered
                        | InterventionState::Acknowledged
                )
        });
        if let Some(previous) = superseded.as_mut() {
            replacement.supersedes = Some(previous.id.clone());
            replacement.supersession_history = previous.supersession_history.clone();
            replacement.supersession_history.push(previous.id.clone());
            previous.transition(
                InterventionState::Superseded,
                now,
                Some(format!("Superseded by {}", replacement.id.as_str())),
            )?;
        }
        self.store
            .persist_intervention_revision(&replacement, superseded.as_ref())?;
        Ok(replacement)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorTargetedAction {
    pub idempotency_key: String,
    pub intervention_id: InterventionId,
    pub goal_id: String,
    pub target_worker_id: WorkerId,
    pub level: InterventionLevel,
    pub summary: String,
    pub evidence_event_ids: Vec<EventId>,
    pub evidence_refs: Vec<String>,
    pub recommendation: String,
    pub requested_action: Option<String>,
}

pub trait SupervisorActionSink: Send + Sync {
    fn deliver(&self, action: &SupervisorTargetedAction) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorExecutionDisposition {
    NoAction,
    Delivered,
    AwaitingHumanEscalation,
    AlreadyProcessed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorExecutionReport {
    pub intervention_id: InterventionId,
    pub disposition: SupervisorExecutionDisposition,
    pub delivered_worker_ids: Vec<WorkerId>,
}

#[derive(Debug, Error)]
pub enum SupervisorExecutionError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("Supervisor intervention {0} was not found")]
    NotFound(String),
    #[error("Supervisor delivery failed for worker {worker_id}: {message}")]
    Delivery { worker_id: String, message: String },
    #[error("invalid persisted Supervisor intervention: {0}")]
    InvalidIntervention(String),
}

#[derive(Clone)]
pub struct SupervisorActionExecutor {
    store: Arc<SqliteCoordinationStore>,
    sink: Arc<dyn SupervisorActionSink>,
}

impl SupervisorActionExecutor {
    pub fn new(store: Arc<SqliteCoordinationStore>, sink: Arc<dyn SupervisorActionSink>) -> Self {
        Self { store, sink }
    }

    pub fn execute(
        &self,
        goal_id: &str,
        intervention_id: &InterventionId,
        now: DateTime<Utc>,
    ) -> Result<SupervisorExecutionReport, SupervisorExecutionError> {
        let mut intervention = self
            .store
            .interventions_for_goal(goal_id, None)?
            .into_iter()
            .find(|candidate| &candidate.id == intervention_id)
            .ok_or_else(|| SupervisorExecutionError::NotFound(intervention_id.as_str().into()))?;
        if intervention.state != InterventionState::Proposed {
            return Ok(SupervisorExecutionReport {
                intervention_id: intervention.id,
                disposition: SupervisorExecutionDisposition::AlreadyProcessed,
                delivered_worker_ids: Vec::new(),
            });
        }
        match intervention.level {
            InterventionLevel::Observe => {
                intervention.transition(
                    InterventionState::Applied,
                    now,
                    Some("No outward action was justified".into()),
                )?;
                self.store.upsert_intervention(&intervention)?;
                Ok(SupervisorExecutionReport {
                    intervention_id: intervention.id,
                    disposition: SupervisorExecutionDisposition::NoAction,
                    delivered_worker_ids: Vec::new(),
                })
            }
            InterventionLevel::Escalate => Ok(SupervisorExecutionReport {
                intervention_id: intervention.id,
                disposition: SupervisorExecutionDisposition::AwaitingHumanEscalation,
                delivered_worker_ids: Vec::new(),
            }),
            InterventionLevel::Inform
            | InterventionLevel::Recommend
            | InterventionLevel::Coordinate
            | InterventionLevel::Block => {
                if intervention.target_workers.is_empty() {
                    return Err(SupervisorExecutionError::InvalidIntervention(
                        "targeted actions require at least one affected worker".into(),
                    ));
                }
                if intervention.level == InterventionLevel::Block
                    && (intervention.confidence_percent.unwrap_or(0) < 90
                        || intervention.requested_action.is_none())
                {
                    return Err(SupervisorExecutionError::InvalidIntervention(
                        "blocks require at least 90% confidence and a concrete requested action"
                            .into(),
                    ));
                }
                let mut delivered = Vec::with_capacity(intervention.target_workers.len());
                for worker_id in &intervention.target_workers {
                    let action = SupervisorTargetedAction {
                        idempotency_key: format!(
                            "supervisor-action:{}:{}",
                            intervention.id.as_str(),
                            worker_id.as_str()
                        ),
                        intervention_id: intervention.id.clone(),
                        goal_id: intervention.goal_id.clone(),
                        target_worker_id: worker_id.clone(),
                        level: intervention.level,
                        summary: intervention.decision_summary.clone(),
                        evidence_event_ids: intervention.signal_event_ids.clone(),
                        evidence_refs: intervention.evidence_refs.clone(),
                        recommendation: intervention.recommendation.clone(),
                        requested_action: intervention.requested_action.clone(),
                    };
                    self.sink.deliver(&action).map_err(|message| {
                        SupervisorExecutionError::Delivery {
                            worker_id: worker_id.as_str().into(),
                            message,
                        }
                    })?;
                    delivered.push(worker_id.clone());
                }
                intervention.transition(
                    InterventionState::Delivered,
                    now,
                    Some(format!(
                        "Delivered {} to {} affected worker(s)",
                        intervention_level_name(intervention.level),
                        delivered.len()
                    )),
                )?;
                self.store.upsert_intervention(&intervention)?;
                Ok(SupervisorExecutionReport {
                    intervention_id: intervention.id,
                    disposition: SupervisorExecutionDisposition::Delivered,
                    delivered_worker_ids: delivered,
                })
            }
        }
    }
}

fn intervention_level_name(level: InterventionLevel) -> &'static str {
    match level {
        InterventionLevel::Observe => "observe",
        InterventionLevel::Inform => "inform",
        InterventionLevel::Recommend => "recommend",
        InterventionLevel::Coordinate => "coordinate",
        InterventionLevel::Block => "block",
        InterventionLevel::Escalate => "escalate",
    }
}

#[derive(Debug, Clone)]
pub struct SupervisorTriggerService {
    store: Arc<SqliteCoordinationStore>,
    policy: SupervisorTriggerPolicy,
}

impl SupervisorTriggerService {
    pub fn new(
        store: Arc<SqliteCoordinationStore>,
        policy: SupervisorTriggerPolicy,
    ) -> Result<Self, SupervisorTriggerError> {
        if policy.minimum_signal_confidence_percent > 100 {
            return Err(SupervisorTriggerError::InvalidPolicy(
                "signal confidence must be between 0 and 100".into(),
            ));
        }
        if policy.budget_window_seconds == 0
            || policy.budget_window_seconds > i64::MAX as u64
            || policy.max_evaluations_per_window == 0
            || policy.max_tokens_per_window == 0
            || policy.max_tokens_per_evaluation == 0
            || policy.max_tokens_per_evaluation > policy.max_tokens_per_window
            || policy.consecutive_failure_limit == 0
            || policy.circuit_breaker_seconds == 0
            || policy.circuit_breaker_seconds > i64::MAX as u64
        {
            return Err(SupervisorTriggerError::InvalidPolicy(
                "Supervisor budgets must be positive, bounded, and fit within the window".into(),
            ));
        }
        Ok(Self { store, policy })
    }

    pub fn eligible_triggers(
        &self,
        goal_id: &str,
        after_event_sequence: u64,
        now: DateTime<Utc>,
    ) -> Result<SupervisorTriggerBatch, SupervisorTriggerError> {
        let mut triggers = BTreeMap::<String, SupervisorTrigger>::new();
        for signal in self
            .store
            .signals_for_goal(goal_id, Some(SignalState::Active))?
        {
            if signal.expires_at <= now
                || signal.confidence_percent < self.policy.minimum_signal_confidence_percent
                || signal.severity.rank() < self.policy.minimum_signal_severity.rank()
            {
                continue;
            }
            let key = format!("signal:{}:{}", signal.id.as_str(), signal.occurrence_count);
            triggers.insert(
                key.clone(),
                SupervisorTrigger {
                    key,
                    goal_id: goal_id.into(),
                    kind: SupervisorTriggerKind::Signal,
                    severity: signal.severity,
                    confidence_percent: signal.confidence_percent,
                    occurred_at: signal.last_seen_at,
                    signal_id: Some(signal.id),
                    request_event_id: None,
                    escalation_id: None,
                    evidence_event_ids: signal.source_event_ids,
                    affected_worker_ids: signal.affected_worker_ids,
                },
            );
        }

        let events = self.store.events_for_goal(
            goal_id,
            after_event_sequence,
            None,
            MAX_TRIGGER_EVENT_SCAN,
        )?;
        let last_event_sequence = events
            .last()
            .and_then(|event| event.sequence)
            .unwrap_or(after_event_sequence);
        for event in events {
            let Some(CoordinationEventPayload::CoordinationRequest(payload)) =
                event.typed_payload()?
            else {
                continue;
            };
            let key = format!("coordination-request:{}", event.id.as_str());
            triggers.insert(
                key.clone(),
                SupervisorTrigger {
                    key,
                    goal_id: goal_id.into(),
                    kind: SupervisorTriggerKind::CoordinationRequest,
                    severity: event.severity,
                    confidence_percent: 100,
                    occurred_at: event.occurred_at,
                    signal_id: None,
                    request_event_id: Some(event.id.clone()),
                    escalation_id: None,
                    evidence_event_ids: vec![event.id],
                    affected_worker_ids: vec![payload.requester_worker_id],
                },
            );
        }

        for escalation in self.store.escalations_for_goal(goal_id, None)? {
            if !matches!(
                escalation.state,
                EscalationState::Open | EscalationState::Acknowledged
            ) {
                continue;
            }
            let key = format!(
                "escalation:{}:{}",
                escalation.id.as_str(),
                escalation.metadata.updated_at.timestamp_millis()
            );
            triggers.insert(
                key.clone(),
                SupervisorTrigger {
                    key,
                    goal_id: goal_id.into(),
                    kind: SupervisorTriggerKind::UnresolvedEscalation,
                    severity: escalation_event_severity(escalation.severity),
                    confidence_percent: 100,
                    occurred_at: escalation.metadata.updated_at,
                    signal_id: None,
                    request_event_id: None,
                    escalation_id: Some(escalation.id),
                    evidence_event_ids: vec![],
                    affected_worker_ids: vec![],
                },
            );
        }

        let mut triggers = triggers.into_values().collect::<Vec<_>>();
        triggers.sort_by(|left, right| {
            right
                .severity
                .rank()
                .cmp(&left.severity.rank())
                .then_with(|| right.confidence_percent.cmp(&left.confidence_percent))
                .then_with(|| left.occurred_at.cmp(&right.occurred_at))
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(SupervisorTriggerBatch {
            goal_id: goal_id.into(),
            after_event_sequence,
            last_event_sequence,
            triggers,
        })
    }

    pub fn dispatch(
        &self,
        goal_id: &str,
        after_event_sequence: u64,
        now: DateTime<Utc>,
        sink: &dyn SupervisorEvaluationSink,
    ) -> Result<SupervisorDispatchReport, SupervisorTriggerError> {
        let batch = self.eligible_triggers(goal_id, after_event_sequence, now)?;
        let mut dispatched_trigger_keys = Vec::new();
        let mut failures = Vec::new();
        let mut skipped = Vec::new();
        let mut budget = self
            .store
            .supervisor_budget(goal_id)?
            .unwrap_or_else(|| SupervisorBudgetLedger::new(goal_id, now));
        let window = chrono::Duration::seconds(self.policy.budget_window_seconds as i64);
        if now < budget.window_started_at || now - budget.window_started_at >= window {
            budget.window_started_at = now;
            budget.evaluation_count = 0;
            budget.consumed_tokens = 0;
            budget.consecutive_failures = 0;
            budget.circuit_open_until = None;
            budget.attempts_by_trigger.clear();
            budget.completed_trigger_keys.clear();
        } else if budget
            .circuit_open_until
            .is_some_and(|open_until| open_until <= now)
        {
            budget.circuit_open_until = None;
            budget.consecutive_failures = 0;
        }
        for trigger in &batch.triggers {
            let skip_reason = if budget
                .completed_trigger_keys
                .iter()
                .any(|key| key == &trigger.key)
            {
                Some("trigger already evaluated successfully in this budget window".into())
            } else if budget
                .circuit_open_until
                .is_some_and(|open_until| open_until > now)
            {
                Some("Supervisor circuit breaker is open".into())
            } else if budget.evaluation_count >= self.policy.max_evaluations_per_window {
                Some("Supervisor evaluation frequency budget is exhausted".into())
            } else if budget.consumed_tokens >= self.policy.max_tokens_per_window {
                Some("Supervisor token budget is exhausted".into())
            } else if budget
                .attempts_by_trigger
                .get(&trigger.key)
                .copied()
                .unwrap_or(0)
                > self.policy.max_retries_per_trigger
            {
                Some("Supervisor retry budget is exhausted for this trigger".into())
            } else {
                None
            };
            if let Some(reason) = skip_reason {
                skipped.push(SupervisorDispatchSkipped {
                    trigger_key: trigger.key.clone(),
                    reason,
                });
                continue;
            }

            let remaining_tokens = self
                .policy
                .max_tokens_per_window
                .saturating_sub(budget.consumed_tokens);
            let allowance = remaining_tokens.min(self.policy.max_tokens_per_evaluation);
            budget.evaluation_count = budget.evaluation_count.saturating_add(1);
            *budget
                .attempts_by_trigger
                .entry(trigger.key.clone())
                .or_default() += 1;
            match sink.evaluate(
                trigger,
                SupervisorEvaluationBudget {
                    max_total_tokens: allowance,
                },
            ) {
                Ok(usage) if usage.total() <= allowance => {
                    budget.consumed_tokens = budget.consumed_tokens.saturating_add(usage.total());
                    budget.consecutive_failures = 0;
                    budget.completed_trigger_keys.push(trigger.key.clone());
                    dispatched_trigger_keys.push(trigger.key.clone());
                }
                Ok(usage) => {
                    budget.consumed_tokens = budget.consumed_tokens.saturating_add(usage.total());
                    budget.consecutive_failures = budget.consecutive_failures.saturating_add(1);
                    failures.push(SupervisorDispatchFailure {
                        trigger_key: trigger.key.clone(),
                        message: format!(
                            "evaluation used {} tokens, exceeding its {} token allowance",
                            usage.total(),
                            allowance
                        ),
                    });
                }
                Err(message) => {
                    budget.consecutive_failures = budget.consecutive_failures.saturating_add(1);
                    failures.push(SupervisorDispatchFailure {
                        trigger_key: trigger.key.clone(),
                        message,
                    });
                }
            }
            if budget.consecutive_failures >= self.policy.consecutive_failure_limit {
                budget.circuit_open_until = Some(
                    now + chrono::Duration::seconds(self.policy.circuit_breaker_seconds as i64),
                );
            }
            budget.touch(now);
            self.store.upsert_supervisor_budget(&budget)?;
        }
        budget.touch(now);
        self.store.upsert_supervisor_budget(&budget)?;
        Ok(SupervisorDispatchReport {
            batch,
            dispatched_trigger_keys,
            failures,
            skipped,
            budget,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SupervisorContextBuilder {
    store: Arc<SqliteCoordinationStore>,
    inventories: ChangeInventoryCollector,
}

impl SupervisorContextBuilder {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            inventories: ChangeInventoryCollector::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn build(
        &self,
        goal_id: &str,
        trigger: &SupervisorTrigger,
        now: DateTime<Utc>,
    ) -> Result<SupervisorContextPacket, SupervisorContextError> {
        if trigger.goal_id != goal_id {
            return Err(SupervisorContextError::GoalMismatch {
                trigger_goal: trigger.goal_id.clone(),
                requested_goal: goal_id.into(),
            });
        }
        let all_claims = self.store.claims_for_goal(goal_id, None)?;
        let all_workers = self.store.workers_for_goal(goal_id, None)?;
        let all_escalations = self.store.escalations_for_goal(goal_id, None)?;
        let mut worker_ids = trigger
            .affected_worker_ids
            .iter()
            .map(|id| (id.as_str().to_string(), id.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut claim_ids = BTreeMap::<String, ClaimId>::new();
        let mut selected_escalation = None;
        if let Some(signal_id) = &trigger.signal_id
            && let Some(signal) = self.store.signal(signal_id)?
        {
            for worker_id in signal.affected_worker_ids {
                worker_ids.insert(worker_id.as_str().into(), worker_id);
            }
            for claim_id in signal.affected_claim_ids {
                claim_ids.insert(claim_id.as_str().into(), claim_id);
            }
        }
        if let Some(event_id) = &trigger.request_event_id
            && let Some(event) = self.store.event(event_id)?
            && let Some(CoordinationEventPayload::CoordinationRequest(payload)) =
                event.typed_payload()?
        {
            worker_ids.insert(
                payload.requester_worker_id.as_str().into(),
                payload.requester_worker_id,
            );
            claim_ids.insert(payload.claim_id.as_str().into(), payload.claim_id);
        }
        if let Some(escalation_id) = &trigger.escalation_id
            && let Some(escalation) = all_escalations
                .iter()
                .find(|item| &item.id == escalation_id)
                .cloned()
        {
            add_escalation_scope(
                &escalation.scope,
                &all_claims,
                &all_workers,
                &mut claim_ids,
                &mut worker_ids,
            );
            selected_escalation = Some(escalation);
        }
        for worker in &all_workers {
            if worker_ids.contains_key(worker.id.as_str()) {
                for claim_id in &worker.active_claims {
                    claim_ids.insert(claim_id.as_str().into(), claim_id.clone());
                }
            }
        }
        for claim in &all_claims {
            if claim_ids.contains_key(claim.id.as_str()) {
                worker_ids.insert(claim.owner.as_str().into(), claim.owner.clone());
            }
        }
        let mut truncated =
            worker_ids.len() > MAX_CONTEXT_WORKERS || claim_ids.len() > MAX_CONTEXT_CLAIMS;
        let worker_ids = worker_ids
            .into_values()
            .take(MAX_CONTEXT_WORKERS)
            .collect::<Vec<_>>();
        let claim_ids = claim_ids
            .into_values()
            .take(MAX_CONTEXT_CLAIMS)
            .collect::<Vec<_>>();

        let workers = all_workers
            .iter()
            .filter(|worker| worker_ids.contains(&worker.id))
            .map(worker_context)
            .collect::<Vec<_>>();
        let claims = all_claims
            .iter()
            .filter(|claim| claim_ids.contains(&claim.id))
            .map(claim_context)
            .collect::<Vec<_>>();
        let mut repository_changes = Vec::new();
        for claim in all_claims
            .iter()
            .filter(|claim| claim_ids.contains(&claim.id) && claim.state == ClaimState::Active)
            .take(MAX_CONTEXT_CHANGES)
        {
            repository_changes.push(
                match self.inventories.collect(&claim.owner, &claim.id, now) {
                    Ok(inventory) => SupervisorRepositoryChangeContext {
                        worker_id: inventory.worker_id,
                        claim_id: inventory.claim_id,
                        repository_id: Some(bounded_text(&inventory.repository_id, 256)),
                        base_revision: bounded_text(&inventory.base_revision, 256),
                        head_revision: Some(bounded_text(&inventory.head_revision, 256)),
                        patch_sha256: Some(inventory.diff.patch_sha256),
                        changed_paths: inventory
                            .changed_files
                            .into_iter()
                            .take(16)
                            .map(|file| bounded_text(&file.path, 256))
                            .collect(),
                        changed_symbols: inventory
                            .changed_symbols
                            .into_iter()
                            .take(16)
                            .map(|symbol| {
                                bounded_text(
                                    &format!("{}:{}:{}", symbol.path, symbol.kind, symbol.name),
                                    384,
                                )
                            })
                            .collect(),
                        migrations: inventory
                            .migrations
                            .into_iter()
                            .take(8)
                            .map(|item| {
                                bounded_text(&format!("{}:{}", item.classification, item.path), 384)
                            })
                            .collect(),
                        validation_summaries: inventory
                            .validation_results
                            .into_iter()
                            .take(8)
                            .map(|item| bounded_text(&item.summary, 500))
                            .collect(),
                        unavailable_reason: None,
                        truncated: inventory.truncated,
                    },
                    Err(error) => SupervisorRepositoryChangeContext {
                        worker_id: claim.owner.clone(),
                        claim_id: claim.id.clone(),
                        repository_id: all_workers
                            .iter()
                            .find(|worker| worker.id == claim.owner)
                            .and_then(|worker| worker.workspace.as_ref())
                            .map(|workspace| bounded_text(&workspace.repository_id, 256)),
                        base_revision: bounded_text(&claim.base_revision, 256),
                        head_revision: None,
                        patch_sha256: None,
                        changed_paths: vec![],
                        changed_symbols: vec![],
                        migrations: vec![],
                        validation_summaries: vec![],
                        unavailable_reason: Some(bounded_text(&error.to_string(), 500)),
                        truncated: false,
                    },
                },
            );
        }
        if all_claims
            .iter()
            .filter(|claim| claim_ids.contains(&claim.id) && claim.state == ClaimState::Active)
            .count()
            > MAX_CONTEXT_CHANGES
        {
            truncated = true;
        }

        let mut relevant_contract_keys = BTreeSet::<String>::new();
        for claim in all_claims
            .iter()
            .filter(|claim| claim_ids.contains(&claim.id))
        {
            relevant_contract_keys.extend(claim.produces_contracts.iter().cloned());
            relevant_contract_keys.extend(
                claim
                    .consumes_contracts
                    .iter()
                    .map(|expectation| expectation.contract_id.clone()),
            );
        }
        if let Some(escalation) = &selected_escalation
            && let EscalationScope::Contract { contract_id } = &escalation.scope
        {
            relevant_contract_keys.insert(contract_id.as_str().into());
        }
        let all_contracts = self.store.contracts_for_goal(goal_id)?;
        let relevant_contracts = all_contracts
            .iter()
            .filter(|contract| {
                relevant_contract_keys.contains(contract.id.as_str())
                    || relevant_contract_keys.contains(&contract.stable_key)
            })
            .collect::<Vec<_>>();
        if relevant_contracts.len() > MAX_CONTEXT_CONTRACTS {
            truncated = true;
        }
        let contracts = relevant_contracts
            .into_iter()
            .take(MAX_CONTEXT_CONTRACTS)
            .map(|contract| SupervisorContractContext {
                contract_id: contract.id.clone(),
                stable_key: bounded_text(&contract.stable_key, 256),
                title: bounded_text(&contract.title, 500),
                kind: contract.kind.clone(),
                current_revision: contract.revision,
                expected_revisions: all_claims
                    .iter()
                    .filter(|claim| claim_ids.contains(&claim.id))
                    .flat_map(|claim| {
                        claim.consumes_contracts.iter().filter_map(|expectation| {
                            (expectation.contract_id == contract.id.as_str()
                                || expectation.contract_id == contract.stable_key)
                                .then_some(SupervisorContractExpectation {
                                    claim_id: claim.id.clone(),
                                    expected_revision: expectation.expected_revision,
                                })
                        })
                    })
                    .collect(),
                compatibility_notes: bounded_text(&contract.compatibility_notes, 1_000),
                updated_at: contract.metadata.updated_at,
            })
            .collect::<Vec<_>>();

        let mut failure_events = BTreeMap::<String, CoordinationEvent>::new();
        for event_id in &trigger.evidence_event_ids {
            if let Some(event) = self.store.event(event_id)? {
                failure_events.insert(event.id.as_str().into(), event);
            }
        }
        for claim_id in &claim_ids {
            for event in self
                .store
                .latest_events_for_goal(goal_id, Some(claim_id.as_str()), 20)?
            {
                if is_failure_event(&event) {
                    failure_events.insert(event.id.as_str().into(), event);
                }
            }
        }
        if failure_events.len() > MAX_CONTEXT_FAILURES {
            truncated = true;
        }
        let failure_events = failure_events
            .into_values()
            .rev()
            .take(MAX_CONTEXT_FAILURES)
            .map(|event| event_context(&event))
            .collect::<Result<Vec<_>, _>>()?;

        let evidence_ids = trigger
            .evidence_event_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<BTreeSet<_>>();
        let mut relevant_interventions = self
            .store
            .interventions_for_goal(goal_id, None)?
            .into_iter()
            .filter(|intervention| {
                intervention
                    .target_workers
                    .iter()
                    .any(|id| worker_ids.contains(id))
                    || intervention
                        .signal_event_ids
                        .iter()
                        .any(|id| evidence_ids.contains(id.as_str()))
            })
            .collect::<Vec<_>>();
        relevant_interventions.sort_by(|left, right| {
            right
                .metadata
                .updated_at
                .cmp(&left.metadata.updated_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        if relevant_interventions.len() > MAX_CONTEXT_INTERVENTIONS {
            truncated = true;
        }
        let prior_interventions = relevant_interventions
            .into_iter()
            .take(MAX_CONTEXT_INTERVENTIONS)
            .map(intervention_context)
            .collect();

        let mut relevant_escalations = all_escalations
            .into_iter()
            .filter(|escalation| {
                matches!(
                    escalation.state,
                    EscalationState::Open | EscalationState::Acknowledged
                ) && escalation_scope_relevant(
                    &escalation.scope,
                    &claim_ids,
                    &worker_ids,
                    &all_claims,
                    &all_workers,
                    &relevant_contract_keys,
                )
            })
            .collect::<Vec<_>>();
        relevant_escalations.sort_by(|left, right| {
            right
                .metadata
                .updated_at
                .cmp(&left.metadata.updated_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        if relevant_escalations.len() > MAX_CONTEXT_ESCALATIONS {
            truncated = true;
        }
        let open_escalations = relevant_escalations
            .into_iter()
            .take(MAX_CONTEXT_ESCALATIONS)
            .map(escalation_context)
            .collect();

        let mut packet = SupervisorContextPacket {
            schema_version: 1,
            goal_id: goal_id.into(),
            generated_at: now,
            trigger: trigger.clone(),
            workers,
            claims,
            repository_changes,
            contracts,
            failure_events,
            prior_interventions,
            open_escalations,
            truncated,
        };
        if serde_json::to_vec(&packet).map_err(StoreError::from)?.len()
            > MAX_SUPERVISOR_CONTEXT_BYTES
        {
            packet.truncated = true;
            packet.repository_changes.truncate(4);
            packet.failure_events.truncate(16);
            packet.prior_interventions.truncate(8);
            packet.open_escalations.truncate(8);
        }
        if serde_json::to_vec(&packet).map_err(StoreError::from)?.len()
            > MAX_SUPERVISOR_CONTEXT_BYTES
        {
            return Err(SupervisorContextError::ContextTooLarge);
        }
        Ok(packet)
    }
}

fn add_escalation_scope(
    scope: &EscalationScope,
    claims: &[super::domain::Claim],
    workers: &[super::domain::Worker],
    claim_ids: &mut BTreeMap<String, ClaimId>,
    worker_ids: &mut BTreeMap<String, WorkerId>,
) {
    match scope {
        EscalationScope::Claim { claim_id } => {
            claim_ids.insert(claim_id.as_str().into(), claim_id.clone());
        }
        EscalationScope::WorkPackage { work_package_id } => {
            for claim in claims {
                if matches!(
                    &claim.scope,
                    ClaimScope::WorkPackage { work_package_id: id } if id == work_package_id
                ) {
                    claim_ids.insert(claim.id.as_str().into(), claim.id.clone());
                }
            }
        }
        EscalationScope::Contract { contract_id } => {
            for claim in claims {
                if claim
                    .produces_contracts
                    .iter()
                    .any(|id| id == contract_id.as_str())
                    || claim
                        .consumes_contracts
                        .iter()
                        .any(|item| item.contract_id == contract_id.as_str())
                {
                    claim_ids.insert(claim.id.as_str().into(), claim.id.clone());
                }
            }
        }
        EscalationScope::IntegrationLane { repository_id } => {
            for worker in workers {
                if worker
                    .workspace
                    .as_ref()
                    .is_some_and(|workspace| workspace.repository_id == *repository_id)
                {
                    worker_ids.insert(worker.id.as_str().into(), worker.id.clone());
                }
            }
        }
        EscalationScope::Goal { .. } => {
            for worker in workers.iter().filter(|worker| !worker.state.is_terminal()) {
                worker_ids.insert(worker.id.as_str().into(), worker.id.clone());
            }
        }
    }
}

fn worker_context(worker: &super::domain::Worker) -> SupervisorWorkerContext {
    SupervisorWorkerContext {
        worker_id: worker.id.clone(),
        state: worker.state,
        updated_at: worker.metadata.updated_at,
        current_run_id: worker
            .current_run_id
            .as_deref()
            .map(|value| bounded_text(value, 256)),
        current_thread_id: worker
            .current_thread_id
            .as_deref()
            .map(|value| bounded_text(value, 256)),
        active_claim_ids: worker.active_claims.iter().take(32).cloned().collect(),
        repository_id: worker
            .workspace
            .as_ref()
            .map(|workspace| bounded_text(&workspace.repository_id, 256)),
        worktree_path: worker
            .workspace
            .as_ref()
            .map(|workspace| bounded_text(&workspace.worktree_path, 512)),
        base_revision: worker
            .workspace
            .as_ref()
            .map(|workspace| bounded_text(&workspace.base_revision, 256)),
        last_heartbeat_at: worker.last_heartbeat_at,
    }
}

fn claim_context(claim: &super::domain::Claim) -> SupervisorClaimContext {
    SupervisorClaimContext {
        claim_id: claim.id.clone(),
        owner_worker_id: claim.owner.clone(),
        state: claim.state,
        scope: claim.scope.clone(),
        updated_at: claim.metadata.updated_at,
        base_revision: bounded_text(&claim.base_revision, 256),
        lease_generation: claim.lease_generation,
        lease_expires_at: claim.lease_expires_at,
        produced_contract_keys: claim
            .produces_contracts
            .iter()
            .take(32)
            .map(|value| bounded_text(value, 256))
            .collect(),
        expected_contract_revisions: claim
            .consumes_contracts
            .iter()
            .take(32)
            .map(|expectation| SupervisorExpectedContractRevision {
                contract_id_or_key: bounded_text(&expectation.contract_id, 256),
                expected_revision: expectation.expected_revision,
            })
            .collect(),
    }
}

fn is_failure_event(event: &CoordinationEvent) -> bool {
    matches!(
        event.kind,
        CoordinationEventKind::DependencyChanged
            | CoordinationEventKind::MigrationOverlapDetected
            | CoordinationEventKind::MergeConflictPredicted
            | CoordinationEventKind::WorkerStalled
            | CoordinationEventKind::RepeatedValidationFailure
            | CoordinationEventKind::WatchdogActionApplied
            | CoordinationEventKind::BlockerRaised
    )
}

fn event_context(event: &CoordinationEvent) -> Result<SupervisorEventContext, DomainError> {
    let summary = match event.typed_payload()? {
        Some(CoordinationEventPayload::ContractMismatch(payload)) => format!(
            "contract {} expected revision {} but found {}",
            payload.stable_key, payload.expected_revision, payload.current_revision
        ),
        Some(CoordinationEventPayload::Dependency(payload)) => format!(
            "dependency {} changed from {} to {}",
            payload.dependency_key, payload.previous_revision, payload.revision
        ),
        Some(CoordinationEventPayload::MigrationOverlap(payload)) => format!(
            "{} migration class overlap(s) between claims",
            payload.overlaps.len()
        ),
        Some(CoordinationEventPayload::MergeConflict(payload)) => format!(
            "predicted merge conflict in {} path(s)",
            payload.conflicting_paths.len()
        ),
        Some(CoordinationEventPayload::WorkerStalled(payload)) => format!(
            "worker heartbeat stale for {} seconds",
            payload.heartbeat_age_seconds
        ),
        Some(CoordinationEventPayload::RepeatedValidationFailure(payload)) => format!(
            "{} validation failures in {} seconds",
            payload.failure_event_ids.len(),
            payload.window_seconds
        ),
        Some(CoordinationEventPayload::WatchdogAction(payload)) => payload.reason,
        Some(CoordinationEventPayload::Blocker(payload)) => payload.summary,
        Some(CoordinationEventPayload::Claim(payload)) => payload.reason,
        _ => format!("{:?}", event.kind),
    };
    Ok(SupervisorEventContext {
        event_id: event.id.clone(),
        kind: event.kind.clone(),
        severity: event.severity,
        occurred_at: event.occurred_at,
        summary: bounded_text(&summary, 1_000),
    })
}

fn intervention_context(
    intervention: super::domain::SupervisorIntervention,
) -> SupervisorInterventionContext {
    SupervisorInterventionContext {
        intervention_id: intervention.id,
        level: intervention.level,
        state: intervention.state,
        updated_at: intervention.metadata.updated_at,
        target_worker_ids: intervention.target_workers.into_iter().take(16).collect(),
        evidence_refs: intervention
            .evidence_refs
            .into_iter()
            .take(16)
            .map(|value| bounded_text(&value, 500))
            .collect(),
        recommendation: bounded_text(&intervention.recommendation, 1_000),
        requested_action: intervention
            .requested_action
            .map(|value| bounded_text(&value, 1_000)),
        outcome: intervention
            .outcome
            .map(|value| bounded_text(&value, 1_000)),
    }
}

fn escalation_context(escalation: super::domain::HumanEscalation) -> SupervisorEscalationContext {
    SupervisorEscalationContext {
        escalation_id: escalation.id,
        kind: escalation.kind,
        severity: escalation.severity,
        state: escalation.state,
        scope: escalation.scope,
        updated_at: escalation.metadata.updated_at,
        summary: bounded_text(&escalation.summary, 1_000),
        evidence_refs: escalation
            .evidence_refs
            .into_iter()
            .take(16)
            .map(|value| bounded_text(&value, 500))
            .collect(),
    }
}

fn escalation_scope_relevant(
    scope: &EscalationScope,
    claim_ids: &[ClaimId],
    worker_ids: &[WorkerId],
    claims: &[super::domain::Claim],
    workers: &[super::domain::Worker],
    contract_keys: &BTreeSet<String>,
) -> bool {
    match scope {
        EscalationScope::Claim { claim_id } => claim_ids.contains(claim_id),
        EscalationScope::WorkPackage { work_package_id } => claims.iter().any(|claim| {
            claim_ids.contains(&claim.id)
                && matches!(
                    &claim.scope,
                    ClaimScope::WorkPackage { work_package_id: id } if id == work_package_id
                )
        }),
        EscalationScope::Contract { contract_id } => contract_keys.contains(contract_id.as_str()),
        EscalationScope::IntegrationLane { repository_id } => workers.iter().any(|worker| {
            worker_ids.contains(&worker.id)
                && worker
                    .workspace
                    .as_ref()
                    .is_some_and(|workspace| workspace.repository_id == *repository_id)
        }),
        EscalationScope::Goal { .. } => true,
    }
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn escalation_event_severity(severity: EscalationSeverity) -> EventSeverity {
    match severity {
        EscalationSeverity::Low => EventSeverity::Info,
        EscalationSeverity::Medium => EventSeverity::Warning,
        EscalationSeverity::High => EventSeverity::Error,
        EscalationSeverity::Critical => EventSeverity::Critical,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::claims::{ClaimPolicy, ClaimService, CompletionEvidence};
    use crate::coordination::detectors::{
        WatchdogOutcomeKind, WatchdogPolicy, WatchdogService, WorkerRuntimeObservation,
    };
    use crate::coordination::domain::{
        ActivityCategory, ActivityEventPayload, BlockerEventPayload, Claim, ClaimId, ClaimScope,
        ContractExpectation, ContractKind, CoordinationActor, CoordinationEvent,
        CoordinationRequestEventPayload, CoordinationSignal, EscalationDecision, EscalationKind,
        EscalationScope, HumanEscalation, SharedContract, SignalKind, SupervisorIntervention,
        Worker, WorkerPermissionProfile,
    };
    use crate::coordination::store::{
        ClaimRepository, ContractRepository, InterventionRepository, WorkerRepository,
    };
    use crate::coordination::workspace::WorktreeManager;
    use crate::models::Status;
    use crate::tracker::Tracker;
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        keys: Mutex<Vec<String>>,
    }

    impl SupervisorEvaluationSink for RecordingSink {
        fn evaluate(
            &self,
            trigger: &SupervisorTrigger,
            budget: SupervisorEvaluationBudget,
        ) -> Result<SupervisorEvaluationUsage, String> {
            self.keys.lock().unwrap().push(trigger.key.clone());
            Ok(SupervisorEvaluationUsage {
                input_tokens: budget.max_total_tokens.min(20),
                output_tokens: budget.max_total_tokens.saturating_sub(20).min(10),
            })
        }
    }

    #[derive(Default)]
    struct RecordingActionSink {
        actions: Mutex<Vec<SupervisorTargetedAction>>,
    }

    impl SupervisorActionSink for RecordingActionSink {
        fn deliver(&self, action: &SupervisorTargetedAction) -> Result<(), String> {
            self.actions.lock().unwrap().push(action.clone());
            Ok(())
        }
    }

    struct BudgetSink {
        usage: SupervisorEvaluationUsage,
        failure: Option<String>,
        allowances: Mutex<Vec<u64>>,
    }

    impl SupervisorEvaluationSink for BudgetSink {
        fn evaluate(
            &self,
            _trigger: &SupervisorTrigger,
            budget: SupervisorEvaluationBudget,
        ) -> Result<SupervisorEvaluationUsage, String> {
            self.allowances
                .lock()
                .unwrap()
                .push(budget.max_total_tokens);
            if let Some(message) = &self.failure {
                Err(message.clone())
            } else {
                Ok(self.usage)
            }
        }
    }

    #[test]
    fn dispatches_only_eligible_signals_requests_and_unresolved_escalations() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let worker_id = WorkerId::new();
        let claim_id = ClaimId::new();
        let low_signal = CoordinationSignal::new(
            "goal-a",
            "overlap:low",
            SignalKind::FileOverlap,
            60,
            EventSeverity::Info,
            EventId::new(),
            vec![worker_id.clone()],
            vec![claim_id.clone()],
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        let high_signal = CoordinationSignal::new(
            "goal-a",
            "overlap:high",
            SignalKind::MergeConflict,
            100,
            EventSeverity::Critical,
            EventId::new(),
            vec![worker_id.clone()],
            vec![claim_id.clone()],
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        store.merge_signal_observation(&low_signal, now).unwrap();
        store.merge_signal_observation(&high_signal, now).unwrap();

        let routine = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim_id.as_str(),
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker_id.clone(),
                claim_id: claim_id.clone(),
                category: ActivityCategory::Progress,
                summary: "routine progress".into(),
                progress_percent: Some(25),
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec![],
                validation_succeeded: None,
            }),
            now,
        )
        .unwrap();
        store.append_event(&routine).unwrap();
        let request = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Warning,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim_id.as_str(),
            CoordinationEventPayload::CoordinationRequest(CoordinationRequestEventPayload {
                claim_id: claim_id.clone(),
                requester_worker_id: worker_id.clone(),
                target_scope: "contract:api.v1".into(),
                desired_outcome: "choose a compatible response schema".into(),
                evidence_refs: vec!["contract:api.v1".into()],
            }),
            now + chrono::Duration::seconds(1),
        )
        .unwrap();
        let request_sequence = store.append_event(&request).unwrap();

        let open_escalation = HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: claim_id.clone(),
            },
            "Choose the public schema",
            now,
        );
        store.upsert_escalation(&open_escalation).unwrap();
        let mut resolved_escalation = HumanEscalation::new(
            "goal-a",
            EscalationKind::HardBlocker,
            EscalationSeverity::Critical,
            EscalationScope::Goal {
                goal_id: "goal-a".into(),
            },
            "Already resolved",
            now,
        );
        resolved_escalation
            .transition(
                EscalationState::Resolved,
                now + chrono::Duration::seconds(1),
                Some(EscalationDecision {
                    option_id: "resolved".into(),
                    decided_by: "user-a".into(),
                    accepted_risk: None,
                    decided_at: now + chrono::Duration::seconds(1),
                }),
            )
            .unwrap();
        store.upsert_escalation(&resolved_escalation).unwrap();

        let service =
            SupervisorTriggerService::new(Arc::clone(&store), SupervisorTriggerPolicy::default())
                .unwrap();
        let batch = service
            .eligible_triggers("goal-a", 0, now + chrono::Duration::seconds(2))
            .unwrap();
        assert_eq!(batch.last_event_sequence, request_sequence);
        assert_eq!(batch.triggers.len(), 3);
        assert_eq!(batch.triggers[0].signal_id, Some(high_signal.id.clone()));
        assert!(batch.triggers.iter().any(|trigger| {
            trigger.kind == SupervisorTriggerKind::CoordinationRequest
                && trigger.request_event_id == Some(request.id.clone())
        }));
        assert!(batch.triggers.iter().any(|trigger| {
            trigger.kind == SupervisorTriggerKind::UnresolvedEscalation
                && trigger.escalation_id == Some(open_escalation.id.clone())
        }));
        assert!(!batch.triggers.iter().any(|trigger| {
            trigger.signal_id == Some(low_signal.id.clone())
                || trigger.escalation_id == Some(resolved_escalation.id.clone())
                || trigger.request_event_id == Some(routine.id.clone())
        }));

        let sink = RecordingSink::default();
        let dispatched = service
            .dispatch("goal-a", 0, now + chrono::Duration::seconds(2), &sink)
            .unwrap();
        assert_eq!(dispatched.dispatched_trigger_keys.len(), 3);
        assert!(dispatched.failures.is_empty());
        assert_eq!(sink.keys.lock().unwrap().len(), 3);

        let after_request = service
            .eligible_triggers(
                "goal-a",
                request_sequence,
                now + chrono::Duration::seconds(2),
            )
            .unwrap();
        assert_eq!(after_request.triggers.len(), 2);
        assert!(
            after_request
                .triggers
                .iter()
                .all(|trigger| trigger.kind != SupervisorTriggerKind::CoordinationRequest)
        );
    }

    #[test]
    fn enforces_durable_frequency_token_retry_and_failure_budgets() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let now = Utc::now();
        let worker = WorkerId::new();
        let claim = ClaimId::new();
        let signal = CoordinationSignal::new(
            "goal-budget",
            "merge:budget",
            SignalKind::MergeConflict,
            99,
            EventSeverity::Critical,
            EventId::new(),
            vec![worker.clone()],
            vec![claim.clone()],
            now,
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        store.merge_signal_observation(&signal, now).unwrap();
        let escalation = HumanEscalation::new(
            "goal-budget",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: claim.clone(),
            },
            "Choose the compatibility policy",
            now,
        );
        store.upsert_escalation(&escalation).unwrap();
        let policy = SupervisorTriggerPolicy {
            budget_window_seconds: 60,
            max_evaluations_per_window: 1,
            max_tokens_per_window: 10,
            max_tokens_per_evaluation: 10,
            ..SupervisorTriggerPolicy::default()
        };
        let service = SupervisorTriggerService::new(store.clone(), policy.clone()).unwrap();
        let sink = BudgetSink {
            usage: SupervisorEvaluationUsage {
                input_tokens: 4,
                output_tokens: 2,
            },
            failure: None,
            allowances: Mutex::new(Vec::new()),
        };
        let first = service.dispatch("goal-budget", 0, now, &sink).unwrap();
        assert_eq!(first.dispatched_trigger_keys.len(), 1);
        assert_eq!(first.skipped.len(), 1);
        assert_eq!(first.budget.evaluation_count, 1);
        assert_eq!(first.budget.consumed_tokens, 6);
        assert_eq!(*sink.allowances.lock().unwrap(), vec![10]);

        let reopened = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let restarted = SupervisorTriggerService::new(reopened.clone(), policy).unwrap();
        let restart_sink = BudgetSink {
            usage: SupervisorEvaluationUsage {
                input_tokens: 1,
                output_tokens: 1,
            },
            failure: None,
            allowances: Mutex::new(Vec::new()),
        };
        let before_reset = restarted
            .dispatch(
                "goal-budget",
                0,
                now + chrono::Duration::seconds(30),
                &restart_sink,
            )
            .unwrap();
        assert!(before_reset.dispatched_trigger_keys.is_empty());
        assert_eq!(before_reset.skipped.len(), 2);
        assert!(restart_sink.allowances.lock().unwrap().is_empty());
        let after_reset = restarted
            .dispatch(
                "goal-budget",
                0,
                now + chrono::Duration::seconds(61),
                &restart_sink,
            )
            .unwrap();
        assert_eq!(after_reset.dispatched_trigger_keys.len(), 1);
        assert_eq!(after_reset.budget.evaluation_count, 1);

        let retry_signal = CoordinationSignal::new(
            "goal-retry",
            "merge:retry",
            SignalKind::MergeConflict,
            99,
            EventSeverity::Critical,
            EventId::new(),
            vec![worker.clone()],
            vec![claim.clone()],
            now,
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        reopened
            .merge_signal_observation(&retry_signal, now)
            .unwrap();
        let retry_service = SupervisorTriggerService::new(
            reopened.clone(),
            SupervisorTriggerPolicy {
                max_retries_per_trigger: 0,
                consecutive_failure_limit: 3,
                ..SupervisorTriggerPolicy::default()
            },
        )
        .unwrap();
        let failing_sink = BudgetSink {
            usage: SupervisorEvaluationUsage {
                input_tokens: 0,
                output_tokens: 0,
            },
            failure: Some("model unavailable".into()),
            allowances: Mutex::new(Vec::new()),
        };
        let failed = retry_service
            .dispatch("goal-retry", 0, now, &failing_sink)
            .unwrap();
        assert_eq!(failed.failures.len(), 1);
        let exhausted = retry_service
            .dispatch(
                "goal-retry",
                0,
                now + chrono::Duration::seconds(1),
                &failing_sink,
            )
            .unwrap();
        assert!(exhausted.failures.is_empty());
        assert_eq!(exhausted.skipped.len(), 1);
        assert!(exhausted.skipped[0].reason.contains("retry budget"));

        let circuit_signal = CoordinationSignal::new(
            "goal-circuit",
            "merge:circuit",
            SignalKind::MergeConflict,
            99,
            EventSeverity::Critical,
            EventId::new(),
            vec![worker.clone()],
            vec![claim.clone()],
            now,
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        reopened
            .merge_signal_observation(&circuit_signal, now)
            .unwrap();
        let circuit_escalation = HumanEscalation::new(
            "goal-circuit",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: claim.clone(),
            },
            "Choose a direction",
            now,
        );
        reopened.upsert_escalation(&circuit_escalation).unwrap();
        let circuit_service = SupervisorTriggerService::new(
            reopened.clone(),
            SupervisorTriggerPolicy {
                consecutive_failure_limit: 1,
                ..SupervisorTriggerPolicy::default()
            },
        )
        .unwrap();
        let circuit = circuit_service
            .dispatch("goal-circuit", 0, now, &failing_sink)
            .unwrap();
        assert_eq!(circuit.failures.len(), 1);
        assert_eq!(circuit.skipped.len(), 1);
        assert!(circuit.skipped[0].reason.contains("circuit breaker"));
        assert!(circuit.budget.circuit_open_until.is_some());

        let token_signal = CoordinationSignal::new(
            "goal-token",
            "merge:token",
            SignalKind::MergeConflict,
            99,
            EventSeverity::Critical,
            EventId::new(),
            vec![worker],
            vec![claim],
            now,
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        reopened
            .merge_signal_observation(&token_signal, now)
            .unwrap();
        let token_service = SupervisorTriggerService::new(
            reopened,
            SupervisorTriggerPolicy {
                max_tokens_per_window: 10,
                max_tokens_per_evaluation: 10,
                ..SupervisorTriggerPolicy::default()
            },
        )
        .unwrap();
        let excessive_sink = BudgetSink {
            usage: SupervisorEvaluationUsage {
                input_tokens: 8,
                output_tokens: 3,
            },
            failure: None,
            allowances: Mutex::new(Vec::new()),
        };
        let excessive = token_service
            .dispatch("goal-token", 0, now, &excessive_sink)
            .unwrap();
        assert_eq!(excessive.failures.len(), 1);
        assert!(excessive.failures[0].message.contains("exceeding"));
        assert_eq!(excessive.budget.consumed_tokens, 11);
    }

    #[test]
    fn supervisor_outage_does_not_gate_watchdog_safety_or_later_replay() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let tracker = Tracker::new(directory.path().join("tracker"));
        tracker
            .create_goal(
                "goal-outage",
                "Supervisor outage",
                "continue deterministically",
            )
            .unwrap();
        tracker
            .add_feature(
                "goal-outage",
                "unaffected-work",
                "Unaffected work",
                "must continue while the Supervisor is unavailable",
                Status::Planned,
            )
            .unwrap();
        let mut worker = Worker::new("goal-outage", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.permission_profile = Some(WorkerPermissionProfile::default());
        store.upsert_worker(&worker).unwrap();
        let mut progress_worker = Worker::new("goal-outage", now);
        progress_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        progress_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        progress_worker.permission_profile = Some(WorkerPermissionProfile::default());
        store.upsert_worker(&progress_worker).unwrap();
        let signal = CoordinationSignal::new(
            "goal-outage",
            "merge:outage",
            SignalKind::MergeConflict,
            99,
            EventSeverity::Critical,
            EventId::new(),
            vec![worker.id.clone()],
            vec![],
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        store.merge_signal_observation(&signal, now).unwrap();
        let service = SupervisorTriggerService::new(
            store.clone(),
            SupervisorTriggerPolicy {
                budget_window_seconds: 1,
                consecutive_failure_limit: 1,
                circuit_breaker_seconds: 1,
                ..SupervisorTriggerPolicy::default()
            },
        )
        .unwrap();
        let unavailable = BudgetSink {
            usage: SupervisorEvaluationUsage {
                input_tokens: 0,
                output_tokens: 0,
            },
            failure: Some("Supervisor provider unavailable".into()),
            allowances: Mutex::new(Vec::new()),
        };
        let failed = service
            .dispatch("goal-outage", 0, now, &unavailable)
            .unwrap();
        assert_eq!(failed.failures.len(), 1);
        assert!(failed.budget.circuit_open_until.is_some());

        let claim_service = ClaimService::new(tracker, store.clone(), ClaimPolicy::default());
        let progress_claim = claim_service
            .claim_ready_unit(
                "goal-outage",
                &progress_worker.id,
                None,
                "base-revision",
                "outage-progress-claim",
                now + chrono::Duration::milliseconds(100),
            )
            .unwrap();
        let renewed_claim = claim_service
            .heartbeat(
                &progress_claim.id,
                &progress_worker.id,
                progress_claim.lease_generation,
                now + chrono::Duration::milliseconds(250),
            )
            .unwrap();
        let completed_claim = claim_service
            .complete_claim(
                &renewed_claim.id,
                &progress_worker.id,
                renewed_claim.lease_generation,
                CompletionEvidence {
                    reason: "deterministic work completed without Supervisor input".into(),
                    artifact_id: "artifact-outage-progress".into(),
                    integration_revision: "integrated-outage-progress".into(),
                    evidence_refs: vec!["test:supervisor-outage-progress".into()],
                    integration_boundary_satisfied: true,
                },
                "outage-progress-complete",
                now + chrono::Duration::milliseconds(400),
            )
            .unwrap();
        assert_eq!(completed_claim.state, ClaimState::Completed);
        let watchdog = WatchdogService::new(store.clone(), WatchdogPolicy::default()).unwrap();
        let safety = watchdog
            .enforce(
                &claim_service,
                "goal-outage",
                &[WorkerRuntimeObservation {
                    worker_id: worker.id.clone(),
                    environment_alive: true,
                    active_turn_started_at: None,
                    observed_tokens: None,
                    consecutive_failures: 0,
                    permission_violation: Some("unexpected network access".into()),
                    ..WorkerRuntimeObservation::default()
                }],
                now + chrono::Duration::milliseconds(500),
            )
            .unwrap();
        assert_eq!(safety.outcomes.len(), 1);
        assert_eq!(
            safety.outcomes[0].kind,
            WatchdogOutcomeKind::PermissionViolation
        );
        assert_eq!(
            store.worker(&worker.id).unwrap().unwrap().state,
            WorkerState::Recovering
        );

        let recovered = RecordingSink::default();
        let replay = service
            .dispatch(
                "goal-outage",
                0,
                now + chrono::Duration::seconds(2),
                &recovered,
            )
            .unwrap();
        assert_eq!(replay.dispatched_trigger_keys.len(), 1);
        assert!(replay.failures.is_empty());
        assert_eq!(recovered.keys.lock().unwrap().len(), 1);
    }

    #[test]
    fn assembles_bounded_context_from_durable_affected_state() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir_all(repository.join("src")).unwrap();
        run_git(&repository, &["init"]);
        run_git(&repository, &["config", "user.email", "test@example.com"]);
        run_git(&repository, &["config", "user.name", "Test User"]);
        fs::write(repository.join("src/lib.rs"), "pub fn api() {}\n").unwrap();
        run_git(&repository, &["add", "."]);
        run_git(&repository, &["commit", "-m", "base"]);
        let base = git_output(&repository, &["rev-parse", "HEAD"]);
        let now = Utc::now();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.permission_profile = Some(WorkerPermissionProfile::default());
        let manager = WorktreeManager::open(&repository).unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker.id, &base, now)
            .unwrap();
        worker.workspace = Some(binding.clone());

        let mut contract = SharedContract::new(
            "goal-a",
            "api.worker.v1",
            "Worker API",
            ContractKind::Api,
            now,
        );
        contract.revise(
            worker.id.clone(),
            "Adds a required field",
            now + chrono::Duration::seconds(1),
        );
        store.upsert_contract(&contract).unwrap();
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "worker-api".into(),
            },
            worker.id.clone(),
            base,
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        claim.consumes_contracts.push(ContractExpectation {
            contract_id: contract.id.as_str().into(),
            expected_revision: 1,
        });
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        fs::write(
            Path::new(&binding.worktree_path).join("src/lib.rs"),
            "pub fn api() { println!(\"changed\"); }\n",
        )
        .unwrap();

        let blocker = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Error,
            CoordinationActor::Worker {
                worker_id: worker.id.clone(),
            },
            claim.id.as_str(),
            CoordinationEventPayload::Blocker(BlockerEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker.id.clone(),
                summary: "Contract revision is stale".into(),
                evidence_refs: vec!["contract:api.worker.v1".into()],
                escalation_id: None,
            }),
            now + chrono::Duration::seconds(2),
        )
        .unwrap();
        store.append_event(&blocker).unwrap();
        let signal = CoordinationSignal::new(
            "goal-a",
            format!("contract:{}:{}", claim.id.as_str(), contract.id.as_str()),
            SignalKind::ContractMismatch,
            95,
            EventSeverity::Error,
            blocker.id.clone(),
            vec![worker.id.clone()],
            vec![claim.id.clone()],
            now + chrono::Duration::seconds(2),
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        store.merge_signal_observation(&signal, now).unwrap();
        let mut prior = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Recommend,
            vec![blocker.id.clone()],
            vec![worker.id.clone()],
            "Refresh the contract before editing",
            now,
        );
        prior.evidence_refs.push("contract:api.worker.v1".into());
        store.upsert_intervention(&prior).unwrap();
        let mut escalation = HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: claim.id.clone(),
            },
            "Choose compatibility strategy",
            now,
        );
        escalation
            .evidence_refs
            .push("contract:api.worker.v1".into());
        store.upsert_escalation(&escalation).unwrap();
        let trigger = SupervisorTrigger {
            key: format!("signal:{}:1", signal.id.as_str()),
            goal_id: "goal-a".into(),
            kind: SupervisorTriggerKind::Signal,
            severity: EventSeverity::Error,
            confidence_percent: 95,
            occurred_at: now + chrono::Duration::seconds(2),
            signal_id: Some(signal.id.clone()),
            request_event_id: None,
            escalation_id: None,
            evidence_event_ids: vec![blocker.id.clone()],
            affected_worker_ids: vec![worker.id.clone()],
        };

        let packet = SupervisorContextBuilder::new(store)
            .build("goal-a", &trigger, now + chrono::Duration::seconds(3))
            .unwrap();
        assert_eq!(packet.workers.len(), 1);
        assert_eq!(packet.claims.len(), 1);
        assert_eq!(packet.repository_changes.len(), 1);
        assert!(
            packet.repository_changes[0]
                .changed_paths
                .contains(&"src/lib.rs".to_string())
        );
        assert_eq!(packet.contracts.len(), 1);
        assert_eq!(packet.contracts[0].current_revision, 2);
        assert_eq!(
            packet.contracts[0].expected_revisions[0].expected_revision,
            1
        );
        assert_eq!(packet.failure_events.len(), 1);
        assert_eq!(packet.failure_events[0].event_id, blocker.id);
        assert_eq!(packet.prior_interventions.len(), 1);
        assert_eq!(packet.open_escalations.len(), 1);
        assert!(serde_json::to_vec(&packet).unwrap().len() <= MAX_SUPERVISOR_CONTEXT_BYTES);
        let instructions = packet.evaluation_instructions().unwrap();
        assert!(instructions.starts_with("# Golazo Exception Supervisor Protocol"));
        assert!(instructions.contains(&trigger.key));
        assert!(instructions.contains("Do not assign routine work"));
        assert!(!instructions.contains("{{CONTEXT_PACKET_JSON}}"));
    }

    #[test]
    fn validates_every_decision_level_and_rejects_untrusted_output() {
        let now = Utc::now();
        let worker_id = WorkerId::new();
        let evidence_event_id = EventId::new();
        let evidence_ref = "contract:api.worker.v1".to_string();
        let trigger_key = "signal:contract-mismatch:1".to_string();
        let packet = SupervisorContextPacket {
            schema_version: 1,
            goal_id: "goal-a".into(),
            generated_at: now,
            trigger: SupervisorTrigger {
                key: trigger_key.clone(),
                goal_id: "goal-a".into(),
                kind: SupervisorTriggerKind::Signal,
                severity: EventSeverity::Error,
                confidence_percent: 95,
                occurred_at: now,
                signal_id: Some(SignalId::new()),
                request_event_id: None,
                escalation_id: None,
                evidence_event_ids: vec![evidence_event_id.clone()],
                affected_worker_ids: vec![worker_id.clone()],
            },
            workers: vec![SupervisorWorkerContext {
                worker_id: worker_id.clone(),
                state: WorkerState::Active,
                updated_at: now,
                current_run_id: Some("run-a".into()),
                current_thread_id: Some("thread-a".into()),
                active_claim_ids: vec![],
                repository_id: Some("repo-a".into()),
                worktree_path: Some("/tmp/repo-a".into()),
                base_revision: Some("base-a".into()),
                last_heartbeat_at: Some(now),
            }],
            claims: vec![],
            repository_changes: vec![],
            contracts: vec![],
            failure_events: vec![SupervisorEventContext {
                event_id: evidence_event_id.clone(),
                kind: CoordinationEventKind::BlockerRaised,
                severity: EventSeverity::Error,
                occurred_at: now,
                summary: "Contract expectation is stale".into(),
            }],
            prior_interventions: vec![SupervisorInterventionContext {
                intervention_id: InterventionId::new(),
                level: InterventionLevel::Recommend,
                state: InterventionState::Delivered,
                updated_at: now,
                target_worker_ids: vec![worker_id.clone()],
                evidence_refs: vec![evidence_ref.clone()],
                recommendation: "Refresh the contract".into(),
                requested_action: None,
                outcome: None,
            }],
            open_escalations: vec![],
            truncated: false,
        };
        let base = |decision: &str| {
            serde_json::json!({
                "schema_version": 1,
                "trigger_key": trigger_key,
                "decision": decision,
                "confidence_percent": 80,
                "summary": "Evidence-grounded assessment",
                "evidence_event_ids": [evidence_event_id],
                "evidence_refs": [],
                "target_worker_ids": [worker_id],
                "recommendation": "",
                "requested_action": null,
                "requires_human_decision": false,
                "escalation": null
            })
        };

        let mut observe = base("observe");
        observe["evidence_event_ids"] = serde_json::json!([]);
        observe["target_worker_ids"] = serde_json::json!([]);
        let inform = base("inform");
        let mut recommend = base("recommend");
        recommend["recommendation"] = serde_json::json!("Refresh the contract expectation");
        let mut coordinate = base("coordinate");
        coordinate["recommendation"] = serde_json::json!("Sequence the contract refresh first");
        coordinate["requested_action"] = serde_json::json!("Pause dependent edits until refresh");
        let mut block = base("block");
        block["confidence_percent"] = serde_json::json!(95);
        block["requested_action"] = serde_json::json!("Pause this worker's affected claim");
        let mut escalate = base("escalate");
        escalate["evidence_event_ids"] = serde_json::json!([]);
        escalate["evidence_refs"] = serde_json::json!([evidence_ref]);
        escalate["target_worker_ids"] = serde_json::json!([]);
        escalate["requires_human_decision"] = serde_json::json!(true);
        escalate["escalation"] = serde_json::json!({
            "kind": "decision_point",
            "severity": "high",
            "scope": "contract:api.worker.v1",
            "question": "Which compatibility strategy should be used?",
            "options": [
                {
                    "id": "preserve-v1",
                    "label": "Preserve v1",
                    "description": "Keep the existing response contract.",
                    "consequences": ["New fields remain unavailable."],
                    "recommended": true
                },
                {
                    "id": "adopt-v2",
                    "label": "Adopt v2",
                    "description": "Move dependents to the revised contract.",
                    "consequences": ["Dependent workers must refresh."],
                    "recommended": false
                }
            ]
        });

        for decision in [observe, inform, recommend, coordinate, block, escalate] {
            let parsed = packet.parse_decision(&decision.to_string()).unwrap();
            assert_eq!(parsed.trigger_key, trigger_key);
        }

        let mut wrong_trigger = base("inform");
        wrong_trigger["trigger_key"] = serde_json::json!("signal:invented:1");
        assert!(packet.parse_decision(&wrong_trigger.to_string()).is_err());

        let mut invented_event = base("inform");
        invented_event["evidence_event_ids"] = serde_json::json!([EventId::new()]);
        assert!(packet.parse_decision(&invented_event.to_string()).is_err());

        let mut invented_worker = base("inform");
        invented_worker["target_worker_ids"] = serde_json::json!([WorkerId::new()]);
        assert!(packet.parse_decision(&invented_worker.to_string()).is_err());

        let mut low_confidence_block = base("block");
        low_confidence_block["requested_action"] = serde_json::json!("Pause the claim");
        assert!(
            packet
                .parse_decision(&low_confidence_block.to_string())
                .is_err()
        );

        let mut excessive_confidence = base("inform");
        excessive_confidence["confidence_percent"] = serde_json::json!(101);
        assert!(
            packet
                .parse_decision(&excessive_confidence.to_string())
                .is_err()
        );

        let mut unknown_field = base("inform");
        unknown_field["invented_authority"] = serde_json::json!(true);
        assert!(packet.parse_decision(&unknown_field.to_string()).is_err());

        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let service = SupervisorInterventionService::new(store.clone());
        let mut recommendation_json = base("recommend");
        recommendation_json["recommendation"] =
            serde_json::json!("Refresh the contract expectation");
        let recommendation = packet
            .parse_decision(&recommendation_json.to_string())
            .unwrap();
        let first = service
            .persist_decision(&packet, &recommendation, now + chrono::Duration::seconds(1))
            .unwrap();
        let retry = service
            .persist_decision(&packet, &recommendation, now + chrono::Duration::seconds(2))
            .unwrap();
        assert_eq!(retry.id, first.id);

        let mut coordination_json = base("coordinate");
        coordination_json["recommendation"] =
            serde_json::json!("Sequence the contract refresh first");
        coordination_json["requested_action"] =
            serde_json::json!("Pause dependent edits until refresh");
        let coordination = packet
            .parse_decision(&coordination_json.to_string())
            .unwrap();
        let replacement = service
            .persist_decision(&packet, &coordination, now + chrono::Duration::seconds(3))
            .unwrap();
        assert_eq!(replacement.supersedes.as_ref(), Some(&first.id));
        assert_eq!(replacement.supersession_history, vec![first.id.clone()]);
        drop(service);
        drop(store);

        let reopened = SqliteCoordinationStore::open(&database).unwrap();
        let records = reopened.interventions_for_goal("goal-a", None).unwrap();
        assert_eq!(records.len(), 2);
        let prior = records.iter().find(|record| record.id == first.id).unwrap();
        let current = records
            .iter()
            .find(|record| record.id == replacement.id)
            .unwrap();
        assert_eq!(prior.state, InterventionState::Superseded);
        assert_eq!(prior.audit_history.len(), 2);
        assert_eq!(prior.audit_history[1].state, InterventionState::Superseded);
        assert_eq!(current.state, InterventionState::Proposed);
        assert_eq!(current.trigger_key.as_deref(), Some(trigger_key.as_str()));
        assert_eq!(current.decision_summary, coordination.summary);
        assert_eq!(current.confidence_percent, Some(80));
        assert_eq!(current.signal_event_ids, vec![evidence_event_id]);
        assert_eq!(current.target_workers, vec![worker_id]);
        assert!(
            current
                .reasoning_input_json
                .as_deref()
                .unwrap()
                .contains(&trigger_key)
        );
    }

    #[test]
    fn executes_only_targeted_supervisor_actions_and_never_assigns_work() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let sink = Arc::new(RecordingActionSink::default());
        let executor = SupervisorActionExecutor::new(store.clone(), sink.clone());
        let now = Utc::now();
        let worker_a = WorkerId::new();
        let worker_b = WorkerId::new();
        let evidence = EventId::new();

        let mut observe = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Observe,
            vec![],
            vec![],
            "",
            now,
        );
        observe.decision_summary = "No material conflict".into();
        store.upsert_intervention(&observe).unwrap();
        let report = executor.execute("goal-a", &observe.id, now).unwrap();
        assert_eq!(report.disposition, SupervisorExecutionDisposition::NoAction);
        assert!(sink.actions.lock().unwrap().is_empty());

        let mut recommend = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Recommend,
            vec![evidence.clone()],
            vec![worker_a.clone()],
            "Refresh the shared contract before continuing",
            now + chrono::Duration::seconds(1),
        );
        recommend.decision_summary = "The expected contract revision is stale".into();
        recommend.confidence_percent = Some(88);
        store.upsert_intervention(&recommend).unwrap();
        let report = executor
            .execute("goal-a", &recommend.id, now + chrono::Duration::seconds(2))
            .unwrap();
        assert_eq!(report.delivered_worker_ids, vec![worker_a.clone()]);
        assert_eq!(sink.actions.lock().unwrap().len(), 1);
        let replay = executor
            .execute("goal-a", &recommend.id, now + chrono::Duration::seconds(3))
            .unwrap();
        assert_eq!(
            replay.disposition,
            SupervisorExecutionDisposition::AlreadyProcessed
        );
        assert_eq!(sink.actions.lock().unwrap().len(), 1);

        let mut coordinate = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Coordinate,
            vec![evidence.clone()],
            vec![worker_a.clone(), worker_b.clone()],
            "Sequence the shared contract update before dependent edits",
            now + chrono::Duration::seconds(4),
        );
        coordinate.decision_summary = "Both workers touch one shared contract".into();
        coordinate.requested_action =
            Some("Refresh after the producer publishes revision 2".into());
        coordinate.confidence_percent = Some(92);
        store.upsert_intervention(&coordinate).unwrap();
        let report = executor
            .execute("goal-a", &coordinate.id, now + chrono::Duration::seconds(5))
            .unwrap();
        assert_eq!(
            report.delivered_worker_ids,
            vec![worker_a.clone(), worker_b.clone()]
        );

        let mut block = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Block,
            vec![evidence],
            vec![worker_b.clone()],
            "Pause only the affected worker",
            now + chrono::Duration::seconds(6),
        );
        block.decision_summary = "Continuing would violate the current contract".into();
        block.requested_action = Some("Pause dependent edits".into());
        block.confidence_percent = Some(95);
        store.upsert_intervention(&block).unwrap();
        let report = executor
            .execute("goal-a", &block.id, now + chrono::Duration::seconds(7))
            .unwrap();
        assert_eq!(report.delivered_worker_ids, vec![worker_b.clone()]);

        let actions = sink.actions.lock().unwrap();
        assert_eq!(actions.len(), 4);
        assert_eq!(actions[0].level, InterventionLevel::Recommend);
        assert_eq!(actions[1].level, InterventionLevel::Coordinate);
        assert_eq!(actions[2].level, InterventionLevel::Coordinate);
        assert_eq!(actions[3].level, InterventionLevel::Block);
        assert_eq!(actions[3].target_worker_id, worker_b);
        assert!(actions.iter().all(|action| !action.summary.is_empty()));
        assert!(
            actions
                .iter()
                .all(|action| action.idempotency_key.starts_with("supervisor-action:"))
        );
        drop(actions);

        let mut escalation = SupervisorIntervention::new(
            "goal-a",
            InterventionLevel::Escalate,
            vec![],
            vec![],
            "",
            now + chrono::Duration::seconds(8),
        );
        escalation.decision_summary = "A product decision is required".into();
        store.upsert_intervention(&escalation).unwrap();
        let report = executor
            .execute("goal-a", &escalation.id, now + chrono::Duration::seconds(9))
            .unwrap();
        assert_eq!(
            report.disposition,
            SupervisorExecutionDisposition::AwaitingHumanEscalation
        );
        assert_eq!(sink.actions.lock().unwrap().len(), 4);

        let persisted = store.interventions_for_goal("goal-a", None).unwrap();
        assert_eq!(
            persisted
                .iter()
                .find(|item| item.id == block.id)
                .unwrap()
                .state,
            InterventionState::Delivered
        );
        assert_eq!(
            persisted
                .iter()
                .find(|item| item.id == escalation.id)
                .unwrap()
                .state,
            InterventionState::Proposed
        );
    }

    fn run_git(repository: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn git_output(repository: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8_lossy(&output.stdout).trim().into()
    }
}
