use super::claims::{BlockEvidence, ClaimService, ClaimServiceError};
use super::domain::{
    Claim, ClaimId, ClaimOutcome, ClaimOutcomeKind, ClaimScope, ClaimState, DomainError,
    EscalationClaimRevision, EscalationContractRevision, EscalationDecision,
    EscalationDecisionAudit, EscalationDownstreamOutcome, EscalationId, EscalationKind,
    EscalationOutcomeKind, EscalationPackageRevision, EscalationScope, EscalationStalenessSnapshot,
    EscalationState, EscalationWorkerRevision, EventSeverity, HumanDecisionEventPayload,
    HumanEscalation, RemediationAction, RemediationOption, WorkPackageState, Worker, WorkerState,
};
use super::protocol::{WorkerContextAssembler, WorkerContextError, WorkerContextPacket};
use super::store::{
    ClaimRepository, ClaimResumeMutation, ContractRepository, EscalationDecisionSideEffects,
    EscalationRepository, EventRepository, ExpectedRecordUpdate, SqliteCoordinationStore,
    StoreError, WorkPackageRepository, WorkerRepository,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemediationDirection {
    pub id: String,
    pub label: String,
    pub description: String,
    pub consequences: Vec<String>,
    pub recommended: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemediationCapabilities {
    pub reconcile_now: bool,
    pub wait: bool,
    pub split_package: bool,
    pub pause_scope: bool,
    pub cancel_scope: bool,
    pub allow_override: bool,
    pub directions: Vec<RemediationDirection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeBlockReport {
    pub escalation_id: EscalationId,
    pub scope: EscalationScope,
    pub blocked_claim_ids: Vec<ClaimId>,
    pub unaffected_active_claim_ids: Vec<ClaimId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationStalenessKind {
    Scope,
    Evidence,
    Options,
    Claim,
    Worker,
    WorkPackage,
    Contract,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationStalenessFinding {
    pub kind: EscalationStalenessKind,
    pub changed_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EscalationStalenessReport {
    pub escalation_id: EscalationId,
    pub captured_at: DateTime<Utc>,
    pub checked_at: DateTime<Utc>,
    pub stale: bool,
    pub findings: Vec<EscalationStalenessFinding>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationDecisionApplication {
    pub escalation: HumanEscalation,
    pub action: RemediationAction,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResumedEscalationWork {
    pub blocked_claim_id: ClaimId,
    pub resumed_claim: Claim,
    pub worker: Worker,
    pub replaced_worker: bool,
    pub context: WorkerContextPacket,
    pub event_sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EscalationResumeReport {
    pub escalation_id: EscalationId,
    pub action: RemediationAction,
    pub resumed: Vec<ResumedEscalationWork>,
}

pub fn generate_remediation_options(
    escalation: &HumanEscalation,
    capabilities: &RemediationCapabilities,
) -> Result<Vec<RemediationOption>, EscalationLifecycleError> {
    let mut options = Vec::new();
    if escalation.kind == EscalationKind::DecisionPoint {
        let mut recommended_direction_seen = false;
        for direction in &capabilities.directions {
            validate_direction(direction)?;
            let recommended = direction.recommended && !recommended_direction_seen;
            recommended_direction_seen |= recommended;
            options.push(RemediationOption {
                id: format!("choose-{}", direction.id),
                action: RemediationAction::ChooseDirection,
                label: direction.label.clone(),
                description: direction.description.clone(),
                consequences: direction.consequences.clone(),
                recommended,
            });
        }
    }
    if capabilities.reconcile_now {
        options.push(RemediationOption {
            id: "reconcile-now".into(),
            action: RemediationAction::ReconcileNow,
            label: "Reconcile now".into(),
            description: "Reconcile the conflicting implementation or contract before continuing."
                .into(),
            consequences: vec![
                "Affected work remains paused until reconciliation validates.".into(),
                "The original workspaces and evidence remain recoverable.".into(),
            ],
            recommended: escalation.kind == EscalationKind::HardBlocker,
        });
    }
    if capabilities.split_package && matches!(escalation.scope, EscalationScope::WorkPackage { .. })
    {
        options.push(RemediationOption {
            id: "split-package".into(),
            action: RemediationAction::SplitPackage,
            label: "Split package".into(),
            description: "Separate the blocked scope from independent work in this package.".into(),
            consequences: vec![
                "Package membership and dependencies will change.".into(),
                "Independent work may resume after the split is validated.".into(),
            ],
            recommended: false,
        });
    }
    if capabilities.wait {
        options.push(RemediationOption {
            id: "wait".into(),
            action: RemediationAction::Wait,
            label: "Wait".into(),
            description: "Keep the current state and wait for the missing dependency or evidence."
                .into(),
            consequences: vec!["Only the affected scope remains pending.".into()],
            recommended: false,
        });
    }
    if capabilities.pause_scope {
        options.push(RemediationOption {
            id: "pause-scope".into(),
            action: RemediationAction::PauseScope,
            label: "Pause affected scope".into(),
            description: "Pause the affected claim, package, contract, or integration lane.".into(),
            consequences: vec!["Unrelated scopes continue to make progress.".into()],
            recommended: false,
        });
    }
    if capabilities.allow_override {
        options.push(RemediationOption {
            id: "override-risk".into(),
            action: RemediationAction::OverrideRisk,
            label: "Override and accept risk".into(),
            description: "Continue under an explicit user-attributed risk acceptance.".into(),
            consequences: vec![
                "The accepted risk is recorded in the permanent audit trail.".into(),
                "Validation and deterministic safety controls still apply.".into(),
            ],
            recommended: false,
        });
    }
    if capabilities.cancel_scope {
        options.push(RemediationOption {
            id: "cancel-scope".into(),
            action: RemediationAction::CancelScope,
            label: "Cancel affected scope".into(),
            description: "Cancel the affected work while preserving its workspace and evidence."
                .into(),
            consequences: vec!["The selected scope will not resume automatically.".into()],
            recommended: false,
        });
    }
    if options.len() < 2 || options.len() > 8 {
        return Err(EscalationLifecycleError::InvalidRemediation(
            "remediation generation requires two to eight applicable options".into(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    if options.iter().any(|option| !seen.insert(&option.id)) {
        return Err(EscalationLifecycleError::InvalidRemediation(
            "remediation option IDs must be unique".into(),
        ));
    }
    if !options.iter().any(|option| option.recommended) {
        options[0].recommended = true;
    } else {
        let mut found = false;
        for option in &mut options {
            if option.recommended {
                if found {
                    option.recommended = false;
                }
                found = true;
            }
        }
    }
    Ok(options)
}

fn validate_direction(direction: &RemediationDirection) -> Result<(), EscalationLifecycleError> {
    if direction.id.trim().is_empty()
        || direction.id.chars().count() > 100
        || !direction.id.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
        || direction.label.trim().is_empty()
        || direction.description.trim().is_empty()
        || direction.consequences.is_empty()
        || direction
            .consequences
            .iter()
            .any(|consequence| consequence.trim().is_empty())
    {
        return Err(EscalationLifecycleError::InvalidRemediation(
            "direction IDs and user-facing fields must be bounded and non-empty".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum EscalationLifecycleError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Claim(#[from] ClaimServiceError),
    #[error(transparent)]
    Context(#[from] WorkerContextError),
    #[error("human escalation was not found: {0}")]
    NotFound(String),
    #[error("escalation {escalation_id} belongs to goal {actual_goal}, not {requested_goal}")]
    GoalMismatch {
        escalation_id: String,
        actual_goal: String,
        requested_goal: String,
    },
    #[error("human escalation changed while the lifecycle transition was in progress")]
    StaleRevision,
    #[error("an existing terminal transition does not match the requested decision")]
    ConflictingReplay,
    #[error("invalid remediation options: {0}")]
    InvalidRemediation(String),
    #[error("invalid scope block: {0}")]
    InvalidScopeBlock(String),
    #[error("escalation has no decision staleness snapshot")]
    MissingStalenessSnapshot,
    #[error("invalid escalation staleness operation: {0}")]
    InvalidStaleness(String),
    #[error("the selected escalation decision is stale: {findings:?}")]
    StaleDecision {
        findings: Vec<EscalationStalenessFinding>,
    },
    #[error("the selected remediation option is unavailable: {0}")]
    InvalidDecisionOption(String),
    #[error("affected work cannot be resumed: {0}")]
    InvalidResume(String),
}

#[derive(Debug, Clone)]
pub struct EscalationLifecycleService {
    store: Arc<SqliteCoordinationStore>,
}

impl EscalationLifecycleService {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn create(
        &self,
        escalation: &HumanEscalation,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        if let Some(existing) = self.store.escalation(&escalation.id)? {
            return if existing == *escalation {
                Ok(existing)
            } else {
                Err(EscalationLifecycleError::ConflictingReplay)
            };
        }
        if escalation.state != EscalationState::Open
            || escalation.audit_history.len() != 1
            || escalation.audit_history[0].state != EscalationState::Open
        {
            return Err(DomainError::InvalidTransition {
                entity: "escalation",
                from: "unpersisted",
                to: "open",
            }
            .into());
        }
        self.store.upsert_escalation(escalation)?;
        Ok(escalation.clone())
    }

    pub fn acknowledge(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        actor: &str,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        self.transition(
            goal_id,
            escalation_id,
            EscalationState::Acknowledged,
            actor,
            reason,
            None,
            now,
        )
    }

    pub fn set_remediation_options(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        capabilities: &RemediationCapabilities,
        actor: &str,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        let mut escalation = self.load_for_goal(goal_id, escalation_id)?;
        if !matches!(
            escalation.state,
            EscalationState::Open | EscalationState::Acknowledged
        ) || actor.trim().is_empty()
            || actor.chars().count() > 200
            || now < escalation.metadata.updated_at
        {
            return Err(EscalationLifecycleError::InvalidRemediation(
                "options can only be generated for a current open or acknowledged escalation"
                    .into(),
            ));
        }
        let options = generate_remediation_options(&escalation, capabilities)?;
        if escalation.options == options {
            return Ok(escalation);
        }
        let expected_updated_at = escalation.metadata.updated_at;
        escalation.options = options;
        escalation
            .audit_history
            .push(super::domain::EscalationAuditEntry {
                state: escalation.state,
                at: now,
                actor: actor.into(),
                reason: Some("remediation options generated".into()),
                decision: None,
            });
        escalation.metadata.touch(now);
        if !self
            .store
            .update_escalation_if_revision(&escalation, expected_updated_at)?
        {
            return Err(EscalationLifecycleError::StaleRevision);
        }
        Ok(escalation)
    }

    pub fn enforce_scope_block(
        &self,
        claim_service: &ClaimService,
        goal_id: &str,
        escalation_id: &EscalationId,
        now: DateTime<Utc>,
    ) -> Result<ScopeBlockReport, EscalationLifecycleError> {
        let escalation = self.load_for_goal(goal_id, escalation_id)?;
        if escalation.kind != EscalationKind::HardBlocker
            || !matches!(
                escalation.state,
                EscalationState::Open | EscalationState::Acknowledged
            )
        {
            return Err(EscalationLifecycleError::InvalidScopeBlock(
                "only an open or acknowledged hard blocker can block execution".into(),
            ));
        }
        if matches!(
            &escalation.scope,
            EscalationScope::Goal { goal_id: scope_goal_id } if scope_goal_id != goal_id
        ) {
            return Err(EscalationLifecycleError::InvalidScopeBlock(
                "goal scope does not match the escalation goal".into(),
            ));
        }

        if let EscalationScope::WorkPackage { work_package_id } = &escalation.scope
            && let Some(mut package) = self.store.work_package(work_package_id)?
        {
            if package.goal_id != goal_id {
                return Err(EscalationLifecycleError::InvalidScopeBlock(
                    "work package scope belongs to another goal".into(),
                ));
            }
            if matches!(
                package.state,
                WorkPackageState::Planned
                    | WorkPackageState::Ready
                    | WorkPackageState::Claimed
                    | WorkPackageState::Integrating
            ) {
                package.transition(WorkPackageState::Blocked, now)?;
                self.store.upsert_work_package(&package)?;
            }
        }

        let workers = self.store.workers_for_goal(goal_id, None)?;
        let active_claims = self
            .store
            .claims_for_goal(goal_id, Some(ClaimState::Active))?;
        let mut blocked_claim_ids = Vec::new();
        let mut unaffected_active_claim_ids = Vec::new();
        for claim in active_claims {
            let worker = workers.iter().find(|worker| worker.id == claim.owner);
            if claim_matches_escalation_scope(&claim, worker, &escalation.scope) {
                let mut evidence_refs = escalation.evidence_refs.clone();
                let escalation_ref = format!("escalation:{}", escalation.id.as_str());
                if !evidence_refs.contains(&escalation_ref) {
                    evidence_refs.push(escalation_ref);
                }
                claim_service.block_claim(
                    &claim.id,
                    &claim.owner,
                    claim.lease_generation,
                    BlockEvidence {
                        reason: format!(
                            "Hard blocker escalation {}: {}",
                            escalation.id.as_str(),
                            escalation.summary
                        ),
                        evidence_refs,
                        escalation_id: Some(escalation.id.clone()),
                    },
                    &format!(
                        "escalation.block:{}:{}",
                        escalation.id.as_str(),
                        claim.id.as_str()
                    ),
                    now,
                )?;
                blocked_claim_ids.push(claim.id);
            } else {
                unaffected_active_claim_ids.push(claim.id);
            }
        }
        blocked_claim_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        unaffected_active_claim_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(ScopeBlockReport {
            escalation_id: escalation.id,
            scope: escalation.scope,
            blocked_claim_ids,
            unaffected_active_claim_ids,
        })
    }

    pub fn capture_staleness_snapshot(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        actor: &str,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        let mut escalation = self.load_for_goal(goal_id, escalation_id)?;
        if !matches!(
            escalation.state,
            EscalationState::Open | EscalationState::Acknowledged
        ) || actor.trim().is_empty()
            || actor.chars().count() > 200
            || now < escalation.metadata.updated_at
        {
            return Err(EscalationLifecycleError::InvalidStaleness(
                "snapshots require a current open or acknowledged escalation and an attributed actor"
                    .into(),
            ));
        }
        let snapshot = self.build_staleness_snapshot(&escalation, now)?;
        let expected_updated_at = escalation.metadata.updated_at;
        escalation.stale_after_revisions = staleness_revision_tokens(&snapshot);
        escalation.staleness_snapshot = Some(snapshot);
        escalation
            .audit_history
            .push(super::domain::EscalationAuditEntry {
                state: escalation.state,
                at: now,
                actor: actor.into(),
                reason: Some("decision staleness snapshot captured".into()),
                decision: None,
            });
        escalation.metadata.touch(now);
        if !self
            .store
            .update_escalation_if_revision(&escalation, expected_updated_at)?
        {
            return Err(EscalationLifecycleError::StaleRevision);
        }
        Ok(escalation)
    }

    pub fn validate_staleness(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        checked_at: DateTime<Utc>,
    ) -> Result<EscalationStalenessReport, EscalationLifecycleError> {
        let escalation = self.load_for_goal(goal_id, escalation_id)?;
        let expected = escalation
            .staleness_snapshot
            .as_ref()
            .ok_or(EscalationLifecycleError::MissingStalenessSnapshot)?;
        if checked_at < expected.captured_at {
            return Err(EscalationLifecycleError::InvalidStaleness(
                "staleness cannot be checked before the snapshot was captured".into(),
            ));
        }
        let current = self.build_staleness_snapshot(&escalation, expected.captured_at)?;
        let findings = compare_staleness_snapshots(expected, &current);
        Ok(EscalationStalenessReport {
            escalation_id: escalation.id,
            captured_at: expected.captured_at,
            checked_at,
            stale: !findings.is_empty(),
            findings,
        })
    }

    pub fn apply_user_decision(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        decision: EscalationDecision,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<EscalationDecisionApplication, EscalationLifecycleError> {
        let mut escalation = self.load_for_goal(goal_id, escalation_id)?;
        if escalation.decision.as_ref() == Some(&decision)
            && matches!(
                escalation.state,
                EscalationState::Acknowledged
                    | EscalationState::Resolved
                    | EscalationState::Overridden
                    | EscalationState::Cancelled
            )
        {
            let action = escalation
                .options
                .iter()
                .find(|option| option.id == decision.option_id)
                .map(|option| option.action)
                .ok_or_else(|| {
                    EscalationLifecycleError::InvalidDecisionOption(decision.option_id.clone())
                })?;
            let event_sequence = self
                .human_decision_event_sequence(goal_id, escalation_id, &decision)?
                .ok_or(EscalationLifecycleError::ConflictingReplay)?;
            return Ok(EscalationDecisionApplication {
                escalation,
                action,
                event_sequence,
            });
        }
        if !matches!(
            escalation.state,
            EscalationState::Open | EscalationState::Acknowledged
        ) || escalation.decision.is_some()
        {
            return Err(EscalationLifecycleError::ConflictingReplay);
        }
        let stale = self.validate_staleness(goal_id, escalation_id, now)?;
        if stale.stale {
            return Err(EscalationLifecycleError::StaleDecision {
                findings: stale.findings,
            });
        }
        let selected_option = escalation
            .options
            .iter()
            .find(|option| option.id == decision.option_id)
            .cloned()
            .ok_or_else(|| {
                EscalationLifecycleError::InvalidDecisionOption(decision.option_id.clone())
            })?;
        let action = selected_option.action;
        let expected_updated_at = escalation.metadata.updated_at;
        let effects = self.prepare_decision_side_effects(&escalation, action, now)?;
        let reason =
            reason.or_else(|| Some(format!("Applied remediation option {}", decision.option_id)));
        match action {
            RemediationAction::Wait | RemediationAction::PauseScope => {
                escalation.record_acknowledged_decision(
                    now,
                    decision.decided_by.clone(),
                    reason,
                    decision.clone(),
                )?;
            }
            RemediationAction::CancelScope => escalation.transition_attributed(
                EscalationState::Cancelled,
                now,
                decision.decided_by.clone(),
                reason,
                Some(decision.clone()),
            )?,
            RemediationAction::OverrideRisk => escalation.transition_attributed(
                EscalationState::Overridden,
                now,
                decision.decided_by.clone(),
                reason,
                Some(decision.clone()),
            )?,
            RemediationAction::ReconcileNow
            | RemediationAction::SplitPackage
            | RemediationAction::ChooseDirection => escalation.transition_attributed(
                EscalationState::Resolved,
                now,
                decision.decided_by.clone(),
                reason,
                Some(decision.clone()),
            )?,
        }
        let mut evidence_refs = escalation.evidence_refs.clone();
        evidence_refs.sort();
        evidence_refs.dedup();
        let mut alternatives = escalation
            .options
            .iter()
            .filter(|option| option.id != selected_option.id)
            .cloned()
            .collect::<Vec<_>>();
        alternatives.sort_by(|left, right| left.id.cmp(&right.id));
        let mut claim_ids = effects
            .claims
            .iter()
            .map(|update| update.record.id.clone())
            .collect::<Vec<_>>();
        claim_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let mut worker_ids = effects
            .workers
            .iter()
            .map(|update| update.record.id.clone())
            .collect::<Vec<_>>();
        worker_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let mut work_package_ids = effects
            .work_packages
            .iter()
            .map(|update| update.record.id.clone())
            .collect::<Vec<_>>();
        work_package_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let outcome_kind = match action {
            RemediationAction::Wait => EscalationOutcomeKind::Waiting,
            RemediationAction::PauseScope => EscalationOutcomeKind::ScopePaused,
            RemediationAction::CancelScope => EscalationOutcomeKind::ScopeCancelled,
            _ => EscalationOutcomeKind::DecisionApplied,
        };
        escalation.decision_audit = Some(EscalationDecisionAudit {
            decided_by: decision.decided_by.clone(),
            decided_at: decision.decided_at,
            selected_option: selected_option.clone(),
            alternatives,
            evidence_refs,
            accepted_risk: decision.accepted_risk.clone(),
            downstream_outcomes: vec![EscalationDownstreamOutcome {
                kind: outcome_kind,
                recorded_at: now,
                summary: format!(
                    "Applied remediation option {} with escalation state {:?}",
                    selected_option.id, escalation.state
                ),
                claim_ids,
                worker_ids,
                work_package_ids,
            }],
        });
        let event = super::domain::CoordinationEvent::from_typed_payload(
            goal_id,
            EventSeverity::Info,
            super::domain::CoordinationActor::User {
                user_id: decision.decided_by.clone(),
            },
            escalation.id.as_str(),
            super::domain::CoordinationEventPayload::HumanDecision(HumanDecisionEventPayload {
                escalation_id: escalation.id.clone(),
                decided_by: decision.decided_by.clone(),
                option_id: decision.option_id.clone(),
                action,
                accepted_risk: decision.accepted_risk.clone(),
            }),
            now,
        )?;
        let event_sequence = self
            .store
            .apply_escalation_decision_with_event(
                &escalation,
                expected_updated_at,
                &effects,
                &event,
                &format!("escalation.decision:{}", escalation.id.as_str()),
            )?
            .ok_or(EscalationLifecycleError::StaleRevision)?;
        Ok(EscalationDecisionApplication {
            escalation,
            action,
            event_sequence,
        })
    }

    fn human_decision_event_sequence(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        decision: &EscalationDecision,
    ) -> Result<Option<u64>, EscalationLifecycleError> {
        for event in self
            .store
            .latest_events_for_goal(goal_id, Some(escalation_id.as_str()), 20)?
        {
            if let Some(super::domain::CoordinationEventPayload::HumanDecision(payload)) =
                event.typed_payload()?
                && payload.escalation_id == *escalation_id
                && payload.option_id == decision.option_id
                && payload.decided_by == decision.decided_by
                && payload.accepted_risk == decision.accepted_risk
            {
                return Ok(event.sequence);
            }
        }
        Ok(None)
    }

    pub fn resume_affected_work(
        &self,
        assembler: &WorkerContextAssembler,
        goal_id: &str,
        escalation_id: &EscalationId,
        lease_duration: Duration,
        now: DateTime<Utc>,
    ) -> Result<EscalationResumeReport, EscalationLifecycleError> {
        if lease_duration <= Duration::zero() {
            return Err(EscalationLifecycleError::InvalidResume(
                "claim lease duration must be positive".into(),
            ));
        }
        let mut escalation = self.load_for_goal(goal_id, escalation_id)?;
        let decision = escalation.decision.as_ref().ok_or_else(|| {
            EscalationLifecycleError::InvalidResume("no decision is applied".into())
        })?;
        let action = escalation
            .options
            .iter()
            .find(|option| option.id == decision.option_id)
            .map(|option| option.action)
            .ok_or_else(|| {
                EscalationLifecycleError::InvalidDecisionOption(decision.option_id.clone())
            })?;
        if !matches!(
            escalation.state,
            EscalationState::Resolved | EscalationState::Overridden
        ) {
            return Ok(EscalationResumeReport {
                escalation_id: escalation.id,
                action,
                resumed: Vec::new(),
            });
        }

        let all_claims = self.store.claims_for_goal(goal_id, None)?;
        let blocked_claims = all_claims
            .iter()
            .filter(|claim| {
                claim.state == ClaimState::Blocked
                    && claim
                        .outcome
                        .as_ref()
                        .and_then(|outcome| outcome.escalation_id.as_ref())
                        == Some(escalation_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut resumed = Vec::new();
        for blocked in blocked_claims {
            if let Some(existing) = all_claims.iter().find(|claim| {
                claim.state == ClaimState::Active
                    && claim.parent_claim_id.as_ref() == Some(&blocked.id)
            }) {
                let worker = self.store.worker(&existing.owner)?.ok_or_else(|| {
                    EscalationLifecycleError::InvalidResume("resumed claim owner is missing".into())
                })?;
                let context = assembler.assemble(&worker.id, &existing.id, now)?;
                let event_sequence = self
                    .store
                    .latest_events_for_goal(goal_id, Some(existing.id.as_str()), 1)?
                    .first()
                    .and_then(|event| event.sequence)
                    .unwrap_or_default();
                resumed.push(ResumedEscalationWork {
                    blocked_claim_id: blocked.id,
                    resumed_claim: existing.clone(),
                    worker,
                    replaced_worker: existing.owner != blocked.owner,
                    context,
                    event_sequence,
                });
                continue;
            }

            let original_worker = self.store.worker(&blocked.owner)?.ok_or_else(|| {
                EscalationLifecycleError::InvalidResume("worker is missing".into())
            })?;
            let workspace = original_worker.workspace.clone().ok_or_else(|| {
                EscalationLifecycleError::InvalidResume(
                    "worker has no recoverable isolated workspace".into(),
                )
            })?;
            let permissions = original_worker.permission_profile.clone().ok_or_else(|| {
                EscalationLifecycleError::InvalidResume(
                    "worker has no recoverable permission profile".into(),
                )
            })?;
            let (mut worker, expected_worker_updated_at, replaced_worker) =
                if original_worker.state.is_terminal() {
                    let mut replacement = Worker::new(goal_id, now);
                    replacement.workspace = Some(workspace.clone());
                    replacement.permission_profile = Some(permissions);
                    replacement.transition(WorkerState::Starting, now, None)?;
                    replacement.transition(WorkerState::Active, now, None)?;
                    (replacement, None, true)
                } else {
                    let expected = original_worker.metadata.updated_at;
                    let mut resumed_worker = original_worker;
                    if resumed_worker.state != WorkerState::Active {
                        resumed_worker.transition(
                            WorkerState::Active,
                            now,
                            Some("human escalation resolved with refreshed context".into()),
                        )?;
                    } else {
                        resumed_worker.metadata.touch(now);
                    }
                    (resumed_worker, Some(expected), false)
                };
            let mut replacement_claim = Claim::new(
                goal_id,
                blocked.scope.clone(),
                worker.id.clone(),
                workspace.base_revision.clone(),
                now,
                now + lease_duration,
            )?;
            replacement_claim.lease_generation = blocked.lease_generation + 1;
            replacement_claim.parent_claim_id = Some(blocked.id.clone());
            replacement_claim.produces_contracts = blocked.produces_contracts.clone();
            replacement_claim.consumes_contracts = blocked.consumes_contracts.clone();
            worker.active_claims.push(replacement_claim.id.clone());
            worker.metadata.touch(now);

            let work_package = if let ClaimScope::WorkPackage { work_package_id } = &blocked.scope {
                let mut package = self.store.work_package(work_package_id)?.ok_or_else(|| {
                    EscalationLifecycleError::InvalidResume("work package is missing".into())
                })?;
                if package.state == WorkPackageState::Blocked {
                    let expected_updated_at = package.metadata.updated_at;
                    package.transition(WorkPackageState::Claimed, now)?;
                    Some(ExpectedRecordUpdate {
                        record: package,
                        expected_updated_at,
                    })
                } else if package.state == WorkPackageState::Claimed {
                    None
                } else {
                    return Err(EscalationLifecycleError::InvalidResume(format!(
                        "work package is {:?}, not blocked or claimed",
                        package.state
                    )));
                }
            } else {
                None
            };
            let event = super::domain::CoordinationEvent::from_typed_payload(
                goal_id,
                EventSeverity::Info,
                super::domain::CoordinationActor::System,
                replacement_claim.id.as_str(),
                super::domain::CoordinationEventPayload::Claim(super::domain::ClaimEventPayload {
                    claim_id: replacement_claim.id.clone(),
                    worker_id: worker.id.clone(),
                    from_state: Some(ClaimState::Blocked),
                    to_state: ClaimState::Active,
                    reason: format!(
                        "resumed after escalation {} was {:?}",
                        escalation.id.as_str(),
                        escalation.state
                    ),
                    artifact_id: None,
                    evidence_refs: vec![
                        format!("escalation:{}", escalation.id.as_str()),
                        format!("blocked-claim:{}", blocked.id.as_str()),
                    ],
                }),
                now,
            )?;
            let expected_escalation_updated_at = escalation.metadata.updated_at;
            let outcome_kind = if replaced_worker {
                EscalationOutcomeKind::WorkerReplaced
            } else {
                EscalationOutcomeKind::WorkResumed
            };
            let mut outcome_worker_ids = vec![blocked.owner.clone(), worker.id.clone()];
            outcome_worker_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            outcome_worker_ids.dedup();
            let outcome_work_package_ids = match &blocked.scope {
                ClaimScope::WorkPackage { work_package_id } => vec![work_package_id.clone()],
                _ => Vec::new(),
            };
            let decision_audit = escalation.decision_audit.as_mut().ok_or_else(|| {
                EscalationLifecycleError::InvalidResume(
                    "the applied decision has no durable decision audit".into(),
                )
            })?;
            decision_audit
                .downstream_outcomes
                .push(EscalationDownstreamOutcome {
                    kind: outcome_kind,
                    recorded_at: now,
                    summary: if replaced_worker {
                        format!(
                            "Replaced worker {} with {} and resumed blocked claim {} as {}",
                            blocked.owner.as_str(),
                            worker.id.as_str(),
                            blocked.id.as_str(),
                            replacement_claim.id.as_str()
                        )
                    } else {
                        format!(
                            "Resumed blocked claim {} as {} with worker {}",
                            blocked.id.as_str(),
                            replacement_claim.id.as_str(),
                            worker.id.as_str()
                        )
                    },
                    claim_ids: vec![blocked.id.clone(), replacement_claim.id.clone()],
                    worker_ids: outcome_worker_ids,
                    work_package_ids: outcome_work_package_ids,
                });
            escalation
                .audit_history
                .push(super::domain::EscalationAuditEntry {
                    state: escalation.state,
                    at: now,
                    actor: "system".into(),
                    reason: Some(format!(
                        "affected claim {} resumed as {}",
                        blocked.id.as_str(),
                        replacement_claim.id.as_str()
                    )),
                    decision: None,
                });
            escalation.metadata.touch(now);
            let event_sequence = self
                .store
                .resume_blocked_claim_with_event(
                    &ClaimResumeMutation {
                        escalation: ExpectedRecordUpdate {
                            record: escalation.clone(),
                            expected_updated_at: expected_escalation_updated_at,
                        },
                        original_claim_id: blocked.id.clone(),
                        expected_original_updated_at: blocked.metadata.updated_at,
                        replacement_claim: replacement_claim.clone(),
                        worker: worker.clone(),
                        expected_worker_updated_at,
                        work_package,
                    },
                    &event,
                    &format!(
                        "escalation.resume:{}:{}",
                        escalation.id.as_str(),
                        blocked.id.as_str()
                    ),
                )?
                .ok_or(EscalationLifecycleError::StaleRevision)?;
            let context = assembler.assemble(&worker.id, &replacement_claim.id, now)?;
            resumed.push(ResumedEscalationWork {
                blocked_claim_id: blocked.id,
                resumed_claim: replacement_claim,
                worker,
                replaced_worker,
                context,
                event_sequence,
            });
        }
        resumed.sort_by(|left, right| {
            left.blocked_claim_id
                .as_str()
                .cmp(right.blocked_claim_id.as_str())
        });
        Ok(EscalationResumeReport {
            escalation_id: escalation.id,
            action,
            resumed,
        })
    }

    fn prepare_decision_side_effects(
        &self,
        escalation: &HumanEscalation,
        action: RemediationAction,
        now: DateTime<Utc>,
    ) -> Result<EscalationDecisionSideEffects, EscalationLifecycleError> {
        if !matches!(
            action,
            RemediationAction::PauseScope | RemediationAction::CancelScope
        ) {
            return Ok(EscalationDecisionSideEffects::default());
        }
        let all_claims = self.store.claims_for_goal(&escalation.goal_id, None)?;
        let all_workers = self.store.workers_for_goal(&escalation.goal_id, None)?;
        let mut workers = all_workers
            .iter()
            .cloned()
            .map(|worker| (worker.id.as_str().to_string(), worker))
            .collect::<BTreeMap<_, _>>();
        let expected_worker_revisions = all_workers
            .iter()
            .map(|worker| (worker.id.as_str().to_string(), worker.metadata.updated_at))
            .collect::<BTreeMap<_, _>>();
        let mut worker_ids = BTreeSet::new();
        let mut package_ids = BTreeSet::new();
        let mut claim_updates = Vec::new();

        for claim in &all_claims {
            let worker = workers.get(claim.owner.as_str());
            if claim_matches_escalation_scope(claim, worker, &escalation.scope) {
                if let ClaimScope::WorkPackage { work_package_id } = &claim.scope {
                    package_ids.insert(work_package_id.as_str().to_string());
                }
                if claim.state != ClaimState::Active {
                    continue;
                }
                let expected_updated_at = claim.metadata.updated_at;
                let mut updated = claim.clone();
                let (state, kind, reason) = match action {
                    RemediationAction::PauseScope => (
                        ClaimState::Blocked,
                        ClaimOutcomeKind::Blocked,
                        "paused by the selected human remediation",
                    ),
                    RemediationAction::CancelScope => (
                        ClaimState::Revoked,
                        ClaimOutcomeKind::Revoked,
                        "cancelled by the selected human remediation",
                    ),
                    _ => unreachable!(),
                };
                updated.transition(
                    state,
                    ClaimOutcome {
                        kind,
                        reason: reason.into(),
                        artifact_id: None,
                        evidence_refs: vec![format!("escalation:{}", escalation.id.as_str())],
                        escalation_id: Some(escalation.id.clone()),
                        at: now,
                    },
                )?;
                worker_ids.insert(claim.owner.as_str().to_string());
                if let Some(worker) = workers.get_mut(claim.owner.as_str()) {
                    worker.active_claims.retain(|id| id != &claim.id);
                }
                claim_updates.push(ExpectedRecordUpdate {
                    record: updated,
                    expected_updated_at,
                });
            }
        }

        match &escalation.scope {
            EscalationScope::WorkPackage { work_package_id } => {
                package_ids.insert(work_package_id.as_str().to_string());
            }
            EscalationScope::Contract { contract_id } => {
                let mut contract_keys = BTreeSet::from([contract_id.as_str().to_string()]);
                if let Some(contract) = self.store.contract(contract_id)? {
                    contract_keys.insert(contract.stable_key);
                }
                for package in self
                    .store
                    .work_packages_for_goal(&escalation.goal_id, None)?
                {
                    if package
                        .produces_contracts
                        .iter()
                        .any(|value| contract_keys.contains(value))
                        || package
                            .consumes_contracts
                            .iter()
                            .any(|expectation| contract_keys.contains(&expectation.contract_id))
                    {
                        package_ids.insert(package.id.as_str().to_string());
                    }
                }
            }
            EscalationScope::Goal { goal_id } if goal_id == &escalation.goal_id => {
                package_ids.extend(
                    self.store
                        .work_packages_for_goal(&escalation.goal_id, None)?
                        .into_iter()
                        .map(|package| package.id.as_str().to_string()),
                );
            }
            _ => {}
        }

        let mut worker_updates = Vec::new();
        for worker_id in worker_ids {
            let Some(mut worker) = workers.remove(&worker_id) else {
                continue;
            };
            let expected_updated_at = expected_worker_revisions[&worker_id];
            if worker.active_claims.is_empty() {
                match action {
                    RemediationAction::PauseScope
                        if matches!(
                            worker.state,
                            WorkerState::Active | WorkerState::Waiting | WorkerState::Blocked
                        ) =>
                    {
                        worker.transition(WorkerState::Paused, now, None)?;
                    }
                    RemediationAction::CancelScope if !worker.state.is_terminal() => {
                        worker.transition(
                            WorkerState::Cancelled,
                            now,
                            Some("affected scope cancelled by user decision".into()),
                        )?;
                    }
                    _ => worker.metadata.touch(now),
                }
            } else {
                worker.metadata.touch(now);
            }
            worker_updates.push(ExpectedRecordUpdate {
                record: worker,
                expected_updated_at,
            });
        }

        let mut package_updates = Vec::new();
        for mut package in self
            .store
            .work_packages_for_goal(&escalation.goal_id, None)?
        {
            if !package_ids.contains(package.id.as_str()) {
                continue;
            }
            let expected_updated_at = package.metadata.updated_at;
            match action {
                RemediationAction::PauseScope
                    if matches!(
                        package.state,
                        WorkPackageState::Planned
                            | WorkPackageState::Ready
                            | WorkPackageState::Claimed
                            | WorkPackageState::Integrating
                    ) =>
                {
                    package.transition(WorkPackageState::Blocked, now)?;
                }
                RemediationAction::CancelScope if !package.state.is_terminal() => {
                    package.transition(WorkPackageState::Cancelled, now)?;
                }
                _ => continue,
            }
            package_updates.push(ExpectedRecordUpdate {
                record: package,
                expected_updated_at,
            });
        }
        Ok(EscalationDecisionSideEffects {
            claims: claim_updates,
            workers: worker_updates,
            work_packages: package_updates,
        })
    }

    fn build_staleness_snapshot(
        &self,
        escalation: &HumanEscalation,
        captured_at: DateTime<Utc>,
    ) -> Result<EscalationStalenessSnapshot, EscalationLifecycleError> {
        let all_claims = self.store.claims_for_goal(&escalation.goal_id, None)?;
        let all_workers = self.store.workers_for_goal(&escalation.goal_id, None)?;
        let all_packages = self
            .store
            .work_packages_for_goal(&escalation.goal_id, None)?;
        let all_contracts = self.store.contracts_for_goal(&escalation.goal_id)?;
        let worker_by_id = all_workers
            .iter()
            .map(|worker| (worker.id.as_str(), worker))
            .collect::<BTreeMap<_, _>>();

        let mut claim_ids = BTreeSet::new();
        let mut worker_ids = BTreeSet::new();
        let mut package_ids = BTreeSet::new();
        let mut contract_keys = BTreeSet::new();
        match &escalation.scope {
            EscalationScope::Claim { claim_id } => {
                claim_ids.insert(claim_id.as_str().to_string());
            }
            EscalationScope::WorkPackage { work_package_id } => {
                package_ids.insert(work_package_id.as_str().to_string());
            }
            EscalationScope::Contract { contract_id } => {
                contract_keys.insert(contract_id.as_str().to_string());
            }
            EscalationScope::IntegrationLane { repository_id } => {
                for worker in &all_workers {
                    if worker
                        .workspace
                        .as_ref()
                        .is_some_and(|workspace| workspace.repository_id == *repository_id)
                    {
                        worker_ids.insert(worker.id.as_str().to_string());
                    }
                }
            }
            EscalationScope::Goal { goal_id } if goal_id == &escalation.goal_id => {
                claim_ids.extend(all_claims.iter().map(|claim| claim.id.as_str().to_string()));
                worker_ids.extend(
                    all_workers
                        .iter()
                        .map(|worker| worker.id.as_str().to_string()),
                );
                package_ids.extend(
                    all_packages
                        .iter()
                        .map(|package| package.id.as_str().to_string()),
                );
                contract_keys.extend(all_contracts.iter().flat_map(|contract| {
                    [
                        contract.id.as_str().to_string(),
                        contract.stable_key.clone(),
                    ]
                }));
            }
            EscalationScope::Goal { .. } => {}
        }

        for claim in &all_claims {
            let worker = worker_by_id.get(claim.owner.as_str()).copied();
            let directly_relevant = claim_ids.contains(claim.id.as_str())
                || worker_ids.contains(claim.owner.as_str())
                || matches!(
                    &claim.scope,
                    ClaimScope::WorkPackage { work_package_id }
                        if package_ids.contains(work_package_id.as_str())
                )
                || matches!(&escalation.scope, EscalationScope::Contract { contract_id }
                    if claim.produces_contracts.iter().any(|value| value == contract_id.as_str())
                        || claim.consumes_contracts.iter().any(|expectation| expectation.contract_id == contract_id.as_str()));
            if directly_relevant || claim_matches_escalation_scope(claim, worker, &escalation.scope)
            {
                claim_ids.insert(claim.id.as_str().to_string());
                worker_ids.insert(claim.owner.as_str().to_string());
                if let ClaimScope::WorkPackage { work_package_id } = &claim.scope {
                    package_ids.insert(work_package_id.as_str().to_string());
                }
                contract_keys.extend(claim.produces_contracts.iter().cloned());
                contract_keys.extend(
                    claim
                        .consumes_contracts
                        .iter()
                        .map(|expectation| expectation.contract_id.clone()),
                );
            }
        }

        for contract in &all_contracts {
            if contract_keys.contains(contract.id.as_str())
                || contract_keys.contains(&contract.stable_key)
            {
                contract_keys.insert(contract.id.as_str().to_string());
                contract_keys.insert(contract.stable_key.clone());
                if let Some(producer) = &contract.producer {
                    package_ids.insert(producer.as_str().to_string());
                }
                package_ids.extend(
                    contract
                        .dependents
                        .iter()
                        .map(|dependent| dependent.work_package_id.as_str().to_string()),
                );
            }
        }
        for package in &all_packages {
            if package_ids.contains(package.id.as_str()) {
                contract_keys.extend(package.produces_contracts.iter().cloned());
                contract_keys.extend(
                    package
                        .consumes_contracts
                        .iter()
                        .map(|expectation| expectation.contract_id.clone()),
                );
            }
        }
        for claim in &all_claims {
            if matches!(
                &claim.scope,
                ClaimScope::WorkPackage { work_package_id }
                    if package_ids.contains(work_package_id.as_str())
            ) {
                claim_ids.insert(claim.id.as_str().to_string());
                worker_ids.insert(claim.owner.as_str().to_string());
                contract_keys.extend(claim.produces_contracts.iter().cloned());
                contract_keys.extend(
                    claim
                        .consumes_contracts
                        .iter()
                        .map(|expectation| expectation.contract_id.clone()),
                );
            }
        }

        let mut claims = all_claims
            .iter()
            .filter(|claim| claim_ids.contains(claim.id.as_str()))
            .map(claim_revision)
            .collect::<Vec<_>>();
        claims.sort_by(|left, right| left.claim_id.as_str().cmp(right.claim_id.as_str()));
        let mut workers = all_workers
            .iter()
            .filter(|worker| worker_ids.contains(worker.id.as_str()))
            .map(worker_revision)
            .collect::<Vec<_>>();
        workers.sort_by(|left, right| left.worker_id.as_str().cmp(right.worker_id.as_str()));
        let mut work_packages = all_packages
            .iter()
            .filter(|package| package_ids.contains(package.id.as_str()))
            .map(package_revision)
            .collect::<Vec<_>>();
        work_packages.sort_by(|left, right| {
            left.work_package_id
                .as_str()
                .cmp(right.work_package_id.as_str())
        });
        let mut contracts = all_contracts
            .iter()
            .filter(|contract| {
                contract_keys.contains(contract.id.as_str())
                    || contract_keys.contains(&contract.stable_key)
            })
            .map(contract_revision)
            .collect::<Vec<_>>();
        contracts.sort_by(|left, right| left.contract_id.as_str().cmp(right.contract_id.as_str()));
        let mut evidence_refs = escalation.evidence_refs.clone();
        evidence_refs.sort();
        evidence_refs.dedup();
        Ok(EscalationStalenessSnapshot {
            captured_at,
            scope: escalation.scope.clone(),
            evidence_refs,
            options: escalation.options.clone(),
            claims,
            workers,
            work_packages,
            contracts,
        })
    }

    pub fn resolve(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        actor: &str,
        reason: Option<String>,
        decision: EscalationDecision,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        self.transition(
            goal_id,
            escalation_id,
            EscalationState::Resolved,
            actor,
            reason,
            Some(decision),
            now,
        )
    }

    pub fn override_decision(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        actor: &str,
        reason: Option<String>,
        decision: EscalationDecision,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        self.transition(
            goal_id,
            escalation_id,
            EscalationState::Overridden,
            actor,
            reason,
            Some(decision),
            now,
        )
    }

    pub fn expire(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        reason: String,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        self.transition(
            goal_id,
            escalation_id,
            EscalationState::Expired,
            "system",
            Some(reason),
            None,
            now,
        )
    }

    pub fn cancel(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        actor: &str,
        reason: String,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        self.transition(
            goal_id,
            escalation_id,
            EscalationState::Cancelled,
            actor,
            Some(reason),
            None,
            now,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn transition(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
        next: EscalationState,
        actor: &str,
        reason: Option<String>,
        decision: Option<EscalationDecision>,
        now: DateTime<Utc>,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        let mut escalation = self.load_for_goal(goal_id, escalation_id)?;
        if escalation.state == next {
            return if escalation.decision == decision {
                Ok(escalation)
            } else {
                Err(EscalationLifecycleError::ConflictingReplay)
            };
        }
        let expected_updated_at = escalation.metadata.updated_at;
        escalation.transition_attributed(next, now, actor, reason, decision)?;
        if !self
            .store
            .update_escalation_if_revision(&escalation, expected_updated_at)?
        {
            return Err(EscalationLifecycleError::StaleRevision);
        }
        Ok(escalation)
    }

    fn load_for_goal(
        &self,
        goal_id: &str,
        escalation_id: &EscalationId,
    ) -> Result<HumanEscalation, EscalationLifecycleError> {
        let escalation = self
            .store
            .escalation(escalation_id)?
            .ok_or_else(|| EscalationLifecycleError::NotFound(escalation_id.as_str().into()))?;
        if escalation.goal_id != goal_id {
            return Err(EscalationLifecycleError::GoalMismatch {
                escalation_id: escalation.id.as_str().into(),
                actual_goal: escalation.goal_id,
                requested_goal: goal_id.into(),
            });
        }
        Ok(escalation)
    }
}

fn claim_matches_escalation_scope(
    claim: &Claim,
    worker: Option<&Worker>,
    scope: &EscalationScope,
) -> bool {
    match scope {
        EscalationScope::Claim { claim_id } => claim.id == *claim_id,
        EscalationScope::WorkPackage { work_package_id } => matches!(
            &claim.scope,
            super::domain::ClaimScope::WorkPackage {
                work_package_id: claim_work_package_id,
            } if claim_work_package_id == work_package_id
        ),
        EscalationScope::Contract { contract_id } => {
            claim
                .produces_contracts
                .iter()
                .any(|value| value == contract_id.as_str())
                || claim
                    .consumes_contracts
                    .iter()
                    .any(|expectation| expectation.contract_id == contract_id.as_str())
        }
        EscalationScope::IntegrationLane { repository_id } => worker
            .and_then(|worker| worker.workspace.as_ref())
            .is_some_and(|workspace| workspace.repository_id == *repository_id),
        EscalationScope::Goal { goal_id } => claim.goal_id == *goal_id,
    }
}

fn claim_revision(claim: &Claim) -> EscalationClaimRevision {
    let mut produces_contracts = claim.produces_contracts.clone();
    produces_contracts.sort();
    produces_contracts.dedup();
    let mut consumes_contracts = claim.consumes_contracts.clone();
    consumes_contracts.sort_by(|left, right| {
        left.contract_id
            .cmp(&right.contract_id)
            .then(left.expected_revision.cmp(&right.expected_revision))
    });
    EscalationClaimRevision {
        claim_id: claim.id.clone(),
        scope: claim.scope.clone(),
        owner: claim.owner.clone(),
        state: claim.state,
        base_revision: claim.base_revision.clone(),
        lease_generation: claim.lease_generation,
        produces_contracts,
        consumes_contracts,
        outcome: claim.outcome.clone(),
    }
}

fn worker_revision(worker: &Worker) -> EscalationWorkerRevision {
    let mut active_claims = worker.active_claims.clone();
    active_claims.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    EscalationWorkerRevision {
        worker_id: worker.id.clone(),
        state: worker.state,
        active_claims,
        current_run_id: worker.current_run_id.clone(),
        current_thread_id: worker.current_thread_id.clone(),
        workspace_repository_id: worker
            .workspace
            .as_ref()
            .map(|workspace| workspace.repository_id.clone()),
        workspace_branch: worker
            .workspace
            .as_ref()
            .map(|workspace| workspace.branch.clone()),
        workspace_base_revision: worker
            .workspace
            .as_ref()
            .map(|workspace| workspace.base_revision.clone()),
    }
}

fn package_revision(package: &super::domain::WorkPackage) -> EscalationPackageRevision {
    let mut feature_ids = package.feature_ids.clone();
    feature_ids.sort();
    feature_ids.dedup();
    let mut depends_on = package.depends_on.clone();
    depends_on.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    let mut produces_contracts = package.produces_contracts.clone();
    produces_contracts.sort();
    produces_contracts.dedup();
    let mut consumes_contracts = package.consumes_contracts.clone();
    consumes_contracts.sort_by(|left, right| {
        left.contract_id
            .cmp(&right.contract_id)
            .then(left.expected_revision.cmp(&right.expected_revision))
    });
    EscalationPackageRevision {
        work_package_id: package.id.clone(),
        state: package.state,
        feature_ids,
        depends_on,
        priority: package.priority,
        produces_contracts,
        consumes_contracts,
    }
}

fn contract_revision(contract: &super::domain::SharedContract) -> EscalationContractRevision {
    let mut dependents = contract.dependents.clone();
    dependents.sort_by(|left, right| {
        left.work_package_id
            .as_str()
            .cmp(right.work_package_id.as_str())
            .then(left.expected_revision.cmp(&right.expected_revision))
    });
    EscalationContractRevision {
        contract_id: contract.id.clone(),
        stable_key: contract.stable_key.clone(),
        revision: contract.revision,
        producer: contract.producer.clone(),
        dependents,
        last_changed_by: contract.last_changed_by.clone(),
    }
}

fn compare_staleness_snapshots(
    expected: &EscalationStalenessSnapshot,
    current: &EscalationStalenessSnapshot,
) -> Vec<EscalationStalenessFinding> {
    let mut findings = Vec::new();
    if expected.scope != current.scope {
        findings.push(EscalationStalenessFinding {
            kind: EscalationStalenessKind::Scope,
            changed_ids: vec!["scope".into()],
        });
    }
    if expected.evidence_refs != current.evidence_refs {
        findings.push(EscalationStalenessFinding {
            kind: EscalationStalenessKind::Evidence,
            changed_ids: changed_strings(&expected.evidence_refs, &current.evidence_refs),
        });
    }
    if expected.options != current.options {
        let mut changed_ids = changed_revision_ids(&expected.options, &current.options, |option| {
            option.id.clone()
        });
        if changed_ids.is_empty() {
            changed_ids.push("option-order".into());
        }
        findings.push(EscalationStalenessFinding {
            kind: EscalationStalenessKind::Options,
            changed_ids,
        });
    }
    push_revision_finding(
        &mut findings,
        EscalationStalenessKind::Claim,
        &expected.claims,
        &current.claims,
        |revision| revision.claim_id.as_str().to_string(),
    );
    push_revision_finding(
        &mut findings,
        EscalationStalenessKind::Worker,
        &expected.workers,
        &current.workers,
        |revision| revision.worker_id.as_str().to_string(),
    );
    push_revision_finding(
        &mut findings,
        EscalationStalenessKind::WorkPackage,
        &expected.work_packages,
        &current.work_packages,
        |revision| revision.work_package_id.as_str().to_string(),
    );
    push_revision_finding(
        &mut findings,
        EscalationStalenessKind::Contract,
        &expected.contracts,
        &current.contracts,
        |revision| revision.contract_id.as_str().to_string(),
    );
    findings
}

fn changed_strings(expected: &[String], current: &[String]) -> Vec<String> {
    let expected = expected.iter().cloned().collect::<BTreeSet<_>>();
    let current = current.iter().cloned().collect::<BTreeSet<_>>();
    expected.symmetric_difference(&current).cloned().collect()
}

fn push_revision_finding<T: PartialEq>(
    findings: &mut Vec<EscalationStalenessFinding>,
    kind: EscalationStalenessKind,
    expected: &[T],
    current: &[T],
    id: impl Fn(&T) -> String,
) {
    let changed_ids = changed_revision_ids(expected, current, id);
    if !changed_ids.is_empty() {
        findings.push(EscalationStalenessFinding { kind, changed_ids });
    }
}

fn changed_revision_ids<T: PartialEq>(
    expected: &[T],
    current: &[T],
    id: impl Fn(&T) -> String,
) -> Vec<String> {
    let expected = expected
        .iter()
        .map(|value| (id(value), value))
        .collect::<BTreeMap<_, _>>();
    let current = current
        .iter()
        .map(|value| (id(value), value))
        .collect::<BTreeMap<_, _>>();
    expected
        .keys()
        .chain(current.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|key| expected.get(key) != current.get(key))
        .collect()
}

fn staleness_revision_tokens(snapshot: &EscalationStalenessSnapshot) -> Vec<String> {
    let mut tokens = Vec::new();
    tokens.extend(snapshot.claims.iter().map(|revision| {
        format!(
            "claim:{}:{}:{}:{:?}",
            revision.claim_id.as_str(),
            revision.base_revision,
            revision.lease_generation,
            revision.state
        )
    }));
    tokens.extend(snapshot.workers.iter().map(|revision| {
        format!(
            "worker:{}:{:?}:{}:{}",
            revision.worker_id.as_str(),
            revision.state,
            revision.current_run_id.as_deref().unwrap_or("none"),
            revision.current_thread_id.as_deref().unwrap_or("none")
        )
    }));
    tokens.extend(snapshot.work_packages.iter().map(|revision| {
        format!(
            "package:{}:{:?}",
            revision.work_package_id.as_str(),
            revision.state
        )
    }));
    tokens.extend(snapshot.contracts.iter().map(|revision| {
        format!(
            "contract:{}:{}",
            revision.contract_id.as_str(),
            revision.revision
        )
    }));
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::claims::ClaimPolicy;
    use crate::coordination::domain::{
        ClaimId, ClaimScope, ContractExpectation, ContractId, ContractKind, EscalationKind,
        EscalationScope, EscalationSeverity, SharedContract, WorkPackage, WorkPackageId,
        WorkerPermissionProfile, WorkerState, WorkspaceBinding,
    };
    use crate::models::{IntegrationScope, Status};
    use crate::tracker::Tracker;

    fn active_worker(goal_id: &str, repository_id: &str, now: DateTime<Utc>) -> Worker {
        let mut worker = Worker::new(goal_id, now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.workspace = Some(WorkspaceBinding {
            repository_id: repository_id.into(),
            canonical_repository_path: format!("/{repository_id}"),
            worktree_path: format!("/{repository_id}/worktrees/{}", worker.id.as_str()),
            branch: format!("codex/{}", worker.id.as_str()),
            base_revision: "base-a".into(),
            created_at: Some(now),
            creation_evidence: vec!["test fixture".into()],
        });
        worker
    }

    fn escalation(now: DateTime<Utc>) -> HumanEscalation {
        HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: ClaimId::new(),
            },
            "Choose a compatibility strategy",
            now,
        )
    }

    fn decision(option_id: &str, actor: &str, now: DateTime<Utc>) -> EscalationDecision {
        EscalationDecision {
            option_id: option_id.into(),
            decided_by: actor.into(),
            accepted_risk: None,
            decided_at: now,
        }
    }

    #[test]
    fn persists_every_lifecycle_terminal_and_a_complete_audit_trail() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let service = EscalationLifecycleService::new(store.clone());
        let now = Utc::now();

        let opened = service.create(&escalation(now)).unwrap();
        let acknowledged = service
            .acknowledge(
                "goal-a",
                &opened.id,
                "user-a",
                Some("Reviewing options".into()),
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        let resolved_at = now + chrono::Duration::seconds(2);
        let resolved = service
            .resolve(
                "goal-a",
                &opened.id,
                "user-a",
                Some("Preserve compatibility".into()),
                decision("preserve-v2", "user-a", resolved_at),
                resolved_at,
            )
            .unwrap();
        assert_eq!(acknowledged.state, EscalationState::Acknowledged);
        assert_eq!(resolved.state, EscalationState::Resolved);
        assert_eq!(resolved.audit_history.len(), 3);
        assert_eq!(resolved.audit_history[1].actor, "user-a");
        assert_eq!(resolved.audit_history[2].decision, resolved.decision);
        assert_eq!(
            service
                .resolve(
                    "goal-a",
                    &opened.id,
                    "user-a",
                    Some("retry".into()),
                    decision("preserve-v2", "user-a", resolved_at),
                    resolved_at,
                )
                .unwrap()
                .id,
            opened.id
        );
        assert!(
            service
                .cancel(
                    "goal-a",
                    &opened.id,
                    "user-a",
                    "too late".into(),
                    now + chrono::Duration::seconds(3),
                )
                .is_err()
        );

        let override_item = service.create(&escalation(now)).unwrap();
        let override_at = now + chrono::Duration::seconds(1);
        assert_eq!(
            service
                .override_decision(
                    "goal-a",
                    &override_item.id,
                    "user-b",
                    Some("Accept migration risk".into()),
                    EscalationDecision {
                        option_id: "adopt-v3".into(),
                        decided_by: "user-b".into(),
                        accepted_risk: Some("Dependents must migrate".into()),
                        decided_at: override_at,
                    },
                    override_at,
                )
                .unwrap()
                .state,
            EscalationState::Overridden
        );
        let expired_item = service.create(&escalation(now)).unwrap();
        assert_eq!(
            service
                .expire(
                    "goal-a",
                    &expired_item.id,
                    "Evidence revision changed".into(),
                    now + chrono::Duration::seconds(1),
                )
                .unwrap()
                .state,
            EscalationState::Expired
        );
        let cancelled_item = service.create(&escalation(now)).unwrap();
        assert_eq!(
            service
                .cancel(
                    "goal-a",
                    &cancelled_item.id,
                    "user-c",
                    "Goal cancelled".into(),
                    now + chrono::Duration::seconds(1),
                )
                .unwrap()
                .state,
            EscalationState::Cancelled
        );
        drop(service);
        drop(store);

        let reopened = SqliteCoordinationStore::open(&database).unwrap();
        let restored = reopened.escalation(&opened.id).unwrap().unwrap();
        assert_eq!(restored.state, EscalationState::Resolved);
        assert_eq!(restored.audit_history.len(), 3);
    }

    #[test]
    fn compare_and_swap_rejects_two_transitions_from_one_revision() {
        let directory = tempfile::tempdir().unwrap();
        let store =
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap();
        let now = Utc::now();
        let original = escalation(now);
        store.upsert_escalation(&original).unwrap();
        let mut acknowledged = store.escalation(&original.id).unwrap().unwrap();
        let mut cancelled = acknowledged.clone();
        let expected = original.metadata.updated_at;
        acknowledged
            .transition_attributed(
                EscalationState::Acknowledged,
                now + chrono::Duration::seconds(1),
                "user-a",
                None,
                None,
            )
            .unwrap();
        cancelled
            .transition_attributed(
                EscalationState::Cancelled,
                now + chrono::Duration::seconds(1),
                "user-b",
                Some("No longer needed".into()),
                None,
            )
            .unwrap();
        assert!(
            store
                .update_escalation_if_revision(&acknowledged, expected)
                .unwrap()
        );
        assert!(
            !store
                .update_escalation_if_revision(&cancelled, expected)
                .unwrap()
        );
        assert_eq!(
            store.escalation(&original.id).unwrap().unwrap().state,
            EscalationState::Acknowledged
        );
    }

    #[test]
    fn distinguishes_scope_blockers_from_continuable_decision_points() {
        let now = Utc::now();
        let blocker = HumanEscalation::new(
            "goal-a",
            EscalationKind::HardBlocker,
            EscalationSeverity::Critical,
            EscalationScope::Claim {
                claim_id: ClaimId::new(),
            },
            "The migration cannot continue safely",
            now,
        );
        let mut decision_point = HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: ClaimId::new(),
            },
            "Choose a compatibility policy",
            now,
        );
        assert!(blocker.impact().blocks_affected_scope);
        assert!(!blocker.impact().may_continue_before_resolution);
        assert_eq!(
            blocker.impact().continuation_mode,
            crate::coordination::domain::EscalationContinuationMode::ScopeBlocked
        );
        assert!(!decision_point.impact().blocks_affected_scope);
        assert!(decision_point.impact().may_continue_before_resolution);
        assert_eq!(
            decision_point.impact().continuation_mode,
            crate::coordination::domain::EscalationContinuationMode::ContinueAtAcceptedRisk
        );
        let override_at = now + chrono::Duration::seconds(1);
        assert!(
            decision_point
                .transition_attributed(
                    EscalationState::Overridden,
                    override_at,
                    "user-a",
                    Some("Continue while accepting the compatibility risk".into()),
                    Some(EscalationDecision {
                        option_id: "continue".into(),
                        decided_by: "user-a".into(),
                        accepted_risk: None,
                        decided_at: override_at,
                    }),
                )
                .is_err()
        );
    }

    #[test]
    fn generates_only_applicable_typed_remediation_options() {
        let now = Utc::now();
        let blocker = HumanEscalation::new(
            "goal-a",
            EscalationKind::HardBlocker,
            EscalationSeverity::Critical,
            EscalationScope::WorkPackage {
                work_package_id: WorkPackageId::new(),
            },
            "Package integration is blocked",
            now,
        );
        let blocker_options = generate_remediation_options(
            &blocker,
            &RemediationCapabilities {
                reconcile_now: true,
                wait: true,
                split_package: true,
                pause_scope: true,
                cancel_scope: true,
                allow_override: true,
                directions: vec![],
            },
        )
        .unwrap();
        for action in [
            RemediationAction::ReconcileNow,
            RemediationAction::Wait,
            RemediationAction::SplitPackage,
            RemediationAction::PauseScope,
            RemediationAction::CancelScope,
            RemediationAction::OverrideRisk,
        ] {
            assert!(blocker_options.iter().any(|option| option.action == action));
        }
        assert_eq!(
            blocker_options
                .iter()
                .filter(|option| option.recommended)
                .count(),
            1
        );

        let decision_point = escalation(now);
        let decision_options = generate_remediation_options(
            &decision_point,
            &RemediationCapabilities {
                reconcile_now: false,
                wait: true,
                split_package: false,
                pause_scope: true,
                cancel_scope: true,
                allow_override: true,
                directions: vec![
                    RemediationDirection {
                        id: "preserve-v2".into(),
                        label: "Preserve v2".into(),
                        description: "Keep the existing contract.".into(),
                        consequences: vec!["New behavior is deferred.".into()],
                        recommended: true,
                    },
                    RemediationDirection {
                        id: "adopt-v3".into(),
                        label: "Adopt v3".into(),
                        description: "Migrate to the new contract.".into(),
                        consequences: vec!["Dependents must update.".into()],
                        recommended: false,
                    },
                ],
            },
        )
        .unwrap();
        assert!(
            decision_options
                .iter()
                .any(|option| option.action == RemediationAction::ChooseDirection)
        );
        assert_eq!(
            decision_options
                .iter()
                .filter(|option| option.recommended)
                .count(),
            1
        );
        assert!(decision_options.iter().all(|option| {
            !option.id.is_empty()
                && !option.label.is_empty()
                && !option.description.is_empty()
                && !option.consequences.is_empty()
        }));

        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let service = EscalationLifecycleService::new(store.clone());
        service.create(&blocker).unwrap();
        let persisted = service
            .set_remediation_options(
                "goal-a",
                &blocker.id,
                &RemediationCapabilities {
                    reconcile_now: true,
                    wait: true,
                    split_package: true,
                    pause_scope: true,
                    cancel_scope: true,
                    allow_override: false,
                    directions: vec![],
                },
                "system",
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        assert!(!persisted.options.is_empty());
        assert_eq!(persisted.audit_history.len(), 2);
        drop(service);
        drop(store);
        assert_eq!(
            SqliteCoordinationStore::open(&database)
                .unwrap()
                .escalation(&blocker.id)
                .unwrap()
                .unwrap()
                .options,
            persisted.options
        );
    }

    #[test]
    fn blocks_only_the_claim_in_scope_and_preserves_unrelated_progress() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "").unwrap();
        for id in ["first", "second"] {
            tracker
                .add_feature("goal-a", id, id, "", Status::Planned)
                .unwrap();
        }
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let first_worker = active_worker("goal-a", "repo-a", now);
        let second_worker = active_worker("goal-a", "repo-b", now);
        store.upsert_worker(&first_worker).unwrap();
        store.upsert_worker(&second_worker).unwrap();
        let claims = ClaimService::new(tracker, store.clone(), ClaimPolicy::default());
        let first = claims
            .claim_ready_unit(
                "goal-a",
                &first_worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-a",
                "claim-first",
                now,
            )
            .unwrap();
        let second = claims
            .claim_ready_unit(
                "goal-a",
                &second_worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "second".into(),
                }),
                "base-a",
                "claim-second",
                now,
            )
            .unwrap();
        let mut blocker = HumanEscalation::new(
            "goal-a",
            EscalationKind::HardBlocker,
            EscalationSeverity::Critical,
            EscalationScope::Claim {
                claim_id: first.id.clone(),
            },
            "The first claim cannot continue safely",
            now,
        );
        blocker.evidence_refs.push("validation:failed".into());
        let lifecycle = EscalationLifecycleService::new(store.clone());
        lifecycle.create(&blocker).unwrap();

        let report = lifecycle
            .enforce_scope_block(
                &claims,
                "goal-a",
                &blocker.id,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();

        assert_eq!(report.blocked_claim_ids, vec![first.id.clone()]);
        assert_eq!(report.unaffected_active_claim_ids, vec![second.id.clone()]);
        let blocked = store.claim(&first.id).unwrap().unwrap();
        assert_eq!(blocked.state, ClaimState::Blocked);
        assert_eq!(
            blocked.outcome.unwrap().escalation_id,
            Some(blocker.id.clone())
        );
        assert_eq!(
            store.claim(&second.id).unwrap().unwrap().state,
            ClaimState::Active
        );
        assert!(
            store
                .worker(&second_worker.id)
                .unwrap()
                .unwrap()
                .active_claims
                .contains(&second.id)
        );

        let decision_point = escalation(now);
        lifecycle.create(&decision_point).unwrap();
        assert!(matches!(
            lifecycle.enforce_scope_block(
                &claims,
                "goal-a",
                &decision_point.id,
                now + chrono::Duration::seconds(2),
            ),
            Err(EscalationLifecycleError::InvalidScopeBlock(_))
        ));
        assert_eq!(
            store.claim(&second.id).unwrap().unwrap().state,
            ClaimState::Active
        );
    }

    #[test]
    fn matches_claim_package_contract_lane_and_goal_scopes_without_broadening() {
        let now = Utc::now();
        let worker = active_worker("goal-a", "repo-a", now);
        let package_id = WorkPackageId::new();
        let contract_id = ContractId::new();
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::WorkPackage {
                work_package_id: package_id.clone(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        claim.produces_contracts = vec![contract_id.as_str().into()];
        claim.consumes_contracts = vec![ContractExpectation {
            contract_id: "contract-consumed".into(),
            expected_revision: 1,
        }];

        for scope in [
            EscalationScope::Claim {
                claim_id: claim.id.clone(),
            },
            EscalationScope::WorkPackage {
                work_package_id: package_id,
            },
            EscalationScope::Contract {
                contract_id: contract_id.clone(),
            },
            EscalationScope::IntegrationLane {
                repository_id: "repo-a".into(),
            },
            EscalationScope::Goal {
                goal_id: "goal-a".into(),
            },
        ] {
            assert!(claim_matches_escalation_scope(
                &claim,
                Some(&worker),
                &scope
            ));
        }
        assert!(claim_matches_escalation_scope(
            &claim,
            Some(&worker),
            &EscalationScope::Contract {
                contract_id: ContractId::parse("contract-consumed").unwrap(),
            }
        ));
        for scope in [
            EscalationScope::Claim {
                claim_id: ClaimId::new(),
            },
            EscalationScope::WorkPackage {
                work_package_id: WorkPackageId::new(),
            },
            EscalationScope::IntegrationLane {
                repository_id: "repo-b".into(),
            },
            EscalationScope::Goal {
                goal_id: "goal-b".into(),
            },
        ] {
            assert!(!claim_matches_escalation_scope(
                &claim,
                Some(&worker),
                &scope
            ));
        }
    }

    #[test]
    fn package_scope_persists_the_narrow_package_block() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut package = super::super::domain::WorkPackage::new(
            "goal-a",
            "Affected package",
            vec!["feature-a".into()],
            now,
        )
        .unwrap();
        package.transition(WorkPackageState::Planned, now).unwrap();
        package.transition(WorkPackageState::Ready, now).unwrap();
        store.upsert_work_package(&package).unwrap();
        let blocker = HumanEscalation::new(
            "goal-a",
            EscalationKind::HardBlocker,
            EscalationSeverity::High,
            EscalationScope::WorkPackage {
                work_package_id: package.id.clone(),
            },
            "Only this package is blocked",
            now,
        );
        let lifecycle = EscalationLifecycleService::new(store.clone());
        lifecycle.create(&blocker).unwrap();
        let claims = ClaimService::new(
            Tracker::new(directory.path().join("goals")),
            store.clone(),
            ClaimPolicy::default(),
        );

        let report = lifecycle
            .enforce_scope_block(
                &claims,
                "goal-a",
                &blocker.id,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();

        assert!(report.blocked_claim_ids.is_empty());
        assert_eq!(
            store.work_package(&package.id).unwrap().unwrap().state,
            WorkPackageState::Blocked
        );
    }

    #[test]
    fn detects_changed_evidence_options_claims_workers_packages_and_contracts() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = active_worker("goal-a", "repo-a", now);
        let mut package =
            WorkPackage::new("goal-a", "Contract package", vec!["feature-a".into()], now).unwrap();
        package.transition(WorkPackageState::Planned, now).unwrap();
        package.transition(WorkPackageState::Ready, now).unwrap();
        package.transition(WorkPackageState::Claimed, now).unwrap();
        let mut contract =
            SharedContract::new("goal-a", "api.v1", "API v1", ContractKind::Api, now);
        contract.producer = Some(package.id.clone());
        package.produces_contracts = vec![contract.id.as_str().into()];
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::WorkPackage {
                work_package_id: package.id.clone(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        claim.produces_contracts = vec![contract.id.as_str().into()];
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.upsert_work_package(&package).unwrap();
        store.upsert_contract(&contract).unwrap();
        store.insert_claim(&claim).unwrap();

        let mut escalation = HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Contract {
                contract_id: contract.id.clone(),
            },
            "Choose the API compatibility direction",
            now,
        );
        escalation.evidence_refs = vec!["event:contract-change".into(), "diff:api".into()];
        escalation.options = vec![
            RemediationOption {
                id: "preserve-v1".into(),
                action: RemediationAction::ChooseDirection,
                label: "Preserve v1".into(),
                description: "Keep compatibility.".into(),
                consequences: vec!["New behavior is deferred.".into()],
                recommended: true,
            },
            RemediationOption {
                id: "adopt-v2".into(),
                action: RemediationAction::ChooseDirection,
                label: "Adopt v2".into(),
                description: "Move dependents to v2.".into(),
                consequences: vec!["Consumers must migrate.".into()],
                recommended: false,
            },
        ];
        let service = EscalationLifecycleService::new(store.clone());
        service.create(&escalation).unwrap();
        let captured = service
            .capture_staleness_snapshot(
                "goal-a",
                &escalation.id,
                "system",
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        assert!(captured.staleness_snapshot.is_some());
        assert!(!captured.stale_after_revisions.is_empty());
        assert!(
            !service
                .validate_staleness("goal-a", &escalation.id, now + chrono::Duration::seconds(2),)
                .unwrap()
                .stale
        );

        let mut heartbeat_only_claim = store.claim(&claim.id).unwrap().unwrap();
        heartbeat_only_claim
            .renew(
                heartbeat_only_claim.lease_generation,
                now + chrono::Duration::seconds(2),
                now + chrono::Duration::minutes(7),
            )
            .unwrap();
        store.upsert_claim(&heartbeat_only_claim).unwrap();
        let mut heartbeat_only_worker = store.worker(&worker.id).unwrap().unwrap();
        heartbeat_only_worker.last_heartbeat_at = Some(now + chrono::Duration::seconds(2));
        store.upsert_worker(&heartbeat_only_worker).unwrap();
        assert!(
            !service
                .validate_staleness("goal-a", &escalation.id, now + chrono::Duration::seconds(3),)
                .unwrap()
                .stale
        );

        let mut changed_escalation = store.escalation(&escalation.id).unwrap().unwrap();
        changed_escalation
            .evidence_refs
            .push("validation:new-failure".into());
        changed_escalation.options[0].recommended = false;
        changed_escalation.options[1].recommended = true;
        store.upsert_escalation(&changed_escalation).unwrap();
        let mut changed_claim = store.claim(&claim.id).unwrap().unwrap();
        changed_claim.base_revision = "base-b".into();
        store.upsert_claim(&changed_claim).unwrap();
        let mut changed_worker = store.worker(&worker.id).unwrap().unwrap();
        changed_worker.current_thread_id = Some("thread-new".into());
        store.upsert_worker(&changed_worker).unwrap();
        package.priority = 50;
        store.upsert_work_package(&package).unwrap();
        contract.revise(
            worker.id.clone(),
            "Breaking response field",
            now + chrono::Duration::seconds(4),
        );
        store.upsert_contract(&contract).unwrap();

        let report = service
            .validate_staleness("goal-a", &escalation.id, now + chrono::Duration::seconds(5))
            .unwrap();
        assert!(report.stale);
        assert_eq!(
            report
                .findings
                .iter()
                .map(|finding| finding.kind)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([
                EscalationStalenessKind::Evidence,
                EscalationStalenessKind::Options,
                EscalationStalenessKind::Claim,
                EscalationStalenessKind::Worker,
                EscalationStalenessKind::WorkPackage,
                EscalationStalenessKind::Contract,
            ])
        );
    }

    #[test]
    fn refuses_staleness_validation_without_a_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let escalation = escalation(now);
        let service = EscalationLifecycleService::new(store);
        service.create(&escalation).unwrap();
        assert!(matches!(
            service.validate_staleness(
                "goal-a",
                &escalation.id,
                now + chrono::Duration::seconds(1),
            ),
            Err(EscalationLifecycleError::MissingStalenessSnapshot)
        ));
    }

    #[test]
    fn applies_every_remediation_action_with_one_attributed_event_and_replay() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = EscalationLifecycleService::new(store.clone());
        let now = Utc::now();
        let cases = [
            (RemediationAction::ReconcileNow, EscalationState::Resolved),
            (RemediationAction::SplitPackage, EscalationState::Resolved),
            (
                RemediationAction::ChooseDirection,
                EscalationState::Resolved,
            ),
            (RemediationAction::Wait, EscalationState::Acknowledged),
            (RemediationAction::PauseScope, EscalationState::Acknowledged),
            (RemediationAction::CancelScope, EscalationState::Cancelled),
            (RemediationAction::OverrideRisk, EscalationState::Overridden),
        ];

        for (index, (action, expected_state)) in cases.into_iter().enumerate() {
            let opened_at = now + chrono::Duration::seconds(index as i64 * 10);
            let mut escalation = HumanEscalation::new(
                "goal-a",
                EscalationKind::DecisionPoint,
                EscalationSeverity::High,
                EscalationScope::Claim {
                    claim_id: ClaimId::new(),
                },
                format!("Decision {index}"),
                opened_at,
            );
            let option_id = format!("option-{index}");
            escalation.evidence_refs = vec![
                format!("event:decision-{index}"),
                format!("validation:decision-{index}"),
            ];
            let selected_option = RemediationOption {
                id: option_id.clone(),
                action,
                label: format!("Option {index}"),
                description: "Apply the selected remediation.".into(),
                consequences: vec!["The result is recorded.".into()],
                recommended: true,
            };
            let alternative = RemediationOption {
                id: format!("alternative-{index}"),
                action: RemediationAction::Wait,
                label: format!("Alternative {index}"),
                description: "Defer the selected remediation.".into(),
                consequences: vec!["The decision remains pending.".into()],
                recommended: false,
            };
            escalation.options = vec![selected_option.clone(), alternative.clone()];
            service.create(&escalation).unwrap();
            service
                .capture_staleness_snapshot(
                    "goal-a",
                    &escalation.id,
                    "system",
                    opened_at + chrono::Duration::seconds(1),
                )
                .unwrap();
            let decided_at = opened_at + chrono::Duration::seconds(2);
            let decision = EscalationDecision {
                option_id,
                decided_by: "user-a".into(),
                accepted_risk: (action == RemediationAction::OverrideRisk)
                    .then(|| "Proceed despite compatibility risk".into()),
                decided_at,
            };
            let applied = service
                .apply_user_decision(
                    "goal-a",
                    &escalation.id,
                    decision.clone(),
                    Some("User selected this remediation".into()),
                    decided_at,
                )
                .unwrap();
            assert_eq!(applied.action, action);
            assert_eq!(applied.escalation.state, expected_state);
            assert_eq!(applied.escalation.decision, Some(decision.clone()));
            let decision_audit = applied.escalation.decision_audit.as_ref().unwrap();
            assert_eq!(decision_audit.decided_by, decision.decided_by);
            assert_eq!(decision_audit.decided_at, decision.decided_at);
            assert_eq!(decision_audit.selected_option, selected_option);
            assert_eq!(decision_audit.alternatives, vec![alternative]);
            assert_eq!(decision_audit.evidence_refs, escalation.evidence_refs);
            assert_eq!(decision_audit.accepted_risk, decision.accepted_risk);
            assert_eq!(decision_audit.downstream_outcomes.len(), 1);
            assert_eq!(
                decision_audit.downstream_outcomes[0].kind,
                match action {
                    RemediationAction::Wait => EscalationOutcomeKind::Waiting,
                    RemediationAction::PauseScope => EscalationOutcomeKind::ScopePaused,
                    RemediationAction::CancelScope => EscalationOutcomeKind::ScopeCancelled,
                    _ => EscalationOutcomeKind::DecisionApplied,
                }
            );
            assert_eq!(
                applied.escalation.audit_history.last().unwrap().actor,
                "user-a"
            );

            let replay = service
                .apply_user_decision("goal-a", &escalation.id, decision.clone(), None, decided_at)
                .unwrap();
            assert_eq!(replay.event_sequence, applied.event_sequence);
            assert_eq!(
                store
                    .escalation(&escalation.id)
                    .unwrap()
                    .unwrap()
                    .decision_audit,
                applied.escalation.decision_audit
            );
            let events = store
                .latest_events_for_goal("goal-a", Some(escalation.id.as_str()), 10)
                .unwrap();
            assert_eq!(events.len(), 1);
            assert!(matches!(
                events[0].typed_payload().unwrap(),
                Some(super::super::domain::CoordinationEventPayload::HumanDecision(
                    HumanDecisionEventPayload { escalation_id, .. }
                )) if escalation_id == escalation.id
            ));
        }
    }

    #[test]
    fn rejects_a_stale_user_decision_without_transition_or_event() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = EscalationLifecycleService::new(store.clone());
        let now = Utc::now();
        let mut escalation = escalation(now);
        escalation.options = vec![RemediationOption {
            id: "choose-a".into(),
            action: RemediationAction::ChooseDirection,
            label: "Choose A".into(),
            description: "Choose direction A.".into(),
            consequences: vec!["Direction B is deferred.".into()],
            recommended: true,
        }];
        service.create(&escalation).unwrap();
        service
            .capture_staleness_snapshot(
                "goal-a",
                &escalation.id,
                "system",
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        let mut changed = store.escalation(&escalation.id).unwrap().unwrap();
        changed.evidence_refs.push("event:new".into());
        store.upsert_escalation(&changed).unwrap();
        let decided_at = now + chrono::Duration::seconds(2);

        assert!(matches!(
            service.apply_user_decision(
                "goal-a",
                &escalation.id,
                EscalationDecision {
                    option_id: "choose-a".into(),
                    decided_by: "user-a".into(),
                    accepted_risk: None,
                    decided_at,
                },
                None,
                decided_at,
            ),
            Err(EscalationLifecycleError::StaleDecision { .. })
        ));
        assert_eq!(
            store.escalation(&escalation.id).unwrap().unwrap().state,
            EscalationState::Open
        );
        assert!(
            store
                .latest_events_for_goal("goal-a", Some(escalation.id.as_str()), 10,)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn concurrent_user_choices_commit_exactly_one_decision_and_event() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = EscalationLifecycleService::new(store.clone());
        let now = Utc::now();
        let mut escalation = escalation(now);
        escalation.options = ["a", "b"]
            .into_iter()
            .map(|id| RemediationOption {
                id: id.into(),
                action: RemediationAction::ChooseDirection,
                label: id.to_uppercase(),
                description: format!("Choose {id}."),
                consequences: vec![format!("Direction {id} is applied.")],
                recommended: id == "a",
            })
            .collect();
        service.create(&escalation).unwrap();
        service
            .capture_staleness_snapshot(
                "goal-a",
                &escalation.id,
                "system",
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        let decided_at = now + chrono::Duration::seconds(2);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut handles = Vec::new();
        for option_id in ["a", "b"] {
            let service = service.clone();
            let barrier = barrier.clone();
            let escalation_id = escalation.id.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                service.apply_user_decision(
                    "goal-a",
                    &escalation_id,
                    EscalationDecision {
                        option_id: option_id.into(),
                        decided_by: format!("user-{option_id}"),
                        accepted_risk: None,
                        decided_at,
                    },
                    None,
                    decided_at,
                )
            }));
        }
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            store
                .latest_events_for_goal("goal-a", Some(escalation.id.as_str()), 10,)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store.escalation(&escalation.id).unwrap().unwrap().state,
            EscalationState::Resolved
        );
    }

    #[test]
    fn pause_and_cancel_decisions_atomically_update_the_affected_scope() {
        for (action, claim_state, worker_state, package_state) in [
            (
                RemediationAction::PauseScope,
                ClaimState::Blocked,
                WorkerState::Paused,
                WorkPackageState::Blocked,
            ),
            (
                RemediationAction::CancelScope,
                ClaimState::Revoked,
                WorkerState::Cancelled,
                WorkPackageState::Cancelled,
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let store = Arc::new(
                SqliteCoordinationStore::open(directory.path().join("coordination.sqlite"))
                    .unwrap(),
            );
            let service = EscalationLifecycleService::new(store.clone());
            let now = Utc::now();
            let mut worker = active_worker("goal-a", "repo-a", now);
            let mut package =
                WorkPackage::new("goal-a", "Affected package", vec!["feature-a".into()], now)
                    .unwrap();
            package.transition(WorkPackageState::Planned, now).unwrap();
            package.transition(WorkPackageState::Ready, now).unwrap();
            package.transition(WorkPackageState::Claimed, now).unwrap();
            let claim = Claim::new(
                "goal-a",
                ClaimScope::WorkPackage {
                    work_package_id: package.id.clone(),
                },
                worker.id.clone(),
                "base-a",
                now,
                now + chrono::Duration::minutes(5),
            )
            .unwrap();
            worker.active_claims.push(claim.id.clone());
            store.upsert_worker(&worker).unwrap();
            store.upsert_work_package(&package).unwrap();
            store.insert_claim(&claim).unwrap();
            let mut escalation = HumanEscalation::new(
                "goal-a",
                EscalationKind::DecisionPoint,
                EscalationSeverity::High,
                EscalationScope::WorkPackage {
                    work_package_id: package.id.clone(),
                },
                "Choose what to do with this package",
                now,
            );
            escalation.options = vec![RemediationOption {
                id: "selected".into(),
                action,
                label: "Selected action".into(),
                description: "Apply this action to the package.".into(),
                consequences: vec!["The affected work stops.".into()],
                recommended: true,
            }];
            service.create(&escalation).unwrap();
            service
                .capture_staleness_snapshot(
                    "goal-a",
                    &escalation.id,
                    "system",
                    now + chrono::Duration::seconds(1),
                )
                .unwrap();
            let decided_at = now + chrono::Duration::seconds(2);
            service
                .apply_user_decision(
                    "goal-a",
                    &escalation.id,
                    EscalationDecision {
                        option_id: "selected".into(),
                        decided_by: "user-a".into(),
                        accepted_risk: None,
                        decided_at,
                    },
                    None,
                    decided_at,
                )
                .unwrap();

            let persisted_claim = store.claim(&claim.id).unwrap().unwrap();
            assert_eq!(persisted_claim.state, claim_state);
            assert_eq!(
                persisted_claim.outcome.unwrap().escalation_id,
                Some(escalation.id.clone())
            );
            assert_eq!(
                store.worker(&worker.id).unwrap().unwrap().state,
                worker_state
            );
            assert_eq!(
                store.work_package(&package.id).unwrap().unwrap().state,
                package_state
            );
            assert_eq!(
                store
                    .latest_events_for_goal("goal-a", Some(escalation.id.as_str()), 10,)
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[test]
    fn human_escalation_journey_presents_blocks_rejects_stale_choice_and_resumes_work() {
        for replace_terminal_worker in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let tracker = Tracker::new(directory.path().join("goals"));
            tracker.create_goal("goal-a", "Goal A", "").unwrap();
            tracker
                .add_feature(
                    "goal-a",
                    "feature-a",
                    "Feature A",
                    "Implement feature A",
                    Status::Planned,
                )
                .unwrap();
            let store = Arc::new(
                SqliteCoordinationStore::open(directory.path().join("coordination.sqlite"))
                    .unwrap(),
            );
            let service = EscalationLifecycleService::new(store.clone());
            let now = Utc::now();
            let mut worker = active_worker("goal-a", "repo-a", now);
            worker.permission_profile = Some(WorkerPermissionProfile::default());
            let mut package =
                WorkPackage::new("goal-a", "Feature A package", vec!["feature-a".into()], now)
                    .unwrap();
            package.transition(WorkPackageState::Planned, now).unwrap();
            package.transition(WorkPackageState::Ready, now).unwrap();
            package.transition(WorkPackageState::Claimed, now).unwrap();
            tracker
                .add_work_package(
                    "goal-a",
                    package.id.as_str(),
                    "Feature A package",
                    "Implement feature A",
                    vec!["feature-a".into()],
                    vec![],
                    10,
                    IntegrationScope::WorkPackage,
                )
                .unwrap();
            let claim = Claim::new(
                "goal-a",
                ClaimScope::WorkPackage {
                    work_package_id: package.id.clone(),
                },
                worker.id.clone(),
                "base-a",
                now,
                now + chrono::Duration::minutes(5),
            )
            .unwrap();
            worker.active_claims.push(claim.id.clone());
            store.upsert_worker(&worker).unwrap();
            store.upsert_work_package(&package).unwrap();
            store.insert_claim(&claim).unwrap();
            let mut escalation = HumanEscalation::new(
                "goal-a",
                EscalationKind::HardBlocker,
                EscalationSeverity::Critical,
                EscalationScope::WorkPackage {
                    work_package_id: package.id.clone(),
                },
                "Choose a safe package direction",
                now,
            );
            escalation.evidence_refs = vec!["validation:package-conflict".into()];
            service.create(&escalation).unwrap();
            let presented = service
                .set_remediation_options(
                    "goal-a",
                    &escalation.id,
                    &RemediationCapabilities {
                        reconcile_now: true,
                        wait: true,
                        split_package: false,
                        pause_scope: false,
                        cancel_scope: false,
                        allow_override: false,
                        directions: vec![],
                    },
                    "system",
                    now + chrono::Duration::seconds(1),
                )
                .unwrap();
            assert_eq!(presented.options.len(), 2);
            assert!(presented.options.iter().all(|option| {
                !option.label.is_empty()
                    && !option.description.is_empty()
                    && !option.consequences.is_empty()
            }));
            assert_eq!(
                presented
                    .options
                    .iter()
                    .filter(|option| option.recommended)
                    .count(),
                1
            );
            let claim_service =
                ClaimService::new(tracker.clone(), store.clone(), ClaimPolicy::default());
            let blocked = service
                .enforce_scope_block(
                    &claim_service,
                    "goal-a",
                    &escalation.id,
                    now + chrono::Duration::seconds(2),
                )
                .unwrap();
            assert_eq!(blocked.blocked_claim_ids, vec![claim.id.clone()]);
            assert!(blocked.unaffected_active_claim_ids.is_empty());
            if replace_terminal_worker {
                let mut failed = store.worker(&worker.id).unwrap().unwrap();
                failed
                    .transition(
                        WorkerState::Failed,
                        now + chrono::Duration::seconds(3),
                        Some("worker runtime failed while blocked".into()),
                    )
                    .unwrap();
                store.upsert_worker(&failed).unwrap();
            }
            service
                .capture_staleness_snapshot(
                    "goal-a",
                    &escalation.id,
                    "system",
                    now + chrono::Duration::seconds(4),
                )
                .unwrap();
            let mut changed = store.escalation(&escalation.id).unwrap().unwrap();
            changed
                .evidence_refs
                .push("validation:new-conflict-evidence".into());
            store.upsert_escalation(&changed).unwrap();
            let stale_at = now + chrono::Duration::seconds(5);
            let stale = service
                .validate_staleness("goal-a", &escalation.id, stale_at)
                .unwrap();
            assert!(stale.stale);
            assert!(
                stale
                    .findings
                    .iter()
                    .any(|finding| finding.kind == EscalationStalenessKind::Evidence)
            );
            assert!(matches!(
                service.apply_user_decision(
                    "goal-a",
                    &escalation.id,
                    EscalationDecision {
                        option_id: "reconcile-now".into(),
                        decided_by: "user-a".into(),
                        accepted_risk: None,
                        decided_at: stale_at,
                    },
                    None,
                    stale_at,
                ),
                Err(EscalationLifecycleError::StaleDecision { .. })
            ));
            assert!(
                store
                    .escalation(&escalation.id)
                    .unwrap()
                    .unwrap()
                    .decision
                    .is_none()
            );
            service
                .capture_staleness_snapshot(
                    "goal-a",
                    &escalation.id,
                    "system",
                    now + chrono::Duration::seconds(6),
                )
                .unwrap();
            let decided_at = now + chrono::Duration::seconds(7);
            service
                .apply_user_decision(
                    "goal-a",
                    &escalation.id,
                    EscalationDecision {
                        option_id: "reconcile-now".into(),
                        decided_by: "user-a".into(),
                        accepted_risk: None,
                        decided_at,
                    },
                    None,
                    decided_at,
                )
                .unwrap();
            let assembler = WorkerContextAssembler::new(
                tracker,
                store.clone(),
                super::super::protocol::WorkerContextLimits::default(),
            );

            let report = service
                .resume_affected_work(
                    &assembler,
                    "goal-a",
                    &escalation.id,
                    chrono::Duration::minutes(5),
                    now + chrono::Duration::seconds(8),
                )
                .unwrap();

            assert_eq!(report.resumed.len(), 1);
            let resumed = &report.resumed[0];
            assert_eq!(resumed.blocked_claim_id, claim.id);
            assert_eq!(resumed.resumed_claim.lease_generation, 2);
            assert_eq!(
                resumed.resumed_claim.parent_claim_id,
                Some(claim.id.clone())
            );
            assert_eq!(resumed.replaced_worker, replace_terminal_worker);
            assert_eq!(
                resumed.context.assignment.claim.id,
                resumed.resumed_claim.id.as_str()
            );
            assert_eq!(
                store.work_package(&package.id).unwrap().unwrap().state,
                WorkPackageState::Claimed
            );
            assert_eq!(
                store.worker(&resumed.worker.id).unwrap().unwrap().state,
                WorkerState::Active
            );
            let persisted_escalation = store.escalation(&escalation.id).unwrap().unwrap();
            let decision_audit = persisted_escalation.decision_audit.as_ref().unwrap();
            assert_eq!(decision_audit.decided_by, "user-a");
            assert_eq!(decision_audit.selected_option.id, "reconcile-now");
            assert_eq!(decision_audit.alternatives.len(), 1);
            assert!(
                decision_audit
                    .evidence_refs
                    .contains(&"validation:new-conflict-evidence".into())
            );
            assert_eq!(decision_audit.downstream_outcomes.len(), 2);
            let downstream = decision_audit.downstream_outcomes.last().unwrap();
            assert_eq!(
                downstream.kind,
                if replace_terminal_worker {
                    EscalationOutcomeKind::WorkerReplaced
                } else {
                    EscalationOutcomeKind::WorkResumed
                }
            );
            assert_eq!(
                downstream.claim_ids,
                vec![claim.id.clone(), resumed.resumed_claim.id.clone()]
            );
            assert!(downstream.worker_ids.contains(&resumed.worker.id));
            assert_eq!(downstream.work_package_ids, vec![package.id.clone()]);
            let reopened =
                SqliteCoordinationStore::open(directory.path().join("coordination.sqlite"))
                    .unwrap();
            assert_eq!(
                reopened
                    .escalation(&escalation.id)
                    .unwrap()
                    .unwrap()
                    .decision_audit,
                persisted_escalation.decision_audit
            );
            let replay = service
                .resume_affected_work(
                    &assembler,
                    "goal-a",
                    &escalation.id,
                    chrono::Duration::minutes(5),
                    now + chrono::Duration::seconds(9),
                )
                .unwrap();
            assert_eq!(replay.resumed[0].resumed_claim.id, resumed.resumed_claim.id);
            assert_eq!(replay.resumed[0].event_sequence, resumed.event_sequence);
        }
    }
}
