use super::domain::*;
use super::store::{
    ClaimRepository, EscalationRepository, IdempotencyRepository, SqliteCoordinationStore,
    StoreError, WorkPackageRepository, WorkerRepository,
};
use crate::tracker::{Tracker, TrackerError};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct ClaimPolicy {
    pub lease_duration: Duration,
}

impl Default for ClaimPolicy {
    fn default() -> Self {
        Self {
            lease_duration: Duration::minutes(5),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReleaseEvidence {
    pub reason: String,
    pub artifact_id: Option<String>,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CompletionEvidence {
    pub reason: String,
    pub artifact_id: String,
    pub integration_revision: String,
    pub evidence_refs: Vec<String>,
    pub integration_boundary_satisfied: bool,
}

#[derive(Debug, Clone)]
pub struct BlockEvidence {
    pub reason: String,
    pub evidence_refs: Vec<String>,
    pub escalation_id: Option<EscalationId>,
}

#[derive(Debug, Clone)]
pub struct CancelEvidence {
    pub reason: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ManualAssignmentEvidence {
    pub reason: String,
    pub decided_by: String,
}

#[derive(Debug, Error)]
pub enum ClaimServiceError {
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("worker was not found: {0}")]
    WorkerNotFound(String),
    #[error("worker {worker_id} belongs to goal {worker_goal}, not {requested_goal}")]
    WorkerGoalMismatch {
        worker_id: String,
        worker_goal: String,
        requested_goal: String,
    },
    #[error("worker {worker_id} is not eligible to claim work while {state:?}")]
    WorkerUnavailable {
        worker_id: String,
        state: WorkerState,
    },
    #[error("no ready work is available for goal {0}")]
    NoReadyWork(String),
    #[error("requested scope is not ready")]
    ScopeNotReady,
    #[error("claim was not found: {0}")]
    ClaimNotFound(String),
    #[error("worker {worker_id} does not own claim {claim_id}")]
    NotOwner { worker_id: String, claim_id: String },
    #[error("claim changed while the lease operation was in progress")]
    StaleClaim,
    #[error("invalid idempotent claim outcome")]
    InvalidIdempotentOutcome,
    #[error("claim completion requires an integration artifact and validation evidence")]
    CompletionEvidenceRequired,
    #[error("claim completion is not allowed before its integration boundary is satisfied")]
    IntegrationBoundaryNotSatisfied,
}

#[derive(Debug, Clone)]
pub struct ClaimService {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    policy: ClaimPolicy,
}

impl ClaimService {
    pub fn new(tracker: Tracker, store: Arc<SqliteCoordinationStore>, policy: ClaimPolicy) -> Self {
        Self {
            tracker,
            store,
            policy,
        }
    }

    pub fn claim_ready_unit(
        &self,
        goal_id: &str,
        worker_id: &WorkerId,
        requested_scope: Option<ClaimScope>,
        base_revision: &str,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        if let Some(existing) = self.idempotent_claim(idempotency_key)? {
            return Ok(existing);
        }
        let mut worker = self.eligible_worker(goal_id, worker_id)?;
        let scope = self.select_ready_scope(goal_id, requested_scope.as_ref(), Some(&worker))?;
        let mut claim = Claim::new(
            goal_id,
            scope.clone(),
            worker_id.clone(),
            base_revision,
            now,
            now + self.policy.lease_duration,
        )?;
        claim.lease_generation = self.store.latest_claim_generation(goal_id, &scope)? + 1;
        if let ClaimScope::WorkPackage { work_package_id } = &scope {
            if let Some(package) = self.store.work_package(work_package_id)? {
                claim.produces_contracts = package.produces_contracts;
                claim.consumes_contracts = package.consumes_contracts;
            }
        }
        worker.active_claims.push(claim.id.clone());
        worker.metadata.updated_at = now;
        let event = claim_lifecycle_event(&claim, None, "claim acquired", vec![], now)?;
        self.store.acquire_claim_for_worker_with_event(
            &claim,
            &worker,
            &event,
            &format!("claim.acquire:{idempotency_key}"),
            "claim.acquire",
            idempotency_key,
        )?;
        Ok(claim)
    }

    pub fn ready_unclaimed_scopes(
        &self,
        goal_id: &str,
    ) -> Result<Vec<ClaimScope>, ClaimServiceError> {
        let ready = self.tracker.ready_work(goal_id)?;
        let mut scopes = Vec::new();
        for item in ready.as_array().cloned().unwrap_or_default() {
            let Some(scope) = scope_from_ready_item(&item) else {
                continue;
            };
            if self
                .store
                .active_claim_for_scope(goal_id, &scope)?
                .is_none()
                && !self.scope_blocked(goal_id, &scope, None)?
            {
                scopes.push(scope);
            }
        }
        Ok(scopes)
    }

    pub fn expand_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        requested_scope: ClaimScope,
        rationale: &str,
        base_revision: &str,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        if let Some(existing) = self.idempotent_claim(idempotency_key)? {
            return Ok(existing);
        }
        let mut parent = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(claim_id.as_str().into()))?;
        self.ensure_owner(&parent, worker_id)?;
        let mut worker = self.eligible_worker(&parent.goal_id, worker_id)?;
        let scope =
            self.select_ready_scope(&parent.goal_id, Some(&requested_scope), Some(&worker))?;
        let mut expansion = Claim::new(
            &parent.goal_id,
            scope.clone(),
            worker_id.clone(),
            base_revision,
            now,
            now + self.policy.lease_duration,
        )?;
        expansion.lease_generation = self
            .store
            .latest_claim_generation(&parent.goal_id, &scope)?
            + 1;
        if let ClaimScope::WorkPackage { work_package_id } = &scope {
            if let Some(package) = self.store.work_package(work_package_id)? {
                expansion.produces_contracts = package.produces_contracts;
                expansion.consumes_contracts = package.consumes_contracts;
            }
        }
        expansion.parent_claim_id = Some(parent.id.clone());
        parent.expansion_history.push(ClaimExpansion {
            added_scope: scope,
            rationale: rationale.into(),
            at: now,
        });
        parent.metadata.updated_at = now;
        worker.active_claims.push(expansion.id.clone());
        worker.metadata.updated_at = now;
        let event = claim_lifecycle_event(
            &expansion,
            None,
            &format!("claim expanded from {}: {rationale}", parent.id.as_str()),
            vec![],
            now,
        )?;
        if self
            .store
            .expand_claim_for_worker_with_event(
                &parent,
                &expansion,
                &worker,
                parent.lease_generation,
                &event,
                &format!("claim.expand:{idempotency_key}"),
                "claim.expand",
                idempotency_key,
            )?
            .is_none()
        {
            return Err(ClaimServiceError::StaleClaim);
        }
        Ok(expansion)
    }

    pub fn heartbeat(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        let mut claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(claim_id.as_str().into()))?;
        self.ensure_owner(&claim, worker_id)?;
        claim.renew(expected_generation, now, now + self.policy.lease_duration)?;
        if !self
            .store
            .compare_and_swap_active_claim(&claim, expected_generation)?
        {
            return Err(ClaimServiceError::StaleClaim);
        }
        Ok(claim)
    }

    pub fn release_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        evidence: ReleaseEvidence,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        self.finalize_claim(
            claim_id,
            worker_id,
            expected_generation,
            ClaimState::Released,
            ClaimOutcome {
                kind: ClaimOutcomeKind::Released,
                reason: evidence.reason,
                artifact_id: evidence.artifact_id,
                evidence_refs: evidence.evidence_refs,
                escalation_id: None,
                at: now,
            },
            "claim.release",
            idempotency_key,
            None,
            now,
        )
    }

    pub fn complete_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        evidence: CompletionEvidence,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        if !evidence.integration_boundary_satisfied {
            return Err(ClaimServiceError::IntegrationBoundaryNotSatisfied);
        }
        if evidence.artifact_id.trim().is_empty()
            || evidence.integration_revision.trim().is_empty()
            || evidence.evidence_refs.is_empty()
        {
            return Err(ClaimServiceError::CompletionEvidenceRequired);
        }
        let integration_revision = evidence.integration_revision.clone();
        self.finalize_claim(
            claim_id,
            worker_id,
            expected_generation,
            ClaimState::Completed,
            ClaimOutcome {
                kind: ClaimOutcomeKind::Completed,
                reason: evidence.reason,
                artifact_id: Some(evidence.artifact_id),
                evidence_refs: evidence.evidence_refs,
                escalation_id: None,
                at: now,
            },
            "claim.complete",
            idempotency_key,
            Some(integration_revision),
            now,
        )
    }

    pub fn block_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        evidence: BlockEvidence,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        self.finalize_claim(
            claim_id,
            worker_id,
            expected_generation,
            ClaimState::Blocked,
            ClaimOutcome {
                kind: ClaimOutcomeKind::Blocked,
                reason: evidence.reason,
                artifact_id: None,
                evidence_refs: evidence.evidence_refs,
                escalation_id: evidence.escalation_id,
                at: now,
            },
            "claim.block",
            idempotency_key,
            None,
            now,
        )
    }

    pub fn cancel_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        evidence: CancelEvidence,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        self.finalize_claim(
            claim_id,
            worker_id,
            expected_generation,
            ClaimState::Revoked,
            ClaimOutcome {
                kind: ClaimOutcomeKind::Revoked,
                reason: evidence.reason,
                artifact_id: None,
                evidence_refs: evidence.evidence_refs,
                escalation_id: None,
                at: now,
            },
            "claim.cancel",
            idempotency_key,
            None,
            now,
        )
    }

    pub fn expire_stale_claims(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
        grace_period: Duration,
    ) -> Result<Vec<Claim>, ClaimServiceError> {
        let active = self
            .store
            .claims_for_goal(goal_id, Some(ClaimState::Active))?;
        let mut expired = Vec::new();
        for mut claim in active
            .into_iter()
            .filter(|claim| claim.lease_expires_at + grace_period <= now)
        {
            let generation = claim.lease_generation;
            let mut worker = self.worker_for_goal(goal_id, &claim.owner)?;
            claim.transition(
                ClaimState::Expired,
                ClaimOutcome {
                    kind: ClaimOutcomeKind::Expired,
                    reason: "lease expired after grace period".into(),
                    artifact_id: None,
                    evidence_refs: vec![format!(
                        "lease_expires_at:{}",
                        claim.lease_expires_at.to_rfc3339()
                    )],
                    escalation_id: None,
                    at: now,
                },
            )?;
            worker.active_claims.retain(|id| id != &claim.id);
            if worker.current_run_id.take().is_some() {
                if let Some(active_turn) = worker
                    .turn_history
                    .iter_mut()
                    .rev()
                    .find(|turn| turn.completed_at.is_none())
                {
                    active_turn.completed_at = Some(now);
                }
            }
            if matches!(
                worker.state,
                WorkerState::Starting
                    | WorkerState::Active
                    | WorkerState::Waiting
                    | WorkerState::Paused
                    | WorkerState::Blocked
            ) {
                worker.transition(
                    WorkerState::Recovering,
                    now,
                    Some("claim lease expired; workspace quarantined".into()),
                )?;
            }
            let publication_key = format!("claim.expire:{}:{generation}", claim.id.as_str());
            let event = claim_lifecycle_event(
                &claim,
                Some(ClaimState::Active),
                "claim lease expired after grace period",
                claim
                    .outcome
                    .as_ref()
                    .map(|outcome| outcome.evidence_refs.clone())
                    .unwrap_or_default(),
                now,
            )?;
            if self
                .store
                .finalize_claim_for_worker_with_event(
                    &claim,
                    &worker,
                    generation,
                    &event,
                    &publication_key,
                    "claim.expire",
                    &publication_key,
                )?
                .is_none()
            {
                continue;
            }
            expired.push(claim);
        }
        Ok(expired)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn expire_and_reclaim(
        &self,
        goal_id: &str,
        worker_id: &WorkerId,
        scope: ClaimScope,
        base_revision: &str,
        idempotency_key: &str,
        now: DateTime<Utc>,
        grace_period: Duration,
    ) -> Result<Claim, ClaimServiceError> {
        self.expire_stale_claims(goal_id, now, grace_period)?;
        self.claim_ready_unit(
            goal_id,
            worker_id,
            Some(scope),
            base_revision,
            idempotency_key,
            now,
        )
    }

    pub fn record_managed_overlap(
        &self,
        left_id: &ClaimId,
        right_id: &ClaimId,
        kind: OverlapKind,
        paths: Vec<String>,
        rationale: &str,
        now: DateTime<Utc>,
    ) -> Result<(Claim, Claim), ClaimServiceError> {
        let mut left = self
            .store
            .claim(left_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(left_id.as_str().into()))?;
        let mut right = self
            .store
            .claim(right_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(right_id.as_str().into()))?;
        if left.goal_id != right.goal_id
            || left.state != ClaimState::Active
            || right.state != ClaimState::Active
        {
            return Err(ClaimServiceError::StaleClaim);
        }
        let left_generation = left.lease_generation;
        let right_generation = right.lease_generation;
        left.overlaps
            .retain(|overlap| overlap.other_claim_id != right.id);
        right
            .overlaps
            .retain(|overlap| overlap.other_claim_id != left.id);
        left.overlaps.push(ManagedOverlap {
            other_claim_id: right.id.clone(),
            kind,
            paths: paths.clone(),
            rationale: rationale.into(),
            recorded_at: now,
        });
        right.overlaps.push(ManagedOverlap {
            other_claim_id: left.id.clone(),
            kind,
            paths,
            rationale: rationale.into(),
            recorded_at: now,
        });
        left.metadata.updated_at = now;
        right.metadata.updated_at = now;
        self.store
            .record_claim_overlap(&left, &right, left_generation, right_generation)?;
        Ok((left, right))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn manually_reassign_claim(
        &self,
        claim_id: &ClaimId,
        replacement_worker_id: &WorkerId,
        expected_generation: u64,
        base_revision: &str,
        evidence: ManualAssignmentEvidence,
        idempotency_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        if let Some(existing) = self.idempotent_claim(idempotency_key)? {
            return Ok(existing);
        }
        let mut prior = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(claim_id.as_str().into()))?;
        if prior.lease_generation != expected_generation || prior.state != ClaimState::Active {
            return Err(ClaimServiceError::StaleClaim);
        }
        let mut prior_worker = self.worker_for_goal(&prior.goal_id, &prior.owner)?;
        let mut replacement_worker = self.eligible_worker(&prior.goal_id, replacement_worker_id)?;
        prior.transition(
            ClaimState::Revoked,
            ClaimOutcome {
                kind: ClaimOutcomeKind::Revoked,
                reason: format!("{} (decided by {})", evidence.reason, evidence.decided_by),
                artifact_id: None,
                evidence_refs: vec![format!("manual-decision:{}", evidence.decided_by)],
                escalation_id: None,
                at: now,
            },
        )?;
        let mut replacement = Claim::new(
            &prior.goal_id,
            prior.scope.clone(),
            replacement_worker_id.clone(),
            base_revision,
            now,
            now + self.policy.lease_duration,
        )?;
        replacement.lease_generation = expected_generation + 1;
        replacement.parent_claim_id = prior.parent_claim_id.clone();
        prior_worker.active_claims.retain(|id| id != claim_id);
        prior_worker.metadata.updated_at = now;
        replacement_worker
            .active_claims
            .push(replacement.id.clone());
        replacement_worker.metadata.updated_at = now;
        if !self.store.reassign_claim(
            &prior,
            &replacement,
            &prior_worker,
            &replacement_worker,
            expected_generation,
        )? {
            return Err(ClaimServiceError::StaleClaim);
        }
        self.store.record_idempotent_outcome(
            idempotency_key,
            "claim.reassign",
            &serde_json::json!({"claimId": replacement.id.as_str()}),
            now,
        )?;
        Ok(replacement)
    }

    #[allow(clippy::too_many_arguments)]
    fn finalize_claim(
        &self,
        claim_id: &ClaimId,
        worker_id: &WorkerId,
        expected_generation: u64,
        state: ClaimState,
        outcome: ClaimOutcome,
        operation: &str,
        idempotency_key: &str,
        integration_revision: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<Claim, ClaimServiceError> {
        if let Some(existing) = self.idempotent_claim(idempotency_key)? {
            return Ok(existing);
        }
        let mut claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| ClaimServiceError::ClaimNotFound(claim_id.as_str().into()))?;
        self.ensure_owner(&claim, worker_id)?;
        if claim.lease_generation != expected_generation {
            return Err(ClaimServiceError::StaleClaim);
        }
        let mut worker = self.worker_for_goal(&claim.goal_id, worker_id)?;
        let reason = outcome.reason.clone();
        let artifact_id = outcome.artifact_id.clone();
        let evidence_refs = outcome.evidence_refs.clone();
        claim.transition(state, outcome)?;
        worker.active_claims.retain(|id| id != claim_id);
        worker.metadata.updated_at = now;
        let event_payload = match state {
            ClaimState::Blocked => CoordinationEventPayload::Blocker(BlockerEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker_id.clone(),
                summary: reason,
                evidence_refs,
                escalation_id: claim
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.escalation_id.clone()),
            }),
            ClaimState::Completed => CoordinationEventPayload::Completion(CompletionEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker_id.clone(),
                artifact_id: artifact_id.clone().unwrap_or_default(),
                evidence_refs,
                integration_revision: integration_revision.unwrap_or_default(),
            }),
            _ => CoordinationEventPayload::Claim(ClaimEventPayload {
                claim_id: claim.id.clone(),
                worker_id: worker_id.clone(),
                from_state: Some(ClaimState::Active),
                to_state: state,
                reason,
                artifact_id,
                evidence_refs,
            }),
        };
        let event = CoordinationEvent::from_typed_payload(
            &claim.goal_id,
            if state == ClaimState::Blocked {
                EventSeverity::Warning
            } else {
                EventSeverity::Info
            },
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            claim.id.as_str(),
            event_payload,
            now,
        )?;
        if self
            .store
            .finalize_claim_for_worker_with_event(
                &claim,
                &worker,
                expected_generation,
                &event,
                &format!("{operation}:{idempotency_key}"),
                operation,
                idempotency_key,
            )?
            .is_none()
        {
            return Err(ClaimServiceError::StaleClaim);
        }
        Ok(claim)
    }

    fn eligible_worker(
        &self,
        goal_id: &str,
        worker_id: &WorkerId,
    ) -> Result<Worker, ClaimServiceError> {
        let worker = self.worker_for_goal(goal_id, worker_id)?;
        if !matches!(
            worker.state,
            WorkerState::Starting | WorkerState::Active | WorkerState::Waiting
        ) {
            return Err(ClaimServiceError::WorkerUnavailable {
                worker_id: worker_id.as_str().into(),
                state: worker.state,
            });
        }
        Ok(worker)
    }

    fn worker_for_goal(
        &self,
        goal_id: &str,
        worker_id: &WorkerId,
    ) -> Result<Worker, ClaimServiceError> {
        let worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| ClaimServiceError::WorkerNotFound(worker_id.as_str().into()))?;
        if worker.goal_id != goal_id {
            return Err(ClaimServiceError::WorkerGoalMismatch {
                worker_id: worker_id.as_str().into(),
                worker_goal: worker.goal_id,
                requested_goal: goal_id.into(),
            });
        }
        Ok(worker)
    }

    fn select_ready_scope(
        &self,
        goal_id: &str,
        requested: Option<&ClaimScope>,
        worker: Option<&Worker>,
    ) -> Result<ClaimScope, ClaimServiceError> {
        let ready = self.tracker.ready_work(goal_id)?;
        let items = ready.as_array().cloned().unwrap_or_default();
        if let Some(requested) = requested {
            if items.iter().any(|item| scope_matches(item, requested))
                && !self.scope_blocked(goal_id, requested, worker)?
            {
                return Ok(requested.clone());
            }
            return Err(ClaimServiceError::ScopeNotReady);
        }
        for item in &items {
            if let Some(scope) = scope_from_ready_item(item)
                && !self.scope_blocked(goal_id, &scope, worker)?
            {
                return Ok(scope);
            }
        }
        Err(ClaimServiceError::NoReadyWork(goal_id.into()))
    }

    fn scope_blocked(
        &self,
        goal_id: &str,
        scope: &ClaimScope,
        worker: Option<&Worker>,
    ) -> Result<bool, ClaimServiceError> {
        for escalation in self.store.escalations_for_goal(goal_id, None)? {
            if escalation.kind != EscalationKind::HardBlocker
                || !matches!(
                    escalation.state,
                    EscalationState::Open | EscalationState::Acknowledged
                )
            {
                continue;
            }
            match (&escalation.scope, scope) {
                (EscalationScope::Goal { goal_id: blocked }, _) if blocked == goal_id => {
                    return Ok(true);
                }
                (EscalationScope::IntegrationLane { repository_id }, _)
                    if worker
                        .and_then(|worker| worker.workspace.as_ref())
                        .is_some_and(|workspace| workspace.repository_id == *repository_id) =>
                {
                    return Ok(true);
                }
                (
                    EscalationScope::WorkPackage {
                        work_package_id: blocked,
                    },
                    ClaimScope::WorkPackage { work_package_id },
                ) if blocked == work_package_id => return Ok(true),
                (
                    EscalationScope::Contract { contract_id },
                    ClaimScope::WorkPackage { work_package_id },
                ) => {
                    if let Some(package) = self.store.work_package(work_package_id)?
                        && (package
                            .produces_contracts
                            .iter()
                            .any(|value| value == contract_id.as_str())
                            || package
                                .consumes_contracts
                                .iter()
                                .any(|expectation| expectation.contract_id == contract_id.as_str()))
                    {
                        return Ok(true);
                    }
                }
                _ => {}
            }
        }
        Ok(false)
    }

    fn ensure_owner(&self, claim: &Claim, worker_id: &WorkerId) -> Result<(), ClaimServiceError> {
        if claim.owner != *worker_id {
            return Err(ClaimServiceError::NotOwner {
                worker_id: worker_id.as_str().into(),
                claim_id: claim.id.as_str().into(),
            });
        }
        Ok(())
    }

    fn idempotent_claim(&self, key: &str) -> Result<Option<Claim>, ClaimServiceError> {
        let Some(outcome) = self.store.idempotent_outcome(key)? else {
            return Ok(None);
        };
        let id = outcome
            .get("claimId")
            .and_then(Value::as_str)
            .ok_or(ClaimServiceError::InvalidIdempotentOutcome)?;
        let id = ClaimId::parse(id)?;
        self.store
            .claim(&id)?
            .map(Some)
            .ok_or(ClaimServiceError::InvalidIdempotentOutcome)
    }
}

fn claim_lifecycle_event(
    claim: &Claim,
    from_state: Option<ClaimState>,
    reason: &str,
    evidence_refs: Vec<String>,
    now: DateTime<Utc>,
) -> Result<CoordinationEvent, DomainError> {
    CoordinationEvent::from_typed_payload(
        &claim.goal_id,
        EventSeverity::Info,
        CoordinationActor::Worker {
            worker_id: claim.owner.clone(),
        },
        claim.id.as_str(),
        CoordinationEventPayload::Claim(ClaimEventPayload {
            claim_id: claim.id.clone(),
            worker_id: claim.owner.clone(),
            from_state,
            to_state: claim.state,
            reason: reason.into(),
            artifact_id: claim
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.artifact_id.clone()),
            evidence_refs,
        }),
        now,
    )
}

fn scope_matches(item: &Value, scope: &ClaimScope) -> bool {
    let kind = item.get("kind").and_then(Value::as_str);
    let id = item.get("id").and_then(Value::as_str);
    match scope {
        ClaimScope::Feature { feature_id } => kind == Some("feature") && id == Some(feature_id),
        ClaimScope::WorkPackage { work_package_id } => {
            kind == Some("work_package") && id == Some(work_package_id.as_str())
        }
    }
}

fn scope_from_ready_item(item: &Value) -> Option<ClaimScope> {
    let kind = item.get("kind")?.as_str()?;
    let id = item.get("id")?.as_str()?;
    match kind {
        "feature" => Some(ClaimScope::Feature {
            feature_id: id.into(),
        }),
        "work_package" => Some(ClaimScope::WorkPackage {
            work_package_id: WorkPackageId::parse(id).ok()?,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::store::EventRepository;
    use crate::models::{IntegrationScope, Status};
    use std::sync::Barrier;
    use std::thread;
    use tempfile::TempDir;

    fn active_worker_for_claim_test(
        goal_id: &str,
        repository_id: &str,
        now: DateTime<Utc>,
    ) -> Worker {
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

    fn fixture() -> (TempDir, ClaimService, Arc<SqliteCoordinationStore>, Worker) {
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
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.workspace = Some(WorkspaceBinding {
            repository_id: "repo-a".into(),
            canonical_repository_path: "/repo".into(),
            worktree_path: "/repo/worktrees/worker-a".into(),
            branch: "codex/worker-a".into(),
            base_revision: "base-a".into(),
            created_at: Some(now),
            creation_evidence: vec!["test fixture".into()],
        });
        store.upsert_worker(&worker).unwrap();
        let service = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        (directory, service, store, worker)
    }

    #[test]
    fn claims_first_ready_unit_and_replays_idempotent_result() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let claim = service
            .claim_ready_unit("goal-a", &worker.id, None, "base-a", "request-1", now)
            .unwrap();
        let replay = service
            .claim_ready_unit("goal-a", &worker.id, None, "base-a", "request-1", now)
            .unwrap();

        assert_eq!(replay.id, claim.id);
        assert!(matches!(
            claim.scope,
            ClaimScope::Feature { ref feature_id } if feature_id == "first"
        ));
        assert!(
            store
                .worker(&worker.id)
                .unwrap()
                .unwrap()
                .active_claims
                .contains(&claim.id)
        );
        let events = store
            .events_for_goal("goal-a", 0, Some(claim.id.as_str()), 100)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0].typed_payload().unwrap(),
            Some(CoordinationEventPayload::Claim(ClaimEventPayload {
                from_state: None,
                to_state: ClaimState::Active,
                ..
            }))
        ));
    }

    #[test]
    fn hard_blockers_gate_only_the_matching_repository_or_contract_scope() {
        let (directory, service, store, worker) = fixture();
        let now = Utc::now();
        let lane_blocker = HumanEscalation::new(
            "goal-a",
            EscalationKind::HardBlocker,
            EscalationSeverity::Critical,
            EscalationScope::IntegrationLane {
                repository_id: "repo-a".into(),
            },
            "Repository lane is unsafe",
            now,
        );
        store.upsert_escalation(&lane_blocker).unwrap();
        assert!(matches!(
            service.claim_ready_unit("goal-a", &worker.id, None, "base-a", "blocked-repo-a", now,),
            Err(ClaimServiceError::NoReadyWork(_))
        ));

        let other_worker = active_worker_for_claim_test("goal-a", "repo-b", now);
        store.upsert_worker(&other_worker).unwrap();
        let unrelated = service
            .claim_ready_unit(
                "goal-a",
                &other_worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-b",
                "allowed-repo-b",
                now,
            )
            .unwrap();
        assert_eq!(unrelated.state, ClaimState::Active);

        let tracker = Tracker::new(directory.path().join("package-goals"));
        tracker
            .create_goal("package-goal", "Package Goal", "")
            .unwrap();
        tracker
            .add_feature(
                "package-goal",
                "contract-work",
                "Contract work",
                "",
                Status::Planned,
            )
            .unwrap();
        let contract_id = ContractId::new();
        let mut package = WorkPackage::new(
            "package-goal",
            "Contract package",
            vec!["contract-work".into()],
            now,
        )
        .unwrap();
        package.transition(WorkPackageState::Planned, now).unwrap();
        package.transition(WorkPackageState::Ready, now).unwrap();
        package.produces_contracts = vec![contract_id.as_str().into()];
        tracker
            .add_work_package(
                "package-goal",
                package.id.as_str(),
                "Contract package",
                "",
                vec!["contract-work".into()],
                vec![],
                10,
                IntegrationScope::WorkPackage,
            )
            .unwrap();
        store.upsert_work_package(&package).unwrap();
        let package_worker = active_worker_for_claim_test("package-goal", "repo-b", now);
        store.upsert_worker(&package_worker).unwrap();
        store
            .upsert_escalation(&HumanEscalation::new(
                "package-goal",
                EscalationKind::HardBlocker,
                EscalationSeverity::High,
                EscalationScope::Contract { contract_id },
                "Contract must be reconciled",
                now,
            ))
            .unwrap();
        let package_service = ClaimService::new(tracker, store.clone(), ClaimPolicy::default());
        assert!(matches!(
            package_service.claim_ready_unit(
                "package-goal",
                &package_worker.id,
                Some(ClaimScope::WorkPackage {
                    work_package_id: package.id,
                }),
                "base-b",
                "blocked-contract",
                now,
            ),
            Err(ClaimServiceError::ScopeNotReady)
        ));
    }

    #[test]
    fn expands_into_another_ready_scope_atomically() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let parent = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-a",
                "request-1",
                now,
            )
            .unwrap();
        let expansion = service
            .expand_claim(
                &parent.id,
                &worker.id,
                ClaimScope::Feature {
                    feature_id: "second".into(),
                },
                "same subsystem",
                "base-a",
                "request-2",
                now,
            )
            .unwrap();

        assert_eq!(expansion.parent_claim_id, Some(parent.id.clone()));
        assert_eq!(
            store
                .claim(&parent.id)
                .unwrap()
                .unwrap()
                .expansion_history
                .len(),
            1
        );
        assert_eq!(
            store
                .worker(&worker.id)
                .unwrap()
                .unwrap()
                .active_claims
                .len(),
            2
        );
    }

    #[test]
    fn heartbeat_extends_lease_only_for_current_owner_and_generation() {
        let (_directory, service, _store, worker) = fixture();
        let now = Utc::now();
        let claim = service
            .claim_ready_unit("goal-a", &worker.id, None, "base-a", "request-1", now)
            .unwrap();
        let renewed = service
            .heartbeat(
                &claim.id,
                &worker.id,
                claim.lease_generation,
                now + Duration::minutes(1),
            )
            .unwrap();

        assert!(renewed.lease_expires_at > claim.lease_expires_at);
        assert!(
            service
                .heartbeat(&claim.id, &worker.id, 99, now + Duration::minutes(2))
                .is_err()
        );
    }

    #[test]
    fn terminal_paths_require_evidence_and_remove_active_worker_binding() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let claim = service
            .claim_ready_unit("goal-a", &worker.id, None, "base-a", "request-1", now)
            .unwrap();
        assert!(matches!(
            service.complete_claim(
                &claim.id,
                &worker.id,
                claim.lease_generation,
                CompletionEvidence {
                    reason: "not integrated".into(),
                    artifact_id: "artifact-1".into(),
                    integration_revision: "commit-1".into(),
                    evidence_refs: vec!["test:passed".into()],
                    integration_boundary_satisfied: false,
                },
                "request-2",
                now,
            ),
            Err(ClaimServiceError::IntegrationBoundaryNotSatisfied)
        ));
        let completed = service
            .complete_claim(
                &claim.id,
                &worker.id,
                claim.lease_generation,
                CompletionEvidence {
                    reason: "validated and integrated".into(),
                    artifact_id: "artifact-1".into(),
                    integration_revision: "commit-1".into(),
                    evidence_refs: vec!["test:passed".into()],
                    integration_boundary_satisfied: true,
                },
                "request-3",
                now,
            )
            .unwrap();

        assert_eq!(completed.state, ClaimState::Completed);
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
    fn release_and_block_preserve_partial_and_escalation_evidence() {
        let (_directory, service, _store, worker) = fixture();
        let now = Utc::now();
        let released_source = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-a",
                "request-1",
                now,
            )
            .unwrap();
        let released = service
            .release_claim(
                &released_source.id,
                &worker.id,
                released_source.lease_generation,
                ReleaseEvidence {
                    reason: "pausing after partial implementation".into(),
                    artifact_id: Some("artifact-partial".into()),
                    evidence_refs: vec!["diff:partial".into()],
                },
                "request-2",
                now,
            )
            .unwrap();
        let blocked_source = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "second".into(),
                }),
                "base-a",
                "request-3",
                now,
            )
            .unwrap();
        let escalation_id = EscalationId::new();
        let blocked = service
            .block_claim(
                &blocked_source.id,
                &worker.id,
                blocked_source.lease_generation,
                BlockEvidence {
                    reason: "needs a user decision".into(),
                    evidence_refs: vec!["contract:ambiguous".into()],
                    escalation_id: Some(escalation_id.clone()),
                },
                "request-4",
                now,
            )
            .unwrap();

        assert_eq!(released.state, ClaimState::Released);
        assert_eq!(blocked.outcome.unwrap().escalation_id, Some(escalation_id));
    }

    #[test]
    fn expired_claim_is_quarantined_and_reclaimed_with_a_new_generation() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let original = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-a",
                "request-1",
                now,
            )
            .unwrap();
        let mut replacement_worker = Worker::new("goal-a", now);
        replacement_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        replacement_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        store.upsert_worker(&replacement_worker).unwrap();

        let replacement = service
            .expire_and_reclaim(
                "goal-a",
                &replacement_worker.id,
                original.scope.clone(),
                "base-b",
                "request-2",
                now + Duration::minutes(7),
                Duration::minutes(1),
            )
            .unwrap();

        let expired = store.claim(&original.id).unwrap().unwrap();
        let quarantined_worker = store.worker(&worker.id).unwrap().unwrap();
        assert_eq!(expired.state, ClaimState::Expired);
        assert_eq!(replacement.lease_generation, original.lease_generation + 1);
        assert_eq!(quarantined_worker.state, WorkerState::Recovering);
        assert!(quarantined_worker.workspace.is_some());
        assert!(!quarantined_worker.active_claims.contains(&original.id));
    }

    #[test]
    fn managed_overlap_is_symmetric_and_auditable() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let left = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-a",
                "request-1",
                now,
            )
            .unwrap();
        let right = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "second".into(),
                }),
                "base-a",
                "request-2",
                now,
            )
            .unwrap();

        let (left, right) = service
            .record_managed_overlap(
                &left.id,
                &right.id,
                OverlapKind::ReconciliationRequired,
                vec!["rust-backend/coordination/mod.rs".into()],
                "both claims extend the coordination boundary",
                now + Duration::seconds(1),
            )
            .unwrap();

        assert_eq!(left.overlaps[0].other_claim_id, right.id);
        assert_eq!(right.overlaps[0].other_claim_id, left.id);
        assert_eq!(
            store.claim(&left.id).unwrap().unwrap().overlaps[0].kind,
            OverlapKind::ReconciliationRequired
        );
    }

    #[test]
    fn manual_assignment_revokes_prior_ownership_atomically() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let original = service
            .claim_ready_unit("goal-a", &worker.id, None, "base-a", "request-1", now)
            .unwrap();
        let mut replacement_worker = Worker::new("goal-a", now);
        replacement_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        replacement_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        store.upsert_worker(&replacement_worker).unwrap();

        let replacement = service
            .manually_reassign_claim(
                &original.id,
                &replacement_worker.id,
                original.lease_generation,
                "base-b",
                ManualAssignmentEvidence {
                    reason: "user selected a different worker".into(),
                    decided_by: "user@example.test".into(),
                },
                "request-2",
                now + Duration::seconds(1),
            )
            .unwrap();

        let revoked = store.claim(&original.id).unwrap().unwrap();
        let old_worker = store.worker(&worker.id).unwrap().unwrap();
        let new_worker = store.worker(&replacement_worker.id).unwrap().unwrap();
        assert_eq!(revoked.state, ClaimState::Revoked);
        assert_eq!(replacement.owner, replacement_worker.id);
        assert_eq!(replacement.lease_generation, original.lease_generation + 1);
        assert!(!old_worker.active_claims.contains(&original.id));
        assert!(new_worker.active_claims.contains(&replacement.id));
    }

    #[test]
    fn concurrent_claim_requests_leave_exactly_one_active_owner() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let mut contender = Worker::new("goal-a", now);
        contender
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        contender
            .transition(WorkerState::Active, now, None)
            .unwrap();
        store.upsert_worker(&contender).unwrap();

        let service = Arc::new(service);
        let barrier = Arc::new(Barrier::new(2));
        let scope = ClaimScope::Feature {
            feature_id: "first".into(),
        };
        let attempts = [(worker.id, "race-1"), (contender.id, "race-2")]
            .into_iter()
            .map(|(worker_id, request_id)| {
                let service = Arc::clone(&service);
                let barrier = Arc::clone(&barrier);
                let scope = scope.clone();
                thread::spawn(move || {
                    barrier.wait();
                    service.claim_ready_unit(
                        "goal-a",
                        &worker_id,
                        Some(scope),
                        "base-a",
                        request_id,
                        now,
                    )
                })
            })
            .collect::<Vec<_>>();
        let results = attempts
            .into_iter()
            .map(|attempt| attempt.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            store
                .claims_for_goal("goal-a", Some(ClaimState::Active))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn concurrent_expansion_requests_create_only_one_adjacent_claim() {
        let (_directory, service, store, worker) = fixture();
        let now = Utc::now();
        let parent = service
            .claim_ready_unit(
                "goal-a",
                &worker.id,
                Some(ClaimScope::Feature {
                    feature_id: "first".into(),
                }),
                "base-a",
                "request-1",
                now,
            )
            .unwrap();
        let service = Arc::new(service);
        let barrier = Arc::new(Barrier::new(2));
        let attempts = ["expand-race-1", "expand-race-2"]
            .into_iter()
            .map(|request_id| {
                let service = Arc::clone(&service);
                let barrier = Arc::clone(&barrier);
                let parent_id = parent.id.clone();
                let worker_id = worker.id.clone();
                thread::spawn(move || {
                    barrier.wait();
                    service.expand_claim(
                        &parent_id,
                        &worker_id,
                        ClaimScope::Feature {
                            feature_id: "second".into(),
                        },
                        "concurrent adjacent work",
                        "base-a",
                        request_id,
                        now,
                    )
                })
            })
            .collect::<Vec<_>>();
        let results = attempts
            .into_iter()
            .map(|attempt| attempt.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let active = store
            .claims_for_goal("goal-a", Some(ClaimState::Active))
            .unwrap();
        assert_eq!(active.len(), 2);
        assert_eq!(
            active
                .iter()
                .filter(|claim| matches!(
                    claim.scope,
                    ClaimScope::Feature { ref feature_id } if feature_id == "second"
                ))
                .count(),
            1
        );
    }
}
