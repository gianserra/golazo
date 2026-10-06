use super::benchmark::RolloutBenchmarkReport;
use super::claims::{ClaimPolicy, ClaimService, ClaimServiceError};
use super::domain::{
    Claim, ClaimScope, CoordinationEventKind, CoordinationSignal, EscalationSeverity,
    EscalationState, EventSeverity, IntegrationJobState, InterventionLevel, SignalKind,
    SignalState, WorkerPermissionPolicy, WorkerState,
};
use super::pool::{PoolSnapshot, WorkerPoolError, WorkerPoolPolicy, WorkerPoolService};
use super::store::{
    ClaimRepository, EscalationRepository, EventRepository, IntegrationJobRepository,
    RolloutRepository, SignalRepository, SqliteCoordinationStore, StoreError, WorkerRepository,
};
use super::supervisor::{
    SupervisorTrigger, SupervisorTriggerError, SupervisorTriggerKind, SupervisorTriggerPolicy,
    SupervisorTriggerService,
};
use super::workspace::{WorkspaceError, WorkspaceIsolationCapability};
use crate::tracker::Tracker;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub const ROLLOUT_CONTROL_SCHEMA: u16 = 1;
pub const SHADOW_OBSERVATION_SCHEMA: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RolloutPhase {
    Disabled,
    Shadow,
    ConstrainedPilot,
    Expanded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RolloutControl {
    pub schema_version: u16,
    pub goal_id: String,
    pub phase: RolloutPhase,
    pub desired_concurrency: usize,
    pub autonomous_workers_enabled: bool,
    pub automatic_integration_enabled: bool,
    pub conservative_permissions_required: bool,
    pub manual_integration_required: bool,
    pub selected_repository_path: Option<String>,
    pub rollback_policy: Option<PilotRollbackPolicy>,
    pub expansion_evidence: Option<ExpansionGateEvidence>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PilotRollbackPolicy {
    pub on_worker_failure: bool,
    pub on_semantic_conflict: bool,
    pub on_permission_violation: bool,
    pub on_integration_failure: bool,
}

impl Default for PilotRollbackPolicy {
    fn default() -> Self {
        Self {
            on_worker_failure: true,
            on_semantic_conflict: true,
            on_permission_violation: true,
            on_integration_failure: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpansionThresholds {
    pub minimum_completed_units_per_cohort: u64,
    pub minimum_throughput_change_percent: f64,
    pub maximum_tokens_per_unit_change_percent: f64,
    pub maximum_conflicts_per_completed_unit: f64,
    pub maximum_average_integration_latency_milliseconds: f64,
    pub maximum_human_interruptions_per_completed_unit: f64,
}

impl Default for ExpansionThresholds {
    fn default() -> Self {
        Self {
            minimum_completed_units_per_cohort: 4,
            minimum_throughput_change_percent: 0.0,
            maximum_tokens_per_unit_change_percent: 10.0,
            maximum_conflicts_per_completed_unit: 0.25,
            maximum_average_integration_latency_milliseconds: 300_000.0,
            maximum_human_interruptions_per_completed_unit: 0.5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExpansionGateEvidence {
    pub approved_by: String,
    pub evaluated_at: DateTime<Utc>,
    pub safety_gate_passed: bool,
    pub quality_gate_passed: bool,
    pub recovery_gate_passed: bool,
    pub thresholds: ExpansionThresholds,
    pub benchmark: RolloutBenchmarkReport,
}

impl RolloutControl {
    pub fn shadow(
        goal_id: impl Into<String>,
        proposed_concurrency: usize,
        now: DateTime<Utc>,
    ) -> Result<Self, RolloutError> {
        if proposed_concurrency == 0 || proposed_concurrency > 32 {
            return Err(RolloutError::InvalidControl(
                "shadow proposed concurrency must be between 1 and 32".into(),
            ));
        }
        Ok(Self {
            schema_version: ROLLOUT_CONTROL_SCHEMA,
            goal_id: goal_id.into(),
            phase: RolloutPhase::Shadow,
            desired_concurrency: proposed_concurrency,
            autonomous_workers_enabled: false,
            automatic_integration_enabled: false,
            conservative_permissions_required: true,
            manual_integration_required: true,
            selected_repository_path: None,
            rollback_policy: None,
            expansion_evidence: None,
            updated_at: now,
        })
    }

    fn constrained_pilot(
        goal_id: impl Into<String>,
        repository_path: String,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: ROLLOUT_CONTROL_SCHEMA,
            goal_id: goal_id.into(),
            phase: RolloutPhase::ConstrainedPilot,
            desired_concurrency: 2,
            autonomous_workers_enabled: true,
            automatic_integration_enabled: false,
            conservative_permissions_required: true,
            manual_integration_required: true,
            selected_repository_path: Some(repository_path),
            rollback_policy: Some(PilotRollbackPolicy::default()),
            expansion_evidence: None,
            updated_at: now,
        }
    }

    fn expanded(
        pilot: &Self,
        desired_concurrency: usize,
        evidence: ExpansionGateEvidence,
        now: DateTime<Utc>,
    ) -> Result<Self, RolloutError> {
        if !(3..=4).contains(&desired_concurrency) {
            return Err(RolloutError::InvalidControl(
                "expanded concurrency must be between 3 and 4".into(),
            ));
        }
        Ok(Self {
            schema_version: ROLLOUT_CONTROL_SCHEMA,
            goal_id: pilot.goal_id.clone(),
            phase: RolloutPhase::Expanded,
            desired_concurrency,
            autonomous_workers_enabled: true,
            automatic_integration_enabled: true,
            conservative_permissions_required: true,
            manual_integration_required: false,
            selected_repository_path: pilot.selected_repository_path.clone(),
            rollback_policy: pilot.rollback_policy.clone(),
            expansion_evidence: Some(evidence),
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowClaimProposal {
    pub slot: usize,
    pub scope: ClaimScope,
    pub base_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowSupervisorDecision {
    pub trigger_key: String,
    pub trigger_kind: SupervisorTriggerKind,
    pub level: InterventionLevel,
    pub confidence_percent: u8,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShadowObservation {
    pub schema_version: u16,
    pub id: String,
    pub goal_id: String,
    pub observed_at: DateTime<Utc>,
    pub after_event_sequence: u64,
    pub last_event_sequence: u64,
    pub ready_scopes: Vec<ClaimScope>,
    pub proposed_claims: Vec<ShadowClaimProposal>,
    pub active_signals: Vec<CoordinationSignal>,
    pub supervisor_decisions: Vec<ShadowSupervisorDecision>,
    pub workers_before: usize,
    pub workers_after: usize,
    pub active_claims_before: usize,
    pub active_claims_after: usize,
}

pub trait ShadowSupervisorEvaluator: Send + Sync {
    fn evaluate(&self, trigger: &SupervisorTrigger) -> Result<ShadowSupervisorDecision, String>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct PilotStartReport {
    pub control: RolloutControl,
    pub pool: PoolSnapshot,
    pub assigned_claims: Vec<Claim>,
    pub shadow_observation_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PilotRollbackReport {
    pub rolled_back: bool,
    pub reasons: Vec<String>,
    pub control: RolloutControl,
    pub pool: PoolSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExpansionReport {
    pub control: RolloutControl,
    pub pool: PoolSnapshot,
    pub newly_assigned_claims: Vec<Claim>,
}

#[derive(Debug, Error)]
pub enum RolloutError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Claims(#[from] ClaimServiceError),
    #[error(transparent)]
    Supervisor(#[from] SupervisorTriggerError),
    #[error(transparent)]
    Pool(#[from] WorkerPoolError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error("invalid rollout control: {0}")]
    InvalidControl(String),
    #[error("goal {0} is not configured for shadow mode")]
    ShadowNotConfigured(String),
    #[error("shadow Supervisor evaluation failed for {trigger_key}: {message}")]
    Evaluation {
        trigger_key: String,
        message: String,
    },
    #[error("shadow mode attempted to mutate autonomous worker or claim state")]
    AutonomousMutationDetected,
    #[error(
        "constrained pilot requires at least one clean shadow observation with two proposed claims"
    )]
    ShadowEvidenceRequired,
    #[error("constrained pilot requires a Git worktree-capable repository")]
    GitRepositoryRequired,
    #[error("goal {0} is not running a constrained pilot")]
    PilotNotConfigured(String),
    #[error("rollout expansion gates failed: {0:?}")]
    ExpansionGateFailed(Vec<String>),
}

pub struct RolloutService {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
}

impl RolloutService {
    pub fn new(tracker: Tracker, store: Arc<SqliteCoordinationStore>) -> Self {
        Self { tracker, store }
    }

    pub fn configure_shadow(
        &self,
        goal_id: &str,
        proposed_concurrency: usize,
        now: DateTime<Utc>,
    ) -> Result<RolloutControl, RolloutError> {
        self.tracker
            .get_goal(goal_id)
            .map_err(ClaimServiceError::from)?;
        let control = RolloutControl::shadow(goal_id, proposed_concurrency, now)?;
        self.store.upsert_rollout_control(&control)?;
        Ok(control)
    }

    pub fn observe_shadow(
        &self,
        goal_id: &str,
        base_revision: &str,
        after_event_sequence: u64,
        evaluator: &dyn ShadowSupervisorEvaluator,
        now: DateTime<Utc>,
    ) -> Result<ShadowObservation, RolloutError> {
        let control = self
            .store
            .rollout_control(goal_id)?
            .filter(|control| {
                control.phase == RolloutPhase::Shadow
                    && !control.autonomous_workers_enabled
                    && !control.automatic_integration_enabled
            })
            .ok_or_else(|| RolloutError::ShadowNotConfigured(goal_id.into()))?;
        if base_revision.trim().is_empty() {
            return Err(RolloutError::InvalidControl(
                "shadow observation requires a base revision".into(),
            ));
        }
        let workers_before = self.store.workers_for_goal(goal_id, None)?.len();
        let active_claims_before = self
            .store
            .claims_for_goal(goal_id, Some(super::domain::ClaimState::Active))?
            .len();
        let ready_scopes = ClaimService::new(
            self.tracker.clone(),
            Arc::clone(&self.store),
            ClaimPolicy::default(),
        )
        .ready_unclaimed_scopes(goal_id)?;
        let proposed_claims = ready_scopes
            .iter()
            .take(control.desired_concurrency)
            .enumerate()
            .map(|(index, scope)| ShadowClaimProposal {
                slot: index + 1,
                scope: scope.clone(),
                base_revision: base_revision.into(),
            })
            .collect::<Vec<_>>();
        let active_signals = self
            .store
            .signals_for_goal(goal_id, Some(SignalState::Active))?;
        let batch = SupervisorTriggerService::new(
            Arc::clone(&self.store),
            SupervisorTriggerPolicy::default(),
        )?
        .eligible_triggers(goal_id, after_event_sequence, now)?;
        let mut supervisor_decisions = Vec::new();
        for trigger in &batch.triggers {
            let decision =
                evaluator
                    .evaluate(trigger)
                    .map_err(|message| RolloutError::Evaluation {
                        trigger_key: trigger.key.clone(),
                        message,
                    })?;
            if decision.trigger_key != trigger.key
                || decision.trigger_kind != trigger.kind
                || decision.confidence_percent > 100
                || decision.summary.trim().is_empty()
            {
                return Err(RolloutError::Evaluation {
                    trigger_key: trigger.key.clone(),
                    message: "evaluator returned an invalid or mismatched decision".into(),
                });
            }
            supervisor_decisions.push(decision);
        }
        let workers_after = self.store.workers_for_goal(goal_id, None)?.len();
        let active_claims_after = self
            .store
            .claims_for_goal(goal_id, Some(super::domain::ClaimState::Active))?
            .len();
        if workers_before != workers_after || active_claims_before != active_claims_after {
            return Err(RolloutError::AutonomousMutationDetected);
        }
        let observation = ShadowObservation {
            schema_version: SHADOW_OBSERVATION_SCHEMA,
            id: format!("shadow-{}", Uuid::new_v4()),
            goal_id: goal_id.into(),
            observed_at: now,
            after_event_sequence,
            last_event_sequence: batch.last_event_sequence,
            ready_scopes,
            proposed_claims,
            active_signals,
            supervisor_decisions,
            workers_before,
            workers_after,
            active_claims_before,
            active_claims_after,
        };
        self.store.insert_shadow_observation(&observation)?;
        Ok(observation)
    }

    pub fn start_constrained_pilot(
        &self,
        goal_id: &str,
        repository_path: &std::path::Path,
        base_revision: &str,
        now: DateTime<Utc>,
    ) -> Result<PilotStartReport, RolloutError> {
        let current = self
            .store
            .rollout_control(goal_id)?
            .filter(|control| control.phase == RolloutPhase::Shadow)
            .ok_or(RolloutError::ShadowEvidenceRequired)?;
        if current.autonomous_workers_enabled || current.automatic_integration_enabled {
            return Err(RolloutError::ShadowEvidenceRequired);
        }
        let shadow_observations = self.store.shadow_observations_for_goal(goal_id)?;
        if shadow_observations.is_empty()
            || !shadow_observations.iter().any(|observation| {
                observation.proposed_claims.len() >= 2
                    && observation.workers_before == observation.workers_after
                    && observation.active_claims_before == observation.active_claims_after
            })
        {
            return Err(RolloutError::ShadowEvidenceRequired);
        }
        let repository_path = match WorkspaceIsolationCapability::detect(repository_path)? {
            WorkspaceIsolationCapability::GitWorktrees { repository_root } => repository_root,
            WorkspaceIsolationCapability::SingleWorkerOnly { .. } => {
                return Err(RolloutError::GitRepositoryRequired);
            }
        };
        if base_revision.trim().is_empty() {
            return Err(RolloutError::InvalidControl(
                "constrained pilot requires a base revision".into(),
            ));
        }
        let claims = ClaimService::new(
            self.tracker.clone(),
            Arc::clone(&self.store),
            ClaimPolicy::default(),
        );
        if claims.ready_unclaimed_scopes(goal_id)?.len() < 2 {
            return Err(RolloutError::InvalidControl(
                "constrained pilot requires at least two ready units".into(),
            ));
        }
        let pool = WorkerPoolService::load(Arc::clone(&self.store), WorkerPoolPolicy::default())?;
        let conservative = WorkerPermissionPolicy::conservative();
        pool.start_for_workspace_with_permission_policy(
            goal_id,
            Some(2),
            2,
            &repository_path,
            conservative.clone(),
            conservative,
            now,
        )?;
        let assigned_claims = pool.fill_ready_claims(&claims, goal_id, base_revision, now)?;
        if assigned_claims.len() != 2 {
            return Err(RolloutError::InvalidControl(format!(
                "constrained pilot assigned {} claims instead of two",
                assigned_claims.len()
            )));
        }
        let control = RolloutControl::constrained_pilot(goal_id, repository_path, now);
        self.store.upsert_rollout_control(&control)?;
        Ok(PilotStartReport {
            control,
            pool: pool.snapshot(goal_id)?,
            assigned_claims,
            shadow_observation_ids: shadow_observations
                .into_iter()
                .map(|observation| observation.id)
                .collect(),
        })
    }

    pub fn evaluate_pilot_rollback(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<PilotRollbackReport, RolloutError> {
        let control = self
            .store
            .rollout_control(goal_id)?
            .filter(|control| control.phase == RolloutPhase::ConstrainedPilot)
            .ok_or_else(|| RolloutError::PilotNotConfigured(goal_id.into()))?;
        let policy = control.rollback_policy.clone().ok_or_else(|| {
            RolloutError::InvalidControl("pilot rollback policy is missing".into())
        })?;
        let mut reasons = Vec::new();
        if policy.on_worker_failure
            && self
                .store
                .workers_for_goal(goal_id, None)?
                .iter()
                .any(|worker| matches!(worker.state, WorkerState::Failed | WorkerState::Recovering))
        {
            reasons.push("worker failure or uncertain recovery state".into());
        }
        if policy.on_semantic_conflict
            && self
                .store
                .signals_for_goal(goal_id, Some(SignalState::Active))?
                .iter()
                .any(|signal| {
                    signal.severity.rank() >= EventSeverity::Error.rank()
                        && signal.kinds.iter().any(|kind| {
                            matches!(
                                kind,
                                SignalKind::FileOverlap
                                    | SignalKind::SymbolOverlap
                                    | SignalKind::MigrationOverlap
                                    | SignalKind::MergeConflict
                                    | SignalKind::ContractMismatch
                            )
                        })
                })
        {
            reasons.push("high-severity semantic or integration conflict".into());
        }
        if policy.on_permission_violation
            && self
                .store
                .events_for_goal(goal_id, 0, None, 1_000)?
                .iter()
                .any(|event| {
                    event.kind == CoordinationEventKind::WatchdogActionApplied
                        && event.severity.rank() >= EventSeverity::Error.rank()
                })
        {
            reasons.push("permission or resource watchdog action".into());
        }
        if policy.on_integration_failure
            && !self
                .store
                .integration_jobs_for_goal(goal_id, Some(IntegrationJobState::Failed))?
                .is_empty()
        {
            reasons.push("integration failure".into());
        }
        let pool = WorkerPoolService::load(Arc::clone(&self.store), WorkerPoolPolicy::default())?;
        if reasons.is_empty() {
            return Ok(PilotRollbackReport {
                rolled_back: false,
                reasons,
                control,
                pool: pool.snapshot(goal_id)?,
            });
        }
        let pool_snapshot = pool.pause(goal_id, now)?;
        let mut shadow = RolloutControl::shadow(goal_id, 2, now)?;
        shadow.selected_repository_path = control.selected_repository_path;
        self.store.upsert_rollout_control(&shadow)?;
        Ok(PilotRollbackReport {
            rolled_back: true,
            reasons,
            control: shadow,
            pool: pool_snapshot,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn expand_rollout(
        &self,
        goal_id: &str,
        desired_concurrency: usize,
        benchmark: RolloutBenchmarkReport,
        thresholds: ExpansionThresholds,
        safety_gate_passed: bool,
        quality_gate_passed: bool,
        recovery_gate_passed: bool,
        approved_by: &str,
        base_revision: &str,
        now: DateTime<Utc>,
    ) -> Result<ExpansionReport, RolloutError> {
        let pilot = self
            .store
            .rollout_control(goal_id)?
            .filter(|control| control.phase == RolloutPhase::ConstrainedPilot)
            .ok_or_else(|| RolloutError::PilotNotConfigured(goal_id.into()))?;
        let repository_path = pilot
            .selected_repository_path
            .clone()
            .ok_or_else(|| RolloutError::InvalidControl("pilot repository is missing".into()))?;
        let mut failures = Vec::new();
        if !(3..=4).contains(&desired_concurrency) {
            failures.push("expanded concurrency must be between 3 and 4".into());
        }
        if base_revision.trim().is_empty() {
            failures.push("an expansion base revision is required".into());
        }
        if approved_by.trim().is_empty() || approved_by.chars().count() > 200 {
            failures.push("an attributed rollout approver is required".into());
        }
        if !safety_gate_passed {
            failures.push("safety gate did not pass".into());
        }
        if !quality_gate_passed {
            failures.push("quality gate did not pass".into());
        }
        if !recovery_gate_passed {
            failures.push("recovery gate did not pass".into());
        }
        if benchmark.single_worker.completed_units < thresholds.minimum_completed_units_per_cohort
            || benchmark.pooled.completed_units < thresholds.minimum_completed_units_per_cohort
        {
            failures.push("benchmark sample coverage is below threshold".into());
        }
        if benchmark
            .pooled_relative_to_single
            .throughput_change_percent
            < thresholds.minimum_throughput_change_percent
        {
            failures.push("pooled throughput is below threshold".into());
        }
        if benchmark
            .pooled_relative_to_single
            .tokens_per_unit_change_percent
            > thresholds.maximum_tokens_per_unit_change_percent
        {
            failures.push("pooled token cost is above threshold".into());
        }
        if benchmark.pooled.conflicts_per_completed_unit
            > thresholds.maximum_conflicts_per_completed_unit
        {
            failures.push("pooled conflict rate is above threshold".into());
        }
        if benchmark.pooled.average_integration_latency_milliseconds
            > thresholds.maximum_average_integration_latency_milliseconds
        {
            failures.push("pooled integration latency is above threshold".into());
        }
        if benchmark.pooled.human_interruptions_per_completed_unit
            > thresholds.maximum_human_interruptions_per_completed_unit
        {
            failures.push("pooled human interruption rate is above threshold".into());
        }
        if !self
            .store
            .workers_for_goal(goal_id, Some(WorkerState::Failed))?
            .is_empty()
        {
            failures.push("failed workers remain in the pilot".into());
        }
        if !self
            .store
            .integration_jobs_for_goal(goal_id, Some(IntegrationJobState::Failed))?
            .is_empty()
        {
            failures.push("failed integrations remain in the pilot".into());
        }
        if self
            .store
            .escalations_for_goal(goal_id, None)?
            .iter()
            .any(|escalation| {
                escalation.severity == EscalationSeverity::Critical
                    && matches!(
                        escalation.state,
                        EscalationState::Open | EscalationState::Acknowledged
                    )
            })
        {
            failures.push("critical human escalations remain unresolved".into());
        }
        let rollback = self.evaluate_pilot_rollback(goal_id, now)?;
        if rollback.rolled_back {
            failures.extend(
                rollback
                    .reasons
                    .into_iter()
                    .map(|reason| format!("pilot rollback gate: {reason}")),
            );
        }
        if !failures.is_empty() {
            return Err(RolloutError::ExpansionGateFailed(failures));
        }
        let evidence = ExpansionGateEvidence {
            approved_by: approved_by.into(),
            evaluated_at: now,
            safety_gate_passed,
            quality_gate_passed,
            recovery_gate_passed,
            thresholds,
            benchmark,
        };
        let control = RolloutControl::expanded(&pilot, desired_concurrency, evidence, now)?;
        let claims = ClaimService::new(
            self.tracker.clone(),
            Arc::clone(&self.store),
            ClaimPolicy::default(),
        );
        let ready_units = claims.ready_unclaimed_scopes(goal_id)?.len();
        let pool = WorkerPoolService::load(Arc::clone(&self.store), WorkerPoolPolicy::default())?;
        pool.set_desired_concurrency_for_workspace(
            goal_id,
            desired_concurrency,
            ready_units,
            &repository_path,
            now,
        )?;
        let newly_assigned_claims = pool.fill_ready_claims(&claims, goal_id, base_revision, now)?;
        self.store.upsert_rollout_control(&control)?;
        Ok(ExpansionReport {
            control,
            pool: pool.snapshot(goal_id)?,
            newly_assigned_claims,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::domain::{
        ClaimId, CoordinationSignal, EventId, EventSeverity, SignalKind, WorkerId,
    };
    use super::super::pool::PoolMode;
    use super::*;
    use crate::coordination::benchmark::BenchmarkInput;
    use crate::models::Status;
    use std::fs;
    use std::process::Command;

    struct DeterministicShadowEvaluator;

    impl ShadowSupervisorEvaluator for DeterministicShadowEvaluator {
        fn evaluate(
            &self,
            trigger: &SupervisorTrigger,
        ) -> Result<ShadowSupervisorDecision, String> {
            Ok(ShadowSupervisorDecision {
                trigger_key: trigger.key.clone(),
                trigger_kind: trigger.kind,
                level: InterventionLevel::Recommend,
                confidence_percent: 90,
                summary: "Recommend reconciliation without applying an action".into(),
            })
        }
    }

    #[test]
    fn shadow_mode_persists_observations_without_launching_workers_or_claims() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker
            .create_goal("goal-shadow", "Shadow", "observe only")
            .unwrap();
        for id in ["alpha", "beta", "gamma"] {
            tracker
                .add_feature("goal-shadow", id, id, "ready", Status::Planned)
                .unwrap();
        }
        let database = directory.path().join("coordination.sqlite");
        let store = Arc::new(SqliteCoordinationStore::open(&database).unwrap());
        let now = Utc::now();
        let signal = CoordinationSignal::new(
            "goal-shadow",
            "merge:shadow",
            SignalKind::MergeConflict,
            95,
            EventSeverity::Critical,
            EventId::new(),
            vec![WorkerId::new()],
            vec![ClaimId::new()],
            now,
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        store.merge_signal_observation(&signal, now).unwrap();
        let service = RolloutService::new(tracker, Arc::clone(&store));
        let control = service.configure_shadow("goal-shadow", 2, now).unwrap();
        assert_eq!(control.phase, RolloutPhase::Shadow);
        assert!(!control.autonomous_workers_enabled);
        assert!(!control.automatic_integration_enabled);

        let observation = service
            .observe_shadow(
                "goal-shadow",
                "base-shadow",
                0,
                &DeterministicShadowEvaluator,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        assert_eq!(observation.ready_scopes.len(), 3);
        assert_eq!(observation.proposed_claims.len(), 2);
        assert_eq!(observation.active_signals.len(), 1);
        assert_eq!(observation.supervisor_decisions.len(), 1);
        assert_eq!(observation.workers_before, 0);
        assert_eq!(observation.workers_after, 0);
        assert_eq!(observation.active_claims_before, 0);
        assert_eq!(observation.active_claims_after, 0);
        assert!(
            store
                .workers_for_goal("goal-shadow", None)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .claims_for_goal("goal-shadow", None)
                .unwrap()
                .is_empty()
        );

        drop(service);
        drop(store);
        let reopened = SqliteCoordinationStore::open(database).unwrap();
        assert_eq!(
            reopened.rollout_control("goal-shadow").unwrap().unwrap(),
            control
        );
        assert_eq!(
            reopened
                .shadow_observations_for_goal("goal-shadow")
                .unwrap(),
            vec![observation]
        );
    }

    #[test]
    fn constrained_pilot_enforces_two_conservative_workers_manual_integration_and_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(&repository, &["config", "user.email", "pilot@example.test"]);
        git(&repository, &["config", "user.name", "Pilot Test"]);
        fs::write(repository.join("README.md"), "pilot\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-q", "-m", "base"]);
        let base_revision = git_output(&repository, &["rev-parse", "HEAD"]);

        let tracker = Tracker::new(directory.path().join("goals"));
        tracker
            .create_goal("goal-pilot", "Pilot", "two workers")
            .unwrap();
        for id in ["alpha", "beta", "gamma"] {
            tracker
                .add_feature("goal-pilot", id, id, "ready", Status::Planned)
                .unwrap();
        }
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = RolloutService::new(tracker, Arc::clone(&store));
        let now = Utc::now();
        service.configure_shadow("goal-pilot", 2, now).unwrap();
        service
            .observe_shadow(
                "goal-pilot",
                &base_revision,
                0,
                &DeterministicShadowEvaluator,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();

        let pilot = service
            .start_constrained_pilot(
                "goal-pilot",
                &repository,
                &base_revision,
                now + chrono::Duration::seconds(2),
            )
            .unwrap();
        assert_eq!(pilot.control.phase, RolloutPhase::ConstrainedPilot);
        assert_eq!(pilot.control.desired_concurrency, 2);
        assert!(pilot.control.autonomous_workers_enabled);
        assert!(!pilot.control.automatic_integration_enabled);
        assert!(pilot.control.manual_integration_required);
        assert!(pilot.control.conservative_permissions_required);
        assert!(pilot.control.rollback_policy.is_some());
        assert_eq!(pilot.pool.goal.mode, PoolMode::Running);
        assert_eq!(pilot.pool.workers.len(), 2);
        assert_eq!(pilot.assigned_claims.len(), 2);
        for worker in &pilot.pool.workers {
            let permissions = worker.permission_profile.as_ref().unwrap();
            assert_eq!(permissions.sandbox, "workspace-write");
            assert_eq!(permissions.approval_policy, "on-request");
            assert_eq!(permissions.approvals_reviewer, "user");
            assert!(!permissions.network_access);
        }
        assert_eq!(
            store
                .claims_for_goal("goal-pilot", Some(super::super::domain::ClaimState::Active))
                .unwrap()
                .len(),
            2
        );
        assert!(
            store
                .integration_jobs_for_goal("goal-pilot", None)
                .unwrap()
                .is_empty()
        );

        let signal = CoordinationSignal::new(
            "goal-pilot",
            "merge:pilot-rollback",
            SignalKind::MergeConflict,
            99,
            EventSeverity::Critical,
            EventId::new(),
            pilot
                .pool
                .workers
                .iter()
                .map(|worker| worker.id.clone())
                .collect(),
            pilot
                .assigned_claims
                .iter()
                .map(|claim| claim.id.clone())
                .collect(),
            now + chrono::Duration::seconds(3),
            now + chrono::Duration::minutes(10),
        )
        .unwrap();
        store
            .merge_signal_observation(&signal, now + chrono::Duration::seconds(3))
            .unwrap();
        let rollback = service
            .evaluate_pilot_rollback("goal-pilot", now + chrono::Duration::seconds(4))
            .unwrap();
        assert!(rollback.rolled_back);
        assert_eq!(rollback.control.phase, RolloutPhase::Shadow);
        assert_eq!(rollback.pool.goal.mode, PoolMode::Paused);
        assert!(
            rollback
                .reasons
                .iter()
                .any(|reason| reason.contains("conflict"))
        );
        assert!(
            rollback
                .pool
                .workers
                .iter()
                .all(|worker| worker.state == WorkerState::Paused)
        );
        assert_eq!(
            store
                .claims_for_goal("goal-pilot", Some(super::super::domain::ClaimState::Active))
                .unwrap()
                .len(),
            2
        );
        assert!(
            store
                .integration_jobs_for_goal("goal-pilot", None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn expansion_requires_every_threshold_and_records_the_approved_gate_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(
            &repository,
            &["config", "user.email", "expand@example.test"],
        );
        git(&repository, &["config", "user.name", "Expansion Test"]);
        fs::write(repository.join("README.md"), "expand\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-q", "-m", "base"]);
        let base_revision = git_output(&repository, &["rev-parse", "HEAD"]);

        let tracker = Tracker::new(directory.path().join("goals"));
        tracker
            .create_goal("goal-expand", "Expansion", "gated rollout")
            .unwrap();
        for id in ["alpha", "beta", "gamma", "delta"] {
            tracker
                .add_feature("goal-expand", id, id, "ready", Status::Planned)
                .unwrap();
        }
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = RolloutService::new(tracker, Arc::clone(&store));
        let now = Utc::now();
        service.configure_shadow("goal-expand", 2, now).unwrap();
        service
            .observe_shadow(
                "goal-expand",
                &base_revision,
                0,
                &DeterministicShadowEvaluator,
                now + chrono::Duration::seconds(1),
            )
            .unwrap();
        service
            .start_constrained_pilot(
                "goal-expand",
                &repository,
                &base_revision,
                now + chrono::Duration::seconds(2),
            )
            .unwrap();
        let benchmark_input: BenchmarkInput =
            serde_json::from_str(include_str!("fixtures/rollout-benchmark-reference.json"))
                .unwrap();
        let benchmark = RolloutBenchmarkReport::compare(&benchmark_input).unwrap();
        let rejected = service.expand_rollout(
            "goal-expand",
            3,
            benchmark.clone(),
            ExpansionThresholds::default(),
            true,
            true,
            false,
            "operator-a",
            &base_revision,
            now + chrono::Duration::seconds(3),
        );
        assert!(matches!(
            rejected,
            Err(RolloutError::ExpansionGateFailed(ref failures))
                if failures.iter().any(|failure| failure.contains("recovery"))
        ));
        assert_eq!(
            store.rollout_control("goal-expand").unwrap().unwrap().phase,
            RolloutPhase::ConstrainedPilot
        );
        assert_eq!(
            WorkerPoolService::load(Arc::clone(&store), WorkerPoolPolicy::default())
                .unwrap()
                .snapshot("goal-expand")
                .unwrap()
                .goal
                .desired_concurrency,
            2
        );

        let expanded = service
            .expand_rollout(
                "goal-expand",
                3,
                benchmark,
                ExpansionThresholds::default(),
                true,
                true,
                true,
                "operator-a",
                &base_revision,
                now + chrono::Duration::seconds(4),
            )
            .unwrap();
        assert_eq!(expanded.control.phase, RolloutPhase::Expanded);
        assert_eq!(expanded.control.desired_concurrency, 3);
        assert!(expanded.control.autonomous_workers_enabled);
        assert!(expanded.control.automatic_integration_enabled);
        assert!(!expanded.control.manual_integration_required);
        assert_eq!(expanded.pool.workers.len(), 3);
        assert_eq!(expanded.newly_assigned_claims.len(), 1);
        let evidence = expanded.control.expansion_evidence.as_ref().unwrap();
        assert_eq!(evidence.approved_by, "operator-a");
        assert!(evidence.safety_gate_passed);
        assert!(evidence.quality_gate_passed);
        assert!(evidence.recovery_gate_passed);
        assert_eq!(evidence.benchmark.pooled.completed_units, 4);
        assert_eq!(
            store.rollout_control("goal-expand").unwrap().unwrap(),
            expanded.control
        );
    }

    fn git(path: &std::path::Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
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

    fn git_output(path: &std::path::Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().into()
    }
}
