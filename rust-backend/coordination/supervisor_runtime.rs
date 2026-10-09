use super::claims::{ClaimPolicy, ClaimService};
use super::domain::{
    CoordinationActor, CoordinationEvent, CoordinationEventKind, EscalationKind, EscalationScope,
    EventSeverity, HumanEscalation, InterventionLevel, InterventionState, RemediationAction,
    RemediationOption, SupervisorBudgetLedger,
};
use super::escalations::EscalationLifecycleService;
use super::notifications::{
    NotificationCandidate, NotificationEnqueueOutcome, NotificationNoisePolicy,
    NotificationQueueService,
};
use super::pool::PoolMode;
use super::signals::{SignalProcessingPolicy, SignalProcessor};
use super::store::{
    EscalationRepository, EventRepository, InterventionRepository, PoolRepository,
    SqliteCoordinationStore, SupervisorBudgetRepository,
};
use super::supervisor::{
    SupervisorActionExecutor, SupervisorActionSink, SupervisorContextBuilder,
    SupervisorContextPacket, SupervisorDecision, SupervisorEvaluationBudget,
    SupervisorEvaluationUsage, SupervisorExecutionDisposition, SupervisorInterventionService,
    SupervisorTargetedAction, SupervisorTrigger, SupervisorTriggerPolicy, SupervisorTriggerService,
};
use crate::models::{Run, WorkMode};
use crate::runner::RunManager;
use crate::tracker::Tracker;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration as TokioDuration, sleep, timeout};

pub const SUPERVISOR_EVENT_CONSUMER: &str = "supervisor-runtime";
pub const SUPERVISOR_RUN_PROMPT_PREFIX: &str = "Evaluate Golazo Supervisor trigger";
const SUPERVISOR_MONITOR_INTERVAL: TokioDuration = TokioDuration::from_secs(2);
const SUPERVISOR_RUN_POLL_INTERVAL: TokioDuration = TokioDuration::from_millis(250);
const SUPERVISOR_RUN_TIMEOUT: TokioDuration = TokioDuration::from_secs(35 * 60);

#[derive(Debug, Clone)]
pub struct SupervisorEvaluationResult {
    pub context: SupervisorContextPacket,
    pub decision: SupervisorDecision,
    pub usage: SupervisorEvaluationUsage,
    pub run_id: String,
    pub thread_id: Option<String>,
}

pub trait SupervisorRuntimeEvaluator: Send + Sync {
    fn evaluate<'a>(
        &'a self,
        trigger: &'a SupervisorTrigger,
        budget: SupervisorEvaluationBudget,
    ) -> Pin<Box<dyn Future<Output = Result<SupervisorEvaluationResult, String>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct CodexSupervisorEvaluator {
    runner: Arc<RunManager>,
    context: SupervisorContextBuilder,
}

impl CodexSupervisorEvaluator {
    pub fn new(runner: Arc<RunManager>, store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            runner,
            context: SupervisorContextBuilder::new(store),
        }
    }

    async fn wait_for_run(&self, run_id: &str) -> Result<Run, String> {
        timeout(SUPERVISOR_RUN_TIMEOUT, async {
            loop {
                let run = self
                    .runner
                    .get(run_id)
                    .await
                    .ok_or_else(|| "Supervisor Codex run disappeared".to_string())?;
                if matches!(run.status.as_str(), "completed" | "failed" | "cancelled") {
                    return Ok(run);
                }
                sleep(SUPERVISOR_RUN_POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| "Supervisor Codex run exceeded its 35 minute runtime limit".to_string())?
    }
}

impl SupervisorRuntimeEvaluator for CodexSupervisorEvaluator {
    fn evaluate<'a>(
        &'a self,
        trigger: &'a SupervisorTrigger,
        budget: SupervisorEvaluationBudget,
    ) -> Pin<Box<dyn Future<Output = Result<SupervisorEvaluationResult, String>> + Send + 'a>> {
        Box::pin(async move {
            let now = Utc::now();
            let context = self
                .context
                .build(&trigger.goal_id, trigger, now)
                .map_err(|error| error.to_string())?;
            let mut execution_prompt = context
                .evaluation_instructions()
                .map_err(|error| error.to_string())?;
            execution_prompt.push_str(&format!(
                "\n\n## Runtime constraints\n\nThis is a read-only Supervisor evaluation. Do not use tools or request approvals. Keep the complete evaluation within {} total tokens.",
                budget.max_total_tokens
            ));
            let prompt = format!(
                "{SUPERVISOR_RUN_PROMPT_PREFIX} {} and return the required structured decision.",
                trigger.key
            );
            let run = self
                .runner
                .create(
                    prompt,
                    vec![],
                    vec![],
                    ".".into(),
                    "read-only".into(),
                    "on-request".into(),
                    "user".into(),
                    true,
                    Some(trigger.goal_id.clone()),
                    None,
                    Some(execution_prompt),
                    Some(supervisor_decision_schema()),
                    WorkMode::Spec,
                    None,
                )
                .await?;
            let run = self.wait_for_run(&run.id).await?;
            if run.status != "completed" {
                return Err(run
                    .terminal_error
                    .as_ref()
                    .map(|error| match error.code.as_deref() {
                        Some(code) => {
                            format!("Supervisor Codex run failed ({code}): {}", error.message)
                        }
                        None => format!("Supervisor Codex run failed: {}", error.message),
                    })
                    .or(run.error.clone())
                    .unwrap_or_else(|| "Supervisor Codex run failed".into()));
            }
            let response = run.final_message.as_deref().ok_or_else(|| {
                "Supervisor Codex run returned no structured decision".to_string()
            })?;
            let decision = context
                .parse_decision(response)
                .map_err(|error| error.to_string())?;
            Ok(SupervisorEvaluationResult {
                context,
                decision,
                usage: SupervisorEvaluationUsage {
                    input_tokens: run.usage.input_tokens.max(0) as u64,
                    output_tokens: run.usage.output_tokens.max(0) as u64,
                },
                run_id: run.id,
                thread_id: run.thread_id,
            })
        })
    }
}

pub fn supervisor_decision_schema() -> Value {
    serde_json::from_str(include_str!("../schemas/supervisor-decision.schema.json"))
        .expect("bundled Supervisor decision schema must be valid JSON")
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRuntimeReport {
    pub goal_id: String,
    pub cursor_before: u64,
    pub cursor_after: u64,
    pub evaluated_trigger_keys: Vec<String>,
    pub existing_trigger_keys: Vec<String>,
    pub skipped: Vec<String>,
    pub failures: Vec<String>,
    pub intervention_ids: Vec<String>,
    pub created_signal_ids: Vec<String>,
    pub updated_signal_ids: Vec<String>,
}

#[derive(Clone)]
pub struct SupervisorRuntimeService {
    store: Arc<SqliteCoordinationStore>,
    evaluator: Arc<dyn SupervisorRuntimeEvaluator>,
    policy: SupervisorTriggerPolicy,
    tracker: Option<Tracker>,
}

impl SupervisorRuntimeService {
    pub fn new(
        store: Arc<SqliteCoordinationStore>,
        evaluator: Arc<dyn SupervisorRuntimeEvaluator>,
    ) -> Self {
        Self {
            store,
            evaluator,
            policy: SupervisorTriggerPolicy::default(),
            tracker: None,
        }
    }

    pub fn with_tracker(mut self, tracker: Tracker) -> Self {
        self.tracker = Some(tracker);
        self
    }

    pub async fn process_goal_once(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<SupervisorRuntimeReport, String> {
        let pool = self
            .store
            .goal_pool(goal_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("goal pool is not configured: {goal_id}"))?;
        let cursor_before = self
            .store
            .event_replay_cursor(SUPERVISOR_EVENT_CONSUMER, goal_id)
            .map_err(|error| error.to_string())?
            .map(|cursor| cursor.acknowledged_sequence)
            .unwrap_or_default();
        let mut report = SupervisorRuntimeReport {
            goal_id: goal_id.into(),
            cursor_before,
            cursor_after: cursor_before,
            ..SupervisorRuntimeReport::default()
        };
        if !matches!(pool.mode, PoolMode::Running | PoolMode::Draining) {
            report.skipped.push(format!(
                "Supervisor is inactive while the pool is {:?}",
                pool.mode
            ));
            return Ok(report);
        }

        let replay = self
            .store
            .replay_events_for_consumer(SUPERVISOR_EVENT_CONSUMER, goal_id, 1_000, now)
            .map_err(|error| error.to_string())?;
        let signal_report =
            SignalProcessor::new(Arc::clone(&self.store), SignalProcessingPolicy::default())
                .map_err(|error| error.to_string())?
                .process(goal_id, &replay.events, now)
                .map_err(|error| error.to_string())?;
        report.created_signal_ids = signal_report
            .created_signal_ids
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect();
        report.updated_signal_ids = signal_report
            .updated_signal_ids
            .into_iter()
            .map(|id| id.as_str().to_string())
            .collect();
        let trigger_service =
            SupervisorTriggerService::new(Arc::clone(&self.store), self.policy.clone())
                .map_err(|error| error.to_string())?;
        let batch = trigger_service
            .eligible_triggers(goal_id, replay.cursor.acknowledged_sequence, now)
            .map_err(|error| error.to_string())?;
        let existing = self
            .store
            .interventions_for_goal(goal_id, None)
            .map_err(|error| error.to_string())?;
        let mut budget = self
            .store
            .supervisor_budget(goal_id)
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| SupervisorBudgetLedger::new(goal_id, now));
        reset_budget_window(&mut budget, &self.policy, now);
        let mut retry_required = false;

        for trigger in batch.triggers {
            if existing.iter().any(|item| {
                item.trigger_key.as_deref() == Some(trigger.key.as_str())
                    && item.state != InterventionState::Proposed
            }) {
                report.existing_trigger_keys.push(trigger.key);
                continue;
            }
            if let Some(reason) = budget_skip_reason(&budget, &self.policy, &trigger, now) {
                report.skipped.push(format!("{}: {reason}", trigger.key));
                retry_required = true;
                continue;
            }
            let allowance = self
                .policy
                .max_tokens_per_window
                .saturating_sub(budget.consumed_tokens)
                .min(self.policy.max_tokens_per_evaluation);
            budget.evaluation_count = budget.evaluation_count.saturating_add(1);
            *budget
                .attempts_by_trigger
                .entry(trigger.key.clone())
                .or_default() += 1;
            budget.touch(Utc::now());
            self.store
                .upsert_supervisor_budget(&budget)
                .map_err(|error| error.to_string())?;

            match self
                .evaluator
                .evaluate(
                    &trigger,
                    SupervisorEvaluationBudget {
                        max_total_tokens: allowance,
                    },
                )
                .await
            {
                Ok(evaluation) => {
                    let used = evaluation
                        .usage
                        .input_tokens
                        .saturating_add(evaluation.usage.output_tokens);
                    budget.consumed_tokens = budget.consumed_tokens.saturating_add(used);
                    if used > allowance {
                        let message = format!(
                            "{} used {used} tokens, exceeding its {allowance} token allowance",
                            trigger.key
                        );
                        record_failure(&mut budget, &self.policy, &message, Utc::now());
                        report.failures.push(message);
                        retry_required = true;
                        continue;
                    }
                    if !self.pool_accepts_results(goal_id)? {
                        let message = format!(
                            "{} completed after the pool stopped or paused; its decision was not applied",
                            trigger.key
                        );
                        record_failure(&mut budget, &self.policy, &message, Utc::now());
                        report.failures.push(message);
                        retry_required = true;
                        continue;
                    }
                    match self.apply_evaluation(evaluation, Utc::now()).await {
                        Ok(intervention_id) => {
                            budget.consecutive_failures = 0;
                            budget.last_failure = None;
                            budget.last_failure_at = None;
                            if !budget.completed_trigger_keys.contains(&trigger.key) {
                                budget.completed_trigger_keys.push(trigger.key.clone());
                            }
                            report.evaluated_trigger_keys.push(trigger.key);
                            report.intervention_ids.push(intervention_id);
                        }
                        Err(error) => {
                            let message = format!("{}: {error}", trigger.key);
                            record_failure(&mut budget, &self.policy, &message, Utc::now());
                            report.failures.push(message);
                            retry_required = true;
                        }
                    }
                }
                Err(error) => {
                    let message = format!("{}: {error}", trigger.key);
                    record_failure(&mut budget, &self.policy, &message, Utc::now());
                    report.failures.push(message);
                    retry_required = true;
                }
            }
            budget.touch(Utc::now());
            self.store
                .upsert_supervisor_budget(&budget)
                .map_err(|error| error.to_string())?;
        }
        budget.touch(Utc::now());
        self.store
            .upsert_supervisor_budget(&budget)
            .map_err(|error| error.to_string())?;

        if !retry_required && replay.cursor.last_delivered_sequence > cursor_before {
            let cursor = self
                .store
                .acknowledge_event_sequence(
                    SUPERVISOR_EVENT_CONSUMER,
                    goal_id,
                    replay.cursor.last_delivered_sequence,
                    Utc::now(),
                )
                .map_err(|error| error.to_string())?;
            report.cursor_after = cursor.acknowledged_sequence;
        }
        Ok(report)
    }

    fn pool_accepts_results(&self, goal_id: &str) -> Result<bool, String> {
        Ok(self
            .store
            .goal_pool(goal_id)
            .map_err(|error| error.to_string())?
            .is_some_and(|pool| matches!(pool.mode, PoolMode::Running | PoolMode::Draining)))
    }

    async fn apply_evaluation(
        &self,
        evaluation: SupervisorEvaluationResult,
        now: DateTime<Utc>,
    ) -> Result<String, String> {
        let mut intervention = SupervisorInterventionService::new(Arc::clone(&self.store))
            .persist_decision(&evaluation.context, &evaluation.decision, now)
            .map_err(|error| error.to_string())?;
        let event = CoordinationEvent::new(
            &intervention.goal_id,
            CoordinationEventKind::InterventionCreated,
            intervention_severity(intervention.level),
            CoordinationActor::Supervisor,
            intervention.id.as_str(),
            json!({
                "interventionId": intervention.id,
                "triggerKey": intervention.trigger_key,
                "level": intervention.level,
                "summary": intervention.decision_summary,
                "runId": evaluation.run_id,
                "threadId": evaluation.thread_id,
                "inputTokens": evaluation.usage.input_tokens,
                "outputTokens": evaluation.usage.output_tokens,
            }),
            now,
        );
        self.store
            .append_event(&event)
            .map_err(|error| error.to_string())?;

        if intervention.level == InterventionLevel::Escalate {
            let escalation_id = self.create_escalation(
                &intervention,
                &evaluation.context,
                &evaluation.decision,
                now,
            )?;
            intervention
                .transition(
                    InterventionState::Delivered,
                    now,
                    Some(format!("Human escalation {} opened", escalation_id)),
                )
                .map_err(|error| error.to_string())?;
            self.store
                .upsert_intervention(&intervention)
                .map_err(|error| error.to_string())?;
        } else {
            let sink = Arc::new(DurableSupervisorActionSink::new(Arc::clone(&self.store)));
            let outcome = SupervisorActionExecutor::new(Arc::clone(&self.store), sink)
                .execute(&intervention.goal_id, &intervention.id, now)
                .map_err(|error| error.to_string())?;
            if outcome.disposition == SupervisorExecutionDisposition::AwaitingHumanEscalation {
                return Err("Supervisor escalation was not materialized".into());
            }
        }
        Ok(intervention.id.as_str().into())
    }

    fn create_escalation(
        &self,
        intervention: &super::domain::SupervisorIntervention,
        context: &SupervisorContextPacket,
        decision: &SupervisorDecision,
        now: DateTime<Utc>,
    ) -> Result<String, String> {
        let proposal = decision.escalation.as_ref().ok_or_else(|| {
            "Supervisor escalation decision has no escalation payload".to_string()
        })?;
        let reference = format!("intervention:{}", intervention.id.as_str());
        if let Some(existing) = self
            .store
            .escalations_for_goal(&intervention.goal_id, None)
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|item| item.evidence_refs.contains(&reference))
        {
            return Ok(existing.id.as_str().into());
        }
        let scope = escalation_scope(&proposal.scope, context)?;
        let mut escalation = HumanEscalation::new(
            &intervention.goal_id,
            proposal.kind,
            proposal.severity,
            scope,
            &proposal.question,
            now,
        );
        escalation.evidence_refs = decision
            .evidence_refs
            .iter()
            .cloned()
            .chain(
                decision
                    .evidence_event_ids
                    .iter()
                    .map(|id| format!("event:{}", id.as_str())),
            )
            .chain(std::iter::once(reference))
            .collect();
        escalation.options = proposal
            .options
            .iter()
            .map(|option| RemediationOption {
                id: option.id.clone(),
                action: remediation_action(&option.id, proposal.kind),
                label: option.label.clone(),
                description: option.description.clone(),
                consequences: option.consequences.clone(),
                recommended: option.recommended,
            })
            .collect();
        let created = EscalationLifecycleService::new(Arc::clone(&self.store))
            .create(&escalation)
            .map_err(|error| error.to_string())?;
        if created.kind == EscalationKind::HardBlocker {
            let tracker = self.tracker.clone().ok_or_else(|| {
                "production Supervisor cannot enforce a hard blocker without the goal tracker"
                    .to_string()
            })?;
            let claims =
                ClaimService::new(tracker, Arc::clone(&self.store), ClaimPolicy::default());
            EscalationLifecycleService::new(Arc::clone(&self.store))
                .enforce_scope_block(&claims, &intervention.goal_id, &created.id, now)
                .map_err(|error| error.to_string())?;
        }
        Ok(created.id.as_str().into())
    }
}

fn reset_budget_window(
    budget: &mut SupervisorBudgetLedger,
    policy: &SupervisorTriggerPolicy,
    now: DateTime<Utc>,
) {
    let window = Duration::seconds(policy.budget_window_seconds as i64);
    if now < budget.window_started_at || now - budget.window_started_at >= window {
        budget.window_started_at = now;
        budget.evaluation_count = 0;
        budget.consumed_tokens = 0;
        budget.consecutive_failures = 0;
        budget.circuit_open_until = None;
        budget.attempts_by_trigger.clear();
        budget.completed_trigger_keys.clear();
        budget.last_failure = None;
        budget.last_failure_at = None;
    } else if budget
        .circuit_open_until
        .is_some_and(|open_until| open_until <= now)
    {
        budget.circuit_open_until = None;
        budget.consecutive_failures = 0;
    }
}

fn budget_skip_reason(
    budget: &SupervisorBudgetLedger,
    policy: &SupervisorTriggerPolicy,
    trigger: &SupervisorTrigger,
    now: DateTime<Utc>,
) -> Option<String> {
    if budget.completed_trigger_keys.contains(&trigger.key) {
        Some("trigger was already evaluated in this budget window".into())
    } else if budget
        .circuit_open_until
        .is_some_and(|open_until| open_until > now)
    {
        Some("Supervisor circuit breaker is open".into())
    } else if budget.evaluation_count >= policy.max_evaluations_per_window {
        Some("Supervisor evaluation frequency budget is exhausted".into())
    } else if budget.consumed_tokens >= policy.max_tokens_per_window {
        Some("Supervisor token budget is exhausted".into())
    } else if budget
        .attempts_by_trigger
        .get(&trigger.key)
        .copied()
        .unwrap_or_default()
        > policy.max_retries_per_trigger
    {
        Some("Supervisor retry budget is exhausted for this trigger".into())
    } else {
        None
    }
}

fn record_failure(
    budget: &mut SupervisorBudgetLedger,
    policy: &SupervisorTriggerPolicy,
    message: &str,
    now: DateTime<Utc>,
) {
    budget.consecutive_failures = budget.consecutive_failures.saturating_add(1);
    if budget.consecutive_failures >= policy.consecutive_failure_limit {
        budget.circuit_open_until =
            Some(now + Duration::seconds(policy.circuit_breaker_seconds as i64));
    }
    budget.last_failure = Some(message.chars().take(2_000).collect());
    budget.last_failure_at = Some(now);
    budget.touch(now);
}

#[derive(Clone)]
struct DurableSupervisorActionSink {
    store: Arc<SqliteCoordinationStore>,
}

impl DurableSupervisorActionSink {
    fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }
}

impl SupervisorActionSink for DurableSupervisorActionSink {
    fn deliver(&self, action: &SupervisorTargetedAction) -> Result<(), String> {
        let source_event_id = match action.evidence_event_ids.first() {
            Some(event_id) => event_id.clone(),
            None => self
                .store
                .latest_events_for_goal(&action.goal_id, Some(action.intervention_id.as_str()), 1)
                .map_err(|error| error.to_string())?
                .into_iter()
                .next()
                .map(|event| event.id)
                .ok_or_else(|| "Supervisor action has no durable source event".to_string())?,
        };
        let now = Utc::now();
        let queue = NotificationQueueService::new(
            Arc::clone(&self.store),
            NotificationNoisePolicy::default(),
        )
        .map_err(|error| error.to_string())?;
        let outcome = queue
            .enqueue(
                NotificationCandidate {
                    goal_id: action.goal_id.clone(),
                    target_worker: action.target_worker_id.clone(),
                    source_event_id,
                    purpose: format!("supervisor_{}", intervention_level_name(action.level)),
                    summary: action.summary.clone(),
                    severity: intervention_severity(action.level),
                    evidence_refs: action
                        .evidence_refs
                        .iter()
                        .cloned()
                        .chain(std::iter::once(format!(
                            "intervention:{}",
                            action.intervention_id.as_str()
                        )))
                        .collect(),
                    recommended_action: action.requested_action.clone().or_else(|| {
                        (!action.recommendation.trim().is_empty())
                            .then(|| action.recommendation.clone())
                    }),
                    required_acknowledgement: matches!(
                        action.level,
                        InterventionLevel::Coordinate | InterventionLevel::Block
                    ),
                    expires_at: Some(now + Duration::minutes(30)),
                },
                now,
            )
            .map_err(|error| error.to_string())?;
        match outcome {
            NotificationEnqueueOutcome::Queued(_)
            | NotificationEnqueueOutcome::Coalesced { .. }
            | NotificationEnqueueOutcome::Suppressed {
                existing_notification_id: Some(_),
                ..
            } => Ok(()),
            NotificationEnqueueOutcome::Suppressed { .. } => {
                Err("Supervisor notification was suppressed without an existing delivery".into())
            }
        }
    }
}

fn intervention_severity(level: InterventionLevel) -> EventSeverity {
    match level {
        InterventionLevel::Observe => EventSeverity::Debug,
        InterventionLevel::Inform => EventSeverity::Info,
        InterventionLevel::Recommend | InterventionLevel::Coordinate => EventSeverity::Warning,
        InterventionLevel::Block => EventSeverity::Error,
        InterventionLevel::Escalate => EventSeverity::Critical,
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

fn remediation_action(id: &str, kind: EscalationKind) -> RemediationAction {
    let normalized = id.to_ascii_lowercase();
    if normalized.contains("reconcile") {
        RemediationAction::ReconcileNow
    } else if normalized.contains("wait") {
        RemediationAction::Wait
    } else if normalized.contains("split") {
        RemediationAction::SplitPackage
    } else if normalized.contains("pause") {
        RemediationAction::PauseScope
    } else if normalized.contains("cancel") {
        RemediationAction::CancelScope
    } else if normalized.contains("override") || normalized.contains("risk") {
        RemediationAction::OverrideRisk
    } else if kind == EscalationKind::HardBlocker {
        RemediationAction::PauseScope
    } else {
        RemediationAction::ChooseDirection
    }
}

fn escalation_scope(
    value: &str,
    context: &SupervisorContextPacket,
) -> Result<EscalationScope, String> {
    let (kind, identifier) = value
        .split_once(':')
        .ok_or_else(|| "Supervisor escalation scope must use kind:identifier".to_string())?;
    match kind.trim().to_ascii_lowercase().as_str() {
        "claim" => context
            .claims
            .iter()
            .find(|claim| claim.claim_id.as_str() == identifier)
            .map(|claim| EscalationScope::Claim {
                claim_id: claim.claim_id.clone(),
            })
            .ok_or_else(|| "Supervisor escalation references an unknown claim".into()),
        "package" | "work_package" => super::domain::WorkPackageId::parse(identifier)
            .map(|work_package_id| EscalationScope::WorkPackage { work_package_id })
            .map_err(|error| error.to_string()),
        "contract" => context
            .contracts
            .iter()
            .find(|contract| {
                contract.contract_id.as_str() == identifier || contract.stable_key == identifier
            })
            .map(|contract| EscalationScope::Contract {
                contract_id: contract.contract_id.clone(),
            })
            .ok_or_else(|| "Supervisor escalation references an unknown contract".into()),
        "integration" | "integration_lane" => context
            .workers
            .iter()
            .filter_map(|worker| worker.repository_id.as_deref())
            .find(|repository_id| *repository_id == identifier)
            .map(|repository_id| EscalationScope::IntegrationLane {
                repository_id: repository_id.into(),
            })
            .ok_or_else(|| "Supervisor escalation references an unknown integration lane".into()),
        "goal" if identifier == context.goal_id => Ok(EscalationScope::Goal {
            goal_id: context.goal_id.clone(),
        }),
        _ => Err("Supervisor escalation references an unsupported or unknown scope".into()),
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SupervisorRuntimeSnapshot {
    pub goal_id: String,
    pub state: String,
    pub pool_mode: Option<PoolMode>,
    pub active_run_id: Option<String>,
    pub active_thread_id: Option<String>,
    pub current_trigger_key: Option<String>,
    pub acknowledged_event_sequence: u64,
    pub last_delivered_event_sequence: u64,
    pub evaluation_count: u32,
    pub consumed_tokens: u64,
    pub circuit_open_until: Option<DateTime<Utc>>,
    pub pending_trigger_count: usize,
    pub last_intervention_id: Option<String>,
    pub last_intervention_level: Option<InterventionLevel>,
    pub last_intervention_state: Option<InterventionState>,
    pub last_intervention_summary: Option<String>,
    pub last_intervention_at: Option<DateTime<Utc>>,
    pub last_failure: Option<String>,
}

pub async fn runtime_snapshot(
    store: Arc<SqliteCoordinationStore>,
    runner: Arc<RunManager>,
    goal_id: &str,
) -> Result<SupervisorRuntimeSnapshot, String> {
    let pool = store
        .goal_pool(goal_id)
        .map_err(|error| error.to_string())?;
    let cursor = store
        .event_replay_cursor(SUPERVISOR_EVENT_CONSUMER, goal_id)
        .map_err(|error| error.to_string())?;
    let budget = store
        .supervisor_budget(goal_id)
        .map_err(|error| error.to_string())?;
    let mut interventions = store
        .interventions_for_goal(goal_id, None)
        .map_err(|error| error.to_string())?;
    interventions.sort_by(|left, right| right.metadata.updated_at.cmp(&left.metadata.updated_at));
    let latest_intervention = interventions.first();
    let mut supervisor_runs = runner
        .list()
        .await
        .into_iter()
        .filter(|run| {
            run.goal_id.as_deref() == Some(goal_id)
                && run.prompt.starts_with(SUPERVISOR_RUN_PROMPT_PREFIX)
        })
        .collect::<Vec<_>>();
    supervisor_runs.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    let active_run = supervisor_runs
        .iter()
        .find(|run| matches!(run.status.as_str(), "queued" | "running"));
    let run_failure = supervisor_runs
        .iter()
        .find(|run| run.status == "failed")
        .and_then(|run| {
            run.terminal_error
                .as_ref()
                .map(|error| error.message.clone())
                .or_else(|| run.error.clone())
        });
    let acknowledged = cursor
        .as_ref()
        .map(|cursor| cursor.acknowledged_sequence)
        .unwrap_or_default();
    let pending_trigger_count =
        SupervisorTriggerService::new(Arc::clone(&store), SupervisorTriggerPolicy::default())
            .map_err(|error| error.to_string())?
            .eligible_triggers(goal_id, acknowledged, Utc::now())
            .map_err(|error| error.to_string())?
            .triggers
            .into_iter()
            .filter(|trigger| {
                !interventions
                    .iter()
                    .any(|intervention| intervention.trigger_key.as_deref() == Some(&trigger.key))
            })
            .count();
    let state = if active_run.is_some() {
        "evaluating"
    } else if budget
        .as_ref()
        .and_then(|budget| budget.circuit_open_until)
        .is_some_and(|until| until > Utc::now())
    {
        "circuit_open"
    } else {
        match pool.as_ref().map(|pool| pool.mode) {
            None | Some(PoolMode::Stopped) => "stopped",
            Some(PoolMode::Paused) => "paused",
            Some(PoolMode::Running) | Some(PoolMode::Draining)
                if run_failure.is_some()
                    || budget
                        .as_ref()
                        .and_then(|budget| budget.last_failure.as_ref())
                        .is_some() =>
            {
                "degraded"
            }
            Some(PoolMode::Running) | Some(PoolMode::Draining) => "idle",
        }
    };
    Ok(SupervisorRuntimeSnapshot {
        goal_id: goal_id.into(),
        state: state.into(),
        pool_mode: pool.map(|pool| pool.mode),
        active_run_id: active_run.map(|run| run.id.clone()),
        active_thread_id: active_run.and_then(|run| run.thread_id.clone()),
        current_trigger_key: active_run.and_then(|run| {
            run.prompt
                .strip_prefix(SUPERVISOR_RUN_PROMPT_PREFIX)
                .map(str::trim)
                .and_then(|value| {
                    value.strip_suffix("and return the required structured decision.")
                })
                .map(str::trim)
                .map(str::to_string)
        }),
        acknowledged_event_sequence: cursor
            .as_ref()
            .map(|cursor| cursor.acknowledged_sequence)
            .unwrap_or_default(),
        last_delivered_event_sequence: cursor
            .as_ref()
            .map(|cursor| cursor.last_delivered_sequence)
            .unwrap_or_default(),
        evaluation_count: budget
            .as_ref()
            .map(|budget| budget.evaluation_count)
            .unwrap_or_default(),
        consumed_tokens: budget
            .as_ref()
            .map(|budget| budget.consumed_tokens)
            .unwrap_or_default(),
        circuit_open_until: budget.as_ref().and_then(|budget| budget.circuit_open_until),
        pending_trigger_count,
        last_intervention_id: latest_intervention.map(|item| item.id.as_str().into()),
        last_intervention_level: latest_intervention.map(|item| item.level),
        last_intervention_state: latest_intervention.map(|item| item.state),
        last_intervention_summary: latest_intervention.map(|item| item.decision_summary.clone()),
        last_intervention_at: latest_intervention.map(|item| item.metadata.updated_at),
        last_failure: run_failure.or_else(|| {
            budget
                .as_ref()
                .and_then(|budget| budget.last_failure.clone())
        }),
    })
}

pub fn spawn_supervisor_runtime_monitor(
    tracker_root: Arc<RwLock<PathBuf>>,
    runner: Arc<RunManager>,
) {
    tokio::spawn(async move {
        let active = Arc::new(Mutex::new(HashSet::<String>::new()));
        loop {
            let root = tracker_root.read().await.clone();
            if let Ok(store) = SqliteCoordinationStore::open(root.join("coordination.sqlite")) {
                let store = Arc::new(store);
                let pools = store.goal_pools().unwrap_or_default();
                for pool in pools
                    .into_iter()
                    .filter(|pool| matches!(pool.mode, PoolMode::Running | PoolMode::Draining))
                {
                    let key = format!("{}::{}", root.display(), pool.goal_id);
                    let mut in_flight = active.lock().await;
                    if !in_flight.insert(key.clone()) {
                        continue;
                    }
                    drop(in_flight);
                    let active = Arc::clone(&active);
                    let runner = Arc::clone(&runner);
                    let store = Arc::clone(&store);
                    let tracker = Tracker::new(&root);
                    tokio::spawn(async move {
                        let evaluator: Arc<dyn SupervisorRuntimeEvaluator> = Arc::new(
                            CodexSupervisorEvaluator::new(Arc::clone(&runner), Arc::clone(&store)),
                        );
                        let service =
                            SupervisorRuntimeService::new(store, evaluator).with_tracker(tracker);
                        match service.process_goal_once(&pool.goal_id, Utc::now()).await {
                            Ok(report) => {
                                for error in report.failures {
                                    tracing::warn!(
                                        target: "golazo_backend::operation",
                                        operation = "supervisor.evaluation",
                                        goal_id = %pool.goal_id,
                                        outcome = "failed",
                                        error = %error,
                                        "Golazo Supervisor evaluation failed and remains replayable"
                                    );
                                }
                            }
                            Err(error) => tracing::warn!(
                                target: "golazo_backend::operation",
                                operation = "supervisor.runtime",
                                goal_id = %pool.goal_id,
                                outcome = "failed",
                                error = %error,
                                "Golazo Supervisor runtime iteration failed"
                            ),
                        }
                        active.lock().await.remove(&key);
                    });
                }
            }
            sleep(SUPERVISOR_MONITOR_INTERVAL).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::super::domain::{
        ClaimId, CoordinationEventPayload, Worker, WorkerId, WorkerPermissionProfile,
        WorkerStalledEventPayload, WorkerState,
    };
    use super::super::pool::GoalPoolState;
    use super::super::security::ResourceQuotaPolicy;
    use super::super::store::{NotificationRepository, WorkerRepository};
    use super::*;
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;

    #[derive(Clone)]
    struct ScriptedEvaluator {
        store: Arc<SqliteCoordinationStore>,
        level: InterventionLevel,
    }

    impl SupervisorRuntimeEvaluator for ScriptedEvaluator {
        fn evaluate<'a>(
            &'a self,
            trigger: &'a SupervisorTrigger,
            _budget: SupervisorEvaluationBudget,
        ) -> Pin<Box<dyn Future<Output = Result<SupervisorEvaluationResult, String>> + Send + 'a>>
        {
            Box::pin(scripted_result(
                Arc::clone(&self.store),
                self.level,
                trigger,
            ))
        }
    }

    async fn scripted_result(
        store: Arc<SqliteCoordinationStore>,
        level: InterventionLevel,
        trigger: &SupervisorTrigger,
    ) -> Result<SupervisorEvaluationResult, String> {
        let context = SupervisorContextBuilder::new(store)
            .build(&trigger.goal_id, trigger, Utc::now())
            .map_err(|error| error.to_string())?;
        let targets = if level == InterventionLevel::Observe {
            vec![]
        } else {
            trigger.affected_worker_ids.clone()
        };
        let decision = SupervisorDecision {
            schema_version: 1,
            trigger_key: trigger.key.clone(),
            decision: level,
            confidence_percent: 95,
            summary: "Scripted production Supervisor decision".into(),
            evidence_event_ids: if level == InterventionLevel::Observe {
                vec![]
            } else {
                trigger.evidence_event_ids.clone()
            },
            evidence_refs: vec![],
            target_worker_ids: targets,
            recommendation: if level == InterventionLevel::Observe {
                String::new()
            } else {
                "Review the affected scope".into()
            },
            requested_action: None,
            requires_human_decision: false,
            escalation: None,
        };
        Ok(SupervisorEvaluationResult {
            context,
            decision,
            usage: SupervisorEvaluationUsage {
                input_tokens: 10,
                output_tokens: 5,
            },
            run_id: "scripted-run".into(),
            thread_id: Some("scripted-thread".into()),
        })
    }

    #[derive(Clone)]
    struct FailingOnceEvaluator {
        store: Arc<SqliteCoordinationStore>,
        calls: Arc<AtomicUsize>,
    }

    impl SupervisorRuntimeEvaluator for FailingOnceEvaluator {
        fn evaluate<'a>(
            &'a self,
            trigger: &'a SupervisorTrigger,
            _budget: SupervisorEvaluationBudget,
        ) -> Pin<Box<dyn Future<Output = Result<SupervisorEvaluationResult, String>> + Send + 'a>>
        {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("provider temporarily unavailable".into());
                }
                scripted_result(Arc::clone(&self.store), InterventionLevel::Observe, trigger).await
            })
        }
    }

    fn configure_pool(
        store: &SqliteCoordinationStore,
        goal_id: &str,
        mode: PoolMode,
        now: DateTime<Utc>,
    ) {
        store
            .upsert_goal_pool(
                &GoalPoolState {
                    goal_id: goal_id.into(),
                    desired_concurrency: 1,
                    mode,
                    worker_permissions: WorkerPermissionProfile::default(),
                    resource_quotas: ResourceQuotaPolicy::default(),
                    started_at: Some(now),
                },
                now,
            )
            .unwrap();
    }

    fn append_stall_event(
        store: &SqliteCoordinationStore,
        goal_id: &str,
        worker_id: WorkerId,
        claim_id: ClaimId,
        now: DateTime<Utc>,
    ) {
        let source = CoordinationEvent::from_typed_payload(
            goal_id,
            EventSeverity::Warning,
            CoordinationActor::System,
            "signal-source",
            CoordinationEventPayload::WorkerStalled(WorkerStalledEventPayload {
                worker_id,
                claim_id,
                last_heartbeat_at: now - Duration::minutes(5),
                last_activity_at: Some(now - Duration::minutes(5)),
                detected_at: now,
                heartbeat_age_seconds: 300,
                activity_age_seconds: Some(300),
                heartbeat_threshold_seconds: 60,
                activity_threshold_seconds: 60,
            }),
            now,
        )
        .unwrap();
        store.append_event(&source).unwrap();
    }

    #[tokio::test]
    async fn live_runtime_persists_and_applies_an_observe_decision_once() {
        let directory = TempDir::new().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        configure_pool(&store, "goal-live-supervisor", PoolMode::Running, now);
        let worker_id = WorkerId::new();
        let claim_id = ClaimId::new();
        append_stall_event(&store, "goal-live-supervisor", worker_id, claim_id, now);
        let evaluator: Arc<dyn SupervisorRuntimeEvaluator> = Arc::new(ScriptedEvaluator {
            store: Arc::clone(&store),
            level: InterventionLevel::Observe,
        });
        let runtime = SupervisorRuntimeService::new(Arc::clone(&store), evaluator);
        let first = runtime
            .process_goal_once("goal-live-supervisor", now)
            .await
            .unwrap();
        assert_eq!(first.evaluated_trigger_keys.len(), 1);
        assert_eq!(first.intervention_ids.len(), 1);
        assert_eq!(first.created_signal_ids.len(), 1);
        let intervention = store
            .interventions_for_goal("goal-live-supervisor", None)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(intervention.state, InterventionState::Applied);
        let second = runtime
            .process_goal_once("goal-live-supervisor", now + Duration::seconds(1))
            .await
            .unwrap();
        assert!(second.evaluated_trigger_keys.is_empty());
        assert_eq!(second.existing_trigger_keys.len(), 1);
    }

    #[tokio::test]
    async fn lifecycle_pause_and_provider_failure_leave_the_trigger_for_safe_replay() {
        let directory = TempDir::new().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        configure_pool(&store, "goal-replay", PoolMode::Paused, now);
        append_stall_event(&store, "goal-replay", WorkerId::new(), ClaimId::new(), now);
        let calls = Arc::new(AtomicUsize::new(0));
        let evaluator: Arc<dyn SupervisorRuntimeEvaluator> = Arc::new(FailingOnceEvaluator {
            store: Arc::clone(&store),
            calls: Arc::clone(&calls),
        });
        let runtime = SupervisorRuntimeService::new(Arc::clone(&store), evaluator);

        let paused = runtime.process_goal_once("goal-replay", now).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(paused.cursor_after, 0);

        configure_pool(
            &store,
            "goal-replay",
            PoolMode::Running,
            now + Duration::seconds(1),
        );
        let failed = runtime
            .process_goal_once("goal-replay", now + Duration::seconds(1))
            .await
            .unwrap();
        assert_eq!(failed.failures.len(), 1);
        assert_eq!(failed.cursor_after, 0);
        assert!(
            store
                .supervisor_budget("goal-replay")
                .unwrap()
                .unwrap()
                .last_failure
                .as_deref()
                .is_some_and(|message| message.contains("provider temporarily unavailable"))
        );

        let recovered = runtime
            .process_goal_once("goal-replay", now + Duration::seconds(2))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(recovered.intervention_ids.len(), 1);
        assert!(recovered.cursor_after > 0);
        assert!(
            store
                .supervisor_budget("goal-replay")
                .unwrap()
                .unwrap()
                .last_failure
                .is_none()
        );
    }

    #[tokio::test]
    async fn targeted_live_decision_queues_one_durable_worker_notification() {
        let directory = TempDir::new().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        configure_pool(&store, "goal-targeted", PoolMode::Running, now);
        let mut worker = Worker::new("goal-targeted", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        store.upsert_worker(&worker).unwrap();
        append_stall_event(
            &store,
            "goal-targeted",
            worker.id.clone(),
            ClaimId::new(),
            now,
        );
        let evaluator: Arc<dyn SupervisorRuntimeEvaluator> = Arc::new(ScriptedEvaluator {
            store: Arc::clone(&store),
            level: InterventionLevel::Recommend,
        });
        let runtime = SupervisorRuntimeService::new(Arc::clone(&store), evaluator);
        let report = runtime
            .process_goal_once("goal-targeted", now)
            .await
            .unwrap();
        assert_eq!(report.intervention_ids.len(), 1);
        let notifications = store.notifications_for_worker(&worker.id, None).unwrap();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].purpose, "supervisor_recommend");
        assert_eq!(notifications[0].target_worker, worker.id);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn production_evaluator_launches_read_only_codex_turn_and_applies_decision() {
        let directory = TempDir::new().unwrap();
        let workspace = directory.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let goal_id = "goal-production-supervisor";
        configure_pool(&store, goal_id, PoolMode::Running, now);
        let mut worker = Worker::new(goal_id, now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        store.upsert_worker(&worker).unwrap();
        append_stall_event(&store, goal_id, worker.id.clone(), ClaimId::new(), now);

        let replay = store
            .replay_events_for_consumer(SUPERVISOR_EVENT_CONSUMER, goal_id, 1_000, now)
            .unwrap();
        SignalProcessor::new(Arc::clone(&store), SignalProcessingPolicy::default())
            .unwrap()
            .process(goal_id, &replay.events, now)
            .unwrap();
        let trigger =
            SupervisorTriggerService::new(Arc::clone(&store), SupervisorTriggerPolicy::default())
                .unwrap()
                .eligible_triggers(goal_id, 0, now)
                .unwrap()
                .triggers
                .into_iter()
                .next()
                .expect("worker event should produce a Supervisor trigger");
        let decision = SupervisorDecision {
            schema_version: 1,
            trigger_key: trigger.key.clone(),
            decision: InterventionLevel::Recommend,
            confidence_percent: 93,
            summary: "The affected worker should inspect its stalled claim before continuing"
                .into(),
            evidence_event_ids: trigger.evidence_event_ids.clone(),
            evidence_refs: vec![],
            target_worker_ids: trigger.affected_worker_ids.clone(),
            recommendation: "Inspect the claim and report whether recovery is safe".into(),
            requested_action: None,
            requires_human_decision: false,
            escalation: None,
        };
        let decision_json = serde_json::to_string(&decision).unwrap();
        let message_started = json!({
            "method": "item/started",
            "params": {
                "threadId": "supervisor-thread",
                "turnId": "supervisor-turn",
                "item": {"id": "decision-message", "type": "agentMessage", "phase": "final_answer"}
            }
        })
        .to_string();
        let message_delta = json!({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": "supervisor-thread",
                "turnId": "supervisor-turn",
                "itemId": "decision-message",
                "delta": decision_json
            }
        })
        .to_string();
        let usage = json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "threadId": "supervisor-thread",
                "turnId": "supervisor-turn",
                "tokenUsage": {"last": {
                    "inputTokens": 17,
                    "cachedInputTokens": 3,
                    "outputTokens": 5,
                    "reasoningOutputTokens": 2,
                    "totalTokens": 22
                }}
            }
        })
        .to_string();
        let completed = json!({
            "method": "turn/completed",
            "params": {
                "threadId": "supervisor-thread",
                "turnId": "supervisor-turn",
                "turn": {"status": "completed"}
            }
        })
        .to_string();
        let executable = directory.path().join("fake-codex");
        let thread_capture = directory.path().join("thread-request.json");
        let turn_capture = directory.path().join("turn-request.json");
        fs::write(
            &executable,
            format!(
                r#"#!/bin/sh
IFS= read -r line
printf '%s\n' '{{"id":1,"result":{{}}}}'
IFS= read -r line
IFS= read -r line
printf '%s\n' "$line" > '{}'
printf '%s\n' '{{"id":2,"result":{{"thread":{{"id":"supervisor-thread"}}}}}}'
IFS= read -r line
printf '%s\n' "$line" > '{}'
printf '%s\n' '{{"id":3,"result":{{"turn":{{"id":"supervisor-turn"}}}}}}'
printf '%s\n' '{}'
printf '%s\n' '{}'
printf '%s\n' '{}'
printf '%s\n' '{}'
"#,
                thread_capture.display(),
                turn_capture.display(),
                message_started,
                message_delta,
                usage,
                completed,
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let runner = RunManager::new(
            workspace,
            executable.to_string_lossy().into(),
            directory.path().join("runs.json"),
        )
        .await;
        let evaluator: Arc<dyn SupervisorRuntimeEvaluator> = Arc::new(
            CodexSupervisorEvaluator::new(Arc::clone(&runner), Arc::clone(&store)),
        );
        let runtime = SupervisorRuntimeService::new(Arc::clone(&store), evaluator);

        let report = runtime.process_goal_once(goal_id, now).await.unwrap();

        assert_eq!(report.intervention_ids.len(), 1);
        assert!(report.failures.is_empty());
        assert!(report.cursor_after > 0);
        let intervention = store
            .interventions_for_goal(goal_id, None)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(intervention.level, InterventionLevel::Recommend);
        assert_eq!(intervention.state, InterventionState::Delivered);
        assert_eq!(intervention.target_workers, vec![worker.id.clone()]);
        let notifications = store.notifications_for_worker(&worker.id, None).unwrap();
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].purpose, "supervisor_recommend");
        let budget = store.supervisor_budget(goal_id).unwrap().unwrap();
        assert_eq!(budget.consumed_tokens, 22);

        let runs = runner.list().await;
        let run = runs
            .iter()
            .find(|run| run.prompt.starts_with(SUPERVISOR_RUN_PROMPT_PREFIX))
            .unwrap();
        assert_eq!(run.status, "completed");
        assert_eq!(run.sandbox, "read-only");
        assert_eq!(run.approval_policy, "on-request");
        assert!(run.ephemeral_thread);
        assert_eq!(run.work_mode, WorkMode::Spec);
        assert_eq!(run.goal_id.as_deref(), Some(goal_id));
        assert_eq!(
            run.output_schema.as_ref(),
            Some(&supervisor_decision_schema())
        );
        assert!(
            run.execution_prompt
                .as_deref()
                .is_some_and(|prompt| prompt.contains("read-only Supervisor evaluation"))
        );

        let thread_request: Value =
            serde_json::from_str(&fs::read_to_string(thread_capture).unwrap()).unwrap();
        assert_eq!(thread_request["method"], "thread/start");
        assert_eq!(thread_request["params"]["sandbox"], "read-only");
        assert_eq!(thread_request["params"]["approvalPolicy"], "on-request");
        assert_eq!(thread_request["params"]["ephemeral"], true);
        let turn_request: Value =
            serde_json::from_str(&fs::read_to_string(turn_capture).unwrap()).unwrap();
        assert_eq!(turn_request["method"], "turn/start");
        assert_eq!(
            turn_request["params"]["outputSchema"],
            supervisor_decision_schema()
        );
    }

    #[test]
    fn decision_schema_requires_the_complete_structured_contract() {
        let schema = supervisor_decision_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("escalation"))
        );
        assert_eq!(
            schema["properties"]["decision"]["enum"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
    }
}
