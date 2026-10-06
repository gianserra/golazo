use super::domain::{
    ClaimState, EscalationSeverity, EscalationState, IntegrationJobState, InterventionState,
    NotificationState, WorkerState,
};
use super::pool::PoolMode;
use super::store::{
    ClaimRepository, EscalationRepository, EventRepository, IntegrationArtifactRepository,
    IntegrationJobRepository, InterventionRepository, NotificationRepository, PoolRepository,
    SqliteCoordinationStore, StoreError, WorkerRepository,
};
use crate::tracker::{Tracker, TrackerError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

pub const GOAL_HEALTH_SCHEMA: &str = "golazo.goal-health.v1";

#[derive(Debug, Error)]
pub enum HealthError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Tracker(#[from] TrackerError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Unknown,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalOperatingMode {
    Normal,
    Idle,
    Paused,
    Draining,
    Degraded,
    RecoveryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshnessStatus {
    Fresh,
    Stale,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthPolicy {
    pub worker_heartbeat_stale_after_seconds: u64,
    pub runtime_binding_stale_after_seconds: u64,
    pub notification_stale_after_seconds: u64,
    pub integration_stale_after_seconds: u64,
    pub supervisor_stale_after_seconds: u64,
    pub goal_data_stale_after_seconds: u64,
    pub lease_warning_seconds: u64,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            worker_heartbeat_stale_after_seconds: 120,
            runtime_binding_stale_after_seconds: 120,
            notification_stale_after_seconds: 5 * 60,
            integration_stale_after_seconds: 15 * 60,
            supervisor_stale_after_seconds: 5 * 60,
            goal_data_stale_after_seconds: 5 * 60,
            lease_warning_seconds: 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthDiagnostic {
    pub code: String,
    pub severity: HealthStatus,
    pub component: String,
    pub summary: String,
    pub recommended_action: String,
    pub entity_kind: Option<String>,
    pub entity_id: Option<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ComponentHealth {
    pub component: String,
    pub status: HealthStatus,
    pub mode: String,
    pub stale: bool,
    pub summary: String,
    pub diagnostics: Vec<HealthDiagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalDataFreshness {
    pub status: FreshnessStatus,
    pub latest_record_at: Option<DateTime<Utc>>,
    pub age_seconds: Option<u64>,
    pub stale_after_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalHealthSnapshot {
    pub schema: String,
    pub goal_id: String,
    pub status: HealthStatus,
    pub operating_mode: GoalOperatingMode,
    pub collected_at: DateTime<Utc>,
    pub freshness: GoalDataFreshness,
    pub components: Vec<ComponentHealth>,
    pub diagnostics: Vec<HealthDiagnostic>,
}

pub struct GoalHealthService {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    policy: HealthPolicy,
}

impl GoalHealthService {
    pub fn new(
        tracker: Tracker,
        store: Arc<SqliteCoordinationStore>,
        policy: HealthPolicy,
    ) -> Self {
        Self {
            tracker,
            store,
            policy,
        }
    }

    pub fn collect(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<GoalHealthSnapshot, HealthError> {
        let goal = self.tracker.get_goal(goal_id)?;
        self.store.integrity_check()?;
        let pool = self.store.goal_pool(goal_id)?;
        let workers = self.store.workers_for_goal(goal_id, None)?;
        let claims = self.store.claims_for_goal(goal_id, None)?;
        let notifications = self.store.notifications_for_goal(goal_id, None)?;
        let jobs = self.store.integration_jobs_for_goal(goal_id, None)?;
        let interventions = self.store.interventions_for_goal(goal_id, None)?;
        let escalations = self.store.escalations_for_goal(goal_id, None)?;
        let latest_event = self
            .store
            .latest_events_for_goal(goal_id, None, 1)?
            .into_iter()
            .next();

        let mut components = vec![
            ComponentHealth {
                component: "tracker".into(),
                status: HealthStatus::Healthy,
                mode: "authoritative".into(),
                stale: false,
                summary: "The implementation tracker is readable.".into(),
                diagnostics: vec![],
            },
            ComponentHealth {
                component: "coordination_store".into(),
                status: HealthStatus::Healthy,
                mode: "read_write".into(),
                stale: false,
                summary: "The coordination store passed its integrity check.".into(),
                diagnostics: vec![],
            },
        ];

        let pool_mode = pool.as_ref().map(|pool| pool.mode);
        components.push(ComponentHealth {
            component: "worker_pool".into(),
            status: HealthStatus::Healthy,
            mode: pool
                .as_ref()
                .map(|pool| enum_name(pool.mode))
                .unwrap_or_else(|| "not_configured".into()),
            stale: false,
            summary: match &pool {
                Some(pool) => format!(
                    "Worker pool is {:?} with desired concurrency {}.",
                    pool.mode, pool.desired_concurrency
                ),
                None => "Worker pool is not configured; the goal is idle.".into(),
            },
            diagnostics: vec![],
        });

        let live_workers = workers
            .iter()
            .filter(|worker| is_live_worker(worker.state))
            .collect::<Vec<_>>();
        let mut worker_diagnostics = Vec::new();
        let mut runtime_diagnostics = Vec::new();
        let mut workspace_diagnostics = Vec::new();
        for worker in &live_workers {
            let heartbeat_at = worker
                .last_heartbeat_at
                .unwrap_or(worker.metadata.updated_at);
            if requires_heartbeat(worker.state)
                && age_seconds(heartbeat_at, now) > self.policy.worker_heartbeat_stale_after_seconds
            {
                worker_diagnostics.push(diagnostic(
                    "worker.heartbeat_stale",
                    HealthStatus::Unhealthy,
                    "workers",
                    format!("Worker {} has a stale heartbeat.", worker.id.as_str()),
                    "Run worker recovery; inspect its claim, run, thread, and workspace before replacement.",
                    Some("worker"),
                    Some(worker.id.as_str()),
                    now,
                ));
            }
            if worker.state == WorkerState::Recovering {
                worker_diagnostics.push(diagnostic(
                    "worker.recovering",
                    HealthStatus::Degraded,
                    "workers",
                    format!("Worker {} is recovering.", worker.id.as_str()),
                    "Complete reconciliation or quarantine the worker before assigning more work.",
                    Some("worker"),
                    Some(worker.id.as_str()),
                    now,
                ));
            }
            let runtime_age = age_seconds(worker.metadata.updated_at, now);
            if worker.state == WorkerState::Active
                && runtime_age > self.policy.runtime_binding_stale_after_seconds
                && (worker.current_run_id.is_none() || worker.current_thread_id.is_none())
            {
                runtime_diagnostics.push(diagnostic(
                    "runtime.binding_missing",
                    HealthStatus::Unhealthy,
                    "runtime_bindings",
                    format!(
                        "Active worker {} is missing a current run or thread binding.",
                        worker.id.as_str()
                    ),
                    "Reconcile Codex run and thread inventory; recover or quarantine the worker if no live runtime exists.",
                    Some("worker"),
                    Some(worker.id.as_str()),
                    now,
                ));
            }
            match &worker.workspace {
                None => workspace_diagnostics.push(diagnostic(
                    "workspace.binding_missing",
                    HealthStatus::Unhealthy,
                    "workspaces",
                    format!("Live worker {} has no workspace binding.", worker.id.as_str()),
                    "Provision an isolated workspace or quarantine the worker before execution continues.",
                    Some("worker"),
                    Some(worker.id.as_str()),
                    now,
                )),
                Some(binding) if !Path::new(&binding.worktree_path).exists() => {
                    workspace_diagnostics.push(diagnostic(
                        "workspace.path_missing",
                        HealthStatus::Unhealthy,
                        "workspaces",
                        format!(
                            "Worker {} workspace path is missing.",
                            worker.id.as_str()
                        ),
                        "Reconcile the worktree inventory and recover from the preserved branch or quarantine record.",
                        Some("workspace"),
                        Some(&binding.worktree_path),
                        now,
                    ));
                }
                Some(_) => {}
            }
        }
        components.push(component(
            "workers",
            if live_workers.is_empty() {
                "idle"
            } else {
                "active"
            },
            format!("{} live worker(s) are registered.", live_workers.len()),
            worker_diagnostics,
        ));
        components.push(component(
            "runtime_bindings",
            if live_workers.is_empty() {
                "idle"
            } else {
                "bound"
            },
            "Durable run and thread bindings were checked for live workers.",
            runtime_diagnostics,
        ));
        components.push(component(
            "workspaces",
            if live_workers.is_empty() {
                "idle"
            } else {
                "isolated"
            },
            "Workspace bindings and paths were checked for live workers.",
            workspace_diagnostics,
        ));

        let mut claim_diagnostics = Vec::new();
        for claim in claims
            .iter()
            .filter(|claim| claim.state == ClaimState::Active)
        {
            let remaining = (claim.lease_expires_at - now).num_seconds();
            if remaining < 0 {
                claim_diagnostics.push(diagnostic(
                    "claim.lease_expired",
                    HealthStatus::Unhealthy,
                    "claims",
                    format!("Claim {} has an expired active lease.", claim.id.as_str()),
                    "Expire and safely reclaim the claim after reconciling its worker and workspace.",
                    Some("claim"),
                    Some(claim.id.as_str()),
                    now,
                ));
            } else if remaining <= self.policy.lease_warning_seconds as i64 {
                claim_diagnostics.push(diagnostic(
                    "claim.lease_expiring",
                    HealthStatus::Degraded,
                    "claims",
                    format!("Claim {} lease is nearing expiry.", claim.id.as_str()),
                    "Confirm worker liveness and renew the lease or preserve partial progress for reclaim.",
                    Some("claim"),
                    Some(claim.id.as_str()),
                    now,
                ));
            }
        }
        components.push(component(
            "claims",
            "leased",
            format!(
                "{} active claim(s) were checked for lease pressure.",
                claims
                    .iter()
                    .filter(|claim| claim.state == ClaimState::Active)
                    .count()
            ),
            claim_diagnostics,
        ));

        let mut supervisor_diagnostics = Vec::new();
        for escalation in escalations.iter().filter(|escalation| {
            matches!(
                escalation.state,
                EscalationState::Open | EscalationState::Acknowledged
            )
        }) {
            let severity = if escalation.severity == EscalationSeverity::Critical {
                HealthStatus::Unhealthy
            } else {
                HealthStatus::Degraded
            };
            supervisor_diagnostics.push(diagnostic(
                "supervisor.escalation_unresolved",
                severity,
                "supervisor",
                format!("Escalation {} remains unresolved.", escalation.id.as_str()),
                "Review current evidence and record an attributed decision or accepted-risk override.",
                Some("escalation"),
                Some(escalation.id.as_str()),
                now,
            ));
        }
        for intervention in interventions.iter().filter(|intervention| {
            matches!(
                intervention.state,
                InterventionState::Proposed | InterventionState::Delivered
            ) && age_seconds(intervention.metadata.updated_at, now)
                > self.policy.supervisor_stale_after_seconds
        }) {
            supervisor_diagnostics.push(diagnostic(
                "supervisor.intervention_stale",
                HealthStatus::Degraded,
                "supervisor",
                format!(
                    "Intervention {} has not been resolved.",
                    intervention.id.as_str()
                ),
                "Acknowledge, apply, dismiss, or supersede the intervention with current evidence.",
                Some("intervention"),
                Some(intervention.id.as_str()),
                now,
            ));
        }
        components.push(component(
            "supervisor",
            "exception_driven",
            format!(
                "{} intervention(s) and {} escalation(s) are durable.",
                interventions.len(),
                escalations.len()
            ),
            supervisor_diagnostics,
        ));

        let mut notification_diagnostics = Vec::new();
        for notification in &notifications {
            if notification.state == NotificationState::Failed {
                notification_diagnostics.push(diagnostic(
                    "notification.delivery_failed",
                    HealthStatus::Degraded,
                    "notifications",
                    format!("Notification {} failed delivery.", notification.id.as_str()),
                    "Retry delivery or supersede the notification after confirming the target worker state.",
                    Some("notification"),
                    Some(notification.id.as_str()),
                    now,
                ));
            } else if notification.state == NotificationState::Queued
                && age_seconds(notification.metadata.created_at, now)
                    > self.policy.notification_stale_after_seconds
            {
                notification_diagnostics.push(diagnostic(
                    "notification.queue_stale",
                    HealthStatus::Degraded,
                    "notifications",
                    format!(
                        "Notification {} is stale in the queue.",
                        notification.id.as_str()
                    ),
                    "Inspect notification delivery and the target worker runtime before retrying.",
                    Some("notification"),
                    Some(notification.id.as_str()),
                    now,
                ));
            }
        }
        components.push(component(
            "notifications",
            "durable_queue",
            format!("{} notification(s) were checked.", notifications.len()),
            notification_diagnostics,
        ));

        let succeeded_artifacts = jobs
            .iter()
            .filter(|job| job.state == IntegrationJobState::Succeeded)
            .map(|job| job.artifact_id.as_str())
            .collect::<BTreeSet<_>>();
        let mut integration_diagnostics = Vec::new();
        for job in &jobs {
            let age = age_seconds(job.started_at.unwrap_or(job.enqueued_at), now);
            if job.state == IntegrationJobState::Failed
                && !succeeded_artifacts.contains(job.artifact_id.as_str())
            {
                integration_diagnostics.push(diagnostic(
                    "integration.job_failed",
                    HealthStatus::Unhealthy,
                    "integration",
                    format!("Integration job {} failed without a successful retry.", job.id.as_str()),
                    "Inspect validation and reconciliation evidence, then retry or escalate the integration lane.",
                    Some("integration_job"),
                    Some(job.id.as_str()),
                    now,
                ));
            } else if matches!(
                job.state,
                IntegrationJobState::Queued | IntegrationJobState::Running
            ) && age > self.policy.integration_stale_after_seconds
            {
                integration_diagnostics.push(diagnostic(
                    "integration.job_stale",
                    HealthStatus::Unhealthy,
                    "integration",
                    format!("Integration job {} is stuck in {:?}.", job.id.as_str(), job.state),
                    "Reconcile the integration lane, preserve the artifact, and retry or escalate with evidence.",
                    Some("integration_job"),
                    Some(job.id.as_str()),
                    now,
                ));
            }
        }
        components.push(component(
            "integration",
            "deterministic_queue",
            format!("{} integration job(s) were checked.", jobs.len()),
            integration_diagnostics,
        ));

        let latest_record_at = std::iter::once(
            goal.get("updated_at")
                .and_then(serde_json::Value::as_str)
                .and_then(parse_time),
        )
        .chain(
            workers
                .iter()
                .map(|record| Some(record.metadata.updated_at)),
        )
        .chain(claims.iter().map(|record| Some(record.metadata.updated_at)))
        .chain(
            notifications
                .iter()
                .map(|record| Some(record.metadata.updated_at)),
        )
        .chain(jobs.iter().map(|record| Some(record.metadata.updated_at)))
        .chain(
            interventions
                .iter()
                .map(|record| Some(record.metadata.updated_at)),
        )
        .chain(
            escalations
                .iter()
                .map(|record| Some(record.metadata.updated_at)),
        )
        .chain(latest_event.iter().map(|event| Some(event.occurred_at)))
        .flatten()
        .max();
        let active_mode = pool_mode == Some(PoolMode::Running) && !live_workers.is_empty();
        let age = latest_record_at.map(|at| age_seconds(at, now));
        let freshness_status = if !active_mode {
            FreshnessStatus::NotApplicable
        } else if age.is_some_and(|age| age > self.policy.goal_data_stale_after_seconds) {
            FreshnessStatus::Stale
        } else {
            FreshnessStatus::Fresh
        };
        if freshness_status == FreshnessStatus::Stale {
            components.push(component(
                "coordination_data",
                "stale",
                "The running goal has not produced fresh durable state within the configured threshold.",
                vec![diagnostic(
                    "coordination.data_stale",
                    HealthStatus::Unhealthy,
                    "coordination_data",
                    "Durable goal activity is stale while the worker pool is running.",
                    "Inspect backend, worker runtimes, event publication, and store writes; run startup reconciliation before resuming.",
                    Some("goal"),
                    Some(goal_id),
                    now,
                )],
            ));
        } else {
            components.push(ComponentHealth {
                component: "coordination_data".into(),
                status: HealthStatus::Healthy,
                mode: enum_name(freshness_status),
                stale: false,
                summary: if active_mode {
                    "Durable goal activity is fresh.".into()
                } else {
                    "Freshness is not applicable while the goal is idle.".into()
                },
                diagnostics: vec![],
            });
        }

        let diagnostics = components
            .iter()
            .flat_map(|component| component.diagnostics.iter().cloned())
            .collect::<Vec<_>>();
        let status = components
            .iter()
            .map(|component| component.status)
            .max()
            .unwrap_or(HealthStatus::Unknown);
        let operating_mode = operating_mode(pool_mode, status);

        Ok(GoalHealthSnapshot {
            schema: GOAL_HEALTH_SCHEMA.into(),
            goal_id: goal_id.into(),
            status,
            operating_mode,
            collected_at: now,
            freshness: GoalDataFreshness {
                status: freshness_status,
                latest_record_at,
                age_seconds: age,
                stale_after_seconds: self.policy.goal_data_stale_after_seconds,
            },
            components,
            diagnostics,
        })
    }
}

fn component(
    name: &str,
    mode: &str,
    summary: impl Into<String>,
    diagnostics: Vec<HealthDiagnostic>,
) -> ComponentHealth {
    let status = diagnostics
        .iter()
        .map(|diagnostic| diagnostic.severity)
        .max()
        .unwrap_or(HealthStatus::Healthy);
    ComponentHealth {
        component: name.into(),
        status,
        mode: mode.into(),
        stale: diagnostics.iter().any(|diagnostic| {
            diagnostic.code.contains("stale") || diagnostic.code.contains("expired")
        }),
        summary: summary.into(),
        diagnostics,
    }
}

#[allow(clippy::too_many_arguments)]
fn diagnostic(
    code: &str,
    severity: HealthStatus,
    component: &str,
    summary: impl Into<String>,
    recommended_action: &str,
    entity_kind: Option<&str>,
    entity_id: Option<&str>,
    observed_at: DateTime<Utc>,
) -> HealthDiagnostic {
    HealthDiagnostic {
        code: code.into(),
        severity,
        component: component.into(),
        summary: summary.into(),
        recommended_action: recommended_action.into(),
        entity_kind: entity_kind.map(str::to_string),
        entity_id: entity_id.map(str::to_string),
        observed_at,
    }
}

fn operating_mode(pool_mode: Option<PoolMode>, status: HealthStatus) -> GoalOperatingMode {
    match status {
        HealthStatus::Unhealthy => GoalOperatingMode::RecoveryRequired,
        HealthStatus::Degraded => GoalOperatingMode::Degraded,
        HealthStatus::Healthy | HealthStatus::Unknown => match pool_mode {
            Some(PoolMode::Running) => GoalOperatingMode::Normal,
            Some(PoolMode::Paused) => GoalOperatingMode::Paused,
            Some(PoolMode::Draining) => GoalOperatingMode::Draining,
            Some(PoolMode::Stopped) | None => GoalOperatingMode::Idle,
        },
    }
}

fn is_live_worker(state: WorkerState) -> bool {
    matches!(
        state,
        WorkerState::Created
            | WorkerState::Starting
            | WorkerState::Active
            | WorkerState::Waiting
            | WorkerState::Paused
            | WorkerState::Blocked
            | WorkerState::Recovering
    )
}

fn requires_heartbeat(state: WorkerState) -> bool {
    matches!(
        state,
        WorkerState::Starting
            | WorkerState::Active
            | WorkerState::Waiting
            | WorkerState::Recovering
    )
}

fn age_seconds(start: DateTime<Utc>, end: DateTime<Utc>) -> u64 {
    (end - start).num_seconds().max(0) as u64
}

fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn enum_name(value: impl std::fmt::Debug) -> String {
    format!("{value:?}").to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        Claim, ClaimScope, IntegrationArtifact, IntegrationArtifactId, IntegrationDiffSummary,
        IntegrationWorkspaceSnapshot, RecordMetadata, Worker,
    };
    use crate::coordination::integration::IntegrationQueueService;
    use crate::coordination::pool::GoalPoolState;
    use crate::models::Status;
    use chrono::Duration;

    #[test]
    fn exposes_degraded_modes_stale_data_and_actionable_diagnostics() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "health").unwrap();
        tracker
            .add_feature(
                "goal-a",
                "feature-a",
                "Feature A",
                "health",
                Status::Planned,
            )
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let goal_updated_at = parse_time(
            tracker
                .get_goal("goal-a")
                .unwrap()
                .get("updated_at")
                .and_then(serde_json::Value::as_str)
                .unwrap(),
        )
        .unwrap();
        let now = goal_updated_at + Duration::minutes(30);
        let stale_at = now - Duration::minutes(30);
        store
            .upsert_goal_pool(
                &GoalPoolState {
                    goal_id: "goal-a".into(),
                    desired_concurrency: 1,
                    mode: PoolMode::Running,
                    worker_permissions: Default::default(),
                    resource_quotas: Default::default(),
                    started_at: Some(stale_at),
                },
                stale_at,
            )
            .unwrap();
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
                feature_id: "feature-a".into(),
            },
            worker.id.clone(),
            "base",
            stale_at,
            now - Duration::minutes(10),
        )
        .unwrap();
        store.insert_claim(&claim).unwrap();

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
        queue.enqueue(&artifact.id, 0, stale_at).unwrap();

        let snapshot = GoalHealthService::new(tracker, store, HealthPolicy::default())
            .collect("goal-a", now)
            .unwrap();
        assert_eq!(snapshot.schema, GOAL_HEALTH_SCHEMA);
        assert_eq!(snapshot.status, HealthStatus::Unhealthy);
        assert_eq!(snapshot.operating_mode, GoalOperatingMode::RecoveryRequired);
        assert_eq!(snapshot.freshness.status, FreshnessStatus::Stale);
        for code in [
            "worker.heartbeat_stale",
            "runtime.binding_missing",
            "workspace.binding_missing",
            "claim.lease_expired",
            "integration.job_stale",
            "coordination.data_stale",
        ] {
            let diagnostic = snapshot
                .diagnostics
                .iter()
                .find(|diagnostic| diagnostic.code == code)
                .unwrap_or_else(|| panic!("missing {code}"));
            assert!(!diagnostic.recommended_action.is_empty());
        }
        assert_eq!(snapshot.components.len(), 11);
    }
}
