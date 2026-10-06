use super::claims::{ClaimService, ClaimServiceError};
use super::domain::{
    Claim, ClaimScope, DomainError, Worker, WorkerId, WorkerPermissionDerivation,
    WorkerPermissionPolicy, WorkerPermissionProfile, WorkerState, WorkerToolCapability,
    WorkerTurnBinding, WorkspaceBinding,
};
use super::security::{ResourceQuotaPolicy, validate_resource_quota_policy};
use super::store::{
    ClaimRepository, PoolRepository, SqliteCoordinationStore, StoreError, WorkerRepository,
};
use super::workspace::{WorkspaceError, WorkspaceIsolationCapability, WorktreeManager};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolMode {
    Running,
    Paused,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerPoolPolicy {
    pub default_concurrency: usize,
    pub max_per_goal: usize,
    pub global_max: usize,
}

impl Default for WorkerPoolPolicy {
    fn default() -> Self {
        Self {
            default_concurrency: 2,
            max_per_goal: 4,
            global_max: 8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalPoolState {
    pub goal_id: String,
    pub desired_concurrency: usize,
    pub mode: PoolMode,
    #[serde(default)]
    pub worker_permissions: WorkerPermissionProfile,
    #[serde(default)]
    pub resource_quotas: ResourceQuotaPolicy,
    #[serde(default)]
    pub started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolSnapshot {
    pub goal: GoalPoolState,
    pub workers: Vec<Worker>,
    pub active_workers: usize,
    pub available_global_slots: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StopReport {
    pub snapshot: PoolSnapshot,
    pub cancelled_worker_ids: Vec<WorkerId>,
    pub preserved_worker_ids: Vec<WorkerId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerFailureKind {
    Retryable,
    Permanent,
    WorkspaceUncertain,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryEvidence {
    pub claim_recovery_complete: bool,
    pub workspace_recovery_complete: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeInventory {
    pub live_run_ids: HashSet<String>,
    pub live_thread_ids: HashSet<String>,
    pub existing_workspace_paths: HashSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolRecoveryReport {
    pub recovered_goal_ids: Vec<String>,
    pub quarantined_worker_ids: Vec<WorkerId>,
    pub removed_stale_claim_bindings: usize,
}

#[derive(Debug, Error)]
pub enum WorkerPoolError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Claim(#[from] ClaimServiceError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error("worker pool state lock was poisoned")]
    LockPoisoned,
    #[error("goal pool is not configured: {0}")]
    GoalNotConfigured(String),
    #[error("worker was not found: {0}")]
    WorkerNotFound(String),
    #[error("worker {0} already has an active run")]
    ActiveRunExists(String),
    #[error("worker {0} has no active run")]
    NoActiveRun(String),
    #[error("worker {0} already has a different workspace binding")]
    WorkspaceAlreadyBound(String),
    #[error("worker permission profile is invalid: {0}")]
    InvalidPermissionProfile(String),
    #[error("worker permissions cannot change while worker {0} has an active run")]
    PermissionChangeDuringActiveRun(String),
    #[error("worker resource quotas cannot change while worker {0} has an active run")]
    ResourceQuotaChangeDuringActiveRun(String),
    #[error("worker resource quota policy is invalid: {0}")]
    InvalidResourceQuotaPolicy(String),
    #[error("workspace isolation does not allow requested concurrency: {0}")]
    IsolationUnavailable(String),
    #[error("worker {0} cannot be replaced until claim and workspace recovery are complete")]
    RecoveryIncomplete(String),
    #[error("desired concurrency {desired} exceeds the per-goal maximum {maximum}")]
    InvalidConcurrency { desired: usize, maximum: usize },
}

#[derive(Debug)]
pub struct WorkerPoolService {
    store: Arc<SqliteCoordinationStore>,
    policy: WorkerPoolPolicy,
    goals: Mutex<HashMap<String, GoalPoolState>>,
}

impl WorkerPoolService {
    pub fn new(store: Arc<SqliteCoordinationStore>, policy: WorkerPoolPolicy) -> Self {
        Self {
            store,
            policy,
            goals: Mutex::new(HashMap::new()),
        }
    }

    pub fn load(
        store: Arc<SqliteCoordinationStore>,
        policy: WorkerPoolPolicy,
    ) -> Result<Self, WorkerPoolError> {
        let states = store.goal_pools()?;
        for state in &states {
            if state.desired_concurrency > policy.max_per_goal {
                return Err(WorkerPoolError::InvalidConcurrency {
                    desired: state.desired_concurrency,
                    maximum: policy.max_per_goal,
                });
            }
            validate_resource_quota_policy(&state.resource_quotas)
                .map_err(WorkerPoolError::InvalidResourceQuotaPolicy)?;
            if state.desired_concurrency > state.resource_quotas.max_goal_concurrency {
                return Err(WorkerPoolError::InvalidConcurrency {
                    desired: state.desired_concurrency,
                    maximum: state.resource_quotas.max_goal_concurrency,
                });
            }
        }
        Ok(Self {
            store,
            policy,
            goals: Mutex::new(
                states
                    .into_iter()
                    .map(|state| (state.goal_id.clone(), state))
                    .collect(),
            ),
        })
    }

    pub fn recover(
        store: Arc<SqliteCoordinationStore>,
        policy: WorkerPoolPolicy,
        inventory: &RuntimeInventory,
        now: DateTime<Utc>,
    ) -> Result<(Self, PoolRecoveryReport), WorkerPoolError> {
        let service = Self::load(store, policy)?;
        let states = service.goals()?.values().cloned().collect::<Vec<_>>();
        let mut report = PoolRecoveryReport {
            recovered_goal_ids: states.iter().map(|state| state.goal_id.clone()).collect(),
            quarantined_worker_ids: Vec::new(),
            removed_stale_claim_bindings: 0,
        };
        for state in states {
            for mut worker in service.store.workers_for_goal(&state.goal_id, None)? {
                if worker.state.is_terminal() {
                    continue;
                }
                let original_claim_count = worker.active_claims.len();
                worker.active_claims.retain(|claim_id| {
                    matches!(
                        service.store.claim(claim_id),
                        Ok(Some(claim))
                            if claim.state == super::domain::ClaimState::Active
                                && claim.owner == worker.id
                    )
                });
                report.removed_stale_claim_bindings +=
                    original_claim_count.saturating_sub(worker.active_claims.len());
                let run_missing = worker
                    .current_run_id
                    .as_ref()
                    .is_some_and(|id| !inventory.live_run_ids.contains(id));
                let thread_missing = worker
                    .current_thread_id
                    .as_ref()
                    .is_some_and(|id| !inventory.live_thread_ids.contains(id));
                let workspace_missing = worker.workspace.as_ref().is_some_and(|workspace| {
                    !inventory
                        .existing_workspace_paths
                        .contains(&workspace.worktree_path)
                });
                let claim_mismatch = original_claim_count != worker.active_claims.len();
                if run_missing || thread_missing || workspace_missing || claim_mismatch {
                    if worker.state != WorkerState::Recovering {
                        worker.transition(
                            WorkerState::Recovering,
                            now,
                            Some("startup reconciliation found stale runtime state".into()),
                        )?;
                    }
                    if run_missing {
                        worker.current_run_id = None;
                    }
                    worker.metadata.touch(now);
                    service.store.upsert_worker(&worker)?;
                    report.quarantined_worker_ids.push(worker.id.clone());
                }
            }
        }
        Ok((service, report))
    }

    pub fn start(
        &self,
        goal_id: &str,
        desired_concurrency: Option<usize>,
        ready_units: usize,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.start_with_profile(goal_id, desired_concurrency, ready_units, None, now)
    }

    pub fn start_with_permission_policy(
        &self,
        goal_id: &str,
        desired_concurrency: Option<usize>,
        ready_units: usize,
        user_policy: WorkerPermissionPolicy,
        goal_policy: WorkerPermissionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        let profile = derive_worker_permissions(user_policy, goal_policy)?;
        self.start_with_profile(
            goal_id,
            desired_concurrency,
            ready_units,
            Some(profile),
            now,
        )
    }

    fn start_with_profile(
        &self,
        goal_id: &str,
        desired_concurrency: Option<usize>,
        ready_units: usize,
        permission_profile: Option<WorkerPermissionProfile>,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        let desired = desired_concurrency.unwrap_or(self.policy.default_concurrency);
        self.validate_desired(desired)?;
        let existing = self.goals()?.get(goal_id).cloned();
        let resource_quotas = existing
            .as_ref()
            .map(|state| state.resource_quotas.clone())
            .unwrap_or_default();
        validate_resource_quota_policy(&resource_quotas)
            .map_err(WorkerPoolError::InvalidResourceQuotaPolicy)?;
        if desired > resource_quotas.max_goal_concurrency {
            return Err(WorkerPoolError::InvalidConcurrency {
                desired,
                maximum: resource_quotas.max_goal_concurrency,
            });
        }
        let worker_permissions = permission_profile
            .or_else(|| {
                existing
                    .as_ref()
                    .map(|state| state.worker_permissions.clone())
            })
            .unwrap_or_default();
        validate_worker_permissions(&worker_permissions)?;
        self.apply_permissions_to_existing_workers(goal_id, &worker_permissions, now)?;
        let state = GoalPoolState {
            goal_id: goal_id.into(),
            desired_concurrency: desired,
            mode: PoolMode::Running,
            worker_permissions,
            resource_quotas,
            started_at: existing.and_then(|state| state.started_at).or(Some(now)),
        };
        self.store.upsert_goal_pool(&state, now)?;
        self.goals()?.insert(goal_id.into(), state);
        self.fill_capacity(goal_id, ready_units, now)?;
        self.snapshot(goal_id)
    }

    pub fn configure(
        &self,
        goal_id: &str,
        desired_concurrency: usize,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.configure_with_profile(goal_id, desired_concurrency, None, now)
    }

    pub fn configure_with_permission_policy(
        &self,
        goal_id: &str,
        desired_concurrency: usize,
        user_policy: WorkerPermissionPolicy,
        goal_policy: WorkerPermissionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        let profile = derive_worker_permissions(user_policy, goal_policy)?;
        self.configure_with_profile(goal_id, desired_concurrency, Some(profile), now)
    }

    fn configure_with_profile(
        &self,
        goal_id: &str,
        desired_concurrency: usize,
        permission_profile: Option<WorkerPermissionProfile>,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.validate_desired(desired_concurrency)?;
        let existing = self.goals()?.get(goal_id).cloned();
        let mode = existing
            .as_ref()
            .map(|state| state.mode)
            .unwrap_or(PoolMode::Stopped);
        let worker_permissions = permission_profile
            .or_else(|| {
                existing
                    .as_ref()
                    .map(|state| state.worker_permissions.clone())
            })
            .unwrap_or_default();
        let resource_quotas = existing
            .as_ref()
            .map(|state| state.resource_quotas.clone())
            .unwrap_or_default();
        validate_resource_quota_policy(&resource_quotas)
            .map_err(WorkerPoolError::InvalidResourceQuotaPolicy)?;
        if desired_concurrency > resource_quotas.max_goal_concurrency {
            return Err(WorkerPoolError::InvalidConcurrency {
                desired: desired_concurrency,
                maximum: resource_quotas.max_goal_concurrency,
            });
        }
        validate_worker_permissions(&worker_permissions)?;
        self.apply_permissions_to_existing_workers(goal_id, &worker_permissions, now)?;
        let state = GoalPoolState {
            goal_id: goal_id.into(),
            desired_concurrency,
            mode,
            worker_permissions,
            resource_quotas,
            started_at: existing.and_then(|state| state.started_at),
        };
        self.store.upsert_goal_pool(&state, now)?;
        self.goals()?.insert(goal_id.into(), state);
        self.snapshot(goal_id)
    }

    fn apply_permissions_to_existing_workers(
        &self,
        goal_id: &str,
        profile: &WorkerPermissionProfile,
        now: DateTime<Utc>,
    ) -> Result<(), WorkerPoolError> {
        for mut worker in self.store.workers_for_goal(goal_id, None)? {
            if worker.state.is_terminal() || worker.permission_profile.as_ref() == Some(profile) {
                continue;
            }
            if worker.current_run_id.is_some() {
                return Err(WorkerPoolError::PermissionChangeDuringActiveRun(
                    worker.id.as_str().into(),
                ));
            }
            worker.permission_profile = Some(profile.clone());
            worker.metadata.touch(now);
            self.store.upsert_worker(&worker)?;
        }
        Ok(())
    }

    pub fn set_resource_quotas(
        &self,
        goal_id: &str,
        policy: ResourceQuotaPolicy,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        validate_resource_quota_policy(&policy)
            .map_err(WorkerPoolError::InvalidResourceQuotaPolicy)?;
        let workers = self.store.workers_for_goal(goal_id, None)?;
        if let Some(worker) = workers
            .iter()
            .find(|worker| !worker.state.is_terminal() && worker.current_run_id.is_some())
        {
            return Err(WorkerPoolError::ResourceQuotaChangeDuringActiveRun(
                worker.id.as_str().into(),
            ));
        }
        let mut goals = self.goals()?;
        let goal = goals
            .get_mut(goal_id)
            .ok_or_else(|| WorkerPoolError::GoalNotConfigured(goal_id.into()))?;
        if goal.desired_concurrency > policy.max_goal_concurrency {
            return Err(WorkerPoolError::InvalidConcurrency {
                desired: goal.desired_concurrency,
                maximum: policy.max_goal_concurrency,
            });
        }
        goal.resource_quotas = policy;
        let state = goal.clone();
        drop(goals);
        self.store.upsert_goal_pool(&state, now)?;
        self.snapshot(goal_id)
    }

    pub fn configure_for_workspace(
        &self,
        goal_id: &str,
        desired_concurrency: usize,
        workspace: impl AsRef<Path>,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        ensure_workspace_concurrency(workspace, desired_concurrency)?;
        self.configure(goal_id, desired_concurrency, now)
    }

    pub fn configure_for_workspace_with_permission_policy(
        &self,
        goal_id: &str,
        desired_concurrency: usize,
        workspace: impl AsRef<Path>,
        user_policy: WorkerPermissionPolicy,
        goal_policy: WorkerPermissionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        ensure_workspace_concurrency(workspace, desired_concurrency)?;
        self.configure_with_permission_policy(
            goal_id,
            desired_concurrency,
            user_policy,
            goal_policy,
            now,
        )
    }

    pub fn start_for_workspace(
        &self,
        goal_id: &str,
        desired_concurrency: Option<usize>,
        ready_units: usize,
        workspace: impl AsRef<Path>,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        let desired = desired_concurrency.unwrap_or(self.policy.default_concurrency);
        ensure_workspace_concurrency(workspace, desired)?;
        self.start(goal_id, Some(desired), ready_units, now)
    }

    pub fn start_for_workspace_with_permission_policy(
        &self,
        goal_id: &str,
        desired_concurrency: Option<usize>,
        ready_units: usize,
        workspace: impl AsRef<Path>,
        user_policy: WorkerPermissionPolicy,
        goal_policy: WorkerPermissionPolicy,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        let desired = desired_concurrency.unwrap_or(self.policy.default_concurrency);
        ensure_workspace_concurrency(workspace, desired)?;
        self.start_with_permission_policy(
            goal_id,
            Some(desired),
            ready_units,
            user_policy,
            goal_policy,
            now,
        )
    }

    pub fn set_desired_concurrency(
        &self,
        goal_id: &str,
        desired: usize,
        ready_units: usize,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.validate_desired(desired)?;
        let mut goals = self.goals()?;
        let goal = goals
            .get_mut(goal_id)
            .ok_or_else(|| WorkerPoolError::GoalNotConfigured(goal_id.into()))?;
        if desired > goal.resource_quotas.max_goal_concurrency {
            return Err(WorkerPoolError::InvalidConcurrency {
                desired,
                maximum: goal.resource_quotas.max_goal_concurrency,
            });
        }
        goal.desired_concurrency = desired;
        let state = goal.clone();
        drop(goals);
        self.store.upsert_goal_pool(&state, now)?;
        self.fill_capacity(goal_id, ready_units, now)?;
        self.snapshot(goal_id)
    }

    pub fn set_desired_concurrency_for_workspace(
        &self,
        goal_id: &str,
        desired: usize,
        ready_units: usize,
        workspace: impl AsRef<Path>,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        ensure_workspace_concurrency(workspace, desired)?;
        self.set_desired_concurrency(goal_id, desired, ready_units, now)
    }

    pub fn pause(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.set_mode(goal_id, PoolMode::Paused, now)?;
        for mut worker in self.store.workers_for_goal(goal_id, None)? {
            if matches!(
                worker.state,
                WorkerState::Active | WorkerState::Waiting | WorkerState::Blocked
            ) {
                worker.transition(WorkerState::Paused, now, Some("goal pool paused".into()))?;
                self.store.upsert_worker(&worker)?;
            }
        }
        self.snapshot(goal_id)
    }

    pub fn resume(
        &self,
        goal_id: &str,
        ready_units: usize,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.set_mode(goal_id, PoolMode::Running, now)?;
        for mut worker in self
            .store
            .workers_for_goal(goal_id, Some(WorkerState::Paused))?
        {
            worker.transition(WorkerState::Active, now, Some("goal pool resumed".into()))?;
            self.store.upsert_worker(&worker)?;
        }
        self.fill_capacity(goal_id, ready_units, now)?;
        self.snapshot(goal_id)
    }

    pub fn drain(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<PoolSnapshot, WorkerPoolError> {
        self.set_mode(goal_id, PoolMode::Draining, now)?;
        for mut worker in self.store.workers_for_goal(goal_id, None)? {
            if !worker.state.is_terminal()
                && worker.active_claims.is_empty()
                && matches!(worker.state, WorkerState::Active | WorkerState::Waiting)
            {
                worker.transition(
                    WorkerState::Completed,
                    now,
                    Some("goal pool drained".into()),
                )?;
                self.store.upsert_worker(&worker)?;
            }
        }
        self.snapshot(goal_id)
    }

    pub fn stop(&self, goal_id: &str, now: DateTime<Utc>) -> Result<StopReport, WorkerPoolError> {
        self.set_mode(goal_id, PoolMode::Stopped, now)?;
        let mut cancelled_worker_ids = Vec::new();
        let mut preserved_worker_ids = Vec::new();
        for mut worker in self.store.workers_for_goal(goal_id, None)? {
            if worker.state.is_terminal() {
                continue;
            }
            if worker.active_claims.is_empty() {
                worker.transition(
                    WorkerState::Cancelled,
                    now,
                    Some("goal pool stopped".into()),
                )?;
                cancelled_worker_ids.push(worker.id.clone());
                self.store.upsert_worker(&worker)?;
            } else {
                preserved_worker_ids.push(worker.id.clone());
            }
        }
        Ok(StopReport {
            snapshot: self.snapshot(goal_id)?,
            cancelled_worker_ids,
            preserved_worker_ids,
        })
    }

    pub fn transition_worker(
        &self,
        worker_id: &WorkerId,
        next: WorkerState,
        reason: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        let mut worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| WorkerPoolError::WorkerNotFound(worker_id.as_str().into()))?;
        worker.transition(next, now, reason)?;
        self.store.upsert_worker(&worker)?;
        Ok(worker)
    }

    pub fn fill_ready_claims(
        &self,
        claim_service: &ClaimService,
        goal_id: &str,
        base_revision: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, WorkerPoolError> {
        let goal = self
            .goals()?
            .get(goal_id)
            .cloned()
            .ok_or_else(|| WorkerPoolError::GoalNotConfigured(goal_id.into()))?;
        if goal.mode != PoolMode::Running {
            return Ok(Vec::new());
        }
        let scopes = claim_service.ready_unclaimed_scopes(goal_id)?;
        if scopes.is_empty() {
            return Ok(Vec::new());
        }
        self.fill_capacity(goal_id, scopes.len(), now)?;
        let mut idle_workers = self
            .store
            .workers_for_goal(goal_id, None)?
            .into_iter()
            .filter(|worker| {
                matches!(
                    worker.state,
                    WorkerState::Starting | WorkerState::Active | WorkerState::Waiting
                ) && worker.active_claims.is_empty()
            })
            .collect::<Vec<_>>();
        idle_workers.sort_by(|left, right| {
            left.metadata
                .created_at
                .cmp(&right.metadata.created_at)
                .then_with(|| left.id.as_str().cmp(right.id.as_str()))
        });
        let slots = goal.desired_concurrency.min(idle_workers.len());
        let mut claims = Vec::new();
        for (worker, scope) in idle_workers.into_iter().take(slots).zip(scopes.into_iter()) {
            let generation = self.store.latest_claim_generation(goal_id, &scope)? + 1;
            let idempotency_key = format!(
                "pool-claim:{goal_id}:{}:{generation}",
                claim_scope_key(&scope)
            );
            match claim_service.claim_ready_unit(
                goal_id,
                &worker.id,
                Some(scope),
                base_revision,
                &idempotency_key,
                now,
            ) {
                Ok(claim) => claims.push(claim),
                Err(ClaimServiceError::Store(StoreError::ActiveClaimExists { .. }))
                | Err(ClaimServiceError::ScopeNotReady) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(claims)
    }

    pub fn report_worker_failure(
        &self,
        worker_id: &WorkerId,
        kind: WorkerFailureKind,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        let mut worker = self.worker(worker_id)?;
        let needs_recovery = !worker.active_claims.is_empty()
            || matches!(
                kind,
                WorkerFailureKind::Retryable | WorkerFailureKind::WorkspaceUncertain
            );
        let next = if needs_recovery {
            WorkerState::Recovering
        } else {
            WorkerState::Failed
        };
        worker.transition(next, now, Some(format!("{kind:?}: {reason}")))?;
        worker.current_run_id = None;
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        Ok(worker)
    }

    pub fn replace_after_recovery(
        &self,
        claim_service: &ClaimService,
        worker_id: &WorkerId,
        base_revision: &str,
        evidence: RecoveryEvidence,
        now: DateTime<Utc>,
    ) -> Result<Vec<Claim>, WorkerPoolError> {
        let mut worker = self.worker(worker_id)?;
        if !worker.active_claims.is_empty()
            || !evidence.claim_recovery_complete
            || !evidence.workspace_recovery_complete
        {
            return Err(WorkerPoolError::RecoveryIncomplete(
                worker_id.as_str().into(),
            ));
        }
        if worker.state == WorkerState::Recovering {
            worker.transition(WorkerState::Failed, now, Some(evidence.reason))?;
            self.store.upsert_worker(&worker)?;
        } else if worker.state != WorkerState::Failed {
            return Err(WorkerPoolError::RecoveryIncomplete(
                worker_id.as_str().into(),
            ));
        }
        self.fill_ready_claims(claim_service, &worker.goal_id, base_revision, now)
    }

    pub fn bind_workspace(
        &self,
        worker_id: &WorkerId,
        workspace: WorkspaceBinding,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        let mut worker = self.worker(worker_id)?;
        if let Some(existing) = &worker.workspace {
            if existing == &workspace {
                return Ok(worker);
            }
            return Err(WorkerPoolError::WorkspaceAlreadyBound(
                worker_id.as_str().into(),
            ));
        }
        worker.workspace = Some(workspace);
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        Ok(worker)
    }

    pub fn provision_worker_workspace(
        &self,
        manager: &WorktreeManager,
        worker_id: &WorkerId,
        base_ref: &str,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        let worker = self.worker(worker_id)?;
        if worker.workspace.is_some() {
            return Err(WorkerPoolError::WorkspaceAlreadyBound(
                worker_id.as_str().into(),
            ));
        }
        let binding = manager.create_for_worker(&worker.goal_id, worker_id, base_ref, now)?;
        self.bind_workspace(worker_id, binding, now)
    }

    pub fn set_worker_permissions(
        &self,
        worker_id: &WorkerId,
        profile: WorkerPermissionProfile,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        validate_worker_permissions(&profile)?;
        let mut worker = self.worker(worker_id)?;
        worker.permission_profile = Some(profile);
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        Ok(worker)
    }

    pub fn begin_turn(
        &self,
        worker_id: &WorkerId,
        run_id: impl Into<String>,
        thread_id: impl Into<String>,
        continuation_of_thread_id: Option<String>,
        context_transfer_artifact_id: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        let mut worker = self.worker(worker_id)?;
        if worker.current_run_id.is_some() {
            return Err(WorkerPoolError::ActiveRunExists(worker_id.as_str().into()));
        }
        let run_id = run_id.into();
        let thread_id = thread_id.into();
        let turn = WorkerTurnBinding {
            sequence: worker.turn_history.len() as u64 + 1,
            run_id: run_id.clone(),
            thread_id: thread_id.clone(),
            started_at: now,
            completed_at: None,
            continuation_of_thread_id,
            context_transfer_artifact_id,
        };
        worker.current_run_id = Some(run_id);
        worker.current_thread_id = Some(thread_id);
        worker.turn_history.push(turn);
        worker.last_heartbeat_at = Some(now);
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        Ok(worker)
    }

    pub fn complete_turn(
        &self,
        worker_id: &WorkerId,
        now: DateTime<Utc>,
    ) -> Result<Worker, WorkerPoolError> {
        let mut worker = self.worker(worker_id)?;
        if worker.current_run_id.is_none() {
            return Err(WorkerPoolError::NoActiveRun(worker_id.as_str().into()));
        }
        let active = worker
            .turn_history
            .last_mut()
            .ok_or_else(|| WorkerPoolError::NoActiveRun(worker_id.as_str().into()))?;
        active.completed_at = Some(now);
        worker.current_run_id = None;
        worker.last_heartbeat_at = Some(now);
        worker.metadata.touch(now);
        self.store.upsert_worker(&worker)?;
        Ok(worker)
    }

    pub fn snapshot(&self, goal_id: &str) -> Result<PoolSnapshot, WorkerPoolError> {
        let goal = self
            .goals()?
            .get(goal_id)
            .cloned()
            .ok_or_else(|| WorkerPoolError::GoalNotConfigured(goal_id.into()))?;
        let workers = self.store.workers_for_goal(goal_id, None)?;
        let active_workers = workers
            .iter()
            .filter(|worker| !worker.state.is_terminal())
            .count();
        Ok(PoolSnapshot {
            goal,
            workers,
            active_workers,
            available_global_slots: self
                .policy
                .global_max
                .saturating_sub(self.global_active_workers()?),
        })
    }

    fn fill_capacity(
        &self,
        goal_id: &str,
        ready_units: usize,
        now: DateTime<Utc>,
    ) -> Result<Vec<Worker>, WorkerPoolError> {
        let goal = self
            .goals()?
            .get(goal_id)
            .cloned()
            .ok_or_else(|| WorkerPoolError::GoalNotConfigured(goal_id.into()))?;
        if goal.mode != PoolMode::Running {
            return Ok(Vec::new());
        }
        let current = self
            .store
            .workers_for_goal(goal_id, None)?
            .into_iter()
            .filter(|worker| !worker.state.is_terminal())
            .count();
        let target = goal
            .desired_concurrency
            .min(current.saturating_add(ready_units));
        let to_start = target.saturating_sub(current).min(
            self.policy
                .global_max
                .saturating_sub(self.global_active_workers()?),
        );
        let mut started = Vec::new();
        for _ in 0..to_start {
            let mut worker = Worker::new(goal_id, now);
            worker.permission_profile = Some(goal.worker_permissions.clone());
            worker.transition(WorkerState::Starting, now, None)?;
            worker.transition(WorkerState::Active, now, None)?;
            self.store.upsert_worker(&worker)?;
            started.push(worker);
        }
        Ok(started)
    }

    fn global_active_workers(&self) -> Result<usize, WorkerPoolError> {
        let goal_ids = self.goals()?.keys().cloned().collect::<Vec<_>>();
        let mut count = 0;
        for goal_id in goal_ids {
            count += self
                .store
                .workers_for_goal(&goal_id, None)?
                .into_iter()
                .filter(|worker| !worker.state.is_terminal())
                .count();
        }
        Ok(count)
    }

    fn validate_desired(&self, desired: usize) -> Result<(), WorkerPoolError> {
        if desired > self.policy.max_per_goal {
            return Err(WorkerPoolError::InvalidConcurrency {
                desired,
                maximum: self.policy.max_per_goal,
            });
        }
        Ok(())
    }

    fn set_mode(
        &self,
        goal_id: &str,
        mode: PoolMode,
        now: DateTime<Utc>,
    ) -> Result<(), WorkerPoolError> {
        let mut goals = self.goals()?;
        let goal = goals
            .get_mut(goal_id)
            .ok_or_else(|| WorkerPoolError::GoalNotConfigured(goal_id.into()))?;
        goal.mode = mode;
        let state = goal.clone();
        drop(goals);
        self.store.upsert_goal_pool(&state, now)?;
        Ok(())
    }

    fn goals(&self) -> Result<MutexGuard<'_, HashMap<String, GoalPoolState>>, WorkerPoolError> {
        self.goals.lock().map_err(|_| WorkerPoolError::LockPoisoned)
    }

    fn worker(&self, worker_id: &WorkerId) -> Result<Worker, WorkerPoolError> {
        self.store
            .worker(worker_id)?
            .ok_or_else(|| WorkerPoolError::WorkerNotFound(worker_id.as_str().into()))
    }
}

fn claim_scope_key(scope: &ClaimScope) -> String {
    match scope {
        ClaimScope::Feature { feature_id } => format!("feature:{feature_id}"),
        ClaimScope::WorkPackage { work_package_id } => {
            format!("package:{}", work_package_id.as_str())
        }
    }
}

fn sandbox_rank(value: &str) -> Option<u8> {
    match value {
        "read-only" => Some(0),
        "workspace-write" => Some(1),
        "danger-full-access" => Some(2),
        _ => None,
    }
}

fn validate_permission_policy(
    policy: &WorkerPermissionPolicy,
    autonomous_goal: bool,
) -> Result<(), WorkerPoolError> {
    let supported = matches!(
        (
            policy.sandbox.as_str(),
            policy.approval_policy.as_str(),
            policy.approvals_reviewer.as_str()
        ),
        ("workspace-write", "on-request", "user")
            | ("workspace-write", "on-request", "auto_review")
            | ("read-only", "on-request", "user")
            | ("danger-full-access", "never", "user")
    );
    if !supported {
        return Err(WorkerPoolError::InvalidPermissionProfile(format!(
            "unsupported policy combination {}/{}/{}",
            policy.sandbox, policy.approval_policy, policy.approvals_reviewer
        )));
    }
    if autonomous_goal
        && (policy.sandbox == "danger-full-access"
            || policy.approval_policy == "never"
            || policy.network_access
            || policy
                .tool_capabilities
                .contains(&WorkerToolCapability::Network))
    {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "autonomous goal policy cannot grant full filesystem access, approval bypass, or network access"
                .into(),
        ));
    }
    let capabilities = policy
        .tool_capabilities
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if !capabilities.contains(&WorkerToolCapability::ReadFiles) {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "worker policy must allow reading files".into(),
        ));
    }
    if capabilities.contains(&WorkerToolCapability::WriteFiles) && policy.sandbox == "read-only" {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "read-only sandbox cannot grant file writes".into(),
        ));
    }
    if capabilities.contains(&WorkerToolCapability::Git)
        && !capabilities.contains(&WorkerToolCapability::RunCommands)
    {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "Git capability requires command execution".into(),
        ));
    }
    if capabilities.contains(&WorkerToolCapability::Network) != policy.network_access {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "network capability and network access flag must agree".into(),
        ));
    }
    Ok(())
}

pub fn derive_worker_permissions(
    user_policy: WorkerPermissionPolicy,
    goal_policy: WorkerPermissionPolicy,
) -> Result<WorkerPermissionProfile, WorkerPoolError> {
    validate_permission_policy(&user_policy, false)?;
    validate_permission_policy(&goal_policy, true)?;
    if sandbox_rank(&goal_policy.sandbox) > sandbox_rank(&user_policy.sandbox) {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "goal sandbox exceeds the user's filesystem authority".into(),
        ));
    }
    if goal_policy.approvals_reviewer == "auto_review"
        && user_policy.approvals_reviewer != "auto_review"
    {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "goal auto-review policy exceeds the user's approval authority".into(),
        ));
    }
    if goal_policy.network_access && !user_policy.network_access {
        return Err(WorkerPoolError::InvalidPermissionProfile(
            "goal network policy exceeds the user's network authority".into(),
        ));
    }
    let user_capabilities = user_policy
        .tool_capabilities
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if let Some(capability) = goal_policy
        .tool_capabilities
        .iter()
        .find(|capability| !user_capabilities.contains(capability))
    {
        return Err(WorkerPoolError::InvalidPermissionProfile(format!(
            "goal tool capability {capability:?} exceeds the user's authority"
        )));
    }
    let profile = WorkerPermissionProfile {
        sandbox: goal_policy.sandbox.clone(),
        approval_policy: goal_policy.approval_policy.clone(),
        approvals_reviewer: goal_policy.approvals_reviewer.clone(),
        network_access: goal_policy.network_access,
        tool_capabilities: goal_policy.tool_capabilities.clone(),
        derived_from: Some(WorkerPermissionDerivation {
            policy_version: 1,
            user_policy,
            goal_policy,
        }),
    };
    validate_worker_permissions(&profile)?;
    Ok(profile)
}

pub fn validate_worker_permissions(
    profile: &WorkerPermissionProfile,
) -> Result<(), WorkerPoolError> {
    let supported = matches!(
        (
            profile.sandbox.as_str(),
            profile.approval_policy.as_str(),
            profile.approvals_reviewer.as_str()
        ),
        ("workspace-write", "on-request", "user")
            | ("workspace-write", "on-request", "auto_review")
            | ("read-only", "on-request", "user")
    );
    if supported {
        if profile.sandbox == "danger-full-access" || profile.network_access {
            return Err(WorkerPoolError::InvalidPermissionProfile(
                "autonomous workers require a bounded filesystem sandbox and network disabled"
                    .into(),
            ));
        }
        let policy = WorkerPermissionPolicy {
            sandbox: profile.sandbox.clone(),
            approval_policy: profile.approval_policy.clone(),
            approvals_reviewer: profile.approvals_reviewer.clone(),
            network_access: profile.network_access,
            tool_capabilities: profile.tool_capabilities.clone(),
        };
        validate_permission_policy(&policy, true)
    } else {
        Err(WorkerPoolError::InvalidPermissionProfile(format!(
            "unsupported autonomous worker combination {}/{}/{}",
            profile.sandbox, profile.approval_policy, profile.approvals_reviewer
        )))
    }
}

fn ensure_workspace_concurrency(
    workspace: impl AsRef<Path>,
    desired: usize,
) -> Result<WorkspaceIsolationCapability, WorkerPoolError> {
    let capability = WorkspaceIsolationCapability::detect(workspace)?;
    if capability.allows_concurrency(desired) {
        Ok(capability)
    } else {
        Err(WorkerPoolError::IsolationUnavailable(
            "non-Git workspaces are limited to one implementation worker".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::claims::{ClaimPolicy, CompletionEvidence};
    use crate::models::Status;
    use crate::tracker::Tracker;
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use tempfile::TempDir;

    fn fixture(
        policy: WorkerPoolPolicy,
    ) -> (TempDir, Arc<SqliteCoordinationStore>, WorkerPoolService) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let service = WorkerPoolService::new(Arc::clone(&store), policy);
        (directory, store, service)
    }

    #[test]
    fn start_respects_ready_desired_per_goal_and_global_capacity() {
        let (_directory, _store, service) = fixture(WorkerPoolPolicy {
            default_concurrency: 2,
            max_per_goal: 3,
            global_max: 3,
        });
        let now = Utc::now();
        let first = service.start("goal-a", None, 5, now).unwrap();
        let second = service.start("goal-b", Some(3), 5, now).unwrap();

        assert_eq!(first.active_workers, 2);
        assert_eq!(second.active_workers, 1);
        assert_eq!(second.available_global_slots, 0);
        assert!(matches!(
            service.start("goal-c", Some(4), 4, now),
            Err(WorkerPoolError::InvalidConcurrency { .. })
        ));
    }

    #[test]
    fn resource_quotas_are_validated_persisted_and_immutable_during_active_turns() {
        let (_directory, store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        let started = service.start("goal-quotas", Some(2), 2, now).unwrap();
        let policy = ResourceQuotaPolicy {
            max_worker_tokens: 100_000,
            max_goal_tokens: 400_000,
            max_worker_turn_seconds: 900,
            max_goal_elapsed_seconds: 7_200,
            max_worker_processes: 4,
            max_goal_processes: 16,
            max_worker_disk_bytes: 1024 * 1024 * 1024,
            max_goal_disk_bytes: 4 * 1024 * 1024 * 1024,
            max_worker_network_requests: 0,
            max_goal_network_requests: 0,
            max_worker_retries: 2,
            max_goal_retries: 8,
            max_goal_concurrency: 2,
        };

        let updated = service
            .set_resource_quotas("goal-quotas", policy.clone(), now)
            .unwrap();
        assert_eq!(updated.goal.resource_quotas, policy);
        assert_eq!(
            store
                .goal_pool("goal-quotas")
                .unwrap()
                .unwrap()
                .resource_quotas,
            policy
        );

        let below_configured_concurrency = ResourceQuotaPolicy {
            max_goal_concurrency: 1,
            ..policy.clone()
        };
        assert!(matches!(
            service.set_resource_quotas("goal-quotas", below_configured_concurrency, now),
            Err(WorkerPoolError::InvalidConcurrency {
                desired: 2,
                maximum: 1
            })
        ));

        service
            .begin_turn(
                &started.workers[0].id,
                "run-quota",
                "thread-quota",
                None,
                None,
                now,
            )
            .unwrap();
        assert!(matches!(
            service.set_resource_quotas(
                "goal-quotas",
                ResourceQuotaPolicy {
                    max_worker_tokens: 90_000,
                    ..policy
                },
                now
            ),
            Err(WorkerPoolError::ResourceQuotaChangeDuringActiveRun(_))
        ));
    }

    #[test]
    fn pause_resume_drain_and_stop_use_valid_worker_transitions() {
        let (_directory, _store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        service.start("goal-a", Some(2), 2, now).unwrap();

        let paused = service.pause("goal-a", now).unwrap();
        assert_eq!(paused.goal.mode, PoolMode::Paused);
        assert!(
            paused
                .workers
                .iter()
                .all(|worker| worker.state == WorkerState::Paused)
        );

        let resumed = service.resume("goal-a", 0, now).unwrap();
        assert_eq!(resumed.goal.mode, PoolMode::Running);
        assert!(
            resumed
                .workers
                .iter()
                .all(|worker| worker.state == WorkerState::Active)
        );

        let drained = service.drain("goal-a", now).unwrap();
        assert_eq!(drained.goal.mode, PoolMode::Draining);
        assert_eq!(drained.active_workers, 0);

        let stopped = service.stop("goal-a", now).unwrap();
        assert_eq!(stopped.snapshot.goal.mode, PoolMode::Stopped);
        assert!(stopped.preserved_worker_ids.is_empty());
    }

    #[test]
    fn stop_preserves_workers_that_still_own_claims() {
        let (_directory, store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        let snapshot = service.start("goal-a", Some(1), 1, now).unwrap();
        let mut worker = snapshot.workers[0].clone();
        worker
            .active_claims
            .push(super::super::domain::ClaimId::new());
        store.upsert_worker(&worker).unwrap();

        let stopped = service.stop("goal-a", now).unwrap();

        assert!(stopped.cancelled_worker_ids.is_empty());
        assert_eq!(stopped.preserved_worker_ids, vec![worker.id]);
        assert_eq!(stopped.snapshot.active_workers, 1);
    }

    #[test]
    fn worker_lifecycle_transitions_are_durable_and_validated() {
        let (_directory, _store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        let worker = service.start("goal-a", Some(1), 1, now).unwrap().workers[0].clone();

        let blocked = service
            .transition_worker(
                &worker.id,
                WorkerState::Blocked,
                Some("dependency unavailable".into()),
                now,
            )
            .unwrap();
        let recovering = service
            .transition_worker(
                &blocked.id,
                WorkerState::Recovering,
                Some("dependency restored".into()),
                now,
            )
            .unwrap();
        let failed = service
            .transition_worker(
                &recovering.id,
                WorkerState::Failed,
                Some("recovery exhausted".into()),
                now,
            )
            .unwrap();

        assert_eq!(failed.state, WorkerState::Failed);
        assert_eq!(failed.termination.unwrap().reason, "recovery exhausted");
        assert!(matches!(
            service.transition_worker(&failed.id, WorkerState::Active, None, now),
            Err(WorkerPoolError::Domain(
                DomainError::InvalidTransition { .. }
            ))
        ));
    }

    #[test]
    fn worker_identity_survives_multiple_turns_and_context_rollover() {
        let (_directory, store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        let worker = service.start("goal-a", Some(1), 1, now).unwrap().workers[0].clone();
        let workspace = WorkspaceBinding {
            repository_id: "repo-a".into(),
            canonical_repository_path: "/repo".into(),
            worktree_path: "/repo/worktrees/worker-a".into(),
            branch: "codex/worker-a".into(),
            base_revision: "base-a".into(),
            created_at: Some(now),
            creation_evidence: vec!["test fixture".into()],
        };
        service
            .bind_workspace(&worker.id, workspace.clone(), now)
            .unwrap();
        service
            .begin_turn(&worker.id, "run-1", "thread-1", None, None, now)
            .unwrap();
        service
            .complete_turn(&worker.id, now + chrono::Duration::minutes(1))
            .unwrap();
        service
            .begin_turn(
                &worker.id,
                "run-2",
                "thread-2",
                Some("thread-1".into()),
                Some("context-packet-1".into()),
                now + chrono::Duration::minutes(2),
            )
            .unwrap();

        let persisted = store.worker(&worker.id).unwrap().unwrap();
        assert_eq!(persisted.id, worker.id);
        assert_eq!(persisted.workspace, Some(workspace));
        assert_eq!(persisted.current_run_id.as_deref(), Some("run-2"));
        assert_eq!(persisted.current_thread_id.as_deref(), Some("thread-2"));
        assert_eq!(persisted.turn_history.len(), 2);
        assert_eq!(
            persisted.turn_history[1]
                .continuation_of_thread_id
                .as_deref(),
            Some("thread-1")
        );
        assert_eq!(
            persisted.turn_history[1]
                .context_transfer_artifact_id
                .as_deref(),
            Some("context-packet-1")
        );
    }

    #[test]
    fn ready_work_is_claimed_in_tracker_order_with_capacity_backpressure() {
        let (directory, store, service) = fixture(WorkerPoolPolicy::default());
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "").unwrap();
        for id in ["first", "second", "third"] {
            tracker
                .add_feature("goal-a", id, id, "", Status::Planned)
                .unwrap();
        }
        let claims = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        let now = Utc::now();
        service.start("goal-a", Some(2), 0, now).unwrap();

        let first_batch = service
            .fill_ready_claims(&claims, "goal-a", "base-a", now)
            .unwrap();
        let second_batch = service
            .fill_ready_claims(&claims, "goal-a", "base-a", now)
            .unwrap();

        assert_eq!(first_batch.len(), 2);
        assert!(second_batch.is_empty());
        assert!(matches!(
            first_batch[0].scope,
            ClaimScope::Feature { ref feature_id } if feature_id == "first"
        ));
        assert!(matches!(
            first_batch[1].scope,
            ClaimScope::Feature { ref feature_id } if feature_id == "second"
        ));

        claims
            .complete_claim(
                &first_batch[0].id,
                &first_batch[0].owner,
                first_batch[0].lease_generation,
                CompletionEvidence {
                    reason: "integrated".into(),
                    artifact_id: "artifact-first".into(),
                    integration_revision: "commit-first".into(),
                    evidence_refs: vec!["test:passed".into()],
                    integration_boundary_satisfied: true,
                },
                "complete-first",
                now,
            )
            .unwrap();
        Tracker::new(directory.path().join("goals"))
            .set_status("goal-a", "first", Status::Done)
            .unwrap();
        let replacement = service
            .fill_ready_claims(&claims, "goal-a", "base-b", now)
            .unwrap();

        assert_eq!(replacement.len(), 1);
        assert!(matches!(
            replacement[0].scope,
            ClaimScope::Feature { ref feature_id } if feature_id == "third"
        ));
    }

    #[test]
    fn failed_worker_is_replaced_only_after_claim_and_workspace_recovery() {
        let (directory, store, service) = fixture(WorkerPoolPolicy::default());
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "").unwrap();
        tracker
            .add_feature("goal-a", "first", "first", "", Status::Planned)
            .unwrap();
        let claims = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        let now = Utc::now();
        service.start("goal-a", Some(1), 0, now).unwrap();
        let claim = service
            .fill_ready_claims(&claims, "goal-a", "base-a", now)
            .unwrap()
            .remove(0);

        let recovering = service
            .report_worker_failure(
                &claim.owner,
                WorkerFailureKind::WorkspaceUncertain,
                "worker process disappeared",
                now + chrono::Duration::minutes(1),
            )
            .unwrap();
        assert_eq!(recovering.state, WorkerState::Recovering);
        assert!(matches!(
            service.replace_after_recovery(
                &claims,
                &recovering.id,
                "base-b",
                RecoveryEvidence {
                    claim_recovery_complete: false,
                    workspace_recovery_complete: false,
                    reason: "not safe".into(),
                },
                now + chrono::Duration::minutes(1),
            ),
            Err(WorkerPoolError::RecoveryIncomplete(_))
        ));

        claims
            .expire_stale_claims(
                "goal-a",
                now + chrono::Duration::minutes(7),
                chrono::Duration::minutes(1),
            )
            .unwrap();
        let replacement = service
            .replace_after_recovery(
                &claims,
                &recovering.id,
                "base-b",
                RecoveryEvidence {
                    claim_recovery_complete: true,
                    workspace_recovery_complete: true,
                    reason: "claim expired and worktree quarantined".into(),
                },
                now + chrono::Duration::minutes(7),
            )
            .unwrap();

        assert_eq!(replacement.len(), 1);
        assert_ne!(replacement[0].owner, recovering.id);
        assert_eq!(
            store.worker(&recovering.id).unwrap().unwrap().state,
            WorkerState::Failed
        );
    }

    #[test]
    fn startup_recovery_restores_pool_settings_and_quarantines_stale_bindings() {
        let (directory, store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        let worker = service.start("goal-a", Some(1), 1, now).unwrap().workers[0].clone();
        service
            .bind_workspace(
                &worker.id,
                WorkspaceBinding {
                    repository_id: "repo-a".into(),
                    canonical_repository_path: "/repo".into(),
                    worktree_path: "/repo/worktrees/missing".into(),
                    branch: "codex/missing".into(),
                    base_revision: "base-a".into(),
                    created_at: Some(now),
                    creation_evidence: vec!["test fixture".into()],
                },
                now,
            )
            .unwrap();
        service
            .begin_turn(&worker.id, "lost-run", "lost-thread", None, None, now)
            .unwrap();
        drop(service);
        drop(store);

        let reopened = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let (recovered, report) = WorkerPoolService::recover(
            Arc::clone(&reopened),
            WorkerPoolPolicy::default(),
            &RuntimeInventory::default(),
            now + chrono::Duration::minutes(1),
        )
        .unwrap();

        assert_eq!(report.recovered_goal_ids, vec!["goal-a"]);
        assert_eq!(report.quarantined_worker_ids, vec![worker.id.clone()]);
        let snapshot = recovered.snapshot("goal-a").unwrap();
        assert_eq!(snapshot.goal.desired_concurrency, 1);
        assert_eq!(snapshot.goal.mode, PoolMode::Running);
        assert_eq!(snapshot.workers[0].state, WorkerState::Recovering);
        assert_eq!(snapshot.workers[0].current_run_id, None);
    }

    #[test]
    fn no_ready_work_creates_no_workers_and_resize_is_persisted() {
        let (directory, store, service) = fixture(WorkerPoolPolicy::default());
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "").unwrap();
        let claims = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        let now = Utc::now();

        let empty = service.start("goal-a", Some(2), 0, now).unwrap();
        assert_eq!(empty.active_workers, 0);
        assert!(
            service
                .fill_ready_claims(&claims, "goal-a", "base-a", now)
                .unwrap()
                .is_empty()
        );
        let resized = service
            .set_desired_concurrency("goal-a", 1, 0, now)
            .unwrap();
        assert_eq!(resized.goal.desired_concurrency, 1);
        assert_eq!(
            store
                .goal_pool("goal-a")
                .unwrap()
                .unwrap()
                .desired_concurrency,
            1
        );
    }

    #[test]
    fn provisioned_worktree_binding_is_persisted_on_the_worker() {
        let (_directory, store, service) = fixture(WorkerPoolPolicy::default());
        let repository_root = tempfile::tempdir().unwrap();
        let repository = repository_root.path().join("repository");
        fs::create_dir(&repository).unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.email", "golazo@example.test"],
            vec!["config", "user.name", "Golazo Tests"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&repository)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        fs::write(repository.join("README.md"), "initial\n").unwrap();
        for args in [
            vec!["add", "README.md"],
            vec!["commit", "-q", "-m", "initial"],
        ] {
            assert!(
                Command::new("git")
                    .arg("-C")
                    .arg(&repository)
                    .args(args)
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let now = Utc::now();
        let worker = service.start("goal-a", Some(1), 1, now).unwrap().workers[0].clone();
        let manager = WorktreeManager::open(&repository).unwrap();

        let bound = service
            .provision_worker_workspace(&manager, &worker.id, "HEAD", now)
            .unwrap();
        service
            .set_worker_permissions(&worker.id, WorkerPermissionProfile::default(), now)
            .unwrap();

        let persisted = store.worker(&worker.id).unwrap().unwrap();
        assert_eq!(persisted.workspace, bound.workspace);
        assert_eq!(
            persisted.permission_profile,
            Some(WorkerPermissionProfile::default())
        );
        let binding = persisted.workspace.unwrap();
        assert_eq!(binding.created_at, Some(now));
        assert_eq!(binding.base_revision.len(), 40);
        assert!(Path::new(&binding.worktree_path).is_dir());
        assert!(matches!(
            service.set_worker_permissions(
                &worker.id,
                WorkerPermissionProfile {
                    sandbox: "danger-full-access".into(),
                    approval_policy: "never".into(),
                    approvals_reviewer: "user".into(),
                    network_access: true,
                    tool_capabilities: vec![
                        WorkerToolCapability::ReadFiles,
                        WorkerToolCapability::WriteFiles,
                        WorkerToolCapability::RunCommands,
                        WorkerToolCapability::Git,
                        WorkerToolCapability::Network,
                    ],
                    derived_from: None,
                },
                now,
            ),
            Err(WorkerPoolError::InvalidPermissionProfile(_))
        ));
    }

    #[test]
    fn derives_worker_permissions_from_user_and_goal_policy_without_broadening_authority() {
        let user_policy = WorkerPermissionPolicy {
            sandbox: "danger-full-access".into(),
            approval_policy: "never".into(),
            approvals_reviewer: "user".into(),
            network_access: true,
            tool_capabilities: vec![
                WorkerToolCapability::ReadFiles,
                WorkerToolCapability::WriteFiles,
                WorkerToolCapability::RunCommands,
                WorkerToolCapability::Git,
                WorkerToolCapability::Network,
            ],
        };
        let goal_policy = WorkerPermissionPolicy::conservative();

        let profile = derive_worker_permissions(user_policy.clone(), goal_policy.clone()).unwrap();

        assert_eq!(profile.sandbox, "workspace-write");
        assert_eq!(profile.approval_policy, "on-request");
        assert_eq!(profile.approvals_reviewer, "user");
        assert!(!profile.network_access);
        assert!(
            !profile
                .tool_capabilities
                .contains(&WorkerToolCapability::Network)
        );
        let derivation = profile.derived_from.unwrap();
        assert_eq!(derivation.policy_version, 1);
        assert_eq!(derivation.user_policy, user_policy);
        assert_eq!(derivation.goal_policy, goal_policy);
    }

    #[test]
    fn rejects_goal_permissions_that_exceed_user_or_autonomous_policy() {
        let read_only_user = WorkerPermissionPolicy {
            sandbox: "read-only".into(),
            approval_policy: "on-request".into(),
            approvals_reviewer: "user".into(),
            network_access: false,
            tool_capabilities: vec![
                WorkerToolCapability::ReadFiles,
                WorkerToolCapability::RunCommands,
            ],
        };
        assert!(matches!(
            derive_worker_permissions(
                read_only_user,
                WorkerPermissionPolicy::conservative(),
            ),
            Err(WorkerPoolError::InvalidPermissionProfile(message))
                if message.contains("filesystem authority")
        ));

        let network_goal = WorkerPermissionPolicy {
            network_access: true,
            tool_capabilities: vec![
                WorkerToolCapability::ReadFiles,
                WorkerToolCapability::WriteFiles,
                WorkerToolCapability::RunCommands,
                WorkerToolCapability::Git,
                WorkerToolCapability::Network,
            ],
            ..WorkerPermissionPolicy::conservative()
        };
        assert!(matches!(
            derive_worker_permissions(WorkerPermissionPolicy::conservative(), network_goal),
            Err(WorkerPoolError::InvalidPermissionProfile(message))
                if message.contains("network access")
        ));
    }

    #[test]
    fn every_started_worker_receives_the_durable_derived_goal_profile() {
        let (_directory, store, service) = fixture(WorkerPoolPolicy::default());
        let now = Utc::now();
        let goal_policy = WorkerPermissionPolicy {
            sandbox: "read-only".into(),
            approval_policy: "on-request".into(),
            approvals_reviewer: "user".into(),
            network_access: false,
            tool_capabilities: vec![
                WorkerToolCapability::ReadFiles,
                WorkerToolCapability::RunCommands,
            ],
        };

        let snapshot = service
            .start_with_permission_policy(
                "goal-permissions",
                Some(2),
                2,
                WorkerPermissionPolicy::conservative(),
                goal_policy.clone(),
                now,
            )
            .unwrap();

        assert_eq!(snapshot.workers.len(), 2);
        assert_eq!(snapshot.goal.worker_permissions.sandbox, "read-only");
        for worker in store.workers_for_goal("goal-permissions", None).unwrap() {
            let profile = worker
                .permission_profile
                .expect("worker permission profile");
            assert_eq!(profile.sandbox, "read-only");
            assert_eq!(
                profile.tool_capabilities,
                vec![
                    WorkerToolCapability::ReadFiles,
                    WorkerToolCapability::RunCommands,
                ]
            );
            assert_eq!(
                profile.derived_from.as_ref().map(|item| &item.goal_policy),
                Some(&goal_policy)
            );
        }
    }
}
