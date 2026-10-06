use super::claims::{ClaimPolicy, ClaimService, ClaimServiceError};
use super::domain::{WorkerId, WorkspaceBinding};
use super::pool::{
    PoolRecoveryReport, RuntimeInventory, WorkerPoolError, WorkerPoolPolicy, WorkerPoolService,
};
use super::store::{
    ClaimRepository, IntegrationArtifactRepository, IntegrationJobRepository, PoolRepository,
    SqliteCoordinationStore, StoreError, WorkerRepository,
};
use super::workspace::{WorkspaceQuarantineRecord, WorkspaceReconciliation, WorktreeManager};
use crate::observability::CorrelationIds;
use crate::runner::{RunManager, RunnerRecoveryReport};
use crate::tracker::{Tracker, TrackerError};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

pub const STARTUP_RECOVERY_SCHEMA: &str = "golazo.startup-recovery.v1";

#[derive(Debug, Error)]
pub enum StartupRecoveryError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Tracker(#[from] TrackerError),
    #[error(transparent)]
    Pool(#[from] WorkerPoolError),
    #[error(transparent)]
    Claims(#[from] ClaimServiceError),
    #[error("startup recovery report serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("startup recovery report I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("startup recovery report could not be persisted: {0}")]
    Persist(#[from] tempfile::PersistError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackerRecoveryReport {
    pub goal_ids: Vec<String>,
    pub validated_goal_ids: Vec<String>,
    pub invalid_goals: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceRecoveryReport {
    pub provider: String,
    pub reconciliation: Option<WorkspaceReconciliation>,
    pub quarantine_records: Vec<WorkspaceQuarantineRecord>,
    pub existing_workspace_paths: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartupRecoveryReport {
    pub schema: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub tracker: TrackerRecoveryReport,
    pub runner: RunnerRecoveryReport,
    pub workspaces: WorkspaceRecoveryReport,
    pub pool: PoolRecoveryReport,
    pub expired_claim_ids: Vec<String>,
    pub requeued_integration_job_ids: Vec<String>,
    pub warnings: Vec<String>,
}

pub struct StartupRecoveryService {
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    runner: Arc<RunManager>,
    repository_root: PathBuf,
    report_path: PathBuf,
}

impl StartupRecoveryService {
    pub fn new(
        tracker: Tracker,
        store: Arc<SqliteCoordinationStore>,
        runner: Arc<RunManager>,
        repository_root: impl Into<PathBuf>,
        report_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            tracker,
            store,
            runner,
            repository_root: repository_root.into(),
            report_path: report_path.into(),
        }
    }

    pub async fn run(
        &self,
        started_at: DateTime<Utc>,
    ) -> Result<StartupRecoveryReport, StartupRecoveryError> {
        self.store.integrity_check()?;
        let tracker = self.reconcile_trackers()?;
        let runner = self.runner.reconcile_after_restart().await;
        let bindings = self.workspace_bindings()?;
        let workspaces = reconcile_workspaces(&self.repository_root, &bindings, started_at);
        let inventory = RuntimeInventory {
            live_run_ids: runner.live_process_run_ids.iter().cloned().collect(),
            live_thread_ids: runner.native_thread_ids.iter().cloned().collect(),
            existing_workspace_paths: workspaces
                .existing_workspace_paths
                .iter()
                .cloned()
                .collect(),
        };
        let (_, pool) = WorkerPoolService::recover(
            Arc::clone(&self.store),
            WorkerPoolPolicy::default(),
            &inventory,
            started_at,
        )?;

        let mut goal_ids = tracker.goal_ids.clone();
        goal_ids.extend(pool.recovered_goal_ids.iter().cloned());
        goal_ids.sort();
        goal_ids.dedup();
        let claims = ClaimService::new(
            self.tracker.clone(),
            Arc::clone(&self.store),
            ClaimPolicy::default(),
        );
        let mut expired_claim_ids = Vec::new();
        for goal_id in &goal_ids {
            if !tracker.validated_goal_ids.contains(goal_id) {
                continue;
            }
            expired_claim_ids.extend(
                claims
                    .expire_stale_claims(goal_id, started_at, Duration::seconds(60))?
                    .into_iter()
                    .map(|claim| claim.id.as_str().to_string()),
            );
        }
        expired_claim_ids.sort();

        let mut requeued_integration_job_ids = self
            .store
            .recover_running_integration_jobs(started_at)?
            .into_iter()
            .map(|job| job.id.as_str().to_string())
            .collect::<Vec<_>>();
        requeued_integration_job_ids.sort();

        let mut warnings = Vec::new();
        if let Some(error) = &runner.thread_inventory_error {
            warnings.push(format!("Codex thread inventory unavailable: {error}"));
        }
        if let Some(error) = &workspaces.error {
            warnings.push(format!("Workspace inventory unavailable: {error}"));
        }
        warnings.extend(
            tracker
                .invalid_goals
                .iter()
                .map(|finding| format!("Invalid tracker: {finding}")),
        );

        let report = StartupRecoveryReport {
            schema: STARTUP_RECOVERY_SCHEMA.into(),
            started_at,
            completed_at: Utc::now(),
            tracker,
            runner,
            workspaces,
            pool,
            expired_claim_ids,
            requeued_integration_job_ids,
            warnings,
        };
        persist_report(&self.report_path, &report)?;
        CorrelationIds::default().emit_info("startup.reconciliation", "completed");
        Ok(report)
    }

    fn reconcile_trackers(&self) -> Result<TrackerRecoveryReport, StartupRecoveryError> {
        if !self.tracker.root.exists() {
            return Ok(TrackerRecoveryReport {
                goal_ids: vec![],
                validated_goal_ids: vec![],
                invalid_goals: vec![],
            });
        }
        let mut goal_ids = std::fs::read_dir(&self.tracker.root)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().join("implementation.md").is_file())
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect::<Vec<_>>();
        goal_ids.sort();
        goal_ids.dedup();
        let mut validated_goal_ids = Vec::new();
        let mut invalid_goals = Vec::new();
        for goal_id in &goal_ids {
            match self.tracker.validate(goal_id) {
                Ok(_) => validated_goal_ids.push(goal_id.clone()),
                Err(error) => invalid_goals.push(format!("{goal_id}: {}", error.message)),
            }
        }
        Ok(TrackerRecoveryReport {
            goal_ids,
            validated_goal_ids,
            invalid_goals,
        })
    }

    fn workspace_bindings(&self) -> Result<Vec<(WorkerId, WorkspaceBinding)>, StoreError> {
        let mut bindings = Vec::new();
        for goal in self.store.goal_pools()? {
            bindings.extend(
                self.store
                    .workers_for_goal(&goal.goal_id, None)?
                    .into_iter()
                    .filter_map(|worker| worker.workspace.map(|binding| (worker.id, binding))),
            );
        }
        bindings.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
        bindings.dedup_by(|left, right| left.0 == right.0);
        Ok(bindings)
    }
}

fn reconcile_workspaces(
    repository_root: &Path,
    bindings: &[(WorkerId, WorkspaceBinding)],
    now: DateTime<Utc>,
) -> WorkspaceRecoveryReport {
    match WorktreeManager::open(repository_root) {
        Ok(manager) => match manager.reconcile(bindings) {
            Ok(reconciliation) => {
                let mut quarantine_records = Vec::new();
                let mut errors = Vec::new();
                for entry in &reconciliation.orphaned_worktrees {
                    match manager.quarantine(
                        entry,
                        "startup reconciliation found an unbound managed worktree",
                        now,
                    ) {
                        Ok(record) => quarantine_records.push(record),
                        Err(error) => errors.push(error.to_string()),
                    }
                }
                let existing_workspace_paths = manager
                    .inventory()
                    .map(|entries| {
                        entries
                            .into_iter()
                            .filter(|entry| !entry.prunable)
                            .map(|entry| entry.path)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_else(|error| {
                        errors.push(error.to_string());
                        bindings
                            .iter()
                            .filter(|(_, binding)| Path::new(&binding.worktree_path).exists())
                            .map(|(_, binding)| binding.worktree_path.clone())
                            .collect()
                    });
                WorkspaceRecoveryReport {
                    provider: "git_worktrees".into(),
                    reconciliation: Some(reconciliation),
                    quarantine_records,
                    existing_workspace_paths,
                    error: (!errors.is_empty()).then(|| errors.join("; ")),
                }
            }
            Err(error) => fallback_workspace_report(bindings, "git_worktrees", error.to_string()),
        },
        Err(error) => fallback_workspace_report(bindings, "filesystem", error.to_string()),
    }
}

fn fallback_workspace_report(
    bindings: &[(WorkerId, WorkspaceBinding)],
    provider: &str,
    error: String,
) -> WorkspaceRecoveryReport {
    let mut existing_workspace_paths = bindings
        .iter()
        .filter(|(_, binding)| Path::new(&binding.worktree_path).exists())
        .map(|(_, binding)| binding.worktree_path.clone())
        .collect::<Vec<_>>();
    existing_workspace_paths.sort();
    existing_workspace_paths.dedup();
    WorkspaceRecoveryReport {
        provider: provider.into(),
        reconciliation: None,
        quarantine_records: vec![],
        existing_workspace_paths,
        error: Some(error),
    }
}

fn persist_report(path: &Path, report: &StartupRecoveryReport) -> Result<(), StartupRecoveryError> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "startup recovery report path has no parent",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, report)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{
        Claim, ClaimScope, IntegrationArtifact, IntegrationArtifactId, IntegrationDiffSummary,
        IntegrationWorkspaceSnapshot, RecordMetadata, Worker, WorkerState, WorkerTurnBinding,
    };
    use crate::coordination::integration::IntegrationQueueService;
    use crate::coordination::pool::{GoalPoolState, PoolMode};
    use crate::models::Status;
    use serde_json::json;
    use std::process::Command;

    fn git(repository: &Path, arguments: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(arguments)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn reconciles_every_durable_startup_boundary_and_persists_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        std::fs::create_dir(&repository).unwrap();
        git(&repository, &["init", "-q"]);
        git(&repository, &["config", "user.email", "test@example.com"]);
        git(&repository, &["config", "user.name", "Test"]);
        std::fs::write(repository.join("README.md"), "recovery\n").unwrap();
        git(&repository, &["add", "README.md"]);
        git(&repository, &["commit", "-qm", "initial"]);

        let tracker_root = repository.join(".goal-manager");
        let tracker = Tracker::new(&tracker_root);
        tracker
            .create_goal("goal-a", "Goal A", "startup recovery")
            .unwrap();
        tracker
            .add_feature(
                "goal-a",
                "feature-a",
                "Feature A",
                "recovery",
                Status::Planned,
            )
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(tracker_root.join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let stale_at = now - Duration::minutes(10);
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
        let manager = WorktreeManager::open(&repository).unwrap();
        worker.workspace = Some(
            manager
                .create_for_worker("goal-a", &worker.id, "HEAD", stale_at)
                .unwrap(),
        );
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "feature-a".into(),
            },
            worker.id.clone(),
            "HEAD",
            stale_at,
            now - Duration::minutes(2),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        worker.current_run_id = Some("run-interrupted".into());
        worker.current_thread_id = Some("thread-lost".into());
        worker.turn_history.push(WorkerTurnBinding {
            sequence: 1,
            run_id: "run-interrupted".into(),
            thread_id: "thread-lost".into(),
            started_at: stale_at,
            completed_at: None,
            continuation_of_thread_id: None,
            context_transfer_artifact_id: None,
        });
        store.upsert_worker(&worker).unwrap();
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
        let queued = queue.enqueue(&artifact.id, 0, stale_at).unwrap();
        queue.acquire_next("repo-a", stale_at).unwrap().unwrap();

        let history_path = tracker_root.join("run-history.json");
        std::fs::write(
            &history_path,
            serde_json::to_vec_pretty(&vec![json!({
                "id": "run-interrupted",
                "prompt": "continue",
                "cwd": repository,
                "sandbox": "workspace-write",
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "execution_prompt": null,
                "goal_id": "goal-a",
                "resumed_from": "thread-lost",
                "status": "running",
                "created_at": stale_at.to_rfc3339(),
                "started_at": stale_at.to_rfc3339(),
                "finished_at": null,
                "return_code": null,
                "thread_id": "thread-lost",
                "final_message": null,
                "error": null
            })])
            .unwrap(),
        )
        .unwrap();
        let runner =
            RunManager::new(repository.clone(), "missing-codex".into(), history_path).await;
        let report_path = tracker_root.join("startup-recovery.json");
        let report = StartupRecoveryService::new(
            tracker,
            Arc::clone(&store),
            runner,
            &repository,
            &report_path,
        )
        .run(now)
        .await
        .unwrap();

        assert_eq!(report.schema, STARTUP_RECOVERY_SCHEMA);
        assert_eq!(report.tracker.validated_goal_ids, vec!["goal-a"]);
        assert_eq!(report.runner.interrupted_run_ids, vec!["run-interrupted"]);
        assert!(report.runner.thread_inventory_error.is_some());
        assert_eq!(report.expired_claim_ids, vec![claim.id.as_str()]);
        assert_eq!(
            report.requeued_integration_job_ids,
            vec![queued.id.as_str()]
        );
        assert!(report.workspaces.error.is_none());
        assert!(report.pool.quarantined_worker_ids.contains(&worker.id));
        assert!(report_path.is_file());
        assert_eq!(
            store.claim(&claim.id).unwrap().unwrap().state,
            super::super::domain::ClaimState::Expired
        );
        assert_eq!(
            store.integration_job(&queued.id).unwrap().unwrap().state,
            super::super::domain::IntegrationJobState::Queued
        );
    }
}
