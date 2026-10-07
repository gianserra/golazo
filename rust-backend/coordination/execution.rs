use super::claims::{CancelEvidence, ClaimPolicy, ClaimService};
use super::domain::{
    Claim, ClaimId, ClaimScope, ClaimState, IntegrationValidationEvidence, ValidationGateKind,
    WorkerId,
};
use super::integration::{
    CaptureIntegrationArtifactRequest, IntegrationArtifactService, IntegrationFinalizationRequest,
    IntegrationFinalizationService, IntegrationQueueService, ValidationGateRunner,
    ValidationGateSpec,
};
use super::pool::{WorkerFailureKind, WorkerPoolPolicy, WorkerPoolService};
use super::protocol::{WorkerContextAssembler, WorkerContextLimits};
use super::store::{
    ClaimRepository, IntegrationArtifactRepository, SqliteCoordinationStore,
    ValidationReportRepository, WorkerRepository,
};
use super::workspace::WorktreeManager;
use crate::models::{Status, WorkMode};
use crate::runner::RunManager;
use crate::tracker::Tracker;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use tokio::time::{Duration as TokioDuration, sleep};

const MONITOR_INTERVAL: TokioDuration = TokioDuration::from_secs(1);
const HEARTBEAT_INTERVAL: Duration = Duration::seconds(60);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDispatch {
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub run_id: String,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionDispatchReport {
    pub recovered_phantom_claim_ids: Vec<ClaimId>,
    pub dispatched: Vec<WorkerDispatch>,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkerCompletion {
    summary: String,
    completed_step_ids: Vec<String>,
    #[serde(default)]
    validation_commands: Vec<String>,
    #[serde(default)]
    evidence_refs: Vec<String>,
    #[serde(default)]
    known_risks: Vec<String>,
}

pub fn resolve_base_revision(repository: &Path, requested: Option<&str>) -> Result<String, String> {
    let requested = requested
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "workspace-current")
        .unwrap_or("HEAD");
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["rev-parse", "--verify", &format!("{requested}^{{commit}}")])
        .output()
        .map_err(|error| format!("could not resolve pool base revision: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "could not resolve pool base revision {requested}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn recover_unstarted_claims(
    tracker: &Tracker,
    store: Arc<SqliteCoordinationStore>,
    goal_id: &str,
    now: DateTime<Utc>,
) -> Result<Vec<ClaimId>, String> {
    let claims = ClaimService::new(tracker.clone(), store.clone(), ClaimPolicy::default());
    let pool = WorkerPoolService::load(store.clone(), WorkerPoolPolicy::default())
        .map_err(|error| error.to_string())?;
    let mut recovered = Vec::new();
    for claim in store
        .claims_for_goal(goal_id, Some(ClaimState::Active))
        .map_err(|error| error.to_string())?
    {
        let Some(worker) = store
            .worker(&claim.owner)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        let never_dispatched = worker.workspace.is_none()
            && worker.current_run_id.is_none()
            && worker.turn_history.is_empty();
        if !never_dispatched || claim.lease_expires_at > now {
            continue;
        }
        claims
            .cancel_claim(
                &claim.id,
                &worker.id,
                claim.lease_generation,
                CancelEvidence {
                    reason: "worker dispatch never started before the claim lease expired".into(),
                    evidence_refs: vec!["recovery:missing-workspace-and-runtime".into()],
                },
                &format!("recover-undispatched:{}", claim.id.as_str()),
                now,
            )
            .map_err(|error| error.to_string())?;
        pool.report_worker_failure(
            &worker.id,
            WorkerFailureKind::Permanent,
            "dispatch never created a workspace or Codex run",
            now,
        )
        .map_err(|error| error.to_string())?;
        recovered.push(claim.id);
    }
    for worker in store
        .workers_for_goal(goal_id, None)
        .map_err(|error| error.to_string())?
    {
        let orphaned_recovery = worker.state == super::domain::WorkerState::Recovering
            && worker.active_claims.is_empty()
            && worker.current_run_id.is_none();
        if orphaned_recovery {
            pool.report_worker_failure(
                &worker.id,
                WorkerFailureKind::Permanent,
                "startup recovery found no active claim or Codex run to recover",
                now,
            )
            .map_err(|error| error.to_string())?;
        }
    }
    Ok(recovered)
}

pub async fn dispatch_claims(
    runner: Arc<RunManager>,
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    repository: PathBuf,
    claims: Vec<Claim>,
    recovered_phantom_claim_ids: Vec<ClaimId>,
) -> ExecutionDispatchReport {
    let mut report = ExecutionDispatchReport {
        recovered_phantom_claim_ids,
        ..ExecutionDispatchReport::default()
    };
    let manager = match WorktreeManager::open(&repository) {
        Ok(manager) => manager,
        Err(error) => {
            report.failures.push(error.to_string());
            return report;
        }
    };
    let pool = match WorkerPoolService::load(store.clone(), WorkerPoolPolicy::default()) {
        Ok(pool) => pool,
        Err(error) => {
            report.failures.push(error.to_string());
            return report;
        }
    };

    for claim in claims {
        let now = Utc::now();
        let result = dispatch_one(
            runner.clone(),
            tracker.clone(),
            store.clone(),
            &pool,
            &manager,
            claim.clone(),
            now,
        )
        .await;
        match result {
            Ok(dispatch) => report.dispatched.push(dispatch),
            Err(error) => {
                fail_dispatch(&tracker, store.clone(), &pool, &claim, &error, now);
                report
                    .failures
                    .push(format!("{}: {error}", claim.id.as_str()));
            }
        }
    }
    report
}

async fn dispatch_one(
    runner: Arc<RunManager>,
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    pool: &WorkerPoolService,
    manager: &WorktreeManager,
    claim: Claim,
    now: DateTime<Utc>,
) -> Result<WorkerDispatch, String> {
    let worker = store
        .worker(&claim.owner)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("worker {} was not found", claim.owner.as_str()))?;
    let worker = if worker.workspace.is_none() {
        pool.provision_worker_workspace(manager, &worker.id, &claim.base_revision, now)
            .map_err(|error| error.to_string())?
    } else {
        worker
    };
    let packet = WorkerContextAssembler::new(
        tracker.clone(),
        store.clone(),
        WorkerContextLimits::default(),
    )
    .assemble(&worker.id, &claim.id, now)
    .map_err(|error| error.to_string())?;
    let execution_prompt = packet
        .execution_instructions()
        .map_err(|error| error.to_string())?;
    let prompt = format!(
        "Implement one coherent slice for {} and return the required structured result.",
        scope_label(&claim.scope)
    );
    let run = runner
        .create_for_worker(
            &worker,
            prompt,
            vec![],
            vec![],
            ".".into(),
            false,
            None,
            Some(execution_prompt),
            Some(worker_completion_schema()),
            WorkMode::Build,
            None,
        )
        .await?;
    let dispatch = WorkerDispatch {
        worker_id: worker.id.clone(),
        claim_id: claim.id.clone(),
        run_id: run.id.clone(),
    };
    tokio::spawn(monitor_run(
        runner,
        tracker,
        store,
        worker.id,
        claim.id,
        claim.lease_generation,
        run.id,
    ));
    Ok(dispatch)
}

async fn monitor_run(
    runner: Arc<RunManager>,
    tracker: Tracker,
    store: Arc<SqliteCoordinationStore>,
    worker_id: WorkerId,
    claim_id: ClaimId,
    claim_generation: u64,
    run_id: String,
) {
    let claims = ClaimService::new(tracker.clone(), store.clone(), ClaimPolicy::default());
    let Ok(pool) = WorkerPoolService::load(store.clone(), WorkerPoolPolicy::default()) else {
        return;
    };
    let mut turn_bound = false;
    let mut last_heartbeat = Utc::now();
    loop {
        sleep(MONITOR_INTERVAL).await;
        let now = Utc::now();
        let Some(run) = runner.get(&run_id).await else {
            fail_running_claim(
                &tracker,
                store.clone(),
                &pool,
                &claim_id,
                &worker_id,
                claim_generation,
                "Codex run disappeared",
                None,
                now,
            );
            return;
        };
        if !turn_bound {
            if let Some(thread_id) = run.thread_id.as_deref() {
                if pool
                    .begin_turn(&worker_id, &run_id, thread_id, None, None, now)
                    .is_ok()
                {
                    turn_bound = true;
                }
            }
        }
        if now - last_heartbeat >= HEARTBEAT_INTERVAL {
            if claims
                .heartbeat(&claim_id, &worker_id, claim_generation, now)
                .is_err()
            {
                fail_running_claim(
                    &tracker,
                    store.clone(),
                    &pool,
                    &claim_id,
                    &worker_id,
                    claim_generation,
                    "claim heartbeat failed",
                    None,
                    now,
                );
                return;
            }
            last_heartbeat = now;
        }
        if !matches!(run.status.as_str(), "completed" | "failed" | "cancelled") {
            continue;
        }
        if turn_bound {
            let _ = pool.complete_turn(&worker_id, now);
        }
        if run.status != "completed" {
            let failure_code = run
                .terminal_error
                .as_ref()
                .and_then(|error| error.code.as_deref());
            let reason = match (run.terminal_error.as_ref(), failure_code) {
                (Some(error), Some(code)) => {
                    format!("Codex worker failed ({code}): {}", error.message)
                }
                (Some(error), None) => format!("Codex worker failed: {}", error.message),
                (None, _) => run
                    .error
                    .clone()
                    .unwrap_or_else(|| "Codex worker run failed".into()),
            };
            fail_running_claim(
                &tracker,
                store.clone(),
                &pool,
                &claim_id,
                &worker_id,
                claim_generation,
                &reason,
                failure_code,
                now,
            );
            return;
        }
        let result = run
            .final_message
            .as_deref()
            .ok_or_else(|| "Codex worker returned no completion result".to_string())
            .and_then(parse_worker_completion)
            .and_then(|completion| {
                complete_worker_run(
                    &tracker,
                    store.clone(),
                    &worker_id,
                    &claim_id,
                    claim_generation,
                    completion,
                    now,
                )
            });
        if let Err(error) = result {
            fail_running_claim(
                &tracker,
                store.clone(),
                &pool,
                &claim_id,
                &worker_id,
                claim_generation,
                &error,
                None,
                now,
            );
        }
        return;
    }
}

fn complete_worker_run(
    tracker: &Tracker,
    store: Arc<SqliteCoordinationStore>,
    worker_id: &WorkerId,
    claim_id: &ClaimId,
    claim_generation: u64,
    completion: WorkerCompletion,
    now: DateTime<Utc>,
) -> Result<(), String> {
    let claims = ClaimService::new(tracker.clone(), store.clone(), ClaimPolicy::default());
    claims
        .heartbeat(claim_id, worker_id, claim_generation, now)
        .map_err(|error| error.to_string())?;
    commit_worker_changes(store.clone(), worker_id, claim_id, &completion.summary)?;
    let validations = completion
        .validation_commands
        .iter()
        .map(|command| IntegrationValidationEvidence {
            command: command.clone(),
            succeeded: true,
            evidence_refs: vec![format!("worker-validation:{command}")],
            completed_at: now,
        })
        .collect::<Vec<_>>();
    let mut evidence_refs = completion.evidence_refs.clone();
    evidence_refs.push(format!("worker-summary:{}", completion.summary));
    for step in &completion.completed_step_ids {
        evidence_refs.push(format!("tracker-step:{step}"));
    }
    let artifact = IntegrationArtifactService::new(store.clone())
        .capture(
            CaptureIntegrationArtifactRequest {
                claim_id: claim_id.clone(),
                worker_id: worker_id.clone(),
                changed_contracts: vec![],
                migrations: vec![],
                validations,
                evidence_refs,
                known_risks: completion.known_risks,
            },
            now,
        )
        .map_err(|error| error.to_string())?;
    if artifact.commits.len() != 1
        || artifact.workspace.staged_paths.len() > 0
        || artifact.workspace.unstaged_paths.len() > 0
        || artifact.workspace.untracked_paths.len() > 0
    {
        return Err("worker completion requires exactly one commit and a clean worktree".into());
    }
    let report = ValidationGateRunner::new(store.clone())
        .run(
            &artifact.id,
            &[ValidationGateSpec {
                id: "repository-diff-check".into(),
                kind: ValidationGateKind::Repository,
                program: "git".into(),
                args: vec![
                    "diff".into(),
                    "--check".into(),
                    format!("{}..HEAD", artifact.base_revision),
                ],
                required: true,
                max_output_bytes: 32 * 1_024,
            }],
            now,
        )
        .map_err(|error| error.to_string())?;
    if !report.passed {
        return Err("worker commit failed repository validation".into());
    }
    IntegrationQueueService::new(store.clone())
        .enqueue(&artifact.id, 0, now)
        .map_err(|error| error.to_string())?;
    drain_integration_queue(tracker, store, &artifact.repository_id)
}

fn commit_worker_changes(
    store: Arc<SqliteCoordinationStore>,
    worker_id: &WorkerId,
    claim_id: &ClaimId,
    summary: &str,
) -> Result<(), String> {
    let worker = store
        .worker(worker_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("worker {} was not found", worker_id.as_str()))?;
    let claim = store
        .claim(claim_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("claim {} was not found", claim_id.as_str()))?;
    if claim.owner != *worker_id || claim.state != ClaimState::Active {
        return Err("Golazo can only commit changes for the active claim owner".into());
    }
    let binding = worker
        .workspace
        .ok_or_else(|| "worker workspace binding is missing".to_string())?;
    if binding.base_revision != claim.base_revision {
        return Err("worker workspace base revision does not match the claim".into());
    }
    commit_worktree_changes_at(
        Path::new(&binding.worktree_path),
        &claim.base_revision,
        summary,
    )
}

fn commit_worktree_changes_at(
    worktree: &Path,
    base_revision: &str,
    summary: &str,
) -> Result<(), String> {
    let existing_commits = git_output(
        worktree,
        &["rev-list", "--count", &format!("{base_revision}..HEAD")],
    )?;
    if existing_commits.trim() != "0" {
        return Err(
            "worker created commits directly; Golazo requires an uncommitted worktree".into(),
        );
    }

    let status = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["status", "--porcelain=v1", "-z"])
        .output()
        .map_err(|error| format!("could not inspect worker changes: {error}"))?;
    if !status.status.success() {
        return Err(format!(
            "could not inspect worker changes: {}",
            String::from_utf8_lossy(&status.stderr).trim()
        ));
    }
    if status.stdout.is_empty() {
        return Err("worker completed without producing implementation changes".into());
    }
    let mut rename_source_follows = false;
    for entry in status
        .stdout
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        let entry = String::from_utf8_lossy(entry);
        let path = if rename_source_follows {
            rename_source_follows = false;
            entry.as_ref()
        } else {
            rename_source_follows = matches!(entry.as_bytes().first(), Some(b'R' | b'C'));
            entry.get(3..).unwrap_or(&entry)
        };
        if path == ".goal-manager" || path.starts_with(".goal-manager/") {
            return Err(
                "worker changes include the coordinator-owned .goal-manager directory".into(),
            );
        }
    }

    git_output(worktree, &["add", "--all"])?;
    let subject = summary
        .lines()
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("complete assigned implementation slice")
        .chars()
        .take(64)
        .collect::<String>();
    git_output(
        worktree,
        &[
            "-c",
            "user.name=Golazo Worker",
            "-c",
            "user.email=golazo@local",
            "commit",
            "--no-verify",
            "-m",
            &format!("golazo: {subject}"),
        ],
    )?;
    let remaining = git_output(worktree, &["status", "--porcelain=v1"])?;
    if !remaining.trim().is_empty() {
        return Err(
            "Golazo created the integration commit but the worker worktree is not clean".into(),
        );
    }
    Ok(())
}

fn git_output(worktree: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()
        .map_err(|error| format!("could not run git {}: {error}", args.join(" ")))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn drain_integration_queue(
    tracker: &Tracker,
    store: Arc<SqliteCoordinationStore>,
    repository_id: &str,
) -> Result<(), String> {
    let queue = IntegrationQueueService::new(store.clone());
    loop {
        let now = Utc::now();
        let Some(job) = queue
            .acquire_next(repository_id, now)
            .map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        let artifact = store
            .integration_artifact(&job.artifact_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("artifact {} was not found", job.artifact_id.as_str()))?;
        let reports = store
            .validation_reports_for_artifact(&artifact.id)
            .map_err(|error| error.to_string())?;
        let report = reports
            .into_iter()
            .rev()
            .find(|report| report.passed)
            .ok_or_else(|| "queued artifact has no passing validation report".to_string())?;
        let claim = store
            .claim(&artifact.claim_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("claim {} was not found", artifact.claim_id.as_str()))?;
        let feature_id = match &claim.scope {
            ClaimScope::Feature { feature_id } => feature_id.clone(),
            ClaimScope::WorkPackage { .. } => {
                queue
                    .finish(
                        &job.id,
                        false,
                        "automatic finalization requires a feature claim",
                        now,
                    )
                    .map_err(|error| error.to_string())?;
                return Err("automatic finalization currently requires a feature claim".into());
            }
        };
        let step_ids = artifact
            .evidence_refs
            .iter()
            .filter_map(|reference| reference.strip_prefix("tracker-step:").map(str::to_string))
            .collect::<Vec<_>>();
        if step_ids.is_empty() {
            queue
                .finish(
                    &job.id,
                    false,
                    "worker did not identify a completed tracker step",
                    now,
                )
                .map_err(|error| error.to_string())?;
            return Err("worker did not identify a completed tracker step".into());
        }
        let repository = store
            .worker(&artifact.worker_id)
            .map_err(|error| error.to_string())?
            .and_then(|worker| worker.workspace)
            .map(|binding| binding.canonical_repository_path)
            .ok_or_else(|| "worker workspace binding is missing".to_string())?;
        let output = Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args(["cherry-pick", &artifact.head_revision])
            .output()
            .map_err(|error| error.to_string())?;
        if !output.status.success() {
            let _ = Command::new("git")
                .arg("-C")
                .arg(&repository)
                .args(["cherry-pick", "--abort"])
                .output();
            queue
                .finish(&job.id, false, "automatic cherry-pick failed", now)
                .map_err(|error| error.to_string())?;
            return Err(format!(
                "automatic cherry-pick failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let integration_revision = resolve_base_revision(Path::new(&repository), Some("HEAD"))?;
        let summary = artifact
            .evidence_refs
            .iter()
            .find_map(|reference| reference.strip_prefix("worker-summary:"))
            .unwrap_or("Worker slice validated and integrated")
            .to_string();
        IntegrationFinalizationService::new(store.clone(), tracker.clone())
            .finalize(
                IntegrationFinalizationRequest {
                    job_id: job.id,
                    validation_report_id: report.id,
                    tracker_feature_id: feature_id,
                    tracker_step_ids: step_ids,
                    tracker_summary: summary,
                    tracker_evidence: artifact.evidence_refs.clone(),
                    tracker_status: Status::Partial,
                    integration_revision,
                },
                Utc::now(),
            )
            .map_err(|error| error.to_string())?;
    }
}

fn fail_dispatch(
    tracker: &Tracker,
    store: Arc<SqliteCoordinationStore>,
    pool: &WorkerPoolService,
    claim: &Claim,
    reason: &str,
    now: DateTime<Utc>,
) {
    fail_running_claim(
        tracker,
        store,
        pool,
        &claim.id,
        &claim.owner,
        claim.lease_generation,
        reason,
        None,
        now,
    );
}

fn fail_running_claim(
    tracker: &Tracker,
    store: Arc<SqliteCoordinationStore>,
    pool: &WorkerPoolService,
    claim_id: &ClaimId,
    worker_id: &WorkerId,
    generation: u64,
    reason: &str,
    failure_code: Option<&str>,
    now: DateTime<Utc>,
) {
    let claims = ClaimService::new(tracker.clone(), store, ClaimPolicy::default());
    let mut evidence_refs = vec!["worker-runtime-failure".into()];
    if let Some(code) = failure_code {
        evidence_refs.push(format!("codex-error:{code}"));
    }
    let _ = claims.cancel_claim(
        claim_id,
        worker_id,
        generation,
        CancelEvidence {
            reason: reason.into(),
            evidence_refs,
        },
        &format!("runtime-failure:{}", claim_id.as_str()),
        now,
    );
    let _ = pool.report_worker_failure(worker_id, WorkerFailureKind::Permanent, reason, now);
}

fn scope_label(scope: &ClaimScope) -> String {
    match scope {
        ClaimScope::Feature { feature_id } => format!("feature {feature_id}"),
        ClaimScope::WorkPackage { work_package_id } => {
            format!("work package {}", work_package_id.as_str())
        }
    }
}

fn parse_worker_completion(message: &str) -> Result<WorkerCompletion, String> {
    let trimmed = message.trim();
    let candidate = if trimmed.starts_with("```") {
        trimmed
            .strip_prefix("```json")
            .or_else(|| trimmed.strip_prefix("```"))
            .and_then(|value| value.strip_suffix("```"))
            .unwrap_or(trimmed)
            .trim()
    } else if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        &trimmed[start..=end]
    } else {
        trimmed
    };
    let result: WorkerCompletion = serde_json::from_str(candidate)
        .map_err(|error| format!("invalid worker completion result: {error}"))?;
    if result.summary.trim().is_empty() || result.completed_step_ids.is_empty() {
        return Err("worker completion requires a summary and at least one tracker step ID".into());
    }
    Ok(result)
}

fn worker_completion_schema() -> Value {
    let strings = || json!({"type": "array", "items": {"type": "string"}});
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["summary", "completedStepIds", "validationCommands", "evidenceRefs", "knownRisks"],
        "properties": {
            "summary": {"type": "string"},
            "completedStepIds": strings(),
            "validationCommands": strings(),
            "evidenceRefs": strings(),
            "knownRisks": strings()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{Worker, WorkerState};
    use std::fs;
    use tempfile::TempDir;

    fn git(directory: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn workspace_current_resolves_to_a_commit() {
        let directory = TempDir::new().unwrap();
        git(directory.path(), &["init", "-q"]);
        git(
            directory.path(),
            &["config", "user.email", "test@example.com"],
        );
        git(directory.path(), &["config", "user.name", "Test"]);
        fs::write(directory.path().join("README.md"), "demo\n").unwrap();
        git(directory.path(), &["add", "README.md"]);
        git(directory.path(), &["commit", "-q", "-m", "initial"]);
        let resolved = resolve_base_revision(directory.path(), Some("workspace-current")).unwrap();
        assert_eq!(resolved.len(), 40);
        assert_eq!(
            resolved,
            resolve_base_revision(directory.path(), Some("HEAD")).unwrap()
        );
    }

    #[test]
    fn parses_structured_worker_completion() {
        let completion = parse_worker_completion(r#"{"summary":"done","completedStepIds":["step-a"],"validationCommands":["cargo test"],"evidenceRefs":[],"knownRisks":[]}"#).unwrap();
        assert_eq!(completion.completed_step_ids, vec!["step-a"]);
    }

    #[test]
    fn coordinator_creates_the_single_worker_commit() {
        let directory = TempDir::new().unwrap();
        git(directory.path(), &["init", "-q"]);
        fs::write(directory.path().join("README.md"), "before\n").unwrap();
        git(directory.path(), &["add", "README.md"]);
        git(
            directory.path(),
            &[
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=Test",
                "commit",
                "-q",
                "-m",
                "initial",
            ],
        );
        let base = resolve_base_revision(directory.path(), Some("HEAD")).unwrap();
        fs::write(directory.path().join("README.md"), "after\n").unwrap();

        commit_worktree_changes_at(directory.path(), &base, "Implement worker slice").unwrap();

        assert_eq!(
            git_output(
                directory.path(),
                &["rev-list", "--count", &format!("{base}..HEAD")]
            )
            .unwrap(),
            "1"
        );
        assert!(
            git_output(directory.path(), &["status", "--porcelain=v1"])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn coordinator_refuses_worker_tracker_changes() {
        let directory = TempDir::new().unwrap();
        git(directory.path(), &["init", "-q"]);
        fs::write(directory.path().join("README.md"), "before\n").unwrap();
        git(directory.path(), &["add", "README.md"]);
        git(
            directory.path(),
            &[
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=Test",
                "commit",
                "-q",
                "-m",
                "initial",
            ],
        );
        let base = resolve_base_revision(directory.path(), Some("HEAD")).unwrap();
        fs::create_dir(directory.path().join(".goal-manager")).unwrap();
        fs::write(
            directory.path().join(".goal-manager/tracker.md"),
            "changed\n",
        )
        .unwrap();

        let error = commit_worktree_changes_at(directory.path(), &base, "Invalid tracker edit")
            .unwrap_err();

        assert!(error.contains("coordinator-owned .goal-manager"));
        assert_eq!(
            git_output(
                directory.path(),
                &["rev-list", "--count", &format!("{base}..HEAD")]
            )
            .unwrap(),
            "0"
        );
    }

    #[test]
    fn terminalizes_orphaned_recovering_workers_before_refill() {
        let directory = TempDir::new().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "recovery").unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker
            .transition(WorkerState::Recovering, now, Some("restart".into()))
            .unwrap();
        store.upsert_worker(&worker).unwrap();

        recover_unstarted_claims(&tracker, store.clone(), "goal-a", now).unwrap();

        assert_eq!(
            store.worker(&worker.id).unwrap().unwrap().state,
            WorkerState::Failed
        );
    }

    #[test]
    fn preserves_codex_failure_code_in_claim_and_worker_outcomes() {
        let directory = TempDir::new().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "recovery").unwrap();
        tracker
            .add_feature(
                "goal-a",
                "feature-a",
                "Feature A",
                "runtime failure",
                Status::Planned,
            )
            .unwrap();
        tracker
            .add_step("goal-a", "feature-a", "step-a", "Step A", false, true)
            .unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "feature-a".into(),
            },
            worker.id.clone(),
            "base",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        let pool = WorkerPoolService::load(store.clone(), WorkerPoolPolicy::default()).unwrap();

        fail_running_claim(
            &tracker,
            store.clone(),
            &pool,
            &claim.id,
            &worker.id,
            claim.lease_generation,
            "Codex worker failed (usageLimitExceeded): Usage limit reached",
            Some("usageLimitExceeded"),
            now,
        );

        let failed_claim = store.claim(&claim.id).unwrap().unwrap();
        let outcome = failed_claim.outcome.unwrap();
        assert_eq!(failed_claim.state, ClaimState::Revoked);
        assert!(outcome.reason.contains("usageLimitExceeded"));
        assert!(
            outcome
                .evidence_refs
                .contains(&"codex-error:usageLimitExceeded".into())
        );
        let failed_worker = store.worker(&worker.id).unwrap().unwrap();
        assert_eq!(failed_worker.state, WorkerState::Failed);
        assert!(
            failed_worker
                .termination
                .unwrap()
                .reason
                .contains("usageLimitExceeded")
        );
    }
}
