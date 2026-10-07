use crate::tracker::Tracker;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use thiserror::Error;

pub const DELIVERY_SCHEMA_VERSION: u16 = 1;
pub const DEFAULT_REMOTE: &str = "origin";
pub const DEFAULT_TARGET_BRANCH: &str = "develop";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalMergePolicy {
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalDeliveryStatus {
    Local,
    Pushed,
    PullRequestOpen,
    NeedsAttention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoalDeliveryState {
    pub schema_version: u16,
    pub goal_id: String,
    pub integration_branch: String,
    pub target_branch: String,
    pub remote: String,
    pub merge_policy: GoalMergePolicy,
    pub status: GoalDeliveryStatus,
    pub integration_worktree: String,
    pub head_revision: String,
    pub pushed_revision: Option<String>,
    pub pull_request_number: Option<u64>,
    pub pull_request_url: Option<String>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum GoalDeliveryError {
    #[error("goal delivery I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("goal delivery serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("goal integration path is unsafe: {0}")]
    UnsafePath(String),
    #[error("goal integration Git command failed: {command}: {message}")]
    Git { command: String, message: String },
}

pub fn goal_branch_name(goal_id: &str) -> String {
    format!("codex/goal-{}", safe_component(goal_id))
}

pub fn ensure_goal_base_revision(
    tracker: &Tracker,
    repository: &Path,
    goal_id: &str,
    requested: Option<&str>,
    now: DateTime<Utc>,
) -> Result<String, GoalDeliveryError> {
    let repository = canonical_repository(repository)?;
    let branch = goal_branch_name(goal_id);
    if let Some(requested) = requested.map(str::trim).filter(|value| {
        !value.is_empty() && *value != "workspace-current" && *value != "goal-current"
    }) {
        return resolve_commit(&repository, requested);
    }
    if ref_exists(&repository, &format!("refs/heads/{branch}"))? {
        return resolve_commit(&repository, &branch);
    }
    let target_ref = initial_target_ref(&repository)?;
    let base = resolve_commit(&repository, &target_ref)?;
    git_output(&repository, &["branch", &branch, &base])?;
    let state = GoalDeliveryState {
        schema_version: DELIVERY_SCHEMA_VERSION,
        goal_id: goal_id.into(),
        integration_branch: branch,
        target_branch: DEFAULT_TARGET_BRANCH.into(),
        remote: DEFAULT_REMOTE.into(),
        merge_policy: GoalMergePolicy::Manual,
        status: GoalDeliveryStatus::Local,
        integration_worktree: integration_worktree_path(&repository, goal_id)?
            .display()
            .to_string(),
        head_revision: base.clone(),
        pushed_revision: None,
        pull_request_number: None,
        pull_request_url: None,
        last_error: None,
        updated_at: now,
    };
    write_state(tracker, &state)?;
    Ok(base)
}

pub fn integrate_and_publish(
    tracker: &Tracker,
    repository: &Path,
    goal_id: &str,
    commit: &str,
    now: DateTime<Utc>,
) -> Result<GoalDeliveryState, GoalDeliveryError> {
    let repository = canonical_repository(repository)?;
    ensure_goal_base_revision(tracker, &repository, goal_id, None, now)?;
    let branch = goal_branch_name(goal_id);
    let worktree = ensure_integration_worktree(&repository, goal_id, &branch)?;
    ensure_clean(&worktree)?;
    if !is_ancestor(&worktree, commit, "HEAD")? && !patch_is_integrated(&worktree, commit)? {
        let output = git(&worktree, &["cherry-pick", commit])?;
        if !output.status.success() {
            let _ = git(&worktree, &["cherry-pick", "--abort"]);
            return Err(git_failure(&["cherry-pick", commit], &output));
        }
    }
    let head_revision = resolve_commit(&worktree, "HEAD")?;
    let mut state = read_state(tracker, goal_id)?.unwrap_or(GoalDeliveryState {
        schema_version: DELIVERY_SCHEMA_VERSION,
        goal_id: goal_id.into(),
        integration_branch: branch.clone(),
        target_branch: DEFAULT_TARGET_BRANCH.into(),
        remote: DEFAULT_REMOTE.into(),
        merge_policy: GoalMergePolicy::Manual,
        status: GoalDeliveryStatus::Local,
        integration_worktree: worktree.display().to_string(),
        head_revision: head_revision.clone(),
        pushed_revision: None,
        pull_request_number: None,
        pull_request_url: None,
        last_error: None,
        updated_at: now,
    });
    state.integration_branch = branch;
    state.integration_worktree = worktree.display().to_string();
    state.head_revision = head_revision.clone();
    state.updated_at = now;
    state.status = GoalDeliveryStatus::Local;
    state.last_error = None;

    let remote_url = match git_output_optional(&repository, &["remote", "get-url", DEFAULT_REMOTE])?
    {
        Some(url) => url,
        None => {
            write_state(tracker, &state)?;
            return Ok(state);
        }
    };
    let push = git(
        &worktree,
        &[
            "push",
            "--set-upstream",
            DEFAULT_REMOTE,
            &state.integration_branch,
        ],
    )?;
    if !push.status.success() {
        state.status = GoalDeliveryStatus::NeedsAttention;
        state.last_error = Some(format!(
            "goal branch is integrated locally but push failed: {}",
            stderr(&push)
        ));
        write_state(tracker, &state)?;
        return Ok(state);
    }
    state.status = GoalDeliveryStatus::Pushed;
    state.pushed_revision = Some(head_revision);

    if is_github_remote(&remote_url) {
        match ensure_pull_request(tracker, &worktree, &state) {
            Ok(Some((number, url))) => {
                state.status = GoalDeliveryStatus::PullRequestOpen;
                state.pull_request_number = Some(number);
                state.pull_request_url = Some(url);
                state.last_error = None;
            }
            Ok(None) => {
                state.status = GoalDeliveryStatus::NeedsAttention;
                state.last_error = Some(
                    "goal branch was pushed, but GitHub CLI is unavailable; create the pull request manually"
                        .into(),
                );
            }
            Err(error) => {
                state.status = GoalDeliveryStatus::NeedsAttention;
                state.last_error = Some(format!(
                    "goal branch was pushed, but pull-request creation failed: {error}"
                ));
            }
        }
    }
    write_state(tracker, &state)?;
    Ok(state)
}

pub fn read_state(
    tracker: &Tracker,
    goal_id: &str,
) -> Result<Option<GoalDeliveryState>, GoalDeliveryError> {
    let path = state_path(tracker, goal_id)?;
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
}

fn ensure_pull_request(
    tracker: &Tracker,
    worktree: &Path,
    state: &GoalDeliveryState,
) -> Result<Option<(u64, String)>, GoalDeliveryError> {
    if which::which("gh").is_err() {
        return Ok(None);
    }
    let list = Command::new("gh")
        .current_dir(worktree)
        .args([
            "pr",
            "list",
            "--head",
            &state.integration_branch,
            "--base",
            &state.target_branch,
            "--state",
            "open",
            "--json",
            "number,url",
            "--limit",
            "1",
        ])
        .output()?;
    if !list.status.success() {
        return Err(GoalDeliveryError::Git {
            command: "gh pr list".into(),
            message: stderr(&list),
        });
    }
    let existing: Vec<Value> = serde_json::from_slice(&list.stdout)?;
    if let Some(pr) = existing.first() {
        if let (Some(number), Some(url)) = (
            pr.get("number").and_then(Value::as_u64),
            pr.get("url").and_then(Value::as_str),
        ) {
            return Ok(Some((number, url.into())));
        }
    }
    let title = tracker
        .get_goal(&state.goal_id)
        .ok()
        .and_then(|goal| {
            goal.get("title")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .unwrap_or_else(|| state.goal_id.clone());
    let create = Command::new("gh")
        .current_dir(worktree)
        .args([
            "pr",
            "create",
            "--base",
            &state.target_branch,
            "--head",
            &state.integration_branch,
            "--title",
            &format!("Golazo goal: {title}"),
            "--body",
            "Managed by Golazo. Validated worker slices are serialized on this goal branch. Merge approval remains manual.",
        ])
        .output()?;
    if !create.status.success() {
        return Err(GoalDeliveryError::Git {
            command: "gh pr create".into(),
            message: stderr(&create),
        });
    }
    let url = String::from_utf8_lossy(&create.stdout).trim().to_string();
    let view = Command::new("gh")
        .current_dir(worktree)
        .args([
            "pr",
            "view",
            &state.integration_branch,
            "--json",
            "number,url",
        ])
        .output()?;
    if !view.status.success() {
        return Err(GoalDeliveryError::Git {
            command: "gh pr view".into(),
            message: stderr(&view),
        });
    }
    let pr: Value = serde_json::from_slice(&view.stdout)?;
    Ok(Some((
        pr.get("number").and_then(Value::as_u64).unwrap_or(0),
        pr.get("url")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or(url),
    )))
}

fn ensure_integration_worktree(
    repository: &Path,
    goal_id: &str,
    branch: &str,
) -> Result<PathBuf, GoalDeliveryError> {
    let path = integration_worktree_path(repository, goal_id)?;
    if path.exists() {
        let actual_branch = git_output(&path, &["rev-parse", "--abbrev-ref", "HEAD"])?;
        if actual_branch != branch {
            return Err(GoalDeliveryError::UnsafePath(format!(
                "{} is checked out on {actual_branch}, expected {branch}",
                path.display()
            )));
        }
        return Ok(path);
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    git_output(
        repository,
        &["worktree", "add", path.to_string_lossy().as_ref(), branch],
    )?;
    path.canonicalize().map_err(GoalDeliveryError::from)
}

fn integration_worktree_path(
    repository: &Path,
    goal_id: &str,
) -> Result<PathBuf, GoalDeliveryError> {
    let parent = repository
        .parent()
        .ok_or_else(|| GoalDeliveryError::UnsafePath(repository.display().to_string()))?;
    let repository_id = {
        let digest = Sha256::digest(repository.to_string_lossy().as_bytes());
        format!("{:x}", digest)[..16].to_string()
    };
    let managed = parent
        .join(".golazo-worktrees")
        .join(repository_id)
        .join("goal-integrations")
        .join(safe_component(goal_id));
    if safe_component(goal_id).is_empty()
        || managed
            .strip_prefix(parent.join(".golazo-worktrees"))
            .is_err()
        || managed
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(GoalDeliveryError::UnsafePath(managed.display().to_string()));
    }
    Ok(managed)
}

fn initial_target_ref(repository: &Path) -> Result<String, GoalDeliveryError> {
    for candidate in [
        format!("refs/remotes/{DEFAULT_REMOTE}/{DEFAULT_TARGET_BRANCH}"),
        format!("refs/heads/{DEFAULT_TARGET_BRANCH}"),
    ] {
        if ref_exists(repository, &candidate)? {
            return Ok(candidate);
        }
    }
    Ok("HEAD".into())
}

fn canonical_repository(repository: &Path) -> Result<PathBuf, GoalDeliveryError> {
    let root = repository.canonicalize()?;
    let actual = git_output(&root, &["rev-parse", "--show-toplevel"])?;
    let actual = PathBuf::from(actual).canonicalize()?;
    if actual != root {
        return Err(GoalDeliveryError::UnsafePath(root.display().to_string()));
    }
    Ok(root)
}

fn ensure_clean(worktree: &Path) -> Result<(), GoalDeliveryError> {
    if !git_output(worktree, &["status", "--porcelain=v1"])?.is_empty() {
        return Err(GoalDeliveryError::Git {
            command: "git status --porcelain=v1".into(),
            message: "goal integration worktree is dirty and requires reconciliation".into(),
        });
    }
    Ok(())
}

fn is_ancestor(
    worktree: &Path,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, GoalDeliveryError> {
    let output = git(
        worktree,
        &["merge-base", "--is-ancestor", ancestor, descendant],
    )?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(git_failure(
            &["merge-base", "--is-ancestor", ancestor, descendant],
            &output,
        )),
    }
}

fn patch_is_integrated(worktree: &Path, commit: &str) -> Result<bool, GoalDeliveryError> {
    let output = git(worktree, &["cherry", "HEAD", commit])?;
    if !output.status.success() {
        return Err(git_failure(&["cherry", "HEAD", commit], &output));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.starts_with("- ")))
}

fn ref_exists(repository: &Path, reference: &str) -> Result<bool, GoalDeliveryError> {
    let output = git(repository, &["show-ref", "--verify", "--quiet", reference])?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(git_failure(
            &["show-ref", "--verify", "--quiet", reference],
            &output,
        )),
    }
}

fn resolve_commit(repository: &Path, reference: &str) -> Result<String, GoalDeliveryError> {
    git_output(
        repository,
        &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
    )
}

fn git_output_optional(
    repository: &Path,
    args: &[&str],
) -> Result<Option<String>, GoalDeliveryError> {
    let output = git(repository, args)?;
    if output.status.success() {
        Ok(Some(String::from_utf8_lossy(&output.stdout).trim().into()))
    } else if output.status.code() == Some(2) {
        Ok(None)
    } else {
        Ok(None)
    }
}

fn git_output(repository: &Path, args: &[&str]) -> Result<String, GoalDeliveryError> {
    let output = git(repository, args)?;
    if !output.status.success() {
        return Err(git_failure(args, &output));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().into())
}

fn git(repository: &Path, args: &[&str]) -> Result<Output, GoalDeliveryError> {
    Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(args)
        .output()
        .map_err(GoalDeliveryError::from)
}

fn git_failure(args: &[&str], output: &Output) -> GoalDeliveryError {
    GoalDeliveryError::Git {
        command: format!("git {}", args.join(" ")),
        message: stderr(output),
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim().to_string()
}

fn is_github_remote(url: &str) -> bool {
    url.contains("github.com/") || url.contains("github.com:")
}

fn state_path(tracker: &Tracker, goal_id: &str) -> Result<PathBuf, GoalDeliveryError> {
    let directory = tracker.root.join("delivery");
    fs::create_dir_all(&directory)?;
    Ok(directory.join(format!("{}.json", safe_component(goal_id))))
}

fn write_state(tracker: &Tracker, state: &GoalDeliveryState) -> Result<(), GoalDeliveryError> {
    let path = state_path(tracker, &state.goal_id)?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(state)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn safe_component(value: &str) -> String {
    let mut safe = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while safe.contains("--") {
        safe = safe.replace("--", "-");
    }
    safe = safe.trim_matches('-').to_string();
    if safe.is_empty() {
        "goal".into()
    } else {
        safe.chars().take(80).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn git_ok(directory: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().into()
    }

    fn repository(directory: &TempDir) -> (PathBuf, Tracker) {
        let repository = directory.path().join("repository");
        fs::create_dir(&repository).unwrap();
        git_ok(&repository, &["init", "-q", "-b", "develop"]);
        git_ok(&repository, &["config", "user.email", "test@example.com"]);
        git_ok(&repository, &["config", "user.name", "Test"]);
        fs::write(repository.join("README.md"), "base\n").unwrap();
        git_ok(&repository, &["add", "README.md"]);
        git_ok(&repository, &["commit", "-q", "-m", "base"]);
        let tracker = Tracker::new(repository.join(".goal-manager"));
        tracker.create_goal("goal-a", "Goal A", "delivery").unwrap();
        (repository, tracker)
    }

    #[test]
    fn integration_branch_does_not_mutate_canonical_checkout() {
        let directory = TempDir::new().unwrap();
        let (repository, tracker) = repository(&directory);
        let canonical_head = git_ok(&repository, &["rev-parse", "HEAD"]);
        let canonical_branch = git_ok(&repository, &["branch", "--show-current"]);
        ensure_goal_base_revision(&tracker, &repository, "goal-a", None, Utc::now()).unwrap();
        let worker = directory.path().join("worker");
        git_ok(
            &repository,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "worker-change",
                worker.to_str().unwrap(),
                &canonical_head,
            ],
        );
        fs::write(worker.join("worker.txt"), "worker\n").unwrap();
        git_ok(&worker, &["add", "worker.txt"]);
        git_ok(&worker, &["commit", "-q", "-m", "worker"]);
        let worker_commit = git_ok(&worker, &["rev-parse", "HEAD"]);

        let state =
            integrate_and_publish(&tracker, &repository, "goal-a", &worker_commit, Utc::now())
                .unwrap();

        assert_eq!(state.status, GoalDeliveryStatus::Local);
        assert_eq!(state.integration_branch, "codex/goal-goal-a");
        assert_eq!(
            git_ok(&repository, &["branch", "--show-current"]),
            canonical_branch
        );
        assert_eq!(git_ok(&repository, &["rev-parse", "HEAD"]), canonical_head);
        assert!(!repository.join("worker.txt").exists());
        assert_eq!(
            git_ok(&repository, &["show", "codex/goal-goal-a:worker.txt"]),
            "worker"
        );
        assert_eq!(read_state(&tracker, "goal-a").unwrap(), Some(state));
    }

    #[test]
    fn publication_pushes_without_force_and_is_idempotent() {
        let directory = TempDir::new().unwrap();
        let (repository, tracker) = repository(&directory);
        let remote = directory.path().join("remote.git");
        fs::create_dir(&remote).unwrap();
        git_ok(&remote, &["init", "-q", "--bare"]);
        git_ok(
            &repository,
            &["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git_ok(&repository, &["push", "-q", "-u", "origin", "develop"]);
        let worker = directory.path().join("worker");
        git_ok(
            &repository,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                "worker-change",
                worker.to_str().unwrap(),
            ],
        );
        fs::write(worker.join("worker.txt"), "worker\n").unwrap();
        git_ok(&worker, &["add", "worker.txt"]);
        git_ok(&worker, &["commit", "-q", "-m", "worker"]);
        let worker_commit = git_ok(&worker, &["rev-parse", "HEAD"]);

        let first =
            integrate_and_publish(&tracker, &repository, "goal-a", &worker_commit, Utc::now())
                .unwrap();
        let second =
            integrate_and_publish(&tracker, &repository, "goal-a", &worker_commit, Utc::now())
                .unwrap();

        assert_eq!(first.status, GoalDeliveryStatus::Pushed);
        assert_eq!(second.head_revision, first.head_revision);
        assert_eq!(
            git_ok(&remote, &["rev-parse", "refs/heads/codex/goal-goal-a"]),
            first.head_revision
        );
    }
}
