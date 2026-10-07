use super::domain::{WorkerId, WorkspaceBinding};
use crate::observability::CorrelationIds;
use crate::redaction::redact_sensitive_text;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ffi::OsStr;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceState {
    pub branch: String,
    pub head_revision: String,
    pub staged_paths: Vec<String>,
    pub unstaged_paths: Vec<String>,
    pub untracked_paths: Vec<String>,
    pub conflicted_paths: Vec<String>,
    pub unpublished_commits: u64,
}

impl WorkspaceState {
    pub fn has_dirty_files(&self) -> bool {
        !self.staged_paths.is_empty()
            || !self.unstaged_paths.is_empty()
            || !self.untracked_paths.is_empty()
            || !self.conflicted_paths.is_empty()
    }

    pub fn safe_for_automatic_cleanup(&self) -> bool {
        !self.has_dirty_files() && self.unpublished_commits == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorktreeInventoryEntry {
    pub path: String,
    pub head_revision: String,
    pub branch: Option<String>,
    pub prunable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceReconciliation {
    pub attached_worker_ids: Vec<WorkerId>,
    pub missing_worker_ids: Vec<WorkerId>,
    pub orphaned_worktrees: Vec<WorktreeInventoryEntry>,
    pub unregistered_directories: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceQuarantineRecord {
    pub worktree_path: String,
    pub branch: Option<String>,
    pub reason: String,
    pub recorded_at: DateTime<Utc>,
    pub record_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupAuthorization {
    Integrated {
        integration_artifact_id: String,
        expected_head_revision: String,
    },
    ExplicitDiscard {
        decided_by: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceCleanupRecord {
    pub worktree_path: String,
    pub retained_branch: String,
    pub head_revision: String,
    pub authorization: String,
    pub recorded_at: DateTime<Utc>,
    pub record_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkspaceIsolationCapability {
    GitWorktrees {
        repository_root: String,
    },
    SingleWorkerOnly {
        workspace_root: String,
        reason: String,
    },
}

impl WorkspaceIsolationCapability {
    pub fn detect(workspace: impl AsRef<Path>) -> Result<Self, WorkspaceError> {
        let workspace = workspace.as_ref().canonicalize()?;
        if !workspace.is_dir() {
            return Err(WorkspaceError::UnsafePath(workspace.display().to_string()));
        }
        let output = Command::new("git")
            .arg("-C")
            .arg(&workspace)
            .args(["rev-parse", "--show-toplevel"])
            .output()?;
        if output.status.success() {
            let root =
                PathBuf::from(String::from_utf8_lossy(&output.stdout).trim()).canonicalize()?;
            Ok(Self::GitWorktrees {
                repository_root: root.display().to_string(),
            })
        } else {
            Ok(Self::SingleWorkerOnly {
                workspace_root: workspace.display().to_string(),
                reason:
                    "non-Git workspaces do not have a recoverable concurrent isolation provider"
                        .into(),
            })
        }
    }

    pub fn allows_concurrency(&self, desired: usize) -> bool {
        matches!(self, Self::GitWorktrees { .. }) || desired <= 1
    }
}

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error("workspace I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("workspace record serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("repository path is not a safe Git repository root: {0}")]
    InvalidRepository(String),
    #[error("managed worktree path is unsafe: {0}")]
    UnsafePath(String),
    #[error("managed worktree already exists: {0}")]
    WorktreeExists(String),
    #[error("worker branch already exists: {0}")]
    BranchExists(String),
    #[error("workspace cleanup is not authorized: {0}")]
    CleanupNotAuthorized(String),
    #[error("Git command failed: {command}: {message}")]
    GitFailed { command: String, message: String },
}

#[derive(Debug, Clone)]
pub struct WorktreeManager {
    repository_root: PathBuf,
    managed_root: PathBuf,
    repository_id: String,
}

impl WorktreeManager {
    pub fn open(repository: impl AsRef<Path>) -> Result<Self, WorkspaceError> {
        let repository_root = repository.as_ref().canonicalize()?;
        validate_repository_root(&repository_root)?;
        let parent = repository_root.parent().ok_or_else(|| {
            WorkspaceError::InvalidRepository(repository_root.display().to_string())
        })?;
        let repository_id = repository_identity(&repository_root);
        let managed_parent = parent.join(".golazo-worktrees");
        fs::create_dir_all(&managed_parent)?;
        let managed_parent = managed_parent.canonicalize()?;
        if managed_parent.parent() != Some(parent) {
            return Err(WorkspaceError::UnsafePath(
                managed_parent.display().to_string(),
            ));
        }
        let managed_root = managed_parent.join(&repository_id);
        fs::create_dir_all(&managed_root)?;
        let managed_root = managed_root.canonicalize()?;
        if managed_root.parent() != Some(managed_parent.as_path()) {
            return Err(WorkspaceError::UnsafePath(
                managed_root.display().to_string(),
            ));
        }
        Ok(Self {
            repository_root,
            managed_root,
            repository_id,
        })
    }

    pub fn repository_root(&self) -> &Path {
        &self.repository_root
    }

    pub fn managed_root(&self) -> &Path {
        &self.managed_root
    }

    pub fn create_for_worker(
        &self,
        goal_id: &str,
        worker_id: &WorkerId,
        base_ref: &str,
        now: DateTime<Utc>,
    ) -> Result<WorkspaceBinding, WorkspaceError> {
        let worker_key = safe_component(worker_id.as_str());
        let goal_key = safe_component(goal_id);
        let branch = format!("codex/golazo/{goal_key}/{worker_key}");
        let worktree_path = self.managed_root.join(&worker_key);
        validate_new_worktree_path(&self.managed_root, &worktree_path)?;
        if worktree_path.exists() {
            return Err(WorkspaceError::WorktreeExists(
                worktree_path.display().to_string(),
            ));
        }
        if self.branch_exists(&branch)? {
            return Err(WorkspaceError::BranchExists(branch));
        }
        let base_revision = self.resolve_commit(base_ref)?;
        self.git([
            OsStr::new("worktree"),
            OsStr::new("add"),
            OsStr::new("-b"),
            OsStr::new(&branch),
            worktree_path.as_os_str(),
            OsStr::new(&base_revision),
        ])?;
        let canonical_worktree = worktree_path.canonicalize()?;
        if canonical_worktree.parent() != Some(self.managed_root.as_path()) {
            return Err(WorkspaceError::UnsafePath(
                canonical_worktree.display().to_string(),
            ));
        }
        let checked_out = git_output(&canonical_worktree, ["rev-parse", "HEAD"])?;
        if checked_out.trim() != base_revision {
            return Err(WorkspaceError::GitFailed {
                command: "git rev-parse HEAD".into(),
                message: "created worktree did not resolve to the requested base commit".into(),
            });
        }
        let binding = WorkspaceBinding {
            repository_id: self.repository_id.clone(),
            canonical_repository_path: self.repository_root.display().to_string(),
            worktree_path: canonical_worktree.display().to_string(),
            branch: branch.clone(),
            base_revision: base_revision.clone(),
            created_at: Some(now),
            creation_evidence: vec![
                format!("git-base:{base_revision}"),
                format!("git-branch:{branch}"),
                format!("managed-root:{}", self.managed_root.display()),
            ],
        };
        CorrelationIds::for_goal(goal_id)
            .with_worker(worker_id.as_str())
            .with_workspace(&binding.worktree_path)
            .emit_info("workspace.created", "ready");
        Ok(binding)
    }

    pub fn inspect(&self, binding: &WorkspaceBinding) -> Result<WorkspaceState, WorkspaceError> {
        if binding.repository_id != self.repository_id {
            return Err(WorkspaceError::UnsafePath(
                "workspace belongs to a different repository".into(),
            ));
        }
        let worktree = PathBuf::from(&binding.worktree_path).canonicalize()?;
        if worktree.parent() != Some(self.managed_root.as_path()) {
            return Err(WorkspaceError::UnsafePath(worktree.display().to_string()));
        }
        let status = git_bytes(
            &worktree,
            ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
        )?;
        let mut staged_paths = Vec::new();
        let mut unstaged_paths = Vec::new();
        let mut untracked_paths = Vec::new();
        let mut conflicted_paths = Vec::new();
        for entry in status
            .split(|byte| *byte == 0)
            .filter(|entry| !entry.is_empty())
        {
            if entry.len() < 3 {
                continue;
            }
            let index = entry[0] as char;
            let worktree_state = entry[1] as char;
            let path = String::from_utf8_lossy(&entry[3..]).to_string();
            if index == '?' && worktree_state == '?' {
                untracked_paths.push(path);
                continue;
            }
            if matches!(
                (index, worktree_state),
                ('D', 'D')
                    | ('A', 'U')
                    | ('U', 'D')
                    | ('U', 'A')
                    | ('D', 'U')
                    | ('A', 'A')
                    | ('U', 'U')
            ) {
                conflicted_paths.push(path.clone());
            }
            if !matches!(index, ' ' | '?') {
                staged_paths.push(path.clone());
            }
            if !matches!(worktree_state, ' ' | '?') {
                unstaged_paths.push(path);
            }
        }
        let branch = git_output(&worktree, ["rev-parse", "--abbrev-ref", "HEAD"])?
            .trim()
            .to_string();
        let head_revision = git_output(&worktree, ["rev-parse", "HEAD"])?
            .trim()
            .to_string();
        let unpublished_base =
            upstream_revision(&worktree).unwrap_or_else(|| binding.base_revision.clone());
        let unpublished_commits = git_output_owned(
            &worktree,
            [
                "rev-list".into(),
                "--count".into(),
                format!("{unpublished_base}..HEAD"),
            ],
        )?
        .trim()
        .parse::<u64>()
        .map_err(|error| WorkspaceError::GitFailed {
            command: "git rev-list --count".into(),
            message: error.to_string(),
        })?;
        Ok(WorkspaceState {
            branch,
            head_revision,
            staged_paths,
            unstaged_paths,
            untracked_paths,
            conflicted_paths,
            unpublished_commits,
        })
    }

    pub fn inventory(&self) -> Result<Vec<WorktreeInventoryEntry>, WorkspaceError> {
        let output = git_output(&self.repository_root, ["worktree", "list", "--porcelain"])?;
        let mut entries = Vec::new();
        let mut path: Option<String> = None;
        let mut head_revision = String::new();
        let mut branch = None;
        let mut prunable = false;
        for line in output.lines().chain(std::iter::once("")) {
            if line.is_empty() {
                if let Some(path) = path.take() {
                    entries.push(WorktreeInventoryEntry {
                        path,
                        head_revision: std::mem::take(&mut head_revision),
                        branch: branch.take(),
                        prunable,
                    });
                }
                prunable = false;
            } else if let Some(value) = line.strip_prefix("worktree ") {
                path = Some(value.into());
            } else if let Some(value) = line.strip_prefix("HEAD ") {
                head_revision = value.into();
            } else if let Some(value) = line.strip_prefix("branch refs/heads/") {
                branch = Some(value.into());
            } else if line == "prunable" || line.starts_with("prunable ") {
                prunable = true;
            }
        }
        Ok(entries)
    }

    pub fn reconcile(
        &self,
        bindings: &[(WorkerId, WorkspaceBinding)],
    ) -> Result<WorkspaceReconciliation, WorkspaceError> {
        let inventory = self.inventory()?;
        let managed_inventory = inventory
            .iter()
            .filter(|entry| Path::new(&entry.path).starts_with(&self.managed_root))
            .cloned()
            .collect::<Vec<_>>();
        let mut attached_worker_ids = Vec::new();
        let mut missing_worker_ids = Vec::new();
        for (worker_id, binding) in bindings {
            if binding.repository_id != self.repository_id {
                continue;
            }
            if managed_inventory
                .iter()
                .any(|entry| entry.path == binding.worktree_path && !entry.prunable)
            {
                attached_worker_ids.push(worker_id.clone());
            } else {
                missing_worker_ids.push(worker_id.clone());
            }
        }
        let bound_paths = bindings
            .iter()
            .filter(|(_, binding)| binding.repository_id == self.repository_id)
            .map(|(_, binding)| binding.worktree_path.as_str())
            .collect::<std::collections::HashSet<_>>();
        let orphaned_worktrees = managed_inventory
            .into_iter()
            .filter(|entry| !bound_paths.contains(entry.path.as_str()))
            .collect::<Vec<_>>();
        let inventory_paths = inventory
            .iter()
            .map(|entry| PathBuf::from(&entry.path))
            .collect::<std::collections::HashSet<_>>();
        let mut unregistered_directories = Vec::new();
        for entry in fs::read_dir(&self.managed_root)? {
            let path = entry?.path();
            if path.is_dir()
                && path.file_name() != Some(OsStr::new(".quarantine"))
                && path.file_name() != Some(OsStr::new(".cleanup-records"))
                && !inventory_paths.contains(&path)
            {
                unregistered_directories.push(path.display().to_string());
            }
        }
        attached_worker_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        missing_worker_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        unregistered_directories.sort();
        let reconciliation = WorkspaceReconciliation {
            attached_worker_ids,
            missing_worker_ids,
            orphaned_worktrees,
            unregistered_directories,
        };
        CorrelationIds::default()
            .with_workspace(&self.repository_id)
            .emit_info("workspace.reconciled", "completed");
        Ok(reconciliation)
    }

    pub fn recover_binding(
        &self,
        binding: &WorkspaceBinding,
    ) -> Result<WorkspaceBinding, WorkspaceError> {
        if binding.repository_id != self.repository_id {
            return Err(WorkspaceError::UnsafePath(
                "workspace belongs to a different repository".into(),
            ));
        }
        let expected_path = PathBuf::from(&binding.worktree_path);
        validate_new_worktree_path(&self.managed_root, &expected_path)?;
        if expected_path.exists() {
            return Err(WorkspaceError::WorktreeExists(
                expected_path.display().to_string(),
            ));
        }
        if !self.branch_exists(&binding.branch)? {
            return Err(WorkspaceError::GitFailed {
                command: "git worktree add".into(),
                message: format!("recovery branch does not exist: {}", binding.branch),
            });
        }
        self.git([OsStr::new("worktree"), OsStr::new("prune")])?;
        self.git([
            OsStr::new("worktree"),
            OsStr::new("add"),
            expected_path.as_os_str(),
            OsStr::new(&binding.branch),
        ])?;
        let canonical = expected_path.canonicalize()?;
        if canonical.parent() != Some(self.managed_root.as_path()) {
            return Err(WorkspaceError::UnsafePath(canonical.display().to_string()));
        }
        let mut recovered = binding.clone();
        recovered.worktree_path = canonical.display().to_string();
        recovered
            .creation_evidence
            .push("recovered-existing-branch".into());
        CorrelationIds::default()
            .with_workspace(&recovered.worktree_path)
            .emit_info("workspace.recovered", "ready");
        Ok(recovered)
    }

    pub fn quarantine(
        &self,
        entry: &WorktreeInventoryEntry,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<WorkspaceQuarantineRecord, WorkspaceError> {
        let canonical = PathBuf::from(&entry.path).canonicalize()?;
        if canonical.parent() != Some(self.managed_root.as_path()) {
            return Err(WorkspaceError::UnsafePath(canonical.display().to_string()));
        }
        let records = self.managed_root.join(".quarantine");
        fs::create_dir_all(&records)?;
        let record_path = records.join(format!(
            "{}-{}.json",
            safe_component(
                canonical
                    .file_name()
                    .and_then(OsStr::to_str)
                    .unwrap_or("worktree")
            ),
            now.timestamp_millis()
        ));
        let record = WorkspaceQuarantineRecord {
            worktree_path: canonical.display().to_string(),
            branch: entry.branch.clone(),
            reason: redact_sensitive_text(reason),
            recorded_at: now,
            record_path: record_path.display().to_string(),
        };
        fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
        CorrelationIds::default()
            .with_workspace(&record.worktree_path)
            .emit_warn("workspace.quarantined", "quarantined");
        Ok(record)
    }

    pub fn cleanup(
        &self,
        binding: &WorkspaceBinding,
        authorization: CleanupAuthorization,
        now: DateTime<Utc>,
    ) -> Result<WorkspaceCleanupRecord, WorkspaceError> {
        let state = self.inspect(binding)?;
        let (authorization_text, force) = match authorization {
            CleanupAuthorization::Integrated {
                integration_artifact_id,
                expected_head_revision,
            } => {
                if integration_artifact_id.trim().is_empty()
                    || state.head_revision != expected_head_revision
                    || state.has_dirty_files()
                {
                    return Err(WorkspaceError::CleanupNotAuthorized(
                        "integrated cleanup requires artifact evidence, the expected HEAD, and no dirty files"
                            .into(),
                    ));
                }
                (format!("integrated:{integration_artifact_id}"), false)
            }
            CleanupAuthorization::ExplicitDiscard { decided_by, reason } => {
                if decided_by.trim().is_empty() || reason.trim().is_empty() {
                    return Err(WorkspaceError::CleanupNotAuthorized(
                        "explicit discard requires an attributed decision and reason".into(),
                    ));
                }
                (format!("discarded:{decided_by}:{reason}"), true)
            }
        };
        let authorization_text = redact_sensitive_text(&authorization_text);
        let records = self.managed_root.join(".cleanup-records");
        fs::create_dir_all(&records)?;
        let record_path = records.join(format!(
            "{}-{}.json",
            safe_component(&binding.branch),
            now.timestamp_millis()
        ));
        let record = WorkspaceCleanupRecord {
            worktree_path: binding.worktree_path.clone(),
            retained_branch: binding.branch.clone(),
            head_revision: state.head_revision,
            authorization: authorization_text,
            recorded_at: now,
            record_path: record_path.display().to_string(),
        };
        fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
        let mut arguments = vec![OsStr::new("worktree"), OsStr::new("remove")];
        if force {
            arguments.push(OsStr::new("--force"));
        }
        arguments.push(OsStr::new(&binding.worktree_path));
        self.git(arguments)?;
        CorrelationIds::default()
            .with_workspace(&binding.worktree_path)
            .emit_info("workspace.cleaned", "removed");
        Ok(record)
    }

    fn resolve_commit(&self, base_ref: &str) -> Result<String, WorkspaceError> {
        if base_ref.trim().is_empty() {
            return Err(WorkspaceError::GitFailed {
                command: "git rev-parse".into(),
                message: "base ref must not be empty".into(),
            });
        }
        Ok(git_output(
            &self.repository_root,
            ["rev-parse", "--verify", &format!("{base_ref}^{{commit}}")],
        )?
        .trim()
        .to_string())
    }

    fn branch_exists(&self, branch: &str) -> Result<bool, WorkspaceError> {
        let output = Command::new("git")
            .arg("-C")
            .arg(&self.repository_root)
            .args(["show-ref", "--verify", "--quiet"])
            .arg(format!("refs/heads/{branch}"))
            .output()?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(git_error("git show-ref --verify --quiet", output)),
        }
    }

    fn git<I, S>(&self, args: I) -> Result<Output, WorkspaceError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = Command::new("git")
            .arg("-C")
            .arg(&self.repository_root)
            .args(args)
            .output()?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(git_error("git worktree add", output))
        }
    }
}

fn validate_repository_root(repository_root: &Path) -> Result<(), WorkspaceError> {
    if repository_root.parent().is_none() || !repository_root.is_dir() {
        return Err(WorkspaceError::InvalidRepository(
            repository_root.display().to_string(),
        ));
    }
    let top_level = git_output(repository_root, ["rev-parse", "--show-toplevel"])?;
    let top_level = PathBuf::from(top_level.trim()).canonicalize()?;
    if top_level != repository_root {
        return Err(WorkspaceError::InvalidRepository(
            repository_root.display().to_string(),
        ));
    }
    Ok(())
}

fn validate_new_worktree_path(managed_root: &Path, candidate: &Path) -> Result<(), WorkspaceError> {
    if candidate.parent() != Some(managed_root)
        || candidate
            .file_name()
            .is_none_or(|name| name.is_empty() || name == "." || name == "..")
        || candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(WorkspaceError::UnsafePath(candidate.display().to_string()));
    }
    Ok(())
}

fn repository_identity(path: &Path) -> String {
    let digest = Sha256::digest(path.as_os_str().as_encoded_bytes());
    format!("repo-{}", hex_prefix(&digest, 16))
}

fn safe_component(value: &str) -> String {
    let mut component = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while component.contains("--") {
        component = component.replace("--", "-");
    }
    component = component.trim_matches('-').to_string();
    if component.is_empty() {
        "worker".into()
    } else {
        component.chars().take(80).collect()
    }
}

fn hex_prefix(bytes: &[u8], length: usize) -> String {
    bytes
        .iter()
        .flat_map(|byte| format!("{byte:02x}").chars().collect::<Vec<_>>())
        .take(length)
        .collect()
}

fn git_output<const N: usize>(cwd: &Path, args: [&str; N]) -> Result<String, WorkspaceError> {
    let output = Command::new("git").arg("-C").arg(cwd).args(args).output()?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(git_error("git", output))
    }
}

fn git_bytes<const N: usize>(cwd: &Path, args: [&str; N]) -> Result<Vec<u8>, WorkspaceError> {
    let output = Command::new("git").arg("-C").arg(cwd).args(args).output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(git_error("git", output))
    }
}

fn git_output_owned<const N: usize>(
    cwd: &Path,
    args: [String; N],
) -> Result<String, WorkspaceError> {
    let output = Command::new("git").arg("-C").arg(cwd).args(args).output()?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(git_error("git", output))
    }
}

fn upstream_revision(worktree: &Path) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["rev-parse", "--verify", "@{upstream}^{commit}"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_error(command: &str, output: Output) -> WorkspaceError {
    let message = String::from_utf8_lossy(&output.stderr).trim().to_string();
    WorkspaceError::GitFailed {
        command: command.into(),
        message: if message.is_empty() {
            format!("exit status {}", output.status)
        } else {
            message
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;

    struct TestRepository {
        _directory: tempfile::TempDir,
        path: PathBuf,
    }

    fn git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn repository() -> TestRepository {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("repository");
        fs::create_dir(&path).unwrap();
        git(&path, &["init", "-q"]);
        git(&path, &["config", "user.email", "golazo@example.test"]);
        git(&path, &["config", "user.name", "Golazo Tests"]);
        fs::write(path.join("README.md"), "initial\n").unwrap();
        git(&path, &["add", "README.md"]);
        git(&path, &["commit", "-q", "-m", "initial"]);
        TestRepository {
            _directory: directory,
            path,
        }
    }

    #[test]
    fn creates_deterministic_isolated_worktree_with_base_evidence() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let worker_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000001").unwrap();
        let now = Utc::now();

        let binding = manager
            .create_for_worker("Goal A", &worker_id, "HEAD", now)
            .unwrap();

        assert_eq!(
            binding.branch,
            "codex/golazo/goal-a/worker-00000000-0000-4000-8000-000000000001"
        );
        assert!(Path::new(&binding.worktree_path).is_dir());
        assert!(Path::new(&binding.worktree_path).starts_with(manager.managed_root()));
        assert_eq!(binding.created_at, Some(now));
        assert_eq!(binding.creation_evidence.len(), 3);
        assert_eq!(
            git_output(Path::new(&binding.worktree_path), ["rev-parse", "HEAD"])
                .unwrap()
                .trim(),
            binding.base_revision
        );
    }

    #[test]
    fn rejects_non_repository_and_duplicate_worker_workspace() {
        let not_repository = tempfile::tempdir().unwrap();
        assert!(matches!(
            WorktreeManager::open(not_repository.path()),
            Err(WorkspaceError::GitFailed { .. })
        ));

        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let worker_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000002").unwrap();
        manager
            .create_for_worker("goal-a", &worker_id, "HEAD", Utc::now())
            .unwrap();
        assert!(matches!(
            manager.create_for_worker("goal-a", &worker_id, "HEAD", Utc::now()),
            Err(WorkspaceError::WorktreeExists(_))
        ));
    }

    #[test]
    fn managed_root_is_a_repository_sibling_with_no_parent_components() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        assert_eq!(
            manager.managed_root().parent().and_then(Path::parent),
            repository.path.canonicalize().unwrap().parent()
        );
        assert!(!manager.managed_root().starts_with(&repository.path));
        assert!(
            manager
                .managed_root()
                .components()
                .all(|component| !matches!(component, Component::ParentDir))
        );
    }

    #[test]
    fn inspects_staged_unstaged_untracked_and_unpublished_state() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let worker_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000003").unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker_id, "HEAD", Utc::now())
            .unwrap();
        let worktree = Path::new(&binding.worktree_path);

        assert!(
            manager
                .inspect(&binding)
                .unwrap()
                .safe_for_automatic_cleanup()
        );
        fs::write(worktree.join("README.md"), "worker edit\n").unwrap();
        let unstaged = manager.inspect(&binding).unwrap();
        assert_eq!(unstaged.unstaged_paths, vec!["README.md"]);
        assert!(!unstaged.safe_for_automatic_cleanup());

        git(worktree, &["add", "README.md"]);
        let staged = manager.inspect(&binding).unwrap();
        assert_eq!(staged.staged_paths, vec!["README.md"]);
        git(worktree, &["commit", "-q", "-m", "worker edit"]);
        let committed = manager.inspect(&binding).unwrap();
        assert_eq!(committed.unpublished_commits, 1);
        assert!(!committed.safe_for_automatic_cleanup());

        fs::write(worktree.join("notes.txt"), "untracked\n").unwrap();
        assert_eq!(
            manager.inspect(&binding).unwrap().untracked_paths,
            vec!["notes.txt"]
        );
    }

    #[test]
    fn inspects_unresolved_merge_conflicts() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let worker_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000004").unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker_id, "HEAD", Utc::now())
            .unwrap();
        let worktree = Path::new(&binding.worktree_path);
        let main_branch = git_output(&repository.path, ["branch", "--show-current"])
            .unwrap()
            .trim()
            .to_string();
        fs::write(worktree.join("README.md"), "worker edit\n").unwrap();
        git(worktree, &["add", "README.md"]);
        git(worktree, &["commit", "-q", "-m", "worker edit"]);
        fs::write(repository.path.join("README.md"), "main edit\n").unwrap();
        git(&repository.path, &["add", "README.md"]);
        git(&repository.path, &["commit", "-q", "-m", "main edit"]);

        let merge = Command::new("git")
            .arg("-C")
            .arg(worktree)
            .args(["merge", &main_branch])
            .output()
            .unwrap();
        assert!(!merge.status.success());
        let state = manager.inspect(&binding).unwrap();
        assert_eq!(state.conflicted_paths, vec!["README.md"]);
        assert!(!state.safe_for_automatic_cleanup());
    }

    #[test]
    fn reconciles_quarantines_and_recovers_orphaned_worktrees() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let first_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000005").unwrap();
        let second_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000006").unwrap();
        let first = manager
            .create_for_worker("goal-a", &first_id, "HEAD", Utc::now())
            .unwrap();
        let second = manager
            .create_for_worker("goal-a", &second_id, "HEAD", Utc::now())
            .unwrap();

        let reconciliation = manager
            .reconcile(&[(first_id.clone(), first.clone())])
            .unwrap();
        assert_eq!(reconciliation.attached_worker_ids, vec![first_id.clone()]);
        assert_eq!(reconciliation.orphaned_worktrees.len(), 1);
        assert_eq!(
            reconciliation.orphaned_worktrees[0].path,
            second.worktree_path
        );
        let quarantine = manager
            .quarantine(
                &reconciliation.orphaned_worktrees[0],
                "no durable worker owns this worktree; token=quarantine-secret",
                Utc::now(),
            )
            .unwrap();
        assert!(Path::new(&quarantine.record_path).is_file());
        assert!(Path::new(&quarantine.worktree_path).is_dir());
        assert!(!quarantine.reason.contains("quarantine-secret"));
        assert!(
            !fs::read_to_string(&quarantine.record_path)
                .unwrap()
                .contains("quarantine-secret")
        );

        git(
            &repository.path,
            &["worktree", "remove", "--force", &first.worktree_path],
        );
        let missing = manager
            .reconcile(&[(first_id.clone(), first.clone())])
            .unwrap();
        assert_eq!(missing.missing_worker_ids, vec![first_id]);
        let recovered = manager.recover_binding(&first).unwrap();
        assert!(Path::new(&recovered.worktree_path).is_dir());
        assert!(
            recovered
                .creation_evidence
                .contains(&"recovered-existing-branch".to_string())
        );
    }

    #[test]
    fn cleanup_requires_integration_evidence_or_an_attributed_discard() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let integrated_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000007").unwrap();
        let integrated = manager
            .create_for_worker("goal-a", &integrated_id, "HEAD", Utc::now())
            .unwrap();
        let head = manager.inspect(&integrated).unwrap().head_revision;

        assert!(matches!(
            manager.cleanup(
                &integrated,
                CleanupAuthorization::Integrated {
                    integration_artifact_id: "".into(),
                    expected_head_revision: head.clone(),
                },
                Utc::now(),
            ),
            Err(WorkspaceError::CleanupNotAuthorized(_))
        ));
        let record = manager
            .cleanup(
                &integrated,
                CleanupAuthorization::Integrated {
                    integration_artifact_id: "artifact-1".into(),
                    expected_head_revision: head,
                },
                Utc::now(),
            )
            .unwrap();
        assert!(!Path::new(&integrated.worktree_path).exists());
        assert!(Path::new(&record.record_path).is_file());
        assert!(manager.branch_exists(&integrated.branch).unwrap());

        let discarded_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000008").unwrap();
        let discarded = manager
            .create_for_worker("goal-a", &discarded_id, "HEAD", Utc::now())
            .unwrap();
        fs::write(
            Path::new(&discarded.worktree_path).join("unsaved.txt"),
            "discard me\n",
        )
        .unwrap();
        let discarded_record = manager
            .cleanup(
                &discarded,
                CleanupAuthorization::ExplicitDiscard {
                    decided_by: "user@example.test".into(),
                    reason: "user confirmed this experiment is unwanted; password=discard-secret"
                        .into(),
                },
                Utc::now(),
            )
            .unwrap();
        assert!(!Path::new(&discarded.worktree_path).exists());
        assert!(discarded_record.authorization.starts_with("discarded:"));
        assert!(!discarded_record.authorization.contains("discard-secret"));
        assert!(
            !fs::read_to_string(&discarded_record.record_path)
                .unwrap()
                .contains("discard-secret")
        );
        assert!(manager.branch_exists(&discarded.branch).unwrap());
    }

    #[test]
    fn non_git_workspace_is_explicitly_limited_to_one_worker() {
        let non_git = tempfile::tempdir().unwrap();
        let capability = WorkspaceIsolationCapability::detect(non_git.path()).unwrap();
        assert!(matches!(
            capability,
            WorkspaceIsolationCapability::SingleWorkerOnly { .. }
        ));
        assert!(capability.allows_concurrency(1));
        assert!(!capability.allows_concurrency(2));

        let repository = repository();
        let capability = WorkspaceIsolationCapability::detect(&repository.path).unwrap();
        assert!(matches!(
            capability,
            WorkspaceIsolationCapability::GitWorktrees { .. }
        ));
        assert!(capability.allows_concurrency(4));
    }

    #[test]
    fn concurrent_creation_leaves_one_worktree_and_one_branch() {
        let repository = repository();
        let manager = Arc::new(WorktreeManager::open(&repository.path).unwrap());
        let barrier = Arc::new(Barrier::new(2));
        let attempts = (0..2)
            .map(|_| {
                let manager = Arc::clone(&manager);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let worker_id =
                        WorkerId::parse("worker-00000000-0000-4000-8000-000000000009").unwrap();
                    barrier.wait();
                    manager.create_for_worker("goal-a", &worker_id, "HEAD", Utc::now())
                })
            })
            .collect::<Vec<_>>();
        let results = attempts
            .into_iter()
            .map(|attempt| attempt.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            manager
                .inventory()
                .unwrap()
                .iter()
                .filter(|entry| Path::new(&entry.path).starts_with(manager.managed_root()))
                .count(),
            1
        );
    }

    #[test]
    fn repository_move_invalidates_old_repository_identity() {
        let repository = repository();
        let manager = WorktreeManager::open(&repository.path).unwrap();
        let worker_id = WorkerId::parse("worker-00000000-0000-4000-8000-000000000010").unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker_id, "HEAD", Utc::now())
            .unwrap();
        drop(manager);
        let moved = repository.path.with_file_name("repository-moved");
        fs::rename(&repository.path, &moved).unwrap();
        let moved_manager = WorktreeManager::open(&moved).unwrap();

        assert!(matches!(
            moved_manager.inspect(&binding),
            Err(WorkspaceError::UnsafePath(_))
        ));
    }
}
