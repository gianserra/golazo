use super::domain::{Claim, ClaimId, ClaimScope, ContractId, Worker, WorkerId, WorkspaceBinding};
use super::store::{
    ContractRepository, IntegrationArtifactRepository, SqliteCoordinationStore, StoreError,
};
use super::workspace::{WorkspaceError, WorkspaceState, WorktreeManager};
use crate::redaction::redact_sensitive_text;
use crate::tracker::Tracker;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

pub const PARTIAL_WORK_ARTIFACT_SCHEMA: &str = "golazo.partial-work.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialWorkArtifactState {
    Preserved,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractRevisionSnapshot {
    pub contract_id: ContractId,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialWorkRecovery {
    pub claim_id: ClaimId,
    pub worker_id: WorkerId,
    pub recovered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialWorkArtifact {
    pub schema: String,
    pub id: String,
    pub goal_id: String,
    pub scope: ClaimScope,
    pub source_claim_id: ClaimId,
    pub source_lease_generation: u64,
    pub source_worker_id: WorkerId,
    pub source_run_id: Option<String>,
    pub source_thread_id: Option<String>,
    pub failure_code: Option<String>,
    pub failure_reason: String,
    pub workspace: WorkspaceBinding,
    pub workspace_state: WorkspaceState,
    pub contract_revisions: Vec<ContractRevisionSnapshot>,
    pub integration_artifact_ids: Vec<String>,
    pub state: PartialWorkArtifactState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub recovery: Option<PartialWorkRecovery>,
}

#[derive(Debug, Error)]
pub enum PartialWorkError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error("partial-work artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("partial-work artifact serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("preserved work cannot be recovered safely: {0}")]
    UnsafeRecovery(String),
}

pub fn preserve_failed_work(
    tracker: &Tracker,
    store: Arc<SqliteCoordinationStore>,
    manager: &WorktreeManager,
    worker: &Worker,
    claim: &Claim,
    run_id: Option<&str>,
    failure_code: Option<&str>,
    failure_reason: &str,
    now: DateTime<Utc>,
) -> Result<Option<PartialWorkArtifact>, PartialWorkError> {
    let Some(workspace) = worker.workspace.clone() else {
        return Ok(None);
    };
    let workspace_state = manager.inspect(&workspace)?;
    if workspace_state.safe_for_automatic_cleanup() {
        return Ok(None);
    }
    validate_dirty_paths(&workspace_state)?;
    let mut contract_revisions = store
        .contracts_for_goal(&claim.goal_id)?
        .into_iter()
        .map(|contract| ContractRevisionSnapshot {
            contract_id: contract.id,
            revision: contract.revision,
        })
        .collect::<Vec<_>>();
    contract_revisions
        .sort_by(|left, right| left.contract_id.as_str().cmp(right.contract_id.as_str()));
    let mut integration_artifact_ids = store
        .integration_artifacts_for_goal(&claim.goal_id)?
        .into_iter()
        .filter(|artifact| artifact.claim_id == claim.id)
        .map(|artifact| artifact.id.as_str().to_string())
        .collect::<Vec<_>>();
    integration_artifact_ids.sort();
    let artifact = PartialWorkArtifact {
        schema: PARTIAL_WORK_ARTIFACT_SCHEMA.into(),
        id: format!("partial-work-{}", Uuid::new_v4()),
        goal_id: claim.goal_id.clone(),
        scope: claim.scope.clone(),
        source_claim_id: claim.id.clone(),
        source_lease_generation: claim.lease_generation,
        source_worker_id: worker.id.clone(),
        source_run_id: run_id.map(str::to_string),
        source_thread_id: worker.current_thread_id.clone(),
        failure_code: failure_code.map(str::to_string),
        failure_reason: redact_sensitive_text(failure_reason),
        workspace,
        workspace_state,
        contract_revisions,
        integration_artifact_ids,
        state: PartialWorkArtifactState::Preserved,
        created_at: now,
        updated_at: now,
        recovery: None,
    };
    write_artifact(tracker, &artifact)?;
    Ok(Some(artifact))
}

pub fn partial_work_for_goal(
    tracker: &Tracker,
    goal_id: &str,
) -> Result<Vec<PartialWorkArtifact>, PartialWorkError> {
    let root = artifact_root(tracker);
    if !root.is_dir() {
        return Ok(Vec::new());
    }
    let mut artifacts = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }
        let artifact: PartialWorkArtifact = serde_json::from_slice(&fs::read(path)?)?;
        if artifact.schema == PARTIAL_WORK_ARTIFACT_SCHEMA && artifact.goal_id == goal_id {
            artifacts.push(artifact);
        }
    }
    artifacts.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    Ok(artifacts)
}

pub fn preserved_work_for_scope(
    tracker: &Tracker,
    goal_id: &str,
    scope: &ClaimScope,
) -> Result<Option<PartialWorkArtifact>, PartialWorkError> {
    Ok(partial_work_for_goal(tracker, goal_id)?
        .into_iter()
        .find(|artifact| {
            artifact.state == PartialWorkArtifactState::Preserved && artifact.scope == *scope
        }))
}

pub fn validate_recovery_candidate(
    store: Arc<SqliteCoordinationStore>,
    manager: &WorktreeManager,
    artifact: &PartialWorkArtifact,
    claim: &Claim,
    source_worker: &Worker,
    replacement_worker: &Worker,
) -> Result<(), PartialWorkError> {
    if artifact.state != PartialWorkArtifactState::Preserved
        || artifact.goal_id != claim.goal_id
        || artifact.scope != claim.scope
        || artifact.source_worker_id != source_worker.id
        || artifact.source_claim_id == claim.id
        || artifact.source_lease_generation >= claim.lease_generation
    {
        return Err(PartialWorkError::UnsafeRecovery(
            "claim scope or lease lineage does not match the preserved artifact".into(),
        ));
    }
    if artifact.workspace.base_revision != claim.base_revision {
        return Err(PartialWorkError::UnsafeRecovery(format!(
            "base revision changed from {} to {}",
            artifact.workspace.base_revision, claim.base_revision
        )));
    }
    if source_worker.workspace.as_ref() != Some(&artifact.workspace)
        || !source_worker.active_claims.is_empty()
        || source_worker.current_run_id.is_some()
        || !source_worker.state.is_terminal()
    {
        return Err(PartialWorkError::UnsafeRecovery(
            "source worker no longer owns an idle terminal preserved workspace".into(),
        ));
    }
    if replacement_worker.workspace.is_some()
        || !replacement_worker.active_claims.contains(&claim.id)
        || claim.owner != replacement_worker.id
    {
        return Err(PartialWorkError::UnsafeRecovery(
            "replacement worker is not the active unbound claim owner".into(),
        ));
    }
    if !artifact.integration_artifact_ids.is_empty() {
        return Err(PartialWorkError::UnsafeRecovery(
            "the failed claim already produced an integration artifact".into(),
        ));
    }
    if artifact.workspace_state.unpublished_commits > 0 {
        return Err(PartialWorkError::UnsafeRecovery(
            "the preserved workspace contains worker-created commits and requires manual reconciliation"
                .into(),
        ));
    }
    let current_state = manager.inspect(&artifact.workspace)?;
    validate_dirty_paths(&current_state)?;
    if current_state != artifact.workspace_state {
        return Err(PartialWorkError::UnsafeRecovery(
            "preserved workspace changed after the recovery artifact was captured".into(),
        ));
    }
    let mut current_contracts = store
        .contracts_for_goal(&claim.goal_id)?
        .into_iter()
        .map(|contract| ContractRevisionSnapshot {
            contract_id: contract.id,
            revision: contract.revision,
        })
        .collect::<Vec<_>>();
    current_contracts
        .sort_by(|left, right| left.contract_id.as_str().cmp(right.contract_id.as_str()));
    if current_contracts != artifact.contract_revisions {
        return Err(PartialWorkError::UnsafeRecovery(
            "shared contract revisions changed after the work was abandoned".into(),
        ));
    }
    Ok(())
}

pub fn mark_recovered(
    tracker: &Tracker,
    artifact: &mut PartialWorkArtifact,
    claim: &Claim,
    worker: &Worker,
    now: DateTime<Utc>,
) -> Result<(), PartialWorkError> {
    artifact.state = PartialWorkArtifactState::Recovered;
    artifact.updated_at = now;
    artifact.recovery = Some(PartialWorkRecovery {
        claim_id: claim.id.clone(),
        worker_id: worker.id.clone(),
        recovered_at: now,
    });
    write_artifact(tracker, artifact)
}

fn validate_dirty_paths(state: &WorkspaceState) -> Result<(), PartialWorkError> {
    for value in state
        .staged_paths
        .iter()
        .chain(&state.unstaged_paths)
        .chain(&state.untracked_paths)
        .chain(&state.conflicted_paths)
    {
        let path = Path::new(value);
        if path.is_absolute()
            || path
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
        {
            return Err(PartialWorkError::UnsafeRecovery(format!(
                "dirty path is outside the isolated workspace: {value}"
            )));
        }
    }
    Ok(())
}

fn artifact_root(tracker: &Tracker) -> PathBuf {
    tracker.root.join("recovery-artifacts")
}

fn artifact_path(tracker: &Tracker, id: &str) -> PathBuf {
    artifact_root(tracker).join(format!("{id}.json"))
}

fn write_artifact(
    tracker: &Tracker,
    artifact: &PartialWorkArtifact,
) -> Result<(), PartialWorkError> {
    let root = artifact_root(tracker);
    fs::create_dir_all(&root)?;
    let destination = artifact_path(tracker, &artifact.id);
    let temporary = root.join(format!(".{}.{}.tmp", artifact.id, Uuid::new_v4()));
    fs::write(&temporary, serde_json::to_vec_pretty(artifact)?)?;
    fs::rename(temporary, destination)?;
    Ok(())
}
