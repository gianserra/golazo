use super::claims::{ClaimService, ClaimServiceError};
use super::contracts::{ContractRegistry, ContractRegistryError};
use super::domain::{
    ActivityCategory, ClaimId, ClaimState, ContractMismatchEventPayload, CoordinationActor,
    CoordinationEvent, CoordinationEventPayload, DomainError, EventId, EventSeverity,
    FileOverlapEventPayload, FileOverlapInput, MergeConflictEventPayload,
    MigrationOverlapEventPayload, MigrationOverlapInput, RepeatedValidationFailureEventPayload,
    SymbolOverlapEventPayload, SymbolOverlapInput, WatchdogActionEventPayload, WatchdogActionKind,
    WorkerId, WorkerStalledEventPayload, WorkerState,
};
use super::pool::{PoolMode, validate_worker_permissions};
use super::security::{
    ResourceQuotaPolicy, ResourceUsage, goal_quota_violation, validate_resource_quota_policy,
    worker_quota_violation,
};
use super::store::{
    ClaimRepository, EventRepository, PoolRepository, SqliteCoordinationStore, WorkerRepository,
};
use super::workspace::{WorkspaceError, WorktreeManager};
use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use thiserror::Error;

const MAX_CHANGED_PATHS: usize = 512;
const MAX_PATCH_BYTES: usize = 64 * 1024;
const MAX_SYMBOLS: usize = 512;
const MAX_VALIDATION_RESULTS: usize = 100;
const MAX_MERGE_DIAGNOSTIC_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthDetectionPolicy {
    pub heartbeat_stale_after_seconds: u64,
    pub activity_stale_after_seconds: u64,
    pub validation_failure_threshold: usize,
    pub validation_window_seconds: u64,
}

impl Default for HealthDetectionPolicy {
    fn default() -> Self {
        Self {
            heartbeat_stale_after_seconds: 120,
            activity_stale_after_seconds: 300,
            validation_failure_threshold: 3,
            validation_window_seconds: 15 * 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchdogPolicy {
    pub lease_grace_seconds: u64,
    pub max_consecutive_failures: usize,
    pub resource_quotas: ResourceQuotaPolicy,
}

impl Default for WatchdogPolicy {
    fn default() -> Self {
        Self {
            lease_grace_seconds: 60,
            max_consecutive_failures: 3,
            resource_quotas: ResourceQuotaPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct WorkerRuntimeObservation {
    pub worker_id: WorkerId,
    pub environment_alive: bool,
    pub active_turn_started_at: Option<DateTime<Utc>>,
    pub observed_tokens: Option<u64>,
    #[serde(default)]
    pub active_processes: usize,
    #[serde(default)]
    pub observed_disk_bytes: u64,
    #[serde(default)]
    pub observed_network_requests: u64,
    #[serde(default)]
    pub retry_attempts: usize,
    pub consecutive_failures: usize,
    pub permission_violation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchdogOutcomeKind {
    LeaseExpired,
    EnvironmentRecovery,
    PermissionViolation,
    ExecutionLimit,
    ResourceQuota,
    FailureLoopCutoff,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchdogOutcome {
    pub worker_id: WorkerId,
    pub claim_ids: Vec<ClaimId>,
    pub kind: WatchdogOutcomeKind,
    pub reason: String,
    pub event_sequence: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchdogReport {
    pub goal_id: String,
    pub observed_at: DateTime<Utc>,
    pub outcomes: Vec<WatchdogOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeInventory {
    pub worker_id: WorkerId,
    pub claim_id: ClaimId,
    pub goal_id: String,
    pub repository_id: String,
    pub base_revision: String,
    pub head_revision: String,
    pub generated_at: DateTime<Utc>,
    pub changed_files: Vec<ChangedFile>,
    pub diff: DiffInventory,
    pub changed_symbols: Vec<ChangedSymbol>,
    pub migrations: Vec<ClassifiedPath>,
    pub dependency_files: Vec<DependencyFile>,
    pub generated_artifacts: Vec<ClassifiedPath>,
    pub validation_results: Vec<ValidationInventory>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangedFile {
    pub path: String,
    pub previous_path: Option<String>,
    pub status: FileChangeStatus,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
    pub binary: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChangeStatus {
    Added,
    Copied,
    Deleted,
    Modified,
    Renamed,
    TypeChanged,
    Unmerged,
    Untracked,
    Unknown,
}

impl FileChangeStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Copied => "copied",
            Self::Deleted => "deleted",
            Self::Modified => "modified",
            Self::Renamed => "renamed",
            Self::TypeChanged => "type_changed",
            Self::Unmerged => "unmerged",
            Self::Untracked => "untracked",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiffInventory {
    pub patch: String,
    pub patch_sha256: String,
    pub patch_truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangedSymbol {
    pub path: String,
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassifiedPath {
    pub path: String,
    pub classification: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyFile {
    pub path: String,
    pub ecosystem: String,
    pub lockfile: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationInventory {
    pub event_id: EventId,
    pub sequence: Option<u64>,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum DetectorError {
    #[error(transparent)]
    Store(#[from] super::store::StoreError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    ContractRegistry(#[from] ContractRegistryError),
    #[error(transparent)]
    ClaimService(#[from] ClaimServiceError),
    #[error("worker was not found: {0}")]
    WorkerNotFound(String),
    #[error("claim was not found: {0}")]
    ClaimNotFound(String),
    #[error("claim is not active and owned by the requested worker")]
    ClaimNotOwned,
    #[error("worker does not have an isolated workspace")]
    WorkspaceMissing,
    #[error("claim and workspace base revisions do not match")]
    BaseRevisionMismatch,
    #[error("invalid detector comparison: {0}")]
    InvalidComparison(String),
    #[error("invalid detector policy: {0}")]
    InvalidPolicy(String),
    #[error("worker changed while a watchdog action was being applied: {0}")]
    StaleWorker(String),
    #[error("Git inventory command failed: {command}: {message}")]
    GitFailed { command: String, message: String },
}

#[derive(Debug, Clone)]
pub struct ChangeInventoryCollector {
    store: Arc<SqliteCoordinationStore>,
}

impl ChangeInventoryCollector {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self { store }
    }

    pub fn collect(
        &self,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<ChangeInventory, DetectorError> {
        let worker = self
            .store
            .worker(worker_id)?
            .ok_or_else(|| DetectorError::WorkerNotFound(worker_id.as_str().into()))?;
        let claim = self
            .store
            .claim(claim_id)?
            .ok_or_else(|| DetectorError::ClaimNotFound(claim_id.as_str().into()))?;
        if claim.state != ClaimState::Active
            || claim.owner != *worker_id
            || claim.goal_id != worker.goal_id
            || !worker.active_claims.contains(claim_id)
        {
            return Err(DetectorError::ClaimNotOwned);
        }
        let binding = worker
            .workspace
            .as_ref()
            .ok_or(DetectorError::WorkspaceMissing)?;
        if claim.base_revision != binding.base_revision {
            return Err(DetectorError::BaseRevisionMismatch);
        }
        let manager = WorktreeManager::open(&binding.canonical_repository_path)?;
        let state = manager.inspect(binding)?;
        let worktree = PathBuf::from(&binding.worktree_path);

        let (mut changed_files, paths_truncated) = changed_files(&worktree, &claim.base_revision)?;
        let diff = collect_diff(&worktree, &claim.base_revision, &changed_files)?;
        let changed_symbols = extract_symbols(&diff.patch);
        let changed_paths = changed_files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        let migrations = changed_paths
            .iter()
            .filter_map(|path| classify_migration(path))
            .collect();
        let dependency_files = changed_paths
            .iter()
            .filter_map(|path| classify_dependency(path))
            .collect();
        let generated_artifacts = changed_paths
            .iter()
            .filter_map(|path| classify_generated(path))
            .collect();
        let validation_results = self
            .store
            .latest_events_for_goal(
                &claim.goal_id,
                Some(claim.id.as_str()),
                MAX_VALIDATION_RESULTS,
            )?
            .into_iter()
            .filter_map(|event| match event.typed_payload().ok().flatten() {
                Some(CoordinationEventPayload::Activity(activity))
                    if activity.category == ActivityCategory::Validation
                        && activity.worker_id == *worker_id
                        && activity.claim_id == *claim_id =>
                {
                    Some(ValidationInventory {
                        event_id: event.id,
                        sequence: event.sequence,
                        summary: activity.summary,
                        evidence_refs: activity.evidence_refs,
                        occurred_at: event.occurred_at,
                    })
                }
                _ => None,
            })
            .collect();
        changed_files.sort_by(|left, right| left.path.cmp(&right.path));

        Ok(ChangeInventory {
            worker_id: worker_id.clone(),
            claim_id: claim_id.clone(),
            goal_id: claim.goal_id,
            repository_id: binding.repository_id.clone(),
            base_revision: claim.base_revision,
            head_revision: state.head_revision,
            generated_at: now,
            changed_files,
            diff: diff.clone(),
            changed_symbols,
            migrations,
            dependency_files,
            generated_artifacts,
            validation_results,
            truncated: paths_truncated || diff.patch_truncated,
        })
    }
}

#[derive(Debug, Clone)]
pub struct FileOverlapDetector {
    store: Arc<SqliteCoordinationStore>,
    inventories: ChangeInventoryCollector,
}

impl FileOverlapDetector {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            inventories: ChangeInventoryCollector::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn detect_and_publish(
        &self,
        left_worker_id: &WorkerId,
        left_claim_id: &ClaimId,
        right_worker_id: &WorkerId,
        right_claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<Option<CoordinationEvent>, DetectorError> {
        if left_worker_id == right_worker_id || left_claim_id == right_claim_id {
            return Err(DetectorError::InvalidComparison(
                "file overlap requires two distinct workers and claims".into(),
            ));
        }
        let left = self
            .inventories
            .collect(left_worker_id, left_claim_id, now)?;
        let right = self
            .inventories
            .collect(right_worker_id, right_claim_id, now)?;
        let (left, right) = if left.worker_id.as_str() <= right.worker_id.as_str() {
            (left, right)
        } else {
            (right, left)
        };
        if left.goal_id != right.goal_id {
            return Err(DetectorError::InvalidComparison(
                "workers from different goals cannot overlap".into(),
            ));
        }
        if left.repository_id != right.repository_id {
            return Ok(None);
        }

        let right_files = right
            .changed_files
            .iter()
            .map(|file| (file.path.as_str(), file))
            .collect::<BTreeMap<_, _>>();
        let migrations = left
            .migrations
            .iter()
            .chain(&right.migrations)
            .map(|item| item.path.as_str())
            .collect::<BTreeSet<_>>();
        let dependency_files = left
            .dependency_files
            .iter()
            .chain(&right.dependency_files)
            .map(|item| item.path.as_str())
            .collect::<BTreeSet<_>>();
        let generated_artifacts = left
            .generated_artifacts
            .iter()
            .chain(&right.generated_artifacts)
            .map(|item| item.path.as_str())
            .collect::<BTreeSet<_>>();
        let paths = left
            .changed_files
            .iter()
            .filter_map(|left_file| {
                right_files
                    .get(left_file.path.as_str())
                    .map(|right_file| FileOverlapInput {
                        path: left_file.path.clone(),
                        left_status: left_file.status.as_str().into(),
                        right_status: right_file.status.as_str().into(),
                        migration: migrations.contains(left_file.path.as_str()),
                        dependency_file: dependency_files.contains(left_file.path.as_str()),
                        generated_artifact: generated_artifacts.contains(left_file.path.as_str()),
                    })
            })
            .collect::<Vec<_>>();
        if paths.is_empty() {
            return Ok(None);
        }
        let severity = if paths
            .iter()
            .any(|path| path.migration || path.dependency_file)
        {
            EventSeverity::Warning
        } else {
            EventSeverity::Info
        };
        let correlation_id = format!(
            "file-overlap:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str()
        );
        let event = CoordinationEvent::from_typed_payload(
            &left.goal_id,
            severity,
            CoordinationActor::System,
            &correlation_id,
            CoordinationEventPayload::FileOverlap(FileOverlapEventPayload {
                left_worker_id: left.worker_id.clone(),
                left_claim_id: left.claim_id.clone(),
                left_base_revision: left.base_revision.clone(),
                left_head_revision: left.head_revision.clone(),
                right_worker_id: right.worker_id.clone(),
                right_claim_id: right.claim_id.clone(),
                right_base_revision: right.base_revision.clone(),
                right_head_revision: right.head_revision.clone(),
                paths,
            }),
            now,
        )?;
        let publication_key = format!(
            "signal.file-overlap:{}:{}:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str(),
            left.diff.patch_sha256,
            right.diff.patch_sha256
        );
        let sequence = self.store.append_event_once(&publication_key, &event)?;
        let stored = self
            .store
            .events_for_goal(
                &left.goal_id,
                sequence.saturating_sub(1),
                Some(&correlation_id),
                10,
            )?
            .into_iter()
            .find(|candidate| candidate.sequence == Some(sequence))
            .ok_or_else(|| {
                super::store::StoreError::NotFound(format!(
                    "file overlap event at sequence {sequence}"
                ))
            })?;
        Ok(Some(stored))
    }
}

#[derive(Debug, Clone)]
pub struct SymbolOverlapDetector {
    store: Arc<SqliteCoordinationStore>,
    inventories: ChangeInventoryCollector,
}

impl SymbolOverlapDetector {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            inventories: ChangeInventoryCollector::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn detect_and_publish(
        &self,
        left_worker_id: &WorkerId,
        left_claim_id: &ClaimId,
        right_worker_id: &WorkerId,
        right_claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<Option<CoordinationEvent>, DetectorError> {
        if left_worker_id == right_worker_id || left_claim_id == right_claim_id {
            return Err(DetectorError::InvalidComparison(
                "symbol overlap requires two distinct workers and claims".into(),
            ));
        }
        let left = self
            .inventories
            .collect(left_worker_id, left_claim_id, now)?;
        let right = self
            .inventories
            .collect(right_worker_id, right_claim_id, now)?;
        let (left, right) = if left.worker_id.as_str() <= right.worker_id.as_str() {
            (left, right)
        } else {
            (right, left)
        };
        if left.goal_id != right.goal_id {
            return Err(DetectorError::InvalidComparison(
                "workers from different goals cannot overlap".into(),
            ));
        }
        if left.repository_id != right.repository_id {
            return Ok(None);
        }

        let right_symbols = right
            .changed_symbols
            .iter()
            .map(|symbol| ((symbol.path.as_str(), symbol.name.as_str()), symbol))
            .collect::<BTreeMap<_, _>>();
        let mut overlaps = left
            .changed_symbols
            .iter()
            .filter_map(|left_symbol| {
                right_symbols
                    .get(&(left_symbol.path.as_str(), left_symbol.name.as_str()))
                    .map(|right_symbol| SymbolOverlapInput {
                        identity: format!("{}:{}", left_symbol.kind, left_symbol.name),
                        match_kind: "symbol".into(),
                        left_path: left_symbol.path.clone(),
                        right_path: right_symbol.path.clone(),
                    })
            })
            .collect::<Vec<_>>();
        if overlaps.is_empty() {
            let left_subsystems = symbols_by_subsystem(&left.changed_symbols);
            let right_subsystems = symbols_by_subsystem(&right.changed_symbols);
            for (subsystem, left_paths) in left_subsystems {
                let Some(right_paths) = right_subsystems.get(&subsystem) else {
                    continue;
                };
                let Some((left_path, right_path)) = left_paths.iter().find_map(|left_path| {
                    right_paths
                        .iter()
                        .find(|right_path| *right_path != left_path)
                        .map(|right_path| (left_path.clone(), right_path.clone()))
                }) else {
                    continue;
                };
                overlaps.push(SymbolOverlapInput {
                    identity: subsystem,
                    match_kind: "subsystem".into(),
                    left_path,
                    right_path,
                });
                if overlaps.len() == MAX_SYMBOLS {
                    break;
                }
            }
        }
        if overlaps.is_empty() {
            return Ok(None);
        }
        overlaps.sort_by(|left, right| {
            (
                &left.match_kind,
                &left.identity,
                &left.left_path,
                &left.right_path,
            )
                .cmp(&(
                    &right.match_kind,
                    &right.identity,
                    &right.left_path,
                    &right.right_path,
                ))
        });
        overlaps.dedup();
        let severity = if overlaps
            .iter()
            .any(|overlap| overlap.match_kind == "symbol")
        {
            EventSeverity::Warning
        } else {
            EventSeverity::Info
        };
        let correlation_id = format!(
            "symbol-overlap:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str()
        );
        let event = CoordinationEvent::from_typed_payload(
            &left.goal_id,
            severity,
            CoordinationActor::System,
            &correlation_id,
            CoordinationEventPayload::SymbolOverlap(SymbolOverlapEventPayload {
                left_worker_id: left.worker_id.clone(),
                left_claim_id: left.claim_id.clone(),
                left_base_revision: left.base_revision.clone(),
                left_head_revision: left.head_revision.clone(),
                right_worker_id: right.worker_id.clone(),
                right_claim_id: right.claim_id.clone(),
                right_base_revision: right.base_revision.clone(),
                right_head_revision: right.head_revision.clone(),
                overlaps,
            }),
            now,
        )?;
        let publication_key = format!(
            "signal.symbol-overlap:{}:{}:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str(),
            left.diff.patch_sha256,
            right.diff.patch_sha256
        );
        let sequence = self.store.append_event_once(&publication_key, &event)?;
        let stored = self
            .store
            .events_for_goal(
                &left.goal_id,
                sequence.saturating_sub(1),
                Some(&correlation_id),
                10,
            )?
            .into_iter()
            .find(|candidate| candidate.sequence == Some(sequence))
            .ok_or_else(|| {
                super::store::StoreError::NotFound(format!(
                    "symbol overlap event at sequence {sequence}"
                ))
            })?;
        Ok(Some(stored))
    }
}

#[derive(Debug, Clone)]
pub struct ContractChangeDetector {
    store: Arc<SqliteCoordinationStore>,
    contracts: ContractRegistry,
}

impl ContractChangeDetector {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            contracts: ContractRegistry::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn scan_goal(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<CoordinationEvent>, DetectorError> {
        let mut events = Vec::new();
        for mismatch in self.contracts.revision_mismatches_for_goal(goal_id)? {
            let correlation_id = mismatch.claim_id.as_str();
            let event = CoordinationEvent::from_typed_payload(
                goal_id,
                EventSeverity::Warning,
                CoordinationActor::System,
                correlation_id,
                CoordinationEventPayload::ContractMismatch(ContractMismatchEventPayload {
                    claim_id: mismatch.claim_id.clone(),
                    worker_id: mismatch.worker_id.clone(),
                    contract_id: mismatch.contract_id.clone(),
                    stable_key: mismatch.stable_key.clone(),
                    expected_revision: mismatch.expected_revision,
                    current_revision: mismatch.current_revision,
                }),
                now,
            )?;
            let publication_key = format!(
                "signal.contract-mismatch:{}:{}:{}:{}",
                mismatch.claim_id.as_str(),
                mismatch.contract_id.as_str(),
                mismatch.expected_revision,
                mismatch.current_revision
            );
            let sequence = self.store.append_event_once(&publication_key, &event)?;
            let stored = self
                .store
                .events_for_goal(
                    goal_id,
                    sequence.saturating_sub(1),
                    Some(correlation_id),
                    10,
                )?
                .into_iter()
                .find(|candidate| candidate.sequence == Some(sequence))
                .ok_or_else(|| {
                    super::store::StoreError::NotFound(format!(
                        "contract mismatch event at sequence {sequence}"
                    ))
                })?;
            events.push(stored);
        }
        events.sort_by_key(|event| event.sequence);
        Ok(events)
    }
}

#[derive(Debug, Clone)]
pub struct MigrationOverlapDetector {
    store: Arc<SqliteCoordinationStore>,
    inventories: ChangeInventoryCollector,
}

impl MigrationOverlapDetector {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            inventories: ChangeInventoryCollector::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn detect_and_publish(
        &self,
        left_worker_id: &WorkerId,
        left_claim_id: &ClaimId,
        right_worker_id: &WorkerId,
        right_claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<Option<CoordinationEvent>, DetectorError> {
        if left_worker_id == right_worker_id || left_claim_id == right_claim_id {
            return Err(DetectorError::InvalidComparison(
                "migration overlap requires two distinct workers and claims".into(),
            ));
        }
        let left = self
            .inventories
            .collect(left_worker_id, left_claim_id, now)?;
        let right = self
            .inventories
            .collect(right_worker_id, right_claim_id, now)?;
        let (left, right) = if left.worker_id.as_str() <= right.worker_id.as_str() {
            (left, right)
        } else {
            (right, left)
        };
        if left.goal_id != right.goal_id {
            return Err(DetectorError::InvalidComparison(
                "workers from different goals cannot overlap".into(),
            ));
        }
        if left.repository_id != right.repository_id {
            return Ok(None);
        }

        let left_by_class = migrations_by_class(&left.migrations);
        let right_by_class = migrations_by_class(&right.migrations);
        let mut overlaps = Vec::new();
        for (classification, left_paths) in left_by_class {
            let Some(right_paths) = right_by_class.get(&classification) else {
                continue;
            };
            let shared_paths = left_paths
                .intersection(right_paths)
                .cloned()
                .collect::<Vec<_>>();
            overlaps.push(MigrationOverlapInput {
                classification,
                left_paths: left_paths.into_iter().collect(),
                right_paths: right_paths.iter().cloned().collect(),
                shared_paths,
            });
        }
        if overlaps.is_empty() {
            return Ok(None);
        }
        let correlation_id = format!(
            "migration-overlap:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str()
        );
        let event = CoordinationEvent::from_typed_payload(
            &left.goal_id,
            EventSeverity::Warning,
            CoordinationActor::System,
            &correlation_id,
            CoordinationEventPayload::MigrationOverlap(MigrationOverlapEventPayload {
                left_worker_id: left.worker_id.clone(),
                left_claim_id: left.claim_id.clone(),
                left_base_revision: left.base_revision.clone(),
                left_head_revision: left.head_revision.clone(),
                right_worker_id: right.worker_id.clone(),
                right_claim_id: right.claim_id.clone(),
                right_base_revision: right.base_revision.clone(),
                right_head_revision: right.head_revision.clone(),
                overlaps,
            }),
            now,
        )?;
        let publication_key = format!(
            "signal.migration-overlap:{}:{}:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str(),
            left.diff.patch_sha256,
            right.diff.patch_sha256
        );
        let sequence = self.store.append_event_once(&publication_key, &event)?;
        let stored = self
            .store
            .events_for_goal(
                &left.goal_id,
                sequence.saturating_sub(1),
                Some(&correlation_id),
                10,
            )?
            .into_iter()
            .find(|candidate| candidate.sequence == Some(sequence))
            .ok_or_else(|| {
                super::store::StoreError::NotFound(format!(
                    "migration overlap event at sequence {sequence}"
                ))
            })?;
        Ok(Some(stored))
    }
}

fn migrations_by_class(items: &[ClassifiedPath]) -> BTreeMap<String, BTreeSet<String>> {
    let mut result = BTreeMap::<String, BTreeSet<String>>::new();
    for item in items {
        result
            .entry(item.classification.clone())
            .or_default()
            .insert(item.path.clone());
    }
    result
}

#[derive(Debug, Clone)]
pub struct MergeConflictPredictor {
    store: Arc<SqliteCoordinationStore>,
    inventories: ChangeInventoryCollector,
}

impl MergeConflictPredictor {
    pub fn new(store: Arc<SqliteCoordinationStore>) -> Self {
        Self {
            inventories: ChangeInventoryCollector::new(Arc::clone(&store)),
            store,
        }
    }

    pub fn detect_and_publish(
        &self,
        left_worker_id: &WorkerId,
        left_claim_id: &ClaimId,
        right_worker_id: &WorkerId,
        right_claim_id: &ClaimId,
        now: DateTime<Utc>,
    ) -> Result<Option<CoordinationEvent>, DetectorError> {
        if left_worker_id == right_worker_id || left_claim_id == right_claim_id {
            return Err(DetectorError::InvalidComparison(
                "merge prediction requires two distinct workers and claims".into(),
            ));
        }
        let left = self
            .inventories
            .collect(left_worker_id, left_claim_id, now)?;
        let right = self
            .inventories
            .collect(right_worker_id, right_claim_id, now)?;
        let (left, right) = if left.worker_id.as_str() <= right.worker_id.as_str() {
            (left, right)
        } else {
            (right, left)
        };
        if left.goal_id != right.goal_id {
            return Err(DetectorError::InvalidComparison(
                "workers from different goals cannot be merge-tested together".into(),
            ));
        }
        if left.repository_id != right.repository_id || left.head_revision == right.head_revision {
            return Ok(None);
        }
        let left_worker = self
            .store
            .worker(&left.worker_id)?
            .ok_or_else(|| DetectorError::WorkerNotFound(left.worker_id.as_str().into()))?;
        let repository = PathBuf::from(
            &left_worker
                .workspace
                .ok_or(DetectorError::WorkspaceMissing)?
                .canonical_repository_path,
        );
        let merge_base = git_text(
            &repository,
            &["merge-base", &left.head_revision, &right.head_revision],
        )?
        .trim()
        .to_string();
        if merge_base.is_empty() {
            return Err(DetectorError::GitFailed {
                command: "git merge-base".into(),
                message: "merge base was empty".into(),
            });
        }
        let output = Command::new("git")
            .arg("-C")
            .arg(&repository)
            .args([
                "merge-tree",
                "--write-tree",
                "--messages",
                "--name-only",
                &left.head_revision,
                &right.head_revision,
            ])
            .output()
            .map_err(WorkspaceError::from)?;
        if output.status.success() {
            return Ok(None);
        }
        if output.status.code() != Some(1) {
            return Err(DetectorError::GitFailed {
                command: "git merge-tree --write-tree --messages --name-only".into(),
                message: String::from_utf8_lossy(&output.stderr).trim().into(),
            });
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let conflicting_paths = merge_tree_conflicting_paths(&stdout);
        if conflicting_paths.is_empty() {
            return Err(DetectorError::GitFailed {
                command: "git merge-tree --write-tree --messages --name-only".into(),
                message: "merge-tree reported a conflict without a parseable path".into(),
            });
        }
        let mut diagnostic_bytes = output.stdout;
        if !output.stderr.is_empty() {
            diagnostic_bytes.extend_from_slice(b"\n");
            diagnostic_bytes.extend_from_slice(&output.stderr);
        }
        let diagnostics_truncated = diagnostic_bytes.len() > MAX_MERGE_DIAGNOSTIC_BYTES;
        let diagnostics = String::from_utf8_lossy(
            &diagnostic_bytes[..diagnostic_bytes.len().min(MAX_MERGE_DIAGNOSTIC_BYTES)],
        )
        .to_string();
        let correlation_id = format!(
            "merge-conflict:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str()
        );
        let event = CoordinationEvent::from_typed_payload(
            &left.goal_id,
            EventSeverity::Error,
            CoordinationActor::System,
            &correlation_id,
            CoordinationEventPayload::MergeConflict(MergeConflictEventPayload {
                left_worker_id: left.worker_id.clone(),
                left_claim_id: left.claim_id.clone(),
                left_head_revision: left.head_revision.clone(),
                right_worker_id: right.worker_id.clone(),
                right_claim_id: right.claim_id.clone(),
                right_head_revision: right.head_revision.clone(),
                merge_base_revision: merge_base,
                conflicting_paths,
                diagnostics,
                diagnostics_truncated,
            }),
            now,
        )?;
        let publication_key = format!(
            "signal.merge-conflict:{}:{}:{}:{}",
            left.claim_id.as_str(),
            right.claim_id.as_str(),
            left.head_revision,
            right.head_revision
        );
        let sequence = self.store.append_event_once(&publication_key, &event)?;
        let stored = self
            .store
            .events_for_goal(
                &left.goal_id,
                sequence.saturating_sub(1),
                Some(&correlation_id),
                10,
            )?
            .into_iter()
            .find(|candidate| candidate.sequence == Some(sequence))
            .ok_or_else(|| {
                super::store::StoreError::NotFound(format!(
                    "merge conflict event at sequence {sequence}"
                ))
            })?;
        Ok(Some(stored))
    }
}

fn merge_tree_conflicting_paths(output: &str) -> Vec<String> {
    let mut paths = output
        .split_once("\n\n")
        .map(|(conflict_section, _)| conflict_section)
        .unwrap_or(output)
        .lines()
        .skip(1)
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths.truncate(MAX_CHANGED_PATHS);
    paths
}

#[derive(Debug, Clone)]
pub struct WorkerHealthDetector {
    store: Arc<SqliteCoordinationStore>,
    policy: HealthDetectionPolicy,
}

impl WorkerHealthDetector {
    pub fn new(
        store: Arc<SqliteCoordinationStore>,
        policy: HealthDetectionPolicy,
    ) -> Result<Self, DetectorError> {
        if policy.heartbeat_stale_after_seconds == 0
            || policy.activity_stale_after_seconds == 0
            || policy.validation_failure_threshold < 2
            || policy.validation_failure_threshold > 100
            || policy.validation_window_seconds == 0
        {
            return Err(DetectorError::InvalidPolicy(
                "health thresholds must be positive and validation failures must be between 2 and 100"
                    .into(),
            ));
        }
        Ok(Self { store, policy })
    }

    pub fn scan_goal(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<CoordinationEvent>, DetectorError> {
        let mut detected = Vec::new();
        for worker in self.store.workers_for_goal(goal_id, None)? {
            if !matches!(
                worker.state,
                WorkerState::Starting
                    | WorkerState::Active
                    | WorkerState::Waiting
                    | WorkerState::Recovering
            ) {
                continue;
            }
            for claim_id in &worker.active_claims {
                let Some(claim) = self.store.claim(claim_id)? else {
                    continue;
                };
                if claim.state != ClaimState::Active || claim.owner != worker.id {
                    continue;
                }
                let history = self.store.latest_events_for_goal(
                    goal_id,
                    Some(claim.id.as_str()),
                    MAX_VALIDATION_RESULTS,
                )?;
                let last_activity_at = history
                    .iter()
                    .filter(|event| {
                        matches!(
                            &event.producer,
                            CoordinationActor::Worker { worker_id }
                                if worker_id == &worker.id
                        )
                    })
                    .map(|event| event.occurred_at)
                    .max();
                let last_heartbeat_at = worker
                    .last_heartbeat_at
                    .map(|heartbeat| heartbeat.max(claim.heartbeat_at))
                    .unwrap_or(claim.heartbeat_at);
                let heartbeat_age_seconds = age_seconds(now, last_heartbeat_at);
                let activity_age_seconds = last_activity_at.map(|at| age_seconds(now, at));
                if heartbeat_age_seconds >= self.policy.heartbeat_stale_after_seconds
                    && activity_age_seconds
                        .is_none_or(|age| age >= self.policy.activity_stale_after_seconds)
                {
                    let event = CoordinationEvent::from_typed_payload(
                        goal_id,
                        EventSeverity::Warning,
                        CoordinationActor::System,
                        claim.id.as_str(),
                        CoordinationEventPayload::WorkerStalled(WorkerStalledEventPayload {
                            worker_id: worker.id.clone(),
                            claim_id: claim.id.clone(),
                            last_heartbeat_at,
                            last_activity_at,
                            detected_at: now,
                            heartbeat_age_seconds,
                            activity_age_seconds,
                            heartbeat_threshold_seconds: self.policy.heartbeat_stale_after_seconds,
                            activity_threshold_seconds: self.policy.activity_stale_after_seconds,
                        }),
                        now,
                    )?;
                    let key = format!(
                        "signal.worker-stalled:{}:{}:{}:{}",
                        worker.id.as_str(),
                        claim.id.as_str(),
                        claim.lease_generation,
                        last_heartbeat_at.timestamp_millis()
                    );
                    detected.push(self.persist_event(goal_id, claim.id.as_str(), &key, &event)?);
                }

                let cutoff = now
                    - chrono::Duration::seconds(
                        self.policy.validation_window_seconds.min(i64::MAX as u64) as i64,
                    );
                let mut failures = Vec::new();
                for event in &history {
                    if event.occurred_at < cutoff {
                        continue;
                    }
                    let Some(CoordinationEventPayload::Activity(activity)) =
                        event.typed_payload().ok().flatten()
                    else {
                        continue;
                    };
                    if activity.category != ActivityCategory::Validation
                        || activity.worker_id != worker.id
                        || activity.claim_id != claim.id
                    {
                        continue;
                    }
                    match activity.validation_succeeded {
                        Some(true) => failures.clear(),
                        Some(false) => failures.push(event),
                        None => {}
                    }
                }
                if failures.len() >= self.policy.validation_failure_threshold {
                    let last_failure = failures.last().expect("failure threshold is non-zero");
                    let failure_event_ids = failures
                        .iter()
                        .map(|event| event.id.clone())
                        .collect::<Vec<_>>();
                    let event = CoordinationEvent::from_typed_payload(
                        goal_id,
                        EventSeverity::Error,
                        CoordinationActor::System,
                        claim.id.as_str(),
                        CoordinationEventPayload::RepeatedValidationFailure(
                            RepeatedValidationFailureEventPayload {
                                worker_id: worker.id.clone(),
                                claim_id: claim.id.clone(),
                                failure_event_ids,
                                last_failure_at: last_failure.occurred_at,
                                window_seconds: self.policy.validation_window_seconds,
                                threshold: self.policy.validation_failure_threshold,
                            },
                        ),
                        now,
                    )?;
                    let key = format!(
                        "signal.repeated-validation:{}:{}:{}",
                        worker.id.as_str(),
                        claim.id.as_str(),
                        last_failure.id.as_str()
                    );
                    detected.push(self.persist_event(goal_id, claim.id.as_str(), &key, &event)?);
                }
            }
        }
        detected.sort_by_key(|event| event.sequence);
        Ok(detected)
    }

    fn persist_event(
        &self,
        goal_id: &str,
        correlation_id: &str,
        publication_key: &str,
        event: &CoordinationEvent,
    ) -> Result<CoordinationEvent, DetectorError> {
        let sequence = self.store.append_event_once(publication_key, event)?;
        self.store
            .events_for_goal(
                goal_id,
                sequence.saturating_sub(1),
                Some(correlation_id),
                10,
            )?
            .into_iter()
            .find(|candidate| candidate.sequence == Some(sequence))
            .ok_or_else(|| {
                super::store::StoreError::NotFound(format!("health event at sequence {sequence}"))
                    .into()
            })
    }
}

#[derive(Debug, Clone)]
pub struct WatchdogService {
    store: Arc<SqliteCoordinationStore>,
    policy: WatchdogPolicy,
}

impl WatchdogService {
    pub fn new(
        store: Arc<SqliteCoordinationStore>,
        policy: WatchdogPolicy,
    ) -> Result<Self, DetectorError> {
        if policy.lease_grace_seconds == 0
            || policy.lease_grace_seconds > i64::MAX as u64
            || policy.max_consecutive_failures < 2
            || policy.max_consecutive_failures > 100
        {
            return Err(DetectorError::InvalidPolicy(
                "watchdog durations and token limits must be positive, and the failure cutoff must be between 2 and 100"
                    .into(),
            ));
        }
        validate_resource_quota_policy(&policy.resource_quotas)
            .map_err(DetectorError::InvalidPolicy)?;
        Ok(Self { store, policy })
    }

    pub fn enforce(
        &self,
        claim_service: &ClaimService,
        goal_id: &str,
        observations: &[WorkerRuntimeObservation],
        now: DateTime<Utc>,
    ) -> Result<WatchdogReport, DetectorError> {
        let mut observations_by_worker = BTreeMap::<String, &WorkerRuntimeObservation>::new();
        for observation in observations {
            if observations_by_worker
                .insert(observation.worker_id.as_str().into(), observation)
                .is_some()
            {
                return Err(DetectorError::InvalidComparison(format!(
                    "duplicate runtime observation for worker {}",
                    observation.worker_id.as_str()
                )));
            }
        }
        let initial_workers = self.store.workers_for_goal(goal_id, None)?;
        let worker_ids = initial_workers
            .iter()
            .map(|worker| worker.id.as_str())
            .collect::<BTreeSet<_>>();
        if let Some(unknown) = observations
            .iter()
            .find(|observation| !worker_ids.contains(observation.worker_id.as_str()))
        {
            return Err(DetectorError::WorkerNotFound(
                unknown.worker_id.as_str().into(),
            ));
        }

        let expired = claim_service.expire_stale_claims(
            goal_id,
            now,
            chrono::Duration::seconds(self.policy.lease_grace_seconds as i64),
        )?;
        let mut outcomes = Vec::new();
        for claim in expired {
            let event_sequence = self
                .store
                .events_for_goal(goal_id, 0, Some(claim.id.as_str()), 100)?
                .into_iter()
                .rev()
                .find_map(|event| {
                    let is_expiry = matches!(
                        event.typed_payload().ok().flatten(),
                        Some(CoordinationEventPayload::Claim(payload))
                            if payload.claim_id == claim.id
                                && payload.to_state == ClaimState::Expired
                    );
                    is_expiry.then_some(event.sequence).flatten()
                });
            outcomes.push(WatchdogOutcome {
                worker_id: claim.owner,
                claim_ids: vec![claim.id],
                kind: WatchdogOutcomeKind::LeaseExpired,
                reason: "claim lease expired after grace period".into(),
                event_sequence,
            });
        }

        let workers = self.store.workers_for_goal(goal_id, None)?;
        let goal_started_at = self
            .store
            .goal_pool(goal_id)?
            .and_then(|pool| pool.started_at)
            .or_else(|| {
                workers
                    .iter()
                    .map(|worker| worker.metadata.created_at)
                    .min()
            });
        let mut goal_usage = ResourceUsage {
            elapsed_seconds: goal_started_at
                .map(|started_at| age_seconds(now, started_at))
                .unwrap_or_default(),
            concurrency: workers
                .iter()
                .filter(|worker| !worker.state.is_terminal())
                .count(),
            ..ResourceUsage::default()
        };
        for observation in observations {
            goal_usage.tokens = goal_usage
                .tokens
                .saturating_add(observation.observed_tokens.unwrap_or_default());
            goal_usage.processes = goal_usage
                .processes
                .saturating_add(observation.active_processes);
            goal_usage.disk_bytes = goal_usage
                .disk_bytes
                .saturating_add(observation.observed_disk_bytes);
            goal_usage.network_requests = goal_usage
                .network_requests
                .saturating_add(observation.observed_network_requests);
            goal_usage.retries = goal_usage
                .retries
                .saturating_add(observation.retry_attempts);
        }
        let goal_quota_reason = goal_quota_violation(&self.policy.resource_quotas, goal_usage);
        if goal_quota_reason.is_some()
            && let Some(mut pool) = self.store.goal_pool(goal_id)?
            && pool.mode != PoolMode::Paused
        {
            pool.mode = PoolMode::Paused;
            self.store.upsert_goal_pool(&pool, now)?;
        }
        for mut worker in workers {
            if !matches!(
                worker.state,
                WorkerState::Starting
                    | WorkerState::Active
                    | WorkerState::Waiting
                    | WorkerState::Paused
                    | WorkerState::Blocked
            ) {
                continue;
            }
            let observation = observations_by_worker.get(worker.id.as_str()).copied();
            let violation = goal_quota_reason
                .as_ref()
                .map(|reason| {
                    (
                        WatchdogActionKind::ResourceQuota,
                        WatchdogOutcomeKind::ResourceQuota,
                        EventSeverity::Critical,
                        reason.clone(),
                    )
                })
                .or_else(|| self.violation_for(&worker, observation, now));
            let Some((action, kind, severity, reason)) = violation else {
                continue;
            };

            let expected_updated_at = worker.metadata.updated_at;
            let mut claim_ids = Vec::new();
            for claim_id in &worker.active_claims {
                if let Some(claim) = self.store.claim(claim_id)?
                    && claim.goal_id == goal_id
                    && claim.owner == worker.id
                    && claim.state == ClaimState::Active
                {
                    claim_ids.push(claim.id);
                }
            }
            claim_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            worker.transition(WorkerState::Recovering, now, Some(reason.clone()))?;
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
            let correlation_id = format!("watchdog:{}", worker.id.as_str());
            let event = CoordinationEvent::from_typed_payload(
                goal_id,
                severity,
                CoordinationActor::System,
                &correlation_id,
                CoordinationEventPayload::WatchdogAction(WatchdogActionEventPayload {
                    worker_id: worker.id.clone(),
                    claim_ids: claim_ids.clone(),
                    action,
                    reason: reason.clone(),
                    observed_at: now,
                }),
                now,
            )?;
            let publication_key = format!(
                "watchdog.{}:{}:{}",
                watchdog_action_key(action),
                worker.id.as_str(),
                expected_updated_at.timestamp_millis()
            );
            let sequence = self
                .store
                .update_worker_with_event_if_revision(
                    &worker,
                    expected_updated_at,
                    &event,
                    &publication_key,
                )?
                .ok_or_else(|| DetectorError::StaleWorker(worker.id.as_str().into()))?;
            outcomes.push(WatchdogOutcome {
                worker_id: worker.id,
                claim_ids,
                kind,
                reason,
                event_sequence: Some(sequence),
            });
        }
        outcomes.sort_by(|left, right| {
            left.worker_id
                .as_str()
                .cmp(right.worker_id.as_str())
                .then_with(|| {
                    watchdog_outcome_rank(left.kind).cmp(&watchdog_outcome_rank(right.kind))
                })
        });
        Ok(WatchdogReport {
            goal_id: goal_id.into(),
            observed_at: now,
            outcomes,
        })
    }

    fn violation_for(
        &self,
        worker: &super::domain::Worker,
        observation: Option<&WorkerRuntimeObservation>,
        now: DateTime<Utc>,
    ) -> Option<(
        WatchdogActionKind,
        WatchdogOutcomeKind,
        EventSeverity,
        String,
    )> {
        let permission_reason = observation
            .and_then(|item| item.permission_violation.as_deref())
            .filter(|reason| !reason.trim().is_empty())
            .map(|reason| format!("runtime permission violation: {}", reason.trim()))
            .or_else(|| match worker.permission_profile.as_ref() {
                None => Some("worker permission profile is missing".into()),
                Some(profile) => validate_worker_permissions(profile)
                    .err()
                    .map(|error| format!("invalid worker permission profile: {error}")),
            });
        if let Some(reason) = permission_reason {
            return Some((
                WatchdogActionKind::PermissionViolation,
                WatchdogOutcomeKind::PermissionViolation,
                EventSeverity::Critical,
                bounded_watchdog_reason(reason),
            ));
        }
        if observation.is_some_and(|item| !item.environment_alive) {
            return Some((
                WatchdogActionKind::EnvironmentRecovery,
                WatchdogOutcomeKind::EnvironmentRecovery,
                EventSeverity::Error,
                "worker execution environment is no longer alive".into(),
            ));
        }

        let turn_started_at = observation
            .and_then(|item| item.active_turn_started_at)
            .or_else(|| {
                worker
                    .turn_history
                    .iter()
                    .rev()
                    .find(|turn| turn.completed_at.is_none())
                    .map(|turn| turn.started_at)
            });
        let usage = ResourceUsage {
            tokens: observation
                .and_then(|item| item.observed_tokens)
                .unwrap_or_default(),
            elapsed_seconds: turn_started_at
                .map(|started_at| age_seconds(now, started_at))
                .unwrap_or_default(),
            processes: observation
                .map(|item| item.active_processes)
                .unwrap_or_default(),
            disk_bytes: observation
                .map(|item| item.observed_disk_bytes)
                .unwrap_or_default(),
            network_requests: observation
                .map(|item| item.observed_network_requests)
                .unwrap_or_default(),
            retries: observation
                .map(|item| item.retry_attempts)
                .unwrap_or_default(),
            concurrency: 0,
        };
        if let Some(reason) = worker_quota_violation(&self.policy.resource_quotas, usage) {
            return Some((
                WatchdogActionKind::ResourceQuota,
                WatchdogOutcomeKind::ResourceQuota,
                EventSeverity::Error,
                reason,
            ));
        }
        if observation
            .is_some_and(|item| item.consecutive_failures >= self.policy.max_consecutive_failures)
        {
            return Some((
                WatchdogActionKind::FailureLoopCutoff,
                WatchdogOutcomeKind::FailureLoopCutoff,
                EventSeverity::Error,
                format!(
                    "worker reached consecutive failure cutoff ({} >= {})",
                    observation
                        .expect("observation was checked")
                        .consecutive_failures,
                    self.policy.max_consecutive_failures
                ),
            ));
        }
        None
    }
}

fn watchdog_action_key(action: WatchdogActionKind) -> &'static str {
    match action {
        WatchdogActionKind::EnvironmentRecovery => "environment-recovery",
        WatchdogActionKind::PermissionViolation => "permission-violation",
        WatchdogActionKind::ExecutionLimit => "execution-limit",
        WatchdogActionKind::ResourceQuota => "resource-quota",
        WatchdogActionKind::FailureLoopCutoff => "failure-loop-cutoff",
    }
}

fn watchdog_outcome_rank(kind: WatchdogOutcomeKind) -> u8 {
    match kind {
        WatchdogOutcomeKind::LeaseExpired => 0,
        WatchdogOutcomeKind::PermissionViolation => 1,
        WatchdogOutcomeKind::EnvironmentRecovery => 2,
        WatchdogOutcomeKind::ExecutionLimit => 3,
        WatchdogOutcomeKind::ResourceQuota => 4,
        WatchdogOutcomeKind::FailureLoopCutoff => 5,
    }
}

fn bounded_watchdog_reason(reason: String) -> String {
    reason.chars().take(2_000).collect()
}

fn age_seconds(now: DateTime<Utc>, then: DateTime<Utc>) -> u64 {
    now.signed_duration_since(then).num_seconds().max(0) as u64
}

fn symbols_by_subsystem(symbols: &[ChangedSymbol]) -> BTreeMap<String, BTreeSet<String>> {
    let mut subsystems = BTreeMap::<String, BTreeSet<String>>::new();
    for symbol in symbols {
        if let Some(subsystem) = source_subsystem(&symbol.path) {
            subsystems
                .entry(subsystem)
                .or_default()
                .insert(symbol.path.clone());
        }
    }
    subsystems
}

fn source_subsystem(path: &str) -> Option<String> {
    let supported = [
        "rs", "ts", "tsx", "js", "jsx", "py", "go", "java", "kt", "c", "cc", "cpp", "h", "hpp",
    ];
    let path = Path::new(path);
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    if !supported.contains(&extension.as_str()) {
        return None;
    }
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.to_string_lossy().to_string())
}

fn collect_diff(
    worktree: &Path,
    base_revision: &str,
    changed_files: &[ChangedFile],
) -> Result<DiffInventory, DetectorError> {
    let tracked = git_bytes(
        worktree,
        &["diff", "--no-ext-diff", "--unified=0", base_revision, "--"],
    )?;
    let mut hasher = Sha256::new();
    hasher.update(&tracked);
    let mut patch = tracked[..tracked.len().min(MAX_PATCH_BYTES)].to_vec();
    let mut patch_truncated = tracked.len() > MAX_PATCH_BYTES;

    for changed in changed_files
        .iter()
        .filter(|file| file.status == FileChangeStatus::Untracked)
    {
        hasher.update(b"\0untracked\0");
        hasher.update(changed.path.as_bytes());
        let mut file = File::open(worktree.join(&changed.path)).map_err(WorkspaceError::from)?;
        let mut preview = Vec::new();
        let mut buffer = [0_u8; 8 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(WorkspaceError::from)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            let remaining = MAX_PATCH_BYTES.saturating_sub(preview.len());
            preview.extend_from_slice(&buffer[..read.min(remaining)]);
            if read > remaining {
                patch_truncated = true;
            }
        }
        if patch.len() >= MAX_PATCH_BYTES {
            patch_truncated = true;
            continue;
        }
        let header = format!(
            "\ndiff --git a/{0} b/{0}\nnew file mode 100644\n--- /dev/null\n+++ b/{0}\n",
            changed.path
        );
        append_bounded(&mut patch, header.as_bytes(), &mut patch_truncated);
        if preview.contains(&0) {
            append_bounded(
                &mut patch,
                b"Binary files /dev/null and untracked file differ\n",
                &mut patch_truncated,
            );
            continue;
        }
        for line in String::from_utf8_lossy(&preview).lines() {
            append_bounded(&mut patch, b"+", &mut patch_truncated);
            append_bounded(&mut patch, line.as_bytes(), &mut patch_truncated);
            append_bounded(&mut patch, b"\n", &mut patch_truncated);
            if patch.len() >= MAX_PATCH_BYTES {
                break;
            }
        }
    }

    Ok(DiffInventory {
        patch: String::from_utf8_lossy(&patch).to_string(),
        patch_sha256: format!("{:x}", hasher.finalize()),
        patch_truncated,
    })
}

fn append_bounded(target: &mut Vec<u8>, bytes: &[u8], truncated: &mut bool) {
    let remaining = MAX_PATCH_BYTES.saturating_sub(target.len());
    target.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
    if bytes.len() > remaining {
        *truncated = true;
    }
}

fn changed_files(
    worktree: &Path,
    base_revision: &str,
) -> Result<(Vec<ChangedFile>, bool), DetectorError> {
    let names = git_bytes(
        worktree,
        &[
            "diff",
            "--name-status",
            "--find-renames",
            "-z",
            base_revision,
            "--",
        ],
    )?;
    let mut files = parse_name_status(&names);
    let numstat = git_text(worktree, &["diff", "--numstat", base_revision, "--"])?;
    let stats = parse_numstat(&numstat);
    for file in &mut files {
        if let Some((additions, deletions, binary)) = stats.get(&file.path) {
            file.additions = *additions;
            file.deletions = *deletions;
            file.binary = *binary;
        }
    }
    let untracked = git_bytes(
        worktree,
        &["ls-files", "--others", "--exclude-standard", "-z"],
    )?;
    for path in split_nul(&untracked) {
        files.push(ChangedFile {
            path,
            previous_path: None,
            status: FileChangeStatus::Untracked,
            additions: None,
            deletions: None,
            binary: false,
        });
    }
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files.dedup_by(|left, right| left.path == right.path);
    let truncated = files.len() > MAX_CHANGED_PATHS;
    files.truncate(MAX_CHANGED_PATHS);
    Ok((files, truncated))
}

fn parse_name_status(bytes: &[u8]) -> Vec<ChangedFile> {
    let fields = split_nul(bytes);
    let mut files = Vec::new();
    let mut index = 0;
    while index < fields.len() {
        let code = &fields[index];
        index += 1;
        let Some(path) = fields.get(index).cloned() else {
            break;
        };
        index += 1;
        let status = match code.chars().next().unwrap_or('?') {
            'A' => FileChangeStatus::Added,
            'C' => FileChangeStatus::Copied,
            'D' => FileChangeStatus::Deleted,
            'M' => FileChangeStatus::Modified,
            'R' => FileChangeStatus::Renamed,
            'T' => FileChangeStatus::TypeChanged,
            'U' => FileChangeStatus::Unmerged,
            _ => FileChangeStatus::Unknown,
        };
        let (previous_path, path) =
            if matches!(status, FileChangeStatus::Copied | FileChangeStatus::Renamed) {
                let Some(destination) = fields.get(index).cloned() else {
                    break;
                };
                index += 1;
                (Some(path), destination)
            } else {
                (None, path)
            };
        files.push(ChangedFile {
            path,
            previous_path,
            status,
            additions: None,
            deletions: None,
            binary: false,
        });
    }
    files
}

fn parse_numstat(output: &str) -> BTreeMap<String, (Option<u64>, Option<u64>, bool)> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let additions = fields.next()?;
            let deletions = fields.next()?;
            let path = fields.next()?.to_string();
            let binary = additions == "-" || deletions == "-";
            Some((
                path,
                (additions.parse().ok(), deletions.parse().ok(), binary),
            ))
        })
        .collect()
}

fn extract_symbols(patch: &str) -> Vec<ChangedSymbol> {
    let file_header = Regex::new(r"^\+\+\+ b/(.+)$").unwrap();
    let hunk_header = Regex::new(r"^@@ [^@]+ @@(?:\s+(.*))?$").unwrap();
    let declaration = Regex::new(
        r"^\s*(?:export\s+)?(?:pub(?:\([^)]*\))?\s+)?(?:(fn|struct|enum|trait|type|class|interface|function|const|let|var|def|impl)\s+)([A-Za-z_$][A-Za-z0-9_$]*)",
    )
    .unwrap();
    let mut current_path = None;
    let mut symbols = BTreeSet::new();
    for line in patch.lines() {
        if let Some(captures) = file_header.captures(line) {
            current_path = Some(captures[1].to_string());
            continue;
        }
        if line.starts_with("+++") {
            continue;
        }
        let candidate = line.strip_prefix('+').or_else(|| {
            hunk_header
                .captures(line)
                .and_then(|captures| captures.get(1).map(|value| value.as_str()))
        });
        if let (Some(path), Some(captures)) = (
            current_path.as_ref(),
            candidate.and_then(|candidate| declaration.captures(candidate)),
        ) {
            symbols.insert((
                path.clone(),
                captures[2].to_string(),
                captures[1].to_string(),
            ));
            if symbols.len() == MAX_SYMBOLS {
                break;
            }
        }
    }
    symbols
        .into_iter()
        .map(|(path, name, kind)| ChangedSymbol { path, name, kind })
        .collect()
}

fn classify_migration(path: &str) -> Option<ClassifiedPath> {
    let lower = path.to_ascii_lowercase();
    let migration_named = lower.contains("migration") || lower.contains("upgrade");
    let classification = if lower.ends_with(".sql")
        || (lower.contains("/migrations/") && !lower.contains("config"))
    {
        "database"
    } else if lower.contains("fixtures/") || lower.contains("seed") {
        "data"
    } else if migration_named
        && (lower.ends_with(".schema.json") || lower.contains("serialization"))
    {
        "serialization"
    } else if migration_named
        && (lower.ends_with(".toml")
            || lower.ends_with(".json")
            || lower.ends_with(".yaml")
            || lower.ends_with(".yml"))
    {
        "configuration"
    } else {
        return None;
    };
    Some(ClassifiedPath {
        path: path.into(),
        classification: classification.into(),
    })
}

fn classify_dependency(path: &str) -> Option<DependencyFile> {
    let name = Path::new(path).file_name()?.to_str()?;
    let (ecosystem, lockfile) = match name {
        "Cargo.toml" => ("rust", false),
        "Cargo.lock" => ("rust", true),
        "package.json" => ("node", false),
        "package-lock.json" | "pnpm-lock.yaml" | "yarn.lock" => ("node", true),
        "pyproject.toml" | "requirements.txt" => ("python", false),
        "poetry.lock" | "uv.lock" => ("python", true),
        "go.mod" => ("go", false),
        "go.sum" => ("go", true),
        "Gemfile" => ("ruby", false),
        "Gemfile.lock" => ("ruby", true),
        _ => return None,
    };
    Some(DependencyFile {
        path: path.into(),
        ecosystem: ecosystem.into(),
        lockfile,
    })
}

fn classify_generated(path: &str) -> Option<ClassifiedPath> {
    let lower = path.to_ascii_lowercase();
    let generated = lower.contains("/generated/")
        || lower.starts_with("generated/")
        || lower.contains(".generated.")
        || lower.starts_with("dist/")
        || lower.starts_with("build/")
        || lower.starts_with("target/")
        || lower.starts_with("rust-backend/static-react/");
    generated.then(|| ClassifiedPath {
        path: path.into(),
        classification: "generated".into(),
    })
}

fn split_nul(bytes: &[u8]) -> Vec<String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .map(|field| String::from_utf8_lossy(field).to_string())
        .collect()
}

fn git_text(worktree: &Path, args: &[&str]) -> Result<String, DetectorError> {
    Ok(String::from_utf8_lossy(&git_bytes(worktree, args)?).to_string())
}

fn git_bytes(worktree: &Path, args: &[&str]) -> Result<Vec<u8>, DetectorError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .output()
        .map_err(WorkspaceError::from)?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(DetectorError::GitFailed {
            command: format!("git {}", args.join(" ")),
            message: String::from_utf8_lossy(&output.stderr).trim().into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::claims::ClaimPolicy;
    use crate::coordination::contracts::ContractRegistration;
    use crate::coordination::domain::{
        ActivityEventPayload, Claim, ClaimScope, ContractExpectation, ContractKind,
        CoordinationActor, CoordinationEvent, EventSeverity, Worker, WorkerPermissionProfile,
        WorkerTurnBinding,
    };
    use crate::coordination::store::{ClaimRepository, EventRepository, WorkerRepository};
    use crate::coordination::workspace::WorktreeManager;
    use crate::tracker::Tracker;
    use chrono::Duration;
    use std::fs;
    use std::time::Instant;

    #[test]
    fn collects_bounded_worker_change_and_validation_inventory() {
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir_all(repository.join("src")).unwrap();
        run(&repository, &["init"]);
        run(&repository, &["config", "user.email", "test@example.com"]);
        run(&repository, &["config", "user.name", "Test User"]);
        fs::write(
            repository.join("Cargo.toml"),
            "[package]\nname = \"fixture\"\n",
        )
        .unwrap();
        fs::write(repository.join("src/lib.rs"), "pub fn original() {}\n").unwrap();
        run(&repository, &["add", "."]);
        run(&repository, &["commit", "-m", "base"]);
        let base = output(&repository, &["rev-parse", "HEAD"]);

        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        let manager = WorktreeManager::open(&repository).unwrap();
        let binding = manager
            .create_for_worker("goal-a", &worker.id, &base, now)
            .unwrap();
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "detectors".into(),
            },
            worker.id.clone(),
            base,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.workspace = Some(binding.clone());
        worker.active_claims.push(claim.id.clone());
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();

        let worktree = Path::new(&binding.worktree_path);
        fs::write(
            worktree.join("src/lib.rs"),
            "pub fn original() {}\npub fn changed_api() {}\n",
        )
        .unwrap();
        fs::write(
            worktree.join("Cargo.toml"),
            "[package]\nname = \"fixture\"\n[dependencies]\nserde = \"1\"\n",
        )
        .unwrap();
        fs::create_dir_all(worktree.join("migrations")).unwrap();
        fs::write(
            worktree.join("migrations/001_create_items.sql"),
            "create table items(id integer);\n",
        )
        .unwrap();
        fs::create_dir_all(worktree.join("src/generated")).unwrap();
        fs::write(
            worktree.join("src/generated/client.generated.ts"),
            "export const generatedClient = true;\n",
        )
        .unwrap();

        let validation = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker.id.clone(),
            },
            claim.id.as_str(),
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker.id.clone(),
                claim_id: claim.id.clone(),
                category: ActivityCategory::Validation,
                summary: "cargo test passed".into(),
                progress_percent: None,
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec!["test:cargo".into()],
                validation_succeeded: Some(true),
            }),
            now,
        )
        .unwrap();
        store.append_event(&validation).unwrap();

        let inventory = ChangeInventoryCollector::new(store)
            .collect(&worker.id, &claim.id, now)
            .unwrap();

        assert_eq!(inventory.worker_id, worker.id);
        assert_eq!(inventory.claim_id, claim.id);
        assert!(
            inventory
                .changed_files
                .iter()
                .any(|file| file.path == "Cargo.toml")
        );
        assert!(inventory.changed_files.iter().any(|file| {
            file.path == "migrations/001_create_items.sql"
                && file.status == FileChangeStatus::Untracked
        }));
        assert!(inventory.changed_symbols.iter().any(|symbol| {
            symbol.path == "src/lib.rs" && symbol.name == "changed_api" && symbol.kind == "fn"
        }));
        assert!(inventory.changed_symbols.iter().any(|symbol| {
            symbol.path == "src/generated/client.generated.ts"
                && symbol.name == "generatedClient"
                && symbol.kind == "const"
        }));
        assert_eq!(inventory.migrations[0].classification, "database");
        assert_eq!(inventory.dependency_files[0].ecosystem, "rust");
        assert_eq!(inventory.generated_artifacts.len(), 1);
        assert_eq!(inventory.validation_results.len(), 1);
        assert_eq!(inventory.validation_results[0].summary, "cargo test passed");
        assert!(!inventory.diff.patch_sha256.is_empty());
        assert!(!inventory.truncated);
    }

    #[test]
    fn publishes_one_typed_file_overlap_signal_for_retried_pair_detection() {
        let benchmark_started = Instant::now();
        let directory = tempfile::tempdir().unwrap();
        let repository = directory.path().join("repository");
        fs::create_dir_all(repository.join("src")).unwrap();
        run(&repository, &["init"]);
        run(&repository, &["config", "user.email", "test@example.com"]);
        run(&repository, &["config", "user.name", "Test User"]);
        fs::write(
            repository.join("src/lib.rs"),
            "pub fn shared() {\n    println!(\"base\");\n}\n",
        )
        .unwrap();
        run(&repository, &["add", "."]);
        run(&repository, &["commit", "-m", "base"]);
        let base = output(&repository, &["rev-parse", "HEAD"]);
        let now = Utc::now();
        let manager = WorktreeManager::open(&repository).unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );

        let mut left_worker = Worker::new("goal-a", now);
        let left_binding = manager
            .create_for_worker("goal-a", &left_worker.id, &base, now)
            .unwrap();
        let left_claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "left".into(),
            },
            left_worker.id.clone(),
            base.clone(),
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        left_worker.workspace = Some(left_binding.clone());
        left_worker.active_claims.push(left_claim.id.clone());
        store.upsert_worker(&left_worker).unwrap();
        store.insert_claim(&left_claim).unwrap();

        let mut right_worker = Worker::new("goal-a", now);
        let right_binding = manager
            .create_for_worker("goal-a", &right_worker.id, &base, now)
            .unwrap();
        let right_claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "right".into(),
            },
            right_worker.id.clone(),
            base.clone(),
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        right_worker.workspace = Some(right_binding.clone());
        right_worker.active_claims.push(right_claim.id.clone());
        store.upsert_worker(&right_worker).unwrap();
        store.insert_claim(&right_claim).unwrap();

        let mut independent_worker = Worker::new("goal-a", now);
        let independent_binding = manager
            .create_for_worker("goal-a", &independent_worker.id, &base, now)
            .unwrap();
        let independent_claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "independent".into(),
            },
            independent_worker.id.clone(),
            base,
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        independent_worker.workspace = Some(independent_binding.clone());
        independent_worker
            .active_claims
            .push(independent_claim.id.clone());
        store.upsert_worker(&independent_worker).unwrap();
        store.insert_claim(&independent_claim).unwrap();

        fs::write(
            Path::new(&left_binding.worktree_path).join("src/lib.rs"),
            "pub fn shared() {\n    println!(\"left\");\n}\n",
        )
        .unwrap();
        fs::write(
            Path::new(&right_binding.worktree_path).join("src/lib.rs"),
            "pub fn shared() {\n    println!(\"right\");\n}\n",
        )
        .unwrap();
        fs::write(
            Path::new(&left_binding.worktree_path).join("left-only.txt"),
            "left\n",
        )
        .unwrap();
        fs::write(
            Path::new(&right_binding.worktree_path).join("right-only.txt"),
            "right\n",
        )
        .unwrap();
        fs::create_dir_all(Path::new(&left_binding.worktree_path).join("migrations")).unwrap();
        fs::create_dir_all(Path::new(&right_binding.worktree_path).join("migrations")).unwrap();
        fs::write(
            Path::new(&left_binding.worktree_path).join("migrations/001_left.sql"),
            "alter table items add column left_value text;\n",
        )
        .unwrap();
        fs::write(
            Path::new(&right_binding.worktree_path).join("migrations/002_right.sql"),
            "alter table items add column right_value text;\n",
        )
        .unwrap();
        fs::create_dir_all(Path::new(&independent_binding.worktree_path).join("docs")).unwrap();
        fs::write(
            Path::new(&independent_binding.worktree_path).join("docs/notes.md"),
            "Independent documentation change.\n",
        )
        .unwrap();

        let detector = FileOverlapDetector::new(Arc::clone(&store));
        let first = detector
            .detect_and_publish(
                &left_worker.id,
                &left_claim.id,
                &right_worker.id,
                &right_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        let retry = detector
            .detect_and_publish(
                &right_worker.id,
                &right_claim.id,
                &left_worker.id,
                &left_claim.id,
                now,
            )
            .unwrap()
            .unwrap();

        assert_eq!(retry.id, first.id);
        assert_eq!(
            first.kind,
            super::super::domain::CoordinationEventKind::FileOverlapDetected
        );
        let Some(CoordinationEventPayload::FileOverlap(payload)) = first.typed_payload().unwrap()
        else {
            panic!("expected file overlap payload");
        };
        assert_eq!(payload.paths.len(), 1);
        assert_eq!(payload.paths[0].path, "src/lib.rs");
        assert_eq!(payload.paths[0].left_status, "modified");
        assert_eq!(payload.paths[0].right_status, "modified");
        assert_eq!(
            store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
            1
        );
        assert!(
            detector
                .detect_and_publish(
                    &left_worker.id,
                    &left_claim.id,
                    &independent_worker.id,
                    &independent_claim.id,
                    now,
                )
                .unwrap()
                .is_none()
        );

        let symbol_detector = SymbolOverlapDetector::new(Arc::clone(&store));
        let symbol = symbol_detector
            .detect_and_publish(
                &left_worker.id,
                &left_claim.id,
                &right_worker.id,
                &right_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        let symbol_retry = symbol_detector
            .detect_and_publish(
                &right_worker.id,
                &right_claim.id,
                &left_worker.id,
                &left_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        assert_eq!(symbol_retry.id, symbol.id);
        assert_eq!(
            symbol.kind,
            super::super::domain::CoordinationEventKind::SymbolOverlapDetected
        );
        let Some(CoordinationEventPayload::SymbolOverlap(payload)) =
            symbol.typed_payload().unwrap()
        else {
            panic!("expected symbol overlap payload");
        };
        assert_eq!(payload.overlaps.len(), 1);
        assert_eq!(payload.overlaps[0].identity, "fn:shared");
        assert_eq!(payload.overlaps[0].match_kind, "symbol");
        assert_eq!(
            store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
            2
        );
        assert!(
            symbol_detector
                .detect_and_publish(
                    &left_worker.id,
                    &left_claim.id,
                    &independent_worker.id,
                    &independent_claim.id,
                    now,
                )
                .unwrap()
                .is_none()
        );

        let migration_detector = MigrationOverlapDetector::new(Arc::clone(&store));
        let migration = migration_detector
            .detect_and_publish(
                &left_worker.id,
                &left_claim.id,
                &right_worker.id,
                &right_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        let migration_retry = migration_detector
            .detect_and_publish(
                &right_worker.id,
                &right_claim.id,
                &left_worker.id,
                &left_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        assert_eq!(migration_retry.id, migration.id);
        assert_eq!(
            migration.kind,
            super::super::domain::CoordinationEventKind::MigrationOverlapDetected
        );
        let Some(CoordinationEventPayload::MigrationOverlap(payload)) =
            migration.typed_payload().unwrap()
        else {
            panic!("expected migration overlap payload");
        };
        assert_eq!(payload.overlaps.len(), 1);
        assert_eq!(payload.overlaps[0].classification, "database");
        assert_eq!(payload.overlaps[0].left_paths.len(), 1);
        assert_eq!(payload.overlaps[0].right_paths.len(), 1);
        assert!(payload.overlaps[0].shared_paths.is_empty());
        assert_eq!(
            store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
            3
        );
        assert!(
            migration_detector
                .detect_and_publish(
                    &left_worker.id,
                    &left_claim.id,
                    &independent_worker.id,
                    &independent_claim.id,
                    now,
                )
                .unwrap()
                .is_none()
        );

        run(Path::new(&left_binding.worktree_path), &["add", "."]);
        run(
            Path::new(&left_binding.worktree_path),
            &["commit", "-m", "left changes"],
        );
        run(Path::new(&right_binding.worktree_path), &["add", "."]);
        run(
            Path::new(&right_binding.worktree_path),
            &["commit", "-m", "right changes"],
        );
        run(Path::new(&independent_binding.worktree_path), &["add", "."]);
        run(
            Path::new(&independent_binding.worktree_path),
            &["commit", "-m", "independent changes"],
        );
        let merge_predictor = MergeConflictPredictor::new(Arc::clone(&store));
        let conflict = merge_predictor
            .detect_and_publish(
                &left_worker.id,
                &left_claim.id,
                &right_worker.id,
                &right_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        let conflict_retry = merge_predictor
            .detect_and_publish(
                &right_worker.id,
                &right_claim.id,
                &left_worker.id,
                &left_claim.id,
                now,
            )
            .unwrap()
            .unwrap();
        assert_eq!(conflict_retry.id, conflict.id);
        assert_eq!(
            conflict.kind,
            super::super::domain::CoordinationEventKind::MergeConflictPredicted
        );
        let Some(CoordinationEventPayload::MergeConflict(payload)) =
            conflict.typed_payload().unwrap()
        else {
            panic!("expected merge conflict payload");
        };
        assert_eq!(payload.merge_base_revision, left_claim.base_revision);
        assert!(
            payload
                .conflicting_paths
                .contains(&"src/lib.rs".to_string())
        );
        assert!(payload.diagnostics.contains("CONFLICT"));
        assert_eq!(
            store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
            4
        );
        assert!(
            merge_predictor
                .detect_and_publish(
                    &left_worker.id,
                    &left_claim.id,
                    &independent_worker.id,
                    &independent_claim.id,
                    now,
                )
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.events_for_goal("goal-a", 0, None, 10).unwrap().len(),
            4,
            "independent changes must not add detector noise"
        );
        assert!(
            benchmark_started.elapsed() < std::time::Duration::from_secs(10),
            "representative three-worktree detector corpus exceeded its 10-second bound"
        );
    }

    #[test]
    fn emits_contract_and_deduplicated_dependency_change_events() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let registry = ContractRegistry::new(Arc::clone(&store));
        let now = Utc::now();
        let contract = registry
            .register(
                ContractRegistration {
                    goal_id: "goal-a".into(),
                    stable_key: "api.worker.v1".into(),
                    title: "Worker API".into(),
                    kind: ContractKind::Api,
                    producer: None,
                    compatibility_notes: "Initial contract".into(),
                },
                CoordinationActor::System,
                None,
                now,
            )
            .unwrap();
        let worker = Worker::new("goal-a", now);
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "consumer".into(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        claim.consumes_contracts.push(ContractExpectation {
            contract_id: contract.id.as_str().into(),
            expected_revision: 1,
        });
        store.insert_claim(&claim).unwrap();
        registry
            .revise(
                &contract.id,
                1,
                worker.id.clone(),
                "Adds a required response field".into(),
                CoordinationActor::Worker {
                    worker_id: worker.id.clone(),
                },
                now,
            )
            .unwrap();

        let detector = ContractChangeDetector::new(Arc::clone(&store));
        let first = detector.scan_goal("goal-a", now).unwrap();
        let retry = detector.scan_goal("goal-a", now).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(retry[0].id, first[0].id);
        let Some(CoordinationEventPayload::ContractMismatch(payload)) =
            first[0].typed_payload().unwrap()
        else {
            panic!("expected contract mismatch payload");
        };
        assert_eq!(payload.claim_id, claim.id);
        assert_eq!(payload.worker_id, worker.id);
        assert_eq!(payload.contract_id, contract.id);
        assert_eq!(payload.expected_revision, 1);
        assert_eq!(payload.current_revision, 2);
        let events = store.events_for_goal("goal-a", 0, None, 10).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.kind == super::super::domain::CoordinationEventKind::ContractChanged
                })
                .count(),
            2
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| {
                    event.kind == super::super::domain::CoordinationEventKind::DependencyChanged
                })
                .count(),
            1
        );
    }

    #[test]
    fn detects_stalled_workers_and_consecutive_validation_failures() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let started_at = now - Duration::minutes(5);
        let mut worker = Worker::new("goal-a", started_at);
        worker
            .transition(WorkerState::Starting, started_at, None)
            .unwrap();
        worker
            .transition(WorkerState::Active, started_at, None)
            .unwrap();
        worker.last_heartbeat_at = Some(started_at);
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "health".into(),
            },
            worker.id.clone(),
            "base-a",
            started_at,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();
        for (index, seconds_ago) in [90, 60, 30].into_iter().enumerate() {
            let failed = CoordinationEvent::from_typed_payload(
                "goal-a",
                EventSeverity::Error,
                CoordinationActor::Worker {
                    worker_id: worker.id.clone(),
                },
                claim.id.as_str(),
                CoordinationEventPayload::Activity(ActivityEventPayload {
                    worker_id: worker.id.clone(),
                    claim_id: claim.id.clone(),
                    category: ActivityCategory::Validation,
                    summary: format!("validation attempt {} failed", index + 1),
                    progress_percent: None,
                    changed_scope: vec![],
                    artifact_id: None,
                    evidence_refs: vec![format!("test:failure-{}", index + 1)],
                    validation_succeeded: Some(false),
                }),
                now - Duration::seconds(seconds_ago),
            )
            .unwrap();
            store.append_event(&failed).unwrap();
        }
        let detector = WorkerHealthDetector::new(
            Arc::clone(&store),
            HealthDetectionPolicy {
                heartbeat_stale_after_seconds: 60,
                activity_stale_after_seconds: 20,
                validation_failure_threshold: 3,
                validation_window_seconds: 10 * 60,
            },
        )
        .unwrap();

        let first = detector.scan_goal("goal-a", now).unwrap();
        let retry = detector.scan_goal("goal-a", now).unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(
            first
                .iter()
                .map(|event| event.id.clone())
                .collect::<Vec<_>>(),
            retry
                .iter()
                .map(|event| event.id.clone())
                .collect::<Vec<_>>()
        );
        assert!(first.iter().any(|event| {
            event.kind == super::super::domain::CoordinationEventKind::WorkerStalled
        }));
        assert!(first.iter().any(|event| {
            event.kind == super::super::domain::CoordinationEventKind::RepeatedValidationFailure
        }));

        let succeeded = CoordinationEvent::from_typed_payload(
            "goal-a",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker.id.clone(),
            },
            claim.id.as_str(),
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker.id.clone(),
                claim_id: claim.id.clone(),
                category: ActivityCategory::Validation,
                summary: "validation recovered".into(),
                progress_percent: None,
                changed_scope: vec![],
                artifact_id: None,
                evidence_refs: vec!["test:recovered".into()],
                validation_succeeded: Some(true),
            }),
            now,
        )
        .unwrap();
        store.append_event(&succeeded).unwrap();
        assert!(
            detector
                .scan_goal("goal-a", now + Duration::seconds(1))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn watchdog_enforces_leases_runtime_health_permissions_and_limits_once() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-a", "Goal A", "").unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let claim_service = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        let now = Utc::now();
        let started_at = now - Duration::minutes(10);

        let mut lease_worker = active_worker("goal-a", started_at);
        lease_worker.current_run_id = Some("run-lease".into());
        lease_worker.current_thread_id = Some("thread-lease".into());
        lease_worker.turn_history.push(WorkerTurnBinding {
            sequence: 1,
            run_id: "run-lease".into(),
            thread_id: "thread-lease".into(),
            started_at,
            completed_at: None,
            continuation_of_thread_id: None,
            context_transfer_artifact_id: None,
        });
        let lease_claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "expired".into(),
            },
            lease_worker.id.clone(),
            "base-a",
            started_at,
            now - Duration::minutes(2),
        )
        .unwrap();
        lease_worker.active_claims.push(lease_claim.id.clone());
        store.upsert_worker(&lease_worker).unwrap();
        store.insert_claim(&lease_claim).unwrap();

        let environment_worker = active_worker("goal-a", started_at);
        let mut permission_worker = active_worker("goal-a", started_at);
        permission_worker.permission_profile = Some(WorkerPermissionProfile {
            sandbox: "danger-full-access".into(),
            ..WorkerPermissionProfile::default()
        });
        let mut execution_worker = active_worker("goal-a", started_at);
        execution_worker.current_run_id = Some("run-limit".into());
        execution_worker.current_thread_id = Some("thread-limit".into());
        execution_worker.turn_history.push(WorkerTurnBinding {
            sequence: 1,
            run_id: "run-limit".into(),
            thread_id: "thread-limit".into(),
            started_at,
            completed_at: None,
            continuation_of_thread_id: None,
            context_transfer_artifact_id: None,
        });
        let failure_worker = active_worker("goal-a", started_at);
        let healthy_worker = active_worker("goal-a", now);
        for worker in [
            &environment_worker,
            &permission_worker,
            &execution_worker,
            &failure_worker,
            &healthy_worker,
        ] {
            store.upsert_worker(worker).unwrap();
        }

        let observations = vec![
            WorkerRuntimeObservation {
                worker_id: environment_worker.id.clone(),
                environment_alive: false,
                active_turn_started_at: None,
                observed_tokens: None,
                consecutive_failures: 0,
                permission_violation: None,
                ..WorkerRuntimeObservation::default()
            },
            WorkerRuntimeObservation {
                worker_id: permission_worker.id.clone(),
                environment_alive: true,
                active_turn_started_at: None,
                observed_tokens: None,
                consecutive_failures: 0,
                permission_violation: Some("network capability exceeded policy".into()),
                ..WorkerRuntimeObservation::default()
            },
            WorkerRuntimeObservation {
                worker_id: execution_worker.id.clone(),
                environment_alive: true,
                active_turn_started_at: Some(started_at),
                observed_tokens: Some(251_000),
                consecutive_failures: 0,
                permission_violation: None,
                ..WorkerRuntimeObservation::default()
            },
            WorkerRuntimeObservation {
                worker_id: failure_worker.id.clone(),
                environment_alive: true,
                active_turn_started_at: None,
                observed_tokens: Some(1_000),
                consecutive_failures: 3,
                permission_violation: None,
                ..WorkerRuntimeObservation::default()
            },
            WorkerRuntimeObservation {
                worker_id: healthy_worker.id.clone(),
                environment_alive: true,
                active_turn_started_at: Some(now),
                observed_tokens: Some(1_000),
                consecutive_failures: 0,
                permission_violation: None,
                ..WorkerRuntimeObservation::default()
            },
        ];
        let mut watchdog_policy = WatchdogPolicy::default();
        watchdog_policy.resource_quotas.max_goal_concurrency = 10;
        let watchdog = WatchdogService::new(Arc::clone(&store), watchdog_policy).unwrap();

        let report = watchdog
            .enforce(&claim_service, "goal-a", &observations, now)
            .unwrap();
        assert_eq!(report.outcomes.len(), 5);
        for kind in [
            WatchdogOutcomeKind::LeaseExpired,
            WatchdogOutcomeKind::EnvironmentRecovery,
            WatchdogOutcomeKind::PermissionViolation,
            WatchdogOutcomeKind::ResourceQuota,
            WatchdogOutcomeKind::FailureLoopCutoff,
        ] {
            assert!(report.outcomes.iter().any(|outcome| outcome.kind == kind));
        }
        assert!(
            report
                .outcomes
                .iter()
                .all(|outcome| outcome.event_sequence.is_some())
        );

        let expired_claim = store.claim(&lease_claim.id).unwrap().unwrap();
        assert_eq!(expired_claim.state, ClaimState::Expired);
        for worker_id in [
            &lease_worker.id,
            &environment_worker.id,
            &permission_worker.id,
            &execution_worker.id,
            &failure_worker.id,
        ] {
            let worker = store.worker(worker_id).unwrap().unwrap();
            assert_eq!(worker.state, WorkerState::Recovering);
        }
        let lease_worker = store.worker(&lease_worker.id).unwrap().unwrap();
        assert!(lease_worker.active_claims.is_empty());
        assert!(lease_worker.current_run_id.is_none());
        assert!(lease_worker.turn_history[0].completed_at.is_some());
        let execution_worker = store.worker(&execution_worker.id).unwrap().unwrap();
        assert!(execution_worker.current_run_id.is_none());
        assert!(execution_worker.turn_history[0].completed_at.is_some());
        assert_eq!(
            store.worker(&healthy_worker.id).unwrap().unwrap().state,
            WorkerState::Active
        );

        let events = store.events_for_goal("goal-a", 0, None, 100).unwrap();
        assert_eq!(events.len(), 5);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.typed_payload().unwrap(),
                    Some(CoordinationEventPayload::WatchdogAction(_))
                ))
                .count(),
            4
        );
        let retry = watchdog
            .enforce(
                &claim_service,
                "goal-a",
                &observations,
                now + Duration::seconds(1),
            )
            .unwrap();
        assert!(retry.outcomes.is_empty());
        assert_eq!(
            store.events_for_goal("goal-a", 0, None, 100).unwrap().len(),
            5
        );
    }

    #[test]
    fn goal_resource_quota_pauses_the_pool_and_contains_every_active_worker() {
        let directory = tempfile::tempdir().unwrap();
        let tracker = Tracker::new(directory.path().join("goals"));
        tracker.create_goal("goal-quota", "Goal quota", "").unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let claim_service = ClaimService::new(tracker, Arc::clone(&store), ClaimPolicy::default());
        let now = Utc::now();
        let first = active_worker("goal-quota", now);
        let second = active_worker("goal-quota", now);
        store.upsert_worker(&first).unwrap();
        store.upsert_worker(&second).unwrap();

        let mut quotas = ResourceQuotaPolicy::default();
        quotas.max_worker_tokens = 100;
        quotas.max_goal_tokens = 100;
        store
            .upsert_goal_pool(
                &super::super::pool::GoalPoolState {
                    goal_id: "goal-quota".into(),
                    desired_concurrency: 2,
                    mode: PoolMode::Running,
                    worker_permissions: WorkerPermissionProfile::default(),
                    resource_quotas: quotas.clone(),
                    started_at: Some(now),
                },
                now,
            )
            .unwrap();
        let observations = [
            WorkerRuntimeObservation {
                worker_id: first.id.clone(),
                environment_alive: true,
                observed_tokens: Some(60),
                ..WorkerRuntimeObservation::default()
            },
            WorkerRuntimeObservation {
                worker_id: second.id.clone(),
                environment_alive: true,
                observed_tokens: Some(60),
                ..WorkerRuntimeObservation::default()
            },
        ];
        let mut policy = WatchdogPolicy::default();
        policy.resource_quotas = quotas;
        let report = WatchdogService::new(Arc::clone(&store), policy)
            .unwrap()
            .enforce(&claim_service, "goal-quota", &observations, now)
            .unwrap();

        assert_eq!(report.outcomes.len(), 2);
        assert!(
            report
                .outcomes
                .iter()
                .all(|outcome| outcome.kind == WatchdogOutcomeKind::ResourceQuota)
        );
        assert!(
            report
                .outcomes
                .iter()
                .all(|outcome| outcome.reason.contains("goal resource quota exceeded"))
        );
        assert_eq!(
            store.goal_pool("goal-quota").unwrap().unwrap().mode,
            PoolMode::Paused
        );
        assert_eq!(
            store.worker(&first.id).unwrap().unwrap().state,
            WorkerState::Recovering
        );
        assert_eq!(
            store.worker(&second.id).unwrap().unwrap().state,
            WorkerState::Recovering
        );
    }

    #[test]
    fn rejects_cross_worker_inventory_requests() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap(),
        );
        let now = Utc::now();
        let owner = Worker::new("goal-a", now);
        let claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "detectors".into(),
            },
            owner.id,
            "base",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        store.insert_claim(&claim).unwrap();
        let other = Worker::new("goal-a", now);
        store.upsert_worker(&other).unwrap();

        assert!(matches!(
            ChangeInventoryCollector::new(store).collect(&other.id, &claim.id, now),
            Err(DetectorError::ClaimNotOwned)
        ));
    }

    fn run(repository: &Path, args: &[&str]) {
        let result = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&result.stderr)
        );
    }

    fn output(repository: &Path, args: &[&str]) -> String {
        let result = Command::new("git")
            .arg("-C")
            .arg(repository)
            .args(args)
            .output()
            .unwrap();
        assert!(result.status.success());
        String::from_utf8_lossy(&result.stdout).trim().into()
    }

    fn active_worker(goal_id: &str, now: DateTime<Utc>) -> Worker {
        let mut worker = Worker::new(goal_id, now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.permission_profile = Some(WorkerPermissionProfile::default());
        worker
    }
}
