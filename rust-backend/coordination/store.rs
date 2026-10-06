use super::domain::*;
use super::pool::GoalPoolState;
use super::rollout::{RolloutControl, ShadowObservation};
use crate::observability::CorrelationIds;
use crate::redaction::redact_sensitive_value;
use chrono::{DateTime, Utc};
use rusqlite::backup::Backup;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use thiserror::Error;

const STORE_SCHEMA_VERSION: i64 = 16;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("coordination store lock was poisoned")]
    LockPoisoned,
    #[error("coordination store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("coordination store database failed: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("coordination record serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("active claim already exists for {goal_id}/{scope_key}")]
    ActiveClaimExists { goal_id: String, scope_key: String },
    #[error("shared contract already exists for {goal_id}/{stable_key}")]
    ContractAlreadyExists { goal_id: String, stable_key: String },
    #[error("coordination record was not found: {0}")]
    NotFound(String),
    #[error("unsupported coordination store schema version {0}")]
    UnsupportedSchemaVersion(i64),
    #[error("coordination store integrity check failed: {0}")]
    Integrity(String),
    #[error("invalid event replay cursor: {0}")]
    InvalidReplayCursor(String),
    #[error("invalid Supervisor intervention revision: {0}")]
    InvalidInterventionRevision(String),
    #[error("notification revision changed while coalescing: {0}")]
    StaleNotificationRevision(String),
    #[error("integration artifact already has an active queue entry: {0}")]
    IntegrationArtifactAlreadyQueued(String),
    #[error(
        "repository {repository_id} has an earlier queued integration job for another goal than {goal_id}"
    )]
    IntegrationQueueGoalMismatch {
        repository_id: String,
        goal_id: String,
    },
    #[error(transparent)]
    Domain(#[from] DomainError),
}

#[derive(Debug, Clone)]
pub struct ExpectedRecordUpdate<T> {
    pub record: T,
    pub expected_updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct EscalationDecisionSideEffects {
    pub claims: Vec<ExpectedRecordUpdate<Claim>>,
    pub workers: Vec<ExpectedRecordUpdate<Worker>>,
    pub work_packages: Vec<ExpectedRecordUpdate<WorkPackage>>,
}

#[derive(Debug, Clone)]
pub struct ClaimResumeMutation {
    pub escalation: ExpectedRecordUpdate<HumanEscalation>,
    pub original_claim_id: ClaimId,
    pub expected_original_updated_at: DateTime<Utc>,
    pub replacement_claim: Claim,
    pub worker: Worker,
    pub expected_worker_updated_at: Option<DateTime<Utc>>,
    pub work_package: Option<ExpectedRecordUpdate<WorkPackage>>,
}

#[derive(Debug, Clone)]
pub struct IntegrationFailureStoreOutcome {
    pub job: IntegrationJob,
    pub worker: Worker,
    pub claim: Claim,
}

pub trait WorkerRepository {
    fn upsert_worker(&self, worker: &Worker) -> Result<(), StoreError>;
    fn worker(&self, id: &WorkerId) -> Result<Option<Worker>, StoreError>;
    fn workers_for_goal(
        &self,
        goal_id: &str,
        state: Option<WorkerState>,
    ) -> Result<Vec<Worker>, StoreError>;
}

pub trait PoolRepository {
    fn upsert_goal_pool(&self, state: &GoalPoolState, now: DateTime<Utc>)
    -> Result<(), StoreError>;
    fn goal_pool(&self, goal_id: &str) -> Result<Option<GoalPoolState>, StoreError>;
    fn goal_pools(&self) -> Result<Vec<GoalPoolState>, StoreError>;
}

pub trait WorkPackageRepository {
    fn upsert_work_package(&self, package: &WorkPackage) -> Result<(), StoreError>;
    fn work_package(&self, id: &WorkPackageId) -> Result<Option<WorkPackage>, StoreError>;
    fn work_packages_for_goal(
        &self,
        goal_id: &str,
        state: Option<WorkPackageState>,
    ) -> Result<Vec<WorkPackage>, StoreError>;
}

pub trait ClaimRepository {
    fn insert_claim(&self, claim: &Claim) -> Result<(), StoreError>;
    fn upsert_claim(&self, claim: &Claim) -> Result<(), StoreError>;
    fn claim(&self, id: &ClaimId) -> Result<Option<Claim>, StoreError>;
    fn claims_for_goal(
        &self,
        goal_id: &str,
        state: Option<ClaimState>,
    ) -> Result<Vec<Claim>, StoreError>;
    fn active_claim_for_scope(
        &self,
        goal_id: &str,
        scope: &ClaimScope,
    ) -> Result<Option<Claim>, StoreError>;
    fn latest_claim_generation(&self, goal_id: &str, scope: &ClaimScope)
    -> Result<u64, StoreError>;
}

pub trait ContractRepository {
    fn upsert_contract(&self, contract: &SharedContract) -> Result<(), StoreError>;
    fn contract(&self, id: &ContractId) -> Result<Option<SharedContract>, StoreError>;
    fn contract_by_stable_key(
        &self,
        goal_id: &str,
        stable_key: &str,
    ) -> Result<Option<SharedContract>, StoreError>;
    fn contracts_for_goal(&self, goal_id: &str) -> Result<Vec<SharedContract>, StoreError>;
}

pub trait EventRepository {
    fn append_event(&self, event: &CoordinationEvent) -> Result<u64, StoreError>;
    fn event(&self, id: &EventId) -> Result<Option<CoordinationEvent>, StoreError>;
    fn events_for_goal(
        &self,
        goal_id: &str,
        after_sequence: u64,
        correlation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CoordinationEvent>, StoreError>;
    fn latest_events_for_goal(
        &self,
        goal_id: &str,
        correlation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CoordinationEvent>, StoreError>;
    fn replay_events_for_consumer(
        &self,
        consumer_id: &str,
        goal_id: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<EventReplayBatch, StoreError>;
    fn acknowledge_event_sequence(
        &self,
        consumer_id: &str,
        goal_id: &str,
        sequence: u64,
        now: DateTime<Utc>,
    ) -> Result<EventReplayCursor, StoreError>;
    fn event_replay_cursor(
        &self,
        consumer_id: &str,
        goal_id: &str,
    ) -> Result<Option<EventReplayCursor>, StoreError>;
    fn compact_events(
        &self,
        goal_id: &str,
        policy: &EventRetentionPolicy,
        now: DateTime<Utc>,
    ) -> Result<EventCompactionReport, StoreError>;
    fn archived_events_for_goal(
        &self,
        goal_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ArchivedCoordinationEvent>, StoreError>;
}

pub trait SignalRepository {
    fn signal(&self, id: &SignalId) -> Result<Option<CoordinationSignal>, StoreError>;
    fn signals_for_goal(
        &self,
        goal_id: &str,
        state: Option<SignalState>,
    ) -> Result<Vec<CoordinationSignal>, StoreError>;
}

pub trait InterventionRepository {
    fn upsert_intervention(&self, intervention: &SupervisorIntervention) -> Result<(), StoreError>;
    fn interventions_for_goal(
        &self,
        goal_id: &str,
        state: Option<InterventionState>,
    ) -> Result<Vec<SupervisorIntervention>, StoreError>;
}

pub trait SupervisorBudgetRepository {
    fn upsert_supervisor_budget(&self, ledger: &SupervisorBudgetLedger) -> Result<(), StoreError>;
    fn supervisor_budget(
        &self,
        goal_id: &str,
    ) -> Result<Option<SupervisorBudgetLedger>, StoreError>;
}

pub trait NotificationRepository {
    fn upsert_notification(&self, notification: &WorkerNotification) -> Result<(), StoreError>;
    fn notification(&self, id: &NotificationId) -> Result<Option<WorkerNotification>, StoreError>;
    fn update_notification_if_revision(
        &self,
        notification: &WorkerNotification,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError>;
    fn notifications_for_worker(
        &self,
        worker_id: &WorkerId,
        state: Option<NotificationState>,
    ) -> Result<Vec<WorkerNotification>, StoreError>;
    fn notifications_for_goal(
        &self,
        goal_id: &str,
        state: Option<NotificationState>,
    ) -> Result<Vec<WorkerNotification>, StoreError>;
}

pub trait ExchangeRepository {
    fn insert_exchange(&self, exchange: &CoordinationExchange) -> Result<(), StoreError>;
    fn exchange(&self, id: &ExchangeId) -> Result<Option<CoordinationExchange>, StoreError>;
    fn update_exchange_if_revision(
        &self,
        exchange: &CoordinationExchange,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError>;
    fn exchanges_for_goal(
        &self,
        goal_id: &str,
        state: Option<CoordinationExchangeState>,
    ) -> Result<Vec<CoordinationExchange>, StoreError>;
}

pub trait IntegrationArtifactRepository {
    fn insert_integration_artifact(&self, artifact: &IntegrationArtifact)
    -> Result<(), StoreError>;
    fn integration_artifact(
        &self,
        id: &IntegrationArtifactId,
    ) -> Result<Option<IntegrationArtifact>, StoreError>;
    fn integration_artifacts_for_goal(
        &self,
        goal_id: &str,
    ) -> Result<Vec<IntegrationArtifact>, StoreError>;
}

pub trait ValidationReportRepository {
    fn insert_validation_report(
        &self,
        report: &IntegrationValidationReport,
    ) -> Result<(), StoreError>;
    fn validation_report(
        &self,
        id: &ValidationReportId,
    ) -> Result<Option<IntegrationValidationReport>, StoreError>;
    fn validation_reports_for_artifact(
        &self,
        artifact_id: &IntegrationArtifactId,
    ) -> Result<Vec<IntegrationValidationReport>, StoreError>;
}

pub trait ReconciliationRepository {
    fn insert_reconciliation(&self, record: &ReconciliationRecord) -> Result<(), StoreError>;
    fn reconciliation(
        &self,
        id: &ReconciliationId,
    ) -> Result<Option<ReconciliationRecord>, StoreError>;
    fn reconciliations_for_artifact(
        &self,
        artifact_id: &IntegrationArtifactId,
    ) -> Result<Vec<ReconciliationRecord>, StoreError>;
}

pub trait IntegrationJobRepository {
    fn integration_job(&self, id: &IntegrationJobId) -> Result<Option<IntegrationJob>, StoreError>;
    fn integration_jobs_for_repository(
        &self,
        repository_id: &str,
        state: Option<IntegrationJobState>,
    ) -> Result<Vec<IntegrationJob>, StoreError>;
    fn integration_jobs_for_goal(
        &self,
        goal_id: &str,
        state: Option<IntegrationJobState>,
    ) -> Result<Vec<IntegrationJob>, StoreError>;
}

pub trait IntegrationFinalizationRepository {
    fn insert_integration_finalization(
        &self,
        finalization: &IntegrationFinalization,
    ) -> Result<(), StoreError>;
    fn integration_finalization(
        &self,
        id: &IntegrationFinalizationId,
    ) -> Result<Option<IntegrationFinalization>, StoreError>;
    fn prepared_integration_finalizations(
        &self,
    ) -> Result<Vec<IntegrationFinalization>, StoreError>;
}

pub trait IntegrationMaintenanceRepository {
    fn insert_integration_maintenance(
        &self,
        record: &IntegrationMaintenanceRecord,
    ) -> Result<(), StoreError>;
    fn integration_maintenance(
        &self,
        id: &IntegrationMaintenanceId,
    ) -> Result<Option<IntegrationMaintenanceRecord>, StoreError>;
    fn integration_maintenance_for_artifact(
        &self,
        artifact_id: &IntegrationArtifactId,
    ) -> Result<Vec<IntegrationMaintenanceRecord>, StoreError>;
}

pub trait EscalationRepository {
    fn upsert_escalation(&self, escalation: &HumanEscalation) -> Result<(), StoreError>;
    fn escalation(&self, id: &EscalationId) -> Result<Option<HumanEscalation>, StoreError>;
    fn update_escalation_if_revision(
        &self,
        escalation: &HumanEscalation,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError>;
    fn escalations_for_goal(
        &self,
        goal_id: &str,
        state: Option<EscalationState>,
    ) -> Result<Vec<HumanEscalation>, StoreError>;
}

pub trait IdempotencyRepository {
    fn idempotent_outcome(&self, key: &str) -> Result<Option<serde_json::Value>, StoreError>;
    fn record_idempotent_outcome(
        &self,
        key: &str,
        operation: &str,
        outcome: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> Result<bool, StoreError>;
}

pub trait RolloutRepository {
    fn upsert_rollout_control(&self, control: &RolloutControl) -> Result<(), StoreError>;
    fn rollout_control(&self, goal_id: &str) -> Result<Option<RolloutControl>, StoreError>;
    fn insert_shadow_observation(&self, observation: &ShadowObservation) -> Result<(), StoreError>;
    fn shadow_observations_for_goal(
        &self,
        goal_id: &str,
    ) -> Result<Vec<ShadowObservation>, StoreError>;
}

pub trait CoordinationStore:
    WorkerRepository
    + PoolRepository
    + WorkPackageRepository
    + ClaimRepository
    + ContractRepository
    + EventRepository
    + SignalRepository
    + InterventionRepository
    + SupervisorBudgetRepository
    + NotificationRepository
    + ExchangeRepository
    + IntegrationArtifactRepository
    + ValidationReportRepository
    + ReconciliationRepository
    + IntegrationJobRepository
    + IntegrationFinalizationRepository
    + IntegrationMaintenanceRepository
    + EscalationRepository
    + IdempotencyRepository
    + RolloutRepository
    + Send
    + Sync
{
}

impl<T> CoordinationStore for T where
    T: WorkerRepository
        + PoolRepository
        + WorkPackageRepository
        + ClaimRepository
        + ContractRepository
        + EventRepository
        + SignalRepository
        + InterventionRepository
        + SupervisorBudgetRepository
        + NotificationRepository
        + ExchangeRepository
        + IntegrationArtifactRepository
        + ValidationReportRepository
        + ReconciliationRepository
        + IntegrationJobRepository
        + IntegrationFinalizationRepository
        + IntegrationMaintenanceRepository
        + EscalationRepository
        + IdempotencyRepository
        + RolloutRepository
        + Send
        + Sync
{
}

#[derive(Debug)]
pub struct SqliteCoordinationStore {
    path: PathBuf,
    connection: Mutex<Connection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalMergeResult {
    pub signal: CoordinationSignal,
    pub created: bool,
    pub changed: bool,
}

impl SqliteCoordinationStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(&path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "busy_timeout", 5_000_i64)?;
        apply_migrations(&connection)?;
        let store = Self {
            path,
            connection: Mutex::new(connection),
        };
        store.integrity_check()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn put_context_transfer<T: Serialize>(
        &self,
        id: &str,
        goal_id: &str,
        worker_id: &WorkerId,
        claim_id: &ClaimId,
        created_at: DateTime<Utc>,
        artifact: &T,
    ) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO context_transfers(
                 id, goal_id, worker_id, claim_id, created_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
            params![
                id,
                goal_id,
                worker_id.as_str(),
                claim_id.as_str(),
                updated_at(created_at),
                encode(artifact)?
            ],
        )?;
        Ok(())
    }

    pub fn context_transfer<T: DeserializeOwned>(&self, id: &str) -> Result<Option<T>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM context_transfers WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    pub fn with_immediate_transaction<T>(
        &self,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let mut connection = self.lock()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = operation(&transaction)?;
        transaction.commit()?;
        Ok(result)
    }

    pub fn enqueue_integration_job(&self, job: &IntegrationJob) -> Result<(), StoreError> {
        let result = self.with_immediate_transaction(|transaction| {
            insert_integration_job_transaction(transaction, job)
        });
        let result = match result {
            Err(StoreError::Database(error)) if is_constraint_violation(&error) => Err(
                StoreError::IntegrationArtifactAlreadyQueued(job.artifact_id.as_str().into()),
            ),
            other => other,
        };
        if result.is_ok() {
            CorrelationIds::for_goal(&job.goal_id)
                .with_integration(job.id.as_str())
                .emit_info("integration.job.persisted", "queued");
        }
        result
    }

    pub fn recover_running_integration_jobs(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<IntegrationJob>, StoreError> {
        let recovered = self.with_immediate_transaction(|transaction| {
            let encoded = {
                let mut statement = transaction.prepare(
                    "SELECT data FROM integration_jobs WHERE state = 'running' ORDER BY rowid",
                )?;
                statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut recovered = Vec::with_capacity(encoded.len());
            for encoded in encoded {
                let mut job: IntegrationJob = decode(encoded)?;
                job.recover_after_restart(now)?;
                update_integration_job_transaction(
                    transaction,
                    &job,
                    IntegrationJobState::Running,
                )?;
                recovered.push(job);
            }
            Ok(recovered)
        })?;
        for job in &recovered {
            CorrelationIds::for_goal(&job.goal_id)
                .with_integration(job.id.as_str())
                .emit_warn("integration.job.recovered", "requeued_after_restart");
        }
        Ok(recovered)
    }

    pub fn acquire_next_integration_job(
        &self,
        repository_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<IntegrationJob>, StoreError> {
        self.acquire_next_integration_job_for_goal(repository_id, None, now)
    }

    pub fn acquire_next_integration_job_for_goal(
        &self,
        repository_id: &str,
        goal_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<Option<IntegrationJob>, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let running: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM integration_jobs WHERE repository_id = ?1 AND state = 'running'",
                [repository_id],
                |row| row.get(0),
            )?;
            if running > 0 {
                return Ok(None);
            }
            let encoded = transaction
                .query_row(
                    "SELECT data FROM integration_jobs WHERE repository_id = ?1 AND state = 'queued' ORDER BY priority DESC, enqueued_at, rowid LIMIT 1",
                    [repository_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            let Some(encoded) = encoded else {
                return Ok(None);
            };
            let mut job: IntegrationJob = decode(encoded)?;
            if let Some(goal_id) = goal_id
                && job.goal_id != goal_id
            {
                return Err(StoreError::IntegrationQueueGoalMismatch {
                    repository_id: repository_id.into(),
                    goal_id: goal_id.into(),
                });
            }
            job.start(now)?;
            update_integration_job_transaction(transaction, &job, IntegrationJobState::Queued)?;
            Ok(Some(job))
        })
    }

    pub fn finish_integration_job(
        &self,
        job_id: &IntegrationJobId,
        succeeded: bool,
        summary: &str,
        now: DateTime<Utc>,
    ) -> Result<IntegrationJob, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let encoded = transaction
                .query_row(
                    "SELECT data FROM integration_jobs WHERE id = ?1",
                    [job_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .ok_or_else(|| StoreError::NotFound(job_id.as_str().into()))?;
            let mut job: IntegrationJob = decode(encoded)?;
            job.finish(succeeded, summary, now)?;
            update_integration_job_transaction(transaction, &job, IntegrationJobState::Running)?;
            Ok(job)
        })
    }

    pub fn commit_integration_finalization(
        &self,
        finalization_id: &IntegrationFinalizationId,
        event: &CoordinationEvent,
        now: DateTime<Utc>,
    ) -> Result<IntegrationFinalization, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let mut finalization: IntegrationFinalization = decode(
                transaction
                    .query_row(
                        "SELECT data FROM integration_finalizations WHERE id = ?1",
                        [finalization_id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .ok_or_else(|| StoreError::NotFound(finalization_id.as_str().into()))?,
            )?;
            if finalization.state == IntegrationFinalizationState::Completed {
                return Ok(finalization);
            }
            if event.id != finalization.completion_event_id
                || event.goal_id != finalization.goal_id
                || event.kind != CoordinationEventKind::CompletionPublished
            {
                return Err(StoreError::Integrity(
                    "completion event does not match the prepared finalization".into(),
                ));
            }
            let artifact: IntegrationArtifact = transaction
                .query_row(
                    "SELECT data FROM integration_artifacts WHERE id = ?1",
                    [finalization.artifact_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(finalization.artifact_id.as_str().into()))?;
            let report: IntegrationValidationReport = transaction
                .query_row(
                    "SELECT data FROM integration_validation_reports WHERE id = ?1",
                    [finalization.validation_report_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| {
                    StoreError::NotFound(finalization.validation_report_id.as_str().into())
                })?;
            if !report.passed || report.artifact_id != artifact.id {
                return Err(StoreError::Integrity(
                    "finalization requires a passing validation report for its artifact".into(),
                ));
            }
            let mut job: IntegrationJob = transaction
                .query_row(
                    "SELECT data FROM integration_jobs WHERE id = ?1",
                    [finalization.job_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(finalization.job_id.as_str().into()))?;
            let mut claim: Claim = transaction
                .query_row(
                    "SELECT data FROM claims WHERE id = ?1",
                    [finalization.claim_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(finalization.claim_id.as_str().into()))?;
            let mut worker: Worker = transaction
                .query_row(
                    "SELECT data FROM workers WHERE id = ?1",
                    [finalization.worker_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(finalization.worker_id.as_str().into()))?;
            if job.state != IntegrationJobState::Running
                || job.artifact_id != artifact.id
                || claim.state != ClaimState::Active
                || claim.owner != worker.id
                || claim.lease_generation != artifact.claim_generation
            {
                return Err(StoreError::Integrity(
                    "job, claim, worker, or artifact changed before finalization".into(),
                ));
            }
            for change in &artifact.changed_contracts {
                let encoded = transaction
                    .query_row(
                        "SELECT data FROM contracts WHERE id = ?1",
                        [change.contract_id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .ok_or_else(|| StoreError::NotFound(change.contract_id.as_str().into()))?;
                let mut contract: SharedContract = decode(encoded)?;
                if contract.revision != change.revision {
                    return Err(StoreError::Integrity(format!(
                        "contract {} changed before finalization",
                        contract.stable_key
                    )));
                }
                contract.compatibility_notes = change.compatibility_notes.clone();
                contract.last_changed_by = Some(worker.id.clone());
                contract.metadata.touch(now);
                transaction.execute(
                    "UPDATE contracts SET revision = ?1, updated_at = ?2, data = ?3 WHERE id = ?4",
                    params![
                        contract.revision,
                        updated_at(contract.metadata.updated_at),
                        encode(&contract)?,
                        contract.id.as_str()
                    ],
                )?;
            }
            job.finish(true, "integration finalized", now)?;
            claim.transition(
                ClaimState::Completed,
                ClaimOutcome {
                    kind: ClaimOutcomeKind::Completed,
                    reason: "validated integration finalized".into(),
                    artifact_id: Some(artifact.id.as_str().into()),
                    evidence_refs: finalization.tracker_evidence.clone(),
                    escalation_id: None,
                    at: now,
                },
            )?;
            worker.active_claims.retain(|id| id != &claim.id);
            if worker.active_claims.is_empty() {
                if worker.state == WorkerState::Starting {
                    worker.transition(WorkerState::Active, now, None)?;
                }
                if matches!(worker.state, WorkerState::Active | WorkerState::Waiting) {
                    worker.transition(
                        WorkerState::Completed,
                        now,
                        Some("integration finalized".into()),
                    )?;
                }
            } else {
                worker.metadata.touch(now);
            }
            update_integration_job_transaction(transaction, &job, IntegrationJobState::Running)?;
            transaction.execute(
                "UPDATE claims SET state = ?1, updated_at = ?2, data = ?3 WHERE id = ?4 AND state = 'active'",
                params![
                    enum_key(&claim.state)?,
                    updated_at(claim.metadata.updated_at),
                    encode(&claim)?,
                    claim.id.as_str()
                ],
            )?;
            upsert_worker_transaction(transaction, &worker)?;
            append_event_transaction(
                transaction,
                event,
                Some(&format!("integration.finalize:{}", finalization.id.as_str())),
            )?;
            finalization.state = IntegrationFinalizationState::Completed;
            finalization.completed_at = Some(now);
            finalization.metadata.touch(now);
            transaction.execute(
                "UPDATE integration_finalizations SET state = ?1, updated_at = ?2, data = ?3 WHERE id = ?4 AND state = 'prepared'",
                params![
                    enum_key(&finalization.state)?,
                    updated_at(finalization.metadata.updated_at),
                    encode(&finalization)?,
                    finalization.id.as_str()
                ],
            )?;
            Ok(finalization)
        })
    }

    pub fn record_integration_failure(
        &self,
        job_id: &IntegrationJobId,
        summary: &str,
        notification: Option<&WorkerNotification>,
        escalation: Option<&HumanEscalation>,
        event: &CoordinationEvent,
        now: DateTime<Utc>,
    ) -> Result<IntegrationFailureStoreOutcome, StoreError> {
        if notification.is_some() == escalation.is_some() {
            return Err(StoreError::Integrity(
                "integration failure must route to exactly one worker notification or escalation"
                    .into(),
            ));
        }
        if let Some(notification) = notification {
            notification.validate()?;
        }
        self.with_immediate_transaction(|transaction| {
            let mut job: IntegrationJob = transaction
                .query_row(
                    "SELECT data FROM integration_jobs WHERE id = ?1",
                    [job_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(job_id.as_str().into()))?;
            let artifact: IntegrationArtifact = transaction
                .query_row(
                    "SELECT data FROM integration_artifacts WHERE id = ?1",
                    [job.artifact_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(job.artifact_id.as_str().into()))?;
            let mut claim: Claim = transaction
                .query_row(
                    "SELECT data FROM claims WHERE id = ?1",
                    [artifact.claim_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(artifact.claim_id.as_str().into()))?;
            let mut worker: Worker = transaction
                .query_row(
                    "SELECT data FROM workers WHERE id = ?1",
                    [artifact.worker_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .ok_or_else(|| StoreError::NotFound(artifact.worker_id.as_str().into()))?;
            if job.state != IntegrationJobState::Running
                || job.artifact_id != artifact.id
                || artifact.goal_id != job.goal_id
                || claim.state != ClaimState::Active
                || claim.owner != worker.id
                || claim.lease_generation != artifact.claim_generation
                || worker.workspace.is_none()
                || event.goal_id != artifact.goal_id
                || event.kind != CoordinationEventKind::BlockerRaised
            {
                return Err(StoreError::Integrity(
                    "integration failure target changed or no longer has a preserved workspace"
                        .into(),
                ));
            }
            if let Some(notification) = notification {
                if notification.goal_id != artifact.goal_id
                    || notification.target_worker != worker.id
                    || notification.source_event_id != event.id
                {
                    return Err(StoreError::Integrity(
                        "integration failure notification does not match the failed artifact"
                            .into(),
                    ));
                }
            }
            if let Some(escalation) = escalation {
                if escalation.goal_id != artifact.goal_id
                    || escalation.state != EscalationState::Open
                    || escalation.scope
                        != (EscalationScope::IntegrationLane {
                            repository_id: artifact.repository_id.clone(),
                        })
                {
                    return Err(StoreError::Integrity(
                        "integration failure escalation does not match the failed lane".into(),
                    ));
                }
            }

            job.finish(false, summary, now)?;
            update_integration_job_transaction(transaction, &job, IntegrationJobState::Running)?;

            if let Some(escalation) = escalation {
                claim.transition(
                    ClaimState::Blocked,
                    ClaimOutcome {
                        kind: ClaimOutcomeKind::Blocked,
                        reason: summary.into(),
                        artifact_id: Some(artifact.id.as_str().into()),
                        evidence_refs: escalation.evidence_refs.clone(),
                        escalation_id: Some(escalation.id.clone()),
                        at: now,
                    },
                )?;
                worker.active_claims.retain(|id| id != &claim.id);
                if worker.active_claims.is_empty()
                    && matches!(worker.state, WorkerState::Active | WorkerState::Waiting)
                {
                    worker.transition(WorkerState::Blocked, now, None)?;
                } else {
                    worker.metadata.touch(now);
                }
                transaction.execute(
                    r#"INSERT INTO escalations(id, goal_id, state, severity, updated_at, data)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    params![
                        escalation.id.as_str(),
                        escalation.goal_id,
                        enum_key(&escalation.state)?,
                        enum_key(&escalation.severity)?,
                        updated_at(escalation.metadata.updated_at),
                        encode(escalation)?
                    ],
                )?;
                transaction.execute(
                    "UPDATE claims SET state = ?1, updated_at = ?2, data = ?3 WHERE id = ?4 AND state = 'active'",
                    params![
                        enum_key(&claim.state)?,
                        updated_at(claim.metadata.updated_at),
                        encode(&claim)?,
                        claim.id.as_str()
                    ],
                )?;
            } else if worker.state == WorkerState::Active {
                worker.transition(WorkerState::Waiting, now, None)?;
            } else {
                worker.metadata.touch(now);
            }
            upsert_worker_transaction(transaction, &worker)?;

            if let Some(notification) = notification {
                transaction.execute(
                    r#"INSERT INTO notifications(id, goal_id, worker_id, state, updated_at, data)
                       VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    params![
                        notification.id.as_str(),
                        notification.goal_id,
                        notification.target_worker.as_str(),
                        enum_key(&notification.state)?,
                        updated_at(notification.metadata.updated_at),
                        encode(notification)?
                    ],
                )?;
            }
            append_event_transaction(
                transaction,
                event,
                Some(&format!("integration.failure:{}", job.id.as_str())),
            )?;
            Ok(IntegrationFailureStoreOutcome { job, worker, claim })
        })
    }

    pub fn persist_intervention_revision(
        &self,
        intervention: &SupervisorIntervention,
        superseded: Option<&SupervisorIntervention>,
    ) -> Result<(), StoreError> {
        if let Some(previous) = superseded {
            if previous.goal_id != intervention.goal_id
                || previous.state != InterventionState::Superseded
                || intervention.supersedes.as_ref() != Some(&previous.id)
            {
                return Err(StoreError::InvalidInterventionRevision(
                    "the superseded record and replacement must belong to one goal and form a valid chain"
                        .into(),
                ));
            }
        } else if intervention.supersedes.is_some() {
            return Err(StoreError::InvalidInterventionRevision(
                "a replacement cannot reference a superseded record that was not supplied".into(),
            ));
        }
        self.with_immediate_transaction(|transaction| {
            if let Some(previous) = superseded {
                upsert_intervention_transaction(transaction, previous)?;
            }
            upsert_intervention_transaction(transaction, intervention)?;
            Ok(())
        })?;
        CorrelationIds::for_goal(&intervention.goal_id)
            .with_intervention(intervention.id.as_str())
            .emit_info(
                "supervisor.intervention.persisted",
                &format!("{:?}", intervention.state).to_ascii_lowercase(),
            );
        Ok(())
    }

    pub fn apply_escalation_decision_with_event(
        &self,
        escalation: &HumanEscalation,
        expected_updated_at: DateTime<Utc>,
        effects: &EscalationDecisionSideEffects,
        event: &CoordinationEvent,
        publication_key: &str,
    ) -> Result<Option<u64>, StoreError> {
        self.with_immediate_transaction(|transaction| {
            for update in &effects.claims {
                let current = transaction
                    .query_row(
                        "SELECT data FROM claims WHERE id = ?1",
                        [update.record.id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .map(decode::<Claim>)
                    .transpose()?;
                if current.as_ref().is_none_or(|claim| {
                    claim.metadata.updated_at != update.expected_updated_at
                        || claim.state != ClaimState::Active
                        || claim.lease_generation != update.record.lease_generation
                }) {
                    return Ok(None);
                }
            }
            for update in &effects.workers {
                let current = transaction
                    .query_row(
                        "SELECT data FROM workers WHERE id = ?1",
                        [update.record.id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .map(decode::<Worker>)
                    .transpose()?;
                if current
                    .as_ref()
                    .is_none_or(|worker| worker.metadata.updated_at != update.expected_updated_at)
                {
                    return Ok(None);
                }
            }
            for update in &effects.work_packages {
                let current = transaction
                    .query_row(
                        "SELECT data FROM work_packages WHERE id = ?1",
                        [update.record.id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .map(decode::<WorkPackage>)
                    .transpose()?;
                if current
                    .as_ref()
                    .is_none_or(|package| package.metadata.updated_at != update.expected_updated_at)
                {
                    return Ok(None);
                }
            }
            let updated = transaction.execute(
                r#"UPDATE escalations
                   SET state = ?1, severity = ?2, updated_at = ?3, data = ?4
                   WHERE id = ?5 AND goal_id = ?6 AND updated_at = ?7"#,
                params![
                    enum_key(&escalation.state)?,
                    enum_key(&escalation.severity)?,
                    updated_at(escalation.metadata.updated_at),
                    encode(escalation)?,
                    escalation.id.as_str(),
                    escalation.goal_id,
                    updated_at(expected_updated_at)
                ],
            )?;
            if updated != 1 {
                return Ok(None);
            }
            for update in &effects.claims {
                transaction.execute(
                    r#"UPDATE claims
                       SET state = ?1, updated_at = ?2, data = ?3
                       WHERE id = ?4 AND state = 'active' AND lease_generation = ?5"#,
                    params![
                        enum_key(&update.record.state)?,
                        updated_at(update.record.metadata.updated_at),
                        encode(&update.record)?,
                        update.record.id.as_str(),
                        update.record.lease_generation,
                    ],
                )?;
            }
            for update in &effects.workers {
                upsert_worker_transaction(transaction, &update.record)?;
            }
            for update in &effects.work_packages {
                transaction.execute(
                    r#"UPDATE work_packages
                       SET state = ?1, priority = ?2, ready_at = ?3, updated_at = ?4, data = ?5
                       WHERE id = ?6 AND goal_id = ?7"#,
                    params![
                        enum_key(&update.record.state)?,
                        update.record.priority,
                        update.record.ready_at.map(updated_at),
                        updated_at(update.record.metadata.updated_at),
                        encode(&update.record)?,
                        update.record.id.as_str(),
                        update.record.goal_id,
                    ],
                )?;
            }
            append_event_transaction(transaction, event, Some(publication_key)).map(Some)
        })
    }

    pub fn resume_blocked_claim_with_event(
        &self,
        mutation: &ClaimResumeMutation,
        event: &CoordinationEvent,
        publication_key: &str,
    ) -> Result<Option<u64>, StoreError> {
        let scope_key = claim_scope_key(&mutation.replacement_claim.scope);
        let result = self.with_immediate_transaction(|transaction| {
            let current_escalation = transaction
                .query_row(
                    "SELECT data FROM escalations WHERE id = ?1",
                    [mutation.escalation.record.id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode::<HumanEscalation>)
                .transpose()?;
            if current_escalation.as_ref().is_none_or(|escalation| {
                escalation.metadata.updated_at != mutation.escalation.expected_updated_at
                    || escalation.goal_id != mutation.replacement_claim.goal_id
            }) {
                return Ok(None);
            }
            let original = transaction
                .query_row(
                    "SELECT data FROM claims WHERE id = ?1",
                    [mutation.original_claim_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode::<Claim>)
                .transpose()?;
            if original.as_ref().is_none_or(|claim| {
                claim.state != ClaimState::Blocked
                    || claim.metadata.updated_at != mutation.expected_original_updated_at
            }) {
                return Ok(None);
            }
            let existing_worker = transaction
                .query_row(
                    "SELECT data FROM workers WHERE id = ?1",
                    [mutation.worker.id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode::<Worker>)
                .transpose()?;
            match (
                existing_worker.as_ref(),
                mutation.expected_worker_updated_at,
            ) {
                (Some(worker), Some(expected)) if worker.metadata.updated_at == expected => {}
                (None, None) => {}
                _ => return Ok(None),
            }
            if let Some(update) = &mutation.work_package {
                let package = transaction
                    .query_row(
                        "SELECT data FROM work_packages WHERE id = ?1",
                        [update.record.id.as_str()],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .map(decode::<WorkPackage>)
                    .transpose()?;
                if package
                    .as_ref()
                    .is_none_or(|package| package.metadata.updated_at != update.expected_updated_at)
                {
                    return Ok(None);
                }
            }
            insert_claim_transaction(transaction, &mutation.replacement_claim)?;
            upsert_worker_transaction(transaction, &mutation.worker)?;
            if let Some(update) = &mutation.work_package {
                transaction.execute(
                    r#"UPDATE work_packages
                       SET state = ?1, priority = ?2, ready_at = ?3, updated_at = ?4, data = ?5
                       WHERE id = ?6 AND goal_id = ?7"#,
                    params![
                        enum_key(&update.record.state)?,
                        update.record.priority,
                        update.record.ready_at.map(updated_at),
                        updated_at(update.record.metadata.updated_at),
                        encode(&update.record)?,
                        update.record.id.as_str(),
                        update.record.goal_id,
                    ],
                )?;
            }
            let changed = transaction.execute(
                r#"UPDATE escalations
                   SET state = ?1, severity = ?2, updated_at = ?3, data = ?4
                   WHERE id = ?5 AND goal_id = ?6 AND updated_at = ?7"#,
                params![
                    enum_key(&mutation.escalation.record.state)?,
                    enum_key(&mutation.escalation.record.severity)?,
                    updated_at(mutation.escalation.record.metadata.updated_at),
                    encode(&mutation.escalation.record)?,
                    mutation.escalation.record.id.as_str(),
                    mutation.escalation.record.goal_id,
                    updated_at(mutation.escalation.expected_updated_at),
                ],
            )?;
            if changed != 1 {
                return Ok(None);
            }
            append_event_transaction(transaction, event, Some(publication_key)).map(Some)
        });
        map_claim_constraint(result, &mutation.replacement_claim.goal_id, &scope_key)
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>, StoreError> {
        self.connection.lock().map_err(|_| StoreError::LockPoisoned)
    }

    pub fn integrity_check(&self) -> Result<(), StoreError> {
        let result: String = self
            .lock()?
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if result == "ok" {
            Ok(())
        } else {
            Err(StoreError::Integrity(result))
        }
    }

    pub fn backup_to(&self, destination: impl AsRef<Path>) -> Result<(), StoreError> {
        let destination = destination.as_ref();
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let source = self.lock()?;
        let mut target = Connection::open(destination)?;
        let backup = Backup::new(&source, &mut target)?;
        backup.run_to_completion(32, std::time::Duration::from_millis(5), None)?;
        Ok(())
    }

    pub fn restore_from(&self, source: impl AsRef<Path>) -> Result<(), StoreError> {
        let source = Connection::open(source)?;
        let mut destination = self.lock()?;
        let backup = Backup::new(&source, &mut destination)?;
        backup.run_to_completion(32, std::time::Duration::from_millis(5), None)?;
        drop(backup);
        drop(destination);
        self.integrity_check()
    }

    pub fn acquire_claim_for_worker(
        &self,
        claim: &Claim,
        worker: &Worker,
    ) -> Result<(), StoreError> {
        let scope_key = claim_scope_key(&claim.scope);
        let result = self.with_immediate_transaction(|transaction| {
            insert_claim_transaction(transaction, claim)?;
            upsert_worker_transaction(transaction, worker)?;
            Ok(())
        });
        map_claim_constraint(result, &claim.goal_id, &scope_key)
    }

    pub fn update_worker_with_event_if_revision(
        &self,
        worker: &Worker,
        expected_updated_at: DateTime<Utc>,
        event: &CoordinationEvent,
        publication_key: &str,
    ) -> Result<Option<u64>, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let changed = transaction.execute(
                r#"UPDATE workers
                   SET goal_id = ?1, state = ?2, updated_at = ?3, data = ?4
                   WHERE id = ?5 AND updated_at = ?6"#,
                params![
                    worker.goal_id,
                    enum_key(&worker.state)?,
                    updated_at(worker.metadata.updated_at),
                    encode(worker)?,
                    worker.id.as_str(),
                    updated_at(expected_updated_at),
                ],
            )?;
            if changed != 1 {
                return Ok(None);
            }
            append_event_transaction(transaction, event, Some(publication_key)).map(Some)
        })
    }

    pub fn acquire_claim_for_worker_with_event(
        &self,
        claim: &Claim,
        worker: &Worker,
        event: &CoordinationEvent,
        publication_key: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<u64, StoreError> {
        let scope_key = claim_scope_key(&claim.scope);
        let result = self.with_immediate_transaction(|transaction| {
            insert_claim_transaction(transaction, claim)?;
            upsert_worker_transaction(transaction, worker)?;
            let sequence = append_event_transaction(transaction, event, Some(publication_key))?;
            record_idempotent_outcome_transaction(
                transaction,
                idempotency_key,
                operation,
                &serde_json::json!({"claimId": claim.id.as_str()}),
                claim.metadata.updated_at,
            )?;
            Ok(sequence)
        });
        map_claim_constraint(result, &claim.goal_id, &scope_key)
    }

    pub fn expand_claim_for_worker(
        &self,
        parent: &Claim,
        expansion: &Claim,
        worker: &Worker,
        expected_parent_generation: u64,
    ) -> Result<bool, StoreError> {
        let scope_key = claim_scope_key(&expansion.scope);
        let result = self.with_immediate_transaction(|transaction| {
            let updated = transaction.execute(
                r#"UPDATE claims SET updated_at = ?1, data = ?2
                   WHERE id = ?3 AND state = 'active' AND lease_generation = ?4"#,
                params![
                    updated_at(parent.metadata.updated_at),
                    encode(parent)?,
                    parent.id.as_str(),
                    expected_parent_generation,
                ],
            )?;
            if updated != 1 {
                return Ok(false);
            }
            insert_claim_transaction(transaction, expansion)?;
            upsert_worker_transaction(transaction, worker)?;
            Ok(true)
        });
        map_claim_constraint(result, &expansion.goal_id, &scope_key)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn expand_claim_for_worker_with_event(
        &self,
        parent: &Claim,
        expansion: &Claim,
        worker: &Worker,
        expected_parent_generation: u64,
        event: &CoordinationEvent,
        publication_key: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<Option<u64>, StoreError> {
        let scope_key = claim_scope_key(&expansion.scope);
        let result = self.with_immediate_transaction(|transaction| {
            let updated = transaction.execute(
                r#"UPDATE claims SET updated_at = ?1, data = ?2
                   WHERE id = ?3 AND state = 'active' AND lease_generation = ?4"#,
                params![
                    updated_at(parent.metadata.updated_at),
                    encode(parent)?,
                    parent.id.as_str(),
                    expected_parent_generation,
                ],
            )?;
            if updated != 1 {
                return Ok(None);
            }
            insert_claim_transaction(transaction, expansion)?;
            upsert_worker_transaction(transaction, worker)?;
            let sequence = append_event_transaction(transaction, event, Some(publication_key))?;
            record_idempotent_outcome_transaction(
                transaction,
                idempotency_key,
                operation,
                &serde_json::json!({"claimId": expansion.id.as_str()}),
                expansion.metadata.updated_at,
            )?;
            Ok(Some(sequence))
        });
        map_claim_constraint(result, &expansion.goal_id, &scope_key)
    }

    pub fn compare_and_swap_active_claim(
        &self,
        claim: &Claim,
        expected_generation: u64,
    ) -> Result<bool, StoreError> {
        let changed = self.lock()?.execute(
            r#"UPDATE claims SET lease_expires_at = ?1, updated_at = ?2, data = ?3
               WHERE id = ?4 AND state = 'active' AND lease_generation = ?5"#,
            params![
                updated_at(claim.lease_expires_at),
                updated_at(claim.metadata.updated_at),
                encode(claim)?,
                claim.id.as_str(),
                expected_generation,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn update_active_claim_contracts(
        &self,
        claim: &Claim,
        expected_generation: u64,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let changed = self.lock()?.execute(
            r#"UPDATE claims SET updated_at = ?1, data = ?2
               WHERE id = ?3 AND state = 'active' AND lease_generation = ?4
                 AND owner_id = ?5 AND updated_at = ?6"#,
            params![
                updated_at(claim.metadata.updated_at),
                encode(claim)?,
                claim.id.as_str(),
                expected_generation,
                claim.owner.as_str(),
                updated_at(expected_updated_at),
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn update_work_package_contracts(
        &self,
        package: &WorkPackage,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let changed = self.lock()?.execute(
            r#"UPDATE work_packages SET updated_at = ?1, data = ?2
               WHERE id = ?3 AND updated_at = ?4"#,
            params![
                updated_at(package.metadata.updated_at),
                encode(package)?,
                package.id.as_str(),
                updated_at(expected_updated_at),
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn finalize_claim_for_worker(
        &self,
        claim: &Claim,
        worker: &Worker,
        expected_generation: u64,
    ) -> Result<bool, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let changed = transaction.execute(
                r#"UPDATE claims SET state = ?1, updated_at = ?2, data = ?3
                   WHERE id = ?4 AND state = 'active' AND lease_generation = ?5 AND owner_id = ?6"#,
                params![
                    enum_key(&claim.state)?,
                    updated_at(claim.metadata.updated_at),
                    encode(claim)?,
                    claim.id.as_str(),
                    expected_generation,
                    claim.owner.as_str(),
                ],
            )?;
            if changed != 1 {
                return Ok(false);
            }
            upsert_worker_transaction(transaction, worker)?;
            Ok(true)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finalize_claim_for_worker_with_event(
        &self,
        claim: &Claim,
        worker: &Worker,
        expected_generation: u64,
        event: &CoordinationEvent,
        publication_key: &str,
        operation: &str,
        idempotency_key: &str,
    ) -> Result<Option<u64>, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let changed = transaction.execute(
                r#"UPDATE claims SET state = ?1, updated_at = ?2, data = ?3
                   WHERE id = ?4 AND state = 'active' AND lease_generation = ?5 AND owner_id = ?6"#,
                params![
                    enum_key(&claim.state)?,
                    updated_at(claim.metadata.updated_at),
                    encode(claim)?,
                    claim.id.as_str(),
                    expected_generation,
                    claim.owner.as_str(),
                ],
            )?;
            if changed != 1 {
                return Ok(None);
            }
            upsert_worker_transaction(transaction, worker)?;
            let sequence = append_event_transaction(transaction, event, Some(publication_key))?;
            record_idempotent_outcome_transaction(
                transaction,
                idempotency_key,
                operation,
                &serde_json::json!({"claimId": claim.id.as_str()}),
                claim.metadata.updated_at,
            )?;
            Ok(Some(sequence))
        })
    }

    pub fn revise_contract_with_event(
        &self,
        contract: &SharedContract,
        expected_revision: u64,
        event: &CoordinationEvent,
        publication_key: &str,
    ) -> Result<Option<u64>, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let changed = transaction.execute(
                r#"UPDATE contracts SET revision = ?1, updated_at = ?2, data = ?3
                   WHERE id = ?4 AND revision = ?5"#,
                params![
                    contract.revision,
                    updated_at(contract.metadata.updated_at),
                    encode(contract)?,
                    contract.id.as_str(),
                    expected_revision,
                ],
            )?;
            if changed != 1 {
                return Ok(None);
            }
            Ok(Some(append_event_transaction(
                transaction,
                event,
                Some(publication_key),
            )?))
        })
    }

    pub fn register_contract_with_event(
        &self,
        contract: &SharedContract,
        event: &CoordinationEvent,
        publication_key: &str,
    ) -> Result<u64, StoreError> {
        let result = self.with_immediate_transaction(|transaction| {
            transaction.execute(
                r#"INSERT INTO contracts(id, goal_id, stable_key, revision, updated_at, data)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                params![
                    contract.id.as_str(),
                    contract.goal_id,
                    contract.stable_key,
                    contract.revision,
                    updated_at(contract.metadata.updated_at),
                    encode(contract)?
                ],
            )?;
            append_event_transaction(transaction, event, Some(publication_key))
        });
        match result {
            Err(StoreError::Database(error)) if is_constraint_violation(&error) => {
                Err(StoreError::ContractAlreadyExists {
                    goal_id: contract.goal_id.clone(),
                    stable_key: contract.stable_key.clone(),
                })
            }
            other => other,
        }
    }

    pub fn append_event_once(
        &self,
        publication_key: &str,
        event: &CoordinationEvent,
    ) -> Result<u64, StoreError> {
        let sequence = self.with_immediate_transaction(|transaction| {
            append_event_transaction(transaction, event, Some(publication_key))
        })?;
        CorrelationIds::from_event(event).emit_info(
            "coordination.event.persisted",
            &format!("sequence:{sequence}"),
        );
        Ok(sequence)
    }

    pub fn merge_signal_observation(
        &self,
        observation: &CoordinationSignal,
        now: DateTime<Utc>,
    ) -> Result<SignalMergeResult, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let existing: Option<CoordinationSignal> = transaction
                .query_row(
                    "SELECT data FROM coordination_signals WHERE goal_id = ?1 AND correlation_key = ?2 AND state = 'active'",
                    params![observation.goal_id, observation.correlation_key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?;
            if let Some(mut signal) = existing {
                let changed = signal.merge_observation(observation, now)?;
                if changed {
                    upsert_signal_transaction(transaction, &signal)?;
                }
                Ok(SignalMergeResult {
                    signal,
                    created: false,
                    changed,
                })
            } else {
                upsert_signal_transaction(transaction, observation)?;
                Ok(SignalMergeResult {
                    signal: observation.clone(),
                    created: true,
                    changed: true,
                })
            }
        })
    }

    pub fn expire_signals(
        &self,
        goal_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Vec<CoordinationSignal>, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let encoded = {
                let mut statement = transaction.prepare(
                    "SELECT data FROM coordination_signals WHERE goal_id = ?1 AND state = 'active' AND expires_at <= ?2 ORDER BY expires_at, id",
                )?;
                statement
                    .query_map(params![goal_id, updated_at(now)], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut expired = Vec::new();
            for value in encoded {
                let mut signal: CoordinationSignal = decode(value)?;
                if signal.expire(now) {
                    upsert_signal_transaction(transaction, &signal)?;
                    expired.push(signal);
                }
            }
            Ok(expired)
        })
    }

    pub fn expire_signals_for_claims(
        &self,
        goal_id: &str,
        claim_ids: &[ClaimId],
        now: DateTime<Utc>,
    ) -> Result<Vec<CoordinationSignal>, StoreError> {
        if claim_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.with_immediate_transaction(|transaction| {
            let encoded = {
                let mut statement = transaction.prepare(
                    "SELECT data FROM coordination_signals WHERE goal_id = ?1 AND state = 'active' ORDER BY updated_at, id",
                )?;
                statement
                    .query_map([goal_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            let mut expired = Vec::new();
            for value in encoded {
                let mut signal: CoordinationSignal = decode(value)?;
                if signal
                    .affected_claim_ids
                    .iter()
                    .any(|candidate| claim_ids.contains(candidate))
                    && signal.expire_obsolete(now)
                {
                    upsert_signal_transaction(transaction, &signal)?;
                    expired.push(signal);
                }
            }
            Ok(expired)
        })
    }

    pub fn record_claim_overlap(
        &self,
        left: &Claim,
        right: &Claim,
        expected_left_generation: u64,
        expected_right_generation: u64,
    ) -> Result<bool, StoreError> {
        self.with_immediate_transaction(|transaction| {
            let left_changed = transaction.execute(
                "UPDATE claims SET updated_at = ?1, data = ?2 WHERE id = ?3 AND state = 'active' AND lease_generation = ?4",
                params![
                    updated_at(left.metadata.updated_at),
                    encode(left)?,
                    left.id.as_str(),
                    expected_left_generation,
                ],
            )?;
            let right_changed = transaction.execute(
                "UPDATE claims SET updated_at = ?1, data = ?2 WHERE id = ?3 AND state = 'active' AND lease_generation = ?4",
                params![
                    updated_at(right.metadata.updated_at),
                    encode(right)?,
                    right.id.as_str(),
                    expected_right_generation,
                ],
            )?;
            if left_changed != 1 || right_changed != 1 {
                return Err(StoreError::NotFound("stale overlap claim".into()));
            }
            Ok(true)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn reassign_claim(
        &self,
        prior: &Claim,
        replacement: &Claim,
        prior_worker: &Worker,
        replacement_worker: &Worker,
        expected_generation: u64,
    ) -> Result<bool, StoreError> {
        let scope_key = claim_scope_key(&replacement.scope);
        let result = self.with_immediate_transaction(|transaction| {
            let changed = transaction.execute(
                r#"UPDATE claims SET state = ?1, updated_at = ?2, data = ?3
                   WHERE id = ?4 AND state = 'active' AND lease_generation = ?5"#,
                params![
                    enum_key(&prior.state)?,
                    updated_at(prior.metadata.updated_at),
                    encode(prior)?,
                    prior.id.as_str(),
                    expected_generation,
                ],
            )?;
            if changed != 1 {
                return Ok(false);
            }
            insert_claim_transaction(transaction, replacement)?;
            upsert_worker_transaction(transaction, prior_worker)?;
            upsert_worker_transaction(transaction, replacement_worker)?;
            Ok(true)
        });
        map_claim_constraint(result, &replacement.goal_id, &scope_key)
    }

    pub fn replace_notifications_atomically(
        &self,
        replacement: &WorkerNotification,
        superseded: &[ExpectedRecordUpdate<WorkerNotification>],
    ) -> Result<(), StoreError> {
        replacement.validate()?;
        for update in superseded {
            update.record.validate()?;
            if update.record.state != NotificationState::Superseded
                || update.record.superseded_by.as_ref() != Some(&replacement.id)
            {
                return Err(StoreError::Integrity(
                    "coalesced source must be superseded by its replacement".into(),
                ));
            }
        }
        self.with_immediate_transaction(|transaction| {
            for update in superseded {
                let changed = transaction.execute(
                    r#"UPDATE notifications
                       SET state = ?1, updated_at = ?2, data = ?3
                       WHERE id = ?4 AND goal_id = ?5 AND worker_id = ?6 AND updated_at = ?7"#,
                    params![
                        enum_key(&update.record.state)?,
                        updated_at(update.record.metadata.updated_at),
                        encode(&update.record)?,
                        update.record.id.as_str(),
                        update.record.goal_id,
                        update.record.target_worker.as_str(),
                        updated_at(update.expected_updated_at),
                    ],
                )?;
                if changed != 1 {
                    return Err(StoreError::StaleNotificationRevision(
                        update.record.id.as_str().into(),
                    ));
                }
            }
            transaction.execute(
                r#"INSERT INTO notifications(id, goal_id, worker_id, state, updated_at, data)
                   VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                params![
                    replacement.id.as_str(),
                    replacement.goal_id,
                    replacement.target_worker.as_str(),
                    enum_key(&replacement.state)?,
                    updated_at(replacement.metadata.updated_at),
                    encode(replacement)?,
                ],
            )?;
            Ok(())
        })
    }
}

fn insert_claim_transaction(
    transaction: &Transaction<'_>,
    claim: &Claim,
) -> Result<(), StoreError> {
    transaction.execute(
        r#"INSERT INTO claims(
             id, goal_id, scope_key, owner_id, state, lease_generation,
             lease_expires_at, updated_at, data
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
        params![
            claim.id.as_str(),
            claim.goal_id,
            claim_scope_key(&claim.scope),
            claim.owner.as_str(),
            enum_key(&claim.state)?,
            claim.lease_generation,
            updated_at(claim.lease_expires_at),
            updated_at(claim.metadata.updated_at),
            encode(claim)?
        ],
    )?;
    Ok(())
}

fn upsert_worker_transaction(
    transaction: &Transaction<'_>,
    worker: &Worker,
) -> Result<(), StoreError> {
    transaction.execute(
        r#"INSERT INTO workers(id, goal_id, state, updated_at, data)
           VALUES (?1, ?2, ?3, ?4, ?5)
           ON CONFLICT(id) DO UPDATE SET goal_id=excluded.goal_id,
             state=excluded.state, updated_at=excluded.updated_at, data=excluded.data"#,
        params![
            worker.id.as_str(),
            worker.goal_id,
            enum_key(&worker.state)?,
            updated_at(worker.metadata.updated_at),
            encode(worker)?,
        ],
    )?;
    Ok(())
}

fn upsert_signal_transaction(
    transaction: &Transaction<'_>,
    signal: &CoordinationSignal,
) -> Result<(), StoreError> {
    transaction.execute(
        r#"INSERT INTO coordination_signals(
             id, goal_id, correlation_key, state, severity, confidence_percent,
             expires_at, updated_at, data
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
           ON CONFLICT(id) DO UPDATE SET
             state=excluded.state, severity=excluded.severity,
             confidence_percent=excluded.confidence_percent,
             expires_at=excluded.expires_at, updated_at=excluded.updated_at,
             data=excluded.data"#,
        params![
            signal.id.as_str(),
            signal.goal_id,
            signal.correlation_key,
            enum_key(&signal.state)?,
            enum_key(&signal.severity)?,
            signal.confidence_percent,
            updated_at(signal.expires_at),
            updated_at(signal.metadata.updated_at),
            encode(signal)?,
        ],
    )?;
    Ok(())
}

fn append_event_transaction(
    transaction: &Transaction<'_>,
    event: &CoordinationEvent,
    publication_key: Option<&str>,
) -> Result<u64, StoreError> {
    if let Some(publication_key) = publication_key {
        if let Some(sequence) = transaction
            .query_row(
                "SELECT sequence FROM event_publications WHERE publication_key = ?1",
                [publication_key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
        {
            return Ok(sequence as u64);
        }
    }
    transaction.execute(
        r#"INSERT INTO coordination_events(
             id, goal_id, kind, correlation_id, occurred_at, data
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
        params![
            event.id.as_str(),
            event.goal_id,
            enum_key(&event.kind)?,
            event.correlation_id,
            updated_at(event.occurred_at),
            encode(event)?
        ],
    )?;
    let sequence = transaction.last_insert_rowid() as u64;
    let mut stored = event.clone();
    stored.sequence = Some(sequence);
    transaction.execute(
        "UPDATE coordination_events SET data = ?1 WHERE sequence = ?2",
        params![encode(&stored)?, sequence],
    )?;
    if let Some(publication_key) = publication_key {
        transaction.execute(
            "INSERT INTO event_publications(publication_key, event_id, sequence, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                publication_key,
                event.id.as_str(),
                sequence,
                updated_at(event.occurred_at)
            ],
        )?;
    }
    Ok(sequence)
}

fn record_idempotent_outcome_transaction(
    transaction: &Transaction<'_>,
    key: &str,
    operation: &str,
    outcome: &serde_json::Value,
    now: DateTime<Utc>,
) -> Result<bool, StoreError> {
    Ok(transaction.execute(
        "INSERT OR IGNORE INTO idempotency_records(key, operation, outcome, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![key, operation, encode(outcome)?, updated_at(now)],
    )? == 1)
}

fn map_claim_constraint<T>(
    result: Result<T, StoreError>,
    goal_id: &str,
    scope_key: &str,
) -> Result<T, StoreError> {
    match result {
        Err(StoreError::Database(error)) if is_constraint_violation(&error) => {
            Err(StoreError::ActiveClaimExists {
                goal_id: goal_id.to_string(),
                scope_key: scope_key.to_string(),
            })
        }
        other => other,
    }
}

fn insert_integration_job_transaction(
    transaction: &Transaction<'_>,
    job: &IntegrationJob,
) -> Result<(), StoreError> {
    transaction.execute(
        r#"INSERT INTO integration_jobs(
             id, artifact_id, goal_id, repository_id, priority, state,
             enqueued_at, updated_at, data
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
        params![
            job.id.as_str(),
            job.artifact_id.as_str(),
            job.goal_id,
            job.repository_id,
            job.priority,
            enum_key(&job.state)?,
            updated_at(job.enqueued_at),
            updated_at(job.metadata.updated_at),
            encode(job)?,
        ],
    )?;
    Ok(())
}

fn update_integration_job_transaction(
    transaction: &Transaction<'_>,
    job: &IntegrationJob,
    expected_state: IntegrationJobState,
) -> Result<(), StoreError> {
    let updated = transaction.execute(
        r#"UPDATE integration_jobs
           SET state = ?1, updated_at = ?2, data = ?3
           WHERE id = ?4 AND state = ?5"#,
        params![
            enum_key(&job.state)?,
            updated_at(job.metadata.updated_at),
            encode(job)?,
            job.id.as_str(),
            enum_key(&expected_state)?,
        ],
    )?;
    if updated != 1 {
        return Err(StoreError::NotFound(format!(
            "stale integration job {}",
            job.id.as_str()
        )));
    }
    Ok(())
}

fn apply_migrations(connection: &Connection) -> Result<(), StoreError> {
    let current: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if current > STORE_SCHEMA_VERSION {
        return Err(StoreError::UnsupportedSchemaVersion(current));
    }
    if current == STORE_SCHEMA_VERSION {
        return Ok(());
    }
    connection.execute_batch(
        r#"
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS coordination_schema (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS workers (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS workers_goal_state
            ON workers(goal_id, state, updated_at);

        CREATE TABLE IF NOT EXISTS goal_pools (
            goal_id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            desired_concurrency INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS goal_pools_mode
            ON goal_pools(mode, updated_at, goal_id);

        CREATE TABLE IF NOT EXISTS work_packages (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            state TEXT NOT NULL,
            priority INTEGER NOT NULL,
            ready_at TEXT,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS work_packages_ready
            ON work_packages(goal_id, state, priority DESC, ready_at, id);

        CREATE TABLE IF NOT EXISTS claims (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            scope_key TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            state TEXT NOT NULL,
            lease_generation INTEGER NOT NULL,
            lease_expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS claims_one_active_owner
            ON claims(goal_id, scope_key) WHERE state = 'active';
        CREATE INDEX IF NOT EXISTS claims_goal_state_lease
            ON claims(goal_id, state, lease_expires_at);
        CREATE INDEX IF NOT EXISTS claims_owner_state
            ON claims(owner_id, state, updated_at);

        CREATE TABLE IF NOT EXISTS contracts (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            stable_key TEXT NOT NULL,
            revision INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL,
            UNIQUE(goal_id, stable_key)
        );
        CREATE INDEX IF NOT EXISTS contracts_goal_revision
            ON contracts(goal_id, revision, stable_key);

        CREATE TABLE IF NOT EXISTS coordination_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE,
            goal_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            correlation_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS events_goal_sequence
            ON coordination_events(goal_id, sequence);
        CREATE INDEX IF NOT EXISTS events_goal_correlation
            ON coordination_events(goal_id, correlation_id, sequence);

        CREATE TABLE IF NOT EXISTS event_publications (
            publication_key TEXT PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            sequence INTEGER NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            FOREIGN KEY(sequence) REFERENCES coordination_events(sequence) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS event_publications_created_at
            ON event_publications(created_at, publication_key);

        CREATE TABLE IF NOT EXISTS event_replay_cursors (
            consumer_id TEXT NOT NULL,
            goal_id TEXT NOT NULL,
            acknowledged_sequence INTEGER NOT NULL,
            last_delivered_sequence INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL,
            PRIMARY KEY(consumer_id, goal_id)
        );
        CREATE INDEX IF NOT EXISTS event_replay_cursors_goal
            ON event_replay_cursors(goal_id, updated_at, consumer_id);

        CREATE TABLE IF NOT EXISTS coordination_event_archive (
            original_sequence INTEGER PRIMARY KEY,
            event_id TEXT NOT NULL UNIQUE,
            goal_id TEXT NOT NULL,
            archived_at TEXT NOT NULL,
            artifact_refs TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS event_archive_goal_sequence
            ON coordination_event_archive(goal_id, original_sequence);

        CREATE TABLE IF NOT EXISTS coordination_signals (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            correlation_key TEXT NOT NULL,
            state TEXT NOT NULL,
            severity TEXT NOT NULL,
            confidence_percent INTEGER NOT NULL,
            expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS signals_one_active_correlation
            ON coordination_signals(goal_id, correlation_key) WHERE state = 'active';
        CREATE INDEX IF NOT EXISTS signals_goal_state_score
            ON coordination_signals(goal_id, state, severity, confidence_percent, updated_at);
        CREATE INDEX IF NOT EXISTS signals_active_expiry
            ON coordination_signals(state, expires_at, goal_id);

        CREATE TABLE IF NOT EXISTS interventions (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS interventions_goal_state
            ON interventions(goal_id, state, updated_at);

        CREATE TABLE IF NOT EXISTS supervisor_budgets (
            goal_id TEXT PRIMARY KEY,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS supervisor_budgets_updated
            ON supervisor_budgets(updated_at, goal_id);

        CREATE TABLE IF NOT EXISTS notifications (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS notifications_worker_state
            ON notifications(worker_id, state, updated_at);

        CREATE TABLE IF NOT EXISTS coordination_exchanges (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            request_event_id TEXT NOT NULL,
            requester_worker_id TEXT NOT NULL,
            target_worker_id TEXT NOT NULL,
            state TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS exchanges_one_open_request_target
            ON coordination_exchanges(request_event_id, target_worker_id) WHERE state = 'open';
        CREATE INDEX IF NOT EXISTS exchanges_goal_state_expiry
            ON coordination_exchanges(goal_id, state, expires_at, updated_at);
        CREATE INDEX IF NOT EXISTS exchanges_participants
            ON coordination_exchanges(requester_worker_id, target_worker_id, state, updated_at);

        CREATE TABLE IF NOT EXISTS integration_artifacts (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            claim_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            repository_id TEXT NOT NULL,
            base_revision TEXT NOT NULL,
            head_revision TEXT NOT NULL,
            created_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS integration_artifacts_goal_created
            ON integration_artifacts(goal_id, created_at, id);
        CREATE INDEX IF NOT EXISTS integration_artifacts_claim_created
            ON integration_artifacts(claim_id, created_at, id);
        CREATE INDEX IF NOT EXISTS integration_artifacts_repository_created
            ON integration_artifacts(repository_id, created_at, id);

        CREATE TABLE IF NOT EXISTS integration_validation_reports (
            id TEXT PRIMARY KEY,
            artifact_id TEXT NOT NULL,
            goal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            passed INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS validation_reports_artifact_created
            ON integration_validation_reports(artifact_id, created_at, id);
        CREATE INDEX IF NOT EXISTS validation_reports_goal_created
            ON integration_validation_reports(goal_id, created_at, id);

        CREATE TABLE IF NOT EXISTS integration_reconciliations (
            id TEXT PRIMARY KEY,
            artifact_id TEXT NOT NULL,
            goal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS reconciliations_artifact_created
            ON integration_reconciliations(artifact_id, created_at, id);
        CREATE INDEX IF NOT EXISTS reconciliations_goal_state_created
            ON integration_reconciliations(goal_id, state, created_at, id);

        CREATE TABLE IF NOT EXISTS integration_jobs (
            id TEXT PRIMARY KEY,
            artifact_id TEXT NOT NULL,
            goal_id TEXT NOT NULL,
            repository_id TEXT NOT NULL,
            priority INTEGER NOT NULL,
            state TEXT NOT NULL,
            enqueued_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS integration_jobs_one_active_artifact
            ON integration_jobs(artifact_id) WHERE state IN ('queued', 'running');
        CREATE UNIQUE INDEX IF NOT EXISTS integration_jobs_one_running_repository
            ON integration_jobs(repository_id) WHERE state = 'running';
        CREATE INDEX IF NOT EXISTS integration_jobs_repository_queue
            ON integration_jobs(repository_id, state, priority DESC, enqueued_at, id);

        CREATE TABLE IF NOT EXISTS integration_finalizations (
            id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            artifact_id TEXT NOT NULL,
            goal_id TEXT NOT NULL,
            state TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS integration_finalizations_state_updated
            ON integration_finalizations(state, updated_at, id);

        CREATE TABLE IF NOT EXISTS integration_maintenance_records (
            id TEXT PRIMARY KEY,
            finalization_id TEXT NOT NULL,
            artifact_id TEXT NOT NULL,
            goal_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS integration_maintenance_artifact_created
            ON integration_maintenance_records(artifact_id, created_at, id);
        CREATE INDEX IF NOT EXISTS integration_maintenance_finalization_kind
            ON integration_maintenance_records(finalization_id, kind, created_at, id);
        CREATE UNIQUE INDEX IF NOT EXISTS integration_maintenance_one_kind
            ON integration_maintenance_records(finalization_id, kind);

        CREATE TABLE IF NOT EXISTS escalations (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            state TEXT NOT NULL,
            severity TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS escalations_goal_state
            ON escalations(goal_id, state, severity, updated_at);

        CREATE TABLE IF NOT EXISTS idempotency_records (
            key TEXT PRIMARY KEY,
            operation TEXT NOT NULL,
            outcome TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idempotency_created_at
            ON idempotency_records(created_at);

        CREATE TABLE IF NOT EXISTS context_transfers (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            claim_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS context_transfers_worker_created
            ON context_transfers(worker_id, created_at, id);
        CREATE INDEX IF NOT EXISTS context_transfers_claim_created
            ON context_transfers(claim_id, created_at, id);

        CREATE TABLE IF NOT EXISTS rollout_controls (
            goal_id TEXT PRIMARY KEY,
            phase TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS rollout_controls_phase_updated
            ON rollout_controls(phase, updated_at, goal_id);

        CREATE TABLE IF NOT EXISTS rollout_shadow_observations (
            id TEXT PRIMARY KEY,
            goal_id TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            data TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS rollout_shadow_goal_observed
            ON rollout_shadow_observations(goal_id, observed_at, id);

        INSERT OR IGNORE INTO coordination_schema(version, applied_at)
            VALUES (16, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
        COMMIT;
        "#,
    )?;
    connection.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
    let version: i64 =
        connection.query_row("SELECT MAX(version) FROM coordination_schema", [], |row| {
            row.get(0)
        })?;
    if version != STORE_SCHEMA_VERSION {
        return Err(StoreError::Database(rusqlite::Error::InvalidQuery));
    }
    Ok(())
}

fn encode<T: Serialize>(value: &T) -> Result<String, StoreError> {
    let mut value = serde_json::to_value(value)?;
    redact_sensitive_value(&mut value);
    Ok(serde_json::to_string(&value)?)
}

fn decode<T: DeserializeOwned>(value: String) -> Result<T, StoreError> {
    Ok(serde_json::from_str(&value)?)
}

fn enum_key<T: Serialize>(value: &T) -> Result<String, StoreError> {
    match serde_json::to_value(value)? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(StoreError::Serialization(serde_json::Error::io(
            std::io::Error::new(std::io::ErrorKind::InvalidData, "expected string enum"),
        ))),
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

fn updated_at(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

fn query_records<T: DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    parameters: impl rusqlite::Params,
) -> Result<Vec<T>, StoreError> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(parameters, |row| row.get::<_, String>(0))?;
    let mut records = Vec::new();
    for row in rows {
        records.push(decode(row?)?);
    }
    Ok(records)
}

impl WorkerRepository for SqliteCoordinationStore {
    fn upsert_worker(&self, worker: &Worker) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO workers(id, goal_id, state, updated_at, data)
               VALUES (?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(id) DO UPDATE SET
                 goal_id=excluded.goal_id, state=excluded.state,
                 updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                worker.id.as_str(),
                worker.goal_id,
                enum_key(&worker.state)?,
                updated_at(worker.metadata.updated_at),
                encode(worker)?
            ],
        )?;
        let mut ids = CorrelationIds::for_goal(&worker.goal_id).with_worker(worker.id.as_str());
        if let Some(claim_id) = worker.active_claims.first() {
            ids = ids.with_claim(claim_id.as_str());
        }
        if let Some(run_id) = &worker.current_run_id {
            ids = ids.with_run(run_id);
        }
        if let Some(thread_id) = &worker.current_thread_id {
            ids = ids.with_thread(thread_id);
        }
        if let Some(workspace) = &worker.workspace {
            ids = ids.with_workspace(&workspace.worktree_path);
        }
        ids.emit_info(
            "coordination.worker.persisted",
            &format!("{:?}", worker.state).to_ascii_lowercase(),
        );
        Ok(())
    }

    fn worker(&self, id: &WorkerId) -> Result<Option<Worker>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM workers WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn workers_for_goal(
        &self,
        goal_id: &str,
        state: Option<WorkerState>,
    ) -> Result<Vec<Worker>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM workers WHERE goal_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM workers WHERE goal_id = ?1 ORDER BY updated_at, id",
                [goal_id],
            ),
        }
    }
}

impl PoolRepository for SqliteCoordinationStore {
    fn upsert_goal_pool(
        &self,
        state: &GoalPoolState,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO goal_pools(goal_id, mode, desired_concurrency, updated_at, data)
               VALUES (?1, ?2, ?3, ?4, ?5)
               ON CONFLICT(goal_id) DO UPDATE SET
                 mode=excluded.mode, desired_concurrency=excluded.desired_concurrency,
                 updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                state.goal_id,
                enum_key(&state.mode)?,
                state.desired_concurrency as i64,
                updated_at(now),
                encode(state)?,
            ],
        )?;
        Ok(())
    }

    fn goal_pool(&self, goal_id: &str) -> Result<Option<GoalPoolState>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM goal_pools WHERE goal_id = ?1",
                [goal_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn goal_pools(&self) -> Result<Vec<GoalPoolState>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM goal_pools ORDER BY updated_at, goal_id",
            [],
        )
    }
}

impl WorkPackageRepository for SqliteCoordinationStore {
    fn upsert_work_package(&self, package: &WorkPackage) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO work_packages(id, goal_id, state, priority, ready_at, updated_at, data)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
               ON CONFLICT(id) DO UPDATE SET
                 goal_id=excluded.goal_id, state=excluded.state, priority=excluded.priority,
                 ready_at=excluded.ready_at, updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                package.id.as_str(),
                package.goal_id,
                enum_key(&package.state)?,
                package.priority,
                package.ready_at.map(updated_at),
                updated_at(package.metadata.updated_at),
                encode(package)?
            ],
        )?;
        Ok(())
    }

    fn work_package(&self, id: &WorkPackageId) -> Result<Option<WorkPackage>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM work_packages WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn work_packages_for_goal(
        &self,
        goal_id: &str,
        state: Option<WorkPackageState>,
    ) -> Result<Vec<WorkPackage>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM work_packages WHERE goal_id = ?1 AND state = ?2 ORDER BY priority DESC, ready_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM work_packages WHERE goal_id = ?1 ORDER BY priority DESC, ready_at, id",
                [goal_id],
            ),
        }
    }
}

impl ClaimRepository for SqliteCoordinationStore {
    fn insert_claim(&self, claim: &Claim) -> Result<(), StoreError> {
        let scope_key = claim_scope_key(&claim.scope);
        let result = self.with_immediate_transaction(|transaction| {
            insert_claim_transaction(transaction, claim)?;
            Ok(())
        });
        let result = map_claim_constraint(result, &claim.goal_id, &scope_key);
        if result.is_ok() {
            CorrelationIds::for_goal(&claim.goal_id)
                .with_worker(claim.owner.as_str())
                .with_claim(claim.id.as_str())
                .emit_info("coordination.claim.persisted", "active");
        }
        result
    }

    fn upsert_claim(&self, claim: &Claim) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO claims(
                 id, goal_id, scope_key, owner_id, state, lease_generation,
                 lease_expires_at, updated_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
               ON CONFLICT(id) DO UPDATE SET
                 goal_id=excluded.goal_id, scope_key=excluded.scope_key,
                 owner_id=excluded.owner_id, state=excluded.state,
                 lease_generation=excluded.lease_generation,
                 lease_expires_at=excluded.lease_expires_at,
                 updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                claim.id.as_str(),
                claim.goal_id,
                claim_scope_key(&claim.scope),
                claim.owner.as_str(),
                enum_key(&claim.state)?,
                claim.lease_generation,
                updated_at(claim.lease_expires_at),
                updated_at(claim.metadata.updated_at),
                encode(claim)?
            ],
        )?;
        CorrelationIds::for_goal(&claim.goal_id)
            .with_worker(claim.owner.as_str())
            .with_claim(claim.id.as_str())
            .emit_info(
                "coordination.claim.persisted",
                &format!("{:?}", claim.state).to_ascii_lowercase(),
            );
        Ok(())
    }

    fn claim(&self, id: &ClaimId) -> Result<Option<Claim>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM claims WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn claims_for_goal(
        &self,
        goal_id: &str,
        state: Option<ClaimState>,
    ) -> Result<Vec<Claim>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM claims WHERE goal_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM claims WHERE goal_id = ?1 ORDER BY updated_at, id",
                [goal_id],
            ),
        }
    }

    fn active_claim_for_scope(
        &self,
        goal_id: &str,
        scope: &ClaimScope,
    ) -> Result<Option<Claim>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM claims WHERE goal_id = ?1 AND scope_key = ?2 AND state = 'active'",
                params![goal_id, claim_scope_key(scope)],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn latest_claim_generation(
        &self,
        goal_id: &str,
        scope: &ClaimScope,
    ) -> Result<u64, StoreError> {
        let generation: Option<i64> = self.lock()?.query_row(
            "SELECT MAX(lease_generation) FROM claims WHERE goal_id = ?1 AND scope_key = ?2",
            params![goal_id, claim_scope_key(scope)],
            |row| row.get(0),
        )?;
        Ok(generation.unwrap_or_default().max(0) as u64)
    }
}

fn is_constraint_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

impl ContractRepository for SqliteCoordinationStore {
    fn upsert_contract(&self, contract: &SharedContract) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO contracts(id, goal_id, stable_key, revision, updated_at, data)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,
                 updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                contract.id.as_str(),
                contract.goal_id,
                contract.stable_key,
                contract.revision,
                updated_at(contract.metadata.updated_at),
                encode(contract)?
            ],
        )?;
        Ok(())
    }

    fn contract(&self, id: &ContractId) -> Result<Option<SharedContract>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM contracts WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn contract_by_stable_key(
        &self,
        goal_id: &str,
        stable_key: &str,
    ) -> Result<Option<SharedContract>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM contracts WHERE goal_id = ?1 AND stable_key = ?2",
                params![goal_id, stable_key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn contracts_for_goal(&self, goal_id: &str) -> Result<Vec<SharedContract>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM contracts WHERE goal_id = ?1 ORDER BY stable_key",
            [goal_id],
        )
    }
}

impl EventRepository for SqliteCoordinationStore {
    fn append_event(&self, event: &CoordinationEvent) -> Result<u64, StoreError> {
        let sequence = self.with_immediate_transaction(|transaction| {
            append_event_transaction(transaction, event, None)
        })?;
        CorrelationIds::from_event(event).emit_info(
            "coordination.event.persisted",
            &format!("sequence:{sequence}"),
        );
        Ok(sequence)
    }

    fn event(&self, id: &EventId) -> Result<Option<CoordinationEvent>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM coordination_events WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn events_for_goal(
        &self,
        goal_id: &str,
        after_sequence: u64,
        correlation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CoordinationEvent>, StoreError> {
        let connection = self.lock()?;
        let limit = limit.clamp(1, 1_000) as i64;
        match correlation_id {
            Some(correlation_id) => query_records(
                &connection,
                "SELECT data FROM coordination_events WHERE goal_id = ?1 AND sequence > ?2 AND correlation_id = ?3 ORDER BY sequence LIMIT ?4",
                params![goal_id, after_sequence, correlation_id, limit],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM coordination_events WHERE goal_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3",
                params![goal_id, after_sequence, limit],
            ),
        }
    }

    fn latest_events_for_goal(
        &self,
        goal_id: &str,
        correlation_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CoordinationEvent>, StoreError> {
        let connection = self.lock()?;
        let limit = limit.clamp(1, 1_000) as i64;
        let mut events = match correlation_id {
            Some(correlation_id) => query_records(
                &connection,
                "SELECT data FROM coordination_events WHERE goal_id = ?1 AND correlation_id = ?2 ORDER BY sequence DESC LIMIT ?3",
                params![goal_id, correlation_id, limit],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM coordination_events WHERE goal_id = ?1 ORDER BY sequence DESC LIMIT ?2",
                params![goal_id, limit],
            ),
        }?;
        events.reverse();
        Ok(events)
    }

    fn replay_events_for_consumer(
        &self,
        consumer_id: &str,
        goal_id: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Result<EventReplayBatch, StoreError> {
        validate_replay_identity(consumer_id, "consumer id")?;
        validate_replay_identity(goal_id, "goal id")?;
        self.with_immediate_transaction(|transaction| {
            let mut cursor = transaction
                .query_row(
                    "SELECT data FROM event_replay_cursors WHERE consumer_id = ?1 AND goal_id = ?2",
                    params![consumer_id, goal_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(decode)
                .transpose()?
                .unwrap_or_else(|| EventReplayCursor {
                    consumer_id: consumer_id.into(),
                    goal_id: goal_id.into(),
                    acknowledged_sequence: 0,
                    last_delivered_sequence: 0,
                    updated_at: now,
                });
            let events: Vec<CoordinationEvent> = query_records(
                transaction,
                "SELECT data FROM coordination_events WHERE goal_id = ?1 AND sequence > ?2 ORDER BY sequence LIMIT ?3",
                params![
                    goal_id,
                    cursor.acknowledged_sequence,
                    limit.clamp(1, 1_000) as i64
                ],
            )?;
            if let Some(sequence) = events.last().and_then(|event| event.sequence) {
                cursor.last_delivered_sequence = cursor.last_delivered_sequence.max(sequence);
            }
            cursor.updated_at = now;
            upsert_replay_cursor_transaction(transaction, &cursor)?;
            Ok(EventReplayBatch { cursor, events })
        })
    }

    fn acknowledge_event_sequence(
        &self,
        consumer_id: &str,
        goal_id: &str,
        sequence: u64,
        now: DateTime<Utc>,
    ) -> Result<EventReplayCursor, StoreError> {
        validate_replay_identity(consumer_id, "consumer id")?;
        validate_replay_identity(goal_id, "goal id")?;
        self.with_immediate_transaction(|transaction| {
            let encoded = transaction
                .query_row(
                    "SELECT data FROM event_replay_cursors WHERE consumer_id = ?1 AND goal_id = ?2",
                    params![consumer_id, goal_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .ok_or_else(|| {
                    StoreError::InvalidReplayCursor(
                        "events must be replayed before acknowledgement".into(),
                    )
                })?;
            let mut cursor: EventReplayCursor = decode(encoded)?;
            if sequence <= cursor.acknowledged_sequence {
                return Ok(cursor);
            }
            if sequence > cursor.last_delivered_sequence {
                return Err(StoreError::InvalidReplayCursor(
                    "cannot acknowledge beyond the last delivered sequence".into(),
                ));
            }
            let belongs_to_goal = transaction
                .query_row(
                    "SELECT 1 FROM coordination_events WHERE goal_id = ?1 AND sequence = ?2",
                    params![goal_id, sequence],
                    |_| Ok(()),
                )
                .optional()?
                .is_some();
            if !belongs_to_goal {
                return Err(StoreError::InvalidReplayCursor(
                    "acknowledged sequence was not delivered for this goal".into(),
                ));
            }
            cursor.acknowledged_sequence = sequence;
            cursor.updated_at = now;
            upsert_replay_cursor_transaction(transaction, &cursor)?;
            Ok(cursor)
        })
    }

    fn event_replay_cursor(
        &self,
        consumer_id: &str,
        goal_id: &str,
    ) -> Result<Option<EventReplayCursor>, StoreError> {
        validate_replay_identity(consumer_id, "consumer id")?;
        validate_replay_identity(goal_id, "goal id")?;
        self.lock()?
            .query_row(
                "SELECT data FROM event_replay_cursors WHERE consumer_id = ?1 AND goal_id = ?2",
                params![consumer_id, goal_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(decode)
            .transpose()
    }

    fn compact_events(
        &self,
        goal_id: &str,
        policy: &EventRetentionPolicy,
        now: DateTime<Utc>,
    ) -> Result<EventCompactionReport, StoreError> {
        validate_replay_identity(goal_id, "goal id")?;
        if policy.hot_retention_seconds == 0
            || policy.retain_latest == 0
            || policy.max_compaction_batch == 0
        {
            return Err(StoreError::InvalidReplayCursor(
                "retention counts must be greater than zero".into(),
            ));
        }
        let older_than = now
            - chrono::Duration::seconds(policy.hot_retention_seconds.min(i64::MAX as u64) as i64);
        self.with_immediate_transaction(|transaction| {
            let maximum: u64 = transaction
                .query_row(
                    "SELECT COALESCE(MAX(sequence), 0) FROM coordination_events WHERE goal_id = ?1",
                    [goal_id],
                    |row| row.get::<_, i64>(0),
                )?
                .max(0) as u64;
            let acknowledged_through = transaction
                .query_row(
                    "SELECT MIN(acknowledged_sequence) FROM event_replay_cursors WHERE goal_id = ?1",
                    [goal_id],
                    |row| row.get::<_, Option<i64>>(0),
                )?
                .map(|value| value.max(0) as u64)
                .unwrap_or(maximum);
            let hot_boundary = transaction
                .query_row(
                    "SELECT sequence FROM coordination_events WHERE goal_id = ?1 ORDER BY sequence DESC LIMIT 1 OFFSET ?2",
                    params![goal_id, policy.retain_latest.saturating_sub(1) as i64],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .map(|value| value.max(0) as u64)
                .unwrap_or(0);
            if hot_boundary == 0 {
                return Ok(EventCompactionReport {
                    goal_id: goal_id.into(),
                    acknowledged_through,
                    compacted_sequences: vec![],
                });
            }
            let candidates: Vec<CoordinationEvent> = query_records(
                transaction,
                r#"SELECT events.data
                   FROM coordination_events events
                   LEFT JOIN coordination_event_archive archive
                     ON archive.original_sequence = events.sequence
                   WHERE events.goal_id = ?1
                     AND events.sequence < ?2
                     AND events.sequence <= ?3
                     AND events.occurred_at < ?4
                     AND archive.original_sequence IS NULL
                   ORDER BY events.sequence
                   LIMIT ?5"#,
                params![
                    goal_id,
                    hot_boundary,
                    acknowledged_through,
                    updated_at(older_than),
                    policy.max_compaction_batch.min(10_000) as i64,
                ],
            )?;
            let mut compacted_sequences = Vec::with_capacity(candidates.len());
            for event in candidates {
                let sequence = event.sequence.ok_or_else(|| {
                    StoreError::Integrity("stored event is missing its sequence".into())
                })?;
                let artifact_refs = event_artifact_refs(&event);
                let archived = ArchivedCoordinationEvent {
                    original_sequence: sequence,
                    archived_at: now,
                    artifact_refs: artifact_refs.clone(),
                    event: event.clone(),
                };
                transaction.execute(
                    r#"INSERT INTO coordination_event_archive(
                         original_sequence, event_id, goal_id, archived_at, artifact_refs, data
                       ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
                    params![
                        sequence,
                        event.id.as_str(),
                        goal_id,
                        updated_at(now),
                        encode(&artifact_refs)?,
                        encode(&archived)?,
                    ],
                )?;
                let mut compacted = event;
                compacted.payload_schema = "coordination.compacted.v1".into();
                compacted.payload = serde_json::json!({
                    "compacted": true,
                    "archiveSequence": sequence,
                    "artifactRefs": artifact_refs,
                });
                transaction.execute(
                    "UPDATE coordination_events SET data = ?1 WHERE sequence = ?2",
                    params![encode(&compacted)?, sequence],
                )?;
                compacted_sequences.push(sequence);
            }
            Ok(EventCompactionReport {
                goal_id: goal_id.into(),
                acknowledged_through,
                compacted_sequences,
            })
        })
    }

    fn archived_events_for_goal(
        &self,
        goal_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ArchivedCoordinationEvent>, StoreError> {
        validate_replay_identity(goal_id, "goal id")?;
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM coordination_event_archive WHERE goal_id = ?1 AND original_sequence > ?2 ORDER BY original_sequence LIMIT ?3",
            params![goal_id, after_sequence, limit.clamp(1, 1_000) as i64],
        )
    }
}

impl SignalRepository for SqliteCoordinationStore {
    fn signal(&self, id: &SignalId) -> Result<Option<CoordinationSignal>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM coordination_signals WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn signals_for_goal(
        &self,
        goal_id: &str,
        state: Option<SignalState>,
    ) -> Result<Vec<CoordinationSignal>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM coordination_signals WHERE goal_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM coordination_signals WHERE goal_id = ?1 ORDER BY updated_at, id",
                [goal_id],
            ),
        }
    }
}

impl RolloutRepository for SqliteCoordinationStore {
    fn upsert_rollout_control(&self, control: &RolloutControl) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO rollout_controls(goal_id, phase, updated_at, data)
               VALUES (?1, ?2, ?3, ?4)
               ON CONFLICT(goal_id) DO UPDATE SET
                 phase=excluded.phase, updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                control.goal_id,
                enum_key(&control.phase)?,
                updated_at(control.updated_at),
                encode(control)?,
            ],
        )?;
        Ok(())
    }

    fn rollout_control(&self, goal_id: &str) -> Result<Option<RolloutControl>, StoreError> {
        let value = self
            .lock()?
            .query_row(
                "SELECT data FROM rollout_controls WHERE goal_id = ?1",
                [goal_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value.map(decode).transpose()
    }

    fn insert_shadow_observation(&self, observation: &ShadowObservation) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO rollout_shadow_observations(id, goal_id, observed_at, data)
               VALUES (?1, ?2, ?3, ?4)"#,
            params![
                observation.id,
                observation.goal_id,
                updated_at(observation.observed_at),
                encode(observation)?,
            ],
        )?;
        Ok(())
    }

    fn shadow_observations_for_goal(
        &self,
        goal_id: &str,
    ) -> Result<Vec<ShadowObservation>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM rollout_shadow_observations WHERE goal_id = ?1 ORDER BY observed_at, id",
            [goal_id],
        )
    }
}

fn event_artifact_refs(event: &CoordinationEvent) -> Vec<String> {
    let mut refs = Vec::new();
    collect_artifact_refs(&event.payload, None, &mut refs);
    refs.sort();
    refs.dedup();
    refs.truncate(64);
    refs
}

fn collect_artifact_refs(value: &serde_json::Value, key: Option<&str>, refs: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value)
            if matches!(
                key,
                Some("artifactId" | "artifact_id" | "evidenceRefs" | "evidence_refs")
            ) =>
        {
            if !value.trim().is_empty() {
                refs.push(value.clone());
            }
        }
        serde_json::Value::Array(values)
            if matches!(key, Some("evidenceRefs" | "evidence_refs")) =>
        {
            for value in values {
                if let Some(value) = value.as_str().filter(|value| !value.trim().is_empty()) {
                    refs.push(value.into());
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_artifact_refs(value, key, refs);
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                collect_artifact_refs(value, Some(key), refs);
            }
        }
        _ => {}
    }
}

fn validate_replay_identity(value: &str, label: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() || value.chars().count() > 200 {
        Err(StoreError::InvalidReplayCursor(format!(
            "{label} must contain 1-200 characters"
        )))
    } else {
        Ok(())
    }
}

fn upsert_replay_cursor_transaction(
    transaction: &Transaction<'_>,
    cursor: &EventReplayCursor,
) -> Result<(), StoreError> {
    transaction.execute(
        r#"INSERT INTO event_replay_cursors(
             consumer_id, goal_id, acknowledged_sequence, last_delivered_sequence, updated_at, data
           ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
           ON CONFLICT(consumer_id, goal_id) DO UPDATE SET
             acknowledged_sequence=excluded.acknowledged_sequence,
             last_delivered_sequence=excluded.last_delivered_sequence,
             updated_at=excluded.updated_at,
             data=excluded.data"#,
        params![
            cursor.consumer_id,
            cursor.goal_id,
            cursor.acknowledged_sequence,
            cursor.last_delivered_sequence,
            updated_at(cursor.updated_at),
            encode(cursor)?
        ],
    )?;
    Ok(())
}

fn upsert_intervention_transaction(
    transaction: &Transaction<'_>,
    intervention: &SupervisorIntervention,
) -> Result<(), StoreError> {
    transaction.execute(
        r#"INSERT INTO interventions(id, goal_id, state, updated_at, data)
           VALUES (?1, ?2, ?3, ?4, ?5)
           ON CONFLICT(id) DO UPDATE SET state=excluded.state,
             updated_at=excluded.updated_at, data=excluded.data"#,
        params![
            intervention.id.as_str(),
            intervention.goal_id,
            enum_key(&intervention.state)?,
            updated_at(intervention.metadata.updated_at),
            encode(intervention)?
        ],
    )?;
    Ok(())
}

macro_rules! impl_state_repository {
    (
        $trait_name:ident,
        $upsert:ident,
        $list:ident,
        $record:ty,
        $table:literal,
        $id_field:ident,
        $goal_field:ident,
        $state_field:ident,
        $state_type:ty
    ) => {
        impl $trait_name for SqliteCoordinationStore {
            fn $upsert(&self, record: &$record) -> Result<(), StoreError> {
                let sql = format!(
                    "INSERT INTO {}(id, goal_id, state, updated_at, data) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     ON CONFLICT(id) DO UPDATE SET state=excluded.state, \
                     updated_at=excluded.updated_at, data=excluded.data",
                    $table
                );
                self.lock()?.execute(
                    &sql,
                    params![
                        record.$id_field.as_str(),
                        record.$goal_field,
                        enum_key(&record.$state_field)?,
                        updated_at(record.metadata.updated_at),
                        encode(record)?
                    ],
                )?;
                CorrelationIds::for_goal(&record.goal_id)
                    .with_intervention(record.id.as_str())
                    .emit_info(
                        "supervisor.intervention.persisted",
                        &format!("{:?}", record.state).to_ascii_lowercase(),
                    );
                Ok(())
            }

            fn $list(
                &self,
                goal_id: &str,
                state: Option<$state_type>,
            ) -> Result<Vec<$record>, StoreError> {
                let connection = self.lock()?;
                let all_sql = format!(
                    "SELECT data FROM {} WHERE goal_id = ?1 ORDER BY updated_at, id",
                    $table
                );
                let state_sql = format!(
                    "SELECT data FROM {} WHERE goal_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                    $table
                );
                match state {
                    Some(state) => {
                        query_records(&connection, &state_sql, params![goal_id, enum_key(&state)?])
                    }
                    None => query_records(&connection, &all_sql, [goal_id]),
                }
            }
        }
    };
}

impl_state_repository!(
    InterventionRepository,
    upsert_intervention,
    interventions_for_goal,
    SupervisorIntervention,
    "interventions",
    id,
    goal_id,
    state,
    InterventionState
);

impl EscalationRepository for SqliteCoordinationStore {
    fn upsert_escalation(&self, escalation: &HumanEscalation) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO escalations(id, goal_id, state, severity, updated_at, data)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(id) DO UPDATE SET state=excluded.state,
                 severity=excluded.severity, updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                escalation.id.as_str(),
                escalation.goal_id,
                enum_key(&escalation.state)?,
                enum_key(&escalation.severity)?,
                updated_at(escalation.metadata.updated_at),
                encode(escalation)?
            ],
        )?;
        CorrelationIds::for_goal(&escalation.goal_id)
            .with_escalation(escalation.id.as_str())
            .emit_info(
                "coordination.escalation.persisted",
                &format!("{:?}", escalation.state).to_ascii_lowercase(),
            );
        Ok(())
    }

    fn escalation(&self, id: &EscalationId) -> Result<Option<HumanEscalation>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM escalations WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn update_escalation_if_revision(
        &self,
        escalation: &HumanEscalation,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let updated = self.lock()?.execute(
            r#"UPDATE escalations
               SET state = ?1, severity = ?2, updated_at = ?3, data = ?4
               WHERE id = ?5 AND goal_id = ?6 AND updated_at = ?7"#,
            params![
                enum_key(&escalation.state)?,
                enum_key(&escalation.severity)?,
                updated_at(escalation.metadata.updated_at),
                encode(escalation)?,
                escalation.id.as_str(),
                escalation.goal_id,
                updated_at(expected_updated_at)
            ],
        )? == 1;
        if updated {
            CorrelationIds::for_goal(&escalation.goal_id)
                .with_escalation(escalation.id.as_str())
                .emit_info(
                    "coordination.escalation.persisted",
                    &format!("{:?}", escalation.state).to_ascii_lowercase(),
                );
        }
        Ok(updated)
    }

    fn escalations_for_goal(
        &self,
        goal_id: &str,
        state: Option<EscalationState>,
    ) -> Result<Vec<HumanEscalation>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM escalations WHERE goal_id = ?1 AND state = ?2 ORDER BY severity DESC, updated_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM escalations WHERE goal_id = ?1 ORDER BY severity DESC, updated_at, id",
                [goal_id],
            ),
        }
    }
}

impl SupervisorBudgetRepository for SqliteCoordinationStore {
    fn upsert_supervisor_budget(&self, ledger: &SupervisorBudgetLedger) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO supervisor_budgets(goal_id, updated_at, data)
               VALUES (?1, ?2, ?3)
               ON CONFLICT(goal_id) DO UPDATE SET
                 updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                ledger.goal_id,
                updated_at(ledger.metadata.updated_at),
                encode(ledger)?
            ],
        )?;
        Ok(())
    }

    fn supervisor_budget(
        &self,
        goal_id: &str,
    ) -> Result<Option<SupervisorBudgetLedger>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM supervisor_budgets WHERE goal_id = ?1",
                [goal_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }
}

impl NotificationRepository for SqliteCoordinationStore {
    fn upsert_notification(&self, notification: &WorkerNotification) -> Result<(), StoreError> {
        notification.validate()?;
        self.lock()?.execute(
            r#"INSERT INTO notifications(id, goal_id, worker_id, state, updated_at, data)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6)
               ON CONFLICT(id) DO UPDATE SET state=excluded.state,
                 updated_at=excluded.updated_at, data=excluded.data"#,
            params![
                notification.id.as_str(),
                notification.goal_id,
                notification.target_worker.as_str(),
                enum_key(&notification.state)?,
                updated_at(notification.metadata.updated_at),
                encode(notification)?
            ],
        )?;
        Ok(())
    }

    fn notification(&self, id: &NotificationId) -> Result<Option<WorkerNotification>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM notifications WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn update_notification_if_revision(
        &self,
        notification: &WorkerNotification,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        notification.validate()?;
        Ok(self.lock()?.execute(
            r#"UPDATE notifications
               SET state = ?1, updated_at = ?2, data = ?3
               WHERE id = ?4 AND goal_id = ?5 AND worker_id = ?6 AND updated_at = ?7"#,
            params![
                enum_key(&notification.state)?,
                updated_at(notification.metadata.updated_at),
                encode(notification)?,
                notification.id.as_str(),
                notification.goal_id,
                notification.target_worker.as_str(),
                updated_at(expected_updated_at)
            ],
        )? == 1)
    }

    fn notifications_for_worker(
        &self,
        worker_id: &WorkerId,
        state: Option<NotificationState>,
    ) -> Result<Vec<WorkerNotification>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM notifications WHERE worker_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                params![worker_id.as_str(), enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM notifications WHERE worker_id = ?1 ORDER BY updated_at, id",
                [worker_id.as_str()],
            ),
        }
    }

    fn notifications_for_goal(
        &self,
        goal_id: &str,
        state: Option<NotificationState>,
    ) -> Result<Vec<WorkerNotification>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM notifications WHERE goal_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM notifications WHERE goal_id = ?1 ORDER BY updated_at, id",
                [goal_id],
            ),
        }
    }
}

impl ExchangeRepository for SqliteCoordinationStore {
    fn insert_exchange(&self, exchange: &CoordinationExchange) -> Result<(), StoreError> {
        exchange.validate()?;
        self.lock()?.execute(
            r#"INSERT INTO coordination_exchanges(
                 id, goal_id, request_event_id, requester_worker_id, target_worker_id,
                 state, expires_at, updated_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
            params![
                exchange.id.as_str(),
                exchange.goal_id,
                exchange.request_event_id.as_str(),
                exchange.requester_worker_id.as_str(),
                exchange.target_worker_id.as_str(),
                enum_key(&exchange.state)?,
                updated_at(exchange.expires_at),
                updated_at(exchange.metadata.updated_at),
                encode(exchange)?,
            ],
        )?;
        Ok(())
    }

    fn exchange(&self, id: &ExchangeId) -> Result<Option<CoordinationExchange>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM coordination_exchanges WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn update_exchange_if_revision(
        &self,
        exchange: &CoordinationExchange,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        exchange.validate()?;
        Ok(self.lock()?.execute(
            r#"UPDATE coordination_exchanges
               SET state = ?1, expires_at = ?2, updated_at = ?3, data = ?4
               WHERE id = ?5 AND goal_id = ?6 AND updated_at = ?7"#,
            params![
                enum_key(&exchange.state)?,
                updated_at(exchange.expires_at),
                updated_at(exchange.metadata.updated_at),
                encode(exchange)?,
                exchange.id.as_str(),
                exchange.goal_id,
                updated_at(expected_updated_at),
            ],
        )? == 1)
    }

    fn exchanges_for_goal(
        &self,
        goal_id: &str,
        state: Option<CoordinationExchangeState>,
    ) -> Result<Vec<CoordinationExchange>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM coordination_exchanges WHERE goal_id = ?1 AND state = ?2 ORDER BY updated_at, id",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM coordination_exchanges WHERE goal_id = ?1 ORDER BY updated_at, id",
                [goal_id],
            ),
        }
    }
}

impl IntegrationArtifactRepository for SqliteCoordinationStore {
    fn insert_integration_artifact(
        &self,
        artifact: &IntegrationArtifact,
    ) -> Result<(), StoreError> {
        artifact.validate()?;
        self.lock()?.execute(
            r#"INSERT INTO integration_artifacts(
                 id, goal_id, claim_id, worker_id, repository_id,
                 base_revision, head_revision, created_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"#,
            params![
                artifact.id.as_str(),
                artifact.goal_id,
                artifact.claim_id.as_str(),
                artifact.worker_id.as_str(),
                artifact.repository_id,
                artifact.base_revision,
                artifact.head_revision,
                updated_at(artifact.metadata.created_at),
                encode(artifact)?,
            ],
        )?;
        CorrelationIds::for_goal(&artifact.goal_id)
            .with_worker(artifact.worker_id.as_str())
            .with_claim(artifact.claim_id.as_str())
            .with_workspace(&artifact.repository_id)
            .with_integration(artifact.id.as_str())
            .emit_info("integration.artifact.persisted", "captured");
        Ok(())
    }

    fn integration_artifact(
        &self,
        id: &IntegrationArtifactId,
    ) -> Result<Option<IntegrationArtifact>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM integration_artifacts WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn integration_artifacts_for_goal(
        &self,
        goal_id: &str,
    ) -> Result<Vec<IntegrationArtifact>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM integration_artifacts WHERE goal_id = ?1 ORDER BY created_at, id",
            [goal_id],
        )
    }
}

impl ValidationReportRepository for SqliteCoordinationStore {
    fn insert_validation_report(
        &self,
        report: &IntegrationValidationReport,
    ) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO integration_validation_reports(
                 id, artifact_id, goal_id, worker_id, passed, created_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
            params![
                report.id.as_str(),
                report.artifact_id.as_str(),
                report.goal_id,
                report.worker_id.as_str(),
                report.passed,
                updated_at(report.metadata.created_at),
                encode(report)?,
            ],
        )?;
        Ok(())
    }

    fn validation_report(
        &self,
        id: &ValidationReportId,
    ) -> Result<Option<IntegrationValidationReport>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM integration_validation_reports WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn validation_reports_for_artifact(
        &self,
        artifact_id: &IntegrationArtifactId,
    ) -> Result<Vec<IntegrationValidationReport>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM integration_validation_reports WHERE artifact_id = ?1 ORDER BY rowid",
            [artifact_id.as_str()],
        )
    }
}

impl ReconciliationRepository for SqliteCoordinationStore {
    fn insert_reconciliation(&self, record: &ReconciliationRecord) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO integration_reconciliations(
                 id, artifact_id, goal_id, worker_id, strategy, state, created_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"#,
            params![
                record.id.as_str(),
                record.artifact_id.as_str(),
                record.goal_id,
                record.worker_id.as_str(),
                enum_key(&record.strategy)?,
                enum_key(&record.state)?,
                updated_at(record.metadata.created_at),
                encode(record)?,
            ],
        )?;
        Ok(())
    }

    fn reconciliation(
        &self,
        id: &ReconciliationId,
    ) -> Result<Option<ReconciliationRecord>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM integration_reconciliations WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn reconciliations_for_artifact(
        &self,
        artifact_id: &IntegrationArtifactId,
    ) -> Result<Vec<ReconciliationRecord>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM integration_reconciliations WHERE artifact_id = ?1 ORDER BY rowid",
            [artifact_id.as_str()],
        )
    }
}

impl IntegrationJobRepository for SqliteCoordinationStore {
    fn integration_job(&self, id: &IntegrationJobId) -> Result<Option<IntegrationJob>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM integration_jobs WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn integration_jobs_for_repository(
        &self,
        repository_id: &str,
        state: Option<IntegrationJobState>,
    ) -> Result<Vec<IntegrationJob>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM integration_jobs WHERE repository_id = ?1 AND state = ?2 ORDER BY priority DESC, enqueued_at, rowid",
                params![repository_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM integration_jobs WHERE repository_id = ?1 ORDER BY rowid",
                [repository_id],
            ),
        }
    }

    fn integration_jobs_for_goal(
        &self,
        goal_id: &str,
        state: Option<IntegrationJobState>,
    ) -> Result<Vec<IntegrationJob>, StoreError> {
        let connection = self.lock()?;
        match state {
            Some(state) => query_records(
                &connection,
                "SELECT data FROM integration_jobs WHERE goal_id = ?1 AND state = ?2 ORDER BY priority DESC, enqueued_at, rowid",
                params![goal_id, enum_key(&state)?],
            ),
            None => query_records(
                &connection,
                "SELECT data FROM integration_jobs WHERE goal_id = ?1 ORDER BY rowid",
                [goal_id],
            ),
        }
    }
}

impl IntegrationFinalizationRepository for SqliteCoordinationStore {
    fn insert_integration_finalization(
        &self,
        finalization: &IntegrationFinalization,
    ) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO integration_finalizations(
                 id, job_id, artifact_id, goal_id, state, updated_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
            params![
                finalization.id.as_str(),
                finalization.job_id.as_str(),
                finalization.artifact_id.as_str(),
                finalization.goal_id,
                enum_key(&finalization.state)?,
                updated_at(finalization.metadata.updated_at),
                encode(finalization)?,
            ],
        )?;
        CorrelationIds::for_goal(&finalization.goal_id)
            .with_claim(finalization.claim_id.as_str())
            .with_worker(finalization.worker_id.as_str())
            .with_integration(finalization.id.as_str())
            .emit_info(
                "integration.finalization.persisted",
                &format!("{:?}", finalization.state).to_ascii_lowercase(),
            );
        Ok(())
    }

    fn integration_finalization(
        &self,
        id: &IntegrationFinalizationId,
    ) -> Result<Option<IntegrationFinalization>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM integration_finalizations WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn prepared_integration_finalizations(
        &self,
    ) -> Result<Vec<IntegrationFinalization>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM integration_finalizations WHERE state = 'prepared' ORDER BY updated_at, rowid",
            [],
        )
    }
}

impl IntegrationMaintenanceRepository for SqliteCoordinationStore {
    fn insert_integration_maintenance(
        &self,
        record: &IntegrationMaintenanceRecord,
    ) -> Result<(), StoreError> {
        self.lock()?.execute(
            r#"INSERT INTO integration_maintenance_records(
                 id, finalization_id, artifact_id, goal_id, kind, state, created_at, data
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"#,
            params![
                record.id.as_str(),
                record.finalization_id.as_str(),
                record.artifact_id.as_str(),
                record.goal_id,
                enum_key(&record.kind)?,
                enum_key(&record.state)?,
                updated_at(record.metadata.created_at),
                encode(record)?,
            ],
        )?;
        CorrelationIds::for_goal(&record.goal_id)
            .with_integration(record.id.as_str())
            .emit_info(
                "integration.maintenance.persisted",
                &format!("{:?}", record.state).to_ascii_lowercase(),
            );
        Ok(())
    }

    fn integration_maintenance(
        &self,
        id: &IntegrationMaintenanceId,
    ) -> Result<Option<IntegrationMaintenanceRecord>, StoreError> {
        let encoded = self
            .lock()?
            .query_row(
                "SELECT data FROM integration_maintenance_records WHERE id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        encoded.map(decode).transpose()
    }

    fn integration_maintenance_for_artifact(
        &self,
        artifact_id: &IntegrationArtifactId,
    ) -> Result<Vec<IntegrationMaintenanceRecord>, StoreError> {
        let connection = self.lock()?;
        query_records(
            &connection,
            "SELECT data FROM integration_maintenance_records WHERE artifact_id = ?1 ORDER BY created_at, rowid",
            [artifact_id.as_str()],
        )
    }
}

impl IdempotencyRepository for SqliteCoordinationStore {
    fn idempotent_outcome(&self, key: &str) -> Result<Option<serde_json::Value>, StoreError> {
        let outcome = self
            .lock()?
            .query_row(
                "SELECT outcome FROM idempotency_records WHERE key = ?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        outcome.map(decode).transpose()
    }

    fn record_idempotent_outcome(
        &self,
        key: &str,
        operation: &str,
        outcome: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let inserted = self.lock()?.execute(
            "INSERT OR IGNORE INTO idempotency_records(key, operation, outcome, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![key, operation, encode(outcome)?, updated_at(now)],
        )?;
        Ok(inserted == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn store() -> (tempfile::TempDir, SqliteCoordinationStore) {
        let directory = tempfile::tempdir().unwrap();
        let store =
            SqliteCoordinationStore::open(directory.path().join("coordination.sqlite")).unwrap();
        (directory, store)
    }

    #[test]
    fn records_survive_store_reopen_and_support_typed_filters() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordination.sqlite");
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        {
            let store = SqliteCoordinationStore::open(&path).unwrap();
            store.upsert_worker(&worker).unwrap();
        }
        let reopened = SqliteCoordinationStore::open(&path).unwrap();

        assert_eq!(reopened.worker(&worker.id).unwrap(), Some(worker.clone()));
        assert_eq!(
            reopened
                .workers_for_goal("goal-a", Some(WorkerState::Starting))
                .unwrap(),
            vec![worker]
        );
    }

    #[test]
    fn immediate_transaction_rolls_back_all_writes_on_error() {
        let (_directory, store) = store();
        let result: Result<(), StoreError> = store.with_immediate_transaction(|transaction| {
            transaction.execute(
                "INSERT INTO workers(id, goal_id, state, updated_at, data) VALUES ('worker-a', 'goal-a', 'created', 'now', '{}')",
                [],
            )?;
            Err(StoreError::NotFound("force rollback".into()))
        });

        assert!(result.is_err());
        let count: i64 = store
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM workers", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn partial_unique_index_prevents_two_active_claim_owners() {
        let (_directory, store) = store();
        let now = Utc::now();
        let scope = ClaimScope::Feature {
            feature_id: "storage".into(),
        };
        let first = Claim::new(
            "goal-a",
            scope.clone(),
            WorkerId::new(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        let second = Claim::new(
            "goal-a",
            scope,
            WorkerId::new(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        store.insert_claim(&first).unwrap();

        assert!(matches!(
            store.insert_claim(&second),
            Err(StoreError::ActiveClaimExists { .. })
        ));
        assert_eq!(
            store
                .claims_for_goal("goal-a", Some(ClaimState::Active))
                .unwrap(),
            vec![first]
        );
    }

    #[test]
    fn event_sequence_is_durable_and_queryable_by_correlation() {
        let (_directory, store) = store();
        let now = Utc::now();
        let first = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Info,
            CoordinationActor::System,
            "worker-a",
            serde_json::json!({"message": "started"}),
            now,
        );
        let second = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Info,
            CoordinationActor::System,
            "worker-b",
            serde_json::json!({"message": "started"}),
            now,
        );
        assert_eq!(store.append_event(&first).unwrap(), 1);
        assert_eq!(store.append_event(&second).unwrap(), 2);

        let events = store
            .events_for_goal("goal-a", 0, Some("worker-b"), 100)
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, Some(2));
    }

    #[test]
    fn redacts_events_escalations_and_idempotent_outcomes_before_sqlite_storage() {
        let (_directory, store) = store();
        let now = Utc::now();
        let event = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Info,
            CoordinationActor::System,
            "worker-a",
            serde_json::json!({
                "prompt": "OPENAI_API_KEY=sk-event-secret",
                "authorization": "Bearer event-authorization-secret",
                "message": "safe event detail"
            }),
            now,
        );
        store.append_event(&event).unwrap();
        let stored_event = store.event(&event.id).unwrap().unwrap();
        let stored_event = serde_json::to_string(&stored_event).unwrap();
        assert!(!stored_event.contains("event-secret"));
        assert!(!stored_event.contains("event-authorization-secret"));
        assert!(stored_event.contains("safe event detail"));

        let mut escalation = HumanEscalation::new(
            "goal-a",
            EscalationKind::DecisionPoint,
            EscalationSeverity::High,
            EscalationScope::Claim {
                claim_id: ClaimId::new(),
            },
            "Investigate password=hunter2 while preserving the safe explanation",
            now,
        );
        escalation
            .evidence_refs
            .push("Authorization: Bearer escalation-secret".into());
        store.upsert_escalation(&escalation).unwrap();
        let stored_escalation = store.escalation(&escalation.id).unwrap().unwrap();
        let stored_escalation = serde_json::to_string(&stored_escalation).unwrap();
        assert!(!stored_escalation.contains("hunter2"));
        assert!(!stored_escalation.contains("escalation-secret"));
        assert!(stored_escalation.contains("safe explanation"));

        let outcome = serde_json::json!({
            "diff": "+ api_key: diff-secret",
            "stdout": "github token ghp_1234567890abcdef",
            "status": "validated"
        });
        assert!(
            store
                .record_idempotent_outcome("redaction-test", "validate", &outcome, now)
                .unwrap()
        );
        let stored_outcome = store.idempotent_outcome("redaction-test").unwrap().unwrap();
        let stored_outcome = stored_outcome.to_string();
        assert!(!stored_outcome.contains("diff-secret"));
        assert!(!stored_outcome.contains("ghp_1234567890abcdef"));
        assert!(stored_outcome.contains("validated"));
    }

    #[test]
    fn publication_keys_make_event_retries_exactly_once() {
        let (_directory, store) = store();
        let now = Utc::now();
        let first = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Info,
            CoordinationActor::System,
            "claim-a",
            serde_json::json!({"message": "first"}),
            now,
        );
        let retry = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Info,
            CoordinationActor::System,
            "claim-a",
            serde_json::json!({"message": "retry"}),
            now,
        );

        let first_sequence = store
            .append_event_once("activity:request-1", &first)
            .unwrap();
        let retry_sequence = store
            .append_event_once("activity:request-1", &retry)
            .unwrap();

        assert_eq!(first_sequence, retry_sequence);
        let events = store.events_for_goal("goal-a", 0, None, 100).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, first.id);
    }

    #[test]
    fn replay_cursors_acknowledge_replay_and_survive_restart() {
        let (directory, store) = store();
        let now = Utc::now();
        for index in 1..=3 {
            let event = CoordinationEvent::new(
                "goal-a",
                CoordinationEventKind::ActivityPublished,
                EventSeverity::Info,
                CoordinationActor::System,
                "claim-a",
                serde_json::json!({"index": index}),
                now,
            );
            store.append_event(&event).unwrap();
        }

        let first = store
            .replay_events_for_consumer("dashboard", "goal-a", 2, now)
            .unwrap();
        assert_eq!(
            first
                .events
                .iter()
                .filter_map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let retry = store
            .replay_events_for_consumer("dashboard", "goal-a", 2, now)
            .unwrap();
        assert_eq!(retry.events, first.events);
        let acknowledged = store
            .acknowledge_event_sequence("dashboard", "goal-a", 2, now)
            .unwrap();
        assert_eq!(acknowledged.acknowledged_sequence, 2);
        assert!(matches!(
            store.acknowledge_event_sequence("dashboard", "goal-a", 4, now),
            Err(StoreError::InvalidReplayCursor(_))
        ));

        let path = directory.path().join("coordination.sqlite");
        drop(store);
        let reopened = SqliteCoordinationStore::open(path).unwrap();
        let remaining = reopened
            .replay_events_for_consumer("dashboard", "goal-a", 10, now)
            .unwrap();
        assert_eq!(remaining.events.len(), 1);
        assert_eq!(remaining.events[0].sequence, Some(3));
        reopened
            .acknowledge_event_sequence("dashboard", "goal-a", 3, now)
            .unwrap();
        assert!(
            reopened
                .replay_events_for_consumer("dashboard", "goal-a", 10, now)
                .unwrap()
                .events
                .is_empty()
        );
        assert_eq!(
            reopened
                .event_replay_cursor("dashboard", "goal-a")
                .unwrap()
                .unwrap()
                .acknowledged_sequence,
            3
        );
        assert_eq!(
            reopened
                .replay_events_for_consumer("supervisor", "goal-a", 10, now)
                .unwrap()
                .events
                .len(),
            3
        );
    }

    #[test]
    fn retention_compacts_only_fully_acknowledged_events_and_preserves_audit_data() {
        let (_directory, store) = store();
        let now = Utc::now();
        let occurred_at = now - Duration::hours(2);
        for index in 1..=5 {
            let event = CoordinationEvent::new(
                "goal-a",
                CoordinationEventKind::ActivityPublished,
                EventSeverity::Info,
                CoordinationActor::System,
                "claim-a",
                serde_json::json!({
                    "index": index,
                    "artifactId": format!("artifact-{index}"),
                    "evidenceRefs": [format!("test:{index}")],
                }),
                occurred_at,
            );
            store.append_event(&event).unwrap();
        }
        store
            .replay_events_for_consumer("dashboard", "goal-a", 10, now)
            .unwrap();
        store
            .acknowledge_event_sequence("dashboard", "goal-a", 3, now)
            .unwrap();
        store
            .replay_events_for_consumer("slow-consumer", "goal-a", 10, now)
            .unwrap();
        store
            .acknowledge_event_sequence("slow-consumer", "goal-a", 1, now)
            .unwrap();
        let policy = EventRetentionPolicy {
            hot_retention_seconds: 60 * 60,
            retain_latest: 1,
            max_compaction_batch: 10,
        };

        let first = store.compact_events("goal-a", &policy, now).unwrap();
        assert_eq!(first.acknowledged_through, 1);
        assert_eq!(first.compacted_sequences, vec![1]);
        store
            .acknowledge_event_sequence("slow-consumer", "goal-a", 3, now)
            .unwrap();
        let second = store.compact_events("goal-a", &policy, now).unwrap();
        assert_eq!(second.compacted_sequences, vec![2, 3]);

        let archive = store.archived_events_for_goal("goal-a", 0, 10).unwrap();
        assert_eq!(archive.len(), 3);
        assert_eq!(archive[0].event.payload["index"], 1);
        assert!(archive[0].artifact_refs.contains(&"artifact-1".to_string()));
        assert!(archive[0].artifact_refs.contains(&"test:1".to_string()));
        let compacted = store.events_for_goal("goal-a", 0, None, 1).unwrap();
        assert_eq!(compacted[0].payload_schema, "coordination.compacted.v1");
        assert_eq!(compacted[0].payload["archiveSequence"], 1);
        assert_eq!(compacted[0].typed_payload().unwrap(), None);
    }

    #[test]
    fn failed_event_publication_rolls_back_the_state_mutation() {
        let (_directory, store) = store();
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        let mut claim = Claim::new(
            "goal-a",
            ClaimScope::Feature {
                feature_id: "storage".into(),
            },
            worker.id.clone(),
            "base-a",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.acquire_claim_for_worker(&claim, &worker).unwrap();
        let duplicate_event = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ClaimLifecycleChanged,
            EventSeverity::Info,
            CoordinationActor::System,
            claim.id.as_str(),
            serde_json::json!({"state": "completed"}),
            now,
        );
        store.append_event(&duplicate_event).unwrap();

        claim
            .transition(
                ClaimState::Completed,
                ClaimOutcome {
                    kind: ClaimOutcomeKind::Completed,
                    reason: "complete".into(),
                    artifact_id: Some("artifact-1".into()),
                    evidence_refs: vec!["test:passed".into()],
                    escalation_id: None,
                    at: now,
                },
            )
            .unwrap();
        worker.active_claims.clear();
        let result = store.finalize_claim_for_worker_with_event(
            &claim,
            &worker,
            claim.lease_generation,
            &duplicate_event,
            "claim.complete:request-1",
            "claim.complete",
            "request-1",
        );

        assert!(matches!(result, Err(StoreError::Database(_))));
        let stored_claim = store.claim(&claim.id).unwrap().unwrap();
        assert_eq!(stored_claim.state, ClaimState::Active);
        assert!(
            store
                .worker(&worker.id)
                .unwrap()
                .unwrap()
                .active_claims
                .contains(&claim.id)
        );
        assert_eq!(store.idempotent_outcome("request-1").unwrap(), None);
    }

    #[test]
    fn failed_contract_event_rolls_back_the_revision() {
        let (_directory, store) = store();
        let now = Utc::now();
        let worker_id = WorkerId::new();
        let mut contract =
            SharedContract::new("goal-a", "api.worker", "Worker API", ContractKind::Api, now);
        store.upsert_contract(&contract).unwrap();
        let duplicate_event = CoordinationEvent::new(
            "goal-a",
            CoordinationEventKind::ContractChanged,
            EventSeverity::Warning,
            CoordinationActor::Worker {
                worker_id: worker_id.clone(),
            },
            "claim-a",
            serde_json::json!({"revision": 2}),
            now,
        );
        store.append_event(&duplicate_event).unwrap();
        contract.revise(worker_id, "adds a field", now);

        let result = store.revise_contract_with_event(
            &contract,
            1,
            &duplicate_event,
            "contract.revise:api.worker:2",
        );

        assert!(matches!(result, Err(StoreError::Database(_))));
        assert_eq!(store.contract(&contract.id).unwrap().unwrap().revision, 1);
    }

    #[test]
    fn idempotency_key_preserves_the_first_recorded_outcome() {
        let (_directory, store) = store();
        let now = Utc::now();
        assert!(
            store
                .record_idempotent_outcome(
                    "request-1",
                    "claim.acquire",
                    &serde_json::json!({"claimId": "claim-1"}),
                    now,
                )
                .unwrap()
        );
        assert!(
            !store
                .record_idempotent_outcome(
                    "request-1",
                    "claim.acquire",
                    &serde_json::json!({"claimId": "claim-2"}),
                    now,
                )
                .unwrap()
        );

        assert_eq!(
            store.idempotent_outcome("request-1").unwrap(),
            Some(serde_json::json!({"claimId": "claim-1"}))
        );
    }

    #[test]
    fn existing_empty_database_is_migrated_without_goal_data() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("existing.sqlite");
        Connection::open(&path).unwrap();

        let store = SqliteCoordinationStore::open(&path).unwrap();
        let version: i64 = store
            .lock()
            .unwrap()
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, STORE_SCHEMA_VERSION);
        assert!(
            store
                .workers_for_goal("legacy-goal", None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn online_backup_and_restore_preserve_coordination_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordination.sqlite");
        let backup_path = directory.path().join("backup.sqlite");
        let store = SqliteCoordinationStore::open(&path).unwrap();
        let worker = Worker::new("goal-a", Utc::now());
        store.upsert_worker(&worker).unwrap();
        store.backup_to(&backup_path).unwrap();
        store
            .lock()
            .unwrap()
            .execute("DELETE FROM workers", [])
            .unwrap();
        assert!(store.worker(&worker.id).unwrap().is_none());

        store.restore_from(&backup_path).unwrap();

        assert_eq!(store.worker(&worker.id).unwrap(), Some(worker));
    }

    #[test]
    fn concurrent_connections_allow_only_one_active_claim_owner() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordination.sqlite");
        SqliteCoordinationStore::open(&path).unwrap();
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for index in 0..2 {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let store = SqliteCoordinationStore::open(path).unwrap();
                let now = Utc::now();
                let claim = Claim::new(
                    "goal-a",
                    ClaimScope::Feature {
                        feature_id: "storage".into(),
                    },
                    WorkerId::parse(format!("worker-{index}")).unwrap(),
                    "base-a",
                    now,
                    now + Duration::minutes(5),
                )
                .unwrap();
                barrier.wait();
                store.insert_claim(&claim)
            }));
        }
        barrier.wait();
        let outcomes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(StoreError::ActiveClaimExists { .. })))
                .count(),
            1
        );
    }

    #[test]
    fn concurrent_event_appends_receive_unique_ordered_sequences() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("coordination.sqlite");
        SqliteCoordinationStore::open(&path).unwrap();
        let barrier = Arc::new(Barrier::new(9));
        let mut handles = Vec::new();
        for index in 0..8 {
            let path = path.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let store = SqliteCoordinationStore::open(path).unwrap();
                let event = CoordinationEvent::new(
                    "goal-a",
                    CoordinationEventKind::ActivityPublished,
                    EventSeverity::Info,
                    CoordinationActor::System,
                    format!("event-{index}"),
                    serde_json::json!({"index": index}),
                    Utc::now(),
                );
                barrier.wait();
                store.append_event(&event).unwrap()
            }));
        }
        barrier.wait();
        let mut sequences = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        sequences.sort_unstable();

        assert_eq!(sequences, (1..=8).collect::<Vec<_>>());
    }
}
