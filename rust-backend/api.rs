use crate::coordination::alerts::{
    AlertError, AlertPolicy, OperationalAlertService, OperationalAlertSnapshot,
};
use crate::coordination::api_contract::{
    ActionConfirmation, ApiProblem, CollectionPage, CommandEnvelope, CoordinationApiManifest,
    PageQuery, ProblemCode, ResourceDocument, ResourceMetadata, resource_version,
};
use crate::coordination::claims::{
    BlockEvidence, CancelEvidence, ClaimPolicy, ClaimService, ClaimServiceError,
    CompletionEvidence, ManualAssignmentEvidence, ReleaseEvidence,
};
use crate::coordination::contracts::{
    ContractRegistration, ContractRegistry, ContractRegistryError,
};
use crate::coordination::delivery::{
    GoalDeliveryState, ensure_goal_base_revision, read_state as read_goal_delivery_state,
};
use crate::coordination::domain::{
    Claim, ClaimId, ClaimScope, ClaimState, ContractId, ContractKind, CoordinationActor,
    CoordinationEvent, CoordinationEventPayload, CoordinationSignal, EscalationDecision,
    EscalationId, EventId, EventSeverity, HumanEscalation, IntegrationArtifact,
    IntegrationArtifactId, IntegrationContractChange, IntegrationFinalization,
    IntegrationFinalizationId, IntegrationJob, IntegrationJobId, IntegrationJobState,
    IntegrationMaintenanceRecord, IntegrationMigration, IntegrationValidationEvidence,
    IntegrationValidationReport, InterventionId, NotificationId, PrivilegedActionEventPayload,
    ReconciliationRecord, ReconciliationStrategy, SharedContract, SignalId, SupervisorIntervention,
    ValidationGateKind, ValidationReportId, WorkPackage, WorkPackageId, Worker, WorkerId,
    WorkerNotification, WorkerPermissionPolicy, WorkerState, WorkspaceBinding,
};
use crate::coordination::escalations::{EscalationLifecycleError, EscalationLifecycleService};
use crate::coordination::execution::{dispatch_claims, recover_unstarted_claims};
use crate::coordination::health::{GoalHealthService, HealthError, HealthPolicy};
use crate::coordination::integration::{
    CaptureIntegrationArtifactRequest, IntegrationArtifactError, IntegrationArtifactService,
    IntegrationFinalizationRequest, IntegrationFinalizationService, IntegrationMaintenanceService,
    IntegrationPreflight, IntegrationPreflightService, IntegrationQueueService,
    ReconciliationPolicy, ReconciliationRequest, ReconciliationService, RegenerationCommand,
    ValidationGateRunner, ValidationGateSpec,
};
use crate::coordination::metrics::{GoalMetricsService, MetricsError};
use crate::coordination::notifications::{
    NotificationLifecycleError, NotificationLifecycleService,
};
use crate::coordination::pool::{WorkerPoolError, WorkerPoolPolicy, WorkerPoolService};
use crate::coordination::protocol::{
    WorkerContextLimits, WorkerProtocolService, WorkerToolError, WorkerToolRequest,
    WorkerToolResponse,
};
use crate::coordination::recovery_artifacts::{PartialWorkArtifact, partial_work_for_goal};
use crate::coordination::security::ResourceQuotaPolicy;
use crate::coordination::store::{
    ClaimRepository, ContractRepository, EscalationRepository, EventRepository,
    IdempotencyRepository, IntegrationArtifactRepository, IntegrationFinalizationRepository,
    IntegrationJobRepository, IntegrationMaintenanceRepository, InterventionRepository,
    NotificationRepository, ReconciliationRepository, SignalRepository, SqliteCoordinationStore,
    StoreError, ValidationReportRepository, WorkPackageRepository, WorkerRepository,
};
use crate::coordination::traces::{ClaimTrace, ClaimTraceService, TraceError};
use crate::models::*;
use crate::projects::{ProjectRecord, ProjectRegistry};
use crate::prompts;
use crate::redaction::redact_sensitive_value;
use crate::runner::RunManager;
use crate::tracker::{Tracker, TrackerError, generate_goal_id};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use chrono::Utc;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::{StreamExt, wrappers::BroadcastStream, wrappers::ReceiverStream};

const MAX_COORDINATION_SSE_DATA_BYTES: usize = 64 * 1_024;
const DEFAULT_COORDINATION_SSE_BATCH: usize = 50;
const MAX_COORDINATION_SSE_BATCH: usize = 200;

fn redacted_json_value<T: Serialize>(value: T) -> Result<Value, serde_json::Error> {
    let mut value = serde_json::to_value(value)?;
    redact_sensitive_value(&mut value);
    Ok(value)
}

#[derive(Clone)]
pub struct AppState {
    pub tracker_root: Arc<RwLock<PathBuf>>,
    pub runner: Arc<RunManager>,
    pub browse_root: PathBuf,
    pub registry: ProjectRegistry,
    pub active_project_id: Arc<RwLock<String>>,
    pub profile_path: PathBuf,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}
impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
    fn bad(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, message)
    }
}
impl From<TrackerError> for ApiError {
    fn from(error: TrackerError) -> Self {
        Self::new(
            StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            error.message,
        )
    }
}
impl From<WorkerPoolError> for ApiError {
    fn from(error: WorkerPoolError) -> Self {
        let status = match error {
            WorkerPoolError::GoalNotConfigured(_) | WorkerPoolError::WorkerNotFound(_) => {
                StatusCode::NOT_FOUND
            }
            WorkerPoolError::InvalidConcurrency { .. }
            | WorkerPoolError::RecoveryIncomplete(_)
            | WorkerPoolError::ActiveRunExists(_)
            | WorkerPoolError::NoActiveRun(_)
            | WorkerPoolError::WorkspaceAlreadyBound(_)
            | WorkerPoolError::InvalidPermissionProfile(_)
            | WorkerPoolError::IsolationUnavailable(_) => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self::new(status, error.to_string())
    }
}
impl From<WorkerToolError> for ApiError {
    fn from(error: WorkerToolError) -> Self {
        let status = match &error {
            WorkerToolError::Claim(
                crate::coordination::claims::ClaimServiceError::ClaimNotFound(_),
            )
            | WorkerToolError::Context(
                crate::coordination::protocol::WorkerContextError::WorkerNotFound(_)
                | crate::coordination::protocol::WorkerContextError::ClaimNotFound(_),
            )
            | WorkerToolError::ContractNotFound(_)
            | WorkerToolError::ContextTransferNotFound(_) => StatusCode::NOT_FOUND,
            WorkerToolError::InactiveOrUnownedClaim
            | WorkerToolError::ContractRegistry(_)
            | WorkerToolError::ContractGoalMismatch
            | WorkerToolError::ContractChangeNotAuthorized(_)
            | WorkerToolError::ContractRevisionMismatch { .. }
            | WorkerToolError::CompletionGateFailed(_)
            | WorkerToolError::NoActiveTurn
            | WorkerToolError::ActiveRunExists
            | WorkerToolError::ContextTransferMismatch
            | WorkerToolError::ContextTransferAlreadyUsed
            | WorkerToolError::InvalidContinuation => StatusCode::CONFLICT,
            WorkerToolError::PayloadTooLarge(_)
            | WorkerToolError::EmptyEvidence
            | WorkerToolError::InvalidActivity(_) => StatusCode::UNPROCESSABLE_ENTITY,
            _ => StatusCode::BAD_REQUEST,
        };
        Self::new(status, error.to_string())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"detail":self.message}))).into_response()
    }
}

#[derive(Debug)]
struct CoordinationApiError {
    status: StatusCode,
    problem: ApiProblem,
}

impl CoordinationApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            problem: ApiProblem::new(ProblemCode::InvalidRequest, message, false),
        }
    }

    fn stale(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            problem: ApiProblem::new(ProblemCode::StaleRevision, message, true),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            problem: ApiProblem::new(ProblemCode::NotFound, message, false),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            problem: ApiProblem::new(ProblemCode::Conflict, message, false),
        }
    }

    fn confirmation(message: impl Into<String>, action: &str, target: &str) -> Self {
        let mut problem = ApiProblem::new(ProblemCode::ConfirmationRequired, message, false);
        problem.details = Some(json!({"action": action, "target": target}));
        Self {
            status: StatusCode::PRECONDITION_REQUIRED,
            problem,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            problem: ApiProblem::new(ProblemCode::Internal, message, true),
        }
    }
}

impl IntoResponse for CoordinationApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.problem)).into_response()
    }
}

#[derive(RustEmbed)]
#[folder = "rust-backend/static-react/"]
struct Frontend;

fn tracker(state: &AppState, root: &Path) -> Tracker {
    let _ = state;
    Tracker::new(root)
}
fn validate_text(value: &str, name: &str, max: usize) -> Result<(), ApiError> {
    if value.is_empty() {
        Err(ApiError::bad(format!("{name} must not be empty")))
    } else if value.len() > max {
        Err(ApiError::bad(format!("{name} is too long")))
    } else {
        Ok(())
    }
}

async fn filesystem_path(state: &AppState, value: Option<&str>) -> Result<PathBuf, ApiError> {
    let candidate = match value {
        Some(path) => PathBuf::from(path),
        None => state.browse_root.clone(),
    }
    .canonicalize()
    .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "directory not found"))?;
    if candidate != state.browse_root && !candidate.starts_with(&state.browse_root) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "path is outside the configured browsing root",
        ));
    }
    if !candidate.is_dir() {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "directory not found"));
    }
    Ok(candidate)
}

async fn project_summary(state: &AppState, record: ProjectRecord) -> Value {
    let path = PathBuf::from(&record.path);
    let exists = path.is_dir();
    let goals = if exists {
        Tracker::new(path.join(".goal-manager"))
            .list_goals()
            .unwrap_or_default()
    } else {
        vec![]
    };
    let usage = if exists {
        RunManager::usage_for_history(&path.join(".goal-manager/run-history.json")).await
    } else {
        json!({"input_tokens":0,"cached_input_tokens":0,"output_tokens":0,"reasoning_output_tokens":0,"total_tokens":0,"runs":0,"completed_runs":0})
    };
    let goal_recency = if exists {
        RunManager::goal_recency_for_history(
            &path.join(".goal-manager/run-history.json"),
            &path.join(".goal-manager/thread-settings.json"),
        )
        .await
    } else {
        Default::default()
    };
    let active = *state.active_project_id.read().await == record.id;
    let compact = goals
        .into_iter()
        .map(|goal| {
            let goal_id = goal["goal_id"].as_str().unwrap_or_default();
            json!({
                "goal_id": goal["goal_id"],
                "title": goal["title"],
                "progress": goal["progress"],
                "features": goal["features"].as_array().map(Vec::len).unwrap_or(0),
                "last_thread_at": goal_recency.get(goal_id),
            })
        })
        .collect::<Vec<_>>();
    json!({"id":record.id,"name":record.name,"path":record.path,"created_at":record.created_at,"last_opened_at":record.last_opened_at,"exists":exists,"active":active,"goals":compact,"usage":usage})
}

async fn activate_project(state: &AppState, record: ProjectRecord) -> Result<Value, ApiError> {
    let selected = filesystem_path(state, Some(&record.path)).await?;
    state
        .runner
        .switch_workspace(
            selected.clone(),
            selected.join(".goal-manager/run-history.json"),
        )
        .await
        .map_err(|error| ApiError::new(StatusCode::CONFLICT, error))?;
    *state.tracker_root.write().await = selected.join(".goal-manager");
    *state.active_project_id.write().await = record.id.clone();
    state
        .registry
        .add(&selected, true)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    Ok(project_summary(state, record).await)
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route(
            "/coordination/v1",
            get(|| async { Json(CoordinationApiManifest::default()) }),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/pool",
            get(get_coordination_pool),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/metrics",
            get(get_coordination_metrics),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/health",
            get(get_coordination_health),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/alerts",
            get(get_coordination_alerts),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/pool/commands",
            post(command_coordination_pool),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers",
            get(list_coordination_workers),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers/{worker_id}",
            get(get_coordination_worker),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers/{worker_id}/commands",
            post(command_coordination_worker),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers/{worker_id}/activity",
            get(get_coordination_worker_activity),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers/{worker_id}/workspace",
            get(get_coordination_worker_workspace),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers/{worker_id}/run",
            get(get_coordination_worker_run),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/workers/{worker_id}/thread",
            get(get_coordination_worker_thread),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/ready-work",
            get(list_coordination_ready_work),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/partial-work",
            get(list_coordination_partial_work),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/delivery",
            get(get_coordination_goal_delivery),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/packages",
            get(list_coordination_packages),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/packages/{package_id}",
            get(get_coordination_package),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/claims",
            get(list_coordination_claims),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/claims/{claim_id}",
            get(get_coordination_claim),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/claims/{claim_id}/trace",
            get(get_coordination_claim_trace),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/claims/commands",
            post(acquire_coordination_claim),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/claims/{claim_id}/commands",
            post(command_coordination_claim),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/contracts",
            get(list_coordination_contracts),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/contracts/{contract_id}",
            get(get_coordination_contract),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/contracts/commands",
            post(register_coordination_contract),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/contracts/{contract_id}/commands",
            post(command_coordination_contract),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/events",
            get(list_coordination_events),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/events/{event_id}",
            get(get_coordination_event),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/events/stream",
            get(stream_coordination_events),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/events/commands",
            post(publish_coordination_event),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/signals",
            get(list_coordination_signals),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/signals/{signal_id}",
            get(get_coordination_signal),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/interventions",
            get(list_coordination_interventions),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/interventions/{intervention_id}",
            get(get_coordination_intervention),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/notifications",
            get(list_coordination_notifications),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/notifications/{notification_id}",
            get(get_coordination_notification),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/notifications/{notification_id}/commands",
            post(command_coordination_notification),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/escalations",
            get(list_coordination_escalations),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/escalations/{escalation_id}",
            get(get_coordination_escalation),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/escalations/{escalation_id}/commands",
            post(command_coordination_escalation),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts",
            get(list_coordination_integration_artifacts),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts/commands",
            post(capture_coordination_integration_artifact),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts/{artifact_id}",
            get(get_coordination_integration_artifact),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts/{artifact_id}/commands",
            post(command_coordination_integration_artifact),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts/{artifact_id}/validations",
            get(list_coordination_integration_validations),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts/{artifact_id}/reconciliations",
            get(list_coordination_integration_reconciliations),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/artifacts/{artifact_id}/maintenance",
            get(list_coordination_integration_maintenance),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/jobs",
            get(list_coordination_integration_jobs),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/jobs/commands",
            post(command_coordination_integration_queue),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/jobs/{job_id}",
            get(get_coordination_integration_job),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/jobs/{job_id}/commands",
            post(command_coordination_integration_job),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/finalizations/{finalization_id}",
            get(get_coordination_integration_finalization),
        )
        .route(
            "/coordination/v1/goals/{goal_id}/integration/finalizations/{finalization_id}/commands",
            post(command_coordination_integration_finalization),
        )
        .route("/workspace", get(get_workspace).post(select_workspace))
        .route("/projects", get(list_projects).post(add_project))
        .route("/projects/{project_id}/activate", post(select_project))
        .route(
            "/projects/{project_id}",
            patch(rename_project).delete(remove_project),
        )
        .route("/filesystem", get(browse_filesystem))
        .route("/goals", get(list_goals).post(create_goal))
        .route("/goals/{goal_id}", get(get_goal))
        .route(
            "/goals/{goal_id}/worker-pool",
            get(get_worker_pool).patch(update_worker_pool),
        )
        .route(
            "/goals/{goal_id}/worker-pool/{action}",
            post(control_worker_pool),
        )
        .route(
            "/goals/{goal_id}/workers/{worker_id}/operations",
            post(execute_worker_operation),
        )
        .route("/goal-scaffolds", post(create_goal_scaffold))
        .route(
            "/greenfield-location-suggestion",
            get(get_greenfield_location_suggestion),
        )
        .route("/goal-scaffolds/{run_id}", get(get_goal_scaffold))
        .route(
            "/goal-scaffolds/{run_id}/accept",
            post(accept_goal_scaffold),
        )
        .route("/goals/{goal_id}/features", post(add_feature))
        .route(
            "/goals/{goal_id}/features/{feature_id}",
            patch(set_feature_status),
        )
        .route(
            "/goals/{goal_id}/features/{feature_id}/steps",
            post(add_step),
        )
        .route(
            "/goals/{goal_id}/features/{feature_id}/steps/{step_id}",
            patch(set_step),
        )
        .route("/goals/{goal_id}/slices", post(add_slice))
        .route("/goals/{goal_id}/validate", post(validate_goal))
        .route("/runs", get(list_runs).post(create_run))
        .route("/runs/{run_id}", get(get_run))
        .route("/runs/{run_id}/images/{image_id}", get(get_run_image))
        .route(
            "/settings/default-llm",
            get(get_default_llm_config).patch(set_default_llm_config),
        )
        .route("/codex-auth/status", get(get_codex_auth_status))
        .route("/codex-auth/login", post(start_codex_login))
        .route("/codex-auth/logout", post(logout_codex))
        .route("/profile", get(get_profile).patch(update_profile))
        .route("/threads", get(list_threads))
        .route("/threads/{thread_id}", patch(update_thread))
        .route("/app-server/threads", get(list_app_server_threads))
        .route(
            "/app-server/threads/{thread_id}",
            get(get_app_server_thread),
        )
        .route(
            "/app-server/threads/{thread_id}/goal",
            patch(assign_app_server_thread_goal),
        )
        .route("/app-server/events", get(stream_app_server_events))
        .route("/app-server/models", get(get_app_server_models))
        .route("/app-server/account", get(get_app_server_account))
        .route("/app-server/rate-limits", get(get_app_server_rate_limits))
        .route("/app-server/approvals", get(list_app_server_approvals))
        .route(
            "/app-server/approvals/{approval_id}",
            post(decide_app_server_approval),
        )
        .route("/usage", get(get_usage))
        .fallback(get(static_asset))
        .with_state(state)
}

async fn get_workspace(State(state): State<AppState>) -> Json<Value> {
    let root = state.runner.workspace_root().await;
    Json(
        json!({"path":root,"name":root.file_name().and_then(|v|v.to_str()).unwrap_or_else(||root.to_str().unwrap_or("project")),"browse_root":state.browse_root,"project_id":*state.active_project_id.read().await}),
    )
}

#[derive(Debug, Deserialize)]
struct WorkerPoolUpdate {
    desired_concurrency: usize,
    #[serde(default)]
    base_revision: Option<String>,
    #[serde(default)]
    user_policy: Option<WorkerPermissionPolicy>,
    #[serde(default)]
    permission_policy: Option<WorkerPermissionPolicy>,
    #[serde(default)]
    resource_policy: Option<ResourceQuotaPolicy>,
    #[serde(default)]
    confirmation: Option<ActionConfirmation>,
}

#[derive(Debug, Deserialize, Default)]
struct WorkerPoolControl {
    #[serde(default)]
    desired_concurrency: Option<usize>,
    #[serde(default)]
    base_revision: Option<String>,
    #[serde(default)]
    user_policy: Option<WorkerPermissionPolicy>,
    #[serde(default)]
    permission_policy: Option<WorkerPermissionPolicy>,
    #[serde(default)]
    resource_policy: Option<ResourceQuotaPolicy>,
    #[serde(default)]
    confirmation: Option<ActionConfirmation>,
}

async fn coordination_services(
    state: &AppState,
) -> Result<(WorkerPoolService, ClaimService), ApiError> {
    let root = state.tracker_root.read().await.clone();
    let store = Arc::new(
        match SqliteCoordinationStore::open(root.join("coordination.sqlite")) {
            Ok(store) => store,
            Err(error) => {
                let _ = OperationalAlertService::record_store_error_for_all(
                    &root,
                    &error.to_string(),
                    Utc::now(),
                );
                return Err(ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    error.to_string(),
                ));
            }
        },
    );
    let pool = WorkerPoolService::load(Arc::clone(&store), WorkerPoolPolicy::default())?;
    let claims = ClaimService::new(Tracker::new(root), store, ClaimPolicy::default());
    Ok((pool, claims))
}

async fn coordination_store(
    state: &AppState,
) -> Result<Arc<SqliteCoordinationStore>, CoordinationApiError> {
    let root = state.tracker_root.read().await.clone();
    match SqliteCoordinationStore::open(root.join("coordination.sqlite")) {
        Ok(store) => Ok(Arc::new(store)),
        Err(error) => {
            let _ = OperationalAlertService::record_store_error_for_all(
                &root,
                &error.to_string(),
                Utc::now(),
            );
            Err(CoordinationApiError::internal(error.to_string()))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum PoolCommand {
    Configure {
        desired_concurrency: usize,
        #[serde(default)]
        base_revision: Option<String>,
        #[serde(default)]
        user_policy: Option<WorkerPermissionPolicy>,
        #[serde(default)]
        permission_policy: Option<WorkerPermissionPolicy>,
        #[serde(default)]
        resource_policy: Option<ResourceQuotaPolicy>,
    },
    Start {
        #[serde(default)]
        desired_concurrency: Option<usize>,
        #[serde(default)]
        base_revision: Option<String>,
        #[serde(default)]
        user_policy: Option<WorkerPermissionPolicy>,
        #[serde(default)]
        permission_policy: Option<WorkerPermissionPolicy>,
        #[serde(default)]
        resource_policy: Option<ResourceQuotaPolicy>,
    },
    Pause,
    Resume {
        #[serde(default)]
        base_revision: Option<String>,
    },
    Drain,
    Stop,
    Reconcile {
        #[serde(default)]
        base_revision: Option<String>,
    },
}

impl PoolCommand {
    fn confirmation_action(&self) -> Option<&'static str> {
        match self {
            Self::Configure {
                user_policy,
                permission_policy,
                resource_policy,
                ..
            } if user_policy.is_some()
                || permission_policy.is_some()
                || resource_policy.is_some() =>
            {
                Some("pool.configure_policy")
            }
            Self::Start {
                user_policy,
                permission_policy,
                resource_policy,
                ..
            } if user_policy.is_some()
                || permission_policy.is_some()
                || resource_policy.is_some() =>
            {
                Some("pool.start_policy")
            }
            Self::Stop => Some("pool.stop"),
            _ => None,
        }
    }
}

fn paired_permission_policies(
    user_policy: Option<WorkerPermissionPolicy>,
    permission_policy: Option<WorkerPermissionPolicy>,
) -> Result<Option<(WorkerPermissionPolicy, WorkerPermissionPolicy)>, CoordinationApiError> {
    match (user_policy, permission_policy) {
        (None, None) => Ok(None),
        (Some(user), Some(goal)) => Ok(Some((user, goal))),
        _ => Err(CoordinationApiError::invalid(
            "userPolicy and permissionPolicy must be supplied together",
        )),
    }
}

fn paired_permission_policies_legacy(
    user_policy: Option<WorkerPermissionPolicy>,
    permission_policy: Option<WorkerPermissionPolicy>,
) -> Result<Option<(WorkerPermissionPolicy, WorkerPermissionPolicy)>, ApiError> {
    match (user_policy, permission_policy) {
        (None, None) => Ok(None),
        (Some(user), Some(goal)) => Ok(Some((user, goal))),
        _ => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "user_policy and permission_policy must be supplied together",
        )),
    }
}

fn require_dangerous_confirmation<T>(
    body: &CommandEnvelope<T>,
    action: &str,
    target: &str,
) -> Result<ActionConfirmation, CoordinationApiError> {
    body.require_confirmation(action, target)
        .cloned()
        .map_err(|error| CoordinationApiError::confirmation(error.to_string(), action, target))
}

fn require_legacy_confirmation(
    confirmation: Option<&ActionConfirmation>,
    action: &str,
    target: &str,
) -> Result<ActionConfirmation, ApiError> {
    let confirmation = confirmation.ok_or_else(|| {
        ApiError::new(
            StatusCode::PRECONDITION_REQUIRED,
            format!("action {action} on {target} requires explicit confirmation"),
        )
    })?;
    confirmation
        .validate(action, target)
        .map_err(|error| ApiError::new(StatusCode::PRECONDITION_REQUIRED, error.to_string()))?;
    Ok(confirmation.clone())
}

fn record_privileged_action_audit(
    store: &SqliteCoordinationStore,
    goal_id: &str,
    confirmation: &ActionConfirmation,
    idempotency_key: &str,
    outcome: &Value,
    now: chrono::DateTime<Utc>,
) -> Result<(), CoordinationApiError> {
    let event = CoordinationEvent::from_typed_payload(
        goal_id,
        EventSeverity::Info,
        CoordinationActor::User {
            user_id: confirmation.confirmed_by.clone(),
        },
        format!("privileged:{idempotency_key}"),
        CoordinationEventPayload::PrivilegedAction(PrivilegedActionEventPayload {
            actor: confirmation.confirmed_by.clone(),
            authority: "explicit_user_confirmation".into(),
            action: confirmation.action.clone(),
            target: confirmation.target.clone(),
            decision: confirmation.reason.clone(),
            evidence_refs: vec![
                format!("idempotency:{idempotency_key}"),
                format!("outcome-version:{}", resource_version(outcome)),
            ],
            outcome: "succeeded".into(),
        }),
        now,
    )
    .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .append_event_once(&format!("privileged-audit:{idempotency_key}"), &event)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(())
}

async fn record_legacy_privileged_action_audit(
    state: &AppState,
    goal_id: &str,
    confirmation: &ActionConfirmation,
    audit_key: &str,
    outcome: &Value,
    now: chrono::DateTime<Utc>,
) -> Result<(), ApiError> {
    let root = state.tracker_root.read().await.clone();
    let store = SqliteCoordinationStore::open(root.join("coordination.sqlite"))
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    record_privileged_action_audit(&store, goal_id, confirmation, audit_key, outcome, now)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.problem.message))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum WorkerLifecycleCommand {
    Pause { reason: String },
    Resume,
    Cancel { reason: String },
    Recover { reason: String },
    Fail { reason: String },
}

impl WorkerLifecycleCommand {
    fn confirmation_action(&self) -> Option<&'static str> {
        matches!(self, Self::Cancel { .. }).then_some("worker.cancel")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClaimAcquireCommand {
    worker_id: String,
    #[serde(default)]
    requested_scope: Option<ClaimScope>,
    base_revision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum ClaimCommand {
    Expand {
        worker_id: String,
        requested_scope: ClaimScope,
        rationale: String,
        base_revision: String,
    },
    Heartbeat {
        worker_id: String,
        expected_generation: u64,
    },
    Release {
        worker_id: String,
        expected_generation: u64,
        reason: String,
        #[serde(default)]
        artifact_id: Option<String>,
        #[serde(default)]
        evidence_refs: Vec<String>,
    },
    Complete {
        worker_id: String,
        expected_generation: u64,
        reason: String,
        artifact_id: String,
        integration_revision: String,
        evidence_refs: Vec<String>,
        integration_boundary_satisfied: bool,
    },
    Block {
        worker_id: String,
        expected_generation: u64,
        reason: String,
        #[serde(default)]
        evidence_refs: Vec<String>,
        #[serde(default)]
        escalation_id: Option<String>,
    },
    Cancel {
        worker_id: String,
        expected_generation: u64,
        reason: String,
        #[serde(default)]
        evidence_refs: Vec<String>,
    },
    Reassign {
        replacement_worker_id: String,
        expected_generation: u64,
        base_revision: String,
        reason: String,
        decided_by: String,
    },
}

impl ClaimCommand {
    fn confirmation_action(&self) -> Option<&'static str> {
        match self {
            Self::Cancel { .. } => Some("claim.cancel"),
            Self::Reassign { .. } => Some("claim.reassign"),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractRegisterCommand {
    stable_key: String,
    title: String,
    kind: ContractKind,
    #[serde(default)]
    producer: Option<String>,
    compatibility_notes: String,
    actor: CoordinationActor,
    #[serde(default)]
    changed_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum ContractCommand {
    Revise {
        expected_revision: u64,
        changed_by: String,
        compatibility_notes: String,
        actor: CoordinationActor,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventPublishCommand {
    severity: EventSeverity,
    producer: CoordinationActor,
    correlation_id: String,
    #[serde(default)]
    causation_id: Option<String>,
    payload: CoordinationEventPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum NotificationCommand {
    Deliver {
        actor: CoordinationActor,
    },
    Acknowledge {
        worker_id: String,
    },
    ActedOn {
        worker_id: String,
        outcome: String,
    },
    Fail {
        actor: CoordinationActor,
        reason: String,
    },
    Replay {
        actor: CoordinationActor,
        recovery_kind: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum EscalationCommand {
    Acknowledge {
        actor: String,
        #[serde(default)]
        reason: Option<String>,
    },
    Resolve {
        actor: String,
        #[serde(default)]
        reason: Option<String>,
        decision: EscalationDecisionInput,
    },
    Override {
        actor: String,
        #[serde(default)]
        reason: Option<String>,
        decision: EscalationDecisionInput,
    },
    Expire {
        reason: String,
    },
    Cancel {
        actor: String,
        reason: String,
    },
}

impl EscalationCommand {
    fn confirmation_action(&self) -> Option<&'static str> {
        matches!(self, Self::Override { .. }).then_some("escalation.override")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EscalationDecisionInput {
    option_id: String,
    decided_by: String,
    #[serde(default)]
    accepted_risk: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum IntegrationArtifactCollectionCommand {
    Capture {
        claim_id: String,
        worker_id: String,
        #[serde(default)]
        changed_contracts: Vec<IntegrationContractChange>,
        #[serde(default)]
        migrations: Vec<IntegrationMigration>,
        #[serde(default)]
        validations: Vec<IntegrationValidationEvidence>,
        #[serde(default)]
        evidence_refs: Vec<String>,
        #[serde(default)]
        known_risks: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValidationGateInput {
    id: String,
    kind: ValidationGateKind,
    program: String,
    #[serde(default)]
    args: Vec<String>,
    required: bool,
    max_output_bytes: usize,
}

impl From<ValidationGateInput> for ValidationGateSpec {
    fn from(input: ValidationGateInput) -> Self {
        Self {
            id: input.id,
            kind: input.kind,
            program: input.program,
            args: input.args,
            required: input.required,
            max_output_bytes: input.max_output_bytes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegenerationCommandInput {
    program: String,
    #[serde(default)]
    args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum IntegrationArtifactCommand {
    Preflight,
    Validate {
        gates: Vec<ValidationGateInput>,
    },
    Reconcile {
        strategy: ReconciliationStrategy,
        #[serde(default)]
        target_revision: String,
        #[serde(default)]
        commit_revisions: Vec<String>,
        #[serde(default)]
        regeneration: Option<RegenerationCommandInput>,
        #[serde(default)]
        manual_instructions: Option<String>,
    },
    Integrate {
        #[serde(default)]
        priority: i32,
    },
    Retry {
        #[serde(default)]
        priority: Option<i32>,
    },
}

impl IntegrationArtifactCommand {
    fn confirmation_action(&self) -> Option<&'static str> {
        match self {
            Self::Validate { .. } => Some("integration.execute_validation"),
            Self::Reconcile { strategy, .. } if *strategy != ReconciliationStrategy::Manual => {
                Some("integration.reconcile")
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum IntegrationQueueCommand {
    Acquire { repository_id: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum IntegrationJobCommand {
    Finish {
        succeeded: bool,
        summary: String,
    },
    Finalize {
        validation_report_id: String,
        tracker_feature_id: String,
        tracker_step_ids: Vec<String>,
        tracker_summary: String,
        #[serde(default)]
        tracker_evidence: Vec<String>,
        tracker_status: Status,
        integration_revision: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
enum IntegrationFinalizationCommand {
    Rollback {
        requested_by: String,
        reason: String,
    },
    Cleanup,
}

impl IntegrationFinalizationCommand {
    fn confirmation_action(&self) -> &'static str {
        match self {
            Self::Rollback { .. } => "integration.rollback",
            Self::Cleanup => "integration.cleanup",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IntegrationJobQuery {
    repository_id: String,
    #[serde(default)]
    state: Option<IntegrationJobState>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct CoordinationEventStreamQuery {
    #[serde(default)]
    after_sequence: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

fn coordination_pool_error(error: WorkerPoolError) -> CoordinationApiError {
    let (status, code, retryable) = match error {
        WorkerPoolError::GoalNotConfigured(_) | WorkerPoolError::WorkerNotFound(_) => {
            (StatusCode::NOT_FOUND, ProblemCode::NotFound, false)
        }
        WorkerPoolError::InvalidConcurrency { .. }
        | WorkerPoolError::IsolationUnavailable(_)
        | WorkerPoolError::RecoveryIncomplete(_)
        | WorkerPoolError::ActiveRunExists(_)
        | WorkerPoolError::NoActiveRun(_)
        | WorkerPoolError::WorkspaceAlreadyBound(_)
        | WorkerPoolError::ResourceQuotaChangeDuringActiveRun(_) => {
            (StatusCode::CONFLICT, ProblemCode::Conflict, true)
        }
        WorkerPoolError::InvalidResourceQuotaPolicy(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            ProblemCode::InvalidRequest,
            false,
        ),
        WorkerPoolError::Domain(_) => (StatusCode::CONFLICT, ProblemCode::Conflict, false),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ProblemCode::Internal,
            true,
        ),
    };
    CoordinationApiError {
        status,
        problem: ApiProblem::new(code, error.to_string(), retryable),
    }
}

fn pool_document(
    goal_id: &str,
    snapshot: &crate::coordination::pool::PoolSnapshot,
    data: Value,
    now: chrono::DateTime<Utc>,
) -> ResourceDocument<Value> {
    ResourceDocument::new(
        "worker_pool",
        ResourceMetadata {
            id: goal_id.into(),
            resource_version: resource_version(snapshot),
            created_at: now,
            updated_at: now,
        },
        data,
    )
}

async fn get_coordination_pool(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
) -> Result<Json<ResourceDocument<Value>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let pool = WorkerPoolService::load(store, WorkerPoolPolicy::default())
        .map_err(coordination_pool_error)?;
    let snapshot = pool.snapshot(&goal_id).map_err(coordination_pool_error)?;
    Ok(Json(pool_document(
        &goal_id,
        &snapshot,
        json!(snapshot),
        Utc::now(),
    )))
}

async fn get_coordination_metrics(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
) -> Result<
    Json<ResourceDocument<crate::coordination::metrics::GoalMetricsSnapshot>>,
    CoordinationApiError,
> {
    let root = state.tracker_root.read().await.clone();
    let store = coordination_store(&state).await?;
    let collected_at = Utc::now();
    let snapshot = GoalMetricsService::new(Tracker::new(root), store)
        .collect(&goal_id, collected_at)
        .map_err(|error| match error {
            MetricsError::Tracker(error) if error.status == 404 => {
                CoordinationApiError::not_found(error.message)
            }
            MetricsError::Tracker(error) => CoordinationApiError::invalid(error.message),
            MetricsError::Store(error) => CoordinationApiError::internal(error.to_string()),
            MetricsError::Claims(error) => CoordinationApiError::internal(error.to_string()),
        })?;
    Ok(Json(ResourceDocument::new(
        "goal_metrics",
        ResourceMetadata {
            id: goal_id,
            resource_version: resource_version(&snapshot),
            created_at: collected_at,
            updated_at: collected_at,
        },
        snapshot,
    )))
}

async fn get_coordination_health(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
) -> Result<
    Json<ResourceDocument<crate::coordination::health::GoalHealthSnapshot>>,
    CoordinationApiError,
> {
    let root = state.tracker_root.read().await.clone();
    let store = coordination_store(&state).await?;
    let collected_at = Utc::now();
    let snapshot = GoalHealthService::new(Tracker::new(root), store, HealthPolicy::default())
        .collect(&goal_id, collected_at)
        .map_err(|error| match error {
            HealthError::Tracker(error) if error.status == 404 => {
                CoordinationApiError::not_found(error.message)
            }
            HealthError::Tracker(error) => CoordinationApiError::invalid(error.message),
            HealthError::Store(error) => CoordinationApiError::internal(error.to_string()),
        })?;
    Ok(Json(ResourceDocument::new(
        "goal_health",
        ResourceMetadata {
            id: goal_id,
            resource_version: resource_version(&snapshot),
            created_at: collected_at,
            updated_at: collected_at,
        },
        snapshot,
    )))
}

async fn get_coordination_alerts(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
) -> Result<Json<ResourceDocument<OperationalAlertSnapshot>>, CoordinationApiError> {
    let root = state.tracker_root.read().await.clone();
    let store = coordination_store(&state).await?;
    let evaluated_at = Utc::now();
    let snapshot = OperationalAlertService::new(Tracker::new(root), store, AlertPolicy::default())
        .evaluate_goal(&goal_id, evaluated_at)
        .map_err(|error| match error {
            AlertError::Tracker(error) if error.status == 404 => {
                CoordinationApiError::not_found(error.message)
            }
            AlertError::Tracker(error) => CoordinationApiError::invalid(error.message),
            AlertError::Store(error) => CoordinationApiError::internal(error.to_string()),
            AlertError::Claims(error) => CoordinationApiError::internal(error.to_string()),
            AlertError::Serialization(error) => CoordinationApiError::internal(error.to_string()),
            AlertError::Io(error) => CoordinationApiError::internal(error.to_string()),
            AlertError::Persist(error) => CoordinationApiError::internal(error.to_string()),
        })?;
    Ok(Json(ResourceDocument::new(
        "operational_alerts",
        ResourceMetadata {
            id: goal_id,
            resource_version: resource_version(&snapshot),
            created_at: evaluated_at,
            updated_at: evaluated_at,
        },
        snapshot,
    )))
}

async fn command_coordination_pool(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<CommandEnvelope<PoolCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let confirmation = if let Some(action) = body.command.confirmation_action() {
        Some(require_dangerous_confirmation(&body, action, &goal_id)?)
    } else {
        None
    };
    let store = coordination_store(&state).await?;
    let idempotency_key = format!("coordination.v1.pool:{goal_id}:{}", body.idempotency_key);
    if let Some(outcome) = store
        .idempotent_outcome(&idempotency_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        if let Some(confirmation) = &confirmation {
            record_privileged_action_audit(
                store.as_ref(),
                &goal_id,
                confirmation,
                &idempotency_key,
                &outcome,
                Utc::now(),
            )?;
        }
        return Ok(Json(outcome));
    }
    let pool = WorkerPoolService::load(store.clone(), WorkerPoolPolicy::default())
        .map_err(coordination_pool_error)?;
    let current = pool.snapshot(&goal_id).ok();
    if let Some(expected) = body.expected_resource_version.as_deref() {
        let actual = current.as_ref().map(resource_version);
        if actual.as_deref() != Some(expected) {
            return Err(CoordinationApiError::stale(format!(
                "worker pool revision changed; expected {expected}, current {}",
                actual.as_deref().unwrap_or("unconfigured")
            )));
        }
    }
    let root = state.tracker_root.read().await.clone();
    let claims = ClaimService::new(
        Tracker::new(root.clone()),
        store.clone(),
        ClaimPolicy::default(),
    );
    let workspace = state.runner.workspace_root().await;
    let now = Utc::now();
    let mut assigned = Vec::new();
    let mut recovered_phantom_claim_ids = Vec::new();
    let mut should_dispatch = false;
    let mut stop_report = None;
    match body.command {
        PoolCommand::Configure {
            desired_concurrency,
            base_revision: _,
            user_policy,
            permission_policy,
            resource_policy,
        } => {
            if let Some((user_policy, goal_policy)) =
                paired_permission_policies(user_policy, permission_policy)?
            {
                pool.configure_for_workspace_with_permission_policy(
                    &goal_id,
                    desired_concurrency,
                    &workspace,
                    user_policy,
                    goal_policy,
                    now,
                )
                .map_err(coordination_pool_error)?;
            } else {
                pool.configure_for_workspace(&goal_id, desired_concurrency, &workspace, now)
                    .map_err(coordination_pool_error)?;
            }
            if let Some(policy) = resource_policy {
                pool.set_resource_quotas(&goal_id, policy, now)
                    .map_err(coordination_pool_error)?;
            }
        }
        PoolCommand::Start {
            desired_concurrency,
            base_revision,
            user_policy,
            permission_policy,
            resource_policy,
        } => {
            recovered_phantom_claim_ids =
                recover_unstarted_claims(&Tracker::new(root.clone()), store.clone(), &goal_id, now)
                    .map_err(CoordinationApiError::internal)?;
            let desired_concurrency = desired_concurrency.or_else(|| {
                current
                    .as_ref()
                    .map(|snapshot| snapshot.goal.desired_concurrency)
            });
            if let Some((user_policy, goal_policy)) =
                paired_permission_policies(user_policy, permission_policy)?
            {
                pool.start_for_workspace_with_permission_policy(
                    &goal_id,
                    desired_concurrency,
                    0,
                    &workspace,
                    user_policy,
                    goal_policy,
                    now,
                )
                .map_err(coordination_pool_error)?;
            } else {
                pool.start_for_workspace(&goal_id, desired_concurrency, 0, &workspace, now)
                    .map_err(coordination_pool_error)?;
            }
            if let Some(policy) = resource_policy {
                pool.set_resource_quotas(&goal_id, policy, now)
                    .map_err(coordination_pool_error)?;
            }
            let base_revision = if claims
                .ready_unclaimed_scopes(&goal_id)
                .map_err(coordination_claim_error)?
                .is_empty()
            {
                base_revision.unwrap_or_else(|| "workspace-current".into())
            } else {
                ensure_goal_base_revision(
                    &Tracker::new(root.clone()),
                    &workspace,
                    &goal_id,
                    base_revision.as_deref(),
                    now,
                )
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?
            };
            assigned = pool
                .fill_ready_claims(&claims, &goal_id, &base_revision, now)
                .map_err(coordination_pool_error)?;
            should_dispatch = true;
        }
        PoolCommand::Pause => {
            pool.pause(&goal_id, now).map_err(coordination_pool_error)?;
        }
        PoolCommand::Resume { base_revision } => {
            recovered_phantom_claim_ids =
                recover_unstarted_claims(&Tracker::new(root.clone()), store.clone(), &goal_id, now)
                    .map_err(CoordinationApiError::internal)?;
            pool.resume(&goal_id, 0, now)
                .map_err(coordination_pool_error)?;
            let base_revision = if claims
                .ready_unclaimed_scopes(&goal_id)
                .map_err(coordination_claim_error)?
                .is_empty()
            {
                base_revision.unwrap_or_else(|| "workspace-current".into())
            } else {
                ensure_goal_base_revision(
                    &Tracker::new(root.clone()),
                    &workspace,
                    &goal_id,
                    base_revision.as_deref(),
                    now,
                )
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?
            };
            assigned = pool
                .fill_ready_claims(&claims, &goal_id, &base_revision, now)
                .map_err(coordination_pool_error)?;
            should_dispatch = true;
        }
        PoolCommand::Drain => {
            pool.drain(&goal_id, now).map_err(coordination_pool_error)?;
        }
        PoolCommand::Stop => {
            stop_report = Some(pool.stop(&goal_id, now).map_err(coordination_pool_error)?);
        }
        PoolCommand::Reconcile { base_revision } => {
            recovered_phantom_claim_ids =
                recover_unstarted_claims(&Tracker::new(root.clone()), store.clone(), &goal_id, now)
                    .map_err(CoordinationApiError::internal)?;
            let base_revision = if claims
                .ready_unclaimed_scopes(&goal_id)
                .map_err(coordination_claim_error)?
                .is_empty()
            {
                base_revision.unwrap_or_else(|| "workspace-current".into())
            } else {
                ensure_goal_base_revision(
                    &Tracker::new(root.clone()),
                    &workspace,
                    &goal_id,
                    base_revision.as_deref(),
                    now,
                )
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?
            };
            assigned = pool
                .fill_ready_claims(&claims, &goal_id, &base_revision, now)
                .map_err(coordination_pool_error)?;
            should_dispatch = true;
        }
    }
    let execution = if should_dispatch && !assigned.is_empty() {
        Some(
            dispatch_claims(
                state.runner.clone(),
                Tracker::new(root),
                store.clone(),
                workspace,
                assigned.clone(),
                recovered_phantom_claim_ids,
            )
            .await,
        )
    } else {
        None
    };
    let snapshot = pool.snapshot(&goal_id).map_err(coordination_pool_error)?;
    let document = pool_document(
        &goal_id,
        &snapshot,
        json!({
            "snapshot": snapshot,
            "assignedClaims": assigned,
            "execution": execution,
            "stopReport": stop_report,
        }),
        now,
    );
    let outcome = redacted_json_value(document)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(&idempotency_key, "coordination.pool.command", &outcome, now)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    if let Some(confirmation) = &confirmation {
        record_privileged_action_audit(
            store.as_ref(),
            &goal_id,
            confirmation,
            &idempotency_key,
            &outcome,
            now,
        )?;
    }
    Ok(Json(outcome))
}

fn worker_document(worker: Worker) -> ResourceDocument<Worker> {
    ResourceDocument::new(
        "worker",
        ResourceMetadata {
            id: worker.id.as_str().into(),
            resource_version: resource_version(&worker),
            created_at: worker.metadata.created_at,
            updated_at: worker.metadata.updated_at,
        },
        worker,
    )
}

fn worker_value_document(worker: &Worker, kind: &str, data: Value) -> ResourceDocument<Value> {
    ResourceDocument::new(
        kind,
        ResourceMetadata {
            id: worker.id.as_str().into(),
            resource_version: resource_version(&data),
            created_at: worker.metadata.created_at,
            updated_at: worker.metadata.updated_at,
        },
        data,
    )
}

fn page_bounds<T>(
    items: &[T],
    query: &PageQuery,
    id: impl Fn(&T) -> &str,
) -> Result<(usize, usize, Option<String>), CoordinationApiError> {
    query
        .validate_cursor()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let limit = query
        .validated_limit()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let start = match query.cursor.as_deref() {
        Some(cursor) => items
            .iter()
            .position(|item| id(item) == cursor)
            .map(|position| position + 1)
            .ok_or_else(|| {
                CoordinationApiError::invalid("cursor is not present in this collection")
            })?,
        None => 0,
    };
    let end = (start + limit).min(items.len());
    let next = (end < items.len() && end > start).then(|| id(&items[end - 1]).to_string());
    Ok((start, end, next))
}

async fn coordination_worker(
    state: &AppState,
    goal_id: &str,
    worker_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, Worker), CoordinationApiError> {
    let id = WorkerId::parse(worker_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let worker = store
        .worker(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .ok_or_else(|| CoordinationApiError::not_found("worker not found"))?;
    if worker.goal_id != goal_id {
        return Err(CoordinationApiError::not_found(
            "worker does not belong to this goal",
        ));
    }
    Ok((store, worker))
}

async fn list_coordination_workers(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<Worker>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let workers = store
        .workers_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&workers, &query, |worker| worker.id.as_str())?;
    Ok(Json(CollectionPage::new(
        workers[start..end]
            .iter()
            .cloned()
            .map(worker_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_worker(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<Worker>>, CoordinationApiError> {
    let (_, worker) = coordination_worker(&state, &goal_id, &worker_id).await?;
    Ok(Json(worker_document(worker)))
}

async fn command_coordination_worker(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<WorkerLifecycleCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let confirmation = if let Some(action) = body.command.confirmation_action() {
        Some(require_dangerous_confirmation(&body, action, &worker_id)?)
    } else {
        None
    };
    let (store, worker) = coordination_worker(&state, &goal_id, &worker_id).await?;
    let idempotency_key = format!(
        "coordination.v1.worker:{goal_id}:{worker_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&idempotency_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        if let Some(confirmation) = &confirmation {
            record_privileged_action_audit(
                store.as_ref(),
                &goal_id,
                confirmation,
                &idempotency_key,
                &outcome,
                Utc::now(),
            )?;
        }
        return Ok(Json(outcome));
    }
    if let Some(expected) = body.expected_resource_version.as_deref()
        && resource_version(&worker) != expected
    {
        return Err(CoordinationApiError::stale(format!(
            "worker revision changed; expected {expected}, current {}",
            resource_version(&worker)
        )));
    }
    let (next, reason) = match body.command {
        WorkerLifecycleCommand::Pause { reason } => (WorkerState::Paused, Some(reason)),
        WorkerLifecycleCommand::Resume => (WorkerState::Active, None),
        WorkerLifecycleCommand::Cancel { reason } => (WorkerState::Cancelled, Some(reason)),
        WorkerLifecycleCommand::Recover { reason } => (WorkerState::Recovering, Some(reason)),
        WorkerLifecycleCommand::Fail { reason } => (WorkerState::Failed, Some(reason)),
    };
    if reason
        .as_ref()
        .is_some_and(|reason| reason.trim().is_empty() || reason.chars().count() > 2_000)
    {
        return Err(CoordinationApiError::invalid(
            "worker lifecycle reason must be non-empty and at most 2,000 characters",
        ));
    }
    let pool = WorkerPoolService::load(store.clone(), WorkerPoolPolicy::default())
        .map_err(coordination_pool_error)?;
    let updated = pool
        .transition_worker(&worker.id, next, reason, Utc::now())
        .map_err(coordination_pool_error)?;
    let document = worker_document(updated);
    let outcome = redacted_json_value(document)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &idempotency_key,
            "coordination.worker.command",
            &outcome,
            Utc::now(),
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    if let Some(confirmation) = &confirmation {
        record_privileged_action_audit(
            store.as_ref(),
            &goal_id,
            confirmation,
            &idempotency_key,
            &outcome,
            Utc::now(),
        )?;
    }
    Ok(Json(outcome))
}

fn value_contains_worker(value: &Value, worker_id: &str) -> bool {
    match value {
        Value::String(value) => value == worker_id,
        Value::Array(values) => values
            .iter()
            .any(|value| value_contains_worker(value, worker_id)),
        Value::Object(values) => values
            .values()
            .any(|value| value_contains_worker(value, worker_id)),
        _ => false,
    }
}

async fn get_coordination_worker_activity(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<CoordinationEvent>>, CoordinationApiError> {
    let (store, worker) = coordination_worker(&state, &goal_id, &worker_id).await?;
    let events = store
        .latest_events_for_goal(&goal_id, None, 1_000)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .into_iter()
        .filter(|event| {
            matches!(&event.producer, crate::coordination::domain::CoordinationActor::Worker { worker_id } if worker_id == &worker.id)
                || value_contains_worker(&event.payload, worker.id.as_str())
        })
        .collect::<Vec<_>>();
    let (start, end, next) = page_bounds(&events, &query, |event| event.id.as_str())?;
    Ok(Json(CollectionPage::new(events[start..end].to_vec(), next)))
}

async fn get_coordination_worker_workspace(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<WorkspaceBinding>>, CoordinationApiError> {
    let (_, worker) = coordination_worker(&state, &goal_id, &worker_id).await?;
    let binding = worker
        .workspace
        .clone()
        .ok_or_else(|| CoordinationApiError::not_found("worker has no workspace binding"))?;
    Ok(Json(ResourceDocument::new(
        "worker_workspace",
        ResourceMetadata {
            id: worker.id.as_str().into(),
            resource_version: resource_version(&binding),
            created_at: worker.metadata.created_at,
            updated_at: worker.metadata.updated_at,
        },
        binding,
    )))
}

async fn get_coordination_worker_run(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<Value>>, CoordinationApiError> {
    let (_, worker) = coordination_worker(&state, &goal_id, &worker_id).await?;
    let data = json!({
        "currentRunId": worker.current_run_id,
        "turns": worker.turn_history.iter().map(|turn| json!({
            "sequence": turn.sequence,
            "runId": turn.run_id,
            "startedAt": turn.started_at,
            "completedAt": turn.completed_at,
        })).collect::<Vec<_>>(),
    });
    Ok(Json(worker_value_document(&worker, "worker_run", data)))
}

async fn get_coordination_worker_thread(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<Value>>, CoordinationApiError> {
    let (_, worker) = coordination_worker(&state, &goal_id, &worker_id).await?;
    let data = json!({
        "currentThreadId": worker.current_thread_id,
        "turns": worker.turn_history.iter().map(|turn| json!({
            "sequence": turn.sequence,
            "threadId": turn.thread_id,
            "continuationOfThreadId": turn.continuation_of_thread_id,
            "contextTransferArtifactId": turn.context_transfer_artifact_id,
        })).collect::<Vec<_>>(),
    });
    Ok(Json(worker_value_document(&worker, "worker_thread", data)))
}

fn coordination_claim_error(error: ClaimServiceError) -> CoordinationApiError {
    let (status, code, retryable) = match &error {
        ClaimServiceError::WorkerNotFound(_) | ClaimServiceError::ClaimNotFound(_) => {
            (StatusCode::NOT_FOUND, ProblemCode::NotFound, false)
        }
        ClaimServiceError::WorkerGoalMismatch { .. } | ClaimServiceError::NotOwner { .. } => {
            (StatusCode::FORBIDDEN, ProblemCode::Forbidden, false)
        }
        ClaimServiceError::StaleClaim => (StatusCode::CONFLICT, ProblemCode::StaleRevision, true),
        ClaimServiceError::WorkerUnavailable { .. }
        | ClaimServiceError::NoReadyWork(_)
        | ClaimServiceError::ScopeNotReady
        | ClaimServiceError::IntegrationBoundaryNotSatisfied => {
            (StatusCode::CONFLICT, ProblemCode::Conflict, true)
        }
        ClaimServiceError::CompletionEvidenceRequired
        | ClaimServiceError::InvalidIdempotentOutcome => (
            StatusCode::UNPROCESSABLE_ENTITY,
            ProblemCode::InvalidRequest,
            false,
        ),
        ClaimServiceError::Store(StoreError::ActiveClaimExists { .. }) => {
            (StatusCode::CONFLICT, ProblemCode::Conflict, true)
        }
        ClaimServiceError::Tracker(error) => (
            StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            if error.status == 404 {
                ProblemCode::NotFound
            } else {
                ProblemCode::InvalidRequest
            },
            false,
        ),
        ClaimServiceError::RecoveryInventory(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ProblemCode::Internal,
            true,
        ),
        ClaimServiceError::Domain(_) => (StatusCode::CONFLICT, ProblemCode::Conflict, false),
        ClaimServiceError::Store(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ProblemCode::Internal,
            true,
        ),
    };
    CoordinationApiError {
        status,
        problem: ApiProblem::new(code, error.to_string(), retryable),
    }
}

fn required_coordination_text(
    value: &str,
    field: &str,
    max: usize,
) -> Result<(), CoordinationApiError> {
    if value.trim().is_empty() || value.chars().count() > max {
        return Err(CoordinationApiError::invalid(format!(
            "{field} must be non-empty and at most {max} characters"
        )));
    }
    Ok(())
}

fn validate_evidence_refs(values: &[String]) -> Result<(), CoordinationApiError> {
    if values.len() > 1_000
        || values
            .iter()
            .any(|value| value.trim().is_empty() || value.chars().count() > 2_000)
    {
        return Err(CoordinationApiError::invalid(
            "evidenceRefs must contain at most 1,000 non-empty bounded values",
        ));
    }
    Ok(())
}

fn claim_document(claim: Claim) -> ResourceDocument<Claim> {
    ResourceDocument::new(
        "claim",
        ResourceMetadata {
            id: claim.id.as_str().into(),
            resource_version: resource_version(&claim),
            created_at: claim.metadata.created_at,
            updated_at: claim.metadata.updated_at,
        },
        claim,
    )
}

fn package_document(package: WorkPackage) -> ResourceDocument<WorkPackage> {
    ResourceDocument::new(
        "work_package",
        ResourceMetadata {
            id: package.id.as_str().into(),
            resource_version: resource_version(&package),
            created_at: package.metadata.created_at,
            updated_at: package.metadata.updated_at,
        },
        package,
    )
}

fn scope_id(scope: &ClaimScope) -> &str {
    match scope {
        ClaimScope::Feature { feature_id } => feature_id,
        ClaimScope::WorkPackage { work_package_id } => work_package_id.as_str(),
    }
}

async fn list_coordination_ready_work(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<ClaimScope>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let root = state.tracker_root.read().await.clone();
    let service = ClaimService::new(Tracker::new(root), store, ClaimPolicy::default());
    let scopes = service
        .ready_unclaimed_scopes(&goal_id)
        .map_err(coordination_claim_error)?;
    let (start, end, next) = page_bounds(&scopes, &query, scope_id)?;
    let now = Utc::now();
    let items = scopes[start..end]
        .iter()
        .cloned()
        .map(|scope| {
            ResourceDocument::new(
                "ready_work",
                ResourceMetadata {
                    id: scope_id(&scope).into(),
                    resource_version: resource_version(&scope),
                    created_at: now,
                    updated_at: now,
                },
                scope,
            )
        })
        .collect();
    Ok(Json(CollectionPage::new(items, next)))
}

async fn list_coordination_partial_work(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<PartialWorkArtifact>>>, CoordinationApiError> {
    let root = state.tracker_root.read().await.clone();
    let artifacts = partial_work_for_goal(&Tracker::new(root), &goal_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .into_iter()
        .map(|artifact| {
            ResourceDocument::new(
                "partial_work",
                ResourceMetadata {
                    id: artifact.id.clone(),
                    resource_version: resource_version(&artifact),
                    created_at: artifact.created_at,
                    updated_at: artifact.updated_at,
                },
                artifact,
            )
        })
        .collect::<Vec<_>>();
    let (start, end, next) =
        page_bounds(&artifacts, &query, |artifact| artifact.metadata.id.as_str())?;
    Ok(Json(CollectionPage::new(
        artifacts[start..end].to_vec(),
        next,
    )))
}

async fn get_coordination_goal_delivery(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
) -> Result<Json<ResourceDocument<GoalDeliveryState>>, CoordinationApiError> {
    let root = state.tracker_root.read().await.clone();
    let delivery = read_goal_delivery_state(&Tracker::new(root), &goal_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .ok_or_else(|| CoordinationApiError::not_found("goal delivery has not started"))?;
    Ok(Json(ResourceDocument::new(
        "goal_delivery",
        ResourceMetadata {
            id: goal_id,
            resource_version: resource_version(&delivery),
            created_at: delivery.updated_at,
            updated_at: delivery.updated_at,
        },
        delivery,
    )))
}

async fn list_coordination_packages(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<WorkPackage>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let packages = store
        .work_packages_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&packages, &query, |package| package.id.as_str())?;
    Ok(Json(CollectionPage::new(
        packages[start..end]
            .iter()
            .cloned()
            .map(package_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_package(
    State(state): State<AppState>,
    AxumPath((goal_id, package_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<WorkPackage>>, CoordinationApiError> {
    let id = WorkPackageId::parse(package_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let package = store
        .work_package(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|package| package.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("work package not found"))?;
    Ok(Json(package_document(package)))
}

async fn list_coordination_claims(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<Claim>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let claims = store
        .claims_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&claims, &query, |claim| claim.id.as_str())?;
    Ok(Json(CollectionPage::new(
        claims[start..end]
            .iter()
            .cloned()
            .map(claim_document)
            .collect(),
        next,
    )))
}

async fn coordination_claim(
    state: &AppState,
    goal_id: &str,
    claim_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, Claim), CoordinationApiError> {
    let id = ClaimId::parse(claim_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let claim = store
        .claim(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|claim| claim.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("claim not found"))?;
    Ok((store, claim))
}

async fn get_coordination_claim(
    State(state): State<AppState>,
    AxumPath((goal_id, claim_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<Claim>>, CoordinationApiError> {
    let (_, claim) = coordination_claim(&state, &goal_id, &claim_id).await?;
    Ok(Json(claim_document(claim)))
}

async fn get_coordination_claim_trace(
    State(state): State<AppState>,
    AxumPath((goal_id, claim_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<ClaimTrace>>, CoordinationApiError> {
    let claim_id = ClaimId::parse(claim_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let collected_at = Utc::now();
    let trace = ClaimTraceService::new(store)
        .collect(&goal_id, &claim_id, collected_at)
        .map_err(|error| match error {
            TraceError::ClaimNotFound(_) | TraceError::GoalMismatch { .. } => {
                CoordinationApiError::not_found(error.to_string())
            }
            TraceError::WorkerNotFound(_) | TraceError::Store(_) | TraceError::Domain(_) => {
                CoordinationApiError::internal(error.to_string())
            }
        })?;
    Ok(Json(ResourceDocument::new(
        "claim_trace",
        ResourceMetadata {
            id: trace.trace_id.clone(),
            resource_version: resource_version(&trace),
            created_at: trace.started_at,
            updated_at: trace.collected_at,
        },
        trace,
    )))
}

async fn acquire_coordination_claim(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<CommandEnvelope<ClaimAcquireCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    required_coordination_text(&body.command.base_revision, "baseRevision", 500)?;
    let worker_id = WorkerId::parse(&body.command.worker_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let api_key = format!(
        "coordination.v1.claim:{goal_id}:acquire:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    let root = state.tracker_root.read().await.clone();
    let service = ClaimService::new(Tracker::new(root), store.clone(), ClaimPolicy::default());
    let claim = service
        .claim_ready_unit(
            &goal_id,
            &worker_id,
            body.command.requested_scope,
            &body.command.base_revision,
            &format!("{api_key}:service"),
            Utc::now(),
        )
        .map_err(coordination_claim_error)?;
    let outcome = redacted_json_value(claim_document(claim))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(&api_key, "coordination.claim.acquire", &outcome, Utc::now())
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn command_coordination_claim(
    State(state): State<AppState>,
    AxumPath((goal_id, claim_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<ClaimCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let confirmation = if let Some(action) = body.command.confirmation_action() {
        Some(require_dangerous_confirmation(&body, action, &claim_id)?)
    } else {
        None
    };
    let (store, current) = coordination_claim(&state, &goal_id, &claim_id).await?;
    let api_key = format!(
        "coordination.v1.claim:{goal_id}:{claim_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        if let Some(confirmation) = &confirmation {
            record_privileged_action_audit(
                store.as_ref(),
                &goal_id,
                confirmation,
                &api_key,
                &outcome,
                Utc::now(),
            )?;
        }
        return Ok(Json(outcome));
    }
    if let Some(expected) = body.expected_resource_version.as_deref()
        && resource_version(&current) != expected
    {
        return Err(CoordinationApiError::stale(format!(
            "claim revision changed; expected {expected}, current {}",
            resource_version(&current)
        )));
    }
    let root = state.tracker_root.read().await.clone();
    let service = ClaimService::new(Tracker::new(root), store.clone(), ClaimPolicy::default());
    let service_key = format!("{api_key}:service");
    let now = Utc::now();
    let claim = match body.command {
        ClaimCommand::Expand {
            worker_id,
            requested_scope,
            rationale,
            base_revision,
        } => {
            required_coordination_text(&rationale, "rationale", 2_000)?;
            required_coordination_text(&base_revision, "baseRevision", 500)?;
            service.expand_claim(
                &current.id,
                &WorkerId::parse(worker_id)
                    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                requested_scope,
                &rationale,
                &base_revision,
                &service_key,
                now,
            )
        }
        ClaimCommand::Heartbeat {
            worker_id,
            expected_generation,
        } => service.heartbeat(
            &current.id,
            &WorkerId::parse(worker_id)
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
            expected_generation,
            now,
        ),
        ClaimCommand::Release {
            worker_id,
            expected_generation,
            reason,
            artifact_id,
            evidence_refs,
        } => {
            required_coordination_text(&reason, "reason", 2_000)?;
            validate_evidence_refs(&evidence_refs)?;
            service.release_claim(
                &current.id,
                &WorkerId::parse(worker_id)
                    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                expected_generation,
                ReleaseEvidence {
                    reason,
                    artifact_id,
                    evidence_refs,
                },
                &service_key,
                now,
            )
        }
        ClaimCommand::Complete {
            worker_id,
            expected_generation,
            reason,
            artifact_id,
            integration_revision,
            evidence_refs,
            integration_boundary_satisfied,
        } => {
            required_coordination_text(&reason, "reason", 2_000)?;
            validate_evidence_refs(&evidence_refs)?;
            service.complete_claim(
                &current.id,
                &WorkerId::parse(worker_id)
                    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                expected_generation,
                CompletionEvidence {
                    reason,
                    artifact_id,
                    integration_revision,
                    evidence_refs,
                    integration_boundary_satisfied,
                },
                &service_key,
                now,
            )
        }
        ClaimCommand::Block {
            worker_id,
            expected_generation,
            reason,
            evidence_refs,
            escalation_id,
        } => {
            required_coordination_text(&reason, "reason", 2_000)?;
            validate_evidence_refs(&evidence_refs)?;
            service.block_claim(
                &current.id,
                &WorkerId::parse(worker_id)
                    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                expected_generation,
                BlockEvidence {
                    reason,
                    evidence_refs,
                    escalation_id: escalation_id
                        .map(EscalationId::parse)
                        .transpose()
                        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                },
                &service_key,
                now,
            )
        }
        ClaimCommand::Cancel {
            worker_id,
            expected_generation,
            reason,
            evidence_refs,
        } => {
            required_coordination_text(&reason, "reason", 2_000)?;
            validate_evidence_refs(&evidence_refs)?;
            service.cancel_claim(
                &current.id,
                &WorkerId::parse(worker_id)
                    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                expected_generation,
                CancelEvidence {
                    reason,
                    evidence_refs,
                },
                &service_key,
                now,
            )
        }
        ClaimCommand::Reassign {
            replacement_worker_id,
            expected_generation,
            base_revision,
            reason,
            decided_by,
        } => {
            required_coordination_text(&base_revision, "baseRevision", 500)?;
            required_coordination_text(&reason, "reason", 2_000)?;
            required_coordination_text(&decided_by, "decidedBy", 500)?;
            service.manually_reassign_claim(
                &current.id,
                &WorkerId::parse(replacement_worker_id)
                    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
                expected_generation,
                &base_revision,
                ManualAssignmentEvidence { reason, decided_by },
                &service_key,
                now,
            )
        }
    }
    .map_err(coordination_claim_error)?;
    let outcome = redacted_json_value(claim_document(claim))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(&api_key, "coordination.claim.command", &outcome, now)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    if let Some(confirmation) = &confirmation {
        record_privileged_action_audit(
            store.as_ref(),
            &goal_id,
            confirmation,
            &api_key,
            &outcome,
            now,
        )?;
    }
    Ok(Json(outcome))
}

fn contract_registry_error(error: ContractRegistryError) -> CoordinationApiError {
    let (status, code, retryable) = match error {
        ContractRegistryError::NotFound(_)
        | ContractRegistryError::WorkPackageNotFound(_)
        | ContractRegistryError::ClaimNotFound(_) => {
            (StatusCode::NOT_FOUND, ProblemCode::NotFound, false)
        }
        ContractRegistryError::RevisionMismatch { .. }
        | ContractRegistryError::StaleDeclaration => {
            (StatusCode::CONFLICT, ProblemCode::StaleRevision, true)
        }
        ContractRegistryError::ClaimNotOwned | ContractRegistryError::ActorMismatch => {
            (StatusCode::FORBIDDEN, ProblemCode::Forbidden, false)
        }
        ContractRegistryError::InvalidRegistration(_)
        | ContractRegistryError::InvalidDependency(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            ProblemCode::InvalidRequest,
            false,
        ),
        ContractRegistryError::Domain(_) => (StatusCode::CONFLICT, ProblemCode::Conflict, false),
        ContractRegistryError::Store(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ProblemCode::Internal,
            true,
        ),
    };
    CoordinationApiError {
        status,
        problem: ApiProblem::new(code, error.to_string(), retryable),
    }
}

fn notification_lifecycle_error(error: NotificationLifecycleError) -> CoordinationApiError {
    let (status, code, retryable) = match error {
        NotificationLifecycleError::NotFound(_) => {
            (StatusCode::NOT_FOUND, ProblemCode::NotFound, false)
        }
        NotificationLifecycleError::GoalMismatch | NotificationLifecycleError::WrongWorker => {
            (StatusCode::FORBIDDEN, ProblemCode::Forbidden, false)
        }
        NotificationLifecycleError::StaleRevision
        | NotificationLifecycleError::NonMonotonicTimestamp => {
            (StatusCode::CONFLICT, ProblemCode::StaleRevision, true)
        }
        NotificationLifecycleError::AcknowledgementRequired
        | NotificationLifecycleError::NotDueForExpiry
        | NotificationLifecycleError::InvalidReplacement
        | NotificationLifecycleError::ExpiredBeforeDelivery => {
            (StatusCode::CONFLICT, ProblemCode::Conflict, false)
        }
        NotificationLifecycleError::InvalidAttribution(_)
        | NotificationLifecycleError::Domain(_) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            ProblemCode::InvalidRequest,
            false,
        ),
        NotificationLifecycleError::Store(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ProblemCode::Internal,
            true,
        ),
    };
    CoordinationApiError {
        status,
        problem: ApiProblem::new(code, error.to_string(), retryable),
    }
}

fn contract_document(contract: SharedContract) -> ResourceDocument<SharedContract> {
    ResourceDocument::new(
        "shared_contract",
        ResourceMetadata {
            id: contract.id.as_str().into(),
            resource_version: resource_version(&contract),
            created_at: contract.metadata.created_at,
            updated_at: contract.metadata.updated_at,
        },
        contract,
    )
}

fn event_document(event: CoordinationEvent) -> ResourceDocument<CoordinationEvent> {
    ResourceDocument::new(
        "coordination_event",
        ResourceMetadata {
            id: event.id.as_str().into(),
            resource_version: resource_version(&event),
            created_at: event.metadata.created_at,
            updated_at: event.metadata.updated_at,
        },
        event,
    )
}

fn signal_document(signal: CoordinationSignal) -> ResourceDocument<CoordinationSignal> {
    ResourceDocument::new(
        "coordination_signal",
        ResourceMetadata {
            id: signal.id.as_str().into(),
            resource_version: resource_version(&signal),
            created_at: signal.metadata.created_at,
            updated_at: signal.metadata.updated_at,
        },
        signal,
    )
}

fn intervention_document(
    intervention: SupervisorIntervention,
) -> ResourceDocument<SupervisorIntervention> {
    ResourceDocument::new(
        "supervisor_intervention",
        ResourceMetadata {
            id: intervention.id.as_str().into(),
            resource_version: resource_version(&intervention),
            created_at: intervention.metadata.created_at,
            updated_at: intervention.metadata.updated_at,
        },
        intervention,
    )
}

fn notification_document(notification: WorkerNotification) -> ResourceDocument<WorkerNotification> {
    ResourceDocument::new(
        "worker_notification",
        ResourceMetadata {
            id: notification.id.as_str().into(),
            resource_version: resource_version(&notification),
            created_at: notification.metadata.created_at,
            updated_at: notification.metadata.updated_at,
        },
        notification,
    )
}

async fn list_coordination_contracts(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<SharedContract>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let contracts = store
        .contracts_for_goal(&goal_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&contracts, &query, |contract| contract.id.as_str())?;
    Ok(Json(CollectionPage::new(
        contracts[start..end]
            .iter()
            .cloned()
            .map(contract_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_contract(
    State(state): State<AppState>,
    AxumPath((goal_id, contract_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<SharedContract>>, CoordinationApiError> {
    let id = ContractId::parse(contract_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let contract = store
        .contract(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|contract| contract.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("shared contract not found"))?;
    Ok(Json(contract_document(contract)))
}

async fn register_coordination_contract(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<CommandEnvelope<ContractRegisterCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let api_key = format!(
        "coordination.v1.contract:{goal_id}:register:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    if store
        .contract_by_stable_key(&goal_id, &body.command.stable_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .is_some()
    {
        return Err(CoordinationApiError::conflict(
            "a contract with this stable key already exists",
        ));
    }
    let producer = body
        .command
        .producer
        .map(WorkPackageId::parse)
        .transpose()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let changed_by = body
        .command
        .changed_by
        .map(WorkerId::parse)
        .transpose()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let contract = ContractRegistry::new(store.clone())
        .register(
            ContractRegistration {
                goal_id,
                stable_key: body.command.stable_key,
                title: body.command.title,
                kind: body.command.kind,
                producer,
                compatibility_notes: body.command.compatibility_notes,
            },
            body.command.actor,
            changed_by,
            Utc::now(),
        )
        .map_err(contract_registry_error)?;
    let outcome = redacted_json_value(contract_document(contract))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.contract.register",
            &outcome,
            Utc::now(),
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn command_coordination_contract(
    State(state): State<AppState>,
    AxumPath((goal_id, contract_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<ContractCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let id = ContractId::parse(&contract_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let api_key = format!(
        "coordination.v1.contract:{goal_id}:{contract_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    let current = store
        .contract(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|contract| contract.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("shared contract not found"))?;
    if let Some(expected) = body.expected_resource_version.as_deref()
        && resource_version(&current) != expected
    {
        return Err(CoordinationApiError::stale("contract revision changed"));
    }
    let contract = match body.command {
        ContractCommand::Revise {
            expected_revision,
            changed_by,
            compatibility_notes,
            actor,
        } => ContractRegistry::new(store.clone()).revise(
            &id,
            expected_revision,
            WorkerId::parse(changed_by)
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
            compatibility_notes,
            actor,
            Utc::now(),
        ),
    }
    .map_err(contract_registry_error)?;
    let outcome = redacted_json_value(contract_document(contract))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.contract.command",
            &outcome,
            Utc::now(),
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn list_coordination_events(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<CoordinationEvent>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let events = store
        .latest_events_for_goal(&goal_id, None, 1_000)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&events, &query, |event| event.id.as_str())?;
    Ok(Json(CollectionPage::new(
        events[start..end]
            .iter()
            .cloned()
            .map(event_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_event(
    State(state): State<AppState>,
    AxumPath((goal_id, event_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<CoordinationEvent>>, CoordinationApiError> {
    let id = EventId::parse(event_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let event = store
        .event(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|event| event.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("coordination event not found"))?;
    Ok(Json(event_document(event)))
}

fn coordination_event_stream_cursor(
    headers: &HeaderMap,
    query: &CoordinationEventStreamQuery,
) -> Result<u64, CoordinationApiError> {
    let header_cursor = headers
        .get("last-event-id")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| CoordinationApiError::invalid("Last-Event-ID must be a sequence"))?
                .parse::<u64>()
                .map_err(|_| CoordinationApiError::invalid("Last-Event-ID must be a sequence"))
        })
        .transpose()?;
    Ok(query
        .after_sequence
        .unwrap_or_default()
        .max(header_cursor.unwrap_or_default()))
}

fn coordination_event_stream_batch_limit(
    query: &CoordinationEventStreamQuery,
) -> Result<usize, CoordinationApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_COORDINATION_SSE_BATCH);
    if !(1..=MAX_COORDINATION_SSE_BATCH).contains(&limit) {
        return Err(CoordinationApiError::invalid(format!(
            "event stream limit must be between 1 and {MAX_COORDINATION_SSE_BATCH}"
        )));
    }
    Ok(limit)
}

fn bounded_coordination_event_stream_value(
    event: &CoordinationEvent,
) -> Result<Value, serde_json::Error> {
    let document = redacted_json_value(event_document(event.clone()))?;
    let encoded = serde_json::to_vec(&document)?;
    if encoded.len() <= MAX_COORDINATION_SSE_DATA_BYTES {
        return Ok(document);
    }
    Ok(json!({
        "apiVersion": "v1",
        "kind": "coordination_event_reference",
        "metadata": {
            "id": event.id.as_str(),
            "resourceVersion": resource_version(event),
            "createdAt": event.metadata.created_at,
            "updatedAt": event.metadata.updated_at
        },
        "data": {
            "eventId": event.id.as_str(),
            "sequence": event.sequence,
            "eventKind": event.kind,
            "severity": event.severity,
            "occurredAt": event.occurred_at,
            "payloadOmitted": true,
            "originalBytes": encoded.len()
        }
    }))
}

async fn stream_coordination_events(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<CoordinationEventStreamQuery>,
    headers: HeaderMap,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, CoordinationApiError>
{
    required_coordination_text(&goal_id, "goalId", 200)?;
    let store = coordination_store(&state).await?;
    let mut after_sequence = coordination_event_stream_cursor(&headers, &query)?;
    let batch_limit = coordination_event_stream_batch_limit(&query)?;
    let (sender, receiver) = tokio::sync::mpsc::channel(batch_limit.min(64));
    tokio::spawn(async move {
        loop {
            if sender.is_closed() {
                break;
            }
            let events = match store.events_for_goal(&goal_id, after_sequence, None, batch_limit) {
                Ok(events) => events,
                Err(error) => {
                    let message = error.to_string().chars().take(2_000).collect::<String>();
                    let problem = ApiProblem::new(ProblemCode::Internal, message, true);
                    if let Ok(event) = Event::default()
                        .event("coordination-error")
                        .retry(std::time::Duration::from_secs(1))
                        .json_data(problem)
                        && sender.send(Ok(event)).await.is_err()
                    {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };
            let caught_up = events.len() < batch_limit;
            for durable_event in events {
                let Some(sequence) = durable_event.sequence else {
                    continue;
                };
                let payload = match bounded_coordination_event_stream_value(&durable_event) {
                    Ok(payload) => payload,
                    Err(_) => continue,
                };
                let event = match Event::default()
                    .id(sequence.to_string())
                    .event("coordination-event")
                    .retry(std::time::Duration::from_secs(1))
                    .json_data(payload)
                {
                    Ok(event) => event,
                    Err(_) => continue,
                };
                if sender.send(Ok(event)).await.is_err() {
                    return;
                }
                after_sequence = sequence;
            }
            if caught_up {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    });
    Ok(Sse::new(ReceiverStream::new(receiver)).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("coordination-keep-alive"),
    ))
}

fn authorize_coordination_event_publication(
    store: &SqliteCoordinationStore,
    goal_id: &str,
    producer: &CoordinationActor,
    payload: &CoordinationEventPayload,
) -> Result<(), CoordinationApiError> {
    let CoordinationActor::Worker { worker_id } = producer else {
        if let CoordinationActor::User { user_id } = producer {
            required_coordination_text(user_id, "producer.userId", 200)?;
        }
        return Ok(());
    };
    let worker = store
        .worker(worker_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|worker| worker.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError {
            status: StatusCode::FORBIDDEN,
            problem: ApiProblem::new(
                ProblemCode::Forbidden,
                "worker event producer does not belong to this goal",
                false,
            ),
        })?;
    let (payload_worker, claim_id) = match payload {
        CoordinationEventPayload::Activity(payload) => (&payload.worker_id, &payload.claim_id),
        CoordinationEventPayload::CoordinationRequest(payload) => {
            (&payload.requester_worker_id, &payload.claim_id)
        }
        CoordinationEventPayload::Blocker(payload) => (&payload.worker_id, &payload.claim_id),
        _ => {
            return Err(CoordinationApiError {
                status: StatusCode::FORBIDDEN,
                problem: ApiProblem::new(
                    ProblemCode::Forbidden,
                    "workers may publish only activity, coordination-request, and blocker events",
                    false,
                ),
            });
        }
    };
    let claim = store
        .claim(claim_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|claim| claim.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError {
            status: StatusCode::FORBIDDEN,
            problem: ApiProblem::new(
                ProblemCode::Forbidden,
                "worker event claim does not belong to this goal",
                false,
            ),
        })?;
    if payload_worker != worker_id
        || claim.owner != worker.id
        || claim.state != ClaimState::Active
        || !worker.active_claims.contains(&claim.id)
    {
        return Err(CoordinationApiError {
            status: StatusCode::FORBIDDEN,
            problem: ApiProblem::new(
                ProblemCode::Forbidden,
                "worker event requires the producer's active owned claim",
                false,
            ),
        });
    }
    Ok(())
}

async fn publish_coordination_event(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<CommandEnvelope<EventPublishCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    required_coordination_text(&body.command.correlation_id, "correlationId", 500)?;
    let store = coordination_store(&state).await?;
    let api_key = format!(
        "coordination.v1.event:{goal_id}:publish:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    authorize_coordination_event_publication(
        &store,
        &goal_id,
        &body.command.producer,
        &body.command.payload,
    )?;
    let mut event = CoordinationEvent::from_typed_payload(
        &goal_id,
        body.command.severity,
        body.command.producer,
        body.command.correlation_id,
        body.command.payload,
        Utc::now(),
    )
    .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    event.causation_id = body
        .command
        .causation_id
        .map(EventId::parse)
        .transpose()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    store
        .append_event_once(&api_key, &event)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let stored = store
        .event(&event.id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .ok_or_else(|| CoordinationApiError::internal("published event could not be reloaded"))?;
    let outcome = redacted_json_value(event_document(stored))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(&api_key, "coordination.event.publish", &outcome, Utc::now())
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn list_coordination_signals(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<CoordinationSignal>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let signals = store
        .signals_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&signals, &query, |signal| signal.id.as_str())?;
    Ok(Json(CollectionPage::new(
        signals[start..end]
            .iter()
            .cloned()
            .map(signal_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_signal(
    State(state): State<AppState>,
    AxumPath((goal_id, signal_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<CoordinationSignal>>, CoordinationApiError> {
    let id = SignalId::parse(signal_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let signal = store
        .signal(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|signal| signal.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("coordination signal not found"))?;
    Ok(Json(signal_document(signal)))
}

async fn list_coordination_interventions(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<SupervisorIntervention>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let interventions = store
        .interventions_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&interventions, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        interventions[start..end]
            .iter()
            .cloned()
            .map(intervention_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_intervention(
    State(state): State<AppState>,
    AxumPath((goal_id, intervention_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<SupervisorIntervention>>, CoordinationApiError> {
    let id = InterventionId::parse(intervention_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(&state).await?;
    let intervention = store
        .interventions_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .into_iter()
        .find(|item| item.id == id)
        .ok_or_else(|| CoordinationApiError::not_found("Supervisor intervention not found"))?;
    Ok(Json(intervention_document(intervention)))
}

async fn goal_notifications(
    store: &SqliteCoordinationStore,
    goal_id: &str,
) -> Result<Vec<WorkerNotification>, CoordinationApiError> {
    let workers = store
        .workers_for_goal(goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let mut notifications = Vec::new();
    for worker in workers {
        notifications.extend(
            store
                .notifications_for_worker(&worker.id, None)
                .map_err(|error| CoordinationApiError::internal(error.to_string()))?
                .into_iter()
                .filter(|notification| notification.goal_id == goal_id),
        );
    }
    notifications.sort_by(|left, right| {
        left.metadata
            .created_at
            .cmp(&right.metadata.created_at)
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });
    notifications.dedup_by(|left, right| left.id == right.id);
    Ok(notifications)
}

async fn list_coordination_notifications(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<WorkerNotification>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let notifications = goal_notifications(store.as_ref(), &goal_id).await?;
    let (start, end, next) = page_bounds(&notifications, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        notifications[start..end]
            .iter()
            .cloned()
            .map(notification_document)
            .collect(),
        next,
    )))
}

async fn coordination_notification(
    state: &AppState,
    goal_id: &str,
    notification_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, WorkerNotification), CoordinationApiError> {
    let id = NotificationId::parse(notification_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let notification = store
        .notification(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|notification| notification.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("worker notification not found"))?;
    Ok((store, notification))
}

async fn get_coordination_notification(
    State(state): State<AppState>,
    AxumPath((goal_id, notification_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<WorkerNotification>>, CoordinationApiError> {
    let (_, notification) = coordination_notification(&state, &goal_id, &notification_id).await?;
    Ok(Json(notification_document(notification)))
}

async fn command_coordination_notification(
    State(state): State<AppState>,
    AxumPath((goal_id, notification_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<NotificationCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let (store, current) = coordination_notification(&state, &goal_id, &notification_id).await?;
    let api_key = format!(
        "coordination.v1.notification:{goal_id}:{notification_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    if let Some(expected) = body.expected_resource_version.as_deref()
        && resource_version(&current) != expected
    {
        return Err(CoordinationApiError::stale("notification revision changed"));
    }
    let service = NotificationLifecycleService::new(store.clone());
    let now = std::cmp::max(
        Utc::now(),
        current.metadata.updated_at + chrono::Duration::microseconds(1),
    );
    let notification = match body.command {
        NotificationCommand::Deliver { actor } => service.mark_delivered(
            &goal_id,
            &current.id,
            current.metadata.updated_at,
            actor,
            now,
        ),
        NotificationCommand::Acknowledge { worker_id } => service.acknowledge(
            &goal_id,
            &current.id,
            current.metadata.updated_at,
            &WorkerId::parse(worker_id)
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
            now,
        ),
        NotificationCommand::ActedOn { worker_id, outcome } => service.mark_acted_on(
            &goal_id,
            &current.id,
            current.metadata.updated_at,
            &WorkerId::parse(worker_id)
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?,
            outcome,
            now,
        ),
        NotificationCommand::Fail { actor, reason } => service.mark_failed(
            &goal_id,
            &current.id,
            current.metadata.updated_at,
            actor,
            reason,
            now,
        ),
        NotificationCommand::Replay {
            actor,
            recovery_kind,
        } => service.record_replayed(
            &goal_id,
            &current.id,
            current.metadata.updated_at,
            actor,
            recovery_kind,
            now,
        ),
    }
    .map_err(notification_lifecycle_error)?;
    let outcome = redacted_json_value(notification_document(notification))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(&api_key, "coordination.notification.command", &outcome, now)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

fn escalation_lifecycle_error(error: EscalationLifecycleError) -> CoordinationApiError {
    match error {
        EscalationLifecycleError::NotFound(message) => CoordinationApiError::not_found(message),
        EscalationLifecycleError::GoalMismatch { .. } => CoordinationApiError {
            status: StatusCode::FORBIDDEN,
            problem: ApiProblem::new(ProblemCode::Forbidden, error.to_string(), false),
        },
        EscalationLifecycleError::StaleRevision
        | EscalationLifecycleError::StaleDecision { .. } => CoordinationApiError {
            status: StatusCode::CONFLICT,
            problem: ApiProblem::new(ProblemCode::StaleRevision, error.to_string(), true),
        },
        EscalationLifecycleError::ConflictingReplay => {
            CoordinationApiError::conflict(error.to_string())
        }
        EscalationLifecycleError::InvalidRemediation(_)
        | EscalationLifecycleError::InvalidScopeBlock(_)
        | EscalationLifecycleError::MissingStalenessSnapshot
        | EscalationLifecycleError::InvalidStaleness(_)
        | EscalationLifecycleError::InvalidDecisionOption(_)
        | EscalationLifecycleError::InvalidResume(_)
        | EscalationLifecycleError::Domain(_) => CoordinationApiError {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            problem: ApiProblem::new(ProblemCode::InvalidRequest, error.to_string(), false),
        },
        EscalationLifecycleError::Claim(error) => coordination_claim_error(error),
        EscalationLifecycleError::Context(error) => {
            CoordinationApiError::conflict(error.to_string())
        }
        EscalationLifecycleError::Store(error) => CoordinationApiError::internal(error.to_string()),
    }
}

fn escalation_document(escalation: HumanEscalation) -> ResourceDocument<HumanEscalation> {
    ResourceDocument::new(
        "human_escalation",
        ResourceMetadata {
            id: escalation.id.as_str().into(),
            resource_version: resource_version(&escalation),
            created_at: escalation.metadata.created_at,
            updated_at: escalation.metadata.updated_at,
        },
        escalation,
    )
}

async fn list_coordination_escalations(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<HumanEscalation>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let escalations = store
        .escalations_for_goal(&goal_id, None)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&escalations, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        escalations[start..end]
            .iter()
            .cloned()
            .map(escalation_document)
            .collect(),
        next,
    )))
}

async fn coordination_escalation(
    state: &AppState,
    goal_id: &str,
    escalation_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, HumanEscalation), CoordinationApiError> {
    let id = EscalationId::parse(escalation_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let escalation = store
        .escalation(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|escalation| escalation.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("human escalation not found"))?;
    Ok((store, escalation))
}

async fn get_coordination_escalation(
    State(state): State<AppState>,
    AxumPath((goal_id, escalation_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<HumanEscalation>>, CoordinationApiError> {
    let (_, escalation) = coordination_escalation(&state, &goal_id, &escalation_id).await?;
    Ok(Json(escalation_document(escalation)))
}

async fn command_coordination_escalation(
    State(state): State<AppState>,
    AxumPath((goal_id, escalation_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<EscalationCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let confirmation = if let Some(action) = body.command.confirmation_action() {
        Some(require_dangerous_confirmation(
            &body,
            action,
            &escalation_id,
        )?)
    } else {
        None
    };
    let (store, current) = coordination_escalation(&state, &goal_id, &escalation_id).await?;
    let api_key = format!(
        "coordination.v1.escalation:{goal_id}:{escalation_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        if let Some(confirmation) = &confirmation {
            record_privileged_action_audit(
                store.as_ref(),
                &goal_id,
                confirmation,
                &api_key,
                &outcome,
                Utc::now(),
            )?;
        }
        return Ok(Json(outcome));
    }
    if let Some(expected) = body.expected_resource_version.as_deref()
        && resource_version(&current) != expected
    {
        return Err(CoordinationApiError::stale("escalation revision changed"));
    }
    let service = EscalationLifecycleService::new(store.clone());
    let now = std::cmp::max(
        Utc::now(),
        current.metadata.updated_at + chrono::Duration::microseconds(1),
    );
    let escalation = match body.command {
        EscalationCommand::Acknowledge { actor, reason } => {
            service.acknowledge(&goal_id, &current.id, &actor, reason, now)
        }
        EscalationCommand::Resolve {
            actor,
            reason,
            decision,
        } => service.resolve(
            &goal_id,
            &current.id,
            &actor,
            reason,
            EscalationDecision {
                option_id: decision.option_id,
                decided_by: decision.decided_by,
                accepted_risk: decision.accepted_risk,
                decided_at: now,
            },
            now,
        ),
        EscalationCommand::Override {
            actor,
            reason,
            decision,
        } => service.override_decision(
            &goal_id,
            &current.id,
            &actor,
            reason,
            EscalationDecision {
                option_id: decision.option_id,
                decided_by: decision.decided_by,
                accepted_risk: decision.accepted_risk,
                decided_at: now,
            },
            now,
        ),
        EscalationCommand::Expire { reason } => {
            required_coordination_text(&reason, "reason", 2_000)?;
            service.expire(&goal_id, &current.id, reason, now)
        }
        EscalationCommand::Cancel { actor, reason } => {
            required_coordination_text(&reason, "reason", 2_000)?;
            service.cancel(&goal_id, &current.id, &actor, reason, now)
        }
    }
    .map_err(escalation_lifecycle_error)?;
    let outcome = redacted_json_value(escalation_document(escalation))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(&api_key, "coordination.escalation.command", &outcome, now)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    if let Some(confirmation) = &confirmation {
        record_privileged_action_audit(
            store.as_ref(),
            &goal_id,
            confirmation,
            &api_key,
            &outcome,
            now,
        )?;
    }
    Ok(Json(outcome))
}

fn coordination_integration_error(error: IntegrationArtifactError) -> CoordinationApiError {
    match error {
        IntegrationArtifactError::ClaimNotFound(message)
        | IntegrationArtifactError::ArtifactNotFound(message)
        | IntegrationArtifactError::WorkerNotFound(message) => {
            CoordinationApiError::not_found(message)
        }
        IntegrationArtifactError::ClaimOwnerMismatch | IntegrationArtifactError::GoalMismatch => {
            CoordinationApiError {
                status: StatusCode::FORBIDDEN,
                problem: ApiProblem::new(ProblemCode::Forbidden, error.to_string(), false),
            }
        }
        IntegrationArtifactError::ClaimNotActive
        | IntegrationArtifactError::BaseRevisionMismatch
        | IntegrationArtifactError::PreflightFailed
        | IntegrationArtifactError::WorkspaceMissing
        | IntegrationArtifactError::GitFailed { .. }
        | IntegrationArtifactError::Tracker(_)
        | IntegrationArtifactError::Workspace(_) => {
            CoordinationApiError::conflict(error.to_string())
        }
        IntegrationArtifactError::InvalidValidationGate(_)
        | IntegrationArtifactError::ReconciliationNotAllowed
        | IntegrationArtifactError::InvalidReconciliation(_)
        | IntegrationArtifactError::Domain(_) => CoordinationApiError::invalid(error.to_string()),
        IntegrationArtifactError::Store(StoreError::NotFound(message)) => {
            CoordinationApiError::not_found(message)
        }
        IntegrationArtifactError::Store(StoreError::IntegrationArtifactAlreadyQueued(_)) => {
            CoordinationApiError::conflict(error.to_string())
        }
        IntegrationArtifactError::Store(StoreError::IntegrationQueueGoalMismatch { .. }) => {
            CoordinationApiError::conflict(error.to_string())
        }
        IntegrationArtifactError::Store(StoreError::Domain(_)) => {
            CoordinationApiError::conflict(error.to_string())
        }
        IntegrationArtifactError::Store(_) | IntegrationArtifactError::Io(_) => {
            CoordinationApiError::internal(error.to_string())
        }
    }
}

fn integration_artifact_document(
    artifact: IntegrationArtifact,
) -> ResourceDocument<IntegrationArtifact> {
    ResourceDocument::new(
        "integration_artifact",
        ResourceMetadata {
            id: artifact.id.as_str().into(),
            resource_version: resource_version(&artifact),
            created_at: artifact.metadata.created_at,
            updated_at: artifact.metadata.updated_at,
        },
        artifact,
    )
}

fn integration_preflight_document(
    artifact: &IntegrationArtifact,
    preflight: IntegrationPreflight,
) -> ResourceDocument<IntegrationPreflight> {
    ResourceDocument::new(
        "integration_preflight",
        ResourceMetadata {
            id: artifact.id.as_str().into(),
            resource_version: resource_version(&preflight),
            created_at: artifact.metadata.created_at,
            updated_at: preflight.evaluated_at,
        },
        preflight,
    )
}

fn integration_validation_document(
    report: IntegrationValidationReport,
) -> ResourceDocument<IntegrationValidationReport> {
    ResourceDocument::new(
        "integration_validation_report",
        ResourceMetadata {
            id: report.id.as_str().into(),
            resource_version: resource_version(&report),
            created_at: report.metadata.created_at,
            updated_at: report.metadata.updated_at,
        },
        report,
    )
}

fn integration_reconciliation_document(
    record: ReconciliationRecord,
) -> ResourceDocument<ReconciliationRecord> {
    ResourceDocument::new(
        "integration_reconciliation",
        ResourceMetadata {
            id: record.id.as_str().into(),
            resource_version: resource_version(&record),
            created_at: record.metadata.created_at,
            updated_at: record.metadata.updated_at,
        },
        record,
    )
}

fn integration_job_document(job: IntegrationJob) -> ResourceDocument<IntegrationJob> {
    ResourceDocument::new(
        "integration_job",
        ResourceMetadata {
            id: job.id.as_str().into(),
            resource_version: resource_version(&job),
            created_at: job.metadata.created_at,
            updated_at: job.metadata.updated_at,
        },
        job,
    )
}

fn integration_finalization_document(
    finalization: IntegrationFinalization,
) -> ResourceDocument<IntegrationFinalization> {
    ResourceDocument::new(
        "integration_finalization",
        ResourceMetadata {
            id: finalization.id.as_str().into(),
            resource_version: resource_version(&finalization),
            created_at: finalization.metadata.created_at,
            updated_at: finalization.metadata.updated_at,
        },
        finalization,
    )
}

fn integration_maintenance_document(
    record: IntegrationMaintenanceRecord,
) -> ResourceDocument<IntegrationMaintenanceRecord> {
    ResourceDocument::new(
        "integration_maintenance",
        ResourceMetadata {
            id: record.id.as_str().into(),
            resource_version: resource_version(&record),
            created_at: record.metadata.created_at,
            updated_at: record.metadata.updated_at,
        },
        record,
    )
}

fn check_coordination_resource_version<T: Serialize>(
    expected: Option<&str>,
    current: &T,
    resource: &str,
) -> Result<(), CoordinationApiError> {
    if let Some(expected) = expected
        && resource_version(current) != expected
    {
        return Err(CoordinationApiError::stale(format!(
            "{resource} revision changed"
        )));
    }
    Ok(())
}

async fn coordination_integration_artifact(
    state: &AppState,
    goal_id: &str,
    artifact_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, IntegrationArtifact), CoordinationApiError> {
    let id = IntegrationArtifactId::parse(artifact_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let artifact = store
        .integration_artifact(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|artifact| artifact.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("integration artifact not found"))?;
    Ok((store, artifact))
}

async fn list_coordination_integration_artifacts(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<IntegrationArtifact>>>, CoordinationApiError> {
    let store = coordination_store(&state).await?;
    let artifacts = store
        .integration_artifacts_for_goal(&goal_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&artifacts, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        artifacts[start..end]
            .iter()
            .cloned()
            .map(integration_artifact_document)
            .collect(),
        next,
    )))
}

async fn get_coordination_integration_artifact(
    State(state): State<AppState>,
    AxumPath((goal_id, artifact_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<IntegrationArtifact>>, CoordinationApiError> {
    let (_, artifact) = coordination_integration_artifact(&state, &goal_id, &artifact_id).await?;
    Ok(Json(integration_artifact_document(artifact)))
}

async fn capture_coordination_integration_artifact(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<CommandEnvelope<IntegrationArtifactCollectionCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    if body.expected_resource_version.is_some() {
        return Err(CoordinationApiError::invalid(
            "artifact capture does not accept expectedResourceVersion",
        ));
    }
    let store = coordination_store(&state).await?;
    let api_key = format!(
        "coordination.v1.integration-artifact:{goal_id}:capture:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    let IntegrationArtifactCollectionCommand::Capture {
        claim_id,
        worker_id,
        changed_contracts,
        migrations,
        validations,
        evidence_refs,
        known_risks,
    } = body.command;
    let claim_id = ClaimId::parse(claim_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let worker_id = WorkerId::parse(worker_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let claim = store
        .claim(&claim_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .ok_or_else(|| CoordinationApiError::not_found("claim not found"))?;
    let worker = store
        .worker(&worker_id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .ok_or_else(|| CoordinationApiError::not_found("worker not found"))?;
    if claim.goal_id != goal_id || worker.goal_id != goal_id {
        return Err(CoordinationApiError {
            status: StatusCode::FORBIDDEN,
            problem: ApiProblem::new(
                ProblemCode::Forbidden,
                "claim and worker must belong to the requested goal",
                false,
            ),
        });
    }
    let now = Utc::now();
    let artifact = IntegrationArtifactService::new(store.clone())
        .capture(
            CaptureIntegrationArtifactRequest {
                claim_id,
                worker_id,
                changed_contracts,
                migrations,
                validations,
                evidence_refs,
                known_risks,
            },
            now,
        )
        .map_err(coordination_integration_error)?;
    let outcome = redacted_json_value(integration_artifact_document(artifact))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.integration-artifact.capture",
            &outcome,
            now,
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn command_coordination_integration_artifact(
    State(state): State<AppState>,
    AxumPath((goal_id, artifact_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<IntegrationArtifactCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let confirmation = if let Some(action) = body.command.confirmation_action() {
        Some(require_dangerous_confirmation(&body, action, &artifact_id)?)
    } else {
        None
    };
    let (store, artifact) =
        coordination_integration_artifact(&state, &goal_id, &artifact_id).await?;
    let api_key = format!(
        "coordination.v1.integration-artifact:{goal_id}:{artifact_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        if let Some(confirmation) = &confirmation {
            record_privileged_action_audit(
                store.as_ref(),
                &goal_id,
                confirmation,
                &api_key,
                &outcome,
                Utc::now(),
            )?;
        }
        return Ok(Json(outcome));
    }
    check_coordination_resource_version(
        body.expected_resource_version.as_deref(),
        &artifact,
        "integration artifact",
    )?;
    let now = std::cmp::max(
        Utc::now(),
        artifact.metadata.updated_at + chrono::Duration::microseconds(1),
    );
    let outcome = match body.command {
        IntegrationArtifactCommand::Preflight => {
            let preflight = IntegrationPreflightService::new(store.clone())
                .run(&artifact.id, now)
                .map_err(coordination_integration_error)?;
            redacted_json_value(integration_preflight_document(&artifact, preflight))
        }
        IntegrationArtifactCommand::Validate { gates } => {
            let gates = gates
                .into_iter()
                .map(ValidationGateSpec::from)
                .collect::<Vec<_>>();
            let report = ValidationGateRunner::new(store.clone())
                .run(&artifact.id, &gates, now)
                .map_err(coordination_integration_error)?;
            redacted_json_value(integration_validation_document(report))
        }
        IntegrationArtifactCommand::Reconcile {
            strategy,
            target_revision,
            commit_revisions,
            regeneration,
            manual_instructions,
        } => {
            let record = ReconciliationService::new(store.clone(), ReconciliationPolicy::default())
                .reconcile(
                    ReconciliationRequest {
                        artifact_id: artifact.id.clone(),
                        strategy,
                        target_revision,
                        commit_revisions,
                        regeneration: regeneration.map(|command| RegenerationCommand {
                            program: command.program,
                            args: command.args,
                        }),
                        manual_instructions,
                    },
                    now,
                )
                .map_err(coordination_integration_error)?;
            redacted_json_value(integration_reconciliation_document(record))
        }
        IntegrationArtifactCommand::Integrate { priority } => {
            let job = IntegrationQueueService::new(store.clone())
                .enqueue(&artifact.id, priority, now)
                .map_err(coordination_integration_error)?;
            redacted_json_value(integration_job_document(job))
        }
        IntegrationArtifactCommand::Retry { priority } => {
            let prior = store
                .integration_jobs_for_repository(
                    &artifact.repository_id,
                    Some(IntegrationJobState::Failed),
                )
                .map_err(|error| CoordinationApiError::internal(error.to_string()))?
                .into_iter()
                .rev()
                .find(|job| job.artifact_id == artifact.id)
                .ok_or_else(|| {
                    CoordinationApiError::conflict(
                        "retry requires a prior failed integration job for this artifact",
                    )
                })?;
            let job = IntegrationQueueService::new(store.clone())
                .enqueue(&artifact.id, priority.unwrap_or(prior.priority), now)
                .map_err(coordination_integration_error)?;
            redacted_json_value(integration_job_document(job))
        }
    }
    .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.integration-artifact.command",
            &outcome,
            now,
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    if let Some(confirmation) = &confirmation {
        record_privileged_action_audit(
            store.as_ref(),
            &goal_id,
            confirmation,
            &api_key,
            &outcome,
            now,
        )?;
    }
    Ok(Json(outcome))
}

async fn list_coordination_integration_validations(
    State(state): State<AppState>,
    AxumPath((goal_id, artifact_id)): AxumPath<(String, String)>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<IntegrationValidationReport>>>, CoordinationApiError>
{
    let (store, artifact) =
        coordination_integration_artifact(&state, &goal_id, &artifact_id).await?;
    let reports = store
        .validation_reports_for_artifact(&artifact.id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&reports, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        reports[start..end]
            .iter()
            .cloned()
            .map(integration_validation_document)
            .collect(),
        next,
    )))
}

async fn list_coordination_integration_reconciliations(
    State(state): State<AppState>,
    AxumPath((goal_id, artifact_id)): AxumPath<(String, String)>,
    Query(query): Query<PageQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<ReconciliationRecord>>>, CoordinationApiError> {
    let (store, artifact) =
        coordination_integration_artifact(&state, &goal_id, &artifact_id).await?;
    let records = store
        .reconciliations_for_artifact(&artifact.id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&records, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        records[start..end]
            .iter()
            .cloned()
            .map(integration_reconciliation_document)
            .collect(),
        next,
    )))
}

async fn list_coordination_integration_maintenance(
    State(state): State<AppState>,
    AxumPath((goal_id, artifact_id)): AxumPath<(String, String)>,
    Query(query): Query<PageQuery>,
) -> Result<
    Json<CollectionPage<ResourceDocument<IntegrationMaintenanceRecord>>>,
    CoordinationApiError,
> {
    let (store, artifact) =
        coordination_integration_artifact(&state, &goal_id, &artifact_id).await?;
    let records = store
        .integration_maintenance_for_artifact(&artifact.id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    let (start, end, next) = page_bounds(&records, &query, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        records[start..end]
            .iter()
            .cloned()
            .map(integration_maintenance_document)
            .collect(),
        next,
    )))
}

async fn list_coordination_integration_jobs(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Query(query): Query<IntegrationJobQuery>,
) -> Result<Json<CollectionPage<ResourceDocument<IntegrationJob>>>, CoordinationApiError> {
    required_coordination_text(&query.repository_id, "repositoryId", 2_000)?;
    let store = coordination_store(&state).await?;
    let jobs = store
        .integration_jobs_for_repository(&query.repository_id, query.state)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .into_iter()
        .filter(|job| job.goal_id == goal_id)
        .collect::<Vec<_>>();
    let page = PageQuery {
        cursor: query.cursor,
        limit: query.limit,
    };
    let (start, end, next) = page_bounds(&jobs, &page, |item| item.id.as_str())?;
    Ok(Json(CollectionPage::new(
        jobs[start..end]
            .iter()
            .cloned()
            .map(integration_job_document)
            .collect(),
        next,
    )))
}

async fn coordination_integration_job(
    state: &AppState,
    goal_id: &str,
    job_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, IntegrationJob), CoordinationApiError> {
    let id = IntegrationJobId::parse(job_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let job = store
        .integration_job(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|job| job.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("integration job not found"))?;
    Ok((store, job))
}

async fn get_coordination_integration_job(
    State(state): State<AppState>,
    AxumPath((goal_id, job_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<IntegrationJob>>, CoordinationApiError> {
    let (_, job) = coordination_integration_job(&state, &goal_id, &job_id).await?;
    Ok(Json(integration_job_document(job)))
}

async fn command_coordination_integration_queue(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<CommandEnvelope<IntegrationQueueCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    if body.expected_resource_version.is_some() {
        return Err(CoordinationApiError::invalid(
            "queue acquisition does not accept expectedResourceVersion",
        ));
    }
    let store = coordination_store(&state).await?;
    let api_key = format!(
        "coordination.v1.integration-queue:{goal_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    let IntegrationQueueCommand::Acquire { repository_id } = body.command;
    required_coordination_text(&repository_id, "repositoryId", 2_000)?;
    let now = Utc::now();
    let job = IntegrationQueueService::new(store.clone())
        .acquire_next_for_goal(&repository_id, &goal_id, now)
        .map_err(coordination_integration_error)?;
    let outcome = match job {
        Some(job) => redacted_json_value(integration_job_document(job)),
        None => redacted_json_value(ResourceDocument::new(
            "integration_queue_acquisition",
            ResourceMetadata {
                id: repository_id,
                resource_version: resource_version(&Option::<IntegrationJob>::None),
                created_at: now,
                updated_at: now,
            },
            Option::<IntegrationJob>::None,
        )),
    }
    .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.integration-queue.acquire",
            &outcome,
            now,
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn command_coordination_integration_job(
    State(state): State<AppState>,
    AxumPath((goal_id, job_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<IntegrationJobCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let (store, job) = coordination_integration_job(&state, &goal_id, &job_id).await?;
    let api_key = format!(
        "coordination.v1.integration-job:{goal_id}:{job_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        return Ok(Json(outcome));
    }
    check_coordination_resource_version(
        body.expected_resource_version.as_deref(),
        &job,
        "integration job",
    )?;
    let now = std::cmp::max(
        Utc::now(),
        job.metadata.updated_at + chrono::Duration::microseconds(1),
    );
    let outcome = match body.command {
        IntegrationJobCommand::Finish { succeeded, summary } => {
            required_coordination_text(&summary, "summary", 4_000)?;
            let job = IntegrationQueueService::new(store.clone())
                .finish(&job.id, succeeded, &summary, now)
                .map_err(coordination_integration_error)?;
            redacted_json_value(integration_job_document(job))
        }
        IntegrationJobCommand::Finalize {
            validation_report_id,
            tracker_feature_id,
            tracker_step_ids,
            tracker_summary,
            tracker_evidence,
            tracker_status,
            integration_revision,
        } => {
            let report_id = ValidationReportId::parse(validation_report_id)
                .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
            let root = state.tracker_root.read().await.clone();
            let finalization =
                IntegrationFinalizationService::new(store.clone(), Tracker::new(root))
                    .finalize(
                        IntegrationFinalizationRequest {
                            job_id: job.id.clone(),
                            validation_report_id: report_id,
                            tracker_feature_id,
                            tracker_step_ids,
                            tracker_summary,
                            tracker_evidence,
                            tracker_status,
                            integration_revision,
                        },
                        now,
                    )
                    .map_err(coordination_integration_error)?;
            redacted_json_value(integration_finalization_document(finalization))
        }
    }
    .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.integration-job.command",
            &outcome,
            now,
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    Ok(Json(outcome))
}

async fn coordination_integration_finalization(
    state: &AppState,
    goal_id: &str,
    finalization_id: &str,
) -> Result<(Arc<SqliteCoordinationStore>, IntegrationFinalization), CoordinationApiError> {
    let id = IntegrationFinalizationId::parse(finalization_id)
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let store = coordination_store(state).await?;
    let finalization = store
        .integration_finalization(&id)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
        .filter(|finalization| finalization.goal_id == goal_id)
        .ok_or_else(|| CoordinationApiError::not_found("integration finalization not found"))?;
    Ok((store, finalization))
}

async fn get_coordination_integration_finalization(
    State(state): State<AppState>,
    AxumPath((goal_id, finalization_id)): AxumPath<(String, String)>,
) -> Result<Json<ResourceDocument<IntegrationFinalization>>, CoordinationApiError> {
    let (_, finalization) =
        coordination_integration_finalization(&state, &goal_id, &finalization_id).await?;
    Ok(Json(integration_finalization_document(finalization)))
}

async fn command_coordination_integration_finalization(
    State(state): State<AppState>,
    AxumPath((goal_id, finalization_id)): AxumPath<(String, String)>,
    Json(body): Json<CommandEnvelope<IntegrationFinalizationCommand>>,
) -> Result<Json<Value>, CoordinationApiError> {
    body.validate()
        .map_err(|error| CoordinationApiError::invalid(error.to_string()))?;
    let confirmation = require_dangerous_confirmation(
        &body,
        body.command.confirmation_action(),
        &finalization_id,
    )?;
    let (store, finalization) =
        coordination_integration_finalization(&state, &goal_id, &finalization_id).await?;
    let api_key = format!(
        "coordination.v1.integration-finalization:{goal_id}:{finalization_id}:{}",
        body.idempotency_key
    );
    if let Some(outcome) = store
        .idempotent_outcome(&api_key)
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?
    {
        record_privileged_action_audit(
            store.as_ref(),
            &goal_id,
            &confirmation,
            &api_key,
            &outcome,
            Utc::now(),
        )?;
        return Ok(Json(outcome));
    }
    check_coordination_resource_version(
        body.expected_resource_version.as_deref(),
        &finalization,
        "integration finalization",
    )?;
    let now = std::cmp::max(
        Utc::now(),
        finalization.metadata.updated_at + chrono::Duration::microseconds(1),
    );
    let service = IntegrationMaintenanceService::new(store.clone());
    let record = match body.command {
        IntegrationFinalizationCommand::Rollback {
            requested_by,
            reason,
        } => service.rollback(&finalization.id, &requested_by, &reason, now),
        IntegrationFinalizationCommand::Cleanup => {
            service.cleanup_after_integration(&finalization.id, now)
        }
    }
    .map_err(coordination_integration_error)?;
    let outcome = redacted_json_value(integration_maintenance_document(record))
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    store
        .record_idempotent_outcome(
            &api_key,
            "coordination.integration-finalization.command",
            &outcome,
            now,
        )
        .map_err(|error| CoordinationApiError::internal(error.to_string()))?;
    record_privileged_action_audit(
        store.as_ref(),
        &goal_id,
        &confirmation,
        &api_key,
        &outcome,
        now,
    )?;
    Ok(Json(outcome))
}

async fn get_worker_pool(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let (pool, _) = coordination_services(&state).await?;
    Ok(Json(json!(pool.snapshot(&goal_id)?)))
}

async fn update_worker_pool(
    State(state): State<AppState>,
    AxumPath(goal_id): AxumPath<String>,
    Json(body): Json<WorkerPoolUpdate>,
) -> Result<Json<Value>, ApiError> {
    let confirmation = if body.user_policy.is_some()
        || body.permission_policy.is_some()
        || body.resource_policy.is_some()
    {
        Some(require_legacy_confirmation(
            body.confirmation.as_ref(),
            "pool.configure_policy",
            &goal_id,
        )?)
    } else {
        None
    };
    let workspace = state.runner.workspace_root().await;
    let (pool, claims) = coordination_services(&state).await?;
    let now = Utc::now();
    let resource_policy = body.resource_policy.clone();
    if let Some((user_policy, goal_policy)) =
        paired_permission_policies_legacy(body.user_policy, body.permission_policy)?
    {
        pool.configure_for_workspace_with_permission_policy(
            &goal_id,
            body.desired_concurrency,
            workspace,
            user_policy,
            goal_policy,
            now,
        )?;
    } else {
        pool.set_desired_concurrency_for_workspace(
            &goal_id,
            body.desired_concurrency,
            0,
            workspace,
            now,
        )?;
    }
    if let Some(policy) = resource_policy {
        pool.set_resource_quotas(&goal_id, policy, now)?;
    }
    let assigned = pool.fill_ready_claims(
        &claims,
        &goal_id,
        body.base_revision.as_deref().unwrap_or("workspace-current"),
        now,
    )?;
    let outcome = json!({"pool":pool.snapshot(&goal_id)?,"assignedClaims":assigned});
    if let Some(confirmation) = &confirmation {
        record_legacy_privileged_action_audit(
            &state,
            &goal_id,
            confirmation,
            &format!("legacy-pool-configure:{}", now.timestamp_micros()),
            &outcome,
            now,
        )
        .await?;
    }
    Ok(Json(outcome))
}

async fn control_worker_pool(
    State(state): State<AppState>,
    AxumPath((goal_id, action)): AxumPath<(String, String)>,
    Json(body): Json<WorkerPoolControl>,
) -> Result<Json<Value>, ApiError> {
    let confirmation = if action == "start"
        && (body.user_policy.is_some()
            || body.permission_policy.is_some()
            || body.resource_policy.is_some())
    {
        Some(require_legacy_confirmation(
            body.confirmation.as_ref(),
            "pool.start_policy",
            &goal_id,
        )?)
    } else if action == "stop" {
        Some(require_legacy_confirmation(
            body.confirmation.as_ref(),
            "pool.stop",
            &goal_id,
        )?)
    } else {
        None
    };
    let workspace = state.runner.workspace_root().await;
    let (pool, claims) = coordination_services(&state).await?;
    let now = Utc::now();
    let base_revision = body.base_revision.as_deref().unwrap_or("workspace-current");
    let resource_policy = body.resource_policy.clone();
    let permission_policies =
        paired_permission_policies_legacy(body.user_policy, body.permission_policy)?;
    let outcome = match action.as_str() {
        "start" => {
            if let Some((user_policy, goal_policy)) = permission_policies {
                pool.start_for_workspace_with_permission_policy(
                    &goal_id,
                    body.desired_concurrency,
                    0,
                    &workspace,
                    user_policy,
                    goal_policy,
                    now,
                )?;
            } else {
                pool.start_for_workspace(&goal_id, body.desired_concurrency, 0, &workspace, now)?;
            }
            if let Some(policy) = resource_policy {
                pool.set_resource_quotas(&goal_id, policy, now)?;
            }
            let assigned = pool.fill_ready_claims(&claims, &goal_id, base_revision, now)?;
            Ok(json!({"pool":pool.snapshot(&goal_id)?,"assignedClaims":assigned}))
        }
        "pause" => Ok(json!({"pool":pool.pause(&goal_id, now)?})),
        "resume" => {
            let desired = pool.snapshot(&goal_id)?.goal.desired_concurrency;
            crate::coordination::workspace::WorkspaceIsolationCapability::detect(&workspace)
                .map_err(WorkerPoolError::from)
                .and_then(|capability| {
                    capability
                        .allows_concurrency(desired)
                        .then_some(())
                        .ok_or_else(|| {
                            WorkerPoolError::IsolationUnavailable(
                                "non-Git workspaces are limited to one implementation worker"
                                    .into(),
                            )
                        })
                })?;
            pool.resume(&goal_id, 0, now)?;
            let assigned = pool.fill_ready_claims(&claims, &goal_id, base_revision, now)?;
            Ok(json!({"pool":pool.snapshot(&goal_id)?,"assignedClaims":assigned}))
        }
        "drain" => Ok(json!({"pool":pool.drain(&goal_id, now)?})),
        "stop" => Ok(json!({"result":pool.stop(&goal_id, now)?})),
        "reconcile" => {
            let assigned = pool.fill_ready_claims(&claims, &goal_id, base_revision, now)?;
            Ok(json!({"pool":pool.snapshot(&goal_id)?,"assignedClaims":assigned}))
        }
        _ => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("unknown worker pool action: {action}"),
        )),
    }?;
    if let Some(confirmation) = &confirmation {
        record_legacy_privileged_action_audit(
            &state,
            &goal_id,
            confirmation,
            &format!("legacy-pool-{action}:{}", now.timestamp_micros()),
            &outcome,
            now,
        )
        .await?;
    }
    Ok(Json(outcome))
}

async fn execute_worker_operation(
    State(state): State<AppState>,
    AxumPath((goal_id, worker_id)): AxumPath<(String, String)>,
    Json(body): Json<WorkerToolRequest>,
) -> Result<Json<WorkerToolResponse>, ApiError> {
    let root = state.tracker_root.read().await.clone();
    let store = Arc::new(
        SqliteCoordinationStore::open(root.join("coordination.sqlite"))
            .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?,
    );
    let worker_id = WorkerId::parse(worker_id).map_err(|error| ApiError::bad(error.to_string()))?;
    let worker = store
        .worker(&worker_id)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "worker not found"))?;
    if worker.goal_id != goal_id {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "worker does not belong to this goal",
        ));
    }
    let service = WorkerProtocolService::new(
        Tracker::new(root),
        store,
        ClaimPolicy::default(),
        WorkerContextLimits::default(),
    );
    Ok(Json(service.execute(&worker_id, body, Utc::now())?))
}
async fn list_projects(State(state): State<AppState>) -> Json<Vec<Value>> {
    let mut values = Vec::new();
    for record in state.registry.list() {
        values.push(project_summary(&state, record).await);
    }
    Json(values)
}
async fn add_project(
    State(state): State<AppState>,
    Json(body): Json<ProjectCreate>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    validate_text(&body.path, "path", 4096)?;
    let selected = filesystem_path(&state, Some(&body.path)).await?;
    let record = state
        .registry
        .add(&selected, body.activate)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    let value = if body.activate {
        activate_project(&state, record).await?
    } else {
        project_summary(&state, record).await
    };
    Ok((StatusCode::CREATED, Json(value)))
}
async fn select_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let record = state
        .registry
        .get(&id)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "project not found"))?;
    Ok(Json(activate_project(&state, record).await?))
}
async fn remove_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<StatusCode, ApiError> {
    if *state.active_project_id.read().await == id {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "cannot remove the active project",
        ));
    }
    if !state
        .registry
        .remove(&id)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?
    {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "project not found"));
    }
    Ok(StatusCode::NO_CONTENT)
}
async fn rename_project(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<NameUpdate>,
) -> Result<Json<Value>, ApiError> {
    validate_text(&body.name, "name", 200)?;
    let record = state
        .registry
        .rename(&id, &body.name)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "project not found"))?;
    Ok(Json(project_summary(&state, record).await))
}
async fn select_workspace(
    State(state): State<AppState>,
    Json(body): Json<WorkspaceSelect>,
) -> Result<Json<Value>, ApiError> {
    let selected = filesystem_path(&state, Some(&body.path)).await?;
    let record = state
        .registry
        .add(&selected, true)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?;
    activate_project(&state, record).await?;
    Ok(Json(
        json!({"path":selected,"name":selected.file_name().and_then(|v|v.to_str()).unwrap_or("project"),"browse_root":state.browse_root}),
    ))
}

#[derive(Deserialize)]
struct BrowseQuery {
    path: Option<String>,
    #[serde(default)]
    show_hidden: bool,
}
async fn browse_filesystem(
    State(state): State<AppState>,
    Query(query): Query<BrowseQuery>,
) -> Result<Json<Value>, ApiError> {
    let directory = filesystem_path(&state, query.path.as_deref()).await?;
    let mut entries = std::fs::read_dir(&directory)
        .map_err(|_| ApiError::new(StatusCode::FORBIDDEN, "directory is not readable"))?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            if !query.show_hidden && name.starts_with('.') {
                return None;
            }
            let path = entry.path().canonicalize().ok()?;
            if !path.is_dir()
                || (path != state.browse_root && !path.starts_with(&state.browse_root))
            {
                return None;
            }
            Some(json!({"name":name,"path":path,"symlink":entry.file_type().ok()?.is_symlink()}))
        })
        .collect::<Vec<_>>();
    entries.sort_by_key(|value| value["name"].as_str().unwrap_or("").to_lowercase());
    let parent = if directory == state.browse_root {
        Value::Null
    } else {
        json!(directory.parent())
    };
    Ok(Json(
        json!({"path":directory,"parent":parent,"directories":entries}),
    ))
}

async fn root_tracker(state: &AppState) -> Tracker {
    tracker(state, &state.tracker_root.read().await)
}
async fn list_goals(State(state): State<AppState>) -> Result<Json<Vec<Value>>, ApiError> {
    Ok(Json(root_tracker(&state).await.list_goals()?))
}
async fn create_goal(
    State(state): State<AppState>,
    Json(body): Json<GoalCreate>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    validate_text(&body.title, "title", 200)?;
    validate_text(&body.description, "description", 4000).or_else(|error| {
        if body.description.is_empty() {
            Ok(())
        } else {
            Err(error)
        }
    })?;
    let id = body
        .goal_id
        .unwrap_or_else(|| generate_goal_id(&body.title));
    Ok((
        StatusCode::CREATED,
        Json(
            root_tracker(&state)
                .await
                .create_goal(&id, &body.title, &body.description)?,
        ),
    ))
}

fn scaffold_execution_prompt(body: &GoalScaffoldCreate) -> String {
    let request = json!({
        "title": body.title,
        "description": body.description,
        "goal_type_hint": body.goal_type_hint,
    });
    prompts::scaffold_goal(&request.to_string())
}

fn scaffold_output_schema() -> Value {
    let string_array = || json!({"type": "array", "items": {"type": "string"}});
    let feature = || {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": [
                "feature_id", "title", "description", "scope", "rationale", "evidence",
                "steps", "acceptance", "verification", "dependencies", "risks", "confidence"
            ],
            "properties": {
                "feature_id": {"type": "string"},
                "title": {"type": "string"},
                "description": {"type": "string"},
                "scope": {"type": "string", "enum": ["required_mvp", "required_release_safety", "optional_hardening", "future"]},
                "rationale": {"type": "string"},
                "evidence": string_array(),
                "steps": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["step_id", "title"],
                        "properties": {
                            "step_id": {"type": "string"},
                            "title": {"type": "string"}
                        }
                    }
                },
                "acceptance": string_array(),
                "verification": string_array(),
                "dependencies": string_array(),
                "risks": string_array(),
                "confidence": {"type": "string", "enum": ["high", "medium", "low"]}
            }
        })
    };
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "goal_interpretation", "success_criteria", "primary_type", "secondary_types",
            "classification_evidence", "confidence", "assumptions", "decisions_required",
            "required_features", "optional_features", "risks", "dependencies", "non_goals"
        ],
        "properties": {
            "goal_interpretation": {"type": "string"},
            "success_criteria": string_array(),
            "primary_type": {"type": "string", "enum": ["Greenfield", "Feature", "Bug", "Refactor", "Migration", "Integration", "Release", "Research", "Mixed"]},
            "secondary_types": {"type": "array", "items": {"type": "string", "enum": ["Greenfield", "Feature", "Bug", "Refactor", "Migration", "Integration", "Release", "Research", "Mixed"]}},
            "classification_evidence": string_array(),
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            "assumptions": string_array(),
            "decisions_required": string_array(),
            "required_features": {"type": "array", "items": feature()},
            "optional_features": {"type": "array", "items": feature()},
            "risks": string_array(),
            "dependencies": string_array(),
            "non_goals": string_array()
        }
    })
}

fn goal_execution_prompt(goal: &str, work_mode: &WorkMode) -> String {
    prompts::goal_execution(goal, work_mode)
}

fn parse_scaffold_proposal(message: &str) -> Result<GoalScaffoldProposal, ApiError> {
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
    let proposal: GoalScaffoldProposal = serde_json::from_str(candidate).map_err(|_| {
        ApiError::bad("Codex returned an incomplete goal plan. Please generate suggestions again.")
    })?;
    if proposal.goal_interpretation.trim().is_empty() {
        return Err(ApiError::bad("scaffold goal interpretation is empty"));
    }
    if proposal.required_features.is_empty() && proposal.optional_features.is_empty() {
        return Err(ApiError::bad("scaffold contains no feature suggestions"));
    }
    Ok(proposal)
}

async fn create_goal_scaffold(
    State(state): State<AppState>,
    Json(body): Json<GoalScaffoldCreate>,
) -> Result<impl IntoResponse, ApiError> {
    validate_text(&body.title, "title", 200)?;
    if !body.description.is_empty() {
        validate_text(&body.description, "description", 4000)?;
    }
    let execution_prompt = scaffold_execution_prompt(&body);
    let display_prompt = format!("Scaffold implementation goal: {}", body.title);
    let run = state
        .runner
        .create(
            display_prompt,
            vec![],
            vec![],
            ".".into(),
            "read-only".into(),
            "on-request".into(),
            "user".into(),
            true,
            None,
            None,
            Some(execution_prompt),
            Some(scaffold_output_schema()),
            WorkMode::Spec,
            body.llm_config,
        )
        .await
        .map_err(ApiError::bad)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(RunManager::redacted_for_display(&run)),
    ))
}

async fn get_goal_scaffold(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    let run = state
        .runner
        .get(&id)
        .await
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "scaffold run not found"))?;
    let proposal = if run.status == "completed" {
        Some(parse_scaffold_proposal(
            run.final_message.as_deref().unwrap_or_default(),
        )?)
    } else {
        None
    };
    Ok(Json(json!({"run": run, "proposal": proposal})))
}

#[derive(Deserialize)]
struct GreenfieldLocationQuery {
    title: String,
}

fn greenfield_folder_name(title: &str) -> String {
    let mut result = String::new();
    for word in title.split(|character: char| !character.is_ascii_alphanumeric()) {
        if word.is_empty() {
            continue;
        }
        let mut characters = word.chars();
        if let Some(first) = characters.next() {
            result.extend(first.to_uppercase());
            result.push_str(characters.as_str());
        }
    }
    if result.is_empty() {
        "NewApplication".into()
    } else {
        result
    }
}

fn suggested_greenfield_path(workspace: &Path, browse_root: &Path, title: &str) -> PathBuf {
    let base = if workspace == browse_root {
        let code = workspace.join("Code");
        if code.is_dir() {
            code
        } else {
            workspace.into()
        }
    } else {
        workspace
            .parent()
            .filter(|parent| *parent == browse_root || parent.starts_with(browse_root))
            .unwrap_or(workspace)
            .to_path_buf()
    };
    base.join(greenfield_folder_name(title))
}

async fn get_greenfield_location_suggestion(
    State(state): State<AppState>,
    Query(query): Query<GreenfieldLocationQuery>,
) -> Result<Json<Value>, ApiError> {
    validate_text(&query.title, "title", 200)?;
    let workspace = state.runner.workspace_root().await;
    let path = suggested_greenfield_path(&workspace, &state.browse_root, &query.title);
    Ok(Json(json!({
        "path": path,
        "exists": path.is_dir(),
        "workspace": workspace,
    })))
}

fn prepare_greenfield_project_path(browse_root: &Path, value: &str) -> Result<PathBuf, ApiError> {
    validate_text(value.trim(), "project path", 4096)?;
    let browse_root = browse_root
        .canonicalize()
        .map_err(|_| ApiError::bad("could not resolve the configured browsing root"))?;
    let requested = PathBuf::from(value.trim());
    if !requested.is_absolute() {
        return Err(ApiError::bad("Greenfield project path must be absolute"));
    }
    if requested.is_dir() {
        return requested
            .canonicalize()
            .map_err(|_| ApiError::bad("could not resolve Greenfield project path"))
            .and_then(|path| {
                if path == browse_root || path.starts_with(&browse_root) {
                    Ok(path)
                } else {
                    Err(ApiError::new(
                        StatusCode::FORBIDDEN,
                        "Greenfield project path is outside the configured browsing root",
                    ))
                }
            });
    }
    if requested.exists() {
        return Err(ApiError::bad("Greenfield project path is not a directory"));
    }
    let name = requested
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty() && *value != "." && *value != "..")
        .ok_or_else(|| ApiError::bad("Greenfield project path needs a folder name"))?;
    let parent = requested
        .parent()
        .ok_or_else(|| ApiError::bad("Greenfield project path needs a parent directory"))?
        .canonicalize()
        .map_err(|_| ApiError::bad("Greenfield project parent directory does not exist"))?;
    if parent != browse_root && !parent.starts_with(&browse_root) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "Greenfield project path is outside the configured browsing root",
        ));
    }
    let path = parent.join(name);
    std::fs::create_dir(&path)
        .map_err(|error| ApiError::new(StatusCode::CONFLICT, error.to_string()))?;
    path.canonicalize()
        .map_err(|_| ApiError::bad("could not resolve created Greenfield project path"))
}

fn is_project_location_decision(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    value.contains("implementation location")
        || value.contains("target repository")
        || value.contains("target project")
        || value.contains("project/repository")
        || value.contains("project path")
}

fn is_project_location_confirmation_step(step: &Step) -> bool {
    let id = step.id.to_ascii_lowercase();
    let title = step.title.to_ascii_lowercase();
    (id.contains("project-location")
        && (id.contains("confirm") || id.contains("choose") || id.contains("select")))
        || (title.contains("project location")
            && (title.contains("confirm") || title.contains("choose") || title.contains("select")))
}

fn select_initial_scaffold_step(
    features: &mut [Feature],
    project_location_confirmed: bool,
) -> Result<(), ApiError> {
    if project_location_confirmed {
        for step in features.iter_mut().flat_map(|feature| &mut feature.steps) {
            if is_project_location_confirmation_step(step) {
                step.done = true;
            }
        }
    }
    let next = features
        .iter_mut()
        .flat_map(|feature| &mut feature.steps)
        .find(|step| !step.done)
        .ok_or_else(|| {
            ApiError::bad("accept at least one incomplete scaffold step as actionable work")
        })?;
    next.next = true;
    Ok(())
}

fn scaffold_feature_description(feature: &GoalScaffoldFeature) -> String {
    let mut sections = Vec::new();
    if !feature.description.trim().is_empty() {
        sections.push(feature.description.trim().to_string());
    }
    sections.push(format!("Scope: {:?}.", feature.scope));
    sections.push(format!("Rationale: {}", feature.rationale.trim()));
    for (label, values) in [
        ("Acceptance", &feature.acceptance),
        ("Verification", &feature.verification),
        ("Dependencies", &feature.dependencies),
        ("Risks", &feature.risks),
    ] {
        if !values.is_empty() {
            sections.push(format!("{label}: {}", values.join("; ")));
        }
    }
    sections.push(format!("Confidence: {}.", feature.confidence.trim()));
    sections.join("\n\n")
}

async fn accept_goal_scaffold(
    State(state): State<AppState>,
    AxumPath(run_id): AxumPath<String>,
    Json(body): Json<GoalScaffoldAccept>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    validate_text(&body.title, "title", 200)?;
    if !body.description.is_empty() {
        validate_text(&body.description, "description", 4000)?;
    }
    if body.features.is_empty() {
        return Err(ApiError::bad("accept at least one scaffold feature"));
    }
    if body.features.len() > 50 {
        return Err(ApiError::bad("too many scaffold features"));
    }
    let run = state
        .runner
        .get(&run_id)
        .await
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "scaffold run not found"))?;
    if run.status != "completed" {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "scaffold run is not complete",
        ));
    }
    parse_scaffold_proposal(run.final_message.as_deref().unwrap_or_default())?;
    let mut total_steps = 0usize;
    let mut features = Vec::with_capacity(body.features.len());
    for feature in &body.features {
        validate_text(&feature.title, "feature title", 200)?;
        validate_text(&feature.rationale, "feature rationale", 4000)?;
        if !matches!(feature.confidence.as_str(), "high" | "medium" | "low") {
            return Err(ApiError::bad(
                "feature confidence must be high, medium, or low",
            ));
        }
        total_steps += feature.steps.len();
        if total_steps > 250 {
            return Err(ApiError::bad("too many scaffold steps"));
        }
        let steps = feature
            .steps
            .iter()
            .map(|step| {
                validate_text(&step.title, "step title", 500)?;
                Ok(Step {
                    id: step.step_id.clone(),
                    title: step.title.clone(),
                    done: false,
                    next: false,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        features.push(Feature {
            id: feature.feature_id.clone(),
            title: feature.title.clone(),
            description: scaffold_feature_description(feature),
            status: Status::Planned,
            steps,
        });
    }
    let greenfield_project = if body.primary_type == GoalType::Greenfield {
        if !body.project_location_confirmed {
            return Err(ApiError::bad(
                "confirm the Greenfield application location before accepting the plan",
            ));
        }
        Some(prepare_greenfield_project_path(
            &state.browse_root,
            body.project_path
                .as_deref()
                .ok_or_else(|| ApiError::bad("Greenfield project path is required"))?,
        )?)
    } else {
        None
    };
    select_initial_scaffold_step(&mut features, greenfield_project.is_some())?;
    let goal_id = body
        .goal_id
        .unwrap_or_else(|| generate_goal_id(&body.title));
    let mut description_sections = Vec::new();
    if !body.description.trim().is_empty() {
        description_sections.push(body.description.trim().to_string());
    }
    description_sections.push(format!("Goal type: {:?}.", body.primary_type));
    if let Some(path) = &greenfield_project {
        description_sections.push(format!("Project location: {}", path.display()));
    }
    let open_decisions = body
        .decisions_required
        .iter()
        .filter(|decision| greenfield_project.is_none() || !is_project_location_decision(decision))
        .cloned()
        .collect::<Vec<_>>();
    for (label, values) in [
        ("Success criteria", &body.success_criteria),
        ("Assumptions", &body.assumptions),
        ("Open decisions", &open_decisions),
        ("Non-goals", &body.non_goals),
    ] {
        if !values.is_empty() {
            description_sections.push(format!("{label}: {}", values.join("; ")));
        }
    }
    let description = description_sections.join("\n\n");
    let tracker = greenfield_project
        .as_ref()
        .map(|path| Tracker::new(path.join(".goal-manager")))
        .unwrap_or(root_tracker(&state).await);
    let goal = tracker.create_goal_with_features(&goal_id, &body.title, &description, features)?;
    tracker.validate(&goal_id)?;
    if let Some(path) = greenfield_project {
        let record = state
            .registry
            .add(&path, true)
            .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?;
        activate_project(&state, record).await?;
    }
    Ok((StatusCode::CREATED, Json(goal)))
}
async fn get_goal(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(root_tracker(&state).await.get_goal(&id)?))
}
async fn add_feature(
    State(state): State<AppState>,
    AxumPath(goal): AxumPath<String>,
    Json(body): Json<FeatureCreate>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    validate_text(&body.title, "title", 200)?;
    Ok((
        StatusCode::CREATED,
        Json(root_tracker(&state).await.add_feature(
            &goal,
            &body.feature_id,
            &body.title,
            &body.description,
            body.status,
        )?),
    ))
}
async fn set_feature_status(
    State(state): State<AppState>,
    AxumPath((goal, feature)): AxumPath<(String, String)>,
    Json(body): Json<StatusUpdate>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(root_tracker(&state).await.set_status(
        &goal,
        &feature,
        body.status,
    )?))
}
async fn add_step(
    State(state): State<AppState>,
    AxumPath((goal, feature)): AxumPath<(String, String)>,
    Json(body): Json<StepCreate>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    validate_text(&body.title, "title", 500)?;
    Ok((
        StatusCode::CREATED,
        Json(root_tracker(&state).await.add_step(
            &goal,
            &feature,
            &body.step_id,
            &body.title,
            body.done,
            body.next,
        )?),
    ))
}
async fn set_step(
    State(state): State<AppState>,
    AxumPath((goal, feature, step)): AxumPath<(String, String, String)>,
    Json(body): Json<StepUpdate>,
) -> Result<Json<Value>, ApiError> {
    if body.done.is_none() && body.next.is_none() {
        return Err(ApiError::bad("provide done or next"));
    }
    let tracker = root_tracker(&state).await;
    let mut result = None;
    if let Some(done) = body.done {
        result = Some(tracker.set_step(&goal, &feature, &step, done)?);
    }
    if let Some(next) = body.next {
        result = Some(tracker.set_next(&goal, &feature, &step, next)?);
    }
    Ok(Json(result.expect("step update was validated")))
}
async fn add_slice(
    State(state): State<AppState>,
    AxumPath(goal): AxumPath<String>,
    Json(body): Json<SliceCreate>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    validate_text(&body.summary, "summary", 4000)?;
    Ok((
        StatusCode::CREATED,
        Json(root_tracker(&state).await.add_slice(
            &goal,
            &body.feature_id,
            &body.summary,
            body.status,
            body.evidence,
        )?),
    ))
}
async fn validate_goal(
    State(state): State<AppState>,
    AxumPath(goal): AxumPath<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(root_tracker(&state).await.validate(&goal)?))
}

async fn create_run(
    State(state): State<AppState>,
    Json(body): Json<CodexRunCreate>,
) -> Result<impl IntoResponse, ApiError> {
    if body.prompt.trim().is_empty() && body.images.is_empty() && body.files.is_empty() {
        return Err(ApiError::bad("prompt or attachment must not be empty"));
    }
    if !body.prompt.is_empty() {
        validate_text(&body.prompt, "prompt", 20_000)?;
    }
    let work_mode = if let Some(mode) = body.work_mode {
        mode
    } else if let Some(thread_id) = body.thread_id.as_deref() {
        state
            .runner
            .thread_work_mode(thread_id)
            .await
            .unwrap_or_default()
    } else {
        WorkMode::default()
    };
    let execution_prompt = if let Some(goal) = &body.goal_id {
        root_tracker(&state).await.get_goal(goal)?;
        Some(goal_execution_prompt(goal, &work_mode))
    } else {
        None
    };
    let mut thread_id = body.thread_id.clone();
    if let (Some(existing_thread_id), Some(instructions)) =
        (thread_id.as_deref(), execution_prompt.as_deref())
    {
        let continued = state
            .runner
            .continue_thread_in_mode(existing_thread_id, work_mode.clone(), instructions)
            .await
            .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "thread not found"))?;
        thread_id = Some(continued.id);
    }
    let run = state
        .runner
        .create(
            body.prompt,
            body.images,
            body.files,
            body.working_directory,
            body.sandbox,
            body.approval_policy,
            body.approvals_reviewer,
            false,
            body.goal_id,
            thread_id,
            execution_prompt,
            None,
            work_mode,
            body.llm_config,
        )
        .await
        .map_err(ApiError::bad)?;
    let location = HeaderValue::from_str(&format!("/runs/{}", run.id))
        .unwrap_or(HeaderValue::from_static("/runs"));
    let mut response = (
        StatusCode::ACCEPTED,
        Json(RunManager::redacted_for_display(&run)),
    )
        .into_response();
    response.headers_mut().insert(header::LOCATION, location);
    Ok(response)
}
async fn get_run_image(
    State(state): State<AppState>,
    AxumPath((run_id, image_id)): AxumPath<(String, String)>,
) -> Result<Response, ApiError> {
    let run = state
        .runner
        .get(&run_id)
        .await
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "run not found"))?;
    let image = run
        .images
        .iter()
        .find(|image| image.id == image_id)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "image not found"))?;
    let attachment_root = state
        .runner
        .workspace_root()
        .await
        .join(".goal-manager")
        .join("attachments")
        .join(&run_id)
        .canonicalize()
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "image file not found"))?;
    let image_path = PathBuf::from(&image.path)
        .canonicalize()
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "image file not found"))?;
    if !image_path.starts_with(&attachment_root) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "image path is outside the run attachment directory",
        ));
    }
    let bytes = tokio::fs::read(image_path)
        .await
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "image file not found"))?;
    let content_type = HeaderValue::from_str(&image.mime_type)
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid image type"))?;
    let mut response = bytes.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    Ok(response)
}
async fn list_runs(State(state): State<AppState>) -> Json<Vec<Run>> {
    Json(
        state
            .runner
            .list()
            .await
            .iter()
            .map(RunManager::redacted_for_display)
            .collect(),
    )
}
async fn get_run(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Run>, ApiError> {
    state
        .runner
        .get(&id)
        .await
        .map(|run| Json(RunManager::redacted_for_display(&run)))
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "run not found"))
}
async fn get_usage(State(state): State<AppState>) -> Json<Value> {
    Json(state.runner.usage().await)
}
async fn get_default_llm_config(State(state): State<AppState>) -> Json<LLMConfig> {
    Json(state.runner.default_llm_config().await)
}
async fn set_default_llm_config(
    State(state): State<AppState>,
    Json(body): Json<LLMConfig>,
) -> Result<Json<LLMConfig>, ApiError> {
    validate_text(&body.model, "model", 200).or_else(|error| {
        if body.model.is_empty() {
            Ok(())
        } else {
            Err(error)
        }
    })?;
    Ok(Json(
        state
            .runner
            .set_default_llm_config(body)
            .await
            .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?,
    ))
}
async fn get_codex_auth_status(State(state): State<AppState>) -> Json<CodexAuthStatus> {
    Json(state.runner.codex_auth_status().await)
}
async fn start_codex_login(
    State(state): State<AppState>,
) -> Result<(StatusCode, Json<CodexAuthAction>), ApiError> {
    Ok((
        StatusCode::ACCEPTED,
        Json(
            state
                .runner
                .start_codex_login()
                .await
                .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?,
        ),
    ))
}
async fn logout_codex(State(state): State<AppState>) -> Result<Json<CodexAuthAction>, ApiError> {
    Ok(Json(state.runner.codex_logout().await.map_err(
        |error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error),
    )?))
}
async fn get_profile(State(state): State<AppState>) -> Json<UserProfile> {
    let profile = match tokio::fs::read_to_string(&state.profile_path).await {
        Ok(value) => serde_json::from_str(&value).unwrap_or_default(),
        Err(_) => UserProfile::default(),
    };
    Json(profile)
}
async fn update_profile(
    State(state): State<AppState>,
    Json(mut profile): Json<UserProfile>,
) -> Result<Json<UserProfile>, ApiError> {
    profile.display_name = profile.display_name.trim().to_string();
    profile.role = profile.role.trim().to_string();
    if profile.display_name.len() > 80 {
        return Err(ApiError::bad("display_name is too long"));
    }
    if profile.role.len() > 120 {
        return Err(ApiError::bad("role is too long"));
    }
    let parent = state
        .profile_path
        .parent()
        .ok_or_else(|| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "invalid profile path"))?;
    tokio::fs::create_dir_all(parent)
        .await
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let temporary = state.profile_path.with_extension("tmp");
    let bytes = serde_json::to_vec_pretty(&profile)
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    tokio::fs::write(&temporary, bytes)
        .await
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    tokio::fs::rename(&temporary, &state.profile_path)
        .await
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(profile))
}
async fn list_threads(State(state): State<AppState>) -> Json<Vec<crate::runner::ThreadSummary>> {
    Json(state.runner.list_threads().await)
}
#[derive(Deserialize)]
struct AppServerThreadQuery {
    cursor: Option<String>,
    limit: Option<u32>,
}
async fn list_app_server_threads(
    State(state): State<AppState>,
    Query(query): Query<AppServerThreadQuery>,
) -> Result<Json<AppServerThreadPage>, ApiError> {
    state
        .runner
        .app_server_threads(query.cursor, query.limit)
        .await
        .map(Json)
        .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))
}
async fn get_app_server_thread(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<AppServerThread>, ApiError> {
    validate_text(&id, "thread_id", 200)?;
    state
        .runner
        .app_server_thread(&id)
        .await
        .map(Json)
        .map_err(|error| {
            let status = if error.contains("does not belong") {
                StatusCode::FORBIDDEN
            } else {
                StatusCode::BAD_GATEWAY
            };
            ApiError::new(status, error)
        })
}
async fn assign_app_server_thread_goal(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<ThreadGoalUpdate>,
) -> Result<Json<AppServerThread>, ApiError> {
    validate_text(&id, "thread_id", 200)?;
    root_tracker(&state).await.get_goal(&body.goal_id)?;
    state
        .runner
        .assign_app_server_thread(&id, body.goal_id)
        .await
        .map(Json)
        .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))
}
async fn stream_app_server_events(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(state.runner.app_server_events()).filter_map(|message| {
        message.ok().and_then(|event| {
            Event::default()
                .id(event.sequence.to_string())
                .event("app-server")
                .json_data(event)
                .ok()
                .map(Ok)
        })
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
async fn list_app_server_approvals(State(state): State<AppState>) -> Json<Vec<AppServerApproval>> {
    Json(state.runner.pending_approvals().await)
}
async fn get_app_server_rate_limits(
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    state
        .runner
        .app_server_rate_limits()
        .await
        .map(Json)
        .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))
}
async fn get_app_server_models(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    state
        .runner
        .app_server_models()
        .await
        .map(Json)
        .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))
}
async fn get_app_server_account(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    state
        .runner
        .account()
        .await
        .map(Json)
        .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))
}
async fn decide_app_server_approval(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<ApprovalDecision>,
) -> Result<StatusCode, ApiError> {
    state
        .runner
        .decide_approval(&id, &body.decision)
        .await
        .map_err(ApiError::bad)?;
    Ok(StatusCode::NO_CONTENT)
}
async fn update_thread(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<ThreadUpdate>,
) -> Result<Json<crate::runner::ThreadSummary>, ApiError> {
    if body.title.is_none() && body.llm_config.is_none() && body.work_mode.is_none() {
        return Err(ApiError::bad("title, llm_config, or work_mode is required"));
    }
    if let Some(title) = &body.title {
        validate_text(title, "title", 200)?;
    }
    let mut target_id = id.clone();
    if let Some(work_mode) = body.work_mode.clone() {
        let goal_id = state.runner.thread_goal_id(&id).await.ok_or_else(|| {
            ApiError::new(
                StatusCode::CONFLICT,
                "assign this thread to a goal before changing its work mode",
            )
        })?;
        let instructions = goal_execution_prompt(&goal_id, &work_mode);
        target_id = state
            .runner
            .continue_thread_in_mode(&id, work_mode, &instructions)
            .await
            .map_err(|error| ApiError::new(StatusCode::BAD_GATEWAY, error))?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "thread not found"))?
            .id;
    }
    state
        .runner
        .update_thread(&target_id, body.title, body.llm_config, None)
        .await
        .map_err(|error| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, error))?
        .map(Json)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "thread not found"))
}

async fn static_asset(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let asset = if path.is_empty() { "index.html" } else { path };
    if let Some(content) = Frontend::get(asset) {
        let mime = mime_guess::from_path(asset).first_or_octet_stream();
        ([(header::CONTENT_TYPE, mime.as_ref())], content.data).into_response()
    } else if let Some(index) = Frontend::get("index.html") {
        (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            index.data,
        )
            .into_response()
    } else {
        (StatusCode::NOT_FOUND, "frontend not built").into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use std::fs;
    use tower::ServiceExt;

    async fn test_app() -> (Router, tempfile::TempDir) {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        fs::create_dir(&project).unwrap();
        let registry = ProjectRegistry::new(temp.path().join("app/projects.json"));
        let active = registry.add(&project, true).unwrap();
        let runner = RunManager::new(
            project.clone(),
            "missing-codex".into(),
            project.join(".goal-manager/run-history.json"),
        )
        .await;
        let state = AppState {
            tracker_root: Arc::new(RwLock::new(project.join(".goal-manager"))),
            runner,
            browse_root: temp.path().to_path_buf(),
            registry,
            active_project_id: Arc::new(RwLock::new(active.id)),
            profile_path: temp.path().join("app/profile.json"),
        };
        (router(state), temp)
    }

    async fn json_request(
        app: Router,
        method: &str,
        uri: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn test_confirmation(action: &str, target: &str) -> Value {
        json!({
            "action": action,
            "target": target,
            "confirmedBy": "api-test-user",
            "reason": format!("Test confirms {action} on {target}")
        })
    }

    async fn v1_pool_command(
        app: Router,
        goal_id: &str,
        idempotency_key: &str,
        expected_resource_version: Option<&str>,
        command: Value,
    ) -> (StatusCode, Value) {
        let confirmation_action = match command["action"].as_str() {
            Some("configure")
                if command.get("userPolicy").is_some()
                    || command.get("permissionPolicy").is_some()
                    || command.get("resourcePolicy").is_some() =>
            {
                Some("pool.configure_policy")
            }
            Some("start")
                if command.get("userPolicy").is_some()
                    || command.get("permissionPolicy").is_some()
                    || command.get("resourcePolicy").is_some() =>
            {
                Some("pool.start_policy")
            }
            Some("stop") => Some("pool.stop"),
            _ => None,
        };
        json_request(
            app,
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/pool/commands"),
            json!({
                "idempotencyKey": idempotency_key,
                "expectedResourceVersion": expected_resource_version,
                "confirmation": confirmation_action.map(|action| test_confirmation(action, goal_id)),
                "command": command,
            }),
        )
        .await
    }

    async fn v1_claim_command(
        app: Router,
        goal_id: &str,
        claim_id: &str,
        idempotency_key: &str,
        expected_resource_version: Option<&str>,
        command: Value,
    ) -> (StatusCode, Value) {
        let confirmation_action = match command["action"].as_str() {
            Some("cancel") => Some("claim.cancel"),
            Some("reassign") => Some("claim.reassign"),
            _ => None,
        };
        json_request(
            app,
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/claims/{claim_id}/commands"),
            json!({
                "idempotencyKey": idempotency_key,
                "expectedResourceVersion": expected_resource_version,
                "confirmation": confirmation_action.map(|action| test_confirmation(action, claim_id)),
                "command": command,
            }),
        )
        .await
    }

    #[test]
    fn suggests_a_dedicated_greenfield_project_location() {
        let temp = tempfile::tempdir().unwrap();
        let code = temp.path().join("Code");
        let current = code.join("ExistingApp");
        fs::create_dir_all(&current).unwrap();
        assert_eq!(
            suggested_greenfield_path(temp.path(), temp.path(), "Homework helper"),
            code.join("HomeworkHelper")
        );
        assert_eq!(
            suggested_greenfield_path(&current, temp.path(), "Homework helper"),
            code.join("HomeworkHelper")
        );
    }

    #[test]
    fn creates_only_a_confirmed_greenfield_folder_inside_the_browse_root() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("HomeworkHelper");
        let created =
            prepare_greenfield_project_path(temp.path(), target.to_str().unwrap()).unwrap();
        assert_eq!(created, target.canonicalize().unwrap());
        assert!(created.is_dir());

        let outside = tempfile::tempdir().unwrap();
        let error = prepare_greenfield_project_path(
            temp.path(),
            outside.path().join("OutsideApp").to_str().unwrap(),
        )
        .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn coordination_api_contract_is_discoverable_and_versioned() {
        let (app, _temp) = test_app().await;
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/coordination/v1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["apiVersion"], "v1");
        assert!(
            body["resources"]
                .as_array()
                .unwrap()
                .contains(&json!("claims"))
        );
        assert!(
            body["resources"]
                .as_array()
                .unwrap()
                .contains(&json!("claim-traces"))
        );
        assert!(
            body["resources"]
                .as_array()
                .unwrap()
                .contains(&json!("metrics"))
        );
        assert!(
            body["resources"]
                .as_array()
                .unwrap()
                .contains(&json!("health"))
        );
        assert!(
            body["resources"]
                .as_array()
                .unwrap()
                .contains(&json!("operational-alerts"))
        );
        assert!(
            body["commandGroups"]
                .as_array()
                .unwrap()
                .contains(&json!("integration-lifecycle"))
        );

        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals",
            json!({"goal_id":"metrics-api","title":"Metrics API"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, metrics) = json_request(
            app.clone(),
            "GET",
            "/coordination/v1/goals/metrics-api/metrics",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{metrics}");
        assert_eq!(metrics["kind"], "goal_metrics");
        assert_eq!(metrics["data"]["goalId"], "metrics-api");
        for section in [
            "utilization",
            "readyQueue",
            "claims",
            "throughput",
            "failures",
            "supervisor",
            "notifications",
            "conflicts",
            "integration",
        ] {
            assert!(metrics["data"].get(section).is_some(), "missing {section}");
        }
        let (status, health) = json_request(
            app.clone(),
            "GET",
            "/coordination/v1/goals/metrics-api/health",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{health}");
        assert_eq!(health["kind"], "goal_health");
        assert_eq!(health["data"]["schema"], "golazo.goal-health.v1");
        assert_eq!(health["data"]["operatingMode"], "idle");
        assert_eq!(health["data"]["components"].as_array().unwrap().len(), 11);
        let (status, alerts) = json_request(
            app,
            "GET",
            "/coordination/v1/goals/metrics-api/alerts",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{alerts}");
        assert_eq!(alerts["kind"], "operational_alerts");
        assert_eq!(alerts["data"]["schema"], "golazo.operational-alerts.v1");
        assert!(alerts["data"]["activeAlerts"].is_array());
        assert!(alerts["data"]["resolvedAlerts"].is_array());
    }

    async fn read_sse_until(response: axum::response::Response, marker: &str) -> String {
        let mut stream = response.into_body().into_data_stream();
        let mut output = String::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(bytes))) => {
                    output.push_str(&String::from_utf8_lossy(&bytes));
                    if output.contains(marker) {
                        return output;
                    }
                }
                Ok(Some(Err(error))) => panic!("SSE body failed: {error}"),
                Ok(None) => break,
                Err(_) => break,
            }
        }
        panic!("SSE output did not contain {marker:?}: {output}");
    }

    #[tokio::test]
    async fn coordination_event_stream_replays_reconnects_polls_and_bounds_payloads() {
        use crate::coordination::domain::CoordinationEventKind;

        let (app, temp) = test_app().await;
        let goal_id = "event-stream-api";
        let now = Utc::now();
        let store = SqliteCoordinationStore::open(
            temp.path()
                .join("project/.goal-manager/coordination.sqlite"),
        )
        .unwrap();
        for index in 1..=3 {
            store
                .append_event(&CoordinationEvent::new(
                    goal_id,
                    CoordinationEventKind::ActivityPublished,
                    EventSeverity::Info,
                    CoordinationActor::System,
                    "stream-replay",
                    json!({"index": index}),
                    now,
                ))
                .unwrap();
        }

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/coordination/v1/goals/{goal_id}/events/stream?limit=2"
                    ))
                    .header("last-event-id", "1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/event-stream"
        );
        let replay = read_sse_until(response, "id: 3").await;
        assert!(replay.contains("id: 2"), "{replay}");
        assert!(replay.contains("event: coordination-event"), "{replay}");

        let live_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/coordination/v1/goals/{goal_id}/events/stream?afterSequence=3"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live_response.status(), StatusCode::OK);
        let oversized = CoordinationEvent::new(
            goal_id,
            CoordinationEventKind::ActivityPublished,
            EventSeverity::Warning,
            CoordinationActor::System,
            "stream-live",
            json!({"blob": "x".repeat(MAX_COORDINATION_SSE_DATA_BYTES * 2)}),
            now,
        );
        store.append_event(&oversized).unwrap();
        let live = read_sse_until(live_response, "\"payloadOmitted\":true").await;
        assert!(live.contains("id: 4"), "{live}");
        assert!(live.len() < MAX_COORDINATION_SSE_DATA_BYTES, "{live}");

        let invalid = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/coordination/v1/goals/{goal_id}/events/stream?limit=201"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let problem: Value =
            serde_json::from_slice(&invalid.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(problem["code"], "invalid_request");
    }

    #[tokio::test]
    async fn coordination_api_conformance_covers_validation_races_authorization_and_v1_compatibility()
     {
        let (app, temp) = test_app().await;
        let goal_id = "api-conformance";
        let tracker_root = temp.path().join("project/.goal-manager");
        let tracker = Tracker::new(&tracker_root);
        tracker.create_goal(goal_id, "API conformance", "").unwrap();
        for feature in ["race-feature", "compat-feature"] {
            tracker
                .add_feature(goal_id, feature, feature, "", Status::Planned)
                .unwrap();
        }

        let now = Utc::now();
        let store =
            SqliteCoordinationStore::open(tracker_root.join("coordination.sqlite")).unwrap();
        let mut first_worker = Worker::new(goal_id, now);
        first_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        first_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        let mut second_worker = Worker::new(goal_id, now);
        second_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        second_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        store.upsert_worker(&first_worker).unwrap();
        store.upsert_worker(&second_worker).unwrap();

        let claim_uri = format!("/coordination/v1/goals/{goal_id}/claims/commands");
        let first_request = json_request(
            app.clone(),
            "POST",
            &claim_uri,
            json!({
                "idempotencyKey": "race-first",
                "command": {
                    "workerId": first_worker.id.as_str(),
                    "requestedScope": {"kind": "feature", "feature_id": "race-feature"},
                    "baseRevision": "base-race"
                }
            }),
        );
        let second_request = json_request(
            app.clone(),
            "POST",
            &claim_uri,
            json!({
                "idempotencyKey": "race-second",
                "command": {
                    "workerId": second_worker.id.as_str(),
                    "requestedScope": {"kind": "feature", "feature_id": "race-feature"},
                    "baseRevision": "base-race"
                }
            }),
        );
        let ((first_status, first_body), (second_status, second_body)) =
            tokio::join!(first_request, second_request);
        let (winner, conflict) = match (first_status, second_status) {
            (StatusCode::OK, StatusCode::CONFLICT) => (first_body, second_body),
            (StatusCode::CONFLICT, StatusCode::OK) => (second_body, first_body),
            statuses => panic!(
                "exactly one concurrent claim must win, got {statuses:?}: {first_body} / {second_body}"
            ),
        };
        assert_eq!(conflict["code"], "conflict");
        assert_eq!(conflict["retryable"], true);
        assert_eq!(
            store
                .claims_for_goal(goal_id, Some(ClaimState::Active))
                .unwrap()
                .len(),
            1
        );
        let winner_worker_id = winner["data"]["owner"].as_str().unwrap().to_string();

        let (status, invalid_limit) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/workers?limit=201"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{invalid_limit}");
        assert_eq!(invalid_limit["code"], "invalid_request");
        let (status, invalid_command) = json_request(
            app.clone(),
            "POST",
            &claim_uri,
            json!({
                "idempotencyKey": "",
                "command": {
                    "workerId": winner_worker_id.as_str(),
                    "requestedScope": {"kind": "feature", "feature_id": "compat-feature"},
                    "baseRevision": "base-v1"
                }
            }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "{invalid_command}"
        );
        assert_eq!(invalid_command["code"], "invalid_request");

        let mut foreign_worker = Worker::new("other-goal", now);
        foreign_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        foreign_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        let foreign_claim = Claim::new(
            "other-goal",
            ClaimScope::Feature {
                feature_id: "foreign-feature".into(),
            },
            foreign_worker.id.clone(),
            "base-foreign",
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        foreign_worker.active_claims.push(foreign_claim.id.clone());
        store.upsert_worker(&foreign_worker).unwrap();
        store.insert_claim(&foreign_claim).unwrap();

        let (status, hidden_worker) = json_request(
            app.clone(),
            "GET",
            &format!(
                "/coordination/v1/goals/other-goal/workers/{}",
                first_worker.id.as_str()
            ),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{hidden_worker}");
        assert_eq!(hidden_worker["code"], "not_found");
        let (status, forbidden_event) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/events/commands"),
            json!({
                "idempotencyKey": "foreign-worker-event",
                "command": {
                    "severity": "info",
                    "producer": {
                        "kind": "worker",
                        "worker_id": foreign_worker.id.as_str()
                    },
                    "correlationId": foreign_claim.id.as_str(),
                    "payload": {
                        "type": "activity",
                        "data": {
                            "workerId": foreign_worker.id.as_str(),
                            "claimId": foreign_claim.id.as_str(),
                            "category": "progress",
                            "summary": "cross-goal publication must be rejected",
                            "progressPercent": 10,
                            "changedScope": [],
                            "evidenceRefs": ["test:authorization"]
                        }
                    }
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{forbidden_event}");
        assert_eq!(forbidden_event["code"], "forbidden");

        let (status, compatible) = json_request(
            app.clone(),
            "POST",
            &claim_uri,
            json!({
                "idempotencyKey": "compat-acquire",
                "futureEnvelopeField": {"ignored": true},
                "command": {
                    "workerId": winner_worker_id.as_str(),
                    "requestedScope": {
                        "kind": "feature",
                        "feature_id": "compat-feature",
                        "futureScopeField": true
                    },
                    "baseRevision": "base-v1",
                    "futureCommandField": "ignored"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{compatible}");
        assert_eq!(compatible["apiVersion"], "v1");
        assert_eq!(compatible["kind"], "claim");
        for field in ["id", "resourceVersion", "createdAt", "updatedAt"] {
            assert!(
                compatible["metadata"].get(field).is_some(),
                "missing stable v1 metadata field {field}: {compatible}"
            );
        }
        assert_eq!(compatible["data"]["goalId"], goal_id);
        assert_eq!(compatible["data"]["state"], "active");
        assert_eq!(compatible["data"]["scope"]["kind"], "feature");
        let compat_claim_id = compatible["metadata"]["id"].as_str().unwrap().to_string();
        let compat_version = compatible["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, first_page) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/claims?limit=1"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{first_page}");
        assert_eq!(first_page["apiVersion"], "v1");
        assert_eq!(first_page["items"].as_array().unwrap().len(), 1);
        let first_page_id = first_page["items"][0]["metadata"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let cursor = first_page["nextCursor"].as_str().unwrap().to_string();
        let (status, second_page) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/claims?limit=1&cursor={cursor}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second_page}");
        assert_eq!(second_page["items"].as_array().unwrap().len(), 1);
        assert_ne!(
            second_page["items"][0]["metadata"]["id"].as_str().unwrap(),
            first_page_id.as_str()
        );

        let (status, heartbeat) = v1_claim_command(
            app.clone(),
            goal_id,
            &compat_claim_id,
            "compat-heartbeat",
            Some(&compat_version),
            json!({
                "action": "heartbeat",
                "workerId": winner_worker_id.as_str(),
                "expectedGeneration": 1
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{heartbeat}");
        assert_ne!(
            heartbeat["metadata"]["resourceVersion"],
            compat_version.as_str()
        );
        let (status, stale) = v1_claim_command(
            app,
            goal_id,
            &compat_claim_id,
            "compat-stale-release",
            Some(&compat_version),
            json!({
                "action": "release",
                "workerId": winner_worker_id.as_str(),
                "expectedGeneration": 1,
                "reason": "stale compatibility request"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{stale}");
        assert_eq!(stale["code"], "stale_revision");
    }

    #[tokio::test]
    async fn coordination_pool_routes_are_versioned_idempotent_and_revision_checked() {
        let (app, temp) = test_app().await;
        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals",
            json!({"goal_id":"pool-api","title":"Pool API"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, configured) = v1_pool_command(
            app.clone(),
            "pool-api",
            "configure-1",
            None,
            json!({
                "action":"configure",
                "desiredConcurrency":1,
                "userPolicy":{
                    "sandbox":"danger-full-access",
                    "approvalPolicy":"never",
                    "approvalsReviewer":"user",
                    "networkAccess":true,
                    "toolCapabilities":["read_files","write_files","run_commands","git","network"]
                },
                "permissionPolicy":{
                    "sandbox":"workspace-write",
                    "approvalPolicy":"on-request",
                    "approvalsReviewer":"user",
                    "networkAccess":false,
                    "toolCapabilities":["read_files","write_files","run_commands","git"]
                },
                "resourcePolicy":{
                    "maxWorkerTokens":100000,
                    "maxGoalTokens":400000,
                    "maxWorkerTurnSeconds":900,
                    "maxGoalElapsedSeconds":7200,
                    "maxWorkerProcesses":4,
                    "maxGoalProcesses":16,
                    "maxWorkerDiskBytes":1073741824,
                    "maxGoalDiskBytes":4294967296_u64,
                    "maxWorkerNetworkRequests":0,
                    "maxGoalNetworkRequests":0,
                    "maxWorkerRetries":2,
                    "maxGoalRetries":8,
                    "maxGoalConcurrency":4
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{configured}");
        assert_eq!(configured["apiVersion"], "v1");
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["desiredConcurrency"],
            1
        );
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["workerPermissions"]["sandbox"],
            "workspace-write"
        );
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["workerPermissions"]["networkAccess"],
            false
        );
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["workerPermissions"]["derivedFrom"]["policyVersion"],
            1
        );
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["resourceQuotas"]["maxWorkerTokens"],
            100000
        );
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["resourceQuotas"]["maxGoalTokens"],
            400000
        );
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["resourceQuotas"]["maxGoalConcurrency"],
            4
        );
        let configured_version = configured["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, replayed) = v1_pool_command(
            app.clone(),
            "pool-api",
            "configure-1",
            None,
            json!({"action":"configure","desiredConcurrency":4}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed, configured);

        let (status, stale) = v1_pool_command(
            app.clone(),
            "pool-api",
            "pause-stale",
            Some("stale-revision"),
            json!({"action":"pause"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stale["code"], "stale_revision");
        assert_eq!(stale["retryable"], true);

        let (status, unconfirmed) = json_request(
            app.clone(),
            "POST",
            "/coordination/v1/goals/pool-api/pool/commands",
            json!({
                "idempotencyKey": "unconfirmed-stop",
                "command": {"action": "stop"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_REQUIRED, "{unconfirmed}");
        assert_eq!(unconfirmed["code"], "confirmation_required");
        assert_eq!(unconfirmed["details"]["action"], "pool.stop");
        assert_eq!(unconfirmed["details"]["target"], "pool-api");

        let mut version = configured_version;
        for (key, action, mode) in [
            ("start-1", json!({"action":"start"}), "running"),
            ("pause-1", json!({"action":"pause"}), "paused"),
            ("resume-1", json!({"action":"resume"}), "running"),
            ("reconcile-1", json!({"action":"reconcile"}), "running"),
            ("drain-1", json!({"action":"drain"}), "draining"),
            ("stop-1", json!({"action":"stop"}), "stopped"),
        ] {
            let (status, body) =
                v1_pool_command(app.clone(), "pool-api", key, Some(&version), action).await;
            assert_eq!(status, StatusCode::OK, "{key}: {body}");
            assert_eq!(body["data"]["snapshot"]["goal"]["mode"], mode);
            version = body["metadata"]["resourceVersion"]
                .as_str()
                .unwrap()
                .to_string();
        }

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/coordination/v1/goals/pool-api/pool")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["metadata"]["resourceVersion"], version);
        assert_eq!(body["data"]["goal"]["mode"], "stopped");

        let store = SqliteCoordinationStore::open(
            temp.path()
                .join("project/.goal-manager/coordination.sqlite"),
        )
        .unwrap();
        let privileged = store
            .latest_events_for_goal("pool-api", None, 100)
            .unwrap()
            .into_iter()
            .filter_map(|event| event.typed_payload().unwrap())
            .filter_map(|payload| match payload {
                CoordinationEventPayload::PrivilegedAction(payload) => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(privileged.len(), 2);
        let stop = privileged
            .iter()
            .find(|payload| payload.action == "pool.stop")
            .unwrap();
        assert_eq!(stop.actor, "api-test-user");
        assert_eq!(stop.authority, "explicit_user_confirmation");
        assert_eq!(stop.target, "pool-api");
        assert_eq!(stop.decision, "Test confirms pool.stop on pool-api");
        assert_eq!(stop.outcome, "succeeded");
        assert!(
            stop.evidence_refs
                .iter()
                .any(|evidence| evidence.contains("stop-1"))
        );
        assert!(
            stop.evidence_refs
                .iter()
                .any(|evidence| evidence.starts_with("outcome-version:"))
        );
    }

    #[tokio::test]
    async fn coordination_worker_routes_expose_runtime_context_and_safe_lifecycle_commands() {
        use crate::coordination::domain::{
            ActivityCategory, ActivityEventPayload, ClaimId, CoordinationActor,
            CoordinationEventPayload, EventSeverity, WorkerTurnBinding,
        };

        let (app, temp) = test_app().await;
        let now = Utc::now();
        let store = SqliteCoordinationStore::open(
            temp.path()
                .join("project/.goal-manager/coordination.sqlite"),
        )
        .unwrap();
        let mut worker = Worker::new("worker-api", now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        worker.workspace = Some(WorkspaceBinding {
            repository_id: "repo-api".into(),
            canonical_repository_path: "/repo".into(),
            worktree_path: "/repo/worktrees/worker-api".into(),
            branch: "codex/worker-api".into(),
            base_revision: "base-api".into(),
            created_at: Some(now),
            creation_evidence: vec!["fixture".into()],
        });
        worker.current_run_id = Some("run-api".into());
        worker.current_thread_id = Some("thread-api".into());
        worker.turn_history.push(WorkerTurnBinding {
            sequence: 1,
            run_id: "run-api".into(),
            thread_id: "thread-api".into(),
            started_at: now,
            completed_at: None,
            continuation_of_thread_id: Some("thread-before-api".into()),
            context_transfer_artifact_id: Some("context-api".into()),
        });
        store.upsert_worker(&worker).unwrap();
        let event = CoordinationEvent::from_typed_payload(
            "worker-api",
            EventSeverity::Info,
            CoordinationActor::Worker {
                worker_id: worker.id.clone(),
            },
            "worker-api-activity",
            CoordinationEventPayload::Activity(ActivityEventPayload {
                worker_id: worker.id.clone(),
                claim_id: ClaimId::new(),
                category: ActivityCategory::Progress,
                summary: "worker route fixture".into(),
                progress_percent: Some(25),
                changed_scope: vec!["rust-backend/api.rs".into()],
                artifact_id: None,
                evidence_refs: vec!["route test".into()],
                validation_succeeded: None,
            }),
            now,
        )
        .unwrap();
        store.append_event(&event).unwrap();
        let worker_id = worker.id.as_str();

        let (status, list) = json_request(
            app.clone(),
            "GET",
            "/coordination/v1/goals/worker-api/workers?limit=1",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["apiVersion"], "v1");
        assert_eq!(list["items"][0]["metadata"]["id"], worker_id);
        assert_eq!(list["items"][0]["data"]["state"], "active");

        let (status, detail) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/worker-api/workers/{worker_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{detail}");
        let active_version = detail["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();

        for (suffix, expected) in [
            ("workspace", "repo-api"),
            ("run", "run-api"),
            ("thread", "thread-api"),
        ] {
            let (status, body) = json_request(
                app.clone(),
                "GET",
                &format!("/coordination/v1/goals/worker-api/workers/{worker_id}/{suffix}"),
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{suffix}: {body}");
            assert!(body.to_string().contains(expected), "{suffix}: {body}");
        }
        let (status, activity) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/worker-api/workers/{worker_id}/activity?limit=1"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{activity}");
        assert_eq!(
            activity["items"][0]["payload"]["data"]["summary"],
            "worker route fixture"
        );

        let command_uri = format!("/coordination/v1/goals/worker-api/workers/{worker_id}/commands");
        let (status, paused) = json_request(
            app.clone(),
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "pause-worker-api",
                "expectedResourceVersion": active_version,
                "command": {"action": "pause", "reason": "operator pause"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{paused}");
        assert_eq!(paused["data"]["state"], "paused");
        let paused_version = paused["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, replayed) = json_request(
            app.clone(),
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "pause-worker-api",
                "confirmation": test_confirmation("worker.cancel", worker.id.as_str()),
                "command": {"action": "cancel", "reason": "must not replace outcome"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed, paused);

        let (status, stale) = json_request(
            app.clone(),
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "resume-worker-stale",
                "expectedResourceVersion": "stale-worker-version",
                "command": {"action": "resume"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stale["code"], "stale_revision");

        let (status, resumed) = json_request(
            app,
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "resume-worker-api",
                "expectedResourceVersion": paused_version,
                "command": {"action": "resume"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resumed}");
        assert_eq!(resumed["data"]["state"], "active");
    }

    #[tokio::test]
    async fn coordination_package_and_claim_routes_cover_the_full_claim_lifecycle() {
        let (app, temp) = test_app().await;
        let goal_id = "claim-api";
        let tracker_root = temp.path().join("project/.goal-manager");
        let tracker = Tracker::new(&tracker_root);
        tracker.create_goal(goal_id, "Claim API", "").unwrap();
        for feature in ["feature-a", "feature-b", "feature-c", "package-feature"] {
            tracker
                .add_feature(goal_id, feature, feature, "", Status::Planned)
                .unwrap();
        }

        let now = Utc::now();
        let store =
            SqliteCoordinationStore::open(tracker_root.join("coordination.sqlite")).unwrap();
        let mut package =
            WorkPackage::new(goal_id, "Package work", vec!["package-feature".into()], now).unwrap();
        package
            .transition(crate::coordination::domain::WorkPackageState::Planned, now)
            .unwrap();
        package
            .transition(crate::coordination::domain::WorkPackageState::Ready, now)
            .unwrap();
        tracker
            .add_work_package(
                goal_id,
                package.id.as_str(),
                "Package work",
                "",
                vec!["package-feature".into()],
                vec![],
                10,
                IntegrationScope::WorkPackage,
            )
            .unwrap();
        store.upsert_work_package(&package).unwrap();

        let mut first_worker = Worker::new(goal_id, now);
        first_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        first_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        let mut second_worker = Worker::new(goal_id, now);
        second_worker
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        second_worker
            .transition(WorkerState::Active, now, None)
            .unwrap();
        store.upsert_worker(&first_worker).unwrap();
        store.upsert_worker(&second_worker).unwrap();

        let (status, ready) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/ready-work?limit=10"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{ready}");
        assert_eq!(ready["items"].as_array().unwrap().len(), 4);

        let (status, packages) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/packages"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{packages}");
        assert_eq!(packages["items"][0]["metadata"]["id"], package.id.as_str());
        let (status, package_detail) = json_request(
            app.clone(),
            "GET",
            &format!(
                "/coordination/v1/goals/{goal_id}/packages/{}",
                package.id.as_str()
            ),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{package_detail}");
        assert_eq!(package_detail["data"]["state"], "ready");

        let acquire = |worker_id: &str, feature: &str, key: &str| {
            json!({
                "idempotencyKey": key,
                "command": {
                    "workerId": worker_id,
                    "requestedScope": {"kind": "feature", "feature_id": feature},
                    "baseRevision": "base-api"
                }
            })
        };
        let (status, acquired) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/claims/commands"),
            acquire(first_worker.id.as_str(), "feature-a", "acquire-a"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{acquired}");
        assert_eq!(acquired["apiVersion"], "v1");
        assert_eq!(acquired["data"]["state"], "active");
        let claim_a = acquired["metadata"]["id"].as_str().unwrap().to_string();
        let acquired_version = acquired["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, claims) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/claims?limit=1"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{claims}");
        assert_eq!(claims["items"][0]["metadata"]["id"], claim_a);
        let (status, detail) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/claims/{claim_a}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{detail}");

        let (status, trace) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/claims/{claim_a}/trace"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{trace}");
        assert_eq!(trace["kind"], "claim_trace");
        assert_eq!(trace["data"]["schema"], "golazo.claim-trace.v1");
        assert_eq!(trace["data"]["claimId"], claim_a);
        assert_eq!(trace["data"]["phases"].as_array().unwrap().len(), 6);
        assert_eq!(trace["data"]["spans"][0]["stage"], "acquisition");

        let (status, heartbeat) = v1_claim_command(
            app.clone(),
            goal_id,
            &claim_a,
            "heartbeat-a",
            Some(&acquired_version),
            json!({
                "action": "heartbeat",
                "workerId": first_worker.id.as_str(),
                "expectedGeneration": 1
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{heartbeat}");
        let heartbeat_version = heartbeat["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(heartbeat_version, acquired_version);

        let (status, stale) = v1_claim_command(
            app.clone(),
            goal_id,
            &claim_a,
            "release-stale",
            Some("stale-claim-version"),
            json!({
                "action": "release",
                "workerId": first_worker.id.as_str(),
                "expectedGeneration": 1,
                "reason": "stale release"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stale["code"], "stale_revision");

        let (status, expanded) = v1_claim_command(
            app.clone(),
            goal_id,
            &claim_a,
            "expand-b",
            Some(&heartbeat_version),
            json!({
                "action": "expand",
                "workerId": first_worker.id.as_str(),
                "requestedScope": {"kind": "feature", "feature_id": "feature-b"},
                "rationale": "adjacent API work",
                "baseRevision": "base-api"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{expanded}");
        let claim_b = expanded["metadata"]["id"].as_str().unwrap().to_string();
        let expanded_version = expanded["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();
        let (status, reassigned) = v1_claim_command(
            app.clone(),
            goal_id,
            &claim_b,
            "reassign-b",
            Some(&expanded_version),
            json!({
                "action": "reassign",
                "replacementWorkerId": second_worker.id.as_str(),
                "expectedGeneration": 1,
                "baseRevision": "base-reassigned",
                "reason": "manual operator assignment",
                "decidedBy": "user:test"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reassigned}");
        assert_eq!(reassigned["data"]["owner"], second_worker.id.as_str());
        assert_eq!(reassigned["data"]["leaseGeneration"], 2);

        let (status, parent_after_expansion) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/claims/{claim_a}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{parent_after_expansion}");
        let parent_after_expansion_version = parent_after_expansion["metadata"]["resourceVersion"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, released) = v1_claim_command(
            app.clone(),
            goal_id,
            &claim_a,
            "release-a",
            Some(&parent_after_expansion_version),
            json!({
                "action": "release",
                "workerId": first_worker.id.as_str(),
                "expectedGeneration": 1,
                "reason": "partial work preserved",
                "artifactId": "artifact-partial",
                "evidenceRefs": ["test:release"]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{released}");
        assert_eq!(released["data"]["state"], "released");
        let (status, replayed) = v1_claim_command(
            app.clone(),
            goal_id,
            &claim_a,
            "release-a",
            None,
            json!({
                "action": "cancel",
                "workerId": first_worker.id.as_str(),
                "expectedGeneration": 999,
                "reason": "must not replace the first outcome"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed, released);

        let (status, acquired_c) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/claims/commands"),
            acquire(first_worker.id.as_str(), "feature-c", "acquire-c"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{acquired_c}");
        let claim_c = acquired_c["metadata"]["id"].as_str().unwrap();
        let version_c = acquired_c["metadata"]["resourceVersion"].as_str().unwrap();
        let (status, completed) = v1_claim_command(
            app.clone(),
            goal_id,
            claim_c,
            "complete-c",
            Some(version_c),
            json!({
                "action": "complete",
                "workerId": first_worker.id.as_str(),
                "expectedGeneration": 1,
                "reason": "verified integration",
                "artifactId": "artifact-c",
                "integrationRevision": "revision-c",
                "evidenceRefs": ["test:complete"],
                "integrationBoundarySatisfied": true
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{completed}");
        assert_eq!(completed["data"]["state"], "completed");

        let (status, acquired_package) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/claims/commands"),
            json!({
                "idempotencyKey": "acquire-package",
                "command": {
                    "workerId": first_worker.id.as_str(),
                    "requestedScope": {
                        "kind": "work_package",
                        "work_package_id": package.id.as_str()
                    },
                    "baseRevision": "base-api"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{acquired_package}");
        let package_claim = acquired_package["metadata"]["id"].as_str().unwrap();
        let package_claim_version = acquired_package["metadata"]["resourceVersion"]
            .as_str()
            .unwrap();
        let (status, blocked) = v1_claim_command(
            app,
            goal_id,
            package_claim,
            "block-package",
            Some(package_claim_version),
            json!({
                "action": "block",
                "workerId": first_worker.id.as_str(),
                "expectedGeneration": 1,
                "reason": "external package dependency",
                "evidenceRefs": ["test:block"]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{blocked}");
        assert_eq!(blocked["data"]["state"], "blocked");
    }

    #[tokio::test]
    async fn coordination_resource_routes_publish_and_acknowledge_durable_context() {
        use crate::coordination::domain::{InterventionLevel, SignalKind, SupervisorIntervention};

        let (app, temp) = test_app().await;
        let goal_id = "coordination-resources";
        let now = Utc::now();
        let store = SqliteCoordinationStore::open(
            temp.path()
                .join("project/.goal-manager/coordination.sqlite"),
        )
        .unwrap();
        let mut worker = Worker::new(goal_id, now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        let claim = Claim::new(
            goal_id,
            ClaimScope::Feature {
                feature_id: "resource-api".into(),
            },
            worker.id.clone(),
            "base-resource-api",
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();

        let (status, registered) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/contracts/commands"),
            json!({
                "idempotencyKey": "register-contract-api",
                "command": {
                    "stableKey": "api.public.v1",
                    "title": "Public API",
                    "kind": "api",
                    "compatibilityNotes": "Initial contract",
                    "actor": {"kind": "system"}
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{registered}");
        let contract_id = registered["metadata"]["id"].as_str().unwrap();
        let contract_version = registered["metadata"]["resourceVersion"].as_str().unwrap();
        let (status, revised) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/contracts/{contract_id}/commands"),
            json!({
                "idempotencyKey": "revise-contract-api",
                "expectedResourceVersion": contract_version,
                "command": {
                    "action": "revise",
                    "expectedRevision": 1,
                    "changedBy": worker.id.as_str(),
                    "compatibilityNotes": "Added an optional response field",
                    "actor": {"kind": "worker", "worker_id": worker.id.as_str()}
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{revised}");
        assert_eq!(revised["data"]["revision"], 2);
        let (status, contracts) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/contracts"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{contracts}");
        assert_eq!(contracts["items"][0]["metadata"]["id"], contract_id);
        let (status, contract_detail) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/contracts/{contract_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{contract_detail}");

        let claim_id = claim.id.clone();
        let (status, published) = json_request(
            app.clone(),
            "POST",
            &format!("/coordination/v1/goals/{goal_id}/events/commands"),
            json!({
                "idempotencyKey": "publish-progress-api",
                "command": {
                    "severity": "info",
                    "producer": {"kind": "worker", "worker_id": worker.id.as_str()},
                    "correlationId": claim_id.as_str(),
                    "payload": {
                        "type": "activity",
                        "data": {
                            "workerId": worker.id.as_str(),
                            "claimId": claim_id.as_str(),
                            "category": "progress",
                            "summary": "Published through the coordination API",
                            "progressPercent": 50,
                            "changedScope": ["rust-backend/api.rs"],
                            "evidenceRefs": ["test:event"]
                        }
                    }
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{published}");
        let event_id = published["metadata"]["id"].as_str().unwrap();
        let event = store
            .event(&EventId::parse(event_id).unwrap())
            .unwrap()
            .unwrap();
        let (status, events) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/events"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{events}");
        assert!(events["items"].as_array().unwrap().len() >= 3);
        let (status, event_detail) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/events/{event_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{event_detail}");

        let signal = CoordinationSignal::new(
            goal_id,
            "api-resource-signal",
            SignalKind::DependencyChanged,
            80,
            EventSeverity::Warning,
            event.id.clone(),
            vec![worker.id.clone()],
            vec![claim_id],
            now,
            now + chrono::Duration::minutes(5),
        )
        .unwrap();
        store.merge_signal_observation(&signal, now).unwrap();
        let intervention = SupervisorIntervention::new(
            goal_id,
            InterventionLevel::Inform,
            vec![event.id.clone()],
            vec![worker.id.clone()],
            "Refresh the changed dependency",
            now,
        );
        store.upsert_intervention(&intervention).unwrap();
        let notification = WorkerNotification::new(
            goal_id,
            worker.id.clone(),
            event.id,
            "dependency_changed",
            "A dependency changed",
            EventSeverity::Warning,
            vec!["test:notification".into()],
            Some("Refresh assumptions".into()),
            true,
            None,
            now,
        )
        .unwrap();
        store.upsert_notification(&notification).unwrap();

        for (resource, id) in [
            ("signals", signal.id.as_str()),
            ("interventions", intervention.id.as_str()),
            ("notifications", notification.id.as_str()),
        ] {
            let (status, list) = json_request(
                app.clone(),
                "GET",
                &format!("/coordination/v1/goals/{goal_id}/{resource}"),
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{resource}: {list}");
            assert_eq!(list["items"][0]["metadata"]["id"], id);
            let (status, detail) = json_request(
                app.clone(),
                "GET",
                &format!("/coordination/v1/goals/{goal_id}/{resource}/{id}"),
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{resource}: {detail}");
        }

        let notification_id = notification.id.as_str();
        let queued_version = resource_version(&notification);
        let command_uri =
            format!("/coordination/v1/goals/{goal_id}/notifications/{notification_id}/commands");
        let (status, delivered) = json_request(
            app.clone(),
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "deliver-notification-api",
                "expectedResourceVersion": queued_version,
                "command": {"action": "deliver", "actor": {"kind": "system"}}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{delivered}");
        assert_eq!(delivered["data"]["state"], "delivered");
        let delivered_version = delivered["metadata"]["resourceVersion"].as_str().unwrap();
        let (status, acknowledged) = json_request(
            app.clone(),
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "ack-notification-api",
                "expectedResourceVersion": delivered_version,
                "command": {"action": "acknowledge", "workerId": worker.id.as_str()}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{acknowledged}");
        assert_eq!(acknowledged["data"]["state"], "acknowledged");
        let acknowledged_version = acknowledged["metadata"]["resourceVersion"]
            .as_str()
            .unwrap();
        let (status, acted) = json_request(
            app,
            "POST",
            &command_uri,
            json!({
                "idempotencyKey": "act-notification-api",
                "expectedResourceVersion": acknowledged_version,
                "command": {
                    "action": "acted_on",
                    "workerId": worker.id.as_str(),
                    "outcome": "Dependency assumptions refreshed"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{acted}");
        assert_eq!(acted["data"]["state"], "acted_on");
    }

    #[tokio::test]
    async fn coordination_escalation_routes_cover_every_lifecycle_command() {
        use crate::coordination::domain::{EscalationKind, EscalationScope, EscalationSeverity};

        let (app, temp) = test_app().await;
        let goal_id = "escalation-api";
        let now = Utc::now();
        let store = SqliteCoordinationStore::open(
            temp.path()
                .join("project/.goal-manager/coordination.sqlite"),
        )
        .unwrap();
        let mut escalations = Vec::new();
        for summary in ["resolve", "override", "expire", "cancel"] {
            let mut escalation = HumanEscalation::new(
                goal_id,
                EscalationKind::DecisionPoint,
                EscalationSeverity::High,
                EscalationScope::Goal {
                    goal_id: goal_id.into(),
                },
                summary,
                now,
            );
            escalation.options = vec![crate::coordination::domain::RemediationOption {
                id: format!("{summary}-option"),
                action: if summary == "override" {
                    crate::coordination::domain::RemediationAction::OverrideRisk
                } else {
                    crate::coordination::domain::RemediationAction::ChooseDirection
                },
                label: summary.into(),
                description: format!("{summary} this escalation"),
                consequences: vec!["Recorded in the audit trail".into()],
                recommended: true,
            }];
            store.upsert_escalation(&escalation).unwrap();
            escalations.push(escalation);
        }

        let (status, list) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/escalations?limit=10"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{list}");
        assert_eq!(list["items"].as_array().unwrap().len(), 4);
        let resolve_id = escalations[0].id.as_str();
        let (status, resolve_detail) = json_request(
            app.clone(),
            "GET",
            &format!("/coordination/v1/goals/{goal_id}/escalations/{resolve_id}"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resolve_detail}");
        let resolve_version = resolve_detail["metadata"]["resourceVersion"]
            .as_str()
            .unwrap();
        let resolve_uri =
            format!("/coordination/v1/goals/{goal_id}/escalations/{resolve_id}/commands");
        let (status, acknowledged) = json_request(
            app.clone(),
            "POST",
            &resolve_uri,
            json!({
                "idempotencyKey": "ack-escalation-api",
                "expectedResourceVersion": resolve_version,
                "command": {
                    "action": "acknowledge",
                    "actor": "user:test",
                    "reason": "review started"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{acknowledged}");
        assert_eq!(acknowledged["data"]["state"], "acknowledged");
        let acknowledged_version = acknowledged["metadata"]["resourceVersion"]
            .as_str()
            .unwrap();
        let (status, resolved) = json_request(
            app.clone(),
            "POST",
            &resolve_uri,
            json!({
                "idempotencyKey": "resolve-escalation-api",
                "expectedResourceVersion": acknowledged_version,
                "command": {
                    "action": "resolve",
                    "actor": "user:test",
                    "reason": "direction selected",
                    "decision": {
                        "optionId": "resolve-option",
                        "decidedBy": "user:test"
                    }
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{resolved}");
        assert_eq!(resolved["data"]["state"], "resolved");

        let override_escalation = &escalations[1];
        let override_version = resource_version(override_escalation);
        let override_uri = format!(
            "/coordination/v1/goals/{goal_id}/escalations/{}/commands",
            override_escalation.id.as_str()
        );
        let (status, stale) = json_request(
            app.clone(),
            "POST",
            &override_uri,
            json!({
                "idempotencyKey": "override-stale-api",
                "expectedResourceVersion": "stale-escalation-version",
                "confirmation": test_confirmation(
                    "escalation.override",
                    override_escalation.id.as_str()
                ),
                "command": {
                    "action": "override",
                    "actor": "user:test",
                    "decision": {
                        "optionId": "override-option",
                        "decidedBy": "user:test",
                        "acceptedRisk": "Accepted test risk"
                    }
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stale["code"], "stale_revision");
        let (status, overridden) = json_request(
            app.clone(),
            "POST",
            &override_uri,
            json!({
                "idempotencyKey": "override-escalation-api",
                "expectedResourceVersion": override_version,
                "confirmation": test_confirmation(
                    "escalation.override",
                    override_escalation.id.as_str()
                ),
                "command": {
                    "action": "override",
                    "actor": "user:test",
                    "reason": "risk accepted",
                    "decision": {
                        "optionId": "override-option",
                        "decidedBy": "user:test",
                        "acceptedRisk": "Accepted test risk"
                    }
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{overridden}");
        assert_eq!(overridden["data"]["state"], "overridden");

        let expire_escalation = &escalations[2];
        let expire_uri = format!(
            "/coordination/v1/goals/{goal_id}/escalations/{}/commands",
            expire_escalation.id.as_str()
        );
        let (status, expired) = json_request(
            app.clone(),
            "POST",
            &expire_uri,
            json!({
                "idempotencyKey": "expire-escalation-api",
                "expectedResourceVersion": resource_version(expire_escalation),
                "command": {"action": "expire", "reason": "decision window elapsed"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{expired}");
        assert_eq!(expired["data"]["state"], "expired");

        let cancel_escalation = &escalations[3];
        let cancel_uri = format!(
            "/coordination/v1/goals/{goal_id}/escalations/{}/commands",
            cancel_escalation.id.as_str()
        );
        let (status, cancelled) = json_request(
            app.clone(),
            "POST",
            &cancel_uri,
            json!({
                "idempotencyKey": "cancel-escalation-api",
                "expectedResourceVersion": resource_version(cancel_escalation),
                "command": {
                    "action": "cancel",
                    "actor": "user:test",
                    "reason": "scope no longer required"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{cancelled}");
        assert_eq!(cancelled["data"]["state"], "cancelled");
        let (status, replayed) = json_request(
            app,
            "POST",
            &cancel_uri,
            json!({
                "idempotencyKey": "cancel-escalation-api",
                "command": {"action": "expire", "reason": "must not replace outcome"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed, cancelled);
        let privileged = store
            .latest_events_for_goal(goal_id, None, 100)
            .unwrap()
            .into_iter()
            .filter_map(|event| event.typed_payload().unwrap())
            .filter_map(|payload| match payload {
                CoordinationEventPayload::PrivilegedAction(payload) => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(privileged.len(), 1);
        assert_eq!(privileged[0].action, "escalation.override");
        assert_eq!(privileged[0].target, override_escalation.id.as_str());
        assert!(
            privileged[0]
                .evidence_refs
                .iter()
                .any(|evidence| evidence.contains("override-escalation-api"))
        );
    }

    fn integration_api_git(cwd: &std::path::Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(cwd)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    #[tokio::test]
    async fn coordination_integration_routes_cover_capture_validation_retry_finalization_and_maintenance()
     {
        use crate::coordination::workspace::WorktreeManager;
        use chrono::Duration;

        let (app, temp) = test_app().await;
        let project = temp.path().join("project");
        let goal_id = "integration-api";
        integration_api_git(&project, &["init", "-q"]);
        integration_api_git(&project, &["config", "user.email", "golazo@example.test"]);
        integration_api_git(&project, &["config", "user.name", "Golazo Test"]);
        fs::write(project.join("README.md"), "initial\n").unwrap();
        integration_api_git(&project, &["add", "README.md"]);
        integration_api_git(&project, &["commit", "-q", "-m", "initial"]);

        let tracker = Tracker::new(project.join(".goal-manager"));
        tracker
            .create_goal_with_features(
                goal_id,
                "Integration API",
                "exercise the integration lifecycle",
                vec![Feature {
                    id: "delivery".into(),
                    title: "Delivery".into(),
                    description: String::new(),
                    status: Status::Partial,
                    steps: vec![Step {
                        id: "integrate".into(),
                        title: "Integrate".into(),
                        done: false,
                        next: true,
                    }],
                }],
            )
            .unwrap();

        let now = Utc::now();
        let manager = WorktreeManager::open(&project).unwrap();
        let mut worker = Worker::new(goal_id, now);
        worker.transition(WorkerState::Starting, now, None).unwrap();
        worker.transition(WorkerState::Active, now, None).unwrap();
        let binding = manager
            .create_for_worker(goal_id, &worker.id, "HEAD", now)
            .unwrap();
        worker.workspace = Some(binding.clone());
        let claim = Claim::new(
            goal_id,
            ClaimScope::Feature {
                feature_id: "delivery".into(),
            },
            worker.id.clone(),
            binding.base_revision.clone(),
            now,
            now + Duration::minutes(10),
        )
        .unwrap();
        worker.active_claims.push(claim.id.clone());
        let store =
            SqliteCoordinationStore::open(project.join(".goal-manager/coordination.sqlite"))
                .unwrap();
        store.upsert_worker(&worker).unwrap();
        store.insert_claim(&claim).unwrap();

        let worktree = std::path::Path::new(&binding.worktree_path);
        fs::write(worktree.join("delivery.txt"), "validated delivery\n").unwrap();
        integration_api_git(worktree, &["add", "delivery.txt"]);
        integration_api_git(worktree, &["commit", "-q", "-m", "deliver slice"]);

        let capture_uri =
            format!("/coordination/v1/goals/{goal_id}/integration/artifacts/commands");
        let capture_body = json!({
            "idempotencyKey": "capture-integration-api",
            "command": {
                "action": "capture",
                "claimId": claim.id.as_str(),
                "workerId": worker.id.as_str(),
                "evidenceRefs": ["slice:test"],
                "knownRisks": ["test-only artifact"]
            }
        });
        let (status, captured) =
            json_request(app.clone(), "POST", &capture_uri, capture_body.clone()).await;
        assert_eq!(status, StatusCode::OK, "{captured}");
        let artifact_id = captured["data"]["id"].as_str().unwrap();
        let artifact_version = captured["metadata"]["resourceVersion"].as_str().unwrap();
        assert_eq!(captured["data"]["goalId"], goal_id);
        let (status, replayed_capture) =
            json_request(app.clone(), "POST", &capture_uri, capture_body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed_capture, captured);

        let artifacts_uri = format!("/coordination/v1/goals/{goal_id}/integration/artifacts");
        let (status, artifacts) =
            json_request(app.clone(), "GET", &artifacts_uri, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{artifacts}");
        assert_eq!(artifacts["items"].as_array().unwrap().len(), 1);
        let artifact_uri =
            format!("/coordination/v1/goals/{goal_id}/integration/artifacts/{artifact_id}");
        let (status, detail) = json_request(app.clone(), "GET", &artifact_uri, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{detail}");
        assert_eq!(detail["data"]["id"], artifact_id);

        let artifact_commands_uri = format!("{artifact_uri}/commands");
        let (status, stale) = json_request(
            app.clone(),
            "POST",
            &artifact_commands_uri,
            json!({
                "idempotencyKey": "stale-preflight-integration-api",
                "expectedResourceVersion": "stale",
                "command": {"action": "preflight"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(stale["code"], "stale_revision");
        let (status, preflight) = json_request(
            app.clone(),
            "POST",
            &artifact_commands_uri,
            json!({
                "idempotencyKey": "preflight-integration-api",
                "expectedResourceVersion": artifact_version,
                "command": {"action": "preflight"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{preflight}");
        assert_eq!(preflight["data"]["passed"], true);

        let (status, validation) = json_request(
            app.clone(),
            "POST",
            &artifact_commands_uri,
            json!({
                "idempotencyKey": "validate-integration-api",
                "expectedResourceVersion": artifact_version,
                "confirmation": test_confirmation(
                    "integration.execute_validation",
                    artifact_id
                ),
                "command": {
                    "action": "validate",
                    "gates": [{
                        "id": "repository-diff",
                        "kind": "repository",
                        "program": "git",
                        "args": ["diff", "--check"],
                        "required": true,
                        "maxOutputBytes": 4096
                    }]
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{validation}");
        assert_eq!(validation["data"]["passed"], true);
        let validation_report_id = validation["data"]["id"].as_str().unwrap();

        let (status, reconciliation) = json_request(
            app.clone(),
            "POST",
            &artifact_commands_uri,
            json!({
                "idempotencyKey": "reconcile-integration-api",
                "expectedResourceVersion": artifact_version,
                "command": {
                    "action": "reconcile",
                    "strategy": "manual",
                    "targetRevision": "HEAD",
                    "manualInstructions": "No automated reconciliation required."
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reconciliation}");
        assert_eq!(reconciliation["data"]["state"], "manual_required");

        for suffix in ["validations", "reconciliations"] {
            let (status, collection) = json_request(
                app.clone(),
                "GET",
                &format!("{artifact_uri}/{suffix}"),
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{collection}");
            assert_eq!(collection["items"].as_array().unwrap().len(), 1);
        }

        let (status, queued) = json_request(
            app.clone(),
            "POST",
            &artifact_commands_uri,
            json!({
                "idempotencyKey": "integrate-integration-api",
                "expectedResourceVersion": artifact_version,
                "command": {"action": "integrate", "priority": 7}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{queued}");
        assert_eq!(queued["data"]["state"], "queued");

        let jobs_uri = format!(
            "/coordination/v1/goals/{goal_id}/integration/jobs?repositoryId={}",
            binding.repository_id
        );
        let (status, jobs) = json_request(app.clone(), "GET", &jobs_uri, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{jobs}");
        assert_eq!(jobs["items"].as_array().unwrap().len(), 1);

        let queue_commands_uri =
            format!("/coordination/v1/goals/{goal_id}/integration/jobs/commands");
        let (status, running) = json_request(
            app.clone(),
            "POST",
            &queue_commands_uri,
            json!({
                "idempotencyKey": "acquire-first-integration-api",
                "command": {"action": "acquire", "repositoryId": binding.repository_id}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{running}");
        assert_eq!(running["data"]["state"], "running");
        let first_job_id = running["data"]["id"].as_str().unwrap();
        let first_job_version = running["metadata"]["resourceVersion"].as_str().unwrap();
        let first_job_commands_uri =
            format!("/coordination/v1/goals/{goal_id}/integration/jobs/{first_job_id}/commands");
        let (status, failed) = json_request(
            app.clone(),
            "POST",
            &first_job_commands_uri,
            json!({
                "idempotencyKey": "fail-first-integration-api",
                "expectedResourceVersion": first_job_version,
                "command": {
                    "action": "finish",
                    "succeeded": false,
                    "summary": "transient integration failure"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{failed}");
        assert_eq!(failed["data"]["state"], "failed");

        let (status, retried) = json_request(
            app.clone(),
            "POST",
            &artifact_commands_uri,
            json!({
                "idempotencyKey": "retry-integration-api",
                "expectedResourceVersion": artifact_version,
                "command": {"action": "retry"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{retried}");
        assert_eq!(retried["data"]["state"], "queued");
        assert_ne!(retried["data"]["id"], first_job_id);

        let (status, retry_running) = json_request(
            app.clone(),
            "POST",
            &queue_commands_uri,
            json!({
                "idempotencyKey": "acquire-retry-integration-api",
                "command": {"action": "acquire", "repositoryId": binding.repository_id}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{retry_running}");
        assert_eq!(retry_running["data"]["state"], "running");
        let retry_job_id = retry_running["data"]["id"].as_str().unwrap();
        let retry_job_version = retry_running["metadata"]["resourceVersion"]
            .as_str()
            .unwrap();

        integration_api_git(&project, &["merge", "--ff-only", &binding.branch]);
        let integration_revision = integration_api_git(&project, &["rev-parse", "HEAD"]);
        let retry_job_commands_uri =
            format!("/coordination/v1/goals/{goal_id}/integration/jobs/{retry_job_id}/commands");
        let (status, finalized) = json_request(
            app.clone(),
            "POST",
            &retry_job_commands_uri,
            json!({
                "idempotencyKey": "finalize-integration-api",
                "expectedResourceVersion": retry_job_version,
                "command": {
                    "action": "finalize",
                    "validationReportId": validation_report_id,
                    "trackerFeatureId": "delivery",
                    "trackerStepIds": ["integrate"],
                    "trackerSummary": "Integrated through the coordination API",
                    "trackerEvidence": ["validation:repository-diff"],
                    "trackerStatus": "Done",
                    "integrationRevision": integration_revision
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{finalized}");
        assert_eq!(finalized["data"]["state"], "completed");
        let finalization_id = finalized["data"]["id"].as_str().unwrap();
        let finalization_version = finalized["metadata"]["resourceVersion"].as_str().unwrap();
        let finalization_uri =
            format!("/coordination/v1/goals/{goal_id}/integration/finalizations/{finalization_id}");
        let (status, finalization_detail) =
            json_request(app.clone(), "GET", &finalization_uri, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{finalization_detail}");
        assert_eq!(finalization_detail["data"]["state"], "completed");

        let finalization_commands_uri = format!("{finalization_uri}/commands");
        let (status, rollback) = json_request(
            app.clone(),
            "POST",
            &finalization_commands_uri,
            json!({
                "idempotencyKey": "rollback-integration-api",
                "expectedResourceVersion": finalization_version,
                "confirmation": test_confirmation(
                    "integration.rollback",
                    finalization_id
                ),
                "command": {
                    "action": "rollback",
                    "requestedBy": "operator@example.test",
                    "reason": "test the recoverable rollback endpoint"
                }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rollback}");
        assert_eq!(rollback["data"]["kind"], "rollback");
        assert_eq!(rollback["data"]["state"], "succeeded");

        let cleanup_body = json!({
            "idempotencyKey": "cleanup-integration-api",
            "expectedResourceVersion": finalization_version,
            "confirmation": test_confirmation("integration.cleanup", finalization_id),
            "command": {"action": "cleanup"}
        });
        let (status, cleanup) = json_request(
            app.clone(),
            "POST",
            &finalization_commands_uri,
            cleanup_body.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{cleanup}");
        assert_eq!(cleanup["data"]["kind"], "cleanup");
        assert!(!worktree.exists());
        let (status, replayed_cleanup) = json_request(
            app.clone(),
            "POST",
            &finalization_commands_uri,
            cleanup_body,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(replayed_cleanup, cleanup);

        let (status, maintenance) = json_request(
            app,
            "GET",
            &format!("{artifact_uri}/maintenance"),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{maintenance}");
        assert_eq!(maintenance["items"].as_array().unwrap().len(), 2);
        let goal = tracker.get_goal(goal_id).unwrap();
        assert_eq!(goal["features"][0]["steps"][0]["done"], true);
        assert_eq!(goal["features"][0]["status"], "Done");

        let privileged = store
            .latest_events_for_goal(goal_id, None, 100)
            .unwrap()
            .into_iter()
            .filter_map(|event| event.typed_payload().unwrap())
            .filter_map(|payload| match payload {
                CoordinationEventPayload::PrivilegedAction(payload) => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(privileged.len(), 3);
        for (action, target, idempotency_key) in [
            (
                "integration.execute_validation",
                artifact_id,
                "validate-integration-api",
            ),
            (
                "integration.rollback",
                finalization_id,
                "rollback-integration-api",
            ),
            (
                "integration.cleanup",
                finalization_id,
                "cleanup-integration-api",
            ),
        ] {
            let audit = privileged
                .iter()
                .find(|payload| payload.action == action)
                .unwrap_or_else(|| panic!("missing privileged audit for {action}"));
            assert_eq!(audit.actor, "api-test-user");
            assert_eq!(audit.authority, "explicit_user_confirmation");
            assert_eq!(audit.target, target);
            assert_eq!(
                audit.decision,
                format!("Test confirms {action} on {target}")
            );
            assert_eq!(audit.outcome, "succeeded");
            assert!(
                audit
                    .evidence_refs
                    .iter()
                    .any(|evidence| evidence.contains(idempotency_key))
            );
            assert!(
                audit
                    .evidence_refs
                    .iter()
                    .any(|evidence| evidence.starts_with("outcome-version:"))
            );
        }
        assert_eq!(
            privileged
                .iter()
                .filter(|payload| payload.action == "integration.cleanup")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn goal_and_project_api_contract() {
        let (app, _temp) = test_app().await;
        let (status, goal) = json_request(
            app.clone(),
            "POST",
            "/goals",
            json!({"goal_id":"v1","title":"Version one"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(goal["goal_id"], "v1");
        let (status, run) = json_request(
            app.clone(),
            "POST",
            "/runs",
            json!({
                "prompt":"",
                "goal_id":"v1",
                "images":[{"name":"clipboard.png","mime_type":"image/png","data":"iVBORw=="}],
                "files":[{"name":"notes.txt","mime_type":"text/plain","data":"aGVsbG8="}]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(run["images"][0]["name"], "clipboard.png");
        assert_eq!(run["files"][0]["name"], "notes.txt");
        let image_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "/runs/{}/images/image-1",
                        run["id"].as_str().unwrap()
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(image_response.status(), StatusCode::OK);
        assert_eq!(image_response.headers()[header::CONTENT_TYPE], "image/png");
        assert_eq!(
            image_response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
            &b"\x89PNG"[..]
        );
        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals/v1/features",
            json!({"feature_id":"api","title":"API"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals/v1/features/api/steps",
            json!({"step_id":"health","title":"Health endpoint"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, goal) = json_request(
            app.clone(),
            "PATCH",
            "/goals/v1/features/api/steps/health",
            json!({"done":true}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(goal["progress"]["completion_rate"], 100);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/projects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let projects: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(projects[0]["goals"][0]["last_thread_at"].is_null());
        let (status, config) = json_request(
            app.clone(),
            "PATCH",
            "/settings/default-llm",
            json!({"provider":"ollama","model":"qwen","reasoning":"high"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(config["provider"], "ollama");
        assert_eq!(config["model"], "qwen");
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/codex-auth/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let auth: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(auth["available"], false);
        let (status, profile) = json_request(
            app.clone(),
            "PATCH",
            "/profile",
            json!({"display_name":"  Gian  ","role":"Builder"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(profile["display_name"], "Gian");
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/profile")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let saved: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(saved["role"], "Builder");
        let (status, renamed) = json_request(
            app.clone(),
            "PATCH",
            &format!(
                "/projects/{}",
                crate::projects::project_id(&_temp.path().join("project"))
            ),
            json!({"name":"Renamed project"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(renamed["name"], "Renamed project");
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let html = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&html).contains("Golazo"));
    }

    #[tokio::test]
    async fn worker_pool_controls_are_durable_across_requests() {
        let (app, _temp) = test_app().await;
        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals",
            json!({"goal_id":"pool-goal","title":"Pool goal"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals/pool-goal/features",
            json!({"feature_id":"first","title":"First","status":"Planned"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, rejected) = json_request(
            app.clone(),
            "POST",
            "/goals/pool-goal/worker-pool/start",
            json!({"desired_concurrency":2,"base_revision":"base-a"}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            rejected["detail"]
                .as_str()
                .unwrap()
                .contains("non-Git workspaces are limited")
        );

        let (status, started) = json_request(
            app.clone(),
            "POST",
            "/goals/pool-goal/worker-pool/start",
            json!({"desired_concurrency":1,"base_revision":"base-a"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(started["pool"]["goal"]["mode"], "running");
        assert_eq!(started["assignedClaims"].as_array().unwrap().len(), 1);
        let worker_id = started["pool"]["workers"][0]["id"].as_str().unwrap();
        let claim_id = started["assignedClaims"][0]["id"].as_str().unwrap();
        let generation = started["assignedClaims"][0]["leaseGeneration"]
            .as_u64()
            .unwrap();
        let (status, heartbeat) = json_request(
            app.clone(),
            "POST",
            &format!("/goals/pool-goal/workers/{worker_id}/operations"),
            json!({
                "operation":"heartbeat",
                "claim_id":claim_id,
                "expected_generation":generation
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(heartbeat["result"], "claim");
        assert_eq!(heartbeat["claim"]["id"], claim_id);

        let (status, snapshot) = json_request(
            app.clone(),
            "GET",
            "/goals/pool-goal/worker-pool",
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(snapshot["goal"]["desiredConcurrency"], 1);

        let (status, paused) =
            json_request(app, "POST", "/goals/pool-goal/worker-pool/pause", json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(paused["pool"]["goal"]["mode"], "paused");
    }

    #[tokio::test]
    async fn security_governance_conformance_covers_isolation_secrets_quotas_bypass_scope_and_audit()
     {
        use crate::coordination::domain::WorkerToolCapability;
        use crate::coordination::pool::derive_worker_permissions;
        use crate::coordination::security::{
            ResourceUsage, goal_quota_violation, worker_quota_violation,
        };
        use crate::redaction::REDACTED_CREDENTIAL;
        use chrono::Duration;

        let (app, temp) = test_app().await;
        let goal_id = "security-conformance";
        let (status, _) = json_request(
            app.clone(),
            "POST",
            "/goals",
            json!({"goal_id": goal_id, "title": "Security conformance"}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        for feature_id in ["owned", "foreign"] {
            let (status, _) = json_request(
                app.clone(),
                "POST",
                &format!("/goals/{goal_id}/features"),
                json!({"feature_id": feature_id, "title": feature_id, "status": "Planned"}),
            )
            .await;
            assert_eq!(status, StatusCode::CREATED);
        }

        let user_policy = WorkerPermissionPolicy::conservative();
        let goal_policy = WorkerPermissionPolicy {
            sandbox: "read-only".into(),
            approval_policy: "on-request".into(),
            approvals_reviewer: "user".into(),
            network_access: false,
            tool_capabilities: vec![WorkerToolCapability::ReadFiles],
        };
        let permission_profile =
            derive_worker_permissions(user_policy.clone(), goal_policy.clone()).unwrap();
        assert_eq!(permission_profile.sandbox, "read-only");
        assert!(!permission_profile.network_access);
        assert_eq!(
            permission_profile.tool_capabilities,
            vec![WorkerToolCapability::ReadFiles]
        );
        let elevated_goal_policy = WorkerPermissionPolicy {
            network_access: true,
            tool_capabilities: vec![
                WorkerToolCapability::ReadFiles,
                WorkerToolCapability::Network,
            ],
            ..goal_policy.clone()
        };
        assert!(derive_worker_permissions(user_policy.clone(), elevated_goal_policy).is_err());

        let quotas = ResourceQuotaPolicy::default();
        let worker_breach = worker_quota_violation(
            &quotas,
            ResourceUsage {
                tokens: quotas.max_worker_tokens,
                elapsed_seconds: quotas.max_worker_turn_seconds,
                processes: quotas.max_worker_processes + 1,
                disk_bytes: quotas.max_worker_disk_bytes + 1,
                network_requests: quotas.max_worker_network_requests + 1,
                retries: quotas.max_worker_retries,
                ..ResourceUsage::default()
            },
        )
        .unwrap();
        for dimension in [
            "tokens",
            "turn seconds",
            "processes",
            "disk bytes",
            "network requests",
            "retries",
        ] {
            assert!(worker_breach.contains(dimension), "missing {dimension}");
        }
        let goal_breach = goal_quota_violation(
            &quotas,
            ResourceUsage {
                tokens: quotas.max_goal_tokens,
                elapsed_seconds: quotas.max_goal_elapsed_seconds,
                processes: quotas.max_goal_processes + 1,
                disk_bytes: quotas.max_goal_disk_bytes + 1,
                network_requests: quotas.max_goal_network_requests + 1,
                retries: quotas.max_goal_retries,
                concurrency: quotas.max_goal_concurrency + 1,
            },
        )
        .unwrap();
        for dimension in [
            "tokens",
            "elapsed seconds",
            "processes",
            "disk bytes",
            "network requests",
            "retries",
            "concurrency",
        ] {
            assert!(goal_breach.contains(dimension), "missing {dimension}");
        }

        let commands_uri = format!("/coordination/v1/goals/{goal_id}/pool/commands");
        let (status, unconfirmed) = json_request(
            app.clone(),
            "POST",
            &commands_uri,
            json!({
                "idempotencyKey": "security-unconfirmed-stop",
                "command": {"action": "stop"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_REQUIRED, "{unconfirmed}");
        let (status, mismatched) = json_request(
            app.clone(),
            "POST",
            &commands_uri,
            json!({
                "idempotencyKey": "security-mismatched-stop",
                "confirmation": test_confirmation("worker.cancel", goal_id),
                "command": {"action": "stop"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_REQUIRED, "{mismatched}");

        let (status, configured) = v1_pool_command(
            app.clone(),
            goal_id,
            "security-configure",
            None,
            json!({
                "action": "configure",
                "desiredConcurrency": 1,
                "userPolicy": user_policy,
                "permissionPolicy": goal_policy,
                "resourcePolicy": quotas
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{configured}");
        assert_eq!(
            configured["data"]["snapshot"]["goal"]["workerPermissions"]["sandbox"],
            "read-only"
        );
        let configured_version = configured["metadata"]["resourceVersion"].as_str().unwrap();
        let (status, stopped) = v1_pool_command(
            app.clone(),
            goal_id,
            "security-stop",
            Some(configured_version),
            json!({"action": "stop"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{stopped}");
        let (status, replayed_stop) = v1_pool_command(
            app.clone(),
            goal_id,
            "security-stop",
            None,
            json!({"action": "stop"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{replayed_stop}");
        assert_eq!(replayed_stop, stopped);

        let store = SqliteCoordinationStore::open(
            temp.path()
                .join("project/.goal-manager/coordination.sqlite"),
        )
        .unwrap();
        let secret_outcome = json!({
            "OPENAI_API_KEY": "sk-conformance-secret",
            "authorization": "Bearer conformance-token",
            "safe": "retained"
        });
        store
            .record_idempotent_outcome(
                "security-secret",
                "security.conformance",
                &secret_outcome,
                Utc::now(),
            )
            .unwrap();
        let stored_secret = store
            .idempotent_outcome("security-secret")
            .unwrap()
            .unwrap();
        assert_eq!(stored_secret["OPENAI_API_KEY"], REDACTED_CREDENTIAL);
        assert_eq!(
            stored_secret["authorization"],
            format!("Bearer {REDACTED_CREDENTIAL}")
        );
        assert_eq!(stored_secret["safe"], "retained");
        assert!(!stored_secret.to_string().contains("conformance-secret"));
        assert!(!stored_secret.to_string().contains("conformance-token"));

        let now = Utc::now();
        let mut owner = Worker::new(goal_id, now);
        owner.transition(WorkerState::Starting, now, None).unwrap();
        owner.transition(WorkerState::Active, now, None).unwrap();
        owner.permission_profile = Some(permission_profile.clone());
        let mut stranger = Worker::new(goal_id, now);
        stranger
            .transition(WorkerState::Starting, now, None)
            .unwrap();
        stranger.transition(WorkerState::Active, now, None).unwrap();
        stranger.permission_profile = Some(permission_profile);
        let claim = Claim::new(
            goal_id,
            ClaimScope::Feature {
                feature_id: "owned".into(),
            },
            owner.id.clone(),
            "base-security",
            now,
            now + Duration::minutes(5),
        )
        .unwrap();
        owner.active_claims.push(claim.id.clone());
        store.upsert_worker(&owner).unwrap();
        store.upsert_worker(&stranger).unwrap();
        store.insert_claim(&claim).unwrap();

        let operation = json!({
            "operation": "heartbeat",
            "claim_id": claim.id.as_str(),
            "expected_generation": claim.lease_generation
        });
        let (status, cross_worker) = json_request(
            app.clone(),
            "POST",
            &format!(
                "/goals/{goal_id}/workers/{}/operations",
                stranger.id.as_str()
            ),
            operation.clone(),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{cross_worker}");
        assert!(
            cross_worker["detail"]
                .as_str()
                .unwrap()
                .contains("active owned claim")
        );
        let (status, owned) = json_request(
            app,
            "POST",
            &format!("/goals/{goal_id}/workers/{}/operations", owner.id.as_str()),
            operation,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{owned}");

        let privileged = store
            .latest_events_for_goal(goal_id, None, 100)
            .unwrap()
            .into_iter()
            .filter_map(|event| event.typed_payload().unwrap())
            .filter_map(|payload| match payload {
                CoordinationEventPayload::PrivilegedAction(payload) => Some(payload),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(privileged.len(), 2);
        for action in ["pool.configure_policy", "pool.stop"] {
            let audit = privileged
                .iter()
                .find(|audit| audit.action == action)
                .unwrap_or_else(|| panic!("missing audit for {action}"));
            assert_eq!(audit.actor, "api-test-user");
            assert_eq!(audit.authority, "explicit_user_confirmation");
            assert_eq!(audit.target, goal_id);
            assert!(!audit.decision.is_empty());
            assert_eq!(audit.outcome, "succeeded");
            assert!(
                audit
                    .evidence_refs
                    .iter()
                    .any(|evidence| evidence.starts_with("idempotency:"))
            );
            assert!(
                audit
                    .evidence_refs
                    .iter()
                    .any(|evidence| evidence.starts_with("outcome-version:"))
            );
        }
        assert_eq!(
            privileged
                .iter()
                .filter(|audit| audit.action == "pool.stop")
                .count(),
            1
        );
    }

    #[test]
    fn parses_fenced_goal_scaffold_proposals() {
        let message = r#"```json
{
  "goal_interpretation":"Create a small application",
  "success_criteria":["The primary workflow runs"],
  "primary_type":"Greenfield",
  "secondary_types":[],
  "classification_evidence":["The repository has no application source"],
  "confidence":"high",
  "assumptions":[],
  "decisions_required":["Choose the deployment target"],
  "required_features":[{
    "feature_id":"first-slice",
    "title":"First vertical slice",
    "description":"Prove the application shape",
    "scope":"required_mvp",
    "rationale":"A runnable workflow validates the architecture",
    "evidence":["User goal"],
    "steps":[{"step_id":"workflow","title":"Implement one end-to-end workflow"}],
    "acceptance":["The workflow is observable"],
    "verification":["Run the smoke test"],
    "dependencies":[],
    "risks":[],
    "confidence":"high"
  }],
  "optional_features":[],
  "risks":[],
  "dependencies":[],
  "non_goals":[]
}
```"#;
        let proposal = parse_scaffold_proposal(message).unwrap();
        assert_eq!(proposal.primary_type, GoalType::Greenfield);
        assert_eq!(proposal.required_features[0].feature_id, "first-slice");
    }

    #[test]
    fn reports_malformed_goal_scaffolds_as_retryable() {
        let error = parse_scaffold_proposal("{\"goal_interpretation\":missing quotes}")
            .expect_err("malformed scaffold should fail");
        assert_eq!(
            error.message,
            "Codex returned an incomplete goal plan. Please generate suggestions again."
        );
    }

    #[test]
    fn accepted_greenfield_scaffolds_select_one_actionable_next_step() {
        let mut features = vec![Feature {
            id: "project-foundation".into(),
            title: "Project foundation".into(),
            description: String::new(),
            status: Status::Planned,
            steps: vec![
                Step {
                    id: "confirm-project-location".into(),
                    title: "Confirm the project location".into(),
                    done: false,
                    next: false,
                },
                Step {
                    id: "scaffold-react-project".into(),
                    title: "Scaffold the React project".into(),
                    done: false,
                    next: false,
                },
                Step {
                    id: "add-basic-styles".into(),
                    title: "Add basic styles".into(),
                    done: false,
                    next: false,
                },
            ],
        }];

        select_initial_scaffold_step(&mut features, true).unwrap();

        assert!(features[0].steps[0].done);
        assert!(!features[0].steps[0].next);
        assert!(features[0].steps[1].next);
        assert_eq!(
            features
                .iter()
                .flat_map(|feature| &feature.steps)
                .filter(|step| step.next)
                .count(),
            1
        );
    }

    #[test]
    fn spec_mode_limits_work_to_tracker_refinement() {
        let prompt = goal_execution_prompt("goal-1", &WorkMode::Spec);
        assert!(prompt.contains("Spec mode"));
        assert!(prompt.contains("entered or re-entered at any point in this thread"));
        assert!(prompt.contains("continue evolving that document"));
        assert!(prompt.contains("Do not modify product source code"));
        assert!(
            prompt.contains("add, revise, reorder, or remove planned tracker features and steps")
        );
        assert!(prompt.contains("Preserve implementation history"));
        assert!(prompt.contains("switch this thread to Build mode"));
    }

    #[test]
    fn build_mode_requires_explicit_intent_and_a_verified_audit_slice() {
        let prompt = goal_execution_prompt("goal-1", &WorkMode::Build);
        assert!(prompt.contains("Build mode"));
        assert!(prompt.contains("does not itself authorize starting work"));
        assert!(prompt.contains("visible user prompt explicitly asks"));
        assert!(prompt.contains("answer without modifying product files"));
        assert!(prompt.contains("one coherent slice"));
        assert!(prompt.contains("mark only verified steps complete"));
        assert!(prompt.contains("record one audit slice"));
        assert!(prompt.contains("validate and show the tracker"));
    }

    #[test]
    fn scaffold_schema_requires_structured_feature_fields() {
        let schema = scaffold_output_schema();
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["required_features"]["items"]["additionalProperties"],
            false
        );
        assert!(
            schema["properties"]["required_features"]["items"]["required"]
                .as_array()
                .is_some_and(|fields| fields.iter().any(|field| field == "steps"))
        );
    }
}
