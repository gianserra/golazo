use crate::models::*;
use crate::projects::{ProjectRecord, ProjectRegistry};
use crate::runner::RunManager;
use crate::tracker::{Tracker, TrackerError, generate_goal_id};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::{StreamExt, wrappers::BroadcastStream};

const SCAFFOLD_SKILL: &str = include_str!("../skills/scaffold-implementation-goal/SKILL.md");
const SCAFFOLD_GOAL_TYPES: &str =
    include_str!("../skills/scaffold-implementation-goal/references/goal-types.md");
const SCAFFOLD_GREENFIELD: &str =
    include_str!("../skills/scaffold-implementation-goal/references/greenfield.md");
const SCAFFOLD_CONTRACT: &str =
    include_str!("../skills/scaffold-implementation-goal/references/suggestion-contract.md");

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
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"detail":self.message}))).into_response()
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
    format!(
        r#"You are running Golazo's bundled $scaffold-implementation-goal workflow.

Follow these instructions and references:

{SCAFFOLD_SKILL}

{SCAFFOLD_GOAL_TYPES}

{SCAFFOLD_GREENFIELD}

{SCAFFOLD_CONTRACT}

Inspect the current repository read-only. Do not modify files, create a goal, or mutate an
implementation tracker. Produce suggestions for review only.

User goal input:
{request}

Return exactly one JSON object with no Markdown fence or surrounding prose. Use this shape:
{{
  "goal_interpretation": "string",
  "success_criteria": ["string"],
  "primary_type": "Greenfield|Feature|Bug|Refactor|Migration|Integration|Release|Research|Mixed",
  "secondary_types": [],
  "classification_evidence": ["string"],
  "confidence": "high|medium|low",
  "assumptions": ["string"],
  "decisions_required": ["string"],
  "required_features": [{{
    "feature_id": "lowercase-hyphenated-id",
    "title": "string",
    "description": "string",
    "scope": "required_mvp|required_release_safety|optional_hardening|future",
    "rationale": "string",
    "evidence": ["string"],
    "steps": [{{"step_id":"lowercase-hyphenated-id","title":"string"}}],
    "acceptance": ["string"],
    "verification": ["string"],
    "dependencies": ["string"],
    "risks": ["string"],
    "confidence": "high|medium|low"
  }}],
  "optional_features": [],
  "risks": ["string"],
  "dependencies": ["string"],
  "non_goals": ["string"]
}}"#
    )
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
    match work_mode {
        WorkMode::Spec => format!(
            "You are in Golazo Spec mode for goal '{goal}'. Use $manage-implementation and treat the existing implementation document and tracker as authoritative. Spec mode may be entered or re-entered at any point in this thread to continue evolving that document. You may inspect the repository read-only and add, revise, reorder, or remove planned tracker features and steps, and refine blockers, statuses, risks, decisions, non-goals, dependencies, and acceptance criteria in response to the user. Preserve implementation history and do not erase completed audit slices. Do not modify product source code, configuration, or tests, and do not mark implementation work complete without existing evidence. If the user asks you to implement code, tell them to switch this thread to Build mode. Validate and show the tracker after any tracker mutation."
        ),
        WorkMode::Build => format!(
            "You are in Golazo Build mode for goal '{goal}'. Use $manage-implementation and treat the existing tracker as authoritative. Build mode permits product changes but does not itself authorize starting work. Do not select or implement a slice unless the visible user prompt explicitly asks you to build, implement, change, fix, or continue implementation. If the user asks a question, requests planning, or only changes modes, answer without modifying product files or tracker completion state. When implementation is explicitly requested, complete one coherent slice that satisfies that request, mark only verified steps complete, record one audit slice with concrete evidence, update feature status when it changes, then validate and show the tracker before reporting completion."
        ),
    }
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
    Ok((StatusCode::ACCEPTED, Json(run)))
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
        )?),
    ))
}
async fn set_step(
    State(state): State<AppState>,
    AxumPath((goal, feature, step)): AxumPath<(String, String, String)>,
    Json(body): Json<StepUpdate>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        root_tracker(&state)
            .await
            .set_step(&goal, &feature, &step, body.done)?,
    ))
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
            body.thread_id,
            execution_prompt,
            None,
            work_mode,
            body.llm_config,
        )
        .await
        .map_err(ApiError::bad)?;
    let location = HeaderValue::from_str(&format!("/runs/{}", run.id))
        .unwrap_or(HeaderValue::from_static("/runs"));
    let mut response = (StatusCode::ACCEPTED, Json(run)).into_response();
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
    Json(state.runner.list().await)
}
async fn get_run(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<Run>, ApiError> {
    state
        .runner
        .get(&id)
        .await
        .map(Json)
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
    state
        .runner
        .update_thread(&id, body.title, body.llm_config, body.work_mode)
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
