use crate::models::*;
use crate::projects::{ProjectRecord, ProjectRegistry};
use crate::runner::RunManager;
use crate::tracker::{Tracker, TrackerError, generate_goal_id};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use rust_embed::RustEmbed;
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct AppState {
    pub tracker_root: Arc<RwLock<PathBuf>>,
    pub runner: Arc<RunManager>,
    pub browse_root: PathBuf,
    pub registry: ProjectRegistry,
    pub active_project_id: Arc<RwLock<String>>,
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
#[folder = "src/golazo/static-react/"]
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
    let active = *state.active_project_id.read().await == record.id;
    let compact = goals.into_iter().map(|goal| json!({"goal_id":goal["goal_id"],"title":goal["title"],"progress":goal["progress"],"features":goal["features"].as_array().map(Vec::len).unwrap_or(0)})).collect::<Vec<_>>();
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
        .route(
            "/settings/default-llm",
            get(get_default_llm_config).patch(set_default_llm_config),
        )
        .route("/threads", get(list_threads))
        .route("/threads/{thread_id}", patch(update_thread))
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
    validate_text(&body.prompt, "prompt", 20_000)?;
    let execution_prompt = if let Some(goal) = &body.goal_id {
        root_tracker(&state).await.get_goal(goal)?;
        Some(format!(
            "Use $manage-implementation and work on goal '{goal}'. Record implementation slices and step completion as work proceeds.\n\n{}",
            body.prompt
        ))
    } else {
        None
    };
    let run = state
        .runner
        .create(
            body.prompt,
            body.working_directory,
            body.sandbox,
            body.goal_id,
            body.thread_id,
            execution_prompt,
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
async fn list_threads(State(state): State<AppState>) -> Json<Vec<crate::runner::ThreadSummary>> {
    Json(state.runner.list_threads().await)
}
async fn update_thread(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<ThreadUpdate>,
) -> Result<Json<crate::runner::ThreadSummary>, ApiError> {
    if body.title.is_none() && body.llm_config.is_none() {
        return Err(ApiError::bad("title or llm_config is required"));
    }
    if let Some(title) = &body.title {
        validate_text(title, "title", 200)?;
    }
    state
        .runner
        .update_thread(&id, body.title, body.llm_config)
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
}
