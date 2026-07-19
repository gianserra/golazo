mod api;
mod app_server;
mod models;
mod projects;
mod runner;
mod tracker;

use api::{AppState, router};
use projects::ProjectRegistry;
use runner::RunManager;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("golazo_backend=info")),
        )
        .init();
    let configured_root = env::var_os("CODEX_WORKSPACE_ROOT").map(PathBuf::from);
    let mut root = configured_root
        .clone()
        .unwrap_or_else(|| env::current_dir().expect("current directory"))
        .canonicalize()
        .expect("workspace root");
    let browse_root = PathBuf::from(
        env::var_os("GOAL_MANAGER_BROWSE_ROOT")
            .or_else(|| env::var_os("HOME"))
            .expect("home directory"),
    )
    .canonicalize()
    .expect("browse root");
    let mut tracking_root = env::var_os("GOAL_MANAGER_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".goal-manager"));
    let app_data = env::var_os("GOAL_MANAGER_APP_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| tracking_root.parent().unwrap_or(&root).join(".golazo-app"));
    let registry = ProjectRegistry::new(app_data.join("projects.json"));
    if configured_root.is_none() {
        if let Some(remembered) = registry.active() {
            let path = PathBuf::from(remembered.path);
            if path.is_dir() {
                root = path.canonicalize().expect("active project");
                tracking_root = root.join(".goal-manager");
            }
        }
    }
    let active = registry.add(&root, true).expect("register active project");
    let executable = env::var("CODEX_EXECUTABLE").unwrap_or_else(|_| "codex".into());
    let runner = RunManager::new(root, executable, tracking_root.join("run-history.json")).await;
    let state = AppState {
        tracker_root: Arc::new(RwLock::new(tracking_root)),
        runner,
        browse_root,
        registry,
        active_project_id: Arc::new(RwLock::new(active.id)),
        profile_path: app_data.join("profile.json"),
    };
    let port = env::var("GOLAZO_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8765);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind Golazo backend");
    tracing::info!("Golazo Rust backend listening on http://127.0.0.1:{port}");
    axum::serve(listener, router(state))
        .await
        .expect("serve Golazo backend");
}
