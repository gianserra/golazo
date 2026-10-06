use crate::app_server::AppServerClient;
use crate::coordination::domain::{Worker, WorkerToolCapability};
use crate::coordination::security::{
    redact_environment_credentials, redact_environment_credentials_text,
};
use crate::models::{
    AppServerThread, AppServerThreadPage, CodexAuthAction, CodexAuthStatus, FileAttachmentCreate,
    ImageAttachmentCreate, LLMConfig, LLMProvider, ReasoningLevel, Run, RunFile, RunImage,
    SpeedMode, Usage, WorkMode, WorkerCredentialIsolation,
};
use crate::observability::CorrelationIds;
use crate::redaction::{redact_sensitive_value, redacted_copy};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::RwLock;
use uuid::Uuid;

struct RunnerState {
    workspace_root: PathBuf,
    executable: String,
    history_path: PathBuf,
    settings_path: PathBuf,
    runs: Vec<Run>,
}
pub struct RunManager {
    state: RwLock<RunnerState>,
    app_server: AppServerClient,
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn prompt_title(prompt: &str) -> String {
    let title = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
    if title.is_empty() {
        "Untitled thread".into()
    } else {
        redact_environment_credentials_text(&title.chars().take(80).collect::<String>())
    }
}

fn redact_app_server_thread(thread: &mut AppServerThread) {
    thread.preview = redact_environment_credentials_text(&thread.preview);
    if let Some(name) = &mut thread.name {
        *name = redact_environment_credentials_text(name);
    }
    redact_sensitive_value(&mut thread.source);
    redact_sensitive_value(&mut thread.status);
    for turn in &mut thread.turns {
        redact_sensitive_value(turn);
    }
}

fn run_correlation(run: &Run) -> CorrelationIds {
    let mut ids = run
        .goal_id
        .as_deref()
        .map(CorrelationIds::for_goal)
        .unwrap_or_default()
        .with_run(&run.id)
        .with_workspace(&run.cwd);
    if let Some(worker_id) = run
        .credential_isolation
        .as_ref()
        .map(|isolation| isolation.worker_id.as_str())
    {
        ids = ids.with_worker(worker_id);
    }
    if let Some(thread_id) = run.thread_id.as_deref().or(run.resumed_from.as_deref()) {
        ids = ids.with_thread(thread_id);
    }
    ids
}

fn app_server_time(value: i64) -> String {
    let seconds = if value > 1_000_000_000_000 {
        value / 1000
    } else {
        value
    };
    DateTime::<Utc>::from_timestamp(seconds, 0)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::AutoSi, true))
        .unwrap_or_else(now)
}

fn validate_permissions(
    sandbox: &str,
    approval_policy: &str,
    approvals_reviewer: &str,
) -> Result<(), String> {
    let supported = matches!(
        (sandbox, approval_policy, approvals_reviewer),
        ("workspace-write", "on-request", "user")
            | ("workspace-write", "on-request", "auto_review")
            | ("danger-full-access", "never", "user")
            | ("read-only", "on-request", "user")
    );
    if supported {
        Ok(())
    } else {
        Err("unsupported sandbox, approval policy, and reviewer combination".into())
    }
}

fn codex_exec_args(sandbox: &str, approval_policy: &str, approvals_reviewer: &str) -> Vec<String> {
    vec![
        "--ask-for-approval".into(),
        approval_policy.into(),
        "exec".into(),
        "--sandbox".into(),
        sandbox.into(),
        "--config".into(),
        format!("approvals_reviewer=\"{approvals_reviewer}\""),
    ]
}

fn worker_shell_environment_config(policy: &WorkerCredentialIsolation) -> Value {
    serde_json::json!({
        "shell_environment_policy": {
            "inherit": policy.inherited_environment,
            "ignore_default_excludes": false,
            "set": policy.environment,
        }
    })
}

fn append_worker_shell_environment_args(
    args: &mut Vec<String>,
    policy: &WorkerCredentialIsolation,
) {
    args.extend([
        "--config".into(),
        format!(
            "shell_environment_policy.inherit={}",
            serde_json::to_string(&policy.inherited_environment)
                .unwrap_or_else(|_| "\"none\"".into())
        ),
        "--config".into(),
        "shell_environment_policy.ignore_default_excludes=false".into(),
    ]);
    for (name, value) in &policy.environment {
        args.extend([
            "--config".into(),
            format!(
                "shell_environment_policy.set.{name}={}",
                serde_json::to_string(value).unwrap_or_else(|_| "\"\"".into())
            ),
        ]);
    }
}

fn decode_attachment_base64(value: &str) -> Result<Vec<u8>, String> {
    let input = value
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    if input.is_empty() || input.len() % 4 != 0 {
        return Err("attachment data must be valid base64".into());
    }
    fn sextet(byte: u8) -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let mut output = Vec::with_capacity(input.len() / 4 * 3);
    let chunks = input.len() / 4;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == chunks;
        let a =
            sextet(chunk[0]).ok_or_else(|| "attachment data must be valid base64".to_string())?;
        let b =
            sextet(chunk[1]).ok_or_else(|| "attachment data must be valid base64".to_string())?;
        let c = if chunk[2] == b'=' {
            None
        } else {
            sextet(chunk[2])
        };
        let d = if chunk[3] == b'=' {
            None
        } else {
            sextet(chunk[3])
        };
        if c.is_none() && chunk[2] != b'=' || d.is_none() && chunk[3] != b'=' {
            return Err("attachment data must be valid base64".into());
        }
        if !last && (c.is_none() || d.is_none()) || c.is_none() && d.is_some() {
            return Err("attachment data must be valid base64".into());
        }
        output.push((a << 2) | (b >> 4));
        if let Some(c) = c {
            output.push((b << 4) | (c >> 2));
            if let Some(d) = d {
                output.push((c << 6) | d);
            }
        }
    }
    Ok(output)
}

fn provider_name(value: &LLMProvider) -> &'static str {
    match value {
        LLMProvider::Openai => "openai",
        LLMProvider::Ollama => "ollama",
        LLMProvider::Lmstudio => "lmstudio",
    }
}

fn reasoning_name(value: &ReasoningLevel) -> &'static str {
    match value {
        ReasoningLevel::Low => "low",
        ReasoningLevel::Medium => "medium",
        ReasoningLevel::High => "high",
        ReasoningLevel::Xhigh => "xhigh",
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredThread {
    title: String,
    llm_config: LLMConfig,
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    work_mode: WorkMode,
    created_at: String,
    #[serde(default)]
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ThreadSettings {
    #[serde(default)]
    default_llm_config: LLMConfig,
    #[serde(default)]
    threads: HashMap<String, StoredThread>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ThreadSummary {
    pub id: String,
    pub title: String,
    pub llm_config: LLMConfig,
    pub goal_id: Option<String>,
    pub work_mode: WorkMode,
    pub run_count: usize,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunnerRecoveryReport {
    pub interrupted_run_ids: Vec<String>,
    pub live_process_run_ids: Vec<String>,
    pub local_thread_ids: Vec<String>,
    pub native_thread_ids: Vec<String>,
    pub thread_inventory_error: Option<String>,
}

impl RunManager {
    pub async fn account(&self) -> Result<Value, String> {
        self.app_server.account().await
    }

    pub async fn new(
        workspace_root: PathBuf,
        executable: String,
        history_path: PathBuf,
    ) -> Arc<Self> {
        let runs = Self::load_history(&history_path).await;
        let workspace_root = workspace_root.canonicalize().unwrap_or(workspace_root);
        let settings_path = history_path.with_file_name("thread-settings.json");
        Arc::new(Self {
            app_server: AppServerClient::new(executable.clone()),
            state: RwLock::new(RunnerState {
                workspace_root,
                executable,
                history_path,
                settings_path,
                runs,
            }),
        })
    }

    pub async fn reconcile_after_restart(&self) -> RunnerRecoveryReport {
        let recovered_at = now();
        let mut interrupted_run_ids = Vec::new();
        {
            let mut state = self.state.write().await;
            for run in &mut state.runs {
                if matches!(run.status.as_str(), "queued" | "running") {
                    interrupted_run_ids.push(run.id.clone());
                    run.status = "failed".into();
                    run.finished_at = Some(recovered_at.clone());
                    run.return_code = None;
                    run.error = Some(
                        "Golazo restarted before this run reached a durable terminal state".into(),
                    );
                }
            }
        }
        interrupted_run_ids.sort();
        if !interrupted_run_ids.is_empty() {
            self.save().await;
        }

        let mut local_thread_ids = self
            .list_threads()
            .await
            .into_iter()
            .map(|thread| thread.id)
            .collect::<Vec<_>>();
        local_thread_ids.sort();
        local_thread_ids.dedup();

        let mut native_thread_ids = Vec::new();
        let mut cursor = None;
        let mut thread_inventory_error = None;
        for _ in 0..100 {
            match self.app_server_threads(cursor.clone(), Some(100)).await {
                Ok(page) => {
                    native_thread_ids.extend(page.data.into_iter().map(|thread| thread.id));
                    let next = page.next_cursor;
                    if next.is_none() || next == cursor {
                        break;
                    }
                    cursor = next;
                }
                Err(error) => {
                    thread_inventory_error = Some(error);
                    break;
                }
            }
        }
        native_thread_ids.sort();
        native_thread_ids.dedup();
        RunnerRecoveryReport {
            interrupted_run_ids,
            live_process_run_ids: vec![],
            local_thread_ids,
            native_thread_ids,
            thread_inventory_error,
        }
    }
    async fn load_history(path: &Path) -> Vec<Run> {
        let Ok(text) = tokio::fs::read_to_string(path).await else {
            return vec![];
        };
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            return vec![];
        };
        redact_sensitive_value(&mut value);
        serde_json::from_value(value).unwrap_or_default()
    }
    async fn save(&self) {
        let (path, runs) = {
            let state = self.state.read().await;
            (state.history_path.clone(), state.runs.clone())
        };
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let bytes = serde_json::to_value(&runs).and_then(|mut value| {
            redact_sensitive_value(&mut value);
            serde_json::to_vec_pretty(&value)
        });
        if let Ok(bytes) = bytes {
            let temporary = path.with_extension("tmp");
            if tokio::fs::write(&temporary, bytes).await.is_ok() {
                let _ = tokio::fs::rename(temporary, path).await;
            }
        }
    }
    async fn load_settings_path(path: &Path) -> ThreadSettings {
        let Ok(text) = tokio::fs::read_to_string(path).await else {
            return ThreadSettings::default();
        };
        let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
            return ThreadSettings::default();
        };
        redact_sensitive_value(&mut value);
        serde_json::from_value(value).unwrap_or_default()
    }
    async fn load_settings(&self) -> ThreadSettings {
        let path = self.state.read().await.settings_path.clone();
        Self::load_settings_path(&path).await
    }
    async fn save_settings(&self, settings: &ThreadSettings) -> Result<(), String> {
        let path = self.state.read().await.settings_path.clone();
        let parent = path.parent().ok_or("invalid settings path")?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let mut temporary =
            tempfile::NamedTempFile::new_in(parent).map_err(|error| error.to_string())?;
        let mut durable = serde_json::to_value(settings).map_err(|error| error.to_string())?;
        redact_sensitive_value(&mut durable);
        serde_json::to_writer_pretty(&mut temporary, &durable)
            .map_err(|error| error.to_string())?;
        temporary
            .write_all(b"\n")
            .map_err(|error| error.to_string())?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| error.to_string())?;
        temporary.persist(path).map_err(|error| error.to_string())?;
        Ok(())
    }
    pub async fn default_llm_config(&self) -> LLMConfig {
        self.load_settings().await.default_llm_config
    }
    pub async fn thread_work_mode(&self, id: &str) -> Option<WorkMode> {
        self.load_settings()
            .await
            .threads
            .get(id)
            .map(|thread| thread.work_mode.clone())
    }
    pub async fn thread_goal_id(&self, id: &str) -> Option<String> {
        if let Some(goal_id) = self
            .load_settings()
            .await
            .threads
            .get(id)
            .and_then(|thread| thread.goal_id.clone())
        {
            return Some(goal_id);
        }
        self.list_threads()
            .await
            .into_iter()
            .find(|thread| thread.id == id)
            .and_then(|thread| thread.goal_id)
    }
    pub async fn set_default_llm_config(&self, config: LLMConfig) -> Result<LLMConfig, String> {
        let mut settings = self.load_settings().await;
        settings.default_llm_config = config.clone();
        self.save_settings(&settings).await?;
        Ok(config)
    }
    async fn thread_exists(&self, id: &str) -> bool {
        let settings = self.load_settings().await;
        settings.threads.contains_key(id)
            || self
                .state
                .read()
                .await
                .runs
                .iter()
                .any(|run| run.thread_id.as_deref() == Some(id))
    }
    async fn ensure_thread(
        &self,
        id: &str,
        prompt: &str,
        config: &LLMConfig,
        goal_id: Option<&str>,
        work_mode: &WorkMode,
    ) {
        let mut settings = self.load_settings().await;
        let mut changed = false;
        if !settings.threads.contains_key(id) {
            let timestamp = now();
            settings.threads.insert(
                id.into(),
                StoredThread {
                    title: prompt_title(prompt),
                    llm_config: config.clone(),
                    goal_id: goal_id.map(str::to_string),
                    work_mode: work_mode.clone(),
                    created_at: timestamp.clone(),
                    updated_at: timestamp,
                },
            );
            changed = true;
        } else if let Some(record) = settings.threads.get_mut(id) {
            if let Some(goal_id) = goal_id {
                if record.goal_id.is_none() {
                    record.goal_id = Some(goal_id.into());
                    record.updated_at = now();
                    changed = true;
                }
            }
            if record.work_mode != *work_mode {
                record.work_mode = work_mode.clone();
                record.updated_at = now();
                changed = true;
            }
        }
        if changed {
            let _ = self.save_settings(&settings).await;
        }
    }
    pub async fn update_thread(
        &self,
        id: &str,
        title: Option<String>,
        llm_config: Option<LLMConfig>,
        work_mode: Option<WorkMode>,
    ) -> Result<Option<ThreadSummary>, String> {
        if !self.thread_exists(id).await {
            return Ok(None);
        }
        if let Some(value) = title.as_deref() {
            self.app_server
                .set_thread_name(id, &redact_environment_credentials_text(value.trim()))
                .await?;
        }
        let mut settings = self.load_settings().await;
        let default = settings.default_llm_config.clone();
        let record = settings
            .threads
            .entry(id.into())
            .or_insert_with(|| StoredThread {
                title: "Untitled thread".into(),
                llm_config: default,
                goal_id: None,
                work_mode: WorkMode::default(),
                created_at: now(),
                updated_at: now(),
            });
        if let Some(value) = title {
            record.title = redact_environment_credentials_text(value.trim());
        }
        if let Some(mut value) = llm_config {
            value.provider = record.llm_config.provider.clone();
            record.llm_config = value;
        }
        if let Some(value) = work_mode {
            record.work_mode = value;
        }
        record.updated_at = now();
        self.save_settings(&settings).await?;
        Ok(self
            .list_threads()
            .await
            .into_iter()
            .find(|thread| thread.id == id))
    }
    pub async fn continue_thread_in_mode(
        &self,
        id: &str,
        work_mode: WorkMode,
        developer_instructions: &str,
    ) -> Result<Option<ThreadSummary>, String> {
        let source = self
            .list_threads()
            .await
            .into_iter()
            .find(|thread| thread.id == id);
        let Some(source) = source else {
            return Ok(None);
        };
        if source.work_mode == work_mode {
            return Ok(Some(source));
        }

        let workspace = self.workspace_root().await;
        let continued_id = self
            .app_server
            .fork_thread(&workspace, id, developer_instructions, &source.llm_config)
            .await?;
        self.app_server
            .set_thread_name(&continued_id, &source.title)
            .await?;

        let timestamp = now();
        let mut settings = self.load_settings().await;
        settings.threads.insert(
            continued_id.clone(),
            StoredThread {
                title: source.title,
                llm_config: source.llm_config,
                goal_id: source.goal_id,
                work_mode,
                created_at: timestamp.clone(),
                updated_at: timestamp,
            },
        );
        self.save_settings(&settings).await?;
        Ok(self
            .list_threads()
            .await
            .into_iter()
            .find(|thread| thread.id == continued_id))
    }
    pub async fn list_threads(&self) -> Vec<ThreadSummary> {
        let settings = self.load_settings().await;
        let runs = self.state.read().await.runs.clone();
        let mut ids: HashSet<String> = settings.threads.keys().cloned().collect();
        ids.extend(runs.iter().filter_map(|run| run.thread_id.clone()));
        let mut result = Vec::new();
        for id in ids {
            let mut grouped = runs
                .iter()
                .filter(|run| run.thread_id.as_deref() == Some(&id))
                .collect::<Vec<_>>();
            grouped.sort_by(|left, right| left.created_at.cmp(&right.created_at));
            let record = settings.threads.get(&id);
            let first = grouped.first().copied();
            let last = grouped.last().copied();
            let title = record
                .map(|item| item.title.clone())
                .or_else(|| first.map(|run| prompt_title(&run.prompt)))
                .unwrap_or_else(|| "Untitled thread".into());
            let llm_config = record
                .map(|item| item.llm_config.clone())
                .or_else(|| first.map(|run| run.llm_config.clone()))
                .unwrap_or_else(|| settings.default_llm_config.clone());
            result.push(ThreadSummary {
                id: id.clone(),
                title,
                llm_config,
                goal_id: record
                    .and_then(|item| item.goal_id.clone())
                    .or_else(|| last.and_then(|run| run.goal_id.clone())),
                work_mode: record
                    .map(|item| item.work_mode.clone())
                    .unwrap_or_default(),
                run_count: grouped.len(),
                created_at: record
                    .map(|item| item.created_at.clone())
                    .or_else(|| first.map(|run| run.created_at.clone()))
                    .unwrap_or_else(now),
                updated_at: record
                    .map(|item| {
                        if item.updated_at.is_empty() {
                            item.created_at.clone()
                        } else {
                            item.updated_at.clone()
                        }
                    })
                    .or_else(|| last.map(|run| run.created_at.clone()))
                    .unwrap_or_else(now),
            });
        }
        result.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        result
    }
    pub async fn assign_app_server_thread(
        &self,
        id: &str,
        goal_id: String,
    ) -> Result<AppServerThread, String> {
        let mut thread = self.app_server_thread(id).await?;
        let mut settings = self.load_settings().await;
        let default = settings.default_llm_config.clone();
        let timestamp = now();
        let record = settings
            .threads
            .entry(id.into())
            .or_insert_with(|| StoredThread {
                title: thread
                    .name
                    .clone()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or_else(|| thread.preview.chars().take(80).collect()),
                llm_config: default,
                goal_id: None,
                work_mode: WorkMode::default(),
                created_at: timestamp.clone(),
                updated_at: timestamp.clone(),
            });
        record.goal_id = Some(goal_id.clone());
        record.updated_at = timestamp;
        self.save_settings(&settings).await?;
        thread.goal_id = Some(goal_id);
        redact_app_server_thread(&mut thread);
        Ok(thread)
    }
    pub async fn workspace_root(&self) -> PathBuf {
        self.state.read().await.workspace_root.clone()
    }
    pub async fn executable(&self) -> String {
        self.state.read().await.executable.clone()
    }
    pub async fn app_server_threads(
        &self,
        cursor: Option<String>,
        limit: Option<u32>,
    ) -> Result<AppServerThreadPage, String> {
        let (workspace, runs) = {
            let state = self.state.read().await;
            (state.workspace_root.clone(), state.runs.clone())
        };
        let mut page = self
            .app_server
            .list_threads(&workspace, cursor, limit)
            .await?;
        let mut settings = self.load_settings().await;
        let default = settings.default_llm_config.clone();
        let mut settings_changed = false;
        for thread in &mut page.data {
            let native_title = thread
                .name
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&thread.preview)
                .trim();
            let is_new = !settings.threads.contains_key(&thread.id);
            let record = settings
                .threads
                .entry(thread.id.clone())
                .or_insert_with(|| StoredThread {
                    title: if native_title.is_empty() {
                        "Untitled thread".into()
                    } else {
                        native_title.into()
                    },
                    llm_config: default.clone(),
                    goal_id: None,
                    work_mode: WorkMode::default(),
                    created_at: app_server_time(thread.created_at),
                    updated_at: app_server_time(thread.updated_at),
                });
            settings_changed |= is_new;
            if !native_title.is_empty() && record.title != native_title {
                record.title = native_title.into();
                settings_changed = true;
            }
            let native_updated_at = app_server_time(thread.updated_at);
            if record.updated_at != native_updated_at {
                record.updated_at = native_updated_at;
                settings_changed = true;
            }
            thread.goal_id = settings
                .threads
                .get(&thread.id)
                .and_then(|record| record.goal_id.clone())
                .or_else(|| {
                    runs.iter()
                        .rev()
                        .find(|run| run.thread_id.as_deref() == Some(&thread.id))
                        .and_then(|run| run.goal_id.clone())
                });
            redact_app_server_thread(thread);
        }
        if settings_changed {
            self.save_settings(&settings).await?;
        }
        Ok(page)
    }
    pub async fn app_server_thread(&self, id: &str) -> Result<AppServerThread, String> {
        let (workspace, run_goal_id) = {
            let state = self.state.read().await;
            (
                state.workspace_root.clone(),
                state
                    .runs
                    .iter()
                    .rev()
                    .find(|run| run.thread_id.as_deref() == Some(id))
                    .and_then(|run| run.goal_id.clone()),
            )
        };
        let mut thread = self.app_server.read_thread(&workspace, id).await?;
        let settings = self.load_settings().await;
        thread.goal_id = settings
            .threads
            .get(id)
            .and_then(|record| record.goal_id.clone())
            .or(run_goal_id);
        redact_app_server_thread(&mut thread);
        Ok(thread)
    }
    pub fn app_server_events(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::models::AppServerEvent> {
        self.app_server.subscribe()
    }
    pub async fn pending_approvals(&self) -> Vec<crate::models::AppServerApproval> {
        self.app_server.pending_approvals().await
    }
    pub async fn app_server_rate_limits(&self) -> Result<Value, String> {
        self.app_server.rate_limits().await
    }
    pub async fn app_server_models(&self) -> Result<Value, String> {
        self.app_server.models().await
    }
    pub async fn decide_approval(&self, id: &str, decision: &str) -> Result<(), String> {
        self.app_server.decide_approval(id, decision).await
    }
    pub async fn codex_auth_status(&self) -> CodexAuthStatus {
        let executable = self.executable().await;
        let output = Command::new(&executable)
            .args(["login", "status"])
            .output()
            .await;
        match output {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let message = if stdout.is_empty() {
                    stderr.clone()
                } else {
                    stdout.clone()
                };
                let lower = message.to_lowercase();
                let authenticated = output.status.success() && lower.contains("logged in");
                let method = if authenticated {
                    if lower.contains("chatgpt") {
                        Some("ChatGPT".into())
                    } else if lower.contains("api") {
                        Some("API key".into())
                    } else if lower.contains("access token") {
                        Some("Access token".into())
                    } else {
                        Some("Codex".into())
                    }
                } else {
                    None
                };
                CodexAuthStatus {
                    executable,
                    available: true,
                    authenticated,
                    method,
                    message: if message.is_empty() {
                        if authenticated {
                            "Logged in".into()
                        } else {
                            "Not logged in".into()
                        }
                    } else {
                        message
                    },
                }
            }
            Err(error) => CodexAuthStatus {
                executable,
                available: false,
                authenticated: false,
                method: None,
                message: format!("Codex executable not found or not runnable: {error}"),
            },
        }
    }
    pub async fn start_codex_login(&self) -> Result<CodexAuthAction, String> {
        let executable = self.executable().await;
        Command::new(&executable)
            .arg("login")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start Codex login: {error}"))?;
        Ok(CodexAuthAction {
            status: "started".into(),
            message: "Started the official Codex login flow. Complete the browser prompt, then refresh status.".into(),
        })
    }
    pub async fn codex_logout(&self) -> Result<CodexAuthAction, String> {
        let executable = self.executable().await;
        let output = Command::new(&executable)
            .arg("logout")
            .output()
            .await
            .map_err(|error| format!("failed to run Codex logout: {error}"))?;
        if output.status.success() {
            Ok(CodexAuthAction {
                status: "logged_out".into(),
                message: "Codex credentials were cleared.".into(),
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            Err(if stderr.is_empty() { stdout } else { stderr })
        }
    }
    pub async fn switch_workspace(
        &self,
        workspace_root: PathBuf,
        history_path: PathBuf,
    ) -> Result<(), String> {
        {
            let state = self.state.read().await;
            if state
                .runs
                .iter()
                .any(|run| run.status == "queued" || run.status == "running")
            {
                return Err("cannot switch workspace while a Codex run is active".into());
            }
        }
        let runs = Self::load_history(&history_path).await;
        let mut state = self.state.write().await;
        state.workspace_root = workspace_root.canonicalize().unwrap_or(workspace_root);
        state.history_path = history_path;
        state.settings_path = state.history_path.with_file_name("thread-settings.json");
        state.runs = runs;
        Ok(())
    }
    fn resolve_cwd(root: &Path, value: &str) -> Result<PathBuf, String> {
        let candidate = root
            .join(value)
            .canonicalize()
            .map_err(|_| "working_directory does not exist".to_string())?;
        if candidate != root && !candidate.starts_with(root) {
            return Err("working_directory must be inside the configured workspace".into());
        }
        if !candidate.is_dir() {
            return Err("working_directory does not exist".into());
        }
        Ok(candidate)
    }

    async fn prepare_worker_credential_isolation(
        &self,
        worker: &Worker,
    ) -> Result<WorkerCredentialIsolation, String> {
        let worker_id = worker.id.as_str();
        if worker_id.is_empty()
            || !worker_id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err("worker identity is unsafe for credential-isolation paths".into());
        }
        let runtime_root = {
            let state = self.state.read().await;
            state
                .history_path
                .parent()
                .ok_or_else(|| "run history has no parent directory".to_string())?
                .join("worker-runtime")
                .join(worker_id)
        };
        let home_directory = runtime_root.join("home");
        let temporary_directory = runtime_root.join("tmp");
        tokio::fs::create_dir_all(&home_directory)
            .await
            .map_err(|error| format!("could not create isolated worker home: {error}"))?;
        tokio::fs::create_dir_all(&temporary_directory)
            .await
            .map_err(|error| {
                format!("could not create isolated worker temporary directory: {error}")
            })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let private = fs::Permissions::from_mode(0o700);
            fs::set_permissions(&runtime_root, private.clone())
                .map_err(|error| format!("could not secure worker runtime directory: {error}"))?;
            fs::set_permissions(&home_directory, private.clone())
                .map_err(|error| format!("could not secure isolated worker home: {error}"))?;
            fs::set_permissions(&temporary_directory, private)
                .map_err(|error| format!("could not secure worker temporary directory: {error}"))?;
        }
        let home = home_directory.to_string_lossy().into_owned();
        let temporary = temporary_directory.to_string_lossy().into_owned();
        let mut environment: BTreeMap<String, String> = BTreeMap::new();
        environment.insert("CI".into(), "1".into());
        environment.insert("GIT_TERMINAL_PROMPT".into(), "0".into());
        environment.insert("HOME".into(), home.clone());
        environment.insert("LOGNAME".into(), "golazo-worker".into());
        environment.insert(
            "PATH".into(),
            std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin:/usr/sbin:/sbin".into()),
        );
        environment.insert("SHELL".into(), "/bin/sh".into());
        environment.insert("TEMP".into(), temporary.clone());
        environment.insert("TMP".into(), temporary.clone());
        environment.insert("TMPDIR".into(), temporary.clone());
        environment.insert("USER".into(), "golazo-worker".into());
        for name in ["LANG", "LC_ALL", "LC_CTYPE", "TERM"] {
            if let Ok(value) = std::env::var(name) {
                environment.insert(name.into(), value);
            }
        }
        debug_assert!(
            environment
                .keys()
                .all(|name| !crate::coordination::security::is_sensitive_environment_name(name))
        );
        Ok(WorkerCredentialIsolation {
            worker_id: worker_id.into(),
            home_directory: home,
            temporary_directory: temporary,
            inherited_environment: "none".into(),
            environment,
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        self: &Arc<Self>,
        prompt: String,
        image_inputs: Vec<ImageAttachmentCreate>,
        file_inputs: Vec<FileAttachmentCreate>,
        working_directory: String,
        sandbox: String,
        approval_policy: String,
        approvals_reviewer: String,
        ephemeral_thread: bool,
        goal_id: Option<String>,
        thread_id: Option<String>,
        execution_prompt: Option<String>,
        output_schema: Option<Value>,
        work_mode: WorkMode,
        llm_config: Option<LLMConfig>,
    ) -> Result<Run, String> {
        self.create_scoped(
            prompt,
            image_inputs,
            file_inputs,
            working_directory,
            sandbox,
            approval_policy,
            approvals_reviewer,
            ephemeral_thread,
            goal_id,
            thread_id,
            execution_prompt,
            output_schema,
            work_mode,
            llm_config,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_for_worker(
        self: &Arc<Self>,
        worker: &Worker,
        prompt: String,
        image_inputs: Vec<ImageAttachmentCreate>,
        file_inputs: Vec<FileAttachmentCreate>,
        working_directory: String,
        ephemeral_thread: bool,
        thread_id: Option<String>,
        execution_prompt: Option<String>,
        output_schema: Option<Value>,
        work_mode: WorkMode,
        llm_config: Option<LLMConfig>,
    ) -> Result<Run, String> {
        let workspace = worker
            .workspace
            .as_ref()
            .ok_or_else(|| "worker has no isolated workspace".to_string())?;
        let permissions = worker
            .permission_profile
            .as_ref()
            .ok_or_else(|| "worker has no permission profile".to_string())?;
        validate_permissions(
            &permissions.sandbox,
            &permissions.approval_policy,
            &permissions.approvals_reviewer,
        )?;
        crate::coordination::pool::validate_worker_permissions(permissions)
            .map_err(|error| error.to_string())?;
        if permissions.sandbox == "danger-full-access" || permissions.network_access {
            return Err(
                "autonomous workers must use a bounded filesystem sandbox with network disabled"
                    .into(),
            );
        }
        if !permissions
            .tool_capabilities
            .contains(&WorkerToolCapability::ReadFiles)
            || !permissions
                .tool_capabilities
                .contains(&WorkerToolCapability::RunCommands)
        {
            return Err(
                "worker permission profile does not allow the Codex execution tools".into(),
            );
        }
        if work_mode == WorkMode::Build
            && !permissions
                .tool_capabilities
                .contains(&WorkerToolCapability::WriteFiles)
        {
            return Err("build-mode worker does not have file-write capability".into());
        }
        let boundary = PathBuf::from(&workspace.worktree_path)
            .canonicalize()
            .map_err(|_| "worker workspace does not exist".to_string())?;
        if boundary.to_string_lossy() != workspace.worktree_path {
            return Err("worker workspace binding is not canonical".into());
        }
        let credential_isolation = self.prepare_worker_credential_isolation(worker).await?;
        self.create_scoped(
            prompt,
            image_inputs,
            file_inputs,
            working_directory,
            permissions.sandbox.clone(),
            permissions.approval_policy.clone(),
            permissions.approvals_reviewer.clone(),
            ephemeral_thread,
            Some(worker.goal_id.clone()),
            thread_id,
            execution_prompt,
            output_schema,
            work_mode,
            llm_config,
            Some(boundary),
            Some(credential_isolation),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_scoped(
        self: &Arc<Self>,
        prompt: String,
        image_inputs: Vec<ImageAttachmentCreate>,
        file_inputs: Vec<FileAttachmentCreate>,
        working_directory: String,
        sandbox: String,
        approval_policy: String,
        approvals_reviewer: String,
        ephemeral_thread: bool,
        goal_id: Option<String>,
        thread_id: Option<String>,
        execution_prompt: Option<String>,
        output_schema: Option<Value>,
        work_mode: WorkMode,
        llm_config: Option<LLMConfig>,
        workspace_boundary: Option<PathBuf>,
        credential_isolation: Option<WorkerCredentialIsolation>,
    ) -> Result<Run, String> {
        validate_permissions(&sandbox, &approval_policy, &approvals_reviewer)?;
        let (root, executable) = {
            let state = self.state.read().await;
            (state.workspace_root.clone(), state.executable.clone())
        };
        let cwd = Self::resolve_cwd(
            workspace_boundary.as_deref().unwrap_or(&root),
            &working_directory,
        )?;
        let effective_config = if let Some(thread_id) = thread_id.as_deref() {
            let thread = self
                .list_threads()
                .await
                .into_iter()
                .find(|thread| thread.id == thread_id);
            let mut config = llm_config
                .clone()
                .or_else(|| thread.as_ref().map(|item| item.llm_config.clone()))
                .unwrap_or_else(|| LLMConfig::default());
            if let Some(thread) = &thread {
                config.provider = thread.llm_config.provider.clone();
            } else {
                self.ensure_thread(thread_id, &prompt, &config, goal_id.as_deref(), &work_mode)
                    .await;
            }
            if llm_config.is_some() && thread.is_some() {
                let _ = self
                    .update_thread(thread_id, None, Some(config.clone()), None)
                    .await;
            }
            config
        } else {
            llm_config.unwrap_or(self.default_llm_config().await)
        };
        let run_id = Uuid::new_v4().to_string();
        let images = Self::persist_images(&root, &run_id, image_inputs).await?;
        let files = Self::persist_files(&root, &run_id, file_inputs).await?;
        let execution_prompt = if files.is_empty() {
            execution_prompt
        } else {
            let file_list = files
                .iter()
                .map(|file| format!("- {} ({})", file.path, file.name))
                .collect::<Vec<_>>()
                .join("\n");
            let file_context = crate::prompts::file_attachments(&file_list);
            Some(match execution_prompt {
                Some(context) if !context.trim().is_empty() => {
                    format!("{context}\n\n{file_context}")
                }
                _ => file_context,
            })
        };
        let run = Run {
            id: run_id,
            prompt,
            images,
            files,
            cwd: cwd.to_string_lossy().into(),
            sandbox,
            approval_policy,
            approvals_reviewer,
            ephemeral_thread,
            credential_isolation,
            execution_prompt,
            output_schema,
            goal_id,
            resumed_from: thread_id.clone(),
            status: "queued".into(),
            created_at: now(),
            started_at: None,
            finished_at: None,
            return_code: None,
            thread_id,
            work_mode,
            llm_config: effective_config,
            final_message: None,
            error: None,
            usage: Usage::default(),
            events: vec![],
        };
        run_correlation(&run).emit_info("run.queued", "queued");
        {
            self.state.write().await.runs.push(run.clone());
        }
        let manager = Arc::clone(self);
        let id = run.id.clone();
        tokio::spawn(async move {
            manager.execute_app_server(id, executable).await;
        });
        Ok(run)
    }
    async fn persist_images(
        root: &Path,
        run_id: &str,
        inputs: Vec<ImageAttachmentCreate>,
    ) -> Result<Vec<RunImage>, String> {
        const MAX_IMAGES: usize = 4;
        const MAX_IMAGE_BYTES: usize = 10 * 1024 * 1024;
        const MAX_TOTAL_BYTES: usize = 20 * 1024 * 1024;
        if inputs.len() > MAX_IMAGES {
            return Err(format!("attach at most {MAX_IMAGES} images"));
        }
        let mut decoded = Vec::with_capacity(inputs.len());
        let mut total_bytes = 0usize;
        for input in inputs {
            if input.name.trim().is_empty() || input.name.len() > 255 {
                return Err("image name must be between 1 and 255 characters".into());
            }
            let (mime_type, extension) = match input.mime_type.to_ascii_lowercase().as_str() {
                "image/png" => ("image/png", "png"),
                "image/jpeg" | "image/jpg" => ("image/jpeg", "jpg"),
                "image/webp" => ("image/webp", "webp"),
                "image/gif" => ("image/gif", "gif"),
                _ => return Err("images must be PNG, JPEG, WebP, or GIF".into()),
            };
            let bytes = decode_attachment_base64(&input.data)?;
            if bytes.is_empty() {
                return Err("image data must not be empty".into());
            }
            if bytes.len() > MAX_IMAGE_BYTES {
                return Err("each image must be 10 MB or smaller".into());
            }
            total_bytes += bytes.len();
            if total_bytes > MAX_TOTAL_BYTES {
                return Err("attached images must total 20 MB or less".into());
            }
            decoded.push((input.name, mime_type.to_string(), extension, bytes));
        }
        if decoded.is_empty() {
            return Ok(Vec::new());
        }
        let directory = root.join(".goal-manager").join("attachments").join(run_id);
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| format!("could not create attachment directory: {error}"))?;
        let mut images = Vec::with_capacity(decoded.len());
        for (index, (name, mime_type, extension, bytes)) in decoded.into_iter().enumerate() {
            let id = format!("image-{}", index + 1);
            let path = directory.join(format!("{id}.{extension}"));
            tokio::fs::write(&path, bytes)
                .await
                .map_err(|error| format!("could not save pasted image: {error}"))?;
            images.push(RunImage {
                id,
                name,
                mime_type,
                path: path.to_string_lossy().into_owned(),
            });
        }
        Ok(images)
    }

    async fn persist_files(
        root: &Path,
        run_id: &str,
        inputs: Vec<FileAttachmentCreate>,
    ) -> Result<Vec<RunFile>, String> {
        const MAX_FILES: usize = 8;
        const MAX_FILE_BYTES: usize = 25 * 1024 * 1024;
        const MAX_TOTAL_BYTES: usize = 50 * 1024 * 1024;
        if inputs.len() > MAX_FILES {
            return Err(format!("attach at most {MAX_FILES} files"));
        }
        let mut decoded = Vec::with_capacity(inputs.len());
        let mut total_bytes = 0usize;
        for input in inputs {
            let name = input.name.trim();
            if name.is_empty() || name.len() > 255 {
                return Err("file name must be between 1 and 255 characters".into());
            }
            let bytes = decode_attachment_base64(&input.data)?;
            if bytes.is_empty() {
                return Err("file data must not be empty".into());
            }
            if bytes.len() > MAX_FILE_BYTES {
                return Err("each file must be 25 MB or smaller".into());
            }
            total_bytes += bytes.len();
            if total_bytes > MAX_TOTAL_BYTES {
                return Err("attached files must total 50 MB or less".into());
            }
            let safe_name: String = name
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                        character
                    } else {
                        '_'
                    }
                })
                .collect();
            decoded.push((name.to_string(), input.mime_type, safe_name, bytes));
        }
        if decoded.is_empty() {
            return Ok(Vec::new());
        }
        let directory = root
            .join(".goal-manager")
            .join("attachments")
            .join(run_id)
            .join("files");
        tokio::fs::create_dir_all(&directory)
            .await
            .map_err(|error| format!("could not create file attachment directory: {error}"))?;
        let mut files = Vec::with_capacity(decoded.len());
        for (index, (name, mime_type, safe_name, bytes)) in decoded.into_iter().enumerate() {
            let id = format!("file-{}", index + 1);
            let path = directory.join(format!("{id}-{safe_name}"));
            tokio::fs::write(&path, bytes)
                .await
                .map_err(|error| format!("could not save attached file: {error}"))?;
            files.push(RunFile {
                id,
                name,
                mime_type,
                path: path.to_string_lossy().into_owned(),
            });
        }
        Ok(files)
    }
    async fn mutate(&self, id: &str, change: impl FnOnce(&mut Run)) {
        if let Some(run) = self
            .state
            .write()
            .await
            .runs
            .iter_mut()
            .find(|run| run.id == id)
        {
            change(run);
        }
    }
    async fn execute_app_server(self: Arc<Self>, id: String, fallback_executable: String) {
        self.mutate(&id, |run| {
            run.status = "running".into();
            run.started_at = Some(now());
        })
        .await;
        let Some(current) = self.get(&id).await else {
            return;
        };
        let mut events = self.app_server.subscribe();
        let environment_config = current
            .credential_isolation
            .as_ref()
            .map(worker_shell_environment_config);
        let started = self
            .app_server
            .start_turn(
                Path::new(&current.cwd),
                current.resumed_from.as_deref(),
                &current.prompt,
                current.execution_prompt.as_deref(),
                current.output_schema.as_ref(),
                &current.images,
                &current.sandbox,
                &current.approval_policy,
                &current.approvals_reviewer,
                &current.llm_config,
                current.ephemeral_thread,
                environment_config.as_ref(),
            )
            .await;
        let started = match started {
            Ok(started) => started,
            Err(_) => {
                self.execute(id, fallback_executable).await;
                return;
            }
        };
        let thread_id = started.thread_id.clone();
        let turn_id = started.turn_id.clone();
        let mut seen_events: HashSet<u64> = HashSet::new();
        let mut agent_message_phases = HashMap::new();
        self.mutate(&id, |run| run.thread_id = Some(thread_id.clone()))
            .await;
        run_correlation(&current)
            .with_thread(&thread_id)
            .emit_info("thread.turn.started", "running");
        if !current.ephemeral_thread {
            self.ensure_thread(
                &thread_id,
                &current.prompt,
                &current.llm_config,
                current.goal_id.as_deref(),
                &current.work_mode,
            )
            .await;
            if current.resumed_from.is_none() {
                let _ = self
                    .app_server
                    .set_thread_name(&thread_id, &prompt_title(&current.prompt))
                    .await;
            }
        }
        loop {
            let event = match events.recv().await {
                Ok(event) => event,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            };
            let event_thread = event.params.get("threadId").and_then(Value::as_str);
            if event_thread != Some(&thread_id) {
                continue;
            }
            let event_turn = event
                .params
                .get("turnId")
                .and_then(Value::as_str)
                .or_else(|| event.params.pointer("/turn/id").and_then(Value::as_str));
            if event_turn != Some(&turn_id) {
                continue;
            }
            if !seen_events.insert(event.sequence) {
                continue;
            }
            if event.method == "item/started"
                && event.params.pointer("/item/type").and_then(Value::as_str)
                    == Some("agentMessage")
                && let (Some(item_id), Some(phase)) = (
                    event.params.pointer("/item/id").and_then(Value::as_str),
                    event.params.pointer("/item/phase").and_then(Value::as_str),
                )
            {
                agent_message_phases.insert(item_id.to_string(), phase.to_string());
            }
            let is_final_answer_delta = event.method == "item/agentMessage/delta"
                && event
                    .params
                    .get("itemId")
                    .and_then(Value::as_str)
                    .and_then(|item_id| agent_message_phases.get(item_id))
                    .is_some_and(|phase| phase == "final_answer");
            let mut durable_params = event.params.clone();
            redact_environment_credentials(&mut durable_params);
            self.mutate(&id, |run| {
                run.events
                    .push(serde_json::json!({"method": event.method, "params": durable_params}));
                if run.events.len() > 1000 {
                    run.events.remove(0);
                }
                if is_final_answer_delta
                    && let Some(delta) = event.params.get("delta").and_then(Value::as_str)
                {
                    run.final_message
                        .get_or_insert_with(String::new)
                        .push_str(&redact_environment_credentials_text(delta));
                }
                if event.method == "thread/tokenUsage/updated"
                    && let Some(last) = event.params.pointer("/tokenUsage/last")
                {
                    run.usage.input_tokens =
                        last.get("inputTokens").and_then(Value::as_i64).unwrap_or(0);
                    run.usage.cached_input_tokens = last
                        .get("cachedInputTokens")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    run.usage.output_tokens = last
                        .get("outputTokens")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    run.usage.reasoning_output_tokens = last
                        .get("reasoningOutputTokens")
                        .and_then(Value::as_i64)
                        .unwrap_or(0);
                    run.usage.total_tokens =
                        last.get("totalTokens").and_then(Value::as_i64).unwrap_or(0);
                }
            })
            .await;
            if event.method == "turn/completed" {
                let status = event
                    .params
                    .pointer("/turn/status")
                    .and_then(Value::as_str)
                    .unwrap_or("completed");
                self.mutate(&id, |run| {
                    run.status = if status == "completed" {
                        "completed".into()
                    } else {
                        "failed".into()
                    };
                    run.finished_at = Some(now());
                    run.return_code = Some(if status == "completed" { 0 } else { 1 });
                    if status != "completed" {
                        run.error = Some(format!("Codex turn ended with status {status}"));
                    }
                })
                .await;
                run_correlation(&current)
                    .with_thread(&thread_id)
                    .emit_info("thread.turn.finished", status);
                self.save().await;
                return;
            }
        }
        self.mutate(&id, |run| {
            run.status = "failed".into();
            run.error = Some("Codex app-server event stream disconnected".into());
            run.finished_at = Some(now());
        })
        .await;
        run_correlation(&current)
            .with_thread(&thread_id)
            .emit_warn("thread.turn.finished", "event_stream_disconnected");
        self.save().await;
    }
    async fn execute(self: Arc<Self>, id: String, executable: String) {
        self.mutate(&id, |run| {
            run.status = "running".into();
            run.started_at = Some(now());
        })
        .await;
        let current = {
            self.state
                .read()
                .await
                .runs
                .iter()
                .find(|run| run.id == id)
                .cloned()
        };
        let Some(run) = current else {
            return;
        };
        run_correlation(&run).emit_info("run.started", "running");
        let cwd = PathBuf::from(&run.cwd);
        let skip_git = !cwd.ancestors().any(|parent| parent.join(".git").exists());
        let prompt = run.prompt.clone();
        let mut args = codex_exec_args(&run.sandbox, &run.approval_policy, &run.approvals_reviewer);
        if let Some(policy) = &run.credential_isolation {
            append_worker_shell_environment_args(&mut args, policy);
        }
        if let Some(instructions) = run.execution_prompt.as_deref() {
            args.extend([
                "--config".into(),
                format!(
                    "developer_instructions={}",
                    serde_json::to_string(instructions).unwrap_or_else(|_| "\"\"".into())
                ),
            ]);
        }
        if let Some(thread) = &run.resumed_from {
            args.extend(["resume".into(), "--json".into()]);
            for image in &run.images {
                args.extend(["--image".into(), image.path.clone()]);
            }
            if !run.llm_config.model.is_empty() {
                args.extend(["--model".into(), run.llm_config.model.clone()]);
            }
            args.extend([
                "--config".into(),
                format!(
                    "model_reasoning_effort=\"{}\"",
                    reasoning_name(&run.llm_config.reasoning)
                ),
            ]);
            if matches!(run.llm_config.speed, SpeedMode::Fast) {
                args.extend([
                    "--config".into(),
                    "service_tier=\"fast\"".into(),
                    "--config".into(),
                    "features.fast_mode=true".into(),
                ]);
            }
            if skip_git {
                args.push("--skip-git-repo-check".into());
            }
            args.extend([thread.clone(), prompt]);
        } else {
            args.push("--json".into());
            for image in &run.images {
                args.extend(["--image".into(), image.path.clone()]);
            }
            if matches!(
                run.llm_config.provider,
                LLMProvider::Ollama | LLMProvider::Lmstudio
            ) {
                args.extend([
                    "--oss".into(),
                    "--local-provider".into(),
                    provider_name(&run.llm_config.provider).into(),
                ]);
            }
            if !run.llm_config.model.is_empty() {
                args.extend(["--model".into(), run.llm_config.model.clone()]);
            }
            args.extend([
                "--config".into(),
                format!(
                    "model_reasoning_effort=\"{}\"",
                    reasoning_name(&run.llm_config.reasoning)
                ),
            ]);
            if matches!(run.llm_config.speed, SpeedMode::Fast) {
                args.extend([
                    "--config".into(),
                    "service_tier=\"fast\"".into(),
                    "--config".into(),
                    "features.fast_mode=true".into(),
                ]);
            }
            if skip_git {
                args.push("--skip-git-repo-check".into());
            }
            args.push(prompt);
        }
        let spawned = Command::new(&executable)
            .args(&args)
            .current_dir(&cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn();
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => {
                self.mutate(&id, |run| {
                    run.status = "failed".into();
                    run.error = Some(format!(
                        "Codex executable not found: {executable} ({error})"
                    ));
                    run.finished_at = Some(now());
                })
                .await;
                run_correlation(&run).emit_warn("run.finished", "spawn_failed");
                self.save().await;
                return;
            }
        };
        let stderr = child.stderr.take().unwrap();
        let stderr_task = tokio::spawn(async move {
            let mut data = Vec::new();
            let mut reader = BufReader::new(stderr);
            let _ = reader.read_to_end(&mut data).await;
            data
        });
        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut event: Value = serde_json::from_str(&line)
                .unwrap_or_else(|_| serde_json::json!({"type":"unparsed","text":line}));
            redact_environment_credentials(&mut event);
            let started_thread = if event["type"] == "thread.started" {
                event["thread_id"].as_str().map(str::to_string)
            } else {
                None
            };
            self.mutate(&id, |run| {
                run.events.push(event.clone());
                if run.events.len() > 1000 {
                    let excess = run.events.len() - 1000;
                    run.events.drain(..excess);
                }
                if event["type"] == "thread.started" {
                    run.thread_id = event["thread_id"].as_str().map(str::to_string);
                }
                if event["type"] == "turn.completed" {
                    let usage = &event["usage"];
                    run.usage.input_tokens = usage["input_tokens"].as_i64().unwrap_or(0);
                    run.usage.cached_input_tokens =
                        usage["cached_input_tokens"].as_i64().unwrap_or(0);
                    run.usage.output_tokens = usage["output_tokens"].as_i64().unwrap_or(0);
                    run.usage.reasoning_output_tokens =
                        usage["reasoning_output_tokens"].as_i64().unwrap_or(0);
                    run.usage.total_tokens = run.usage.input_tokens + run.usage.output_tokens;
                }
                if event["type"] == "item.completed" && event["item"]["type"] == "agent_message" {
                    run.final_message = event["item"]["text"].as_str().map(str::to_string);
                }
            })
            .await;
            if let Some(thread) = started_thread
                && !run.ephemeral_thread
            {
                run_correlation(&run)
                    .with_thread(&thread)
                    .emit_info("thread.started", "running");
                self.ensure_thread(
                    &thread,
                    &run.prompt,
                    &run.llm_config,
                    run.goal_id.as_deref(),
                    &run.work_mode,
                )
                .await;
            }
        }
        let result = child.wait().await;
        let stderr = stderr_task.await.unwrap_or_default();
        self.mutate(&id, |run| {
            run.finished_at = Some(now());
            match result {
                Ok(status) if status.success() => {
                    run.status = "completed".into();
                    run.return_code = status.code();
                }
                Ok(status) => {
                    run.status = "failed".into();
                    run.return_code = status.code();
                    let message =
                        redact_environment_credentials_text(&String::from_utf8_lossy(&stderr));
                    run.error = Some(if message.is_empty() {
                        "Codex exited with an error".into()
                    } else {
                        message
                            .chars()
                            .rev()
                            .take(8000)
                            .collect::<String>()
                            .chars()
                            .rev()
                            .collect()
                    });
                }
                Err(error) => {
                    run.status = "failed".into();
                    run.error = Some(error.to_string());
                }
            }
        })
        .await;
        if let Some(completed) = self.get(&id).await {
            let outcome = completed.status.clone();
            if outcome == "completed" {
                run_correlation(&completed).emit_info("run.finished", &outcome);
            } else {
                run_correlation(&completed).emit_warn("run.finished", &outcome);
            }
        }
        self.save().await;
    }
    pub async fn get(&self, id: &str) -> Option<Run> {
        self.state
            .read()
            .await
            .runs
            .iter()
            .find(|run| run.id == id)
            .cloned()
    }
    pub async fn list(&self) -> Vec<Run> {
        self.state.read().await.runs.iter().rev().cloned().collect()
    }
    pub fn redacted_for_display(run: &Run) -> Run {
        redacted_copy(run).expect("Run always supports JSON redaction round-trip")
    }
    pub async fn usage(&self) -> Value {
        let state = self.state.read().await;
        Self::usage_value(&state.runs)
    }
    fn usage_value(runs: &[Run]) -> Value {
        let mut usage = Usage::default();
        let mut completed = 0;
        for run in runs {
            if run.status == "completed" {
                completed += 1;
            }
            usage.input_tokens += run.usage.input_tokens;
            usage.cached_input_tokens += run.usage.cached_input_tokens;
            usage.output_tokens += run.usage.output_tokens;
            usage.reasoning_output_tokens += run.usage.reasoning_output_tokens;
            usage.total_tokens += run.usage.total_tokens;
        }
        serde_json::json!({"input_tokens":usage.input_tokens,"cached_input_tokens":usage.cached_input_tokens,"output_tokens":usage.output_tokens,"reasoning_output_tokens":usage.reasoning_output_tokens,"total_tokens":usage.total_tokens,"runs":runs.len(),"completed_runs":completed})
    }
    pub async fn usage_for_history(path: &Path) -> Value {
        Self::usage_value(&Self::load_history(path).await)
    }

    pub async fn goal_recency_for_history(
        history_path: &Path,
        settings_path: &Path,
    ) -> HashMap<String, String> {
        let runs = Self::load_history(history_path).await;
        let settings = Self::load_settings_path(settings_path).await;
        let mut result = HashMap::new();
        let mut record = |goal_id: &str, timestamp: &str| {
            if timestamp.is_empty() {
                return;
            }
            let current = result
                .entry(goal_id.to_string())
                .or_insert_with(String::new);
            if timestamp > current.as_str() {
                *current = timestamp.to_string();
            }
        };
        for run in &runs {
            if let Some(goal_id) = run.goal_id.as_deref() {
                let timestamp = run
                    .finished_at
                    .as_deref()
                    .or(run.started_at.as_deref())
                    .unwrap_or(&run.created_at);
                record(goal_id, timestamp);
            }
        }
        for thread in settings.threads.values() {
            if let Some(goal_id) = thread.goal_id.as_deref() {
                let timestamp = if thread.updated_at.is_empty() {
                    &thread.created_at
                } else {
                    &thread.updated_at
                };
                record(goal_id, timestamp);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::domain::{WorkerPermissionProfile, WorkspaceBinding};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn accepts_only_supported_permission_presets() {
        assert!(validate_permissions("workspace-write", "on-request", "user").is_ok());
        assert!(validate_permissions("workspace-write", "on-request", "auto_review").is_ok());
        assert!(validate_permissions("danger-full-access", "never", "user").is_ok());
        assert!(validate_permissions("read-only", "on-request", "user").is_ok());
        assert!(validate_permissions("danger-full-access", "on-request", "user").is_err());
        assert!(validate_permissions("workspace-write", "never", "user").is_err());
    }

    #[test]
    fn places_global_approval_option_before_exec_subcommand() {
        assert_eq!(
            codex_exec_args("workspace-write", "on-request", "user"),
            vec![
                "--ask-for-approval",
                "on-request",
                "exec",
                "--sandbox",
                "workspace-write",
                "--config",
                "approvals_reviewer=\"user\"",
            ]
        );
    }

    #[test]
    fn worker_shell_environment_is_explicit_and_non_inheriting() {
        let policy = WorkerCredentialIsolation {
            worker_id: "worker-a".into(),
            home_directory: "/runtime/worker-a/home".into(),
            temporary_directory: "/runtime/worker-a/tmp".into(),
            inherited_environment: "none".into(),
            environment: BTreeMap::from([
                ("HOME".into(), "/runtime/worker-a/home".into()),
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("TMPDIR".into(), "/runtime/worker-a/tmp".into()),
            ]),
        };
        let config = worker_shell_environment_config(&policy);
        assert_eq!(
            config.pointer("/shell_environment_policy/inherit"),
            Some(&Value::String("none".into()))
        );
        assert_eq!(
            config.pointer("/shell_environment_policy/set/HOME"),
            Some(&Value::String("/runtime/worker-a/home".into()))
        );
        assert!(config.to_string().find("OPENAI_API_KEY").is_none());

        let mut args = Vec::new();
        append_worker_shell_environment_args(&mut args, &policy);
        assert!(
            args.iter()
                .any(|argument| argument == "shell_environment_policy.inherit=\"none\"")
        );
        assert!(args.iter().any(|argument| {
            argument == "shell_environment_policy.set.HOME=\"/runtime/worker-a/home\""
        }));
        assert!(!args.iter().any(|argument| argument.contains("API_KEY")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn worker_runs_are_rooted_in_the_bound_worktree_and_use_worker_permissions() {
        let temp = tempfile::tempdir().unwrap();
        let project = temp.path().join("project");
        let worktree = temp.path().join("worktree");
        fs::create_dir(&project).unwrap();
        fs::create_dir(&worktree).unwrap();
        let executable = temp.path().join("fake-codex");
        fs::write(&executable, "#!/bin/sh\nexit 1\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let manager = RunManager::new(
            project.clone(),
            executable.to_string_lossy().into(),
            project.join(".goal-manager/run-history.json"),
        )
        .await;
        let now = Utc::now();
        let mut worker = Worker::new("goal-a", now);
        let canonical_project = project.canonicalize().unwrap();
        let canonical_worktree = worktree.canonicalize().unwrap();
        worker.workspace = Some(WorkspaceBinding {
            repository_id: "repo-a".into(),
            canonical_repository_path: canonical_project.display().to_string(),
            worktree_path: canonical_worktree.display().to_string(),
            branch: "codex/golazo/goal-a/worker-a".into(),
            base_revision: "abc123".into(),
            created_at: Some(now),
            creation_evidence: vec!["test fixture".into()],
        });
        worker.permission_profile = Some(WorkerPermissionProfile::default());

        let run = manager
            .create_for_worker(
                &worker,
                "worker task".into(),
                vec![],
                vec![],
                ".".into(),
                true,
                None,
                None,
                None,
                WorkMode::Build,
                None,
            )
            .await
            .unwrap();

        assert_eq!(Path::new(&run.cwd), canonical_worktree);
        assert_eq!(run.sandbox, "workspace-write");
        assert_eq!(run.approval_policy, "on-request");
        assert_eq!(run.goal_id.as_deref(), Some("goal-a"));
        let isolation = run.credential_isolation.as_ref().unwrap();
        assert_eq!(isolation.worker_id, worker.id.as_str());
        assert_eq!(isolation.inherited_environment, "none");
        assert_eq!(
            isolation.environment.get("HOME"),
            Some(&isolation.home_directory)
        );
        assert_eq!(
            isolation.environment.get("TMPDIR"),
            Some(&isolation.temporary_directory)
        );
        assert!(Path::new(&isolation.home_directory).is_dir());
        assert!(Path::new(&isolation.temporary_directory).is_dir());
        assert!(
            isolation.environment.keys().all(|name| {
                !crate::coordination::security::is_sensitive_environment_name(name)
            })
        );
        assert_eq!(
            fs::metadata(&isolation.home_directory)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let second_worker = Worker::new("goal-a", now);
        let second_isolation = manager
            .prepare_worker_credential_isolation(&second_worker)
            .await
            .unwrap();
        assert_ne!(isolation.home_directory, second_isolation.home_directory);
        assert_ne!(
            isolation.temporary_directory,
            second_isolation.temporary_directory
        );
        assert!(
            manager
                .create_for_worker(
                    &worker,
                    "escape".into(),
                    vec![],
                    vec![],
                    "../project".into(),
                    true,
                    None,
                    None,
                    None,
                    WorkMode::Build,
                    None,
                )
                .await
                .is_err()
        );

        worker.permission_profile.as_mut().unwrap().network_access = true;
        assert!(
            manager
                .create_for_worker(
                    &worker,
                    "network".into(),
                    vec![],
                    vec![],
                    ".".into(),
                    true,
                    None,
                    None,
                    None,
                    WorkMode::Build,
                    None,
                )
                .await
                .is_err()
        );

        let permissions = worker.permission_profile.as_mut().unwrap();
        permissions.network_access = false;
        permissions
            .tool_capabilities
            .retain(|capability| *capability != WorkerToolCapability::WriteFiles);
        let error = manager
            .create_for_worker(
                &worker,
                "write without capability".into(),
                vec![],
                vec![],
                ".".into(),
                true,
                None,
                None,
                None,
                WorkMode::Build,
                None,
            )
            .await
            .unwrap_err();
        assert!(error.contains("file-write capability"));
    }

    #[tokio::test]
    async fn validates_and_persists_image_attachments() {
        let temp = tempfile::tempdir().unwrap();
        let images = RunManager::persist_images(
            temp.path(),
            "run-123",
            vec![ImageAttachmentCreate {
                name: "screenshot.png".into(),
                mime_type: "image/png".into(),
                data: "iVBORw==".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(images.len(), 1);
        assert_eq!(fs::read(&images[0].path).unwrap(), b"\x89PNG");
        assert!(images[0].path.contains(".goal-manager/attachments/run-123"));

        let error = RunManager::persist_images(
            temp.path(),
            "run-invalid",
            vec![ImageAttachmentCreate {
                name: "bad.bmp".into(),
                mime_type: "image/bmp".into(),
                data: "iVBORw==".into(),
            }],
        )
        .await
        .unwrap_err();
        assert!(error.contains("PNG, JPEG, WebP, or GIF"));
    }

    #[tokio::test]
    async fn validates_and_persists_file_attachments() {
        let temp = tempfile::tempdir().unwrap();
        let files = RunManager::persist_files(
            temp.path(),
            "run-files",
            vec![FileAttachmentCreate {
                name: "../requirements draft.md".into(),
                mime_type: "text/markdown".into(),
                data: "aGVsbG8=".into(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "../requirements draft.md");
        assert_eq!(fs::read(&files[0].path).unwrap(), b"hello");
        assert!(
            files[0]
                .path
                .contains("run-files/files/file-1-.._requirements_draft.md")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn defaults_new_threads_to_spec_mode_and_persists_mode_changes() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let settings_path = temp.path().join("thread-settings.json");
        let manager = RunManager::new(
            workspace,
            "/usr/bin/false".into(),
            temp.path().join("run-history.json"),
        )
        .await;

        manager
            .ensure_thread(
                "thread-1",
                "Define the implementation",
                &LLMConfig::default(),
                Some("goal-1"),
                &WorkMode::Spec,
            )
            .await;
        let created = manager
            .list_threads()
            .await
            .into_iter()
            .find(|thread| thread.id == "thread-1")
            .unwrap();
        assert_eq!(created.work_mode, WorkMode::Spec);

        let updated = manager
            .update_thread("thread-1", None, None, Some(WorkMode::Build))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.work_mode, WorkMode::Build);
        let stored: Value =
            serde_json::from_str(&fs::read_to_string(settings_path).unwrap()).unwrap();
        assert_eq!(stored["threads"]["thread-1"]["work_mode"], "build");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mode_changes_continue_in_a_forked_thread() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let executable = temp.path().join("fake-codex");
        let capture = temp.path().join("fork-request.json");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":1,\"result\":{{}}}}'\nIFS= read -r line\nIFS= read -r line\nprintf '%s\\n' \"$line\" > '{}'\nprintf '%s\\n' '{{\"id\":2,\"result\":{{\"thread\":{{\"id\":\"thread-build\"}}}}}}'\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":3,\"result\":{{}}}}'\n",
                capture.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let settings_path = temp.path().join("thread-settings.json");
        let manager = RunManager::new(
            workspace,
            executable.to_string_lossy().to_string(),
            temp.path().join("run-history.json"),
        )
        .await;
        manager
            .ensure_thread(
                "thread-spec",
                "Define the implementation",
                &LLMConfig::default(),
                Some("goal-1"),
                &WorkMode::Spec,
            )
            .await;

        let continued = manager
            .continue_thread_in_mode("thread-spec", WorkMode::Build, "Build mode guidance")
            .await
            .unwrap()
            .unwrap();

        assert_eq!(continued.id, "thread-build");
        assert_eq!(continued.goal_id.as_deref(), Some("goal-1"));
        assert_eq!(continued.work_mode, WorkMode::Build);
        let stored: Value =
            serde_json::from_str(&fs::read_to_string(settings_path).unwrap()).unwrap();
        assert_eq!(stored["threads"]["thread-spec"]["work_mode"], "spec");
        assert_eq!(stored["threads"]["thread-build"]["work_mode"], "build");
        let request: Value = serde_json::from_str(&fs::read_to_string(capture).unwrap()).unwrap();
        assert_eq!(request["method"], "thread/fork");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconciles_native_codex_thread_metadata_into_local_settings() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let executable = temp.path().join("fake-codex");
        let result = serde_json::json!({
            "data": [{
                "id": "native-thread-1",
                "cwd": workspace,
                "preview": "Original prompt",
                "name": "Renamed in Codex",
                "createdAt": 1_700_000_000_i64,
                "updatedAt": 1_700_000_100_i64,
                "modelProvider": "openai",
                "source": "appServer",
                "status": "idle",
                "turns": []
            }],
            "nextCursor": null,
            "backwardsCursor": null
        });
        let response = serde_json::json!({"id": 2, "result": result}).to_string();
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":1,\"result\":{{}}}}'\nIFS= read -r line\nIFS= read -r line\nprintf '%s\\n' '{}'\n",
                response
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let manager = RunManager::new(
            workspace,
            executable.to_string_lossy().into(),
            temp.path().join("run-history.json"),
        )
        .await;

        let page = manager.app_server_threads(None, Some(100)).await.unwrap();
        assert_eq!(page.data[0].name.as_deref(), Some("Renamed in Codex"));
        let threads = manager.list_threads().await;
        assert_eq!(threads[0].id, "native-thread-1");
        assert_eq!(threads[0].title, "Renamed in Codex");
        assert_eq!(threads[0].updated_at, "2023-11-14T22:15:00Z");
        assert!(threads[0].goal_id.is_none());
    }

    #[tokio::test]
    async fn captures_codex_jsonl_and_resumes_threads() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let executable = temp.path().join("fake-codex");
        let args_capture = temp.path().join("args.txt");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nif [ \"$1\" != \"app-server\" ]; then\n  printf '%s\\n' \"$@\" > '{}'\nfi\nprintf '%s\\n' '{{\"type\":\"thread.started\",\"thread_id\":\"thread-123\"}}'\nprintf '%s\\n' '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"completed\"}}}}'\nprintf '%s\\n' '{{\"type\":\"turn.completed\",\"usage\":{{\"input_tokens\":10,\"cached_input_tokens\":4,\"output_tokens\":3,\"reasoning_output_tokens\":1}}}}'\n",
                args_capture.display()
            ),
        )
        .unwrap();
        #[cfg(unix)]
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let manager = RunManager::new(
            workspace,
            executable.to_string_lossy().into(),
            temp.path().join("runs.json"),
        )
        .await;
        let created = manager
            .create(
                "Do work".into(),
                vec![],
                vec![FileAttachmentCreate {
                    name: "brief.txt".into(),
                    mime_type: "text/plain".into(),
                    data: "aGVsbG8=".into(),
                }],
                ".".into(),
                "workspace-write".into(),
                "on-request".into(),
                "user".into(),
                false,
                Some("goal-123".into()),
                Some("thread-old".into()),
                None,
                None,
                WorkMode::Build,
                Some(LLMConfig {
                    provider: LLMProvider::Openai,
                    model: "gpt-test".into(),
                    reasoning: ReasoningLevel::High,
                    speed: SpeedMode::Standard,
                }),
            )
            .await
            .unwrap();
        let execution_prompt = created.execution_prompt.as_deref().unwrap();
        assert!(execution_prompt.contains("The user uploaded these files"));
        assert!(execution_prompt.contains("brief.txt"));
        let completed = loop {
            let run = manager.get(&created.id).await.unwrap();
            if run.status == "completed" || run.status == "failed" {
                break run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        assert_eq!(completed.status, "completed");
        assert_eq!(completed.thread_id.as_deref(), Some("thread-123"));
        assert_eq!(completed.resumed_from.as_deref(), Some("thread-old"));
        assert_eq!(completed.final_message.as_deref(), Some("completed"));
        assert_eq!(completed.usage.total_tokens, 13);
        assert_eq!(completed.events.len(), 3);
        let args = fs::read_to_string(args_capture).unwrap();
        assert!(args.lines().any(|argument| argument == "Do work"));
        assert!(
            args.lines()
                .any(|argument| argument.starts_with("developer_instructions=")
                    && argument.contains("brief.txt"))
        );
        while !temp.path().join("runs.json").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(temp.path().join("runs.json").exists());
        let threads = manager.list_threads().await;
        assert!(threads.iter().any(|thread| thread.id == "thread-123"));
        assert_eq!(
            threads
                .iter()
                .find(|thread| thread.id == "thread-123")
                .unwrap()
                .goal_id
                .as_deref(),
            Some("goal-123")
        );
        assert_eq!(
            threads
                .iter()
                .find(|thread| thread.id == "thread-old")
                .unwrap()
                .llm_config
                .model,
            "gpt-test"
        );
        assert_eq!(
            threads
                .iter()
                .find(|thread| thread.id == "thread-old")
                .unwrap()
                .goal_id
                .as_deref(),
            Some("goal-123")
        );
        let saved = manager
            .set_default_llm_config(LLMConfig {
                provider: LLMProvider::Ollama,
                model: "local-model".into(),
                reasoning: ReasoningLevel::Low,
                speed: SpeedMode::Standard,
            })
            .await
            .unwrap();
        assert_eq!(saved.provider, LLMProvider::Ollama);
        assert_eq!(manager.default_llm_config().await.model, "local-model");
        let recency = RunManager::goal_recency_for_history(
            &temp.path().join("runs.json"),
            &temp.path().join("thread-settings.json"),
        )
        .await;
        assert!(recency.contains_key("goal-123"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preserves_repeated_identical_app_server_deltas() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let executable = temp.path().join("fake-codex");
        fs::write(
            &executable,
            r#"#!/bin/sh
IFS= read -r line
printf '%s\n' '{"id":1,"result":{}}'
IFS= read -r line
IFS= read -r line
printf '%s\n' '{"id":2,"result":{"thread":{"id":"thread-123"}}}'
IFS= read -r line
printf '%s\n' '{"id":3,"result":{"turn":{"id":"turn-123"}}}'
printf '%s\n' '{"method":"item/started","params":{"threadId":"thread-123","turnId":"turn-123","item":{"id":"message-1","type":"agentMessage","phase":"final_answer"}}}'
printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-123","turnId":"turn-123","itemId":"message-1","delta":"\""}}'
printf '%s\n' '{"method":"item/agentMessage/delta","params":{"threadId":"thread-123","turnId":"turn-123","itemId":"message-1","delta":"\""}}'
printf '%s\n' '{"method":"turn/completed","params":{"threadId":"thread-123","turnId":"turn-123","turn":{"status":"completed"}}}'
"#,
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let manager = RunManager::new(
            workspace,
            executable.to_string_lossy().into(),
            temp.path().join("runs.json"),
        )
        .await;

        let created = manager
            .create(
                "Return JSON".into(),
                vec![],
                vec![],
                ".".into(),
                "read-only".into(),
                "on-request".into(),
                "user".into(),
                true,
                None,
                None,
                None,
                None,
                WorkMode::Spec,
                None,
            )
            .await
            .unwrap();
        let completed = loop {
            let run = manager.get(&created.id).await.unwrap();
            if run.status == "completed" || run.status == "failed" {
                break run;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };

        assert_eq!(completed.status, "completed");
        assert_eq!(completed.final_message.as_deref(), Some("\"\""));
        assert_eq!(completed.events.len(), 4);
    }

    #[tokio::test]
    async fn redacts_run_history_and_display_projection_without_mutating_live_execution() {
        let temp = tempfile::tempdir().unwrap();
        let history = temp.path().join("runs.json");
        let manager = RunManager::new(temp.path().into(), "codex".into(), history.clone()).await;
        let run = Run {
            id: "run-redaction".into(),
            prompt: "Use OPENAI_API_KEY=sk-prompt-history-secret".into(),
            images: Vec::new(),
            files: Vec::new(),
            cwd: temp.path().to_string_lossy().into_owned(),
            sandbox: "read-only".into(),
            approval_policy: "on-request".into(),
            approvals_reviewer: "user".into(),
            ephemeral_thread: false,
            credential_isolation: None,
            execution_prompt: Some("Authorization: Bearer execution-history-secret".into()),
            output_schema: None,
            goal_id: Some("goal-a".into()),
            resumed_from: None,
            status: "completed".into(),
            created_at: now(),
            started_at: Some(now()),
            finished_at: Some(now()),
            return_code: Some(0),
            thread_id: Some("thread-a".into()),
            work_mode: WorkMode::Spec,
            llm_config: LLMConfig::default(),
            final_message: Some("password=final-message-secret".into()),
            error: None,
            usage: Usage::default(),
            events: vec![serde_json::json!({
                "diff": "+ api_key: event-diff-secret",
                "safe": "visible"
            })],
        };
        manager.state.write().await.runs.push(run.clone());
        manager.save().await;

        assert!(
            manager
                .get("run-redaction")
                .await
                .unwrap()
                .prompt
                .contains("prompt-history-secret"),
            "live execution state must keep the original prompt"
        );
        let display = RunManager::redacted_for_display(&run);
        let display = serde_json::to_string(&display).unwrap();
        let durable = std::fs::read_to_string(&history).unwrap();
        for secret in [
            "prompt-history-secret",
            "execution-history-secret",
            "final-message-secret",
            "event-diff-secret",
        ] {
            assert!(!display.contains(secret));
            assert!(!durable.contains(secret));
        }
        assert!(display.contains("visible"));
        assert!(durable.contains("visible"));

        manager
            .ensure_thread(
                "thread-redaction",
                "OPENAI_API_KEY=thread-title-secret",
                &LLMConfig::default(),
                Some("goal-a"),
                &WorkMode::Spec,
            )
            .await;
        let settings = std::fs::read_to_string(temp.path().join("thread-settings.json")).unwrap();
        assert!(!settings.contains("thread-title-secret"));
        assert!(
            manager
                .list_threads()
                .await
                .iter()
                .all(|thread| !thread.title.contains("thread-title-secret"))
        );

        let mut native_thread = AppServerThread {
            id: "native-redaction".into(),
            cwd: temp.path().to_string_lossy().into_owned(),
            preview: "password=native-preview-secret".into(),
            name: Some("Authorization: Bearer native-name-secret".into()),
            created_at: 0,
            updated_at: 0,
            model_provider: "openai".into(),
            source: serde_json::json!({"apiKey": "native-source-secret"}),
            status: serde_json::json!({"message": "safe"}),
            turns: vec![serde_json::json!({"prompt": "token=turn-secret"})],
            goal_id: Some("goal-a".into()),
        };
        redact_app_server_thread(&mut native_thread);
        let native_thread = serde_json::to_string(&native_thread).unwrap();
        for secret in [
            "native-preview-secret",
            "native-name-secret",
            "native-source-secret",
            "turn-secret",
        ] {
            assert!(!native_thread.contains(secret));
        }

        let reloaded = RunManager::new(temp.path().into(), "codex".into(), history).await;
        let reloaded = serde_json::to_string(&reloaded.list().await).unwrap();
        assert!(!reloaded.contains("prompt-history-secret"));
    }
}
