use crate::app_server::AppServerClient;
use crate::models::{
    AppServerThread, AppServerThreadPage, CodexAuthAction, CodexAuthStatus, FileAttachmentCreate,
    ImageAttachmentCreate, LLMConfig, LLMProvider, ReasoningLevel, Run, RunFile, RunImage,
    SpeedMode, Usage, WorkMode,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
        title.chars().take(80).collect()
    }
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
    async fn load_history(path: &Path) -> Vec<Run> {
        let Ok(text) = tokio::fs::read_to_string(path).await else {
            return vec![];
        };
        serde_json::from_str(&text).unwrap_or_default()
    }
    async fn save(&self) {
        let (path, runs) = {
            let state = self.state.read().await;
            (state.history_path.clone(), state.runs.clone())
        };
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        if let Ok(bytes) = serde_json::to_vec_pretty(&runs) {
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
        serde_json::from_str(&text).unwrap_or_default()
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
        serde_json::to_writer_pretty(&mut temporary, settings)
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
            self.app_server.set_thread_name(id, value.trim()).await?;
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
            record.title = value.trim().into();
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
                .or_else(|| {
                    first.map(|run| {
                        run.prompt
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join(" ")
                            .chars()
                            .take(80)
                            .collect()
                    })
                })
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
        validate_permissions(&sandbox, &approval_policy, &approvals_reviewer)?;
        let (root, executable) = {
            let state = self.state.read().await;
            (state.workspace_root.clone(), state.executable.clone())
        };
        let cwd = Self::resolve_cwd(&root, &working_directory)?;
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
            let file_context =
                format!("The user uploaded these files. Inspect them as needed:\n{file_list}");
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
            self.mutate(&id, |run| {
                run.events
                    .push(serde_json::json!({"method": event.method, "params": event.params}));
                if run.events.len() > 1000 {
                    run.events.remove(0);
                }
                if is_final_answer_delta
                    && let Some(delta) = event.params.get("delta").and_then(Value::as_str)
                {
                    run.final_message
                        .get_or_insert_with(String::new)
                        .push_str(delta);
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
        let cwd = PathBuf::from(&run.cwd);
        let skip_git = !cwd.ancestors().any(|parent| parent.join(".git").exists());
        let prompt = run.prompt.clone();
        let mut args = codex_exec_args(&run.sandbox, &run.approval_policy, &run.approvals_reviewer);
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
            let event: Value = serde_json::from_str(&line)
                .unwrap_or_else(|_| serde_json::json!({"type":"unparsed","text":line}));
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
                    let message = String::from_utf8_lossy(&stderr);
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
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s\\n' '{{\"type\":\"thread.started\",\"thread_id\":\"thread-123\"}}'\nprintf '%s\\n' '{{\"type\":\"item.completed\",\"item\":{{\"type\":\"agent_message\",\"text\":\"completed\"}}}}'\nprintf '%s\\n' '{{\"type\":\"turn.completed\",\"usage\":{{\"input_tokens\":10,\"cached_input_tokens\":4,\"output_tokens\":3,\"reasoning_output_tokens\":1}}}}'\n",
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
}
