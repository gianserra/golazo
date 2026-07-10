use crate::models::{LLMConfig, LLMProvider, ReasoningLevel, Run, Usage};
use chrono::{SecondsFormat, Utc};
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
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::AutoSi, true)
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
        ReasoningLevel::None => "none",
        ReasoningLevel::Minimal => "minimal",
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
    pub run_count: usize,
    pub created_at: String,
    pub updated_at: String,
}

impl RunManager {
    pub async fn new(
        workspace_root: PathBuf,
        executable: String,
        history_path: PathBuf,
    ) -> Arc<Self> {
        let runs = Self::load_history(&history_path).await;
        let workspace_root = workspace_root.canonicalize().unwrap_or(workspace_root);
        let settings_path = history_path.with_file_name("thread-settings.json");
        Arc::new(Self {
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
    async fn ensure_thread(&self, id: &str, prompt: &str, config: &LLMConfig) {
        let mut settings = self.load_settings().await;
        if !settings.threads.contains_key(id) {
            let title = prompt.split_whitespace().collect::<Vec<_>>().join(" ");
            let timestamp = now();
            settings.threads.insert(
                id.into(),
                StoredThread {
                    title: if title.is_empty() {
                        "Untitled thread".into()
                    } else {
                        title.chars().take(80).collect()
                    },
                    llm_config: config.clone(),
                    created_at: timestamp.clone(),
                    updated_at: timestamp,
                },
            );
            let _ = self.save_settings(&settings).await;
        }
    }
    pub async fn update_thread(
        &self,
        id: &str,
        title: Option<String>,
        llm_config: Option<LLMConfig>,
    ) -> Result<Option<ThreadSummary>, String> {
        if !self.thread_exists(id).await {
            return Ok(None);
        }
        let mut settings = self.load_settings().await;
        let default = settings.default_llm_config.clone();
        let record = settings
            .threads
            .entry(id.into())
            .or_insert_with(|| StoredThread {
                title: "Untitled thread".into(),
                llm_config: default,
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
                goal_id: last.and_then(|run| run.goal_id.clone()),
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
    pub async fn workspace_root(&self) -> PathBuf {
        self.state.read().await.workspace_root.clone()
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
        working_directory: String,
        sandbox: String,
        goal_id: Option<String>,
        thread_id: Option<String>,
        execution_prompt: Option<String>,
        llm_config: Option<LLMConfig>,
    ) -> Result<Run, String> {
        if sandbox != "read-only" && sandbox != "workspace-write" {
            return Err("sandbox must be read-only or workspace-write".into());
        }
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
                self.ensure_thread(thread_id, &prompt, &config).await;
            }
            if llm_config.is_some() && thread.is_some() {
                let _ = self
                    .update_thread(thread_id, None, Some(config.clone()))
                    .await;
            }
            config
        } else {
            llm_config.unwrap_or(self.default_llm_config().await)
        };
        let run = Run {
            id: Uuid::new_v4().to_string(),
            prompt,
            cwd: cwd.to_string_lossy().into(),
            sandbox,
            execution_prompt,
            goal_id,
            resumed_from: thread_id.clone(),
            status: "queued".into(),
            created_at: now(),
            started_at: None,
            finished_at: None,
            return_code: None,
            thread_id,
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
            manager.execute(id, executable).await;
        });
        Ok(run)
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
        let prompt = run
            .execution_prompt
            .clone()
            .unwrap_or_else(|| run.prompt.clone());
        let mut args = vec!["exec".to_string()];
        if let Some(thread) = &run.resumed_from {
            args.extend(["resume".into(), "--json".into()]);
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
            if skip_git {
                args.push("--skip-git-repo-check".into());
            }
            args.extend([thread.clone(), prompt]);
        } else {
            args.extend(["--json".into(), "--sandbox".into(), run.sandbox.clone()]);
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
            if let Some(thread) = started_thread {
                self.ensure_thread(&thread, &run.prompt, &run.llm_config)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn captures_codex_jsonl_and_resumes_threads() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let executable = temp.path().join("fake-codex");
        fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"thread.started\",\"thread_id\":\"thread-123\"}'\nprintf '%s\\n' '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"completed\"}}'\nprintf '%s\\n' '{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":10,\"cached_input_tokens\":4,\"output_tokens\":3,\"reasoning_output_tokens\":1}}'\n",
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
                ".".into(),
                "workspace-write".into(),
                None,
                Some("thread-old".into()),
                None,
                Some(LLMConfig {
                    provider: LLMProvider::Openai,
                    model: "gpt-test".into(),
                    reasoning: ReasoningLevel::High,
                }),
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
        assert_eq!(completed.thread_id.as_deref(), Some("thread-123"));
        assert_eq!(completed.resumed_from.as_deref(), Some("thread-old"));
        assert_eq!(completed.final_message.as_deref(), Some("completed"));
        assert_eq!(completed.usage.total_tokens, 13);
        assert_eq!(completed.events.len(), 3);
        assert!(temp.path().join("runs.json").exists());
        let threads = manager.list_threads().await;
        assert!(threads.iter().any(|thread| thread.id == "thread-123"));
        assert_eq!(
            threads
                .iter()
                .find(|thread| thread.id == "thread-old")
                .unwrap()
                .llm_config
                .model,
            "gpt-test"
        );
        let saved = manager
            .set_default_llm_config(LLMConfig {
                provider: LLMProvider::Ollama,
                model: "local-model".into(),
                reasoning: ReasoningLevel::Low,
            })
            .await
            .unwrap();
        assert_eq!(saved.provider, LLMProvider::Ollama);
        assert_eq!(manager.default_llm_config().await.model, "local-model");
    }
}
