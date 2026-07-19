use crate::models::{
    AppServerApproval, AppServerEvent, AppServerThread, AppServerThreadPage, LLMConfig, RunImage,
    SpeedMode,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use tokio::time::{Duration, sleep, timeout};

#[derive(Clone)]
pub struct AppServerClient {
    commands: mpsc::Sender<RpcCommand>,
    events: broadcast::Sender<AppServerEvent>,
    approvals: Arc<RwLock<HashMap<String, PendingApproval>>>,
}

pub struct StartedTurn {
    pub thread_id: String,
    pub turn_id: String,
}

struct PendingApproval {
    public: AppServerApproval,
    rpc_id: Value,
}

enum RpcCommand {
    Request {
        method: String,
        params: Value,
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Respond {
        id: Value,
        result: Value,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

struct Session {
    child: Child,
    stdin: tokio::process::ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    next_id: u64,
}

impl AppServerClient {
    pub fn new(executable: impl Into<String>) -> Self {
        let (commands, receiver) = mpsc::channel(128);
        let (events, _) = broadcast::channel(1024);
        let approvals = Arc::new(RwLock::new(HashMap::new()));
        tokio::spawn(run_actor(
            executable.into(),
            receiver,
            events.clone(),
            Arc::clone(&approvals),
        ));
        Self {
            commands,
            events,
            approvals,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<AppServerEvent> {
        self.events.subscribe()
    }

    async fn request<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T, String> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(RpcCommand::Request {
                method: method.into(),
                params,
                reply,
            })
            .await
            .map_err(|_| "app-server connection manager stopped".to_string())?;
        let value = timeout(Duration::from_secs(30), response)
            .await
            .map_err(|_| format!("app-server request {method} timed out"))?
            .map_err(|_| format!("app-server request {method} was cancelled"))??;
        serde_json::from_value(value)
            .map_err(|error| format!("invalid app-server {method} result: {error}"))
    }

    pub async fn list_threads(
        &self,
        workspace: &Path,
        cursor: Option<String>,
        limit: Option<u32>,
    ) -> Result<AppServerThreadPage, String> {
        self.request(
            "thread/list",
            json!({
                "cwd": workspace,
                "cursor": cursor,
                "limit": limit.unwrap_or(50).min(100),
                "sortKey": "updated_at",
                "sortDirection": "desc"
            }),
        )
        .await
    }

    pub async fn read_thread(
        &self,
        workspace: &Path,
        thread_id: &str,
    ) -> Result<AppServerThread, String> {
        let response: ThreadReadResponse = self
            .request(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": true}),
            )
            .await?;
        if !same_workspace(workspace, Path::new(&response.thread.cwd)) {
            return Err("thread does not belong to the active workspace".into());
        }
        Ok(response.thread)
    }

    pub async fn set_thread_name(&self, thread_id: &str, name: &str) -> Result<(), String> {
        let _: Value = self
            .request(
                "thread/name/set",
                json!({"threadId": thread_id, "name": name}),
            )
            .await?;
        Ok(())
    }

    pub async fn start_turn(
        &self,
        workspace: &Path,
        thread_id: Option<&str>,
        prompt: &str,
        application_context: Option<&str>,
        output_schema: Option<&Value>,
        images: &[RunImage],
        sandbox: &str,
        approval_policy: &str,
        approvals_reviewer: &str,
        config: &LLMConfig,
        ephemeral: bool,
    ) -> Result<StartedTurn, String> {
        let thread_id = if let Some(thread_id) = thread_id {
            let response: Value = self.request("thread/resume", json!({
                "threadId": thread_id,
                "cwd": workspace,
                "developerInstructions": application_context,
                "sandbox": sandbox,
                "approvalPolicy": approval_policy,
                "approvalsReviewer": approvals_reviewer,
                "model": optional_string(&config.model),
                "serviceTier": if matches!(config.speed, SpeedMode::Fast) { Some("fast") } else { None }
            })).await?;
            response
                .pointer("/thread/id")
                .and_then(Value::as_str)
                .unwrap_or(thread_id)
                .to_string()
        } else {
            let response: Value = self.request("thread/start", json!({
                "cwd": workspace,
                "developerInstructions": application_context,
                "sandbox": sandbox,
                "approvalPolicy": approval_policy,
                "approvalsReviewer": approvals_reviewer,
                "model": optional_string(&config.model),
                "serviceTier": if matches!(config.speed, SpeedMode::Fast) { Some("fast") } else { None },
                "ephemeral": ephemeral
            })).await?;
            response
                .pointer("/thread/id")
                .and_then(Value::as_str)
                .ok_or("thread/start returned no thread id")?
                .to_string()
        };
        let input = turn_input(prompt, images);
        let response: Value = self.request("turn/start", json!({
            "threadId": thread_id,
            "input": input,
            "outputSchema": output_schema,
            "cwd": workspace,
            "model": optional_string(&config.model),
            "effort": reasoning_name(&config.reasoning),
            "serviceTier": if matches!(config.speed, SpeedMode::Fast) { Some("fast") } else { None },
            "approvalPolicy": approval_policy,
            "approvalsReviewer": approvals_reviewer
        })).await?;
        let turn_id = response
            .pointer("/turn/id")
            .and_then(Value::as_str)
            .ok_or("turn/start returned no turn id")?
            .to_string();
        Ok(StartedTurn { thread_id, turn_id })
    }

    pub async fn pending_approvals(&self) -> Vec<AppServerApproval> {
        let cutoff = chrono::Utc::now().timestamp_millis() - 60 * 60 * 1000;
        let mut approvals = self.approvals.write().await;
        approvals.retain(|_, approval| approval.public.created_at >= cutoff);
        let mut values = approvals
            .values()
            .map(|approval| approval.public.clone())
            .collect::<Vec<_>>();
        values.sort_by_key(|approval| approval.created_at);
        values
    }

    pub async fn rate_limits(&self) -> Result<Value, String> {
        self.request("account/rateLimits/read", json!({})).await
    }

    pub async fn account(&self) -> Result<Value, String> {
        self.request("account/read", json!({"refreshToken": false}))
            .await
    }

    pub async fn models(&self) -> Result<Value, String> {
        self.request("model/list", json!({"limit": 100, "includeHidden": false}))
            .await
    }

    pub async fn decide_approval(&self, id: &str, decision: &str) -> Result<(), String> {
        if !matches!(
            decision,
            "accept" | "acceptForSession" | "decline" | "cancel"
        ) {
            return Err("invalid approval decision".into());
        }
        let approval = self
            .approvals
            .write()
            .await
            .remove(id)
            .ok_or("approval request is no longer pending")?;
        let result = if approval.public.method == "item/permissions/requestApproval" {
            let permissions = if matches!(decision, "accept" | "acceptForSession") {
                approval
                    .public
                    .params
                    .get("permissions")
                    .cloned()
                    .unwrap_or_else(|| json!({}))
            } else {
                json!({})
            };
            json!({
                "permissions": permissions,
                "scope": if decision == "acceptForSession" { "session" } else { "turn" }
            })
        } else {
            json!({"decision": decision})
        };
        let (reply, response) = oneshot::channel();
        self.commands
            .send(RpcCommand::Respond {
                id: approval.rpc_id,
                result,
                reply,
            })
            .await
            .map_err(|_| "app-server connection manager stopped".to_string())?;
        response
            .await
            .map_err(|_| "approval response was cancelled".to_string())??;
        let _ = self.events.send(AppServerEvent::approval_resolved(
            id,
            &approval.public.method,
            decision,
        ));
        Ok(())
    }
}

fn optional_string(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn turn_input(prompt: &str, images: &[RunImage]) -> Vec<Value> {
    let mut input = vec![json!({"type": "text", "text": prompt, "text_elements": []})];
    input.extend(
        images
            .iter()
            .map(|image| json!({"type": "localImage", "path": image.path})),
    );
    input
}

fn reasoning_name(value: &crate::models::ReasoningLevel) -> &'static str {
    match value {
        crate::models::ReasoningLevel::Low => "low",
        crate::models::ReasoningLevel::Medium => "medium",
        crate::models::ReasoningLevel::High => "high",
        crate::models::ReasoningLevel::Xhigh => "xhigh",
    }
}

#[derive(serde::Deserialize)]
struct ThreadReadResponse {
    thread: AppServerThread,
}

fn same_workspace(expected: &Path, actual: &Path) -> bool {
    let expected = expected
        .canonicalize()
        .unwrap_or_else(|_| expected.to_path_buf());
    let actual = actual
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(actual));
    expected == actual
}

async fn run_actor(
    executable: String,
    mut commands: mpsc::Receiver<RpcCommand>,
    events: broadcast::Sender<AppServerEvent>,
    approvals: Arc<RwLock<HashMap<String, PendingApproval>>>,
) {
    let sequence = AtomicU64::new(1);
    loop {
        let mut session = match Session::start(&executable).await {
            Ok(session) => session,
            Err(error) => {
                let _ = events.send(AppServerEvent::connection("disconnected", Some(error)));
                tokio::select! {
                    command = commands.recv() => {
                        let Some(command) = command else { return; };
                        match command {
                            RpcCommand::Request { reply, .. } => { let _ = reply.send(Err("Codex app server is unavailable".into())); }
                            RpcCommand::Respond { reply, .. } => { let _ = reply.send(Err("Codex app server is unavailable".into())); }
                        }
                    }
                    _ = sleep(Duration::from_secs(2)) => {}
                }
                continue;
            }
        };
        let _ = events.send(AppServerEvent::connection("connected", None));
        let mut pending: HashMap<u64, oneshot::Sender<Result<Value, String>>> = HashMap::new();
        let disconnected = loop {
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else { return; };
                    match command {
                        RpcCommand::Request { method, params, reply } => {
                            let id = session.next_id;
                            session.next_id += 1;
                            if let Err(error) = session.write(&json!({"id": id, "method": method, "params": params})).await {
                                let _ = reply.send(Err(error.clone()));
                                break error;
                            }
                            pending.insert(id, reply);
                        }
                        RpcCommand::Respond { id, result, reply } => {
                            let result = session.write(&json!({"id": id, "result": result})).await;
                            let failed = result.as_ref().err().cloned();
                            let _ = reply.send(result);
                            if let Some(error) = failed { break error; }
                        }
                    }
                }
                line = session.lines.next_line() => {
                    let line = match line {
                        Ok(Some(line)) => line,
                        Ok(None) => break "app server closed the connection".into(),
                        Err(error) => break format!("failed to read app-server message: {error}"),
                    };
                    let message: Value = match serde_json::from_str(&line) {
                        Ok(message) => message,
                        Err(_) => continue,
                    };
                    if let Some(id) = message.get("id").and_then(Value::as_u64)
                        && message.get("method").is_none()
                    {
                        if let Some(reply) = pending.remove(&id) {
                            let result = if let Some(error) = message.get("error") {
                                Err(format!("app-server request failed: {error}"))
                            } else {
                                message.get("result").cloned().ok_or("app-server response had no result".into())
                            };
                            let _ = reply.send(result);
                        }
                        continue;
                    }
                    let method = message.get("method").and_then(Value::as_str).unwrap_or("unknown");
                    let params = message.get("params").cloned().unwrap_or(Value::Null);
                    let event = AppServerEvent {
                        sequence: sequence.fetch_add(1, Ordering::Relaxed),
                        method: method.into(),
                        params: params.clone(),
                    };
                    let _ = events.send(event);
                    if method == "serverRequest/resolved"
                        && let Some(request_id) = params.get("requestId")
                    {
                        approvals.write().await.retain(|_, approval| approval.rpc_id != *request_id);
                    }
                    if let Some(rpc_id) = message.get("id").cloned()
                        && method.ends_with("/requestApproval")
                    {
                        let approval_id = format!("approval-{}", sequence.fetch_add(1, Ordering::Relaxed));
                        let approval = AppServerApproval::new(&approval_id, method, params);
                        approvals.write().await.insert(
                            approval_id,
                            PendingApproval { public: approval, rpc_id },
                        );
                    }
                }
            }
        };
        for (_, reply) in pending.drain() {
            let _ = reply.send(Err(format!("app-server disconnected: {disconnected}")));
        }
        approvals.write().await.clear();
        let _ = events.send(AppServerEvent::connection(
            "disconnected",
            Some(disconnected),
        ));
        let _ = session.child.kill().await;
        sleep(Duration::from_millis(500)).await;
    }
}

impl Session {
    async fn start(executable: &str) -> Result<Self, String> {
        let mut child = Command::new(executable)
            .args(["app-server", "--stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to start Codex app server: {error}"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or("app server stdin is unavailable")?;
        let stdout = child
            .stdout
            .take()
            .ok_or("app server stdout is unavailable")?;
        let mut session = Self {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
            next_id: 2,
        };
        session.write(&json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {"name": "golazo", "title": "Golazo", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {}
            }
        })).await?;
        timeout(Duration::from_secs(15), async {
            loop {
                let line = session
                    .lines
                    .next_line()
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or("app server closed during initialization")?;
                let message: Value =
                    serde_json::from_str(&line).map_err(|error| error.to_string())?;
                if message.get("id").and_then(Value::as_u64) == Some(1) {
                    if let Some(error) = message.get("error") {
                        return Err(format!("app-server initialize failed: {error}"));
                    }
                    break Ok(());
                }
            }
        })
        .await
        .map_err(|_| "app-server initialize timed out".to_string())??;
        session
            .write(&json!({"method": "initialized", "params": {}}))
            .await?;
        Ok(session)
    }

    async fn write(&mut self, message: &Value) -> Result<(), String> {
        self.stdin
            .write_all(format!("{message}\n").as_bytes())
            .await
            .map_err(|error| error.to_string())?;
        self.stdin.flush().await.map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn workspace_comparison_normalizes_existing_paths() {
        let temp = tempfile::tempdir().unwrap();
        assert!(same_workspace(temp.path(), &temp.path().join(".")));
        assert!(!same_workspace(temp.path(), &temp.path().join("other")));
    }

    #[test]
    fn turn_input_includes_local_images_after_text() {
        let input = turn_input(
            "Describe this screenshot",
            &[RunImage {
                id: "image-1".into(),
                name: "screenshot.png".into(),
                mime_type: "image/png".into(),
                path: "/tmp/screenshot.png".into(),
            }],
        );
        assert_eq!(input[0]["type"], "text");
        assert_eq!(input[1]["type"], "localImage");
        assert_eq!(input[1]["path"], "/tmp/screenshot.png");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sends_native_thread_name_updates() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("fake-codex");
        let capture = temp.path().join("request.json");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":1,\"result\":{{}}}}'\nIFS= read -r line\nIFS= read -r line\nprintf '%s\\n' \"$line\" > '{}'\nprintf '%s\\n' '{{\"id\":2,\"result\":{{}}}}'\n",
                capture.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let client = AppServerClient::new(executable.to_string_lossy());

        client
            .set_thread_name("thread-123", "Shared conversation")
            .await
            .unwrap();

        let request: Value = serde_json::from_str(&fs::read_to_string(capture).unwrap()).unwrap();
        assert_eq!(request["method"], "thread/name/set");
        assert_eq!(request["params"]["threadId"], "thread-123");
        assert_eq!(request["params"]["name"], "Shared conversation");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn keeps_application_context_out_of_the_visible_user_message() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("fake-codex");
        let thread_capture = temp.path().join("thread-request.json");
        let capture = temp.path().join("turn-request.json");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":1,\"result\":{{}}}}'\nIFS= read -r line\nIFS= read -r line\nprintf '%s\\n' \"$line\" > '{}'\nprintf '%s\\n' '{{\"id\":2,\"result\":{{\"thread\":{{\"id\":\"thread-123\"}}}}}}'\nIFS= read -r line\nprintf '%s\\n' \"$line\" > '{}'\nprintf '%s\\n' '{{\"id\":3,\"result\":{{\"turn\":{{\"id\":\"turn-123\"}}}}}}'\n",
                thread_capture.display(),
                capture.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let client = AppServerClient::new(executable.to_string_lossy());
        let output_schema = json!({"type": "object"});

        client
            .start_turn(
                temp.path(),
                None,
                "Visible user prompt",
                Some("Private Golazo execution context"),
                Some(&output_schema),
                &[],
                "workspace-write",
                "on-request",
                "auto_review",
                &LLMConfig::default(),
                false,
            )
            .await
            .unwrap();

        let thread_request: Value =
            serde_json::from_str(&fs::read_to_string(thread_capture).unwrap()).unwrap();
        assert_eq!(thread_request["method"], "thread/start");
        assert_eq!(
            thread_request["params"]["developerInstructions"],
            "Private Golazo execution context"
        );
        let request: Value = serde_json::from_str(&fs::read_to_string(capture).unwrap()).unwrap();
        assert_eq!(request["method"], "turn/start");
        assert_eq!(request["params"]["input"][0]["text"], "Visible user prompt");
        assert_eq!(request["params"]["approvalPolicy"], "on-request");
        assert_eq!(request["params"]["approvalsReviewer"], "auto_review");
        assert!(request["params"].get("additionalContext").is_none());
        assert_eq!(request["params"]["outputSchema"], output_schema);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refreshes_developer_instructions_when_resuming_a_thread() {
        let temp = tempfile::tempdir().unwrap();
        let executable = temp.path().join("fake-codex");
        let capture = temp.path().join("resume-request.json");
        fs::write(
            &executable,
            format!(
                "#!/bin/sh\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":1,\"result\":{{}}}}'\nIFS= read -r line\nIFS= read -r line\nprintf '%s\\n' \"$line\" > '{}'\nprintf '%s\\n' '{{\"id\":2,\"result\":{{\"thread\":{{\"id\":\"thread-123\"}}}}}}'\nIFS= read -r line\nprintf '%s\\n' '{{\"id\":3,\"result\":{{\"turn\":{{\"id\":\"turn-123\"}}}}}}'\n",
                capture.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let client = AppServerClient::new(executable.to_string_lossy());

        client
            .start_turn(
                temp.path(),
                Some("thread-123"),
                "Visible follow-up",
                Some("Updated Build mode guidance"),
                None,
                &[],
                "workspace-write",
                "on-request",
                "user",
                &LLMConfig::default(),
                false,
            )
            .await
            .unwrap();

        let request: Value = serde_json::from_str(&fs::read_to_string(capture).unwrap()).unwrap();
        assert_eq!(request["method"], "thread/resume");
        assert_eq!(request["params"]["threadId"], "thread-123");
        assert_eq!(
            request["params"]["developerInstructions"],
            "Updated Build mode guidance"
        );
    }
}
