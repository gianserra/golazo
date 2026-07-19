use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppServerEvent {
    pub sequence: u64,
    pub method: String,
    pub params: Value,
}

impl AppServerEvent {
    pub fn connection(status: &str, error: Option<String>) -> Self {
        Self {
            sequence: 0,
            method: "connection/status".into(),
            params: serde_json::json!({"status": status, "error": error}),
        }
    }

    pub fn approval_resolved(id: &str, method: &str, decision: &str) -> Self {
        Self {
            sequence: 0,
            method: "approval/resolved".into(),
            params: serde_json::json!({"id": id, "method": method, "decision": decision}),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerApproval {
    pub id: String,
    pub method: String,
    pub params: Value,
    pub created_at: i64,
}

impl AppServerApproval {
    pub fn new(id: &str, method: &str, params: Value) -> Self {
        Self {
            id: id.into(),
            method: method.into(),
            params,
            created_at: Utc::now().timestamp_millis(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ApprovalDecision {
    pub decision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerThread {
    pub id: String,
    pub cwd: String,
    pub preview: String,
    pub name: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub model_provider: String,
    pub source: Value,
    pub status: Value,
    #[serde(default)]
    pub turns: Vec<Value>,
    #[serde(default, skip_deserializing)]
    pub goal_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppServerThreadPage {
    pub data: Vec<AppServerThread>,
    pub next_cursor: Option<String>,
    pub backwards_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Status {
    Planned,
    Blocked,
    Partial,
    Done,
}

impl Default for Status {
    fn default() -> Self {
        Self::Planned
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    pub title: String,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Feature {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub status: Status,
    #[serde(default)]
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalRecord {
    pub version: u8,
    pub kind: String,
    pub goal_id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub features: Vec<Feature>,
    #[serde(default)]
    pub slice_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceRecord {
    pub id: String,
    pub feature_id: String,
    pub at: String,
    pub summary: String,
    pub status: Status,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliceFile {
    pub version: u8,
    pub kind: String,
    pub goal_id: String,
    pub index: usize,
    #[serde(default)]
    pub slices: Vec<SliceRecord>,
}

#[derive(Debug, Deserialize)]
pub struct GoalCreate {
    pub goal_id: Option<String>,
    pub title: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GoalType {
    Greenfield,
    Feature,
    Bug,
    Refactor,
    Migration,
    Integration,
    Release,
    Research,
    Mixed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionScope {
    RequiredMvp,
    RequiredReleaseSafety,
    OptionalHardening,
    Future,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalScaffoldStep {
    pub step_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalScaffoldFeature {
    pub feature_id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub scope: SuggestionScope,
    pub rationale: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub steps: Vec<GoalScaffoldStep>,
    #[serde(default)]
    pub acceptance: Vec<String>,
    #[serde(default)]
    pub verification: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub risks: Vec<String>,
    pub confidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoalScaffoldProposal {
    pub goal_interpretation: String,
    #[serde(default)]
    pub success_criteria: Vec<String>,
    pub primary_type: GoalType,
    #[serde(default)]
    pub secondary_types: Vec<GoalType>,
    #[serde(default)]
    pub classification_evidence: Vec<String>,
    pub confidence: String,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub decisions_required: Vec<String>,
    #[serde(default)]
    pub required_features: Vec<GoalScaffoldFeature>,
    #[serde(default)]
    pub optional_features: Vec<GoalScaffoldFeature>,
    #[serde(default)]
    pub risks: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub non_goals: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct GoalScaffoldCreate {
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub goal_type_hint: Option<GoalType>,
    pub llm_config: Option<LLMConfig>,
}

#[derive(Debug, Deserialize)]
pub struct GoalScaffoldAccept {
    pub goal_id: Option<String>,
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub primary_type: GoalType,
    #[serde(default)]
    pub success_criteria: Vec<String>,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub decisions_required: Vec<String>,
    #[serde(default)]
    pub non_goals: Vec<String>,
    pub project_path: Option<String>,
    #[serde(default)]
    pub project_location_confirmed: bool,
    pub features: Vec<GoalScaffoldFeature>,
}

#[derive(Debug, Deserialize)]
pub struct FeatureCreate {
    pub feature_id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub status: Status,
}

#[derive(Debug, Deserialize)]
pub struct StatusUpdate {
    pub status: Status,
}

#[derive(Debug, Deserialize)]
pub struct StepCreate {
    pub step_id: String,
    pub title: String,
    #[serde(default)]
    pub done: bool,
}

#[derive(Debug, Deserialize)]
pub struct StepUpdate {
    pub done: bool,
}

#[derive(Debug, Deserialize)]
pub struct SliceCreate {
    pub feature_id: String,
    pub summary: String,
    pub status: Option<Status>,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CodexRunCreate {
    pub prompt: String,
    #[serde(default)]
    pub images: Vec<ImageAttachmentCreate>,
    #[serde(default)]
    pub files: Vec<FileAttachmentCreate>,
    pub goal_id: Option<String>,
    pub thread_id: Option<String>,
    #[serde(default = "default_working_directory")]
    pub working_directory: String,
    #[serde(default = "default_sandbox")]
    pub sandbox: String,
    #[serde(default = "default_approval_policy")]
    pub approval_policy: String,
    #[serde(default = "default_approvals_reviewer")]
    pub approvals_reviewer: String,
    pub work_mode: Option<WorkMode>,
    pub llm_config: Option<LLMConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ImageAttachmentCreate {
    pub name: String,
    pub mime_type: String,
    pub data: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileAttachmentCreate {
    pub name: String,
    pub mime_type: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunImage {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunFile {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    pub path: String,
}

fn default_working_directory() -> String {
    ".".into()
}
fn default_sandbox() -> String {
    "workspace-write".into()
}
fn default_approval_policy() -> String {
    "on-request".into()
}
fn default_approvals_reviewer() -> String {
    "user".into()
}

#[derive(Debug, Deserialize)]
pub struct WorkspaceSelect {
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct ProjectCreate {
    pub path: String,
    #[serde(default = "default_true")]
    pub activate: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct NameUpdate {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LLMProvider {
    Openai,
    Ollama,
    Lmstudio,
}

impl Default for LLMProvider {
    fn default() -> Self {
        Self::Openai
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ReasoningLevel {
    #[serde(alias = "minimal", alias = "light")]
    Low,
    #[serde(alias = "none")]
    Medium,
    High,
    #[serde(alias = "extra_high")]
    Xhigh,
}

impl Default for ReasoningLevel {
    fn default() -> Self {
        Self::Medium
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SpeedMode {
    Standard,
    Fast,
}

impl Default for SpeedMode {
    fn default() -> Self {
        Self::Standard
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct LLMConfig {
    #[serde(default)]
    pub provider: LLMProvider,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub reasoning: ReasoningLevel,
    #[serde(default)]
    pub speed: SpeedMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WorkMode {
    #[default]
    Spec,
    Build,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexAuthStatus {
    pub executable: String,
    pub available: bool,
    pub authenticated: bool,
    pub method: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CodexAuthAction {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UserProfile {
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub role: String,
}

#[derive(Debug, Deserialize)]
pub struct ThreadUpdate {
    pub title: Option<String>,
    pub llm_config: Option<LLMConfig>,
    pub work_mode: Option<WorkMode>,
}

#[derive(Debug, Deserialize)]
pub struct ThreadGoalUpdate {
    pub goal_id: String,
}

#[derive(Debug, Serialize)]
pub struct Progress {
    pub completed_steps: usize,
    pub total_steps: usize,
    pub completion_rate: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Usage {
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_output_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub prompt: String,
    #[serde(default)]
    pub images: Vec<RunImage>,
    #[serde(default)]
    pub files: Vec<RunFile>,
    pub cwd: String,
    pub sandbox: String,
    #[serde(default = "default_approval_policy")]
    pub approval_policy: String,
    #[serde(default = "default_approvals_reviewer")]
    pub approvals_reviewer: String,
    #[serde(default)]
    pub ephemeral_thread: bool,
    pub execution_prompt: Option<String>,
    #[serde(default, skip_serializing)]
    pub output_schema: Option<Value>,
    pub goal_id: Option<String>,
    pub resumed_from: Option<String>,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub return_code: Option<i32>,
    pub thread_id: Option<String>,
    #[serde(default)]
    pub work_mode: WorkMode,
    #[serde(default)]
    pub llm_config: LLMConfig,
    pub final_message: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub usage: Usage,
    #[serde(default)]
    pub events: Vec<Value>,
}
