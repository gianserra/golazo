use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub goal_id: Option<String>,
    pub thread_id: Option<String>,
    #[serde(default = "default_working_directory")]
    pub working_directory: String,
    #[serde(default = "default_sandbox")]
    pub sandbox: String,
    pub llm_config: Option<LLMConfig>,
}

fn default_working_directory() -> String {
    ".".into()
}
fn default_sandbox() -> String {
    "workspace-write".into()
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
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
}

impl Default for ReasoningLevel {
    fn default() -> Self {
        Self::Medium
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
}

#[derive(Debug, Deserialize)]
pub struct ThreadUpdate {
    pub title: Option<String>,
    pub llm_config: Option<LLMConfig>,
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
    pub cwd: String,
    pub sandbox: String,
    pub execution_prompt: Option<String>,
    pub goal_id: Option<String>,
    pub resumed_from: Option<String>,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub return_code: Option<i32>,
    pub thread_id: Option<String>,
    #[serde(default)]
    pub llm_config: LLMConfig,
    pub final_message: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub usage: Usage,
    #[serde(default)]
    pub events: Vec<Value>,
}
