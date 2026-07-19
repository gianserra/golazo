import { ClipboardEvent, FormEvent, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";

const brandIcon = new URL("../../electron/assets/icon.png", import.meta.url).href;

type Progress = { completed_steps: number; total_steps: number; completion_rate: number };
type ImplementationStep = { id: string; title: string; done: boolean };
type ImplementationSlice = {
  id: string;
  feature_id: string;
  at: string;
  summary: string;
  status: "Planned" | "Blocked" | "Partial" | "Done";
  evidence: string[];
};
type Feature = {
  id: string;
  title: string;
  description: string;
  status: "Planned" | "Blocked" | "Partial" | "Done";
  steps: ImplementationStep[];
  progress: Progress;
  slice_count: number;
};
type Goal = { goal_id: string; title: string; features: Feature[]; slices: ImplementationSlice[]; progress: Progress };
type GoalType = "Greenfield" | "Feature" | "Bug" | "Refactor" | "Migration" | "Integration" | "Release" | "Research" | "Mixed";
type SuggestionScope = "required_mvp" | "required_release_safety" | "optional_hardening" | "future";
type ScaffoldStep = { step_id: string; title: string };
type ScaffoldFeature = {
  feature_id: string;
  title: string;
  description: string;
  scope: SuggestionScope;
  rationale: string;
  evidence: string[];
  steps: ScaffoldStep[];
  acceptance: string[];
  verification: string[];
  dependencies: string[];
  risks: string[];
  confidence: "high" | "medium" | "low";
};
type ScaffoldProposal = {
  goal_interpretation: string;
  success_criteria: string[];
  primary_type: GoalType;
  secondary_types: GoalType[];
  classification_evidence: string[];
  confidence: "high" | "medium" | "low";
  assumptions: string[];
  decisions_required: string[];
  required_features: ScaffoldFeature[];
  optional_features: ScaffoldFeature[];
  risks: string[];
  dependencies: string[];
  non_goals: string[];
};
type ScaffoldStatus = { run: Run; proposal: ScaffoldProposal | null };
type GreenfieldLocationSuggestion = { path: string; exists: boolean; workspace: string };
type Usage = {
  input_tokens: number;
  cached_input_tokens: number;
  output_tokens: number;
  reasoning_output_tokens: number;
  total_tokens: number;
};
type Run = {
  id: string;
  prompt: string;
  images: RunImage[];
  files: RunFile[];
  goal_id: string | null;
  thread_id: string | null;
  work_mode: WorkMode;
  resumed_from: string | null;
  status: "queued" | "running" | "completed" | "failed";
  created_at: string;
  final_message: string | null;
  error: string | null;
  usage: Usage;
  events: unknown[];
};
type RunImage = { id: string; name: string; mime_type: string; path: string };
type PendingImage = RunImage & { data: string; preview: string };
type RunFile = { id: string; name: string; mime_type: string; path: string };
type PendingFile = RunFile & { data: string; size: number };
type Workspace = { path: string; name: string; browse_root: string; project_id: string };
type ProjectGoal = { goal_id: string; title: string; progress: Progress; features: number; last_thread_at: string | null };
type Project = {
  id: string;
  name: string;
  path: string;
  active: boolean;
  exists: boolean;
  goals: ProjectGoal[];
  usage: Usage;
};
type Directory = { name: string; path: string; symlink: boolean };
type DirectoryListing = { path: string; parent: string | null; directories: Directory[] };
type LLMProvider = "openai" | "ollama" | "lmstudio";
type ReasoningLevel = "low" | "medium" | "high" | "xhigh";
type SpeedMode = "standard" | "fast";
type WorkMode = "spec" | "build";
type PermissionMode = "ask" | "auto-review" | "full-access";
type LLMConfig = { provider: LLMProvider; model: string; reasoning: ReasoningLevel; speed: SpeedMode };
type CodexAuthStatus = {
  executable: string;
  available: boolean;
  authenticated: boolean;
  method: string | null;
  message: string;
};
type CodexAuthAction = { status: string; message: string };
type UserProfile = { display_name: string; role: string };
type CodexAccount = { type: string; email?: string | null; planType?: string | null };
type CodexAccountResponse = { account: CodexAccount | null; requiresOpenaiAuth: boolean };
type ThreadSummary = {
  id: string;
  title: string;
  llm_config: LLMConfig;
  goal_id: string | null;
  work_mode: WorkMode;
  run_count: number;
  created_at: string;
  updated_at: string;
};
type AppServerThread = {
  id: string; cwd: string; preview: string; name: string | null; createdAt: number; updatedAt: number;
  modelProvider: string; source: unknown; status: unknown;
  turns: Array<{ id: string; status: string; items: Array<Record<string, unknown>> }>;
  goalId: string | null;
};
type AppServerThreadPage = { data: AppServerThread[]; nextCursor: string | null; backwardsCursor: string | null };
type AppServerApproval = { id: string; method: string; params: Record<string, unknown>; createdAt: number };
type AppServerEvent = { sequence: number; method: string; params: Record<string, unknown> };
type RateLimitWindow = { usedPercent: number; resetsAt: number | null; windowDurationMins: number | null };
type RateLimitSnapshot = {
  limitId: string | null; limitName: string | null; planType: string | null;
  primary: RateLimitWindow | null; secondary: RateLimitWindow | null;
};
type RateLimitResponse = { rateLimits: RateLimitSnapshot; rateLimitsByLimitId: Record<string, RateLimitSnapshot> | null };
type AppServerModel = { id: string; model: string; displayName: string; isDefault: boolean; hidden: boolean };
type AppServerModelPage = { data: AppServerModel[]; nextCursor: string | null };
type HistoryMessage = { id: string; role: "user" | "assistant"; text: string; status: string; images?: string[] };
type HistoryTurn = { id: string; messages: HistoryMessage[]; working: HistoryMessage[] };
type AgentMessagePhase = "commentary" | "final_answer";

const emptyUsage: Usage = {
  input_tokens: 0,
  cached_input_tokens: 0,
  output_tokens: 0,
  reasoning_output_tokens: 0,
  total_tokens: 0,
};
const fallbackLLMConfig: LLMConfig = { provider: "openai", model: "", reasoning: "medium", speed: "standard" };
const modelOptions = [
  { label: "5.6 Sol", value: "gpt-5.6-sol" },
  { label: "5.6 Terra", value: "gpt-5.6-terra" },
  { label: "5.6 Luna", value: "gpt-5.6-luna" },
  { label: "5.5", value: "gpt-5.5" },
  { label: "5.4", value: "gpt-5.4" },
  { label: "5.4 Mini", value: "gpt-5.4-mini" },
];
const effortOptions: Array<{ label: string; value: ReasoningLevel }> = [
  { label: "Light", value: "low" },
  { label: "Medium", value: "medium" },
  { label: "High", value: "high" },
  { label: "Extra High", value: "xhigh" },
];
const permissionOptions: Array<{
  value: PermissionMode;
  label: string;
  description: string;
  sandbox: "workspace-write" | "danger-full-access";
  approvalPolicy: "on-request" | "never";
  approvalsReviewer: "user" | "auto_review";
}> = [
  {
    value: "ask",
    label: "Ask for approval",
    description: "Always ask to edit external files and use the internet",
    sandbox: "workspace-write",
    approvalPolicy: "on-request",
    approvalsReviewer: "user",
  },
  {
    value: "auto-review",
    label: "Approve for me",
    description: "Automatically review requests that cross the workspace boundary",
    sandbox: "workspace-write",
    approvalPolicy: "on-request",
    approvalsReviewer: "auto_review",
  },
  {
    value: "full-access",
    label: "Full access",
    description: "Unrestricted access to the internet and any file on your computer",
    sandbox: "danger-full-access",
    approvalPolicy: "never",
    approvalsReviewer: "user",
  },
];
const threadLabel = (thread: AppServerThread) => thread.name || thread.preview || "Untitled thread";
const appThreadTimestamp = (value: number) => value > 0 && value < 1_000_000_000_000 ? value * 1000 : value;

function orderedGoals(project: Project, appThreads: AppServerThread[]): ProjectGoal[] {
  const activeThreadRecency = new Map<string, number>();
  if (project.active) {
    for (const thread of appThreads) {
      if (!thread.goalId) continue;
      const timestamp = appThreadTimestamp(thread.updatedAt);
      activeThreadRecency.set(thread.goalId, Math.max(timestamp, activeThreadRecency.get(thread.goalId) || 0));
    }
  }
  return project.goals
    .map((goal, index) => ({
      goal,
      index,
      recency: Math.max(
        goal.last_thread_at ? Date.parse(goal.last_thread_at) || 0 : 0,
        activeThreadRecency.get(goal.goal_id) || 0,
      ),
    }))
    .sort((left, right) => right.recency - left.recency || left.index - right.index)
    .map(({ goal }) => goal);
}
const speedOptions: Array<{ label: string; value: SpeedMode; note: string }> = [
  { label: "Standard", value: "standard", note: "Default speed" },
  { label: "Fast", value: "fast", note: "1.5x speed, more usage" },
];
const fallbackCodexAuth: CodexAuthStatus = {
  executable: "codex",
  available: false,
  authenticated: false,
  method: null,
  message: "Checking Codex login…",
};
const emptyProfile: UserProfile = { display_name: "", role: "" };
const API_BASE = import.meta.env.DEV ? "/api" : "";
const COLLAPSED_PROJECTS_KEY = "golazo.collapsed-projects";

function loadCollapsedProjects(): string[] {
  try {
    const value = JSON.parse(window.localStorage.getItem(COLLAPSED_PROJECTS_KEY) || "[]");
    return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
  } catch {
    return [];
  }
}

async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "content-type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed (${response.status})`);
  }
  return response.json();
}

function normalizeLLMConfig(config: LLMConfig): LLMConfig {
  return {
    provider: config.provider || "openai",
    model: config.model || "",
    reasoning: config.reasoning || "medium",
    speed: config.speed || "standard",
  };
}

const number = (value = 0) => new Intl.NumberFormat().format(value);
const time = (value: string) => new Date(value).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

function historyTurns(thread: AppServerThread | null): HistoryTurn[] {
  if (!thread) return [];
  return thread.turns.map((turn) => {
    const messages: HistoryMessage[] = [];
    const working: HistoryMessage[] = [];
    for (const item of turn.items) {
      if (item.type === "agentMessage" && typeof item.text === "string" && item.text.trim()) {
        const message = { id: String(item.id), role: "assistant" as const, text: item.text, status: turn.status };
        if (item.phase === "final_answer") messages.push(message);
        else if (item.phase === "commentary") working.push(message);
      } else if (item.type === "userMessage" && Array.isArray(item.content)) {
        const text = item.content.map((part) => typeof part === "object" && part && "text" in part ? String(part.text) : "").join("").trim();
        const images = item.content.flatMap((part) => {
          if (typeof part !== "object" || !part) return [];
          if ("path" in part && typeof part.path === "string") return [part.path];
          if ("url" in part && typeof part.url === "string") return [part.url];
          return [];
        });
        const visibleText = isGolazoExecutionGuidance(text) ? "" : text;
        if (visibleText || images.length) messages.push({ id: String(item.id), role: "user", text: visibleText, status: turn.status, images });
      }
    }
    return { id: turn.id, messages, working };
  }).filter((turn) => turn.messages.length || turn.working.length);
}

function restoreLocalUserPrompts(turns: HistoryTurn[], localRuns: Run[]): HistoryTurn[] {
  return turns.map((turn, index) => {
    if (turn.messages.some((message) => message.role === "user")) return turn;
    const run = localRuns[index];
    if (!run || (!run.prompt.trim() && !run.images.length)) return turn;
    return {
      ...turn,
      messages: [{
        id: `local-${run.id}`,
        role: "user",
        text: run.prompt,
        status: run.status,
        images: run.images.map((image) => `${API_BASE}/runs/${encodeURIComponent(run.id)}/images/${encodeURIComponent(image.id)}`),
      }, ...turn.messages],
    };
  });
}

function readImageAttachment(file: File, index: number): Promise<PendingImage> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error(`Could not read ${file.name || "pasted image"}`));
    reader.onload = () => {
      const preview = String(reader.result || "");
      const separator = preview.indexOf(",");
      if (separator < 0) {
        reject(new Error("Could not encode pasted image"));
        return;
      }
      resolve({
        id: `${Date.now()}-${index}-${Math.random().toString(36).slice(2)}`,
        name: file.name || `pasted-image-${index + 1}.png`,
        mime_type: file.type,
        path: "",
        data: preview.slice(separator + 1),
        preview,
      });
    };
    reader.readAsDataURL(file);
  });
}

function readFileAttachment(file: File, index: number): Promise<PendingFile> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error(`Could not read ${file.name || "attached file"}`));
    reader.onload = () => {
      const encoded = String(reader.result || "");
      const separator = encoded.indexOf(",");
      if (separator < 0) {
        reject(new Error(`Could not encode ${file.name || "attached file"}`));
        return;
      }
      resolve({
        id: `${Date.now()}-${index}-${Math.random().toString(36).slice(2)}`,
        name: file.name || `attachment-${index + 1}`,
        mime_type: file.type || "application/octet-stream",
        path: "",
        data: encoded.slice(separator + 1),
        size: file.size,
      });
    };
    reader.readAsDataURL(file);
  });
}

function rateLimitWindowLabel(window: RateLimitWindow, fallback: string): string {
  const minutes = window.windowDurationMins;
  if (!minutes) return fallback;
  if (minutes === 10080) return "Week";
  if (minutes % 1440 === 0) return `${minutes / 1440}d`;
  if (minutes % 60 === 0) return `${minutes / 60}h`;
  return `${minutes}m`;
}

function remainingPercent(window: RateLimitWindow): number {
  return Math.max(0, Math.min(100, 100 - window.usedPercent));
}

function isProjectLocationDecision(value: string): boolean {
  const decision = value.toLowerCase();
  return decision.includes("implementation location")
    || decision.includes("target repository")
    || decision.includes("target project")
    || decision.includes("project/repository");
}

function isGolazoExecutionGuidance(value: string): boolean {
  return value.startsWith("You are in Golazo Spec mode for goal")
    || value.startsWith("You are in Golazo Build mode for goal")
    || value.startsWith("You are running Golazo's bundled");
}

function resetLabel(timestamp: number | null): string {
  if (!timestamp) return "Reset time unavailable";
  return `Resets ${new Date(timestamp * 1000).toLocaleString([], { dateStyle: "medium", timeStyle: "short" })}`;
}

function profileInitials(value: string): string {
  const parts = value.trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "U";
  return parts.slice(0, 2).map((part) => part[0]).join("").toUpperCase();
}

function PermissionIcon({ mode }: { mode: PermissionMode }) {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 2.8 19 6v5.2c0 4.7-2.8 8.2-7 10-4.2-1.8-7-5.3-7-10V6l7-3.2Z" />
      {mode === "ask" && <path d="M9.2 11.8h5.6M12 9v5.6" />}
      {mode === "auto-review" && <path d="m8.7 12.1 2.1 2.1 4.5-4.7" />}
      {mode === "full-access" && <path d="M12 8v5.2M12 16.5h.01" />}
    </svg>
  );
}

function FeatureCard({ feature, slices }: { feature: Feature; slices: ImplementationSlice[] }) {
  return (
    <details className="feature">
      <summary className="feature-summary">
        <div className="feature-top"><strong>{feature.title}</strong><span className={`feature-status ${feature.status}`}>{feature.status}</span><i aria-hidden="true" /></div>
        <div className="mini-track"><i style={{ width: `${feature.progress.completion_rate}%` }} /></div>
        <small>{feature.progress.completed_steps}/{feature.progress.total_steps} steps · {feature.slice_count} slices</small>
      </summary>
      <div className="feature-details">
        {feature.description && <p>{feature.description}</p>}
        <section className="feature-steps" aria-label={`${feature.title} implementation steps`}>
          <h3>Steps</h3>
          {feature.steps.length ? feature.steps.map((step) => (
            <div className={step.done ? "done" : ""} key={step.id}><span aria-hidden="true">{step.done ? "✓" : "○"}</span><p>{step.title}</p></div>
          )) : <p className="feature-empty">No steps recorded.</p>}
        </section>
        <section className="feature-slices" aria-label={`${feature.title} implementation slices`}>
          <h3>Slice summaries</h3>
          {slices.length ? slices.map((slice) => (
            <article key={slice.id}><div><span>{slice.id.replace("slice-", "Slice ")}</span><em className={`feature-status ${slice.status}`}>{slice.status}</em></div><p>{slice.summary}</p></article>
          )) : <p className="feature-empty">No implementation slices yet.</p>}
        </section>
      </div>
    </details>
  );
}

function GoalDialog({ workspace, onClose, onCreated }: { workspace: Workspace; onClose: () => void; onCreated: (goal: Goal) => void }) {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [goalTypeHint, setGoalTypeHint] = useState<GoalType | "">("");
  const [stage, setStage] = useState<"input" | "generating" | "review">("input");
  const [scaffoldRunId, setScaffoldRunId] = useState<string | null>(null);
  const [proposal, setProposal] = useState<ScaffoldProposal | null>(null);
  const [selectedFeatures, setSelectedFeatures] = useState<Record<string, boolean>>({});
  const [decisionsReviewed, setDecisionsReviewed] = useState(false);
  const [projectPath, setProjectPath] = useState("");
  const [projectPathExists, setProjectPathExists] = useState(false);
  const [projectLocationConfirmed, setProjectLocationConfirmed] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!scaffoldRunId || stage !== "generating") return undefined;
    let cancelled = false;
    let timer = 0;
    const poll = async () => {
      try {
        const status = await api<ScaffoldStatus>(`/goal-scaffolds/${encodeURIComponent(scaffoldRunId)}`);
        if (cancelled) return;
        if (status.run.status === "failed") {
          setError(status.run.error || "Codex could not scaffold this goal.");
          setStage("input");
          setScaffoldRunId(null);
          return;
        }
        if (status.proposal) {
          const selected: Record<string, boolean> = {};
          status.proposal.required_features.forEach((feature) => { selected[feature.feature_id] = true; });
          status.proposal.optional_features.forEach((feature) => { selected[feature.feature_id] = false; });
          setProposal(status.proposal);
          setSelectedFeatures(selected);
          const greenfield = status.proposal.primary_type === "Greenfield";
          const openDecisions = status.proposal.decisions_required.filter((decision) => !greenfield || !isProjectLocationDecision(decision));
          setDecisionsReviewed(openDecisions.length === 0);
          setProjectLocationConfirmed(false);
          if (greenfield) {
            const suggestion = await api<GreenfieldLocationSuggestion>(`/greenfield-location-suggestion?title=${encodeURIComponent(title)}`);
            setProjectPath(suggestion.path);
            setProjectPathExists(suggestion.exists);
          } else {
            setProjectPath("");
            setProjectPathExists(false);
          }
          setStage("review");
          return;
        }
        timer = window.setTimeout(() => void poll(), 900);
      } catch (pollError) {
        if (!cancelled) {
          setError(pollError instanceof Error ? pollError.message : String(pollError));
          setStage("input");
          setScaffoldRunId(null);
        }
      }
    };
    void poll();
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [scaffoldRunId, stage]);

  async function createBlank() {
    setSaving(true);
    setError("");
    try {
      const goal = await api<Goal>("/goals", {
        method: "POST",
        body: JSON.stringify({ title, description }),
      });
      onCreated(goal);
    } finally {
      setSaving(false);
    }
  }

  async function startScaffold(event: FormEvent) {
    event.preventDefault();
    if (!title.trim()) return;
    setSaving(true);
    setError("");
    try {
      const run = await api<Run>("/goal-scaffolds", {
        method: "POST",
        body: JSON.stringify({ title, description, goal_type_hint: goalTypeHint || null }),
      });
      setScaffoldRunId(run.id);
      setStage("generating");
    } catch (scaffoldError) {
      setError(scaffoldError instanceof Error ? scaffoldError.message : String(scaffoldError));
    } finally {
      setSaving(false);
    }
  }

  async function acceptScaffold(event: FormEvent) {
    event.preventDefault();
    if (!proposal || !scaffoldRunId) return;
    const features = [...proposal.required_features, ...proposal.optional_features]
      .filter((feature) => selectedFeatures[feature.feature_id]);
    if (!features.length) {
      setError("Select at least one feature to create the implementation tracker.");
      return;
    }
    const openDecisions = proposal.decisions_required.filter((decision) => proposal.primary_type !== "Greenfield" || !isProjectLocationDecision(decision));
    if (openDecisions.length > 0 && !decisionsReviewed) {
      setError("Review the open decisions before accepting this plan.");
      return;
    }
    if (proposal.primary_type === "Greenfield" && (!projectPath.trim() || !projectLocationConfirmed)) {
      setError("Confirm where the Greenfield application and its tracker should be created.");
      return;
    }
    setSaving(true);
    setError("");
    try {
      const goal = await api<Goal>(`/goal-scaffolds/${encodeURIComponent(scaffoldRunId)}/accept`, {
        method: "POST",
        body: JSON.stringify({
          title,
          description,
          primary_type: proposal.primary_type,
          success_criteria: proposal.success_criteria,
          assumptions: proposal.assumptions,
          decisions_required: openDecisions,
          non_goals: proposal.non_goals,
          project_path: proposal.primary_type === "Greenfield" ? projectPath.trim() : null,
          project_location_confirmed: proposal.primary_type === "Greenfield" && projectLocationConfirmed,
          features,
        }),
      });
      onCreated(goal);
    } catch (acceptError) {
      setError(acceptError instanceof Error ? acceptError.message : String(acceptError));
    } finally {
      setSaving(false);
    }
  }

  const suggestedFeatures = proposal ? [...proposal.required_features, ...proposal.optional_features] : [];
  const openDecisions = proposal?.decisions_required.filter((decision) => proposal.primary_type !== "Greenfield" || !isProjectLocationDecision(decision)) || [];

  async function chooseExistingProjectFolder() {
    if (!window.desktop) return;
    const path = await window.desktop.selectDirectory(projectPath || workspace.path);
    if (path) {
      setProjectPath(path);
      setProjectPathExists(true);
      setProjectLocationConfirmed(false);
    }
  }

  return (
    <div className="modal-layer" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <section className={`react-dialog goal-dialog ${stage === "review" ? "goal-review-dialog" : ""}`} role="dialog" aria-modal="true" aria-labelledby="goal-dialog-title">
        <form onSubmit={stage === "review" ? acceptScaffold : startScaffold}>
          <div className="dialog-heading">
            <div><p className="eyebrow">{stage === "review" ? "Review scaffold" : "Start tracking"}</p><h2 id="goal-dialog-title">{stage === "review" ? "Choose the implementation plan" : "Create a goal"}</h2></div>
            <button type="button" onClick={onClose} aria-label="Close">×</button>
          </div>
          {stage === "input" && <>
            <label>Title<input autoFocus value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Build a customer support portal" required /></label>
            <label>Success description<textarea value={description} onChange={(event) => setDescription(event.target.value)} rows={9} placeholder="Who is it for, what should it accomplish, and what constraints matter?" /></label>
            <label>Goal type
              <select value={goalTypeHint} onChange={(event) => setGoalTypeHint(event.target.value as GoalType | "")}>
                <option value="">Let Codex evaluate</option>
                {(["Greenfield", "Feature", "Bug", "Refactor", "Migration", "Integration", "Release", "Research", "Mixed"] as GoalType[]).map((type) => <option key={type} value={type}>{type}</option>)}
              </select>
            </label>
            <p className="generated-id-note">Codex will inspect this project read-only, classify the goal, and suggest a reviewable tracker. Nothing is created until you accept the plan.</p>
          </>}
          {stage === "generating" && <div className="scaffold-loading" role="status"><i className="spinner" /><div><strong>Codex is evaluating the goal</strong><span>Inspecting the repository and proposing features, steps, risks, and verification.</span></div></div>}
          {stage === "review" && proposal && <div className="scaffold-review">
            <section className="scaffold-summary">
              <div><span className="scaffold-type">{proposal.primary_type}</span><em>{proposal.confidence} confidence</em></div>
              <p>{proposal.goal_interpretation}</p>
              {proposal.classification_evidence.length > 0 && <small>{proposal.classification_evidence.join(" · ")}</small>}
            </section>
            {proposal.primary_type === "Greenfield" && <section className="greenfield-location">
              <div><strong>Application location</strong><span>Suggested dedicated project</span></div>
              <p>Golazo will keep the new application code and its implementation tracker together here.</p>
              <label>Project folder
                <span className="location-input"><input value={projectPath} onChange={(event) => { setProjectPath(event.target.value); setProjectPathExists(false); setProjectLocationConfirmed(false); }} required /><button type="button" className="quiet-button" onClick={() => void chooseExistingProjectFolder()}>Browse…</button></span>
              </label>
              <small>{projectPathExists ? "This folder already exists and will be used as the project." : "This folder will be created when you accept the plan."}</small>
              <label className="decision-review"><input type="checkbox" checked={projectLocationConfirmed} onChange={(event) => setProjectLocationConfirmed(event.target.checked)} /><span>Use this location for the Greenfield application and tracker.</span></label>
            </section>}
            {openDecisions.length > 0 && <section className="scaffold-decisions"><strong>Other decisions to confirm</strong>{openDecisions.map((decision) => <p key={decision}>{decision}</p>)}<label className="decision-review"><input type="checkbox" checked={decisionsReviewed} onChange={(event) => setDecisionsReviewed(event.target.checked)} /><span>I reviewed these decisions; keep unresolved items visible in the tracker.</span></label></section>}
            <div className="scaffold-feature-list">
              {suggestedFeatures.map((feature) => <label className={`scaffold-feature ${selectedFeatures[feature.feature_id] ? "selected" : ""}`} key={feature.feature_id}>
                <input type="checkbox" checked={Boolean(selectedFeatures[feature.feature_id])} onChange={(event) => setSelectedFeatures({ ...selectedFeatures, [feature.feature_id]: event.target.checked })} />
                <div>
                  <div className="scaffold-feature-heading"><strong>{feature.title}</strong><span>{feature.scope.replaceAll("_", " ")}</span></div>
                  <p>{feature.rationale}</p>
                  <ul>{feature.steps.map((step) => <li key={step.step_id}>{step.title}</li>)}</ul>
                  {feature.acceptance.length > 0 && <small>Done when: {feature.acceptance.join("; ")}</small>}
                </div>
              </label>)}
            </div>
          </div>}
          {error && <p className="dialog-error" role="alert">{error}</p>}
          {stage !== "generating" && <div className="dialog-actions scaffold-actions">
            {stage === "input" ? <>
              <button type="button" className="quiet-button" onClick={onClose}>Cancel</button>
              <button type="button" className="quiet-button" disabled={saving || !title.trim()} onClick={() => void createBlank()}>Create blank</button>
              <button type="submit" className="primary-button" disabled={saving || !title.trim()}>{saving ? "Starting…" : "Get Codex suggestions"}</button>
            </> : <>
              <button type="button" className="quiet-button" disabled={saving} onClick={() => { setStage("input"); setProposal(null); setScaffoldRunId(null); setDecisionsReviewed(false); setProjectPath(""); setProjectLocationConfirmed(false); }}>Start over</button>
              <button type="submit" className="primary-button" disabled={saving}>{saving ? "Creating tracker…" : "Accept selected plan"}</button>
            </>}
          </div>}
        </form>
      </section>
    </div>
  );
}

function WorkspaceDialog({ initialPath, onClose, onSelect }: {
  initialPath: string;
  onClose: () => void;
  onSelect: (path: string) => Promise<void>;
}) {
  const [listing, setListing] = useState<DirectoryListing | null>(null);
  const [hidden, setHidden] = useState(false);
  const [saving, setSaving] = useState(false);

  const browse = useCallback(async (path: string) => {
    const params = new URLSearchParams({ path });
    if (hidden) params.set("show_hidden", "true");
    setListing(await api<DirectoryListing>(`/filesystem?${params}`));
  }, [hidden]);

  useEffect(() => { void browse(listing?.path || initialPath); }, [browse, hidden]); // eslint-disable-line react-hooks/exhaustive-deps

  async function select() {
    if (!listing) return;
    setSaving(true);
    try { await onSelect(listing.path); } finally { setSaving(false); }
  }

  return (
    <div className="modal-layer" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <section className="react-dialog workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="workspace-dialog-title">
        <div className="dialog-heading">
          <div><p className="eyebrow">Local filesystem</p><h2 id="workspace-dialog-title">Add a project</h2></div>
          <button type="button" onClick={onClose} aria-label="Close">×</button>
        </div>
        <p className="workspace-note">Choose a local folder for this project. Its goals, implementation tracking, chat history, and token usage stay associated with that folder.</p>
        <div className="path-bar">
          <button disabled={!listing?.parent} onClick={() => listing?.parent && void browse(listing.parent)} aria-label="Parent directory">←</button>
          <code>{listing?.path || "Loading…"}</code>
        </div>
        <div className="directory-list" aria-label="Directories">
          {listing?.directories.length ? listing.directories.map((directory) => (
            <button className="directory-row" key={directory.path} onClick={() => void browse(directory.path)}>
              <span className="folder-icon">▰</span><strong>{directory.name}</strong><small>{directory.symlink ? "linked folder" : "open"} →</small>
            </button>
          )) : <div className="empty-small">No subfolders in this directory.</div>}
        </div>
        <div className="workspace-actions">
          <label className="hidden-toggle"><input type="checkbox" checked={hidden} onChange={(event) => setHidden(event.target.checked)} /> Show hidden folders</label>
          <div><button className="quiet-button" onClick={onClose}>Cancel</button><button className="primary-button" onClick={() => void select()} disabled={!listing || saving}>{saving ? "Adding…" : "Add project"}</button></div>
        </div>
      </section>
    </div>
  );
}

function ProjectRenameDialog({ project, onClose, onRename }: {
  project: Project;
  onClose: () => void;
  onRename: (project: Project, name: string) => Promise<void>;
}) {
  const [name, setName] = useState(project.name);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  async function submit(event: FormEvent) {
    event.preventDefault();
    const nextName = name.trim();
    if (!nextName) {
      setError("Project name must not be empty");
      return;
    }
    if (nextName === project.name) {
      onClose();
      return;
    }
    setSaving(true);
    setError("");
    try {
      await onRename(project, nextName);
      onClose();
    } catch (renameError) {
      setError(renameError instanceof Error ? renameError.message : String(renameError));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="modal-layer" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <section className="react-dialog" role="dialog" aria-modal="true" aria-labelledby="project-rename-dialog-title">
        <form onSubmit={submit}>
          <div className="dialog-heading">
            <div><p className="eyebrow">Project</p><h2 id="project-rename-dialog-title">Rename project</h2></div>
            <button type="button" onClick={onClose} aria-label="Close">×</button>
          </div>
          <label>Project name<input autoFocus value={name} onChange={(event) => setName(event.target.value)} maxLength={200} required /></label>
          {error && <p className="dialog-error" role="alert">{error}</p>}
          <div className="dialog-actions">
            <button type="button" className="quiet-button" onClick={onClose}>Cancel</button>
            <button type="submit" className="primary-button" disabled={saving}>{saving ? "Renaming…" : "Rename"}</button>
          </div>
        </form>
      </section>
    </div>
  );
}

export default function App() {
  const [goals, setGoals] = useState<Goal[]>([]);
  const [runs, setRuns] = useState<Run[]>([]);
  const [usage, setUsage] = useState<Usage>(emptyUsage);
  const [workspace, setWorkspace] = useState<Workspace | null>(null);
  const [projects, setProjects] = useState<Project[]>([]);
  const [collapsedProjects, setCollapsedProjects] = useState<string[]>(loadCollapsedProjects);
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [appThreads, setAppThreads] = useState<AppServerThread[]>([]);
  const [appHistory, setAppHistory] = useState<AppServerThread | null>(null);
  const [approvals, setApprovals] = useState<AppServerApproval[]>([]);
  const [codexRateLimits, setCodexRateLimits] = useState<RateLimitResponse | null>(null);
  const [appServerModels, setAppServerModels] = useState<AppServerModel[]>([]);
  const [appServerConnected, setAppServerConnected] = useState(false);
  const [liveAssistant, setLiveAssistant] = useState("");
  const [liveWorking, setLiveWorking] = useState("");
  const [defaultLLMConfig, setDefaultLLMConfig] = useState<LLMConfig>(fallbackLLMConfig);
  const [llmConfig, setLLMConfig] = useState<LLMConfig>(fallbackLLMConfig);
  const [codexAuth, setCodexAuth] = useState<CodexAuthStatus>(fallbackCodexAuth);
  const [codexAuthBusy, setCodexAuthBusy] = useState(false);
  const [profile, setProfile] = useState<UserProfile>(emptyProfile);
  const [profileDraft, setProfileDraft] = useState<UserProfile>(emptyProfile);
  const [codexAccount, setCodexAccount] = useState<CodexAccount | null>(null);
  const [profileOpen, setProfileOpen] = useState(false);
  const [profileSaving, setProfileSaving] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [settingsPanel, setSettingsPanel] = useState<"main" | "model" | "effort" | "speed">("main");
  const [selectedGoal, setSelectedGoal] = useState<string | null>(null);
  const [threadId, setThreadId] = useState<string | null>(null);
  const [pendingRunId, setPendingRunId] = useState<string | null>(null);
  const [freshChat, setFreshChat] = useState(false);
  const [prompt, setPrompt] = useState("");
  const [pendingImages, setPendingImages] = useState<PendingImage[]>([]);
  const [pendingFiles, setPendingFiles] = useState<PendingFile[]>([]);
  const [permissionMode, setPermissionMode] = useState<PermissionMode>("ask");
  const [permissionsOpen, setPermissionsOpen] = useState(false);
  const [workMode, setWorkMode] = useState<WorkMode>("spec");
  const [goalDialog, setGoalDialog] = useState(false);
  const [workspaceDialog, setWorkspaceDialog] = useState(false);
  const [renamingProject, setRenamingProject] = useState<Project | null>(null);
  const [connected, setConnected] = useState(false);
  const [conversationSyncing, setConversationSyncing] = useState(false);
  const [conversationSyncedAt, setConversationSyncedAt] = useState<number | null>(null);
  const [toast, setToast] = useState("");
  const [showScrollToBottom, setShowScrollToBottom] = useState(false);
  const promptRef = useRef<HTMLTextAreaElement | null>(null);
  const attachmentInputRef = useRef<HTMLInputElement | null>(null);
  const settingsRef = useRef<HTMLDivElement | null>(null);
  const permissionsRef = useRef<HTMLDivElement | null>(null);
  const profileRef = useRef<HTMLDivElement | null>(null);
  const messagesRef = useRef<HTMLDivElement | null>(null);
  const pendingInitialScroll = useRef<string | null>(null);
  const messagesAtBottom = useRef(true);
  const lastEventSequence = useRef(0);
  const liveMessagePhases = useRef(new Map<string, AgentMessagePhase>());

  const refresh = useCallback(async () => {
    try {
      const [nextGoals, nextRuns, nextUsage, nextWorkspace, nextProjects, nextThreads, nextDefaultLLMConfig, nextCodexAuth, nextProfile] = await Promise.all([
        api<Goal[]>("/goals"), api<Run[]>("/runs"), api<Usage>("/usage"), api<Workspace>("/workspace"), api<Project[]>("/projects"),
        api<ThreadSummary[]>("/threads"), api<LLMConfig>("/settings/default-llm"), api<CodexAuthStatus>("/codex-auth/status"), api<UserProfile>("/profile"),
      ]);
      setGoals(nextGoals); setRuns(nextRuns); setUsage(nextUsage); setWorkspace(nextWorkspace); setProjects(nextProjects);
      setThreads(nextThreads.map((thread) => ({ ...thread, llm_config: normalizeLLMConfig(thread.llm_config) }))); setDefaultLLMConfig(normalizeLLMConfig(nextDefaultLLMConfig)); setCodexAuth(nextCodexAuth); setProfile(nextProfile); setConnected(true);
      setSelectedGoal((current) => current && nextGoals.some((goal) => goal.goal_id === current) ? current : nextGoals[0]?.goal_id || null);
    } catch (error) {
      setConnected(false); setToast(error instanceof Error ? error.message : String(error));
    }
  }, []);

  const refreshAppServer = useCallback(async (showProgress = false) => {
    if (showProgress) setConversationSyncing(true);
    try {
      const [page, pending] = await Promise.all([
        api<AppServerThreadPage>("/app-server/threads?limit=100"),
        api<AppServerApproval[]>("/app-server/approvals"),
      ]);
      const nextThreads = await api<ThreadSummary[]>("/threads");
      setAppThreads(page.data);
      setThreads(nextThreads.map((thread) => ({ ...thread, llm_config: normalizeLLMConfig(thread.llm_config) })));
      setApprovals(pending);
      setAppServerConnected(true);
      setConversationSyncedAt(Date.now());
    } catch { setAppServerConnected(false); }
    finally { if (showProgress) setConversationSyncing(false); }
  }, []);

  const refreshAppServerModels = useCallback(async () => {
    try { setAppServerModels((await api<AppServerModelPage>("/app-server/models")).data); }
    catch { setAppServerModels([]); }
  }, []);

  const refreshCodexRateLimits = useCallback(async () => {
    try { setCodexRateLimits(await api<RateLimitResponse>("/app-server/rate-limits")); }
    catch { setCodexRateLimits(null); }
  }, []);

  const refreshCodexAccount = useCallback(async () => {
    try { setCodexAccount((await api<CodexAccountResponse>("/app-server/account")).account); }
    catch { setCodexAccount(null); }
  }, []);

  useEffect(() => {
    void refresh();
    void refreshAppServer();
    void refreshAppServerModels();
    void refreshCodexRateLimits();
    void refreshCodexAccount();
    const timer = window.setInterval(() => void refresh(), 10000);
    const syncTimer = window.setInterval(() => void refreshAppServer(), 5000);
    const rateLimitTimer = window.setInterval(() => void refreshCodexRateLimits(), 60000);
    return () => { window.clearInterval(timer); window.clearInterval(syncTimer); window.clearInterval(rateLimitTimer); };
  }, [refresh, refreshAppServer, refreshAppServerModels, refreshCodexAccount, refreshCodexRateLimits]);

  useEffect(() => {
    const source = new EventSource(`${API_BASE}/app-server/events`);
    source.addEventListener("app-server", (message) => {
      const event = JSON.parse((message as MessageEvent).data) as AppServerEvent;
      if (event.sequence > 0 && event.sequence <= lastEventSequence.current) return;
      if (event.sequence > 0) lastEventSequence.current = event.sequence;
      if (event.method === "connection/status") {
        const isConnected = event.params.status === "connected";
        setAppServerConnected(isConnected);
        if (isConnected) { void refreshCodexRateLimits(); void refreshAppServerModels(); void refreshCodexAccount(); }
      }
      if (event.method === "account/rateLimits/updated") void refreshCodexRateLimits();
      if (event.method === "account/updated") void refreshCodexAccount();
      if (event.method === "turn/started" && event.params.threadId === threadId) {
        setLiveAssistant(""); setLiveWorking(""); liveMessagePhases.current.clear();
      }
      if (event.method === "item/started" && event.params.threadId === threadId) {
        const item = event.params.item;
        if (typeof item === "object" && item && "type" in item && item.type === "agentMessage" && "id" in item && typeof item.id === "string" && "phase" in item && (item.phase === "commentary" || item.phase === "final_answer")) {
          liveMessagePhases.current.set(item.id, item.phase);
        }
      }
      if (event.method === "item/agentMessage/delta" && event.params.threadId === threadId && typeof event.params.delta === "string") {
        const itemId = typeof event.params.itemId === "string" ? event.params.itemId : "";
        const phase = liveMessagePhases.current.get(itemId);
        if (phase === "final_answer") setLiveAssistant((current) => current + String(event.params.delta));
        else if (phase === "commentary") setLiveWorking((current) => current + String(event.params.delta));
      }
      void refreshAppServer();
      if (threadId && event.params.threadId === threadId) {
        void api<AppServerThread>(`/app-server/threads/${encodeURIComponent(threadId)}`).then(setAppHistory);
      }
      if (event.method === "turn/completed") {
        setLiveAssistant(""); setLiveWorking(""); liveMessagePhases.current.clear(); void refresh();
      }
    });
    source.onerror = () => setAppServerConnected(false);
    return () => source.close();
  }, [refresh, refreshAppServer, refreshAppServerModels, refreshCodexAccount, refreshCodexRateLimits, threadId]);

  useEffect(() => {
    if (freshChat || threadId || pendingRunId || !selectedGoal) return;
    const latest = runs.find((run) => run.goal_id === selectedGoal && run.thread_id);
    if (latest?.thread_id) setThreadId(latest.thread_id);
  }, [freshChat, pendingRunId, runs, selectedGoal, threadId]);

  useEffect(() => {
    if (!pendingRunId) return;
    const run = runs.find((item) => item.id === pendingRunId);
    if (run?.thread_id) { setThreadId(run.thread_id); setPendingRunId(null); }
    else if (run?.status === "failed") setPendingRunId(null);
  }, [pendingRunId, runs]);

  useEffect(() => {
    if (!toast) return;
    const timer = window.setTimeout(() => setToast(""), 3500);
    return () => window.clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    window.localStorage.setItem(COLLAPSED_PROJECTS_KEY, JSON.stringify(collapsedProjects));
  }, [collapsedProjects]);

  useEffect(() => {
    if (!settingsOpen) return undefined;
    const close = () => { setSettingsOpen(false); setSettingsPanel("main"); };
    const onPointerDown = (event: PointerEvent) => {
      if (event.target instanceof Node && !settingsRef.current?.contains(event.target)) close();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [settingsOpen]);

  useEffect(() => {
    if (!permissionsOpen) return undefined;
    const close = () => setPermissionsOpen(false);
    const onPointerDown = (event: PointerEvent) => {
      if (event.target instanceof Node && !permissionsRef.current?.contains(event.target)) close();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [permissionsOpen]);

  useEffect(() => {
    if (!profileOpen) return undefined;
    const close = () => setProfileOpen(false);
    const onPointerDown = (event: PointerEvent) => {
      if (event.target instanceof Node && !profileRef.current?.contains(event.target)) close();
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [profileOpen]);

  const goal = goals.find((item) => item.goal_id === selectedGoal) || null;
  const activeThread = threads.find((item) => item.id === threadId) || null;
  const usingCodexAccount = llmConfig.provider === "openai" && codexAuth.authenticated && codexAuth.method === "ChatGPT";
  const providerLabel = llmConfig.provider === "ollama" ? "Ollama" : llmConfig.provider === "lmstudio" ? "LM Studio" : "OpenAI";
  const effectiveDefaultModel = appServerModels.find((model) => model.isDefault && !model.hidden);
  const defaultModelLabel = effectiveDefaultModel?.displayName || effectiveDefaultModel?.model || "Codex default";
  const modelLabel = modelOptions.find((option) => option.value === llmConfig.model)?.label || (llmConfig.model || defaultModelLabel);
  const effortLabel = effortOptions.find((option) => option.value === llmConfig.reasoning)?.label || "Medium";
  const speedLabel = speedOptions.find((option) => option.value === llmConfig.speed)?.label || "Standard";
  const settingsSummary = usingCodexAccount ? `${modelLabel} · ${effortLabel}` : `${providerLabel} · ${modelLabel} · ${effortLabel}`;
  const selectedPermission = permissionOptions.find((option) => option.value === permissionMode) || permissionOptions[0];
  const profileName = profile.display_name || codexAccount?.email?.split("@")[0] || "Your profile";
  const profileIdentity = profile.display_name || codexAccount?.email || "User";

  useEffect(() => {
    setLLMConfig(normalizeLLMConfig(activeThread?.llm_config || defaultLLMConfig));
  }, [threadId, activeThread?.updated_at, defaultLLMConfig.provider, defaultLLMConfig.model, defaultLLMConfig.reasoning, defaultLLMConfig.speed]);
  useEffect(() => {
    setWorkMode(activeThread?.work_mode || "spec");
  }, [threadId, activeThread?.updated_at, activeThread?.work_mode]);
  const chatRuns = useMemo(() => {
    if (freshChat) return [];
    const relevant = runs.filter((run) => run.goal_id === selectedGoal);
    if (!threadId) return relevant.filter((run) => !run.resumed_from).slice(0, 1).reverse();
    return relevant.filter((run) => run.thread_id === threadId || run.resumed_from === threadId).reverse();
  }, [freshChat, runs, selectedGoal, threadId]);
  const busy = chatRuns.some((run) => run.status === "queued" || run.status === "running");
  const restoredTurns = useMemo(() => restoreLocalUserPrompts(historyTurns(appHistory), chatRuns), [appHistory, chatRuns]);
  const codexLimitSnapshot = useMemo(() => {
    if (!codexRateLimits) return null;
    const buckets = Object.values(codexRateLimits.rateLimitsByLimitId || {});
    return codexRateLimits.rateLimitsByLimitId?.codex
      || buckets.find((snapshot) => snapshot.limitId === "codex")
      || codexRateLimits.rateLimits;
  }, [codexRateLimits]);
  const codexLimitWindows = [
    codexLimitSnapshot?.primary ? { key: "primary", label: rateLimitWindowLabel(codexLimitSnapshot.primary, "Primary"), window: codexLimitSnapshot.primary } : null,
    codexLimitSnapshot?.secondary ? { key: "secondary", label: rateLimitWindowLabel(codexLimitSnapshot.secondary, "Secondary"), window: codexLimitSnapshot.secondary } : null,
  ].filter((item): item is { key: string; label: string; window: RateLimitWindow } => Boolean(item));

  const updateMessageScrollState = useCallback(() => {
    const messages = messagesRef.current;
    if (!messages) return;
    const distanceFromBottom = messages.scrollHeight - messages.clientHeight - messages.scrollTop;
    const atBottom = distanceFromBottom <= 48;
    messagesAtBottom.current = atBottom;
    setShowScrollToBottom(!atBottom && messages.scrollHeight > messages.clientHeight);
  }, []);

  const scrollToLatestMessage = useCallback((behavior: ScrollBehavior = "smooth") => {
    const messages = messagesRef.current;
    if (!messages) return;
    messagesAtBottom.current = true;
    setShowScrollToBottom(false);
    messages.scrollTo({ top: messages.scrollHeight, behavior });
  }, []);

  useEffect(() => {
    pendingInitialScroll.current = threadId;
    messagesAtBottom.current = true;
    setShowScrollToBottom(false);
    if (!threadId) {
      setAppHistory(null);
      return;
    }

    let cancelled = false;
    setAppHistory((current) => current?.id === threadId ? current : null);
    const loadHistory = () => api<AppServerThread>(`/app-server/threads/${encodeURIComponent(threadId)}`)
      .then((history) => { if (!cancelled) setAppHistory(history); })
      .catch(() => {
        if (cancelled) return;
        setAppHistory(null);
        pendingInitialScroll.current = null;
        window.requestAnimationFrame(() => scrollToLatestMessage("auto"));
      });
    void loadHistory();
    const timer = window.setInterval(() => void loadHistory(), 5000);
    return () => { cancelled = true; window.clearInterval(timer); };
  }, [scrollToLatestMessage, threadId]);

  useLayoutEffect(() => {
    if (!threadId || appHistory?.id !== threadId || pendingInitialScroll.current !== threadId) return;
    scrollToLatestMessage("auto");
    pendingInitialScroll.current = null;
  }, [appHistory, restoredTurns, scrollToLatestMessage, threadId]);

  useLayoutEffect(() => {
    if (pendingInitialScroll.current) return;
    if (messagesAtBottom.current) scrollToLatestMessage("auto");
    else updateMessageScrollState();
  }, [approvals.length, chatRuns, liveAssistant, liveWorking, restoredTurns, scrollToLatestMessage, updateMessageScrollState]);

  async function send(event: FormEvent) {
    event.preventDefault();
    const text = prompt.trim();
    if ((!text && !pendingImages.length && !pendingFiles.length) || !selectedGoal || busy) return;
    try {
      const created = await api<Run>("/runs", {
        method: "POST",
        body: JSON.stringify({
          prompt: text,
          images: pendingImages.map(({ name, mime_type, data }) => ({ name, mime_type, data })),
          files: pendingFiles.map(({ name, mime_type, data }) => ({ name, mime_type, data })),
          goal_id: selectedGoal,
          thread_id: threadId,
          sandbox: selectedPermission.sandbox,
          approval_policy: selectedPermission.approvalPolicy,
          approvals_reviewer: selectedPermission.approvalsReviewer,
          work_mode: workMode,
          llm_config: llmConfig,
        }),
      });
      setPrompt(""); setPendingImages([]); setPendingFiles([]); setFreshChat(false);
      if (!threadId) setPendingRunId(created.id);
      await refresh();
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  async function changeWorkMode(nextMode: WorkMode) {
    if (nextMode === workMode) return;
    const previousMode = workMode;
    setWorkMode(nextMode);
    setToast(nextMode === "build"
      ? "Build mode selected. Nothing runs until you send an explicit implementation prompt."
      : "Spec mode selected. Continue evolving the implementation document when you send your next prompt.");
    if (!threadId) return;
    try {
      const updated = await api<ThreadSummary>(`/threads/${encodeURIComponent(threadId)}`, {
        method: "PATCH",
        body: JSON.stringify({ work_mode: nextMode }),
      });
      setThreads((current) => current.map((thread) => thread.id === updated.id
        ? { ...updated, llm_config: normalizeLLMConfig(updated.llm_config) }
        : thread));
    } catch (error) {
      setWorkMode(previousMode);
      setToast(error instanceof Error ? error.message : String(error));
    }
  }

  async function addImageAttachments(files: File[]) {
    if (!files.length) return;
    const supported = new Set(["image/png", "image/jpeg", "image/webp", "image/gif"]);
    const available = Math.max(0, 4 - pendingImages.length);
    if (files.length > available) {
      setToast("You can attach up to 4 images");
      return;
    }
    if (files.some((file) => !supported.has(file.type))) {
      setToast("Attach PNG, JPEG, WebP, or GIF images");
      return;
    }
    if (files.some((file) => file.size > 10 * 1024 * 1024)) {
      setToast("Each image must be 10 MB or smaller");
      return;
    }
    const currentBytes = pendingImages.reduce((total, image) => total + Math.ceil(image.data.length * 0.75), 0);
    if (currentBytes + files.reduce((total, file) => total + file.size, 0) > 20 * 1024 * 1024) {
      setToast("Attached images must total 20 MB or less");
      return;
    }
    try {
      const images = await Promise.all(files.map(readImageAttachment));
      setPendingImages((current) => [...current, ...images]);
    } catch (error) {
      setToast(error instanceof Error ? error.message : String(error));
    }
  }

  async function pasteImages(event: ClipboardEvent<HTMLTextAreaElement>) {
    let files = Array.from(event.clipboardData.files)
      .filter((file) => file.type.startsWith("image/"));
    if (!files.length) files = Array.from(event.clipboardData.items)
      .filter((item) => item.kind === "file")
      .map((item) => item.getAsFile())
      .filter((file): file is File => Boolean(file))
      .filter((file) => file.type.startsWith("image/"));
    if (!files.length && navigator.clipboard?.read) {
      try {
        const clipboardItems = await navigator.clipboard.read();
        const pasted: File[] = [];
        for (const item of clipboardItems) {
          const type = item.types.find((candidate) => candidate.startsWith("image/"));
          if (!type) continue;
          const blob = await item.getType(type);
          const extension = type.split("/")[1]?.replace("jpeg", "jpg") || "png";
          pasted.push(new File([blob], `pasted-image-${Date.now()}.${extension}`, { type }));
        }
        files = pasted;
      } catch {
        // The event clipboard remains the primary path when async clipboard access is unavailable.
      }
    }
    if (!files.length) return;
    event.preventDefault();
    await addImageAttachments(files);
  }

  async function addFileAttachments(files: File[]) {
    if (!files.length) return;
    if (pendingFiles.length + files.length > 8) {
      setToast("You can attach up to 8 files");
      return;
    }
    if (files.some((file) => file.size > 25 * 1024 * 1024)) {
      setToast("Each file must be 25 MB or smaller");
      return;
    }
    const currentBytes = pendingFiles.reduce((total, file) => total + file.size, 0);
    if (currentBytes + files.reduce((total, file) => total + file.size, 0) > 50 * 1024 * 1024) {
      setToast("Attached files must total 50 MB or less");
      return;
    }
    try {
      const attachments = await Promise.all(files.map(readFileAttachment));
      setPendingFiles((current) => [...current, ...attachments]);
    } catch (error) {
      setToast(error instanceof Error ? error.message : String(error));
    }
  }

  async function addSelectedAttachments(files: File[]) {
    const supportedImages = new Set(["image/png", "image/jpeg", "image/webp", "image/gif"]);
    const unsupportedImage = files.find((file) => file.type.startsWith("image/") && !supportedImages.has(file.type));
    if (unsupportedImage) {
      setToast("Images must be PNG, JPEG, WebP, or GIF");
      return;
    }
    await addImageAttachments(files.filter((file) => supportedImages.has(file.type)));
    await addFileAttachments(files.filter((file) => !file.type.startsWith("image/")));
  }

  async function addProject(path: string) {
    await api<Project>("/projects", { method: "POST", body: JSON.stringify({ path, activate: true }) });
    setSelectedGoal(null); setThreadId(null); setFreshChat(true); setPendingImages([]); setPendingFiles([]); setWorkspaceDialog(false); await refresh();
  }

  function toggleProjectCollapsed(id: string) {
    setCollapsedProjects((current) => current.includes(id)
      ? current.filter((projectId) => projectId !== id)
      : [...current, id]);
  }

  async function chooseProject() {
    try {
      if (window.desktop) {
        const path = await window.desktop.selectDirectory(workspace?.path);
        if (path) await addProject(path);
      } else setWorkspaceDialog(true);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  function selectGoal(id: string) { setSelectedGoal(id); setThreadId(null); setFreshChat(false); setPendingImages([]); setPendingFiles([]); }

  async function selectThread(id: string) {
    setLiveAssistant(""); setLiveWorking(""); liveMessagePhases.current.clear();
    setPendingImages([]);
    setPendingFiles([]);
    if (!id) { setThreadId(null); setFreshChat(true); return; }
    const thread = threads.find((item) => item.id === id);
    const appThread = appThreads.find((item) => item.id === id);
    setThreadId(id); setFreshChat(false);
    const mappedGoal = thread?.goal_id || appThread?.goalId;
    if (mappedGoal) setSelectedGoal(mappedGoal);
    else if (selectedGoal && appThread) {
      await api<AppServerThread>(`/app-server/threads/${encodeURIComponent(id)}/goal`, {
        method: "PATCH", body: JSON.stringify({ goal_id: selectedGoal }),
      });
      await refreshAppServer();
    }
  }

  async function decideApproval(id: string, decision: "accept" | "acceptForSession" | "decline" | "cancel") {
    try {
      await api<void>(`/app-server/approvals/${encodeURIComponent(id)}`, { method: "POST", body: JSON.stringify({ decision }) });
      await refreshAppServer();
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  function storedImageSource(path: string): string | null {
    if (/^(data:|https?:)/.test(path)) return path;
    for (const run of runs) {
      const image = (run.images || []).find((candidate) => candidate.path === path);
      if (image) return `${API_BASE}/runs/${encodeURIComponent(run.id)}/images/${encodeURIComponent(image.id)}`;
    }
    return null;
  }

  async function saveLLMConfig() {
    try {
      if (threadId) {
        await api<ThreadSummary>(`/threads/${encodeURIComponent(threadId)}`, {
          method: "PATCH", body: JSON.stringify({ llm_config: llmConfig }),
        });
        setToast("Thread configuration saved");
      } else {
        const saved = await api<LLMConfig>("/settings/default-llm", {
          method: "PATCH", body: JSON.stringify(llmConfig),
        });
        setDefaultLLMConfig(saved); setToast("Default configuration saved");
      }
      await refresh();
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  async function connectCodex() {
    setCodexAuthBusy(true);
    try {
      const action = await api<CodexAuthAction>("/codex-auth/login", { method: "POST", body: "{}" });
      setToast(action.message);
      setSettingsOpen(false);
      window.setTimeout(() => { void refresh(); void refreshCodexAccount(); }, 1500);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
    finally { setCodexAuthBusy(false); }
  }

  async function disconnectCodex() {
    setCodexAuthBusy(true);
    try {
      const action = await api<CodexAuthAction>("/codex-auth/logout", { method: "POST", body: "{}" });
      setToast(action.message);
      setSettingsOpen(false);
      setProfileOpen(false);
      setCodexAccount(null);
      await refresh();
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
    finally { setCodexAuthBusy(false); }
  }

  async function saveProfile(event: FormEvent) {
    event.preventDefault();
    setProfileSaving(true);
    try {
      const saved = await api<UserProfile>("/profile", { method: "PATCH", body: JSON.stringify(profileDraft) });
      setProfile(saved); setProfileDraft(saved); setToast("Profile saved"); setProfileOpen(false);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
    finally { setProfileSaving(false); }
  }

  async function renameProject(project: Project, name: string) {
    const renamed = await api<Project>(`/projects/${encodeURIComponent(project.id)}`, { method: "PATCH", body: JSON.stringify({ name }) });
    setProjects((current) => current.map((item) => item.id === renamed.id ? renamed : item));
    await refresh();
  }

  async function renameThread() {
    if (!activeThread) return;
    const title = window.prompt("Thread name", activeThread.title)?.trim();
    if (!title || title === activeThread.title) return;
    try {
      await api<ThreadSummary>(`/threads/${encodeURIComponent(activeThread.id)}`, { method: "PATCH", body: JSON.stringify({ title }) });
      await Promise.all([refresh(), refreshAppServer()]);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  async function activateProject(id: string, goalId: string | null = null) {
    try {
      await api<Project>(`/projects/${encodeURIComponent(id)}/activate`, { method: "POST" });
      setSelectedGoal(null); setThreadId(null); setFreshChat(Boolean(goalId)); setPendingImages([]); setPendingFiles([]);
      await refresh();
      if (goalId) selectGoal(goalId);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  const activeProject = projects.find((project) => project.active) || null;
  const projectGoalsByRecency = useMemo(
    () => new Map(projects.map((project) => [project.id, orderedGoals(project, appThreads)])),
    [appThreads, projects],
  );
  const knownGoalIds = new Set(goals.map((item) => item.goal_id));
  const unassignedThreads = appThreads.filter((thread) => !thread.goalId || !knownGoalIds.has(thread.goalId));
  const selectedGoalThreads = selectedGoal
    ? appThreads.filter((thread) => thread.goalId === selectedGoal)
    : [];

  useEffect(() => {
    if (!window.desktop?.onMenuCommand) return undefined;
    return window.desktop.onMenuCommand((command) => {
      if (command === "add-project") void chooseProject();
      else if (command === "new-goal") setGoalDialog(true);
      else if (command === "rename-project") {
        if (activeProject) setRenamingProject(activeProject);
        else setToast("Choose a project before renaming");
      } else if (command === "new-chat") selectThread("");
      else if (command === "focus-prompt") promptRef.current?.focus();
    });
  });

  return (
    <>
      <div className="app-shell">
        <aside className={`sidebar ${window.desktop ? "desktop-sidebar" : ""}`}>
          <div className="brand"><img className="brand-mark" src={brandIcon} alt="" aria-hidden="true" /><div><strong>Golazo</strong><span>Gooooo(aaa)llllllllllll</span></div></div>
          <div className="projects-heading"><span>Projects</span><button onClick={() => void chooseProject()} aria-label="Add project" title="Add project">＋</button></div>
          <nav className="project-tree" aria-label="Projects and goals">
            {projects.map((project) => {
              const collapsed = collapsedProjects.includes(project.id);
              return <section className={`project-block ${collapsed ? "collapsed" : ""}`} key={project.id}>
                <div className="project-header-row">
                  <button type="button" className="project-collapse-button" onClick={() => toggleProjectCollapsed(project.id)} aria-expanded={!collapsed} aria-controls={`project-goals-${project.id}`} aria-label={`${collapsed ? "Expand" : "Collapse"} ${project.name}`} title={`${collapsed ? "Expand" : "Collapse"} project`}><i aria-hidden="true" /></button>
                  <button
                    className={`project-header ${project.active ? "active" : ""}`}
                    onClick={() => !project.active && void activateProject(project.id)}
                    title={project.path}
                  >
                    <span className="project-folder" aria-hidden="true" />
                    <strong>{project.name}</strong>
                    {!project.exists && <span className="project-warning" title="Folder is unavailable">!</span>}
                  </button>
                  <button type="button" className="rename-icon-button" onClick={() => setRenamingProject(project)} aria-label={`Rename ${project.name}`} title="Rename project">✎</button>
                </div>
                {!collapsed && <div className="project-goals" id={`project-goals-${project.id}`}>
                  {project.active && <button className="project-new-goal" onClick={() => setGoalDialog(true)}><span>＋</span> New goal</button>}
                  {(projectGoalsByRecency.get(project.id) || project.goals).map((item) => (
                    <div className="goal-tree-item" key={item.goal_id}>
                      <button
                        className={`project-goal ${project.active && item.goal_id === selectedGoal ? "active" : ""}`}
                        onClick={() => project.active ? selectGoal(item.goal_id) : void activateProject(project.id, item.goal_id)}
                        title={`${item.features} features · ${item.progress.completion_rate}% complete`}
                      >
                        <span>{item.title}</span><em>{item.progress.completion_rate}%</em>
                      </button>
                      {project.active && (
                        <div className="goal-threads">
                          {appThreads.filter((thread) => thread.goalId === item.goal_id).map((thread) => (
                            <button
                              type="button"
                              className={`goal-thread ${thread.id === threadId ? "active" : ""}`}
                              key={thread.id}
                              onClick={() => void selectThread(thread.id)}
                              title={threadLabel(thread)}
                            >
                              <span aria-hidden="true">↳</span><strong>{threadLabel(thread)}</strong>
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                  {!project.goals.length && !project.active && <div className="project-empty">No goals</div>}
                </div>}
              </section>;
            })}
            {!projects.length && <div className="empty-small">Add a local folder to begin.</div>}
          </nav>
          <div className="sidebar-footer connection-footer" title={workspace?.path || "Local workspace"}>
            <span className={`status-dot ${connected ? "" : "offline"}`} /><div><strong>{activeProject?.name || workspace?.name || "Local workspace"}</strong><small>{connected ? (window.desktop ? "Desktop backend connected" : "API connected") : "Connecting…"}</small></div>
          </div>
        </aside>

        <main className="workspace">
          <header className={`topbar ${window.desktop ? "desktop-drag-region" : ""}`}>
            <div><p className="eyebrow">Active goal</p><h1>{goal?.title || "Choose a goal"}</h1></div>
            <div className="topbar-metrics">
              <div className="token-strip" aria-label="Token usage">
                <div><span>Total tokens</span><strong>{number(usage.total_tokens)}</strong></div>
                <div><span>Input</span><strong>{number(usage.input_tokens)}</strong></div>
                <div><span>Output</span><strong>{number(usage.output_tokens)}</strong></div>
                <div className="cache-stat"><span>Cached</span><strong>{number(usage.cached_input_tokens)}</strong></div>
              </div>
              <section className="codex-usage" aria-label="Codex usage remaining">
                <div className="codex-usage-heading"><span><i /> Codex left</span>{codexLimitSnapshot?.planType && <em>{codexLimitSnapshot.planType}</em>}</div>
                {codexLimitWindows.length ? <div className="codex-usage-windows">
                  {codexLimitWindows.map((item) => {
                    const remaining = remainingPercent(item.window);
                    return <div className="codex-usage-window" key={item.key} title={resetLabel(item.window.resetsAt)}>
                      <span>{item.label}</span><strong>{remaining}%</strong><i><b style={{ width: `${remaining}%` }} /></i>
                    </div>;
                  })}
                </div> : <div className="codex-usage-unavailable"><strong>—</strong><span>Unavailable</span></div>}
              </section>
              <div className="profile-control" ref={profileRef}>
                <button type="button" className="profile-button" onClick={() => { setProfileDraft(profile); setProfileOpen((open) => !open); }} aria-expanded={profileOpen} aria-controls="profile-popover" title="Profile">
                  <span className="profile-avatar">{profileInitials(profileIdentity)}</span>
                  <span><strong>{profileName}</strong><small>{profile.role || codexAccount?.planType || "Profile"}</small></span>
                </button>
                {profileOpen && (
                  <form id="profile-popover" className="profile-popover" onSubmit={saveProfile}>
                    <div className="profile-heading">
                      <span className="profile-avatar large">{profileInitials(profileIdentity)}</span>
                      <div><strong>{profileName}</strong><small>{codexAccount?.email || (codexAccount?.type === "apiKey" ? "API key account" : "Local profile")}</small></div>
                      {(codexAccount?.planType || codexLimitSnapshot?.planType) && <em>{codexAccount?.planType || codexLimitSnapshot?.planType}</em>}
                    </div>
                    <div className="profile-account-row"><span>Codex account</span><strong>{codexAccount ? (codexAccount.type === "chatgpt" ? "ChatGPT" : "API key") : codexAuth.authenticated ? "Connected" : "Not connected"}</strong></div>
                    <label>Display name<input autoFocus value={profileDraft.display_name} onChange={(event) => setProfileDraft({ ...profileDraft, display_name: event.target.value })} maxLength={80} placeholder={codexAccount?.email?.split("@")[0] || "Your name"} /></label>
                    <label>Role or title<input value={profileDraft.role} onChange={(event) => setProfileDraft({ ...profileDraft, role: event.target.value })} maxLength={120} placeholder="Builder, designer, founder…" /></label>
                    <div className="profile-account-actions">
                      <button type="button" onClick={() => void refreshCodexAccount()}>Refresh account</button>
                      {codexAuth.authenticated && <button type="button" onClick={() => void disconnectCodex()} disabled={codexAuthBusy}>Sign out</button>}
                    </div>
                    <button type="submit" className="profile-save" disabled={profileSaving}>{profileSaving ? "Saving…" : "Save profile"}</button>
                  </form>
                )}
              </div>
            </div>
          </header>

          <div className="content-grid">
            <section className="chat-panel" aria-label="Codex conversation">
              <div className="chat-header">
                <div className="thread-controls">
                  <button
                    type="button"
                    className={`live-pill sync-pill ${appServerConnected ? "" : "offline"}`}
                    onClick={() => void refreshAppServer(true)}
                    disabled={conversationSyncing}
                    title={conversationSyncedAt ? `Last synchronized ${new Date(conversationSyncedAt).toLocaleTimeString()}` : "Synchronize conversations with Codex"}
                  ><i /> {conversationSyncing ? "Syncing…" : appServerConnected ? "Codex synced" : "Codex offline"}</button>
                  <select value={threadId || ""} onChange={(event) => void selectThread(event.target.value)} aria-label="Thread">
                    <option value="">New thread</option>
                    {selectedGoal
                      ? selectedGoalThreads.map((thread) => (
                        <option key={thread.id} value={thread.id}>{threadLabel(thread)}</option>
                      ))
                      : <>
                        {goals.map((item) => {
                          const goalThreads = appThreads.filter((thread) => thread.goalId === item.goal_id);
                          return goalThreads.length ? (
                            <optgroup key={item.goal_id} label={item.title}>
                              {goalThreads.map((thread) => <option key={thread.id} value={thread.id}>{threadLabel(thread)}</option>)}
                            </optgroup>
                          ) : null;
                        })}
                        {unassignedThreads.length > 0 && (
                          <optgroup label="Unassigned threads">
                            {unassignedThreads.map((thread) => <option key={thread.id} value={thread.id}>{threadLabel(thread)}</option>)}
                          </optgroup>
                        )}
                      </>}
                  </select>
                  {activeThread && <button className="icon-button" onClick={() => void renameThread()} aria-label="Rename thread" title="Rename thread">✎</button>}
                </div>
                <button className="quiet-button" onClick={() => void selectThread("")}>New chat</button>
              </div>
              <div className="messages-frame">
              <div ref={messagesRef} className="messages" aria-live="polite" onScroll={updateMessageScrollState}>
                {restoredTurns.length ? restoredTurns.map((turn) => (
                  <div className="history-turn" key={turn.id}>
                    {turn.messages.filter((message) => message.role === "user").map((message) => (
                      <article className="message user" key={message.id}>
                        <div className="message-meta"><span className="avatar">Y</span><span>You · {message.status}</span></div>
                        <div className="bubble">
                          {message.images?.length ? <div className="message-images">{message.images.map(storedImageSource).filter((source): source is string => Boolean(source)).map((source) => <img src={source} alt="Pasted attachment" key={source} />)}</div> : null}
                          {message.text && <span>{message.text}</span>}
                        </div>
                      </article>
                    ))}
                    {turn.working.length > 0 && (
                      <details className="working-updates">
                        <summary><span>Working updates</span><em>{turn.working.length}</em></summary>
                        <div className="working-update-list">
                          {turn.working.map((message) => <p key={message.id}>{message.text}</p>)}
                        </div>
                      </details>
                    )}
                    {turn.messages.filter((message) => message.role === "assistant").map((message) => (
                      <article className="message assistant" key={message.id}>
                        <div className="message-meta"><span className="avatar">C</span><span>Codex · {message.status}</span></div>
                        <div className="bubble">{message.text}</div>
                      </article>
                    ))}
                  </div>
                )) : chatRuns.length ? chatRuns.map((run) => (
                  <div key={run.id}>
                    <article className="message user"><div className="message-meta"><span className="avatar">Y</span><span>You · {time(run.created_at)}</span></div><div className="bubble">{run.images?.length ? <div className="message-images">{run.images.map((image) => <img src={`${API_BASE}/runs/${encodeURIComponent(run.id)}/images/${encodeURIComponent(image.id)}`} alt={image.name} key={image.id} />)}</div> : null}{run.files?.length ? <div className="message-files">{run.files.map((file) => <span key={file.id}>▤ {file.name}</span>)}</div> : null}{run.prompt && <span>{run.prompt}</span>}</div></article>
                    <article className="message assistant"><div className="message-meta"><span className="avatar">C</span><span>Codex · {run.status}</span></div>
                      {run.status === "queued" || run.status === "running" ? <div className="bubble"><span className="running-line"><i className="spinner" />Codex is working · {run.events.length} events</span></div>
                        : run.status === "failed" ? <div className="bubble error-bubble">{run.error || "The run failed."}</div>
                          : <><div className="bubble">{run.final_message || "Completed without a final message."}</div><div className="usage-chip"><b>{number(run.usage.total_tokens)} tokens</b><span>{number(run.usage.input_tokens)} in</span><span>{number(run.usage.output_tokens)} out</span><span>{number(run.usage.cached_input_tokens)} cached</span><span>{number(run.usage.reasoning_output_tokens)} reasoning</span></div></>}
                    </article>
                  </div>
                )) : <div className="empty-chat"><div className="empty-orbit"><span>✦</span></div><h2>From intent to implementation.</h2><p>Shape the goal with Codex, review the plan, and build it one deliberate slice at a time.</p></div>}
                {approvals.map((approval) => (
                  <section className="approval-request" key={approval.id}>
                    <div><strong>Approval required</strong><span>{approval.method.includes("fileChange") ? "Apply proposed file changes" : approval.method.includes("permissions") ? "Grant additional permissions" : "Run requested command"}</span></div>
                    <div><button onClick={() => void decideApproval(approval.id, "decline")}>Deny</button><button onClick={() => void decideApproval(approval.id, "accept")}>Allow</button><button onClick={() => void decideApproval(approval.id, "acceptForSession")}>Always allow</button></div>
                  </section>
                ))}
                {liveWorking && <details className="working-updates live-working"><summary><span>Working updates</span><em>Live</em></summary><div className="working-update-list"><p>{liveWorking}</p></div></details>}
                {liveAssistant && <article className="message assistant live-message"><div className="message-meta"><span className="avatar">C</span><span>Codex · streaming</span></div><div className="bubble">{liveAssistant}<i className="stream-caret" /></div></article>}
              </div>
              {showScrollToBottom && (
                <button type="button" className="scroll-to-bottom" onClick={() => scrollToLatestMessage()} aria-label="Scroll to latest message" title="Scroll to latest message">
                  <svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 8 5 5 5-5" /></svg>
                </button>
              )}
              </div>
              <form className="composer" onSubmit={send}>
                {(pendingImages.length > 0 || pendingFiles.length > 0) && <div className="attachment-tray" aria-label="Attachments">{pendingImages.map((image) => <figure key={image.id}><img src={image.preview} alt={image.name} /><button type="button" onClick={() => setPendingImages((current) => current.filter((item) => item.id !== image.id))} aria-label={`Remove ${image.name}`} title={`Remove ${image.name}`}>×</button><figcaption>{image.name}</figcaption></figure>)}{pendingFiles.map((file) => <figure className="attachment-file" key={file.id}><div aria-hidden="true">▤<small>{file.name.split(".").pop()?.slice(0, 5) || "FILE"}</small></div><button type="button" onClick={() => setPendingFiles((current) => current.filter((item) => item.id !== file.id))} aria-label={`Remove ${file.name}`} title={`Remove ${file.name}`}>×</button><figcaption>{file.name}</figcaption></figure>)}</div>}
                <textarea ref={promptRef} rows={1} value={prompt} onChange={(event) => setPrompt(event.target.value)} onPaste={(event) => void pasteImages(event)} placeholder={selectedGoal ? (workMode === "spec" ? "Continue evolving the implementation document…" : "Ask Codex to implement one coherent tracker slice…") : "Create or select a goal first…"} aria-label={workMode === "spec" ? "Spec mode prompt" : "Build mode prompt"} required={!pendingImages.length && !pendingFiles.length} />
                <div className="composer-footer">
                  <div className="composer-options">
                    <div className="mode-toggle" role="group" aria-label="Goal work mode">
                      <button type="button" className={workMode === "spec" ? "active" : ""} onClick={() => void changeWorkMode("spec")} aria-pressed={workMode === "spec"} title="Continue evolving the implementation document without changing product files">Spec</button>
                      <button type="button" className={workMode === "build" ? "active" : ""} onClick={() => void changeWorkMode("build")} aria-pressed={workMode === "build"} title="Allow code changes only when your prompt explicitly requests them">Build</button>
                    </div>
                    <span className="mode-hint">{workMode === "build" ? "Changes only when your prompt asks" : "Evolve implementation document"}</span>
                    <input
                      ref={attachmentInputRef}
                      className="attachment-input"
                      type="file"
                      multiple
                      tabIndex={-1}
                      aria-hidden="true"
                      onChange={(event) => {
                        const files = Array.from(event.currentTarget.files || []);
                        event.currentTarget.value = "";
                        void addSelectedAttachments(files);
                      }}
                    />
                    <button
                      type="button"
                      className="attachment-button"
                      onClick={() => attachmentInputRef.current?.click()}
                      disabled={!selectedGoal || busy || (pendingImages.length >= 4 && pendingFiles.length >= 8)}
                      aria-label="Upload files or images"
                      title="Upload files or images"
                    >
                      <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M7.5 10.8 12.9 5.4a3 3 0 0 1 4.2 4.2l-7 7a4.5 4.5 0 0 1-6.4-6.3l7.1-7.1a2 2 0 0 1 2.8 2.8l-7 7a.7.7 0 0 1-1-1l6.5-6.5" /></svg>
                    </button>
                    <div className="composer-settings" ref={settingsRef}>
                      <button type="button" className="settings-pill" onClick={() => { setPermissionsOpen(false); setSettingsPanel("main"); setSettingsOpen((open) => !open); }} aria-expanded={settingsOpen} aria-controls="composer-settings-popover">
                        <span>{settingsSummary}</span><i className="pill-chevron" aria-hidden="true" />
                      </button>
                      {settingsOpen && (
                        <section id="composer-settings-popover" className="settings-popover" aria-label={threadId ? "Thread LLM configuration" : "Default LLM configuration"}>
                          {settingsPanel === "main" && (
                            <>
                              <div className="settings-row codex-settings-row">
                                <div><span>Codex account</span><strong>{codexAuth.authenticated ? `Connected${codexAuth.method ? ` · ${codexAuth.method}` : ""}` : codexAuth.available ? "Not connected" : "Codex unavailable"}</strong><small title={codexAuth.executable}>{codexAuth.message}</small></div>
                                <div className="settings-actions">
                                  <button type="button" onClick={() => void refresh()} disabled={codexAuthBusy}>Refresh</button>
                                  {codexAuth.authenticated
                                    ? <button type="button" onClick={() => void disconnectCodex()} disabled={codexAuthBusy}>Disconnect</button>
                                    : <button type="button" onClick={() => void connectCodex()} disabled={codexAuthBusy || !codexAuth.available}>{codexAuthBusy ? "Opening…" : "Connect"}</button>}
                                </div>
                              </div>
                              {!usingCodexAccount && (
                                <label className="settings-row">
                                  <span>Provider</span>
                                  <select value={llmConfig.provider} disabled={Boolean(threadId)} onChange={(event) => setLLMConfig({ ...llmConfig, provider: event.target.value as LLMProvider })}><option value="openai">OpenAI</option><option value="ollama">Ollama</option><option value="lmstudio">LM Studio</option></select>
                                </label>
                              )}
                              <button type="button" className="settings-row picker-row" onClick={() => setSettingsPanel("model")}><span>Model</span><strong>{modelLabel}</strong><i aria-hidden="true">›</i></button>
                              <button type="button" className="settings-row picker-row" onClick={() => setSettingsPanel("effort")}><span>Effort</span><strong>{effortLabel}</strong><i aria-hidden="true">›</i></button>
                              <button type="button" className="settings-row picker-row speed-row" onClick={() => setSettingsPanel("speed")}><span>Speed</span><strong>{speedLabel}</strong><small>{llmConfig.speed === "fast" ? "1.5x speed, more usage" : "Default speed"}</small><i aria-hidden="true">›</i></button>
                              <button type="button" className="settings-save" onClick={() => { void saveLLMConfig(); setSettingsOpen(false); setSettingsPanel("main"); }}>{threadId ? "Save thread settings" : "Save default settings"}</button>
                            </>
                          )}
                          {settingsPanel === "model" && (
                            <div className="picker-panel">
                              <button type="button" className="picker-back" onClick={() => setSettingsPanel("main")}>Model</button>
                              {modelOptions.map((option) => <button type="button" className="picker-option" key={option.value} onClick={() => { setLLMConfig({ ...llmConfig, model: option.value }); setSettingsPanel("main"); }}><span>{option.label}</span>{llmConfig.model === option.value && <i aria-hidden="true">✓</i>}</button>)}
                            </div>
                          )}
                          {settingsPanel === "effort" && (
                            <div className="picker-panel effort-panel">
                              <button type="button" className="picker-back" onClick={() => setSettingsPanel("main")}>Effort</button>
                              {effortOptions.map((option) => <button type="button" className="picker-option" key={option.value} onClick={() => { setLLMConfig({ ...llmConfig, reasoning: option.value }); setSettingsPanel("main"); }}><span>{option.label}</span>{llmConfig.reasoning === option.value && <i aria-hidden="true">✓</i>}</button>)}
                            </div>
                          )}
                          {settingsPanel === "speed" && (
                            <div className="picker-panel speed-panel">
                              <button type="button" className="picker-back" onClick={() => setSettingsPanel("main")}>Speed</button>
                              {speedOptions.map((option) => <button type="button" className="picker-option with-note" key={option.value} onClick={() => { setLLMConfig({ ...llmConfig, speed: option.value }); setSettingsPanel("main"); }}><span>{option.label}<small>{option.note}</small></span>{llmConfig.speed === option.value && <i aria-hidden="true">✓</i>}</button>)}
                            </div>
                          )}
                        </section>
                      )}
                    </div>
                    <div className="permission-picker" ref={permissionsRef}>
                      <button
                        type="button"
                        className={`permission-trigger ${permissionMode === "full-access" ? "danger" : ""}`}
                        onClick={() => { setSettingsOpen(false); setPermissionsOpen((open) => !open); }}
                        aria-expanded={permissionsOpen}
                        aria-controls="permissions-popover"
                      >
                        <PermissionIcon mode={permissionMode} />
                        <span>{selectedPermission.label}</span>
                        <i className="permission-chevron" aria-hidden="true" />
                      </button>
                      {permissionsOpen && (
                        <section id="permissions-popover" className="permissions-popover" aria-label="Codex permissions">
                          <header><span>How should Codex actions be approved?</span></header>
                          {permissionOptions.map((option) => (
                            <button
                              type="button"
                              className={`permission-option ${option.value === "full-access" ? "danger" : ""}`}
                              onClick={() => { setPermissionMode(option.value); setPermissionsOpen(false); }}
                              aria-pressed={permissionMode === option.value}
                              key={option.value}
                            >
                              <PermissionIcon mode={option.value} />
                              <span><strong>{option.label}</strong><small>{option.description}</small></span>
                              {permissionMode === option.value && <b aria-label="Selected">✓</b>}
                            </button>
                          ))}
                        </section>
                      )}
                    </div>
                    <span className="shortcut">⌘ ↵ to send</span>
                  </div>
                  <button type="submit" className="send-button" disabled={!selectedGoal || busy || (!prompt.trim() && !pendingImages.length && !pendingFiles.length)} aria-label="Send prompt">↑</button>
                </div>
              </form>
            </section>

            <aside className="inspector">
              <section className="progress-card"><div className="card-heading"><span>Goal progress</span><strong>{goal?.progress.completion_rate || 0}%</strong></div><div className="progress-track"><i style={{ width: `${goal?.progress.completion_rate || 0}%` }} /></div><p>{goal?.progress.total_steps ? `${goal.progress.completed_steps} of ${goal.progress.total_steps} steps complete` : "No implementation steps yet"}</p></section>
              <section className="detail-section"><div className="section-heading"><h2>Features</h2><span>{goal?.features.length || 0}</span></div><div className="feature-list">{goal?.features.length ? goal.features.map((feature) => <FeatureCard feature={feature} slices={(goal.slices || []).filter((slice) => slice.feature_id === feature.id)} key={feature.id} />) : <div className="empty-small">Features added by the skill will appear here.</div>}</div></section>
              <section className="detail-section activity-section"><div className="section-heading"><h2>Run activity</h2><span>{runs.filter((run) => !selectedGoal || run.goal_id === selectedGoal).length}</span></div><div className="activity-list">{runs.filter((run) => !selectedGoal || run.goal_id === selectedGoal).slice(0, 8).map((run) => <div className={`activity ${run.status}`} key={run.id}><i /><div><strong>{run.prompt}</strong><small>{run.events.length} events · {number(run.usage.total_tokens)} tokens</small></div><span>{time(run.created_at)}</span></div>)}</div></section>
            </aside>
          </div>
        </main>
      </div>

      {goalDialog && workspace && <GoalDialog workspace={workspace} onClose={() => setGoalDialog(false)} onCreated={(created) => { setSelectedGoal(created.goal_id); setThreadId(null); setFreshChat(true); setGoalDialog(false); void refresh(); }} />}
      {workspaceDialog && workspace && <WorkspaceDialog initialPath={workspace.path} onClose={() => setWorkspaceDialog(false)} onSelect={addProject} />}
      {renamingProject && <ProjectRenameDialog project={renamingProject} onClose={() => setRenamingProject(null)} onRename={renameProject} />}
      <div className={`toast ${toast ? "show" : ""}`} role="status">{toast}</div>
    </>
  );
}
