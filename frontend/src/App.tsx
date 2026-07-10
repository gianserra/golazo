import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

const brandIcon = new URL("../../electron/assets/icon.png", import.meta.url).href;

type Progress = { completed_steps: number; total_steps: number; completion_rate: number };
type Feature = {
  id: string;
  title: string;
  status: "Planned" | "Blocked" | "Partial" | "Done";
  progress: Progress;
  slice_count: number;
};
type Goal = { goal_id: string; title: string; features: Feature[]; progress: Progress };
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
  goal_id: string | null;
  thread_id: string | null;
  resumed_from: string | null;
  status: "queued" | "running" | "completed" | "failed";
  created_at: string;
  final_message: string | null;
  error: string | null;
  usage: Usage;
  events: unknown[];
};
type Workspace = { path: string; name: string; browse_root: string; project_id: string };
type ProjectGoal = { goal_id: string; title: string; progress: Progress; features: number };
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
type ReasoningLevel = "none" | "minimal" | "low" | "medium" | "high" | "xhigh";
type LLMConfig = { provider: LLMProvider; model: string; reasoning: ReasoningLevel };
type ThreadSummary = {
  id: string;
  title: string;
  llm_config: LLMConfig;
  goal_id: string | null;
  run_count: number;
  created_at: string;
  updated_at: string;
};

const emptyUsage: Usage = {
  input_tokens: 0,
  cached_input_tokens: 0,
  output_tokens: 0,
  reasoning_output_tokens: 0,
  total_tokens: 0,
};
const fallbackLLMConfig: LLMConfig = { provider: "openai", model: "", reasoning: "medium" };
const API_BASE = import.meta.env.DEV ? "/api" : "";

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

const number = (value = 0) => new Intl.NumberFormat().format(value);
const time = (value: string) => new Date(value).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

function GoalDialog({ onClose, onCreated }: { onClose: () => void; onCreated: (goal: Goal) => void }) {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [saving, setSaving] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault();
    setSaving(true);
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

  return (
    <div className="modal-layer" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
      <section className="react-dialog" role="dialog" aria-modal="true" aria-labelledby="goal-dialog-title">
        <form onSubmit={submit}>
          <div className="dialog-heading">
            <div><p className="eyebrow">Start tracking</p><h2 id="goal-dialog-title">Create a goal</h2></div>
            <button type="button" onClick={onClose} aria-label="Close">×</button>
          </div>
          <label>Title<input autoFocus value={title} onChange={(event) => setTitle(event.target.value)} placeholder="Ship the first release" required /></label>
          <label>Description<textarea value={description} onChange={(event) => setDescription(event.target.value)} rows={3} placeholder="What does success look like?" /></label>
          <p className="generated-id-note">A stable ID will be generated from the title with a unique eight-character hash.</p>
          <div className="dialog-actions">
            <button type="button" className="quiet-button" onClick={onClose}>Cancel</button>
            <button type="submit" className="primary-button" disabled={saving}>{saving ? "Creating…" : "Create goal"}</button>
          </div>
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
  const [threads, setThreads] = useState<ThreadSummary[]>([]);
  const [defaultLLMConfig, setDefaultLLMConfig] = useState<LLMConfig>(fallbackLLMConfig);
  const [llmConfig, setLLMConfig] = useState<LLMConfig>(fallbackLLMConfig);
  const [selectedGoal, setSelectedGoal] = useState<string | null>(null);
  const [threadId, setThreadId] = useState<string | null>(null);
  const [pendingRunId, setPendingRunId] = useState<string | null>(null);
  const [freshChat, setFreshChat] = useState(false);
  const [prompt, setPrompt] = useState("");
  const [sandbox, setSandbox] = useState("workspace-write");
  const [goalDialog, setGoalDialog] = useState(false);
  const [workspaceDialog, setWorkspaceDialog] = useState(false);
  const [renamingProject, setRenamingProject] = useState<Project | null>(null);
  const [connected, setConnected] = useState(false);
  const [toast, setToast] = useState("");
  const promptRef = useRef<HTMLTextAreaElement | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [nextGoals, nextRuns, nextUsage, nextWorkspace, nextProjects, nextThreads, nextDefaultLLMConfig] = await Promise.all([
        api<Goal[]>("/goals"), api<Run[]>("/runs"), api<Usage>("/usage"), api<Workspace>("/workspace"), api<Project[]>("/projects"),
        api<ThreadSummary[]>("/threads"), api<LLMConfig>("/settings/default-llm"),
      ]);
      setGoals(nextGoals); setRuns(nextRuns); setUsage(nextUsage); setWorkspace(nextWorkspace); setProjects(nextProjects);
      setThreads(nextThreads); setDefaultLLMConfig(nextDefaultLLMConfig); setConnected(true);
      setSelectedGoal((current) => current && nextGoals.some((goal) => goal.goal_id === current) ? current : nextGoals[0]?.goal_id || null);
    } catch (error) {
      setConnected(false); setToast(error instanceof Error ? error.message : String(error));
    }
  }, []);

  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 1000);
    return () => window.clearInterval(timer);
  }, [refresh]);

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

  const goal = goals.find((item) => item.goal_id === selectedGoal) || null;
  const activeThread = threads.find((item) => item.id === threadId) || null;

  useEffect(() => {
    setLLMConfig(activeThread?.llm_config || defaultLLMConfig);
  }, [threadId, activeThread?.updated_at, defaultLLMConfig.provider, defaultLLMConfig.model, defaultLLMConfig.reasoning]);
  const chatRuns = useMemo(() => {
    if (freshChat) return [];
    const relevant = runs.filter((run) => run.goal_id === selectedGoal);
    if (!threadId) return relevant.filter((run) => !run.resumed_from).slice(0, 1).reverse();
    return relevant.filter((run) => run.thread_id === threadId || run.resumed_from === threadId).reverse();
  }, [freshChat, runs, selectedGoal, threadId]);
  const busy = chatRuns.some((run) => run.status === "queued" || run.status === "running");

  async function send(event: FormEvent) {
    event.preventDefault();
    const text = prompt.trim();
    if (!text || !selectedGoal || busy) return;
    try {
      const created = await api<Run>("/runs", {
        method: "POST",
        body: JSON.stringify({ prompt: text, goal_id: selectedGoal, thread_id: threadId, sandbox, llm_config: llmConfig }),
      });
      setPrompt(""); setFreshChat(false);
      if (!threadId) setPendingRunId(created.id);
      await refresh();
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  async function addProject(path: string) {
    await api<Project>("/projects", { method: "POST", body: JSON.stringify({ path, activate: true }) });
    setSelectedGoal(null); setThreadId(null); setFreshChat(true); setWorkspaceDialog(false); await refresh();
  }

  async function chooseProject() {
    try {
      if (window.desktop) {
        const path = await window.desktop.selectDirectory(workspace?.path);
        if (path) await addProject(path);
      } else setWorkspaceDialog(true);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  function selectGoal(id: string) { setSelectedGoal(id); setThreadId(null); setFreshChat(false); }

  function selectThread(id: string) {
    if (!id) { setThreadId(null); setFreshChat(true); return; }
    const thread = threads.find((item) => item.id === id);
    setThreadId(id); setFreshChat(false);
    if (thread?.goal_id) setSelectedGoal(thread.goal_id);
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
      await refresh();
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  async function activateProject(id: string, goalId: string | null = null) {
    try {
      await api<Project>(`/projects/${encodeURIComponent(id)}/activate`, { method: "POST" });
      setSelectedGoal(null); setThreadId(null); setFreshChat(Boolean(goalId));
      await refresh();
      if (goalId) selectGoal(goalId);
    } catch (error) { setToast(error instanceof Error ? error.message : String(error)); }
  }

  const activeProject = projects.find((project) => project.active) || null;

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
          <div className="brand"><img className="brand-mark" src={brandIcon} alt="" aria-hidden="true" /><div><strong>Golazo</strong><span>LLM Projects</span></div></div>
          <div className="projects-heading"><span>Projects</span><button onClick={() => void chooseProject()} aria-label="Add project" title="Add project">＋</button></div>
          <nav className="project-tree" aria-label="Projects and goals">
            {projects.map((project) => (
              <section className="project-block" key={project.id}>
                <div className="project-header-row">
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
                <div className="project-goals">
                  {project.active && <button className="project-new-goal" onClick={() => setGoalDialog(true)}><span>＋</span> New goal</button>}
                  {project.goals.map((item) => (
                    <button
                      key={item.goal_id}
                      className={`project-goal ${project.active && item.goal_id === selectedGoal ? "active" : ""}`}
                      onClick={() => project.active ? selectGoal(item.goal_id) : void activateProject(project.id, item.goal_id)}
                      title={`${item.features} features · ${item.progress.completion_rate}% complete`}
                    >
                      <span>{item.title}</span><em>{item.progress.completion_rate}%</em>
                    </button>
                  ))}
                  {!project.goals.length && !project.active && <div className="project-empty">No goals</div>}
                </div>
              </section>
            ))}
            {!projects.length && <div className="empty-small">Add a local folder to begin.</div>}
          </nav>
          <div className="sidebar-footer connection-footer" title={workspace?.path || "Local workspace"}>
            <span className={`status-dot ${connected ? "" : "offline"}`} /><div><strong>{activeProject?.name || workspace?.name || "Local workspace"}</strong><small>{connected ? (window.desktop ? "Desktop backend connected" : "API connected") : "Connecting…"}</small></div>
          </div>
        </aside>

        <main className="workspace">
          <header className={`topbar ${window.desktop ? "desktop-drag-region" : ""}`}>
            <div><p className="eyebrow">Active goal</p><h1>{goal?.title || "Choose a goal"}</h1></div>
            <div className="token-strip" aria-label="Token usage">
              <div><span>Total tokens</span><strong>{number(usage.total_tokens)}</strong></div>
              <div><span>Input</span><strong>{number(usage.input_tokens)}</strong></div>
              <div><span>Output</span><strong>{number(usage.output_tokens)}</strong></div>
              <div className="cache-stat"><span>Cached</span><strong>{number(usage.cached_input_tokens)}</strong></div>
            </div>
          </header>

          <div className="content-grid">
            <section className="chat-panel" aria-label="Codex conversation">
              <div className="chat-header">
                <div className="thread-controls">
                  <span className="live-pill"><i /> Codex CLI</span>
                  <select value={threadId || ""} onChange={(event) => selectThread(event.target.value)} aria-label="Thread">
                    <option value="">New thread</option>
                    {threads.map((thread) => <option key={thread.id} value={thread.id}>{thread.title}</option>)}
                  </select>
                  {activeThread && <button className="icon-button" onClick={() => void renameThread()} aria-label="Rename thread" title="Rename thread">✎</button>}
                </div>
                <button className="quiet-button" onClick={() => selectThread("")}>New chat</button>
              </div>
              <div className="messages" aria-live="polite">
                {chatRuns.length ? chatRuns.map((run) => (
                  <div key={run.id}>
                    <article className="message user"><div className="message-meta"><span className="avatar">Y</span><span>You · {time(run.created_at)}</span></div><div className="bubble">{run.prompt}</div></article>
                    <article className="message assistant"><div className="message-meta"><span className="avatar">C</span><span>Codex · {run.status}</span></div>
                      {run.status === "queued" || run.status === "running" ? <div className="bubble"><span className="running-line"><i className="spinner" />Codex is working · {run.events.length} events</span></div>
                        : run.status === "failed" ? <div className="bubble error-bubble">{run.error || "The run failed."}</div>
                          : <><div className="bubble">{run.final_message || "Completed without a final message."}</div><div className="usage-chip"><b>{number(run.usage.total_tokens)} tokens</b><span>{number(run.usage.input_tokens)} in</span><span>{number(run.usage.output_tokens)} out</span><span>{number(run.usage.cached_input_tokens)} cached</span><span>{number(run.usage.reasoning_output_tokens)} reasoning</span></div></>}
                    </article>
                  </div>
                )) : <div className="empty-chat"><div className="empty-orbit"><span>✦</span></div><h2>Turn a goal into working software</h2><p>Send the first implementation prompt. Follow-ups continue in the same Codex thread while usage is tracked turn by turn.</p></div>}
              </div>
              <form className="composer" onSubmit={send}>
                <textarea ref={promptRef} rows={1} value={prompt} onChange={(event) => setPrompt(event.target.value)} placeholder={selectedGoal ? (threadId ? "Send a follow-up prompt…" : "Ask Codex to implement the next slice…") : "Create or select a goal first…"} aria-label="Prompt" required />
                <div className="llm-config" aria-label={threadId ? "Thread LLM configuration" : "Default LLM configuration"}>
                  <label><span>Provider</span><select value={llmConfig.provider} disabled={Boolean(threadId)} onChange={(event) => setLLMConfig({ ...llmConfig, provider: event.target.value as LLMProvider })}><option value="openai">OpenAI</option><option value="ollama">Ollama</option><option value="lmstudio">LM Studio</option></select></label>
                  <label className="model-field"><span>Model</span><input value={llmConfig.model} onChange={(event) => setLLMConfig({ ...llmConfig, model: event.target.value })} placeholder="Use provider default" /></label>
                  <label><span>Reasoning</span><select value={llmConfig.reasoning} onChange={(event) => setLLMConfig({ ...llmConfig, reasoning: event.target.value as ReasoningLevel })}><option value="none">None</option><option value="minimal">Minimal</option><option value="low">Low</option><option value="medium">Medium</option><option value="high">High</option><option value="xhigh">Extra high</option></select></label>
                  <button type="button" className="config-save" onClick={() => void saveLLMConfig()}>{threadId ? "Save thread" : "Save default"}</button>
                </div>
                <div className="composer-footer"><div className="composer-options"><label><span>Sandbox</span><select value={sandbox} onChange={(event) => setSandbox(event.target.value)}><option value="workspace-write">Workspace write</option><option value="read-only">Read only</option></select></label><span className="shortcut">⌘ ↵ to send</span></div><button type="submit" className="send-button" disabled={!selectedGoal || busy} aria-label="Send prompt">↑</button></div>
              </form>
            </section>

            <aside className="inspector">
              <section className="progress-card"><div className="card-heading"><span>Goal progress</span><strong>{goal?.progress.completion_rate || 0}%</strong></div><div className="progress-track"><i style={{ width: `${goal?.progress.completion_rate || 0}%` }} /></div><p>{goal?.progress.total_steps ? `${goal.progress.completed_steps} of ${goal.progress.total_steps} steps complete` : "No implementation steps yet"}</p></section>
              <section className="detail-section"><div className="section-heading"><h2>Features</h2><span>{goal?.features.length || 0}</span></div><div className="feature-list">{goal?.features.length ? goal.features.map((feature) => <article className="feature" key={feature.id}><div className="feature-top"><strong>{feature.title}</strong><span className={`feature-status ${feature.status}`}>{feature.status}</span></div><div className="mini-track"><i style={{ width: `${feature.progress.completion_rate}%` }} /></div><small>{feature.progress.completed_steps}/{feature.progress.total_steps} steps · {feature.slice_count} slices</small></article>) : <div className="empty-small">Features added by the skill will appear here.</div>}</div></section>
              <section className="detail-section activity-section"><div className="section-heading"><h2>Run activity</h2><span>{runs.filter((run) => !selectedGoal || run.goal_id === selectedGoal).length}</span></div><div className="activity-list">{runs.filter((run) => !selectedGoal || run.goal_id === selectedGoal).slice(0, 8).map((run) => <div className={`activity ${run.status}`} key={run.id}><i /><div><strong>{run.prompt}</strong><small>{run.events.length} events · {number(run.usage.total_tokens)} tokens</small></div><span>{time(run.created_at)}</span></div>)}</div></section>
            </aside>
          </div>
        </main>
      </div>

      {goalDialog && <GoalDialog onClose={() => setGoalDialog(false)} onCreated={(created) => { setSelectedGoal(created.goal_id); setThreadId(null); setFreshChat(true); setGoalDialog(false); void refresh(); }} />}
      {workspaceDialog && workspace && <WorkspaceDialog initialPath={workspace.path} onClose={() => setWorkspaceDialog(false)} onSelect={addProject} />}
      {renamingProject && <ProjectRenameDialog project={renamingProject} onClose={() => setRenamingProject(null)} onRename={renameProject} />}
      <div className={`toast ${toast ? "show" : ""}`} role="status">{toast}</div>
    </>
  );
}
