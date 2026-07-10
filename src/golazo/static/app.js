const state = { goals: [], runs: [], workspace: null, browserPath: null, selectedGoal: null, threadId: null, freshChat: false, polling: null };
const $ = (id) => document.getElementById(id);
const formatNumber = (value) => new Intl.NumberFormat().format(value || 0);
const escapeHtml = (value = "") => String(value).replace(/[&<>'"]/g, (char) => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[char]));
const shortId = (value) => value ? `${value.slice(0, 8)}…` : "New conversation";
const timeLabel = (value) => value ? new Date(value).toLocaleTimeString([], {hour:"numeric", minute:"2-digit"}) : "now";

async function api(path, options = {}) {
  const response = await fetch(path, { headers: {"content-type":"application/json"}, ...options });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed (${response.status})`);
  }
  return response.json();
}

async function browseDirectory(path = null) {
  const params = new URLSearchParams();
  if (path) params.set("path", path);
  if ($("showHiddenDirectories").checked) params.set("show_hidden", "true");
  const listing = await api(`/filesystem?${params}`);
  state.browserPath = listing.path;
  $("currentDirectoryPath").textContent = listing.path;
  $("parentDirectoryButton").disabled = !listing.parent;
  $("parentDirectoryButton").dataset.path = listing.parent || "";
  $("directoryList").innerHTML = listing.directories.length ? listing.directories.map((directory) => `
    <button class="directory-row" data-directory="${escapeHtml(directory.path)}">
      <span class="folder-icon">▰</span><strong>${escapeHtml(directory.name)}</strong><small>${directory.symlink ? "linked folder" : "open"} →</small>
    </button>`).join("") : `<div class="empty-small">No subfolders in this directory.</div>`;
  document.querySelectorAll("[data-directory]").forEach((button) => button.addEventListener("click", () => browseDirectory(button.dataset.directory).catch((error) => toast(error.message))));
}

function toast(message) {
  $("toast").textContent = message;
  $("toast").classList.add("show");
  setTimeout(() => $("toast").classList.remove("show"), 3500);
}

function runsForCurrentChat() {
  if (state.freshChat) return [];
  const goalRuns = state.runs.filter((run) => run.goal_id === state.selectedGoal);
  if (!state.threadId) return goalRuns.filter((run) => !run.resumed_from).slice(0, 1).reverse();
  return goalRuns.filter((run) => run.thread_id === state.threadId || run.resumed_from === state.threadId).reverse();
}

function selectLatestThread() {
  if (state.freshChat || state.threadId || !state.selectedGoal) return;
  const latest = state.runs.find((run) => run.goal_id === state.selectedGoal && run.thread_id);
  if (latest) state.threadId = latest.thread_id;
}

function renderGoals() {
  $("goalList").innerHTML = state.goals.length ? state.goals.map((goal) => `
    <button class="goal-item ${goal.goal_id === state.selectedGoal ? "active" : ""}" data-goal="${escapeHtml(goal.goal_id)}">
      <strong>${escapeHtml(goal.title)}</strong><small>${goal.features.length} features</small><em>${goal.progress.completion_rate}%</em>
    </button>`).join("") : `<div class="empty-small">No goals yet. Create one to begin.</div>`;
  document.querySelectorAll("[data-goal]").forEach((button) => button.addEventListener("click", () => {
    state.selectedGoal = button.dataset.goal; state.threadId = null; state.freshChat = false;
    localStorage.setItem("selectedGoal", state.selectedGoal); selectLatestThread(); render();
  }));
}

function renderGoalDetail() {
  const goal = state.goals.find((item) => item.goal_id === state.selectedGoal);
  $("goalTitle").textContent = goal?.title || "Choose a goal";
  const progress = goal?.progress || {completion_rate:0, completed_steps:0, total_steps:0};
  $("progressPercent").textContent = `${progress.completion_rate}%`;
  $("progressBar").style.width = `${progress.completion_rate}%`;
  $("progressCaption").textContent = progress.total_steps ? `${progress.completed_steps} of ${progress.total_steps} steps complete` : "No implementation steps yet";
  $("featureCount").textContent = goal?.features.length || 0;
  $("featureList").innerHTML = goal?.features.length ? goal.features.map((feature) => `
    <article class="feature"><div class="feature-top"><strong>${escapeHtml(feature.title)}</strong><span class="feature-status ${feature.status}">${feature.status}</span></div>
      <div class="mini-track"><i style="width:${feature.progress.completion_rate}%"></i></div>
      <small>${feature.progress.completed_steps}/${feature.progress.total_steps} steps · ${feature.slice_count} slices</small></article>`).join("") : `<div class="empty-small">Features added by the skill will appear here.</div>`;
}

function renderUsage(usage) {
  $("totalTokens").textContent = formatNumber(usage.total_tokens);
  $("inputTokens").textContent = formatNumber(usage.input_tokens);
  $("outputTokens").textContent = formatNumber(usage.output_tokens);
  $("cachedTokens").textContent = formatNumber(usage.cached_input_tokens);
}

function messageHtml(run) {
  const usage = run.usage || {};
  const assistant = run.status === "running" || run.status === "queued"
    ? `<div class="bubble"><span class="running-line"><i class="spinner"></i>${run.status === "queued" ? "Queued" : "Codex is working"} · ${run.events.length} events</span></div>`
    : run.status === "failed"
      ? `<div class="bubble error-bubble">${escapeHtml(run.error || "The run failed.")}</div>`
      : `<div class="bubble">${escapeHtml(run.final_message || "Completed without a final message.")}</div>
         <div class="usage-chip"><b>${formatNumber(usage.total_tokens)} tokens</b><span>${formatNumber(usage.input_tokens)} in</span><span>${formatNumber(usage.output_tokens)} out</span><span>${formatNumber(usage.cached_input_tokens)} cached</span><span>${formatNumber(usage.reasoning_output_tokens)} reasoning</span></div>`;
  return `<article class="message user"><div class="message-meta"><span class="avatar">Y</span><span>You · ${timeLabel(run.created_at)}</span></div><div class="bubble">${escapeHtml(run.prompt)}</div></article>
    <article class="message assistant"><div class="message-meta"><span class="avatar">C</span><span>Codex · ${run.status}</span></div>${assistant}</article>`;
}

function renderChat() {
  const runs = runsForCurrentChat();
  $("threadLabel").textContent = state.threadId ? `Thread ${shortId(state.threadId)}` : "New conversation";
  $("messages").innerHTML = runs.length ? runs.map(messageHtml).join("") : `<div class="empty-chat"><div class="empty-orbit"><span>✦</span></div><h2>Turn a goal into working software</h2><p>Send the first implementation prompt. Follow-ups continue in the same Codex thread while usage is tracked turn by turn.</p></div>`;
  $("messages").scrollTop = $("messages").scrollHeight;
  const busy = runs.some((run) => ["queued","running"].includes(run.status));
  $("sendButton").disabled = busy || !state.selectedGoal;
  $("promptInput").placeholder = state.selectedGoal ? (state.threadId ? "Send a follow-up prompt…" : "Ask Codex to implement the next slice…") : "Create or select a goal first…";
}

function renderActivity() {
  const runs = state.runs.filter((run) => !state.selectedGoal || run.goal_id === state.selectedGoal).slice(0, 8);
  $("runCount").textContent = runs.length;
  $("activityList").innerHTML = runs.length ? runs.map((run) => `<div class="activity ${run.status}"><i></i><div><strong>${escapeHtml(run.prompt)}</strong><small>${run.events.length} events · ${formatNumber(run.usage?.total_tokens)} tokens</small></div><span>${timeLabel(run.created_at)}</span></div>`).join("") : `<div class="empty-small">No runs for this goal.</div>`;
}

function render() { renderGoals(); renderGoalDetail(); renderChat(); renderActivity(); }

async function refresh() {
  try {
    const [goals, runs, usage, workspace] = await Promise.all([api("/goals"), api("/runs"), api("/usage"), api("/workspace")]);
    state.goals = goals; state.runs = runs; state.workspace = workspace;
    $("activeWorkspaceText").textContent = workspace.name;
    $("workspaceButton").title = `Workspace: ${workspace.path}`;
    if (!state.selectedGoal || !goals.some((goal) => goal.goal_id === state.selectedGoal)) state.selectedGoal = localStorage.getItem("selectedGoal") || goals[0]?.goal_id || null;
    selectLatestThread();
    const active = runs.find((run) => run.goal_id === state.selectedGoal && ["running","queued"].includes(run.status));
    if (active?.thread_id) state.threadId = active.thread_id;
    renderUsage(usage); render();
    $("connectionText").textContent = "API connected";
    const shouldPoll = runs.some((run) => ["queued","running"].includes(run.status));
    if (shouldPoll && !state.polling) state.polling = setInterval(refresh, 1000);
    if (!shouldPoll && state.polling) { clearInterval(state.polling); state.polling = null; }
  } catch (error) { $("connectionText").textContent = "Connection unavailable"; toast(error.message); }
}

$("promptForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  const prompt = $("promptInput").value.trim();
  if (!prompt || !state.selectedGoal) return;
  $("sendButton").disabled = true;
  try {
    await api("/runs", {method:"POST", body:JSON.stringify({prompt, goal_id:state.selectedGoal, thread_id:state.threadId, sandbox:$("sandboxSelect").value})});
    state.freshChat = false; $("promptInput").value = ""; $("promptInput").style.height = "auto"; await refresh();
  } catch (error) { toast(error.message); $("sendButton").disabled = false; }
});

$("promptInput").addEventListener("input", (event) => { event.target.style.height = "auto"; event.target.style.height = `${Math.min(event.target.scrollHeight,180)}px`; });
$("promptInput").addEventListener("keydown", (event) => { if ((event.metaKey || event.ctrlKey) && event.key === "Enter") $("promptForm").requestSubmit(); });
$("newChatButton").addEventListener("click", () => { state.threadId = null; state.freshChat = true; renderChat(); $("promptInput").focus(); });
$("newGoalButton").addEventListener("click", () => $("goalDialog").showModal());
$("workspaceButton").addEventListener("click", async () => {
  $("workspaceDialog").showModal();
  try { await browseDirectory(state.workspace?.path || null); } catch (error) { toast(error.message); }
});
$("closeWorkspaceButton").addEventListener("click", () => $("workspaceDialog").close());
$("cancelWorkspaceButton").addEventListener("click", () => $("workspaceDialog").close());
$("parentDirectoryButton").addEventListener("click", () => {
  const path = $("parentDirectoryButton").dataset.path;
  if (path) browseDirectory(path).catch((error) => toast(error.message));
});
$("showHiddenDirectories").addEventListener("change", () => browseDirectory(state.browserPath).catch((error) => toast(error.message)));
$("selectWorkspaceButton").addEventListener("click", async () => {
  if (!state.browserPath) return;
  try {
    await api("/workspace", {method:"POST", body:JSON.stringify({path:state.browserPath})});
    state.selectedGoal = null; state.threadId = null; state.freshChat = true;
    $("workspaceDialog").close(); await refresh();
  } catch (error) { toast(error.message); }
});
$("goalForm").addEventListener("submit", async (event) => {
  const submitter = event.submitter;
  if (submitter?.value === "cancel") return;
  event.preventDefault();
  try {
    const goal = await api("/goals", {method:"POST", body:JSON.stringify({title:$("newGoalTitle").value, description:$("goalDescription").value})});
    state.selectedGoal = goal.goal_id; state.threadId = null; state.freshChat = true; localStorage.setItem("selectedGoal", goal.goal_id);
    $("goalDialog").close(); $("goalForm").reset(); await refresh();
  } catch (error) { toast(error.message); }
});

refresh();
