# Autonomous worker recovery runbook

This runbook covers recovery of Golazo's autonomous worker coordination layer.
It assumes the tracker is under `.goal-manager/`, the coordination database is
`.goal-manager/coordination.sqlite`, and the backend is reachable only on its
configured local address.

## Recovery invariants

Preserve these invariants in every incident:

1. Stop or pause new autonomous work before changing uncertain ownership.
2. Never assign a second owner while an active claim may still have a live
   worker. Reconcile the run, thread, workspace, and lease first.
3. Never delete a dirty, conflicted, unpublished, orphaned, or unintegrated
   worktree. Quarantine retains the branch, files, and recovery evidence.
4. Do not mark tracker steps complete from chat history. Require durable
   implementation evidence and mutate the tracker through `golazo-tracker`.
5. Preserve `.goal-manager/coordination.sqlite`, its `-wal` and `-shm` files,
   run history, startup recovery report, alert snapshots, worktree quarantine
   records, and relevant Git refs before repair.
6. Resume only after health, ownership, audit history, and integration state
   agree. If they cannot be reconciled, keep the affected scope blocked and
   escalate; unrelated healthy work may continue.

## Common triage

Capture these read-only artifacts before changing state:

- `GET /health` for backend liveness;
- `GET /coordination/v1/goals/{goal_id}/health` for component diagnostics;
- `GET /coordination/v1/goals/{goal_id}/alerts` for active operational alerts;
- `GET /coordination/v1/goals/{goal_id}/pool`, `/workers`, `/claims`, and
  `/integration/jobs` for durable ownership and queue state;
- `GET /coordination/v1/goals/{goal_id}/claims/{claim_id}/trace` for the claim's
  acquisition-through-completion timeline;
- `.goal-manager/startup-recovery.json`,
  `.goal-manager/{goal_id}/operational-alerts.json`, and JSON operation logs;
- `git worktree list --porcelain`, plus `git status --porcelain=v2 --branch`
  inside every affected worker worktree.

Use the goal pool's pause control when state is uncertain and drain when live
workers should finish but no replacement work should start. Stop is reserved
for an intentional terminal shutdown; it does not authorize workspace cleanup.

## Backend crash or restart

Detection:

- `/health` is unreachable, live runs stop updating, or the next process emits
  `startup.reconciliation` warnings.

Containment:

- Do not start a second backend against the same coordination directory.
- Preserve the database sidecar files and run history. Leave worker worktrees
  in place.

Recovery:

1. Restart one backend with the same workspace and goal-manager data paths.
2. Allow pre-serve startup reconciliation to finish. It validates trackers and
   SQLite, terminalizes abandoned run-history entries, inventories Codex
   threads and worktrees, recovers worker bindings, expires eligible leases
   after the configured grace period, and requeues interrupted integration
   jobs.
3. Read `.goal-manager/startup-recovery.json`. Resolve every warning and inspect
   every quarantined worker or workspace before resuming the pool.

Verification:

- The recovery report uses `golazo.startup-recovery.v1` and has a later
  `completedAt` than the crash.
- Goal health no longer reports `recovery_required`; no claim has two active
  owners; running integration jobs have either resumed as queued work or have
  an explicit terminal result.

Escalate when tracker or store integrity fails, runtime inventory is still
unavailable after retry, or a live process and durable ownership disagree.

## Worker crash or heartbeat loss

Detection:

- An active worker has a stale heartbeat, an `heartbeat_loss` alert is active,
  or health reports `worker_heartbeat_stale`.

Containment:

- Pause replacement for the affected claim. Do not infer worker death from one
  missed heartbeat and do not revoke ownership while its process may still be
  writing to the worktree.

Recovery:

1. Correlate the worker, claim, run, thread, workspace, and last event using the
   worker resource and claim trace.
2. If the runtime is live, renew only after observing current activity.
3. If the runtime is gone, preserve partial progress, transition the worker to
   recovery, and quarantine its workspace before expiring or reclaiming the
   claim through the claim service.
4. Start a replacement only after the previous generation is terminal and the
   new generation has exclusive ownership.

Verification:

- The former worker has a terminal or recovering state with a recorded reason;
  its active-claim binding is removed; the replacement claim generation is
  greater than the abandoned generation; the preserved workspace remains
  available for reconciliation.

Escalate when liveness is ambiguous, the same run or workspace appears bound to
multiple workers, or partial work cannot be safely classified.

## Lost Codex thread

Detection:

- A durable worker references a thread missing from native app-server inventory,
  or health reports a missing runtime binding after inventory succeeds.

Containment:

- Keep the claim and workspace intact. Do not create a replacement thread that
  silently assumes the old conversational state.

Recovery:

1. Confirm the run is also stopped or detached; a temporary app-server outage
   is not proof that the thread is gone.
2. Mark the worker recovering and create a new thread using the bounded worker
   context packet: goal, package, claim generation, repository/base revision,
   dependency and contract revisions, pending notifications, durable events,
   prior evidence, and workspace state.
3. Persist the new thread binding and an event linking it to the lost thread
   before the next turn begins.

Verification:

- Exactly one current thread is bound to the worker; the replacement prompt is
  visible; pending relevant notifications replay once; the claim trace retains
  the prior run/thread links and shows the resumed turns.

Escalate if the old thread returns with active work, the context packet cannot
be built from durable state, or the workspace revision has changed unexpectedly.

## Expired claim lease

Detection:

- Health or alerts report an expired/stuck claim and the lease plus the
  configured 60-second reclaim grace period have elapsed.

Containment:

- Block new ownership while checking heartbeat, event, process, thread, and
  workspace evidence. Clock expiry alone does not prove safe reclaim.

Recovery:

1. Reconcile the current owner. If it is live and making durable progress,
   renew through the normal heartbeat path.
2. Otherwise quarantine the workspace, record partial progress and follow-up
   metadata, expire the old claim, and remove the stale worker binding.
3. Reclaim atomically. The new claim must use a higher generation and the
   current tracker and base revision.

Verification:

- At most one active exclusive claim exists for the scope; stale-generation
  heartbeats and completion attempts are rejected; the old workspace and audit
  events remain recoverable.

Escalate when the former owner may still be live, overlap is semantic rather
than reconcilable, or repository state no longer matches the recorded base.

## Corrupt or unavailable coordination store

Detection:

- Startup integrity validation, goal health, or any coordination API reports a
  store failure. A `store_error` alert is written directly to the per-goal
  snapshot even when SQLite cannot open.

Containment:

- Stop autonomous execution and all writers. Preserve the database, `-wal`, and
  `-shm` files together; never replace or copy a live SQLite database piecemeal.

Recovery:

1. Work on copies and retain the original incident set unchanged.
2. Select the newest known-good online backup created by
   `SqliteCoordinationStore::backup_to`. Verify its integrity before use.
3. With the backend stopped, restore through
   `SqliteCoordinationStore::restore_from`; do not use tracker Markdown or chat
   transcripts to reconstruct ownership records.
4. Start one backend and let startup reconciliation compare the restored store
   with trackers, runs, threads, worktrees, leases, and integration jobs.

Verification:

- SQLite integrity passes; typed records and append-only events are readable;
  no claim has double ownership; the event cursor remains ordered; every live
  workspace is either attached or quarantined; store-error alerts resolve only
  after a successful normal evaluation.

Escalate if no verified backup exists, restore loses audit/event history, or
the restored ownership state cannot be reconciled with preserved workspaces.

## Orphaned worktree or branch

Detection:

- Startup reconciliation or an `orphaned_workspace` alert finds a managed
  worktree without a durable worker binding, a missing registered worktree, or
  an unregistered directory under the managed root.

Containment:

- Do not prune, delete, force-remove, or reuse the branch. Create/retain the
  non-destructive quarantine record and inspect dirty, conflicted, untracked,
  and unpublished state.

Recovery:

1. Match repository identity, base/head revisions, branch, claim evidence, and
   worker records.
2. Reattach only when one durable worker and claim unambiguously own the work.
3. Otherwise keep it quarantined and either capture an integration artifact,
   reconcile useful commits into the proper lane, or obtain an attributed
   explicit-discard decision.
4. Cleanup is allowed only for a clean, verified integrated head or an explicit
   discard with decision author and reason.

Verification:

- The worktree is attached or has a durable quarantine/cleanup record; useful
  commits and untracked files are preserved; Git worktree inventory and durable
  workspace bindings agree.

Escalate when ownership is ambiguous, the branch contains unrelated user work,
or repository identity/path safety checks fail.

## Supervisor outage

Detection:

- Supervisor evaluation records a provider failure, circuit breaker, or budget
  exhaustion while deterministic watchdogs and worker services remain healthy.

Containment:

- Do not stop safe independent work solely because the Supervisor is absent.
  Deterministic claim, permission, lease, contract, integration, and watchdog
  rules remain authoritative. Consequential ambiguity stays blocked.

Recovery:

1. Allow workers to continue only within current claims and known-compatible
   contracts. Queue durable signals and notifications without broad broadcast.
2. Apply deterministic watchdog actions for stalls, lease risk, repeated
   failures, and resource budgets. Open a human escalation when policy requires
   judgment that cannot safely wait.
3. When the Supervisor returns, replay the bounded unresolved signal set,
   validate structured decisions, and persist any targeted intervention before
   delivery.

Verification:

- Useful unrelated work continued; no authority or claim boundary broadened;
  queued signals replay exactly once or are explicitly superseded; intervention
  audit records link evidence, targets, actions, and outcomes.

Escalate when deterministic policy cannot decide safely, a contract or semantic
conflict affects multiple workers, or unavailable supervision exceeds the
configured failure budget.

## Return to service checklist

- Goal health is healthy or an explicitly accepted degraded mode.
- No critical operational alerts remain active.
- Claims have exclusive owners and valid generations; stale writers are rejected.
- Worker/run/thread/workspace bindings agree with live inventory.
- Integration jobs are queued, succeeded, or explicitly failed—none are silently
  left running.
- Tracker status and slice evidence reflect verified implementation only.
- Incident artifacts and operator decisions are retained for audit.
