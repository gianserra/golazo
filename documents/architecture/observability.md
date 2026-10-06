# Golazo observability contract

Golazo emits newline-delimited JSON logs through `tracing` using schema
`golazo.operation.v1`. The default `golazo_backend=info` filter includes the
`golazo_backend::operation` target. Operators can override filtering with
`RUST_LOG` without changing the log shape.

Every operation record includes:

- `log_schema`, `operation`, `outcome`, `level`, `target`, and `timestamp`;
- `goal_id`, `worker_id`, `claim_id`, `run_id`, and `thread_id`;
- `workspace_id`, `event_id`, `intervention_id`, `escalation_id`, and
  `integration_id`;
- `correlation_id` for durable event causality or another operation-specific
  correlation boundary.

Unavailable identifiers are emitted as empty strings so log processors receive
a stable set of searchable columns. Identifiers are emitted only from typed
durable records and runtime bindings; prompts, command output, credentials, and
free-form evidence are excluded from this channel.

Current operation families are:

- `backend.*` for process readiness;
- `run.*` and `thread.*` for Codex execution;
- `coordination.worker.*`, `coordination.claim.*`, and
  `coordination.event.*` for durable coordination;
- `workspace.*` for worktree creation, recovery, reconciliation, quarantine,
  and cleanup;
- `supervisor.intervention.*` and `coordination.escalation.*` for exceptional
  supervision;
- `integration.*` for artifacts, queue jobs, finalization, and maintenance.

The schema is the foundation for the metrics, traces, health diagnostics, and
alerts tracked in the remaining observability work. Changes to field names or
meaning require a schema version change.

## Goal metrics

`GET /coordination/v1/goals/{goal_id}/metrics` returns a versioned point-in-time
snapshot derived from the authoritative tracker and coordination store. It
includes worker utilization and state counts, unclaimed ready work, claim age
and lease pressure, completed-claim throughput, worker/claim/integration
failures, Supervisor actions, notification volume and delivery attempts,
conflict event counts, and integration queue state plus average, p95, and
maximum latency. Event counts page through the full durable event stream rather
than sampling only its newest records.

## Claim traces

`GET /coordination/v1/goals/{goal_id}/claims/{claim_id}/trace` returns a
versioned `golazo.claim-trace.v1` projection with a stable trace ID. The trace
is rebuilt from durable coordination state, so it remains available after a
backend restart and does not depend on an in-memory telemetry collector.

Every trace contains six ordered phase summaries—claim acquisition, worker
turns, notifications, validation, integration, and completion—and an ordered
set of spans for the records that have actually been observed. Links preserve
run and thread IDs, source event IDs, notification IDs, validation report IDs,
integration artifact and job IDs, and bounded evidence references. Missing
phase counts remain explicit for active or interrupted claims instead of being
reported as successful work.

The claim ID is the correlation boundary used by structured logs and durable
coordination events. Worker turns are restricted to the claim lifetime;
notifications are included only when their source or coalesced source event is
correlated to the claim; validation and integration records must reference an
artifact captured for the claim; and completion is derived from the typed
completion event with the terminal claim outcome as a recovery fallback.

## Goal health

`GET /coordination/v1/goals/{goal_id}/health` returns a versioned
`golazo.goal-health.v1` snapshot. Overall state is one of healthy, degraded, or
unhealthy, while operating mode distinguishes normal, idle, paused, draining,
degraded, and recovery-required behavior. The response also reports the latest
durable record timestamp, its age, the configured freshness threshold, and
whether freshness is applicable while the pool is idle.

The health model checks the tracker, SQLite integrity, worker pool, live worker
heartbeats, run and thread bindings, workspace bindings and paths, active claim
leases, Supervisor interventions and escalations, notification delivery, the
integration queue, and aggregate coordination-data freshness. Each finding has
a stable diagnostic code, severity, affected component and entity, observation
time, and a concrete recommended action. Historical integration failures are
not reported as current failures after the same artifact has a successful
retry.

## Startup reconciliation

Before the backend begins serving requests, it validates every tracker and the
coordination store, converts non-terminal run-history entries left by the prior
process into explicit interrupted failures, and asks the Codex app server for
the current thread inventory. It then reconciles registered worktree bindings,
records non-destructive quarantine evidence for orphaned managed worktrees,
and compares that runtime inventory with durable workers and claims.

Workers with missing runs, threads, workspaces, or claim ownership enter
`recovering`; expired claims pass through the normal atomic expiry path after
the policy's 60-second grace period; and integration jobs left `running` are
returned to the deterministic queue without discarding their attempt count or
artifact. The complete `golazo.startup-recovery.v1` report is atomically saved
as `.goal-manager/startup-recovery.json`. Codex or workspace inventory failure
is preserved as a warning in that report and causes conservative recovery
rather than treating stale local metadata as proof of a live runtime.

## Operational alerts

`GET /coordination/v1/goals/{goal_id}/alerts` evaluates and returns the
versioned `golazo.operational-alerts.v1` snapshot for a goal. A background
evaluator runs every 30 seconds and checks for stuck active claims, lost worker
heartbeats, repeated validation or watchdog failures, orphaned or unregistered
workspaces, ready-work or integration-queue backlog, and failed integrations
that have not subsequently succeeded.

Alerts have stable IDs and correlation keys, severity, bounded evidence,
recommended recovery actions, occurrence counts, and explicit active or
resolved lifecycle state. Each goal's snapshot is locked and atomically written
to `.goal-manager/{goal_id}/operational-alerts.json`; resolved history is
bounded while active alerts are retained. A normal evaluation clears
conditions that are no longer observed instead of leaving stale warnings.

Store failures use the same durable alert shape but do not depend on SQLite:
startup, API access, and the background evaluator write the fallback snapshot
directly from tracker discovery. This keeps the coordination-store outage
visible even when the store being monitored cannot be opened. Store alerts
instruct operators to stop autonomous execution and preserve the database
before following the recovery runbook.

The operator procedures for backend, worker, thread, lease, store, workspace,
and Supervisor failures are maintained in
[`../runbooks/autonomous-worker-recovery.md`](../runbooks/autonomous-worker-recovery.md).
