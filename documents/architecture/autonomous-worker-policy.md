# Autonomous Worker Architecture Policy

Status: Accepted for implementation  
Accepted: 2026-10-03  
Source: ADR-0001 through ADR-0004 in `golazo-adrs.zip`

## Decision

Golazo will add an autonomous peer worker pool beside the existing `RunManager`. Workers select
ready Features or Work Packages through atomic claims, execute in isolated workspaces, coordinate
through durable structured state, and integrate through a deterministic per-repository boundary.
An exception-driven Supervisor may improve awareness and coordination, but it does not assign
routine work and is not required for worker safety or forward progress.

The four source ADRs are accepted with the concrete policies below. These defaults are versioned
application policy, not model discretion. User overrides must be explicit, durable, scoped, and
auditable.

## Preserved Boundaries

- The Rust backend owns durable coordination, validation, recovery, and safety invariants.
- Codex threads and turns remain the coding runtime; a thread is not a durable Worker identity.
- `RunManager` remains the run lifecycle substrate. `WorkerPool` composes it rather than replacing it.
- The Markdown implementation tracker remains authoritative for feature, step, and slice history.
- Coordination records supplement the tracker; they do not move completion truth into chat history.
- The `manage-implementation` skill and `golazo-tracker` CLI remain the supported tracker mutation path.

## Durable Coordination Store

- Use SQLite in the Golazo application-data directory with WAL mode, foreign keys, and an explicit
  schema version table.
- Use immediate transactions for claim acquisition, lease changes, terminal transitions, contract
  revision changes, escalation resolution, and integration finalization.
- Use an append-only event table plus typed current-state tables. Events provide audit and replay;
  current-state tables provide efficient scheduling and UI queries.
- Mutating commands carry idempotency keys. A repeated key returns the recorded outcome and cannot
  apply the mutation twice.
- Back up with SQLite's online backup mechanism before destructive migrations. Startup performs
  integrity checks and refuses autonomous execution when coordination integrity is uncertain.
- Existing goals require no coordination rows until autonomous execution is enabled for that goal.

## Worker Pool Policy

- Autonomous execution is disabled by default during the compatibility and shadow phases.
- Initial enabled default: two active workers per goal.
- Initial hard maximum: four active workers per goal and eight globally.
- Ready work is selected deterministically by explicit priority, dependency readiness, oldest-ready
  timestamp, and stable identifier order.
- Backpressure prevents new workers when the integration queue has four pending artifacts, the goal
  has an unresolved blocking escalation, or resource budgets are exhausted.
- Users may lower limits at any time. Raising limits above the hard maximum requires an explicit
  advanced override recorded in the audit log.

## Claim and Lease Policy

- Claims apply to coherent Features or Work Packages, never individual checklist steps.
- Claim acquisition is atomic and records the goal, unit, Worker, base repository revision, lease
  generation, and policy version.
- Default lease duration: five minutes.
- Heartbeat cadence: thirty seconds while a worker is active; heartbeats extend from server time.
- Grace period: sixty seconds after lease expiry before reclaim evaluation.
- Reclaim quarantines the former workspace and increments the lease generation. A stale generation
  cannot publish terminal state or integrate an artifact.
- A worker may release work voluntarily with durable partial-progress and artifact metadata.
- Claims describe scheduling ownership, not filesystem locks. Managed overlap remains representable.

## Workspace Isolation Policy

- Git repositories use one managed worktree and worker branch per active implementation Worker.
- Worktree metadata records canonical repository identity, base commit, branch, path, and creation
  evidence. Paths are validated before any create or cleanup operation.
- Uncommitted, untracked, conflicted, or unpublished state blocks automatic cleanup.
- Integrated worktrees may be archived and removed; abandoned worktrees are quarantined until a
  recovery or explicit discard decision is recorded.
- Non-Git projects may use only one implementation Worker. Autonomous concurrency is blocked until
  an isolation provider with equivalent recovery guarantees exists.

## Integration Policy

- Implementation remains parallel; integration is serialized per repository.
- Workers synchronize near integration, or earlier only when a material dependency or shared
  contract revision invalidates their assumptions.
- Each integration artifact records base/head revisions, commits, changed paths and contracts,
  migrations, validation evidence, and known risks.
- Preflight rejects stale claim generations, missing ownership, dirty or ambiguous repository state,
  incompatible contracts, unresolved blocking signals, and failed required validation.
- Reconciliation follows repository policy using rebase, merge, cherry-pick, or regeneration. Golazo
  never discards worker state to make integration appear clean.
- Claim completion, tracker evidence, contract updates, events, and integration outcome finalize as
  one recoverable operation. A failed integration leaves the artifact and workspace recoverable.

## Shared Contract Policy

- Register contracts whose changes can invalidate parallel work: public APIs, persisted schemas,
  migrations, domain models, configuration formats, generated interfaces, and serialization formats.
- Contract revisions are monotonic integers scoped to a stable contract identifier.
- Work Packages declare produced contracts, consumed contracts, and expected revisions.
- Material changes publish a typed event and mark affected active claims stale until refreshed or
  explicitly reconciled.
- Documentation-only or proven backward-compatible changes may retain the current revision when the
  compatibility decision and evidence are recorded.

## Supervisor Policy

- Deterministic detectors and watchdogs run before any model evaluation.
- The Supervisor is invoked only for deduplicated eligible signals, explicit coordination requests,
  or unresolved escalations.
- Allowed decisions are `Observe`, `Inform`, `Recommend`, `Coordinate`, `Block`, and `Escalate`.
- The Supervisor cannot acquire claims, assign routine work, broaden permissions, approve dangerous
  actions, integrate changes, or mark implementation complete.
- Model output must match a versioned schema and cite durable evidence identifiers.
- Initial budget: at most one evaluation per correlated signal group per five minutes, three retries
  for transient failures, and a configurable per-goal token ceiling.
- On Supervisor outage, workers continue when deterministic policy permits; signals remain queued for
  replay. Watchdog safety actions remain available without the Supervisor.

## Human Escalation Policy

- Consequential ambiguity becomes a durable Escalation with evidence, affected scope, viable options,
  consequences, staleness conditions, and a recommended default when safe.
- Only the affected claim, package, contract, or integration lane is blocked. Unrelated ready work may
  continue.
- Applying a decision revalidates its evidence and revisions, records the actor and accepted risk,
  publishes resulting events, and refreshes or replaces affected workers.

## Communication Policy

- Structured events and targeted notifications are the normal worker communication path.
- Notifications are delivered at safe turn boundaries, have bounded payloads, and record delivery,
  acknowledgement, expiry, and supersession state.
- Direct worker conversation is exceptional, short-lived, mediated by Golazo, and preserved as an
  observable coordination artifact. Workers never gain hidden authority over one another.

## Rollout and Rollback Policy

1. Compatibility: ship schema and APIs with autonomous execution disabled.
2. Shadow: compute ready work, proposed claims, detector signals, and Supervisor decisions without
   launching autonomous Workers.
3. Constrained pilot: two Workers, conservative permissions, manual integration, selected Git goals.
4. Assisted integration: enable deterministic integration after reliability gates pass.
5. General availability: raise limits only after safety, quality, recovery, cost, and usability gates.

Each phase is protected by a feature flag and can return to the preceding phase without deleting
coordination history or breaking existing single-worker goals. Rollback disables new autonomous
actions, drains or pauses active Workers, preserves worktrees and artifacts, and retains audit state.

## Acceptance Gates

- No double ownership under concurrent claim attempts or restart recovery.
- No cleanup path can delete unintegrated or unverified user work.
- Existing goals and single-thread execution continue to work with no migration ceremony.
- Deterministic safety and lease recovery work during Supervisor and Codex runtime outages.
- Every intervention, override, escalation decision, and integration outcome is attributable.
- Shadow and pilot measurements demonstrate useful throughput without unacceptable conflict,
  interruption, token, or recovery cost before broader enablement.
