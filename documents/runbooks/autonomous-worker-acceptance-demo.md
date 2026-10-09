# Autonomous worker acceptance demo

Use this checklist to demonstrate the autonomous-worker architecture on a clean checkout before expanding rollout. The automated command exercises deterministic fixtures and never launches external Codex work or modifies a real user repository.

## Run the automated acceptance sequence

From the repository root:

```sh
pnpm test:autonomous-worker-acceptance
```

The command stops at the first failed section and exits nonzero. A successful run ends with `Autonomous worker acceptance demo passed.` It verifies the tracker but does not change tracker state.

## Operator checklist

### Preparation

- [ ] Use a clean disposable Git repository for any manual pilot demonstration.
- [ ] Confirm the coordination database and `.goal-manager` tracker have current backups.
- [ ] Confirm worker permissions are `workspace-write`, `on-request`, user-reviewed, and network-disabled for the constrained pilot.
- [ ] Confirm the selected goal has at least two independent ready units and no unresolved critical escalation.

### Normal work and completion

- [ ] Observe two peer workers claim different ready units without duplicate ownership.
- [ ] Confirm each worker receives its own Git worktree, branch, base revision, claim, run, and thread identity.
- [ ] Capture integration artifacts and validation evidence from both workers.
- [ ] Confirm the single integration lane serializes both artifacts and records the final revisions.
- [ ] Confirm claims, workers, tracker steps, and slice history reach their terminal completed states together.

Automated evidence: `two_peer_workers_integrate_independent_packages_end_to_end`.

### Conflict and reconciliation

- [ ] Exercise file, symbol, migration, predicted Git, and shared-contract overlap signals.
- [ ] Confirm benign overlap informs, semantic overlap coordinates, and stale contracts produce a recommendation.
- [ ] Confirm signals and Supervisor proposals are durable and deduplicated.
- [ ] Confirm no conflicting artifact is integrated before reconciliation and validation pass.

Automated evidence: `overlap_reconciliation_matrix_distinguishes_all_required_scenarios`.

### Human escalation

- [ ] Confirm the user sees a concise summary, evidence, recommendation, alternatives, and consequences.
- [ ] Confirm only the affected claim or package is blocked.
- [ ] Change relevant evidence and confirm the stale choice is rejected.
- [ ] Refresh the decision packet, select a remediation, and confirm actor, evidence, alternatives, and outcome are audited.
- [ ] Confirm the blocked work resumes with a higher-generation claim and refreshed context, replacing a terminal worker when necessary.

Automated evidence: `human_escalation_journey_presents_blocks_rejects_stale_choice_and_resumes_work`.

### Worker failure and Supervisor outage

- [ ] Stop worker heartbeat and activity; confirm the worker is detected as stalled.
- [ ] Confirm partial work is quarantined and remains readable.
- [ ] Confirm the expired claim is replaced exactly once and the stale owner cannot heartbeat it.
- [ ] Make the Supervisor unavailable; confirm deterministic watchdog safety still runs and an unaffected worker can complete work.
- [ ] Restore the Supervisor and confirm the durable trigger is replayed.

Automated evidence: `stalled_worker_is_quarantined_replaced_and_recovered_without_duplicate_ownership` and `supervisor_outage_does_not_gate_watchdog_safety_or_later_replay`.

### Live Supervisor runtime

- [ ] Start a pool and produce a real typed worker signal; confirm the backend launches one ephemeral Spec-mode Codex turn with read-only sandboxing and the Supervisor decision schema.
- [ ] Confirm the structured decision is validated, persisted as an intervention, and delivered only to the affected worker or escalation scope.
- [ ] Confirm the autonomous console shows Supervisor state, pending triggers, evaluation and token budgets, replay position, current trigger, last decision, and any failure.
- [ ] Pause or stop the pool during an evaluation; confirm the late result is not applied and the event cursor remains replayable.
- [ ] Simulate a malformed response or provider failure; confirm workers continue independently, the failure is visible, bounded retry and circuit policy apply, and a later healthy iteration consumes the same durable trigger exactly once.
- [ ] Restart the backend with an unfinished Supervisor run; confirm run reconciliation leaves the durable trigger available and the monitor safely reevaluates it without duplicating a completed intervention or notification.

Automated evidence: `coordination::supervisor_runtime::tests`, including `production_evaluator_launches_read_only_codex_turn_and_applies_decision`, plus the operator dashboard tests.

### Restart and disaster recovery

- [ ] Restore a coordination backup and confirm exclusive higher-generation ownership and ordered audit history.
- [ ] Confirm an interrupted integration job returns to an explicit queued recovery state.
- [ ] Confirm a dirty unintegrated worktree cannot be cleaned automatically.
- [ ] Run startup reconciliation and inspect its durable recovery report before resuming the pool.

Automated evidence: `coordination::disaster_recovery` plus the recovery runbook in [autonomous-worker-recovery.md](autonomous-worker-recovery.md).

### Staged rollout and rollback

- [ ] Run shadow mode and confirm it records ready work, proposed claims, signals, and proposed Supervisor decisions without creating workers or claims.
- [ ] Start the constrained pilot only after clean shadow evidence; confirm exactly two conservative workers and manual integration.
- [ ] Trigger a rollback gate and confirm the pool pauses, returns to shadow, and preserves active claims and artifacts.
- [ ] Compare matched single-worker and pooled observations for throughput, token cost, conflicts, integration latency, and human interruptions.
- [ ] Attempt expansion with one failed gate and confirm concurrency remains two.
- [ ] Pass all safety, quality, cost, recovery, and live-state gates; confirm the approver and evidence are durable before concurrency or automation increases.

Automated evidence: `coordination::rollout::tests` and the benchmark described in [autonomous-worker-reference.md](../benchmarks/autonomous-worker-reference.md).

## Final acceptance record

- [ ] `pnpm test:autonomous-worker-acceptance` passes.
- [ ] The complete Rust, tracker, frontend, typecheck, production build, formatting, and diff gates pass.
- [ ] `golazo-tracker validate autonomous-worker-architecture` reports a valid tracker.
- [ ] The tracker has one or zero Next items; zero is expected after the final acceptance slice is recorded.
- [ ] Every implementation step is complete and the final feature status is `Done`.
- [ ] Known operational limits and rollback criteria remain visible in the architecture policy and recovery runbook.
