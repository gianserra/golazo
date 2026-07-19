---
name: manage-implementation
description: Create and maintain Markdown implementation tracking for software goals, features, implementation steps, and audit slices. Use when Codex plans feature work, records incremental implementation, marks steps complete, reports completion rates, changes feature status among Planned, Blocked, Partial, and Done, or validates a goal's implementation history.
---

# Manage implementation

Use the Rust `golazo-tracker` CLI for every tracking mutation. Do not hand-edit the embedded metadata or calculate completion rates yourself.

## Workflow

1. Locate the tracking root. Default to `.goal-manager` in the repository.
2. Run `create-goal` if the requested goal does not exist.
3. Add each independently deliverable capability with `add-feature`.
4. Add verifiable work units with `add-step` before implementation begins.
5. After a coherent implementation slice:
   - Run `set-step ... done` for newly verified steps.
   - Run `add-slice` with a concise summary and concrete evidence such as paths, tests, or commands.
   - Set the feature status explicitly when it changed.
6. Run `validate` before reporting the goal state.
7. Use `show` for the computed completion rate and current audit trail.

## Commands

Run from the repository root:

```bash
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager create-goal api-v1 "API v1"
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager add-feature api-v1 auth "Authentication"
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager add-step api-v1 auth tokens "Issue access tokens"
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager set-step api-v1 auth tokens done
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager add-slice api-v1 auth "Implemented token issuance" --status Partial --evidence tests/test_auth.rs
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager validate api-v1
node scripts/run-cargo.mjs run --quiet --bin golazo-tracker -- --root .goal-manager format api-v1
```

Repeat `--evidence` for multiple evidence entries. Treat evidence as a short factual pointer, not a narrative.

## Rules

- Use only `Planned`, `Blocked`, `Partial`, or `Done` for feature and slice statuses.
- Derive completion as completed steps divided by total steps. A feature with no steps is 0% complete.
- Keep status independent from completion. Status expresses delivery state; checkboxes provide the deterministic rate.
- Record one slice per coherent change set. The CLI automatically starts the next file after 100 slices.
- Never rewrite or delete historical slices to make progress appear cleaner.
- Use `format [goal-id]` only to regenerate the human-readable projection and move legacy metadata comments to the footer; it does not alter tracking history.
- Validate after conflict resolution or manual Markdown repair.

Read [references/format.md](references/format.md) only when integrating another tool with the Markdown files or repairing invalid tracking data.
