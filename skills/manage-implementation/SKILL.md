---
name: manage-implementation
description: Create and maintain Markdown implementation tracking for software goals, features, implementation steps, and audit slices. Use when Codex plans feature work, records incremental implementation, marks steps complete, reports completion rates, changes feature status among Planned, Blocked, Partial, and Done, or validates a goal's implementation history.
---

# Manage implementation

Use the Rust `golazo-tracker` CLI for every tracking mutation. Golazo places this command on `PATH`, including when the selected project does not contain Golazo's source code. Do not hand-edit the embedded metadata, Status Rollup, counts, percentages, or checklist progress. The CLI must calculate and render all rollup values. If `golazo-tracker` is unavailable, stop and report that configuration error; never substitute manual tracker edits.

## Workflow

1. Locate the tracking root. Default to `.goal-manager` in the repository.
2. Run `create-goal` if the requested goal does not exist.
3. Add each independently deliverable capability with `add-feature`.
4. Add verifiable work units with `add-step` before implementation begins.
5. Mark only the work selected for the next coherent slice with `set-next ... next`; use `set-next ... later` when it leaves that queue.
6. After a coherent implementation slice:
   - Run `set-step ... done` for newly verified steps.
   - Run `add-slice` with a concise summary and concrete evidence such as paths, tests, or commands.
   - Set the feature status explicitly when it changed.
7. Run `validate` before reporting the goal state.
8. Use `show` for the computed completion rate and current audit trail.

## Commands

Run from the repository root:

```bash
golazo-tracker --root .goal-manager create-goal api-v1 "API v1"
golazo-tracker --root .goal-manager add-feature api-v1 auth "Authentication"
golazo-tracker --root .goal-manager add-package api-v1 foundation "Foundation" --features auth
golazo-tracker --root .goal-manager ready-work api-v1
golazo-tracker --root .goal-manager add-step api-v1 auth tokens "Issue access tokens"
golazo-tracker --root .goal-manager set-next api-v1 auth tokens next
golazo-tracker --root .goal-manager set-step api-v1 auth tokens done
golazo-tracker --root .goal-manager add-slice api-v1 auth "Implemented token issuance" --status Partial --evidence tests/test_auth.rs
golazo-tracker --root .goal-manager validate api-v1
golazo-tracker --root .goal-manager format api-v1
```

Repeat `--evidence` for multiple evidence entries. Treat evidence as a short factual pointer, not a narrative.

## Rules

- Use only `Planned`, `Blocked`, `Partial`, or `Done` for feature and slice statuses.
- Treat CLI output as authoritative for completion. The CLI derives completion as completed steps divided by total steps; a feature with no steps is 0% complete.
- Never ask an agent to count matrix rows or calculate Status Rollup values. Run a tracker mutation or `format [goal-id]`, then use `show` for computed values.
- Keep status independent from completion. Status expresses delivery state; checkboxes provide the deterministic rate.
- The Next Slice Checklist is a deliberately small handoff queue. It contains only steps explicitly marked `next`, never every incomplete step.
- Use Work Packages when multiple Features form one coherent claim or integration boundary. Features not assigned to a package remain individually claimable for backward compatibility.
- Record one slice per coherent change set. The CLI automatically starts the next file after 100 slices.
- Never rewrite or delete historical slices to make progress appear cleaner.
- Use `format [goal-id]` only to regenerate the human-readable projection and move legacy metadata comments to the footer; it does not alter tracking history.
- Validate after conflict resolution or manual Markdown repair.

Read [references/format.md](references/format.md) only when integrating another tool with the Markdown files or repairing invalid tracking data.
