---
name: manage-implementation
description: Create and maintain Markdown implementation tracking for software goals, features, implementation steps, and audit slices. Use when Codex plans feature work, records incremental implementation, marks steps complete, reports completion rates, changes feature status among Planned, Blocked, Partial, and Done, or validates a goal's implementation history.
---

# Manage implementation

Use `scripts/implementation_tracker.py` for every tracking mutation. Do not hand-edit the embedded metadata or calculate completion rates yourself.

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
python skills/manage-implementation/scripts/implementation_tracker.py --root .goal-manager create-goal api-v1 "API v1"
python skills/manage-implementation/scripts/implementation_tracker.py --root .goal-manager add-feature api-v1 auth "Authentication"
python skills/manage-implementation/scripts/implementation_tracker.py --root .goal-manager add-step api-v1 auth tokens "Issue access tokens"
python skills/manage-implementation/scripts/implementation_tracker.py --root .goal-manager set-step api-v1 auth tokens done
python skills/manage-implementation/scripts/implementation_tracker.py --root .goal-manager add-slice api-v1 auth "Implemented token issuance" --status Partial --evidence tests/test_auth.py
python skills/manage-implementation/scripts/implementation_tracker.py --root .goal-manager validate api-v1
```

Repeat `--evidence` for multiple evidence entries. Treat evidence as a short factual pointer, not a narrative.

## Rules

- Use only `Planned`, `Blocked`, `Partial`, or `Done` for feature and slice statuses.
- Derive completion as completed steps divided by total steps. A feature with no steps is 0% complete.
- Keep status independent from completion. Status expresses delivery state; checkboxes provide the deterministic rate.
- Record one slice per coherent change set. The script automatically starts the next file after 100 slices.
- Never rewrite or delete historical slices to make progress appear cleaner.
- Validate after conflict resolution or manual Markdown repair.

Read [references/format.md](references/format.md) only when integrating another tool with the Markdown files or repairing invalid tracking data.

