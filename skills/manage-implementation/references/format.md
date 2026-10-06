# Tracking format

Every generated Markdown file ends with a single HTML comment:

```text
<!-- codex-goal-manager:{...canonical JSON...} -->
```

The JSON is the machine record. It is required for deterministic application behavior—not primarily for model reasoning—and stores stable IDs, statuses, step state, timestamps, and slice-file links. It lives at the bottom so rendered Markdown stays human-first. The preceding production-style Markdown is a deterministic projection and may be regenerated after any mutation. The tracker continues to read legacy files whose metadata comment is at the top.

The visible goal projection follows the production implementation tracker shape where the current schema has authoritative data: status legend, update rules, status rollup, implementation matrix, next-slice checklist, detailed step checklists, and slice-record links. `golazo-tracker` calculates Status Rollup counts, Done %, and checklist progress from canonical state whenever it renders the file. Agents must not calculate or hand-edit those values; mutate through `golazo-tracker` or run `format [goal-id]`, then use `show` for the computed result.

## Goal file

Each goal lives at `<root>/<goal-id>/implementation.md`. Its record contains:

- `version`: format version, currently `1`.
- `kind`: `goal`.
- `goal_id`, `title`, and `description`.
- `created_at` and `updated_at` UTC timestamps.
- `features`: ordered feature records with `id`, `title`, `description`, `status`, and ordered `steps`.
- `work_packages`: optional ordered scheduling records with stable IDs, member Feature IDs, dependency package IDs, priority, status, readiness policy, and integration scope. An absent list is equivalent to an empty list; incomplete ungrouped Features remain individually claimable.
- `slice_files`: ordered filenames associated with the goal.

Each step contains `id`, `title`, Boolean `done`, and an optional Boolean `next`. The Next Slice Checklist renders only steps whose `next` value is true. Overall completion remains `round(done / total * 100)` and is zero when there are no steps; next-slice progress is calculated only from explicitly selected steps.

## Slice files

Slice files are named `slices-001.md`, `slices-002.md`, and so on. Each contains at most 100 ordered records. Slice IDs are contiguous across files (`slice-000001`, `slice-000002`, ...).

Each slice contains an ID, feature ID, UTC timestamp, summary, feature status after the slice, and zero or more evidence strings.

## Repair

Prefer restoring the last valid file from version control. If manual repair is unavoidable, update the footer JSON and run `golazo-tracker format <goal-id>` to reconstruct the visible Markdown. Running `golazo-tracker format` without a goal ID migrates every tracking document beneath the selected root. Always finish with `validate`.
