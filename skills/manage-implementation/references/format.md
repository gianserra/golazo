# Tracking format

Every generated Markdown file ends with a single HTML comment:

```text
<!-- codex-goal-manager:{...canonical JSON...} -->
```

The JSON is the machine record. It lives at the bottom so the document opens with its human-readable title and content. The preceding Markdown is a deterministic projection and may be regenerated after any mutation. The tracker continues to read legacy files whose metadata comment is at the top.

## Goal file

Each goal lives at `<root>/<goal-id>/implementation.md`. Its record contains:

- `version`: format version, currently `1`.
- `kind`: `goal`.
- `goal_id`, `title`, and `description`.
- `created_at` and `updated_at` UTC timestamps.
- `features`: ordered feature records with `id`, `title`, `description`, `status`, and ordered `steps`.
- `slice_files`: ordered filenames associated with the goal.

Each step contains `id`, `title`, and Boolean `done`. Completion is `round(done / total * 100)` and is zero when there are no steps.

## Slice files

Slice files are named `slices-001.md`, `slices-002.md`, and so on. Each contains at most 100 ordered records. Slice IDs are contiguous across files (`slice-000001`, `slice-000002`, ...).

Each slice contains an ID, feature ID, UTC timestamp, summary, feature status after the slice, and zero or more evidence strings.

## Repair

Prefer restoring the last valid file from version control. If manual repair is unavoidable, update the footer JSON and run `golazo-tracker format <goal-id>` to reconstruct the visible Markdown. Running `golazo-tracker format` without a goal ID migrates every tracking document beneath the selected root. Always finish with `validate`.
