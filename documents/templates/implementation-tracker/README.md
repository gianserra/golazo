# Implementation Tracker Template

Copy `production-implementation-tracker.template.md` into the target repo's `documents/`
directory and rename it for the app, for example:

```text
documents/<app-slug>-production-implementation-tracker.md
```

Copy the slice archive templates into the target repo's tracker slice folder:

```text
documents/implementation-tracker/slices/README.md
documents/implementation-tracker/slices/slice-records-001-100.md
```

Replace the placeholders:

- `<APP_NAME>`: human-readable app name.
- `<APP_REPO>`: repository/service name.
- `<APP_SCOPE>`: one-sentence implementation boundary.
- `<FEATURE_PREFIX>`: short row id prefix, for example `CLAPI`.
- `<FAILURE_PREFIX>`: short failure row id prefix, for example `CLFAIL`.
- `<YYYY-MM-DD>`: last update date.

Use `row-snippets.md` when adding new implementation or failure rows.

Use `slice-records-001-100.md` for the first 100 implementation slice records. When that file
contains 100 `### Slice ...` headings, create `slice-records-101-200.md` and add a new row to the
slice archive README. The archive README is the count/index source of truth.

Do not use slice records as a handoff queue. Put actionable remaining work in the active tracker:
Implementation Matrix completion bars, Next Slice Checklist, Risk Register, or Failure Scenario
Coverage.

Keep this tracker production-focused. Do not add research bibliography, experiment inventory, or
source extraction sections unless the app itself is a research artifact.
