# <APP_NAME> Production Implementation Tracker

Last updated: <YYYY-MM-DD>

This document tracks production implementation work for `<APP_REPO>`.

<APP_SCOPE>

## Status Legend

| Status | Meaning |
| --- | --- |
| Done | Implemented, tested, and usable in the current app slice. |
| Partial | A useful version exists, but production gaps remain; the row states what completes it. |
| Planned | Design direction is known, implementation has not started. |
| Blocked | Work depends on an external decision, credential, service, or infrastructure choice. |

## Update Rules

- Update this tracker after every meaningful implementation slice, verification slice, deployment
  receipt, or production-readiness status change.
- Keep rows production-focused. Do not add experiment inventories, paper/source inventories, or
  benchmark bibliography rows here.
- Keep the Status Rollup current whenever a row is added, removed, or changes status.
- Count only exact `Done`, `Partial`, `Planned`, and `Blocked` cells in the Implementation Matrix.
- Calculate Done % as `Done / (Done + Partial + Planned + Blocked) * 100`, rounded to one decimal
  place.
- Do not mark a row Done for documentation-only progress; Done requires runnable code and the
  smallest useful verification.
- Slice archive files are audit-only. AI agents should not read through, summarize, rewrite, or
  update them during routine tracker work unless a human explicitly asks for that audit.

## Current Shape

Describe what the app already owns today:

- Runtime/API surface:
- Persistence:
- Frontend/client surface:
- Deployment/config:
- Known fake/stub/local-only pieces:

## Production Definition

`<APP_REPO>` is production-ready when:

1. <Production condition 1>
2. <Production condition 2>
3. <Production condition 3>

## Status Rollup

Last counted: <YYYY-MM-DD>

Scope: current status rows in the Implementation Matrix only.

| Status | Count |
| --- | ---: |
| Done | 0 |
| Partial | 0 |
| Planned | 0 |
| Blocked | 0 |
| Total | 0 |

Done %: **0.0%**

Next Slice Checklist progress: **0 / 0 complete (0.0%)**

## Latest Evidence Notes

- **<YYYY-MM-DD> tracker created:** initial implementation tracker created for `<APP_REPO>`.

## Slice Records

Slice records live under `documents/implementation-tracker/slices/`. The archive README is the
count/index source of truth. Each slice record file holds up to 100 `### Slice ...` headings before
a new range file is started. Slice records are audit-only and should only be read or updated by AI
agents when a human explicitly asks for that audit. Slice records are not a handoff queue; actionable
remaining work belongs in this tracker.

| Date | Slice | Record |
| --- | --- | --- |
| <YYYY-MM-DD> | Tracker created | `documents/implementation-tracker/slices/slice-records-001-100.md#slice-001-tracker-created` |

## Implementation Matrix

| ID | Status | Area | Production scope | Completion bar |
| --- | --- | --- | --- | --- |
| <FEATURE_PREFIX>-001 | Planned | <Area> | <What this feature owns.> | <What proves this feature is complete.> |

## Current Risk Register

| Risk | Lifecycle | Mitigation / next action |
| --- | --- | --- |
| <Risk> | Open | <Mitigation or next action.> |

## Failure Scenario Coverage

Use this table to prove whether simple infrastructure is enough before adding heavier platform
dependencies. Name the default strategy here, for example Postgres-backed records, step leases,
attempts, artifacts, idempotency keys, and durable events.

| ID | Coverage status | Scenario | Simple-infra handling |
| --- | --- | --- | --- |
| <FAILURE_PREFIX>-001 | Planned | <Failure scenario> | <How the simple design handles this.> |

Escalate to heavier infrastructure only when this table cannot be covered cleanly with the chosen
simple strategy.

## Next Slice Checklist

- [ ] <Next implementation task>
- [ ] Update this tracker rollup after the first implementation slice.

## Non-Goals

- <Non-goal 1>
- <Non-goal 2>
