# Goal type patterns

Use these patterns as starting points, not mandatory templates. Remove irrelevant areas and add
repository-specific work supported by evidence.

## Greenfield

Create a complete application where no meaningful inherited application architecture exists.

Typical areas: product definition, architecture decisions, project foundation, first vertical
slice, core capabilities, quality, operations, and release readiness. Read `greenfield.md`.

## Feature

Extend an established system while preserving its conventions and boundaries.

Typical areas: domain or data changes, backend behavior, user experience, integration points,
compatibility, tests, documentation, and rollout.

## Bug

Correct observable behavior with regression protection.

Typical areas: reproduction, root cause, minimal fix, regression test, adjacent-case verification,
and deployment or backport considerations.

## Refactor

Change internal structure while preserving declared behavior.

Typical areas: behavior baseline, target boundary, incremental migration, compatibility bridge,
consumer conversion, obsolete-path removal, and equivalence verification.

## Migration

Move data, runtime, framework, API, or infrastructure between representations.

Typical areas: inventory, target design, compatibility, conversion, validation, staged cutover,
rollback, observability, and cleanup.

## Integration

Connect an external service, API, SDK, identity provider, or event source.

Typical areas: authentication, client boundary, data mapping, rate limits, retries, idempotency,
failure states, secrets, sandbox testing, observability, and disconnection or removal.

## Release

Prepare, package, deploy, or distribute an existing product slice.

Typical areas: build reproducibility, configuration, versioning, signing, artifacts, smoke tests,
deployment, monitoring, rollback, and release evidence.

## Research

Reduce uncertainty without presenting exploratory work as delivered product functionality.

Typical areas: explicit questions, constraints, experiment design, evidence, decision criteria,
timebox, decision record, and separately tracked follow-up implementation.

## Mixed

Use one primary type and apply secondary patterns only where they alter the work. For example, a
Greenfield application with an external integration remains primarily Greenfield and incorporates
the Integration failure and authentication pattern for that feature.
