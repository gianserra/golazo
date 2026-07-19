# Greenfield application scaffolding

Treat Greenfield goals as product and architecture formation, not merely repository setup.

## Discovery

Establish:

- target users and the problem being solved;
- measurable success criteria;
- the smallest useful MVP workflow;
- delivery environment and operational constraints;
- required integrations, data sensitivity, and compliance constraints;
- explicit non-goals.

Before tracker creation, propose where the new application code will live. Prefer a dedicated
folder beside related projects or under an existing code workspace. Show the absolute suggested
path, state whether it already exists, and let the user edit it or browse to another folder. Do not
default to a broad home directory or the currently active unrelated repository merely because it
was used for discovery.

Ask only questions that would change architecture or MVP scope. Record remaining assumptions.
Project location is always a required confirmation for Greenfield work because it determines the
repository boundary, tracker root, and execution workspace.

## Architecture decisions

Propose options with tradeoffs for decisions that materially affect the implementation, such as
application shape, client/runtime, persistence, deployment target, and testing strategy. Prefer the
simplest architecture that satisfies stated constraints. Do not choose merely because a technology
is common or familiar.

## Suggested tracker sequence

1. Product definition and acceptance boundaries
2. Architecture decisions and initial technical constraints
3. Project foundation: structure, configuration, formatting, linting, tests, and CI as needed
4. First vertical slice proving one real user workflow end to end
5. Remaining MVP capabilities as independently deliverable features
6. Cross-cutting quality required by the goal: accessibility, security, performance, or resilience
7. Operations required for the delivery target: logging, errors, secrets, deployment, monitoring,
   backups, and rollback
8. Release readiness and smallest useful smoke verification

Do not front-load every foundation or operational task. Pull forward only what the first vertical
slice needs, then expand based on demonstrated requirements.

## First vertical slice

Define an early runnable milestone that proves the architecture. A useful slice usually starts at a
real user action, crosses the key application boundaries, persists or retrieves real state when the
product requires it, and ends in observable behavior with an automated or repeatable verification.

Avoid milestones such as "repository created" or "all infrastructure scaffolded" as the first proof
of product value.

## Scope discipline

Separate suggestions into:

- required MVP work;
- required release or safety work;
- optional production hardening;
- future capabilities outside the current goal.

Do not assume authentication, teams, billing, administration, analytics, queues, caching,
microservices, containers, orchestration, or multi-region infrastructure. Suggest them only when
requirements or evidence justify them.
