---
name: scaffold-implementation-goal
description: Inspect a software goal and its repository, classify the goal as Greenfield, feature, bug, refactor, migration, integration, release, research, or mixed work, and propose reviewable implementation-tracker features, steps, acceptance criteria, risks, and verification. Use when a user creates a goal, asks what should be implemented, wants Codex to scaffold an implementation plan, or needs suggestions before controlled goal execution begins.
---

# Scaffold implementation goal

Create an evidence-grounded proposal for an implementation tracker. Keep suggestions advisory until
the user accepts them. Do not modify product code while scaffolding.

## Workflow

1. Capture the goal's objective, success criteria, repository or project, known constraints, and
   explicit non-goals. Ask only questions whose answers would materially change the plan.
2. Inspect the repository read-only. Identify its current shape, conventions, architecture, tests,
   delivery configuration, and existing tracker. For an empty or skeletal repository, treat the lack
   of inherited architecture as evidence for Greenfield classification.
3. Classify the goal using [references/goal-types.md](references/goal-types.md). Choose one primary
   type and optional secondary types. State the evidence and confidence. Confirm Greenfield or any
   low-confidence classification before proposing architecture-dependent work.
4. Generate candidate features and steps using the selected type's pattern. Apply
   [references/suggestion-contract.md](references/suggestion-contract.md) to every suggestion. For a
   Greenfield goal, also read [references/greenfield.md](references/greenfield.md).
5. Present a reviewable proposal. Separate required MVP work from optional hardening or follow-up
   work. Give the user clear choices to accept, edit, remove, regenerate, or add suggestions. For a
   Greenfield application, suggest a dedicated project path and require the user to confirm or
   change it before tracker creation.
6. Revise only the requested portion. Preserve accepted decisions and do not regenerate unrelated
   sections.
7. Write only accepted items to the implementation tracker. Use the `manage-implementation` skill
   and its tracker CLI when available; never hand-edit tracker metadata or compute progress.
8. Validate the tracker and show its computed state before handing it to controlled goal execution.
9. Produce a compact execution handoff containing the goal ID, tracker path, classification,
   accepted constraints and decisions, current feature and step, and configured execution limits.

## Classification rules

- Prefer repository evidence over keywords in the goal title.
- Use `Greenfield` when the goal establishes a complete application without meaningful inherited
  architecture. Do not misclassify a new module inside an established system as Greenfield.
- Use `Mixed` only when multiple types materially change the plan; still select a primary type.
- Explain uncertainty instead of forcing a confident classification.
- Reclassify when user answers or repository inspection invalidate the initial choice.

## Suggestion rules

- Suggest independently deliverable capabilities as features and verifiable work units as steps.
- Ground repository-specific suggestions in concrete paths, configuration, tests, or observed gaps.
- Include a rationale, acceptance criteria, verification, dependencies, risks, confidence, and
  required-or-optional designation for every feature.
- Prefer the smallest end-to-end vertical slice that proves the approach.
- Keep MVP scope distinct from production hardening and speculative future work.
- Do not recommend authentication, billing, distributed infrastructure, microservices, containers,
  orchestration, or cloud services without a goal requirement or repository evidence.
- Do not silently select a framework, database, hosting platform, or irreversible architecture.
  Present materially different options and tradeoffs, then record the user's decision.
- Do not silently place a Greenfield application in the active repository. Suggest a concrete code
  location based on the active workspace and goal title, explain whether a new directory will be
  created, allow the user to edit or browse for another location, and record the confirmed path.
- Reuse existing conventions unless the goal explicitly includes replacing them.
- Include regression coverage for bugs, compatibility and rollback for migrations, and failure
  behavior for integrations.
- Do not create steps merely to make the tracker appear comprehensive.

## Review contract

Present the proposal in this order:

1. Goal interpretation and success criteria
2. Primary and secondary goal types, evidence, and confidence
3. Assumptions and decisions requiring confirmation
4. Required feature suggestions
5. Optional hardening or follow-up suggestions
6. Risks, dependencies, and non-goals
7. User review choices

Do not create or mutate the tracker until the user has accepted the proposal or explicitly asked for
automatic acceptance. Treat edits, removals, and partial acceptance as authoritative.

## Tracker handoff

After acceptance:

1. Create or select the goal tracker.
   For Greenfield work, create or select the confirmed application project first and keep its
   `.goal-manager` tracker beside the application code.
2. Add accepted features and steps with stable, concise IDs.
3. Record the planning change as one coherent planned slice when the tracker workflow requires it.
4. Validate the tracker and report its computed progress.
5. Hand the validated tracker to the goal execution layer; do not begin implementation unless the
   user also requested execution.

The execution handoff for Greenfield work must include the confirmed absolute project path. Treat
the location as unresolved until the user explicitly accepts it; reviewing a list of open decisions
is not sufficient confirmation.

Keep the active tracker authoritative for remaining work. Keep audit slices historical; do not use
slice archives as the context-transfer queue.
