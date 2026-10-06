# Golazo Goal Scaffolding

You are running Golazo's bundled `$scaffold-implementation-goal` workflow.

Follow the skill instructions and references below.

## Skill

{{SCAFFOLD_SKILL}}

## Goal Types Reference

{{SCAFFOLD_GOAL_TYPES}}

## Greenfield Reference

{{SCAFFOLD_GREENFIELD}}

## Suggestion Contract

{{SCAFFOLD_CONTRACT}}

Inspect the current repository read-only. Do not modify files, create a goal, or mutate an implementation tracker. Produce suggestions for review only.

## User Goal Input

```json
{{USER_GOAL_JSON}}
```

Return exactly one JSON object with no Markdown fence or surrounding prose. Use this shape:

```json
{
  "goal_interpretation": "string",
  "success_criteria": ["string"],
  "primary_type": "Greenfield|Feature|Bug|Refactor|Migration|Integration|Release|Research|Mixed",
  "secondary_types": [],
  "classification_evidence": ["string"],
  "confidence": "high|medium|low",
  "assumptions": ["string"],
  "decisions_required": ["string"],
  "required_features": [{
    "feature_id": "lowercase-hyphenated-id",
    "title": "string",
    "description": "string",
    "scope": "required_mvp|required_release_safety|optional_hardening|future",
    "rationale": "string",
    "evidence": ["string"],
    "steps": [{"step_id": "lowercase-hyphenated-id", "title": "string"}],
    "acceptance": ["string"],
    "verification": ["string"],
    "dependencies": ["string"],
    "risks": ["string"],
    "confidence": "high|medium|low"
  }],
  "optional_features": [],
  "risks": ["string"],
  "dependencies": ["string"],
  "non_goals": ["string"]
}
```
