# Implementation Tracker Row Snippets

## Implementation Row

```markdown
| <FEATURE_PREFIX>-### | Planned | <Area> | <What this feature owns.> | <What proves this feature is complete.> |
```

Use statuses exactly as `Done`, `Partial`, `Planned`, or `Blocked`.

## Risk Row

```markdown
| <Risk> | Open | <Mitigation or next action.> |
```

Risk lifecycle values should stay separate from implementation status. Suggested values:
`Open`, `Mitigating`, `Evidence Needed`, `Accepted`, `Closed`.

## Failure Scenario Row

```markdown
| <FAILURE_PREFIX>-### | Planned | <Failure scenario> | <How the simple design handles this.> |
```

Use the failure table to decide whether the current infrastructure is sufficient before adding
heavier workflow or orchestration tools.

## Evidence Note

```markdown
- **<YYYY-MM-DD> <short label>:** <what changed, what was verified, and what remains.>
```

## Slice Archive Row

```markdown
| 001-100 | [slice-records-001-100.md](slice-records-001-100.md) | <count> | Slice 001 | Slice <last> |
```

## Slice Record Heading

```markdown
### Slice 001: <Slice Title>
```

Keep up to 100 `### Slice ...` headings per slice record file.
