# Suggestion contract

Represent each proposed feature with the following information:

| Field | Requirement |
| --- | --- |
| Feature ID | Stable, concise, lowercase hyphenated identifier |
| Title | User-facing independently deliverable capability |
| Scope | Required MVP, required release/safety, optional hardening, or future |
| Rationale | Why the feature is relevant to this goal |
| Evidence | Repository path, observed gap, explicit requirement, or stated constraint |
| Steps | Ordered, verifiable work units with stable IDs |
| Acceptance | Observable conditions proving delivery |
| Verification | Smallest useful tests, builds, checks, or manual evidence |
| Dependencies | Prior decisions, features, services, credentials, or external work |
| Risks | Material failure, migration, security, data, or delivery concerns |
| Confidence | High, medium, or low with a short reason |

## Quality bar

- Make each feature independently understandable and deliverable.
- Make each step objectively markable as open or done.
- State unknowns rather than converting them into invented requirements.
- Avoid duplicate features split by technical layer when one vertical capability is clearer.
- Split a capability when parts can ship, fail, or be verified independently.
- Match verification effort to risk.
- Prefer explicit acceptance criteria over implementation prescriptions.

## Review operations

Support these user decisions without disturbing accepted work:

- Accept all required suggestions
- Accept or reject one feature
- Edit a feature, step, criterion, or scope designation
- Regenerate one feature or one missing area
- Move an item between MVP, hardening, and future scope
- Add a user-defined feature

Write only the accepted result to the tracker.
