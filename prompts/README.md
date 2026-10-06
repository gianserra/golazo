# Golazo agent prompts

Golazo-authored instructions sent to Codex live in this directory as Markdown templates. Runtime values use `{{PLACEHOLDER}}` tokens and are rendered by `rust-backend/prompts.rs`.

Workflows already owned by a skill remain authoritative in that skill's `SKILL.md` and references. The scaffold and goal-mode wrappers compose those skill files rather than copying their instructions into Rust source.

| File | Purpose |
| --- | --- |
| `spec-mode.md` | Read-only implementation-document evolution contract. |
| `build-mode.md` | Explicit-intent implementation contract. |
| `scaffold-goal.md` | Goal-scaffolding wrapper and structured response contract. |
| `file-attachments.md` | Developer context for uploaded files. |
| `autonomous-worker.md` | Claim-scoped autonomous worker protocol and bounded context packet wrapper. |
| `supervisor.md` | Exception-only Supervisor policy, authority boundary, and structured decision contract. |
