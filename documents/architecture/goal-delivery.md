# Goal branch delivery

Golazo isolates autonomous implementation from the user's active checkout. A worker never commits, pushes, merges, rebases, or cherry-picks. The trusted coordinator performs those operations after validation.

## Delivery boundary

```text
worker worktree
  -> coordinator-owned worker commit
  -> validation gates
  -> per-goal integration worktree and codex/goal-<goal-id> branch
  -> non-force push to origin
  -> one open pull request targeting develop
  -> manual merge
```

The canonical project checkout is used to discover the repository and create managed worktrees. Golazo does not change its checked-out branch, index, working tree, or `HEAD` during worker integration.

## Policy

- The integration branch is deterministic: `codex/goal-<sanitized-goal-id>`.
- `goal-current` resolves to the integration branch head. On first use, the branch starts from `origin/develop`, then local `develop`, then `HEAD` as a compatibility fallback.
- Integration is serialized per repository and uses a dedicated managed worktree.
- Publication uses a normal non-force push. A non-fast-forward rejection is surfaced instead of overwritten.
- GitHub repositories use the authenticated `gh` CLI to find or create one open pull request from the goal branch to `develop`.
- Merge policy is manual. Golazo never merges the pull request automatically under this policy.
- Local integration remains recoverable if pushing or pull-request creation fails. Durable delivery state records the branch, revisions, pull request, and actionable error.
- Retried delivery recognizes both identical commits and patch-equivalent cherry-picks.

## User-visible states

- `local`: validated commits exist only on the local goal branch.
- `pushed`: the goal branch is available on the configured remote.
- `pull_request_open`: an open pull request targets the delivery branch.
- `needs_attention`: local integration is preserved, but publication or pull-request creation requires intervention.
