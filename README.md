# Golazo

A desktop application around `codex exec` with a native Rust backend and deterministic, Markdown-backed implementation tracking.

The product has three application layers:

- `frontend/`: React + TypeScript + Vite renderer shared by the browser and desktop app.
- `electron/`: secure Electron main/preload processes that own the native window, directory picker, and backend lifecycle.
- `rust-backend/`: Axum + Tokio service for projects, goals, Codex runs, usage, and static frontend delivery.

## Run locally

```bash
pnpm install
pnpm build:frontend
cargo run --bin golazo-backend
```

The service listens on `http://127.0.0.1:8765` by default. Tracking files default to `.goal-manager/`. Set `GOLAZO_PORT`, `GOAL_MANAGER_DATA_DIR`, `CODEX_WORKSPACE_ROOT`, or `CODEX_EXECUTABLE` to override the defaults.

The browser interface is available at `http://127.0.0.1:8765/`. Its sidebar groups goals beneath persistent local projects, similar to Codex. It also supports project browsing and selection, goal creation, Codex chat threads, resumable follow-up prompts, live run activity, implementation progress, and persistent per-turn and cumulative token usage.

New goals receive a readable generated ID consisting of a title slug and an eight-character unique hash, such as `ship-the-first-release-4fa91c2e`. Existing IDs are not changed.

Click the plus button beside **Projects** to add a local folder. Click a project heading to activate it, or click one of its nested goals to activate both the project and goal. Browsing is restricted to your home directory by default. Set `GOAL_MANAGER_BROWSE_ROOT` when starting the server to expose a narrower or different directory tree:

```bash
GOAL_MANAGER_BROWSE_ROOT="/Users/me/Code" cargo run --bin golazo-backend
```

Each project uses that directory's `.goal-manager` goals and run history. The project registry and last active project persist separately in `GOAL_MANAGER_APP_DATA_DIR`; Electron defaults this to its platform application-data directory. The app refuses to switch projects while a Codex run is active.

## Electron desktop app

Install Rust and the Node dependencies, then start the desktop development environment:

```bash
pnpm install
pnpm dev
```

Electron starts the Rust backend on `127.0.0.1:8765`, Vite serves the React renderer during development, and the desktop window uses a context-isolated preload bridge for the native folder picker. Node integration is disabled in the renderer.

Create an installable build on the current platform with:

```bash
pnpm dist
```

The packaging pipeline builds React, compiles a release-mode Rust binary, and places it in the Electron application resources. Build separately on macOS, Windows, and Linux because both the Rust backend and Electron runtime are platform-specific. Production signing and notarization credentials are intentionally not committed.

## Basic flow

```bash
curl -X POST http://127.0.0.1:8765/goals \
  -H 'content-type: application/json' \
  -d '{"goal_id":"first-release","title":"First release"}'

curl -X POST http://127.0.0.1:8765/goals/first-release/features \
  -H 'content-type: application/json' \
  -d '{"feature_id":"api","title":"Goal API"}'

curl -X POST http://127.0.0.1:8765/runs \
  -H 'content-type: application/json' \
  -d '{"goal_id":"first-release","prompt":"Implement the next planned API step"}'
```

`POST /runs` starts `codex exec --json` in the configured workspace and returns a run resource immediately. Poll `GET /runs/{run_id}` for JSONL events, final output, and completion status. Working directories are constrained to the configured workspace, and the API exposes only `read-only` and `workspace-write` sandboxes.

## Tracker Skill CLI

The Rust backend owns the runtime tracker implementation. The Codex skill includes a small portable Python CLI for direct repository automation and Markdown audit maintenance:

```bash
python skills/manage-implementation/scripts/implementation_tracker.py --help
```

The checked-in `.agents/skills/manage-implementation` discovery link exposes this skill to Codex CLI runs launched in the repository.

Feature status is explicit (`Planned`, `Blocked`, `Partial`, or `Done`). Completion rate is deterministic: completed steps divided by total steps. Slice logs roll over after 100 entries per Markdown file.
