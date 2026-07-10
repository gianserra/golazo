from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.staticfiles import StaticFiles

from .codex_runner import CodexRunner
from .models import (
    CodexRunCreate,
    FeatureCreate,
    GoalCreate,
    LLMConfig,
    NameUpdate,
    ProjectCreate,
    SliceCreate,
    StatusUpdate,
    StepCreate,
    StepUpdate,
    ThreadUpdate,
    WorkspaceSelect,
)
from .projects import ProjectRegistry
from .tracker_adapter import Tracker, TrackerError, generate_goal_id


def create_app(
    data_dir: Path | None = None,
    workspace_root: Path | None = None,
    browse_root: Path | None = None,
    app_data_dir: Path | None = None,
) -> FastAPI:
    configured_root = workspace_root or (Path(os.environ["CODEX_WORKSPACE_ROOT"]) if "CODEX_WORKSPACE_ROOT" in os.environ else None)
    root = (configured_root or Path.cwd()).expanduser().resolve()
    filesystem_root = (browse_root or Path(os.getenv("GOAL_MANAGER_BROWSE_ROOT", Path.home()))).expanduser().resolve()
    tracking_root = (data_dir or Path(os.getenv("GOAL_MANAGER_DATA_DIR", root / ".goal-manager"))).resolve()
    default_app_data = tracking_root.parent / ".golazo-app"
    registry_root = (app_data_dir or Path(os.getenv("GOAL_MANAGER_APP_DATA_DIR", default_app_data))).expanduser().resolve()
    projects = ProjectRegistry(registry_root / "projects.json")
    remembered_project = projects.active()
    if configured_root is None and remembered_project and Path(remembered_project["path"]).is_dir():
        root = Path(remembered_project["path"]).resolve()
        tracking_root = root / ".goal-manager"
    tracker = Tracker(tracking_root)
    runner = CodexRunner(root, os.getenv("CODEX_EXECUTABLE", "codex"), tracking_root / "run-history.json")
    active_project = projects.add(root, activate=True)
    app = FastAPI(title="Golazo", version="0.1.0")
    app.state.tracker = tracker
    app.state.runner = runner
    app.state.browse_root = filesystem_root
    app.state.projects = projects
    app.state.active_project_id = active_project["id"]

    def call(method: str, *args, **kwargs):
        try:
            return getattr(app.state.tracker, method)(*args, **kwargs)
        except TrackerError as exc:
            raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

    def filesystem_path(value: str | None) -> Path:
        candidate = Path(value).expanduser().resolve() if value else app.state.browse_root
        allowed = app.state.browse_root
        if candidate != allowed and allowed not in candidate.parents:
            raise HTTPException(status_code=403, detail="path is outside the configured browsing root")
        if not candidate.is_dir():
            raise HTTPException(status_code=404, detail="directory not found")
        return candidate

    def project_summary(record: dict) -> dict:
        path = Path(record["path"])
        exists = path.is_dir()
        goals = []
        usage = {"input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0, "reasoning_output_tokens": 0, "total_tokens": 0, "runs": 0, "completed_runs": 0}
        if exists:
            try:
                goals = Tracker(path / ".goal-manager").list_goals()
                usage = CodexRunner(path, history_path=path / ".goal-manager" / "run-history.json").usage()
            except (OSError, TrackerError):
                goals = []
        return {
            **record,
            "exists": exists,
            "active": record["id"] == app.state.active_project_id,
            "goals": [
                {"goal_id": goal["goal_id"], "title": goal["title"], "progress": goal["progress"], "features": len(goal["features"])}
                for goal in goals
            ],
            "usage": usage,
        }

    def activate_project(record: dict) -> dict:
        selected = filesystem_path(record["path"])
        tracking = selected / ".goal-manager"
        try:
            runner.switch_workspace(selected, tracking / "run-history.json")
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        app.state.tracker = Tracker(tracking)
        app.state.active_project_id = record["id"]
        app.state.projects.add(selected, activate=True)
        return project_summary(record)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/workspace")
    def get_workspace():
        return {
            "path": str(runner.workspace_root),
            "name": runner.workspace_root.name or str(runner.workspace_root),
            "browse_root": str(app.state.browse_root),
            "project_id": app.state.active_project_id,
        }

    @app.get("/projects")
    def list_projects():
        return [project_summary(record) for record in app.state.projects.list()]

    @app.post("/projects", status_code=status.HTTP_201_CREATED)
    def add_project(body: ProjectCreate):
        selected = filesystem_path(body.path)
        record = app.state.projects.add(selected, activate=body.activate)
        return activate_project(record) if body.activate else project_summary(record)

    @app.post("/projects/{project_id}/activate")
    def select_project(project_id: str):
        record = app.state.projects.get(project_id)
        if record is None:
            raise HTTPException(status_code=404, detail="project not found")
        return activate_project(record)

    @app.patch("/projects/{project_id}")
    def rename_project(project_id: str, body: NameUpdate):
        record = app.state.projects.rename(project_id, body.name)
        if record is None:
            raise HTTPException(status_code=404, detail="project not found")
        return project_summary(record)

    @app.delete("/projects/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
    def remove_project(project_id: str):
        if project_id == app.state.active_project_id:
            raise HTTPException(status_code=409, detail="cannot remove the active project")
        if not app.state.projects.remove(project_id):
            raise HTTPException(status_code=404, detail="project not found")

    @app.get("/filesystem")
    def browse_filesystem(path: str | None = None, show_hidden: bool = False):
        directory = filesystem_path(path)
        entries = []
        try:
            children = sorted(directory.iterdir(), key=lambda item: item.name.casefold())
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail="directory is not readable") from exc
        for child in children:
            if not show_hidden and child.name.startswith("."):
                continue
            try:
                resolved = child.resolve()
                inside_root = resolved == app.state.browse_root or app.state.browse_root in resolved.parents
                if child.is_dir() and inside_root:
                    entries.append({"name": child.name, "path": str(resolved), "symlink": child.is_symlink()})
            except (OSError, PermissionError):
                continue
        parent = None if directory == app.state.browse_root else str(directory.parent)
        return {"path": str(directory), "parent": parent, "directories": entries}

    @app.post("/workspace")
    def select_workspace(body: WorkspaceSelect):
        selected = filesystem_path(body.path)
        record = app.state.projects.add(selected, activate=True)
        activate_project(record)
        return {"path": str(selected), "name": selected.name or str(selected), "browse_root": str(app.state.browse_root)}

    @app.get("/goals")
    def list_goals():
        return call("list_goals")

    @app.post("/goals", status_code=status.HTTP_201_CREATED)
    def create_goal(body: GoalCreate):
        goal_id = body.goal_id or generate_goal_id(body.title)
        return call("create_goal", goal_id, body.title, body.description)

    @app.get("/goals/{goal_id}")
    def get_goal(goal_id: str):
        return call("get_goal", goal_id)

    @app.post("/goals/{goal_id}/features", status_code=status.HTTP_201_CREATED)
    def add_feature(goal_id: str, body: FeatureCreate):
        return call("add_feature", goal_id, body.feature_id, body.title, body.description, body.status)

    @app.patch("/goals/{goal_id}/features/{feature_id}")
    def set_feature_status(goal_id: str, feature_id: str, body: StatusUpdate):
        return call("set_feature_status", goal_id, feature_id, body.status)

    @app.post("/goals/{goal_id}/features/{feature_id}/steps", status_code=status.HTTP_201_CREATED)
    def add_step(goal_id: str, feature_id: str, body: StepCreate):
        return call("add_step", goal_id, feature_id, body.step_id, body.title, body.done)

    @app.patch("/goals/{goal_id}/features/{feature_id}/steps/{step_id}")
    def set_step(goal_id: str, feature_id: str, step_id: str, body: StepUpdate):
        return call("set_step", goal_id, feature_id, step_id, body.done)

    @app.post("/goals/{goal_id}/slices", status_code=status.HTTP_201_CREATED)
    def add_slice(goal_id: str, body: SliceCreate):
        return call("add_slice", goal_id, body.feature_id, body.summary, body.status, body.evidence)

    @app.post("/goals/{goal_id}/validate")
    def validate_goal(goal_id: str):
        return call("validate", goal_id)

    @app.post("/runs", status_code=status.HTTP_202_ACCEPTED)
    async def create_run(body: CodexRunCreate, response: Response):
        prompt = body.prompt
        if body.goal_id:
            call("get_goal", body.goal_id)
            prompt = (
                f"Use $manage-implementation and work on goal '{body.goal_id}'. "
                f"Record implementation slices and step completion as work proceeds.\n\n{body.prompt}"
            )
        try:
            run = runner.create(
                body.prompt,
                body.working_directory,
                body.sandbox,
                goal_id=body.goal_id,
                thread_id=body.thread_id,
                execution_prompt=prompt,
                llm_config=body.llm_config.model_dump() if body.llm_config else None,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        response.headers["Location"] = f"/runs/{run.id}"
        return runner.get(run.id)

    @app.get("/runs")
    def list_runs():
        return runner.list()

    @app.get("/settings/default-llm")
    def get_default_llm_config():
        return runner.default_llm_config()

    @app.patch("/settings/default-llm")
    def set_default_llm_config(body: LLMConfig):
        return runner.set_default_llm_config(body.model_dump())

    @app.get("/threads")
    def list_threads():
        return runner.list_threads()

    @app.patch("/threads/{thread_id}")
    def update_thread(thread_id: str, body: ThreadUpdate):
        if body.title is None and body.llm_config is None:
            raise HTTPException(status_code=422, detail="title or llm_config is required")
        thread = runner.update_thread(
            thread_id,
            title=body.title,
            llm_config=body.llm_config.model_dump() if body.llm_config else None,
        )
        if thread is None:
            raise HTTPException(status_code=404, detail="thread not found")
        return thread

    @app.get("/usage")
    def get_usage():
        return runner.usage()

    @app.get("/runs/{run_id}")
    def get_run(run_id: str):
        run = runner.get(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return run

    configured_frontend = os.getenv("CODEX_FRONTEND_DIR")
    react_frontend = Path(__file__).with_name("static-react")
    static_dir = Path(configured_frontend).resolve() if configured_frontend else (
        react_frontend if (react_frontend / "index.html").exists() else Path(__file__).with_name("static")
    )
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")
    return app


app = create_app()
