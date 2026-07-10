from pathlib import Path
import re
import time

from fastapi.testclient import TestClient

from golazo.main import create_app


def test_goal_api_flow(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with TestClient(create_app(tmp_path / "data", workspace)) as client:
        response = client.post("/goals", json={"goal_id": "v1", "title": "Version one"})
        assert response.status_code == 201
        response = client.post(
            "/goals/v1/features", json={"feature_id": "api", "title": "API", "status": "Planned"}
        )
        assert response.status_code == 201
        response = client.post(
            "/goals/v1/features/api/steps", json={"step_id": "health", "title": "Health endpoint"}
        )
        assert response.status_code == 201
        response = client.patch("/goals/v1/features/api/steps/health", json={"done": True})
        assert response.status_code == 200
        response = client.post(
            "/goals/v1/slices",
            json={"feature_id": "api", "summary": "Added health endpoint", "status": "Done"},
        )
        assert response.status_code == 201
        goal = client.get("/goals/v1").json()
        assert goal["progress"]["completion_rate"] == 100
        assert goal["features"][0]["status"] == "Done"


def test_goal_id_is_generated_from_title_with_hash(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with TestClient(create_app(tmp_path / "data", workspace)) as client:
        first = client.post("/goals", json={"title": "Ship Authentication!"}).json()
        second = client.post("/goals", json={"title": "Ship Authentication!"}).json()
    assert re.fullmatch(r"ship-authentication-[0-9a-f]{8}", first["goal_id"])
    assert re.fullmatch(r"ship-authentication-[0-9a-f]{8}", second["goal_id"])
    assert first["goal_id"] != second["goal_id"]


def test_run_rejects_directory_escape(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with TestClient(create_app(tmp_path / "data", workspace)) as client:
        response = client.post("/runs", json={"prompt": "Do work", "working_directory": "../"})
    assert response.status_code == 422
    assert "inside the configured workspace" in response.json()["detail"]


def test_validation_errors_are_http_errors(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with TestClient(create_app(tmp_path / "data", workspace)) as client:
        response = client.get("/goals/missing")
        assert response.status_code == 404
        response = client.post("/goals", json={"goal_id": "UPPER", "title": "Invalid"})
        assert response.status_code == 422


def test_codex_jsonl_run_is_captured(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    executable = tmp_path / "fake-codex"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "print(json.dumps({'type': 'thread.started', 'thread_id': 'thread-123'}))\n"
        "print(json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': ' '.join(sys.argv)}}))\n"
        "print(json.dumps({'type': 'turn.completed', 'usage': {'input_tokens': 10, 'cached_input_tokens': 4, 'output_tokens': 3, 'reasoning_output_tokens': 1}}))\n"
    )
    executable.chmod(0o755)
    app = create_app(tmp_path / "data", workspace)
    app.state.runner.executable = str(executable)
    with TestClient(app) as client:
        response = client.post("/runs", json={"prompt": "Do work"})
        assert response.status_code == 202
        run_id = response.json()["id"]
        for _ in range(100):
            run = client.get(f"/runs/{run_id}").json()
            if run["status"] in {"completed", "failed"}:
                break
            time.sleep(0.01)
    assert run["status"] == "completed"
    assert run["thread_id"] == "thread-123"
    assert "exec --json" in run["final_message"]
    assert run["usage"]["total_tokens"] == 13
    assert len(run["events"]) == 3
    with TestClient(create_app(tmp_path / "data", workspace)) as client:
        usage = client.get("/usage").json()
        assert usage["total_tokens"] == 13
        assert usage["cached_input_tokens"] == 4


def test_followup_resumes_thread(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    executable = tmp_path / "fake-codex"
    executable.write_text(
        "#!/usr/bin/env python3\nimport json, sys\n"
        "print(json.dumps({'type':'thread.started','thread_id':'thread-123'}))\n"
        "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':' '.join(sys.argv)}}))\n"
        "print(json.dumps({'type':'turn.completed','usage':{'input_tokens':2,'output_tokens':1}}))\n"
    )
    executable.chmod(0o755)
    app = create_app(tmp_path / "data", workspace)
    app.state.runner.executable = str(executable)
    with TestClient(app) as client:
        response = client.post("/runs", json={"prompt":"Follow up", "thread_id":"thread-123"})
        run_id = response.json()["id"]
        for _ in range(100):
            run = client.get(f"/runs/{run_id}").json()
            if run["status"] in {"completed", "failed"}: break
            time.sleep(.01)
        assert run["status"] == "completed"
        assert "exec resume --json" in run["final_message"]
        assert run["resumed_from"] == "thread-123"


def test_ui_is_served(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    with TestClient(create_app(tmp_path / "data", workspace)) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert "Golazo" in response.text


def test_filesystem_browser_and_workspace_switch(tmp_path: Path) -> None:
    browse_root = tmp_path / "projects"
    project_a = browse_root / "project-a"
    project_b = browse_root / "project-b"
    project_a.mkdir(parents=True)
    project_b.mkdir()
    with TestClient(create_app(project_a / ".goal-manager", project_a, browse_root)) as client:
        listing = client.get("/filesystem").json()
        assert [item["name"] for item in listing["directories"]] == ["project-a", "project-b"]
        assert listing["parent"] is None
        assert client.get("/filesystem", params={"path": str(tmp_path)}).status_code == 403

        client.post("/goals", json={"goal_id": "alpha", "title": "Alpha"})
        projects = client.get("/projects").json()
        assert len(projects) == 1
        assert projects[0]["active"] is True
        assert [goal["goal_id"] for goal in projects[0]["goals"]] == ["alpha"]
        project_a_id = projects[0]["id"]

        response = client.post("/workspace", json={"path": str(project_b)})
        assert response.status_code == 200
        assert response.json()["path"] == str(project_b)
        assert client.get("/goals").json() == []

        client.post("/goals", json={"goal_id": "beta", "title": "Beta"})
        projects = client.get("/projects").json()
        assert [project["name"] for project in projects] == ["project-a", "project-b"]
        assert next(project for project in projects if project["active"])["name"] == "project-b"

        response = client.post(f"/projects/{project_a_id}/activate")
        assert response.status_code == 200
        assert [goal["goal_id"] for goal in client.get("/goals").json()] == ["alpha"]


def test_project_registry_persists_active_project(tmp_path: Path) -> None:
    browse_root = tmp_path / "projects"
    project_a = browse_root / "project-a"
    project_b = browse_root / "project-b"
    project_a.mkdir(parents=True)
    project_b.mkdir()
    app_data = tmp_path / "app-data"

    with TestClient(create_app(project_a / ".goal-manager", project_a, browse_root, app_data)) as client:
        response = client.post("/projects", json={"path": str(project_b), "activate": True})
        assert response.status_code == 201
        assert response.json()["active"] is True

    with TestClient(create_app(workspace_root=None, browse_root=browse_root, app_data_dir=app_data)) as client:
        assert client.get("/workspace").json()["path"] == str(project_b)
        projects = client.get("/projects").json()
        assert len(projects) == 2
        assert next(project for project in projects if project["active"])["name"] == "project-b"


def test_project_display_name_can_be_renamed_and_persists(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    app_data = tmp_path / "app-data"
    with TestClient(create_app(workspace / ".goal-manager", workspace, tmp_path, app_data)) as client:
        project = client.get("/projects").json()[0]
        response = client.patch(f"/projects/{project['id']}", json={"name": "Research Lab"})
        assert response.status_code == 200
        assert response.json()["name"] == "Research Lab"
        assert response.json()["path"] == str(workspace)

    with TestClient(create_app(workspace / ".goal-manager", workspace, tmp_path, app_data)) as client:
        assert client.get("/projects").json()[0]["name"] == "Research Lab"


def test_default_and_per_thread_llm_configuration_and_thread_rename(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    executable = tmp_path / "fake-codex"
    executable.write_text(
        "#!/usr/bin/env python3\nimport json, sys\n"
        "print(json.dumps({'type':'thread.started','thread_id':'thread-configured'}))\n"
        "print(json.dumps({'type':'item.completed','item':{'type':'agent_message','text':' '.join(sys.argv)}}))\n"
        "print(json.dumps({'type':'turn.completed','usage':{'input_tokens':1,'output_tokens':1}}))\n"
    )
    executable.chmod(0o755)
    app = create_app(workspace / ".goal-manager", workspace)
    app.state.runner.executable = str(executable)

    with TestClient(app) as client:
        default_config = {"provider": "ollama", "model": "llama3.2", "reasoning": "high"}
        response = client.patch("/settings/default-llm", json=default_config)
        assert response.status_code == 200
        assert client.get("/settings/default-llm").json() == default_config

        response = client.post("/runs", json={"prompt": "Design the API"})
        run_id = response.json()["id"]
        for _ in range(100):
            run = client.get(f"/runs/{run_id}").json()
            if run["status"] in {"completed", "failed"}:
                break
            time.sleep(.01)
        assert run["status"] == "completed"
        assert "--oss --local-provider ollama" in run["final_message"]
        assert "--model llama3.2" in run["final_message"]
        assert 'model_reasoning_effort="high"' in run["final_message"]

        thread = client.get("/threads").json()[0]
        assert thread["id"] == "thread-configured"
        assert thread["title"] == "Design the API"
        assert thread["llm_config"] == default_config

        updated_config = {"provider": "openai", "model": "gpt-5.4", "reasoning": "low"}
        response = client.patch(
            "/threads/thread-configured",
            json={"title": "API design", "llm_config": updated_config},
        )
        assert response.status_code == 200
        assert response.json()["title"] == "API design"
        assert response.json()["llm_config"] == {**updated_config, "provider": "ollama"}

        response = client.post(
            "/runs",
            json={"prompt": "Continue", "thread_id": "thread-configured", "llm_config": updated_config},
        )
        followup_id = response.json()["id"]
        for _ in range(100):
            followup = client.get(f"/runs/{followup_id}").json()
            if followup["status"] in {"completed", "failed"}:
                break
            time.sleep(.01)
        assert followup["status"] == "completed"
        assert "exec resume --json --model gpt-5.4" in followup["final_message"]
        assert 'model_reasoning_effort="low"' in followup["final_message"]
        assert followup["llm_config"]["provider"] == "ollama"
