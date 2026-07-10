from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class Run:
    id: str
    prompt: str
    cwd: str
    sandbox: str
    execution_prompt: str | None = None
    goal_id: str | None = None
    resumed_from: str | None = None
    status: str = "queued"
    created_at: str = field(default_factory=utc_now)
    started_at: str | None = None
    finished_at: str | None = None
    return_code: int | None = None
    thread_id: str | None = None
    llm_config: dict[str, str] = field(default_factory=lambda: {
        "provider": "openai",
        "model": "",
        "reasoning": "medium",
    })
    final_message: str | None = None
    error: str | None = None
    usage: dict[str, int] = field(default_factory=lambda: {
        "input_tokens": 0,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": 0,
    })
    events: list[dict[str, Any]] = field(default_factory=list)


class CodexRunner:
    def __init__(self, workspace_root: Path, executable: str = "codex", history_path: Path | None = None) -> None:
        self.workspace_root = workspace_root.resolve()
        self.executable = executable
        self.history_path = history_path
        self.settings_path = history_path.with_name("thread-settings.json") if history_path else None
        self.runs: dict[str, Run] = {}
        self._load_history()

    @staticmethod
    def _fallback_config() -> dict[str, str]:
        return {"provider": "openai", "model": "", "reasoning": "medium"}

    def _load_settings(self) -> dict[str, Any]:
        fallback = {"default_llm_config": self._fallback_config(), "threads": {}}
        if self.settings_path is None or not self.settings_path.exists():
            return fallback
        try:
            value = json.loads(self.settings_path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                return fallback
            config = {**self._fallback_config(), **value.get("default_llm_config", {})}
            threads = value.get("threads", {})
            return {"default_llm_config": config, "threads": threads if isinstance(threads, dict) else {}}
        except (OSError, ValueError, TypeError):
            return fallback

    def _save_settings(self, settings: dict[str, Any]) -> None:
        if self.settings_path is None:
            return
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".thread-settings.", dir=self.settings_path.parent, text=True)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(settings, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.settings_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def default_llm_config(self) -> dict[str, str]:
        return dict(self._load_settings()["default_llm_config"])

    def set_default_llm_config(self, config: dict[str, str]) -> dict[str, str]:
        settings = self._load_settings()
        settings["default_llm_config"] = dict(config)
        self._save_settings(settings)
        return dict(config)

    def _thread_exists(self, thread_id: str) -> bool:
        settings = self._load_settings()
        return thread_id in settings["threads"] or any(run.thread_id == thread_id for run in self.runs.values())

    def _ensure_thread(self, thread_id: str, prompt: str, config: dict[str, str]) -> None:
        settings = self._load_settings()
        if thread_id not in settings["threads"]:
            title = " ".join(prompt.split())[:80] or "Untitled thread"
            now = utc_now()
            settings["threads"][thread_id] = {
                "title": title,
                "llm_config": dict(config),
                "created_at": now,
                "updated_at": now,
            }
            self._save_settings(settings)

    def update_thread(
        self,
        thread_id: str,
        *,
        title: str | None = None,
        llm_config: dict[str, str] | None = None,
    ) -> dict[str, Any] | None:
        if not self._thread_exists(thread_id):
            return None
        settings = self._load_settings()
        record = settings["threads"].setdefault(thread_id, {
            "title": "Untitled thread",
            "llm_config": self.default_llm_config(),
            "created_at": utc_now(),
        })
        if title is not None:
            record["title"] = title.strip()
        if llm_config is not None:
            existing_provider = record.get("llm_config", {}).get("provider")
            record["llm_config"] = dict(llm_config)
            if existing_provider:
                record["llm_config"]["provider"] = existing_provider
        record["updated_at"] = utc_now()
        self._save_settings(settings)
        return self.get_thread(thread_id)

    def get_thread(self, thread_id: str) -> dict[str, Any] | None:
        return next((thread for thread in self.list_threads() if thread["id"] == thread_id), None)

    def list_threads(self) -> list[dict[str, Any]]:
        settings = self._load_settings()
        grouped: dict[str, list[Run]] = {}
        for run in self.runs.values():
            if run.thread_id:
                grouped.setdefault(run.thread_id, []).append(run)
        thread_ids = set(grouped) | set(settings["threads"])
        threads = []
        for thread_id in thread_ids:
            runs = grouped.get(thread_id, [])
            record = settings["threads"].get(thread_id, {})
            first = min(runs, key=lambda run: run.created_at) if runs else None
            last = max(runs, key=lambda run: run.created_at) if runs else None
            stored_config = record.get("llm_config") or (first.llm_config if first else self.default_llm_config())
            config = {**self._fallback_config(), **stored_config}
            threads.append({
                "id": thread_id,
                "title": record.get("title") or (" ".join(first.prompt.split())[:80] if first else "Untitled thread"),
                "llm_config": dict(config),
                "goal_id": last.goal_id if last else None,
                "run_count": len(runs),
                "created_at": record.get("created_at") or (first.created_at if first else utc_now()),
                "updated_at": record.get("updated_at") or (last.created_at if last else utc_now()),
            })
        return sorted(threads, key=lambda item: item["updated_at"], reverse=True)

    def _load_history(self) -> None:
        if self.history_path is None or not self.history_path.exists():
            return
        try:
            items = json.loads(self.history_path.read_text(encoding="utf-8"))
            for item in items:
                run = Run(**item)
                self.runs[run.id] = run
        except (OSError, ValueError, TypeError):
            return

    def _save_history(self) -> None:
        if self.history_path is None:
            return
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.history_path.with_suffix(".tmp")
        temporary.write_text(json.dumps([asdict(run) for run in self.runs.values()], indent=2), encoding="utf-8")
        temporary.replace(self.history_path)

    def resolve_cwd(self, value: str) -> Path:
        candidate = (self.workspace_root / value).resolve()
        if candidate != self.workspace_root and self.workspace_root not in candidate.parents:
            raise ValueError("working_directory must be inside the configured workspace")
        if not candidate.is_dir():
            raise ValueError("working_directory does not exist")
        return candidate

    def switch_workspace(self, workspace_root: Path, history_path: Path) -> None:
        if any(run.status in {"queued", "running"} for run in self.runs.values()):
            raise ValueError("cannot switch workspace while a Codex run is active")
        self.workspace_root = workspace_root.resolve()
        self.history_path = history_path
        self.settings_path = history_path.with_name("thread-settings.json")
        self.runs = {}
        self._load_history()

    def create(
        self,
        prompt: str,
        working_directory: str,
        sandbox: str,
        goal_id: str | None = None,
        thread_id: str | None = None,
        execution_prompt: str | None = None,
        llm_config: dict[str, str] | None = None,
    ) -> Run:
        cwd = self.resolve_cwd(working_directory)
        if thread_id:
            thread = self.get_thread(thread_id)
            effective_config = dict(llm_config or (thread["llm_config"] if thread else self.default_llm_config()))
            if thread:
                effective_config["provider"] = thread["llm_config"]["provider"]
            else:
                self._ensure_thread(thread_id, prompt, effective_config)
            if llm_config is not None and thread:
                self.update_thread(thread_id, llm_config=effective_config)
        else:
            effective_config = dict(llm_config or self.default_llm_config())
        run = Run(
            id=str(uuid.uuid4()),
            prompt=prompt,
            cwd=str(cwd),
            sandbox=sandbox,
            execution_prompt=execution_prompt,
            goal_id=goal_id,
            resumed_from=thread_id,
            thread_id=thread_id,
            llm_config=effective_config,
        )
        self.runs[run.id] = run
        asyncio.create_task(self._execute(run))
        return run

    async def _execute(self, run: Run) -> None:
        run.status = "running"
        run.started_at = utc_now()
        if shutil.which(self.executable) is None:
            run.status = "failed"
            run.error = f"Codex executable not found: {self.executable}"
            run.finished_at = utc_now()
            self._save_history()
            return
        try:
            skip_git_check = not any((parent / ".git").exists() for parent in (Path(run.cwd), *Path(run.cwd).parents))
            effective_prompt = run.execution_prompt or run.prompt
            if run.resumed_from:
                command = [self.executable, "exec", "resume", "--json"]
                if run.llm_config.get("model"):
                    command.extend(["--model", run.llm_config["model"]])
                command.extend(["--config", f'model_reasoning_effort="{run.llm_config["reasoning"]}"'])
                if skip_git_check:
                    command.append("--skip-git-repo-check")
                command.extend([run.resumed_from, effective_prompt])
            else:
                command = [self.executable, "exec", "--json", "--sandbox", run.sandbox]
                provider = run.llm_config.get("provider", "openai")
                if provider in {"ollama", "lmstudio"}:
                    command.extend(["--oss", "--local-provider", provider])
                if run.llm_config.get("model"):
                    command.extend(["--model", run.llm_config["model"]])
                command.extend(["--config", f'model_reasoning_effort="{run.llm_config["reasoning"]}"'])
                if skip_git_check:
                    command.append("--skip-git-repo-check")
                command.append(effective_prompt)
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=run.cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            assert process.stdout is not None
            assert process.stderr is not None
            stderr_task = asyncio.create_task(process.stderr.read())
            async for raw in process.stdout:
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    event = {"type": "unparsed", "text": raw.decode(errors="replace").rstrip()}
                run.events.append(event)
                run.events[:] = run.events[-1000:]
                if event.get("type") == "thread.started":
                    run.thread_id = event.get("thread_id")
                    if run.thread_id:
                        self._ensure_thread(run.thread_id, run.prompt, run.llm_config)
                if event.get("type") == "turn.completed":
                    usage = event.get("usage", {})
                    for key in ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"):
                        run.usage[key] = int(usage.get(key, 0) or 0)
                    run.usage["total_tokens"] = run.usage["input_tokens"] + run.usage["output_tokens"]
                item = event.get("item", {})
                if event.get("type") == "item.completed" and item.get("type") == "agent_message":
                    run.final_message = item.get("text")
            await process.wait()
            stderr = await stderr_task
            run.return_code = process.returncode
            if process.returncode == 0:
                run.status = "completed"
            else:
                run.status = "failed"
                run.error = stderr.decode(errors="replace")[-8000:] or "Codex exited with an error"
        except Exception as exc:  # keep background failures observable through the API
            run.status = "failed"
            run.error = str(exc)
        finally:
            run.finished_at = utc_now()
            self._save_history()

    def get(self, run_id: str) -> dict[str, Any] | None:
        run = self.runs.get(run_id)
        return asdict(run) if run else None

    def list(self) -> list[dict[str, Any]]:
        return [asdict(run) for run in reversed(list(self.runs.values()))]

    def usage(self) -> dict[str, Any]:
        totals = {
            "input_tokens": 0,
            "cached_input_tokens": 0,
            "output_tokens": 0,
            "reasoning_output_tokens": 0,
            "total_tokens": 0,
        }
        completed_runs = 0
        for run in self.runs.values():
            if run.status == "completed":
                completed_runs += 1
            for key in totals:
                totals[key] += run.usage[key]
        return {**totals, "runs": len(self.runs), "completed_runs": completed_runs}
