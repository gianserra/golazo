from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def project_id(path: Path) -> str:
    resolved = path.resolve()
    slug = re.sub(r"[^a-z0-9]+", "-", (resolved.name or "project").lower()).strip("-") or "project"
    digest = hashlib.sha256(str(resolved).encode()).hexdigest()[:8]
    return f"{slug[:54].rstrip('-')}-{digest}"


class ProjectRegistry:
    def __init__(self, path: Path) -> None:
        self.path = path

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"active_project_id": None, "projects": []}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(value, list):
                return {"active_project_id": None, "projects": value}
            if isinstance(value, dict) and isinstance(value.get("projects"), list):
                return value
            return {"active_project_id": None, "projects": []}
        except (OSError, json.JSONDecodeError):
            return {"active_project_id": None, "projects": []}

    def _save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".projects.", dir=self.path.parent, text=True)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(state, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def add(self, path: Path, *, activate: bool = False) -> dict[str, Any]:
        resolved = path.resolve()
        identifier = project_id(resolved)
        state = self._load()
        projects = state["projects"]
        record = next((item for item in projects if item["id"] == identifier), None)
        if record is None:
            now = utc_now()
            record = {
                "id": identifier,
                "name": resolved.name or str(resolved),
                "path": str(resolved),
                "created_at": now,
                "last_opened_at": now,
            }
            projects.append(record)
        if activate:
            record["last_opened_at"] = utc_now()
            state["active_project_id"] = identifier
        self._save(state)
        return dict(record)

    def get(self, identifier: str) -> dict[str, Any] | None:
        return next((dict(item) for item in self._load()["projects"] if item["id"] == identifier), None)

    def active(self) -> dict[str, Any] | None:
        state = self._load()
        identifier = state.get("active_project_id")
        return next((dict(item) for item in state["projects"] if item["id"] == identifier), None)

    def list(self) -> list[dict[str, Any]]:
        return [dict(item) for item in self._load()["projects"]]

    def remove(self, identifier: str) -> bool:
        state = self._load()
        projects = state["projects"]
        remaining = [item for item in projects if item["id"] != identifier]
        if len(remaining) == len(projects):
            return False
        state["projects"] = remaining
        if state.get("active_project_id") == identifier:
            state["active_project_id"] = None
        self._save(state)
        return True

    def rename(self, identifier: str, name: str) -> dict[str, Any] | None:
        state = self._load()
        record = next((item for item in state["projects"] if item["id"] == identifier), None)
        if record is None:
            return None
        record["name"] = name.strip()
        self._save(state)
        return dict(record)
