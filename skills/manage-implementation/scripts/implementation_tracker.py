#!/usr/bin/env python3
"""Deterministic Markdown-backed implementation tracker (standard library only)."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import fcntl
import json
import os
import re
import secrets
import tempfile
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


VALID_STATUSES = ("Planned", "Blocked", "Partial", "Done")
MAX_SLICES_PER_FILE = 100
MARKER = "codex-goal-manager"
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


class TrackerError(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.status_code = status_code


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def generate_goal_id(title: str) -> str:
    """Create a readable, unique ID such as ``ship-auth-4fa91c2e``."""
    normalized = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-") or "goal"
    digest = hashlib.sha256(f"{title}\0{secrets.token_hex(16)}".encode()).hexdigest()[:8]
    slug = slug[:54].rstrip("-") or "goal"
    return f"{slug}-{digest}"


def _validate_id(value: str, label: str) -> None:
    if not ID_PATTERN.fullmatch(value):
        raise TrackerError(f"invalid {label}: use 1-63 lowercase letters, digits, or hyphens", 422)


def _validate_status(value: str) -> None:
    if value not in VALID_STATUSES:
        raise TrackerError(f"invalid status: expected one of {', '.join(VALID_STATUSES)}", 422)


def _escape(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _metadata_line(data: dict[str, Any]) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return f"<!-- {MARKER}:{payload} -->"


def _parse(path: Path, kind: str) -> dict[str, Any]:
    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
    except (FileNotFoundError, IndexError) as exc:
        raise TrackerError(f"missing or empty {kind} file: {path}", 404) from exc
    prefix = f"<!-- {MARKER}:"
    if not first_line.startswith(prefix) or not first_line.endswith(" -->"):
        raise TrackerError(f"invalid metadata marker in {path}")
    try:
        data = json.loads(first_line[len(prefix) : -4])
    except json.JSONDecodeError as exc:
        raise TrackerError(f"invalid metadata JSON in {path}: {exc}") from exc
    if data.get("kind") != kind or data.get("version") != 1:
        raise TrackerError(f"unsupported {kind} metadata in {path}")
    return data


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _feature_progress(feature: dict[str, Any]) -> dict[str, int]:
    total = len(feature["steps"])
    completed = sum(1 for step in feature["steps"] if step["done"])
    percent = round(completed * 100 / total) if total else 0
    return {"completed_steps": completed, "total_steps": total, "completion_rate": percent}


def _goal_progress(features: list[dict[str, Any]]) -> dict[str, int]:
    total = sum(len(feature["steps"]) for feature in features)
    completed = sum(sum(1 for step in feature["steps"] if step["done"]) for feature in features)
    percent = round(completed * 100 / total) if total else 0
    return {"completed_steps": completed, "total_steps": total, "completion_rate": percent}


def _render_goal(data: dict[str, Any]) -> str:
    progress = _goal_progress(data["features"])
    lines = [
        _metadata_line(data),
        "",
        f"# {data['title']}",
        "",
        data.get("description", ""),
        "",
        "## Progress",
        "",
        f"{progress['completed_steps']} of {progress['total_steps']} steps complete ({progress['completion_rate']}%).",
        "",
        "## Features",
        "",
        "| Feature | Status | Steps | Completion |",
        "| --- | --- | ---: | ---: |",
    ]
    for feature in data["features"]:
        item = _feature_progress(feature)
        lines.append(
            f"| `{_escape(feature['id'])}` — {_escape(feature['title'])} | {feature['status']} | "
            f"{item['completed_steps']}/{item['total_steps']} | {item['completion_rate']}% |"
        )
    if not data["features"]:
        lines.append("| _No features yet_ | Planned | 0/0 | 0% |")
    for feature in data["features"]:
        lines.extend(["", f"### {feature['title']} (`{feature['id']}`)", "", feature.get("description", ""), ""])
        if feature["steps"]:
            for step in feature["steps"]:
                mark = "x" if step["done"] else " "
                lines.append(f"- [{mark}] `{step['id']}` — {step['title']}")
        else:
            lines.append("_No implementation steps yet._")
    lines.extend(["", "## Slice audit files", ""])
    files = data.get("slice_files", [])
    lines.extend(f"- [{name}](./{name})" for name in files)
    if not files:
        lines.append("_No implementation slices yet._")
    return "\n".join(lines).rstrip() + "\n"


def _render_slices(data: dict[str, Any]) -> str:
    lines = [
        _metadata_line(data),
        "",
        f"# Implementation slices {data['index']:03d}",
        "",
        f"Goal: `{data['goal_id']}` · Entries: {len(data['slices'])}/{MAX_SLICES_PER_FILE}",
    ]
    for item in data["slices"]:
        lines.extend(
            [
                "",
                f"## {item['id']} — {item['feature_id']}",
                "",
                f"- Timestamp: {item['at']}",
                f"- Status after slice: {item['status']}",
                f"- Summary: {item['summary']}",
                "- Evidence:",
            ]
        )
        evidence = item.get("evidence", [])
        lines.extend(f"  - {entry}" for entry in evidence)
        if not evidence:
            lines.append("  - _None recorded._")
    return "\n".join(lines).rstrip() + "\n"


class Tracker:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @contextlib.contextmanager
    def _lock(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / ".lock").open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _directory(self, goal_id: str) -> Path:
        _validate_id(goal_id, "goal_id")
        return self.root / goal_id

    def _read_goal(self, goal_id: str) -> dict[str, Any]:
        return _parse(self._directory(goal_id) / "implementation.md", "goal")

    @staticmethod
    def _feature(data: dict[str, Any], feature_id: str) -> dict[str, Any]:
        for feature in data["features"]:
            if feature["id"] == feature_id:
                return feature
        raise TrackerError(f"feature not found: {feature_id}", 404)

    def _save_goal(self, goal_id: str, data: dict[str, Any]) -> None:
        data["updated_at"] = utc_now()
        _write_atomic(self._directory(goal_id) / "implementation.md", _render_goal(data))

    def create_goal(self, goal_id: str, title: str, description: str = "") -> dict[str, Any]:
        directory = self._directory(goal_id)
        with self._lock():
            path = directory / "implementation.md"
            if path.exists():
                raise TrackerError(f"goal already exists: {goal_id}", 409)
            now = utc_now()
            data = {
                "version": 1,
                "kind": "goal",
                "goal_id": goal_id,
                "title": title,
                "description": description,
                "created_at": now,
                "updated_at": now,
                "features": [],
                "slice_files": [],
            }
            _write_atomic(path, _render_goal(data))
            return self._enrich(data, [])

    def list_goals(self) -> list[dict[str, Any]]:
        if not self.root.exists():
            return []
        result = []
        with self._lock():
            for path in sorted(self.root.glob("*/implementation.md")):
                data = _parse(path, "goal")
                result.append(self._enrich(data, self._read_slices(data["goal_id"], data)))
        return result

    def _read_slices(self, goal_id: str, data: dict[str, Any]) -> list[dict[str, Any]]:
        slices: list[dict[str, Any]] = []
        for name in data.get("slice_files", []):
            slices.extend(_parse(self._directory(goal_id) / name, "slices")["slices"])
        return slices

    def _enrich(self, data: dict[str, Any], slices: list[dict[str, Any]]) -> dict[str, Any]:
        copy = json.loads(json.dumps(data))
        for feature in copy["features"]:
            feature["progress"] = _feature_progress(feature)
            feature["slice_count"] = sum(1 for item in slices if item["feature_id"] == feature["id"])
        copy["progress"] = _goal_progress(copy["features"])
        copy["slice_count"] = len(slices)
        copy["slices"] = slices
        return copy

    def get_goal(self, goal_id: str) -> dict[str, Any]:
        with self._lock():
            data = self._read_goal(goal_id)
            return self._enrich(data, self._read_slices(goal_id, data))

    def add_feature(
        self, goal_id: str, feature_id: str, title: str, description: str = "", status: str = "Planned"
    ) -> dict[str, Any]:
        _validate_id(feature_id, "feature_id")
        _validate_status(status)
        with self._lock():
            data = self._read_goal(goal_id)
            if any(item["id"] == feature_id for item in data["features"]):
                raise TrackerError(f"feature already exists: {feature_id}", 409)
            data["features"].append(
                {"id": feature_id, "title": title, "description": description, "status": status, "steps": []}
            )
            self._save_goal(goal_id, data)
            return self._enrich(data, self._read_slices(goal_id, data))

    def set_feature_status(self, goal_id: str, feature_id: str, status: str) -> dict[str, Any]:
        _validate_status(status)
        with self._lock():
            data = self._read_goal(goal_id)
            self._feature(data, feature_id)["status"] = status
            self._save_goal(goal_id, data)
            return self._enrich(data, self._read_slices(goal_id, data))

    def add_step(
        self, goal_id: str, feature_id: str, step_id: str, title: str, done: bool = False
    ) -> dict[str, Any]:
        _validate_id(step_id, "step_id")
        with self._lock():
            data = self._read_goal(goal_id)
            feature = self._feature(data, feature_id)
            if any(item["id"] == step_id for item in feature["steps"]):
                raise TrackerError(f"step already exists: {step_id}", 409)
            feature["steps"].append({"id": step_id, "title": title, "done": bool(done)})
            self._save_goal(goal_id, data)
            return self._enrich(data, self._read_slices(goal_id, data))

    def set_step(self, goal_id: str, feature_id: str, step_id: str, done: bool) -> dict[str, Any]:
        with self._lock():
            data = self._read_goal(goal_id)
            feature = self._feature(data, feature_id)
            for step in feature["steps"]:
                if step["id"] == step_id:
                    step["done"] = bool(done)
                    self._save_goal(goal_id, data)
                    return self._enrich(data, self._read_slices(goal_id, data))
            raise TrackerError(f"step not found: {step_id}", 404)

    def add_slice(
        self,
        goal_id: str,
        feature_id: str,
        summary: str,
        status: str | None = None,
        evidence: list[str] | None = None,
    ) -> dict[str, Any]:
        if status is not None:
            _validate_status(status)
        with self._lock():
            data = self._read_goal(goal_id)
            feature = self._feature(data, feature_id)
            if status is not None:
                feature["status"] = status
            current_status = feature["status"]
            files = data.setdefault("slice_files", [])
            slice_data: dict[str, Any] | None = None
            if files:
                candidate = _parse(self._directory(goal_id) / files[-1], "slices")
                if len(candidate["slices"]) < MAX_SLICES_PER_FILE:
                    slice_data = candidate
            if slice_data is None:
                index = len(files) + 1
                name = f"slices-{index:03d}.md"
                files.append(name)
                slice_data = {
                    "version": 1,
                    "kind": "slices",
                    "goal_id": goal_id,
                    "index": index,
                    "slices": [],
                }
            sequence = sum(
                len(_parse(self._directory(goal_id) / name, "slices")["slices"])
                for name in files[:-1]
            ) + len(slice_data["slices"]) + 1
            item = {
                "id": f"slice-{sequence:06d}",
                "feature_id": feature_id,
                "at": utc_now(),
                "summary": summary,
                "status": current_status,
                "evidence": list(evidence or []),
            }
            slice_data["slices"].append(item)
            name = files[slice_data["index"] - 1]
            _write_atomic(self._directory(goal_id) / name, _render_slices(slice_data))
            self._save_goal(goal_id, data)
            return item

    def validate(self, goal_id: str) -> dict[str, Any]:
        with self._lock():
            data = self._read_goal(goal_id)
            seen_features: set[str] = set()
            seen_steps: set[tuple[str, str]] = set()
            seen_slices: set[str] = set()
            for feature in data["features"]:
                _validate_id(feature["id"], "feature_id")
                _validate_status(feature["status"])
                if feature["id"] in seen_features:
                    raise TrackerError(f"duplicate feature: {feature['id']}")
                seen_features.add(feature["id"])
                for step in feature["steps"]:
                    key = (feature["id"], step["id"])
                    if key in seen_steps:
                        raise TrackerError(f"duplicate step in feature {feature['id']}: {step['id']}")
                    seen_steps.add(key)
            expected = 1
            count = 0
            for index, name in enumerate(data.get("slice_files", []), 1):
                if name != f"slices-{index:03d}.md":
                    raise TrackerError(f"unexpected slice filename or order: {name}")
                part = _parse(self._directory(goal_id) / name, "slices")
                if part["goal_id"] != goal_id or part["index"] != index:
                    raise TrackerError(f"slice file identity mismatch: {name}")
                if len(part["slices"]) > MAX_SLICES_PER_FILE:
                    raise TrackerError(f"slice file exceeds {MAX_SLICES_PER_FILE} entries: {name}")
                for item in part["slices"]:
                    if item["id"] != f"slice-{expected:06d}":
                        raise TrackerError(f"non-contiguous slice id: {item['id']}")
                    if item["id"] in seen_slices or item["feature_id"] not in seen_features:
                        raise TrackerError(f"invalid slice reference: {item['id']}")
                    seen_slices.add(item["id"])
                    _validate_status(item["status"])
                    expected += 1
                    count += 1
            return {"valid": True, "goal_id": goal_id, "features": len(seen_features), "slices": count}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".goal-manager", help="tracking data directory")
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-goal")
    create.add_argument("goal_id"); create.add_argument("title"); create.add_argument("--description", default="")
    feature = commands.add_parser("add-feature")
    feature.add_argument("goal_id"); feature.add_argument("feature_id"); feature.add_argument("title")
    feature.add_argument("--description", default=""); feature.add_argument("--status", choices=VALID_STATUSES, default="Planned")
    status = commands.add_parser("set-status")
    status.add_argument("goal_id"); status.add_argument("feature_id"); status.add_argument("status", choices=VALID_STATUSES)
    step = commands.add_parser("add-step")
    step.add_argument("goal_id"); step.add_argument("feature_id"); step.add_argument("step_id"); step.add_argument("title")
    complete = commands.add_parser("set-step")
    complete.add_argument("goal_id"); complete.add_argument("feature_id"); complete.add_argument("step_id")
    complete.add_argument("state", choices=("done", "open"))
    slice_cmd = commands.add_parser("add-slice")
    slice_cmd.add_argument("goal_id"); slice_cmd.add_argument("feature_id"); slice_cmd.add_argument("summary")
    slice_cmd.add_argument("--status", choices=VALID_STATUSES); slice_cmd.add_argument("--evidence", action="append", default=[])
    show = commands.add_parser("show"); show.add_argument("goal_id")
    validate = commands.add_parser("validate"); validate.add_argument("goal_id")
    commands.add_parser("list")
    return parser


def main() -> None:
    args = _parser().parse_args()
    tracker = Tracker(args.root)
    try:
        if args.command == "create-goal": result = tracker.create_goal(args.goal_id, args.title, args.description)
        elif args.command == "add-feature": result = tracker.add_feature(args.goal_id, args.feature_id, args.title, args.description, args.status)
        elif args.command == "set-status": result = tracker.set_feature_status(args.goal_id, args.feature_id, args.status)
        elif args.command == "add-step": result = tracker.add_step(args.goal_id, args.feature_id, args.step_id, args.title)
        elif args.command == "set-step": result = tracker.set_step(args.goal_id, args.feature_id, args.step_id, args.state == "done")
        elif args.command == "add-slice": result = tracker.add_slice(args.goal_id, args.feature_id, args.summary, args.status, args.evidence)
        elif args.command == "show": result = tracker.get_goal(args.goal_id)
        elif args.command == "validate": result = tracker.validate(args.goal_id)
        else: result = tracker.list_goals()
    except TrackerError as exc:
        raise SystemExit(f"error: {exc}") from exc
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
