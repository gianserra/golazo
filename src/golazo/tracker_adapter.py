"""Load the skill's deterministic tracker as the service's single source of logic."""

from __future__ import annotations

import importlib.util
from pathlib import Path


repository_tracker = (
    Path(__file__).resolve().parents[2]
    / "skills"
    / "manage-implementation"
    / "scripts"
    / "implementation_tracker.py"
)
TRACKER_PATH = repository_tracker if repository_tracker.exists() else Path(__file__).with_name("implementation_tracker.py")

spec = importlib.util.spec_from_file_location("implementation_tracker", TRACKER_PATH)
if spec is None or spec.loader is None:
    raise RuntimeError(f"Unable to load implementation tracker at {TRACKER_PATH}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

Tracker = module.Tracker
TrackerError = module.TrackerError
VALID_STATUSES = module.VALID_STATUSES
generate_goal_id = module.generate_goal_id

__all__ = ["Tracker", "TrackerError", "VALID_STATUSES", "generate_goal_id"]
