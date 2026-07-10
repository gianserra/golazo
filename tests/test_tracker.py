from pathlib import Path

import pytest

from golazo.tracker_adapter import Tracker, TrackerError


def test_progress_status_and_markdown(tmp_path: Path) -> None:
    tracker = Tracker(tmp_path)
    tracker.create_goal("release", "Release", "Ship the first release")
    tracker.add_feature("release", "api", "API")
    tracker.add_step("release", "api", "routes", "Create routes")
    tracker.add_step("release", "api", "tests", "Test routes")
    tracker.set_step("release", "api", "routes", True)
    item = tracker.add_slice("release", "api", "Added routes", "Partial", ["src/api.py"])

    goal = tracker.get_goal("release")
    assert item["id"] == "slice-000001"
    assert goal["progress"] == {"completed_steps": 1, "total_steps": 2, "completion_rate": 50}
    assert goal["features"][0]["status"] == "Partial"
    assert goal["features"][0]["slice_count"] == 1
    assert tracker.validate("release")["valid"] is True
    markdown = (tmp_path / "release" / "implementation.md").read_text()
    assert "1 of 2 steps complete (50%)." in markdown
    assert "[slices-001.md](./slices-001.md)" in markdown


def test_slice_files_roll_over_at_100(tmp_path: Path) -> None:
    tracker = Tracker(tmp_path)
    tracker.create_goal("rollover", "Rollover")
    tracker.add_feature("rollover", "core", "Core")
    for number in range(101):
        tracker.add_slice("rollover", "core", f"Slice {number + 1}")

    goal = tracker.get_goal("rollover")
    assert goal["slice_files"] == ["slices-001.md", "slices-002.md"]
    assert goal["slice_count"] == 101
    assert goal["slices"][-1]["id"] == "slice-000101"
    assert tracker.validate("rollover")["slices"] == 101


def test_rejects_duplicate_feature(tmp_path: Path) -> None:
    tracker = Tracker(tmp_path)
    tracker.create_goal("goal", "Goal")
    tracker.add_feature("goal", "api", "API")
    with pytest.raises(TrackerError) as error:
        tracker.add_feature("goal", "api", "Duplicate")
    assert error.value.status_code == 409
