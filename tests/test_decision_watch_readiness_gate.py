from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from scripts.check_decision_watch_readiness import (
    evaluate_from_paths,
    evaluate_phase2_readiness,
)


def test_phase2_readiness_accepts_only_same_snapshot() -> None:
    daily = {"snapshot_id": "snapshot-current"}
    same = {"source": {"snapshot_id": "snapshot-current"}}
    stale = {"source": {"snapshot_id": "snapshot-old"}}

    ready = evaluate_phase2_readiness(daily=daily, phase2=same)
    assert ready["ready"] is True
    assert ready["reason"] == "same_snapshot_ready"

    blocked = evaluate_phase2_readiness(daily=daily, phase2=stale)
    assert blocked["ready"] is False
    assert blocked["reason"] == (
        "phase2_snapshot_not_ready:snapshot-old:snapshot-current"
    )

    missing = evaluate_phase2_readiness(daily=daily, phase2=None)
    assert missing["ready"] is False
    assert missing["reason"] == "phase2_artifact_missing"


def test_phase2_readiness_cli_writes_fail_success_outputs(tmp_path: Path) -> None:
    root = tmp_path
    research = root / "artifacts" / "research"
    research.mkdir(parents=True)
    daily = research / "daily_research.json"
    phase2 = research / "probability_calibration_2.json"
    output = root / "github_output"

    daily.write_text(
        json.dumps({"snapshot_id": "snapshot-current"}),
        encoding="utf-8",
    )
    phase2.write_text(
        json.dumps({"source": {"snapshot_id": "snapshot-old"}}),
        encoding="utf-8",
    )

    project = Path(__file__).resolve().parents[1]
    command = [
        sys.executable,
        str(project / "scripts" / "check_decision_watch_readiness.py"),
        "--root",
        str(root),
        "--github-output",
        str(output),
    ]
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"ready": false' in completed.stdout.lower()
    assert output.read_text(encoding="utf-8") == (
        "ready=false\n"
        "reason=phase2_snapshot_not_ready:snapshot-old:snapshot-current\n"
    )

    phase2.write_text(
        json.dumps({"source": {"snapshot_id": "snapshot-current"}}),
        encoding="utf-8",
    )
    output.unlink()
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"ready": true' in completed.stdout.lower()
    assert output.read_text(encoding="utf-8") == (
        "ready=true\n"
        "reason=same_snapshot_ready\n"
    )


def test_phase2_readiness_from_paths_missing_phase2_is_deferred(tmp_path: Path) -> None:
    research = tmp_path / "artifacts" / "research"
    research.mkdir(parents=True)
    daily = research / "daily_research.json"
    daily.write_text(
        json.dumps({"snapshot_id": "snapshot-current"}),
        encoding="utf-8",
    )

    result = evaluate_from_paths(
        daily_path=daily,
        phase2_path=research / "probability_calibration_2.json",
    )
    assert result["ready"] is False
    assert result["reason"] == "phase2_artifact_missing"
