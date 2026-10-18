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


def test_sealed_same_snapshot_does_not_rewrite_w10(tmp_path: Path) -> None:
    research = tmp_path / "artifacts" / "research"
    research.mkdir(parents=True)
    daily = research / "daily_research.json"
    phase2 = research / "probability_calibration_2.json"
    w10 = research / "decision_snapshot_w10.json"
    daily.write_text(json.dumps({"snapshot_id": "new"}), encoding="utf-8")
    phase2.write_text(json.dumps({"source": {"snapshot_id": "new"}}), encoding="utf-8")
    w10.write_text(json.dumps({"snapshot_id": "new", "status": "sealed"}), encoding="utf-8")
    result = evaluate_from_paths(daily_path=daily, phase2_path=phase2, w10_path=w10)
    assert result["ready"] is False
    assert result["reason"] == "same_snapshot_already_sealed"
    w10.write_text(json.dumps({"snapshot_id": "old", "status": "sealed"}), encoding="utf-8")
    result = evaluate_from_paths(daily_path=daily, phase2_path=phase2, w10_path=w10)
    assert result["ready"] is True
    assert result["reason"] == "same_snapshot_ready"
    w10.write_text(json.dumps({"snapshot_id": "new", "status": "draft"}), encoding="utf-8")
    result = evaluate_from_paths(daily_path=daily, phase2_path=phase2, w10_path=w10)
    assert result["ready"] is True


def test_exact_snapshot_late_elliott_reseals_once_and_not_backdated(tmp_path: Path) -> None:
    research = tmp_path / "artifacts" / "research"
    research.mkdir(parents=True)
    daily = research / "daily_research.json"
    phase2 = research / "probability_calibration_2.json"
    w10 = research / "decision_snapshot_w10.json"
    capture_file = research / "late-elliott.json"
    daily.write_text(json.dumps({"snapshot_id": "UEC-snap", "as_of": "2026-10-18"}), encoding="utf-8")
    phase2.write_text(json.dumps({"source": {"snapshot_id": "UEC-snap"}}), encoding="utf-8")
    w10.write_text(json.dumps({
        "snapshot_id": "UEC-snap", "status": "sealed", "sealed_at": "2026-10-18T04:37:00Z",
        "stages": {"phase6_elliott": {"status": "not_supplied"}},
    }), encoding="utf-8")
    prospective = {
        "snapshot_id": "UEC-snap", "as_of": "2026-10-18",
        "output_count": 1, "outputs": [{"symbol": "UEC", "timeframe": "1d"}],
    }
    capture_file.write_text(json.dumps(prospective), encoding="utf-8")
    paths = dict(daily_path=daily, phase2_path=phase2, w10_path=w10,
                 late_elliott_capture_path=capture_file)
    result = evaluate_from_paths(**paths)
    assert result["ready"] is True
    assert result["reason"] == "exact_snapshot_late_elliott_requires_new_w10_seal"
    assert json.loads(w10.read_text())["sealed_at"] == "2026-10-18T04:37:00Z"

    # Once admitted, completion events cannot infinitely replay this snapshot.
    w10.write_text(json.dumps({
        "snapshot_id": "UEC-snap", "status": "sealed",
        "stages": {"phase6_elliott": {"status": "available"}},
    }), encoding="utf-8")
    assert evaluate_from_paths(**paths)["reason"] == "same_snapshot_already_sealed"

    # An earlier capture, another symbol run or incomplete capture is not evidence.
    w10.write_text(json.dumps({
        "snapshot_id": "UEC-snap", "status": "sealed",
        "stages": {"phase6_elliott": {"status": "not_supplied"}},
    }), encoding="utf-8")
    for changed in (
        {"snapshot_id": "other-snapshot"},
        {"as_of": "2026-10-17"},
        {"output_count": 0, "outputs": []},
        {"output_count": 2},
    ):
        capture_file.write_text(json.dumps({**prospective, **changed}), encoding="utf-8")
        assert evaluate_from_paths(**paths)["reason"] == "same_snapshot_already_sealed"

    capture_file.unlink()
    assert evaluate_from_paths(**paths)["reason"] == "same_snapshot_already_sealed"


def test_late_capture_never_bypasses_stale_phase2_gate(tmp_path: Path) -> None:
    research = tmp_path / "artifacts" / "research"
    research.mkdir(parents=True)
    daily = research / "daily_research.json"
    p2 = research / "probability_calibration_2.json"
    w10 = research / "decision_snapshot_w10.json"
    late = research / "late.json"
    daily.write_text(json.dumps({"snapshot_id": "UEC-snap", "as_of": "2026-10-18"}))
    p2.write_text(json.dumps({"source": {"snapshot_id": "old-snap"}}))
    w10.write_text(json.dumps({"snapshot_id": "UEC-snap", "status": "sealed",
                               "stages": {"phase6_elliott": {"status": "not_supplied"}}}))
    late.write_text(json.dumps({"snapshot_id": "UEC-snap", "as_of": "2026-10-18",
                                "output_count": 1, "outputs": [{"symbol": "UEC"}]}))
    result = evaluate_from_paths(daily_path=daily, phase2_path=p2, w10_path=w10,
                                 late_elliott_capture_path=late)
    assert result["ready"] is False
    assert result["reason"].startswith("phase2_snapshot_not_ready:")
