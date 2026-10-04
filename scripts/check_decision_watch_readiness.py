#!/usr/bin/env python3
"""Fail-success readiness gate for Decision Watch same-snapshot integration."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping


class DecisionWatchReadinessError(ValueError):
    pass


def _load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionWatchReadinessError(f"unreadable_json:{path}") from exc
    if not isinstance(value, dict):
        raise DecisionWatchReadinessError(f"json_object_required:{path}")
    return value


def evaluate_phase2_readiness(
    *,
    daily: Mapping[str, Any],
    phase2: Mapping[str, Any] | None,
) -> dict[str, object]:
    current = str(daily.get("snapshot_id") or "").strip()
    if not current:
        raise DecisionWatchReadinessError("daily_snapshot_id_required")

    if phase2 is None:
        return {
            "ready": False,
            "reason": "phase2_artifact_missing",
            "current_snapshot_id": current,
            "phase2_snapshot_id": None,
        }

    source = phase2.get("source")
    source = source if isinstance(source, Mapping) else {}
    persisted = str(source.get("snapshot_id") or "").strip()
    ready = bool(persisted) and persisted == current
    return {
        "ready": ready,
        "reason": (
            "same_snapshot_ready"
            if ready
            else f"phase2_snapshot_not_ready:{persisted}:{current}"
        ),
        "current_snapshot_id": current,
        "phase2_snapshot_id": persisted or None,
    }


def evaluate_from_paths(
    *,
    daily_path: Path,
    phase2_path: Path,
) -> dict[str, object]:
    daily = _load_object(daily_path)
    phase2 = _load_object(phase2_path) if phase2_path.exists() else None
    return evaluate_phase2_readiness(daily=daily, phase2=phase2)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--daily",
        default="artifacts/research/daily_research.json",
    )
    parser.add_argument(
        "--phase2",
        default="artifacts/research/probability_calibration_2.json",
    )
    parser.add_argument(
        "--github-output",
        default=os.environ.get("GITHUB_OUTPUT"),
    )
    args = parser.parse_args()

    root = args.root.resolve()
    result = evaluate_from_paths(
        daily_path=root / args.daily,
        phase2_path=root / args.phase2,
    )

    if args.github_output:
        with Path(args.github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"ready={'true' if result['ready'] else 'false'}\n")
            handle.write(f"reason={result['reason']}\n")

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
