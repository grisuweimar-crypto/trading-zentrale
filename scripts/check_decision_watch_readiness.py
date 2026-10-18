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
    w10_path: Path | None = None,
    elliott_capture_path: Path | None = None,
) -> dict[str, object]:
    daily = _load_object(daily_path)
    phase2 = _load_object(phase2_path) if phase2_path.exists() else None
    result = evaluate_phase2_readiness(daily=daily, phase2=phase2)
    if result["ready"] and w10_path is not None and w10_path.exists():
        sealed = _load_object(w10_path)
        if (str(sealed.get("snapshot_id") or "") == result["current_snapshot_id"]
                and sealed.get("status") == "sealed"):
            # Stage-3 late evidence is a *new* prospective W10 revision with a
            # new freeze/seal, never retrospective insertion into the old seal.
            # Only promote from explicitly not_supplied to a matching, genuine
            # capture with nonempty evidence; repeat/stale captures stay closed.
            phase6 = (sealed.get("stages") or {}).get("phase6_elliott") or {}
            if (
                elliott_capture_path is not None
                and phase6.get("status") == "not_supplied"
            ):
                capture = _load_object(elliott_capture_path)
                guards = capture.get("guards") or {}
                if (
                    capture.get("schema_version") == "elliott_vnext_prospective_capture_v1"
                    and str(capture.get("snapshot_id") or "") == result["current_snapshot_id"]
                    and str(capture.get("as_of") or "") == str(daily.get("as_of") or "")
                    and isinstance(capture.get("capture_id"), str)
                    and len(capture["capture_id"]) == 64
                    and isinstance(capture.get("outputs"), list)
                    and int(capture.get("output_count") or 0) == len(capture["outputs"])
                    and len(capture["outputs"]) > 0
                    and guards.get("research_only") is True
                    and guards.get("productive_integration_enabled") is False
                    and guards.get("w10_source_emitted") is False
                    and guards.get("changes_universal_stance") is False
                    and guards.get("changes_portfolio_action") is False
                    and guards.get("direct_ordering_allowed") is False
                    and guards.get("future_rows_used") is False
                ):
                    return {**result, "ready": True,
                            "reason": "same_snapshot_new_elliott_capture_reseal"}
            return {**result, "ready": False, "reason": "same_snapshot_already_sealed"}
    return result


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
        "--w10", default="artifacts/research/decision_snapshot_w10.json",
    )
    parser.add_argument(
        "--elliott-capture",
        type=Path,
        help="Optional validated current prospective 6H capture for one safe same-snapshot W10 revision",
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
        w10_path=root / args.w10,
        elliott_capture_path=(
            None if args.elliott_capture is None
            else (args.elliott_capture if args.elliott_capture.is_absolute()
                  else root / args.elliott_capture)
        ),
    )

    if args.github_output:
        with Path(args.github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"ready={'true' if result['ready'] else 'false'}\n")
            handle.write(f"reason={result['reason']}\n")

    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
