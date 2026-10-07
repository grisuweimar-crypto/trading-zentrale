from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

import pandas as pd

from scanner.research.governance.qm_c import HypothesisRegistry
from scanner.research.governance.qm_c_analysis_plan import AnalysisPlanRegistry
from scanner.research.pattern_discovery import (
    build_prospective_capture,
    persist_prospective_capture,
)


def _json(path: str | Path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _records(path: str | Path):
    frame = pd.read_csv(path)
    frame = frame.where(pd.notna(frame), None)
    return frame.to_dict(orient="records")


def _sha256(path: str | Path) -> str:
    return sha256(Path(path).read_bytes()).hexdigest()


def _repo_path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture Pattern Discovery Lab L7 prospective claims."
    )
    parser.add_argument(
        "--l5-snapshot",
        action="append",
        required=True,
        help="Path to an L5 freeze snapshot. Repeat for multiple frozen runs.",
    )
    parser.add_argument("--snapshot-metadata", required=True)
    parser.add_argument("--current-snapshot", required=True)
    parser.add_argument("--history", required=True)
    parser.add_argument("--market-sessions", required=True)
    parser.add_argument("--capture-at", required=True)
    parser.add_argument("--hypothesis-registry", required=True)
    parser.add_argument("--analysis-plan-registry", required=True)
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--actor-role", default="researcher")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    l5_paths = [_repo_path(root, path) for path in args.l5_snapshot]
    metadata_path = _repo_path(root, args.snapshot_metadata)
    current_path = _repo_path(root, args.current_snapshot)
    history_path = _repo_path(root, args.history)
    sessions_path = _repo_path(root, args.market_sessions)
    hypotheses_path = _repo_path(root, args.hypothesis_registry)
    plans_path = _repo_path(root, args.analysis_plan_registry)

    session_payload = _json(sessions_path)
    sessions = (
        session_payload["sessions"]
        if isinstance(session_payload, dict)
        else session_payload
    )
    if not isinstance(sessions, list):
        raise ValueError("market_sessions_json_must_be_list_or_sessions_object")

    report = build_prospective_capture(
        [_json(path) for path in l5_paths],
        _json(metadata_path),
        _records(current_path),
        _records(history_path),
        sessions,
        capture_at=args.capture_at,
        snapshot_file_sha256=_sha256(current_path),
        history_file_sha256=_sha256(history_path),
        hypothesis_registry=HypothesisRegistry(hypotheses_path),
        analysis_plan_registry=AnalysisPlanRegistry(plans_path),
    )
    persisted = persist_prospective_capture(
        root,
        report,
        actor_id=args.actor_id,
        actor_role=args.actor_role,
    )
    print(
        json.dumps(
            {
                "capture_id": report["capture_id"],
                "capture_hash": report["capture_hash"],
                "snapshot_id": report["snapshot_binding"]["snapshot_id"],
                "frozen_pattern_count": report["counts"]["frozen_pattern_count"],
                "post_freeze_pattern_count": report["counts"][
                    "post_freeze_pattern_count"
                ],
                "prospective_claim_count": report["counts"][
                    "prospective_claim_count"
                ],
                "excluded_item_count": report["counts"]["excluded_item_count"],
                "registry": persisted["registry"],
                "capture_report_path": persisted["capture_report_path"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
