from __future__ import annotations

import argparse
import csv
from hashlib import sha256
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    build_outcome_maturation_check,
    persist_outcome_maturation,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Mature Pattern Discovery Lab L8 prospective claims."
    )
    parser.add_argument("--capture-report", required=True)
    parser.add_argument("--peer-snapshot", required=True)
    parser.add_argument(
        "--start-sessions",
        required=True,
        help=(
            "JSON list (or {sessions:[...]}) with explicit symbol/session_id/"
            "calendar_id/session_date/start_at/source bindings."
        ),
    )
    parser.add_argument("--prices", required=True)
    parser.add_argument("--checked-at", required=True)
    parser.add_argument("--price-as-of", required=True)
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--actor-role", default="researcher")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    capture_path = _path(root, args.capture_report)
    peer_path = _path(root, args.peer_snapshot)
    sessions_path = _path(root, args.start_sessions)
    price_path = _path(root, args.prices)
    session_payload = _json(sessions_path)
    sessions = (
        session_payload["sessions"]
        if isinstance(session_payload, dict)
        else session_payload
    )
    if not isinstance(sessions, list):
        raise ValueError("start_sessions_json_must_be_list_or_sessions_object")

    report = build_outcome_maturation_check(
        _json(capture_path),
        _csv(peer_path),
        sessions,
        _csv(price_path),
        checked_at=args.checked_at,
        price_as_of=args.price_as_of,
        peer_snapshot_file_sha256=_sha256(peer_path),
        start_session_binding_file_sha256=_sha256(sessions_path),
        price_file_sha256=_sha256(price_path),
    )
    persisted = persist_outcome_maturation(
        root,
        report,
        actor_id=args.actor_id,
        actor_role=args.actor_role,
    )
    print(
        json.dumps(
            {
                "check_id": report["check_id"],
                "check_hash": report["check_hash"],
                "capture_id": report["l7_capture_id"],
                "claim_count": report["counts"]["claim_count"],
                "matured_count": report["counts"]["matured_count"],
                "status_counts": report["counts"]["status_counts"],
                "registry": persisted["registry"],
                "check_report_path": persisted["check_report_path"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
