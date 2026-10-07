from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.pattern_discovery import (
    ConfirmationLookRegistry,
    build_rating_history,
    confirmation_registry_repo_path,
    persist_rating_history,
)


def _json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _path(root: Path, value: str) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def _patterns(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("frozen_patterns"), list):
        return payload["frozen_patterns"]
    raise ValueError("frozen_patterns_json_must_be_list_or_l5_snapshot")


def _registry_reports(path: Path):
    if not path.exists():
        return []
    reports = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict) or not isinstance(value.get("report"), dict):
            raise ValueError(f"confirmation_registry_event_invalid:{line_number}")
        reports.append(value["report"])
    return reports


def _pending_reports(paths: list[str], root: Path):
    values = []
    for raw in paths:
        payload = _json(_path(root, raw))
        if not isinstance(payload, dict):
            raise ValueError("pending_l9_report_must_be_object")
        values.append(payload)
    return values


def _retirements(path: Path | None):
    if path is None:
        return {}
    payload = _json(path)
    if not isinstance(payload, list):
        raise ValueError("retirements_json_must_be_list")
    result = {}
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("retirement_entry_must_be_object")
        key = f"{item.get('pattern_id')}::{item.get('pattern_version')}"
        if key in result:
            raise ValueError(f"duplicate_retirement:{key}")
        result[key] = item
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build and persist Pattern Discovery L10 rating histories "
            "from immutable L5/L9 evidence."
        )
    )
    parser.add_argument("--frozen-patterns", required=True)
    parser.add_argument(
        "--pending-l9-report",
        action="append",
        default=[],
        help=(
            "Optional hash-verifiable UNRESOLVED_NOT_DUE L9 report(s) "
            "that are intentionally not persisted as consumed L9 looks."
        ),
    )
    parser.add_argument(
        "--retirements",
        help=(
            "Optional JSON list of explicit L10 retirement records keyed "
            "by pattern_id/pattern_version."
        ),
    )
    parser.add_argument("--actor-id", required=True)
    parser.add_argument("--actor-role", default="researcher")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    root = Path(args.repo_root).resolve()
    patterns = _patterns(_json(_path(root, args.frozen_patterns)))
    registry_path = _path(root, confirmation_registry_repo_path())
    if registry_path.exists():
        ConfirmationLookRegistry(registry_path).verify_integrity()
    reports = _registry_reports(registry_path)
    reports.extend(_pending_reports(args.pending_l9_report, root))
    reports.sort(key=lambda item: str(item.get("evaluated_at") or ""))
    retirements = _retirements(
        _path(root, args.retirements) if args.retirements else None
    )

    output = []
    for pattern in patterns:
        key = f"{pattern.get('pattern_id')}::{pattern.get('pattern_version')}"
        retirement = retirements.get(key)
        history = build_rating_history(
            pattern,
            reports,
            retirement=retirement,
        )
        persisted = persist_rating_history(
            root,
            history,
            actor_id=args.actor_id,
            actor_role=args.actor_role,
        )
        output.append(
            {
                "pattern_id": history["pattern_id"],
                "pattern_version": history["pattern_version"],
                "current_rating": history["current_rating"],
                "transition_count": history["transition_count"],
                "history_hash": history["history_hash"],
                "persisted": persisted,
            }
        )

    print(json.dumps({"phase": "L10", "ratings": output}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
