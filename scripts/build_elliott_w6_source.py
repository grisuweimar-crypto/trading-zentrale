#!/usr/bin/env python3
from __future__ import annotations

"""Build one Stage-3 W6 source from a prospective Elliott-vNext capture."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.phase6_elliott import (
    build_elliott_6h_source_from_prospective_capture,
)


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _write(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--capture", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--evidence-available-from")
    parser.add_argument("--available-from")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    root = args.root.resolve()
    capture_path = args.capture if args.capture.is_absolute() else root / args.capture
    output_path = args.output if args.output.is_absolute() else root / args.output
    daily = validate_daily_research(root)
    capture = _load(capture_path)
    available_from = (
        args.available_from
        or datetime.now(timezone.utc).isoformat()
    )

    source = build_elliott_6h_source_from_prospective_capture(
        capture,
        source_commit=args.source_commit,
        available_from=available_from,
        expected_snapshot_id=str(daily["snapshot_id"]),
        expected_as_of=str(daily["as_of"]),
        evidence_available_from=args.evidence_available_from,
    )
    _write(output_path, source)

    print(
        json.dumps(
            {
                "schema_version": source["schema_version"],
                "snapshot_id": source["snapshot_id"],
                "as_of": source["as_of"],
                "source_capture_id": source["source_capture_id"],
                "output_count": source["output_count"],
                "symbol_count": source["symbol_count"],
                "available_from": source["available_from"],
                "multi_degree_reducer_used": False,
                "elliott_direction_used_as_vote": False,
                "changes_universal_stance": False,
                "output": str(output_path),
            },
            indent=2,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
