#!/usr/bin/env python3
"""Run the private Depot Watch from the authoritative snapshot and archived 7A evidence."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.evidence_archive import load_evidence_archive


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the Phase-7H Watch from current daily_research, archived 7A evidence and private positions"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repository root containing artifacts/research/daily_research.json",
    )
    parser.add_argument("--positions", required=True, type=Path, help="Private decision_depot_position_book_v1 JSON")
    parser.add_argument("--archive", default=DEFAULT_ARCHIVE, help="Prospective decision_evidence_7a JSONL path relative to root")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional private output path. Omit to print the Watch to stdout.",
    )
    parser.add_argument(
        "--diagnostics-output",
        type=Path,
        help="Optional private diagnostics path. Diagnostics contain no position rows.",
    )
    args = parser.parse_args()

    daily = validate_daily_research(args.root)
    positions = _load(args.positions)
    packets, archive_metadata = load_evidence_archive(args.root / args.archive, missing_ok=True)
    watch, diagnostics = build_orchestrated_depot_watch(daily, positions, packets)
    diagnostics = {**diagnostics, "archive_status": archive_metadata.get("status")}

    text = json.dumps(watch, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")

    if args.diagnostics_output:
        args.diagnostics_output.parent.mkdir(parents=True, exist_ok=True)
        args.diagnostics_output.write_text(
            json.dumps(diagnostics, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    else:
        print(json.dumps({"orchestration": diagnostics}, ensure_ascii=False), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
