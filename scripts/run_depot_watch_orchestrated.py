#!/usr/bin/env python3
"""Run the private Depot Watch from one sealed W10 snapshot and positions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.current_evidence import DEFAULT_ARCHIVE
from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.evidence_archive import load_evidence_archive
from scanner.research.decision_layer.w10_orchestration import validate_sealed_manifest


DEFAULT_W10_MANIFEST = "artifacts/research/decision_snapshot_w10.json"


def _load(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build the Phase-7H Watch from one sealed W10 snapshot, "
            "archived 7A evidence and private positions"
        )
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
        "--w10-manifest",
        default=DEFAULT_W10_MANIFEST,
        help=(
            "Sealed W10 same-snapshot manifest relative to root. "
            "The private 7D->7H chain refuses to run without it."
        ),
    )
    parser.add_argument(
        "--elliott-6h-source",
        type=Path,
        help=(
            "Optional PIT-stamped decision_elliott_6h_source_v1 JSON. "
            "Only frozen 6H review contexts are passed to 7F; no Elliott direction becomes a vote."
        ),
    )
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
    manifest_path = Path(args.w10_manifest)
    if not manifest_path.is_absolute():
        manifest_path = args.root / manifest_path
    manifest = validate_sealed_manifest(
        _load(manifest_path),
        expected_snapshot_id=str(daily["snapshot_id"]),
    )

    positions = _load(args.positions)
    elliott_6h_source = _load(args.elliott_6h_source) if args.elliott_6h_source else None
    packets, archive_metadata = load_evidence_archive(args.root / args.archive, missing_ok=True)
    watch, diagnostics = build_orchestrated_depot_watch(
        daily,
        positions,
        packets,
        elliott_6h_source=elliott_6h_source,
    )
    diagnostics = {
        **diagnostics,
        "archive_status": archive_metadata.get("status"),
        "w10_manifest_status": manifest["status"],
        "w10_snapshot_id": manifest["snapshot_id"],
        "w10_pre_7a_frozen_at": manifest["pre_7a_frozen_at"],
        "w10_sealed_at": manifest["sealed_at"],
        "w10_no_backdating_guard": True,
    }

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
