#!/usr/bin/env python3
"""Build the final same-snapshot Decision evidence after Phase 2/3/4 are available."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.current_evidence import (
    CURRENT_PACKET_SET_SCHEMA_VERSION,
    DEFAULT_ARCHIVE,
    DEFAULT_HISTORY,
    DEFAULT_OUTPUT,
    build_current_packet_set,
    merge_packet_set_into_archive,
)
from scanner.research.decision_layer.integrated_evidence import (
    INTEGRATED_STAGE,
    integrate_current_packet_set,
)
from scanner.research.decision_layer.prospective_gate import assert_orchestration_snapshot_eligible


def _existing_integrated_output(path: Path, snapshot_id: str) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    if value.get("schema_version") != CURRENT_PACKET_SET_SCHEMA_VERSION:
        return None
    if str(value.get("snapshot_id") or "") != snapshot_id:
        return None
    if not (value.get("semantics") or {}).get("integrated_evidence_v1"):
        return None
    packets = value.get("packets")
    if not isinstance(packets, list) or not packets:
        return None
    if any(not isinstance(packet, dict) or packet.get("orchestration_stage") != INTEGRATED_STAGE for packet in packets):
        return None
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--phase4-report", required=True)
    parser.add_argument("--history", default=DEFAULT_HISTORY)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--archive-path", default=DEFAULT_ARCHIVE)
    parser.add_argument("--archive", action="store_true")
    parser.add_argument("--force-revision", action="store_true")
    args = parser.parse_args()

    daily = validate_daily_research(args.root)
    prospective_gate = assert_orchestration_snapshot_eligible(daily)
    snapshot_id = str(daily["snapshot_id"])
    output = args.root / args.output

    packet_set = None if args.force_revision else _existing_integrated_output(output, snapshot_id)
    reused = packet_set is not None
    if packet_set is None:
        base = build_current_packet_set(args.root)
        history = pd.read_csv(args.root / args.history, low_memory=False)
        phase4_path = Path(args.phase4_report)
        if not phase4_path.is_absolute():
            phase4_path = args.root / phase4_path
        phase4 = json.loads(phase4_path.read_text(encoding="utf-8"))
        packet_set = integrate_current_packet_set(
            packet_set=base,
            daily=daily,
            history=history,
            phase4_report=phase4,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(packet_set, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    archive = None
    if args.archive:
        archive = merge_packet_set_into_archive(args.root / args.archive_path, packet_set)

    print(json.dumps({
        "snapshot_id": packet_set["snapshot_id"],
        "as_of": packet_set["as_of"],
        "packet_count": packet_set["packet_count"],
        "phase4_symbol_count": packet_set.get("phase4_symbol_count"),
        "path_review_count": packet_set.get("path_review_count"),
        "integrated_output_reused": reused,
        "prospective_gate": prospective_gate,
        "output": str(output),
        "archive": archive,
    }, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
