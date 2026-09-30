#!/usr/bin/env python3
"""Build the current snapshot's typed Phase-7A packets and optionally archive them."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.daily_research import validate_daily_research
from scanner.research.decision_layer.current_evidence import (
    DEFAULT_ARCHIVE,
    DEFAULT_OUTPUT,
    build_current_packet_set,
    merge_packet_set_into_archive,
)
from scanner.research.decision_layer.prospective_gate import assert_orchestration_snapshot_eligible


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--archive-path", default=DEFAULT_ARCHIVE)
    parser.add_argument(
        "--archive",
        action="store_true",
        help="Idempotently merge the current packets into the prospective 7A JSONL archive.",
    )
    args = parser.parse_args()

    # Fail closed before building any packet from a pre-deployment snapshot. This
    # keeps the new Decision-Evidence history genuinely prospective instead of
    # retroactively reconstructing yesterday's scanner state after seeing later data.
    daily = validate_daily_research(args.root)
    prospective_gate = assert_orchestration_snapshot_eligible(daily)

    packet_set = build_current_packet_set(args.root)
    output = args.root / args.output
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
        "timing_claim_count": packet_set["timing_claim_count"],
        "symbols_with_timing_claims": packet_set["symbols_with_timing_claims"],
        "prospective_gate": prospective_gate,
        "output": str(output),
        "archive": archive,
    }, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
