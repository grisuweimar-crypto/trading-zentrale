#!/usr/bin/env python3
"""Build the current snapshot's typed Phase-7A packets and optionally archive them."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.current_evidence import (
    DEFAULT_ARCHIVE,
    DEFAULT_OUTPUT,
    build_current_packet_set,
    merge_packet_set_into_archive,
)


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
        "output": str(output),
        "archive": archive,
    }, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
