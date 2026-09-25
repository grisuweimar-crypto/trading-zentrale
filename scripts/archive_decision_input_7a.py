#!/usr/bin/env python3
"""Validate and append one Phase-7A packet to the prospective research archive."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer.evidence_archive import append_prospective_packet


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive one validated Phase-7A evidence packet")
    parser.add_argument("--input", required=True, type=Path, help="Validated or raw 7A packet JSON")
    parser.add_argument("--archive", default="artifacts/research/decision_evidence_7a.jsonl")
    parser.add_argument("--prospective-start", default="2026-09-26")
    parser.add_argument(
        "--allow-spent",
        action="store_true",
        help="Allow pre-prospective packets for diagnostics only; never makes them unspent evidence.",
    )
    args = parser.parse_args()

    packet = json.loads(args.input.read_text(encoding="utf-8"))
    metadata = append_prospective_packet(
        args.archive,
        packet,
        prospective_start=args.prospective_start,
        allow_spent=args.allow_spent,
    )
    print(json.dumps({
        "archive": args.archive,
        "packet_count": metadata["packet_count"],
        "partitions": metadata["partitions"],
        "families": metadata["families"],
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
