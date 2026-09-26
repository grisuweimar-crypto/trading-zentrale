#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.bulk_content_completion import (
    acquire_and_extract_bulk_content,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Complete Phase 8C-F/G on the authoritative local SEC bulk bundle by "
            "fetching only selected official SEC Archives 8-K/6-K full submission texts "
            "and running the frozen content-anchor parser."
        )
    )
    parser.add_argument(
        "--bundle-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"),
    )
    parser.add_argument(
        "--research-history",
        default=str(ROOT / "artifacts" / "research" / "history_recent.csv"),
    )
    parser.add_argument(
        "--parser-contract",
        default=str(ROOT / "configs" / "external_evidence_8c_content_parser_v1.json"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "8c_real_content"),
    )
    parser.add_argument(
        "--anchors-output",
        default=str(ROOT / "artifacts" / "research" / "external_evidence_8c_content_anchors.json"),
    )
    parser.add_argument(
        "--user-agent",
        required=True,
        help="Descriptive SEC User-Agent including project/contact identity.",
    )
    parser.add_argument("--minimum-interval-seconds", type=float, default=0.22)
    parser.add_argument("--content-lookback-days", type=int, default=365)
    args = parser.parse_args()

    result = acquire_and_extract_bulk_content(
        bundle_dir=Path(args.bundle_dir),
        research_history_path=Path(args.research_history),
        parser_contract_path=Path(args.parser_contract),
        output_dir=Path(args.output_dir),
        anchors_output_path=Path(args.anchors_output),
        user_agent=args.user_agent,
        minimum_interval_seconds=args.minimum_interval_seconds,
        content_lookback_days=args.content_lookback_days,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
