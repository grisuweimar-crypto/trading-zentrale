#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.finra_short_interest_acquisition import (
    collect_finra_publication_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Collect one prospective FINRA Consolidated Short Interest publication "
            "snapshot for one settlement date and normalize it under the Phase 8D PIT contract."
        )
    )
    parser.add_argument("--settlement-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--publication-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--output-dir",
        default=str(
            ROOT
            / "artifacts"
            / "external_evidence"
            / "8d_finra_short_interest"
        ),
    )
    parser.add_argument(
        "--collected-at",
        default=None,
        help="Optional timezone-aware ISO timestamp. Normally omitted.",
    )
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--minimum-interval-seconds", type=float, default=0.25)
    parser.add_argument("--max-pages", type=int, default=200)
    args = parser.parse_args()

    manifest = collect_finra_publication_snapshot(
        settlement_date=args.settlement_date,
        publication_date=args.publication_date,
        output_dir=Path(args.output_dir),
        collected_at=args.collected_at,
        limit=args.limit,
        minimum_interval_seconds=args.minimum_interval_seconds,
        max_pages=args.max_pages,
    )
    print(
        json.dumps(
            {
                "schema_version": manifest["schema_version"],
                "settlement_date": manifest["settlement_date"],
                "publication_date": manifest["publication_date"],
                "published_at": manifest["published_at"],
                "collected_at": manifest["collected_at"],
                "raw_row_count": manifest["raw_row_count"],
                "record_total_header": manifest["record_total_header"],
                "strict_pit_eligible": manifest["strict_pit_eligible"],
                "output_dir": str(Path(args.output_dir)),
                "market_outcomes_read": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
