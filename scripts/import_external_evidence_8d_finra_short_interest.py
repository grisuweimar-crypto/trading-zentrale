#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scanner.research.external_evidence.finra_short_interest import (
    HISTORICAL_MODE,
    PROSPECTIVE_MODE,
    build_finra_snapshot,
    write_finra_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize a FINRA Consolidated Short Interest export into the Phase 8D "
            "outcome-blind PIT evidence contract."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        help="FINRA Consolidated Short Interest .json or .csv export",
    )
    parser.add_argument(
        "--publication-date",
        required=True,
        help="FINRA publication date, YYYY-MM-DD",
    )
    parser.add_argument(
        "--source-mode",
        required=True,
        choices=[PROSPECTIVE_MODE, HISTORICAL_MODE],
        help=(
            "Use PROSPECTIVE_PUBLICATION_SNAPSHOT only when this exact raw snapshot "
            "was captured prospectively on/after the FINRA publication time. Historical "
            "exports must use HISTORICAL_BACKFILL_LATEST_AVAILABLE_VINTAGE."
        ),
    )
    parser.add_argument(
        "--ingested-at",
        default=None,
        help="UTC/offset ISO timestamp. Defaults to the current UTC time.",
    )
    parser.add_argument(
        "--source-url",
        default="https://api.finra.org/data/group/otcmarket/name/consolidatedShortInterest",
    )
    parser.add_argument(
        "--output",
        default=str(
            ROOT
            / "artifacts"
            / "external_evidence"
            / "8d_finra_short_interest"
            / "snapshot.json"
        ),
    )
    args = parser.parse_args()

    ingested_at = args.ingested_at or datetime.now(timezone.utc).isoformat()
    payload = build_finra_snapshot(
        source_path=Path(args.input),
        publication_date=args.publication_date,
        ingested_at=ingested_at,
        source_mode=args.source_mode,
        source_url=args.source_url,
    )
    output = Path(args.output)
    write_finra_snapshot(payload, output)

    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "source_mode": payload["source_mode"],
                "strict_pit_eligible": payload["strict_pit_eligible"],
                "vintage_status": payload["vintage_status"],
                "publication_date": payload["publication_date"],
                "published_at": payload["published_at"],
                "ingested_at": payload["ingested_at"],
                "row_count": payload["row_count"],
                "rejected_row_count": payload["rejected_row_count"],
                "output": str(output),
                "market_outcomes_read": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
