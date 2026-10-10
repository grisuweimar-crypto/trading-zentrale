#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.finra_short_interest_coverage import (
    audit_finra_current_universe,
    write_coverage,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run Phase 8D-A2 current-universe coverage over one normalized FINRA "
            "short-interest settlement snapshot. No market outcomes are read."
        )
    )
    parser.add_argument(
        "--scanner",
        default=str(ROOT / "artifacts" / "research" / "latest_scanner.csv"),
    )
    parser.add_argument(
        "--finra-snapshot",
        default=str(
            ROOT
            / "artifacts"
            / "external_evidence"
            / "8d_finra_short_interest"
            / "snapshot.json"
        ),
    )
    parser.add_argument(
        "--output",
        default=str(
            ROOT
            / "artifacts"
            / "external_evidence"
            / "8d_finra_short_interest"
            / "current_universe_coverage.json"
        ),
    )
    args = parser.parse_args()

    payload = audit_finra_current_universe(
        scanner_path=Path(args.scanner),
        finra_snapshot_path=Path(args.finra_snapshot),
    )
    output = Path(args.output)
    write_coverage(payload, output)
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "scanner_as_of": payload["scanner_as_of"],
                "finra_settlement_date": payload["finra_settlement_date"],
                "finra_published_at": payload["finra_published_at"],
                "finra_strict_pit_eligible": payload["finra_strict_pit_eligible"],
                "scanner_symbol_count": payload["scanner_symbol_count"],
                "exact_known_symbol_count": payload["exact_known_symbol_count"],
                "exact_known_symbol_fraction": payload["exact_known_symbol_fraction"],
                "status_counts": payload["status_counts"],
                "known_feature_counts": payload["known_feature_counts"],
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
