#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_bulk import (
    import_sec_insider_quarter,
    write_sec_insider_evidence,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Import one official SEC quarterly Insider Transactions Data Set and join "
            "Form 4/4-A P/S transactions to PIT-safe acceptance metadata from the "
            "operator-attested SEC submissions bulk bundle."
        )
    )
    parser.add_argument("--insider-zip", required=True)
    parser.add_argument(
        "--sec-bulk-bundle",
        default=str(ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"),
    )
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--quarter-label", required=True, help="Example: 2026Q2")
    parser.add_argument(
        "--output",
        default=str(
            ROOT
            / "artifacts"
            / "external_evidence"
            / "8d_sec_insider"
            / "sec_insider_evidence.json"
        ),
    )
    args = parser.parse_args()

    payload = import_sec_insider_quarter(
        insider_zip_path=Path(args.insider_zip),
        sec_bulk_bundle_dir=Path(args.sec_bulk_bundle),
        source_url=args.source_url,
        quarter_label=args.quarter_label,
    )
    output = Path(args.output)
    write_sec_insider_evidence(payload, output)
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "source_quarter": payload["source_quarter"],
                "counts": payload["counts"],
                "candidate_status_counts": payload["candidate_status_counts"],
                "output": str(output),
                "market_outcomes_read": payload["guards"]["market_outcomes_read"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
