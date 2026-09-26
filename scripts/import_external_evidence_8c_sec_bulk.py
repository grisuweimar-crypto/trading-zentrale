#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.sec_bulk_import import (
    DIRECT_BULK_MODE,
    MIRROR_MODE,
    import_sec_bulk_bundle,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import local SEC submissions/companyfacts bulk archives into a deterministic Phase 8C-J bundle."
    )
    parser.add_argument("--scanner", default=str(ROOT / "artifacts" / "research" / "latest_scanner.csv"))
    parser.add_argument("--submissions-zip", required=True)
    parser.add_argument("--companyfacts-zip", required=True)
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"),
    )
    parser.add_argument(
        "--source-mode",
        required=True,
        choices=[DIRECT_BULK_MODE, MIRROR_MODE],
        help="Direct mode requires operator-attested archives downloaded from official SEC HTTPS URLs. Mirror mode is challenger-only.",
    )
    parser.add_argument("--submissions-source-url", required=True)
    parser.add_argument("--companyfacts-source-url", required=True)
    parser.add_argument(
        "--acquired-at",
        help="Optional ISO-8601 acquisition timestamp. Defaults to current UTC time.",
    )
    parser.add_argument(
        "--filing-content-dir",
        help="Optional directory containing accession-named 8-K/6-K .txt files. Without it, structured-event semantic validation stays blocked.",
    )
    args = parser.parse_args()

    manifest = import_sec_bulk_bundle(
        scanner_path=Path(args.scanner),
        submissions_zip=Path(args.submissions_zip),
        companyfacts_zip=Path(args.companyfacts_zip),
        output_dir=Path(args.output_dir),
        source_mode=args.source_mode,
        submissions_source_url=args.submissions_source_url,
        companyfacts_source_url=args.companyfacts_source_url,
        acquired_at=args.acquired_at,
        filing_content_dir=Path(args.filing_content_dir) if args.filing_content_dir else None,
    )
    print(
        json.dumps(
            {
                "schema_version": manifest["schema_version"],
                "source_mode": manifest["source_mode"],
                "coverage": manifest["coverage"],
                "eligibility": manifest["eligibility"],
                "bulk_manifest": str(Path(args.output_dir) / "bulk_manifest.json"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
