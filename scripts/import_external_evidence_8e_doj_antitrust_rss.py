#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from scanner.research.external_evidence.doj_antitrust_rss_8e import (
    build_doj_antitrust_case_filing_evidence,
    write_doj_antitrust_evidence,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8e_doj_antitrust_rss_v1.json"
DEFAULT_OUT = ROOT / "artifacts" / "external_evidence" / "8e_doj_antitrust"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import an official DOJ Antitrust case-filings RSS snapshot into the outcome-blind Phase 8E event evidence format."
    )
    parser.add_argument("--xml", required=True, help="Local official DOJ RSS XML snapshot")
    parser.add_argument(
        "--feed-kind",
        required=True,
        choices=["CIVIL_CASE_FILINGS", "CRIMINAL_CASE_FILINGS"],
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--ingested-at", default=None, help="Timezone-aware ISO timestamp; defaults to current UTC")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    ingested_at = args.ingested_at or datetime.now(timezone.utc).isoformat()
    output = Path(args.output) if args.output else DEFAULT_OUT / f"{args.feed_kind.lower()}.json"
    payload = build_doj_antitrust_case_filing_evidence(
        xml_path=Path(args.xml),
        feed_kind=args.feed_kind,
        ingested_at=ingested_at,
        config=config,
    )
    write_doj_antitrust_evidence(payload, output)
    print(json.dumps({
        "status": payload["status"],
        "feed_kind": payload["feed_kind"],
        "row_count": payload["row_count"],
        "output": str(output),
        "market_outcomes_read": False,
        "phase7_integration_enabled": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
