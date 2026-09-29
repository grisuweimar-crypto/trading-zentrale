#!/usr/bin/env python3
"""CLI for QM-B historical listing-metadata source assessment."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_listing_metadata_evidence import (  # noqa: E402
    ListingMetadataEvidenceError,
    assess_listing_metadata_sources,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B assess historical listing metadata sources")
    p.add_argument("--contract", default=str(ROOT / "configs" / "qm_b_listing_metadata_sources_v1.json"))
    p.add_argument("--output")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        payload = assess_listing_metadata_sources(contract_path=args.contract)
        text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        else:
            print(text, end="")
        return 0
    except (ListingMetadataEvidenceError, OSError, json.JSONDecodeError) as exc:
        print(f"QM-B LISTING METADATA EVIDENCE ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
