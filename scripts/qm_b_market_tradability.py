#!/usr/bin/env python3
"""Audit market-tradability evidence for a membership snapshot."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_market_tradability import (  # noqa: E402
    MarketTradabilityError,
    audit_membership_snapshot,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B market tradability evidence audit")
    p.add_argument("--membership", required=True, help="Prospective membership snapshot JSON")
    p.add_argument("--evidence", help="Optional JSON list or object with records[]")
    p.add_argument("--output", required=True)
    return p


def _load_evidence(path: str | None) -> list[dict]:
    if not path:
        return []
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict) and isinstance(payload.get("records"), list):
        rows = payload["records"]
    else:
        raise MarketTradabilityError("evidence_json_must_be_list_or_records_object")
    if not all(isinstance(row, dict) for row in rows):
        raise MarketTradabilityError("evidence_rows_must_be_objects")
    return rows


def main() -> int:
    args = parser().parse_args()
    try:
        membership = json.loads(Path(args.membership).read_text(encoding="utf-8"))
        result = audit_membership_snapshot(membership, evidence_records=_load_evidence(args.evidence))
        Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({
            "instrument_count": result["instrument_count"],
            "registered_source_count": result["registered_source_count"],
            "status_counts": result["status_counts"],
            "positive_tradability_count": result["positive_tradability_count"],
            "current_gap_status": result["current_gap_status"],
        }, indent=2, sort_keys=True))
        return 0
    except (OSError, json.JSONDecodeError, MarketTradabilityError) as exc:
        print(f"QM-B MARKET TRADABILITY ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
