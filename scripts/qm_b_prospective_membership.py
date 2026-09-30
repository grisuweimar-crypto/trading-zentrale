#!/usr/bin/env python3
"""CLI for QM-B prospective project-universe membership evidence."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_prospective_identity_match import load_universe_snapshot  # noqa: E402
from scanner.research.governance.qm_b_prospective_membership import (  # noqa: E402
    ProspectiveMembershipError,
    append_membership_snapshot,
    build_membership_snapshot,
    membership_as_of,
    verify_membership_ledger,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B prospective project-universe membership ledger")
    sub = p.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="build one membership snapshot from an observed universe CSV")
    build.add_argument("--universe", default=str(ROOT / "data" / "inputs" / "universe_master.csv"))
    build.add_argument("--universe-snapshot-id", required=True)
    build.add_argument("--observed-at", required=True, help="timezone-aware ISO-8601 observation timestamp")
    build.add_argument("--output")
    build.add_argument("--ledger", help="optional JSONL ledger to append after validation")

    verify = sub.add_parser("verify", help="verify append-only membership ledger integrity")
    verify.add_argument("--ledger", required=True)

    query = sub.add_parser("query", help="query membership evidence as of a timestamp")
    query.add_argument("--ledger", required=True)
    query.add_argument("--instrument-id", required=True)
    query.add_argument("--as-of", required=True)
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "build":
            rows, digest = load_universe_snapshot(args.universe)
            snapshot = build_membership_snapshot(
                rows,
                universe_snapshot_id=args.universe_snapshot_id,
                universe_observed_at=args.observed_at,
                universe_sha256=digest,
            )
            if args.output:
                target = Path(args.output)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            if args.ledger:
                ledger_state = append_membership_snapshot(args.ledger, snapshot)
            else:
                ledger_state = None
            summary = {
                "membership_snapshot_id": snapshot["membership_snapshot_id"],
                "universe_observed_at": snapshot["universe_observed_at"],
                "source_row_count": snapshot["source_row_count"],
                "stable_instrument_claim_count": snapshot["stable_instrument_claim_count"],
                "unresolved_row_count": snapshot["unresolved_row_count"],
                "claim_status_counts": snapshot["claim_status_counts"],
                "unresolved_status_counts": snapshot["unresolved_status_counts"],
                "absence_interpreted_as_out_of_scope": snapshot["absence_interpreted_as_out_of_scope"],
                "ledger": ledger_state,
            }
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 0
        if args.command == "verify":
            print(json.dumps(verify_membership_ledger(args.ledger), indent=2, sort_keys=True))
            return 0
        if args.command == "query":
            result = membership_as_of(args.ledger, instrument_id=args.instrument_id, as_of=args.as_of)
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        raise AssertionError(args.command)
    except (ProspectiveMembershipError, OSError, ValueError) as exc:
        print(f"QM-B PROSPECTIVE MEMBERSHIP ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
