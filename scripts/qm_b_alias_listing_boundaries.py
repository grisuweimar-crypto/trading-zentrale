#!/usr/bin/env python3
"""CLI for QM-B alias PIT and listing-evidence boundary audit."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_alias_listing_boundaries import (  # noqa: E402
    AliasListingBoundaryError,
    build_alias_listing_boundary_audit,
)
from scanner.research.governance.qm_b_identity_reconciliation import IdentityReconciliationError  # noqa: E402
from scanner.research.governance.qm_b_observed_membership import ObservedMembershipError  # noqa: E402


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B audit PIT alias boundaries and listing evidence availability")
    p.add_argument("--history", default=str(ROOT / "artifacts" / "research" / "history_analysis.csv"))
    p.add_argument("--current-universe", default=str(ROOT / "data" / "inputs" / "universe_master.csv"))
    p.add_argument("--repo-root", default=str(ROOT))
    p.add_argument("--output")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        payload = build_alias_listing_boundary_audit(
            args.history,
            args.current_universe,
            repo_root=args.repo_root,
        )
        text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        else:
            print(text, end="")
        return 0
    except (AliasListingBoundaryError, IdentityReconciliationError, ObservedMembershipError, OSError) as exc:
        print(f"QM-B ALIAS/LISTING BOUNDARY ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
