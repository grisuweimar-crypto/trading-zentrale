#!/usr/bin/env python3
"""CLI for QM-B pre-strict as-of candidate evidence fusion."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_asof_candidate_fusion import (  # noqa: E402
    AsOfCandidateFusionError,
    fuse_asof_candidates,
    load_json,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B as-of candidate fusion")
    p.add_argument("--membership", required=True, help="normalized prospective membership snapshot JSON")
    p.add_argument("--listing", help="prospective listing snapshot JSON")
    p.add_argument("--identity-match", help="prospective identity-match result JSON")
    p.add_argument("--output")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        if bool(args.listing) != bool(args.identity_match):
            raise AsOfCandidateFusionError("listing_and_identity_match_must_be_supplied_together")
        membership = load_json(args.membership)
        listing = load_json(args.listing) if args.listing else None
        identity = load_json(args.identity_match) if args.identity_match else None
        result = fuse_asof_candidates(membership, listing_snapshot=listing, identity_match=identity)
        if args.output:
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({
            "schema_version": result["schema_version"],
            "candidate_count": result["candidate_count"],
            "fused_candidate_count": result["fused_candidate_count"],
            "status_counts": result["status_counts"],
            "strict_bundle_promotion_performed": result["strict_bundle_promotion_performed"],
            "strict_bundle_blocker": result["strict_bundle_blocker"],
        }, indent=2, sort_keys=True))
        return 0
    except (AsOfCandidateFusionError, OSError, ValueError) as exc:
        print(f"QM-B AS-OF FUSION ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
