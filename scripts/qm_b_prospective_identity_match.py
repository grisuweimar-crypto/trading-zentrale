#!/usr/bin/env python3
"""CLI for QM-B prospective listing snapshot identity matching."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_prospective_identity_match import (  # noqa: E402
    ProspectiveIdentityMatchError,
    load_universe_snapshot,
    match_listing_snapshot,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B match a prospective listing snapshot to stable instrument candidates")
    p.add_argument("--snapshot", required=True, help="normalized prospective listing snapshot JSON")
    p.add_argument("--universe", default=str(ROOT / "data" / "inputs" / "universe_master.csv"))
    p.add_argument("--universe-snapshot-id", required=True)
    p.add_argument("--universe-observed-at", required=True, help="timezone-aware ISO timestamp")
    p.add_argument("--output", help="optional JSON output path")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
        if not isinstance(snapshot, dict):
            raise ProspectiveIdentityMatchError("snapshot_json_must_be_object")
        rows, universe_sha = load_universe_snapshot(args.universe)
        result = match_listing_snapshot(
            snapshot,
            universe_rows=rows,
            universe_snapshot_id=args.universe_snapshot_id,
            universe_observed_at=args.universe_observed_at,
            universe_sha256=universe_sha,
        )
        text = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        else:
            print(text, end="")
        return 0
    except (OSError, json.JSONDecodeError, ProspectiveIdentityMatchError) as exc:
        print(f"QM-B IDENTITY MATCH ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
