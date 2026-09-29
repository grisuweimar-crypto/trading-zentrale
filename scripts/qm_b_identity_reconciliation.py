#!/usr/bin/env python3
"""CLI for QM-B historical identity reconciliation candidates."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_identity_reconciliation import (  # noqa: E402
    IdentityReconciliationError,
    build_identity_reconciliation,
)
from scanner.research.governance.qm_b_observed_membership import ObservedMembershipError  # noqa: E402


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B reconcile historical identifier candidates")
    p.add_argument(
        "--history",
        default=str(ROOT / "artifacts" / "research" / "history_analysis.csv"),
        help="preserved research-history CSV",
    )
    p.add_argument(
        "--current-universe",
        default=str(ROOT / "data" / "inputs" / "universe_master.csv"),
        help="current universe used as current-only cross-check",
    )
    p.add_argument("--repo-root", default=str(ROOT), help="git repository root used for immutable historical snapshots")
    p.add_argument("--output", help="optional JSON output path")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        payload = build_identity_reconciliation(
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
    except (IdentityReconciliationError, ObservedMembershipError, OSError) as exc:
        print(f"QM-B IDENTITY RECONCILIATION ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
