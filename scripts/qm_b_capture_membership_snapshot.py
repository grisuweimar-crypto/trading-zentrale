#!/usr/bin/env python3
"""CLI for durable QM-B prospective project-universe membership capture."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_durable_membership_capture import (  # noqa: E402
    DurableMembershipCaptureError,
    capture_universe_membership,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Capture durable prospective QM-B universe membership evidence")
    p.add_argument("--universe", required=True, help="exact universe_master CSV bytes from triggering commit")
    p.add_argument("--repo-root", default=str(ROOT))
    p.add_argument("--observed-at", required=True, help="actual timezone-aware capture timestamp")
    p.add_argument("--source-commit-sha", required=True)
    p.add_argument("--trigger", required=True)
    p.add_argument("--result-file", help="optional JSON result path")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        result = capture_universe_membership(
            args.universe,
            repo_root=args.repo_root,
            observed_at=args.observed_at,
            source_commit_sha=args.source_commit_sha,
            trigger=args.trigger,
        )
        text = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.result_file:
            target = Path(args.result_file)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        print(text, end="")
        return 0
    except (DurableMembershipCaptureError, OSError, ValueError) as exc:
        print(f"QM-B DURABLE MEMBERSHIP CAPTURE ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
