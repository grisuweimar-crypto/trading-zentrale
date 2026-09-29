#!/usr/bin/env python3
"""CLI for legacy alias-presence audit of QM-B pre-boundary observations."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_legacy_alias_presence import (  # noqa: E402
    LegacyAliasPresenceError,
    audit_pre_boundary_legacy_alias_presence,
)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B audit legacy watchlist alias presence for pre-boundary observations")
    p.add_argument("--boundary-audit", required=True)
    p.add_argument("--repo-root", default=str(ROOT))
    p.add_argument("--output")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        boundary = json.loads(Path(args.boundary_audit).read_text(encoding="utf-8"))
        payload = audit_pre_boundary_legacy_alias_presence(boundary, repo_root=args.repo_root)
        text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        else:
            print(text, end="")
        return 0
    except (LegacyAliasPresenceError, OSError, json.JSONDecodeError) as exc:
        print(f"QM-B LEGACY ALIAS PRESENCE ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
