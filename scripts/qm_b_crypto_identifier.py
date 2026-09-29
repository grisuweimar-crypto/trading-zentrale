#!/usr/bin/env python3
"""CLI for QM-B crypto identifier lineage diagnostics."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_crypto_identifier import (  # noqa: E402
    CryptoIdentifierError,
    build_crypto_identifier_timeline,
)
from scanner.research.governance.qm_b_observed_membership import ObservedMembershipError  # noqa: E402


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B diagnose historical crypto identifier namespace migration")
    p.add_argument(
        "--history",
        default=str(ROOT / "artifacts" / "research" / "history_analysis.csv"),
        help="preserved research-history CSV",
    )
    p.add_argument(
        "--current-universe",
        default=str(ROOT / "data" / "inputs" / "universe_master.csv"),
        help="current universe master used only to show current crypto symbol candidates",
    )
    p.add_argument("--output", help="optional JSON output path")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        history = Path(args.history)
        payload = build_crypto_identifier_timeline(
            history,
            current_universe_path=Path(args.current_universe) if args.current_universe else None,
            source_path_label=history.as_posix(),
        )
        text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        if args.output:
            target = Path(args.output)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        else:
            print(text, end="")
        return 0
    except (CryptoIdentifierError, ObservedMembershipError, OSError) as exc:
        print(f"QM-B CRYPTO IDENTIFIER ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
