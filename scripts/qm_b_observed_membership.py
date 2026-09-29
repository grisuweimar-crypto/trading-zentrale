#!/usr/bin/env python3
"""CLI for QM-B historical observed-membership evidence classification."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from scanner.research.governance.qm_b_observed_membership import (  # noqa: E402
    ObservedMembershipError,
    analyze_history,
    scanner_membership_evidence,
)


def _write(payload, path: str | None) -> None:
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    else:
        print(text, end="")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QM-B classify historical scanner membership evidence")
    p.add_argument(
        "--history",
        default=str(ROOT / "artifacts" / "research" / "history_analysis.csv"),
        help="preserved research-history CSV",
    )
    p.add_argument(
        "--current-universe",
        default=str(ROOT / "data" / "inputs" / "universe_master.csv"),
        help="current universe master used only for non-causal symbol-set diagnostics",
    )
    p.add_argument("--summary-output")
    p.add_argument("--candidate-output")
    p.add_argument("--scanner-evidence-output")
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        history = Path(args.history)
        current = Path(args.current_universe) if args.current_universe else None
        ledger, summary = analyze_history(
            history,
            current_universe_path=current,
            source_path_label=history.as_posix(),
        )
        _write(summary, args.summary_output)
        if not args.summary_output:
            pass
        if args.candidate_output:
            _write(ledger, args.candidate_output)
        if args.scanner_evidence_output:
            _write(
                {
                    "schema_version": "qm_b_scanner_observed_presence_evidence_v1",
                    "strict_membership_ledger": False,
                    "stable_identity_verified": False,
                    "absence_interpreted_as_out_of_scope": False,
                    "rows": scanner_membership_evidence(ledger),
                },
                args.scanner_evidence_output,
            )
        return 0
    except (ObservedMembershipError, OSError, json.JSONDecodeError) as exc:
        print(f"QM-B OBSERVED MEMBERSHIP ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
