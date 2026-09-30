#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.governance.qm_b_provider_outcome_availability import run_provider_outcome_audit


def main() -> int:
    parser = argparse.ArgumentParser(description="Run frozen QM-B provider coverage and outcome availability audit")
    parser.add_argument("--root", default=".")
    parser.add_argument("--contract", default="configs/qm_b_provider_outcome_availability_v1.json")
    parser.add_argument("--output")
    args = parser.parse_args()

    report = run_provider_outcome_audit(Path(args.root), contract_path=Path(args.contract))
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    summary = {
        "audit_gate_status": report["audit_gate_status"],
        "remaining_research_status": report["remaining_research_status"],
        "metadata_as_of": report["metadata_as_of"],
        "provider_classification_counts": report["provider_coverage"]["classification_counts"],
        "provider_snapshot_pit_reachability_count": report["provider_coverage"]["snapshot_pit_reachability_count"],
        "scanner_event_count": report["outcome_availability"]["scanner_event_count"],
        "outcome_horizons": {
            horizon: data["availability_counts"]
            for horizon, data in report["outcome_availability"]["horizons"].items()
        },
        "price_diagnostics": {
            key: report["outcome_availability"]["price_diagnostics"][key]
            for key in ("valid_rows_as_of", "symbol_count", "retrieved_at_known_rows", "retrieved_at_unknown_rows")
        },
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
