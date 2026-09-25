#!/usr/bin/env python3
"""Run Phase-7C confirmation/conflict research on the frozen 7B dataset."""
from __future__ import annotations

import argparse
import json

from scanner.research.decision_layer.conflict_research import run_conflict_research


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase-7C evidence confirmation/conflict research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--timing-catalog", default="artifacts/research/timing_patterns_1b_frozen.json")
    parser.add_argument("--dataset-contract", default="configs/decision_research_dataset_v1.json")
    parser.add_argument("--conflict-contract", default="configs/decision_conflict_research_v1.json")
    parser.add_argument("--output", default="artifacts/research/decision_conflict_7c.json")
    args = parser.parse_args()

    report = run_conflict_research(
        args.history,
        args.prices,
        args.timing_catalog,
        args.dataset_contract,
        args.conflict_contract,
        args.output,
    )
    summary = {
        "schema_version": report["schema_version"],
        "rows_used_for_discovery": report["rows_used_for_discovery"],
        "symbols_used_for_discovery": report["symbols_used_for_discovery"],
        "promotion_gate": report["promotion_gate"],
        "output": args.output,
        "comparisons": {
            horizon: {
                name: block["discovery_status"]
                for name, block in details["pre_registered_comparisons"].items()
            }
            for horizon, details in report["horizons"].items()
        },
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
