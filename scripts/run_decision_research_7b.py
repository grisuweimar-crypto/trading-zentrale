#!/usr/bin/env python3
"""Build the Phase-7B point-in-time Decision Research Dataset."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.decision_layer import DecisionDatasetConfig, run_dataset_build


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Phase-7B Decision Research Dataset")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--timing-catalog", default="artifacts/research/timing_patterns_1b_frozen.json")
    parser.add_argument("--contract", default="configs/decision_research_dataset_v1.json")
    parser.add_argument("--output", default="artifacts/research/decision_research_7b.csv")
    parser.add_argument("--metadata", default="artifacts/research/decision_research_7b_metadata.json")
    args = parser.parse_args()

    contract = json.loads(Path(args.contract).read_text(encoding="utf-8"))
    config = DecisionDatasetConfig.from_contract(contract)
    metadata = run_dataset_build(
        args.history,
        args.prices,
        args.timing_catalog,
        args.output,
        args.metadata,
        config,
    )
    print(json.dumps({
        "schema_version": metadata["schema_version"],
        "rows": metadata["rows"],
        "symbols": metadata["symbols"],
        "date_min": metadata["date_min"],
        "date_max": metadata["date_max"],
        "partitions": metadata["partitions"],
        "output": args.output,
        "metadata": args.metadata,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
