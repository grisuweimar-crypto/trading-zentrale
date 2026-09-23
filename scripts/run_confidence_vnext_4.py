from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext import Phase4Config, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 4 Confidence-vNext research audit")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--output", default="artifacts/research/confidence_vnext_4.json")
    parser.add_argument("--metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--stable-start", default="2026-04-15")
    args = parser.parse_args()

    config = Phase4Config(stable_start=args.stable_start)
    result = run(
        Path(args.history),
        Path(args.output),
        config,
        metadata_path=Path(args.metadata) if args.metadata else None,
    )
    audit = result["history_audit"]
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "scanner_rows": audit["scanner_rows_after_same_day_symbol_dedup"],
                "scanner_dates": audit["scanner_dates"],
                "scanner_symbols": audit["scanner_symbols"],
                "first_confidence": audit["aggregate_confidence"]["first_observed_non_null_date"],
                "formula_epochs": audit["aggregate_confidence"]["formula_epochs"],
                "output": args.output,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
