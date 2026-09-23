from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.risk_vnext import Phase3Config, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 3 Risk-vNext research audit")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--output", default="artifacts/research/risk_vnext_3.json")
    parser.add_argument("--metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    parser.add_argument("--min-feature-n", type=int, default=30)
    args = parser.parse_args()

    config = Phase3Config(
        cluster_bootstrap_reps=args.bootstrap_reps,
        min_feature_n=args.min_feature_n,
    )
    result = run(
        Path(args.history),
        Path(args.prices),
        Path(args.output),
        config,
        metadata_path=Path(args.metadata) if args.metadata else None,
    )
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "events": result["events"],
                "coverage": result["coverage"]["features"],
                "output": args.output,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
