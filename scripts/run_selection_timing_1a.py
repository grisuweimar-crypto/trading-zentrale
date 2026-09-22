from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.selection_timing import Phase1AConfig, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 1A: selection vs timing research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--output", default="artifacts/research/selection_timing_1a.json")
    parser.add_argument("--stable-start", default="2026-04-15")
    parser.add_argument("--cooldown", type=int, default=5)
    args = parser.parse_args()

    config = Phase1AConfig(stable_start=args.stable_start, cooldown_sessions=args.cooldown)
    result = run(args.history, args.prices, args.output, config)
    print(json.dumps({
        "phase": result["phase"],
        "coverage": result["coverage"],
        "output": str(Path(args.output)),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
