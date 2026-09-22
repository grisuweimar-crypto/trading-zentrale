from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.timing_patterns import Phase1BConfig, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 1B: timing-pattern research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--output", default="artifacts/research/timing_patterns_1b.json")
    parser.add_argument("--stable-start", default="2026-04-15")
    parser.add_argument("--discovery-end", default="2026-07-31")
    parser.add_argument("--validation-start", default="2026-08-01")
    parser.add_argument("--cooldown", type=int, default=5)
    args = parser.parse_args()

    config = Phase1BConfig(
        stable_start=args.stable_start,
        discovery_end=args.discovery_end,
        validation_start=args.validation_start,
        cooldown_sessions=args.cooldown,
    )
    result = run(args.history, args.prices, args.output, config)
    print(json.dumps({
        "phase": result["phase"],
        "events": result["events"],
        "coverage": result["coverage"],
        "output": str(Path(args.output)),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
