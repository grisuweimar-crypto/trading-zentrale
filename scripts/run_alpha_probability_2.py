from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.alpha_probability import Phase2Config, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 2: alpha/probability research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--output", default="artifacts/research/alpha_probability_2.json")
    parser.add_argument("--stable-start", default="2026-04-15")
    parser.add_argument("--discovery-end", default="2026-07-31")
    parser.add_argument("--validation-start", default="2026-08-01")
    parser.add_argument("--cooldown", type=int, default=5)
    parser.add_argument("--prior-strength", type=float, default=20.0)
    parser.add_argument("--bootstrap-rounds", type=int, default=250)
    args = parser.parse_args()

    config = Phase2Config(
        stable_start=args.stable_start,
        discovery_end=args.discovery_end,
        validation_start=args.validation_start,
        cooldown_sessions=args.cooldown,
        prior_strength=args.prior_strength,
        bootstrap_rounds=args.bootstrap_rounds,
    )
    result = run(args.history, args.prices, args.output, config)
    print(json.dumps({
        "phase": result["phase"],
        "events": result["events"],
        "output": str(Path(args.output)),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
