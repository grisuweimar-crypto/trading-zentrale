from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.probability_calibration import Phase2Config, run


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 2: probability calibration research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--frozen-patterns", default="artifacts/research/timing_patterns_1b_frozen.json")
    parser.add_argument("--metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--output", default="artifacts/research/probability_calibration_2.json")
    parser.add_argument("--stable-start", default="2026-04-15")
    parser.add_argument("--discovery-end", default="2026-07-31")
    parser.add_argument("--validation-start", default="2026-08-01")
    parser.add_argument("--cooldown", type=int, default=5)
    parser.add_argument("--prior-strength", type=float, default=20.0)
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    args = parser.parse_args()

    config = Phase2Config(
        stable_start=args.stable_start,
        discovery_end=args.discovery_end,
        validation_start=args.validation_start,
        cooldown_sessions=args.cooldown,
        prior_strength=args.prior_strength,
        cluster_bootstrap_reps=args.bootstrap_reps,
    )
    result = run(
        args.history,
        args.prices,
        args.output,
        config,
        frozen_patterns_path=args.frozen_patterns,
        metadata_path=args.metadata,
    )
    summary = {
        "phase": result["phase"],
        "source": result.get("source", {}),
        "coverage": result["coverage"],
        "validation": {
            h: {
                "maturity": payload.get("validation_maturity", {}),
                "alpha_supported": len(payload.get("timing_patterns", {}).get("alpha_supported", [])),
                "joint_supported": len(payload.get("timing_patterns", {}).get("joint_supported", [])),
                "strong_supported": len(payload.get("timing_patterns", {}).get("strong_supported", [])),
            }
            for h, payload in result["horizons"].items()
        },
        "output": str(Path(args.output)),
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
