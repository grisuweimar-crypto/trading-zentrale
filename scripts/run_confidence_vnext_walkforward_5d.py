from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_walkforward_v2 import (
    run_walkforward_evaluation,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 5D prospective walk-forward evaluation"
    )
    parser.add_argument(
        "--claims",
        default="artifacts/research/confidence_vnext_shadow_claims_5b_v2.csv",
    )
    parser.add_argument(
        "--outcomes",
        default="artifacts/research/confidence_vnext_shadow_outcomes_5b_v2.csv",
    )
    parser.add_argument(
        "--versions",
        default="artifacts/research/confidence_vnext_model_versions_5c.jsonl",
    )
    parser.add_argument(
        "--evaluations",
        default="artifacts/research/confidence_vnext_walkforward_evaluations_5d.jsonl",
    )
    parser.add_argument(
        "--report",
        default="artifacts/research/confidence_vnext_walkforward_5d.json",
    )
    parser.add_argument("--bootstrap-reps", type=int, default=200)
    parser.add_argument("--random-seed", type=int, default=20260925)
    args = parser.parse_args()

    result = run_walkforward_evaluation(
        Path(args.claims),
        Path(args.outcomes),
        Path(args.versions),
        Path(args.evaluations),
        Path(args.report),
        bootstrap_reps=args.bootstrap_reps,
        random_seed=args.random_seed,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
