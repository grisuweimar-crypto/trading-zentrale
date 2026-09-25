from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_promotion import run_phase5_completion


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 5E adaptive shadow inference and promotion gate"
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
        default="artifacts/research/confidence_vnext_promotion_5e.json",
    )
    parser.add_argument("--bootstrap-reps", type=int, default=500)
    parser.add_argument("--random-seed", type=int, default=20260925)
    args = parser.parse_args()

    result = run_phase5_completion(
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
