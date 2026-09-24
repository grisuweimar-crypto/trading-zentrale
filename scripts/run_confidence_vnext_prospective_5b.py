from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_frozen_baseline import run_frozen_baseline
from scanner.reports.confidence_vnext_prospective_v2 import run_v2


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 5B prospective v2 evidence and frozen baseline")
    parser.add_argument("--phase4-report", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--latest", default="artifacts/research/latest_scanner.csv")
    parser.add_argument("--metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--claim-prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--phase2", default="artifacts/research/probability_calibration_2.json")
    parser.add_argument("--phase3", default="artifacts/research/risk_vnext_3.json")
    parser.add_argument("--risk-scale", default="artifacts/research/confidence_vnext_risk_scale_4.json")
    parser.add_argument("--claims", default="artifacts/research/confidence_vnext_shadow_claims_5b_v2.csv")
    parser.add_argument("--outcomes", default="artifacts/research/confidence_vnext_shadow_outcomes_5b_v2.csv")
    parser.add_argument("--baseline", default="artifacts/research/confidence_vnext_frozen_baseline_5b.json")
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    args = parser.parse_args()

    risk_scale = Path(args.risk_scale) if args.risk_scale and Path(args.risk_scale).exists() else None
    stream = run_v2(
        Path(args.phase4_report),
        Path(args.history),
        Path(args.latest),
        Path(args.metadata),
        Path(args.claim_prices),
        Path(args.prices),
        Path(args.phase2),
        Path(args.phase3),
        Path(args.claims),
        Path(args.outcomes),
        source_commit=args.source_commit,
        risk_scale_path=risk_scale,
    )
    baseline = run_frozen_baseline(
        Path(args.claims),
        Path(args.outcomes),
        Path(args.baseline),
        bootstrap_reps=args.bootstrap_reps,
    )
    print(
        json.dumps(
            {
                "stream": stream,
                "baseline_status": baseline["status"],
                "claims_path": args.claims,
                "outcomes_path": args.outcomes,
                "baseline_path": args.baseline,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
