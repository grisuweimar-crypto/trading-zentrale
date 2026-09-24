from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_prospective import run


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 4E prospective Confidence-vNext shadow validation")
    parser.add_argument("--phase4-report", required=True)
    parser.add_argument("--latest", default="artifacts/research/latest_scanner.csv")
    parser.add_argument("--metadata", default="artifacts/research/history_metadata.json")
    parser.add_argument("--claim-prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--prices", default="artifacts/research/price_backfill.csv")
    parser.add_argument("--phase2", default="artifacts/research/probability_calibration_2.json")
    parser.add_argument("--phase3", default="artifacts/research/risk_vnext_3.json")
    parser.add_argument("--risk-scale", default="artifacts/research/confidence_vnext_risk_scale_4.json")
    parser.add_argument("--claims", default="artifacts/research/confidence_vnext_shadow_claims_4e.csv")
    parser.add_argument("--outcomes", default="artifacts/research/confidence_vnext_shadow_outcomes_4e.csv")
    parser.add_argument("--summary", default="artifacts/research/confidence_vnext_shadow_4e.json")
    args = parser.parse_args()

    risk_scale = Path(args.risk_scale) if args.risk_scale and Path(args.risk_scale).exists() else None
    result = run(
        Path(args.phase4_report),
        Path(args.latest),
        Path(args.metadata),
        Path(args.claim_prices),
        Path(args.prices),
        Path(args.phase2),
        Path(args.phase3),
        Path(args.claims),
        Path(args.outcomes),
        Path(args.summary),
        risk_scale_path=risk_scale,
    )
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "status": result["status"],
                "claims": result["claims"],
                "mature_outcomes": result["mature_outcomes"],
                "current_snapshot": result["current_snapshot"],
                "claims_path": args.claims,
                "outcomes_path": args.outcomes,
                "summary_path": args.summary,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
