from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_research import Phase4ResearchConfig
from scanner.reports.confidence_vnext_research_guarded import run


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 4B-D Confidence-vNext empirical research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--latest", default="artifacts/research/latest_scanner.csv")
    parser.add_argument("--phase2", default="artifacts/research/probability_calibration_2.json")
    parser.add_argument("--risk", default="artifacts/research/risk_vnext_3.json")
    parser.add_argument("--risk-scale", default="artifacts/research/confidence_vnext_risk_scale_4.json")
    parser.add_argument("--output", default="artifacts/research/confidence_vnext_research_4.json")
    args = parser.parse_args()

    result = run(
        Path(args.history),
        Path(args.latest),
        Path(args.phase2),
        Path(args.risk),
        Path(args.output),
        Phase4ResearchConfig(),
        risk_scale_path=Path(args.risk_scale) if args.risk_scale else None,
    )
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "as_of": result["current"]["as_of"],
                "scanner_rows": result["current"]["scanner_rows"],
                "excluded_unsupported_crypto_rows": result["current"]["excluded_unsupported_crypto_rows"],
                "agreement_counts": result["agreement_counts"],
                "risk_metric_applicability": result["current"]["risk_metric_applicability"],
                "output": args.output,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
