from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_prospective import CLAIM_COLUMNS, OUTCOME_COLUMNS
from scanner.reports.confidence_vnext_walkforward import (
    Phase5WalkForwardConfig,
    frozen_baseline_evaluator,
    readiness_audit,
    read_shadow_csv,
)
from scanner.reports.selection_timing import HORIZONS


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 5A Confidence-vNext readiness and frozen-baseline audit"
    )
    parser.add_argument(
        "--claims",
        default="artifacts/research/confidence_vnext_shadow_claims_4e.csv",
    )
    parser.add_argument(
        "--outcomes",
        default="artifacts/research/confidence_vnext_shadow_outcomes_4e.csv",
    )
    parser.add_argument("--output", default="artifacts/research/confidence_vnext_readiness_5a.json")
    parser.add_argument("--freeze-commit", default="")
    parser.add_argument("--freeze-time", default="")
    args = parser.parse_args()

    claims = read_shadow_csv(args.claims, CLAIM_COLUMNS)
    outcomes = read_shadow_csv(args.outcomes, OUTCOME_COLUMNS)
    config = Phase5WalkForwardConfig()

    result = readiness_audit(
        claims,
        outcomes,
        freeze_commit=args.freeze_commit or None,
        freeze_time=args.freeze_time or None,
        config=config,
    )
    result["frozen_baseline"] = {
        str(horizon): frozen_baseline_evaluator(claims, outcomes, horizon=horizon)
        for horizon in HORIZONS
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "status": result["status"],
                "claims": result["shadow_archive"]["claims"],
                "mature_outcomes": result["shadow_archive"]["mature_outcomes"],
                "blockers": result["blockers"],
                "output": str(output),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
