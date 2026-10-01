from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scanner.reports.confidence_vnext_research import Phase4ResearchConfig
from scanner.reports.confidence_vnext_research_guarded import run


def _stamp_current_snapshot_identity(result: dict, latest_path: Path, output_path: Path) -> None:
    """Bind the guarded Phase-4 registry to the exact scanner publication.

    This adds provenance only. It does not change any Confidence formula,
    evidence state, threshold, model-agreement rule or current applicability
    guard.
    """
    latest = pd.read_csv(latest_path, dtype=str, keep_default_na=False)
    required = {"snapshot_id", "generated_at", "as_of"}
    missing = sorted(required.difference(latest.columns))
    if missing:
        raise ValueError("phase4_snapshot_identity_columns_missing:" + ",".join(missing))
    if latest.empty:
        raise ValueError("phase4_latest_scanner_empty")

    def unique(field: str) -> str:
        values = {str(value).strip() for value in latest[field] if str(value).strip()}
        if len(values) != 1:
            raise ValueError(f"phase4_{field}_not_unique")
        return next(iter(values))

    current = result.get("current")
    if not isinstance(current, dict):
        raise ValueError("phase4_current_registry_missing")
    current["snapshot_id"] = unique("snapshot_id")
    current["generated_at"] = unique("generated_at")
    scanner_as_of = unique("as_of")
    if str(current.get("as_of") or "") != scanner_as_of:
        raise ValueError("phase4_current_as_of_snapshot_mismatch")

    output_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 4B-D Confidence-vNext empirical research")
    parser.add_argument("--history", default="artifacts/research/history_analysis.csv")
    parser.add_argument("--latest", default="artifacts/research/latest_scanner.csv")
    parser.add_argument("--phase2", default="artifacts/research/probability_calibration_2.json")
    parser.add_argument("--risk", default="artifacts/research/risk_vnext_3.json")
    parser.add_argument("--risk-scale", default="artifacts/research/confidence_vnext_risk_scale_4.json")
    parser.add_argument("--output", default="artifacts/research/confidence_vnext_research_4.json")
    args = parser.parse_args()

    latest_path = Path(args.latest)
    output_path = Path(args.output)
    result = run(
        Path(args.history),
        latest_path,
        Path(args.phase2),
        Path(args.risk),
        output_path,
        Phase4ResearchConfig(),
        risk_scale_path=Path(args.risk_scale) if args.risk_scale else None,
    )
    _stamp_current_snapshot_identity(result, latest_path, output_path)
    print(
        json.dumps(
            {
                "phase": result["phase"],
                "as_of": result["current"]["as_of"],
                "snapshot_id": result["current"]["snapshot_id"],
                "scanner_generated_at": result["current"]["generated_at"],
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
