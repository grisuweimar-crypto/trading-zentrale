"""Publish validated scanner views and a separate price backfill."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.research_views import ValidationPolicy, build_views


def publish_history_analysis(root: Path, *, policy=None) -> Path:
    """Compatibility entry point; inspect history_metadata.json for validation."""
    build_views(root, policy=policy)
    return root / "artifacts/research/history_analysis.csv"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-symbol-count", type=int)
    parser.add_argument("--min-symbol-ratio", type=float, default=1.0)
    parser.add_argument("--min-score-ratio", type=float, default=0.90)
    args = parser.parse_args()
    policy = ValidationPolicy(expected_symbol_count=args.expected_symbol_count,
                              min_symbol_ratio=args.min_symbol_ratio,
                              min_score_ratio=args.min_score_ratio)
    metadata = build_views(Path(__file__).resolve().parents[1], policy=policy)
    print(json.dumps(metadata, indent=2, ensure_ascii=False))
    # Incomplete scans are a recorded state; publish metadata and retain good views.
    # Failed writes and errors in retained files still raise and fail CI.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
