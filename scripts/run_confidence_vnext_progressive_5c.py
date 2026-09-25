from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.reports.confidence_vnext_progressive import run_progressive_learning


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Phase 5C progressive horizon-specific shadow learning"
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
        "--output",
        default="artifacts/research/confidence_vnext_progressive_5c.json",
    )
    args = parser.parse_args()

    result = run_progressive_learning(
        Path(args.claims),
        Path(args.outcomes),
        Path(args.versions),
        Path(args.output),
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
