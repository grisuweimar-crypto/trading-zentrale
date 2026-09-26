#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scanner.research.external_evidence.bulk_fundamental_evaluation import (
    evaluate_sec_bulk_fundamentals,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the frozen Phase 8C-C fundamental engine against a local, "
            "authoritative SEC bulk snapshot and emit compact research artifacts."
        )
    )
    parser.add_argument(
        "--bundle-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "sec_bulk_snapshot"),
    )
    parser.add_argument(
        "--config",
        default=str(ROOT / "configs" / "external_evidence_8c_fundamental_change_v1.json"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "artifacts" / "external_evidence" / "8c_real_fundamentals"),
    )
    args = parser.parse_args()

    summary = evaluate_sec_bulk_fundamentals(
        bundle_dir=Path(args.bundle_dir),
        config_path=Path(args.config),
        output_dir=Path(args.output_dir),
    )
    print(
        json.dumps(
            {
                "schema_version": summary["schema_version"],
                "counts": summary["counts"],
                "metric_coverage": summary["metric_coverage"],
                "feature_counts": summary["feature_counts"],
                "output_dir": str(Path(args.output_dir)),
                "summary": str(Path(args.output_dir) / "summary.json"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
