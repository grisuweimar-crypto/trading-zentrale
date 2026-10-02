#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_features import (
    build_insider_features,
    load_insider_features_inputs,
    write_insider_features,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8d_insider_features_v1.json"
DEFAULT_DIR = ROOT / "artifacts" / "external_evidence" / "8d_sec_insider"


def _read_asof_grid(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    if not rows:
        raise ValueError("as-of grid is empty")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build outcome-blind PIT-safe 8D-B4 insider activity features."
    )
    parser.add_argument(
        "--evidence",
        action="append",
        required=True,
        help="One or more external_evidence_8d_sec_insider_bulk_v1 JSON files; repeat the option per quarter.",
    )
    parser.add_argument(
        "--validation-result",
        default=str(DEFAULT_DIR / "validation_result_2026q2.json"),
    )
    parser.add_argument("--asof-grid", required=True, help="CSV with issuer_cik,as_of columns")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument(
        "--output",
        default=str(DEFAULT_DIR / "insider_features.json"),
    )
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if config.get("schema_version") != "external_evidence_8d_insider_features_v1":
        raise ValueError("unsupported B4 feature config")
    if config.get("market_outcomes_may_be_read") is not False:
        raise ValueError("B4 config must remain outcome-blind")

    evidence, validation = load_insider_features_inputs(
        evidence_paths=[Path(path) for path in args.evidence],
        validation_result_path=Path(args.validation_result),
    )
    windows = config["windows"]
    payload = build_insider_features(
        evidence_payloads=evidence,
        validation_result=validation,
        asof_grid=_read_asof_grid(Path(args.asof_grid)),
        primary_days=int(windows["primary_calendar_days"]),
        robustness_days=[int(value) for value in windows["robustness_calendar_days"]],
    )
    write_insider_features(payload, Path(args.output))
    print(
        json.dumps(
            {
                "status": payload["status"],
                "source_quarters": payload["source_quarters"],
                "row_count": len(payload["rows"]),
                "primary_window_days": payload["primary_window_days"],
                "robustness_window_days": payload["robustness_window_days"],
                "output": args.output,
                "market_outcomes_read": payload["guards"]["market_outcomes_read"],
                "numeric_features_promoted": payload["guards"]["numeric_features_promoted"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
