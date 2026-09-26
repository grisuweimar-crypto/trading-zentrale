#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from scanner.research.external_evidence.sec_insider_identity import (
    load_identity_evidence,
    resolve_insider_asof_identity,
    write_identity_result,
)
from scanner.research.external_evidence.sec_insider_identity_grid import (
    verified_feature_grid,
    write_feature_grid_csv,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIR = ROOT / "artifacts" / "external_evidence" / "8d_sec_insider"
DEFAULT_CONFIG = ROOT / "configs" / "external_evidence_8d_insider_identity_v1.json"


def _read_scanner_observations(path: Path, *, symbol_column: str, asof_column: str) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("scanner history CSV has no header")
        if symbol_column not in reader.fieldnames:
            raise ValueError(f"scanner history missing symbol column {symbol_column!r}")
        if asof_column not in reader.fieldnames:
            raise ValueError(f"scanner history missing as-of column {asof_column!r}")
        rows = []
        for raw in reader:
            rows.append(
                {
                    "symbol": raw.get(symbol_column),
                    "as_of": raw.get(asof_column),
                    "listing_venue": raw.get("listing_venue") or raw.get("exchange") or None,
                }
            )
    if not rows:
        raise ValueError("scanner history CSV is empty")
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve Phase 8D-B5 family-specific SEC insider issuer CIK identity as of historical scanner observations."
    )
    parser.add_argument(
        "--evidence",
        action="append",
        required=True,
        help="One or more external_evidence_8d_sec_insider_bulk_v1 JSON files; repeat per source quarter.",
    )
    parser.add_argument("--scanner-history", required=True, help="Historical scanner CSV used as the project observability ledger.")
    parser.add_argument("--symbol-column", default="symbol")
    parser.add_argument(
        "--asof-column",
        default="as_of",
        help="Must contain timezone-aware timestamps. Date-only values are intentionally rejected.",
    )
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--output", default=str(DEFAULT_DIR / "insider_asof_identity.json"))
    parser.add_argument(
        "--feature-grid-output",
        default=str(DEFAULT_DIR / "insider_feature_grid.csv"),
        help="B4 grid containing only VERIFIED_FAMILY_PIT_IDENTITY issuer_cik/as_of pairs.",
    )
    args = parser.parse_args()

    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    if config.get("schema_version") != "external_evidence_8d_insider_identity_v1":
        raise ValueError("unsupported B5 identity config")
    if config.get("market_outcomes_may_be_read") is not False:
        raise ValueError("B5 identity config must remain outcome-blind")

    observations = _read_scanner_observations(
        Path(args.scanner_history),
        symbol_column=args.symbol_column,
        asof_column=args.asof_column,
    )
    payload = resolve_insider_asof_identity(
        scanner_observations=observations,
        evidence_payloads=load_identity_evidence([Path(path) for path in args.evidence]),
    )
    write_identity_result(payload, Path(args.output))
    feature_grid = verified_feature_grid(payload)
    write_feature_grid_csv(feature_grid, Path(args.feature_grid_output))
    print(
        json.dumps(
            {
                "status": payload["status"],
                "row_count": payload["row_count"],
                "identity_evidence_point_count": payload["identity_evidence_point_count"],
                "identity_status_counts": payload["identity_status_counts"],
                "verified_feature_grid_row_count": len(feature_grid),
                "output": args.output,
                "feature_grid_output": args.feature_grid_output,
                "market_outcomes_read": payload["guards"]["market_outcomes_read"],
                "current_ticker_retrojection_enabled": payload["guards"]["current_ticker_retrojection_enabled"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
