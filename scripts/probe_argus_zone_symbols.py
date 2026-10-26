"""Optional read-only Yahoo coverage probe for missing ARGUS zone symbols.

Outputs isolated research artifacts under --output-dir; NEVER changes the
canonical OHLCV cache, scanner, Watch or brokerage state.  A failed provider
lookup produces no fabricated OHLCV sessions or price zones.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, date, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import yfinance

from scripts.prefetch_market_history import _download
from scanner.research.zones_v01 import build_zones, read_rows
from scanner.research.zones_m3 import write_readable_tables

MISSING = ("AMD", "INTC", "ARM", "SMH")
CURRENCY = {s: "USD" for s in MISSING}
FIELDS = ("date", "symbol", "currency", "open", "high", "low", "close",
          "adj_close", "volume", "retrieved_at")


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--asof", default="2026-10-21")
    cli.add_argument("--base-cache", type=Path, default=Path("artifacts/market_data/yahoo_ohlcv.csv"))
    cli.add_argument("--output-dir", type=Path, required=True)
    args = cli.parse_args()
    end = (date.fromisoformat(args.asof) + timedelta(days=1)).isoformat()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    downloaded_at = datetime.now(timezone.utc).isoformat()
    cache = read_rows(args.base_cache, {"NOW"})
    records = list(cache["NOW"])
    provider_issue = None
    errors = {}
    try:
        # Existing productive adapter: unadjusted OHLCV + separate adj_close.
        frame = _download(list(MISSING), start="2024-06-01", end=end, threads=2)
        errors = dict(frame.attrs.get("errors", {}))
        for row in frame.to_dict("records"):
            symbol = str(row.get("symbol", ""))
            if symbol not in CURRENCY or str(row.get("date", "")) > args.asof:
                continue
            records.append({
                "date": str(row["date"]), "symbol": symbol,
                "currency": CURRENCY[symbol],
                **{key: "" if pd.isna(row.get(key)) else str(row[key])
                   for key in ("open", "high", "low", "close", "adj_close", "volume")},
                "retrieved_at": downloaded_at,
            })
    except Exception as exc:
        provider_issue = f"{type(exc).__name__}: {exc}"
    # The receipt is preserved even for empty or bad provider data.
    data_csv = out / "isolated_source_ohlcv.csv"
    with data_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(FIELDS))
        writer.writeheader()
        for row in records:
            writer.writerow({k: row.get(k, "") for k in FIELDS})
    parsed = read_rows(data_csv, set(MISSING) | {"NOW"})
    coverage = {}
    for sym in (*MISSING, "NOW"):
        sample = parsed[sym]
        coverage[sym] = {
            "rows": len(sample),
            "first": min((x["date"] for x in sample), default=None),
            "last": max((x["date"] for x in sample), default=None),
            "adjusted_missing": sum(not x.get("adj_close") for x in sample),
            "provider_error": errors.get(sym) if sym in MISSING else None,
        }
        report = build_zones(sample, sym, asof=args.asof, exchange="unknown",
                             source_sha=None if sym in MISSING else "e0d758a7b3060d57ce3918927de41950fc10f44d")
        report_path = out / f"{sym}_{args.asof}_zones.json"
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8")
        write_readable_tables(report, report_path)
        coverage[sym]["candidate_zones"] = len(report["zones"])
        coverage[sym]["weekly_zones"] = len(report["weekly_zones"])
        coverage[sym]["m3_clusters"] = len(report["zone_clusters"])
        coverage[sym]["m3_plans"] = len(report["scenario_plans"])
        by_cluster = {c["cluster_id"]: c for c in report["zone_clusters"]}
        def zone_band(cluster_id):
            z = by_cluster[cluster_id]
            return [z["lower"], z["upper"]]
        sample = []
        for side in ("bull", "bear"):
            scenario = next((p for p in report["scenario_plans"]
                             if p["side"] == side), None)
            if scenario:
                sample.append({
                    "side": side, "entry": zone_band(scenario["entry_cluster_id"]),
                    "invalidation": zone_band(scenario["invalidation_cluster_id"]),
                    "targets": [zone_band(x) for x in scenario["target_cluster_ids"]],
                    "status": scenario["state"], "evidence": scenario["evidence_status"],
                })
        coverage[sym]["sample_m3_plans"] = sample
        coverage[sym]["quality"] = report["data_quality"]
    receipt = {
        "source": "read_only_existing_yahoo_adapter", "retrieved_at": downloaded_at,
        "requested_symbols": MISSING, "asof": args.asof,
        "provider_version": yfinance.__version__, "provider_issue": provider_issue,
        "adapter_errors": errors, "input_csv_sha256": hashlib.sha256(data_csv.read_bytes()).hexdigest(),
        "coverage": coverage,
        "not_historical_point_in_time_verified": True,
        "not_productive": True,
    }
    (out / "fetch_receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(receipt, sort_keys=True))
    # Data unavailability is a valid research result; inspect the receipt.
    # It must never silently become a positive backtest observation.


if __name__ == "__main__":
    main()
