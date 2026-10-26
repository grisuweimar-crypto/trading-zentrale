"""M4.1 historical fixed-date-zone tests on canonical completed daily OHLCV.

Output is a *later-retrieved historical proxy*, not certified market PIT. All
forecasts, including matched null bands, are created BEFORE future-bar outcomes.
Does not modify M4, M5, Scanner, Watch, Order or canonical market archives.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date
import hashlib
import json
from pathlib import Path

from scanner.research.zones_m41 import (
    M41_VERSION, ZoneObservationPolicy, freeze_zone, matched_control,
    evaluate_zone, summarize_events,
)
from scanner.research.zones_m4 import normalize_future_bars
from scanner.research.zones_v01 import build_zones, read_rows


def _json(x):
    return json.dumps(x, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False, default=str)


def replay_one(rows, symbol, *, cutoff, first_index=150, stride=60,
               max_snapshots=12, policy=ZoneObservationPolicy(), source_sha=None):
    if first_index < 80 or stride < 1 or max_snapshots < 1:
        raise ValueError("invalid_predeclared_grid")
    data = sorted((r for r in rows if r["date"] <= cutoff), key=lambda r:r["date"])
    if len({r["date"] for r in data}) != len(data):
        raise ValueError("duplicate_historical_market_day")
    indices = list(range(first_index, len(data) - policy.horizon_bars,
                         stride))[:max_snapshots]
    coverage = []
    forecast_rows, observed_rows = [], []
    for index in indices:
        historical = data[:index+1]
        asof = historical[-1]["date"]
        report = build_zones(historical, symbol, asof=asof, source_sha=source_sha)
        status = {
            "symbol": symbol, "asof": asof, "price_rows": len(historical),
            "snapshot_hash": report["data_hash"], "data_quality": report["data_quality"],
            "reason": report.get("warnings", []),
            "raw_daily": len(report["zones"]), "raw_weekly": len(report["weekly_zones"]),
            "clusters": len(report["zone_clusters"]), "frozen_predictions": 0,
            "matched_controls": 0,
        }
        coverage.append(status)
        if report["data_quality"] == "blocked":
            continue
        if not historical[-1].get("adj_close"):
            status["data_quality"] = "blocked"
            status["reason"].append("no_adj_close_asof")
            continue
        # NO outcomes read before predictions and null-control parameters freeze.
        forecast = []
        for scope, zones in (("raw_daily", report["zones"]),
                             ("raw_weekly", report["weekly_zones"]),
                             ("cluster", report["zone_clusters"])):
            for z in zones:
                f = freeze_zone(report, z, scope=scope, policy=policy)
                forecast.append(f)
                control = matched_control(f, policy)
                if control:
                    forecast.append(control)
                    status["matched_controls"] += 1
        status["frozen_predictions"] = len(forecast)
        later = data[index+1:index+1+policy.horizon_bars]
        observed = normalize_future_bars(
            later, asof=asof, asof_raw_close=historical[-1]["close"],
            asof_adj_close=historical[-1]["adj_close"])
        if len(observed) != policy.horizon_bars:
            raise ValueError("selected_history_incomplete")
        # ZONE evaluator can see outcomes; freeze_zone / matched_control cannot.
        outcome = [evaluate_zone(f, observed, policy=policy) for f in forecast]
        forecast_rows.extend(forecast)
        observed_rows.extend(outcome)
    return {
        "symbol": symbol, "input_rows": len(data),
        "coverage": coverage, "forecasts": forecast_rows, "outcomes": observed_rows,
    }


def run(input_path, symbols, *, cutoff, output_dir,
        first_index=150, stride=60, max_snapshots=12,
        policy=ZoneObservationPolicy()):
    date.fromisoformat(cutoff)
    inp = Path(input_path)
    source_sha = hashlib.sha256(inp.read_bytes()).hexdigest()
    by_symbol = read_rows(inp, set(symbols))
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    all_forecasts, all_outcomes, all_coverage = [], [], []
    symbols_coverage = {}
    for sym in sorted(set(symbols)):
        result = replay_one(by_symbol.get(sym, []), sym,
                            cutoff=cutoff, first_index=first_index,
                            stride=stride, max_snapshots=max_snapshots,
                            policy=policy, source_sha=source_sha)
        all_forecasts.extend(result["forecasts"])
        all_outcomes.extend(result["outcomes"])
        all_coverage.extend(result["coverage"])
        symbols_coverage[sym] = {
            "source_rows": result["input_rows"],
            "snapshots": len(result["coverage"]),
            "blocked_snapshots": sum(x["data_quality"] == "blocked" for x in result["coverage"]),
            "forecast_rows": len(result["forecasts"]),
            "counts_by_scope": dict(sorted(Counter(x["scope"]
                for x in result["forecasts"]).items())),
            "outcome_statuses": dict(sorted(Counter(x["status"]
                for x in result["outcomes"]).items())),
        }
    paths = {
        "m41_frozen_zone_forecasts.jsonl": all_forecasts,
        "m41_observed_zone_outcomes.jsonl": all_outcomes,
        "m41_asof_coverage.jsonl": all_coverage,
    }
    for name, rows in paths.items():
        (root/name).write_text("".join(_json(row)+"\n" for row in rows), encoding="utf-8")
    summary = summarize_events(all_outcomes, all_forecasts)
    summary.update({
        "source_sha256": source_sha,
        "forecast_ledger_sha256": hashlib.sha256((root/"m41_frozen_zone_forecasts.jsonl").read_bytes()).hexdigest(),
        "outcome_ledger_sha256": hashlib.sha256((root/"m41_observed_zone_outcomes.jsonl").read_bytes()).hexdigest(),
        "cutoff": cutoff, "source_retrospective_PIT_certified": False,
        "policy": policy.__dict__,
        "grid": {"first_index": first_index, "stride": stride, "max_snapshots": max_snapshots},
        "coverage": symbols_coverage,
    })
    (root/"m41_summary.json").write_text(
        json.dumps(summary, sort_keys=True, indent=2, ensure_ascii=False, default=str)+"\n",
        encoding="utf-8")
    return summary


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, default=Path("artifacts/market_data/yahoo_ohlcv.csv"))
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--cutoff", default="2026-10-21")
    p.add_argument("--output-dir", required=True)
    p.add_argument("--first-index", type=int, default=150)
    p.add_argument("--stride", type=int, default=60)
    p.add_argument("--max-snapshots", type=int, default=12)
    args=p.parse_args()
    print(_json(run(args.input, args.symbols, cutoff=args.cutoff,
                    output_dir=args.output_dir, first_index=args.first_index,
                    stride=args.stride, max_snapshots=args.max_snapshots)))


if __name__ == "__main__":
    main()
