"""M4 fixed-grid historical zone replay; isolated research files, no production writes.

The entire CSV is a later-acquired archive, not vintage point-in-time market
data. Forecasts are rebuilt with a hard as-of slice; outcome bars are handed
only to the evaluator. Deterministic sampling is fixed by index/stride, never
conditioned on future returns or favorable results.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from scanner.research.zones_v01 import build_zones, read_rows
from scanner.research.zones_m4 import (
    M4_VERSION, ReplayPolicy, _digest, evaluate_frozen,
    normalize_future_bars, summarize, _cost_adjusted, _pct,
)


def _encode(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False, default=str)


def replay_symbol(rows, symbol, *, cutoff, stride=20, first_index=150,
                  max_snapshots=30, policy=ReplayPolicy(), source_sha=None):
    if stride < 1 or first_index < 80 or max_snapshots < 1:
        raise ValueError("invalid_selection_grid")
    # Never silently drop duplicate sessions (first/last wins would be biased).
    sorted_rows = sorted((r for r in rows if r["date"] <= cutoff), key=lambda r: r["date"])
    if any(x["date"] == y["date"] for x, y in zip(sorted_rows, sorted_rows[1:])):
        raise ValueError("duplicate_market_sessions")
    eligible = list(range(first_index, len(sorted_rows) - policy.horizon_bars,
                          stride))[:max_snapshots]
    frozen, outcomes, index_rows = [], [], []
    for i in eligible:
        historical = sorted_rows[:i + 1]
        asof = historical[-1]["date"]
        # build_zones sees ONLY observations through asof.
        report = build_zones(historical, symbol, asof=asof,
                             source_sha=source_sha)
        plans = report["scenario_plans"]
        index_rows.append({
            "symbol": symbol, "asof": asof, "price_rows": len(historical),
            "baseline_long_20bar_net_pct": None,
            "baseline_short_20bar_net_pct": None,
            "data_quality": report["data_quality"],
            "model_version": report["model_version"],
            "m3_status": report["m3_status"],
            "frozen_plans": len(plans),
            "snapshot_hash": report["data_hash"],
        })
        if (historical[-1].get("adj_close") in ("", None) or
                report["data_quality"] == "blocked"):
            # No usable price scale => no evaluation rather than guess.
            continue
        later = sorted_rows[i + 1:i + 1 + policy.horizon_bars]
        observed = normalize_future_bars(
            later, asof=asof,
            asof_raw_close=historical[-1]["close"],
            asof_adj_close=historical[-1]["adj_close"])
        if len(observed) != policy.horizon_bars:
            raise ValueError("incomplete_selected_future_window")
        # Truly plan-independent controls, ONE baseline per eligible as-of
        # snapshot, even when that snapshot generated no setup.
        index_rows[-1]["baseline_long_20bar_net_pct"] = _cost_adjusted(
            _pct("bull", observed[0]["open"], observed[-1]["close"]), policy)
        index_rows[-1]["baseline_short_20bar_net_pct"] = _cost_adjusted(
            _pct("bear", observed[0]["open"], observed[-1]["close"]), policy)
        if not plans:
            continue
        lookup = {c["cluster_id"]: c for c in report["zone_clusters"]}
        for p in plans:
            selected_ids = [p["entry_cluster_id"], p["invalidation_cluster_id"],
                            *p["target_cluster_ids"]]
            referenced = [lookup[z] for z in selected_ids]
            snapshot = {
                "symbol": symbol, "asof": asof,
                "forecast_known_at": p["known_at"],
                "data_quality": report["data_quality"],
                "snapshot_hash": report["data_hash"],
                "source_sha256": source_sha,
                "model_version": report["model_version"],
                "m3_version": report["m3_version"],
                "m4_version": M4_VERSION,
                "scenario": p,
                "clusters": referenced,
                "policy": policy.__dict__,
            }
            snapshot["forecast_sha256"] = _digest(snapshot)
            frozen.append(snapshot)
            event = evaluate_frozen(report, p, observed, policy)
            event["forecast_sha256"] = snapshot["forecast_sha256"]
            event["source_sha256"] = source_sha
            outcomes.append(event)
    return {"symbol": symbol, "asof_index": index_rows,
            "frozen": frozen, "outcomes": outcomes,
            "full_source_rows": len(sorted_rows)}


def run(input_path, symbols, *, cutoff, output_dir, stride=20,
        first_index=150, max_snapshots=30, policy=ReplayPolicy()):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    input_path = Path(input_path)
    source_sha = hashlib.sha256(input_path.read_bytes()).hexdigest()
    by_symbol = read_rows(input_path, set(symbols))
    all_frozen, all_outcomes, indexed = [], [], []
    coverage = {}
    for symbol in sorted(set(symbols)):
        result = replay_symbol(by_symbol.get(symbol, []), symbol,
                               cutoff=cutoff, stride=stride,
                               first_index=first_index,
                               max_snapshots=max_snapshots,
                               policy=policy, source_sha=source_sha)
        coverage[symbol] = {
            "rows": result["full_source_rows"],
            "snapshots": len(result["asof_index"]),
            "blocked_snapshots": sum(
                x["data_quality"] == "blocked" for x in result["asof_index"]),
            "no_plan_snapshots": sum(
                x["frozen_plans"] == 0 for x in result["asof_index"]),
            "forecasts": len(result["outcomes"]),
            "outcome_counts": dict(sorted(Counter(
                x["outcome"] for x in result["outcomes"]).items())),
        }
        all_frozen.extend(result["frozen"])
        all_outcomes.extend(result["outcomes"])
        indexed.extend(result["asof_index"])
    # Serialize forecasts FIRST, in a distinct file, before outcome data.
    files = {
        "forecast_ledger.jsonl": all_frozen,
        "outcome_ledger.jsonl": all_outcomes,
        "asof_coverage.jsonl": indexed,
    }
    for name, records in files.items():
        (output_dir / name).write_text(
            "".join(_encode(obj) + "\n" for obj in records), encoding="utf-8")
    summary = summarize(all_outcomes)
    independent_baselines = [
        x for x in indexed if x["baseline_long_20bar_net_pct"] is not None]
    summary["naive_directional_controls"] = {
        "comparison_note": "one independent-of-plan same-symbol 20-bar hold per selected asof; NOT matched holding periods or risk exposures",
        "snapshots": len(independent_baselines),
        "mean_long_net_pct": (
            round(sum(x["baseline_long_20bar_net_pct"] for x in independent_baselines)
                  / len(independent_baselines), 6)
            if independent_baselines else None),
        "mean_short_net_pct": (
            round(sum(x["baseline_short_20bar_net_pct"] for x in independent_baselines)
                  / len(independent_baselines), 6)
            if independent_baselines else None),
        "independent_repeated_observations": False,
        "not_an_alpha_test": True,
    }
    summary.update({
        "source_sha256": source_sha, "selection": {
            "cutoff": cutoff, "stride": stride, "first_index": first_index,
            "max_snapshots": max_snapshots,
        }, "policy": policy.__dict__,
        "forecasts_sha256": hashlib.sha256(
            (output_dir / "forecast_ledger.jsonl").read_bytes()).hexdigest(),
        "outcomes_sha256": hashlib.sha256(
            (output_dir / "outcome_ledger.jsonl").read_bytes()).hexdigest(),
        "coverage": coverage,
    })
    (output_dir / "m4_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", type=Path,
                    default=Path("artifacts/market_data/yahoo_ohlcv.csv"))
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--cutoff", required=True, help="last historical daily date YYYY-MM-DD")
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--stride", type=int, default=20)
    ap.add_argument("--first-index", type=int, default=150)
    ap.add_argument("--max-snapshots", type=int, default=30)
    args = ap.parse_args()
    summary = run(args.input, args.symbols, cutoff=args.cutoff,
                  output_dir=args.output_dir, stride=args.stride,
                  first_index=args.first_index, max_snapshots=args.max_snapshots)
    print(_encode(summary))


if __name__ == "__main__":
    main()
