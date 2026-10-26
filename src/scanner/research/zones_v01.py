"""ARGUS research-only zone candidates v0.1 (M1/M2).

Reuse Module 6A causal OHLCV/pivots; no scanner decision changes, forecasts or orders.
The adjusted-to-asof raw price scale is a research proxy, not a corporate-action
ledger or an authentic provider snapshot recorded at the historical asof date.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date
import hashlib
import json
import math
from pathlib import Path

import pandas as pd

from scanner.research.elliott_vnext.pivots import (
    PivotSpec, atr_series, detect_confirmed_pivots, prepare_daily_ohlcv,
)

VERSION = "argus-zones-v0.1.0-research"
# Fixed exploratory conventions, deliberately not tuned to future outcomes.
SPECS = (
    PivotSpec("minor", "daily", 2, 2, 10, 0.75),
    PivotSpec("intermediate", "daily", 3, 3, 14, 1.0),
    PivotSpec("major", "daily", 5, 5, 20, 1.5),
)
RETRACEMENTS = (0.382, 0.500, 0.618, 0.786)
EXTENSIONS = (1.000, 1.272, 1.618)
MAX_BARS = 450
MIN_BARS = 80


def digest(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def read_rows(path: Path, symbols: set[str]) -> dict[str, list[dict]]:
    """CSV streaming adapter for the canonical Yahoo OHLCV/research price view."""
    out = {s: [] for s in symbols}
    with path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        required = {"date", "symbol", "currency", "open", "high", "low", "close", "adj_close", "volume"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"missing_market_columns:{sorted(required - set(reader.fieldnames or []))}")
        for row in reader:
            sym = row["symbol"].strip()
            if sym in out and (not row.get("observation_type") or row["observation_type"] == "price_backfill"):
                out[sym].append(row)
    return out


def chronological(rows: list[dict], asof: str) -> list[dict]:
    date.fromisoformat(asof)
    selected = sorted((r for r in rows if r.get("date", "") <= asof), key=lambda r: r["date"])
    if len({r["date"] for r in selected}) != len(selected):
        raise ValueError("duplicate_market_sessions")
    return selected[-MAX_BARS:]


def _alternating(pivots: list[dict]) -> list[dict]:
    """Causal anchor selection from confirmed pivots. Old *snapshots* never change."""
    selected: list[dict] = []
    for p in sorted(pivots, key=lambda x: (x["confirmed_time"], x["pivot_time"], x["kind"])):
        if p.get("sequence_ambiguous"):
            continue
        if selected and selected[-1]["kind"] == p["kind"]:
            extreme = p["price"] > selected[-1]["price"] if p["kind"] == "high" else p["price"] < selected[-1]["price"]
            if extreme:
                selected[-1] = p
        else:
            selected.append(p)
    return selected


def _ref(p):
    return {
        "degree": p["degree"], "kind": p["kind"], "pivot_time": p["pivot_time"],
        "known_at": p["confirmed_time"], "price": round(float(p["price"]), 6),
    }


def _zone(family: str, role: str, side: str, center: float, atr: float,
          anchors: list[dict], current, *, level=None, reason="") -> dict | None:
    if not math.isfinite(center) or center <= 0:
        return None
    # Full width <= 6% of level, preventing arbitrary widening to manufacture hits.
    half = min(max(0.35 * atr, 0.003 * center), 0.03 * center)
    lower, upper = round(center - half, 6), round(center + half, 6)
    known_at = max(a["confirmed_time"] for a in anchors)
    last_date = current["date"]
    last_close, last_low, last_high = (float(current[k]) for k in ("close", "low", "high"))
    status = "watch"
    if known_at < last_date:
        if role == "support" and last_close < lower:
            status = "broken"
        elif role == "resistance" and last_close > upper:
            status = "broken"
        elif last_low <= upper and last_high >= lower:
            status = "touched"
    refs = [_ref(p) for p in anchors]
    identifying = {"version": VERSION, "family": family, "role": role, "side": side,
                   "center": round(center, 8), "level": level, "anchors": refs}
    return {
        "zone_id": "AZ-" + digest(identifying)[:16], "role": role, "side": side,
        "lower": lower, "upper": upper, "center": round(center, 6),
        "known_at": known_at, "status": status, "timeframe": "daily",
        "drivers": [{"family": family, "level": level, "reason": reason, "anchors": refs}],
        "technical_strength": None, "empirical_probability": None,
        "evidence_status": "not_validated",
    }


def _invalidation(side: str, origin: dict, atr: float, last: dict):
    return _zone("swing_origin", "invalidation", side, float(origin["price"]), atr, [origin], last,
                 reason="candidate scenario fails beyond origin; not an order stop")


def build_zones(rows: list[dict], symbol: str, *, asof: str, exchange: str = "unknown",
                asset_id: str | None = None, source_sha: str | None = None) -> dict:
    """Derive horizontal, Fib retracement and 3-anchor projection bands."""
    selected = chronological(rows, asof)
    result = {
        "symbol": symbol, "asset_id": asset_id or symbol, "exchange": exchange,
        "currency": None, "asof": asof, "last_completed_bar": None,
        "model_version": VERSION, "model_parameters": {
            "bars": MAX_BARS, "minimum_bars": MIN_BARS,
            "degrees": [{k: getattr(s, k) for k in ("degree", "left_bars", "right_bars", "atr_window", "min_excursion_atr")} for s in SPECS],
            "retracements": RETRACEMENTS, "extensions": EXTENSIONS,
            "half_width": "min(max(0.35*ATR14,0.003*center),0.03*center)",
        },
        "source_blob_sha": source_sha, "data_hash": digest(selected),
        "data_quality": "blocked", "zones": [], "scenarios": [], "warnings": [],
        "zone_status": "no_qualifying_zone",
        "m3_version": "argus-zones-m3-v0.1-research", "zone_clusters": [], "scenario_plans": [],
        "m3_status": "no_valid_setup",
        "weekly_zones": [], "last_confirmed_week": None,
        "cross_timeframe_overlaps": [],
    }
    if len(selected) < MIN_BARS:
        result["warnings"].append(f"insufficient_completed_daily_bars:{len(selected)}")
        return result
    currencies = {str(r["currency"]).strip() for r in selected}
    if len(currencies) != 1 or not next(iter(currencies)):
        result["warnings"].append("missing_or_mixed_currency")
        return result
    result["currency"] = next(iter(currencies))
    if exchange == "unknown" or not asset_id:
        result["warnings"].append("exchange_or_asset_identity_not_independently_verified")
    if any(not r.get("adj_close") or not math.isfinite(float(r["adj_close"])) or float(r["adj_close"]) <= 0 for r in selected):
        result["warnings"].append("adjusted_ohlc_proxy_unavailable_or_incomplete")
        return result
    try:
        frame = prepare_daily_ohlcv(selected, symbol, as_of=asof, price_basis="adjusted")
        # Rescale the whole historical series into raw units *as of* the last
        # available session; avoids mixing adjusted close with raw high/low.
        ratio = float(selected[-1]["close"]) / float(frame.iloc[-1]["close"])
        if not math.isfinite(ratio) or ratio <= 0:
            raise ValueError("invalid_raw_to_adjusted_scale")
        for column in ("open", "high", "low", "close"):
            frame[column] *= ratio
        frame["price_basis"] = "adjusted_ohlc_ratio_rescaled_to_last_raw_proxy"
        atr_values = atr_series(frame, 14).dropna()
        if atr_values.empty or float(atr_values.iloc[-1]) <= 0:
            raise ValueError("atr14_unavailable")
        atr = float(atr_values.iloc[-1])
        current = {"date": frame.iloc[-1]["date"].date().isoformat(),
                   **{c: float(frame.iloc[-1][c]) for c in ("open", "high", "low", "close")}}
        result["last_completed_bar"] = current["date"]
        result["price_basis"] = "adjusted_ohlc_ratio_rescaled_to_last_raw_proxy"
        result["input_sessions"] = len(frame)
        result["asof_raw_close"] = round(current["close"], 6)
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        result["warnings"].append(f"input_quality_failed:{exc}")
        return result

    # Original provider rows are not an archived true historical point-in-time feed.
    result["data_quality"] = "degraded"
    result["warnings"].append("reconstructed_adjusted_ohlc_not_verified_corporate_action_ledger")
    result["warnings"].append("asof_simulation_not_historical_provider_snapshot")
    last = current
    zones, scenario_refs = [], []
    for spec in SPECS:
        pivots = detect_confirmed_pivots(frame, spec, as_of=asof)
        anchors = _alternating(pivots)
        # Z1: confirmed horizontal pivot bands; no invented independent touches.
        for p in anchors[-8:]:
            z = _zone("horizontal", "support" if p["kind"] == "low" else "resistance",
                      "bull" if p["kind"] == "low" else "bear",
                      float(p["price"]), atr, [p], last, reason="confirmed historical swing")
            if z:
                zones.append(z)
        # Z2: from last two *alternating*, confirmed swing legs per degree.
        for a, b in list(zip(anchors, anchors[1:]))[-2:]:
            sign = 1 if (a["kind"], b["kind"]) == ("low", "high") else -1 if (a["kind"], b["kind"]) == ("high", "low") else 0
            if not sign:
                continue
            size = abs(float(b["price"]) - float(a["price"]))
            if size < 0.5 * atr:  # reject microscopic swings
                continue
            side = "bull" if sign > 0 else "bear"
            role = "support" if sign > 0 else "resistance"
            for f in RETRACEMENTS:
                center = float(b["price"]) - sign * f * size
                z = _zone("fib_retracement", role, side, center, atr, [a, b], last,
                          level=f, reason="confirmed A-B swing, not a confirmed Elliott wave")
                if z:
                    zones.append(z)
            inv = _invalidation(side, a, atr, last)
            if inv:
                zones.append(inv)
        # Z3: last confirmed alternating A-B-C triple, origin/extension distinct.
        for a, b, c in list(zip(anchors, anchors[1:], anchors[2:]))[-2:]:
            up = (a["kind"], b["kind"], c["kind"]) == ("low", "high", "low")
            down = (a["kind"], b["kind"], c["kind"]) == ("high", "low", "high")
            if not (up or down):
                continue
            size = abs(float(b["price"]) - float(a["price"]))
            if size < 0.5 * atr:
                continue
            side = "bull" if up else "bear"
            sign = 1 if up else -1
            target_ids = []
            for f in EXTENSIONS:
                center = float(c["price"]) + sign * f * size
                z = _zone("swing_abc_projection", "target", side, center, atr, [a, b, c], last,
                          level=f, reason="three confirmed swing anchors, not certified Elliott count")
                if z:
                    zones.append(z)
                    target_ids.append(z["zone_id"])
            inv = _invalidation(side, a, atr, last)
            if inv:
                zones.append(inv)
            scenario_refs.append({
                "scenario_id": "AS-" + digest([_ref(a), _ref(b), _ref(c), side, VERSION])[:16],
                "side": side, "known_at": c["confirmed_time"],
                "target_zone_ids": target_ids,
                "invalidation_zone_ids": [inv["zone_id"]] if inv else [],
                "entry_zone_ids": [],
                "activation_rule": "untriggered: explicit touch/confirmation/retest rules await M3",
                "horizon_bars": None, "risk_reward_after_costs": None,
                "evidence_status": "not_validated",
            })
    from scanner.research.zones_m3_weekly import build_weekly_zones
    result["weekly_zones"], result["last_confirmed_week"] = build_weekly_zones(
        frame, asof=asof, current=last)
    unique = {z["zone_id"]: z for z in zones}
    result["zones"] = sorted(unique.values(), key=lambda z: (z["lower"], z["role"], z["zone_id"]))
    result["scenarios"] = scenario_refs
    result["zone_status"] = "candidate_zones_unvalidated" if unique else "no_qualifying_zone"
    from scanner.research.zones_m3 import enrich_m3
    return enrich_m3(result)


def main() -> None:
    parser = argparse.ArgumentParser(description="ARGUS independent causal OHLCV zone research prototype")
    parser.add_argument("--input", type=Path, default=Path("artifacts/market_data/yahoo_ohlcv.csv"))
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--asof", required=True, help="completed daily session cutoff YYYY-MM-DD")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--exchange", default="unknown")
    parser.add_argument("--source-git-blob", default=None)
    args = parser.parse_args()
    data = read_rows(args.input, set(args.symbols))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for symbol in args.symbols:
        report = build_zones(data[symbol], symbol, asof=args.asof, exchange=args.exchange,
                             source_sha=args.source_git_blob)
        path = args.output_dir / f"{symbol.replace('/', '_')}_{args.asof}_zones.json"
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        from scanner.research.zones_m3 import write_readable_tables
        write_readable_tables(report, path)
        print(json.dumps({"symbol": symbol, "source_bars": len(data[symbol]),
                          "zones": len(report["zones"]), "status": report["zone_status"],
                          "weekly_zones": len(report["weekly_zones"]),
                          "clusters": len(report["zone_clusters"]),
                          "plans": len(report["scenario_plans"]),
                          "data_quality": report["data_quality"], "output": str(path)}))


if __name__ == "__main__":
    main()
