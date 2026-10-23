"""ARGUS research-only autonomous M2 price zones.

Z1 horizontal, Z2 Fibonacci retracement, Z3 ABC swing projection.
Uses only causal, already confirmed pivots. No Elliott count, no trade signals.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
import math
from typing import Iterable, Mapping

from scanner.data.price_history import validated_rows
from scanner.research.elliott_vnext.pivots import (
    PivotSpec, atr_series, detect_confirmed_pivots, prepare_daily_ohlcv,
)

VERSION = "argus-zones-m2-v0.1"
SPECS = (
    PivotSpec("minor", "daily", 2, 2, 10, 0.75),
    PivotSpec("intermediate", "daily", 3, 3, 14, 1.0),
    PivotSpec("major", "daily", 5, 5, 20, 1.5),
)
RETRACE = (0.382, 0.500, 0.618, 0.786)
EXTEND = (1.000, 1.272, 1.618)


class ZoneInputError(ValueError):
    pass


def frozen(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(obj):
    return sha256(frozen(obj).encode("utf-8")).hexdigest()


def instant(value, label):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.utcoffset() is None:
            raise ValueError("offset")
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError) as exc:
        raise ZoneInputError(label + "_timezone_required") from exc


def iso_day(value, label):
    try:
        return date.fromisoformat(str(value))
    except (ValueError, TypeError) as exc:
        raise ZoneInputError(label + "_iso_date_required") from exc


def anchor(p):
    return {
        "kind": p["kind"], "pivot_time": p["pivot_time"],
        "confirmed_time": p["confirmed_time"], "price": float(p["price"]),
    }


def make_zone(role, side, price, atr, family, degree, anchors,
              publication, current, candle_low, candle_high):
    if not math.isfinite(price) or price <= 0:
        return None
    half = max(atr * 0.30, price * 0.0015)
    if price <= half:
        return None
    lower, upper = round(price - half, 10), round(price + half, 10)
    if role == "support" and current < lower:
        status = "broken"
    elif role == "resistance" and current > upper:
        status = "broken"
    elif candle_low <= upper and candle_high >= lower:
        status = "touched"
    else:
        status = "watch"
    if role in {"invalidation", "target"}:
        status = "touched" if candle_low <= upper and candle_high >= lower else "watch"
    key = {"role": role, "side": side, "family": family,
           "degree": degree, "anchors": anchors, "price": round(price, 10)}
    return {
        "zone_id": digest(key)[:24], "role": role, "side": side,
        "lower": lower, "upper": upper, "center": round(price, 10),
        "status": status, "known_at": publication, "created_at": publication,
        "degree": degree,
        "drivers": [{"family": family, "evidence_ref": digest(anchors)[:20],
                     "anchors": anchors}],
        "technical_strength": None, "empirical_probability": None,
        "evidence_status": "not_validated", "model_version": VERSION,
    }


def latest_pair(pivots, first, second):
    for i in range(len(pivots) - 1, 0, -1):
        b = pivots[i]
        if b["kind"] != second:
            continue
        for a in reversed(pivots[:i]):
            if a["kind"] == first and a["pivot_time"] < b["pivot_time"]:
                return a, b
    return None


def latest_abc(pivots, first, second):
    for ci in range(len(pivots) - 1, 1, -1):
        c = pivots[ci]
        if c["kind"] != first:
            continue
        for bi in range(ci - 1, 0, -1):
            b = pivots[bi]
            if b["kind"] != second or b["pivot_time"] >= c["pivot_time"]:
                continue
            for a in reversed(pivots[:bi]):
                if a["kind"] != first or a["pivot_time"] >= b["pivot_time"]:
                    continue
                if first == "low" and float(a["price"]) < float(c["price"]) < float(b["price"]):
                    return a, b, c
                if first == "high" and float(a["price"]) > float(c["price"]) > float(b["price"]):
                    return a, b, c
    return None


def build_zones(rows: Iterable[Mapping[str, object]], *, symbol: str,
                exchange: str, asset_id: str, currency: str, asof: str,
                bar_end_utc: str, published_at: str,
                price_basis: str = "auto", minimum_bars: int = 80):
    """Compute price geometry from archived daily OHLCV only, as-of bounded.

    Time/currency/exchange identity must be independently verified by caller.
    Publication is actual model run time, never backdated to candle date.
    """
    if not all(isinstance(x, str) and x.strip()
               for x in (symbol, exchange, asset_id, currency)):
        raise ZoneInputError("asset_identity_missing")
    cutoff = iso_day(asof, "asof")
    bar_end = instant(bar_end_utc, "bar_end")
    pub = instant(published_at, "publication")
    if pub < bar_end or pub > datetime.now(timezone.utc) or bar_end.date() < cutoff:
        raise ZoneInputError("bar_completion_or_publication_gate")
    if minimum_bars < 30:
        raise ZoneInputError("minimum_bars_below_30")
    selected = []
    for input_row in rows:
        row = dict(input_row)
        if str(row.get("symbol", "")).strip() != symbol:
            continue
        if iso_day(row.get("date", ""), "row_date") <= cutoff:
            selected.append(row)
    if not selected:
        raise ZoneInputError("history_missing")
    selected.sort(key=lambda x: (str(x["date"]), str(x.get("retrieved_at", ""))))
    clean, errors = validated_rows(selected)
    if errors or len(clean) != len(selected):
        raise ZoneInputError("history_invalid_or_duplicate")
    clean.sort(key=lambda x: x["date"])
    if {x.get("currency") for x in clean} != {currency}:
        raise ZoneInputError("history_currency_mismatch")
    frame = prepare_daily_ohlcv(clean, symbol, as_of=asof, price_basis=price_basis)
    if len(frame) < minimum_bars:
        raise ZoneInputError("insufficient_daily_history")
    last = frame.iloc[-1]
    if last["date"].date().isoformat() != asof:
        raise ZoneInputError("asof_daily_candle_missing")
    atr = float(atr_series(frame, 14).iloc[-1])
    if not math.isfinite(atr) or atr <= 0:
        raise ZoneInputError("ATR_missing")
    basis = str(last["price_basis"])
    if basis not in {"raw_unadjusted", "adjusted_via_adj_close_ratio"}:
        raise ZoneInputError("unsupported_price_basis")
    close, low, high = float(last["close"]), float(last["low"]), float(last["high"])
    price_fields = ("date", "symbol", "currency", "open", "high", "low",
                    "close", "adj_close", "volume")
    data_hash = digest([{k: r.get(k, "") for k in price_fields} for r in clean])

    pivots = []
    for spec in SPECS:
        pivots.extend(detect_confirmed_pivots(frame, spec, as_of=asof))
    zones, keys = [], set()
    counts = {"horizontal": 0, "fibonacci_retracement": 0,
              "swing_abc_projection": 0, "swing_origin_invalidation": 0}

    def add(zone, family):
        if zone and zone["zone_id"] not in keys:
            keys.add(zone["zone_id"])
            zones.append(zone)
            counts[family] += 1

    # Horizontal Z1: latest causal swing high/low for each degree and side.
    # Nearby anchors are deduplicated within degree, without fake confidence.
    min_date = frame.iloc[max(0, len(frame) - 160)]["date"].date().isoformat()
    for degree in ("minor", "intermediate", "major"):
        pool = [p for p in pivots if p["degree"] == degree and p["pivot_time"] >= min_date]
        pool.sort(key=lambda p: (p["confirmed_time"], p["pivot_time"]), reverse=True)
        for kind, role, side in (("low", "support", "bull"),
                                 ("high", "resistance", "bear")):
            accepted = []
            for p in pool:
                if p["kind"] != kind:
                    continue
                price = float(p["price"])
                if any(abs(price - x) <= 2 * max(atr * 0.3, price * .0015)
                       for x in accepted):
                    continue
                accepted.append(price)
                add(make_zone(role, side, price, atr, "horizontal", degree,
                              {"pivot": anchor(p)}, published_at, close, low, high),
                    "horizontal")
                if len(accepted) == 4:
                    break

    # Fibonacci Z2: confirmed A->B swing, never an inferred Elliott count.
    # Z3: A->B->C correction; optional if structure is not causally observed.
    for degree in ("minor", "intermediate", "major"):
        chain = sorted((p for p in pivots if p["degree"] == degree),
                       key=lambda p: (p["confirmed_time"], p["pivot_time"], p["kind"]))
        for first, second, side, role, sign in (
            ("low", "high", "bull", "support", 1),
            ("high", "low", "bear", "resistance", -1),
        ):
            pair = latest_pair(chain, first, second)
            if pair:
                a, b = pair
                length = abs(float(b["price"]) - float(a["price"]))
                for ratio in RETRACE:
                    point = float(b["price"]) - sign * length * ratio
                    add(make_zone(role, side, point, atr, "fibonacci_retracement",
                                  degree, {"A": anchor(a), "B": anchor(b),
                                           "ratio": ratio}, published_at,
                                  close, low, high), "fibonacci_retracement")
                add(make_zone("invalidation", side, float(a["price"]), atr,
                              "swing_origin_invalidation", degree,
                              {"A": anchor(a), "B": anchor(b)},
                              published_at, close, low, high),
                    "swing_origin_invalidation")
            tri = latest_abc(chain, first, second)
            if tri:
                a, b, c = tri
                length = abs(float(b["price"]) - float(a["price"]))
                for ratio in EXTEND:
                    point = float(c["price"]) + sign * length * ratio
                    add(make_zone("target", side, point, atr, "swing_abc_projection",
                                  degree, {"A": anchor(a), "B": anchor(b),
                                           "C": anchor(c), "ratio": ratio},
                                  published_at, close, low, high),
                        "swing_abc_projection")

    zones.sort(key=lambda z: (z["role"], z["side"], z["lower"], z["zone_id"]))
    return {
        "schema_version": "argus_zones_v0.1", "origin": "argus_zone_engine",
        "symbol": symbol, "exchange": exchange, "asset_id": asset_id,
        "currency": currency, "timeframe": "daily", "asof": asof,
        "last_completed_bar": asof, "bar_end_utc": bar_end.isoformat(),
        "published_at": pub.isoformat(), "price_basis": basis,
        "data_hash": data_hash,
        "data_quality": "degraded" if basis == "raw_unadjusted" else "pass",
        "model_version": VERSION, "close": close, "atr14": atr,
        "zones": zones, "scenarios": [],
        "no_qualifying_zone": not bool(zones),
        "diagnostics": {"input_bars": len(clean), "confirmed_pivots": len(pivots),
                        "family_count": counts, "no_lookahead_asof_filter": True},
        "warnings": [
            "technical_research_only_not_validated",
            "historical_adjustment_point_in_time_and_corporate_actions_not_audited",
            "M3_confluence_and_scenarios_not_available",
        ],
        "data_source": {
            "name": "repo_archive_yahoo_ohlcv_csv",
            "reference": "artifacts/market_data/yahoo_ohlcv.csv",
            "raw_rows_asof_sha256": data_hash,
        },
    }
