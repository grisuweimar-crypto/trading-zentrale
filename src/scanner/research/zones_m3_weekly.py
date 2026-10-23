"""Closed-week M3 swing zones based on existing Module 6A causal OHLCV functions.

A weekly bar becomes usable only after the FIRST actual session of the next
ISO week. This remains research-only; there are no inferred Friday closes.
"""
from __future__ import annotations

from scanner.research.elliott_vnext.pivots import (
    PivotSpec, aggregate_weekly, atr_series, detect_confirmed_pivots,
)
from scanner.research.zones_v01 import (
    EXTENSIONS, RETRACEMENTS, _alternating, _invalidation, _zone, digest,
)

WEEKLY_SPEC = PivotSpec("primary", "weekly", 2, 2, 10, 1.0)
MAX_WEEKLY_ZONES_PER_FAMILY = 8


def build_weekly_zones(daily_frame, *, asof, current):
    """Generate weekly structure/levels from *confirmed* ISO-week bars only.

    Returns (zones, last_confirmed_week_info). Uses consistent as-of raw scale
    from daily_frame; adjusted-close proxy remains unverified and degraded.
    """
    weekly = aggregate_weekly(daily_frame, as_of=asof, require_confirmed=True)
    if len(weekly) < 20:
        return [], None
    last = weekly.iloc[-1]
    completed = {
        "bar_end": last["date"].date().isoformat(),
        "known_at": last["bar_confirmed_time"].date().isoformat(),
        "confirmed_week_count": len(weekly),
    }
    pivot_list = detect_confirmed_pivots(weekly, WEEKLY_SPEC, as_of=asof)
    anchors = _alternating(pivot_list)
    if not anchors:
        return [], completed
    weekly_atr = atr_series(weekly, 10).dropna()
    if weekly_atr.empty or float(weekly_atr.iloc[-1]) <= 0:
        return [], completed
    volatility = float(weekly_atr.iloc[-1])
    out = []

    def append(family, role, side, center, pivots, *, level=None, reason=""):
        z = _zone(family, role, side, center, volatility, list(pivots), current,
                  level=level, reason=reason)
        if z is not None:
            # Keep daily IDs unchanged. The weekly ID also binds its timeframe.
            z["zone_id"] = "AW-" + digest({
                "timeframe": "weekly", "zone_id": z["zone_id"],
            })[:16]
            z["timeframe"] = "weekly"
            z["drivers"][0]["timeframe"] = "weekly"
            out.append(z)

    for p in anchors[-MAX_WEEKLY_ZONES_PER_FAMILY:]:
        append("horizontal", "support" if p["kind"] == "low" else "resistance",
               "bull" if p["kind"] == "low" else "bear", float(p["price"]),
               [p], reason="confirmed primary weekly swing")
    for a, b in list(zip(anchors, anchors[1:]))[-2:]:
        up = (a["kind"], b["kind"]) == ("low", "high")
        down = (a["kind"], b["kind"]) == ("high", "low")
        if not (up or down):
            continue
        side, sign = ("bull", 1) if up else ("bear", -1)
        span = abs(float(b["price"]) - float(a["price"]))
        if span < 0.5 * volatility:
            continue
        for level in RETRACEMENTS:
            append("fib_retracement", "support" if up else "resistance", side,
                   float(b["price"]) - sign * level * span, [a, b], level=level,
                   reason="confirmed weekly A-B swing; not verified Elliott")
        append("swing_origin", "invalidation", side, float(a["price"]), [a],
               reason="weekly origin invalidation candidate, not a stop order")
    for a, b, c in list(zip(anchors, anchors[1:], anchors[2:]))[-2:]:
        up = (a["kind"], b["kind"], c["kind"]) == ("low", "high", "low")
        down = (a["kind"], b["kind"], c["kind"]) == ("high", "low", "high")
        if not (up or down):
            continue
        side, sign = ("bull", 1) if up else ("bear", -1)
        span = abs(float(b["price"]) - float(a["price"]))
        if span < 0.5 * volatility:
            continue
        for level in EXTENSIONS:
            append("swing_abc_projection", "target", side,
                   float(c["price"]) + sign * level * span, [a, b, c],
                   level=level, reason="confirmed weekly ABC projection; no Elliott certification")
        append("swing_origin", "invalidation", side, float(a["price"]), [a],
               reason="weekly ABC origin invalidation candidate, not a stop order")
    unique = {z["zone_id"]: z for z in out}
    return sorted(unique.values(), key=lambda z: (z["lower"], z["role"], z["zone_id"])), completed
