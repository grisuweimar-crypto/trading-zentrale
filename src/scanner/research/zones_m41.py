"""M4.1 closed-daily zone geometry and multi-session response ledger.

Research only. NO trade simulation and NO Elliott-wave count certification.
A complete adjusted-OHLC session (open/high/low/close) is used: H/L to measure
intersections and depth, CLOSE to certify exits/reclaims. At no point is an
unfinished/current session eligible. Historical Yahoo adjusts remain a proxy,
not contemporaneous archived PIT. Never interpret exploratory controls as alpha.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math

M41_VERSION = "argus-zones-m4.1-v0.1-research"
WINDOWS = (1, 3, 5, 10, 20, 40, 60)
ASOF_WINDOWS = (20, 40, 60)


@dataclass(frozen=True)
class ZoneObservationPolicy:
    horizon_bars: int = 60
    touch_windows: tuple[int, ...] = WINDOWS
    asof_windows: tuple[int, ...] = ASOF_WINDOWS
    # Four fixed deterministic offsets, applied BEFORE future bars are read.
    null_distance_multipliers: tuple[float, ...] = (0.7, 0.85, 1.15, 1.3)
    comparison: str = "frozen_matched_distance_width_timeframe_side_proxy"


def _hash(x):
    return hashlib.sha256(json.dumps(x, sort_keys=True, separators=(",", ":"),
                                     default=str, allow_nan=False).encode()).hexdigest()


def _f(x, label):
    try:
        v = float(x)
        if not math.isfinite(v) or v <= 0:
            raise ValueError
        return v
    except (TypeError, ValueError, OverflowError):
        raise ValueError("invalid_" + label) from None


def validate_closed_bars(bars, *, asof, horizon=60):
    """Incoming rows must already be FULL COMPLETED OHLC daily sessions.

    Historical future dates require an explicit cutoff upstream. This helper
    rejects duplicates, bad OHLC, ordering failures and pre-asof rows.
    """
    date.fromisoformat(asof)
    prev = asof
    out = []
    for b in bars:
        day = str(b["date"])
        date.fromisoformat(day)
        if day <= prev:
            raise ValueError("nonfuture_duplicate_or_unsorted_bar")
        prev = day
        row = {"date": day}
        for k in ("open", "high", "low", "close"):
            row[k] = _f(b[k], "closed_" + k)
        if not (row["low"] <= min(row["open"], row["close"]) <=
                max(row["open"], row["close"]) <= row["high"]):
            raise ValueError("inconsistent_closed_bar_ohlc")
        out.append(row)
        if len(out) > horizon:
            raise ValueError("outcome_exceeds_registered_horizon")
    return out


def freeze_zone(report, zone, *, scope, policy=ZoneObservationPolicy()):
    """Proof of pre-cutoff known_at plus immutable, outcome-free descriptor."""
    if scope not in ("raw_daily", "raw_weekly", "cluster"):
        raise ValueError("invalid_zone_scope")
    asof = str(report["asof"])
    date.fromisoformat(asof)
    lo, hi = _f(zone["lower"], "lower"), _f(zone["upper"], "upper")
    if lo > hi:
        raise ValueError("inverted_zone")
    if str(zone["known_at"]) > asof:
        raise ValueError("zone_known_after_asof")
    role = str(zone["role"])
    if role not in ("support", "resistance", "target", "invalidation"):
        raise ValueError("unknown_zone_role")
    price = _f(report["asof_raw_close"], "asof_raw_close")
    zid = zone.get("zone_id") if scope != "cluster" else zone.get("cluster_id")
    if not zid:
        raise ValueError("missing_zone_identity")
    fam = sorted(set(zone.get("families") or [
        d["family"] for d in zone.get("drivers", [])]))
    approach = ("from_above" if price > hi else
                "from_below" if price < lo else "inside_at_asof")
    if approach == "from_above":
        distance = price - hi
    elif approach == "from_below":
        distance = lo - price
    else:
        distance = 0.0
    record = {
        "version": M41_VERSION,
        "symbol": report["symbol"], "asof": asof,
        "data_quality": report["data_quality"], "source_snapshot_hash": report["data_hash"],
        "exchange": report.get("exchange", "unknown"),
        "currency": report.get("currency"),
        "scope": scope, "zone_id": zid, "source_zone_ids":
            zone.get("source_zone_ids", [zid]),
        "families": fam, "role": role,
        "side": zone["side"], "timeframe": zone["timeframe"],
        "known_at": zone["known_at"], "lower": lo, "upper": hi,
        "asof_close": price, "asof_position": approach,
        "asof_distance_pct": round(distance / price * 100, 6),
        "width_pct": round((hi - lo) / price * 100, 6),
        "status_at_asof": zone.get("status"),
        "policy": asdict(policy),
        "evidence_status": "not_validated",
        "data_warning": "reconstructed_adjusted_OHLC_not_authentic_market_PIT",
    }
    record["forecast_id"] = "ZF-" + _hash(record)[:20]
    return record


def matched_control(forecast, policy=ZoneObservationPolicy()):
    """Predeclare deterministic null interval, matched width and near distance.

    A CONTROL HAS NO PIVOT/FIBONACCI OR ELLIOTT ANCHOR. Relative distance
    factor (0.7/0.85/1.15/1.3) is randomized by hash, not fitted to outcomes.
    Exact width and side/timeframe/role preserved. As-of-inside, broken or
    wrong-side zones do not enter a response-rate comparison.
    """
    role, side = forecast["role"], forecast["side"]
    approach = forecast["asof_position"]
    expected = ("from_above" if role == "support" else "from_below")
    if role not in ("support", "resistance") or (
            (side, role) not in (("bull", "support"), ("bear", "resistance"))) or (
            approach != expected) or forecast["status_at_asof"] == "broken":
        return None
    delta = (forecast["asof_close"] - forecast["upper"] if role == "support"
             else forecast["lower"] - forecast["asof_close"])
    width = forecast["upper"] - forecast["lower"]
    if delta < 0:
        raise ValueError("unexpected_negative_distance")
    factors = policy.null_distance_multipliers
    if not factors or any(not 0 < x < 3 for x in factors):
        raise ValueError("invalid_null_factor")
    j = int(_hash({"forecast_id": forecast["forecast_id"], "v": M41_VERSION})[:8], 16) % len(factors)
    factor = factors[j]
    if role == "support":
        hi = forecast["asof_close"] - delta * factor
        lo = hi - width
    else:
        lo = forecast["asof_close"] + delta * factor
        hi = lo + width
    if lo <= 0:
        return None
    out = dict(forecast)
    out.update({
        "scope": "matched_control",
        "zone_id": "CTRL-" + _hash([forecast["forecast_id"], factor])[:16],
        "source_zone_ids": [],
        "families": ["matched_offset_null"],
        "lower": round(lo, 8), "upper": round(hi, 8),
        "asof_distance_pct": round(delta * factor / forecast["asof_close"] * 100, 6),
        "parent_forecast_id": forecast["forecast_id"],
        "null_distance_multiplier": factor,
        "data_warning": "deterministic_distance_matched_null_not_independent_PIT",
    })
    out["forecast_id"] = "ZF-" + _hash(out)[:20]
    return out


def _state(bar, lower, upper, role):
    """Pure bar classification. Intraday ordering is UNKNOWN."""
    intersect = bar["low"] <= upper and bar["high"] >= lower
    inside = lower <= bar["close"] <= upper
    if role == "support":
        favorable = bar["close"] > upper
        broken = bar["close"] < lower
        depth = (upper - bar["low"]) / (upper - lower) if upper > lower else float(bar["low"] <= lower)
    elif role == "resistance":
        favorable = bar["close"] < lower
        broken = bar["close"] > upper
        depth = (bar["high"] - lower) / (upper - lower) if upper > lower else float(bar["high"] >= upper)
    else:
        favorable = False
        broken = False
        depth = None
    return intersect, inside, favorable, broken, (
        round(max(0, min(1, depth)), 6) if intersect and depth is not None else None)


def evaluate_zone(forecast, future_bars, *, policy=ZoneObservationPolicy()):
    """Touch/response tracking across completed 60-day outcome window.

    Touch windows start AFTER first intersection. Same-bar favorable closes
    are recorded but do NOT certify a later rebound. A same-bar break is
    observable at close and may later reclaim. Re-entries are independent
    episodes in this one frozen study, not independent predictions.
    """
    asof = forecast["asof"]
    bars = validate_closed_bars(future_bars, asof=asof, horizon=policy.horizon_bars)
    lower, upper = forecast["lower"], forecast["upper"]
    role = forecast["role"]
    support_resistance = role in ("support", "resistance")
    close_at_asof = forecast["asof_close"]
    touches = []
    intersect_before = False
    active_inside_closes = 0
    max_inside_run = 0
    first = None
    first_index = None
    entry_approach = None
    max_depth = 0.0
    inside_days = 0
    first_favorable = None
    first_broken = None
    reclaimed = None
    first_exit = None
    wick_cross_far_boundary = False
    min_low = None
    max_high = None
    prior = close_at_asof
    contact_close = None
    series = []
    for i, b in enumerate(bars):
        contact, inside, favorable, broken, depth = _state(b, lower, upper, role)
        if first_index is not None:
            min_low = min(min_low, b["low"])
            max_high = max(max_high, b["high"])
        if contact and not intersect_before:
            touches.append(b["date"])
        if first_index is None and contact:
            first_index, first = i, b["date"]
            contact_close = b["close"]
            entry_approach = ("from_above" if prior > upper else
                              "from_below" if prior < lower else "inside_from_asof_or_prior")
            min_low, max_high = b["low"], b["high"]
        if first_index is not None:
            if contact and depth is not None:
                max_depth = max(max_depth, depth)
            if inside:
                inside_days += 1
                active_inside_closes += 1
            else:
                active_inside_closes = 0
            max_inside_run = max(max_inside_run, active_inside_closes)
            if support_resistance:
                wick_cross_far_boundary |= (
                    b["low"] < lower if role == "support" else b["high"] > upper)
                if broken and first_broken is None:
                    first_broken = b["date"]
                if (i > first_index and favorable and first_favorable is None):
                    first_favorable = b["date"]
                if i > first_index and first_exit is None and (favorable or broken):
                    first_exit = "favorable" if favorable else "broken"
                if (first_broken and i > first_index and favorable
                        and b["date"] > first_broken and reclaimed is None):
                    reclaimed = b["date"]
        # Track full close and observed extremes for time horizons only, no
        # imagined intraday sequence or future-asof signal inputs.
        series.append({"date": b["date"], "close": b["close"], "low": b["low"],
                       "high": b["high"], "intersect": contact})
        intersect_before = contact
        prior = b["close"]
    measured = {}
    for n in policy.touch_windows:
        if first_index is None:
            measured[str(n)] = {"status": "no_touch_or_censored"}
            continue
        final = first_index + n
        if final >= len(series):
            measured[str(n)] = {"status": "right_censored"}
            continue
        point = series[final]
        subset = series[first_index + 1: final + 1]
        sign = 1 if role != "resistance" else -1
        # All extrema are *after the first contact bar*; do not infer whether
        # target/stop was reached first within any following daily candle.
        excursion = (
            (max(b["high"] for b in subset) - contact_close) / contact_close * 100
            if sign == 1 else
            (contact_close - min(b["low"] for b in subset)) / contact_close * 100)
        adverse = (
            (min(b["low"] for b in subset) - contact_close) / contact_close * 100
            if sign == 1 else
            (contact_close - max(b["high"] for b in subset)) / contact_close * 100)
        measured[str(n)] = {
            "status": "observed", "end_date": point["date"],
            "signed_close_return_pct": round(sign * (point["close"] - contact_close) / contact_close * 100, 6),
            "favorable_extreme_pct": round(max(0, excursion), 6),
            "adverse_extreme_pct": round(min(0, adverse), 6),
            "favorable_close_outside": (
                point["close"] > upper if role == "support" else
                point["close"] < lower if role == "resistance" else None),
            "broken_close_outside": (
                point["close"] < lower if role == "support" else
                point["close"] > upper if role == "resistance" else None),
        }
    asof_windows = {}
    for n in policy.asof_windows:
        if len(series) < n:
            asof_windows[str(n)] = {"status": "right_censored"}
        else:
            asof_windows[str(n)] = {
                "status": "observed",
                "touched": first_index is not None and first_index < n,
                "first_touch_day_index": first_index + 1 if first_index is not None and first_index < n else None,
            }
    if first_index is None:
        result_status = "no_touch" if len(series) == policy.horizon_bars else "right_censored_before_touch"
    elif not support_resistance:
        result_status = "level_reached_no_directional_response_claim"
    elif first_broken and reclaimed:
        result_status = "break_then_reclaim"
    elif first_favorable:
        result_status = "rebound_or_rejection"
    elif first_broken:
        result_status = "broken_no_reclaim_observed"
    elif len(series) < policy.horizon_bars:
        result_status = "right_censored_after_touch"
    else:
        result_status = "contact_without_outside_reaction"
    return {
        "forecast_id": forecast["forecast_id"], "parent_forecast_id": forecast.get("parent_forecast_id"),
        "symbol": forecast["symbol"], "asof": asof, "scope": forecast["scope"],
        "zone_id": forecast["zone_id"], "role": role, "family_labels": forecast["families"],
        "future_sessions_observed": len(series),
        "first_touch_date": first, "touch_bar_index": first_index,
        "approach_to_first_touch": entry_approach,
        "first_touch_close": contact_close,
        "touch_episodes": len(touches), "retests": max(0, len(touches)-1),
        "touch_episode_starts": touches, "close_inside_days": inside_days,
        "longest_consecutive_inside_closes": max_inside_run,
        "max_penetration_depth_fraction": round(max_depth, 6) if first_index is not None and support_resistance else None,
        "max_penetration_beyond_far_edge": max_depth == 1.0 if first_index is not None and support_resistance else None,
        "low_after_first_touch": min_low, "high_after_first_touch": max_high,
        "intraday_far_edge_breached": wick_cross_far_boundary if first_index is not None and support_resistance else None,
        "first_favorable_close_after_touch": first_favorable,
        "first_broken_close": first_broken,
        "first_reclaim_close": reclaimed,
        "first_decisive_exit_after_touch": first_exit,
        "status": result_status,
        "touch_windows": measured,
        "asof_windows": asof_windows,
        "probability": None, "trade_return": None,
        "evidence_status": "historical_proxy_descriptive_only",
    }


def summarize_events(events, predictions):
    """No pooled hit-rate claims for repeated correlated pivot families."""
    from collections import Counter
    key = lambda x: x["role"] + ":" + ",".join(x["family_labels"])
    def pack(group):
        return {
            "predictions": len(group),
            "any_touch": sum(bool(e["first_touch_date"]) for e in group),
            "no_touch": sum(e["status"] == "no_touch" for e in group),
            "statuses": dict(sorted(Counter(e["status"] for e in group).items())),
            "three_day_observed": sum(e["touch_windows"]["3"]["status"] == "observed" for e in group),
            "three_day_signed_close_mean_pct": (
                round(sum(e["touch_windows"]["3"]["signed_close_return_pct"] for e in group
                          if e["touch_windows"]["3"]["status"] == "observed")
                      / sum(e["touch_windows"]["3"]["status"] == "observed" for e in group), 6)
                if any(e["touch_windows"]["3"]["status"] == "observed" for e in group)
                else None),
        }
    groups = {}
    for kind in ("raw_daily", "raw_weekly", "cluster", "matched_control"):
        g = [e for e in events if e["scope"] == kind]
        groups[kind] = pack(g)
        groups[kind]["by_family_role"] = {
            label: pack([e for e in g if key(e) == label])
            for label in sorted(set(map(key, g)))
        }
    ids = {p["forecast_id"] for p in predictions}
    if len(ids) != len(predictions):
        raise ValueError("duplicate_frozen_prediction_id")
    if len(events) != len(predictions) or set(e["forecast_id"] for e in events) != ids:
        raise ValueError("forecast_outcome_ledger_mismatch")
    comparable = [e for e in events if e["scope"] == "matched_control"]
    lookup = {e["forecast_id"]: e for e in events}
    pairs = [(lookup[c["parent_forecast_id"]], c) for c in comparable
             if c["parent_forecast_id"] in lookup]
    return {
        "version": M41_VERSION, "predictions": len(predictions),
        "outcomes": len(events), "summary_by_scope": groups,
        "matched_pairs": len(pairs),
        "both_touched_pairs": sum(bool(a["first_touch_date"] and b["first_touch_date"])
                                   for a,b in pairs),
        "real_touch_only_pairs": sum(bool(a["first_touch_date"] and not b["first_touch_date"])
                                      for a,b in pairs),
        "null_touch_only_pairs": sum(bool(not a["first_touch_date"] and b["first_touch_date"])
                                      for a,b in pairs),
        "sample_independent": False,
        "predeclared_independent_OOS_evidence": False,
        "calibrated_probability": None,
        "research_only": True,
    }
