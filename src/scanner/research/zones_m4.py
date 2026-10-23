"""ARGUS M4 v0.1 – fixed-snapshot, closed-bar event replay (research only).

An M3 hypothesis is frozen at the original as-of close. Future OHLC is read
*only* by this outcome evaluator, never by the signal generator. The archive
is a later-retrieved adjusted-OHLC proxy, NOT authentic historical PIT data.
No real orders, calibrated hit probabilities, independent trades or alpha claims.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import hashlib
import json
import math


M4_VERSION = "argus-zones-m4-v0.1-research"


@dataclass(frozen=True)
class ReplayPolicy:
    # Fixed before examining historical outcomes. Not estimated costs.
    horizon_bars: int = 20        # measured from as-of, incl. watch & holding
    max_initial_risk_pct: float = 0.12
    max_reward_risk: float = 100.0  # only guards malformed price data
    min_reward_risk: float = 1.0
    per_side_cost_bps: float = 15.0  # combined assumed spread/slippage/fee proxy
    require_next_bar_confirmation: bool = True
    fill: str = "next_completed_session_open"
    same_bar_conflict: str = "stop_first_pessimistic"
    gap_stop: str = "worse_of_open_or_stop"
    target_fill: str = "limit_to_target_at_open_gaps"
    comparison: str = "same_exit_session_close_directional_hold"


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False, default=str).encode()).hexdigest()


def _finite(value, field):
    try:
        x = float(value)
        if not math.isfinite(x) or x <= 0:
            raise ValueError
        return x
    except (ValueError, TypeError, OverflowError):
        raise ValueError("invalid_" + field) from None


def normalize_future_bars(rows, *, asof, asof_raw_close, asof_adj_close):
    """Bring future adjusted-OHLC on the frozen *asof raw* price scale.

    The factor for future sessions is based on observed adjusted_close/close;
    it is *outcome-only*, never used to generate the earlier M3 predictions.
    Provider later revisions and dividends still undermine historical PIT proof.
    """
    date.fromisoformat(asof)
    ratio = _finite(asof_raw_close, "asof_raw_close") / _finite(
        asof_adj_close, "asof_adj_close")
    normalized = []
    seen = set()
    for row in sorted(rows, key=lambda r: r["date"]):
        day = str(row["date"])
        date.fromisoformat(day)
        if day <= asof:
            continue
        if day in seen:
            raise ValueError("duplicate_outcome_bar")
        seen.add(day)
        close = _finite(row.get("close"), "future_close")
        adj = _finite(row.get("adj_close"), "future_adj_close")
        factor = adj / close * ratio
        converted = {"date": day}
        for k in ("open", "high", "low", "close"):
            converted[k] = _finite(row.get(k), "future_" + k) * factor
        if not (converted["low"] <= min(converted["open"], converted["close"]) <=
                max(converted["open"], converted["close"]) <= converted["high"]):
            raise ValueError("inconsistent_future_bar")
        normalized.append(converted)
    return normalized


def _pct(side, entry, exit_price):
    sign = 1 if side == "bull" else -1
    return sign * (exit_price - entry) / entry * 100


def _cost_adjusted(net_gross_pct, policy):
    # Single simplified round-trip price-notional bps proxy.
    return round(net_gross_pct - 2 * policy.per_side_cost_bps / 100.0, 6)


def evaluate_frozen(report, scenario, future_bars, policy=ReplayPolicy()):
    """Exactly one prospectively frozen M3 scenario produces exactly one row.

    No future state or closing price can modify entry/stop/target selection.
    A touch can be confirmed ONLY on the immediately following session close.
    A resulting research entry is the *next session open* (never same close).
    """
    if policy.horizon_bars < 3 or policy.per_side_cost_bps < 0:
        raise ValueError("invalid_replay_policy")
    asof = str(report["asof"])
    date.fromisoformat(asof)
    if scenario["asof"] != asof or scenario["known_at"] > asof:
        raise ValueError("forecast_known_after_cutoff")
    if scenario.get("state") != "untriggered":
        raise ValueError("scenario_not_frozen")
    if report.get("data_quality") not in ("degraded", "ok"):
        raise ValueError("blocked_asof_snapshot")
    side = scenario["side"]
    if side not in ("bull", "bear"):
        raise ValueError("invalid_side")
    clusters = {c["cluster_id"]: c for c in report["zone_clusters"]}
    entry_band = clusters[scenario["entry_cluster_id"]]
    inv_band = clusters[scenario["invalidation_cluster_id"]]
    first_target = clusters[scenario["target_cluster_ids"][0]]
    entry_low, entry_high = float(entry_band["lower"]), float(entry_band["upper"])
    invalid = float(inv_band["lower" if side == "bull" else "upper"])
    target = float(first_target["lower" if side == "bull" else "upper"])
    for c in (entry_band, inv_band, first_target):
        if c["known_at"] > asof:
            raise ValueError("future_zone_anchor")
    if not (entry_low > invalid and target > entry_high if side == "bull"
            else entry_high < invalid and target < entry_low):
        raise ValueError("invalid_scenario_geometry")
    ident = {"v": M4_VERSION, "scenario": scenario["scenario_id"],
             "asof": asof, "snapshot_hash": report["data_hash"],
             "policy": asdict(policy)}
    event = {
        "event_id": "AE-" + _digest(ident)[:20],
        "model_version": M4_VERSION,
        "symbol": report["symbol"], "asof": asof,
        "snapshot_hash": report["data_hash"], "scenario_id": scenario["scenario_id"],
        "side": side, "asof_quality": report["data_quality"],
        "entry_cluster_id": entry_band["cluster_id"],
        "invalidation_cluster_id": inv_band["cluster_id"],
        "target_cluster_id": first_target["cluster_id"],
        "entry_lower": entry_low, "entry_upper": entry_high,
        "stop_threshold": invalid, "target_threshold": target,
        "prediction_known_at": scenario["known_at"],
        "policy": asdict(policy), "touch_date": None, "confirm_date": None,
        "fill_date": None, "fill_price": None,
        "exit_date": None, "exit_price": None, "exit_reason": None,
        "outcome": None, "mae_pct": None, "mfe_pct": None,
        "gross_return_pct": None, "net_return_pct": None,
        "same_exit_close_hold_net_pct": None,
        "risk_pct_at_fill": None, "planned_reward_risk_at_fill": None,
        "empirical_probability": None, "evidence_status": "descriptive_replay_only",
        "warning": "posthoc_adjusted_OHLC_proxy_not_true_historical_PIT",
    }
    # If fewer than horizon sessions exist, completed terminal events are still
    # observable. Open or untriggered windows remain censored.
    bars = [b for b in future_bars if b["date"] > asof][:policy.horizon_bars]
    if not bars:
        event["outcome"] = "right_censored"
        return event
    pending_touch_at = None
    entered = False
    entry_price = 0.0
    fill_index = None
    worst, best = 0.0, 0.0
    for index, b in enumerate(bars):
        day = b["date"]
        # STOP/target are evaluated only *after* the asof-created order would
        # be filled. Confirmation at close never allows same-bar OHLC fills.
        if entered:
            adverse = _pct(side, entry_price, b["low"] if side == "bull" else b["high"])
            favorable = _pct(side, entry_price, b["high"] if side == "bull" else b["low"])
            worst, best = min(worst, adverse), max(best, favorable)
            stop_hit = b["low"] <= invalid if side == "bull" else b["high"] >= invalid
            target_hit = b["high"] >= target if side == "bull" else b["low"] <= target
            # If both touch within one OHLC bar, path unknowable: stop first.
            if stop_hit:
                px = (min(b["open"], invalid) if side == "bull"
                      else max(b["open"], invalid))
                reason = "stop_first_on_same_bar" if target_hit else "stop"
            elif target_hit:
                # No intrabar improvement claimed on gaps through targets.
                px, reason = target, "target"
            elif index + 1 == policy.horizon_bars:
                px, reason = b["close"], "horizon_close"
            else:
                continue
            event.update(outcome=reason, exit_reason=reason,
                         exit_date=day, exit_price=round(px, 8),
                         mae_pct=round(worst, 6), mfe_pct=round(best, 6),
                         gross_return_pct=round(_pct(side, entry_price, px), 6),
                         net_return_pct=_cost_adjusted(_pct(side, entry_price, px), policy),
                         same_exit_close_hold_net_pct=_cost_adjusted(
                             _pct(side, entry_price, b["close"]), policy))
            return event
        # After close-confirmation: next session open entry, not earlier.
        if event["confirm_date"] is not None:
            entry_price = b["open"]
            risk = ((entry_price - invalid) if side == "bull"
                    else (invalid - entry_price))
            reward = ((target - entry_price) if side == "bull"
                      else (entry_price - target))
            event["risk_pct_at_fill"] = round(risk / entry_price * 100, 6)
            event["planned_reward_risk_at_fill"] = (
                round(reward / risk, 6) if risk > 0 else None)
            # Fail closed; gap can invalidate geometry or risk constraint.
            if (risk <= 0 or reward <= 0 or
                    risk / entry_price > policy.max_initial_risk_pct or
                    reward / risk < policy.min_reward_risk or
                    reward / risk > policy.max_reward_risk):
                event.update(outcome="risk_rejected", fill_date=None)
                return event
            entered, fill_index = True, index
            event["fill_date"] = day
            event["fill_price"] = round(entry_price, 8)
            # The entry bar range is *after* its open: a gap already past the
            # stop is not opened because geometry gate above rejected it.
            adverse = _pct(side, entry_price, b["low"] if side == "bull" else b["high"])
            favorable = _pct(side, entry_price, b["high"] if side == "bull" else b["low"])
            worst, best = min(0.0, adverse), max(0.0, favorable)
            stop_hit = b["low"] <= invalid if side == "bull" else b["high"] >= invalid
            target_hit = b["high"] >= target if side == "bull" else b["low"] <= target
            if stop_hit or target_hit or index + 1 == policy.horizon_bars:
                if stop_hit:
                    px = min(b["open"], invalid) if side == "bull" else max(b["open"], invalid)
                    reason = "stop_first_on_same_bar" if target_hit else "stop"
                elif target_hit:
                    px, reason = target, "target"
                else:
                    px, reason = b["close"], "horizon_close"
                event.update(outcome=reason, exit_reason=reason, exit_date=day,
                             exit_price=round(px, 8), mae_pct=round(worst, 6),
                             mfe_pct=round(best, 6),
                             gross_return_pct=round(_pct(side, entry_price, px), 6),
                             net_return_pct=_cost_adjusted(_pct(side, entry_price, px), policy),
                             same_exit_close_hold_net_pct=_cost_adjusted(
                                 _pct(side, entry_price, b["close"]), policy))
                return event
            continue
        # An invalidated thesis before confirmation cannot be resurrected.
        breached = b["close"] < invalid if side == "bull" else b["close"] > invalid
        if breached:
            event["outcome"] = "invalidated_before_entry"
            event["exit_date"] = day
            return event
        if pending_touch_at is not None:
            if (b["close"] > entry_high if side == "bull" else b["close"] < entry_low):
                event["confirm_date"] = day
            else:
                event["outcome"] = "touch_not_confirmed"
            if event["outcome"] is not None:
                return event
            continue
        if b["low"] <= entry_high and b["high"] >= entry_low:
            event["touch_date"] = day
            pending_touch_at = index
    if entered or event["confirm_date"] is not None or pending_touch_at is not None:
        event["outcome"] = "right_censored" if len(bars) < policy.horizon_bars else (
            "confirmed_no_fill_within_horizon" if event["confirm_date"] is not None else
            "touch_at_horizon_no_confirmation" if pending_touch_at is not None else "right_censored")
    else:
        event["outcome"] = "unreached" if len(bars) >= policy.horizon_bars else "right_censored"
    if entered:
        event["mae_pct"] = round(worst, 6)
        event["mfe_pct"] = round(best, 6)
    return event


def summarize(events):
    """Count every forecast including rejects; NEVER infer independent trades."""
    outcomes = {}
    for event in events:
        k = event["outcome"]
        outcomes[k] = outcomes.get(k, 0) + 1
    closed = [e for e in events if e["net_return_pct"] is not None]
    return {
        "version": M4_VERSION, "total_forecasts": len(events),
        "outcome_counts": dict(sorted(outcomes.items())),
        "closed_experimental_fills": len(closed),
        "mean_net_pct_closed_only": (
            round(sum(e["net_return_pct"] for e in closed) / len(closed), 6)
            if closed else None),
        "mean_same_exit_close_hold_net_pct_closed_only": (
            round(sum(e["same_exit_close_hold_net_pct"] for e in closed) / len(closed), 6)
            if closed else None),
        "success_probability": None,
        "causal_market_PIT_verified": False,
        "independent_observations": False,
        "outcome_bias_warning": "closed_only_mean_excludes_censored_and_untriggered",
        "research_only": True,
    }
