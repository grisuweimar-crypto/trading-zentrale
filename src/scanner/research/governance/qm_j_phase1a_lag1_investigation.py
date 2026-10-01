"""Post-trigger investigation for QM-H-QMJ-PHASE1A-LAG1-001.

Descriptive only. This module does not change the frozen QM-J decision rule,
Selection logic, scanner weights, evidence state, or promotion status. It
reconstructs the exact Lag-1 matched grid and measures temporal persistence,
observation spacing, daily predictive deltas and subperiod concentration.

No diagnostic threshold is introduced and no root cause is assigned by code.
"""
from __future__ import annotations

from hashlib import sha256
import math
from pathlib import Path
from typing import Any

import pandas as pd

from scanner.reports.selection_timing import Phase1AConfig, _score_percentile, build_events
from scanner.research.governance.qm_j_negative_controls import NegativeControlError, content_hash

SCHEMA_VERSION = "qm_j_phase1a_lag1_investigation_v1"
FINDING_ID = "QM-H-QMJ-PHASE1A-LAG1-001"


def _finite_or_none(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _quantiles(series: pd.Series) -> dict[str, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"min": None, "p10": None, "p25": None, "median": None, "p75": None, "p90": None, "max": None, "mean": None}
    return {
        "min": float(values.min()),
        "p10": float(values.quantile(0.10)),
        "p25": float(values.quantile(0.25)),
        "median": float(values.median()),
        "p75": float(values.quantile(0.75)),
        "p90": float(values.quantile(0.90)),
        "max": float(values.max()),
        "mean": float(values.mean()),
    }


def _pearson(left: pd.Series, right: pd.Series) -> float | None:
    frame = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(frame) < 3 or frame["left"].nunique() < 2 or frame["right"].nunique() < 2:
        return None
    return _finite_or_none(frame["left"].corr(frame["right"], method="pearson"))


def _spearman(left: pd.Series, right: pd.Series) -> float | None:
    """Spearman rho as Pearson correlation of average ranks; no scipy dependency."""
    frame = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(frame) < 3 or frame["left"].nunique() < 2 or frame["right"].nunique() < 2:
        return None
    left_rank = frame["left"].rank(method="average")
    right_rank = frame["right"].rank(method="average")
    return _pearson(left_rank, right_rank)


def _reconstruct_matched_grid(events: pd.DataFrame, *, min_cross_section: int, horizon: int) -> pd.DataFrame:
    ret = f"return_{horizon}t"
    needed = ["obs_date", "symbol", "score", ret]
    if any(column not in events.columns for column in needed):
        raise NegativeControlError("qm_j_lag1_investigation_event_schema_invalid")
    frame = events[needed].copy()
    frame["obs_date"] = pd.to_datetime(frame["obs_date"], errors="raise")
    frame["score"] = pd.to_numeric(frame["score"], errors="coerce")
    frame[ret] = pd.to_numeric(frame[ret], errors="coerce")
    frame = frame.dropna(subset=["obs_date", "symbol", "score"])
    frame["symbol"] = frame["symbol"].astype(str)
    frame = frame.sort_values(["symbol", "obs_date"], kind="mergesort").reset_index(drop=True)
    frame["lag_obs_date"] = frame.groupby("symbol", sort=False)["obs_date"].shift(1)
    frame["lag_score"] = frame.groupby("symbol", sort=False)["score"].shift(1)
    frame["observation_gap_calendar_days"] = (frame["obs_date"] - frame["lag_obs_date"]).dt.days
    matched = frame.dropna(subset=["score", "lag_score", ret]).copy()
    counts = matched.groupby("obs_date")[ret].transform("count")
    matched = matched.loc[counts >= min_cross_section].copy()
    if matched.empty:
        raise NegativeControlError("qm_j_lag1_investigation_no_matched_sample")
    matched["current_score_pct"] = matched.groupby("obs_date", group_keys=False)["score"].apply(_score_percentile)
    matched["lag_score_pct"] = matched.groupby("obs_date", group_keys=False)["lag_score"].apply(_score_percentile)
    matched["score_change"] = matched["score"] - matched["lag_score"]
    matched["abs_score_change"] = matched["score_change"].abs()
    matched["rank_change"] = matched["current_score_pct"] - matched["lag_score_pct"]
    matched = matched.dropna(subset=["current_score_pct", "lag_score_pct", ret]).copy()
    return matched.sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)


def _daily_diagnostics(matched: pd.DataFrame, horizon: int) -> pd.DataFrame:
    ret = f"return_{horizon}t"
    rows: list[dict[str, Any]] = []
    for obs_date, group in matched.groupby("obs_date", sort=True):
        current = _spearman(group["current_score_pct"], group[ret])
        lag = _spearman(group["lag_score_pct"], group[ret])
        rows.append({
            "obs_date": pd.Timestamp(obs_date),
            "n": int(len(group)),
            "current_return_spearman": current,
            "lag_return_spearman": lag,
            "current_minus_lag_return_spearman": (current - lag) if current is not None and lag is not None else None,
            "current_lag_score_spearman": _spearman(group["score"], group["lag_score"]),
            "current_lag_score_pearson": _pearson(group["score"], group["lag_score"]),
            "score_change_return_spearman": _spearman(group["score_change"], group[ret]),
            "rank_change_return_spearman": _spearman(group["rank_change"], group[ret]),
            "mean_abs_score_change": float(group["abs_score_change"].mean()),
            "median_abs_score_change": float(group["abs_score_change"].median()),
            "exact_same_score_rate": float((group["score_change"] == 0).mean()),
        })
    return pd.DataFrame(rows)


def _monthly_summary(daily: pd.DataFrame) -> list[dict[str, Any]]:
    frame = daily.copy()
    frame["month"] = frame["obs_date"].dt.to_period("M").astype(str)
    rows: list[dict[str, Any]] = []
    for month, group in frame.groupby("month", sort=True):
        rows.append({
            "month": str(month),
            "days": int(len(group)),
            "current_mean_daily_spearman": _finite_or_none(group["current_return_spearman"].mean()),
            "lag_mean_daily_spearman": _finite_or_none(group["lag_return_spearman"].mean()),
            "current_minus_lag_mean": _finite_or_none(group["current_minus_lag_return_spearman"].mean()),
            "score_change_mean_daily_spearman": _finite_or_none(group["score_change_return_spearman"].mean()),
            "mean_daily_current_lag_score_spearman": _finite_or_none(group["current_lag_score_spearman"].mean()),
        })
    return rows


def run_investigation(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    stable_start: str = "2026-04-15",
    min_cross_section: int = 20,
    horizon: int = 20,
    history_source_hash: str | None = None,
    price_source_hash: str | None = None,
) -> dict[str, Any]:
    history_before = history.copy(deep=True)
    prices_before = prices.copy(deep=True)
    events = build_events(history, prices, Phase1AConfig(stable_start=stable_start, min_cross_section=min_cross_section))
    matched = _reconstruct_matched_grid(events, min_cross_section=min_cross_section, horizon=horizon)
    daily = _daily_diagnostics(matched, horizon)
    if not history.equals(history_before) or not prices.equals(prices_before):
        raise NegativeControlError("qm_j_lag1_investigation_source_frames_mutated")

    delta = daily["current_minus_lag_return_spearman"].dropna()
    current = daily["current_return_spearman"].dropna()
    lag = daily["lag_return_spearman"].dropna()
    score_change_daily = daily["score_change_return_spearman"].dropna()
    rank_change_daily = daily["rank_change_return_spearman"].dropna()

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "module": "QM-J",
        "business_area": "BA-QM7",
        "finding_id": FINDING_ID,
        "investigation_type": "POST_TRIGGER_DESCRIPTIVE_ROOT_CAUSE_INVESTIGATION",
        "research_only": True,
        "root_cause_assigned_by_code": False,
        "diagnostic_thresholds_introduced": False,
        "promotion_performed": False,
        "production_logic_changed": False,
        "source": {
            "history_sha256": history_source_hash,
            "price_sha256": price_source_hash,
            "stable_start": stable_start,
            "horizon_sessions": horizon,
            "min_cross_section": min_cross_section,
        },
        "coverage": {
            "phase1a_events": int(len(events)),
            "matched_observations": int(len(matched)),
            "matched_days": int(matched["obs_date"].nunique()),
            "matched_symbols": int(matched["symbol"].nunique()),
            "event_date_min": str(matched["obs_date"].min().date()),
            "event_date_max": str(matched["obs_date"].max().date()),
        },
        "score_persistence": {
            "global_current_lag_spearman": _spearman(matched["score"], matched["lag_score"]),
            "global_current_lag_pearson": _pearson(matched["score"], matched["lag_score"]),
            "daily_current_lag_spearman": _quantiles(daily["current_lag_score_spearman"]),
            "daily_current_lag_pearson": _quantiles(daily["current_lag_score_pearson"]),
            "exact_same_score_rate": float((matched["score_change"] == 0).mean()),
            "absolute_score_change": _quantiles(matched["abs_score_change"]),
        },
        "observation_spacing_calendar_days": _quantiles(matched["observation_gap_calendar_days"]),
        "predictive_comparison": {
            "current_mean_daily_spearman": _finite_or_none(current.mean()),
            "lag_mean_daily_spearman": _finite_or_none(lag.mean()),
            "daily_current_minus_lag": _quantiles(delta),
            "fraction_days_current_stronger_than_lag": float((delta > 0).mean()) if len(delta) else None,
            "fraction_days_equal": float((delta == 0).mean()) if len(delta) else None,
            "score_change_mean_daily_spearman": _finite_or_none(score_change_daily.mean()),
            "rank_change_mean_daily_spearman": _finite_or_none(rank_change_daily.mean()),
            "daily_score_change_return_spearman": _quantiles(score_change_daily),
            "daily_rank_change_return_spearman": _quantiles(rank_change_daily),
        },
        "subperiods": _monthly_summary(daily),
        "interpretation_guard": {
            "descriptive_only": True,
            "post_trigger": True,
            "may_not_be_used_as_confirmation": True,
            "may_not_select_new_thresholds": True,
            "root_cause_requires_separate_governance_transition": True,
            "finding_remains_promotion_blocked_until_evidence_disposition": True,
        },
    }
    body = dict(result)
    result["result_hash"] = content_hash(body)
    return result


def run_files(history_path: str | Path, prices_path: str | Path) -> dict[str, Any]:
    history_target = Path(history_path)
    price_target = Path(prices_path)
    history_raw = history_target.read_bytes()
    price_raw = price_target.read_bytes()
    return run_investigation(
        pd.read_csv(history_target, low_memory=False),
        pd.read_csv(price_target, low_memory=False),
        history_source_hash=sha256(history_raw).hexdigest(),
        price_source_hash=sha256(price_raw).hexdigest(),
    )
