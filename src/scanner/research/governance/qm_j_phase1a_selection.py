"""QM-J application: falsify Phase-1A cross-sectional Selection with score placebos.

The real estimand is the existing Phase-1A mean daily Spearman between
cross-sectional score percentile and 20-session forward return. Negative
controls only permute score percentile within the same observation date; event
identity, forward returns and sample eligibility remain unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import Phase1AConfig, build_events, cross_sectional_summary
from scanner.research.governance.qm_j_negative_controls import (
    NegativeControlError,
    content_hash,
    evaluate_falsification,
)

SCHEMA_VERSION = "qm_j_phase1a_selection_placebo_v1"
RESULT_SCHEMA_VERSION = "qm_j_phase1a_selection_falsification_result_v1"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_j_phase1a_selection_placebo_v1.json"


@dataclass(frozen=True)
class Phase1APlaceboPlan:
    stable_start: str
    min_cross_section: int
    horizon_sessions: int
    metric: str
    metric_direction: str
    similarity_margin: float
    permutation_count: int
    permutation_quantile: float
    seed_prefix: str
    outcome_visibility_at_freeze: str


def _read_config(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NegativeControlError(f"qm_j_phase1a_config_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise NegativeControlError("qm_j_phase1a_config_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise NegativeControlError("qm_j_phase1a_scope_invalid")
    if payload.get("execution_allowed") is not False:
        raise NegativeControlError("qm_j_phase1a_execution_forbidden")
    return payload


def load_plan(path: str | Path | None = None) -> Phase1APlaceboPlan:
    payload = _read_config(path)
    source = payload.get("source_contract") or {}
    primary = payload.get("primary_test") or {}
    if primary.get("outcome_visibility_at_freeze") != "NONE":
        raise NegativeControlError("phase1a_placebo_plan_not_frozen_pre_outcome")
    if payload.get("secondary_horizons_used_for_selection") != []:
        raise NegativeControlError("phase1a_placebo_secondary_horizon_selection_forbidden")
    horizon = int(primary.get("horizon_sessions"))
    if horizon != 20:
        raise NegativeControlError("phase1a_placebo_primary_horizon_must_remain_20")
    count = int(primary.get("permutation_count"))
    quantile = float(primary.get("permutation_quantile"))
    margin = float(primary.get("similarity_margin"))
    min_cross = int(source.get("min_cross_section"))
    if count < 20:
        raise NegativeControlError("phase1a_placebo_permutation_count_too_small")
    if not 0.5 < quantile < 1.0:
        raise NegativeControlError("phase1a_placebo_quantile_invalid")
    if not math.isfinite(margin) or margin < 0:
        raise NegativeControlError("phase1a_placebo_similarity_margin_invalid")
    if min_cross < 3:
        raise NegativeControlError("phase1a_placebo_min_cross_section_invalid")
    return Phase1APlaceboPlan(
        stable_start=str(source.get("stable_start")),
        min_cross_section=min_cross,
        horizon_sessions=horizon,
        metric=str(primary.get("metric")),
        metric_direction=str(primary.get("metric_direction")),
        similarity_margin=margin,
        permutation_count=count,
        permutation_quantile=quantile,
        seed_prefix=str(primary.get("seed_prefix")),
        outcome_visibility_at_freeze="NONE",
    )


def _metric_sample(events: pd.DataFrame, horizon: int, min_n: int) -> pd.DataFrame:
    ret = f"return_{horizon}t"
    needed = ["obs_date", "symbol", "score_pct_full", ret]
    if any(column not in events.columns for column in needed):
        raise NegativeControlError("phase1a_placebo_event_schema_invalid")
    sample = events[needed].copy()
    sample["score_pct_full"] = pd.to_numeric(sample["score_pct_full"], errors="coerce")
    sample[ret] = pd.to_numeric(sample[ret], errors="coerce")
    sample = sample.dropna(subset=needed)
    counts = sample.groupby("obs_date")[ret].transform("count")
    sample = sample.loc[counts >= min_n].copy()
    if sample.empty:
        raise NegativeControlError("phase1a_placebo_no_eligible_metric_sample")
    return sample.sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)


def comparison_context_hash(sample: pd.DataFrame, horizon: int) -> str:
    ret = f"return_{horizon}t"
    rows = [
        {
            "obs_date": pd.Timestamp(row.obs_date).isoformat(),
            "symbol": str(row.symbol),
            "forward_return": float(getattr(row, ret)),
        }
        for row in sample.itertuples(index=False)
    ]
    return content_hash({"horizon_sessions": horizon, "event_grid": rows})


def _permutation_order(group: pd.DataFrame, seed: str) -> list[int]:
    keys: list[tuple[str, int]] = []
    for position, row in enumerate(group.itertuples(index=False)):
        identity = {
            "seed": seed,
            "obs_date": pd.Timestamp(row.obs_date).isoformat(),
            "symbol": str(row.symbol),
        }
        keys.append((sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest(), position))
    order = [position for _, position in sorted(keys)]
    if len(order) > 1 and order == list(range(len(order))):
        order = order[1:] + order[:1]
    return order


def permute_score_within_date(events: pd.DataFrame, *, seed: str) -> pd.DataFrame:
    if not seed:
        raise NegativeControlError("phase1a_placebo_seed_required")
    out = events.copy(deep=True)
    if "score_pct_full" not in out.columns or "obs_date" not in out.columns or "symbol" not in out.columns:
        raise NegativeControlError("phase1a_placebo_event_schema_invalid")
    original_non_score = out.drop(columns=["score_pct_full"]).copy(deep=True)
    for _, index in out.groupby("obs_date", sort=False).groups.items():
        positions = list(index)
        if len(positions) < 2:
            continue
        group = out.loc[positions, ["obs_date", "symbol", "score_pct_full"]].reset_index(drop=True)
        order = _permutation_order(group, seed)
        values = group["score_pct_full"].tolist()
        out.loc[positions, "score_pct_full"] = [values[source] for source in order]
    if not out.drop(columns=["score_pct_full"]).equals(original_non_score):
        raise NegativeControlError("phase1a_placebo_changed_non_score_fields")
    return out


def _higher_quantile(values: list[float], q: float) -> float:
    if not values:
        raise NegativeControlError("phase1a_placebo_values_required")
    ordered = sorted(float(value) for value in values)
    index = max(0, min(len(ordered) - 1, math.ceil(q * len(ordered)) - 1))
    return ordered[index]


def run_falsification(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    plan: Phase1APlaceboPlan | None = None,
    history_source_hash: str | None = None,
    price_source_hash: str | None = None,
) -> dict[str, Any]:
    plan = plan or load_plan()
    phase1_config = Phase1AConfig(
        stable_start=plan.stable_start,
        min_cross_section=plan.min_cross_section,
    )
    events = build_events(history, prices, phase1_config)
    sample = _metric_sample(events, plan.horizon_sessions, plan.min_cross_section)
    real_summary = cross_sectional_summary(events, plan.horizon_sessions, plan.min_cross_section)
    real_value = real_summary.get("mean_daily_spearman")
    if real_summary.get("days", 0) <= 0 or real_value is None or not math.isfinite(float(real_value)):
        raise NegativeControlError("phase1a_placebo_real_metric_unavailable")

    context_hash = comparison_context_hash(sample, plan.horizon_sessions)
    placebo_values: list[float] = []
    for number in range(plan.permutation_count):
        seed = f"{plan.seed_prefix}:{number:03d}"
        permuted = permute_score_within_date(events, seed=seed)
        control_sample = _metric_sample(permuted, plan.horizon_sessions, plan.min_cross_section)
        if comparison_context_hash(control_sample, plan.horizon_sessions) != context_hash:
            raise NegativeControlError("phase1a_placebo_comparison_grid_changed")
        summary = cross_sectional_summary(permuted, plan.horizon_sessions, plan.min_cross_section)
        value = summary.get("mean_daily_spearman")
        if summary.get("days") != real_summary.get("days") or value is None or not math.isfinite(float(value)):
            raise NegativeControlError("phase1a_placebo_control_metric_unavailable")
        placebo_values.append(float(value))

    p95 = _higher_quantile(placebo_values, plan.permutation_quantile)
    evaluation = evaluate_falsification(
        plan={
            "plan_id": "QM-J-PHASE1A-SELECTION-PERMUTATION",
            "plan_version": "v1",
            "frozen_at": "2026-10-01T10:53:00+00:00",
            "outcome_visibility_at_freeze": plan.outcome_visibility_at_freeze,
            "primary_metric": plan.metric,
            "metric_direction": plan.metric_direction,
            "similarity_margin": plan.similarity_margin,
        },
        real_result={
            "result_id": "phase1a-real-selection",
            "metric_name": plan.metric,
            "metric_value": float(real_value),
            "comparison_context_hash": context_hash,
            "n_observations": int(len(sample)),
        },
        control_results=[{
            "result_id": f"phase1a-score-permutation-q{int(plan.permutation_quantile * 100)}",
            "metric_name": plan.metric,
            "metric_value": p95,
            "comparison_context_hash": context_hash,
            "n_observations": int(len(sample)),
        }],
    )

    exceedance_rate = float(np.mean(np.asarray(placebo_values) >= float(real_value)))
    result: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "module": "QM-J",
        "business_area": "BA-QM7",
        "application": "PHASE1A_CROSS_SECTIONAL_SELECTION_SCORE_PERMUTATION",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "source": {
            "history_sha256": history_source_hash,
            "price_sha256": price_source_hash,
        },
        "plan": {
            "stable_start": plan.stable_start,
            "min_cross_section": plan.min_cross_section,
            "horizon_sessions": plan.horizon_sessions,
            "primary_metric": plan.metric,
            "permutation_count": plan.permutation_count,
            "permutation_quantile": plan.permutation_quantile,
            "similarity_margin": plan.similarity_margin,
            "secondary_horizons_used_for_selection": [],
        },
        "coverage": {
            "phase1a_events": int(len(events)),
            "metric_observations": int(len(sample)),
            "metric_days": int(real_summary["days"]),
            "symbols": int(sample["symbol"].nunique()),
            "event_date_min": str(pd.Timestamp(sample["obs_date"].min()).date()),
            "event_date_max": str(pd.Timestamp(sample["obs_date"].max()).date()),
        },
        "real_metric": {
            "mean_daily_spearman": float(real_value),
        },
        "placebo_distribution": {
            "min": float(min(placebo_values)),
            "median": float(np.median(placebo_values)),
            "p95_higher": float(p95),
            "max": float(max(placebo_values)),
            "placebo_ge_real_rate": exceedance_rate,
        },
        "comparison_context_hash": context_hash,
        "falsification_evaluation": evaluation,
        "phase1a_logic_changed": False,
        "history_or_prices_changed": False,
        "promotion_performed": False,
    }
    body = dict(result)
    result["result_hash"] = content_hash(body)
    return result


def run_files(
    history_path: str | Path,
    prices_path: str | Path,
    *,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    history_target = Path(history_path)
    price_target = Path(prices_path)
    history_raw = history_target.read_bytes()
    price_raw = price_target.read_bytes()
    history = pd.read_csv(history_target, low_memory=False)
    prices = pd.read_csv(price_target, low_memory=False)
    return run_falsification(
        history,
        prices,
        plan=load_plan(config_path),
        history_source_hash=sha256(history_raw).hexdigest(),
        price_source_hash=sha256(price_raw).hexdigest(),
    )
