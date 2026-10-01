"""BA-QM7 / QM-J follow-on falsification on Phase-1A data and research levels.

The original Phase-1A real 20-session Selection result is already known from the
preceding QM-J score-permutation test.  Therefore these controls are deliberately
one-way falsification diagnostics: a strong control can stop promotion and open
investigation/CAPA, while a weak control can never strengthen confirmation or
perform promotion.

Two frozen attacks are implemented:
1) DATA / SHIFTED_DATA: replace each score with the same symbol's immediately
   preceding observed score (past-only, never future information) and evaluate
   both real and shifted scores on the exact same eligible daily grid.
2) RESEARCH / PSEUDO_SIGNAL: generate deterministic outcome-blind pseudo signals
   from (obs_date, symbol) identities and compare the predeclared p95 null metric
   with the real Phase-1A 20-session metric.

No source data, scanner logic, Decision Layer, Depot Watch or production artifact
is mutated.
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

from scanner.reports.selection_timing import (
    Phase1AConfig,
    _score_percentile,
    build_events,
    cross_sectional_summary,
)
from scanner.research.governance.qm_j_negative_controls import (
    NegativeControlError,
    content_hash,
    pseudo_signal_control,
    shifted_data_control,
)
from scanner.research.governance.qm_j_phase1a_selection import (
    _higher_quantile,
    _metric_sample,
    comparison_context_hash,
)

SCHEMA_VERSION = "qm_j_phase1a_data_research_controls_v1"
RESULT_SCHEMA_VERSION = "qm_j_phase1a_data_research_falsification_result_v1"
DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "qm_j_phase1a_data_research_controls_v1.json"
)


@dataclass(frozen=True)
class FollowOnPlan:
    stable_start: str
    min_cross_section: int
    horizon_sessions: int
    metric: str
    metric_direction: str
    similarity_margin: float
    shift_by_observations: int
    pseudo_signal_count: int
    pseudo_signal_quantile: float
    pseudo_seed_prefix: str
    prior_real_result_visibility: str = "KNOWN"
    new_control_outcomes_visibility: str = "NONE"
    confirmatory_promotion_use_allowed: bool = False


def _read_config(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONFIG_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NegativeControlError(f"qm_j_followon_config_unreadable:{target}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise NegativeControlError("qm_j_followon_config_schema_invalid")
    if payload.get("status") != "frozen_before_control_execution":
        raise NegativeControlError("qm_j_followon_plan_not_frozen")
    if payload.get("research_only") is not True:
        raise NegativeControlError("qm_j_followon_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise NegativeControlError("qm_j_followon_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise NegativeControlError("qm_j_followon_execution_forbidden")
    return payload


def load_plan(path: str | Path | None = None) -> FollowOnPlan:
    payload = _read_config(path)
    freeze = payload.get("freeze_evidence") or {}
    knowledge = payload.get("knowledge_state") or {}
    source = payload.get("source_contract") or {}
    data = payload.get("data_control") or {}
    research = payload.get("research_control") or {}
    decision = payload.get("decision_rule") or {}
    boundaries = payload.get("boundaries") or {}

    predeclared = str(freeze.get("predeclaration_commit") or "").strip().lower()
    if len(predeclared) != 40:
        raise NegativeControlError("qm_j_followon_predeclaration_commit_invalid")
    if freeze.get("new_control_outcomes_visibility_at_freeze") != "NONE":
        raise NegativeControlError("qm_j_followon_new_control_outcomes_were_visible")
    if knowledge.get("prior_phase1a_real_result_visibility_at_freeze") != "KNOWN":
        raise NegativeControlError("qm_j_followon_must_disclose_known_real_baseline")
    if knowledge.get("new_control_outcomes_visibility_at_freeze") != "NONE":
        raise NegativeControlError("qm_j_followon_control_outcomes_not_blind")
    if knowledge.get("confirmatory_promotion_use_allowed") is not False:
        raise NegativeControlError("qm_j_followon_confirmatory_promotion_forbidden")

    if source.get("primary_metric") != "phase1a_20t_mean_daily_spearman":
        raise NegativeControlError("qm_j_followon_primary_metric_changed")
    if source.get("metric_direction") != "HIGHER_IS_BETTER":
        raise NegativeControlError("qm_j_followon_metric_direction_changed")
    if int(source.get("horizon_sessions", 0)) != 20:
        raise NegativeControlError("qm_j_followon_horizon_must_remain_20")
    if list(source.get("secondary_horizons_used_for_selection") or []) != []:
        raise NegativeControlError("qm_j_followon_secondary_horizon_selection_forbidden")
    min_cross = int(source.get("min_cross_section", 0))
    if min_cross < 3:
        raise NegativeControlError("qm_j_followon_min_cross_section_invalid")
    margin = float(source.get("similarity_margin"))
    if not math.isfinite(margin) or margin != 0.01:
        raise NegativeControlError("qm_j_followon_similarity_margin_must_remain_0_01")

    if data.get("control_type") != "SHIFTED_DATA":
        raise NegativeControlError("qm_j_followon_data_control_type_invalid")
    if data.get("source_field") != "score" or data.get("entity_field") != "symbol":
        raise NegativeControlError("qm_j_followon_data_control_fields_invalid")
    shift_by = int(data.get("shift_by_observations", 0))
    if shift_by != 1 or data.get("direction") != "PAST_TO_CURRENT_ONLY":
        raise NegativeControlError("qm_j_followon_shift_contract_changed")
    if data.get("uses_future_rows") is not False:
        raise NegativeControlError("qm_j_followon_future_shift_forbidden")
    if data.get("leading_missing_preserved_as_missing") is not True:
        raise NegativeControlError("qm_j_followon_shift_missing_guard_required")
    if data.get("matched_grid_required") is not True:
        raise NegativeControlError("qm_j_followon_shift_matched_grid_required")
    if data.get("recompute_real_and_control_score_percentiles_on_identical_matched_daily_cross_sections") is not True:
        raise NegativeControlError("qm_j_followon_shift_percentile_recompute_required")

    if research.get("control_type") != "PSEUDO_SIGNAL":
        raise NegativeControlError("qm_j_followon_research_control_type_invalid")
    if list(research.get("identity_fields") or []) != ["obs_date", "symbol"]:
        raise NegativeControlError("qm_j_followon_pseudo_identity_changed")
    if list(research.get("states") or []) != ["NEGATIVE", "NEUTRAL", "POSITIVE"]:
        raise NegativeControlError("qm_j_followon_pseudo_states_changed")
    numeric_map = research.get("numeric_map") or {}
    if numeric_map != {"NEGATIVE": -1.0, "NEUTRAL": 0.0, "POSITIVE": 1.0}:
        raise NegativeControlError("qm_j_followon_pseudo_numeric_map_changed")
    pseudo_count = int(research.get("control_count", 0))
    pseudo_quantile = float(research.get("control_quantile", 0.0))
    if pseudo_count != 64 or pseudo_quantile != 0.95:
        raise NegativeControlError("qm_j_followon_pseudo_null_contract_changed")
    if research.get("outcomes_used_to_generate_signals") is not False:
        raise NegativeControlError("qm_j_followon_pseudo_outcome_use_forbidden")
    if research.get("matched_grid_required") is not True:
        raise NegativeControlError("qm_j_followon_pseudo_matched_grid_required")

    if decision.get("automatic_promotion_allowed") is not False:
        raise NegativeControlError("qm_j_followon_automatic_promotion_forbidden")
    if decision.get("nontrigger_is_validation") is not False:
        raise NegativeControlError("qm_j_followon_nontrigger_validation_forbidden")
    if decision.get("automatic_qm_h_mutation_from_ci") is not False:
        raise NegativeControlError("qm_j_followon_ci_may_not_mutate_qm_h")

    required_false = (
        "history_or_prices_mutated",
        "phase1a_logic_changed",
        "scanner_weights_changed",
        "decision_layer_changed",
        "depot_watch_changed",
        "productive_artifacts_replaced",
        "orders_generated",
        "execution_enabled",
        "promotion_performed",
    )
    if any(boundaries.get(field) is not False for field in required_false):
        raise NegativeControlError("qm_j_followon_production_boundary_invalid")

    return FollowOnPlan(
        stable_start=str(source.get("stable_start") or ""),
        min_cross_section=min_cross,
        horizon_sessions=20,
        metric="phase1a_20t_mean_daily_spearman",
        metric_direction="HIGHER_IS_BETTER",
        similarity_margin=margin,
        shift_by_observations=shift_by,
        pseudo_signal_count=pseudo_count,
        pseudo_signal_quantile=pseudo_quantile,
        pseudo_seed_prefix=str(research.get("seed_prefix") or ""),
    )


def _finite_metric(summary: Mapping[str, Any], label: str) -> float:
    value = summary.get("mean_daily_spearman")
    if int(summary.get("days", 0) or 0) <= 0 or value is None:
        raise NegativeControlError(f"qm_j_followon_metric_unavailable:{label}")
    number = float(value)
    if not math.isfinite(number):
        raise NegativeControlError(f"qm_j_followon_metric_unavailable:{label}")
    return number


def _one_way_evaluation(
    *,
    plan: FollowOnPlan,
    control_id: str,
    real_value: float,
    control_value: float,
    comparison_context_hash_value: str,
    n_observations: int,
) -> dict[str, Any]:
    if plan.prior_real_result_visibility != "KNOWN":
        raise NegativeControlError("qm_j_followon_real_baseline_visibility_must_be_known")
    if plan.new_control_outcomes_visibility != "NONE":
        raise NegativeControlError("qm_j_followon_control_outcome_visibility_invalid")
    if plan.confirmatory_promotion_use_allowed:
        raise NegativeControlError("qm_j_followon_confirmatory_promotion_forbidden")
    difference = float(real_value) - float(control_value)
    triggered = difference <= plan.similarity_margin
    return {
        "schema_version": "qm_j_followon_falsification_evaluation_v1",
        "control_id": control_id,
        "primary_metric": plan.metric,
        "metric_direction": plan.metric_direction,
        "real_metric_value": float(real_value),
        "control_metric_value": float(control_value),
        "real_minus_control": difference,
        "similarity_margin": plan.similarity_margin,
        "comparison_context_hash": comparison_context_hash_value,
        "n_observations": int(n_observations),
        "prior_real_result_visibility_at_freeze": "KNOWN",
        "new_control_outcomes_visibility_at_freeze": "NONE",
        "one_way_falsification_only": True,
        "nontrigger_is_validation": False,
        "confirmatory_promotion_use_allowed": False,
        "triggered": triggered,
        "status": (
            "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
            if triggered
            else "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG"
        ),
        "promotion_blocked_by_qm_j": triggered,
        "investigation_required": triggered,
        "capa_required": triggered,
        "promotion_performed": False,
    }


def _context_hash(sample: pd.DataFrame, horizon: int) -> str:
    return comparison_context_hash(
        sample[["obs_date", "symbol", "score_pct_full", f"return_{horizon}t"]].copy(),
        horizon,
    )


def _run_shifted_data(events: pd.DataFrame, plan: FollowOnPlan) -> dict[str, Any]:
    ret = f"return_{plan.horizon_sessions}t"
    needed = ["obs_date", "symbol", "score", ret]
    if any(column not in events.columns for column in needed):
        raise NegativeControlError("qm_j_followon_shift_event_schema_invalid")

    ordered = events[needed].copy()
    ordered["score"] = pd.to_numeric(ordered["score"], errors="coerce")
    ordered[ret] = pd.to_numeric(ordered[ret], errors="coerce")
    ordered = ordered.dropna(subset=["obs_date", "symbol", "score"])
    ordered = ordered.sort_values(["symbol", "obs_date"], kind="mergesort").reset_index(drop=True)
    records = [
        {
            "obs_date": pd.Timestamp(row.obs_date).isoformat(),
            "symbol": str(row.symbol),
            "score": float(row.score),
        }
        for row in ordered.itertuples(index=False)
    ]
    artifact = shifted_data_control(
        records,
        source_snapshot_id="phase1a-history-analysis-followon",
        value_fields=["score"],
        entity_fields=["symbol"],
        shift_by=plan.shift_by_observations,
    )
    transformed = pd.DataFrame(artifact["records"]).rename(columns={"score": "shifted_score"})
    transformed["obs_date"] = pd.to_datetime(transformed["obs_date"], errors="raise")
    transformed["symbol"] = transformed["symbol"].astype(str)

    merged = ordered.merge(
        transformed[["obs_date", "symbol", "shifted_score"]],
        on=["obs_date", "symbol"],
        how="left",
        validate="one_to_one",
    )
    merged["shifted_score"] = pd.to_numeric(merged["shifted_score"], errors="coerce")
    matched = merged.dropna(subset=["score", "shifted_score", ret]).copy()
    counts = matched.groupby("obs_date")[ret].transform("count")
    matched = matched.loc[counts >= plan.min_cross_section].copy()
    if matched.empty:
        raise NegativeControlError("qm_j_followon_shift_no_matched_sample")

    matched["real_score_pct"] = (
        matched.groupby("obs_date", group_keys=False)["score"].apply(_score_percentile)
    )
    matched["shifted_score_pct"] = (
        matched.groupby("obs_date", group_keys=False)["shifted_score"].apply(_score_percentile)
    )
    matched = matched.dropna(subset=["real_score_pct", "shifted_score_pct", ret]).copy()

    real_sample = matched[["obs_date", "symbol", "real_score_pct", ret]].rename(
        columns={"real_score_pct": "score_pct_full"}
    )
    control_sample = matched[["obs_date", "symbol", "shifted_score_pct", ret]].rename(
        columns={"shifted_score_pct": "score_pct_full"}
    )
    real_sample = real_sample.sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)
    control_sample = control_sample.sort_values(["obs_date", "symbol"], kind="mergesort").reset_index(drop=True)
    real_context = _context_hash(real_sample, plan.horizon_sessions)
    control_context = _context_hash(control_sample, plan.horizon_sessions)
    if real_context != control_context:
        raise NegativeControlError("qm_j_followon_shift_comparison_grid_changed")

    real_summary = cross_sectional_summary(real_sample, plan.horizon_sessions, plan.min_cross_section)
    control_summary = cross_sectional_summary(control_sample, plan.horizon_sessions, plan.min_cross_section)
    if int(real_summary.get("days", 0)) != int(control_summary.get("days", 0)):
        raise NegativeControlError("qm_j_followon_shift_metric_days_changed")
    real_value = _finite_metric(real_summary, "shift_real")
    control_value = _finite_metric(control_summary, "shift_control")
    evaluation = _one_way_evaluation(
        plan=plan,
        control_id="phase1a-score-lag-1-observation",
        real_value=real_value,
        control_value=control_value,
        comparison_context_hash_value=real_context,
        n_observations=len(real_sample),
    )
    return {
        "control_type": "SHIFTED_DATA",
        "artifact_hash": artifact["artifact_hash"],
        "source_content_hash": artifact["source_content_hash"],
        "control_content_hash": artifact["control_content_hash"],
        "shift_by_observations": plan.shift_by_observations,
        "uses_future_rows": False,
        "matched_grid": True,
        "metric_observations": int(len(real_sample)),
        "metric_days": int(real_summary["days"]),
        "symbols": int(real_sample["symbol"].nunique()),
        "event_date_min": str(pd.Timestamp(real_sample["obs_date"].min()).date()),
        "event_date_max": str(pd.Timestamp(real_sample["obs_date"].max()).date()),
        "real_metric": real_value,
        "control_metric": control_value,
        "evaluation": evaluation,
    }


def _run_pseudo_signals(events: pd.DataFrame, plan: FollowOnPlan) -> dict[str, Any]:
    sample = _metric_sample(events, plan.horizon_sessions, plan.min_cross_section)
    context_hash_value = comparison_context_hash(sample, plan.horizon_sessions)
    real_summary = cross_sectional_summary(events, plan.horizon_sessions, plan.min_cross_section)
    real_value = _finite_metric(real_summary, "pseudo_real")
    ret = f"return_{plan.horizon_sessions}t"
    identity_records = [
        {
            "obs_date": pd.Timestamp(row.obs_date).isoformat(),
            "symbol": str(row.symbol),
        }
        for row in sample.itertuples(index=False)
    ]
    state_map = {"NEGATIVE": -1.0, "NEUTRAL": 0.0, "POSITIVE": 1.0}
    values: list[float] = []
    artifact_hashes: list[str] = []
    for number in range(plan.pseudo_signal_count):
        seed = f"{plan.pseudo_seed_prefix}:{number:03d}"
        artifact = pseudo_signal_control(
            identity_records,
            source_snapshot_id="phase1a-history-analysis-followon",
            identity_fields=["obs_date", "symbol"],
            signal_field="qm_j_pseudo_signal",
            seed=seed,
        )
        artifact_hashes.append(str(artifact["artifact_hash"]))
        signal_frame = pd.DataFrame(artifact["records"])
        signal_frame["obs_date"] = pd.to_datetime(signal_frame["obs_date"], errors="raise")
        signal_frame["score_pct_full"] = signal_frame["qm_j_pseudo_signal"].map(state_map)
        signal_frame[ret] = sample[ret].to_numpy(copy=True)
        signal_frame = signal_frame[["obs_date", "symbol", "score_pct_full", ret]]
        if _context_hash(signal_frame, plan.horizon_sessions) != context_hash_value:
            raise NegativeControlError("qm_j_followon_pseudo_comparison_grid_changed")
        summary = cross_sectional_summary(signal_frame, plan.horizon_sessions, plan.min_cross_section)
        value = _finite_metric(summary, f"pseudo_{number:03d}")
        if int(summary.get("days", 0)) != int(real_summary.get("days", 0)):
            raise NegativeControlError("qm_j_followon_pseudo_metric_days_changed")
        values.append(value)

    p95 = _higher_quantile(values, plan.pseudo_signal_quantile)
    evaluation = _one_way_evaluation(
        plan=plan,
        control_id="phase1a-pseudo-signal-p95",
        real_value=real_value,
        control_value=p95,
        comparison_context_hash_value=context_hash_value,
        n_observations=len(sample),
    )
    return {
        "control_type": "PSEUDO_SIGNAL",
        "control_count": plan.pseudo_signal_count,
        "control_quantile": plan.pseudo_signal_quantile,
        "outcomes_used_to_generate_signals": False,
        "matched_grid": True,
        "metric_observations": int(len(sample)),
        "metric_days": int(real_summary["days"]),
        "symbols": int(sample["symbol"].nunique()),
        "real_metric": real_value,
        "control_distribution": {
            "min": float(min(values)),
            "median": float(np.median(values)),
            "p95_higher": float(p95),
            "max": float(max(values)),
            "control_ge_real_rate": float(np.mean(np.asarray(values) >= real_value)),
        },
        "control_set_hash": content_hash(artifact_hashes),
        "comparison_context_hash": context_hash_value,
        "evaluation": evaluation,
    }


def run_falsification(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    plan: FollowOnPlan | None = None,
    history_source_hash: str | None = None,
    price_source_hash: str | None = None,
) -> dict[str, Any]:
    plan = plan or load_plan()
    history_before = history.copy(deep=True)
    prices_before = prices.copy(deep=True)
    config = Phase1AConfig(
        stable_start=plan.stable_start,
        min_cross_section=plan.min_cross_section,
    )
    events = build_events(history, prices, config)
    if events.empty:
        raise NegativeControlError("qm_j_followon_phase1a_events_missing")

    data_result = _run_shifted_data(events, plan)
    research_result = _run_pseudo_signals(events, plan)
    if not history.equals(history_before) or not prices.equals(prices_before):
        raise NegativeControlError("qm_j_followon_source_frames_mutated")

    triggered = bool(
        data_result["evaluation"]["triggered"]
        or research_result["evaluation"]["triggered"]
    )
    result: dict[str, Any] = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "module": "QM-J",
        "business_area": "BA-QM7",
        "application": "PHASE1A_DATA_AND_RESEARCH_FOLLOWON_FALSIFICATION",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "source": {
            "history_sha256": history_source_hash,
            "price_sha256": price_source_hash,
        },
        "knowledge_state": {
            "prior_phase1a_real_result_visibility_at_freeze": "KNOWN",
            "new_control_outcomes_visibility_at_freeze": "NONE",
            "one_way_falsification_only": True,
            "nontrigger_is_validation": False,
            "confirmatory_promotion_use_allowed": False,
        },
        "plan": {
            "stable_start": plan.stable_start,
            "min_cross_section": plan.min_cross_section,
            "horizon_sessions": plan.horizon_sessions,
            "primary_metric": plan.metric,
            "similarity_margin": plan.similarity_margin,
            "shift_by_observations": plan.shift_by_observations,
            "pseudo_signal_count": plan.pseudo_signal_count,
            "pseudo_signal_quantile": plan.pseudo_signal_quantile,
        },
        "coverage": {
            "phase1a_events": int(len(events)),
            "event_symbols": int(events["symbol"].nunique()),
        },
        "data_control": data_result,
        "research_control": research_result,
        "status": (
            "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
            if triggered
            else "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG"
        ),
        "promotion_blocked_by_qm_j": triggered,
        "investigation_required": triggered,
        "capa_required": triggered,
        "history_or_prices_mutated": False,
        "phase1a_logic_changed": False,
        "scanner_weights_changed": False,
        "decision_layer_changed": False,
        "depot_watch_changed": False,
        "productive_artifacts_replaced": False,
        "orders_generated": False,
        "execution_enabled": False,
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
