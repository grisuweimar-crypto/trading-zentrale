"""Prospective effectiveness verification for QM-H-QMJ-PHASE1A-LAG1-001.

The plan is frozen before the eligible observation partition begins.  Historical
spent evidence cannot verify CAPA effectiveness.  This module evaluates only
scanner observation dates on/after the frozen prospective boundary and never
releases the finding or promotion block automatically.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha1, sha256
import json
import math
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from scanner.reports.selection_timing import (
    Phase1AConfig,
    _score_percentile,
    _spearman,
    build_events,
)

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_CONFIG = ROOT / "configs" / "qm_j_phase1a_lag1_effectiveness_v1.json"
DEFAULT_FREEZE = ROOT / "configs" / "qm_j_phase1a_lag1_effectiveness_freeze_v1.json"
SCHEMA_VERSION = "qm_j_phase1a_lag1_effectiveness_v1"
FREEZE_SCHEMA_VERSION = "qm_j_phase1a_lag1_effectiveness_freeze_v1"


class Lag1EffectivenessError(ValueError):
    pass


@dataclass(frozen=True)
class Lag1EffectivenessPlan:
    eligible_observation_from: str
    history_context_start: str
    horizon_sessions: int
    min_cross_section: int
    similarity_margin: float
    block_length_observation_dates: int
    bootstrap_repetitions: int
    confidence_level: float
    seed: int
    minimum_mature_observation_dates: int
    minimum_temporal_support_regions: int


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Lag1EffectivenessError(f"json_unreadable:{path}") from exc
    if not isinstance(value, dict):
        raise Lag1EffectivenessError(f"json_object_required:{path}")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise Lag1EffectivenessError(f"mapping_required:{field}")
    return value


def _git_blob_sha(raw: bytes) -> str:
    header = f"blob {len(raw)}\0".encode("utf-8")
    return sha1(header + raw).hexdigest()


def load_plan(
    config_path: str | Path = DEFAULT_CONFIG,
    freeze_path: str | Path = DEFAULT_FREEZE,
) -> Lag1EffectivenessPlan:
    config_path = Path(config_path)
    freeze_path = Path(freeze_path)
    raw = config_path.read_bytes()
    value = _read(config_path)
    freeze = _read(freeze_path)

    if value.get("schema_version") != SCHEMA_VERSION:
        raise Lag1EffectivenessError("lag1_effectiveness_schema_invalid")
    if freeze.get("schema_version") != FREEZE_SCHEMA_VERSION:
        raise Lag1EffectivenessError("lag1_effectiveness_freeze_schema_invalid")
    if value.get("finding_id") != "QM-H-QMJ-PHASE1A-LAG1-001":
        raise Lag1EffectivenessError("lag1_effectiveness_finding_invalid")
    if value.get("capa_id") != "QM-H-CAPA-QMJ-PHASE1A-LAG1-001":
        raise Lag1EffectivenessError("lag1_effectiveness_capa_invalid")
    if value.get("status") != "FROZEN_AWAITING_PROSPECTIVE_UNSPENT_EVIDENCE":
        raise Lag1EffectivenessError("lag1_effectiveness_status_invalid")
    if value.get("research_only") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_research_only_required")
    if value.get("productive_integration_enabled") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_productive_integration_forbidden")
    if value.get("execution_allowed") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_execution_forbidden")

    frozen = _mapping(value.get("freeze_evidence"), "freeze_evidence")
    eligible = str(frozen.get("eligible_observation_from") or "")
    if eligible != "2026-10-06":
        raise Lag1EffectivenessError("lag1_effectiveness_prospective_boundary_changed")
    if frozen.get("historical_spent_evidence_may_verify_effectiveness") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_spent_evidence_forbidden")
    if freeze.get("eligible_observation_from") != eligible:
        raise Lag1EffectivenessError("lag1_effectiveness_freeze_boundary_mismatch")
    if freeze.get("outcome_visibility_for_eligible_partition_at_freeze") != "NONE":
        raise Lag1EffectivenessError("lag1_effectiveness_outcomes_visible_at_freeze")
    if freeze.get("historical_spent_evidence_may_verify_effectiveness") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_freeze_spent_evidence_invalid")
    expected_blob = str(freeze.get("plan_blob_sha") or "").lower()
    if _git_blob_sha(raw) != expected_blob:
        raise Lag1EffectivenessError("lag1_effectiveness_plan_changed_after_freeze")
    if len(str(freeze.get("plan_commit") or "")) != 40:
        raise Lag1EffectivenessError("lag1_effectiveness_freeze_commit_invalid")

    source = _mapping(value.get("source_contract"), "source_contract")
    if int(source.get("horizon_sessions") or 0) != 20:
        raise Lag1EffectivenessError("lag1_effectiveness_horizon_changed")
    if int(source.get("min_cross_section") or 0) != 20:
        raise Lag1EffectivenessError("lag1_effectiveness_min_cross_section_changed")
    if source.get("adjusted_close_required") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_adjusted_close_required")
    context_start = str(source.get("history_context_start") or "")
    if context_start != "2026-04-15":
        raise Lag1EffectivenessError("lag1_effectiveness_context_start_changed")

    estimand = _mapping(value.get("primary_estimand"), "primary_estimand")
    if estimand.get("metric") != "mean_daily_spearman_current_minus_lag1":
        raise Lag1EffectivenessError("lag1_effectiveness_estimand_changed")
    if estimand.get("matched_grid_required") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_matched_grid_required")
    if estimand.get("score_percentiles_recomputed_on_identical_matched_daily_cross_section") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_percentile_recompute_required")
    if estimand.get("lag_direction") != "PAST_TO_CURRENT_ONLY":
        raise Lag1EffectivenessError("lag1_effectiveness_lag_direction_changed")
    if estimand.get("uses_future_predictor_rows") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_future_predictor_forbidden")

    acceptance = _mapping(value.get("acceptance_rule"), "acceptance_rule")
    margin = float(acceptance.get("inherited_similarity_margin"))
    if margin != 0.01:
        raise Lag1EffectivenessError("lag1_effectiveness_margin_changed")
    if acceptance.get("support_rule") != "lower_95_confidence_bound_current_minus_lag1 > 0.01":
        raise Lag1EffectivenessError("lag1_effectiveness_support_rule_changed")
    if acceptance.get("automatic_finding_closure") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_auto_closure_forbidden")
    if acceptance.get("automatic_promotion_release") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_auto_release_forbidden")

    dependence = _mapping(value.get("dependence_control"), "dependence_control")
    if dependence.get("method") != "CIRCULAR_MOVING_BLOCK_BOOTSTRAP_OVER_OBSERVATION_DATES":
        raise Lag1EffectivenessError("lag1_effectiveness_dependence_method_changed")
    block = int(dependence.get("block_length_observation_dates") or 0)
    reps = int(dependence.get("bootstrap_repetitions") or 0)
    confidence = float(dependence.get("confidence_level") or 0.0)
    seed = int(dependence.get("seed") or 0)
    if block != 20 or reps != 5000 or confidence != 0.95 or seed != 20261005:
        raise Lag1EffectivenessError("lag1_effectiveness_bootstrap_contract_changed")
    if dependence.get("overlapping_forward_windows_treated_as_iid") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_iid_overlap_forbidden")

    maturity = _mapping(value.get("maturity_gate"), "maturity_gate")
    min_dates = int(maturity.get("minimum_mature_observation_dates") or 0)
    min_regions = int(maturity.get("minimum_temporal_support_regions") or 0)
    if min_dates != 40 or min_regions != 2:
        raise Lag1EffectivenessError("lag1_effectiveness_maturity_gate_changed")
    if maturity.get("forward_outcome_must_be_fully_observed") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_mature_outcome_required")

    multiplicity = _mapping(value.get("multiplicity"), "multiplicity")
    if int(multiplicity.get("primary_tests") or 0) != 1:
        raise Lag1EffectivenessError("lag1_effectiveness_primary_test_count_changed")
    if multiplicity.get("secondary_horizons_may_release_block") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_secondary_release_forbidden")

    release = _mapping(value.get("release_guard"), "release_guard")
    if release.get("automatic_release_allowed") is not False:
        raise Lag1EffectivenessError("lag1_effectiveness_automatic_release_forbidden")
    if release.get("qm_h_effectiveness_transition_required") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_qmh_transition_required")
    if release.get("independent_review_required") is not True:
        raise Lag1EffectivenessError("lag1_effectiveness_review_required")

    return Lag1EffectivenessPlan(
        eligible_observation_from=eligible,
        history_context_start=context_start,
        horizon_sessions=20,
        min_cross_section=20,
        similarity_margin=margin,
        block_length_observation_dates=block,
        bootstrap_repetitions=reps,
        confidence_level=confidence,
        seed=seed,
        minimum_mature_observation_dates=min_dates,
        minimum_temporal_support_regions=min_regions,
    )


def build_daily_differences(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    plan: Lag1EffectivenessPlan | None = None,
) -> pd.DataFrame:
    plan = plan or load_plan()
    config = Phase1AConfig(
        stable_start=plan.history_context_start,
        min_cross_section=plan.min_cross_section,
    )
    events = build_events(history, prices, config)
    columns = ["obs_date", "current_spearman", "lag1_spearman", "current_minus_lag1", "N"]
    if events.empty:
        return pd.DataFrame(columns=columns)

    ret = f"return_{plan.horizon_sessions}t"
    work = events[["obs_date", "symbol", "score", ret]].copy()
    work["obs_date"] = pd.to_datetime(work["obs_date"], errors="coerce")
    work["score"] = pd.to_numeric(work["score"], errors="coerce")
    work[ret] = pd.to_numeric(work[ret], errors="coerce")
    work = work.sort_values(["symbol", "obs_date"], kind="mergesort")
    work["lag1_score"] = work.groupby("symbol", sort=False)["score"].shift(1)
    work = work.loc[
        work["obs_date"] >= pd.Timestamp(plan.eligible_observation_from)
    ].dropna(subset=["obs_date", "symbol", "score", "lag1_score", ret])

    rows: list[dict[str, Any]] = []
    for day, group in work.groupby("obs_date", sort=True):
        matched = group[["score", "lag1_score", ret]].dropna().copy()
        if len(matched) < plan.min_cross_section:
            continue
        matched["current_pct"] = _score_percentile(matched["score"])
        matched["lag1_pct"] = _score_percentile(matched["lag1_score"])
        current = _spearman(matched["current_pct"], matched[ret])
        lagged = _spearman(matched["lag1_pct"], matched[ret])
        if current is None or lagged is None:
            continue
        rows.append({
            "obs_date": pd.Timestamp(day).normalize(),
            "current_spearman": float(current),
            "lag1_spearman": float(lagged),
            "current_minus_lag1": float(current - lagged),
            "N": int(len(matched)),
        })
    return pd.DataFrame(rows, columns=columns)


def _circular_block_bootstrap_lower_bound(
    values: np.ndarray,
    *,
    block_length: int,
    repetitions: int,
    confidence_level: float,
    seed: int,
) -> float:
    arr = np.asarray(values, dtype=float)
    if len(arr) == 0 or not np.isfinite(arr).all():
        raise Lag1EffectivenessError("lag1_effectiveness_bootstrap_values_invalid")
    if block_length <= 0 or repetitions <= 0:
        raise Lag1EffectivenessError("lag1_effectiveness_bootstrap_parameters_invalid")
    rng = np.random.default_rng(seed)
    n = len(arr)
    blocks_needed = int(math.ceil(n / block_length))
    means = np.empty(repetitions, dtype=float)
    offsets = np.arange(block_length, dtype=int)
    for i in range(repetitions):
        starts = rng.integers(0, n, size=blocks_needed)
        indices = ((starts[:, None] + offsets[None, :]) % n).reshape(-1)[:n]
        means[i] = float(arr[indices].mean())
    alpha = 1.0 - confidence_level
    return float(np.quantile(means, alpha, method="linear"))


def evaluate_daily_differences(
    daily: pd.DataFrame,
    plan: Lag1EffectivenessPlan | None = None,
) -> dict[str, Any]:
    plan = plan or load_plan()
    z = daily.copy()
    if "obs_date" not in z.columns or "current_minus_lag1" not in z.columns:
        raise Lag1EffectivenessError("lag1_effectiveness_daily_schema_invalid")
    z["obs_date"] = pd.to_datetime(z["obs_date"], errors="coerce")
    z["current_minus_lag1"] = pd.to_numeric(z["current_minus_lag1"], errors="coerce")
    z = z.dropna(subset=["obs_date", "current_minus_lag1"]).sort_values("obs_date")
    z = z.loc[z["obs_date"] >= pd.Timestamp(plan.eligible_observation_from)].copy()

    mature_dates = int(z["obs_date"].nunique())
    support_regions = mature_dates // plan.block_length_observation_dates
    base = {
        "schema_version": "qm_j_phase1a_lag1_effectiveness_result_v1",
        "finding_id": "QM-H-QMJ-PHASE1A-LAG1-001",
        "capa_id": "QM-H-CAPA-QMJ-PHASE1A-LAG1-001",
        "eligible_observation_from": plan.eligible_observation_from,
        "mature_observation_dates": mature_dates,
        "minimum_mature_observation_dates": plan.minimum_mature_observation_dates,
        "temporal_support_regions": support_regions,
        "minimum_temporal_support_regions": plan.minimum_temporal_support_regions,
        "inherited_similarity_margin": plan.similarity_margin,
        "automatic_finding_closure": False,
        "automatic_release_allowed": False,
        "promotion_released": False,
        "historical_spent_evidence_used_for_effectiveness": False,
    }
    if (
        mature_dates < plan.minimum_mature_observation_dates
        or support_regions < plan.minimum_temporal_support_regions
    ):
        return {
            **base,
            "status": "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE",
            "effectiveness_verified": False,
            "observed_mean_current_minus_lag1": (
                float(z["current_minus_lag1"].mean()) if len(z) else None
            ),
            "lower_95_confidence_bound_current_minus_lag1": None,
            "review_required": False,
        }

    values = z["current_minus_lag1"].to_numpy(dtype=float)
    mean = float(values.mean())
    lower = _circular_block_bootstrap_lower_bound(
        values,
        block_length=plan.block_length_observation_dates,
        repetitions=plan.bootstrap_repetitions,
        confidence_level=plan.confidence_level,
        seed=plan.seed,
    )
    supported = lower > plan.similarity_margin
    return {
        **base,
        "status": (
            "CAPA_EFFECTIVENESS_EVIDENCE_SUPPORTS_REVIEW"
            if supported
            else "CAPA_EFFECTIVENESS_NOT_ESTABLISHED"
        ),
        "effectiveness_verified": False,
        "observed_mean_current_minus_lag1": mean,
        "lower_95_confidence_bound_current_minus_lag1": lower,
        "review_required": supported,
    }


def evaluate_frames(
    history: pd.DataFrame,
    prices: pd.DataFrame,
    plan: Lag1EffectivenessPlan | None = None,
) -> dict[str, Any]:
    plan = plan or load_plan()
    daily = build_daily_differences(history, prices, plan)
    result = evaluate_daily_differences(daily, plan)
    result["matched_mature_daily_rows"] = int(len(daily))
    if len(daily):
        result["mature_date_min"] = str(pd.Timestamp(daily["obs_date"].min()).date())
        result["mature_date_max"] = str(pd.Timestamp(daily["obs_date"].max()).date())
    else:
        result["mature_date_min"] = None
        result["mature_date_max"] = None
    return result


def run_files(
    history_path: str | Path,
    prices_path: str | Path,
    *,
    config_path: str | Path = DEFAULT_CONFIG,
    freeze_path: str | Path = DEFAULT_FREEZE,
) -> dict[str, Any]:
    history_path = Path(history_path)
    prices_path = Path(prices_path)
    plan = load_plan(config_path, freeze_path)
    history_raw = history_path.read_bytes()
    prices_raw = prices_path.read_bytes()
    history = pd.read_csv(history_path, low_memory=False)
    prices = pd.read_csv(prices_path, low_memory=False)
    result = evaluate_frames(history, prices, plan)
    result["source_hashes"] = {
        "history_sha256": sha256(history_raw).hexdigest(),
        "prices_sha256": sha256(prices_raw).hexdigest(),
    }
    return result
