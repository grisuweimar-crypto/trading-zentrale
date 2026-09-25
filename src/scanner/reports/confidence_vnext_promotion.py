from __future__ import annotations

"""Phase 5E adaptive shadow inference and promotion gate.

This is the technical completion layer for Confidence vNext Phase 5. It
applies one frozen, deterministic policy to frozen Phase-5C state reliability
tables, evaluates that policy only on finalized Phase-5D future epochs, and
feeds the resulting evidence into the Phase-5A promotion gate.

Research-only: no production Confidence, scanner weights, thresholds, R-codes,
Depot-Watch, portfolio logic, or trading actions are changed here.
"""

from hashlib import sha256
from pathlib import Path
import json

import numpy as np
import pandas as pd

from scanner.reports.confidence_vnext_frozen_baseline import _baseline_dates, _support_regions
from scanner.reports.confidence_vnext_progressive import MULTI_STATE_FIELDS, SINGLE_STATE_FIELDS, _read_versions
from scanner.reports.confidence_vnext_prospective_v2 import CLAIM_COLUMNS_V2, OUTCOME_COLUMNS_V2, validate_v2_archives
from scanner.reports.confidence_vnext_walkforward import promotion_assessment
from scanner.reports.confidence_vnext_walkforward_v2 import _epoch_frame, _evaluation_fingerprint, _read_evaluations
from scanner.reports.selection_timing import HORIZONS


SCHEMA_VERSION = "phase5e_adaptive_shadow_v1"
POLICY_VERSION = "phase5e_state_reliability_follow_flip_v1"
MIN_PROMOTION_EPOCHS = 2
MAX_SYMBOL_SHARE = 0.25
MAX_OBSERVATION_DATE_SHARE = 0.60

POLICY_CONTRACT = {
    "policy_version": POLICY_VERSION,
    "research_only": True,
    "inputs": {
        "frozen_phase5c_exact_state_tables": True,
        "evaluation_row_claim_time_states_only": True,
        "evaluation_outcome_used_to_choose_action": False,
        "raw_scanner_score_used": False,
    },
    "state_semantics": {
        "state_names_assumed_ordinal": False,
        "single_state_fields": list(SINGLE_STATE_FIELDS),
        "multi_state_fields": list(MULTI_STATE_FIELDS),
    },
    "aggregation": {
        "within_multi_state_field": "directional_N_weighted_mean_of_training_mean_signed_peer_excess",
        "across_fields": "equal_weight_mean_of_available_field_signals",
        "minimum_state_support_threshold": None,
    },
    "action": {
        "positive_or_zero_signal": "follow_frozen_phase4_direction",
        "negative_signal": "invert_frozen_phase4_direction",
        "no_empirical_signal": "follow_frozen_phase4_direction",
        "abstention": False,
    },
    "promotion": {
        "horizon_specific": True,
        "minimum_robust_non_overlapping_epochs": MIN_PROMOTION_EPOCHS,
        "maximum_symbol_share": MAX_SYMBOL_SHARE,
        "maximum_observation_date_share": MAX_OBSERVATION_DATE_SHARE,
        "primary_advantage_metric": "paired_mean_signed_peer_excess_delta",
        "primary_advantage_requirement": "robust_95_interval_lower_bound_above_zero",
        "secondary_non_degradation_metric": "paired_direction_hit_rate_delta",
        "secondary_non_degradation_requirement": "point_estimate_greater_than_or_equal_to_zero",
        "temporal_stability_requirement": "primary_point_delta_positive_in_every_robust_epoch",
    },
}


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


POLICY_SHA256 = sha256(_canonical_json(POLICY_CONTRACT).encode("utf-8")).hexdigest()


def _read_csv(path: str | Path, columns: tuple[str, ...]) -> pd.DataFrame:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(p, dtype=str, keep_default_na=False)
    if list(frame.columns) != list(columns):
        raise ValueError(f"unexpected Phase 5E source schema in {p}")
    return frame


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _state_values(row: pd.Series, field: str) -> list[str]:
    raw = str(row.get(field) or "")
    if field in MULTI_STATE_FIELDS:
        return sorted(set(value for value in raw.split("|") if value)) or ["missing"]
    return [raw or "missing"]


def _field_signal(row: pd.Series, learned_tables: dict[str, object], field: str) -> float | None:
    table = dict(learned_tables.get(field) or {})
    weighted_sum = 0.0
    total_weight = 0
    for state in _state_values(row, field):
        summary = dict(table.get(state) or {})
        signal = _optional_float(summary.get("mean_signed_peer_excess"))
        try:
            support = int(summary.get("directional_N") or 0)
        except (TypeError, ValueError):
            support = 0
        if signal is None or support <= 0:
            continue
        weighted_sum += signal * support
        total_weight += support
    return float(weighted_sum / total_weight) if total_weight > 0 else None


def shadow_policy_decision(row: pd.Series, version: dict[str, object]) -> dict[str, object]:
    """Choose follow/invert using only frozen training-state reliability."""

    learned_tables = dict(version.get("learned_state_reliability") or {})
    signals: list[float] = []
    for field in (*SINGLE_STATE_FIELDS, *MULTI_STATE_FIELDS):
        signal = _field_signal(row, learned_tables, field)
        if signal is not None:
            signals.append(signal)

    if not signals:
        return {"multiplier": 1, "action": "follow", "reason": "no_empirical_state_signal", "fields_used": 0}

    combined = float(np.mean(np.asarray(signals, dtype=float)))
    if combined < 0.0:
        return {"multiplier": -1, "action": "invert", "reason": "negative_training_reliability", "fields_used": int(len(signals))}
    return {"multiplier": 1, "action": "follow", "reason": "nonnegative_training_reliability", "fields_used": int(len(signals))}


def _score_directional_frame(frame: pd.DataFrame, version: dict[str, object]) -> pd.DataFrame:
    columns = [
        "claim_id", "symbol", "as_of", "baseline_signed_peer_excess",
        "baseline_direction_hit", "adaptive_signed_peer_excess",
        "adaptive_direction_hit", "delta_signed_peer_excess",
        "delta_direction_hit", "adaptive_action", "adaptive_reason",
        "policy_fields_used",
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    work = frame.copy()
    work["_signed"] = pd.to_numeric(work["signed_peer_excess"], errors="coerce")
    work["_hit"] = pd.to_numeric(work["direction_hit"], errors="coerce")
    work = work.loc[work["_signed"].notna() & work["_hit"].notna()].copy()
    rows: list[dict[str, object]] = []
    for _, row in work.iterrows():
        decision = shadow_policy_decision(row, version)
        baseline_signed = float(row["_signed"])
        baseline_hit = int(float(row["_hit"]) > 0.0)
        adaptive_signed = baseline_signed * int(decision["multiplier"])
        adaptive_hit = int(adaptive_signed > 0.0)
        rows.append({
            "claim_id": str(row["claim_id"]),
            "symbol": str(row["symbol"]),
            "as_of": str(row["as_of"]),
            "baseline_signed_peer_excess": baseline_signed,
            "baseline_direction_hit": baseline_hit,
            "adaptive_signed_peer_excess": float(adaptive_signed),
            "adaptive_direction_hit": adaptive_hit,
            "delta_signed_peer_excess": float(adaptive_signed - baseline_signed),
            "delta_direction_hit": int(adaptive_hit - baseline_hit),
            "adaptive_action": str(decision["action"]),
            "adaptive_reason": str(decision["reason"]),
            "policy_fields_used": int(decision["fields_used"]),
        })
    return pd.DataFrame(rows, columns=columns)


def _concentration(scored: pd.DataFrame) -> dict[str, object]:
    if scored.empty:
        return {"max_symbol_share": None, "max_observation_date_share": None, "passed": False}
    n = float(len(scored))
    symbol_share = float(scored["symbol"].astype(str).value_counts().max() / n)
    dates = pd.to_datetime(scored["as_of"], errors="coerce").dt.normalize()
    date_share = float(dates.value_counts().max() / n) if dates.notna().any() else None
    passed = date_share is not None and symbol_share <= MAX_SYMBOL_SHARE and date_share <= MAX_OBSERVATION_DATE_SHARE
    return {
        "max_symbol_share": symbol_share,
        "max_observation_date_share": date_share,
        "maximum_symbol_share_allowed": MAX_SYMBOL_SHARE,
        "maximum_observation_date_share_allowed": MAX_OBSERVATION_DATE_SHARE,
        "passed": bool(passed),
    }


def _interval(values: list[float]) -> list[float] | None:
    if not values:
        return None
    low, high = np.quantile(np.asarray(values, dtype=float), [0.025, 0.975])
    return [float(low), float(high)]


def _paired_bootstrap(scored: pd.DataFrame, horizon: int, *, reps: int, seed: int) -> dict[str, object]:
    dates = _baseline_dates(scored)
    support_regions = _support_regions(scored, dates, horizon)
    block_length = max(1, 2 * int(horizon))
    diagnostics = {
        "block_length_sessions": block_length,
        "observation_date_count": int(len(dates)),
        "support_regions": int(support_regions),
        "bootstrap_reps": int(reps),
    }
    if scored.empty or len(dates) < 2 or support_regions < 2 or reps <= 0:
        return {
            **diagnostics,
            "delta_mean_signed_peer_excess_95": None,
            "delta_direction_hit_rate_95": None,
            "adaptive_mean_signed_peer_excess_95": None,
            "baseline_mean_signed_peer_excess_95": None,
        }

    work = scored.copy()
    work["_obs"] = pd.to_datetime(work["as_of"], errors="coerce").dt.normalize()
    by_day = {day: work.loc[work["_obs"].eq(day)].copy() for day in dates}
    span = min(block_length, len(dates))
    blocks = [[dates[(start + offset) % len(dates)] for offset in range(span)] for start in range(len(dates))]
    draws_per_rep = int(np.ceil(len(dates) / block_length))
    rng = np.random.default_rng(seed)
    delta_signed_means: list[float] = []
    delta_hit_means: list[float] = []
    adaptive_signed_means: list[float] = []
    baseline_signed_means: list[float] = []

    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for idx in chosen for day in blocks[idx]][: len(dates)]
        parts = [by_day[day] for day in sampled_dates if not by_day[day].empty]
        if not parts:
            continue
        sample = pd.concat(parts, ignore_index=True)
        delta_signed_means.append(float(sample["delta_signed_peer_excess"].mean()))
        delta_hit_means.append(float(sample["delta_direction_hit"].mean()))
        adaptive_signed_means.append(float(sample["adaptive_signed_peer_excess"].mean()))
        baseline_signed_means.append(float(sample["baseline_signed_peer_excess"].mean()))

    return {
        **diagnostics,
        "delta_mean_signed_peer_excess_95": _interval(delta_signed_means),
        "delta_direction_hit_rate_95": _interval(delta_hit_means),
        "adaptive_mean_signed_peer_excess_95": _interval(adaptive_signed_means),
        "baseline_mean_signed_peer_excess_95": _interval(baseline_signed_means),
    }


def _directional_summary(scored: pd.DataFrame) -> dict[str, object]:
    if scored.empty:
        return {
            "directional_N": 0, "symbols": 0, "observation_dates": 0,
            "baseline_direction_hit_rate": None, "adaptive_direction_hit_rate": None,
            "delta_direction_hit_rate": None, "baseline_mean_signed_peer_excess": None,
            "adaptive_mean_signed_peer_excess": None, "delta_mean_signed_peer_excess": None,
            "follow_N": 0, "invert_N": 0, "fallback_follow_N": 0,
        }
    baseline_hit = float(scored["baseline_direction_hit"].mean())
    adaptive_hit = float(scored["adaptive_direction_hit"].mean())
    baseline_signed = float(scored["baseline_signed_peer_excess"].mean())
    adaptive_signed = float(scored["adaptive_signed_peer_excess"].mean())
    return {
        "directional_N": int(len(scored)),
        "symbols": int(scored["symbol"].astype(str).nunique()),
        "observation_dates": int(pd.to_datetime(scored["as_of"], errors="coerce").nunique()),
        "baseline_direction_hit_rate": baseline_hit,
        "adaptive_direction_hit_rate": adaptive_hit,
        "delta_direction_hit_rate": float(adaptive_hit - baseline_hit),
        "baseline_mean_signed_peer_excess": baseline_signed,
        "adaptive_mean_signed_peer_excess": adaptive_signed,
        "delta_mean_signed_peer_excess": float(adaptive_signed - baseline_signed),
        "follow_N": int(scored["adaptive_action"].eq("follow").sum()),
        "invert_N": int(scored["adaptive_action"].eq("invert").sum()),
        "fallback_follow_N": int(scored["adaptive_reason"].eq("no_empirical_state_signal").sum()),
    }


def _verify_and_reconstruct_epoch(record: dict[str, object], versions_by_id: dict[str, dict[str, object]], claims: pd.DataFrame, outcomes: pd.DataFrame) -> tuple[dict[str, object], pd.DataFrame]:
    version = versions_by_id.get(str(record.get("version_id") or ""))
    successor = versions_by_id.get(str(record.get("successor_version_id") or ""))
    if version is None or successor is None:
        raise ValueError("Phase 5E evaluation references unknown Phase 5C version")
    if str(version.get("version_sha256") or "") != str(record.get("version_sha256") or ""):
        raise ValueError("Phase 5E evaluated version hash mismatch")
    if str(successor.get("version_sha256") or "") != str(record.get("successor_version_sha256") or ""):
        raise ValueError("Phase 5E successor version hash mismatch")

    horizon = int(record.get("horizon_sessions", -1))
    if horizon not in HORIZONS or int(version.get("horizon_sessions", -1)) != horizon or int(successor.get("horizon_sessions", -1)) != horizon:
        raise ValueError("Phase 5E horizon/version mismatch")
    if str(version.get("training_cutoff")) != str(record.get("training_cutoff")):
        raise ValueError("Phase 5E training cutoff mismatch")
    if str(successor.get("training_cutoff")) != str(record.get("evaluation_end_cutoff")):
        raise ValueError("Phase 5E successor cutoff mismatch")

    frame = _epoch_frame(claims, outcomes, horizon=horizon, training_cutoff=str(version["training_cutoff"]), evaluation_known_by=str(successor["training_cutoff"]))
    if frame.empty:
        raise ValueError("Phase 5E finalized evaluation reconstructed as empty")
    if int(record.get("evaluation_rows") or -1) != len(frame):
        raise ValueError("Phase 5E evaluation row-count mismatch")
    if _evaluation_fingerprint(frame) != str(record.get("evidence_fingerprint") or ""):
        raise ValueError("Phase 5E evaluation fingerprint mismatch")
    return version, frame


def _assess_epoch(record: dict[str, object], version: dict[str, object], frame: pd.DataFrame, *, bootstrap_reps: int, random_seed: int) -> tuple[dict[str, object], pd.DataFrame]:
    horizon = int(record["horizon_sessions"])
    scored = _score_directional_frame(frame, version)
    summary = _directional_summary(scored)
    robust = _paired_bootstrap(scored, horizon, reps=bootstrap_reps, seed=random_seed + horizon)
    concentration = _concentration(scored)
    robust_available = int(robust.get("support_regions") or 0) >= 2 and robust.get("delta_mean_signed_peer_excess_95") is not None
    return ({
        "evaluation_id": str(record["evaluation_id"]),
        "evaluation_sha256": str(record["evaluation_sha256"]),
        "version_id": str(record["version_id"]),
        "version_sha256": str(record["version_sha256"]),
        "successor_version_id": str(record["successor_version_id"]),
        "horizon_sessions": horizon,
        "evaluation_claim_generated_start": str(record["evaluation_claim_generated_start"]),
        "evaluation_claim_generated_end": str(record["evaluation_claim_generated_end"]),
        "directional": summary,
        "robust_paired_uncertainty": robust,
        "concentration": concentration,
        "promotion_evidence_eligible_by_support": bool(robust_available),
        "policy_version": POLICY_VERSION,
        "policy_sha256": POLICY_SHA256,
    }, scored)


def _aggregate_scored(parts: list[pd.DataFrame], horizon: int, *, bootstrap_reps: int, random_seed: int) -> dict[str, object]:
    combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=_score_directional_frame(pd.DataFrame(), {}).columns)
    return {
        "directional": _directional_summary(combined),
        "robust_paired_uncertainty": _paired_bootstrap(combined, horizon, reps=bootstrap_reps, seed=random_seed + horizon + 5000),
    }


def _lower_bound(interval: object) -> float | None:
    if not isinstance(interval, list) or len(interval) != 2:
        return None
    return _optional_float(interval[0])


def _promotion_for_horizon(assessments: list[dict[str, object]], robust_scored_parts: list[pd.DataFrame], horizon: int, *, bootstrap_reps: int, random_seed: int) -> dict[str, object]:
    robust_assessments = [item for item in assessments if bool(item.get("promotion_evidence_eligible_by_support"))]
    aggregate = _aggregate_scored(robust_scored_parts, horizon, bootstrap_reps=bootstrap_reps, random_seed=random_seed)
    directional = dict(aggregate["directional"])
    robust = dict(aggregate["robust_paired_uncertainty"])
    walkforward_evaluations = [{
        "version_id": str(item["version_id"]),
        "evaluation_start": str(item["evaluation_claim_generated_start"]),
        "evaluation_end": str(item["evaluation_claim_generated_end"]),
    } for item in robust_assessments]

    multiple = len(robust_assessments) >= MIN_PROMOTION_EPOCHS
    concentration_passed = multiple and all(bool(item["concentration"].get("passed")) for item in robust_assessments)
    temporal_stability = multiple and all((_optional_float(item["directional"].get("delta_mean_signed_peer_excess")) or 0.0) > 0.0 for item in robust_assessments)
    primary_low = _lower_bound(robust.get("delta_mean_signed_peer_excess_95"))
    hit_delta = _optional_float(directional.get("delta_direction_hit_rate"))
    baseline_advantage = bool(multiple and primary_low is not None and primary_low > 0.0 and hit_delta is not None and hit_delta >= 0.0)
    robust_available = bool(multiple and robust.get("delta_mean_signed_peer_excess_95") is not None and robust.get("delta_direction_hit_rate_95") is not None)

    generic = promotion_assessment(
        walkforward_evaluations=walkforward_evaluations,
        pit_leakage_audit_passed=bool(assessments),
        reproducible_versions=bool(assessments),
        robust_uncertainty_available=robust_available,
        concentration_check_passed=concentration_passed,
        temporal_stability_check_passed=temporal_stability,
        baseline_advantage_demonstrated=baseline_advantage,
    )
    gates = dict(generic["gates"])
    gates["minimum_robust_epochs_pre_registered"] = bool(multiple)
    return {
        "status": str(generic["status"]),
        "gates": gates,
        "robust_promotion_epochs": int(len(robust_assessments)),
        "aggregate_paired_evidence": aggregate,
        "policy_version": POLICY_VERSION,
        "policy_sha256": POLICY_SHA256,
        "production_change_performed": False,
    }


def run_phase5_completion(claims_path: str | Path, outcomes_path: str | Path, versions_path: str | Path, evaluations_path: str | Path, report_path: str | Path, *, bootstrap_reps: int = 500, random_seed: int = 20260925) -> dict[str, object]:
    claims = _read_csv(claims_path, CLAIM_COLUMNS_V2)
    outcomes = _read_csv(outcomes_path, OUTCOME_COLUMNS_V2)
    validate_v2_archives(claims, outcomes)
    versions = _read_versions(versions_path)
    evaluations = _read_evaluations(evaluations_path)
    versions_by_id = {str(version["version_id"]): version for version in versions}
    if len(versions_by_id) != len(versions):
        raise ValueError("duplicate Phase 5C version_id in Phase 5E")

    assessments_by_horizon = {int(horizon): [] for horizon in HORIZONS}
    robust_scored_by_horizon = {int(horizon): [] for horizon in HORIZONS}
    for record in evaluations:
        version, frame = _verify_and_reconstruct_epoch(record, versions_by_id, claims, outcomes)
        assessment, scored = _assess_epoch(record, version, frame, bootstrap_reps=bootstrap_reps, random_seed=random_seed)
        horizon = int(record["horizon_sessions"])
        assessments_by_horizon[horizon].append(assessment)
        if assessment["promotion_evidence_eligible_by_support"]:
            robust_scored_by_horizon[horizon].append(scored)

    horizon_reports: dict[str, object] = {}
    any_eligible = False
    for horizon in HORIZONS:
        assessments = sorted(assessments_by_horizon[int(horizon)], key=lambda item: (str(item["evaluation_claim_generated_start"]), str(item["evaluation_claim_generated_end"]), str(item["version_id"])))
        promotion = _promotion_for_horizon(assessments, robust_scored_by_horizon[int(horizon)], int(horizon), bootstrap_reps=bootstrap_reps, random_seed=random_seed)
        any_eligible = any_eligible or promotion["status"] == "eligible_for_separate_promotion_review"
        horizon_reports[str(horizon)] = {
            "finalized_phase5d_epochs": int(len(assessments)),
            "epoch_assessments": assessments,
            "promotion": promotion,
        }

    result = {
        "phase": "5E_adaptive_shadow_and_promotion",
        "schema_version": SCHEMA_VERSION,
        "status": "promotion_review_eligible_for_at_least_one_horizon" if any_eligible else "phase5_engineering_complete_collecting_evidence",
        "technical_phase5_complete": True,
        "claims": int(len(claims)),
        "mature_outcomes": int(len(outcomes)),
        "phase5c_versions": int(len(versions)),
        "phase5d_finalized_evaluations": int(len(evaluations)),
        "policy": {"version": POLICY_VERSION, "sha256": POLICY_SHA256, "contract": POLICY_CONTRACT},
        "horizons": horizon_reports,
        "semantics": {
            "research_only": True,
            "adaptive_shadow_inference_active": True,
            "promotion_is_horizon_specific": True,
            "latest_provisional_phase5d_epoch_counts_as_promotion_evidence": False,
            "state_names_assumed_ordinal": False,
            "raw_scanner_score_used_for_shadow_policy": False,
            "evaluation_outcome_used_to_choose_shadow_action": False,
            "shorter_horizon_labels_borrowed": False,
            "production_confidence_changed": False,
            "adaptive_production_weights_created": False,
            "portfolio_or_depot_watch_changed": False,
            "production_change_performed": False,
            "eligible_status_requires_separate_promotion_review": True,
        },
    }
    output = Path(report_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
    return result
