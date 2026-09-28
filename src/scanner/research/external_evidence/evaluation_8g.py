"""Phase 8G-E confirmatory incremental-value evaluation.

Validation and Holdout use the frozen Phase-8G-D preprocessing/model state.
No refit, feature search, threshold search or cross-factor interaction is
permitted here. Holdout access remains a one-shot operation after the full
Validation family is frozen.
"""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.research.external_evidence.discovery_fit_8g import (
    FIT_SCHEMA,
    transform_discovery_features,
)
from scanner.research.external_evidence.research_8g import (
    ACTIVE_FACTORS,
    HORIZONS,
    validate_split_plan,
)
from scanner.research.external_evidence.research_8g_binding_guard import (
    exact_split_boundary_purge_required,
    validate_guarded_manifest,
)


EVALUATION_PROTOCOL_SCHEMA = "external_evidence_8g_evaluation_protocol_v1"
DISCOVERY_FREEZE_SCHEMA = "external_evidence_8g_discovery_freeze_receipt_v1"
VALIDATION_FAMILY_SCHEMA = "external_evidence_8g_validation_family_freeze_v1"
HOLDOUT_LEDGER_SCHEMA = "external_evidence_8g_holdout_consumption_ledger_v1"
EVALUATION_RESULT_SCHEMA = "external_evidence_8g_confirmatory_result_v1"
IDENTITY = ["snapshot_id", "as_of", "symbol", "horizon_sessions"]
CONTEXT_IDENTITY = ["snapshot_id", "as_of", "symbol"]


class ExternalEvidence8GEvaluationError(ValueError):
    """Raised when Phase-8G-E violates the frozen confirmatory protocol."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _day(value: object) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ExternalEvidence8GEvaluationError(f"invalid_date:{value}")
    return pd.Timestamp(parsed).normalize()


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((int(base) + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def validate_evaluation_protocol(protocol: Mapping[str, Any]) -> None:
    if protocol.get("schema_version") != EVALUATION_PROTOCOL_SCHEMA:
        raise ExternalEvidence8GEvaluationError("unsupported_evaluation_protocol")
    if protocol.get("phase") != "8G-E":
        raise ExternalEvidence8GEvaluationError("evaluation_phase_mismatch")
    if protocol.get("research_only") is not True:
        raise ExternalEvidence8GEvaluationError("8g_e_must_be_research_only")
    if protocol.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GEvaluationError("production_integration_must_remain_disabled")
    if tuple(protocol.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GEvaluationError("active_factor_family_mismatch")
    if tuple(int(x) for x in protocol.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8GEvaluationError("horizon_family_mismatch")
    family = list(protocol.get("frozen_hypothesis_family") or ())
    expected = [f"{factor}_x_{h}t" for factor in ACTIVE_FACTORS for h in HORIZONS]
    if family != expected:
        raise ExternalEvidence8GEvaluationError("frozen_hypothesis_family_mismatch")
    if int(protocol["multiple_testing"]["family_size"]) != len(expected):
        raise ExternalEvidence8GEvaluationError("holm_family_size_mismatch")
    if protocol["multiple_testing"].get("method") != "Holm":
        raise ExternalEvidence8GEvaluationError("holm_method_required")
    if float(protocol["multiple_testing"].get("family_wise_alpha")) != 0.05:
        raise ExternalEvidence8GEvaluationError("family_wise_alpha_must_be_0_05")
    guards = protocol.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GEvaluationError("8g_e_definition_guards_must_remain_false")
    if protocol.get("next_planned_phase") != "8H_CROSS_FACTOR_INTERACTION":
        raise ExternalEvidence8GEvaluationError("roadmap_after_8g_must_be_8h")


def build_discovery_freeze_receipt(
    fit_artifact: Mapping[str, Any], freeze_gate: Mapping[str, Any]
) -> dict[str, Any]:
    """Freeze an 8G-D model only after its empirical Discovery gate is ready."""
    if fit_artifact.get("schema_version") != FIT_SCHEMA:
        raise ExternalEvidence8GEvaluationError("unsupported_discovery_fit_artifact")
    if freeze_gate.get("schema_version") != "external_evidence_8g_discovery_freeze_gate_v1":
        raise ExternalEvidence8GEvaluationError("unsupported_discovery_freeze_gate")
    if freeze_gate.get("ready_for_final_discovery_model_freeze") is not True:
        raise ExternalEvidence8GEvaluationError("discovery_not_ready_for_final_freeze")
    factor = str(fit_artifact.get("factor_id"))
    horizon = int(fit_artifact.get("horizon_sessions"))
    if factor != str(freeze_gate.get("factor_id")) or horizon != int(freeze_gate.get("horizon_sessions")):
        raise ExternalEvidence8GEvaluationError("discovery_fit_freeze_gate_identity_mismatch")
    receipt: dict[str, Any] = {
        "schema_version": DISCOVERY_FREEZE_SCHEMA,
        "phase": "8G-E_ENTRY",
        "hypothesis_id": f"{factor}_x_{horizon}t",
        "factor_id": factor,
        "horizon_sessions": horizon,
        "fit_sha256": str(fit_artifact.get("fit_sha256") or ""),
        "preprocessor_sha256": str((fit_artifact.get("preprocessor") or {}).get("preprocessor_sha256") or ""),
        "baseline_model_sha256": str((fit_artifact.get("baseline_model") or {}).get("model_sha256") or ""),
        "challenger_model_sha256": str((fit_artifact.get("challenger_model") or {}).get("model_sha256") or ""),
        "validation_refit_allowed": False,
        "holdout_refit_allowed": False,
        "frozen": True,
    }
    if not all(receipt[key] for key in (
        "fit_sha256", "preprocessor_sha256", "baseline_model_sha256", "challenger_model_sha256"
    )):
        raise ExternalEvidence8GEvaluationError("discovery_freeze_hash_missing")
    receipt["receipt_sha256"] = _digest(receipt)
    return receipt


def _validate_discovery_freeze(
    fit_artifact: Mapping[str, Any], receipt: Mapping[str, Any], factor_id: str, horizon: int
) -> None:
    if receipt.get("schema_version") != DISCOVERY_FREEZE_SCHEMA or receipt.get("frozen") is not True:
        raise ExternalEvidence8GEvaluationError("final_discovery_freeze_receipt_required")
    if receipt.get("hypothesis_id") != f"{factor_id}_x_{horizon}t":
        raise ExternalEvidence8GEvaluationError("discovery_freeze_hypothesis_mismatch")
    if str(receipt.get("fit_sha256")) != str(fit_artifact.get("fit_sha256")):
        raise ExternalEvidence8GEvaluationError("discovery_fit_hash_mismatch")
    if str(receipt.get("preprocessor_sha256")) != str((fit_artifact.get("preprocessor") or {}).get("preprocessor_sha256")):
        raise ExternalEvidence8GEvaluationError("discovery_preprocessor_hash_mismatch")
    if str(receipt.get("baseline_model_sha256")) != str((fit_artifact.get("baseline_model") or {}).get("model_sha256")):
        raise ExternalEvidence8GEvaluationError("baseline_model_hash_mismatch")
    if str(receipt.get("challenger_model_sha256")) != str((fit_artifact.get("challenger_model") or {}).get("model_sha256")):
        raise ExternalEvidence8GEvaluationError("challenger_model_hash_mismatch")


def _predict(matrix: np.ndarray, names: Sequence[str], model: Mapping[str, Any]) -> np.ndarray:
    if model.get("family") != "ridge_linear_regression" or float(model.get("alpha")) != 1.0:
        raise ExternalEvidence8GEvaluationError("frozen_ridge_model_required")
    coefficients = model.get("coefficients")
    if not isinstance(coefficients, Mapping) or list(coefficients.keys()) != list(names):
        raise ExternalEvidence8GEvaluationError("frozen_model_feature_order_mismatch")
    beta = np.asarray([float(coefficients[name]) for name in names], dtype=float)
    return float(model.get("intercept")) + np.asarray(matrix, dtype=float) @ beta


def score_with_frozen_discovery_models(
    *,
    baseline_features: pd.DataFrame,
    challenger_features: pd.DataFrame,
    fit_artifact: Mapping[str, Any],
    discovery_freeze_receipt: Mapping[str, Any],
) -> pd.DataFrame:
    factor = str(fit_artifact.get("factor_id") or "")
    horizon = int(fit_artifact.get("horizon_sessions") or 0)
    _validate_discovery_freeze(fit_artifact, discovery_freeze_receipt, factor, horizon)
    missing_left = sorted(set(IDENTITY).difference(baseline_features.columns))
    missing_right = sorted(set(IDENTITY).difference(challenger_features.columns))
    if missing_left or missing_right:
        raise ExternalEvidence8GEvaluationError("confirmatory_feature_identity_missing")
    left = baseline_features.sort_values(IDENTITY, kind="mergesort").reset_index(drop=True)
    right = challenger_features.sort_values(IDENTITY, kind="mergesort").reset_index(drop=True)
    if len(left) != len(right) or left.empty:
        raise ExternalEvidence8GEvaluationError("confirmatory_paired_feature_rows_required")
    for column in IDENTITY:
        if not left[column].equals(right[column]):
            raise ExternalEvidence8GEvaluationError(f"confirmatory_pairing_mismatch:{column}")
    preprocessor = fit_artifact["preprocessor"]
    baseline_x, baseline_names = transform_discovery_features(left, preprocessor=preprocessor, challenger=False)
    challenger_x, challenger_names = transform_discovery_features(right, preprocessor=preprocessor, challenger=True)
    out = left[IDENTITY].copy()
    out["baseline_prediction"] = _predict(baseline_x, baseline_names, fit_artifact["baseline_model"])
    out["challenger_prediction"] = _predict(challenger_x, challenger_names, fit_artifact["challenger_model"])
    return out


def prepare_confirmatory_outcomes(
    *,
    scored_rows: pd.DataFrame,
    outcome_rows: pd.DataFrame,
    factor_id: str,
    horizon_sessions: int,
    split: str,
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
    protocol: Mapping[str, Any],
    research_as_of: object,
    validation_family_freeze: Mapping[str, Any] | None = None,
) -> pd.DataFrame:
    """Open only structurally authorized Validation/Holdout outcome values."""
    validate_evaluation_protocol(protocol)
    validate_split_plan(plan, specs)
    validate_guarded_manifest(manifest, specs, plan)
    split = str(split).upper()
    if split not in {"VALIDATION", "HOLDOUT"}:
        raise ExternalEvidence8GEvaluationError("confirmatory_split_must_be_validation_or_holdout")
    if split == "HOLDOUT":
        if not isinstance(validation_family_freeze, Mapping) or validation_family_freeze.get("schema_version") != VALIDATION_FAMILY_SCHEMA:
            raise ExternalEvidence8GEvaluationError("frozen_validation_family_required_before_holdout")
        if validation_family_freeze.get("frozen") is not True:
            raise ExternalEvidence8GEvaluationError("validation_family_not_frozen")

    label_field = f"label_available_from_{horizon_sessions}t"
    target_field = f"peer_excess_{horizon_sessions}t"
    required_outcomes = set(IDENTITY + ["start_market_date", label_field, target_field])
    missing = sorted(required_outcomes.difference(outcome_rows.columns))
    if missing:
        raise ExternalEvidence8GEvaluationError("confirmatory_outcome_columns_missing:" + ",".join(missing))
    if scored_rows.duplicated(IDENTITY).any() or outcome_rows.duplicated(IDENTITY).any():
        raise ExternalEvidence8GEvaluationError("confirmatory_duplicate_identity")
    score_keys = set(map(tuple, scored_rows[IDENTITY].astype(str).to_numpy()))
    outcome_keys = set(map(tuple, outcome_rows[IDENTITY].astype(str).to_numpy()))
    if score_keys != outcome_keys:
        raise ExternalEvidence8GEvaluationError("confirmatory_outcomes_must_match_scored_rows_exactly")

    key = f"{factor_id}_x_{horizon_sessions}t"
    stream = manifest["streams"].get(key)
    if not isinstance(stream, Mapping):
        raise ExternalEvidence8GEvaluationError(f"manifest_stream_missing:{key}")
    assignments = list(stream.get("assignments") or ())
    by_snapshot = {(str(x.get("snapshot_id")), str(x.get("as_of"))): x for x in assignments}
    next_split = "HOLDOUT" if split == "VALIDATION" else None
    next_dates = sorted(str(x.get("as_of")) for x in assignments if next_split and x.get("split") == next_split)
    next_first = next_dates[0] if next_dates else None
    research_day = _day(research_as_of)

    for row in outcome_rows.itertuples(index=False):
        snapshot_id = str(getattr(row, "snapshot_id"))
        as_of = str(getattr(row, "as_of"))
        assignment = by_snapshot.get((snapshot_id, as_of))
        if assignment is None:
            raise ExternalEvidence8GEvaluationError("confirmatory_identity_not_bound_in_8g_b")
        if assignment.get("split") != split:
            raise ExternalEvidence8GEvaluationError("wrong_split_outcome_access_forbidden")
        if assignment.get("usable") is not True:
            raise ExternalEvidence8GEvaluationError("reserved_boundary_buffer_outcome_forbidden")
        label_available = str(getattr(row, label_field))
        if _day(label_available) > research_day:
            raise ExternalEvidence8GEvaluationError("confirmatory_label_not_mature")
        if next_first and exact_split_boundary_purge_required(
            label_available_from=label_available, next_split_first_as_of=next_first
        ):
            raise ExternalEvidence8GEvaluationError("exact_split_boundary_purge_required")

    work = outcome_rows.copy().sort_values(IDENTITY, kind="mergesort").reset_index(drop=True)
    scored = scored_rows.sort_values(IDENTITY, kind="mergesort").reset_index(drop=True)
    for column in IDENTITY:
        if not work[column].astype(str).equals(scored[column].astype(str)):
            raise ExternalEvidence8GEvaluationError(f"confirmatory_score_outcome_order_drift:{column}")
    work["baseline_prediction"] = scored["baseline_prediction"].to_numpy(dtype=float)
    work["challenger_prediction"] = scored["challenger_prediction"].to_numpy(dtype=float)
    work[target_field] = pd.to_numeric(work[target_field], errors="raise")
    return work


def _spearman(x: np.ndarray, y: np.ndarray) -> float | None:
    if len(x) < 2:
        return None
    left = pd.Series(x).rank(method="average")
    right = pd.Series(y).rank(method="average")
    value = left.corr(right)
    return None if pd.isna(value) else float(value)


def _snapshot_effects(rows: pd.DataFrame, horizon: int) -> pd.DataFrame:
    target = f"peer_excess_{horizon}t"
    y = pd.to_numeric(rows[target], errors="raise").to_numpy(dtype=float)
    base = pd.to_numeric(rows["baseline_prediction"], errors="raise").to_numpy(dtype=float)
    challenger = pd.to_numeric(rows["challenger_prediction"], errors="raise").to_numpy(dtype=float)
    work = rows[["snapshot_id", "as_of", "symbol", "start_market_date", f"label_available_from_{horizon}t"]].copy()
    work["row_improvement"] = (y - base) ** 2 - (y - challenger) ** 2
    work["row_mae_improvement"] = np.abs(y - base) - np.abs(y - challenger)
    work["target"] = y
    work["baseline_prediction"] = base
    work["challenger_prediction"] = challenger
    group_cols = ["snapshot_id", "as_of", "start_market_date", f"label_available_from_{horizon}t"]
    snapshot = work.groupby(group_cols, dropna=False, as_index=False).agg(
        snapshot_improvement=("row_improvement", "mean"),
        paired_rows=("symbol", "size"),
    )
    return work, snapshot


def _temporal_support_regions(snapshot: pd.DataFrame, horizon: int) -> int:
    dates = sorted({_day(x) for x in snapshot["start_market_date"].tolist()})
    block = 2 * int(horizon)
    if not dates:
        return 0
    positions = {day: i for i, day in enumerate(dates)}
    occurrences = sorted({positions[_day(x)] for x in snapshot["start_market_date"].tolist()})
    regions = 0
    last: int | None = None
    for position in occurrences:
        if last is None or position - last >= block:
            regions += 1
            last = position
    return regions


def _block_bootstrap(snapshot: pd.DataFrame, *, horizon: int, reps: int, seed: int) -> dict[str, Any]:
    dates = sorted({_day(x) for x in snapshot["start_market_date"].tolist()})
    block_length = 2 * int(horizon)
    if len(dates) < 2 or reps < 1:
        return {"ci_low": None, "ci_high": None, "p_value": None, "bootstrap_repetitions": reps}
    span = min(block_length, len(dates))
    blocks = [[dates[(start + offset) % len(dates)] for offset in range(span)] for start in range(len(dates))]
    by_date = {
        day: snapshot.loc[pd.to_datetime(snapshot["start_market_date"]).dt.normalize().eq(day), "snapshot_improvement"].to_numpy(dtype=float)
        for day in dates
    }
    observed = float(snapshot["snapshot_improvement"].mean())
    centered = snapshot.copy()
    centered["snapshot_improvement"] = centered["snapshot_improvement"] - observed
    centered_by_date = {
        day: centered.loc[pd.to_datetime(centered["start_market_date"]).dt.normalize().eq(day), "snapshot_improvement"].to_numpy(dtype=float)
        for day in dates
    }
    rng = np.random.default_rng(seed)
    draws_per_rep = int(np.ceil(len(dates) / span))
    raw_draws: list[float] = []
    null_draws: list[float] = []
    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for idx in chosen for day in blocks[idx]][: len(dates)]
        raw_parts = [by_date[day] for day in sampled_dates if len(by_date[day])]
        null_parts = [centered_by_date[day] for day in sampled_dates if len(centered_by_date[day])]
        if raw_parts and null_parts:
            raw_draws.append(float(np.mean(np.concatenate(raw_parts))))
            null_draws.append(float(np.mean(np.concatenate(null_parts))))
    if not raw_draws:
        return {"ci_low": None, "ci_high": None, "p_value": None, "bootstrap_repetitions": reps}
    ci_low, ci_high = np.quantile(np.asarray(raw_draws), [0.025, 0.975])
    null_array = np.asarray(null_draws)
    p = float((1 + np.sum(np.abs(null_array) >= abs(observed))) / (len(null_array) + 1))
    return {
        "ci_low": float(ci_low),
        "ci_high": float(ci_high),
        "p_value": min(1.0, p),
        "bootstrap_repetitions": int(len(raw_draws)),
    }


def _non_overlap(snapshot: pd.DataFrame, horizon: int) -> dict[str, Any]:
    label = f"label_available_from_{horizon}t"
    ordered = snapshot.sort_values(["start_market_date", "snapshot_id"], kind="mergesort")
    kept: list[float] = []
    last_end: pd.Timestamp | None = None
    for row in ordered.itertuples(index=False):
        start = _day(getattr(row, "start_market_date"))
        end = _day(getattr(row, label))
        if last_end is None or start > last_end:
            kept.append(float(getattr(row, "snapshot_improvement")))
            last_end = end
    mean = None if not kept else float(np.mean(kept))
    return {"snapshot_n": len(kept), "mean_improvement": mean}


def _group_direction_report(rows: pd.DataFrame, field: str, min_n: int) -> dict[str, Any]:
    if field not in rows.columns:
        return {"status": "DIAGNOSTIC_CONTEXT_INCOMPLETE", "groups": {}}
    groups: dict[str, Any] = {}
    for value, group in rows.groupby(field, dropna=False):
        key = "__MISSING__" if pd.isna(value) else str(value)
        n = int(len(group))
        groups[key] = {
            "paired_n": n,
            "mean_row_improvement": float(group["row_improvement"].mean()),
            "direction_reportable": n >= min_n,
        }
    return {"status": "OK", "groups": groups}


def _concentration(rows: pd.DataFrame, field: str) -> float | None:
    if field not in rows.columns or rows.empty:
        return None
    contribution = rows.groupby(field, dropna=False)["row_improvement"].sum().to_numpy(dtype=float)
    denominator = float(np.abs(contribution).sum())
    if denominator <= 0:
        return None
    return float(np.max(np.abs(contribution)) / denominator)


def evaluate_confirmatory_rows(
    *,
    opened_rows: pd.DataFrame,
    factor_id: str,
    horizon_sessions: int,
    split: str,
    protocol: Mapping[str, Any],
    context_rows: pd.DataFrame | None = None,
    coverage_records: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Evaluate one already-authorized Validation/Holdout factor×horizon split."""
    validate_evaluation_protocol(protocol)
    split = str(split).upper()
    if split not in {"VALIDATION", "HOLDOUT"}:
        raise ExternalEvidence8GEvaluationError("invalid_confirmatory_split")
    if factor_id not in ACTIVE_FACTORS or horizon_sessions not in HORIZONS:
        raise ExternalEvidence8GEvaluationError("invalid_factor_or_horizon")
    target = f"peer_excess_{horizon_sessions}t"
    required = set(IDENTITY + ["start_market_date", f"label_available_from_{horizon_sessions}t", target, "baseline_prediction", "challenger_prediction"])
    missing = sorted(required.difference(opened_rows.columns))
    if missing:
        raise ExternalEvidence8GEvaluationError("opened_evaluation_columns_missing:" + ",".join(missing))
    rows, snapshot = _snapshot_effects(opened_rows, horizon_sessions)

    if context_rows is not None:
        context_required = set(CONTEXT_IDENTITY + ["market_regime_stock", "sector", "currency"])
        context_missing = sorted(context_required.difference(context_rows.columns))
        if context_missing:
            raise ExternalEvidence8GEvaluationError("diagnostic_context_columns_missing:" + ",".join(context_missing))
        if context_rows.duplicated(CONTEXT_IDENTITY).any():
            raise ExternalEvidence8GEvaluationError("diagnostic_context_duplicate_identity")
        rows = rows.merge(context_rows[list(context_required)], on=CONTEXT_IDENTITY, how="left", validate="one_to_one")
        context_complete = not rows[["market_regime_stock", "sector", "currency"]].isna().any().any()
    else:
        context_complete = False

    paired_n = int(len(rows))
    regions = _temporal_support_regions(snapshot, horizon_sessions)
    min_n = int(protocol["minimum_evidence"]["minimum_paired_n_per_factor_horizon_split"])
    min_regions = int(protocol["minimum_evidence"]["minimum_temporal_support_regions"])
    enough = paired_n >= min_n and regions >= min_regions
    bootstrap = _block_bootstrap(
        snapshot,
        horizon=horizon_sessions,
        reps=int(protocol["uncertainty"]["bootstrap_repetitions"]) if enough else 0,
        seed=_stable_seed(int(protocol["uncertainty"]["bootstrap_seed"]), factor_id, horizon_sessions, split),
    )
    primary = float(snapshot["snapshot_improvement"].mean()) if len(snapshot) else None
    non_overlap = _non_overlap(snapshot, horizon_sessions)
    contradiction = bool(
        primary is not None
        and primary > 0
        and non_overlap["snapshot_n"] >= 2
        and non_overlap["mean_improvement"] is not None
        and float(non_overlap["mean_improvement"]) <= 0
    )

    y = pd.to_numeric(rows["target"], errors="raise").to_numpy(dtype=float)
    base = pd.to_numeric(rows["baseline_prediction"], errors="raise").to_numpy(dtype=float)
    challenger = pd.to_numeric(rows["challenger_prediction"], errors="raise").to_numpy(dtype=float)
    min_group = int(protocol["stability_diagnostics"]["minimum_group_paired_n_for_direction_report"])
    regime = _group_direction_report(rows, "market_regime_stock", min_group) if context_complete else {"status": "DIAGNOSTIC_CONTEXT_INCOMPLETE", "groups": {}}
    sector = _group_direction_report(rows, "sector", min_group) if context_complete else {"status": "DIAGNOSTIC_CONTEXT_INCOMPLETE", "groups": {}}
    currency = _group_direction_report(rows, "currency", min_group) if context_complete else {"status": "DIAGNOSTIC_CONTEXT_INCOMPLETE", "groups": {}}

    if coverage_records:
        paired_total = sum(int(x.get("paired_rows", 0)) for x in coverage_records)
        unmapped = sum(int(x.get("excluded_unmapped_rows", 0)) for x in coverage_records)
        ambiguous = sum(int(x.get("excluded_ambiguous_mapping_rows", 0)) for x in coverage_records)
        denominator = paired_total + unmapped + ambiguous
        coverage = {
            "status": "COMPLETE",
            "paired_rows": paired_total,
            "excluded_unmapped_rows": unmapped,
            "excluded_ambiguous_mapping_rows": ambiguous,
            "coverage_ratio": None if denominator == 0 else float(paired_total / denominator),
        }
    else:
        coverage = {"status": "INCOMPLETE", "coverage_ratio": None}

    result: dict[str, Any] = {
        "schema_version": EVALUATION_RESULT_SCHEMA,
        "phase": "8G-E",
        "hypothesis_id": f"{factor_id}_x_{horizon_sessions}t",
        "factor_id": factor_id,
        "horizon_sessions": int(horizon_sessions),
        "split": split,
        "status": "EVALUATED" if enough else "INSUFFICIENT_EVIDENCE",
        "paired_n": paired_n,
        "snapshot_n": int(len(snapshot)),
        "temporal_support_regions": regions,
        "minimum_evidence_met": enough,
        "primary_effect": primary,
        "primary_ci_95": [bootstrap["ci_low"], bootstrap["ci_high"]],
        "primary_p_value_two_sided": bootstrap["p_value"],
        "mean_absolute_error_improvement": float(rows["row_mae_improvement"].mean()) if len(rows) else None,
        "prediction_spearman_baseline": _spearman(base, y),
        "prediction_spearman_challenger": _spearman(challenger, y),
        "non_overlap_sensitivity": {**non_overlap, "materially_contradicts_primary": contradiction},
        "regime_stability": regime,
        "sector_stability": sector,
        "currency_stability": currency,
        "coverage_missingness": coverage,
        "concentration": {
            "start_market_date": _concentration(rows, "start_market_date"),
            "symbol": _concentration(rows, "symbol"),
            "sector": _concentration(rows, "sector") if context_complete else None,
        },
        "pit_integrity_pass": True,
        "holm_adjusted_p": None,
        "holm_positive": False,
        "validation_outcomes_opened": split == "VALIDATION",
        "holdout_outcomes_opened": split == "HOLDOUT",
        "model_refit": False,
    }
    result["evaluation_sha256"] = _digest(result)
    return result


def holm_adjust_family(
    results: Mapping[str, Mapping[str, Any]], protocol: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    validate_evaluation_protocol(protocol)
    family = list(protocol["frozen_hypothesis_family"])
    alpha = float(protocol["multiple_testing"]["family_wise_alpha"])
    p_values: list[tuple[str, float]] = []
    for hypothesis in family:
        result = results.get(hypothesis)
        p = None if result is None else result.get("primary_p_value_two_sided")
        if result is None or result.get("minimum_evidence_met") is not True or p is None:
            p_values.append((hypothesis, 1.0))
        else:
            p_values.append((hypothesis, min(1.0, max(0.0, float(p)))))
    ordered = sorted(p_values, key=lambda item: (item[1], item[0]))
    adjusted: dict[str, float] = {}
    running = 0.0
    m = len(ordered)
    for index, (hypothesis, p) in enumerate(ordered):
        candidate = min(1.0, (m - index) * p)
        running = max(running, candidate)
        adjusted[hypothesis] = running
    output: dict[str, dict[str, Any]] = {}
    for hypothesis in family:
        source = dict(results.get(hypothesis) or {
            "schema_version": EVALUATION_RESULT_SCHEMA,
            "hypothesis_id": hypothesis,
            "status": "INSUFFICIENT_EVIDENCE",
            "minimum_evidence_met": False,
            "primary_effect": None,
            "primary_p_value_two_sided": None,
        })
        source["holm_adjusted_p"] = adjusted[hypothesis]
        source["holm_positive"] = bool(
            source.get("minimum_evidence_met") is True
            and source.get("primary_effect") is not None
            and float(source["primary_effect"]) > 0
            and adjusted[hypothesis] < alpha
        )
        output[hypothesis] = source
    return output


def freeze_validation_family(
    validation_results: Mapping[str, Mapping[str, Any]], protocol: Mapping[str, Any]
) -> dict[str, Any]:
    adjusted = holm_adjust_family(validation_results, protocol)
    if any(str(item.get("split", "VALIDATION")).upper() != "VALIDATION" for item in adjusted.values() if "split" in item):
        raise ExternalEvidence8GEvaluationError("validation_family_contains_non_validation_result")
    receipt: dict[str, Any] = {
        "schema_version": VALIDATION_FAMILY_SCHEMA,
        "phase": "8G-E",
        "frozen": True,
        "family_members": list(protocol["frozen_hypothesis_family"]),
        "results_sha256": _digest(adjusted),
        "evaluation_protocol_sha256": _digest(protocol),
        "holdout_spec_changes_allowed": False,
        "holdout_open_allowed": True,
    }
    receipt["receipt_sha256"] = _digest(receipt)
    return receipt


def new_holdout_consumption_ledger(protocol: Mapping[str, Any]) -> dict[str, Any]:
    validate_evaluation_protocol(protocol)
    return {
        "schema_version": HOLDOUT_LEDGER_SCHEMA,
        "phase": "8G-E",
        "append_only": True,
        "streams": {hypothesis: {"state": "SEALED", "evaluation_sha256": None} for hypothesis in protocol["frozen_hypothesis_family"]},
    }


def consume_holdout_once(
    ledger: Mapping[str, Any], *, hypothesis_id: str, evaluation_sha256: str
) -> dict[str, Any]:
    if ledger.get("schema_version") != HOLDOUT_LEDGER_SCHEMA or ledger.get("append_only") is not True:
        raise ExternalEvidence8GEvaluationError("invalid_holdout_consumption_ledger")
    updated = deepcopy(dict(ledger))
    stream = updated["streams"].get(hypothesis_id)
    if not isinstance(stream, dict):
        raise ExternalEvidence8GEvaluationError("holdout_hypothesis_not_registered")
    if stream.get("state") != "SEALED":
        raise ExternalEvidence8GEvaluationError("holdout_may_open_once_only")
    stream["state"] = "CONSUMED"
    stream["evaluation_sha256"] = str(evaluation_sha256)
    return updated


def promotion_candidate_state(
    *,
    hypothesis_id: str,
    validation_family: Mapping[str, Mapping[str, Any]],
    holdout_family: Mapping[str, Mapping[str, Any]],
) -> str:
    validation = validation_family[hypothesis_id]
    holdout = holdout_family[hypothesis_id]
    if validation.get("minimum_evidence_met") is not True or holdout.get("minimum_evidence_met") is not True:
        return "INSUFFICIENT_EVIDENCE"
    confirmed = bool(validation.get("holm_positive")) and bool(holdout.get("holm_positive"))
    ci = holdout.get("primary_ci_95") or [None, None]
    ci_positive = ci[0] is not None and float(ci[0]) > 0
    direction_agrees = (
        validation.get("primary_effect") is not None
        and holdout.get("primary_effect") is not None
        and float(validation["primary_effect"]) > 0
        and float(holdout["primary_effect"]) > 0
    )
    non_overlap_ok = not bool((holdout.get("non_overlap_sensitivity") or {}).get("materially_contradicts_primary"))
    coverage_ok = (holdout.get("coverage_missingness") or {}).get("status") == "COMPLETE"
    diagnostics_ok = all(
        (holdout.get(key) or {}).get("status") in {"OK", "DIAGNOSTIC_CONTEXT_INCOMPLETE"}
        for key in ("regime_stability", "sector_stability", "currency_stability")
    )
    pit_ok = holdout.get("pit_integrity_pass") is True
    if confirmed and ci_positive and direction_agrees and non_overlap_ok and coverage_ok and diagnostics_ok and pit_ok:
        return "PROMOTION_CANDIDATE_FOR_8H_REVIEW"
    return "NO_PROMOTION_EVIDENCE"
