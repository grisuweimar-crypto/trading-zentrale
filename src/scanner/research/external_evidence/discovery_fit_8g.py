"""Phase 8G-D deterministic Discovery preprocessing and Ridge fitting.

All feature-side preprocessing is fitted before any Discovery outcome value is
opened. The frozen Phase-8G-B/C feature family, transform family and Ridge
hyperparameters are not searched or tuned here.
"""
from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Mapping

import numpy as np
import pandas as pd

from scanner.research.external_evidence.discovery_8g import (
    prepare_discovery_outcome_rows,
    validate_discovery_protocol,
)
from scanner.research.external_evidence.model_8g import (
    assert_feature_frame_outcome_blind,
    resolved_baseline_columns,
)
from scanner.research.external_evidence.research_8g import (
    ACTIVE_FACTORS,
    HORIZONS,
)
from scanner.research.external_evidence.research_8g_binding_guard import (
    validate_guarded_manifest,
)


PREPROCESSOR_SCHEMA = "external_evidence_8g_discovery_preprocessor_v1"
FIT_SCHEMA = "external_evidence_8g_discovery_fit_v1"
IDENTITY = ["snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions"]
OUTCOME_IDENTITY = ["snapshot_id", "as_of", "symbol", "horizon_sessions"]
MISSING_CATEGORY = "__MISSING__"
ZERO_TOL = 1e-12


class ExternalEvidence8GDiscoveryFitError(ValueError):
    """Raised when deterministic Discovery fitting violates the frozen design."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _cat(value: object) -> str:
    try:
        if pd.isna(value):
            return MISSING_CATEGORY
    except (TypeError, ValueError):
        pass
    if isinstance(value, (bool, np.bool_)):
        return "true" if bool(value) else "false"
    return str(value)


def _sorted_pair(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
    baseline_feature_columns: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    assert_feature_frame_outcome_blind(baseline)
    assert_feature_frame_outcome_blind(challenger)
    missing_b = sorted(set(IDENTITY + baseline_feature_columns).difference(baseline.columns))
    missing_c = sorted(set(IDENTITY + baseline_feature_columns).difference(challenger.columns))
    if missing_b:
        raise ExternalEvidence8GDiscoveryFitError("baseline_columns_missing:" + ",".join(missing_b))
    if missing_c:
        raise ExternalEvidence8GDiscoveryFitError("challenger_columns_missing:" + ",".join(missing_c))
    if baseline.duplicated(IDENTITY).any() or challenger.duplicated(IDENTITY).any():
        raise ExternalEvidence8GDiscoveryFitError("duplicate_feature_identity")

    left = baseline.sort_values(IDENTITY, kind="mergesort").reset_index(drop=True)
    right = challenger.sort_values(IDENTITY, kind="mergesort").reset_index(drop=True)
    if len(left) != len(right):
        raise ExternalEvidence8GDiscoveryFitError("baseline_challenger_row_count_mismatch")
    for column in IDENTITY:
        if not left[column].equals(right[column]):
            raise ExternalEvidence8GDiscoveryFitError(f"baseline_challenger_identity_mismatch:{column}")
    for column in baseline_feature_columns:
        if not left[column].equals(right[column]):
            raise ExternalEvidence8GDiscoveryFitError(f"challenger_changed_baseline_feature:{column}")
    return left, right


def _fit_numeric(series: pd.Series, *, drop_if_zero_variance: bool) -> dict[str, Any]:
    values = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    if np.isinf(values).any():
        raise ExternalEvidence8GDiscoveryFitError("infinite_numeric_feature")
    missing = np.isnan(values)
    if missing.all():
        raise ExternalEvidence8GDiscoveryFitError("entirely_missing_numeric_feature")
    median = float(np.nanmedian(values))
    imputed = np.where(missing, median, values)
    mean = float(np.mean(imputed))
    std = float(np.std(imputed, ddof=0))
    zero_variance = bool(std <= ZERO_TOL)
    return {
        "median": median,
        "mean": mean,
        "std": std,
        "scale": 1.0 if zero_variance else std,
        "zero_variance": zero_variance,
        "dropped": bool(drop_if_zero_variance and zero_variance),
        "missing_indicator": True,
        "fit_n": int(len(values)),
        "missing_n": int(missing.sum()),
    }


def _fit_categories(series: pd.Series) -> list[str]:
    observed = {_cat(value) for value in series.tolist()}
    observed.add(MISSING_CATEGORY)
    return sorted(observed)


def fit_discovery_preprocessor(
    *,
    baseline_features: pd.DataFrame,
    challenger_features: pd.DataFrame,
    factor_id: str,
    horizon_sessions: int,
    specs: Mapping[str, Any],
    protocol: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    """Fit feature-only preprocessing before any label/outcome join."""
    validate_discovery_protocol(protocol, specs, plan)
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GDiscoveryFitError(f"factor_not_active:{factor_id}")
    if horizon_sessions not in HORIZONS:
        raise ExternalEvidence8GDiscoveryFitError(f"unsupported_horizon:{horizon_sessions}")
    numeric, categorical = resolved_baseline_columns(specs, horizon_sessions)
    baseline_columns = numeric + categorical
    left, right = _sorted_pair(baseline_features, challenger_features, baseline_columns)
    if left.empty:
        raise ExternalEvidence8GDiscoveryFitError("empty_discovery_feature_frame")
    horizons = {int(x) for x in left["horizon_sessions"].tolist()}
    if horizons != {horizon_sessions}:
        raise ExternalEvidence8GDiscoveryFitError("mixed_or_wrong_feature_horizon")

    expected_external = [
        f"external__{field}" for field in specs["factor_specs"][factor_id]["feature_fields"]
    ]
    missing_external = sorted(set(expected_external + ["external_relationship_class"]).difference(right.columns))
    if missing_external:
        raise ExternalEvidence8GDiscoveryFitError("challenger_external_columns_missing:" + ",".join(missing_external))
    actual_external = sorted(str(c) for c in right.columns if str(c).startswith("external__"))
    if actual_external != sorted(expected_external):
        raise ExternalEvidence8GDiscoveryFitError("challenger_external_factor_family_mismatch")
    if any(str(c).startswith("external") for c in left.columns):
        raise ExternalEvidence8GDiscoveryFitError("baseline_contains_external_evidence")

    baseline_numeric = {column: _fit_numeric(left[column], drop_if_zero_variance=False) for column in numeric}
    baseline_categorical = {column: _fit_categories(left[column]) for column in categorical}

    external_numeric: dict[str, dict[str, Any]] = {}
    for column in expected_external:
        state = _fit_numeric(right[column], drop_if_zero_variance=True)
        if state["missing_n"]:
            raise ExternalEvidence8GDiscoveryFitError(f"external_factor_missing_in_eligible_rows:{column}")
        external_numeric[column] = state

    relationship_values = [_cat(value) for value in right["external_relationship_class"].tolist()]
    if MISSING_CATEGORY in relationship_values:
        raise ExternalEvidence8GDiscoveryFitError("external_relationship_class_missing")
    relationship_classes = sorted(set(relationship_values))
    if not relationship_classes:
        raise ExternalEvidence8GDiscoveryFitError("external_relationship_class_empty")

    baseline_output: list[str] = []
    for column in numeric:
        baseline_output.extend([f"num::{column}", f"missing::{column}"])
    for column in categorical:
        baseline_output.extend(f"cat::{column}::{level}" for level in baseline_categorical[column])

    retained_external = [column for column in expected_external if not external_numeric[column]["dropped"]]
    challenger_output = list(baseline_output)
    for column in retained_external:
        challenger_output.append(f"external::{column}")
        challenger_output.extend(
            f"interaction::{column}::{relationship}" for relationship in relationship_classes
        )

    state: dict[str, Any] = {
        "schema_version": PREPROCESSOR_SCHEMA,
        "phase": "8G-D",
        "factor_id": factor_id,
        "horizon_sessions": int(horizon_sessions),
        "fit_partition": "DISCOVERY_USABLE_FEATURE_SIDE_ONLY",
        "outcomes_read_while_fitting_preprocessor": False,
        "numeric_ddof": 0,
        "zero_variance_tolerance": ZERO_TOL,
        "unknown_categorical_policy": "ALL_ZERO_HANDLE_UNKNOWN_IGNORE",
        "baseline_numeric": baseline_numeric,
        "baseline_categorical_levels": baseline_categorical,
        "external_numeric": external_numeric,
        "relationship_classes": relationship_classes,
        "baseline_output_columns": baseline_output,
        "challenger_output_columns": challenger_output,
        "feature_fit_rows": int(len(left)),
        "feature_snapshot_ids": sorted({str(x) for x in left["snapshot_id"].tolist()}),
        "external_zero_variance_dropped": sorted(
            column for column, item in external_numeric.items() if item["dropped"]
        ),
    }
    state["preprocessor_sha256"] = _digest(state)
    return state


def _transform_baseline(frame: pd.DataFrame, state: Mapping[str, Any]) -> tuple[np.ndarray, list[str]]:
    parts: list[np.ndarray] = []
    names: list[str] = []
    for column, item in state["baseline_numeric"].items():
        if column not in frame.columns:
            raise ExternalEvidence8GDiscoveryFitError(f"transform_numeric_missing:{column}")
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
        if np.isinf(values).any():
            raise ExternalEvidence8GDiscoveryFitError(f"transform_numeric_infinite:{column}")
        missing = np.isnan(values)
        imputed = np.where(missing, float(item["median"]), values)
        scaled = (imputed - float(item["mean"])) / float(item["scale"])
        parts.extend([scaled.reshape(-1, 1), missing.astype(float).reshape(-1, 1)])
        names.extend([f"num::{column}", f"missing::{column}"])

    for column, levels in state["baseline_categorical_levels"].items():
        if column not in frame.columns:
            raise ExternalEvidence8GDiscoveryFitError(f"transform_categorical_missing:{column}")
        values = np.asarray([_cat(value) for value in frame[column].tolist()], dtype=object)
        for level in levels:
            parts.append((values == level).astype(float).reshape(-1, 1))
            names.append(f"cat::{column}::{level}")

    matrix = np.hstack(parts) if parts else np.empty((len(frame), 0), dtype=float)
    if names != list(state["baseline_output_columns"]):
        raise ExternalEvidence8GDiscoveryFitError("baseline_output_column_order_drift")
    return matrix, names


def transform_discovery_features(
    frame: pd.DataFrame,
    *,
    preprocessor: Mapping[str, Any],
    challenger: bool,
) -> tuple[np.ndarray, list[str]]:
    if preprocessor.get("schema_version") != PREPROCESSOR_SCHEMA:
        raise ExternalEvidence8GDiscoveryFitError("unsupported_preprocessor_schema")
    matrix, names = _transform_baseline(frame, preprocessor)
    if not challenger:
        return matrix, names

    relationship_values = np.asarray(
        [_cat(value) for value in frame["external_relationship_class"].tolist()], dtype=object
    )
    extra_parts: list[np.ndarray] = []
    extra_names: list[str] = []
    for column, item in preprocessor["external_numeric"].items():
        if item["dropped"]:
            continue
        if column not in frame.columns:
            raise ExternalEvidence8GDiscoveryFitError(f"transform_external_missing:{column}")
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
        if np.isnan(values).any() or np.isinf(values).any():
            raise ExternalEvidence8GDiscoveryFitError(f"transform_external_nonfinite:{column}")
        scaled = (values - float(item["mean"])) / float(item["scale"])
        extra_parts.append(scaled.reshape(-1, 1))
        extra_names.append(f"external::{column}")
        for relationship in preprocessor["relationship_classes"]:
            interaction = scaled * (relationship_values == relationship).astype(float)
            extra_parts.append(interaction.reshape(-1, 1))
            extra_names.append(f"interaction::{column}::{relationship}")

    if extra_parts:
        matrix = np.hstack([matrix, *extra_parts])
    names = names + extra_names
    if names != list(preprocessor["challenger_output_columns"]):
        raise ExternalEvidence8GDiscoveryFitError("challenger_output_column_order_drift")
    return matrix, names


def fit_fixed_ridge(
    matrix: np.ndarray,
    target: np.ndarray,
    *,
    feature_names: list[str],
    alpha: float = 1.0,
) -> dict[str, Any]:
    x = np.asarray(matrix, dtype=float)
    y = np.asarray(target, dtype=float).reshape(-1)
    if x.ndim != 2 or x.shape[0] != y.shape[0] or x.shape[1] != len(feature_names):
        raise ExternalEvidence8GDiscoveryFitError("ridge_shape_mismatch")
    if len(y) < 1 or not np.isfinite(x).all() or not np.isfinite(y).all():
        raise ExternalEvidence8GDiscoveryFitError("ridge_requires_finite_nonempty_data")
    if float(alpha) != 1.0:
        raise ExternalEvidence8GDiscoveryFitError("ridge_alpha_must_remain_frozen_at_1")

    design = np.column_stack([np.ones(len(y), dtype=float), x])
    penalty = np.eye(design.shape[1], dtype=float)
    penalty[0, 0] = 0.0
    system = design.T @ design + float(alpha) * penalty
    rhs = design.T @ y
    try:
        beta = np.linalg.solve(system, rhs)
        solver = "solve"
    except np.linalg.LinAlgError:
        beta = np.linalg.pinv(system) @ rhs
        solver = "pinv_fallback"
    prediction = design @ beta
    residual = y - prediction
    result: dict[str, Any] = {
        "family": "ridge_linear_regression",
        "alpha": 1.0,
        "fit_intercept": True,
        "intercept": float(beta[0]),
        "coefficients": {name: float(value) for name, value in zip(feature_names, beta[1:])},
        "training_n": int(len(y)),
        "training_mse": float(np.mean(residual ** 2)),
        "training_mae": float(np.mean(np.abs(residual))),
        "solver": solver,
    }
    result["model_sha256"] = _digest(result)
    return result


def _subset_features_to_outcome_identities(
    frame: pd.DataFrame, outcome_rows: pd.DataFrame
) -> pd.DataFrame:
    missing = sorted(set(OUTCOME_IDENTITY).difference(outcome_rows.columns))
    if missing:
        raise ExternalEvidence8GDiscoveryFitError("outcome_identity_missing:" + ",".join(missing))
    keys = {
        (str(row.snapshot_id), str(row.as_of), str(row.symbol), int(row.horizon_sessions))
        for row in outcome_rows[OUTCOME_IDENTITY].itertuples(index=False)
    }
    mask = [
        (str(row.snapshot_id), str(row.as_of), str(row.symbol), int(row.horizon_sessions)) in keys
        for row in frame[OUTCOME_IDENTITY].itertuples(index=False)
    ]
    subset = frame.loc[mask].copy()
    if len(subset) != len(keys):
        raise ExternalEvidence8GDiscoveryFitError("outcome_identity_not_unique_in_feature_frame")
    return subset.sort_values(OUTCOME_IDENTITY, kind="mergesort").reset_index(drop=True)


def fit_guarded_discovery_models(
    *,
    baseline_features: pd.DataFrame,
    challenger_features: pd.DataFrame,
    outcome_rows: pd.DataFrame,
    factor_id: str,
    horizon_sessions: int,
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
    protocol: Mapping[str, Any],
    research_as_of: object,
) -> dict[str, Any]:
    """Fit paired provisional Discovery models under the frozen access gates."""
    validate_discovery_protocol(protocol, specs, plan)
    validate_guarded_manifest(manifest, specs, plan)
    numeric, categorical = resolved_baseline_columns(specs, horizon_sessions)
    baseline_columns = numeric + categorical
    all_baseline, all_challenger = _sorted_pair(
        baseline_features, challenger_features, baseline_columns
    )

    # This operation intentionally happens before prepare_discovery_outcome_rows,
    # which is the only operation below permitted to expose peer-excess values.
    preprocessor = fit_discovery_preprocessor(
        baseline_features=all_baseline,
        challenger_features=all_challenger,
        factor_id=factor_id,
        horizon_sessions=horizon_sessions,
        specs=specs,
        protocol=protocol,
        plan=plan,
    )

    fit_baseline = _subset_features_to_outcome_identities(all_baseline, outcome_rows)
    fit_challenger = _subset_features_to_outcome_identities(all_challenger, outcome_rows)
    opened = prepare_discovery_outcome_rows(
        paired_rows=fit_baseline[OUTCOME_IDENTITY],
        outcome_rows=outcome_rows,
        factor_id=factor_id,
        horizon_sessions=horizon_sessions,
        manifest=manifest,
        specs=specs,
        plan=plan,
        protocol=protocol,
        research_as_of=research_as_of,
    )
    opened = opened.sort_values(OUTCOME_IDENTITY, kind="mergesort").reset_index(drop=True)
    fit_baseline = fit_baseline.sort_values(OUTCOME_IDENTITY, kind="mergesort").reset_index(drop=True)
    fit_challenger = fit_challenger.sort_values(OUTCOME_IDENTITY, kind="mergesort").reset_index(drop=True)
    for column in OUTCOME_IDENTITY:
        if not fit_baseline[column].equals(opened[column]):
            raise ExternalEvidence8GDiscoveryFitError(f"opened_outcome_pairing_drift:{column}")

    baseline_x, baseline_names = transform_discovery_features(
        fit_baseline, preprocessor=preprocessor, challenger=False
    )
    challenger_x, challenger_names = transform_discovery_features(
        fit_challenger, preprocessor=preprocessor, challenger=True
    )
    target_column = f"peer_excess_{horizon_sessions}t"
    y = pd.to_numeric(opened[target_column], errors="raise").to_numpy(dtype=float)
    baseline_model = fit_fixed_ridge(baseline_x, y, feature_names=baseline_names, alpha=1.0)
    challenger_model = fit_fixed_ridge(challenger_x, y, feature_names=challenger_names, alpha=1.0)

    artifact: dict[str, Any] = {
        "schema_version": FIT_SCHEMA,
        "phase": "8G-D",
        "status": "PROVISIONAL_DISCOVERY_FIT",
        "factor_id": factor_id,
        "horizon_sessions": int(horizon_sessions),
        "target": target_column,
        "preprocessing_fitted_before_outcome_open": True,
        "preprocessor": preprocessor,
        "baseline_model": baseline_model,
        "challenger_model": challenger_model,
        "feature_fit_rows": int(len(all_baseline)),
        "outcome_fit_rows": int(len(opened)),
        "feature_snapshot_ids": sorted({str(x) for x in all_baseline["snapshot_id"].tolist()}),
        "outcome_snapshot_ids": sorted({str(x) for x in opened["snapshot_id"].tolist()}),
        "outcome_as_of_dates": sorted({str(x) for x in opened["as_of"].tolist()}),
        "descriptive_training_mse_improvement": float(
            baseline_model["training_mse"] - challenger_model["training_mse"]
        ),
        "validation_outcomes_opened": False,
        "holdout_outcomes_opened": False,
        "validation_open_allowed": False,
        "promotion_decision_allowed": False,
    }
    artifact["fit_sha256"] = _digest(artifact)
    return artifact


def discovery_final_freeze_gate(
    *,
    fit_artifact: Mapping[str, Any],
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
    minimum_paired_n: int = 30,
    minimum_temporal_support_regions: int = 2,
) -> dict[str, Any]:
    """Check whether Discovery is complete enough to freeze before Validation."""
    validate_guarded_manifest(manifest, specs, plan)
    if fit_artifact.get("schema_version") != FIT_SCHEMA:
        raise ExternalEvidence8GDiscoveryFitError("unsupported_discovery_fit_schema")
    factor_id = str(fit_artifact.get("factor_id") or "")
    horizon = int(fit_artifact.get("horizon_sessions"))
    key = f"{factor_id}_x_{horizon}t"
    stream = manifest["streams"].get(key)
    if not isinstance(stream, Mapping):
        raise ExternalEvidence8GDiscoveryFitError(f"stream_missing:{key}")
    assignments = list(stream.get("assignments") or ())
    discovery = [item for item in assignments if item.get("split") == "DISCOVERY"]
    usable = [item for item in discovery if item.get("usable") is True]
    validation = [item for item in assignments if item.get("split") == "VALIDATION"]
    expected_raw = 8 * horizon
    expected_usable = 7 * horizon
    expected_snapshot_ids = {str(item["snapshot_id"]) for item in usable}
    trained_snapshot_ids = {str(x) for x in fit_artifact.get("outcome_snapshot_ids") or ()}

    dates = sorted({str(x) for x in fit_artifact.get("outcome_as_of_dates") or ()})
    block_length = 2 * horizon
    temporal_regions = 0
    last_start: int | None = None
    for position, _day in enumerate(dates):
        if last_start is None or position - last_start >= block_length:
            temporal_regions += 1
            last_start = position

    reasons: list[str] = []
    if len(discovery) < expected_raw:
        reasons.append("DISCOVERY_RAW_COHORT_INCOMPLETE")
    if len(usable) != expected_usable:
        reasons.append("DISCOVERY_USABLE_COHORT_INCOMPLETE")
    if not validation:
        reasons.append("FIRST_VALIDATION_FEATURE_BINDING_REQUIRED_FOR_EXACT_BOUNDARY")
    if trained_snapshot_ids != expected_snapshot_ids:
        reasons.append("NOT_ALL_USABLE_DISCOVERY_SNAPSHOTS_HAVE_MATURED_PAIRED_OUTCOMES")
    if int(fit_artifact.get("outcome_fit_rows") or 0) < int(minimum_paired_n):
        reasons.append("MINIMUM_PAIRED_N_NOT_MET")
    if temporal_regions < int(minimum_temporal_support_regions):
        reasons.append("MINIMUM_TEMPORAL_SUPPORT_NOT_MET")

    ready = not reasons
    return {
        "schema_version": "external_evidence_8g_discovery_freeze_gate_v1",
        "factor_id": factor_id,
        "horizon_sessions": horizon,
        "ready_for_final_discovery_model_freeze": ready,
        "state": "DISCOVERY_READY_FOR_FINAL_MODEL_FREEZE" if ready else "DISCOVERY_NOT_READY_TO_FREEZE",
        "reasons": reasons,
        "discovery_raw_assignments": len(discovery),
        "discovery_usable_assignments": len(usable),
        "expected_discovery_raw_assignments": expected_raw,
        "expected_discovery_usable_assignments": expected_usable,
        "paired_outcome_n": int(fit_artifact.get("outcome_fit_rows") or 0),
        "temporal_support_regions": temporal_regions,
        "minimum_temporal_support_regions": int(minimum_temporal_support_regions),
        "block_length_sessions": block_length,
        "validation_outcomes_opened": False,
        "holdout_outcomes_opened": False,
    }
