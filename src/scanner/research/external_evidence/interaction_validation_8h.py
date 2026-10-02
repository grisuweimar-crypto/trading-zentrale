"""Phase 8H-F validation-only interaction evaluation.

8H-F consumes only the model pairs frozen by 8H-E. It never refits them, opens
only the VALIDATION split bound before outcome access, evaluates every frozen
interaction hypothesis, applies Holm across the complete frozen family, and
freezes the validation family for 8H-G. Holdout remains sealed here.
"""
from __future__ import annotations

from hashlib import sha256
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.research.external_evidence.interaction_discovery_8h import (
    DISCOVERY_FAMILY_FROZEN,
    DISCOVERY_RESULT_SCHEMA,
    OUTCOME_IDENTITY,
    SPLIT_MANIFEST_SCHEMA,
)
from scanner.research.external_evidence.interaction_model_8h import (
    DESIGN_FAMILY_CONSTRUCTED,
    INTERACTION_MODEL_RESULT_SCHEMA,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
)


VALIDATION_CONTRACT_SCHEMA = "external_evidence_8h_interaction_validation_evaluation_v1"
VALIDATION_RESULT_SCHEMA = "external_evidence_8h_interaction_validation_result_v1"
VALIDATION_FAMILY_RECEIPT_SCHEMA = "external_evidence_8h_interaction_validation_family_receipt_v1"
WAITING_FOR_DISCOVERY_MODELS = "WAITING_FOR_FROZEN_DISCOVERY_MODELS"
VALIDATION_FAMILY_FROZEN = "VALIDATION_FAMILY_FROZEN"
VALIDATION_FAMILY_INSUFFICIENT = "VALIDATION_FAMILY_INSUFFICIENT_EVIDENCE"


class ExternalEvidence8HValidationError(ValueError):
    """Raised when 8H-F violates the frozen interaction protocol."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    expected = str(row.get(field) or "")
    if not expected:
        raise ExternalEvidence8HValidationError(error)
    payload = dict(row)
    payload.pop(field, None)
    if _digest(payload) != expected:
        raise ExternalEvidence8HValidationError(error)
    return expected


def _timestamp(value: object, error: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ExternalEvidence8HValidationError(error)
    return pd.Timestamp(parsed)


def _day(value: object) -> pd.Timestamp:
    return _timestamp(value, "8h_f_invalid_date").normalize()


def _json_scalar(value: object) -> object:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            value = value.item()  # type: ignore[assignment]
        except (ValueError, AttributeError):
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def _frame_digest(frame: pd.DataFrame, columns: Sequence[str]) -> str:
    records = [
        {column: _json_scalar(value) for column, value in zip(columns, row)}
        for row in frame.loc[:, list(columns)].itertuples(index=False, name=None)
    ]
    return _digest({"columns": list(columns), "records": records})


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((int(base) + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def validate_validation_contract(
    contract: Mapping[str, Any], discovery_contract: Mapping[str, Any]
) -> None:
    if contract.get("schema_version") != VALIDATION_CONTRACT_SCHEMA or contract.get("phase") != "8H-F":
        raise ExternalEvidence8HValidationError("8h_f_contract_schema_or_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HValidationError("8h_f_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase7_mutation_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HValidationError(f"8h_f_forbidden_contract_authorization:{key}")
    if discovery_contract.get("schema_version") != "external_evidence_8h_interaction_discovery_evaluation_v1":
        raise ExternalEvidence8HValidationError("8h_f_8h_e_contract_schema_mismatch")
    model = contract.get("model_freeze")
    if not isinstance(model, Mapping) or model.get("source") != "8H-E frozen_model_pairs":
        raise ExternalEvidence8HValidationError("8h_f_model_freeze_missing")
    if model.get("estimator") != "ridge_linear_regression" or float(model.get("alpha")) != 1.0:
        raise ExternalEvidence8HValidationError("8h_f_frozen_ridge_required")
    for key in (
        "validation_refit_allowed",
        "feature_selection_allowed",
        "hyperparameter_tuning_allowed",
        "sign_search_allowed",
        "threshold_search_allowed",
        "transform_search_allowed",
        "family_reduction_from_discovery_result_allowed",
    ):
        if model.get(key) is not False:
            raise ExternalEvidence8HValidationError(f"8h_f_search_or_refit_must_be_false:{key}")
    access = contract.get("validation_outcome_access")
    if not isinstance(access, Mapping) or access.get("allowed_split") != "VALIDATION":
        raise ExternalEvidence8HValidationError("8h_f_only_validation_split_may_open")
    if access.get("holdout_outcomes_allowed_in_8h_f") is not False:
        raise ExternalEvidence8HValidationError("8h_f_holdout_must_remain_sealed")
    multiplicity = contract.get("multiplicity_control")
    if not isinstance(multiplicity, Mapping) or multiplicity.get("method") != "Holm":
        raise ExternalEvidence8HValidationError("8h_f_holm_required")
    if float(multiplicity.get("family_wise_alpha")) != 0.05:
        raise ExternalEvidence8HValidationError("8h_f_alpha_must_be_0_05")


def _validate_upstream(
    *,
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    discovery_result: Mapping[str, Any],
    split_manifest: Mapping[str, Any] | None,
) -> tuple[bool, str, str, str, list[dict[str, Any]], dict[str, dict[str, Any]], str | None]:
    if freeze_result.get("schema_version") != INTERACTION_FREEZE_RESULT_SCHEMA:
        raise ExternalEvidence8HValidationError("8h_f_freeze_schema_mismatch")
    freeze_sha = _verify_digest(freeze_result, "freeze_sha256", "8h_f_freeze_digest_mismatch")
    if design_result.get("schema_version") != INTERACTION_MODEL_RESULT_SCHEMA:
        raise ExternalEvidence8HValidationError("8h_f_design_schema_mismatch")
    design_sha = _verify_digest(design_result, "construction_sha256", "8h_f_design_digest_mismatch")
    if discovery_result.get("schema_version") != DISCOVERY_RESULT_SCHEMA:
        raise ExternalEvidence8HValidationError("8h_f_discovery_schema_mismatch")
    discovery_sha = _verify_digest(discovery_result, "discovery_sha256", "8h_f_discovery_digest_mismatch")
    family = list(freeze_result.get("confirmatory_family") or ())
    if list(design_result.get("confirmatory_family") or ()) != family or list(discovery_result.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HValidationError("8h_f_confirmatory_family_drift")
    if str(design_result.get("8h_c_freeze_sha256") or "") != freeze_sha:
        raise ExternalEvidence8HValidationError("8h_f_design_freeze_binding_mismatch")
    if str(discovery_result.get("8h_c_freeze_sha256") or "") != freeze_sha or str(discovery_result.get("8h_d_construction_sha256") or "") != design_sha:
        raise ExternalEvidence8HValidationError("8h_f_discovery_parent_binding_mismatch")

    if discovery_result.get("state") != DISCOVERY_FAMILY_FROZEN:
        if discovery_result.get("8h_f_validation_evaluation_eligible") is True:
            raise ExternalEvidence8HValidationError("8h_f_nonfrozen_discovery_cannot_authorize_validation")
        return False, freeze_sha, design_sha, discovery_sha, family, {}, None
    if freeze_result.get("state") != SPECS_FROZEN or design_result.get("state") != DESIGN_FAMILY_CONSTRUCTED:
        raise ExternalEvidence8HValidationError("8h_f_frozen_discovery_requires_frozen_upstream")
    if discovery_result.get("full_family_model_freeze_complete") is not True or discovery_result.get("8h_f_validation_evaluation_eligible") is not True:
        raise ExternalEvidence8HValidationError("8h_f_discovery_entry_gate_not_met")
    if discovery_result.get("validation_outcomes_authorized") is not False or discovery_result.get("holdout_outcomes_authorized") is not False:
        raise ExternalEvidence8HValidationError("8h_f_upstream_must_not_preopen_confirmatory_outcomes")

    models: dict[str, dict[str, Any]] = {}
    for raw in discovery_result.get("frozen_model_pairs") or ():
        row = dict(raw)
        spec_id = str(row.get("interaction_spec_id") or "")
        if not spec_id or spec_id in models:
            raise ExternalEvidence8HValidationError("8h_f_duplicate_or_missing_model_pair")
        _verify_digest(row, "model_pair_sha256", f"8h_f_model_pair_digest_mismatch:{spec_id}")
        for key in ("baseline_model", "challenger_model"):
            model = row.get(key)
            if not isinstance(model, Mapping):
                raise ExternalEvidence8HValidationError(f"8h_f_model_missing:{spec_id}:{key}")
            _verify_digest(model, "model_sha256", f"8h_f_model_digest_mismatch:{spec_id}:{key}")
            if model.get("family") != "ridge_linear_regression" or float(model.get("alpha")) != 1.0 or model.get("fit_intercept") is not True:
                raise ExternalEvidence8HValidationError(f"8h_f_frozen_ridge_model_invalid:{spec_id}:{key}")
        if row.get("validation_refit_allowed") is not False or row.get("holdout_refit_allowed") is not False:
            raise ExternalEvidence8HValidationError(f"8h_f_refit_permission_drift:{spec_id}")
        models[spec_id] = row
    family_ids = {str(x.get("interaction_spec_id") or "") for x in family}
    if set(models) != family_ids:
        raise ExternalEvidence8HValidationError("8h_f_model_pairs_must_equal_full_family")

    if not isinstance(split_manifest, Mapping) or split_manifest.get("schema_version") != SPLIT_MANIFEST_SCHEMA:
        raise ExternalEvidence8HValidationError("8h_f_exact_8h_e_split_manifest_required")
    manifest_sha = _verify_digest(split_manifest, "manifest_sha256", "8h_f_split_manifest_digest_mismatch")
    if manifest_sha != str(discovery_result.get("split_manifest_sha256") or ""):
        raise ExternalEvidence8HValidationError("8h_f_split_manifest_not_identical_to_8h_e")
    if split_manifest.get("validation_outcomes_opened") is not False or split_manifest.get("holdout_outcomes_opened") is not False:
        raise ExternalEvidence8HValidationError("8h_f_split_manifest_must_enter_with_confirmatory_splits_sealed")
    if list(split_manifest.get("family_members") or ()) != family:
        raise ExternalEvidence8HValidationError("8h_f_split_manifest_family_drift")
    return True, freeze_sha, design_sha, discovery_sha, family, models, manifest_sha


def _predict(frame: pd.DataFrame, model: Mapping[str, Any]) -> np.ndarray:
    coefficients = model.get("coefficients")
    if not isinstance(coefficients, Mapping):
        raise ExternalEvidence8HValidationError("8h_f_model_coefficients_missing")
    names = list(coefficients.keys())
    if any(name not in frame.columns for name in names):
        raise ExternalEvidence8HValidationError("8h_f_model_feature_missing_from_design")
    x = frame[names].to_numpy(dtype=float)
    if not np.isfinite(x).all():
        raise ExternalEvidence8HValidationError("8h_f_nonfinite_validation_design")
    beta = np.asarray([float(coefficients[name]) for name in names], dtype=float)
    return float(model.get("intercept")) + x @ beta


def _snapshot_effects(rows: pd.DataFrame, horizon: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    target = f"peer_excess_{horizon}t"
    y = pd.to_numeric(rows[target], errors="raise").to_numpy(dtype=float)
    base = pd.to_numeric(rows["baseline_prediction"], errors="raise").to_numpy(dtype=float)
    challenger = pd.to_numeric(rows["challenger_prediction"], errors="raise").to_numpy(dtype=float)
    if not np.isfinite(y).all() or not np.isfinite(base).all() or not np.isfinite(challenger).all():
        raise ExternalEvidence8HValidationError("8h_f_nonfinite_outcome_or_prediction")
    work = rows.copy()
    work["row_improvement"] = (y - base) ** 2 - (y - challenger) ** 2
    work["row_mae_improvement"] = np.abs(y - base) - np.abs(y - challenger)
    label = f"label_available_from_{horizon}t"
    grouped: list[dict[str, Any]] = []
    for (snapshot_id, as_of), group in work.groupby(["snapshot_id", "as_of"], sort=True):
        if group["start_market_date"].astype(str).nunique() != 1 or group[label].astype(str).nunique() != 1:
            raise ExternalEvidence8HValidationError("8h_f_snapshot_date_identity_ambiguous")
        grouped.append({
            "snapshot_id": str(snapshot_id),
            "as_of": str(as_of),
            "start_market_date": str(group["start_market_date"].iloc[0]),
            label: str(group[label].iloc[0]),
            "snapshot_improvement": float(group["row_improvement"].mean()),
            "paired_rows": int(len(group)),
        })
    return work, pd.DataFrame(grouped)


def _temporal_regions(snapshot: pd.DataFrame, horizon: int) -> int:
    if snapshot.empty:
        return 0
    dates = sorted({_day(x) for x in snapshot["start_market_date"].tolist()})
    positions = {day: index for index, day in enumerate(dates)}
    occurrences = sorted({positions[_day(x)] for x in snapshot["start_market_date"].tolist()})
    block = 2 * int(horizon)
    regions = 0
    last: int | None = None
    for position in occurrences:
        if last is None or position - last >= block:
            regions += 1
            last = position
    return regions


def _bootstrap(snapshot: pd.DataFrame, *, horizon: int, reps: int, seed: int) -> dict[str, Any]:
    dates = sorted({_day(x) for x in snapshot["start_market_date"].tolist()})
    if len(dates) < 2 or reps < 1:
        return {"ci_low": None, "ci_high": None, "p_value": None, "bootstrap_repetitions": 0}
    span = min(2 * int(horizon), len(dates))
    blocks = [[dates[(start + offset) % len(dates)] for offset in range(span)] for start in range(len(dates))]
    date_series = pd.to_datetime(snapshot["start_market_date"], utc=True).dt.normalize()
    by_date = {day: snapshot.loc[date_series.eq(day), "snapshot_improvement"].to_numpy(dtype=float) for day in dates}
    observed = float(snapshot["snapshot_improvement"].mean())
    centered = snapshot.copy()
    centered["snapshot_improvement"] = centered["snapshot_improvement"] - observed
    centered_dates = pd.to_datetime(centered["start_market_date"], utc=True).dt.normalize()
    centered_by_date = {day: centered.loc[centered_dates.eq(day), "snapshot_improvement"].to_numpy(dtype=float) for day in dates}
    rng = np.random.default_rng(seed)
    draws_per_rep = int(np.ceil(len(dates) / span))
    raw_draws: list[float] = []
    null_draws: list[float] = []
    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for index in chosen for day in blocks[index]][: len(dates)]
        raw_parts = [by_date[day] for day in sampled_dates if len(by_date[day])]
        null_parts = [centered_by_date[day] for day in sampled_dates if len(centered_by_date[day])]
        if raw_parts and null_parts:
            raw_draws.append(float(np.mean(np.concatenate(raw_parts))))
            null_draws.append(float(np.mean(np.concatenate(null_parts))))
    if not raw_draws:
        return {"ci_low": None, "ci_high": None, "p_value": None, "bootstrap_repetitions": 0}
    ci_low, ci_high = np.quantile(np.asarray(raw_draws), [0.025, 0.975])
    null = np.asarray(null_draws)
    p_value = float((1 + np.sum(np.abs(null) >= abs(observed))) / (len(null) + 1))
    return {
        "ci_low": float(ci_low),
        "ci_high": float(ci_high),
        "p_value": min(1.0, p_value),
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
    return {
        "snapshot_n": len(kept),
        "mean_improvement": None if not kept else float(np.mean(kept)),
    }


def _concentration(rows: pd.DataFrame, field: str) -> float | None:
    if field not in rows.columns or rows.empty:
        return None
    contribution = rows.groupby(field, dropna=False)["row_improvement"].sum().to_numpy(dtype=float)
    denominator = float(np.abs(contribution).sum())
    return None if denominator <= 0 else float(np.max(np.abs(contribution)) / denominator)


def _group_report(rows: pd.DataFrame, field: str, min_n: int) -> dict[str, Any]:
    if field not in rows.columns:
        return {"status": "DIAGNOSTIC_CONTEXT_INCOMPLETE", "groups": {}}
    groups: dict[str, Any] = {}
    for value, group in rows.groupby(field, dropna=False):
        key = "__MISSING__" if pd.isna(value) else str(value)
        groups[key] = {
            "paired_n": int(len(group)),
            "mean_row_improvement": float(group["row_improvement"].mean()),
            "direction_reportable": int(len(group)) >= int(min_n),
        }
    return {"status": "OK", "groups": groups}


def _holm(results: Sequence[dict[str, Any]], alpha: float) -> list[dict[str, Any]]:
    source = [dict(row) for row in results]
    pvals = [
        (index, float(row["primary_p_value_two_sided"]) if row.get("minimum_evidence_met") is True and row.get("primary_p_value_two_sided") is not None else 1.0)
        for index, row in enumerate(source)
    ]
    ordered = sorted(pvals, key=lambda item: (item[1], source[item[0]]["hypothesis_id"]))
    adjusted: dict[int, float] = {}
    running = 0.0
    m = len(ordered)
    for rank, (index, p_value) in enumerate(ordered):
        candidate = min(1.0, (m - rank) * max(0.0, min(1.0, p_value)))
        running = max(running, candidate)
        adjusted[index] = running
    for index, row in enumerate(source):
        row["holm_adjusted_p"] = adjusted[index]
        row["holm_positive"] = bool(
            row.get("minimum_evidence_met") is True
            and row.get("primary_effect") is not None
            and float(row["primary_effect"]) > 0
            and adjusted[index] < float(alpha)
        )
        row["evaluation_sha256"] = _digest({k: v for k, v in row.items() if k != "evaluation_sha256"})
    return source


def _waiting_result(*, freeze_sha: str, design_sha: str, discovery_sha: str, family: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": VALIDATION_RESULT_SCHEMA,
        "phase": "8H-F",
        "state": WAITING_FOR_DISCOVERY_MODELS,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "8h_e_discovery_sha256": discovery_sha,
        "confirmatory_family": list(family),
        "validation_results": [],
        "validation_family_receipt": None,
        "8h_g_holdout_evaluation_eligible": False,
        "validation_outcomes_opened": False,
        "holdout_outcomes_authorized": False,
        "model_refit": False,
        "family_shrunk_or_expanded": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-F_WAIT_FOR_FROZEN_DISCOVERY_MODELS",
    }
    result["validation_sha256"] = _digest(result)
    return result


def evaluate_interaction_validation_family(
    *,
    contract: Mapping[str, Any],
    discovery_contract: Mapping[str, Any],
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    discovery_result: Mapping[str, Any],
    split_manifest: Mapping[str, Any] | None = None,
    baseline_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    challenger_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    outcome_rows_by_spec: Mapping[str, pd.DataFrame] | None = None,
    research_as_of: object | None = None,
    context_rows_by_spec: Mapping[str, pd.DataFrame] | None = None,
    coverage_records_by_spec: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Evaluate the complete frozen interaction family on Validation only."""
    validate_validation_contract(contract, discovery_contract)
    ready, freeze_sha, design_sha, discovery_sha, family, models, manifest_sha = _validate_upstream(
        freeze_result=freeze_result,
        design_result=design_result,
        discovery_result=discovery_result,
        split_manifest=split_manifest,
    )
    baselines = dict(baseline_frames_by_spec or {})
    challengers = dict(challenger_frames_by_spec or {})
    outcomes = dict(outcome_rows_by_spec or {})
    contexts = dict(context_rows_by_spec or {})
    coverages = dict(coverage_records_by_spec or {})
    if not ready:
        if baselines or challengers or outcomes or contexts or coverages or research_as_of is not None or split_manifest is not None:
            raise ExternalEvidence8HValidationError("8h_f_empirical_inputs_forbidden_before_discovery_freeze")
        return _waiting_result(freeze_sha=freeze_sha, design_sha=design_sha, discovery_sha=discovery_sha, family=family)

    spec_ids = [str(row["interaction_spec_id"]) for row in family]
    if set(baselines) != set(spec_ids) or set(challengers) != set(spec_ids) or set(outcomes) != set(spec_ids):
        raise ExternalEvidence8HValidationError("8h_f_empirical_input_family_must_equal_full_frozen_family")
    if research_as_of is None:
        raise ExternalEvidence8HValidationError("8h_f_research_as_of_required")
    research_time = _timestamp(research_as_of, "8h_f_research_as_of_invalid")

    receipts_by_spec = {str(row["interaction_spec_id"]): row for row in design_result.get("constructed_interaction_designs") or ()}
    if set(receipts_by_spec) != set(spec_ids):
        raise ExternalEvidence8HValidationError("8h_f_design_receipts_must_equal_full_family")
    stats = contract["validation_statistics"]
    min_n = int(stats["minimum_paired_n_per_interaction_horizon_validation"])
    min_regions = int(stats["minimum_temporal_support_regions"])
    reps = int(stats["bootstrap_repetitions"])
    seed = int(stats["bootstrap_seed"])
    min_group = int(contract["robustness_diagnostics"]["minimum_group_paired_n_for_direction_report"])

    raw_results: list[dict[str, Any]] = []
    all_sufficient = True
    for family_member in family:
        spec_id = str(family_member["interaction_spec_id"])
        horizon = int(family_member["horizon_sessions"])
        receipt = receipts_by_spec[spec_id]
        _verify_digest(receipt, "construction_sha256", f"8h_f_design_receipt_digest_mismatch:{spec_id}")
        baseline = baselines[spec_id].copy()
        challenger = challengers[spec_id].copy()
        baseline_columns = list(receipt["baseline_columns"])
        challenger_columns = list(receipt["challenger_columns"])
        if list(baseline.columns) != baseline_columns or list(challenger.columns) != challenger_columns:
            raise ExternalEvidence8HValidationError(f"8h_f_design_column_order_drift:{spec_id}")
        if _frame_digest(baseline, baseline_columns) != str(receipt["baseline_design_sha256"]):
            raise ExternalEvidence8HValidationError(f"8h_f_baseline_design_hash_mismatch:{spec_id}")
        if _frame_digest(challenger, challenger_columns) != str(receipt["challenger_design_sha256"]):
            raise ExternalEvidence8HValidationError(f"8h_f_challenger_design_hash_mismatch:{spec_id}")
        for column in OUTCOME_IDENTITY:
            if not baseline[column].astype(str).equals(challenger[column].astype(str)):
                raise ExternalEvidence8HValidationError(f"8h_f_baseline_challenger_identity_drift:{spec_id}:{column}")

        stream = split_manifest["streams"].get(spec_id)
        if not isinstance(stream, Mapping):
            raise ExternalEvidence8HValidationError(f"8h_f_split_stream_missing:{spec_id}")
        assignments = list(stream.get("assignments") or ())
        validation_keys = {
            (str(row["snapshot_id"]), str(row["as_of"]))
            for row in assignments if row.get("split") == "VALIDATION" and row.get("usable") is True
        }
        holdout_dates = [
            _timestamp(row["as_of"], f"8h_f_holdout_as_of_invalid:{spec_id}")
            for row in assignments if row.get("split") == "HOLDOUT" and row.get("usable") is True
        ]
        if not validation_keys or not holdout_dates:
            raise ExternalEvidence8HValidationError(f"8h_f_validation_and_holdout_binding_required:{spec_id}")
        first_holdout = min(holdout_dates)
        baseline_mask = [
            (str(row.snapshot_id), str(row.as_of)) in validation_keys
            for row in baseline[["snapshot_id", "as_of"]].itertuples(index=False)
        ]
        challenger_mask = [
            (str(row.snapshot_id), str(row.as_of)) in validation_keys
            for row in challenger[["snapshot_id", "as_of"]].itertuples(index=False)
        ]
        val_baseline = baseline.loc[baseline_mask].copy().sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        val_challenger = challenger.loc[challenger_mask].copy().sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        observed_keys = {(str(row.snapshot_id), str(row.as_of)) for row in val_baseline[["snapshot_id", "as_of"]].itertuples(index=False)}
        if observed_keys != validation_keys or len(val_baseline) != len(val_challenger) or val_baseline.empty:
            raise ExternalEvidence8HValidationError(f"8h_f_validation_design_binding_incomplete:{spec_id}")

        outcome = outcomes[spec_id].copy()
        target = f"peer_excess_{horizon}t"
        label = f"label_available_from_{horizon}t"
        required = set(OUTCOME_IDENTITY) | {"start_market_date", label, target}
        missing = sorted(required.difference(str(c) for c in outcome.columns))
        if missing:
            raise ExternalEvidence8HValidationError(f"8h_f_outcome_columns_missing:{spec_id}:" + ",".join(missing))
        if outcome.duplicated(list(OUTCOME_IDENTITY)).any():
            raise ExternalEvidence8HValidationError(f"8h_f_duplicate_outcome_identity:{spec_id}")
        outcome = outcome.sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        if len(outcome) != len(val_baseline):
            raise ExternalEvidence8HValidationError(f"8h_f_outcomes_must_match_all_validation_design_rows:{spec_id}")
        for column in OUTCOME_IDENTITY:
            if not outcome[column].astype(str).equals(val_baseline[column].astype(str)):
                raise ExternalEvidence8HValidationError(f"8h_f_outcome_identity_mismatch:{spec_id}:{column}")
        for row in outcome.itertuples(index=False):
            label_time = _timestamp(getattr(row, label), f"8h_f_label_available_invalid:{spec_id}")
            if label_time > research_time:
                raise ExternalEvidence8HValidationError(f"8h_f_validation_label_not_mature:{spec_id}")
            if label_time >= first_holdout:
                raise ExternalEvidence8HValidationError(f"8h_f_exact_validation_holdout_boundary_purge_required:{spec_id}")

        model_pair = models[spec_id]
        baseline_prediction = _predict(val_baseline, model_pair["baseline_model"])
        challenger_prediction = _predict(val_challenger, model_pair["challenger_model"])
        opened = outcome.copy()
        opened["baseline_prediction"] = baseline_prediction
        opened["challenger_prediction"] = challenger_prediction
        row_effects, snapshot = _snapshot_effects(opened, horizon)
        paired_n = int(len(row_effects))
        regions = _temporal_regions(snapshot, horizon)
        sufficient = paired_n >= min_n and regions >= min_regions
        all_sufficient = all_sufficient and sufficient
        bootstrap = _bootstrap(
            snapshot,
            horizon=horizon,
            reps=reps if sufficient else 0,
            seed=_stable_seed(seed, spec_id, horizon, "VALIDATION"),
        )
        primary = None if snapshot.empty else float(snapshot["snapshot_improvement"].mean())
        non_overlap = _non_overlap(snapshot, horizon)

        context = contexts.get(spec_id)
        context_complete = False
        if context is not None:
            identity = ["snapshot_id", "as_of", "symbol"]
            if any(column not in context.columns for column in identity) or context.duplicated(identity).any():
                raise ExternalEvidence8HValidationError(f"8h_f_context_identity_invalid:{spec_id}")
            available_fields = [field for field in ("market_regime_stock", "sector", "currency", "domain") if field in context.columns]
            merge_columns = identity + available_fields
            row_effects = row_effects.merge(context[merge_columns], on=identity, how="left", validate="one_to_one")
            context_complete = bool(available_fields) and not row_effects[available_fields].isna().all().all()
        else:
            available_fields = []

        coverage_records = list(coverages.get(spec_id) or ())
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

        stability = {
            field: _group_report(row_effects, field, min_group)
            for field in ("market_regime_stock", "sector", "currency", "domain")
        }
        result = {
            "interaction_spec_id": spec_id,
            "hypothesis_id": str(family_member["hypothesis_id"]),
            "horizon_sessions": horizon,
            "split": "VALIDATION",
            "status": "VALIDATION_EVALUATED" if sufficient else "INSUFFICIENT_VALIDATION_EVIDENCE",
            "paired_n": paired_n,
            "snapshot_n": int(len(snapshot)),
            "temporal_support_regions": regions,
            "minimum_evidence_met": sufficient,
            "primary_effect": primary,
            "primary_ci_95": [bootstrap["ci_low"], bootstrap["ci_high"]],
            "primary_p_value_two_sided": bootstrap["p_value"],
            "mean_absolute_error_improvement": float(row_effects["row_mae_improvement"].mean()),
            "non_overlap_sensitivity": non_overlap,
            "context_complete": context_complete,
            "stability_diagnostics": stability,
            "coverage_missingness": coverage,
            "concentration": {
                "start_market_date": _concentration(row_effects, "start_market_date"),
                "symbol": _concentration(row_effects, "symbol"),
                "sector": _concentration(row_effects, "sector"),
                "domain": _concentration(row_effects, "domain"),
            },
            "model_pair_sha256": str(model_pair["model_pair_sha256"]),
            "model_refit": False,
            "family_membership_changed": False,
            "holdout_outcomes_opened": False,
            "holm_adjusted_p": None,
            "holm_positive": False,
        }
        result["evaluation_sha256"] = _digest(result)
        raw_results.append(result)

    expected_ids = [str(x["hypothesis_id"]) for x in family]
    if [row["hypothesis_id"] for row in raw_results] != expected_ids:
        raise ExternalEvidence8HValidationError("8h_f_result_family_order_or_membership_drift")
    adjusted = _holm(raw_results, float(contract["multiplicity_control"]["family_wise_alpha"]))
    state = VALIDATION_FAMILY_FROZEN if all_sufficient else VALIDATION_FAMILY_INSUFFICIENT
    receipt = None
    if all_sufficient:
        receipt = {
            "schema_version": VALIDATION_FAMILY_RECEIPT_SCHEMA,
            "phase": "8H-F",
            "frozen": True,
            "8h_e_discovery_sha256": discovery_sha,
            "split_manifest_sha256": manifest_sha,
            "confirmatory_family": family,
            "validation_result_sha256s": [str(row["evaluation_sha256"]) for row in adjusted],
            "holm_method": "Holm",
            "family_wise_alpha": 0.05,
            "holdout_model_refit_allowed": False,
            "holdout_family_reduction_allowed": False,
            "holdout_outcomes_opened": False,
        }
        receipt["receipt_sha256"] = _digest(receipt)

    family_result: dict[str, Any] = {
        "schema_version": VALIDATION_RESULT_SCHEMA,
        "phase": "8H-F",
        "state": state,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "8h_e_discovery_sha256": discovery_sha,
        "split_manifest_sha256": manifest_sha,
        "confirmatory_family": family,
        "validation_results": adjusted,
        "validation_family_receipt": receipt,
        "full_family_validation_complete": all_sufficient,
        "holm_applied_to_full_family": True,
        "8h_g_holdout_evaluation_eligible": all_sufficient,
        "validation_outcomes_opened": True,
        "holdout_outcomes_authorized": False,
        "model_refit": False,
        "family_shrunk_or_expanded": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-G_HOLDOUT_PROSPECTIVE_CONFIRMATION" if all_sufficient else "8H-F_INSUFFICIENT_VALIDATION_EVIDENCE",
    }
    family_result["validation_sha256"] = _digest(family_result)
    return family_result
