"""Phase 8H-D outcome-blind interaction dataset/model construction.

This module materializes paired numeric design matrices for the complete
Phase-8H-C frozen interaction family. It never reads forward outcomes, fits a
preprocessor, fits an estimator, makes predictions, or computes losses.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Mapping, Sequence

import pandas as pd

from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    INTERACTION_SPEC_CONTRACT_SCHEMA,
    SPECS_FROZEN,
)


INTERACTION_MODEL_CONTRACT_SCHEMA = "external_evidence_8h_interaction_model_construction_v1"
INTERACTION_MODEL_RESULT_SCHEMA = "external_evidence_8h_interaction_model_construction_result_v1"
WAITING_FOR_SPECS = "WAITING_FOR_FROZEN_INTERACTION_SPECS"
DESIGN_FAMILY_CONSTRUCTED = "INTERACTION_DESIGN_FAMILY_CONSTRUCTED"
IDENTITY_COLUMNS = ("snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions")
FORBIDDEN_DECISION_COLUMNS = frozenset({
    "universal_stance",
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "buy_signal",
    "sell_signal",
    "position_size",
    "target_weight",
})
FORBIDDEN_FEATURE_COLUMN_PARTS = (
    "peer_excess",
    "adverse_excursion",
    "path_max_drawdown",
    "label_",
    "future_return",
    "return_5t",
    "return_20t",
    "return_40t",
    "return_60t",
    "end_date_",
    "peer_direction_",
)


class ExternalEvidence8HModelError(ValueError):
    """Raised when Phase-8H-D construction violates the frozen contract."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_embedded_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    expected = str(row.get(field) or "")
    if not expected:
        raise ExternalEvidence8HModelError(error)
    payload = dict(row)
    payload.pop(field, None)
    if _digest(payload) != expected:
        raise ExternalEvidence8HModelError(error)
    return expected


def _as_list(value: object, error: str) -> list[Any]:
    if not isinstance(value, (list, tuple)):
        raise ExternalEvidence8HModelError(error)
    return list(value)


def _json_scalar(value: object) -> object:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()  # type: ignore[assignment]
        except (ValueError, AttributeError):
            pass
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            return str(value)
        return value
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def _frame_digest(frame: pd.DataFrame, columns: Sequence[str]) -> str:
    records: list[dict[str, object]] = []
    for row in frame.loc[:, list(columns)].itertuples(index=False, name=None):
        records.append({column: _json_scalar(value) for column, value in zip(columns, row)})
    return _digest({"columns": list(columns), "records": records})


def assert_feature_frame_outcome_blind(frame: pd.DataFrame) -> None:
    forbidden_exact = sorted(FORBIDDEN_DECISION_COLUMNS.intersection(str(c) for c in frame.columns))
    if forbidden_exact:
        raise ExternalEvidence8HModelError(
            "8h_d_forbidden_decision_columns:" + ",".join(forbidden_exact)
        )
    contaminated: list[str] = []
    for column in frame.columns:
        lowered = str(column).lower()
        if any(part in lowered for part in FORBIDDEN_FEATURE_COLUMN_PARTS):
            contaminated.append(str(column))
    if contaminated:
        raise ExternalEvidence8HModelError(
            "8h_d_outcome_or_forward_label_columns_forbidden:" + ",".join(sorted(contaminated))
        )
    supplied_interactions = [str(c) for c in frame.columns if str(c).startswith("interaction__")]
    if supplied_interactions:
        raise ExternalEvidence8HModelError(
            "8h_d_interaction_values_must_be_computed_not_supplied:" + ",".join(sorted(supplied_interactions))
        )


def validate_model_contract(
    contract: Mapping[str, Any],
    spec_contract: Mapping[str, Any],
    challenger_specs: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != INTERACTION_MODEL_CONTRACT_SCHEMA:
        raise ExternalEvidence8HModelError("8h_d_contract_schema_mismatch")
    if contract.get("phase") != "8H-D":
        raise ExternalEvidence8HModelError("8h_d_contract_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HModelError("8h_d_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HModelError(f"8h_d_forbidden_contract_authorization:{key}")

    if spec_contract.get("schema_version") != INTERACTION_SPEC_CONTRACT_SCHEMA:
        raise ExternalEvidence8HModelError("8h_d_8h_c_contract_schema_mismatch")
    if spec_contract.get("phase") != "8H-C":
        raise ExternalEvidence8HModelError("8h_d_8h_c_contract_phase_mismatch")
    math_contract = spec_contract.get("interaction_math")
    if not isinstance(math_contract, Mapping):
        raise ExternalEvidence8HModelError("8h_d_8h_c_math_contract_missing")
    if math_contract.get("operator") != "ELEMENTWISE_PRODUCT":
        raise ExternalEvidence8HModelError("8h_d_interaction_operator_changed")
    if math_contract.get("operator_applied_to") != "POST_PREPROCESSING_STANDARDIZED_NUMERIC_DESIGN_COLUMNS":
        raise ExternalEvidence8HModelError("8h_d_interaction_input_stage_changed")
    if math_contract.get("both_underlying_main_effects_remain_present") is not True:
        raise ExternalEvidence8HModelError("8h_d_main_effect_retention_not_frozen")

    estimator = contract.get("estimator_state")
    frozen_estimator = challenger_specs.get("estimator")
    if not isinstance(estimator, Mapping) or not isinstance(frozen_estimator, Mapping):
        raise ExternalEvidence8HModelError("8h_d_estimator_contract_missing")
    if estimator.get("family") != frozen_estimator.get("family"):
        raise ExternalEvidence8HModelError("8h_d_estimator_family_changed")
    if float(estimator.get("alpha")) != float(frozen_estimator.get("alpha")):
        raise ExternalEvidence8HModelError("8h_d_estimator_alpha_changed")
    if bool(estimator.get("fit_intercept")) != bool(frozen_estimator.get("fit_intercept")):
        raise ExternalEvidence8HModelError("8h_d_estimator_intercept_changed")
    for key in ("fit_allowed_in_8h_d", "prediction_allowed_in_8h_d", "loss_computation_allowed_in_8h_d"):
        if estimator.get(key) is not False:
            raise ExternalEvidence8HModelError(f"8h_d_estimator_action_must_be_false:{key}")

    outcome = contract.get("outcome_boundary")
    if not isinstance(outcome, Mapping):
        raise ExternalEvidence8HModelError("8h_d_outcome_boundary_missing")
    for key in (
        "market_outcomes_read_in_8h_d",
        "forward_labels_read_in_8h_d",
        "peer_excess_read_in_8h_d",
        "discovery_results_computed_in_8h_d",
        "interaction_effect_direction_computed_in_8h_d",
        "interaction_effect_magnitude_computed_in_8h_d",
        "interaction_rank_computed_in_8h_d",
    ):
        if outcome.get(key) is not False:
            raise ExternalEvidence8HModelError(f"8h_d_outcome_boundary_must_be_false:{key}")


def _rederive_confirmatory_family(specs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    family: list[dict[str, Any]] = []
    for spec in sorted(specs, key=lambda row: str(row.get("interaction_spec_id") or "")):
        spec_id = str(spec.get("interaction_spec_id") or "")
        horizon = int(spec.get("horizon_sessions", -1))
        family.append(
            {
                "hypothesis_id": f"{spec_id}__peer_excess_{horizon}t",
                "interaction_spec_id": spec_id,
                "horizon_sessions": horizon,
                "primary_outcome": "peer_excess",
            }
        )
    return family


def _validate_freeze_result(freeze_result: Mapping[str, Any]) -> tuple[str, list[Mapping[str, Any]], str]:
    if freeze_result.get("schema_version") != INTERACTION_FREEZE_RESULT_SCHEMA:
        raise ExternalEvidence8HModelError("8h_d_freeze_result_schema_mismatch")
    if freeze_result.get("phase") != "8H-C":
        raise ExternalEvidence8HModelError("8h_d_freeze_result_phase_mismatch")
    freeze_sha = _verify_embedded_digest(
        freeze_result, "freeze_sha256", "8h_d_freeze_result_digest_mismatch"
    )
    if freeze_result.get("interaction_outcome_access_authorized") is not False:
        raise ExternalEvidence8HModelError("8h_d_8h_c_outcome_access_must_be_false")
    if freeze_result.get("empirical_interaction_research_enabled") is not False:
        raise ExternalEvidence8HModelError("8h_d_8h_c_empirical_research_must_be_closed")

    state = str(freeze_result.get("state") or "")
    raw_specs = _as_list(freeze_result.get("frozen_interaction_specs") or [], "8h_d_frozen_specs_invalid")
    if state != SPECS_FROZEN:
        if raw_specs or list(freeze_result.get("confirmatory_family") or ()):
            raise ExternalEvidence8HModelError("8h_d_nonfrozen_8h_c_state_must_be_empty")
        if freeze_result.get("interaction_dataset_model_construction_authorized") is not False:
            raise ExternalEvidence8HModelError("8h_d_nonfrozen_8h_c_state_cannot_authorize_construction")
        return state, [], freeze_sha

    if freeze_result.get("interaction_dataset_model_construction_authorized") is not True:
        raise ExternalEvidence8HModelError("8h_d_construction_not_authorized_by_8h_c")
    if not raw_specs:
        raise ExternalEvidence8HModelError("8h_d_frozen_state_requires_specs")

    specs: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_specs:
        if not isinstance(raw, Mapping):
            raise ExternalEvidence8HModelError("8h_d_frozen_spec_row_invalid")
        spec_id = str(raw.get("interaction_spec_id") or "")
        if not spec_id or spec_id in seen:
            raise ExternalEvidence8HModelError("8h_d_duplicate_or_missing_interaction_spec_id")
        seen.add(spec_id)
        _verify_embedded_digest(
            raw, "interaction_spec_sha256", f"8h_d_interaction_spec_digest_mismatch:{spec_id}"
        )
        if raw.get("operator") != "ELEMENTWISE_PRODUCT":
            raise ExternalEvidence8HModelError(f"8h_d_spec_operator_changed:{spec_id}")
        if raw.get("main_effects_retained") is not True or raw.get("exactly_one_new_interaction_term") is not True:
            raise ExternalEvidence8HModelError(f"8h_d_spec_model_shape_invalid:{spec_id}")
        specs.append(raw)

    actual_family = list(freeze_result.get("confirmatory_family") or ())
    expected_family = _rederive_confirmatory_family(specs)
    if actual_family != expected_family:
        raise ExternalEvidence8HModelError("8h_d_confirmatory_family_drift")
    return state, sorted(specs, key=lambda row: str(row["interaction_spec_id"])), freeze_sha


def _component_column(component: Mapping[str, Any]) -> str:
    kind = str(component.get("kind") or "")
    if kind == "CORE_NUMERIC":
        field = str(component.get("resolved_field") or "")
    elif kind == "EXTERNAL_NUMERIC":
        field = str(component.get("feature_field") or "")
    else:
        raise ExternalEvidence8HModelError(f"8h_d_unknown_component_kind:{kind}")
    if not field:
        raise ExternalEvidence8HModelError("8h_d_interaction_component_field_missing")
    return field


def _build_pair(
    *,
    frame: pd.DataFrame,
    main_effect_columns: Sequence[str],
    spec: Mapping[str, Any],
    freeze_sha: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    assert_feature_frame_outcome_blind(frame)
    spec_id = str(spec["interaction_spec_id"])
    horizon = int(spec["horizon_sessions"])
    components = _as_list(spec.get("components") or [], "8h_d_spec_components_invalid")
    if len(components) != 2 or any(not isinstance(row, Mapping) for row in components):
        raise ExternalEvidence8HModelError(f"8h_d_spec_requires_two_components:{spec_id}")

    missing_identity = sorted(set(IDENTITY_COLUMNS).difference(str(c) for c in frame.columns))
    if missing_identity:
        raise ExternalEvidence8HModelError(
            "8h_d_identity_columns_missing:" + ",".join(missing_identity)
        )
    if frame.empty:
        raise ExternalEvidence8HModelError(f"8h_d_feature_frame_empty:{spec_id}")
    if frame[list(IDENTITY_COLUMNS)].isna().any(axis=None):
        raise ExternalEvidence8HModelError(f"8h_d_identity_value_missing:{spec_id}")
    if frame.duplicated(list(IDENTITY_COLUMNS)).any():
        raise ExternalEvidence8HModelError(f"8h_d_duplicate_row_identity:{spec_id}")
    observed_horizons = {int(x) for x in pd.to_numeric(frame["horizon_sessions"], errors="raise").tolist()}
    if observed_horizons != {horizon}:
        raise ExternalEvidence8HModelError(f"8h_d_mixed_or_wrong_horizon:{spec_id}")

    main_effects = [str(x) for x in main_effect_columns]
    if not main_effects or len(set(main_effects)) != len(main_effects):
        raise ExternalEvidence8HModelError(f"8h_d_main_effect_columns_invalid:{spec_id}")
    if set(main_effects).intersection(IDENTITY_COLUMNS):
        raise ExternalEvidence8HModelError(f"8h_d_identity_cannot_be_main_effect:{spec_id}")
    missing_main = sorted(set(main_effects).difference(str(c) for c in frame.columns))
    if missing_main:
        raise ExternalEvidence8HModelError(
            f"8h_d_main_effect_columns_missing:{spec_id}:" + ",".join(missing_main)
        )

    component_columns = [_component_column(row) for row in components]  # type: ignore[arg-type]
    for column in component_columns:
        if column not in main_effects:
            raise ExternalEvidence8HModelError(
                f"8h_d_interaction_component_not_retained_as_main_effect:{spec_id}:{column}"
            )

    numeric = pd.DataFrame(index=frame.index)
    for column in main_effects:
        raw = frame[column]
        converted = pd.to_numeric(raw, errors="coerce")
        invalid_text = raw.notna() & converted.isna()
        if invalid_text.any():
            raise ExternalEvidence8HModelError(f"8h_d_nonnumeric_main_effect:{spec_id}:{column}")
        numeric[column] = converted.astype(float)

    finite_mask = pd.Series(True, index=frame.index)
    for column in main_effects:
        finite_mask &= numeric[column].notna() & numeric[column].map(math.isfinite)
    kept = frame.loc[finite_mask, list(IDENTITY_COLUMNS)].copy()
    kept_numeric = numeric.loc[finite_mask, main_effects].copy()
    if kept.empty:
        raise ExternalEvidence8HModelError(f"8h_d_no_complete_paired_rows:{spec_id}")

    baseline = pd.concat([kept.reset_index(drop=True), kept_numeric.reset_index(drop=True)], axis=1)
    challenger = baseline.copy()
    interaction_column = f"interaction__{spec_id}"
    if interaction_column in frame.columns or interaction_column in baseline.columns:
        raise ExternalEvidence8HModelError(f"8h_d_interaction_column_collision:{spec_id}")
    challenger[interaction_column] = (
        challenger[component_columns[0]].astype(float) * challenger[component_columns[1]].astype(float)
    )
    if not challenger[interaction_column].map(math.isfinite).all():
        raise ExternalEvidence8HModelError(f"8h_d_nonfinite_interaction_value:{spec_id}")

    baseline_columns = list(IDENTITY_COLUMNS) + main_effects
    challenger_columns = baseline_columns + [interaction_column]
    if not baseline[list(IDENTITY_COLUMNS)].equals(challenger[list(IDENTITY_COLUMNS)]):
        raise ExternalEvidence8HModelError(f"8h_d_row_identity_drift:{spec_id}")
    if not baseline[main_effects].equals(challenger[main_effects]):
        raise ExternalEvidence8HModelError(f"8h_d_main_effect_value_drift:{spec_id}")

    receipt = {
        "interaction_spec_id": spec_id,
        "interaction_spec_sha256": str(spec["interaction_spec_sha256"]),
        "8h_c_freeze_sha256": freeze_sha,
        "confirmatory_hypothesis_id": f"{spec_id}__peer_excess_{horizon}t",
        "horizon_sessions": horizon,
        "component_columns": component_columns,
        "interaction_column": interaction_column,
        "baseline_columns": baseline_columns,
        "challenger_columns": challenger_columns,
        "input_rows": int(len(frame)),
        "paired_rows": int(len(baseline)),
        "dropped_missing_or_nonfinite_rows": int(len(frame) - len(baseline)),
        "row_identity_sha256": _frame_digest(baseline, IDENTITY_COLUMNS),
        "baseline_design_sha256": _frame_digest(baseline, baseline_columns),
        "challenger_design_sha256": _frame_digest(challenger, challenger_columns),
        "outcomes_read": False,
        "preprocessing_fit_in_8h_d": False,
        "estimator_fit": False,
        "predictions_computed": False,
        "losses_computed": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    receipt["construction_sha256"] = _digest(receipt)
    return baseline, challenger, receipt


def _waiting_receipt(upstream_state: str, freeze_sha: str) -> dict[str, Any]:
    row = {
        "schema_version": INTERACTION_MODEL_RESULT_SCHEMA,
        "phase": "8H-D",
        "state": WAITING_FOR_SPECS,
        "8h_c_state": upstream_state,
        "8h_c_freeze_sha256": freeze_sha,
        "confirmatory_family": [],
        "constructed_interaction_designs": [],
        "8h_e_discovery_evaluation_eligible": False,
        "interaction_outcome_access_authorized": False,
        "validation_or_holdout_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-D_WAIT_FOR_8H_C_NONEMPTY_FREEZE",
    }
    row["construction_sha256"] = _digest(row)
    return row


def construct_interaction_design_family(
    *,
    contract: Mapping[str, Any],
    spec_contract: Mapping[str, Any],
    challenger_specs: Mapping[str, Any],
    freeze_result: Mapping[str, Any],
    standardized_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    main_effect_columns_by_spec: Mapping[str, Sequence[str]] | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame], dict[str, Any]]:
    """Construct the complete 8H-C family atomically without reading outcomes."""
    validate_model_contract(contract, spec_contract, challenger_specs)
    upstream_state, specs, freeze_sha = _validate_freeze_result(freeze_result)
    frames = dict(standardized_frames_by_spec or {})
    columns = dict(main_effect_columns_by_spec or {})

    if upstream_state != SPECS_FROZEN:
        if frames or columns:
            raise ExternalEvidence8HModelError("8h_d_feature_inputs_forbidden_without_frozen_specs")
        return {}, {}, _waiting_receipt(upstream_state, freeze_sha)

    spec_ids = [str(spec["interaction_spec_id"]) for spec in specs]
    if set(frames) != set(spec_ids):
        raise ExternalEvidence8HModelError("8h_d_feature_frame_family_must_equal_frozen_spec_family")
    if set(columns) != set(spec_ids):
        raise ExternalEvidence8HModelError("8h_d_main_effect_family_must_equal_frozen_spec_family")

    baseline_frames: dict[str, pd.DataFrame] = {}
    challenger_frames: dict[str, pd.DataFrame] = {}
    receipts: list[dict[str, Any]] = []
    by_id = {str(spec["interaction_spec_id"]): spec for spec in specs}
    for spec_id in sorted(spec_ids):
        baseline, challenger, receipt = _build_pair(
            frame=frames[spec_id],
            main_effect_columns=columns[spec_id],
            spec=by_id[spec_id],
            freeze_sha=freeze_sha,
        )
        baseline_frames[spec_id] = baseline
        challenger_frames[spec_id] = challenger
        receipts.append(receipt)

    confirmatory_family = list(freeze_result["confirmatory_family"])
    result = {
        "schema_version": INTERACTION_MODEL_RESULT_SCHEMA,
        "phase": "8H-D",
        "state": DESIGN_FAMILY_CONSTRUCTED,
        "8h_c_state": SPECS_FROZEN,
        "8h_c_freeze_sha256": freeze_sha,
        "confirmatory_family": confirmatory_family,
        "constructed_interaction_designs": receipts,
        "8h_e_discovery_evaluation_eligible": True,
        "interaction_outcome_access_authorized": False,
        "validation_or_holdout_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-E_DISCOVERY_EVALUATION",
    }
    result["construction_sha256"] = _digest(result)
    return baseline_frames, challenger_frames, result
