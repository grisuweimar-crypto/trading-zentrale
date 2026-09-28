"""Phase 8G-C outcome-blind baseline/single-factor challenger construction.

This module deliberately stops before preprocessing or estimator fitting. It
constructs paired feature rows only after the Phase-8G-B feature-side split
binding is frozen, and it refuses any forward-label/outcome columns.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

import pandas as pd

from scanner.research.external_evidence.research_8g import (
    ACTIVE_FACTORS,
    HORIZONS,
    active_factor_mappings,
    factor_snapshot_eligibility,
    validate_challenger_specs,
    validate_split_plan,
)
from scanner.research.external_evidence.research_8g_binding_guard import (
    validate_guarded_manifest,
)


MODEL_CONTRACT_SCHEMA = "external_evidence_8g_model_construction_v1"
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
IDENTITY_COLUMNS = ("snapshot_id", "as_of", "generated_at", "symbol", "horizon_sessions")


class ExternalEvidence8GCError(ValueError):
    """Raised when Phase-8G-C construction violates the frozen contract."""


def _utc_timestamp(value: object) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ExternalEvidence8GCError(f"invalid_timestamp:{value}")
    return pd.Timestamp(parsed)


def _iso_day(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ExternalEvidence8GCError(f"invalid_as_of:{value}")
    return pd.Timestamp(parsed).date().isoformat()


def _resolved(name: object, horizon: int) -> str:
    return str(name).replace("{H}", str(horizon))


def resolved_baseline_columns(
    specs: Mapping[str, Any], horizon_sessions: int
) -> tuple[list[str], list[str]]:
    validate_challenger_specs(specs)
    if horizon_sessions not in HORIZONS:
        raise ExternalEvidence8GCError(f"unsupported_horizon:{horizon_sessions}")
    baseline = specs.get("scanner_baseline")
    if not isinstance(baseline, Mapping):
        raise ExternalEvidence8GCError("scanner_baseline_missing")
    numeric = [_resolved(name, horizon_sessions) for name in baseline.get("numeric_features") or ()]
    categorical = [_resolved(name, horizon_sessions) for name in baseline.get("categorical_features") or ()]
    if not numeric or not categorical:
        raise ExternalEvidence8GCError("scanner_baseline_features_missing")
    if len(set(numeric + categorical)) != len(numeric) + len(categorical):
        raise ExternalEvidence8GCError("duplicate_resolved_baseline_feature")
    return numeric, categorical


def assert_feature_frame_outcome_blind(frame: pd.DataFrame) -> None:
    forbidden_exact = sorted(FORBIDDEN_DECISION_COLUMNS.intersection(frame.columns))
    if forbidden_exact:
        raise ExternalEvidence8GCError(
            "forbidden_decision_columns:" + ",".join(forbidden_exact)
        )
    contaminated = []
    for column in frame.columns:
        lowered = str(column).lower()
        if any(part in lowered for part in FORBIDDEN_FEATURE_COLUMN_PARTS):
            contaminated.append(str(column))
    if contaminated:
        raise ExternalEvidence8GCError(
            "outcome_or_forward_label_columns_forbidden:" + ",".join(sorted(contaminated))
        )


def validate_model_contract(
    contract: Mapping[str, Any], specs: Mapping[str, Any], plan: Mapping[str, Any]
) -> None:
    validate_challenger_specs(specs)
    validate_split_plan(plan, specs)
    if contract.get("schema_version") != MODEL_CONTRACT_SCHEMA:
        raise ExternalEvidence8GCError("unsupported_model_construction_contract")
    if contract.get("phase") != "8G-C":
        raise ExternalEvidence8GCError("model_construction_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8GCError("model_construction_must_be_research_only")
    if contract.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GCError("productive_integration_must_remain_disabled")
    if tuple(contract.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GCError("active_factor_ids_mismatch")
    if tuple(int(x) for x in contract.get("horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8GCError("horizons_mismatch")

    construction = contract.get("construction_scope")
    if not isinstance(construction, Mapping):
        raise ExternalEvidence8GCError("construction_scope_missing")
    if construction.get("cross_factor_features_allowed") is not False:
        raise ExternalEvidence8GCError("cross_factor_features_forbidden")
    if construction.get("multiple_factor_families_per_challenger_allowed") is not False:
        raise ExternalEvidence8GCError("single_factor_challenger_required")
    if construction.get("baseline_external_evidence_allowed") is not False:
        raise ExternalEvidence8GCError("baseline_must_not_contain_external_evidence")
    for key in (
        "same_snapshot_required",
        "same_symbol_required",
        "same_horizon_required",
        "same_baseline_values_required",
        "same_row_set_required",
    ):
        if construction.get(key) is not True:
            raise ExternalEvidence8GCError(f"pairing_invariant_required:{key}")

    input_contract = contract.get("input_contract")
    if not isinstance(input_contract, Mapping):
        raise ExternalEvidence8GCError("input_contract_missing")
    if input_contract.get("exact_snapshot_identity_required") is not True:
        raise ExternalEvidence8GCError("exact_snapshot_identity_required")
    if input_contract.get("date_only_snapshot_join_allowed") is not False:
        raise ExternalEvidence8GCError("date_only_snapshot_join_forbidden")
    if input_contract.get("outcome_or_forward_label_columns_allowed") is not False:
        raise ExternalEvidence8GCError("outcome_columns_must_be_forbidden")
    if input_contract.get("bound_8g_b_assignment_required") is not True:
        raise ExternalEvidence8GCError("8g_b_binding_required")

    preprocessing = contract.get("preprocessing_state")
    estimator = contract.get("estimator_state")
    if not isinstance(preprocessing, Mapping) or preprocessing.get("status") != "NOT_FIT_IN_8G_C":
        raise ExternalEvidence8GCError("preprocessing_must_remain_unfit_in_8g_c")
    if not isinstance(estimator, Mapping) or estimator.get("status") != "NOT_FIT_IN_8G_C":
        raise ExternalEvidence8GCError("estimator_must_remain_unfit_in_8g_c")
    if estimator.get("fit_allowed_in_8g_c") is not False:
        raise ExternalEvidence8GCError("estimator_fit_forbidden_in_8g_c")
    frozen_estimator = specs.get("estimator")
    if not isinstance(frozen_estimator, Mapping):
        raise ExternalEvidence8GCError("8g_b_estimator_missing")
    if estimator.get("family") != frozen_estimator.get("family"):
        raise ExternalEvidence8GCError("estimator_family_changed_after_8g_b")
    if float(estimator.get("alpha")) != float(frozen_estimator.get("alpha")):
        raise ExternalEvidence8GCError("estimator_alpha_changed_after_8g_b")

    guards = contract.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GCError("8g_c_guards_must_remain_false")


def _source_filtered_ledger(
    macro_ledger: Mapping[str, Any], specs: Mapping[str, Any]
) -> dict[str, Any]:
    rows = macro_ledger.get("observations")
    if not isinstance(rows, list):
        raise ExternalEvidence8GCError("macro_ledger_observations_missing")
    filtered: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        factor_id = str(row.get("factor_id") or "")
        if factor_id not in ACTIVE_FACTORS:
            continue
        factor_spec = specs["factor_specs"][factor_id]
        expected_series = {str(x) for x in factor_spec["series_ids"]}
        series_id = str(row.get("series_id") or "")
        if series_id not in expected_series:
            continue
        expected_source = str(factor_spec["source_id"])
        source_id = str(row.get("source_id") or "")
        if source_id != expected_source:
            raise ExternalEvidence8GCError(
                f"frozen_source_mismatch:{factor_id}:{series_id}:{source_id}"
            )
        filtered.append(row)
    out = dict(macro_ledger)
    out["observations"] = filtered
    return out


def _snapshot_identity(
    frame: pd.DataFrame, snapshot_metadata: Mapping[str, Any]
) -> tuple[str, str, pd.Timestamp]:
    if frame.empty:
        raise ExternalEvidence8GCError("prepared_baseline_frame_empty")
    required = {"snapshot_id", "as_of", "generated_at", "symbol"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ExternalEvidence8GCError("prepared_baseline_identity_missing:" + ",".join(missing))

    snapshot_id = str(snapshot_metadata.get("snapshot_id") or "").strip()
    as_of = _iso_day(snapshot_metadata.get("as_of"))
    generated_raw = snapshot_metadata.get("generated_at") or snapshot_metadata.get("views_generated_at")
    if not snapshot_id or generated_raw is None:
        raise ExternalEvidence8GCError("snapshot_metadata_identity_incomplete")
    generated_at = _utc_timestamp(generated_raw)

    frame_snapshot_ids = {str(x).strip() for x in frame["snapshot_id"].tolist()}
    if frame_snapshot_ids != {snapshot_id}:
        raise ExternalEvidence8GCError("prepared_frame_snapshot_id_mismatch")
    frame_as_of = {_iso_day(x) for x in frame["as_of"].tolist()}
    if frame_as_of != {as_of}:
        raise ExternalEvidence8GCError("prepared_frame_as_of_mismatch")
    frame_generated = {_utc_timestamp(x).isoformat() for x in frame["generated_at"].tolist()}
    if frame_generated != {generated_at.isoformat()}:
        raise ExternalEvidence8GCError("prepared_frame_generated_at_mismatch")

    symbols = frame["symbol"].astype(str).str.strip()
    if symbols.eq("").any():
        raise ExternalEvidence8GCError("prepared_frame_symbol_missing")
    if symbols.duplicated().any():
        raise ExternalEvidence8GCError("duplicate_symbol_within_snapshot")
    return snapshot_id, as_of, generated_at


def _bound_assignment(
    *,
    manifest: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
    factor_id: str,
    horizon_sessions: int,
    snapshot_id: str,
    as_of: str,
) -> Mapping[str, Any]:
    validate_guarded_manifest(manifest, specs, plan)
    stream_key = f"{factor_id}_x_{horizon_sessions}t"
    stream = manifest["streams"].get(stream_key)
    if not isinstance(stream, Mapping):
        raise ExternalEvidence8GCError(f"8g_b_stream_missing:{stream_key}")
    matches = [
        item for item in stream.get("assignments") or ()
        if str(item.get("snapshot_id")) == snapshot_id and str(item.get("as_of")) == as_of
    ]
    if len(matches) != 1:
        raise ExternalEvidence8GCError(
            f"snapshot_not_uniquely_bound_in_8g_b:{stream_key}:{snapshot_id}:{as_of}"
        )
    return matches[0]


def _relationship_classes_by_symbol(
    exposure_map: Mapping[str, Any], factor_id: str, generated_at: pd.Timestamp
) -> dict[str, set[str]]:
    active = active_factor_mappings(exposure_map, factor_id, generated_at.to_pydatetime())
    grouped: dict[str, set[str]] = defaultdict(set)
    for mapping in active:
        symbol = str(mapping.get("subject_id") or "").strip()
        relationship = str(mapping.get("relationship_class") or "").strip()
        if symbol and relationship:
            grouped[symbol].add(relationship)
    return dict(grouped)


def build_paired_feature_frames(
    *,
    prepared_baseline_frame: pd.DataFrame,
    snapshot_metadata: Mapping[str, Any],
    factor_id: str,
    horizon_sessions: int,
    macro_ledger: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
    specs: Mapping[str, Any],
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    model_contract: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Construct paired baseline/challenger rows without reading outcomes.

    The input frame must already contain the frozen Scanner_vNext baseline
    feature state for one exact snapshot. Phase 8G-C does not fit a scaler,
    encoder or estimator and does not read any forward label values.
    """
    validate_model_contract(model_contract, specs, plan)
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GCError(f"factor_not_active_confirmatory:{factor_id}")
    if horizon_sessions not in HORIZONS:
        raise ExternalEvidence8GCError(f"unsupported_horizon:{horizon_sessions}")
    assert_feature_frame_outcome_blind(prepared_baseline_frame)
    snapshot_id, as_of, generated_at = _snapshot_identity(
        prepared_baseline_frame, snapshot_metadata
    )

    numeric, categorical = resolved_baseline_columns(specs, horizon_sessions)
    required_features = numeric + categorical
    missing = sorted(set(required_features).difference(prepared_baseline_frame.columns))
    if missing:
        raise ExternalEvidence8GCError(
            "prepared_baseline_features_missing:" + ",".join(missing)
        )
    entirely_missing_numeric = [
        column for column in numeric
        if pd.to_numeric(prepared_baseline_frame[column], errors="coerce").notna().sum() == 0
    ]
    if entirely_missing_numeric:
        raise ExternalEvidence8GCError(
            "entirely_missing_required_numeric_feature:" + ",".join(entirely_missing_numeric)
        )

    assignment = _bound_assignment(
        manifest=manifest,
        specs=specs,
        plan=plan,
        factor_id=factor_id,
        horizon_sessions=horizon_sessions,
        snapshot_id=snapshot_id,
        as_of=as_of,
    )

    filtered_ledger = _source_filtered_ledger(macro_ledger, specs)
    eligibility = factor_snapshot_eligibility(
        factor_id=factor_id,
        snapshot_metadata=snapshot_metadata,
        macro_ledger=filtered_ledger,
        exposure_map=exposure_map,
        specs=specs,
    )
    if eligibility.get("eligible") is not True:
        raise ExternalEvidence8GCError(
            f"factor_snapshot_no_longer_eligible:{factor_id}:{eligibility.get('reason')}"
        )
    if str(assignment.get("factor_state_sha256")) != str(eligibility["factor_state_sha256"]):
        raise ExternalEvidence8GCError("8g_b_factor_state_hash_mismatch")
    if str(assignment.get("active_mapping_ids_sha256")) != str(eligibility["active_mapping_ids_sha256"]):
        raise ExternalEvidence8GCError("8g_b_mapping_hash_mismatch")
    if int(assignment.get("active_mapping_count")) != int(eligibility["active_mapping_count"]):
        raise ExternalEvidence8GCError("8g_b_mapping_count_mismatch")

    relationship_classes = _relationship_classes_by_symbol(
        exposure_map, factor_id, generated_at
    )
    work = prepared_baseline_frame.copy()
    work["symbol"] = work["symbol"].astype(str).str.strip()
    unmapped_mask = ~work["symbol"].isin(relationship_classes)
    ambiguous_symbols = {
        symbol for symbol, classes in relationship_classes.items() if len(classes) != 1
    }
    ambiguous_mask = work["symbol"].isin(ambiguous_symbols)
    eligible_mask = ~unmapped_mask & ~ambiguous_mask
    paired = work.loc[eligible_mask].copy()
    if paired.empty:
        raise ExternalEvidence8GCError("no_unambiguous_mapped_rows_for_factor")
    paired = paired.sort_values("symbol", kind="mergesort").reset_index(drop=True)

    identity = paired[["snapshot_id", "as_of", "generated_at", "symbol"]].copy()
    identity["horizon_sessions"] = int(horizon_sessions)
    baseline = pd.concat(
        [identity.reset_index(drop=True), paired[required_features].reset_index(drop=True)],
        axis=1,
    )

    challenger = baseline.copy()
    factor_fields = [str(x) for x in specs["factor_specs"][factor_id]["feature_fields"]]
    feature_values = eligibility.get("feature_values")
    if not isinstance(feature_values, Mapping):
        raise ExternalEvidence8GCError("eligible_factor_features_missing")
    if set(feature_values) != set(factor_fields):
        raise ExternalEvidence8GCError("factor_feature_fields_changed_after_8g_b")
    for field in factor_fields:
        challenger[f"external__{field}"] = float(feature_values[field])
    challenger["external_relationship_class"] = [
        next(iter(relationship_classes[symbol])) for symbol in challenger["symbol"]
    ]

    metadata: dict[str, Any] = {
        "schema_version": "external_evidence_8g_model_construction_result_v1",
        "phase": "8G-C",
        "factor_id": factor_id,
        "horizon_sessions": int(horizon_sessions),
        "snapshot_id": snapshot_id,
        "as_of": as_of,
        "split": str(assignment.get("split")),
        "slot_ordinal": int(assignment.get("ordinal")),
        "slot_usable": bool(assignment.get("usable")),
        "paired_rows": int(len(baseline)),
        "excluded_unmapped_rows": int(unmapped_mask.sum()),
        "excluded_ambiguous_mapping_rows": int(ambiguous_mask.sum()),
        "factor_state_sha256": str(eligibility["factor_state_sha256"]),
        "active_mapping_ids_sha256": str(eligibility["active_mapping_ids_sha256"]),
        "baseline_feature_columns": required_features,
        "external_factor_feature_columns": [f"external__{field}" for field in factor_fields],
        "outcomes_read": False,
        "preprocessing_fit": False,
        "estimator_fit": False,
    }
    validate_paired_feature_frames(baseline, challenger, metadata, specs)
    return baseline, challenger, metadata


def validate_paired_feature_frames(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
    metadata: Mapping[str, Any],
    specs: Mapping[str, Any],
) -> None:
    assert_feature_frame_outcome_blind(baseline)
    assert_feature_frame_outcome_blind(challenger)
    if len(baseline) != len(challenger) or baseline.empty:
        raise ExternalEvidence8GCError("paired_row_count_mismatch")
    missing_identity = sorted(set(IDENTITY_COLUMNS).difference(baseline.columns))
    if missing_identity:
        raise ExternalEvidence8GCError("paired_identity_missing:" + ",".join(missing_identity))
    for column in IDENTITY_COLUMNS:
        if not baseline[column].equals(challenger[column]):
            raise ExternalEvidence8GCError(f"paired_identity_mismatch:{column}")
    baseline_external = [column for column in baseline.columns if str(column).startswith("external")]
    if baseline_external:
        raise ExternalEvidence8GCError("baseline_contains_external_evidence")

    factor_id = str(metadata.get("factor_id") or "")
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GCError("metadata_factor_invalid")
    expected_external = {
        f"external__{field}" for field in specs["factor_specs"][factor_id]["feature_fields"]
    }
    actual_external = {
        str(column) for column in challenger.columns if str(column).startswith("external__")
    }
    if actual_external != expected_external:
        raise ExternalEvidence8GCError("challenger_external_factor_family_mismatch")
    if "external_relationship_class" not in challenger.columns:
        raise ExternalEvidence8GCError("challenger_relationship_class_missing")

    baseline_feature_columns = [str(x) for x in metadata.get("baseline_feature_columns") or ()]
    for column in baseline_feature_columns:
        if not baseline[column].equals(challenger[column]):
            raise ExternalEvidence8GCError(f"challenger_changed_baseline_feature:{column}")
    if metadata.get("outcomes_read") is not False:
        raise ExternalEvidence8GCError("8g_c_must_not_read_outcomes")
    if metadata.get("preprocessing_fit") is not False:
        raise ExternalEvidence8GCError("8g_c_must_not_fit_preprocessing")
    if metadata.get("estimator_fit") is not False:
        raise ExternalEvidence8GCError("8g_c_must_not_fit_estimator")
