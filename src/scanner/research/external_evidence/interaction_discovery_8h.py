"""Phase 8H-E Discovery-only interaction evaluation and model freeze.

The Phase-8H-C hypothesis family is already frozen before this module may read
any interaction outcomes. 8H-E binds a chronological 8H split manifest, opens
DISCOVERY labels only, fits the fixed Ridge baseline/challenger pair for every
frozen interaction, and freezes those models for 8H-F. Discovery results may
never shrink, expand, invert, tune, or otherwise mutate the frozen family.
"""
from __future__ import annotations

from hashlib import sha256
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.research.external_evidence.discovery_fit_8g import fit_fixed_ridge
from scanner.research.external_evidence.interaction_model_8h import (
    DESIGN_FAMILY_CONSTRUCTED,
    IDENTITY_COLUMNS,
    INTERACTION_MODEL_CONTRACT_SCHEMA,
    INTERACTION_MODEL_RESULT_SCHEMA,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
)


DISCOVERY_CONTRACT_SCHEMA = "external_evidence_8h_interaction_discovery_evaluation_v1"
SPLIT_MANIFEST_SCHEMA = "external_evidence_8h_interaction_split_manifest_v1"
DISCOVERY_RESULT_SCHEMA = "external_evidence_8h_interaction_discovery_result_v1"
WAITING_FOR_DESIGNS = "WAITING_FOR_CONSTRUCTED_INTERACTION_DESIGNS"
DISCOVERY_FAMILY_FROZEN = "DISCOVERY_FAMILY_MODELS_FROZEN"
DISCOVERY_FAMILY_INSUFFICIENT = "DISCOVERY_FAMILY_INSUFFICIENT_EVIDENCE"
OUTCOME_IDENTITY = tuple(IDENTITY_COLUMNS)
SPLIT_ORDER = {"DISCOVERY": 0, "VALIDATION": 1, "HOLDOUT": 2}


class ExternalEvidence8HDiscoveryError(ValueError):
    """Raised when 8H-E violates the frozen interaction research protocol."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_embedded_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    expected = str(row.get(field) or "")
    if not expected:
        raise ExternalEvidence8HDiscoveryError(error)
    payload = dict(row)
    payload.pop(field, None)
    if _digest(payload) != expected:
        raise ExternalEvidence8HDiscoveryError(error)
    return expected


def _timestamp(value: object, error: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ExternalEvidence8HDiscoveryError(error)
    return pd.Timestamp(parsed)


def _day(value: object, error: str = "8h_e_invalid_date") -> pd.Timestamp:
    return _timestamp(value, error).normalize()


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
    records: list[dict[str, object]] = []
    for row in frame.loc[:, list(columns)].itertuples(index=False, name=None):
        records.append({column: _json_scalar(value) for column, value in zip(columns, row)})
    return _digest({"columns": list(columns), "records": records})


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((int(base) + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def validate_discovery_contract(
    contract: Mapping[str, Any],
    model_contract: Mapping[str, Any],
) -> None:
    if contract.get("schema_version") != DISCOVERY_CONTRACT_SCHEMA:
        raise ExternalEvidence8HDiscoveryError("8h_e_contract_schema_mismatch")
    if contract.get("phase") != "8H-E":
        raise ExternalEvidence8HDiscoveryError("8h_e_contract_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HDiscoveryError("8h_e_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase7_mutation_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_forbidden_contract_authorization:{key}")

    if model_contract.get("schema_version") != INTERACTION_MODEL_CONTRACT_SCHEMA:
        raise ExternalEvidence8HDiscoveryError("8h_e_8h_d_contract_schema_mismatch")
    estimator = contract.get("estimator_freeze")
    upstream = model_contract.get("estimator_state")
    if not isinstance(estimator, Mapping) or not isinstance(upstream, Mapping):
        raise ExternalEvidence8HDiscoveryError("8h_e_estimator_contract_missing")
    if estimator.get("family") != upstream.get("family"):
        raise ExternalEvidence8HDiscoveryError("8h_e_estimator_family_changed")
    if float(estimator.get("alpha")) != float(upstream.get("alpha")) or float(estimator.get("alpha")) != 1.0:
        raise ExternalEvidence8HDiscoveryError("8h_e_ridge_alpha_changed")
    if bool(estimator.get("fit_intercept")) != bool(upstream.get("fit_intercept")):
        raise ExternalEvidence8HDiscoveryError("8h_e_intercept_setting_changed")
    for key in (
        "hyperparameter_tuning_allowed",
        "feature_selection_allowed",
        "sign_search_allowed",
        "threshold_search_allowed",
        "transform_search_allowed",
        "validation_refit_allowed",
        "holdout_refit_allowed",
    ):
        if estimator.get(key) is not False:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_search_or_refit_must_be_false:{key}")

    family = contract.get("family_integrity")
    if not isinstance(family, Mapping):
        raise ExternalEvidence8HDiscoveryError("8h_e_family_integrity_missing")
    for key in (
        "family_must_remain_unchanged_after_discovery_outcomes",
        "full_family_model_freeze_required_before_8h_f",
    ):
        if family.get(key) is not True:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_family_guard_required:{key}")
    for key in (
        "discovery_effect_sign_may_change_family_membership",
        "discovery_effect_magnitude_may_change_family_membership",
        "discovery_p_value_may_change_family_membership",
        "failed_discovery_hypothesis_inversion_allowed",
        "interaction_spec_mutation_after_discovery_allowed",
        "main_effect_mutation_after_discovery_allowed",
        "partial_family_validation_release_allowed",
    ):
        if family.get(key) is not False:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_family_mutation_must_be_false:{key}")

    access = contract.get("discovery_outcome_access")
    if not isinstance(access, Mapping) or access.get("allowed_split") != "DISCOVERY":
        raise ExternalEvidence8HDiscoveryError("8h_e_only_discovery_split_may_open")
    if access.get("validation_outcomes_allowed_in_8h_e") is not False or access.get("holdout_outcomes_allowed_in_8h_e") is not False:
        raise ExternalEvidence8HDiscoveryError("8h_e_validation_holdout_must_remain_sealed")


def _validate_freeze_and_design(
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]], str, str]:
    if freeze_result.get("schema_version") != INTERACTION_FREEZE_RESULT_SCHEMA:
        raise ExternalEvidence8HDiscoveryError("8h_e_freeze_result_schema_mismatch")
    freeze_sha = _verify_embedded_digest(
        freeze_result, "freeze_sha256", "8h_e_freeze_result_digest_mismatch"
    )
    if design_result.get("schema_version") != INTERACTION_MODEL_RESULT_SCHEMA:
        raise ExternalEvidence8HDiscoveryError("8h_e_design_result_schema_mismatch")
    design_sha = _verify_embedded_digest(
        design_result, "construction_sha256", "8h_e_design_result_digest_mismatch"
    )
    if str(design_result.get("8h_c_freeze_sha256") or "") != freeze_sha:
        raise ExternalEvidence8HDiscoveryError("8h_e_design_freeze_binding_mismatch")
    if list(design_result.get("confirmatory_family") or ()) != list(freeze_result.get("confirmatory_family") or ()):
        raise ExternalEvidence8HDiscoveryError("8h_e_confirmatory_family_drift")

    state = str(design_result.get("state") or "")
    if state != DESIGN_FAMILY_CONSTRUCTED:
        if list(design_result.get("constructed_interaction_designs") or ()):
            raise ExternalEvidence8HDiscoveryError("8h_e_nonconstructed_state_must_have_no_design_receipts")
        return state, [], freeze_sha, design_sha
    if freeze_result.get("state") != SPECS_FROZEN:
        raise ExternalEvidence8HDiscoveryError("8h_e_constructed_design_requires_frozen_specs")
    if design_result.get("8h_e_discovery_evaluation_eligible") is not True:
        raise ExternalEvidence8HDiscoveryError("8h_e_not_authorized_by_8h_d")
    if design_result.get("interaction_outcome_access_authorized") is not False:
        raise ExternalEvidence8HDiscoveryError("8h_e_8h_d_must_not_preopen_outcomes")

    receipts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in design_result.get("constructed_interaction_designs") or ():
        if not isinstance(raw, Mapping):
            raise ExternalEvidence8HDiscoveryError("8h_e_design_receipt_invalid")
        row = dict(raw)
        spec_id = str(row.get("interaction_spec_id") or "")
        if not spec_id or spec_id in seen:
            raise ExternalEvidence8HDiscoveryError("8h_e_duplicate_or_missing_design_spec_id")
        seen.add(spec_id)
        _verify_embedded_digest(
            row, "construction_sha256", f"8h_e_design_receipt_digest_mismatch:{spec_id}"
        )
        if row.get("outcomes_read") is not False or row.get("estimator_fit") is not False or row.get("losses_computed") is not False:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_8h_d_receipt_not_outcome_blind:{spec_id}")
        receipts.append(row)
    family_ids = {str(x.get("interaction_spec_id") or "") for x in freeze_result.get("confirmatory_family") or ()}
    if seen != family_ids:
        raise ExternalEvidence8HDiscoveryError("8h_e_design_receipts_must_equal_full_family")
    return state, sorted(receipts, key=lambda x: x["interaction_spec_id"]), freeze_sha, design_sha


def freeze_interaction_split_manifest(
    *,
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    assignments_by_spec: Mapping[str, Sequence[Mapping[str, Any]]],
    author_identity: str,
    authored_at: object,
    outcomes_read_while_assigning_splits: bool = False,
) -> dict[str, Any]:
    """Freeze the chronological 8H split before any interaction outcome join."""
    state, receipts, freeze_sha, design_sha = _validate_freeze_and_design(freeze_result, design_result)
    if state != DESIGN_FAMILY_CONSTRUCTED:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_requires_constructed_design_family")
    if outcomes_read_while_assigning_splits is not False:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_assignment_must_be_outcome_blind")
    if not str(author_identity or "").strip():
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_author_required")
    authored = _timestamp(authored_at, "8h_e_split_manifest_timestamp_invalid").isoformat()
    freeze_time = _timestamp(freeze_result.get("frozen_at"), "8h_e_8h_c_freeze_timestamp_invalid")

    spec_ids = [str(row["interaction_spec_id"]) for row in receipts]
    if set(assignments_by_spec) != set(spec_ids):
        raise ExternalEvidence8HDiscoveryError("8h_e_split_streams_must_equal_full_family")

    streams: dict[str, Any] = {}
    for spec_id in sorted(spec_ids):
        raw_assignments = list(assignments_by_spec[spec_id])
        if not raw_assignments:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_split_stream_empty:{spec_id}")
        normalized: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for raw in raw_assignments:
            snapshot_id = str(raw.get("snapshot_id") or "")
            as_of = str(raw.get("as_of") or "")
            split = str(raw.get("split") or "").upper()
            usable = raw.get("usable")
            if not snapshot_id or not as_of:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_split_identity_missing:{spec_id}")
            if split not in SPLIT_ORDER:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_split_invalid:{spec_id}:{split}")
            if usable is not True:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_nonusable_split_assignment_forbidden:{spec_id}")
            key = (snapshot_id, as_of)
            if key in seen:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_duplicate_snapshot_assignment:{spec_id}")
            seen.add(key)
            as_of_ts = _timestamp(as_of, f"8h_e_split_as_of_invalid:{spec_id}")
            if as_of_ts < freeze_time:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_split_assignment_predates_spec_freeze:{spec_id}")
            normalized.append({
                "snapshot_id": snapshot_id,
                "as_of": as_of,
                "split": split,
                "usable": True,
            })
        normalized.sort(key=lambda x: (_timestamp(x["as_of"], "8h_e_split_as_of_invalid"), x["snapshot_id"]))
        ranks = [SPLIT_ORDER[row["split"]] for row in normalized]
        if ranks != sorted(ranks):
            raise ExternalEvidence8HDiscoveryError(f"8h_e_split_order_not_chronological:{spec_id}")
        counts = {name: sum(1 for row in normalized if row["split"] == name) for name in SPLIT_ORDER}
        if any(counts[name] < 1 for name in SPLIT_ORDER):
            raise ExternalEvidence8HDiscoveryError(f"8h_e_all_three_splits_required_before_outcome_open:{spec_id}")
        total = len(normalized)
        streams[spec_id] = {
            "horizon_sessions": int(next(row["horizon_sessions"] for row in receipts if row["interaction_spec_id"] == spec_id)),
            "assignments": normalized,
            "counts": counts,
            "realized_fractions": {name: counts[name] / total for name in SPLIT_ORDER},
        }

    manifest: dict[str, Any] = {
        "schema_version": SPLIT_MANIFEST_SCHEMA,
        "phase": "8H-E",
        "author_identity": str(author_identity),
        "authored_at": authored,
        "outcomes_read_while_assigning_splits": False,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "family_members": list(freeze_result.get("confirmatory_family") or ()),
        "target_fractions": {"DISCOVERY": 0.5, "VALIDATION": 0.25, "HOLDOUT": 0.25},
        "streams": streams,
        "split_reassignment_allowed": False,
        "validation_outcomes_opened": False,
        "holdout_outcomes_opened": False,
    }
    manifest["manifest_sha256"] = _digest(manifest)
    return manifest


def _validate_split_manifest(
    manifest: Mapping[str, Any],
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    receipts: Sequence[Mapping[str, Any]],
) -> str:
    if manifest.get("schema_version") != SPLIT_MANIFEST_SCHEMA:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_schema_mismatch")
    manifest_sha = _verify_embedded_digest(
        manifest, "manifest_sha256", "8h_e_split_manifest_digest_mismatch"
    )
    if manifest.get("outcomes_read_while_assigning_splits") is not False:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_outcome_tainted")
    if manifest.get("split_reassignment_allowed") is not False:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_reassignment_must_be_false")
    if manifest.get("validation_outcomes_opened") is not False or manifest.get("holdout_outcomes_opened") is not False:
        raise ExternalEvidence8HDiscoveryError("8h_e_validation_holdout_already_open_in_manifest")
    if str(manifest.get("8h_c_freeze_sha256") or "") != str(freeze_result.get("freeze_sha256") or ""):
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_freeze_binding_mismatch")
    if str(manifest.get("8h_d_construction_sha256") or "") != str(design_result.get("construction_sha256") or ""):
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_design_binding_mismatch")
    if list(manifest.get("family_members") or ()) != list(freeze_result.get("confirmatory_family") or ()):
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_family_drift")
    expected_ids = {str(row["interaction_spec_id"]) for row in receipts}
    streams = manifest.get("streams")
    if not isinstance(streams, Mapping) or set(streams) != expected_ids:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_stream_family_mismatch")
    return manifest_sha


def _verify_design_frame(
    frame: pd.DataFrame,
    receipt: Mapping[str, Any],
    *,
    challenger: bool,
) -> None:
    spec_id = str(receipt["interaction_spec_id"])
    columns = list(receipt["challenger_columns"] if challenger else receipt["baseline_columns"])
    missing = sorted(set(columns).difference(str(c) for c in frame.columns))
    if missing:
        raise ExternalEvidence8HDiscoveryError(f"8h_e_design_columns_missing:{spec_id}:" + ",".join(missing))
    if list(frame.columns) != columns:
        raise ExternalEvidence8HDiscoveryError(f"8h_e_design_column_order_drift:{spec_id}")
    if frame.duplicated(list(OUTCOME_IDENTITY)).any():
        raise ExternalEvidence8HDiscoveryError(f"8h_e_duplicate_design_identity:{spec_id}")
    field = "challenger_design_sha256" if challenger else "baseline_design_sha256"
    if _frame_digest(frame, columns) != str(receipt[field]):
        raise ExternalEvidence8HDiscoveryError(f"8h_e_design_hash_mismatch:{spec_id}:{field}")


def _predict(frame: pd.DataFrame, feature_names: Sequence[str], model: Mapping[str, Any]) -> np.ndarray:
    coefficients = model.get("coefficients")
    if not isinstance(coefficients, Mapping) or list(coefficients.keys()) != list(feature_names):
        raise ExternalEvidence8HDiscoveryError("8h_e_model_feature_order_mismatch")
    x = frame.loc[:, list(feature_names)].to_numpy(dtype=float)
    beta = np.asarray([float(coefficients[name]) for name in feature_names], dtype=float)
    return float(model.get("intercept")) + x @ beta


def _snapshot_effects(rows: pd.DataFrame, horizon: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    target = f"peer_excess_{horizon}t"
    y = pd.to_numeric(rows[target], errors="raise").to_numpy(dtype=float)
    base = pd.to_numeric(rows["baseline_prediction"], errors="raise").to_numpy(dtype=float)
    challenger = pd.to_numeric(rows["challenger_prediction"], errors="raise").to_numpy(dtype=float)
    if not np.isfinite(y).all() or not np.isfinite(base).all() or not np.isfinite(challenger).all():
        raise ExternalEvidence8HDiscoveryError("8h_e_nonfinite_outcome_or_prediction")
    work = rows.copy()
    work["row_improvement"] = (y - base) ** 2 - (y - challenger) ** 2
    work["row_mae_improvement"] = np.abs(y - base) - np.abs(y - challenger)
    label = f"label_available_from_{horizon}t"
    grouped: list[dict[str, Any]] = []
    for (snapshot_id, as_of), group in work.groupby(["snapshot_id", "as_of"], sort=True):
        if group["start_market_date"].astype(str).nunique() != 1 or group[label].astype(str).nunique() != 1:
            raise ExternalEvidence8HDiscoveryError("8h_e_snapshot_date_identity_ambiguous")
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
    by_date = {
        day: snapshot.loc[pd.to_datetime(snapshot["start_market_date"], utc=True).dt.normalize().eq(day), "snapshot_improvement"].to_numpy(dtype=float)
        for day in dates
    }
    observed = float(snapshot["snapshot_improvement"].mean())
    centered = snapshot.copy()
    centered["snapshot_improvement"] = centered["snapshot_improvement"] - observed
    centered_by_date = {
        day: centered.loc[pd.to_datetime(centered["start_market_date"], utc=True).dt.normalize().eq(day), "snapshot_improvement"].to_numpy(dtype=float)
        for day in dates
    }
    rng = np.random.default_rng(seed)
    draws_per_rep = int(np.ceil(len(dates) / span))
    raw_draws: list[float] = []
    null_draws: list[float] = []
    for _ in range(reps):
        chosen = rng.integers(0, len(blocks), size=draws_per_rep)
        sampled_dates = [day for index in chosen for day in blocks[index]][: len(dates)]
        raw_parts = [by_date[day] for day in sampled_dates]
        null_parts = [centered_by_date[day] for day in sampled_dates]
        raw_draws.append(float(np.mean(np.concatenate(raw_parts))))
        null_draws.append(float(np.mean(np.concatenate(null_parts))))
    raw = np.asarray(raw_draws)
    null = np.asarray(null_draws)
    ci_low, ci_high = np.quantile(raw, [0.025, 0.975])
    p = float((1 + np.sum(np.abs(null) >= abs(observed))) / (len(null) + 1))
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
    return {
        "snapshot_n": len(kept),
        "mean_improvement": None if not kept else float(np.mean(kept)),
    }


def _waiting_result(upstream_state: str, freeze_sha: str, design_sha: str) -> dict[str, Any]:
    result = {
        "schema_version": DISCOVERY_RESULT_SCHEMA,
        "phase": "8H-E",
        "state": WAITING_FOR_DESIGNS,
        "8h_d_state": upstream_state,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "confirmatory_family": [],
        "split_manifest_sha256": None,
        "discovery_results": [],
        "frozen_model_pairs": [],
        "8h_f_validation_evaluation_eligible": False,
        "validation_outcomes_authorized": False,
        "holdout_outcomes_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-E_WAIT_FOR_8H_D_NONEMPTY_DESIGN_FAMILY",
    }
    result["discovery_sha256"] = _digest(result)
    return result


def evaluate_interaction_discovery_family(
    *,
    contract: Mapping[str, Any],
    model_contract: Mapping[str, Any],
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    baseline_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    challenger_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    split_manifest: Mapping[str, Any] | None = None,
    outcome_rows_by_spec: Mapping[str, pd.DataFrame] | None = None,
    research_as_of: object | None = None,
) -> dict[str, Any]:
    """Fit and describe the complete frozen family using DISCOVERY outcomes only."""
    validate_discovery_contract(contract, model_contract)
    state, receipts, freeze_sha, design_sha = _validate_freeze_and_design(freeze_result, design_result)
    baselines = dict(baseline_frames_by_spec or {})
    challengers = dict(challenger_frames_by_spec or {})
    outcomes = dict(outcome_rows_by_spec or {})

    if state != DESIGN_FAMILY_CONSTRUCTED:
        if baselines or challengers or outcomes or split_manifest is not None or research_as_of is not None:
            raise ExternalEvidence8HDiscoveryError("8h_e_empirical_inputs_forbidden_without_constructed_designs")
        return _waiting_result(state, freeze_sha, design_sha)

    spec_ids = [str(row["interaction_spec_id"]) for row in receipts]
    if set(baselines) != set(spec_ids) or set(challengers) != set(spec_ids) or set(outcomes) != set(spec_ids):
        raise ExternalEvidence8HDiscoveryError("8h_e_empirical_input_family_must_equal_full_frozen_family")
    if split_manifest is None:
        raise ExternalEvidence8HDiscoveryError("8h_e_split_manifest_required_before_outcomes")
    if research_as_of is None:
        raise ExternalEvidence8HDiscoveryError("8h_e_research_as_of_required")
    manifest_sha = _validate_split_manifest(split_manifest, freeze_result, design_result, receipts)
    research_time = _timestamp(research_as_of, "8h_e_research_as_of_invalid")

    stats_cfg = contract["discovery_statistics"]
    min_n = int(stats_cfg["minimum_paired_n_per_interaction_horizon_discovery"])
    min_regions = int(stats_cfg["minimum_temporal_support_regions"])
    reps = int(stats_cfg["bootstrap_repetitions"])
    seed = int(stats_cfg["bootstrap_seed"])

    results: list[dict[str, Any]] = []
    frozen_models: list[dict[str, Any]] = []
    all_ready = True
    for receipt in receipts:
        spec_id = str(receipt["interaction_spec_id"])
        horizon = int(receipt["horizon_sessions"])
        baseline = baselines[spec_id].copy()
        challenger = challengers[spec_id].copy()
        _verify_design_frame(baseline, receipt, challenger=False)
        _verify_design_frame(challenger, receipt, challenger=True)
        for column in OUTCOME_IDENTITY:
            if not baseline[column].astype(str).equals(challenger[column].astype(str)):
                raise ExternalEvidence8HDiscoveryError(f"8h_e_baseline_challenger_identity_drift:{spec_id}:{column}")
        baseline_features = list(receipt["baseline_columns"])[len(OUTCOME_IDENTITY):]
        challenger_features = list(receipt["challenger_columns"])[len(OUTCOME_IDENTITY):]
        if challenger_features[:-1] != baseline_features or challenger_features[-1] != receipt["interaction_column"]:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_challenger_shape_drift:{spec_id}")

        stream = split_manifest["streams"][spec_id]
        assignments = list(stream["assignments"])
        discovery_keys = {
            (str(row["snapshot_id"]), str(row["as_of"]))
            for row in assignments if row["split"] == "DISCOVERY" and row["usable"] is True
        }
        validation_dates = [
            _timestamp(row["as_of"], "8h_e_validation_as_of_invalid")
            for row in assignments if row["split"] == "VALIDATION" and row["usable"] is True
        ]
        if not discovery_keys or not validation_dates:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_discovery_and_validation_binding_required:{spec_id}")
        first_validation = min(validation_dates)
        baseline_mask = [
            (str(row.snapshot_id), str(row.as_of)) in discovery_keys
            for row in baseline[["snapshot_id", "as_of"]].itertuples(index=False)
        ]
        challenger_mask = [
            (str(row.snapshot_id), str(row.as_of)) in discovery_keys
            for row in challenger[["snapshot_id", "as_of"]].itertuples(index=False)
        ]
        fit_baseline = baseline.loc[baseline_mask].copy().sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        fit_challenger = challenger.loc[challenger_mask].copy().sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        observed_discovery_keys = {
            (str(row.snapshot_id), str(row.as_of))
            for row in fit_baseline[["snapshot_id", "as_of"]].itertuples(index=False)
        }
        if observed_discovery_keys != discovery_keys:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_discovery_assignment_missing_design_rows:{spec_id}")
        if len(fit_baseline) != len(fit_challenger) or fit_baseline.empty:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_discovery_paired_design_rows_required:{spec_id}")

        outcome = outcomes[spec_id].copy()
        target = f"peer_excess_{horizon}t"
        label = f"label_available_from_{horizon}t"
        required_outcome = set(OUTCOME_IDENTITY) | {"start_market_date", label, target}
        missing = sorted(required_outcome.difference(str(c) for c in outcome.columns))
        if missing:
            raise ExternalEvidence8HDiscoveryError(f"8h_e_outcome_columns_missing:{spec_id}:" + ",".join(missing))
        if outcome.duplicated(list(OUTCOME_IDENTITY)).any():
            raise ExternalEvidence8HDiscoveryError(f"8h_e_duplicate_outcome_identity:{spec_id}")
        outcome = outcome.sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        if len(outcome) != len(fit_baseline):
            raise ExternalEvidence8HDiscoveryError(f"8h_e_outcomes_must_match_all_discovery_design_rows:{spec_id}")
        for column in OUTCOME_IDENTITY:
            if not outcome[column].astype(str).equals(fit_baseline[column].astype(str)):
                raise ExternalEvidence8HDiscoveryError(f"8h_e_outcome_identity_mismatch:{spec_id}:{column}")
        for row in outcome.itertuples(index=False):
            label_time = _timestamp(getattr(row, label), f"8h_e_label_available_invalid:{spec_id}")
            if label_time > research_time:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_discovery_label_not_mature:{spec_id}")
            if label_time >= first_validation:
                raise ExternalEvidence8HDiscoveryError(f"8h_e_exact_discovery_validation_boundary_purge_required:{spec_id}")

        y = pd.to_numeric(outcome[target], errors="raise").to_numpy(dtype=float)
        if not np.isfinite(y).all():
            raise ExternalEvidence8HDiscoveryError(f"8h_e_nonfinite_discovery_target:{spec_id}")
        baseline_x = fit_baseline[baseline_features].to_numpy(dtype=float)
        challenger_x = fit_challenger[challenger_features].to_numpy(dtype=float)
        if not np.isfinite(baseline_x).all() or not np.isfinite(challenger_x).all():
            raise ExternalEvidence8HDiscoveryError(f"8h_e_nonfinite_discovery_design:{spec_id}")
        baseline_model = fit_fixed_ridge(baseline_x, y, feature_names=baseline_features, alpha=1.0)
        challenger_model = fit_fixed_ridge(challenger_x, y, feature_names=challenger_features, alpha=1.0)
        baseline_prediction = _predict(fit_baseline, baseline_features, baseline_model)
        challenger_prediction = _predict(fit_challenger, challenger_features, challenger_model)

        opened = outcome.copy()
        opened["baseline_prediction"] = baseline_prediction
        opened["challenger_prediction"] = challenger_prediction
        row_effects, snapshot = _snapshot_effects(opened, horizon)
        paired_n = int(len(row_effects))
        regions = _temporal_regions(snapshot, horizon)
        enough = paired_n >= min_n and regions >= min_regions
        all_ready = all_ready and enough
        bootstrap = _bootstrap(
            snapshot,
            horizon=horizon,
            reps=reps if enough else 0,
            seed=_stable_seed(seed, spec_id, horizon, "DISCOVERY"),
        )
        primary = None if snapshot.empty else float(snapshot["snapshot_improvement"].mean())
        non_overlap = _non_overlap(snapshot, horizon)
        result = {
            "interaction_spec_id": spec_id,
            "hypothesis_id": str(receipt["confirmatory_hypothesis_id"]),
            "horizon_sessions": horizon,
            "split": "DISCOVERY",
            "status": "DISCOVERY_EVALUATED" if enough else "INSUFFICIENT_DISCOVERY_EVIDENCE",
            "paired_n": paired_n,
            "snapshot_n": int(len(snapshot)),
            "temporal_support_regions": regions,
            "minimum_evidence_met": enough,
            "primary_effect": primary,
            "primary_ci_95": [bootstrap["ci_low"], bootstrap["ci_high"]],
            "descriptive_p_value_two_sided": bootstrap["p_value"],
            "mean_absolute_error_improvement": float(row_effects["row_mae_improvement"].mean()),
            "non_overlap_sensitivity": non_overlap,
            "discovery_p_value_is_confirmatory": False,
            "holm_adjusted_p": None,
            "family_membership_changed": False,
            "validation_outcomes_opened": False,
            "holdout_outcomes_opened": False,
        }
        result["evaluation_sha256"] = _digest(result)
        results.append(result)

        model_pair = {
            "interaction_spec_id": spec_id,
            "hypothesis_id": str(receipt["confirmatory_hypothesis_id"]),
            "horizon_sessions": horizon,
            "8h_c_freeze_sha256": freeze_sha,
            "8h_d_construction_sha256": design_sha,
            "8h_d_design_receipt_sha256": str(receipt["construction_sha256"]),
            "split_manifest_sha256": manifest_sha,
            "baseline_model": baseline_model,
            "challenger_model": challenger_model,
            "discovery_paired_n": paired_n,
            "minimum_evidence_met": enough,
            "hyperparameter_tuning_used": False,
            "feature_selection_used": False,
            "sign_search_used": False,
            "threshold_search_used": False,
            "validation_refit_allowed": False,
            "holdout_refit_allowed": False,
            "validation_outcomes_opened": False,
            "holdout_outcomes_opened": False,
        }
        model_pair["model_pair_sha256"] = _digest(model_pair)
        frozen_models.append(model_pair)

    expected_family = list(freeze_result.get("confirmatory_family") or ())
    if [row["hypothesis_id"] for row in results] != [str(row["hypothesis_id"]) for row in expected_family]:
        raise ExternalEvidence8HDiscoveryError("8h_e_result_family_order_or_membership_drift")
    result_state = DISCOVERY_FAMILY_FROZEN if all_ready else DISCOVERY_FAMILY_INSUFFICIENT
    family_result: dict[str, Any] = {
        "schema_version": DISCOVERY_RESULT_SCHEMA,
        "phase": "8H-E",
        "state": result_state,
        "8h_d_state": DESIGN_FAMILY_CONSTRUCTED,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "confirmatory_family": expected_family,
        "split_manifest_sha256": manifest_sha,
        "discovery_results": results,
        "frozen_model_pairs": frozen_models,
        "full_family_model_freeze_complete": all_ready,
        "8h_f_validation_evaluation_eligible": all_ready,
        "validation_outcomes_authorized": False,
        "holdout_outcomes_authorized": False,
        "family_shrunk_or_expanded": False,
        "holm_applied_in_discovery": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-F_VALIDATION_EVALUATION" if all_ready else "8H-E_WAIT_FOR_COMPLETE_DISCOVERY_EVIDENCE",
    }
    family_result["discovery_sha256"] = _digest(family_result)
    return family_result
