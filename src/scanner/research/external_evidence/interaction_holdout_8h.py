"""Phase 8H-G one-shot Holdout / prospective interaction confirmation.

The complete 8H-C family must enter Holdout together after 8H-F has frozen
Validation.  Holdout authorization is outcome-independent and family-wide.
Exactly one terminal Holdout opening is permitted.  Models are never refit,
Holm is applied to the complete family, and this module never promotes an
interaction: it only produces evidence for the manual 8H-H review.
"""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from scanner.research.external_evidence.interaction_discovery_8h import (
    DISCOVERY_RESULT_SCHEMA,
    OUTCOME_IDENTITY,
    SPLIT_MANIFEST_SCHEMA,
)
from scanner.research.external_evidence.interaction_model_8h import (
    INTERACTION_MODEL_RESULT_SCHEMA,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
)
from scanner.research.external_evidence.interaction_validation_8h import (
    VALIDATION_FAMILY_FROZEN,
    VALIDATION_FAMILY_RECEIPT_SCHEMA,
    VALIDATION_RESULT_SCHEMA,
)


HOLDOUT_CONTRACT_SCHEMA = "external_evidence_8h_interaction_holdout_confirmation_v1"
HOLDOUT_AUTHORIZATION_SCHEMA = "external_evidence_8h_interaction_holdout_authorization_v1"
HOLDOUT_LEDGER_SCHEMA = "external_evidence_8h_interaction_holdout_consumption_ledger_v1"
HOLDOUT_RESULT_SCHEMA = "external_evidence_8h_interaction_holdout_result_v1"
HOLDOUT_COMPLETION_SCHEMA = "external_evidence_8h_interaction_holdout_completion_v1"
WAITING_FOR_VALIDATION = "WAITING_FOR_FROZEN_VALIDATION_FAMILY"
HOLDOUT_FAMILY_AUTHORIZED = "HOLDOUT_FAMILY_AUTHORIZED"
HOLDOUT_FAMILY_COMPLETE = "HOLDOUT_FAMILY_COMPLETE"
HOLDOUT_FAMILY_CONSUMED_INSUFFICIENT = "HOLDOUT_FAMILY_CONSUMED_INSUFFICIENT_EVIDENCE"


class ExternalEvidence8HHoldoutError(ValueError):
    """Raised when 8H-G violates the one-shot Holdout protocol."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    expected = str(row.get(field) or "")
    if not expected:
        raise ExternalEvidence8HHoldoutError(error)
    payload = dict(row)
    payload.pop(field, None)
    if _digest(payload) != expected:
        raise ExternalEvidence8HHoldoutError(error)
    return expected


def _timestamp(value: object, error: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        raise ExternalEvidence8HHoldoutError(error)
    return pd.Timestamp(parsed)


def _day(value: object) -> pd.Timestamp:
    return _timestamp(value, "8h_g_invalid_date").normalize()


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


def validate_holdout_contract(
    contract: Mapping[str, Any], validation_contract: Mapping[str, Any]
) -> None:
    if contract.get("schema_version") != HOLDOUT_CONTRACT_SCHEMA or contract.get("phase") != "8H-G":
        raise ExternalEvidence8HHoldoutError("8h_g_contract_schema_or_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HHoldoutError("8h_g_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase7_mutation_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HHoldoutError(f"8h_g_forbidden_contract_authorization:{key}")
    if validation_contract.get("schema_version") != "external_evidence_8h_interaction_validation_evaluation_v1":
        raise ExternalEvidence8HHoldoutError("8h_g_8h_f_contract_schema_mismatch")
    auth = contract.get("holdout_authorization")
    if not isinstance(auth, Mapping):
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_contract_missing")
    if auth.get("one_shot_family_open") is not True or auth.get("authorization_trigger_must_be_outcome_independent") is not True:
        raise ExternalEvidence8HHoldoutError("8h_g_one_shot_outcome_blind_authorization_required")
    if auth.get("repeated_interim_significance_looks_allowed") is not False or auth.get("hypothesis_by_hypothesis_opening_allowed") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_repeated_or_partial_opening_forbidden")
    model = contract.get("model_freeze")
    if not isinstance(model, Mapping) or model.get("estimator") != "ridge_linear_regression" or float(model.get("alpha")) != 1.0:
        raise ExternalEvidence8HHoldoutError("8h_g_frozen_ridge_required")
    for key in (
        "holdout_refit_allowed",
        "feature_selection_allowed",
        "hyperparameter_tuning_allowed",
        "sign_search_allowed",
        "threshold_search_allowed",
        "transform_search_allowed",
        "validation_result_may_change_model",
    ):
        if model.get(key) is not False:
            raise ExternalEvidence8HHoldoutError(f"8h_g_search_or_refit_must_be_false:{key}")
    access = contract.get("holdout_outcome_access")
    if not isinstance(access, Mapping) or access.get("allowed_split") != "HOLDOUT":
        raise ExternalEvidence8HHoldoutError("8h_g_only_holdout_split_may_open")
    if access.get("holdout_may_be_opened_more_than_once") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_holdout_must_be_one_shot")
    mult = contract.get("multiplicity_control")
    if not isinstance(mult, Mapping) or mult.get("method") != "Holm" or float(mult.get("family_wise_alpha")) != 0.05:
        raise ExternalEvidence8HHoldoutError("8h_g_full_family_holm_required")


def _validate_upstream(
    *,
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    discovery_result: Mapping[str, Any],
    validation_result: Mapping[str, Any],
    split_manifest: Mapping[str, Any] | None,
) -> tuple[
    bool,
    str,
    str,
    str,
    str,
    list[dict[str, Any]],
    dict[str, dict[str, Any]],
    str | None,
    str | None,
]:
    if freeze_result.get("schema_version") != INTERACTION_FREEZE_RESULT_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_freeze_schema_mismatch")
    freeze_sha = _verify_digest(freeze_result, "freeze_sha256", "8h_g_freeze_digest_mismatch")
    if design_result.get("schema_version") != INTERACTION_MODEL_RESULT_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_design_schema_mismatch")
    design_sha = _verify_digest(design_result, "construction_sha256", "8h_g_design_digest_mismatch")
    if discovery_result.get("schema_version") != DISCOVERY_RESULT_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_discovery_schema_mismatch")
    discovery_sha = _verify_digest(discovery_result, "discovery_sha256", "8h_g_discovery_digest_mismatch")
    if validation_result.get("schema_version") != VALIDATION_RESULT_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_schema_mismatch")
    validation_sha = _verify_digest(validation_result, "validation_sha256", "8h_g_validation_digest_mismatch")

    family = list(freeze_result.get("confirmatory_family") or ())
    if list(design_result.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HHoldoutError("8h_g_design_family_drift")
    if list(discovery_result.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HHoldoutError("8h_g_discovery_family_drift")
    if list(validation_result.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_family_drift")

    if validation_result.get("state") != VALIDATION_FAMILY_FROZEN:
        if validation_result.get("8h_g_holdout_evaluation_eligible") is True:
            raise ExternalEvidence8HHoldoutError("8h_g_nonfrozen_validation_cannot_authorize_holdout")
        return False, freeze_sha, design_sha, discovery_sha, validation_sha, family, {}, None, None
    if validation_result.get("full_family_validation_complete") is not True or validation_result.get("8h_g_holdout_evaluation_eligible") is not True:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_entry_gate_not_met")
    if validation_result.get("holdout_outcomes_authorized") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_8h_f_must_not_preopen_holdout")
    if validation_result.get("family_shrunk_or_expanded") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_family_mutation_detected")

    receipt = validation_result.get("validation_family_receipt")
    if not isinstance(receipt, Mapping) or receipt.get("schema_version") != VALIDATION_FAMILY_RECEIPT_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_family_receipt_required")
    receipt_sha = _verify_digest(receipt, "receipt_sha256", "8h_g_validation_family_receipt_digest_mismatch")
    if receipt.get("frozen") is not True or receipt.get("holdout_outcomes_opened") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_receipt_not_sealed")
    if receipt.get("holdout_model_refit_allowed") is not False or receipt.get("holdout_family_reduction_allowed") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_receipt_allows_forbidden_change")
    if list(receipt.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_receipt_family_drift")
    if str(receipt.get("8h_e_discovery_sha256") or "") != discovery_sha:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_receipt_discovery_binding_mismatch")

    models: dict[str, dict[str, Any]] = {}
    for raw in discovery_result.get("frozen_model_pairs") or ():
        row = dict(raw)
        spec_id = str(row.get("interaction_spec_id") or "")
        if not spec_id or spec_id in models:
            raise ExternalEvidence8HHoldoutError("8h_g_duplicate_or_missing_model_pair")
        _verify_digest(row, "model_pair_sha256", f"8h_g_model_pair_digest_mismatch:{spec_id}")
        for key in ("baseline_model", "challenger_model"):
            model = row.get(key)
            if not isinstance(model, Mapping):
                raise ExternalEvidence8HHoldoutError(f"8h_g_model_missing:{spec_id}:{key}")
            _verify_digest(model, "model_sha256", f"8h_g_model_digest_mismatch:{spec_id}:{key}")
            if model.get("family") != "ridge_linear_regression" or float(model.get("alpha")) != 1.0 or model.get("fit_intercept") is not True:
                raise ExternalEvidence8HHoldoutError(f"8h_g_frozen_model_invalid:{spec_id}:{key}")
        if row.get("holdout_refit_allowed") is not False:
            raise ExternalEvidence8HHoldoutError(f"8h_g_refit_permission_drift:{spec_id}")
        models[spec_id] = row
    family_ids = {str(x.get("interaction_spec_id") or "") for x in family}
    if set(models) != family_ids:
        raise ExternalEvidence8HHoldoutError("8h_g_model_pairs_must_equal_full_family")

    if not isinstance(split_manifest, Mapping) or split_manifest.get("schema_version") != SPLIT_MANIFEST_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_exact_8h_e_split_manifest_required")
    manifest_sha = _verify_digest(split_manifest, "manifest_sha256", "8h_g_split_manifest_digest_mismatch")
    if manifest_sha != str(discovery_result.get("split_manifest_sha256") or "") or manifest_sha != str(validation_result.get("split_manifest_sha256") or ""):
        raise ExternalEvidence8HHoldoutError("8h_g_split_manifest_not_identical_to_upstream")
    if split_manifest.get("holdout_outcomes_opened") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_split_manifest_holdout_already_open")
    if list(split_manifest.get("family_members") or ()) != family:
        raise ExternalEvidence8HHoldoutError("8h_g_split_manifest_family_drift")
    return True, freeze_sha, design_sha, discovery_sha, validation_sha, family, models, manifest_sha, receipt_sha


def _temporal_regions_from_dates(values: Sequence[object], horizon: int) -> int:
    dates = sorted({_day(value) for value in values})
    if not dates:
        return 0
    block = 2 * int(horizon)
    regions = 0
    last: int | None = None
    for position in range(len(dates)):
        if last is None or position - last >= block:
            regions += 1
            last = position
    return regions


def freeze_holdout_authorization(
    *,
    contract: Mapping[str, Any],
    validation_contract: Mapping[str, Any],
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    discovery_result: Mapping[str, Any],
    validation_result: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    availability_rows_by_spec: Mapping[str, Sequence[Mapping[str, Any]]],
    research_as_of: object,
    author_identity: str,
    authored_at: object,
    outcomes_read_while_authorizing: bool = False,
) -> dict[str, Any]:
    """Freeze the one-shot full-family Holdout token using metadata only."""
    validate_holdout_contract(contract, validation_contract)
    ready, freeze_sha, design_sha, discovery_sha, validation_sha, family, models, manifest_sha, receipt_sha = _validate_upstream(
        freeze_result=freeze_result,
        design_result=design_result,
        discovery_result=discovery_result,
        validation_result=validation_result,
        split_manifest=split_manifest,
    )
    if not ready:
        raise ExternalEvidence8HHoldoutError("8h_g_holdout_authorization_requires_frozen_validation_family")
    if outcomes_read_while_authorizing is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_must_be_outcome_blind")
    if not str(author_identity or "").strip():
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_author_required")
    research_time = _timestamp(research_as_of, "8h_g_authorization_research_as_of_invalid")
    authored = _timestamp(authored_at, "8h_g_authorization_timestamp_invalid")
    if authored > research_time:
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_timestamp_after_research_as_of")

    spec_ids = [str(row["interaction_spec_id"]) for row in family]
    if set(availability_rows_by_spec) != set(spec_ids):
        raise ExternalEvidence8HHoldoutError("8h_g_availability_family_must_equal_full_family")
    min_n = int(contract["holdout_authorization"]["minimum_paired_n_per_interaction_horizon"])
    min_regions = int(contract["holdout_authorization"]["minimum_temporal_support_regions"])
    forbidden = {
        "peer_excess", "outcome", "label_value", "primary_effect", "p_value",
        "primary_p_value_two_sided", "holm_adjusted_p", "loss_improvement",
    }
    streams: dict[str, Any] = {}
    for member in family:
        spec_id = str(member["interaction_spec_id"])
        horizon = int(member["horizon_sessions"])
        stream = split_manifest["streams"].get(spec_id)
        if not isinstance(stream, Mapping):
            raise ExternalEvidence8HHoldoutError(f"8h_g_split_stream_missing:{spec_id}")
        expected = sorted(
            (str(row["snapshot_id"]), str(row["as_of"]))
            for row in stream.get("assignments") or ()
            if row.get("split") == "HOLDOUT" and row.get("usable") is True
        )
        if not expected:
            raise ExternalEvidence8HHoldoutError(f"8h_g_holdout_assignments_missing:{spec_id}")
        records: list[dict[str, str]] = []
        for raw in availability_rows_by_spec[spec_id]:
            if forbidden.intersection(str(key) for key in raw.keys()):
                raise ExternalEvidence8HHoldoutError(f"8h_g_outcome_value_in_authorization_metadata:{spec_id}")
            snapshot_id = str(raw.get("snapshot_id") or "")
            as_of = str(raw.get("as_of") or "")
            label_available = str(raw.get("label_available_from") or "")
            if not snapshot_id or not as_of or not label_available:
                raise ExternalEvidence8HHoldoutError(f"8h_g_availability_identity_missing:{spec_id}")
            if _timestamp(label_available, f"8h_g_label_availability_invalid:{spec_id}") > research_time:
                raise ExternalEvidence8HHoldoutError(f"8h_g_holdout_labels_not_all_mature:{spec_id}")
            records.append({
                "snapshot_id": snapshot_id,
                "as_of": as_of,
                "label_available_from": label_available,
            })
        records.sort(key=lambda row: (row["as_of"], row["snapshot_id"]))
        observed = sorted((row["snapshot_id"], row["as_of"]) for row in records)
        if observed != expected or len(observed) != len(set(observed)):
            raise ExternalEvidence8HHoldoutError(f"8h_g_availability_rows_must_match_all_holdout_assignments:{spec_id}")
        regions = _temporal_regions_from_dates([row["as_of"] for row in records], horizon)
        if len(records) < min_n:
            raise ExternalEvidence8HHoldoutError(f"8h_g_holdout_minimum_paired_n_not_met_before_open:{spec_id}")
        if regions < min_regions:
            raise ExternalEvidence8HHoldoutError(f"8h_g_holdout_temporal_support_not_met_before_open:{spec_id}")
        streams[spec_id] = {
            "horizon_sessions": horizon,
            "paired_n": len(records),
            "temporal_support_regions": regions,
            "first_as_of": min(row["as_of"] for row in records),
            "last_as_of": max(row["as_of"] for row in records),
            "latest_label_available_from": max(row["label_available_from"] for row in records),
            "holdout_identity_sha256": _digest(expected),
            "availability_sha256": _digest(records),
            "model_pair_sha256": str(models[spec_id]["model_pair_sha256"]),
        }

    authorization: dict[str, Any] = {
        "schema_version": HOLDOUT_AUTHORIZATION_SCHEMA,
        "phase": "8H-G",
        "state": HOLDOUT_FAMILY_AUTHORIZED,
        "frozen": True,
        "one_shot": True,
        "author_identity": str(author_identity),
        "authored_at": authored.isoformat(),
        "research_as_of": research_time.isoformat(),
        "outcomes_read_while_authorizing": False,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "8h_e_discovery_sha256": discovery_sha,
        "8h_f_validation_sha256": validation_sha,
        "8h_f_validation_family_receipt_sha256": receipt_sha,
        "split_manifest_sha256": manifest_sha,
        "confirmatory_family": family,
        "streams": streams,
        "model_refit_allowed": False,
        "family_reduction_allowed": False,
        "outcomes_opened": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    authorization["authorization_sha256"] = _digest(authorization)
    return authorization


def new_holdout_consumption_ledger(authorization: Mapping[str, Any]) -> dict[str, Any]:
    if authorization.get("schema_version") != HOLDOUT_AUTHORIZATION_SCHEMA or authorization.get("frozen") is not True:
        raise ExternalEvidence8HHoldoutError("8h_g_frozen_holdout_authorization_required")
    auth_sha = _verify_digest(authorization, "authorization_sha256", "8h_g_authorization_digest_mismatch")
    ledger: dict[str, Any] = {
        "schema_version": HOLDOUT_LEDGER_SCHEMA,
        "phase": "8H-G",
        "append_only": True,
        "authorization_sha256": auth_sha,
        "state": "ARMED",
        "holdout_open_count": 0,
        "terminal_result_sha256": None,
        "completion_sha256": None,
    }
    ledger["ledger_sha256"] = _digest(ledger)
    return ledger


def _validate_ledger(ledger: Mapping[str, Any], authorization: Mapping[str, Any]) -> str:
    if ledger.get("schema_version") != HOLDOUT_LEDGER_SCHEMA or ledger.get("append_only") is not True:
        raise ExternalEvidence8HHoldoutError("8h_g_holdout_ledger_invalid")
    ledger_sha = _verify_digest(ledger, "ledger_sha256", "8h_g_holdout_ledger_digest_mismatch")
    if str(ledger.get("authorization_sha256") or "") != str(authorization.get("authorization_sha256") or ""):
        raise ExternalEvidence8HHoldoutError("8h_g_ledger_authorization_binding_mismatch")
    if ledger.get("state") != "ARMED" or int(ledger.get("holdout_open_count") or 0) != 0:
        raise ExternalEvidence8HHoldoutError("8h_g_holdout_may_be_opened_once_only")
    if ledger.get("terminal_result_sha256") is not None or ledger.get("completion_sha256") is not None:
        raise ExternalEvidence8HHoldoutError("8h_g_armed_ledger_must_be_unconsumed")
    return ledger_sha


def _predict(frame: pd.DataFrame, model: Mapping[str, Any]) -> np.ndarray:
    coefficients = model.get("coefficients")
    if not isinstance(coefficients, Mapping):
        raise ExternalEvidence8HHoldoutError("8h_g_model_coefficients_missing")
    names = list(coefficients.keys())
    if any(name not in frame.columns for name in names):
        raise ExternalEvidence8HHoldoutError("8h_g_model_feature_missing_from_design")
    x = frame[names].to_numpy(dtype=float)
    if not np.isfinite(x).all():
        raise ExternalEvidence8HHoldoutError("8h_g_nonfinite_holdout_design")
    beta = np.asarray([float(coefficients[name]) for name in names], dtype=float)
    return float(model.get("intercept")) + x @ beta


def _snapshot_effects(rows: pd.DataFrame, horizon: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    target = f"peer_excess_{horizon}t"
    y = pd.to_numeric(rows[target], errors="raise").to_numpy(dtype=float)
    base = pd.to_numeric(rows["baseline_prediction"], errors="raise").to_numpy(dtype=float)
    challenger = pd.to_numeric(rows["challenger_prediction"], errors="raise").to_numpy(dtype=float)
    if not np.isfinite(y).all() or not np.isfinite(base).all() or not np.isfinite(challenger).all():
        raise ExternalEvidence8HHoldoutError("8h_g_nonfinite_outcome_or_prediction")
    work = rows.copy()
    work["row_improvement"] = (y - base) ** 2 - (y - challenger) ** 2
    work["row_mae_improvement"] = np.abs(y - base) - np.abs(y - challenger)
    label = f"label_available_from_{horizon}t"
    grouped: list[dict[str, Any]] = []
    for (snapshot_id, as_of), group in work.groupby(["snapshot_id", "as_of"], sort=True):
        if group["start_market_date"].astype(str).nunique() != 1 or group[label].astype(str).nunique() != 1:
            raise ExternalEvidence8HHoldoutError("8h_g_snapshot_date_identity_ambiguous")
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
    return _temporal_regions_from_dates(snapshot["start_market_date"].tolist(), horizon)


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
    return {"snapshot_n": len(kept), "mean_improvement": None if not kept else float(np.mean(kept))}


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
        if row.get("minimum_evidence_met") is not True:
            row["terminal_state"] = "INSUFFICIENT_HOLDOUT_EVIDENCE"
        elif row["holm_positive"]:
            row["terminal_state"] = "HOLDOUT_CONFIRMED"
        else:
            row["terminal_state"] = "HOLDOUT_NOT_CONFIRMED"
        row["evaluation_sha256"] = _digest({k: v for k, v in row.items() if k != "evaluation_sha256"})
    return source


def _waiting_result(
    *, freeze_sha: str, design_sha: str, discovery_sha: str, validation_sha: str, family: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": HOLDOUT_RESULT_SCHEMA,
        "phase": "8H-G",
        "state": WAITING_FOR_VALIDATION,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "8h_e_discovery_sha256": discovery_sha,
        "8h_f_validation_sha256": validation_sha,
        "confirmatory_family": list(family),
        "holdout_authorization_sha256": None,
        "holdout_results": [],
        "holdout_completion": None,
        "8h_h_promotion_review_eligible": False,
        "holdout_outcomes_opened": False,
        "family_shrunk_or_expanded": False,
        "model_refit": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-G_WAIT_FOR_FROZEN_VALIDATION_FAMILY",
    }
    result["holdout_sha256"] = _digest(result)
    return result


def evaluate_interaction_holdout_family(
    *,
    contract: Mapping[str, Any],
    validation_contract: Mapping[str, Any],
    freeze_result: Mapping[str, Any],
    design_result: Mapping[str, Any],
    discovery_result: Mapping[str, Any],
    validation_result: Mapping[str, Any],
    split_manifest: Mapping[str, Any] | None = None,
    authorization: Mapping[str, Any] | None = None,
    ledger: Mapping[str, Any] | None = None,
    baseline_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    challenger_frames_by_spec: Mapping[str, pd.DataFrame] | None = None,
    outcome_rows_by_spec: Mapping[str, pd.DataFrame] | None = None,
    research_as_of: object | None = None,
    opened_by: str | None = None,
    opened_at: object | None = None,
    human_outcome_inspection_occurred: bool = False,
    human_inspector_identity: str | None = None,
    inspection_note: str | None = None,
    context_rows_by_spec: Mapping[str, pd.DataFrame] | None = None,
    coverage_records_by_spec: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Open the complete Holdout family exactly once and return result + consumed ledger."""
    validate_holdout_contract(contract, validation_contract)
    ready, freeze_sha, design_sha, discovery_sha, validation_sha, family, models, manifest_sha, receipt_sha = _validate_upstream(
        freeze_result=freeze_result,
        design_result=design_result,
        discovery_result=discovery_result,
        validation_result=validation_result,
        split_manifest=split_manifest,
    )
    baselines = dict(baseline_frames_by_spec or {})
    challengers = dict(challenger_frames_by_spec or {})
    outcomes = dict(outcome_rows_by_spec or {})
    contexts = dict(context_rows_by_spec or {})
    coverages = dict(coverage_records_by_spec or {})
    if not ready:
        if any((authorization is not None, ledger is not None, bool(baselines), bool(challengers), bool(outcomes), bool(contexts), bool(coverages), research_as_of is not None, opened_by is not None, opened_at is not None)):
            raise ExternalEvidence8HHoldoutError("8h_g_empirical_inputs_forbidden_before_validation_freeze")
        return _waiting_result(
            freeze_sha=freeze_sha,
            design_sha=design_sha,
            discovery_sha=discovery_sha,
            validation_sha=validation_sha,
            family=family,
        ), None

    if not isinstance(authorization, Mapping) or authorization.get("schema_version") != HOLDOUT_AUTHORIZATION_SCHEMA:
        raise ExternalEvidence8HHoldoutError("8h_g_holdout_authorization_required_before_outcomes")
    auth_sha = _verify_digest(authorization, "authorization_sha256", "8h_g_authorization_digest_mismatch")
    if authorization.get("state") != HOLDOUT_FAMILY_AUTHORIZED or authorization.get("frozen") is not True or authorization.get("outcomes_opened") is not False:
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_not_frozen_and_sealed")
    if str(authorization.get("8h_f_validation_sha256") or "") != validation_sha or str(authorization.get("8h_f_validation_family_receipt_sha256") or "") != receipt_sha:
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_validation_binding_mismatch")
    if str(authorization.get("split_manifest_sha256") or "") != manifest_sha:
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_split_binding_mismatch")
    if list(authorization.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HHoldoutError("8h_g_authorization_family_drift")
    if not isinstance(ledger, Mapping):
        raise ExternalEvidence8HHoldoutError("8h_g_holdout_consumption_ledger_required")
    ledger_before_sha = _validate_ledger(ledger, authorization)

    spec_ids = [str(row["interaction_spec_id"]) for row in family]
    if set(baselines) != set(spec_ids) or set(challengers) != set(spec_ids) or set(outcomes) != set(spec_ids):
        raise ExternalEvidence8HHoldoutError("8h_g_empirical_input_family_must_equal_full_frozen_family")
    if research_as_of is None or opened_at is None or not str(opened_by or "").strip():
        raise ExternalEvidence8HHoldoutError("8h_g_outcome_access_audit_fields_required")
    research_time = _timestamp(research_as_of, "8h_g_research_as_of_invalid")
    open_time = _timestamp(opened_at, "8h_g_opened_at_invalid")
    if open_time > research_time:
        raise ExternalEvidence8HHoldoutError("8h_g_opened_at_after_research_as_of")
    if human_outcome_inspection_occurred and (not str(human_inspector_identity or "").strip() or not str(inspection_note or "").strip()):
        raise ExternalEvidence8HHoldoutError("8h_g_human_inspection_log_incomplete")

    receipts_by_spec = {str(row["interaction_spec_id"]): row for row in design_result.get("constructed_interaction_designs") or ()}
    if set(receipts_by_spec) != set(spec_ids):
        raise ExternalEvidence8HHoldoutError("8h_g_design_receipts_must_equal_full_family")
    validation_by_hypothesis = {str(row["hypothesis_id"]): row for row in validation_result.get("validation_results") or ()}
    if set(validation_by_hypothesis) != {str(row["hypothesis_id"]) for row in family}:
        raise ExternalEvidence8HHoldoutError("8h_g_validation_result_family_incomplete")

    stats = contract["holdout_statistics"]
    min_n = int(stats["minimum_paired_n_per_interaction_horizon_holdout"])
    min_regions = int(stats["minimum_temporal_support_regions"])
    reps = int(stats["bootstrap_repetitions"])
    seed = int(stats["bootstrap_seed"])
    min_group = int(contract["robustness_diagnostics"]["minimum_group_paired_n_for_direction_report"])

    raw_results: list[dict[str, Any]] = []
    all_sufficient = True
    for member in family:
        spec_id = str(member["interaction_spec_id"])
        hypothesis_id = str(member["hypothesis_id"])
        horizon = int(member["horizon_sessions"])
        receipt = receipts_by_spec[spec_id]
        _verify_digest(receipt, "construction_sha256", f"8h_g_design_receipt_digest_mismatch:{spec_id}")
        baseline = baselines[spec_id].copy()
        challenger = challengers[spec_id].copy()
        baseline_columns = list(receipt["baseline_columns"])
        challenger_columns = list(receipt["challenger_columns"])
        if list(baseline.columns) != baseline_columns or list(challenger.columns) != challenger_columns:
            raise ExternalEvidence8HHoldoutError(f"8h_g_design_column_order_drift:{spec_id}")
        if _frame_digest(baseline, baseline_columns) != str(receipt["baseline_design_sha256"]):
            raise ExternalEvidence8HHoldoutError(f"8h_g_baseline_design_hash_mismatch:{spec_id}")
        if _frame_digest(challenger, challenger_columns) != str(receipt["challenger_design_sha256"]):
            raise ExternalEvidence8HHoldoutError(f"8h_g_challenger_design_hash_mismatch:{spec_id}")
        for column in OUTCOME_IDENTITY:
            if not baseline[column].astype(str).equals(challenger[column].astype(str)):
                raise ExternalEvidence8HHoldoutError(f"8h_g_baseline_challenger_identity_drift:{spec_id}:{column}")

        stream = split_manifest["streams"].get(spec_id)
        if not isinstance(stream, Mapping):
            raise ExternalEvidence8HHoldoutError(f"8h_g_split_stream_missing:{spec_id}")
        holdout_keys = {
            (str(row["snapshot_id"]), str(row["as_of"]))
            for row in stream.get("assignments") or ()
            if row.get("split") == "HOLDOUT" and row.get("usable") is True
        }
        baseline_mask = [
            (str(row.snapshot_id), str(row.as_of)) in holdout_keys
            for row in baseline[["snapshot_id", "as_of"]].itertuples(index=False)
        ]
        challenger_mask = [
            (str(row.snapshot_id), str(row.as_of)) in holdout_keys
            for row in challenger[["snapshot_id", "as_of"]].itertuples(index=False)
        ]
        hold_baseline = baseline.loc[baseline_mask].copy().sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        hold_challenger = challenger.loc[challenger_mask].copy().sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        observed_keys = {(str(row.snapshot_id), str(row.as_of)) for row in hold_baseline[["snapshot_id", "as_of"]].itertuples(index=False)}
        if observed_keys != holdout_keys or len(hold_baseline) != len(hold_challenger) or hold_baseline.empty:
            raise ExternalEvidence8HHoldoutError(f"8h_g_holdout_design_binding_incomplete:{spec_id}")

        outcome = outcomes[spec_id].copy()
        target = f"peer_excess_{horizon}t"
        label = f"label_available_from_{horizon}t"
        required = set(OUTCOME_IDENTITY) | {"start_market_date", label, target}
        missing = sorted(required.difference(str(c) for c in outcome.columns))
        if missing:
            raise ExternalEvidence8HHoldoutError(f"8h_g_outcome_columns_missing:{spec_id}:" + ",".join(missing))
        if outcome.duplicated(list(OUTCOME_IDENTITY)).any():
            raise ExternalEvidence8HHoldoutError(f"8h_g_duplicate_outcome_identity:{spec_id}")
        outcome = outcome.sort_values(list(OUTCOME_IDENTITY), kind="mergesort").reset_index(drop=True)
        if len(outcome) != len(hold_baseline):
            raise ExternalEvidence8HHoldoutError(f"8h_g_outcomes_must_match_all_holdout_design_rows:{spec_id}")
        for column in OUTCOME_IDENTITY:
            if not outcome[column].astype(str).equals(hold_baseline[column].astype(str)):
                raise ExternalEvidence8HHoldoutError(f"8h_g_outcome_identity_mismatch:{spec_id}:{column}")
        availability_records: list[dict[str, str]] = []
        for row in outcome.itertuples(index=False):
            label_value = str(getattr(row, label))
            if _timestamp(label_value, f"8h_g_label_available_invalid:{spec_id}") > research_time:
                raise ExternalEvidence8HHoldoutError(f"8h_g_holdout_label_not_mature:{spec_id}")
            availability_records.append({
                "snapshot_id": str(row.snapshot_id),
                "as_of": str(row.as_of),
                "label_available_from": label_value,
            })
        availability_records.sort(key=lambda row: (row["as_of"], row["snapshot_id"]))
        auth_stream = authorization["streams"].get(spec_id)
        if not isinstance(auth_stream, Mapping) or _digest(availability_records) != str(auth_stream.get("availability_sha256") or ""):
            raise ExternalEvidence8HHoldoutError(f"8h_g_outcome_availability_metadata_differs_from_authorization:{spec_id}")
        if str(models[spec_id]["model_pair_sha256"]) != str(auth_stream.get("model_pair_sha256") or ""):
            raise ExternalEvidence8HHoldoutError(f"8h_g_model_pair_differs_from_authorization:{spec_id}")

        baseline_prediction = _predict(hold_baseline, models[spec_id]["baseline_model"])
        challenger_prediction = _predict(hold_challenger, models[spec_id]["challenger_model"])
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
            seed=_stable_seed(seed, spec_id, horizon, "HOLDOUT"),
        )
        primary = None if snapshot.empty else float(snapshot["snapshot_improvement"].mean())

        context = contexts.get(spec_id)
        if context is not None:
            identity = ["snapshot_id", "as_of", "symbol"]
            if any(column not in context.columns for column in identity) or context.duplicated(identity).any():
                raise ExternalEvidence8HHoldoutError(f"8h_g_context_identity_invalid:{spec_id}")
            available_fields = [field for field in ("market_regime_stock", "sector", "currency", "domain") if field in context.columns]
            row_effects = row_effects.merge(context[identity + available_fields], on=identity, how="left", validate="one_to_one")
            context_complete = bool(available_fields) and not row_effects[available_fields].isna().all().all()
        else:
            available_fields = []
            context_complete = False

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

        validation_row = validation_by_hypothesis[hypothesis_id]
        validation_effect = validation_row.get("primary_effect")
        result = {
            "interaction_spec_id": spec_id,
            "hypothesis_id": hypothesis_id,
            "horizon_sessions": horizon,
            "split": "HOLDOUT",
            "status": "HOLDOUT_EVALUATED" if sufficient else "INSUFFICIENT_HOLDOUT_EVIDENCE",
            "paired_n": paired_n,
            "snapshot_n": int(len(snapshot)),
            "temporal_support_regions": regions,
            "minimum_evidence_met": sufficient,
            "primary_effect": primary,
            "primary_ci_95": [bootstrap["ci_low"], bootstrap["ci_high"]],
            "primary_p_value_two_sided": bootstrap["p_value"],
            "mean_absolute_error_improvement": float(row_effects["row_mae_improvement"].mean()),
            "non_overlap_sensitivity": _non_overlap(snapshot, horizon),
            "validation_primary_effect": validation_effect,
            "validation_holm_positive": bool(validation_row.get("holm_positive")),
            "validation_direction_consistent": bool(
                validation_effect is not None and primary is not None
                and np.sign(float(validation_effect)) == np.sign(float(primary))
            ),
            "context_complete": context_complete,
            "stability_diagnostics": {
                field: _group_report(row_effects, field, min_group)
                for field in ("market_regime_stock", "sector", "currency", "domain")
            },
            "coverage_missingness": coverage,
            "concentration": {
                "start_market_date": _concentration(row_effects, "start_market_date"),
                "symbol": _concentration(row_effects, "symbol"),
                "sector": _concentration(row_effects, "sector"),
                "domain": _concentration(row_effects, "domain"),
            },
            "model_pair_sha256": str(models[spec_id]["model_pair_sha256"]),
            "model_refit": False,
            "family_membership_changed": False,
            "holm_adjusted_p": None,
            "holm_positive": False,
            "terminal_state": None,
        }
        result["evaluation_sha256"] = _digest(result)
        raw_results.append(result)

    if [row["hypothesis_id"] for row in raw_results] != [str(row["hypothesis_id"]) for row in family]:
        raise ExternalEvidence8HHoldoutError("8h_g_result_family_order_or_membership_drift")
    adjusted = _holm(raw_results, float(contract["multiplicity_control"]["family_wise_alpha"]))
    hypothesis_states = {str(row["hypothesis_id"]): str(row["terminal_state"]) for row in adjusted}
    confirmed = [str(row["hypothesis_id"]) for row in adjusted if row["terminal_state"] == "HOLDOUT_CONFIRMED"]
    state = HOLDOUT_FAMILY_COMPLETE if all_sufficient else HOLDOUT_FAMILY_CONSUMED_INSUFFICIENT

    completion = None
    if all_sufficient:
        completion = {
            "schema_version": HOLDOUT_COMPLETION_SCHEMA,
            "phase": "8H-G",
            "empirically_complete": True,
            "8h_f_validation_sha256": validation_sha,
            "8h_f_validation_family_receipt_sha256": receipt_sha,
            "holdout_authorization_sha256": auth_sha,
            "ledger_before_sha256": ledger_before_sha,
            "confirmatory_family": family,
            "hypothesis_states": hypothesis_states,
            "confirmed_hypotheses": confirmed,
            "holdout_result_sha256s": [str(row["evaluation_sha256"]) for row in adjusted],
            "holm_method": "Holm",
            "family_wise_alpha": 0.05,
            "model_refit": False,
            "family_shrunk_or_expanded": False,
            "zero_confirmed_hypotheses_is_valid_terminal_result": True,
            "automatic_promotion_authorized": False,
            "maximum_future_approval_scope": "APPROVED_FOR_8I_RESEARCH_ONLY",
            "8h_h_promotion_review_eligible": True,
            "production_authorized": False,
            "phase7_mutation_authorized": False,
            "phase8i_integration_authorized": False,
            "orders_or_trades_authorized": False,
            "next_subphase": "8H-H_INTERACTION_PROMOTION_REVIEW",
        }
        completion["completion_sha256"] = _digest(completion)

    result: dict[str, Any] = {
        "schema_version": HOLDOUT_RESULT_SCHEMA,
        "phase": "8H-G",
        "state": state,
        "8h_c_freeze_sha256": freeze_sha,
        "8h_d_construction_sha256": design_sha,
        "8h_e_discovery_sha256": discovery_sha,
        "8h_f_validation_sha256": validation_sha,
        "8h_f_validation_family_receipt_sha256": receipt_sha,
        "split_manifest_sha256": manifest_sha,
        "holdout_authorization_sha256": auth_sha,
        "confirmatory_family": family,
        "holdout_results": adjusted,
        "hypothesis_states": hypothesis_states,
        "confirmed_hypotheses": confirmed,
        "holdout_completion": completion,
        "full_family_holdout_complete": all_sufficient,
        "holm_applied_to_full_family": True,
        "8h_h_promotion_review_eligible": all_sufficient,
        "holdout_outcomes_opened": True,
        "holdout_open_count": 1,
        "model_refit": False,
        "family_shrunk_or_expanded": False,
        "outcome_access_log": {
            "opened_by": str(opened_by),
            "opened_at": open_time.isoformat(),
            "human_outcome_inspection_occurred": bool(human_outcome_inspection_occurred),
            "human_inspector_identity": str(human_inspector_identity) if human_outcome_inspection_occurred else None,
            "inspection_note": str(inspection_note) if human_outcome_inspection_occurred else None,
        },
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subphase": "8H-H_INTERACTION_PROMOTION_REVIEW" if all_sufficient else "8H-G_TERMINAL_INSUFFICIENT_NO_REOPEN",
    }
    result["holdout_sha256"] = _digest(result)

    consumed = deepcopy(dict(ledger))
    consumed.pop("ledger_sha256", None)
    consumed["state"] = "CONSUMED"
    consumed["holdout_open_count"] = 1
    consumed["terminal_result_sha256"] = str(result["holdout_sha256"])
    consumed["completion_sha256"] = None if completion is None else str(completion["completion_sha256"])
    consumed["ledger_sha256"] = _digest(consumed)
    return result, consumed
