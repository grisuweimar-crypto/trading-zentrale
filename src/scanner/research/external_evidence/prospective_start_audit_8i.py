"""Outcome-blind Phase 8I-E prospective-start audit.

The blanket start stays 2026-09-29T00:00:00Z. Exactly one pinned late-28-Sep
scanner snapshot can be admitted as the first prospective observation. All
other pre-start observations stay shadow-only. No outcomes or Phase-7 changes.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping

from scanner.research.external_evidence.decision_binding_8i import (
    _validate_identity_correction,
    validate_binding_contract,
)

AUDIT_SCHEMA = "external_evidence_8i_prospective_start_audit_v1"
CORRECTION_SCHEMA = "external_evidence_upstream_source_identity_correction_v1"
AUDITED_PRESTART_SHADOW = "AUDITED_PRESTART_SHADOW"
ADMITTED_FIRST_PROSPECTIVE = "ADMITTED_FIRST_PROSPECTIVE"
STANDARD_PROSPECTIVE = "STANDARD_PROSPECTIVE"
NOT_ELIGIBLE = "NOT_ELIGIBLE"
_FORBIDDEN_DATA_KEYS = {
    "peer_excess", "future_return", "adverse_excursion", "path_max_drawdown",
    "outcome", "outcome_value", "outcomes", "p_value", "confidence_interval",
    "holm_adjusted_p",
}


class ExternalEvidence8IProspectiveStartAuditError(ValueError):
    pass


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: object, error: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or "").strip().replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ExternalEvidence8IProspectiveStartAuditError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IProspectiveStartAuditError(error + "_timezone_required")
    return parsed


def _verify_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    recorded = str(row.get(field) or "")
    payload = dict(row)
    payload.pop(field, None)
    if len(recorded) != 64 or digest(payload) != recorded:
        raise ExternalEvidence8IProspectiveStartAuditError(error)
    return recorded


def _reject_outcome_data(value: object, path: str = "root") -> None:
    """Reject realized/statistical data while allowing boundary metadata such as outcomes_read=false."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            lower = str(key).lower()
            if lower in _FORBIDDEN_DATA_KEYS or lower.startswith("label_"):
                raise ExternalEvidence8IProspectiveStartAuditError(
                    f"8i_e_start_audit_outcome_field_forbidden:{path}.{key}"
                )
            _reject_outcome_data(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_outcome_data(child, f"{path}[{index}]")


def validate_source_identity_correction(
    correction: Mapping[str, Any], binding_contract: Mapping[str, Any]
) -> str:
    validate_binding_contract(binding_contract)
    if correction.get("schema_version") != CORRECTION_SCHEMA:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_schema_mismatch")
    correction_sha = _verify_digest(correction, "correction_sha256", "8i_e_source_correction_digest_mismatch")
    if correction.get("state") != "RESOLVED_OUTCOME_BLIND_VERSIONED":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_state_invalid")
    if correction.get("outcomes_read") is not False or correction.get("retroactive_evidence_rewrite") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_must_be_outcome_blind_nonretroactive")
    if correction.get("identity_only") is not True:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_must_be_identity_only")
    required_forbidden = {
        "observed_source_values", "factor_values", "predictions", "model_pair_or_model_hashes",
        "direction_or_sign_rules", "thresholds_or_deadbands", "horizons", "mapping_semantics",
        "outcome_labels_or_values", "evidence_consumption_state", "historical_promotion_receipts",
    }
    if set(correction.get("forbidden_changes") or ()) != required_forbidden:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_scope_drift")
    if correction.get("binding_contract", {}).get("git_blob_sha") != "0bba5affe58aa837f9750ae46431dc6306f7af06":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_binding_contract_drift")
    try:
        b_sha = _validate_identity_correction(
            correction, binding_contract, ("federal_reserve_board_h15", "ecb_data_portal")
        )
    except ValueError as exc:
        raise ExternalEvidence8IProspectiveStartAuditError(str(exc)) from exc
    if b_sha != correction_sha:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_b_digest_mismatch")
    return correction_sha


def validate_audit_contract(
    audit: Mapping[str, Any], correction: Mapping[str, Any], binding_contract: Mapping[str, Any]
) -> str:
    _reject_outcome_data(audit)
    if audit.get("schema_version") != AUDIT_SCHEMA or audit.get("phase") != "8I-E":
        raise ExternalEvidence8IProspectiveStartAuditError("unsupported_8i_e_start_audit")
    if audit.get("state") != "FROZEN_OUTCOME_BLIND_EXACT_SNAPSHOT_EXCEPTION":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_start_audit_state_drift")
    if audit.get("research_only") is not True or audit.get("shadow_mode") is not True or audit.get("outcomes_read") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_start_audit_boundary_drift")
    audit_sha = _verify_digest(audit, "audit_sha256", "8i_e_start_audit_digest_mismatch")
    if audit.get("canonical_blanket_start_utc") != "2026-09-29T00:00:00+00:00" or audit.get("canonical_blanket_start_unchanged") is not True:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_canonical_start_must_remain_sep29")

    shadow = audit.get("audited_prestart_shadow") or {}
    if shadow.get("default_classification") != AUDITED_PRESTART_SHADOW or shadow.get("counts_toward_8i_e_confirmatory_family") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_shadow_must_remain_nonconfirmatory")

    correction_sha = validate_source_identity_correction(correction, binding_contract)
    pin = audit.get("source_identity_correction") or {}
    if pin.get("git_blob_sha") != "e87f5f643defd6868bf1a43f18aa6e25287061c9" or pin.get("correction_sha256") != correction_sha:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_pin_mismatch")

    exception = audit.get("exact_first_prospective_exception") or {}
    expected = {
        "snapshot_id": "e2444010-ad30-4c08-af92-037bb0cc28d6",
        "attempt_id": "e2444010-ad30-4c08-af92-037bb0cc28d6",
        "started_at": "2026-09-28T16:17:13.819811+00:00",
        "generated_at": "2026-09-28T16:17:31.855174+00:00",
        "completed_tickers": 213,
        "total_tickers": 213,
        "scanner_commit_sha": "efd4d60fd9fa1ca57555f9d68f2515995bc4439b",
        "history_metadata_git_blob_sha": "78097bc67ab4d2d2d727d5ebc634aade912f99a6",
        "latest_scanner_git_blob_sha": "92463ee9cb2ca570659a99f5afcbf648d00f96bb",
        "run_source": "github-36449782706-1",
    }
    if exception.get("classification") != ADMITTED_FIRST_PROSPECTIVE:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_exact_snapshot_classification_drift")
    for key, expected_value in expected.items():
        if exception.get(key) != expected_value:
            raise ExternalEvidence8IProspectiveStartAuditError(f"8i_e_exact_snapshot_pin_drift:{key}")
    freezes = exception.get("required_prior_freezes") or {}
    if freezes != {
        "8i_d_final_commit_sha": "bac30fa99b921c5cd1c85e89cfa6942bbe5c79f8",
        "8i_e_contract_commit_sha": "7b43953ab40e927b8c8f9da02d78f96dff36d201",
        "8i_e_contract_frozen_at_utc": "2026-09-28T15:32:28+00:00",
    }:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_prior_freeze_identity_drift")
    if _parse_time(exception["generated_at"], "8i_e_exact_snapshot_time_invalid") <= _parse_time(freezes["8i_e_contract_frozen_at_utc"], "8i_e_freeze_time_invalid"):
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_snapshot_must_follow_contract_freeze")

    decision = audit.get("decision_boundary") or {}
    for key in ("phase7_mutation_authorized", "extended_reliability_enabled", "extended_stance_enabled", "portfolio_action_change_authorized", "orders_or_trades_authorized"):
        if decision.get(key) is not False:
            raise ExternalEvidence8IProspectiveStartAuditError(f"8i_e_start_audit_decision_leak:{key}")
    return audit_sha


def classify_snapshot(
    *, audit: Mapping[str, Any], correction: Mapping[str, Any],
    binding_contract: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> dict[str, Any]:
    audit_sha = validate_audit_contract(audit, correction, binding_contract)
    _reject_outcome_data(snapshot)
    generated = _parse_time(snapshot.get("generated_at"), "8i_e_snapshot_generated_at_invalid")
    canonical = _parse_time(audit["canonical_blanket_start_utc"], "8i_e_canonical_start_invalid")
    shadow_start = _parse_time(audit["audited_prestart_shadow"]["start_utc"], "8i_e_shadow_start_invalid")
    exception = audit["exact_first_prospective_exception"]
    snapshot_id = str(snapshot.get("snapshot_id") or "")

    if snapshot_id == exception["snapshot_id"]:
        exact_fields = ("snapshot_id", "attempt_id", "started_at", "generated_at", "completed_tickers", "total_tickers", "scanner_commit_sha", "history_metadata_git_blob_sha", "latest_scanner_git_blob_sha", "run_source")
        mismatches = [key for key in exact_fields if snapshot.get(key) != exception.get(key)]
        if mismatches:
            raise ExternalEvidence8IProspectiveStartAuditError("8i_e_exact_snapshot_provenance_mismatch:" + ",".join(mismatches))
        classification, confirmatory, reason = ADMITTED_FIRST_PROSPECTIVE, True, "exact_post_freeze_snapshot_exception"
    elif generated >= canonical:
        classification, confirmatory, reason = STANDARD_PROSPECTIVE, True, "on_or_after_canonical_blanket_start"
    elif generated >= shadow_start:
        classification, confirmatory, reason = AUDITED_PRESTART_SHADOW, False, "prestart_shadow_not_confirmatory"
    else:
        classification, confirmatory, reason = NOT_ELIGIBLE, False, "before_audited_window"

    result = {
        "schema_version": "external_evidence_8i_prospective_start_classification_v1",
        "phase": "8I-E", "snapshot_id": snapshot_id, "generated_at": generated.isoformat(),
        "classification": classification,
        "counts_toward_8i_e_confirmatory_family_if_other_gates_pass": confirmatory,
        "reason": reason, "audit_sha256": audit_sha,
        "canonical_blanket_start_utc": audit["canonical_blanket_start_utc"],
        "canonical_blanket_start_unchanged": True, "outcomes_read": False,
        "phase7_mutation_authorized": False, "extended_reliability_enabled": False,
        "extended_stance_enabled": False, "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False, "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
    }
    result["classification_sha256"] = digest(result)
    return result
