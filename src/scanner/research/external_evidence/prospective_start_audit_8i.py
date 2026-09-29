"""Outcome-blind Phase 8I-E prospective-start audit.

The canonical blanket start remains 2026-09-29T00:00:00Z. One exact scanner
snapshot from late 2026-09-28 may be classified as the first prospective
observation because its identity is pinned after the D/E freezes. Earlier rows
remain shadow-only. This module never reads outcomes or changes Phase 7.
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

_OUTCOMEISH = (
    "peer_excess",
    "future_return",
    "adverse_excursion",
    "path_max_drawdown",
    "label_",
    "outcome",
    "p_value",
    "confidence_interval",
    "holm_adjusted_p",
)


class ExternalEvidence8IProspectiveStartAuditError(ValueError):
    """Raised when prospective-start audit identity or outcome-blindness drifts."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: object, error: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError) as exc:
        raise ExternalEvidence8IProspectiveStartAuditError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IProspectiveStartAuditError(error + "_timezone_required")
    return parsed


def _verify_embedded_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    recorded = str(row.get(field) or "")
    if len(recorded) != 64:
        raise ExternalEvidence8IProspectiveStartAuditError(error)
    payload = dict(row)
    payload.pop(field, None)
    if digest(payload) != recorded:
        raise ExternalEvidence8IProspectiveStartAuditError(error)
    return recorded


def _reject_outcomeish(value: object, path: str = "root") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            lower = str(key).lower()
            if any(part in lower for part in _OUTCOMEISH):
                raise ExternalEvidence8IProspectiveStartAuditError(
                    f"8i_e_start_audit_outcome_field_forbidden:{path}.{key}"
                )
            _reject_outcomeish(child, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_outcomeish(child, f"{path}[{index}]")


def validate_source_identity_correction(
    correction: Mapping[str, Any], binding_contract: Mapping[str, Any]
) -> str:
    """Validate the versioned identity-only correction against frozen 8I-B."""
    validate_binding_contract(binding_contract)
    if correction.get("schema_version") != CORRECTION_SCHEMA:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_schema_mismatch")
    correction_sha = _verify_embedded_digest(
        correction, "correction_sha256", "8i_e_source_correction_digest_mismatch"
    )
    if correction.get("state") != "RESOLVED_OUTCOME_BLIND_VERSIONED":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_state_invalid")
    if correction.get("outcomes_read") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_not_outcome_blind")
    if correction.get("retroactive_evidence_rewrite") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_rewrite_forbidden")
    if correction.get("identity_only") is not True:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_must_be_identity_only")
    required_forbidden = {
        "observed_source_values",
        "factor_values",
        "predictions",
        "model_pair_or_model_hashes",
        "direction_or_sign_rules",
        "thresholds_or_deadbands",
        "horizons",
        "mapping_semantics",
        "outcome_labels_or_values",
        "evidence_consumption_state",
        "historical_promotion_receipts",
    }
    if set(correction.get("forbidden_changes") or ()) != required_forbidden:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_scope_drift")
    expected_blob = str(correction.get("binding_contract", {}).get("git_blob_sha") or "")
    if expected_blob != "0bba5affe58aa837f9750ae46431dc6306f7af06":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_binding_contract_drift")
    try:
        b_sha = _validate_identity_correction(
            correction,
            binding_contract,
            ("federal_reserve_board_h15", "ecb_data_portal"),
        )
    except ValueError as exc:
        raise ExternalEvidence8IProspectiveStartAuditError(str(exc)) from exc
    if b_sha != correction_sha:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_b_digest_mismatch")
    return correction_sha


def validate_audit_contract(
    audit: Mapping[str, Any],
    correction: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
) -> str:
    """Validate the frozen snapshot-level exception without opening outcomes."""
    _reject_outcomeish(audit)
    if audit.get("schema_version") != AUDIT_SCHEMA or audit.get("phase") != "8I-E":
        raise ExternalEvidence8IProspectiveStartAuditError("unsupported_8i_e_start_audit")
    if audit.get("state") != "FROZEN_OUTCOME_BLIND_EXACT_SNAPSHOT_EXCEPTION":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_start_audit_state_drift")
    if audit.get("research_only") is not True or audit.get("shadow_mode") is not True:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_start_audit_must_be_research_shadow")
    if audit.get("outcomes_read") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_start_audit_must_be_outcome_blind")
    audit_sha = _verify_embedded_digest(audit, "audit_sha256", "8i_e_start_audit_digest_mismatch")
    if audit.get("canonical_blanket_start_utc") != "2026-09-29T00:00:00+00:00":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_canonical_start_must_remain_sep29")
    if audit.get("canonical_blanket_start_unchanged") is not True:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_canonical_start_change_forbidden")

    shadow = audit.get("audited_prestart_shadow") or {}
    if shadow.get("default_classification") != AUDITED_PRESTART_SHADOW:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_shadow_classification_drift")
    if shadow.get("counts_toward_8i_e_confirmatory_family") is not False:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_shadow_must_not_count")

    correction_sha = validate_source_identity_correction(correction, binding_contract)
    pinned = audit.get("source_identity_correction") or {}
    if pinned.get("git_blob_sha") != "e87f5f643defd6868bf1a43f18aa6e25287061c9":
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_source_correction_blob_drift")
    if pinned.get("correction_sha256") != correction_sha:
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
    for key, value in expected.items():
        if exception.get(key) != value:
            raise ExternalEvidence8IProspectiveStartAuditError(f"8i_e_exact_snapshot_pin_drift:{key}")
    if exception.get("classification") != ADMITTED_FIRST_PROSPECTIVE:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_exact_snapshot_classification_drift")
    freezes = exception.get("required_prior_freezes") or {}
    if freezes != {
        "8i_d_final_commit_sha": "bac30fa99b921c5cd1c85e89cfa6942bbe5c79f8",
        "8i_e_contract_commit_sha": "7b43953ab40e927b8c8f9da02d78f96dff36d201",
        "8i_e_contract_frozen_at_utc": "2026-09-28T15:32:28+00:00",
    }:
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_prior_freeze_identity_drift")
    if _parse_time(exception["generated_at"], "8i_e_exact_snapshot_time_invalid") <= _parse_time(
        freezes["8i_e_contract_frozen_at_utc"], "8i_e_freeze_time_invalid"
    ):
        raise ExternalEvidence8IProspectiveStartAuditError("8i_e_snapshot_must_follow_contract_freeze")

    decision = audit.get("decision_boundary") or {}
    for key in (
        "phase7_mutation_authorized",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_authorized",
        "orders_or_trades_authorized",
    ):
        if decision.get(key) is not False:
            raise ExternalEvidence8IProspectiveStartAuditError(f"8i_e_start_audit_decision_leak:{key}")
    return audit_sha


def classify_snapshot(
    *,
    audit: Mapping[str, Any],
    correction: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Classify one outcome-free snapshot under the frozen start audit."""
    audit_sha = validate_audit_contract(audit, correction, binding_contract)
    _reject_outcomeish(snapshot)
    generated = _parse_time(snapshot.get("generated_at"), "8i_e_snapshot_generated_at_invalid")
    canonical = _parse_time(audit["canonical_blanket_start_utc"], "8i_e_canonical_start_invalid")
    shadow_start = _parse_time(
        audit["audited_prestart_shadow"]["start_utc"], "8i_e_shadow_start_invalid"
    )
    exception = audit["exact_first_prospective_exception"]

    snapshot_id = str(snapshot.get("snapshot_id") or "")
    if snapshot_id == exception["snapshot_id"]:
        exact_fields = (
            "snapshot_id",
            "attempt_id",
            "started_at",
            "generated_at",
            "completed_tickers",
            "total_tickers",
            "scanner_commit_sha",
            "history_metadata_git_blob_sha",
            "latest_scanner_git_blob_sha",
            "run_source",
        )
        mismatches = [key for key in exact_fields if snapshot.get(key) != exception.get(key)]
        if mismatches:
            raise ExternalEvidence8IProspectiveStartAuditError(
                "8i_e_exact_snapshot_provenance_mismatch:" + ",".join(mismatches)
            )
        classification = ADMITTED_FIRST_PROSPECTIVE
        confirmatory = True
        reason = "exact_post_freeze_snapshot_exception"
    elif generated >= canonical:
        classification = STANDARD_PROSPECTIVE
        confirmatory = True
        reason = "on_or_after_canonical_blanket_start"
    elif generated >= shadow_start:
        classification = AUDITED_PRESTART_SHADOW
        confirmatory = False
        reason = "prestart_shadow_not_confirmatory"
    else:
        classification = NOT_ELIGIBLE
        confirmatory = False
        reason = "before_audited_window"

    result = {
        "schema_version": "external_evidence_8i_prospective_start_classification_v1",
        "phase": "8I-E",
        "snapshot_id": snapshot_id,
        "generated_at": generated.isoformat(),
        "classification": classification,
        "counts_toward_8i_e_confirmatory_family_if_other_gates_pass": confirmatory,
        "reason": reason,
        "audit_sha256": audit_sha,
        "canonical_blanket_start_utc": audit["canonical_blanket_start_utc"],
        "canonical_blanket_start_unchanged": True,
        "outcomes_read": False,
        "phase7_mutation_authorized": False,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
    }
    result["classification_sha256"] = digest(result)
    return result
