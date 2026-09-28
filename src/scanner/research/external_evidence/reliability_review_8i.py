"""Real Phase 8I-E evaluation/readiness review without opening decision outcomes.

This module is deliberately metadata-only until the frozen terminal family gate
passes.  It records whether the real 8I-E evaluation can progress, while never
reading peer_excess or other forward outcomes and never changing Phase 7.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping


REVIEW_SCHEMA = "external_evidence_8i_reliability_review_status_v1"
CORRECTION_SCHEMA = "external_evidence_upstream_source_identity_correction_v1"


class ExternalEvidence8IReviewError(ValueError):
    """Raised when the metadata-only 8I-E review boundary is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: object, error: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8IReviewError(error)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8IReviewError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IReviewError(error + "_timezone_required")
    return parsed


def _verify_correction(receipt: Mapping[str, Any]) -> str:
    if receipt.get("schema_version") != CORRECTION_SCHEMA:
        raise ExternalEvidence8IReviewError("8i_review_source_identity_receipt_schema_mismatch")
    if receipt.get("state") != "RESOLVED_OUTCOME_BLIND_VERSIONED":
        raise ExternalEvidence8IReviewError("8i_review_source_identity_receipt_not_resolved")
    if receipt.get("outcomes_read") is not False or receipt.get("retroactive_evidence_rewrite") is not False:
        raise ExternalEvidence8IReviewError("8i_review_source_identity_receipt_boundary_violation")
    expected_aliases = {
        "FED_H15": "federal_reserve_board_h15",
        "ECB_EXR": "ecb_data_portal",
    }
    if dict(receipt.get("alias_to_canonical") or {}) != expected_aliases:
        raise ExternalEvidence8IReviewError("8i_review_source_identity_alias_map_mismatch")
    recorded = str(receipt.get("correction_sha256") or "")
    payload = dict(receipt)
    payload.pop("correction_sha256", None)
    if len(recorded) != 64 or digest(payload) != recorded:
        raise ExternalEvidence8IReviewError("8i_review_source_identity_receipt_digest_mismatch")
    return recorded


def _reject_outcome_fields(value: Mapping[str, Any], *, path: str) -> None:
    forbidden = (
        "peer_excess",
        "future_return",
        "forward_return",
        "adverse_excursion",
        "path_max_drawdown",
        "observed_outcome",
        "realized_outcome",
        "p_value",
        "holm_adjusted",
        "confidence_interval",
    )
    for key, item in value.items():
        lowered = str(key).lower()
        if any(token in lowered for token in forbidden):
            raise ExternalEvidence8IReviewError(f"8i_review_outcome_field_forbidden:{path}.{key}")
        if isinstance(item, Mapping):
            _reject_outcome_fields(item, path=f"{path}.{key}")


def build_real_review_status(
    *,
    reliability_contract: Mapping[str, Any],
    binding_contract: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    holdout_ledger: Mapping[str, Any],
    source_identity_correction: Mapping[str, Any],
    research_as_of: object,
) -> dict[str, Any]:
    """Start/re-run the real 8I-E review using metadata only.

    This is not the one-shot terminal outcome evaluation.  It establishes the
    real readiness state and documents every blocker before any outcome can be
    opened.
    """
    for name, row in (
        ("reliability_contract", reliability_contract),
        ("binding_contract", binding_contract),
        ("split_manifest", split_manifest),
        ("holdout_ledger", holdout_ledger),
    ):
        if not isinstance(row, Mapping):
            raise ExternalEvidence8IReviewError(f"8i_review_{name}_required")
        _reject_outcome_fields(row, path=name)

    if reliability_contract.get("schema_version") != "external_evidence_8i_reliability_extension_research_v1":
        raise ExternalEvidence8IReviewError("8i_review_reliability_contract_mismatch")
    if binding_contract.get("schema_version") != "external_evidence_8i_promotion_provenance_binding_v1":
        raise ExternalEvidence8IReviewError("8i_review_binding_contract_mismatch")
    if split_manifest.get("schema_version") != "external_evidence_8g_split_manifest_v1":
        raise ExternalEvidence8IReviewError("8i_review_8g_manifest_mismatch")
    if holdout_ledger.get("schema_version") != "external_evidence_8g_holdout_consumption_ledger_v1":
        raise ExternalEvidence8IReviewError("8i_review_8g_holdout_ledger_mismatch")

    correction_sha = _verify_correction(source_identity_correction)
    now = _parse_time(research_as_of, "8i_review_research_as_of_invalid")
    not_before = _parse_time(
        reliability_contract.get("prospective_evidence", {}).get("prospective_not_before"),
        "8i_review_prospective_not_before_invalid",
    )

    streams = split_manifest.get("streams") or {}
    if not isinstance(streams, Mapping):
        raise ExternalEvidence8IReviewError("8i_review_8g_streams_required")
    assignment_count = 0
    collecting_streams = 0
    for stream in streams.values():
        if not isinstance(stream, Mapping):
            raise ExternalEvidence8IReviewError("8i_review_invalid_8g_stream")
        assignment_count += len(list(stream.get("assignments") or ()))
        if stream.get("status") == "COLLECTING":
            collecting_streams += 1

    ledger_streams = holdout_ledger.get("streams") or {}
    if not isinstance(ledger_streams, Mapping):
        raise ExternalEvidence8IReviewError("8i_review_8g_holdout_streams_required")
    holdout_evaluation_count = sum(
        1 for stream in ledger_streams.values()
        if isinstance(stream, Mapping) and str(stream.get("evaluation_sha256") or "")
    )

    current_binding = binding_contract.get("current_repository_state") or {}
    bound_8g = list(current_binding.get("bound_8g_main_effect_ids") or ())
    bound_8h = list(current_binding.get("bound_8h_interaction_ids") or ())
    upstream_8g_promotions_present = bool(current_binding.get("real_8g_promotions_present"))
    upstream_8h_promotions_present = bool(current_binding.get("real_8h_promotions_present"))

    blockers: list[str] = []
    if now < not_before:
        blockers.append("BEFORE_8I_E_PROSPECTIVE_START")
    if assignment_count == 0:
        blockers.append("NO_8G_PROSPECTIVE_ASSIGNMENTS")
    if holdout_evaluation_count == 0:
        blockers.append("NO_8G_HOLDOUT_EVALUATIONS")
    if not upstream_8g_promotions_present and not upstream_8h_promotions_present:
        blockers.append("NO_UPSTREAM_8G_OR_8H_PROMOTIONS")
    if not bound_8g and not bound_8h:
        blockers.append("NO_8I_BOUND_EXTERNAL_COMPONENTS")

    if now < not_before:
        state = "STARTED_WAITING_FOR_PROSPECTIVE_START"
    elif assignment_count == 0:
        state = "STARTED_WAITING_FOR_FIRST_8G_PROSPECTIVE_ASSIGNMENT"
    elif holdout_evaluation_count == 0:
        state = "STARTED_COLLECTING_UPSTREAM_8G_EVIDENCE"
    elif not upstream_8g_promotions_present and not upstream_8h_promotions_present:
        state = "STARTED_WAITING_FOR_UPSTREAM_PROMOTIONS"
    elif not bound_8g and not bound_8h:
        state = "STARTED_WAITING_FOR_8I_BINDING"
    else:
        state = "READY_FOR_REAL_8I_E_ANNOTATION_AND_TERMINAL_GATE_TRACKING"

    result: dict[str, Any] = {
        "schema_version": REVIEW_SCHEMA,
        "phase": "8I-E",
        "state": state,
        "evaluation_started": True,
        "review_started": True,
        "research_as_of": now.isoformat(),
        "prospective_not_before": not_before.isoformat(),
        "before_prospective_start": now < not_before,
        "source_identity_correction": {
            "state": "RESOLVED_OUTCOME_BLIND_VERSIONED_RECEIPT_AVAILABLE",
            "correction_sha256": correction_sha,
            "retroactive_evidence_rewrite": False,
        },
        "upstream_8g": {
            "manifest_id": split_manifest.get("manifest_id"),
            "stream_count": len(streams),
            "collecting_stream_count": collecting_streams,
            "prospective_assignment_count": assignment_count,
            "holdout_evaluation_count": holdout_evaluation_count,
            "promotion_present": upstream_8g_promotions_present,
        },
        "upstream_8h": {
            "promotion_present": upstream_8h_promotions_present,
        },
        "8i_binding": {
            "bound_8g_main_effect_ids": bound_8g,
            "bound_8h_interaction_ids": bound_8h,
            "bound_component_count": len(bound_8g) + len(bound_8h),
        },
        "blockers": blockers,
        "terminal_outcome_gate_ready": False,
        "real_outcomes_opened": False,
        "real_outcome_values_read_by_review": False,
        "terminal_family_evaluation_complete": False,
        "empirical_review_complete": False,
        "phase7_reliability_preserved": True,
        "extended_reliability_enabled": False,
        "extended_stance_enabled": False,
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }
    result["review_sha256"] = digest(result)
    return result
