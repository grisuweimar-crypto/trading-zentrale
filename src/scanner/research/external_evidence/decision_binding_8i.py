"""Phase 8I-B promotion and provenance binding.

This module binds already-promoted external components for later 8I research.
It never reads market/decision outcomes, aggregates external evidence, mutates
Phase 7, changes portfolio actions, or authorizes orders/trades.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence


BINDING_CONTRACT_SCHEMA = "external_evidence_8i_promotion_provenance_binding_v1"
MAIN_EFFECT_AUTH_SCHEMA = "external_evidence_8i_main_effect_authorization_v1"
SOURCE_IDENTITY_CORRECTION_SCHEMA = "external_evidence_upstream_source_identity_correction_v1"
BINDING_RESULT_SCHEMA = "external_evidence_8i_promotion_provenance_binding_result_v1"

UPSTREAM_8G_DECISION_SCHEMA = "external_evidence_8g_promotion_decision_v1"
UPSTREAM_8H_DECISION_SCHEMA = "external_evidence_8h_interaction_promotion_decision_v1"
APPROVED_8G = "APPROVED_FOR_8H_RESEARCH_ONLY"
APPROVED_8H = "APPROVED_FOR_8I_RESEARCH_ONLY"
AUTHORIZED_8I = "AUTHORIZED_FOR_8I_RESEARCH_BINDING_ONLY"

WAITING = "WAITING_FOR_UPSTREAM_PROMOTION_EVIDENCE"
NO_PROMOTED = "NO_PROMOTED_EXTERNAL_EVIDENCE"
NOT_BINDABLE = "PROMOTED_EXTERNAL_EVIDENCE_NOT_BINDABLE"
UPSTREAM_IDENTITY_BLOCKED = "BLOCKED_UPSTREAM_SOURCE_IDENTITY_CONTRACT"
BOUND = "BOUND_FOR_8I_RESEARCH_ONLY"


class ExternalEvidence8IBindingError(ValueError):
    """Raised when an 8I-B governance/provenance boundary is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_embedded_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    recorded = str(row.get(field) or "")
    if len(recorded) != 64:
        raise ExternalEvidence8IBindingError(error)
    payload = dict(row)
    payload.pop(field, None)
    if digest(payload) != recorded:
        raise ExternalEvidence8IBindingError(error)
    return recorded


def _parse_time(value: object, error: str) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8IBindingError(error)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8IBindingError(error) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExternalEvidence8IBindingError(error + "_timezone_required")
    return parsed


def _unique_text_list(value: object, error: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ExternalEvidence8IBindingError(error)
    rows = [str(x) for x in value]
    if not rows or any(not x for x in rows) or len(rows) != len(set(rows)):
        raise ExternalEvidence8IBindingError(error)
    return rows


def validate_binding_contract(contract: Mapping[str, Any]) -> None:
    if contract.get("schema_version") != BINDING_CONTRACT_SCHEMA or contract.get("phase") != "8I-B":
        raise ExternalEvidence8IBindingError("unsupported_8i_b_contract")
    if contract.get("research_only") is not True or contract.get("shadow_mode") is not True:
        raise ExternalEvidence8IBindingError("8i_b_must_be_research_only_shadow")
    for key in (
        "productive_integration_enabled",
        "phase7_mutation_enabled",
        "external_state_engine_enabled",
        "extended_reliability_enabled",
        "extended_stance_enabled",
        "portfolio_action_change_enabled",
        "orders_or_trades_enabled",
        "real_decision_outcome_read_allowed",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8IBindingError(f"8i_b_boundary_must_remain_false:{key}")

    boundary = contract.get("outcome_blind_boundary") or {}
    forbidden_true = (
        "real_decision_outcomes_read_in_8i_b",
        "outcome_columns_allowed_as_binding_inputs",
        "metric_selection_allowed",
        "threshold_selection_allowed",
        "sign_selection_allowed",
        "aggregation_allowed",
        "reliability_recompute_allowed",
        "stance_recompute_allowed",
        "portfolio_action_recompute_allowed",
    )
    if any(boundary.get(key) is not False for key in forbidden_true):
        raise ExternalEvidence8IBindingError("8i_b_outcome_blind_boundary_drift")
    if boundary.get("aggregation_belongs_to") != "8I-C":
        raise ExternalEvidence8IBindingError("8i_b_aggregation_must_remain_deferred_to_8i_c")

    main = contract.get("component_types", {}).get("8g_main_effect", {})
    if main.get("required_upstream_state") != APPROVED_8G:
        raise ExternalEvidence8IBindingError("8i_b_8g_upstream_state_drift")
    if main.get("separate_manual_8i_authorization_required") is not True:
        raise ExternalEvidence8IBindingError("8i_b_8g_manual_authorization_required")
    if main.get("8i_authorization_state") != AUTHORIZED_8I:
        raise ExternalEvidence8IBindingError("8i_b_8g_authorization_state_drift")
    if main.get("automatic_transitive_authorization_forbidden") is not True:
        raise ExternalEvidence8IBindingError("8i_b_transitive_8g_promotion_must_be_forbidden")

    interaction = contract.get("component_types", {}).get("8h_interaction", {})
    if interaction.get("required_upstream_state") != APPROVED_8H:
        raise ExternalEvidence8IBindingError("8i_b_8h_upstream_state_drift")
    if interaction.get("automatic_decision_activation_forbidden") is not True:
        raise ExternalEvidence8IBindingError("8i_b_8h_auto_activation_must_be_forbidden")


def current_binding_status(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Return the repository's current fail-closed 8I-B state without outcomes."""
    validate_binding_contract(contract)
    current = dict(contract.get("current_repository_state") or {})
    expected = contract.get("upstream_source_identity_issue", {}).get("status")
    if expected == "OPEN_TECHNICAL_UPSTREAM_CONTRACT_BLOCKER":
        state = UPSTREAM_IDENTITY_BLOCKED
    else:
        state = WAITING
    return {
        "schema_version": BINDING_RESULT_SCHEMA,
        "phase": "8I-B",
        "state": state,
        "bound_8g_main_effect_ids": list(current.get("bound_8g_main_effect_ids") or ()),
        "bound_8h_interaction_ids": list(current.get("bound_8h_interaction_ids") or ()),
        "external_evidence_state": "INSUFFICIENT_EXTERNAL",
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "external_decision_influence_enabled": False,
        "phase7_mutation_authorized": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
    }


def record_main_effect_authorization(
    *,
    contract: Mapping[str, Any],
    component_id: str,
    upstream_promotion_decision: Mapping[str, Any],
    decision: str,
    reviewer_identity: str,
    reviewed_at: str,
    rationale: str,
) -> dict[str, Any]:
    """Create the additional manual 8I research-binding authorization for 8G."""
    validate_binding_contract(contract)
    if upstream_promotion_decision.get("schema_version") != UPSTREAM_8G_DECISION_SCHEMA:
        raise ExternalEvidence8IBindingError("8i_b_8g_promotion_decision_required")
    upstream_sha = _verify_embedded_digest(
        upstream_promotion_decision,
        "decision_sha256",
        "8i_b_8g_promotion_decision_digest_mismatch",
    )
    if str(upstream_promotion_decision.get("hypothesis_id") or "") != str(component_id):
        raise ExternalEvidence8IBindingError("8i_b_8g_component_identity_mismatch")
    if upstream_promotion_decision.get("state") != APPROVED_8G:
        raise ExternalEvidence8IBindingError("8i_b_8g_component_not_upstream_approved")
    if upstream_promotion_decision.get("authorizes_8i_integration") is not False:
        raise ExternalEvidence8IBindingError("8i_b_8g_upstream_scope_drift")

    reviewer = str(reviewer_identity or "").strip()
    why = str(rationale or "").strip()
    if not reviewer:
        raise ExternalEvidence8IBindingError("8i_b_reviewer_identity_required")
    if not why:
        raise ExternalEvidence8IBindingError("8i_b_review_rationale_required")
    reviewed = _parse_time(reviewed_at, "8i_b_review_timestamp_invalid")

    decision = str(decision or "").upper()
    allowed = set(contract["main_effect_manual_authorization"]["allowed_decisions"])
    if decision not in allowed:
        raise ExternalEvidence8IBindingError("8i_b_main_effect_authorization_decision_invalid")
    state = {
        "AUTHORIZE_FOR_8I_RESEARCH_BINDING_ONLY": AUTHORIZED_8I,
        "DEFER": "DEFERRED_8I_RESEARCH_BINDING_AUTHORIZATION",
        "REJECT": "REJECTED_8I_RESEARCH_BINDING_AUTHORIZATION",
    }[decision]
    receipt: dict[str, Any] = {
        "schema_version": MAIN_EFFECT_AUTH_SCHEMA,
        "phase": "8I-B",
        "component_type": "8g_main_effect",
        "component_id": str(component_id),
        "upstream_promotion_state": APPROVED_8G,
        "upstream_promotion_decision_sha256": upstream_sha,
        "decision": decision,
        "state": state,
        "reviewer_identity": reviewer,
        "reviewed_at": reviewed.isoformat(),
        "rationale": why,
        "authorizes_8i_research_binding": state == AUTHORIZED_8I,
        "authorizes_decision_influence": False,
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_portfolio_action_change": False,
        "authorizes_orders_or_trades": False,
        "rejection_authorizes_inverse_signal": False,
    }
    receipt["authorization_sha256"] = digest(receipt)
    return receipt


def _source_catalog_index(source_catalog: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    if source_catalog.get("schema_version") != "external_evidence_8f_source_candidates_v1":
        raise ExternalEvidence8IBindingError("8i_b_8f_source_catalog_required")
    rows = source_catalog.get("sources")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        raise ExternalEvidence8IBindingError("8i_b_source_catalog_rows_missing")
    result: dict[str, Mapping[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        source_id = str(raw.get("source_id") or "")
        if not source_id:
            continue
        if source_id in result:
            raise ExternalEvidence8IBindingError(f"8i_b_duplicate_source_id:{source_id}")
        result[source_id] = raw
    return result


def _validate_identity_correction(
    correction: Mapping[str, Any] | None,
    contract: Mapping[str, Any],
    canonical_source_ids: Sequence[str],
) -> str:
    issue = contract.get("upstream_source_identity_issue") or {}
    if issue.get("status") != "OPEN_TECHNICAL_UPSTREAM_CONTRACT_BLOCKER":
        return "NOT_REQUIRED"
    if not isinstance(correction, Mapping):
        raise ExternalEvidence8IBindingError("8i_b_upstream_source_identity_correction_required")
    if correction.get("schema_version") != SOURCE_IDENTITY_CORRECTION_SCHEMA:
        raise ExternalEvidence8IBindingError("8i_b_source_identity_correction_schema_mismatch")
    correction_sha = _verify_embedded_digest(
        correction, "correction_sha256", "8i_b_source_identity_correction_digest_mismatch"
    )
    if correction.get("state") != "RESOLVED_OUTCOME_BLIND_VERSIONED":
        raise ExternalEvidence8IBindingError("8i_b_source_identity_blocker_not_resolved")
    if correction.get("outcomes_read") is not False:
        raise ExternalEvidence8IBindingError("8i_b_source_identity_correction_must_be_outcome_blind")
    aliases = dict(correction.get("alias_to_canonical") or {})
    if aliases != dict(issue.get("8g_frozen_aliases") or {}):
        raise ExternalEvidence8IBindingError("8i_b_source_identity_correction_alias_map_mismatch")
    if not set(canonical_source_ids).issubset(set(aliases.values())):
        raise ExternalEvidence8IBindingError("8i_b_corrected_source_not_covered_by_identity_receipt")
    if correction.get("retroactive_evidence_rewrite") is not False:
        raise ExternalEvidence8IBindingError("8i_b_retroactive_source_identity_rewrite_forbidden")
    return correction_sha


def bind_promoted_component(
    *,
    contract: Mapping[str, Any],
    component: Mapping[str, Any],
    upstream_promotion_decision: Mapping[str, Any],
    provenance: Mapping[str, Any],
    source_catalog: Mapping[str, Any],
    component_artifact: Mapping[str, Any],
    mapping_artifact: Mapping[str, Any],
    main_effect_authorization: Mapping[str, Any] | None = None,
    source_identity_correction: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind one promoted component without granting any decision influence."""
    validate_binding_contract(contract)
    component_type = str(component.get("component_type") or "")
    component_id = str(component.get("component_id") or "")
    factor_id = str(component.get("factor_id") or "")
    if component_type not in {"8g_main_effect", "8h_interaction"} or not component_id or not factor_id:
        raise ExternalEvidence8IBindingError("8i_b_component_identity_required")
    try:
        horizon = int(component.get("horizon_sessions"))
    except (TypeError, ValueError) as exc:
        raise ExternalEvidence8IBindingError("8i_b_component_horizon_required") from exc
    if horizon <= 0:
        raise ExternalEvidence8IBindingError("8i_b_component_horizon_invalid")

    if component_type == "8g_main_effect":
        if upstream_promotion_decision.get("schema_version") != UPSTREAM_8G_DECISION_SCHEMA:
            raise ExternalEvidence8IBindingError("8i_b_8g_promotion_decision_required")
        promotion_sha = _verify_embedded_digest(
            upstream_promotion_decision, "decision_sha256", "8i_b_8g_promotion_decision_digest_mismatch"
        )
        if upstream_promotion_decision.get("state") != APPROVED_8G:
            raise ExternalEvidence8IBindingError("8i_b_8g_component_not_upstream_approved")
        if str(upstream_promotion_decision.get("hypothesis_id") or "") != component_id:
            raise ExternalEvidence8IBindingError("8i_b_8g_component_identity_mismatch")
        if not isinstance(main_effect_authorization, Mapping):
            raise ExternalEvidence8IBindingError("8i_b_8g_separate_authorization_required")
        if main_effect_authorization.get("schema_version") != MAIN_EFFECT_AUTH_SCHEMA:
            raise ExternalEvidence8IBindingError("8i_b_8g_authorization_schema_mismatch")
        auth_sha = _verify_embedded_digest(
            main_effect_authorization, "authorization_sha256", "8i_b_8g_authorization_digest_mismatch"
        )
        if main_effect_authorization.get("state") != AUTHORIZED_8I or main_effect_authorization.get("authorizes_8i_research_binding") is not True:
            raise ExternalEvidence8IBindingError("8i_b_8g_not_authorized_for_research_binding")
        if str(main_effect_authorization.get("component_id") or "") != component_id:
            raise ExternalEvidence8IBindingError("8i_b_8g_authorization_component_mismatch")
        if str(main_effect_authorization.get("upstream_promotion_decision_sha256") or "") != promotion_sha:
            raise ExternalEvidence8IBindingError("8i_b_8g_authorization_upstream_digest_mismatch")
    else:
        if upstream_promotion_decision.get("schema_version") != UPSTREAM_8H_DECISION_SCHEMA:
            raise ExternalEvidence8IBindingError("8i_b_8h_promotion_decision_required")
        promotion_sha = _verify_embedded_digest(
            upstream_promotion_decision, "decision_sha256", "8i_b_8h_promotion_decision_digest_mismatch"
        )
        if upstream_promotion_decision.get("state") != APPROVED_8H:
            raise ExternalEvidence8IBindingError("8i_b_8h_component_not_upstream_approved")
        if str(upstream_promotion_decision.get("hypothesis_id") or "") != component_id:
            raise ExternalEvidence8IBindingError("8i_b_8h_component_identity_mismatch")
        if upstream_promotion_decision.get("authorizes_8i_research_design") is not True:
            raise ExternalEvidence8IBindingError("8i_b_8h_research_scope_missing")
        auth_sha = None

    source_ids = _unique_text_list(provenance.get("source_ids"), "8i_b_source_ids_required")
    catalog = _source_catalog_index(source_catalog)
    missing = [source_id for source_id in source_ids if source_id not in catalog]
    if missing:
        raise ExternalEvidence8IBindingError("8i_b_unresolved_source_ids:" + ",".join(sorted(missing)))
    expected_source_hashes = {source_id: digest(catalog[source_id]) for source_id in source_ids}
    if dict(provenance.get("source_identity_hashes") or {}) != expected_source_hashes:
        raise ExternalEvidence8IBindingError("8i_b_source_identity_hash_mismatch")

    correction_sha = _validate_identity_correction(source_identity_correction, contract, source_ids)

    component_hash = digest(component_artifact)
    if str(provenance.get("component_artifact_hash") or "") != component_hash:
        raise ExternalEvidence8IBindingError("8i_b_component_artifact_hash_mismatch")
    mapping_hash = digest(mapping_artifact)
    if str(provenance.get("mapping_artifact_hash") or "") != mapping_hash:
        raise ExternalEvidence8IBindingError("8i_b_mapping_artifact_hash_mismatch")
    if not str(provenance.get("mapping_version") or "").strip():
        raise ExternalEvidence8IBindingError("8i_b_mapping_version_required")
    if not str(provenance.get("model_or_spec_version") or "").strip():
        raise ExternalEvidence8IBindingError("8i_b_model_or_spec_version_required")

    as_of = _parse_time(provenance.get("as_of"), "8i_b_as_of_invalid")
    valid_from = _parse_time(provenance.get("valid_from"), "8i_b_valid_from_invalid")
    if valid_from > as_of:
        raise ExternalEvidence8IBindingError("8i_b_valid_from_after_as_of")
    pit_status = str(provenance.get("pit_status") or "")
    allowed_pit = set(contract["provenance_rules"]["pit_status_allowed"])
    if pit_status not in allowed_pit:
        raise ExternalEvidence8IBindingError("8i_b_pit_status_not_allowed")
    if pit_status == "PARTIAL" and not str(provenance.get("partial_pit_justification") or "").strip():
        raise ExternalEvidence8IBindingError("8i_b_partial_pit_justification_required")
    consumption = str(provenance.get("evidence_consumption_status") or "")
    if consumption not in set(contract["provenance_rules"]["evidence_consumption_status_allowed"]):
        raise ExternalEvidence8IBindingError("8i_b_evidence_consumption_status_not_allowed")

    if str(provenance.get("upstream_promotion_receipt_sha256") or "") != promotion_sha:
        raise ExternalEvidence8IBindingError("8i_b_promotion_receipt_hash_mismatch")
    if component_type == "8g_main_effect" and str(provenance.get("8i_authorization_receipt_sha256_if_required") or "") != auth_sha:
        raise ExternalEvidence8IBindingError("8i_b_authorization_receipt_hash_mismatch")

    result: dict[str, Any] = {
        "schema_version": BINDING_RESULT_SCHEMA,
        "phase": "8I-B",
        "state": BOUND,
        "component_type": component_type,
        "component_id": component_id,
        "factor_id": factor_id,
        "interaction_id_if_applicable": component_id if component_type == "8h_interaction" else None,
        "parent_factor_ids_if_applicable": list(component.get("parent_factor_ids") or ()) if component_type == "8h_interaction" else [],
        "horizon_sessions": horizon,
        "model_or_spec_version": str(provenance["model_or_spec_version"]),
        "component_artifact_hash": component_hash,
        "upstream_promotion_state": APPROVED_8G if component_type == "8g_main_effect" else APPROVED_8H,
        "upstream_promotion_receipt_sha256": promotion_sha,
        "8i_authorization_receipt_sha256_if_required": auth_sha,
        "source_ids": source_ids,
        "source_registry_or_catalog_identity": "external_evidence_8f_source_candidates_v1",
        "source_identity_hashes": expected_source_hashes,
        "source_identity_correction_sha256": correction_sha,
        "as_of": as_of.isoformat(),
        "valid_from": valid_from.isoformat(),
        "mapping_version": str(provenance["mapping_version"]),
        "mapping_artifact_hash": mapping_hash,
        "pit_status": pit_status,
        "evidence_consumption_status": consumption,
        "external_decision_influence_enabled": False,
        "decision_outcome_access_authorized": False,
        "phase7_mutation_authorized": False,
        "productive_integration_enabled": False,
        "portfolio_action_change_authorized": False,
        "orders_or_trades_authorized": False,
        "next_subblock": "8I-C_OUTCOME_BLIND_EXTERNAL_AGGREGATION_DESIGN",
    }
    result["binding_sha256"] = digest(result)
    return result
