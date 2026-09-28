"""Phase 8H-B interaction eligibility / promotion binding.

This module binds only upstream-approved factor x horizon components.  It never
selects an interaction candidate and never reads an interaction outcome.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence

from scanner.research.external_evidence.promotion_review_8g import (
    PROMOTION_COMPLETION_SCHEMA,
    PROMOTION_DECISION_SCHEMA,
    PROMOTION_PACKET_SCHEMA,
)
from scanner.research.external_evidence.prospective_8g import PROSPECTIVE_COMPLETION_SCHEMA
from scanner.research.external_evidence.research_8g import ACTIVE_FACTORS, HORIZONS


ELIGIBILITY_BINDING_SCHEMA = "external_evidence_8h_interaction_eligibility_binding_v1"
ELIGIBILITY_RESULT_SCHEMA = "external_evidence_8h_interaction_eligibility_result_v1"
WAITING_STATE = "WAITING_FOR_8G_H_EMPIRICAL_COMPLETION"
NO_PROMOTED_STATE = "NO_PROMOTED_FACTORS"
BOUND_STATE = "ELIGIBLE_FACTOR_HORIZONS_BOUND"
APPROVED_STATE = "APPROVED_FOR_8H_RESEARCH_ONLY"


class ExternalEvidence8HEligibilityError(ValueError):
    """Raised when an upstream promotion cannot be safely bound into 8H."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_embedded_digest(value: Mapping[str, Any], field: str, error: str) -> str:
    recorded = str(value.get(field) or "")
    if len(recorded) != 64:
        raise ExternalEvidence8HEligibilityError(error)
    payload = dict(value)
    payload.pop(field, None)
    if _digest(payload) != recorded:
        raise ExternalEvidence8HEligibilityError(error)
    return recorded


def _unique_text_list(value: object, error: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ExternalEvidence8HEligibilityError(error)
    rows = [str(x) for x in value]
    if len(rows) != len(set(rows)):
        raise ExternalEvidence8HEligibilityError(error)
    return rows


def _parse_hypothesis_id(hypothesis_id: str, contract: Mapping[str, Any]) -> tuple[str, int]:
    try:
        factor_id, horizon_text = hypothesis_id.rsplit("_x_", 1)
        horizon = int(horizon_text.removesuffix("t"))
    except (ValueError, AttributeError) as exc:
        raise ExternalEvidence8HEligibilityError(f"invalid_factor_horizon_id:{hypothesis_id}") from exc
    unit = contract["eligibility_unit"]
    if factor_id not in set(unit["allowed_factor_ids"]):
        raise ExternalEvidence8HEligibilityError(f"factor_not_allowed_for_8h:{factor_id}")
    if horizon not in {int(x) for x in unit["allowed_horizons_sessions"]}:
        raise ExternalEvidence8HEligibilityError(f"horizon_not_allowed_for_8h:{horizon}")
    if hypothesis_id != f"{factor_id}_x_{horizon}t":
        raise ExternalEvidence8HEligibilityError(f"noncanonical_factor_horizon_id:{hypothesis_id}")
    return factor_id, horizon


def _source_registry_index(source_registry: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    if source_registry.get("schema_version") != "external_source_registry_v1":
        raise ExternalEvidence8HEligibilityError("unsupported_source_registry")
    rows = source_registry.get("sources")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        raise ExternalEvidence8HEligibilityError("source_registry_sources_missing")
    out: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        source_id = str(row.get("source_id") or "")
        if not source_id:
            continue
        if source_id in out:
            raise ExternalEvidence8HEligibilityError(f"duplicate_source_registry_id:{source_id}")
        out[source_id] = row
    return out


def validate_binding_contract(
    contract: Mapping[str, Any], challenger_specs: Mapping[str, Any]
) -> None:
    if contract.get("schema_version") != ELIGIBILITY_BINDING_SCHEMA:
        raise ExternalEvidence8HEligibilityError("unsupported_8h_b_binding_contract")
    if contract.get("phase") != "8H-B":
        raise ExternalEvidence8HEligibilityError("8h_b_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HEligibilityError("8h_b_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HEligibilityError(f"8h_b_boundary_must_remain_false:{key}")

    unit = contract.get("eligibility_unit") or {}
    if tuple(unit.get("allowed_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8HEligibilityError("8h_b_active_factor_family_mismatch")
    if tuple(int(x) for x in unit.get("allowed_horizons_sessions") or ()) != HORIZONS:
        raise ExternalEvidence8HEligibilityError("8h_b_horizon_family_mismatch")
    if unit.get("required_upstream_state") != APPROVED_STATE:
        raise ExternalEvidence8HEligibilityError("8h_b_required_approval_state_mismatch")

    if challenger_specs.get("schema_version") != "external_evidence_8g_challenger_specs_v1":
        raise ExternalEvidence8HEligibilityError("8g_challenger_specs_required")
    if tuple(challenger_specs.get("active_confirmatory_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8HEligibilityError("8g_challenger_active_factor_mismatch")
    factor_specs = challenger_specs.get("factor_specs")
    if not isinstance(factor_specs, Mapping) or set(factor_specs) != set(ACTIVE_FACTORS):
        raise ExternalEvidence8HEligibilityError("8g_factor_specs_identity_mismatch")

    boundary = contract.get("outcome_blind_boundary") or {}
    if boundary.get("interaction_outcomes_may_be_read_in_8h_b") is not False:
        raise ExternalEvidence8HEligibilityError("8h_b_interaction_outcomes_must_remain_closed")
    if boundary.get("interaction_candidates_may_be_selected_in_8h_b") is not False:
        raise ExternalEvidence8HEligibilityError("8h_b_candidate_selection_must_remain_closed")
    if boundary.get("economic_theory_ranking_allowed") is not False:
        raise ExternalEvidence8HEligibilityError("8h_b_theory_ranking_must_remain_closed")


def _result(
    *,
    state: str,
    eligible_components: list[dict[str, Any]],
    upstream_complete: bool,
    upstream_completion_sha256: str | None,
    wait_reason: str | None = None,
) -> dict[str, Any]:
    ids = [row["factor_horizon_id"] for row in eligible_components]
    result: dict[str, Any] = {
        "schema_version": ELIGIBILITY_RESULT_SCHEMA,
        "phase": "8H-B",
        "state": state,
        "upstream_promotion_review_empirically_complete": upstream_complete,
        "upstream_completion_sha256": upstream_completion_sha256,
        "wait_reason": wait_reason,
        "eligible_factor_horizon_ids": ids,
        "eligible_components": eligible_components,
        "interaction_spec_freeze_authorized": bool(ids),
        "interaction_outcome_access_authorized": False,
        "empirical_interaction_research_enabled": False,
        "concrete_interaction_specs": [],
        "automatic_candidate_generation_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "hard_rule": "NO_PROMOTED_FACTORS -> NO_EMPIRICAL_INTERACTION_RESEARCH",
        "next_subphase": "8H-C_OUTCOME_BLIND_INTERACTION_SPEC_FREEZE",
    }
    result["binding_sha256"] = _digest(result)
    return result


def _validate_timestamp(value: object) -> None:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8HEligibilityError("promotion_review_timestamp_required")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8HEligibilityError("promotion_review_timestamp_invalid") from exc
    if parsed.tzinfo is None:
        raise ExternalEvidence8HEligibilityError("promotion_review_timestamp_timezone_required")


def bind_interaction_eligibility(
    *,
    contract: Mapping[str, Any],
    challenger_specs: Mapping[str, Any],
    source_registry: Mapping[str, Any],
    promotion_completion: Mapping[str, Any] | None = None,
    prospective_completion: Mapping[str, Any] | None = None,
    review_packets: Mapping[str, Mapping[str, Any]] | None = None,
    decisions: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Bind 8G-H approvals without selecting or evaluating any interaction."""
    validate_binding_contract(contract, challenger_specs)
    packets = dict(review_packets or {})
    decision_rows = dict(decisions or {})

    if promotion_completion is None:
        if packets or decision_rows:
            raise ExternalEvidence8HEligibilityError("orphan_8g_h_review_artifacts_without_completion")
        return _result(
            state=WAITING_STATE,
            eligible_components=[],
            upstream_complete=False,
            upstream_completion_sha256=None,
            wait_reason="8G_H_COMPLETION_ARTIFACT_MISSING",
        )

    if promotion_completion.get("schema_version") != PROMOTION_COMPLETION_SCHEMA:
        raise ExternalEvidence8HEligibilityError("8g_h_completion_schema_mismatch")
    if promotion_completion.get("phase") != "8G-H":
        raise ExternalEvidence8HEligibilityError("8g_h_completion_phase_mismatch")
    completion_sha = _verify_embedded_digest(
        promotion_completion, "completion_sha256", "8g_h_completion_digest_mismatch"
    )
    state = str(promotion_completion.get("state") or "")
    empirically_complete = promotion_completion.get("empirically_complete") is True

    if state == "PROMOTION_REVIEW_INCOMPLETE" or not empirically_complete:
        return _result(
            state=WAITING_STATE,
            eligible_components=[],
            upstream_complete=False,
            upstream_completion_sha256=completion_sha,
            wait_reason="8G_H_PROMOTION_REVIEW_NOT_EMPIRICALLY_COMPLETE",
        )

    accepted_terminal = set(contract["upstream_artifact_contract"]["accepted_terminal_completion_states"])
    if state not in accepted_terminal:
        raise ExternalEvidence8HEligibilityError(f"8g_h_completion_state_not_bindable:{state}")
    if prospective_completion is None:
        raise ExternalEvidence8HEligibilityError("8g_g_prospective_completion_required")
    if prospective_completion.get("schema_version") != PROSPECTIVE_COMPLETION_SCHEMA:
        raise ExternalEvidence8HEligibilityError("8g_g_prospective_completion_schema_mismatch")
    if prospective_completion.get("empirically_complete") is not True:
        raise ExternalEvidence8HEligibilityError("8g_g_prospective_completion_not_empirically_complete")
    prospective_sha = _verify_embedded_digest(
        prospective_completion, "completion_sha256", "8g_g_prospective_completion_digest_mismatch"
    )

    hypothesis_states = prospective_completion.get("hypothesis_states")
    if not isinstance(hypothesis_states, Mapping):
        raise ExternalEvidence8HEligibilityError("8g_g_hypothesis_states_missing")
    confirmed = [str(h) for h, hstate in hypothesis_states.items() if hstate == "PROSPECTIVE_CONFIRMED"]
    for hypothesis_id in confirmed:
        _parse_hypothesis_id(hypothesis_id, contract)

    eligible = _unique_text_list(
        promotion_completion.get("eligible_hypotheses") or [], "8g_h_eligible_hypotheses_invalid"
    )
    pending = _unique_text_list(
        promotion_completion.get("pending_hypotheses") or [], "8g_h_pending_hypotheses_invalid"
    )
    approved = _unique_text_list(
        promotion_completion.get("approved_for_8h_research") or [], "8g_h_approved_hypotheses_invalid"
    )
    if eligible != confirmed:
        raise ExternalEvidence8HEligibilityError("8g_h_eligible_set_does_not_match_prospective_confirmation")
    if pending:
        raise ExternalEvidence8HEligibilityError("terminal_8g_h_completion_cannot_have_pending_hypotheses")

    dispositions = promotion_completion.get("dispositions")
    if not isinstance(dispositions, Mapping):
        raise ExternalEvidence8HEligibilityError("8g_h_dispositions_missing")
    if set(str(k) for k in dispositions) != set(eligible):
        raise ExternalEvidence8HEligibilityError("8g_h_disposition_identity_mismatch")
    derived_approved = [h for h in eligible if str(dispositions[h]) == APPROVED_STATE]
    if approved != derived_approved:
        raise ExternalEvidence8HEligibilityError("8g_h_approved_list_does_not_match_dispositions")

    if set(packets) != set(eligible) or set(decision_rows) != set(eligible):
        if eligible or packets or decision_rows:
            raise ExternalEvidence8HEligibilityError("8g_h_packet_decision_family_mismatch")

    if state == "NO_PROMOTION_CANDIDATES":
        if eligible or approved or dispositions:
            raise ExternalEvidence8HEligibilityError("no_promotion_candidates_state_contains_candidates")
        return _result(
            state=NO_PROMOTED_STATE,
            eligible_components=[],
            upstream_complete=True,
            upstream_completion_sha256=completion_sha,
        )
    if state != "PROMOTION_REVIEW_COMPLETE":
        raise ExternalEvidence8HEligibilityError("terminal_8g_h_completion_state_invalid")

    registry = _source_registry_index(source_registry)
    factor_specs = challenger_specs["factor_specs"]
    bound_components: list[dict[str, Any]] = []

    for hypothesis_id in eligible:
        packet = packets[hypothesis_id]
        decision = decision_rows[hypothesis_id]
        factor_id, horizon = _parse_hypothesis_id(hypothesis_id, contract)

        if packet.get("schema_version") != PROMOTION_PACKET_SCHEMA or packet.get("phase") != "8G-H":
            raise ExternalEvidence8HEligibilityError(f"promotion_packet_schema_mismatch:{hypothesis_id}")
        if str(packet.get("hypothesis_id")) != hypothesis_id:
            raise ExternalEvidence8HEligibilityError(f"promotion_packet_identity_mismatch:{hypothesis_id}")
        packet_sha = _verify_embedded_digest(
            packet, "packet_sha256", f"promotion_packet_digest_mismatch:{hypothesis_id}"
        )
        if str(packet.get("prospective_completion_sha256") or "") != prospective_sha:
            raise ExternalEvidence8HEligibilityError(
                f"promotion_packet_prospective_digest_mismatch:{hypothesis_id}"
            )
        if str(packet.get("factor_id") or "") != factor_id or int(packet.get("horizon_sessions", -1)) != horizon:
            raise ExternalEvidence8HEligibilityError(f"promotion_packet_factor_horizon_mismatch:{hypothesis_id}")

        if decision.get("schema_version") != PROMOTION_DECISION_SCHEMA or decision.get("phase") != "8G-H":
            raise ExternalEvidence8HEligibilityError(f"promotion_decision_schema_mismatch:{hypothesis_id}")
        if str(decision.get("hypothesis_id")) != hypothesis_id:
            raise ExternalEvidence8HEligibilityError(f"promotion_decision_identity_mismatch:{hypothesis_id}")
        decision_sha = _verify_embedded_digest(
            decision, "decision_sha256", f"promotion_decision_digest_mismatch:{hypothesis_id}"
        )
        if str(decision.get("packet_sha256") or "") != packet_sha:
            raise ExternalEvidence8HEligibilityError(f"promotion_decision_packet_digest_mismatch:{hypothesis_id}")
        if str(decision.get("state") or "") != str(dispositions[hypothesis_id]):
            raise ExternalEvidence8HEligibilityError(f"promotion_decision_disposition_mismatch:{hypothesis_id}")
        if not str(decision.get("reviewer_identity") or "").strip():
            raise ExternalEvidence8HEligibilityError(f"promotion_reviewer_identity_required:{hypothesis_id}")
        if not str(decision.get("rationale") or "").strip():
            raise ExternalEvidence8HEligibilityError(f"promotion_review_rationale_required:{hypothesis_id}")
        _validate_timestamp(decision.get("reviewed_at"))

        for field in (
            "authorizes_production",
            "authorizes_phase7_mutation",
            "authorizes_8i_integration",
            "authorizes_orders_or_trades",
            "rejection_authorizes_inverse_signal",
        ):
            if decision.get(field) is not False:
                raise ExternalEvidence8HEligibilityError(
                    f"promotion_decision_forbidden_authorization:{hypothesis_id}:{field}"
                )

        is_approved = str(decision.get("state")) == APPROVED_STATE
        if bool(decision.get("authorizes_8h_research")) != is_approved:
            raise ExternalEvidence8HEligibilityError(f"promotion_8h_authorization_state_mismatch:{hypothesis_id}")
        if not is_approved:
            continue
        if str(decision.get("decision")) != "APPROVE_FOR_8H_RESEARCH_ONLY":
            raise ExternalEvidence8HEligibilityError(f"approved_state_without_approval_decision:{hypothesis_id}")

        if packet.get("promotion_eligible_from_8g_g") is not True:
            raise ExternalEvidence8HEligibilityError(f"approved_packet_not_prospectively_eligible:{hypothesis_id}")
        if packet.get("8g_g_state") != "PROSPECTIVE_CONFIRMED":
            raise ExternalEvidence8HEligibilityError(f"approved_packet_not_prospectively_confirmed:{hypothesis_id}")
        if packet.get("all_10_criteria_pass") is not True:
            raise ExternalEvidence8HEligibilityError(f"approved_packet_criteria_not_passed:{hypothesis_id}")
        criteria = packet.get("criteria")
        if not isinstance(criteria, Mapping) or len(criteria) != 10:
            raise ExternalEvidence8HEligibilityError(f"approved_packet_requires_exact_10_criteria:{hypothesis_id}")
        if any(not isinstance(row, Mapping) or row.get("status") != "PASS" for row in criteria.values()):
            raise ExternalEvidence8HEligibilityError(f"approved_packet_contains_nonpass_criterion:{hypothesis_id}")
        if packet.get("governance_clear") is not True or list(packet.get("blockers") or ()):
            raise ExternalEvidence8HEligibilityError(f"approved_packet_governance_not_clear:{hypothesis_id}")

        source_governance = packet.get("source_governance")
        if not isinstance(source_governance, Mapping):
            raise ExternalEvidence8HEligibilityError(f"approved_packet_source_governance_missing:{hypothesis_id}")
        if source_governance.get("factor_id") != factor_id:
            raise ExternalEvidence8HEligibilityError(f"approved_packet_source_factor_mismatch:{hypothesis_id}")
        if source_governance.get("all_sources_clear") is not True or list(source_governance.get("blockers") or ()):
            raise ExternalEvidence8HEligibilityError(f"approved_packet_source_governance_blocked:{hypothesis_id}")

        spec = factor_specs[factor_id]
        expected_source_ids = [str(spec.get("source_id") or "")]
        required_source_ids = _unique_text_list(
            source_governance.get("required_source_ids") or [],
            f"approved_packet_required_source_ids_invalid:{hypothesis_id}",
        )
        if required_source_ids != expected_source_ids or not expected_source_ids[0]:
            raise ExternalEvidence8HEligibilityError(f"approved_packet_source_spec_mismatch:{hypothesis_id}")
        source_states = source_governance.get("source_states")
        if not isinstance(source_states, Mapping):
            raise ExternalEvidence8HEligibilityError(f"approved_packet_source_states_missing:{hypothesis_id}")

        source_row_hashes: dict[str, str] = {}
        for source_id in required_source_ids:
            if source_id not in registry:
                raise ExternalEvidence8HEligibilityError(f"approved_source_not_resolved_in_bound_registry:{source_id}")
            state_row = source_states.get(source_id)
            if not isinstance(state_row, Mapping) or state_row.get("status") != "CLEAR":
                raise ExternalEvidence8HEligibilityError(
                    f"approved_source_not_clear_in_review_packet:{hypothesis_id}:{source_id}"
                )
            source_row_hashes[source_id] = _digest(registry[source_id])

        bound_components.append(
            {
                "factor_horizon_id": hypothesis_id,
                "factor_id": factor_id,
                "horizon_sessions": horizon,
                "eligibility_state": "ELIGIBLE_FOR_8H_INTERACTION_SPEC_FREEZE_ONLY",
                "factor_spec_sha256": _digest(spec),
                "factor_spec_feature_fields": list(spec.get("feature_fields") or ()),
                "factor_spec_source_id": str(spec.get("source_id") or ""),
                "factor_spec_series_ids": list(spec.get("series_ids") or ()),
                "8g_challenger_specs_git_blob_sha": contract["parent_freeze"]["8g_challenger_specs_git_blob_sha"],
                "source_registry_git_blob_sha": contract["parent_freeze"]["source_registry_git_blob_sha"],
                "source_registry_row_sha256_by_source": source_row_hashes,
                "prospective_completion_sha256": prospective_sha,
                "promotion_packet_sha256": packet_sha,
                "promotion_decision_sha256": decision_sha,
                "promotion_completion_sha256": completion_sha,
                "reviewer_identity": str(decision["reviewer_identity"]),
                "reviewed_at": str(decision["reviewed_at"]),
                "interaction_outcome_access_authorized": False,
                "production_authorized": False,
                "phase7_mutation_authorized": False,
                "phase8i_integration_authorized": False,
                "orders_or_trades_authorized": False,
            }
        )

    if [row["factor_horizon_id"] for row in bound_components] != approved:
        raise ExternalEvidence8HEligibilityError("bound_component_family_does_not_match_approved_family")

    return _result(
        state=BOUND_STATE if bound_components else NO_PROMOTED_STATE,
        eligible_components=bound_components,
        upstream_complete=True,
        upstream_completion_sha256=completion_sha,
    )
