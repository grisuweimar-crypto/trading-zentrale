"""Phase 8H-H manual promotion review for confirmed interaction hypotheses.

8H-H is a governance boundary, not another model-selection stage. Only hypotheses
terminally confirmed by the one-shot 8H-G Holdout may receive a review packet.
Approval can authorize 8I research design only; production, Phase-7 mutation,
8I integration and orders/trades remain forbidden.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence

from scanner.research.external_evidence.interaction_holdout_8h import (
    HOLDOUT_COMPLETION_SCHEMA,
    HOLDOUT_FAMILY_COMPLETE,
    HOLDOUT_LEDGER_SCHEMA,
    HOLDOUT_RESULT_SCHEMA,
)
from scanner.research.external_evidence.interaction_spec_8h import (
    INTERACTION_FREEZE_RESULT_SCHEMA,
    SPECS_FROZEN,
)


PROMOTION_CONTRACT_SCHEMA = "external_evidence_8h_interaction_promotion_review_v1"
PROMOTION_PACKET_SCHEMA = "external_evidence_8h_interaction_promotion_review_packet_v1"
PROMOTION_DECISION_SCHEMA = "external_evidence_8h_interaction_promotion_decision_v1"
PROMOTION_COMPLETION_SCHEMA = "external_evidence_8h_interaction_promotion_review_completion_v1"
WAITING_FOR_8H_G = "WAITING_FOR_8H_G_EMPIRICAL_COMPLETION"
NO_PROMOTION_CANDIDATES = "NO_PROMOTION_CANDIDATES"
PROMOTION_REVIEW_INCOMPLETE = "PROMOTION_REVIEW_INCOMPLETE"
PROMOTION_REVIEW_COMPLETE = "PROMOTION_REVIEW_COMPLETE"
APPROVED_STATE = "APPROVED_FOR_8I_RESEARCH_ONLY"


class ExternalEvidence8HPromotionError(ValueError):
    """Raised when the 8H-H manual promotion boundary is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _verify_digest(row: Mapping[str, Any], field: str, error: str) -> str:
    expected = str(row.get(field) or "")
    if not expected:
        raise ExternalEvidence8HPromotionError(error)
    payload = dict(row)
    payload.pop(field, None)
    if _digest(payload) != expected:
        raise ExternalEvidence8HPromotionError(error)
    return expected


def _parse_time(value: object) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8HPromotionError("8h_h_review_timestamp_required")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8HPromotionError("8h_h_review_timestamp_invalid") from exc
    if parsed.tzinfo is None:
        raise ExternalEvidence8HPromotionError("8h_h_review_timestamp_timezone_required")
    return parsed


def validate_promotion_contract(contract: Mapping[str, Any]) -> None:
    if contract.get("schema_version") != PROMOTION_CONTRACT_SCHEMA or contract.get("phase") != "8H-H":
        raise ExternalEvidence8HPromotionError("8h_h_contract_schema_or_phase_mismatch")
    if contract.get("research_only") is not True:
        raise ExternalEvidence8HPromotionError("8h_h_must_be_research_only")
    for key in (
        "productive_integration_enabled",
        "automatic_promotion_enabled",
        "phase7_mutation_enabled",
        "phase8i_integration_enabled",
        "orders_or_trades_enabled",
    ):
        if contract.get(key) is not False:
            raise ExternalEvidence8HPromotionError(f"8h_h_forbidden_contract_authorization:{key}")
    criteria = list(contract.get("promotion_criteria") or ())
    if len(criteria) != 10 or [int(x.get("criterion", 0)) for x in criteria] != list(range(1, 11)):
        raise ExternalEvidence8HPromotionError("8h_h_exact_10_ordered_criteria_required")
    if any(x.get("required") is not True for x in criteria):
        raise ExternalEvidence8HPromotionError("8h_h_all_criteria_must_be_required")
    manual = contract.get("manual_review")
    if not isinstance(manual, Mapping) or manual.get("required") is not True:
        raise ExternalEvidence8HPromotionError("8h_h_manual_review_required")
    if manual.get("automatic_approval_allowed") is not False or manual.get("rejection_authorizes_inverse_signal") is not False:
        raise ExternalEvidence8HPromotionError("8h_h_auto_or_inverse_promotion_forbidden")
    if contract.get("approval_scope", {}).get("maximum_state") != APPROVED_STATE:
        raise ExternalEvidence8HPromotionError("8h_h_maximum_scope_must_be_8i_research_only")
    scope = contract.get("approval_scope") or {}
    for key in ("authorizes_8i_integration", "authorizes_production", "authorizes_phase7_mutation", "authorizes_orders_or_trades"):
        if scope.get(key) is not False:
            raise ExternalEvidence8HPromotionError(f"8h_h_scope_must_remain_false:{key}")
    guards = contract.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8HPromotionError("8h_h_guards_must_all_remain_false")


def _validate_holdout_chain(
    holdout_result: Mapping[str, Any] | None,
    consumed_ledger: Mapping[str, Any] | None,
) -> tuple[bool, str | None, dict[str, Any] | None, list[dict[str, Any]], dict[str, dict[str, Any]]]:
    if holdout_result is None:
        if consumed_ledger is not None:
            raise ExternalEvidence8HPromotionError("8h_h_orphan_consumed_ledger_without_holdout_result")
        return False, None, None, [], {}
    if holdout_result.get("schema_version") != HOLDOUT_RESULT_SCHEMA:
        raise ExternalEvidence8HPromotionError("8h_h_holdout_result_schema_mismatch")
    holdout_sha = _verify_digest(holdout_result, "holdout_sha256", "8h_h_holdout_result_digest_mismatch")
    family = [dict(x) for x in holdout_result.get("confirmatory_family") or ()]
    if holdout_result.get("state") != HOLDOUT_FAMILY_COMPLETE:
        if holdout_result.get("8h_h_promotion_review_eligible") is True:
            raise ExternalEvidence8HPromotionError("8h_h_noncomplete_holdout_cannot_authorize_review")
        if holdout_result.get("holdout_completion") is not None:
            raise ExternalEvidence8HPromotionError("8h_h_noncomplete_holdout_must_not_have_completion")
        if consumed_ledger is not None and consumed_ledger.get("completion_sha256"):
            raise ExternalEvidence8HPromotionError("8h_h_noncomplete_holdout_has_completion_ledger_hash")
        return False, holdout_sha, None, family, {}

    completion = holdout_result.get("holdout_completion")
    if not isinstance(completion, Mapping) or completion.get("schema_version") != HOLDOUT_COMPLETION_SCHEMA:
        raise ExternalEvidence8HPromotionError("8h_h_holdout_completion_required")
    completion = dict(completion)
    completion_sha = _verify_digest(completion, "completion_sha256", "8h_h_holdout_completion_digest_mismatch")
    if completion.get("empirically_complete") is not True or completion.get("8h_h_promotion_review_eligible") is not True:
        raise ExternalEvidence8HPromotionError("8h_h_empirically_complete_holdout_required")
    if holdout_result.get("full_family_holdout_complete") is not True:
        raise ExternalEvidence8HPromotionError("8h_h_full_family_holdout_required")
    if holdout_result.get("holm_applied_to_full_family") is not True:
        raise ExternalEvidence8HPromotionError("8h_h_full_family_holm_required")
    if holdout_result.get("family_shrunk_or_expanded") is not False or holdout_result.get("model_refit") is not False:
        raise ExternalEvidence8HPromotionError("8h_h_holdout_family_or_model_mutation_detected")
    if holdout_result.get("holdout_outcomes_opened") is not True or int(holdout_result.get("holdout_open_count") or 0) != 1:
        raise ExternalEvidence8HPromotionError("8h_h_one_terminal_holdout_open_required")
    if list(completion.get("confirmatory_family") or ()) != family:
        raise ExternalEvidence8HPromotionError("8h_h_completion_family_drift")
    if completion.get("model_refit") is not False or completion.get("family_shrunk_or_expanded") is not False:
        raise ExternalEvidence8HPromotionError("8h_h_completion_mutation_detected")
    if completion.get("automatic_promotion_authorized") is not False:
        raise ExternalEvidence8HPromotionError("8h_h_upstream_automatic_promotion_forbidden")
    if completion.get("maximum_future_approval_scope") != APPROVED_STATE:
        raise ExternalEvidence8HPromotionError("8h_h_upstream_maximum_scope_drift")

    if not isinstance(consumed_ledger, Mapping) or consumed_ledger.get("schema_version") != HOLDOUT_LEDGER_SCHEMA:
        raise ExternalEvidence8HPromotionError("8h_h_consumed_holdout_ledger_required")
    ledger_sha = _verify_digest(consumed_ledger, "ledger_sha256", "8h_h_consumed_ledger_digest_mismatch")
    if consumed_ledger.get("state") != "CONSUMED" or int(consumed_ledger.get("holdout_open_count") or 0) != 1:
        raise ExternalEvidence8HPromotionError("8h_h_holdout_ledger_must_be_consumed_once")
    if str(consumed_ledger.get("terminal_result_sha256") or "") != holdout_sha:
        raise ExternalEvidence8HPromotionError("8h_h_ledger_terminal_result_binding_mismatch")
    if str(consumed_ledger.get("completion_sha256") or "") != completion_sha:
        raise ExternalEvidence8HPromotionError("8h_h_ledger_completion_binding_mismatch")

    family_hypotheses = [str(x.get("hypothesis_id") or "") for x in family]
    if not family_hypotheses or len(set(family_hypotheses)) != len(family_hypotheses):
        raise ExternalEvidence8HPromotionError("8h_h_invalid_or_duplicate_family_hypothesis")
    states = completion.get("hypothesis_states")
    if not isinstance(states, Mapping) or set(states) != set(family_hypotheses):
        raise ExternalEvidence8HPromotionError("8h_h_completion_states_must_equal_full_family")
    if dict(holdout_result.get("hypothesis_states") or {}) != dict(states):
        raise ExternalEvidence8HPromotionError("8h_h_result_completion_state_drift")
    confirmed = [h for h in family_hypotheses if states[h] == "HOLDOUT_CONFIRMED"]
    if list(completion.get("confirmed_hypotheses") or ()) != confirmed or list(holdout_result.get("confirmed_hypotheses") or ()) != confirmed:
        raise ExternalEvidence8HPromotionError("8h_h_confirmed_hypothesis_rederivation_mismatch")

    evaluations: dict[str, dict[str, Any]] = {}
    for raw in holdout_result.get("holdout_results") or ():
        row = dict(raw)
        hypothesis = str(row.get("hypothesis_id") or "")
        if not hypothesis or hypothesis in evaluations:
            raise ExternalEvidence8HPromotionError("8h_h_duplicate_or_missing_holdout_evaluation")
        _verify_digest(row, "evaluation_sha256", f"8h_h_holdout_evaluation_digest_mismatch:{hypothesis}")
        if hypothesis not in states or str(row.get("terminal_state") or "") != str(states[hypothesis]):
            raise ExternalEvidence8HPromotionError(f"8h_h_holdout_terminal_state_mismatch:{hypothesis}")
        if states[hypothesis] == "HOLDOUT_CONFIRMED":
            if row.get("minimum_evidence_met") is not True:
                raise ExternalEvidence8HPromotionError(f"8h_h_confirmed_without_minimum_evidence:{hypothesis}")
            if row.get("primary_effect") is None or float(row["primary_effect"]) <= 0:
                raise ExternalEvidence8HPromotionError(f"8h_h_confirmed_without_positive_effect:{hypothesis}")
            if row.get("holm_adjusted_p") is None or float(row["holm_adjusted_p"]) >= 0.05:
                raise ExternalEvidence8HPromotionError(f"8h_h_confirmed_without_holm_significance:{hypothesis}")
        evaluations[hypothesis] = row
    if set(evaluations) != set(family_hypotheses):
        raise ExternalEvidence8HPromotionError("8h_h_holdout_evaluations_must_equal_full_family")
    expected_hashes = [str(evaluations[h]["evaluation_sha256"]) for h in family_hypotheses]
    if list(completion.get("holdout_result_sha256s") or ()) != expected_hashes:
        raise ExternalEvidence8HPromotionError("8h_h_completion_evaluation_hash_binding_mismatch")
    completion["consumed_ledger_sha256"] = ledger_sha
    return True, holdout_sha, completion, family, evaluations


def promotion_review_status(
    *, contract: Mapping[str, Any], holdout_result: Mapping[str, Any] | None = None,
    consumed_ledger: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the current fail-closed 8H-H entry state without creating decisions."""
    validate_promotion_contract(contract)
    ready, holdout_sha, completion, family, _ = _validate_holdout_chain(holdout_result, consumed_ledger)
    if not ready:
        result = {
            "schema_version": PROMOTION_COMPLETION_SCHEMA,
            "phase": "8H-H",
            "state": WAITING_FOR_8H_G,
            "holdout_sha256": holdout_sha,
            "holdout_completion_sha256": None,
            "confirmatory_family": family,
            "eligible_hypotheses": [],
            "approved_for_8i_research": [],
            "pending_manual_review": [],
            "dispositions": {},
            "empirically_complete": False,
            "8i_research_handoff_authorized": False,
            "productive_integration_enabled": False,
            "phase7_mutation_authorized": False,
            "phase8i_integration_authorized": False,
            "orders_or_trades_authorized": False,
            "next_phase": "8H-H_WAIT_FOR_8H_G_EMPIRICAL_COMPLETION",
        }
        result["completion_sha256"] = _digest(result)
        return result
    assert completion is not None
    eligible = list(completion.get("confirmed_hypotheses") or ())
    state = NO_PROMOTION_CANDIDATES if not eligible else PROMOTION_REVIEW_INCOMPLETE
    result = {
        "schema_version": PROMOTION_COMPLETION_SCHEMA,
        "phase": "8H-H",
        "state": state,
        "holdout_sha256": holdout_sha,
        "holdout_completion_sha256": str(completion["completion_sha256"]),
        "confirmatory_family": family,
        "eligible_hypotheses": eligible,
        "approved_for_8i_research": [],
        "pending_manual_review": eligible,
        "dispositions": {h: "PENDING_MANUAL_REVIEW" for h in eligible},
        "empirically_complete": not eligible,
        "8i_research_handoff_authorized": False,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "next_phase": "8I_EXTERNAL_EVIDENCE_INTERPRETATION_RESEARCH" if not eligible else "8H-H_MANUAL_REVIEW_REQUIRED",
    }
    result["completion_sha256"] = _digest(result)
    return result


def _validate_freeze_for_packet(freeze_result: Mapping[str, Any], holdout_result: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    if freeze_result.get("schema_version") != INTERACTION_FREEZE_RESULT_SCHEMA or freeze_result.get("state") != SPECS_FROZEN:
        raise ExternalEvidence8HPromotionError("8h_h_frozen_8h_c_specs_required")
    freeze_sha = _verify_digest(freeze_result, "freeze_sha256", "8h_h_freeze_digest_mismatch")
    if str(holdout_result.get("8h_c_freeze_sha256") or "") != freeze_sha:
        raise ExternalEvidence8HPromotionError("8h_h_holdout_freeze_binding_mismatch")
    specs: dict[str, dict[str, Any]] = {}
    for raw in freeze_result.get("frozen_interaction_specs") or ():
        row = dict(raw)
        spec_id = str(row.get("interaction_spec_id") or "")
        if not spec_id or spec_id in specs:
            raise ExternalEvidence8HPromotionError("8h_h_duplicate_or_missing_interaction_spec")
        _verify_digest(row, "interaction_spec_sha256", f"8h_h_interaction_spec_digest_mismatch:{spec_id}")
        if not str(row.get("eligibility_binding_sha256") or ""):
            raise ExternalEvidence8HPromotionError(f"8h_h_missing_upstream_eligibility_binding:{spec_id}")
        specs[spec_id] = row
    return freeze_sha, specs


def build_promotion_review_packet(
    *, contract: Mapping[str, Any], holdout_result: Mapping[str, Any], consumed_ledger: Mapping[str, Any],
    freeze_result: Mapping[str, Any], hypothesis_id: str,
    criteria_evidence: Mapping[str, Mapping[str, Any]], governance_blockers: Sequence[str] = (),
) -> dict[str, Any]:
    """Build the immutable manual-review packet for one HOLDOUT_CONFIRMED hypothesis."""
    validate_promotion_contract(contract)
    ready, holdout_sha, completion, family, evaluations = _validate_holdout_chain(holdout_result, consumed_ledger)
    if not ready or completion is None:
        raise ExternalEvidence8HPromotionError("8h_h_empirically_complete_holdout_required_for_packet")
    states = completion["hypothesis_states"]
    if hypothesis_id not in states:
        raise ExternalEvidence8HPromotionError("8h_h_hypothesis_not_in_confirmatory_family")
    if states[hypothesis_id] != "HOLDOUT_CONFIRMED":
        raise ExternalEvidence8HPromotionError("8h_h_only_holdout_confirmed_is_promotion_eligible")
    family_member = next((x for x in family if str(x.get("hypothesis_id")) == hypothesis_id), None)
    if family_member is None:
        raise ExternalEvidence8HPromotionError("8h_h_family_member_missing")
    freeze_sha, specs = _validate_freeze_for_packet(freeze_result, holdout_result)
    spec_id = str(family_member.get("interaction_spec_id") or "")
    if spec_id not in specs:
        raise ExternalEvidence8HPromotionError("8h_h_frozen_interaction_spec_missing")
    spec = specs[spec_id]
    evaluation = evaluations[hypothesis_id]

    expected_criteria = [str(x["id"]) for x in contract["promotion_criteria"]]
    if set(criteria_evidence) != set(expected_criteria):
        raise ExternalEvidence8HPromotionError("8h_h_criteria_evidence_must_equal_exact_10_criteria")
    criteria_rows: dict[str, Any] = {}
    blockers = [str(x) for x in governance_blockers if str(x)]
    for criterion in contract["promotion_criteria"]:
        cid = str(criterion["id"])
        evidence = criteria_evidence[cid]
        if not isinstance(evidence, Mapping):
            raise ExternalEvidence8HPromotionError(f"8h_h_criterion_evidence_invalid:{cid}")
        status = str(evidence.get("status") or "MISSING").upper()
        evidence_sha = str(evidence.get("evidence_sha256") or "")
        if not evidence_sha:
            evidence_sha = _digest(evidence)
        if status != "PASS":
            blockers.append(f"CRITERION_NOT_PASSED:{cid}:{status}")
        criteria_rows[cid] = {
            "criterion": int(criterion["criterion"]),
            "status": status,
            "evidence_sha256": evidence_sha,
        }

    binding_checks = {
        "UPSTREAM_ELIGIBILITY_INTEGRITY": str(criteria_evidence["UPSTREAM_ELIGIBILITY_INTEGRITY"].get("eligibility_binding_sha256") or "") == str(spec["eligibility_binding_sha256"]),
        "INCREMENTAL_HOLDOUT_VALUE": str(criteria_evidence["INCREMENTAL_HOLDOUT_VALUE"].get("holdout_evaluation_sha256") or "") == str(evaluation["evaluation_sha256"]),
        "MULTIPLE_TESTING_PROTECTED": str(criteria_evidence["MULTIPLE_TESTING_PROTECTED"].get("holdout_evaluation_sha256") or "") == str(evaluation["evaluation_sha256"]) and float(evaluation.get("holm_adjusted_p", 1.0)) < 0.05,
        "SPEC_MODEL_IMMUTABILITY": criteria_evidence["SPEC_MODEL_IMMUTABILITY"].get("model_refit") is False and holdout_result.get("family_shrunk_or_expanded") is False,
        "EVIDENCE_CONSUMPTION_DOCUMENTED": str(criteria_evidence["EVIDENCE_CONSUMPTION_DOCUMENTED"].get("consumed_ledger_sha256") or "") == str(completion["consumed_ledger_sha256"]) and int(consumed_ledger.get("holdout_open_count") or 0) == 1,
    }
    for cid, passed in binding_checks.items():
        if not passed:
            blockers.append(f"CRITERION_BINDING_FAILED:{cid}")

    external_components = [x for x in spec.get("components") or () if isinstance(x, Mapping) and x.get("kind") == "EXTERNAL_NUMERIC"]
    if not external_components or any(not str(x.get("component_identity_sha256") or "") for x in external_components):
        blockers.append("EXTERNAL_COMPONENT_IDENTITY_INCOMPLETE")
    provenance = criteria_evidence["PROVENANCE_AND_SOURCE_IDENTITY_CLEAR"]
    if provenance.get("source_identity_clear") is not True:
        blockers.append("SOURCE_IDENTITY_NOT_CLEAR")
    fresh = criteria_evidence["PIT_AND_FRESH_EVIDENCE_CLEAN"]
    if fresh.get("pit_clean") is not True or fresh.get("fresh_evidence_after_spec_freeze") is not True:
        blockers.append("PIT_OR_FRESH_EVIDENCE_NOT_CLEAN")

    packet = {
        "schema_version": PROMOTION_PACKET_SCHEMA,
        "phase": "8H-H",
        "hypothesis_id": hypothesis_id,
        "interaction_spec_id": spec_id,
        "horizon_sessions": int(family_member.get("horizon_sessions")),
        "8h_g_terminal_state": "HOLDOUT_CONFIRMED",
        "promotion_eligible_from_8h_g": True,
        "8h_c_freeze_sha256": freeze_sha,
        "interaction_spec_sha256": str(spec["interaction_spec_sha256"]),
        "eligibility_binding_sha256": str(spec["eligibility_binding_sha256"]),
        "holdout_sha256": holdout_sha,
        "holdout_completion_sha256": str(completion["completion_sha256"]),
        "holdout_evaluation_sha256": str(evaluation["evaluation_sha256"]),
        "model_pair_sha256": str(evaluation.get("model_pair_sha256") or ""),
        "components": list(spec.get("components") or ()),
        "criteria": criteria_rows,
        "all_10_criteria_pass": all(x["status"] == "PASS" for x in criteria_rows.values()),
        "blockers": sorted(set(blockers)),
        "governance_clear": not blockers,
        "manual_review_required": True,
        "automatic_promotion_authorized": False,
        "production_authorized": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
    }
    packet["packet_sha256"] = _digest(packet)
    return packet


def record_manual_review(
    *, contract: Mapping[str, Any], packet: Mapping[str, Any], decision: str,
    reviewer_identity: str, reviewed_at: str, rationale: str,
) -> dict[str, Any]:
    validate_promotion_contract(contract)
    if packet.get("schema_version") != PROMOTION_PACKET_SCHEMA:
        raise ExternalEvidence8HPromotionError("8h_h_promotion_packet_required")
    packet_sha = _verify_digest(packet, "packet_sha256", "8h_h_promotion_packet_digest_mismatch")
    decision = str(decision or "").upper()
    allowed = set(contract["manual_review"]["allowed_decisions"])
    if decision not in allowed:
        raise ExternalEvidence8HPromotionError("8h_h_manual_decision_invalid")
    reviewer = str(reviewer_identity or "").strip()
    rationale = str(rationale or "").strip()
    if not reviewer:
        raise ExternalEvidence8HPromotionError("8h_h_reviewer_identity_required")
    if not rationale:
        raise ExternalEvidence8HPromotionError("8h_h_review_rationale_required")
    reviewed = _parse_time(reviewed_at)
    if decision == "APPROVE_FOR_8I_RESEARCH_ONLY":
        if packet.get("promotion_eligible_from_8h_g") is not True or packet.get("8h_g_terminal_state") != "HOLDOUT_CONFIRMED":
            raise ExternalEvidence8HPromotionError("8h_h_approval_requires_holdout_confirmation")
        if packet.get("all_10_criteria_pass") is not True:
            raise ExternalEvidence8HPromotionError("8h_h_approval_requires_all_10_criteria")
        if packet.get("governance_clear") is not True or packet.get("blockers"):
            raise ExternalEvidence8HPromotionError("8h_h_approval_requires_no_blockers")
        state = APPROVED_STATE
    elif decision == "DEFER":
        state = "DEFERRED_8I_RESEARCH_PROMOTION"
    else:
        state = "REJECTED_8I_RESEARCH_PROMOTION"
    result = {
        "schema_version": PROMOTION_DECISION_SCHEMA,
        "phase": "8H-H",
        "hypothesis_id": str(packet.get("hypothesis_id")),
        "interaction_spec_id": str(packet.get("interaction_spec_id")),
        "packet_sha256": packet_sha,
        "decision": decision,
        "state": state,
        "reviewer_identity": reviewer,
        "reviewed_at": reviewed.isoformat(),
        "rationale": rationale,
        "authorizes_8i_research_design": state == APPROVED_STATE,
        "authorizes_8i_integration": False,
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_orders_or_trades": False,
        "rejection_authorizes_inverse_signal": False,
    }
    result["decision_sha256"] = _digest(result)
    return result


def finalize_promotion_review(
    *, contract: Mapping[str, Any], holdout_result: Mapping[str, Any], consumed_ledger: Mapping[str, Any],
    packets: Mapping[str, Mapping[str, Any]], decisions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Finalize 8H-H without creating, removing or reinterpreting candidates."""
    validate_promotion_contract(contract)
    ready, holdout_sha, completion, family, _ = _validate_holdout_chain(holdout_result, consumed_ledger)
    if not ready or completion is None:
        if packets or decisions:
            raise ExternalEvidence8HPromotionError("8h_h_review_material_forbidden_before_empirical_completion")
        return promotion_review_status(contract=contract, holdout_result=holdout_result, consumed_ledger=consumed_ledger)
    eligible = list(completion.get("confirmed_hypotheses") or ())
    if set(packets) - set(eligible):
        raise ExternalEvidence8HPromotionError("8h_h_packet_for_noneligible_hypothesis")
    if set(decisions) - set(eligible):
        raise ExternalEvidence8HPromotionError("8h_h_decision_for_noneligible_hypothesis")
    for hypothesis, packet in packets.items():
        if packet.get("schema_version") != PROMOTION_PACKET_SCHEMA or str(packet.get("hypothesis_id")) != hypothesis:
            raise ExternalEvidence8HPromotionError(f"8h_h_invalid_packet:{hypothesis}")
        _verify_digest(packet, "packet_sha256", f"8h_h_packet_digest_mismatch:{hypothesis}")
        if str(packet.get("holdout_completion_sha256") or "") != str(completion["completion_sha256"]):
            raise ExternalEvidence8HPromotionError(f"8h_h_packet_completion_binding_mismatch:{hypothesis}")
    pending = [h for h in eligible if h not in decisions]
    approved: list[str] = []
    dispositions: dict[str, str] = {}
    decision_hashes: dict[str, str] = {}
    for hypothesis in eligible:
        if hypothesis in pending:
            dispositions[hypothesis] = "PENDING_MANUAL_REVIEW"
            continue
        if hypothesis not in packets:
            raise ExternalEvidence8HPromotionError(f"8h_h_decision_missing_packet:{hypothesis}")
        decision = decisions[hypothesis]
        if decision.get("schema_version") != PROMOTION_DECISION_SCHEMA or str(decision.get("hypothesis_id")) != hypothesis:
            raise ExternalEvidence8HPromotionError(f"8h_h_invalid_decision:{hypothesis}")
        decision_sha = _verify_digest(decision, "decision_sha256", f"8h_h_decision_digest_mismatch:{hypothesis}")
        if str(decision.get("packet_sha256") or "") != str(packets[hypothesis].get("packet_sha256") or ""):
            raise ExternalEvidence8HPromotionError(f"8h_h_decision_packet_binding_mismatch:{hypothesis}")
        if any(decision.get(key) is not False for key in ("authorizes_8i_integration", "authorizes_production", "authorizes_phase7_mutation", "authorizes_orders_or_trades", "rejection_authorizes_inverse_signal")):
            raise ExternalEvidence8HPromotionError(f"8h_h_forbidden_decision_authorization:{hypothesis}")
        state = str(decision.get("state") or "")
        dispositions[hypothesis] = state
        decision_hashes[hypothesis] = decision_sha
        if state == APPROVED_STATE:
            if packets[hypothesis].get("all_10_criteria_pass") is not True or packets[hypothesis].get("governance_clear") is not True or packets[hypothesis].get("blockers"):
                raise ExternalEvidence8HPromotionError(f"8h_h_invalid_approved_packet:{hypothesis}")
            if decision.get("authorizes_8i_research_design") is not True:
                raise ExternalEvidence8HPromotionError(f"8h_h_approved_decision_missing_8i_research_scope:{hypothesis}")
            approved.append(hypothesis)
        elif decision.get("authorizes_8i_research_design") is not False:
            raise ExternalEvidence8HPromotionError(f"8h_h_nonapproved_decision_authorizes_8i_research:{hypothesis}")

    if not eligible:
        state = NO_PROMOTION_CANDIDATES
        complete = True
    elif pending:
        state = PROMOTION_REVIEW_INCOMPLETE
        complete = False
    else:
        state = PROMOTION_REVIEW_COMPLETE
        complete = True
    result = {
        "schema_version": PROMOTION_COMPLETION_SCHEMA,
        "phase": "8H-H",
        "state": state,
        "holdout_sha256": holdout_sha,
        "holdout_completion_sha256": str(completion["completion_sha256"]),
        "confirmatory_family": family,
        "eligible_hypotheses": eligible,
        "packet_sha256s": {h: str(packets[h]["packet_sha256"]) for h in eligible if h in packets},
        "decision_sha256s": decision_hashes,
        "approved_for_8i_research": approved,
        "pending_manual_review": pending,
        "dispositions": dispositions,
        "empirically_complete": complete,
        "8h_complete": complete,
        "8i_research_handoff_authorized": bool(approved) and complete,
        "maximum_approval_scope": APPROVED_STATE,
        "productive_integration_enabled": False,
        "phase7_mutation_authorized": False,
        "phase8i_integration_authorized": False,
        "orders_or_trades_authorized": False,
        "rejection_authorizes_inverse_signal": False,
        "next_phase": "8I_EXTERNAL_EVIDENCE_INTERPRETATION_RESEARCH" if complete else "8H-H_MANUAL_REVIEW_REQUIRED",
    }
    result["completion_sha256"] = _digest(result)
    return result
