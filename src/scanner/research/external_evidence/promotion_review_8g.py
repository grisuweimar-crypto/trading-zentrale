"""Phase 8G-H manual promotion review governance.

8G-H does not discover or tune anything. It converts completed 8G-G evidence into
an auditable manual review packet using the ten Phase-8 promotion criteria.
Statistical success alone never promotes a factor. Source provenance, licensing,
Survivorship/as-of-universe evidence and evidence-consumption status are hard gates.
Approval authorizes research in top-level Phase 8H only.
"""
from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Mapping, Sequence

from scanner.research.external_evidence.prospective_8g import (
    PROSPECTIVE_COMPLETION_SCHEMA,
)
from scanner.research.external_evidence.research_8g import ACTIVE_FACTORS, HORIZONS


PROMOTION_PROTOCOL_SCHEMA = "external_evidence_8g_promotion_review_v1"
PROMOTION_PACKET_SCHEMA = "external_evidence_8g_promotion_review_packet_v1"
PROMOTION_DECISION_SCHEMA = "external_evidence_8g_promotion_decision_v1"
PROMOTION_COMPLETION_SCHEMA = "external_evidence_8g_promotion_review_completion_v1"


class ExternalEvidence8GPromotionError(ValueError):
    """Raised when 8G-H promotion governance is violated."""


def _stable_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: object) -> str:
    return sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: object) -> datetime:
    text = str(value or "").strip().replace("Z", "+00:00")
    if not text:
        raise ExternalEvidence8GPromotionError("review_timestamp_required")
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExternalEvidence8GPromotionError("review_timestamp_invalid") from exc


def validate_promotion_protocol(protocol: Mapping[str, Any]) -> None:
    if protocol.get("schema_version") != PROMOTION_PROTOCOL_SCHEMA:
        raise ExternalEvidence8GPromotionError("unsupported_promotion_protocol")
    if protocol.get("phase") != "8G-H":
        raise ExternalEvidence8GPromotionError("promotion_phase_mismatch")
    if protocol.get("research_only") is not True:
        raise ExternalEvidence8GPromotionError("8g_h_must_be_research_only")
    if protocol.get("productive_integration_enabled") is not False:
        raise ExternalEvidence8GPromotionError("production_must_remain_disabled")
    if protocol.get("automatic_promotion_enabled") is not False:
        raise ExternalEvidence8GPromotionError("automatic_promotion_must_remain_disabled")
    if tuple(protocol.get("active_factor_ids") or ()) != ACTIVE_FACTORS:
        raise ExternalEvidence8GPromotionError("active_factor_family_mismatch")
    expected_sources = {
        "rates_policy": ["FED_H15"],
        "yield_curve": ["FED_H15"],
        "fx": ["ECB_EXR"],
    }
    if protocol.get("frozen_source_ids_by_factor") != expected_sources:
        raise ExternalEvidence8GPromotionError("frozen_source_identity_mismatch")
    criteria = list(protocol.get("promotion_criteria") or ())
    if len(criteria) != 10:
        raise ExternalEvidence8GPromotionError("exactly_10_promotion_criteria_required")
    if [int(x.get("plan_criterion", 0)) for x in criteria] != list(range(1, 11)):
        raise ExternalEvidence8GPromotionError("promotion_criteria_order_mismatch")
    if any(x.get("required") is not True for x in criteria):
        raise ExternalEvidence8GPromotionError("all_promotion_criteria_must_be_required")
    if protocol["completion_gate"].get("next_phase") != "8H_CROSS_FACTOR_INTERACTION":
        raise ExternalEvidence8GPromotionError("roadmap_after_8g_h_must_be_8h")
    guards = protocol.get("guards")
    if not isinstance(guards, Mapping) or any(value is not False for value in guards.values()):
        raise ExternalEvidence8GPromotionError("8g_h_guards_must_remain_false")


def _source_registry_index(source_registry: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    if source_registry.get("schema_version") != "external_source_registry_v1":
        raise ExternalEvidence8GPromotionError("unsupported_source_registry")
    rows = source_registry.get("sources")
    if not isinstance(rows, Sequence):
        raise ExternalEvidence8GPromotionError("source_registry_sources_missing")
    index: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        source_id = str(row.get("source_id") or "")
        if source_id:
            if source_id in index:
                raise ExternalEvidence8GPromotionError(f"duplicate_source_registry_id:{source_id}")
            index[source_id] = row
    return index


def source_governance_review(
    *,
    factor_id: str,
    source_registry: Mapping[str, Any],
    collection_receipt: Mapping[str, Any],
    completion_gate_8f: Mapping[str, Any],
    protocol: Mapping[str, Any],
    explicit_source_clearances: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Review frozen sources without silently treating an unknown license as cleared."""
    validate_promotion_protocol(protocol)
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GPromotionError("unknown_factor_for_source_review")
    if completion_gate_8f.get("schema_version") != "external_evidence_8f_completion_gate_v1":
        raise ExternalEvidence8GPromotionError("8f_completion_gate_required")
    if completion_gate_8f.get("freeze_allowed") is not True or completion_gate_8f.get("status") != "PASS_8F_COMPLETION":
        raise ExternalEvidence8GPromotionError("8f_completion_gate_not_passed")
    if collection_receipt.get("schema_version") != "external_evidence_8f_prospective_collection_v1":
        raise ExternalEvidence8GPromotionError("8f_collection_receipt_required")
    guards = collection_receipt.get("guards") or {}
    if guards.get("raw_payloads_persisted_before_parsing") is not True:
        raise ExternalEvidence8GPromotionError("raw_payload_provenance_required")
    if guards.get("historical_current_values_retrojected") is not False:
        raise ExternalEvidence8GPromotionError("retrojected_external_values_forbidden")

    registry = _source_registry_index(source_registry)
    clearances = explicit_source_clearances or {}
    required = list(protocol["frozen_source_ids_by_factor"][factor_id])
    accepted_pit = set(protocol["source_governance"]["accepted_pit_status"])
    accepted_license = set(protocol["source_governance"]["accepted_license_status"])
    source_states: dict[str, Any] = {}
    blockers: list[str] = []
    for source_id in required:
        row = registry.get(source_id)
        clearance = clearances.get(source_id) or {}
        if row is None:
            if clearance.get("registry_resolution") is not True:
                source_states[source_id] = {"status": "UNRESOLVED_IN_REGISTRY"}
                blockers.append(f"SOURCE_NOT_RESOLVED_IN_REGISTRY:{source_id}")
                continue
            pit_status = str(clearance.get("pit_status") or "UNKNOWN")
            license_status = str(clearance.get("license_status") or "UNKNOWN")
            provenance_status = str(clearance.get("provenance_status") or "UNKNOWN")
        else:
            pit_status = str(row.get("pit_status") or "UNKNOWN")
            license_status = str(row.get("license_status") or "UNKNOWN")
            provenance_status = "DOCUMENTED" if row.get("documentation_url") else "UNKNOWN"
            if clearance.get("license_status"):
                license_status = str(clearance["license_status"])
            if clearance.get("provenance_status"):
                provenance_status = str(clearance["provenance_status"])

        pit_ok = pit_status in accepted_pit
        if pit_status == "PARTIAL" and protocol["source_governance"].get("partial_requires_explicit_review_justification"):
            pit_ok = pit_ok and bool(clearance.get("partial_pit_justification"))
        license_ok = license_status in accepted_license
        provenance_ok = provenance_status in {"DOCUMENTED", "CLEARED"}
        if not pit_ok:
            blockers.append(f"SOURCE_PIT_NOT_CLEARED:{source_id}:{pit_status}")
        if not license_ok:
            blockers.append(f"SOURCE_LICENSE_NOT_CLEARED:{source_id}:{license_status}")
        if not provenance_ok:
            blockers.append(f"SOURCE_PROVENANCE_NOT_CLEARED:{source_id}:{provenance_status}")
        source_states[source_id] = {
            "status": "CLEAR" if pit_ok and license_ok and provenance_ok else "BLOCKED",
            "pit_status": pit_status,
            "license_status": license_status,
            "provenance_status": provenance_status,
        }
    return {
        "factor_id": factor_id,
        "required_source_ids": required,
        "source_states": source_states,
        "blockers": blockers,
        "all_sources_clear": not blockers,
        "8f_completion_gate_pass": True,
        "raw_snapshot_provenance_pass": True,
    }


def build_promotion_review_packet(
    *,
    hypothesis_id: str,
    prospective_completion: Mapping[str, Any],
    criteria_evidence: Mapping[str, Mapping[str, Any]],
    source_governance: Mapping[str, Any],
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Build an auditable review packet; it never self-approves."""
    validate_promotion_protocol(protocol)
    if prospective_completion.get("schema_version") != PROSPECTIVE_COMPLETION_SCHEMA:
        raise ExternalEvidence8GPromotionError("8g_g_completion_required")
    if prospective_completion.get("empirically_complete") is not True:
        raise ExternalEvidence8GPromotionError("8g_g_must_be_empirically_complete")
    states = prospective_completion.get("hypothesis_states")
    if not isinstance(states, Mapping) or hypothesis_id not in states:
        raise ExternalEvidence8GPromotionError("8g_g_hypothesis_state_missing")
    factor_id = hypothesis_id.split("_x_", 1)[0]
    if factor_id not in ACTIVE_FACTORS:
        raise ExternalEvidence8GPromotionError("promotion_hypothesis_factor_unknown")
    try:
        horizon = int(hypothesis_id.rsplit("_x_", 1)[1].removesuffix("t"))
    except (IndexError, ValueError) as exc:
        raise ExternalEvidence8GPromotionError("promotion_hypothesis_id_invalid") from exc
    if horizon not in HORIZONS:
        raise ExternalEvidence8GPromotionError("promotion_horizon_unknown")

    eligible = states[hypothesis_id] == "PROSPECTIVE_CONFIRMED"
    criteria_rows: dict[str, Any] = {}
    blockers: list[str] = []
    for criterion in protocol["promotion_criteria"]:
        criterion_id = str(criterion["id"])
        evidence = criteria_evidence.get(criterion_id)
        if not isinstance(evidence, Mapping):
            status = "MISSING"
            evidence_sha = None
        else:
            status = str(evidence.get("status") or "MISSING").upper()
            evidence_sha = str(evidence.get("evidence_sha256") or "") or _digest(evidence)
        if status != "PASS":
            blockers.append(f"CRITERION_NOT_PASSED:{criterion_id}:{status}")
        criteria_rows[criterion_id] = {
            "plan_criterion": int(criterion["plan_criterion"]),
            "status": status,
            "evidence_sha256": evidence_sha,
        }

    source_blockers = list(source_governance.get("blockers") or ())
    if source_governance.get("factor_id") != factor_id:
        source_blockers.append("SOURCE_GOVERNANCE_FACTOR_MISMATCH")
    if source_governance.get("all_sources_clear") is not True:
        blockers.extend(source_blockers or ["SOURCE_GOVERNANCE_INCOMPLETE"])
    if not eligible:
        blockers.append(f"NOT_PROSPECTIVE_CONFIRMED:{states[hypothesis_id]}")

    packet: dict[str, Any] = {
        "schema_version": PROMOTION_PACKET_SCHEMA,
        "phase": "8G-H",
        "hypothesis_id": hypothesis_id,
        "factor_id": factor_id,
        "horizon_sessions": horizon,
        "8g_g_state": states[hypothesis_id],
        "promotion_eligible_from_8g_g": eligible,
        "criteria": criteria_rows,
        "source_governance": dict(source_governance),
        "blockers": sorted(set(blockers)),
        "all_10_criteria_pass": all(x["status"] == "PASS" for x in criteria_rows.values()),
        "governance_clear": not blockers,
        "manual_review_required": True,
        "automatic_promotion_authorized": False,
        "production_authorized": False,
        "prospective_completion_sha256": str(prospective_completion.get("completion_sha256") or _digest(prospective_completion)),
    }
    packet["packet_sha256"] = _digest(packet)
    return packet


def record_manual_review(
    *,
    packet: Mapping[str, Any],
    decision: str,
    reviewer_identity: str,
    reviewed_at: str,
    rationale: str,
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Record a human disposition. Approval is research-only and hard-gated."""
    validate_promotion_protocol(protocol)
    if packet.get("schema_version") != PROMOTION_PACKET_SCHEMA:
        raise ExternalEvidence8GPromotionError("promotion_review_packet_required")
    allowed = set(protocol["manual_review"]["allowed_decisions"])
    decision = str(decision).upper()
    if decision not in allowed:
        raise ExternalEvidence8GPromotionError("promotion_review_decision_invalid")
    reviewer_identity = str(reviewer_identity or "").strip()
    rationale = str(rationale or "").strip()
    if not reviewer_identity:
        raise ExternalEvidence8GPromotionError("reviewer_identity_required")
    if not rationale:
        raise ExternalEvidence8GPromotionError("review_rationale_required")
    parsed_time = _parse_time(reviewed_at)
    if decision == "APPROVE_FOR_8H_RESEARCH_ONLY":
        if packet.get("promotion_eligible_from_8g_g") is not True:
            raise ExternalEvidence8GPromotionError("approval_requires_prospective_confirmation")
        if packet.get("all_10_criteria_pass") is not True:
            raise ExternalEvidence8GPromotionError("approval_requires_all_10_criteria")
        if packet.get("governance_clear") is not True or packet.get("blockers"):
            raise ExternalEvidence8GPromotionError("approval_requires_no_governance_blockers")
        state = "APPROVED_FOR_8H_RESEARCH_ONLY"
    elif decision == "DEFER":
        state = "DEFERRED_PROMOTION_REVIEW"
    else:
        state = "REJECTED_PROMOTION_REVIEW"
    result: dict[str, Any] = {
        "schema_version": PROMOTION_DECISION_SCHEMA,
        "phase": "8G-H",
        "hypothesis_id": str(packet.get("hypothesis_id")),
        "packet_sha256": str(packet.get("packet_sha256")),
        "decision": decision,
        "state": state,
        "reviewer_identity": reviewer_identity,
        "reviewed_at": parsed_time.isoformat(),
        "rationale": rationale,
        "authorizes_8h_research": state == "APPROVED_FOR_8H_RESEARCH_ONLY",
        "authorizes_production": False,
        "authorizes_phase7_mutation": False,
        "authorizes_8i_integration": False,
        "authorizes_orders_or_trades": False,
        "rejection_authorizes_inverse_signal": False,
    }
    result["decision_sha256"] = _digest(result)
    return result


def finalize_promotion_review(
    *,
    prospective_completion: Mapping[str, Any],
    decisions: Mapping[str, Mapping[str, Any]],
    protocol: Mapping[str, Any],
) -> dict[str, Any]:
    """Finish 8G-H when every prospective-confirmed hypothesis has a disposition."""
    validate_promotion_protocol(protocol)
    if prospective_completion.get("schema_version") != PROSPECTIVE_COMPLETION_SCHEMA:
        raise ExternalEvidence8GPromotionError("8g_g_completion_required")
    if prospective_completion.get("empirically_complete") is not True:
        raise ExternalEvidence8GPromotionError("8g_g_must_be_empirically_complete")
    states = prospective_completion.get("hypothesis_states") or {}
    eligible = [h for h, state in states.items() if state == "PROSPECTIVE_CONFIRMED"]
    unexpected = sorted(set(decisions).difference(eligible))
    if unexpected:
        raise ExternalEvidence8GPromotionError("decision_for_noneligible_hypothesis:" + ",".join(unexpected))
    pending = [h for h in eligible if h not in decisions]
    approved: list[str] = []
    dispositions: dict[str, str] = {}
    for hypothesis in eligible:
        if hypothesis in pending:
            dispositions[hypothesis] = "PENDING_MANUAL_REVIEW"
            continue
        row = decisions[hypothesis]
        if row.get("schema_version") != PROMOTION_DECISION_SCHEMA:
            raise ExternalEvidence8GPromotionError(f"invalid_promotion_decision:{hypothesis}")
        if str(row.get("hypothesis_id")) != hypothesis:
            raise ExternalEvidence8GPromotionError(f"promotion_decision_identity_mismatch:{hypothesis}")
        state = str(row.get("state"))
        dispositions[hypothesis] = state
        if state == "APPROVED_FOR_8H_RESEARCH_ONLY":
            approved.append(hypothesis)
    complete = not pending
    if not eligible:
        overall = "NO_PROMOTION_CANDIDATES"
    elif pending:
        overall = "PROMOTION_REVIEW_INCOMPLETE"
    else:
        overall = "PROMOTION_REVIEW_COMPLETE"
    completion: dict[str, Any] = {
        "schema_version": PROMOTION_COMPLETION_SCHEMA,
        "phase": "8G-H",
        "state": overall,
        "empirically_complete": complete,
        "eligible_hypotheses": eligible,
        "pending_hypotheses": pending,
        "approved_for_8h_research": approved,
        "dispositions": dispositions,
        "automatic_promotion_authorized": False,
        "production_external_evidence_enabled": False,
        "next_phase": "8H_CROSS_FACTOR_INTERACTION",
    }
    completion["completion_sha256"] = _digest(completion)
    return completion
