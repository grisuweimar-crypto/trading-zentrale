"""Phase-7G research-only Reliability & Explainability.

7G explains already-computed Decision-Layer outputs. It consumes the validated
7A evidence packet plus preserved 7D, 7E and 7F outputs from the same snapshot.
It must never recompute a stance, resolve a conflict, change a portfolio action,
create a weighted score or generate an order.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from hashlib import sha256
import json
from typing import Mapping, Sequence

from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.portfolio_action import (
    ACTION_STATES,
    validate_portfolio_action,
)
from scanner.research.decision_layer.state_transition import validate_state_transition
from scanner.research.decision_layer.universal_stance import validate_universal_stance


SCHEMA_VERSION = "decision_reliability_explainability_v1"
RELIABILITY_STATES = frozenset({
    "blocked_conflict",
    "blocked_insufficient",
    "pending_confirmation",
    "provisional_cross_family_support",
    "provisional_same_family_support",
    "provisional_unopposed_support",
})
DIRECTIONAL_FAMILIES = frozenset({"selection", "timing"})
ANNOTATION_FAMILIES = frozenset({"probability", "confidence"})
CONTEXT_FAMILIES = frozenset({"risk", "elliott"})
PENDING_TRANSITIONS = frozenset({"bootstrap_pending", "transition_pending"})
COST_SENSITIVE_ACTIONS = frozenset({"ENTER_REVIEW", "ADD_REVIEW", "REDUCE_REVIEW", "EXIT_REVIEW"})
FORBIDDEN_OUTPUT_KEYS = frozenset({
    "new_portfolio_action",
    "recommended_action",
    "trade_decision",
    "order_instruction",
    "order_quantity",
    "position_size",
    "target_weight",
    "limit_price",
    "stop_price",
    "buy_signal",
    "sell_signal",
})


class ReliabilityExplainabilityError(ValueError):
    """Raised when Phase-7G source consistency or output guards fail."""


def _forbidden_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if str(key) in FORBIDDEN_OUTPUT_KEYS:
                found.append(child)
            found.extend(_forbidden_paths(item, child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{index}]"))
    return found


def _direction(row: Mapping[str, object]) -> str | None:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        return None
    value = str(payload.get("direction") or "").strip().lower()
    return value if value in {"positive", "negative"} else None


def _safe_scalar_facts(payload: object) -> dict[str, object]:
    """Expose compact source-reported scalar facts without interpreting them."""
    if not isinstance(payload, Mapping):
        return {}
    facts: dict[str, object] = {}
    for key in sorted(map(str, payload.keys())):
        if len(facts) >= 20:
            break
        value = payload.get(key)
        if isinstance(value, (bool, int, float)) or value is None:
            facts[key] = value
        elif isinstance(value, str) and len(value) <= 160:
            facts[key] = value
    return facts


def _evidence_summary(row: Mapping[str, object], role: str) -> dict[str, object]:
    payload = row.get("payload")
    return {
        "claim_id": str(row.get("claim_id") or ""),
        "family": str(row.get("family") or ""),
        "claim_ref": str(row.get("claim_ref") or "") or None,
        "direction": _direction(row),
        "source_version": str(row.get("source_version") or ""),
        "coverage_state": row.get("coverage_state"),
        "maturity_state": row.get("maturity_state"),
        "pit_state": row.get("pit_state"),
        "integration_mode": row.get("integration_mode"),
        "role": role,
        "directional_vote": str(row.get("family")) in DIRECTIONAL_FAMILIES,
        "reported_scalar_facts": _safe_scalar_facts(payload),
    }


def _source_consistency(
    packet: Mapping[str, object],
    stance: Mapping[str, object],
    transition: Mapping[str, object],
    action: Mapping[str, object],
) -> None:
    keys = ("symbol", "as_of", "source_snapshot_id")
    for key in keys:
        values = [str(source.get(key) or "") for source in (packet, stance, transition, action)]
        if len(set(values)) != 1:
            raise ReliabilityExplainabilityError(f"source_{key}_mismatch")

    packet_coverage = packet.get("coverage")
    stance_coverage = stance.get("coverage")
    if packet_coverage != stance_coverage:
        raise ReliabilityExplainabilityError("stance_coverage_does_not_match_packet")

    universal = stance.get("universal_stance")
    raw = transition.get("raw_stance")
    action_stance = action.get("universal_stance_context")
    if not all(isinstance(item, Mapping) for item in (universal, raw, action_stance)):
        raise ReliabilityExplainabilityError("stance_context_missing")
    assert isinstance(universal, Mapping) and isinstance(raw, Mapping) and isinstance(action_stance, Mapping)
    if raw.get("state") != universal.get("state") or raw.get("direction") != universal.get("direction"):
        raise ReliabilityExplainabilityError("transition_raw_stance_mismatch")
    if action_stance.get("raw_state") != universal.get("state") or action_stance.get("raw_direction") != universal.get("direction"):
        raise ReliabilityExplainabilityError("action_stance_context_mismatch")

    transition_state = transition.get("transition_state")
    action_transition = action.get("transition_context")
    if not isinstance(transition_state, Mapping) or not isinstance(action_transition, Mapping):
        raise ReliabilityExplainabilityError("transition_context_missing")
    checks = (
        ("status", "status"),
        ("stable_directional_anchor", "stable_directional_anchor"),
        ("pending_direction", "pending_direction"),
        ("stable_anchor_is_current_stance", "stable_anchor_is_current_stance"),
    )
    for source_key, action_key in checks:
        if transition_state.get(source_key) != action_transition.get(action_key):
            raise ReliabilityExplainabilityError(f"action_transition_context_mismatch:{source_key}")


def _claim_index(packet: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    rows = packet.get("evidence", [])
    assert isinstance(rows, list)
    index: dict[str, Mapping[str, object]] = {}
    for row in rows:
        assert isinstance(row, Mapping)
        index[str(row.get("claim_id") or "")] = row
    return index


def _explanation_groups(
    packet: Mapping[str, object], stance: Mapping[str, object]
) -> dict[str, object]:
    index = _claim_index(packet)
    structure = stance.get("evidence_structure")
    universal = stance.get("universal_stance")
    if not isinstance(structure, Mapping) or not isinstance(universal, Mapping):
        raise ReliabilityExplainabilityError("stance_evidence_structure_missing")

    known_ids = [str(value) for value in structure.get("known_directional_claim_ids", [])]
    unknown_ids = {str(value) for value in structure.get("unknown_direction_claim_ids", [])}
    ineligible_rows = structure.get("ineligible_directional_claims", [])
    ineligible_reason: dict[str, str] = {}
    if isinstance(ineligible_rows, list):
        for item in ineligible_rows:
            if isinstance(item, Mapping):
                ineligible_reason[str(item.get("claim_id") or "")] = str(item.get("eligibility_reason") or "ineligible")

    missing_claim_ids = [claim_id for claim_id in known_ids if claim_id not in index]
    if missing_claim_ids:
        raise ReliabilityExplainabilityError("stance_claim_missing_from_packet:" + ",".join(missing_claim_ids))

    selected_direction = universal.get("direction")
    supporting: list[dict[str, object]] = []
    counter: list[dict[str, object]] = []
    positive: list[dict[str, object]] = []
    negative: list[dict[str, object]] = []
    unknown: list[dict[str, object]] = []
    ineligible: list[dict[str, object]] = []
    annotations: list[dict[str, object]] = []
    context: list[dict[str, object]] = []

    rows = packet.get("evidence", [])
    assert isinstance(rows, list)
    for row in rows:
        assert isinstance(row, Mapping)
        claim_id = str(row.get("claim_id") or "")
        family = str(row.get("family") or "")
        if family in DIRECTIONAL_FAMILIES:
            if claim_id in known_ids:
                summary = _evidence_summary(row, "eligible_directional")
                direction = summary["direction"]
                if direction == "positive":
                    positive.append(summary)
                elif direction == "negative":
                    negative.append(summary)
                else:
                    raise ReliabilityExplainabilityError("known_directional_claim_without_direction")
                if selected_direction is not None:
                    if direction == selected_direction:
                        supporting.append(summary)
                    else:
                        counter.append(summary)
            elif claim_id in unknown_ids:
                unknown.append(_evidence_summary(row, "eligible_direction_unknown"))
            else:
                summary = _evidence_summary(row, "ineligible_directional")
                summary["eligibility_reason"] = ineligible_reason.get(claim_id, "not_in_7d_known_directional_set")
                ineligible.append(summary)
        elif family in ANNOTATION_FAMILIES:
            summary = _evidence_summary(row, "annotation_not_vote")
            summary["directional_vote"] = False
            annotations.append(summary)
        elif family in CONTEXT_FAMILIES:
            summary = _evidence_summary(row, "context_not_vote")
            summary["directional_vote"] = False
            context.append(summary)

    if selected_direction is not None and counter:
        raise ReliabilityExplainabilityError("directional_stance_cannot_hide_counter_evidence")

    conflicts = structure.get("conflicts", [])
    support_relations = structure.get("support_relations", [])
    return {
        "selected_direction": selected_direction,
        "supporting_evidence": supporting,
        "counter_evidence": counter,
        "positive_directional_evidence": positive,
        "negative_directional_evidence": negative,
        "unknown_direction_evidence": unknown,
        "ineligible_directional_evidence": ineligible,
        "annotations": annotations,
        "non_directional_context": context,
        "support_relations": deepcopy(list(support_relations)) if isinstance(support_relations, list) else [],
        "unresolved_conflicts": deepcopy(list(conflicts)) if isinstance(conflicts, list) else [],
    }


def _coverage_gaps(packet: Mapping[str, object], groups: Mapping[str, object]) -> list[dict[str, object]]:
    coverage = packet.get("coverage")
    if not isinstance(coverage, Mapping):
        raise ReliabilityExplainabilityError("coverage_required")
    families = coverage.get("families")
    if not isinstance(families, Mapping):
        raise ReliabilityExplainabilityError("coverage_families_required")

    gaps: list[dict[str, object]] = []
    for family, raw in sorted(families.items(), key=lambda item: str(item[0])):
        if not isinstance(raw, Mapping):
            continue
        family_name = str(family)
        if raw.get("present") is not True:
            gaps.append({"scope": "family", "family": family_name, "issue": "family_missing"})
            continue
        for state in raw.get("coverage_states", []):
            if state == "limited":
                gaps.append({"scope": "family", "family": family_name, "issue": "coverage_limited", "state": state})
            elif state in {"insufficient", "unavailable", "invalid"}:
                gaps.append({"scope": "family", "family": family_name, "issue": "coverage_problem", "state": state})
        for state in raw.get("maturity_states", []):
            if state in {"directional_but_immature", "not_yet_mature", "insufficient_evidence", "unavailable"}:
                gaps.append({"scope": "family", "family": family_name, "issue": "maturity_limited", "state": state})
        for mode in raw.get("integration_modes", []):
            if mode in {"research_only", "shadow_only", "eligible_after_promotion_review"}:
                gaps.append({"scope": "family", "family": family_name, "issue": "non_production_integration", "state": mode})

    for row in groups.get("unknown_direction_evidence", []):
        if isinstance(row, Mapping):
            gaps.append({
                "scope": "claim",
                "family": row.get("family"),
                "claim_id": row.get("claim_id"),
                "issue": "eligible_direction_unknown",
            })
    for row in groups.get("ineligible_directional_evidence", []):
        if isinstance(row, Mapping):
            gaps.append({
                "scope": "claim",
                "family": row.get("family"),
                "claim_id": row.get("claim_id"),
                "issue": "directional_claim_ineligible",
                "state": row.get("eligibility_reason"),
            })

    unique: list[dict[str, object]] = []
    seen: set[str] = set()
    for gap in gaps:
        key = json.dumps(gap, sort_keys=True, ensure_ascii=True)
        if key not in seen:
            seen.add(key)
            unique.append(gap)
    return unique


def _reliability_state(stance: Mapping[str, object], transition: Mapping[str, object]) -> str:
    universal = stance.get("universal_stance")
    structure = stance.get("evidence_structure")
    state = transition.get("transition_state")
    if not isinstance(universal, Mapping) or not isinstance(structure, Mapping) or not isinstance(state, Mapping):
        raise ReliabilityExplainabilityError("reliability_source_context_missing")
    stance_state = str(universal.get("state") or "")
    transition_status = str(state.get("status") or "")
    if stance_state == "conflicted":
        return "blocked_conflict"
    if stance_state == "insufficient_evidence":
        return "blocked_insufficient"
    if transition_status in PENDING_TRANSITIONS:
        return "pending_confirmation"
    support = str(structure.get("support_structure") or "")
    mapping = {
        "cross_family_confirmation": "provisional_cross_family_support",
        "same_family_correlated_support": "provisional_same_family_support",
        "single_direction_or_unopposed": "provisional_unopposed_support",
    }
    if support not in mapping:
        raise ReliabilityExplainabilityError("unsupported_support_structure_for_directional_stance")
    return mapping[support]


def _profile(rows: Sequence[Mapping[str, object]], field: str) -> dict[str, int]:
    counts = Counter(str(row.get(field) or "missing") for row in rows)
    return dict(sorted(counts.items()))


def _decision_change_triggers(
    stance: Mapping[str, object],
    transition: Mapping[str, object],
    action: Mapping[str, object],
) -> list[dict[str, object]]:
    universal = stance["universal_stance"]
    state = transition["transition_state"]
    action_row = action["portfolio_action"]
    position = action["position_context"]
    swing = action["swing_management"]
    assert all(isinstance(item, Mapping) for item in (universal, state, action_row, position, swing))
    stance_state = str(universal.get("state"))
    direction = universal.get("direction")
    transition_status = str(state.get("status"))
    action_state = str(action_row.get("state"))
    position_state = str(position.get("position_state"))
    contexts = set(map(str, swing.get("review_contexts", [])))
    triggers: list[dict[str, object]] = []

    def add(trigger_id: str, source_phase: str, condition: str, effect: str, **extra: object) -> None:
        row: dict[str, object] = {
            "trigger_id": trigger_id,
            "source_phase": source_phase,
            "condition": condition,
            "effect": effect,
            "changes_decision_directly": False,
            "requires_recompute_from_source": True,
        }
        row.update(extra)
        triggers.append(row)

    if transition_status in PENDING_TRANSITIONS:
        pending = state.get("pending_direction")
        required = state.get("required_confirmation_count")
        current = state.get("pending_confirmation_count")
        add(
            "pending_direction_reaches_confirmation_depth",
            "7E",
            f"pending direction {pending} reaches required confirmation depth {required}",
            "7E may confirm the transition; 7F must then be recomputed",
            current_confirmation_count=current,
            required_confirmation_count=required,
        )

    if stance_state == "conflicted":
        add(
            "future_conflict_resolves_directionally",
            "7D",
            "future admissible directional evidence no longer contains unresolved positive/negative conflict",
            "7D may become directional and downstream 7E/7F must be recomputed",
        )
    elif stance_state == "insufficient_evidence":
        add(
            "future_eligible_directional_evidence_appears",
            "7D",
            "future packet contains at least one eligible directional claim with explicit direction",
            "7D may become directional and downstream 7E/7F must be recomputed",
        )
    elif direction in {"positive", "negative"}:
        opposite = "negative" if direction == "positive" else "positive"
        add(
            "opposite_direction_becomes_confirmed",
            "7D/7E",
            f"future 7D stance becomes {opposite} and 7E confirms that direction",
            "7F action must be recomputed for the confirmed opposite direction",
        )

    if direction == "positive" and position_state == "long":
        add_context = bool(contexts & {"entry_or_add_review", "reentry_or_add_review"})
        reduce_context = bool(contexts & {"partial_reduce_review", "profit_protection_review", "larger_reduce_or_exit_review"})
        capacity = position.get("add_capacity_state")
        if action_state == "HOLD" and not reduce_context:
            add(
                "swing_reduce_context_appears",
                "6H/7F",
                "a valid Elliott review context for partial reduction or profit protection appears while positive stance remains confirmed",
                "7F may move from HOLD to REDUCE_REVIEW",
            )
        if action_state == "HOLD" and not add_context and capacity == "available":
            add(
                "swing_add_context_appears_with_capacity",
                "6H/7F",
                "a valid Elliott entry/reentry review context appears while explicit add capacity remains available",
                "7F may move from HOLD to ADD_REVIEW",
            )
        if action_state == "ADD_REVIEW":
            add(
                "add_context_or_capacity_clears",
                "6H/7F",
                "the add review context disappears or explicit add capacity becomes unavailable",
                "7F must be recomputed and may return to HOLD",
            )
        if action_state == "REDUCE_REVIEW":
            add(
                "reduce_context_clears",
                "6H/7F",
                "the reduction/profit-protection review context disappears while positive stance remains confirmed",
                "7F must be recomputed and may return to HOLD",
            )

    if action_state == "EXIT_REVIEW" and position_state == "long":
        add(
            "positive_direction_reconfirmed_after_exit_review",
            "7D/7E",
            "future 7D stance becomes positive and 7E confirms that direction",
            "7F must be recomputed; EXIT_REVIEW may no longer apply",
        )

    return triggers


def _information_completion_triggers(
    packet: Mapping[str, object], action: Mapping[str, object], gaps: Sequence[Mapping[str, object]]
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    action_row = action["portfolio_action"]
    position = action["position_context"]
    pnl = action["pnl_context"]
    cost = action["cost_context"]
    assert all(isinstance(item, Mapping) for item in (action_row, position, pnl, cost))

    if str(action_row.get("state")) in COST_SENSITIVE_ACTIONS and cost.get("cost_model_present") is not True:
        result.append({
            "trigger_id": "transaction_cost_model_supplied",
            "condition": "transaction_cost_bps becomes available from a valid position snapshot",
            "effect": "net-benefit analysis may become possible; directional stance/action is not changed by this fact alone",
            "changes_decision_directly": False,
        })
    if pnl.get("complete") is not True and position.get("position_state") == "long":
        result.append({
            "trigger_id": "pnl_context_completed",
            "condition": "average entry price and current price become available in original currency",
            "effect": "P/L explanation becomes complete; stance/action direction remains unchanged",
            "changes_decision_directly": False,
        })
    if position.get("add_capacity_state") == "unknown" and position.get("position_state") == "long":
        result.append({
            "trigger_id": "add_capacity_clarified",
            "condition": "explicit add capacity is supplied by the portfolio context",
            "effect": "future add-review eligibility can be evaluated without inventing capital",
            "changes_decision_directly": False,
        })
    if gaps:
        missing_families = sorted({str(row.get("family")) for row in gaps if row.get("issue") == "family_missing"})
        if missing_families:
            result.append({
                "trigger_id": "missing_evidence_family_becomes_available",
                "condition": "one or more currently missing evidence families become PIT-valid and admissible",
                "families": missing_families,
                "effect": "coverage/explanation may improve; upstream phases decide whether stance changes",
                "changes_decision_directly": False,
            })
    return result


def build_reliability_explanation(
    packet: Mapping[str, object],
    stance: Mapping[str, object],
    transition: Mapping[str, object],
    action: Mapping[str, object],
) -> dict[str, object]:
    """Build a faithful explanation of an existing 7F decision state."""
    packet_valid = validate_input_packet(packet)
    stance_valid = validate_universal_stance(stance)
    transition_valid = validate_state_transition(transition)
    action_valid = validate_portfolio_action(action)
    _source_consistency(packet_valid, stance_valid, transition_valid, action_valid)

    groups = _explanation_groups(packet_valid, stance_valid)
    gaps = _coverage_gaps(packet_valid, groups)
    assessment = _reliability_state(stance_valid, transition_valid)

    universal = stance_valid["universal_stance"]
    structure = stance_valid["evidence_structure"]
    transition_state = transition_valid["transition_state"]
    action_row = action_valid["portfolio_action"]
    position = action_valid["position_context"]
    swing = action_valid["swing_management"]
    assert all(isinstance(item, Mapping) for item in (universal, structure, transition_state, action_row, position, swing))

    directional_rows = [
        row for row in groups["positive_directional_evidence"] + groups["negative_directional_evidence"]
        if isinstance(row, Mapping)
    ]
    validation_action = action_valid.get("validation", {})
    validation_transition = transition_valid.get("validation", {})
    validation_stance = stance_valid.get("validation", {})

    output: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7G",
        "symbol": packet_valid["symbol"],
        "as_of": packet_valid["as_of"],
        "source_snapshot_id": packet_valid["source_snapshot_id"],
        "decision_context": {
            "universal_stance_state": universal.get("state"),
            "universal_stance_direction": universal.get("direction"),
            "support_structure": structure.get("support_structure"),
            "transition_status": transition_state.get("status"),
            "stable_directional_anchor": transition_state.get("stable_directional_anchor"),
            "pending_direction": transition_state.get("pending_direction"),
            "portfolio_action_state": action_row.get("state"),
            "portfolio_action_reason_code": action_row.get("reason_code"),
            "position_state": position.get("position_state"),
            "swing_mode": swing.get("mode"),
            "preserved": True,
        },
        "explanation": {
            "decision_path": [
                f"7D:{universal.get('state')}:{structure.get('support_structure')}",
                f"7E:{transition_state.get('status')}",
                f"7F:{action_row.get('state')}:{action_row.get('reason_code')}",
            ],
            "directional_evidence": {
                "selected_direction": groups["selected_direction"],
                "supporting": groups["supporting_evidence"],
                "counter": groups["counter_evidence"],
                "positive": groups["positive_directional_evidence"],
                "negative": groups["negative_directional_evidence"],
                "unknown_direction": groups["unknown_direction_evidence"],
                "ineligible": groups["ineligible_directional_evidence"],
                "support_relations": groups["support_relations"],
                "unresolved_conflicts": groups["unresolved_conflicts"],
            },
            "annotations": groups["annotations"],
            "non_directional_context": groups["non_directional_context"],
            "missing_or_limited_evidence": gaps,
            "source_claim_count": len(packet_valid.get("evidence", [])),
        },
        "reliability": {
            "assessment": assessment,
            "numeric_reliability_score": None,
            "weighted_score_used": False,
            "assessment_is_empirical_success_probability": False,
            "coverage_admission_state": packet_valid.get("coverage", {}).get("admission_state"),
            "coverage_gaps_present": bool(gaps),
            "eligible_directional_claim_count": len(directional_rows),
            "directional_pit_profile": _profile(directional_rows, "pit_state"),
            "directional_maturity_profile": _profile(directional_rows, "maturity_state"),
            "directional_coverage_profile": _profile(directional_rows, "coverage_state"),
            "validation_chain": {
                "stance_validation_status": validation_stance.get("status") if isinstance(validation_stance, Mapping) else None,
                "transition_validation_status": validation_transition.get("status") if isinstance(validation_transition, Mapping) else None,
                "hysteresis_rule_empirically_validated": validation_transition.get("hysteresis_rule_empirically_validated") if isinstance(validation_transition, Mapping) else None,
                "portfolio_action_rule_empirically_validated": validation_action.get("portfolio_action_rule_empirically_validated") if isinstance(validation_action, Mapping) else None,
                "swing_action_edge_empirically_validated": validation_action.get("swing_action_edge_empirically_validated") if isinstance(validation_action, Mapping) else None,
                "future_mature_outcomes_required": True,
            },
        },
        "change_triggers": {
            "decision_change_triggers": _decision_change_triggers(stance_valid, transition_valid, action_valid),
            "information_completion_triggers": _information_completion_triggers(packet_valid, action_valid, gaps),
        },
        "semantics": {
            "stance_recomputed": False,
            "conflict_resolved": False,
            "transition_recomputed": False,
            "portfolio_action_changed": False,
            "new_portfolio_action_generated": False,
            "position_sizing_computed": False,
            "target_weight_computed": False,
            "weighted_super_score_used": False,
            "probability_or_confidence_used_as_vote": False,
            "risk_or_elliott_used_as_directional_vote": False,
            "missing_evidence_treated_as_neutral": False,
            "broker_order_generated": False,
        },
        "validation": {
            "research_only": True,
            "source_consistency_verified": True,
            "explanation_is_descriptive_not_predictive": True,
            "reliability_model_empirically_validated": False,
            "future_mature_outcomes_required": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "promotion_eligible": False,
        },
    }
    canonical = json.dumps(output, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    output["explanation_id"] = sha256(canonical.encode("utf-8")).hexdigest()
    validate_reliability_explanation(output)
    return output


def validate_reliability_explanation(value: Mapping[str, object]) -> dict[str, object]:
    """Fail closed if a 7G explanation alters or overstates its source decision."""
    if value.get("schema_version") != SCHEMA_VERSION:
        raise ReliabilityExplainabilityError("unsupported_reliability_explainability_schema")
    decision = value.get("decision_context")
    if not isinstance(decision, Mapping) or decision.get("preserved") is not True:
        raise ReliabilityExplainabilityError("preserved_decision_context_required")
    if decision.get("portfolio_action_state") not in ACTION_STATES:
        raise ReliabilityExplainabilityError("invalid_preserved_portfolio_action_state")

    reliability = value.get("reliability")
    if not isinstance(reliability, Mapping):
        raise ReliabilityExplainabilityError("reliability_required")
    if reliability.get("assessment") not in RELIABILITY_STATES:
        raise ReliabilityExplainabilityError("invalid_reliability_assessment")
    if reliability.get("numeric_reliability_score") is not None:
        raise ReliabilityExplainabilityError("numeric_reliability_score_forbidden_v1")
    if reliability.get("weighted_score_used") is not False:
        raise ReliabilityExplainabilityError("weighted_reliability_score_forbidden")
    if reliability.get("assessment_is_empirical_success_probability") is not False:
        raise ReliabilityExplainabilityError("reliability_must_not_claim_success_probability")

    explanation = value.get("explanation")
    if not isinstance(explanation, Mapping):
        raise ReliabilityExplainabilityError("explanation_required")
    directional = explanation.get("directional_evidence")
    if not isinstance(directional, Mapping):
        raise ReliabilityExplainabilityError("directional_explanation_required")
    selected = directional.get("selected_direction")
    if selected != decision.get("universal_stance_direction"):
        raise ReliabilityExplainabilityError("selected_direction_must_match_preserved_stance")

    triggers = value.get("change_triggers")
    if not isinstance(triggers, Mapping):
        raise ReliabilityExplainabilityError("change_triggers_required")
    for row in triggers.get("decision_change_triggers", []):
        if not isinstance(row, Mapping):
            raise ReliabilityExplainabilityError("invalid_decision_change_trigger")
        if row.get("changes_decision_directly") is not False or row.get("requires_recompute_from_source") is not True:
            raise ReliabilityExplainabilityError("trigger_must_require_upstream_recompute")
    for row in triggers.get("information_completion_triggers", []):
        if not isinstance(row, Mapping) or row.get("changes_decision_directly") is not False:
            raise ReliabilityExplainabilityError("information_trigger_cannot_change_decision_directly")

    semantics = value.get("semantics")
    if not isinstance(semantics, Mapping):
        raise ReliabilityExplainabilityError("semantics_required")
    required_false = (
        "stance_recomputed",
        "conflict_resolved",
        "transition_recomputed",
        "portfolio_action_changed",
        "new_portfolio_action_generated",
        "position_sizing_computed",
        "target_weight_computed",
        "weighted_super_score_used",
        "probability_or_confidence_used_as_vote",
        "risk_or_elliott_used_as_directional_vote",
        "missing_evidence_treated_as_neutral",
        "broker_order_generated",
    )
    if any(semantics.get(key) is not False for key in required_false):
        raise ReliabilityExplainabilityError("explainability_semantic_guard_violation")

    forbidden = _forbidden_paths(value)
    if forbidden:
        raise ReliabilityExplainabilityError("forbidden_decision_or_execution_fields:" + ",".join(forbidden))

    validation = value.get("validation")
    if not isinstance(validation, Mapping):
        raise ReliabilityExplainabilityError("validation_required")
    if validation.get("research_only") is not True or validation.get("source_consistency_verified") is not True:
        raise ReliabilityExplainabilityError("research_or_source_consistency_guard_missing")
    if validation.get("explanation_is_descriptive_not_predictive") is not True:
        raise ReliabilityExplainabilityError("explanation_must_remain_descriptive")
    if validation.get("reliability_model_empirically_validated") is not False:
        raise ReliabilityExplainabilityError("reliability_model_validation_must_remain_false")
    if validation.get("productive_integration_enabled") is not False or validation.get("execution_allowed") is not False:
        raise ReliabilityExplainabilityError("productive_execution_must_remain_disabled")
    if validation.get("promotion_eligible") is not False:
        raise ReliabilityExplainabilityError("promotion_must_remain_closed")

    explanation_id = str(value.get("explanation_id") or "")
    if not explanation_id:
        raise ReliabilityExplainabilityError("explanation_id_required")
    unsigned = deepcopy(dict(value))
    unsigned.pop("explanation_id", None)
    canonical = json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    expected = sha256(canonical.encode("utf-8")).hexdigest()
    if explanation_id != expected:
        raise ReliabilityExplainabilityError("explanation_id_integrity_failure")
    return dict(value)
