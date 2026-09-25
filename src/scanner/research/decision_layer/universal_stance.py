"""Phase-7D portfolio-independent Universal Stance.

The stance is a semantic interpretation of validated Phase-7A directional claims
and the Phase-7C relation topology. It is deliberately not a weighted score,
portfolio action, hysteresis engine or order generator.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Mapping

from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.relation_graph import packet_relation_graph


SCHEMA_VERSION = "decision_universal_stance_v1"
STANCE_STATES = frozenset({"positive", "negative", "conflicted", "insufficient_evidence"})
PROSPECTIVE_CONFIRMATION_START = "2026-09-26"
FORBIDDEN_OUTPUT_KEYS = frozenset({
    "portfolio_action", "trade_decision", "order_instruction", "buy_signal",
    "sell_signal", "position_size", "target_weight", "previous_stance",
    "hysteresis_state",
})


class UniversalStanceError(ValueError):
    """Raised when a 7D stance object is structurally inconsistent."""


def _calendar_date(value: object) -> date:
    """Return the calendar date encoded by the packet timestamp.

    The Phase-7 spent/unspent freeze is defined by observation calendar date,
    not by a UTC instant. Preserve the timestamp's own offset when deciding
    whether an observation belongs to 2026-09-25 or 2026-09-26.
    """
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise UniversalStanceError("invalid_as_of") from exc
    return parsed.date()


def _forbidden_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if str(key) in FORBIDDEN_OUTPUT_KEYS:
                found.append(child)
            found.extend(_forbidden_paths(item, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{index}]"))
    return found


def _research_partition(as_of: object) -> str:
    cutoff = date(2026, 9, 26)
    return (
        "prospective_unspent"
        if _calendar_date(as_of) >= cutoff
        else "legacy_replay_spent"
    )


def _support_structure(relation_state: str) -> str:
    return {
        "cross_family_confirmation_present": "cross_family_confirmation",
        "correlated_same_family_support": "same_family_correlated_support",
        "single_direction_or_unopposed": "single_direction_or_unopposed",
        "conflict_present": "unresolved_conflict",
        "insufficient_directional_relation": "insufficient_directional_relation",
    }.get(relation_state, "invalid_relation_state")


def compute_universal_stance(packet: Mapping[str, object]) -> dict[str, object]:
    """Compute the research-only, portfolio-independent Phase-7D stance."""
    validated = validate_input_packet(packet)
    graph = packet_relation_graph(validated)
    relation_state = str(graph.get("relation_state") or "")

    known = [
        row for row in graph.get("directional_claims", [])
        if isinstance(row, Mapping)
        and row.get("eligible_for_relation_topology") is True
        and row.get("direction") in {"positive", "negative"}
    ]
    directions = {str(row["direction"]) for row in known}

    if relation_state == "conflict_present":
        state = "conflicted"
        direction = None
    elif relation_state == "insufficient_directional_relation":
        state = "insufficient_evidence"
        direction = None
    elif directions == {"positive"}:
        state = "positive"
        direction = "positive"
    elif directions == {"negative"}:
        state = "negative"
        direction = "negative"
    else:
        raise UniversalStanceError("relation_graph_direction_inconsistent")

    partition = _research_partition(validated["as_of"])
    family_counts: dict[str, int] = {}
    for row in known:
        family = str(row.get("family") or "")
        family_counts[family] = family_counts.get(family, 0) + 1

    output = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7D",
        "symbol": validated["symbol"],
        "as_of": validated["as_of"],
        "source_snapshot_id": validated["source_snapshot_id"],
        "research_partition": partition,
        "universal_stance": {
            "state": state,
            "direction": direction,
            "portfolio_independent": True,
            "research_only": True,
        },
        "evidence_structure": {
            "relation_state": relation_state,
            "support_structure": _support_structure(relation_state),
            "known_directional_claim_ids": [str(row["claim_id"]) for row in known],
            "known_directional_family_counts": family_counts,
            "unknown_direction_claim_ids": list(graph.get("unknown_direction_claim_ids", [])),
            "ineligible_directional_claims": list(graph.get("ineligible_directional_claims", [])),
            "support_relations": list(graph.get("support_relations", [])),
            "conflicts": list(graph.get("conflicts", [])),
            "annotation_count": len(graph.get("annotations", [])),
            "context_count": len(graph.get("context", [])),
        },
        "coverage": validated.get("coverage"),
        "semantics": {
            "neutral_inferred_from_missing_evidence": False,
            "conflict_treated_as_neutral": False,
            "probability_and_confidence_count_as_votes": False,
            "risk_and_elliott_count_as_votes": False,
            "same_family_timing_patterns_are_independent_votes": False,
            "weighted_super_score_used": False,
            "conflict_resolved": False,
            "hysteresis_applied": False,
            "portfolio_action_computed": False,
            "order_instruction_computed": False,
        },
        "validation": {
            "status": (
                "prospective_unconfirmed"
                if partition == "prospective_unspent"
                else "spent_replay_only"
            ),
            "prospective_confirmation_start": PROSPECTIVE_CONFIRMATION_START,
            "prospective_confirmation_required": True,
            "historical_7c_discovery_is_validation": False,
            "productive_integration_enabled": False,
            "promotion_eligible": False,
        },
    }
    validate_universal_stance(output)
    return output


def validate_universal_stance(value: Mapping[str, object]) -> dict[str, object]:
    """Validate a Phase-7D output and fail closed on semantic contradictions."""
    if value.get("schema_version") != SCHEMA_VERSION:
        raise UniversalStanceError("unsupported_stance_schema")
    stance = value.get("universal_stance")
    if not isinstance(stance, Mapping):
        raise UniversalStanceError("universal_stance_required")
    state = str(stance.get("state") or "")
    direction = stance.get("direction")
    if state not in STANCE_STATES:
        raise UniversalStanceError("invalid_stance_state")
    if state == "positive" and direction != "positive":
        raise UniversalStanceError("positive_stance_direction_mismatch")
    if state == "negative" and direction != "negative":
        raise UniversalStanceError("negative_stance_direction_mismatch")
    if state in {"conflicted", "insufficient_evidence"} and direction is not None:
        raise UniversalStanceError("nondirectional_stance_must_not_have_direction")
    if stance.get("portfolio_independent") is not True or stance.get("research_only") is not True:
        raise UniversalStanceError("stance_guard_missing")
    forbidden = _forbidden_paths(value)
    if forbidden:
        raise UniversalStanceError("forbidden_portfolio_or_action_fields:" + ",".join(forbidden))
    semantics = value.get("semantics")
    if not isinstance(semantics, Mapping):
        raise UniversalStanceError("semantics_required")
    required_false = (
        "neutral_inferred_from_missing_evidence",
        "conflict_treated_as_neutral",
        "probability_and_confidence_count_as_votes",
        "risk_and_elliott_count_as_votes",
        "same_family_timing_patterns_are_independent_votes",
        "weighted_super_score_used",
        "conflict_resolved",
        "hysteresis_applied",
        "portfolio_action_computed",
        "order_instruction_computed",
    )
    if any(semantics.get(key) is not False for key in required_false):
        raise UniversalStanceError("stance_semantic_guard_violation")
    validation = value.get("validation")
    if not isinstance(validation, Mapping):
        raise UniversalStanceError("validation_required")
    if validation.get("productive_integration_enabled") is not False:
        raise UniversalStanceError("productive_integration_must_remain_disabled")
    if validation.get("promotion_eligible") is not False:
        raise UniversalStanceError("promotion_must_remain_closed")
    return dict(value)
