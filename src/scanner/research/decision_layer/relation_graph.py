"""Prospective Phase-7C evidence relation topology.

This module describes how already validated Phase-7A claims relate to each
other. It does not resolve conflicts and does not compute a stance or action.
"""
from __future__ import annotations

from typing import Mapping

from scanner.research.decision_layer.input_contract import (
    DIRECTIONAL_FAMILIES,
    validate_input_packet,
)


RELATION_GRAPH_SCHEMA_VERSION = "decision_relation_graph_v1"


def _explicit_direction(row: Mapping[str, object]) -> str | None:
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        return None
    value = str(payload.get("direction") or "").strip().lower()
    return value if value in {"positive", "negative"} else None


def _directional_eligibility(row: Mapping[str, object]) -> tuple[bool, str]:
    coverage = str(row.get("coverage_state") or "")
    maturity = str(row.get("maturity_state") or "")
    pit = str(row.get("pit_state") or "")
    if coverage not in {"available", "limited"}:
        return False, f"coverage:{coverage or 'missing'}"
    if pit not in {"verified", "partial"}:
        return False, f"pit:{pit or 'missing'}"
    if maturity not in {"robust", "directional_but_immature", "not_applicable"}:
        return False, f"maturity:{maturity or 'missing'}"
    return True, "eligible"


def packet_relation_graph(packet: Mapping[str, object]) -> dict[str, object]:
    """Return confirmation/conflict topology for one validated 7A packet.

    Probability/Confidence remain annotations. Risk/Elliott remain context.
    Same-family Timing patterns may corroborate a direction but are explicitly
    not treated as independent votes.
    """
    validated = validate_input_packet(packet)
    rows = validated.get("evidence", [])
    assert isinstance(rows, list)

    directional: list[dict[str, object]] = []
    eligible_directional: list[dict[str, object]] = []
    ineligible_directional: list[dict[str, object]] = []
    annotations: list[dict[str, object]] = []
    context: list[dict[str, object]] = []
    unknown_direction: list[str] = []

    for row in rows:
        assert isinstance(row, Mapping)
        family = str(row["family"])
        claim_id = str(row["claim_id"])
        if family in DIRECTIONAL_FAMILIES:
            direction = _explicit_direction(row)
            eligible, reason = _directional_eligibility(row)
            item = {
                "claim_id": claim_id,
                "family": family,
                "direction": direction,
                "eligible_for_relation_topology": eligible,
                "eligibility_reason": reason,
            }
            directional.append(item)
            if eligible:
                eligible_directional.append(item)
                if direction is None:
                    unknown_direction.append(claim_id)
            else:
                ineligible_directional.append(item)
        elif family in {"probability", "confidence"}:
            annotations.append({
                "claim_id": claim_id,
                "family": family,
                "claim_ref": str(row.get("claim_ref") or ""),
                "coverage_state": row.get("coverage_state"),
                "maturity_state": row.get("maturity_state"),
                "pit_state": row.get("pit_state"),
            })
        elif family in {"risk", "elliott"}:
            context.append({
                "claim_id": claim_id,
                "family": family,
                "coverage_state": row.get("coverage_state"),
                "maturity_state": row.get("maturity_state"),
                "pit_state": row.get("pit_state"),
            })

    known = [
        row for row in eligible_directional
        if row["direction"] in {"positive", "negative"}
    ]
    positives = [row for row in known if row["direction"] == "positive"]
    negatives = [row for row in known if row["direction"] == "negative"]

    support_relations: list[dict[str, object]] = []
    for direction, group in (("positive", positives), ("negative", negatives)):
        if len(group) < 2:
            continue
        families = sorted({str(item["family"]) for item in group})
        cross_family = len(families) > 1
        support_relations.append({
            "direction": direction,
            "claim_ids": [str(item["claim_id"]) for item in group],
            "families": families,
            "relation_type": (
                "cross_family_confirmation"
                if cross_family else "same_family_correlated_support"
            ),
            "independent_votes": False,
        })

    conflicts: list[dict[str, object]] = []
    if positives and negatives:
        pos_families = {str(item["family"]) for item in positives}
        neg_families = {str(item["family"]) for item in negatives}
        conflicts.append({
            "positive_claim_ids": [str(item["claim_id"]) for item in positives],
            "negative_claim_ids": [str(item["claim_id"]) for item in negatives],
            "relation_type": (
                "cross_family_conflict"
                if pos_families != neg_families or len(pos_families | neg_families) > 1
                else "within_family_conflict"
            ),
            "resolved": False,
        })

    has_cross_family_confirmation = any(
        item["relation_type"] == "cross_family_confirmation"
        for item in support_relations
    )
    has_correlated_same_family = any(
        item["relation_type"] == "same_family_correlated_support"
        for item in support_relations
    )
    if conflicts:
        relation_state = "conflict_present"
    elif has_cross_family_confirmation:
        relation_state = "cross_family_confirmation_present"
    elif has_correlated_same_family:
        relation_state = "correlated_same_family_support"
    elif known:
        relation_state = "single_direction_or_unopposed"
    else:
        relation_state = "insufficient_directional_relation"

    return {
        "schema_version": RELATION_GRAPH_SCHEMA_VERSION,
        "symbol": validated["symbol"],
        "as_of": validated["as_of"],
        "source_snapshot_id": validated["source_snapshot_id"],
        "relation_state": relation_state,
        "directional_claims": directional,
        "eligible_directional_claim_ids": [
            str(item["claim_id"]) for item in eligible_directional
        ],
        "ineligible_directional_claims": ineligible_directional,
        "unknown_direction_claim_ids": unknown_direction,
        "support_relations": support_relations,
        "conflicts": conflicts,
        "annotations": annotations,
        "context": context,
        "probability_and_confidence_count_as_votes": False,
        "risk_and_elliott_count_as_votes": False,
        "same_family_timing_patterns_are_independent_votes": False,
        "missing_direction_is_neutral": False,
        "conflict_resolved": False,
        "universal_stance_computed": False,
        "portfolio_action_computed": False,
    }
