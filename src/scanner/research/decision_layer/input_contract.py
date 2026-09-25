"""Phase 7A typed evidence admission for the future Decision Layer.

This module deliberately stops before evidence fusion.  Its job is to preserve
semantic boundaries, point-in-time validity and explicit coverage/maturity so a
later phase cannot accidentally turn missing or research-only evidence into an
action.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping, Sequence


SCHEMA_VERSION = "decision_layer_input_contract_v1"
ALLOWED_FAMILIES = frozenset({
    "selection", "timing", "probability", "risk", "confidence", "elliott"
})
DIRECTIONAL_FAMILIES = frozenset({"selection", "timing"})
ADMISSION_STATES = frozenset({
    "admissible",
    "admissible_with_gaps",
    "insufficient_directional_evidence",
    "rejected",
})
COVERAGE_STATES = frozenset({"available", "limited", "insufficient", "unavailable", "invalid"})
MATURITY_STATES = frozenset({
    "robust",
    "directional_but_immature",
    "not_yet_mature",
    "insufficient_evidence",
    "unavailable",
    "not_applicable",
})
PIT_STATES = frozenset({"verified", "partial", "unverified", "invalid"})
INTEGRATION_MODES = frozenset({
    "production_existing",
    "research_only",
    "shadow_only",
    "eligible_after_promotion_review",
})
COMMON_EVIDENCE_FIELDS = (
    "family",
    "claim_id",
    "as_of",
    "available_from",
    "source_version",
    "coverage_state",
    "maturity_state",
    "pit_state",
    "integration_mode",
    "payload",
)
FORBIDDEN_KEYS = frozenset({
    "universal_stance",
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "buy_signal",
    "sell_signal",
    "position_size",
    "target_weight",
})
NON_DIRECTIONAL_FORBIDDEN_PAYLOAD_KEYS = frozenset({"direction", "stance", "vote"})


class DecisionInputError(ValueError):
    """Raised when a Phase-7A packet violates the frozen admission boundary."""


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise DecisionInputError(f"{field}_required")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DecisionInputError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _forbidden_paths(value: object, forbidden: frozenset[str], path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            child = f"{path}.{key_text}"
            if key_text in forbidden:
                found.append(child)
            found.extend(_forbidden_paths(item, forbidden, child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, forbidden, f"{path}[{index}]"))
    return found


def _required(mapping: Mapping[str, object], fields: Sequence[str], prefix: str) -> None:
    missing = [field for field in fields if field not in mapping]
    if missing:
        raise DecisionInputError(f"{prefix}_missing_required_fields:" + ",".join(missing))


def _validate_common(evidence: Mapping[str, object], packet_as_of: datetime) -> None:
    _required(evidence, COMMON_EVIDENCE_FIELDS, "evidence")
    family = str(evidence.get("family"))
    if family not in ALLOWED_FAMILIES:
        raise DecisionInputError(f"unsupported_evidence_family:{family}")
    if not str(evidence.get("claim_id") or "").strip():
        raise DecisionInputError("claim_id_required")
    if not str(evidence.get("source_version") or "").strip():
        raise DecisionInputError("source_version_required")
    if evidence.get("coverage_state") not in COVERAGE_STATES:
        raise DecisionInputError("invalid_coverage_state")
    if evidence.get("maturity_state") not in MATURITY_STATES:
        raise DecisionInputError("invalid_maturity_state")
    if evidence.get("pit_state") not in PIT_STATES:
        raise DecisionInputError("invalid_pit_state")
    if evidence.get("integration_mode") not in INTEGRATION_MODES:
        raise DecisionInputError("invalid_integration_mode")
    if not isinstance(evidence.get("payload"), Mapping):
        raise DecisionInputError("payload_must_be_object")

    evidence_as_of = _timestamp(evidence.get("as_of"), "evidence_as_of")
    available_from = _timestamp(evidence.get("available_from"), "available_from")
    if evidence_as_of > packet_as_of:
        raise DecisionInputError("future_evidence_as_of")
    if available_from > packet_as_of:
        raise DecisionInputError("future_evidence_available_from")


def _validate_timing(evidence: Mapping[str, object]) -> None:
    payload = evidence["payload"]
    assert isinstance(payload, Mapping)
    _required(
        payload,
        ("pattern_id", "horizon_sessions", "pattern_frozen", "match_from_pit_features"),
        "timing_payload",
    )
    if not str(payload.get("pattern_id") or "").strip():
        raise DecisionInputError("timing_pattern_id_required")
    try:
        horizon = int(payload.get("horizon_sessions"))
    except (TypeError, ValueError) as exc:
        raise DecisionInputError("invalid_timing_horizon") from exc
    if horizon <= 0:
        raise DecisionInputError("invalid_timing_horizon")
    if payload.get("pattern_frozen") is not True:
        raise DecisionInputError("timing_pattern_must_be_frozen")
    if payload.get("match_from_pit_features") is not True:
        raise DecisionInputError("timing_match_must_be_pit")


def _validate_probability(
    evidence: Mapping[str, object],
    claims: Mapping[str, Mapping[str, object]],
) -> None:
    claim_ref = str(evidence.get("claim_ref") or "").strip()
    if not claim_ref:
        raise DecisionInputError("probability_claim_ref_required")
    referenced = claims.get(claim_ref)
    if referenced is None:
        raise DecisionInputError("probability_claim_ref_unresolved")
    if referenced.get("family") not in DIRECTIONAL_FAMILIES:
        raise DecisionInputError("probability_must_reference_selection_or_timing")
    payload = evidence["payload"]
    assert isinstance(payload, Mapping)
    _required(payload, ("horizon_sessions",), "probability_payload")
    if _forbidden_paths(payload, NON_DIRECTIONAL_FORBIDDEN_PAYLOAD_KEYS):
        raise DecisionInputError("probability_must_not_be_directional_vote")


def _validate_risk(evidence: Mapping[str, object]) -> None:
    payload = evidence["payload"]
    assert isinstance(payload, Mapping)
    if _forbidden_paths(payload, NON_DIRECTIONAL_FORBIDDEN_PAYLOAD_KEYS):
        raise DecisionInputError("risk_must_not_be_directional_vote")


def _validate_confidence(
    evidence: Mapping[str, object],
    claims: Mapping[str, Mapping[str, object]],
) -> None:
    claim_ref = str(evidence.get("claim_ref") or "").strip()
    if not claim_ref:
        raise DecisionInputError("confidence_claim_ref_required")
    referenced = claims.get(claim_ref)
    if referenced is None or referenced.get("family") == "confidence":
        raise DecisionInputError("confidence_claim_ref_unresolved")
    payload = evidence["payload"]
    assert isinstance(payload, Mapping)
    forbidden = NON_DIRECTIONAL_FORBIDDEN_PAYLOAD_KEYS | frozenset({"attractiveness"})
    if _forbidden_paths(payload, forbidden):
        raise DecisionInputError("confidence_must_not_encode_attractiveness_or_direction")


def _validate_elliott(evidence: Mapping[str, object]) -> None:
    payload = evidence["payload"]
    assert isinstance(payload, Mapping)
    if evidence.get("integration_mode") != "research_only":
        raise DecisionInputError("elliott_6h_must_remain_research_only")
    if payload.get("research_only") is not True:
        raise DecisionInputError("elliott_6h_research_only_guard_missing")
    integration = payload.get("integration")
    if not isinstance(integration, Mapping):
        raise DecisionInputError("elliott_integration_guard_missing")
    if integration.get("decision_layer_required") is not True:
        raise DecisionInputError("elliott_decision_layer_requirement_missing")
    if integration.get("productive_integration_enabled") is not False:
        raise DecisionInputError("elliott_productive_integration_must_remain_disabled")
    if integration.get("direct_ordering_allowed") is not False:
        raise DecisionInputError("elliott_direct_ordering_must_remain_disabled")
    if payload.get("routing_is_trade_decision") is not False:
        raise DecisionInputError("elliott_routing_must_remain_review_only")
    if payload.get("structural_fit") is not None or payload.get("confirmation_strength") is not None:
        raise DecisionInputError("uncalibrated_elliott_fields_must_remain_missing")


def validate_input_packet(packet: Mapping[str, object]) -> dict[str, object]:
    """Validate and return a defensive copy of one Phase-7A evidence packet.

    Hard contract violations raise ``DecisionInputError``.  Missing or immature
    evidence is not a hard error; it is preserved for the coverage summary and
    later Decision-Layer research.
    """
    _required(packet, ("schema_version", "symbol", "as_of", "source_snapshot_id", "evidence"), "packet")
    if packet.get("schema_version") != SCHEMA_VERSION:
        raise DecisionInputError("unsupported_packet_schema")
    if not str(packet.get("symbol") or "").strip():
        raise DecisionInputError("symbol_required")
    if not str(packet.get("source_snapshot_id") or "").strip():
        raise DecisionInputError("source_snapshot_id_required")
    packet_as_of = _timestamp(packet.get("as_of"), "packet_as_of")
    evidence_rows = packet.get("evidence")
    if not isinstance(evidence_rows, list):
        raise DecisionInputError("evidence_must_be_list")

    forbidden = _forbidden_paths(packet, FORBIDDEN_KEYS)
    if forbidden:
        raise DecisionInputError("forbidden_decision_or_portfolio_fields:" + ",".join(forbidden))

    claims: dict[str, Mapping[str, object]] = {}
    for row in evidence_rows:
        if not isinstance(row, Mapping):
            raise DecisionInputError("evidence_row_must_be_object")
        _validate_common(row, packet_as_of)
        claim_id = str(row.get("claim_id"))
        if claim_id in claims:
            raise DecisionInputError(f"duplicate_claim_id:{claim_id}")
        claims[claim_id] = row

    for row in evidence_rows:
        family = str(row.get("family"))
        if family == "timing":
            _validate_timing(row)
        elif family == "probability":
            _validate_probability(row, claims)
        elif family == "risk":
            _validate_risk(row)
        elif family == "confidence":
            _validate_confidence(row, claims)
        elif family == "elliott":
            _validate_elliott(row)

    result = deepcopy(dict(packet))
    result["coverage"] = summarize_coverage(result)
    return result


def summarize_coverage(packet: Mapping[str, object]) -> dict[str, object]:
    """Summarize family coverage without creating a stance or action."""
    rows = packet.get("evidence", [])
    if not isinstance(rows, list):
        raise DecisionInputError("evidence_must_be_list")
    by_family: dict[str, dict[str, object]] = {}
    for family in sorted(ALLOWED_FAMILIES):
        family_rows = [row for row in rows if isinstance(row, Mapping) and row.get("family") == family]
        by_family[family] = {
            "present": bool(family_rows),
            "claim_count": len(family_rows),
            "coverage_states": sorted({str(row.get("coverage_state")) for row in family_rows}),
            "maturity_states": sorted({str(row.get("maturity_state")) for row in family_rows}),
            "integration_modes": sorted({str(row.get("integration_mode")) for row in family_rows}),
        }

    directional_rows = [
        row for row in rows
        if isinstance(row, Mapping)
        and row.get("family") in DIRECTIONAL_FAMILIES
        and row.get("coverage_state") in {"available", "limited"}
        and row.get("pit_state") in {"verified", "partial"}
    ]
    if not directional_rows:
        admission = "insufficient_directional_evidence"
    else:
        gap = any(
            not by_family[family]["present"]
            or any(state in {"insufficient", "unavailable", "invalid"} for state in by_family[family]["coverage_states"])
            or any(state in {"not_yet_mature", "insufficient_evidence", "unavailable"} for state in by_family[family]["maturity_states"])
            for family in ALLOWED_FAMILIES
        )
        admission = "admissible_with_gaps" if gap else "admissible"

    return {
        "admission_state": admission,
        "directional_claim_count": len(directional_rows),
        "families": by_family,
        "missing_is_neutral": False,
        "stance_computed": False,
        "portfolio_action_computed": False,
    }


def build_input_packet(
    *,
    symbol: str,
    as_of: str,
    source_snapshot_id: str,
    evidence: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Build and validate a canonical Phase-7A research packet."""
    packet = {
        "schema_version": SCHEMA_VERSION,
        "symbol": str(symbol),
        "as_of": str(as_of),
        "source_snapshot_id": str(source_snapshot_id),
        "evidence": [deepcopy(dict(row)) for row in evidence],
        "research_only": True,
        "productive_integration_enabled": False,
    }
    return validate_input_packet(packet)
