"""W7 state/history context for the Decision Layer.

W7 does not create a trading rule.  It takes the already-existing scanner path
memory plus the published daily-research blocks and transports that evidence to
Phase 7F as typed, point-in-time context.  Portfolio Action remains unchanged;
W8 owns any later action-policy review that consumes this context.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping

from .input_contract import validate_input_packet
from .integrated_evidence import PATH_CONTEXT_TYPE
from .portfolio_action import validate_portfolio_action


SCHEMA_VERSION = "decision_state_history_context_v1"
CANONICAL_STATES = frozenset({
    "never_overextended",
    "overextended",
    "overextension_with_momentum_loss",
    "post_overextension_correction",
})


class StateHistoryContextError(ValueError):
    """Raised when W7 context cannot be transported without inference."""


def _utc(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise StateHistoryContextError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise StateHistoryContextError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _path_claim(packet: Mapping[str, object]) -> dict[str, object] | None:
    matches: list[dict[str, object]] = []
    evidence = packet.get("evidence")
    if not isinstance(evidence, list):
        return None
    for row in evidence:
        if not isinstance(row, Mapping) or row.get("family") != "risk":
            continue
        payload = row.get("payload")
        if isinstance(payload, Mapping) and payload.get("context_type") == PATH_CONTEXT_TYPE:
            matches.append(deepcopy(dict(row)))
    if len(matches) > 1:
        raise StateHistoryContextError(
            f"duplicate_scanner_path_state:{packet.get('symbol')}"
        )
    return matches[0] if matches else None


def _canonical_state(payload: Mapping[str, object]) -> tuple[str, list[str]]:
    active = payload.get("overextension_active") is True
    last_date = str(payload.get("last_overextension_date") or "").strip() or None
    deterioration = payload.get("deterioration")
    deterioration = deterioration if isinstance(deterioration, Mapping) else {}

    rs3m_falling = deterioration.get("rs3m_falling_5t") is True
    secondary_deterioration = any(
        deterioration.get(key) is True
        for key in (
            "score_falling_5t",
            "rank_worsening_5t",
            "trend200_falling_5t",
            "r_code_downgrade",
        )
    )

    if active and rs3m_falling and secondary_deterioration:
        return (
            "overextension_with_momentum_loss",
            ["overextended", "overextension_with_momentum_loss"],
        )
    if active:
        return "overextended", ["overextended"]
    if last_date is not None:
        return (
            "post_overextension_correction",
            ["overextended", "post_overextension_correction"],
        )
    return "never_overextended", ["never_overextended"]


def build_state_history_context(
    packet: Mapping[str, object],
    daily_symbol: Mapping[str, object],
) -> dict[str, object] | None:
    """Build W7 context from existing PIT evidence without changing semantics."""
    validated = validate_input_packet(packet)
    claim = _path_claim(validated)
    if claim is None:
        return None

    payload = claim.get("payload")
    if not isinstance(payload, Mapping):
        raise StateHistoryContextError("scanner_path_payload_required")
    if payload.get("review_is_trade_decision") is not False:
        raise StateHistoryContextError("scanner_path_review_must_not_be_trade_decision")
    if payload.get("execution_allowed") is not False:
        raise StateHistoryContextError("scanner_path_execution_must_remain_disabled")

    current = daily_symbol.get("current")
    dynamics = daily_symbol.get("dynamics")
    persistence = daily_symbol.get("persistence")
    classification = daily_symbol.get("classification")
    historical_matches = daily_symbol.get("historical_matches")
    for name, value in (
        ("current", current),
        ("dynamics", dynamics),
        ("persistence", persistence),
        ("classification", classification),
    ):
        if value is not None and not isinstance(value, Mapping):
            raise StateHistoryContextError(f"daily_{name}_must_be_object")
    if historical_matches is not None and not isinstance(historical_matches, Mapping):
        raise StateHistoryContextError("daily_historical_matches_must_be_object")

    classification_map = classification if isinstance(classification, Mapping) else {}
    classification_overextended = classification_map.get("overextension_warning")
    if isinstance(classification_overextended, bool):
        if classification_overextended != (payload.get("overextension_active") is True):
            raise StateHistoryContextError("overextension_state_source_mismatch")

    state, state_sequence = _canonical_state(payload)
    if state not in CANONICAL_STATES:
        raise StateHistoryContextError("unsupported_state_history_state")

    packet_as_of = str(validated.get("as_of") or "")
    claim_available = str(claim.get("available_from") or "")
    if _utc(claim_available, "state_history_available_from") > _utc(
        packet_as_of, "packet_as_of"
    ):
        raise StateHistoryContextError("future_state_history_claim")

    return {
        "schema_version": SCHEMA_VERSION,
        "symbol": str(validated["symbol"]),
        "as_of": packet_as_of,
        "source_snapshot_id": str(validated["source_snapshot_id"]),
        "source_claim_id": str(claim.get("claim_id") or ""),
        "source_claim_available_from": claim_available,
        "source_context_type": PATH_CONTEXT_TYPE,
        "state": state,
        "state_sequence": state_sequence,
        "path_memory": {
            "overextension_active": payload.get("overextension_active"),
            "last_overextension_date": payload.get("last_overextension_date"),
            "last_overextension_rs3m": payload.get("last_overextension_rs3m"),
            "sessions_since_last_overextension": payload.get(
                "sessions_since_last_overextension"
            ),
            "recent_overextension": payload.get("recent_overextension"),
            "upstream_sequence_state": payload.get("sequence_state"),
            "upstream_review_state": payload.get("review_state"),
            "deterioration": deepcopy(dict(payload.get("deterioration") or {})),
        },
        "current": deepcopy(dict(current or {})),
        "dynamics": deepcopy(dict(dynamics or {})),
        "persistence": deepcopy(dict(persistence or {})),
        "classification": deepcopy(dict(classification or {})),
        "historical_matches": deepcopy(dict(historical_matches or {})),
        "semantics": {
            "existing_history_transported_not_reconstructed": True,
            "new_overextension_threshold_created": False,
            "new_trading_rule_created": False,
            "directional_vote_created": False,
            "universal_stance_changed": False,
            "portfolio_action_changed": False,
            "w8_action_policy_evaluated": False,
            "missing_values_remain_missing": True,
        },
        "research_only": True,
    }


def attach_state_history_to_7f(
    action: Mapping[str, object],
    context: Mapping[str, object] | None,
) -> dict[str, object]:
    """Attach W7 to an existing 7F result while proving the action is unchanged."""
    validated_action = validate_portfolio_action(action)
    if context is None:
        return deepcopy(validated_action)
    if context.get("schema_version") != SCHEMA_VERSION:
        raise StateHistoryContextError("unsupported_state_history_schema")
    if context.get("research_only") is not True:
        raise StateHistoryContextError("state_history_must_remain_research_only")
    semantics = context.get("semantics")
    if not isinstance(semantics, Mapping):
        raise StateHistoryContextError("state_history_semantics_required")
    required_false = (
        "new_overextension_threshold_created",
        "new_trading_rule_created",
        "directional_vote_created",
        "universal_stance_changed",
        "portfolio_action_changed",
        "w8_action_policy_evaluated",
    )
    if any(semantics.get(key) is not False for key in required_false):
        raise StateHistoryContextError("state_history_semantic_guard_violation")

    for key in ("symbol", "as_of", "source_snapshot_id"):
        if str(context.get(key) or "") != str(validated_action.get(key) or ""):
            raise StateHistoryContextError(f"state_history_7f_{key}_mismatch")

    original_action = deepcopy(validated_action["portfolio_action"])
    original_stance = deepcopy(validated_action["universal_stance_context"])
    out = deepcopy(validated_action)
    out["state_history_context"] = deepcopy(dict(context))
    out_semantics = deepcopy(dict(out.get("semantics") or {}))
    out_semantics.update({
        "state_history_is_directional_vote": False,
        "state_history_is_trade_decision": False,
        "state_history_changed_universal_stance": False,
        "state_history_changed_portfolio_action": False,
        "state_history_w8_action_policy_evaluated": False,
    })
    out["semantics"] = out_semantics
    out_validation = deepcopy(dict(out.get("validation") or {}))
    out_validation.update({
        "state_history_context_attached": True,
        "state_history_action_policy_evaluated": False,
    })
    out["validation"] = out_validation

    validate_portfolio_action(out)
    if out["portfolio_action"] != original_action:
        raise StateHistoryContextError("w7_changed_portfolio_action")
    if out["universal_stance_context"] != original_stance:
        raise StateHistoryContextError("w7_changed_universal_stance")
    return out
