"""Phase-7F research-only Portfolio Action and Swing Management.

7F is the first position-aware layer. It consumes a validated 7E transition plus
an explicit point-in-time position snapshot. Position/P&L context never rewrites
the portfolio-independent 7D stance. Elliott routing may request review but never
becomes a directional vote or broker order.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import math
from typing import Mapping, Sequence

from scanner.research.decision_layer.state_transition import validate_state_transition


SCHEMA_VERSION = "decision_portfolio_action_v1"
POSITION_SCHEMA_VERSION = "decision_position_snapshot_v1"
POSITION_STATES = frozenset({"flat", "long"})
ACTION_STATES = frozenset({
    "NO_ACTION",
    "WAIT_CONFIRMATION",
    "ENTER_REVIEW",
    "HOLD",
    "ADD_REVIEW",
    "REDUCE_REVIEW",
    "EXIT_REVIEW",
})
PENDING_STATUSES = frozenset({"bootstrap_pending", "transition_pending"})
BLOCKING_STATUSES = frozenset({"blocked_conflict", "blocked_insufficient"})
CONFIRMED_STATUSES = frozenset({"bootstrap_confirmed", "stable_confirmed", "transition_confirmed"})
SWING_CONTEXTS = frozenset({
    "entry_or_add_review",
    "hold_review",
    "partial_reduce_review",
    "reentry_or_add_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
})
ADD_CONTEXTS = frozenset({"entry_or_add_review", "reentry_or_add_review"})
REDUCE_CONTEXTS = frozenset({
    "partial_reduce_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
})
COST_SENSITIVE_ACTIONS = frozenset({"ENTER_REVIEW", "ADD_REVIEW", "REDUCE_REVIEW", "EXIT_REVIEW"})
FORBIDDEN_OUTPUT_KEYS = frozenset({
    "position_size",
    "target_weight",
    "order_quantity",
    "limit_price",
    "stop_price",
    "buy_signal",
    "sell_signal",
})


class PortfolioActionError(ValueError):
    """Raised when Phase-7F inputs or outputs violate the frozen contract."""


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise PortfolioActionError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise PortfolioActionError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _number(value: object, field: str, *, allow_zero: bool = False) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioActionError(f"invalid_{field}") from exc
    if not math.isfinite(number):
        raise PortfolioActionError(f"invalid_{field}")
    if number < 0 or (number == 0 and not allow_zero):
        raise PortfolioActionError(f"invalid_{field}")
    return number


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


def validate_position_snapshot(value: Mapping[str, object]) -> dict[str, object]:
    if value.get("schema_version") != POSITION_SCHEMA_VERSION:
        raise PortfolioActionError("unsupported_position_snapshot_schema")
    symbol = str(value.get("symbol") or "").strip()
    snapshot_id = str(value.get("source_snapshot_id") or "").strip()
    if not symbol:
        raise PortfolioActionError("position_symbol_required")
    if not snapshot_id:
        raise PortfolioActionError("position_source_snapshot_id_required")
    _timestamp(value.get("as_of"), "position_as_of")

    state = str(value.get("position_state") or "")
    if state not in POSITION_STATES:
        raise PortfolioActionError("unsupported_position_state_long_only_v1")

    quantity = _number(value.get("quantity"), "quantity", allow_zero=True)
    market_value = _number(value.get("market_value"), "market_value", allow_zero=True)
    entry = _number(value.get("average_entry_price"), "average_entry_price")
    current = _number(value.get("current_price"), "current_price")
    cost_bps = _number(value.get("transaction_cost_bps"), "transaction_cost_bps", allow_zero=True)

    monetary_present = any(item is not None for item in (market_value, entry, current))
    currency = value.get("currency")
    if monetary_present and not str(currency or "").strip():
        raise PortfolioActionError("currency_required_for_monetary_fields")

    if state == "flat":
        if quantity not in (None, 0.0) or market_value not in (None, 0.0):
            raise PortfolioActionError("flat_position_cannot_have_positive_size")
    else:
        if quantity is not None and quantity <= 0:
            raise PortfolioActionError("long_quantity_must_be_positive_if_supplied")
        if market_value is not None and market_value <= 0:
            raise PortfolioActionError("long_market_value_must_be_positive_if_supplied")

    can_add = value.get("can_add")
    if can_add is not None and not isinstance(can_add, bool):
        raise PortfolioActionError("can_add_must_be_boolean_or_null")
    remaining = value.get("remaining_adds")
    if remaining is not None:
        if isinstance(remaining, bool):
            raise PortfolioActionError("remaining_adds_must_be_nonnegative_integer")
        try:
            remaining_int = int(remaining)
        except (TypeError, ValueError) as exc:
            raise PortfolioActionError("remaining_adds_must_be_nonnegative_integer") from exc
        if remaining_int < 0 or float(remaining_int) != float(remaining):
            raise PortfolioActionError("remaining_adds_must_be_nonnegative_integer")
        remaining = remaining_int
    if can_add is False and remaining is not None and remaining > 0:
        raise PortfolioActionError("contradictory_add_capacity")
    if can_add is True and remaining == 0:
        raise PortfolioActionError("contradictory_add_capacity")

    out = deepcopy(dict(value))
    out["symbol"] = symbol
    out["position_state"] = state
    out["quantity"] = quantity
    out["market_value"] = market_value
    out["average_entry_price"] = entry
    out["current_price"] = current
    out["transaction_cost_bps"] = cost_bps
    out["remaining_adds"] = remaining
    out["currency"] = str(currency).strip() if currency is not None else None
    return out


def _validate_swing_context(value: Mapping[str, object] | None, decision_as_of: datetime) -> dict[str, object]:
    if value is None:
        return {
            "source": None,
            "source_output_id": None,
            "as_of": None,
            "review_contexts": [],
            "routing_is_trade_decision": False,
            "research_only": True,
        }
    if value.get("source") != "elliott_vnext_6h":
        raise PortfolioActionError("unsupported_swing_context_source")
    if not str(value.get("source_output_id") or "").strip():
        raise PortfolioActionError("swing_source_output_id_required")
    context_time = _timestamp(value.get("as_of"), "swing_as_of")
    if context_time > decision_as_of:
        raise PortfolioActionError("future_swing_context")
    if value.get("routing_is_trade_decision") is not False:
        raise PortfolioActionError("swing_routing_must_remain_review_only")
    if value.get("research_only") is not True:
        raise PortfolioActionError("swing_context_must_be_research_only")
    contexts = value.get("review_contexts")
    if not isinstance(contexts, list):
        raise PortfolioActionError("swing_review_contexts_must_be_list")
    normalized = sorted(set(map(str, contexts)))
    unknown = [item for item in normalized if item not in SWING_CONTEXTS]
    if unknown:
        raise PortfolioActionError("unsupported_swing_review_context:" + ",".join(unknown))
    return {
        "source": "elliott_vnext_6h",
        "source_output_id": str(value["source_output_id"]),
        "as_of": str(value["as_of"]),
        "review_contexts": normalized,
        "routing_is_trade_decision": False,
        "research_only": True,
    }


def _add_capacity(position: Mapping[str, object]) -> str:
    can_add = position.get("can_add")
    remaining = position.get("remaining_adds")
    if can_add is False or remaining == 0:
        return "blocked"
    if can_add is True or (isinstance(remaining, int) and remaining > 0):
        return "available"
    return "unknown"


def _pnl_context(position: Mapping[str, object]) -> dict[str, object]:
    entry = position.get("average_entry_price")
    current = position.get("current_price")
    quantity = position.get("quantity")
    if entry is None or current is None:
        return {
            "unrealized_return_pct": None,
            "unrealized_pnl": None,
            "currency": position.get("currency"),
            "complete": False,
            "used_for_stance_direction": False,
            "used_for_action_direction": False,
        }
    entry_f = float(entry)
    current_f = float(current)
    pct = (current_f / entry_f - 1.0) * 100.0
    pnl = None if quantity is None else (current_f - entry_f) * float(quantity)
    return {
        "unrealized_return_pct": pct,
        "unrealized_pnl": pnl,
        "currency": position.get("currency"),
        "complete": True,
        "used_for_stance_direction": False,
        "used_for_action_direction": False,
    }


def _base_action(status: str, raw_state: str, position_state: str) -> tuple[str, str]:
    if status in PENDING_STATUSES:
        return "WAIT_CONFIRMATION", "directional_transition_not_yet_confirmed"
    if status in BLOCKING_STATUSES:
        return ("HOLD", "blocking_raw_state_position_unchanged") if position_state == "long" else ("NO_ACTION", "blocking_raw_state_flat")
    if status not in CONFIRMED_STATUSES:
        raise PortfolioActionError("unsupported_transition_status")
    if raw_state == "positive":
        return ("HOLD", "positive_confirmed_existing_long") if position_state == "long" else ("ENTER_REVIEW", "positive_confirmed_flat")
    if raw_state == "negative":
        return ("EXIT_REVIEW", "negative_confirmed_existing_long") if position_state == "long" else ("NO_ACTION", "negative_confirmed_flat")
    raise PortfolioActionError("confirmed_transition_requires_directional_raw_stance")


def compute_portfolio_action(
    transition: Mapping[str, object],
    position: Mapping[str, object],
    swing_context: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Compute one research-only position-aware action review state."""
    transition_valid = validate_state_transition(transition)
    position_valid = validate_position_snapshot(position)
    if str(transition_valid.get("symbol")) != str(position_valid.get("symbol")):
        raise PortfolioActionError("transition_position_symbol_mismatch")
    decision_time = _timestamp(transition_valid.get("as_of"), "decision_as_of")
    position_time = _timestamp(position_valid.get("as_of"), "position_as_of")
    if position_time > decision_time:
        raise PortfolioActionError("future_position_snapshot")
    swing = _validate_swing_context(swing_context, decision_time)

    raw = transition_valid.get("raw_stance")
    state = transition_valid.get("transition_state")
    assert isinstance(raw, Mapping) and isinstance(state, Mapping)
    raw_state = str(raw.get("state"))
    status = str(state.get("status"))
    position_state = str(position_valid["position_state"])
    action, reason = _base_action(status, raw_state, position_state)

    contexts = set(swing["review_contexts"])
    add_context = bool(contexts & ADD_CONTEXTS)
    reduce_context = bool(contexts & REDUCE_CONTEXTS)
    context_conflict = add_context and reduce_context
    capacity = _add_capacity(position_valid)
    swing_adjustment = "none"

    if (
        position_state == "long"
        and status in CONFIRMED_STATUSES
        and raw_state == "positive"
        and action == "HOLD"
    ):
        if context_conflict:
            swing_adjustment = "conflicting_add_reduce_contexts_preserved"
            reason = "positive_confirmed_but_swing_contexts_conflict"
        elif reduce_context:
            action = "REDUCE_REVIEW"
            swing_adjustment = "positive_stance_swing_reduction_review"
            reason = "positive_thesis_with_profit_or_structure_reduction_review"
        elif add_context and capacity == "available":
            action = "ADD_REVIEW"
            swing_adjustment = "positive_stance_add_review_with_explicit_capacity"
            reason = "positive_confirmed_add_context_and_capacity"
        elif add_context:
            swing_adjustment = "add_review_blocked_by_capacity"
            reason = "positive_confirmed_add_context_without_explicit_capacity"

    position_mode = {
        "NO_ACTION": "flat_monitor",
        "WAIT_CONFIRMATION": "position_transition_watch" if position_state == "long" else "flat_transition_watch",
        "ENTER_REVIEW": "flat_entry_review",
        "HOLD": "position_hold",
        "ADD_REVIEW": "position_add_review",
        "REDUCE_REVIEW": "position_reduce_review",
        "EXIT_REVIEW": "position_exit_review",
    }[action]
    cost_sensitive = action in COST_SENSITIVE_ACTIONS
    cost_bps = position_valid.get("transaction_cost_bps")

    output = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7F",
        "symbol": transition_valid["symbol"],
        "as_of": transition_valid["as_of"],
        "source_snapshot_id": transition_valid["source_snapshot_id"],
        "universal_stance_context": {
            "raw_state": raw_state,
            "raw_direction": raw.get("direction"),
            "preserved": True,
        },
        "transition_context": {
            "status": status,
            "stable_directional_anchor": state.get("stable_directional_anchor"),
            "pending_direction": state.get("pending_direction"),
            "stable_anchor_is_current_stance": state.get("stable_anchor_is_current_stance"),
            "preserved": True,
        },
        "position_context": {
            "schema_version": POSITION_SCHEMA_VERSION,
            "source_snapshot_id": position_valid["source_snapshot_id"],
            "as_of": position_valid["as_of"],
            "position_state": position_state,
            "quantity": position_valid.get("quantity"),
            "market_value": position_valid.get("market_value"),
            "currency": position_valid.get("currency"),
            "average_entry_price": position_valid.get("average_entry_price"),
            "current_price": position_valid.get("current_price"),
            "add_capacity_state": capacity,
            "remaining_adds": position_valid.get("remaining_adds"),
        },
        "pnl_context": _pnl_context(position_valid),
        "portfolio_action": {
            "state": action,
            "reason_code": reason,
            "review_only": True,
            "execution_allowed": False,
            "order_instruction": None,
        },
        "swing_management": {
            "mode": position_mode,
            "source": swing["source"],
            "source_output_id": swing["source_output_id"],
            "review_contexts": list(swing["review_contexts"]),
            "review_contexts_are_actions": False,
            "context_conflict": context_conflict,
            "adjustment": swing_adjustment,
            "elliott_changed_stance_direction": False,
        },
        "cost_context": {
            "cost_sensitive_action": cost_sensitive,
            "transaction_cost_bps": cost_bps,
            "cost_model_present": cost_bps is not None,
            "net_benefit_claim_made": False,
            "missing_cost_model_blocks_net_benefit_claim": cost_sensitive and cost_bps is None,
        },
        "semantics": {
            "position_state_changed_universal_stance": False,
            "pnl_changed_universal_stance": False,
            "pnl_changed_action_direction": False,
            "elliott_is_directional_vote": False,
            "elliott_review_context_is_order": False,
            "weighted_super_score_used": False,
            "position_sizing_computed": False,
            "target_weight_computed": False,
            "score_used_as_price_proxy": False,
            "broker_order_generated": False,
        },
        "validation": {
            "status": transition_valid.get("validation", {}).get("status"),
            "research_only": True,
            "portfolio_action_rule_empirically_validated": False,
            "swing_action_edge_empirically_validated": False,
            "future_mature_outcomes_required": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "promotion_eligible": False,
        },
    }
    validate_portfolio_action(output)
    return output


def validate_portfolio_action(value: Mapping[str, object]) -> dict[str, object]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioActionError("unsupported_portfolio_action_schema")
    action = value.get("portfolio_action")
    if not isinstance(action, Mapping) or action.get("state") not in ACTION_STATES:
        raise PortfolioActionError("valid_portfolio_action_required")
    if action.get("review_only") is not True or action.get("execution_allowed") is not False:
        raise PortfolioActionError("portfolio_action_must_remain_review_only")
    if action.get("order_instruction") is not None:
        raise PortfolioActionError("order_instruction_must_remain_null")
    stance = value.get("universal_stance_context")
    transition = value.get("transition_context")
    if not isinstance(stance, Mapping) or stance.get("preserved") is not True:
        raise PortfolioActionError("universal_stance_preservation_guard_missing")
    if not isinstance(transition, Mapping) or transition.get("preserved") is not True:
        raise PortfolioActionError("transition_preservation_guard_missing")
    semantics = value.get("semantics")
    if not isinstance(semantics, Mapping):
        raise PortfolioActionError("semantics_required")
    required_false = (
        "position_state_changed_universal_stance",
        "pnl_changed_universal_stance",
        "pnl_changed_action_direction",
        "elliott_is_directional_vote",
        "elliott_review_context_is_order",
        "weighted_super_score_used",
        "position_sizing_computed",
        "target_weight_computed",
        "score_used_as_price_proxy",
        "broker_order_generated",
    )
    if any(semantics.get(key) is not False for key in required_false):
        raise PortfolioActionError("portfolio_action_semantic_guard_violation")
    forbidden = _forbidden_paths(value)
    if forbidden:
        raise PortfolioActionError("forbidden_sizing_or_order_fields:" + ",".join(forbidden))
    validation = value.get("validation")
    if not isinstance(validation, Mapping):
        raise PortfolioActionError("validation_required")
    if validation.get("research_only") is not True:
        raise PortfolioActionError("portfolio_action_must_remain_research_only")
    if validation.get("portfolio_action_rule_empirically_validated") is not False:
        raise PortfolioActionError("portfolio_action_validation_must_remain_false")
    if validation.get("swing_action_edge_empirically_validated") is not False:
        raise PortfolioActionError("swing_action_validation_must_remain_false")
    if validation.get("productive_integration_enabled") is not False or validation.get("execution_allowed") is not False:
        raise PortfolioActionError("productive_execution_must_remain_disabled")
    if validation.get("promotion_eligible") is not False:
        raise PortfolioActionError("promotion_must_remain_closed")
    return dict(value)
