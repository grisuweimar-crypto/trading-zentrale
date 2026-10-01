"""W6 adapter from Elliott-vNext 6H output into the frozen Phase-7F interface.

The adapter consumes an already-built 6H research object.  It does not run an
Elliott count, choose a scenario, infer direction, resolve 6D route conflicts or
create an order.  It only transports the already-existing review contexts to
7F, where portfolio-aware review logic already exists.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Mapping, Sequence


SOURCE_SET_SCHEMA = "decision_elliott_6h_source_v1"
ELLIOTT_SCHEMA = "elliott_vnext_output_v2"
ELLIOTT_MODULE = "6H_module_output"
ELLIOTT_CONTRACT = "elliott_vnext_integration_contract_v1"
SOURCE_NAME = "elliott_vnext_6h"

# W6 deliberately exposes only the action-relevant review contexts named in the
# Watch build plan.  6D's hold_review remains a valid Elliott research route but
# has no 7F action effect and is therefore omitted from the transported list.
W6_REVIEW_CONTEXTS = frozenset({
    "entry_or_add_review",
    "reentry_or_add_review",
    "partial_reduce_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
})
SIX_D_REVIEW_CONTEXTS = W6_REVIEW_CONTEXTS | {"hold_review"}
FORBIDDEN_TRADING_KEYS = frozenset({"trade_decision", "order_instruction"})
REQUIRED_6H_FIELDS = frozenset({
    "symbol",
    "as_of",
    "timeframe",
    "degree",
    "primary_scenario",
    "alternative_scenarios",
    "pivots",
    "fibonacci",
    "current_wave_stage",
    "projection_zones",
    "wave_cycle_map",
    "hard_invalidations",
    "structural_fit",
    "confirmation_strength",
    "historical_expectancy",
    "routing_triggers",
    "swing_routing",
    "warnings",
})


class Elliott6HAdapterError(ValueError):
    """Raised when 6H evidence cannot cross the W6 integration boundary."""


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise Elliott6HAdapterError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Elliott6HAdapterError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Elliott6HAdapterError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _date(value: object, field: str) -> date:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise Elliott6HAdapterError(f"invalid_{field}") from exc


def _forbidden_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if str(key) in FORBIDDEN_TRADING_KEYS:
                found.append(child)
            found.extend(_forbidden_paths(item, child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{index}]"))
    return found


def validate_elliott_6h_output(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the 6H boundary needed by W6 without re-running Elliott."""
    if value.get("schema_version") != ELLIOTT_SCHEMA:
        raise Elliott6HAdapterError("unsupported_elliott_6h_schema")
    if value.get("module") != ELLIOTT_MODULE:
        raise Elliott6HAdapterError("elliott_6h_module_identity_invalid")
    missing = sorted(REQUIRED_6H_FIELDS - set(map(str, value.keys())))
    if missing:
        raise Elliott6HAdapterError("elliott_6h_required_fields_missing:" + ",".join(missing))
    if value.get("research_only") is not True:
        raise Elliott6HAdapterError("elliott_6h_must_remain_research_only")
    if value.get("routing_is_trade_decision") is not False:
        raise Elliott6HAdapterError("elliott_6h_routing_must_remain_review_only")
    if value.get("single_true_count_claimed") is not False:
        raise Elliott6HAdapterError("elliott_single_true_count_forbidden")
    if value.get("fibonacci_selects_wave_count") is not False:
        raise Elliott6HAdapterError("elliott_fibonacci_count_selection_forbidden")
    if not str(value.get("output_id") or "").strip():
        raise Elliott6HAdapterError("elliott_6h_output_id_required")
    if not str(value.get("symbol") or "").strip():
        raise Elliott6HAdapterError("elliott_6h_symbol_required")
    output_as_of = _date(value.get("as_of"), "elliott_6h_as_of")

    forbidden = _forbidden_paths(value)
    if forbidden:
        raise Elliott6HAdapterError("elliott_6h_forbidden_trading_fields:" + ",".join(forbidden))

    integration = value.get("integration")
    if not isinstance(integration, Mapping):
        raise Elliott6HAdapterError("elliott_6h_integration_required")
    if integration.get("contract_version") != ELLIOTT_CONTRACT:
        raise Elliott6HAdapterError("elliott_6h_integration_contract_invalid")
    if integration.get("decision_layer_required") is not True:
        raise Elliott6HAdapterError("elliott_decision_layer_requirement_missing")
    if integration.get("productive_integration_enabled") is not False:
        raise Elliott6HAdapterError("elliott_productive_integration_must_remain_disabled")
    if integration.get("direct_ordering_allowed") is not False:
        raise Elliott6HAdapterError("elliott_direct_ordering_must_remain_disabled")
    if integration.get("review_contexts_are_not_actions") is not True:
        raise Elliott6HAdapterError("elliott_review_context_guard_missing")

    routes = value.get("swing_routing")
    if not isinstance(routes, list):
        raise Elliott6HAdapterError("elliott_6h_swing_routing_must_be_list")
    for index, route in enumerate(routes):
        if not isinstance(route, Mapping):
            raise Elliott6HAdapterError(f"elliott_6h_invalid_swing_route:{index}")
        context = str(route.get("review_context") or "")
        if context not in SIX_D_REVIEW_CONTEXTS:
            raise Elliott6HAdapterError(f"elliott_6h_unknown_review_context:{context}")
        if route.get("actionability") != "review_only_not_trade_instruction":
            raise Elliott6HAdapterError(f"elliott_6h_route_actionability_invalid:{index}")
        route_date = _date(route.get("available_from"), "elliott_route_available_from")
        if route_date > output_as_of:
            raise Elliott6HAdapterError(f"elliott_6h_future_route:{index}")
        if route.get("final_decision_owned_by_global_layer") is False:
            raise Elliott6HAdapterError(f"elliott_6h_global_decision_guard_invalid:{index}")

    return deepcopy(dict(value))


def index_elliott_6h_source(
    source: Mapping[str, object] | None,
    *,
    decision_as_of: str,
) -> tuple[dict[str, dict[str, object]], dict[str, object]]:
    """Validate one optional W6 source envelope and index its outputs by symbol."""
    if source is None:
        return {}, {
            "status": "not_supplied",
            "source_commit": None,
            "available_from": None,
            "output_count": 0,
        }
    if source.get("schema_version") != SOURCE_SET_SCHEMA:
        raise Elliott6HAdapterError("unsupported_elliott_6h_source_schema")
    source_commit = str(source.get("source_commit") or "").strip()
    if len(source_commit) < 12:
        raise Elliott6HAdapterError("elliott_6h_source_commit_required")
    available_text = str(source.get("available_from") or "").strip()
    available = _timestamp(available_text, "elliott_6h_source_available_from")
    decision_time = _timestamp(decision_as_of, "decision_as_of")
    if available > decision_time:
        raise Elliott6HAdapterError("future_elliott_6h_source")
    outputs = source.get("outputs")
    if not isinstance(outputs, list):
        raise Elliott6HAdapterError("elliott_6h_outputs_must_be_list")

    indexed: dict[str, dict[str, object]] = {}
    for raw in outputs:
        if not isinstance(raw, Mapping):
            raise Elliott6HAdapterError("elliott_6h_output_must_be_object")
        output = validate_elliott_6h_output(raw)
        symbol = str(output["symbol"])
        if symbol in indexed:
            raise Elliott6HAdapterError(f"duplicate_elliott_6h_symbol:{symbol}")
        if _date(output["as_of"], "elliott_6h_as_of") > available.date():
            raise Elliott6HAdapterError(f"elliott_6h_output_after_source_availability:{symbol}")
        indexed[symbol] = output

    return indexed, {
        "status": "available",
        "source_commit": source_commit,
        "available_from": available.isoformat(),
        "output_count": len(indexed),
    }


def build_elliott_7f_swing_context(
    output: Mapping[str, object],
    *,
    source_commit: str,
    source_available_from: str,
) -> dict[str, object]:
    """Transport 6H review contexts into the existing 7F input vocabulary."""
    validated = validate_elliott_6h_output(output)
    routes = validated["swing_routing"]
    assert isinstance(routes, list)
    actionable = sorted({
        str(route["review_context"])
        for route in routes
        if isinstance(route, Mapping) and str(route.get("review_context")) in W6_REVIEW_CONTEXTS
    })
    hold_count = sum(
        1
        for route in routes
        if isinstance(route, Mapping) and route.get("review_context") == "hold_review"
    )
    add_present = bool(set(actionable) & {"entry_or_add_review", "reentry_or_add_review"})
    reduce_present = bool(set(actionable) & {
        "partial_reduce_review", "profit_protection_review", "larger_reduce_or_exit_review"
    })
    return {
        "source": SOURCE_NAME,
        "source_output_id": str(validated["output_id"]),
        "as_of": _timestamp(source_available_from, "elliott_6h_source_available_from").isoformat(),
        "review_contexts": actionable,
        "routing_is_trade_decision": False,
        "research_only": True,
        # Audit-only W6 metadata. Phase 7F ignores extra fields and consumes only
        # the frozen keys above.
        "w6": {
            "source_commit": str(source_commit),
            "elliott_output_as_of": str(validated["as_of"]),
            "timeframe": str(validated.get("timeframe") or ""),
            "degree": str(validated.get("degree") or ""),
            "route_count": len(routes),
            "actionable_route_context_count": len(actionable),
            "hold_review_routes_omitted": hold_count,
            "add_reduce_conflict_preserved": add_present and reduce_present,
            "direction_from_elliott_used": False,
            "stance_from_elliott_used": False,
            "review_contexts_are_actions": False,
            "changes_universal_stance": False,
        },
    }
