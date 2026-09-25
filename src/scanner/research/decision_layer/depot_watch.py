"""Phase-7H research-only Depot-Watch integration.

7H joins the authoritative current ``daily_research`` snapshot with explicitly
supplied position snapshots and already-computed 7A->7G Decision-Layer bundles.
It is a presentation/integration layer: missing evidence is never rebuilt from
scanner scalars, and stance, hysteresis, portfolio action and execution remain
outside 7H authority.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Mapping, Sequence

from scanner.research.decision_layer.input_contract import validate_input_packet
from scanner.research.decision_layer.portfolio_action import (
    ACTION_STATES,
    validate_portfolio_action,
    validate_position_snapshot,
)
from scanner.research.decision_layer.reliability_explainability import (
    RELIABILITY_STATES,
    validate_reliability_explanation,
)
from scanner.research.decision_layer.state_transition import validate_state_transition
from scanner.research.decision_layer.universal_stance import validate_universal_stance


SCHEMA_VERSION = "decision_depot_watch_v1"
POSITION_BOOK_SCHEMA_VERSION = "decision_depot_position_book_v1"
BUNDLE_SCHEMA_VERSION = "decision_chain_bundle_v1"
BUNDLE_SET_SCHEMA_VERSION = "decision_chain_bundle_set_v1"

AVAILABILITY_STATES = frozenset({
    "decision_available",
    "symbol_not_in_daily_research",
    "decision_bundle_missing",
    "decision_bundle_snapshot_mismatch",
    "decision_bundle_invalid",
    "position_context_mismatch",
})
WATCH_STATUSES = frozenset({"complete", "partial", "unavailable"})
PRESENTATION_GROUPS = frozenset({
    "review_now", "waiting_confirmation", "hold_or_no_action", "unavailable"
})
REVIEW_ACTIONS = frozenset({"ENTER_REVIEW", "ADD_REVIEW", "REDUCE_REVIEW", "EXIT_REVIEW"})
WAITING_ACTIONS = frozenset({"WAIT_CONFIRMATION"})
PASSIVE_ACTIONS = frozenset({"HOLD", "NO_ACTION"})
FORBIDDEN_OUTPUT_KEYS = frozenset({
    "order_instruction",
    "order_quantity",
    "position_size",
    "target_weight",
    "limit_price",
    "stop_price",
    "buy_signal",
    "sell_signal",
    "trade_decision",
    "recommended_action",
})


class DepotWatchError(ValueError):
    """Raised when Phase-7H integration input or output violates the contract."""


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise DepotWatchError(f"{field}_required")
    text = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DepotWatchError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _canonical_hash(value: Mapping[str, object]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(raw.encode("utf-8")).hexdigest()


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


def validate_daily_research_snapshot(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the file-system-independent 7H-facing daily snapshot shape.

    The runtime CLI additionally calls the repository's full
    ``validate_daily_research`` validator before reaching this boundary.
    """
    snapshot_id = str(value.get("snapshot_id") or "").strip()
    as_of = str(value.get("as_of") or "").strip()
    symbols = value.get("symbols")
    if not snapshot_id:
        raise DepotWatchError("daily_research_snapshot_id_required")
    if not as_of:
        raise DepotWatchError("daily_research_as_of_required")
    _timestamp(as_of, "daily_research_as_of")
    if not isinstance(symbols, Mapping):
        raise DepotWatchError("daily_research_symbols_required")

    normalized_symbols: dict[str, object] = {}
    for raw_symbol, payload in symbols.items():
        symbol = str(raw_symbol).strip()
        if not symbol:
            raise DepotWatchError("daily_research_empty_symbol")
        if symbol in normalized_symbols:
            raise DepotWatchError(f"daily_research_duplicate_symbol:{symbol}")
        if not isinstance(payload, Mapping):
            raise DepotWatchError(f"daily_research_symbol_payload_invalid:{symbol}")
        normalized_symbols[symbol] = deepcopy(dict(payload))

    universe_size = value.get("universe_size")
    if universe_size is not None:
        try:
            parsed_size = int(universe_size)
        except (TypeError, ValueError) as exc:
            raise DepotWatchError("daily_research_universe_size_invalid") from exc
        if parsed_size != len(normalized_symbols):
            raise DepotWatchError("daily_research_universe_size_mismatch")

    out = deepcopy(dict(value))
    out["snapshot_id"] = snapshot_id
    out["as_of"] = as_of
    out["symbols"] = normalized_symbols
    return out


def validate_position_book(
    value: Mapping[str, object], *, decision_as_of: object | None = None
) -> dict[str, object]:
    if value.get("schema_version") != POSITION_BOOK_SCHEMA_VERSION:
        raise DepotWatchError("unsupported_position_book_schema")
    source_snapshot_id = str(value.get("source_snapshot_id") or "").strip()
    if not source_snapshot_id:
        raise DepotWatchError("position_book_source_snapshot_id_required")
    as_of = str(value.get("as_of") or "").strip()
    book_time = _timestamp(as_of, "position_book_as_of")
    decision_time = _timestamp(decision_as_of, "decision_as_of") if decision_as_of is not None else None
    if decision_time is not None and book_time > decision_time:
        raise DepotWatchError("future_position_book")

    raw_positions = value.get("positions")
    if not isinstance(raw_positions, list):
        raise DepotWatchError("position_book_positions_must_be_list")
    positions: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in raw_positions:
        if not isinstance(raw, Mapping):
            raise DepotWatchError("position_book_row_must_be_object")
        position = validate_position_snapshot(raw)
        symbol = str(position["symbol"])
        if symbol in seen:
            raise DepotWatchError(f"duplicate_position_symbol:{symbol}")
        seen.add(symbol)
        position_time = _timestamp(position["as_of"], "position_as_of")
        if position_time > book_time:
            raise DepotWatchError(f"position_newer_than_position_book:{symbol}")
        if decision_time is not None and position_time > decision_time:
            raise DepotWatchError(f"future_position_snapshot:{symbol}")
        positions.append(position)

    out = deepcopy(dict(value))
    out["source_snapshot_id"] = source_snapshot_id
    out["as_of"] = as_of
    out["positions"] = positions
    return out


def validate_decision_bundle(value: Mapping[str, object]) -> dict[str, object]:
    """Validate one complete already-computed 7A/7D/7E/7F/7G chain."""
    if value.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise DepotWatchError("unsupported_decision_bundle_schema")
    keys = ("packet", "stance", "transition", "action", "explanation")
    if any(not isinstance(value.get(key), Mapping) for key in keys):
        raise DepotWatchError("decision_bundle_members_required")

    packet = validate_input_packet(value["packet"])
    stance = validate_universal_stance(value["stance"])
    transition = validate_state_transition(value["transition"])
    action = validate_portfolio_action(value["action"])
    explanation = validate_reliability_explanation(value["explanation"])
    sources = (packet, stance, transition, action, explanation)
    for key in ("symbol", "as_of", "source_snapshot_id"):
        values = [str(source.get(key) or "") for source in sources]
        if len(set(values)) != 1:
            raise DepotWatchError(f"decision_bundle_{key}_mismatch")

    universal = stance.get("universal_stance")
    raw = transition.get("raw_stance")
    transition_state = transition.get("transition_state")
    action_stance = action.get("universal_stance_context")
    action_transition = action.get("transition_context")
    action_row = action.get("portfolio_action")
    explanation_context = explanation.get("decision_context")
    if not all(isinstance(item, Mapping) for item in (
        universal, raw, transition_state, action_stance, action_transition,
        action_row, explanation_context,
    )):
        raise DepotWatchError("decision_bundle_context_missing")
    assert isinstance(universal, Mapping)
    assert isinstance(raw, Mapping)
    assert isinstance(transition_state, Mapping)
    assert isinstance(action_stance, Mapping)
    assert isinstance(action_transition, Mapping)
    assert isinstance(action_row, Mapping)
    assert isinstance(explanation_context, Mapping)

    if (raw.get("state"), raw.get("direction")) != (universal.get("state"), universal.get("direction")):
        raise DepotWatchError("decision_bundle_7d_7e_stance_mismatch")
    if (action_stance.get("raw_state"), action_stance.get("raw_direction")) != (universal.get("state"), universal.get("direction")):
        raise DepotWatchError("decision_bundle_7d_7f_stance_mismatch")
    for key in ("status", "stable_directional_anchor", "pending_direction", "stable_anchor_is_current_stance"):
        if action_transition.get(key) != transition_state.get(key):
            raise DepotWatchError(f"decision_bundle_7e_7f_transition_mismatch:{key}")
    if explanation_context.get("portfolio_action_state") != action_row.get("state"):
        raise DepotWatchError("decision_bundle_7f_7g_action_mismatch")

    out = deepcopy(dict(value))
    out.update({
        "packet": packet,
        "stance": stance,
        "transition": transition,
        "action": action,
        "explanation": explanation,
    })
    return out


def validate_bundle_set(value: Mapping[str, object]) -> dict[str, dict[str, object]]:
    if value.get("schema_version") != BUNDLE_SET_SCHEMA_VERSION:
        raise DepotWatchError("unsupported_decision_bundle_set_schema")
    raw_bundles = value.get("bundles")
    if not isinstance(raw_bundles, list):
        raise DepotWatchError("decision_bundle_set_bundles_must_be_list")
    bundles: dict[str, dict[str, object]] = {}
    for raw in raw_bundles:
        if not isinstance(raw, Mapping):
            raise DepotWatchError("decision_bundle_must_be_object")
        packet = raw.get("packet")
        symbol = str(packet.get("symbol") or "").strip() if isinstance(packet, Mapping) else ""
        if not symbol:
            raise DepotWatchError("decision_bundle_symbol_required")
        if symbol in bundles:
            raise DepotWatchError(f"duplicate_decision_bundle_symbol:{symbol}")
        bundles[symbol] = deepcopy(dict(raw))
    return bundles


def _daily_context(value: Mapping[str, object]) -> dict[str, object]:
    current = value.get("current")
    if not isinstance(current, Mapping):
        return {}
    allowed = (
        "name", "score", "rank", "rank_percentile", "r_code", "rs3m",
        "trend200", "cycle", "confidence", "confidence_label", "close",
        "currency", "sector", "cluster", "cluster_official",
    )
    return {key: deepcopy(current.get(key)) for key in allowed if key in current}


def _position_add_capacity(position: Mapping[str, object]) -> str:
    """Mirror the frozen 7F capacity semantics without inventing capacity."""
    can_add = position.get("can_add")
    remaining = position.get("remaining_adds")
    if can_add is False or remaining == 0:
        return "blocked"
    if can_add is True or (isinstance(remaining, int) and remaining > 0):
        return "available"
    return "unknown"


def _position_matches_action(position: Mapping[str, object], action: Mapping[str, object]) -> bool:
    context = action.get("position_context")
    if not isinstance(context, Mapping):
        return False
    direct_pairs = (
        ("source_snapshot_id", "source_snapshot_id"),
        ("as_of", "as_of"),
        ("position_state", "position_state"),
        ("quantity", "quantity"),
        ("market_value", "market_value"),
        ("currency", "currency"),
        ("average_entry_price", "average_entry_price"),
        ("current_price", "current_price"),
        ("remaining_adds", "remaining_adds"),
    )
    if not all(position.get(left) == context.get(right) for left, right in direct_pairs):
        return False
    return context.get("add_capacity_state") == _position_add_capacity(position)


def _group_for_action(action_state: str) -> str:
    if action_state in REVIEW_ACTIONS:
        return "review_now"
    if action_state in WAITING_ACTIONS:
        return "waiting_confirmation"
    if action_state in PASSIVE_ACTIONS:
        return "hold_or_no_action"
    raise DepotWatchError(f"unsupported_watch_action:{action_state}")


def _position_view(position: Mapping[str, object]) -> dict[str, object]:
    fields = (
        "source_snapshot_id", "as_of", "position_state", "quantity",
        "market_value", "currency", "average_entry_price", "current_price",
        "remaining_adds",
    )
    return {field: deepcopy(position.get(field)) for field in fields}


def _unavailable_row(
    *,
    symbol: str,
    position: Mapping[str, object],
    daily_context: Mapping[str, object],
    availability: str,
    reason: str,
    order: int,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "position_order": order,
        "availability": availability,
        "availability_reason": reason,
        "presentation_group": "unavailable",
        "attention_required": False,
        "daily_scanner_context": deepcopy(dict(daily_context)),
        "position": _position_view(position),
        "decision": None,
        "execution_allowed": False,
    }


def _available_row(
    *,
    symbol: str,
    daily_context: Mapping[str, object],
    bundle: Mapping[str, object],
    order: int,
) -> dict[str, object]:
    packet = bundle["packet"]
    stance = bundle["stance"]
    transition = bundle["transition"]
    action = bundle["action"]
    explanation = bundle["explanation"]
    assert all(isinstance(item, Mapping) for item in (packet, stance, transition, action, explanation))
    universal = stance["universal_stance"]
    transition_state = transition["transition_state"]
    action_row = action["portfolio_action"]
    reliability = explanation["reliability"]
    assert all(isinstance(item, Mapping) for item in (universal, transition_state, action_row, reliability))

    explanation_body = explanation.get("explanation")
    gaps = explanation_body.get("missing_or_limited_evidence", []) if isinstance(explanation_body, Mapping) else []
    gaps = gaps if isinstance(gaps, list) else []
    triggers = explanation.get("change_triggers")
    triggers = triggers if isinstance(triggers, Mapping) else {}
    decision_triggers = triggers.get("decision_change_triggers", [])
    information_triggers = triggers.get("information_completion_triggers", [])
    decision_triggers = decision_triggers if isinstance(decision_triggers, list) else []
    information_triggers = information_triggers if isinstance(information_triggers, list) else []

    action_state = str(action_row["state"])
    group = _group_for_action(action_state)
    coverage = packet.get("coverage")
    return {
        "symbol": symbol,
        "position_order": order,
        "availability": "decision_available",
        "availability_reason": "same_snapshot_decision_chain_validated",
        "presentation_group": group,
        "attention_required": group in {"review_now", "waiting_confirmation"},
        "daily_scanner_context": deepcopy(dict(daily_context)),
        "position": deepcopy(dict(action.get("position_context", {}))),
        "decision": {
            "universal_stance_state": universal.get("state"),
            "universal_stance_direction": universal.get("direction"),
            "transition_status": transition_state.get("status"),
            "stable_directional_anchor": transition_state.get("stable_directional_anchor"),
            "pending_direction": transition_state.get("pending_direction"),
            "pending_confirmation_count": transition_state.get("pending_confirmation_count"),
            "required_confirmation_count": transition_state.get("required_confirmation_count"),
            "portfolio_action_state": action_state,
            "portfolio_action_reason_code": action_row.get("reason_code"),
            "swing_management": deepcopy(action.get("swing_management")),
            "pnl_context": deepcopy(action.get("pnl_context")),
            "reliability_assessment": reliability.get("assessment"),
            "numeric_reliability_score": reliability.get("numeric_reliability_score"),
            "coverage_admission_state": coverage.get("admission_state") if isinstance(coverage, Mapping) else None,
            "evidence_gap_count": len(gaps),
            "missing_or_limited_evidence": deepcopy(gaps),
            "decision_change_triggers": deepcopy(decision_triggers),
            "information_completion_triggers": deepcopy(information_triggers),
            "explanation_id": explanation.get("explanation_id"),
        },
        "execution_allowed": False,
    }


def build_depot_watch(
    daily_research: Mapping[str, object],
    position_book: Mapping[str, object],
    bundle_set: Mapping[str, object],
) -> dict[str, object]:
    """Build a compact non-ranking Depot-Watch from preserved decisions."""
    daily = validate_daily_research_snapshot(daily_research)
    positions = validate_position_book(position_book, decision_as_of=daily["as_of"])
    raw_bundles = validate_bundle_set(bundle_set)
    daily_symbols = daily["symbols"]
    assert isinstance(daily_symbols, Mapping)

    rows: list[dict[str, object]] = []
    used_bundles: set[str] = set()
    for order, position in enumerate(positions["positions"]):
        assert isinstance(position, Mapping)
        symbol = str(position["symbol"])
        daily_symbol = daily_symbols.get(symbol)
        context = _daily_context(daily_symbol) if isinstance(daily_symbol, Mapping) else {}

        if not isinstance(daily_symbol, Mapping):
            rows.append(_unavailable_row(
                symbol=symbol, position=position, daily_context=context,
                availability="symbol_not_in_daily_research",
                reason="exact_symbol_absent_from_authoritative_daily_snapshot",
                order=order,
            ))
            continue

        raw_bundle = raw_bundles.get(symbol)
        if raw_bundle is None:
            rows.append(_unavailable_row(
                symbol=symbol, position=position, daily_context=context,
                availability="decision_bundle_missing",
                reason="complete_7a_7g_bundle_not_supplied",
                order=order,
            ))
            continue

        packet = raw_bundle.get("packet")
        bundle_snapshot = str(packet.get("source_snapshot_id") or "") if isinstance(packet, Mapping) else ""
        bundle_as_of = str(packet.get("as_of") or "") if isinstance(packet, Mapping) else ""
        if bundle_snapshot != daily["snapshot_id"] or bundle_as_of != daily["as_of"]:
            rows.append(_unavailable_row(
                symbol=symbol, position=position, daily_context=context,
                availability="decision_bundle_snapshot_mismatch",
                reason="decision_bundle_not_from_authoritative_daily_snapshot",
                order=order,
            ))
            continue

        try:
            bundle = validate_decision_bundle(raw_bundle)
        except Exception as exc:  # per-row fail-closed boundary
            rows.append(_unavailable_row(
                symbol=symbol, position=position, daily_context=context,
                availability="decision_bundle_invalid",
                reason=f"decision_bundle_validation_failed:{type(exc).__name__}:{exc}",
                order=order,
            ))
            continue

        used_bundles.add(symbol)
        if not _position_matches_action(position, bundle["action"]):
            rows.append(_unavailable_row(
                symbol=symbol, position=position, daily_context=context,
                availability="position_context_mismatch",
                reason="7f_action_was_not_computed_from_supplied_position_snapshot",
                order=order,
            ))
            continue

        rows.append(_available_row(
            symbol=symbol,
            daily_context=context,
            bundle=bundle,
            order=order,
        ))

    availability_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    reliability_counts: dict[str, int] = {}
    group_counts: dict[str, int] = {}
    for row in rows:
        availability = str(row["availability"])
        group = str(row["presentation_group"])
        availability_counts[availability] = availability_counts.get(availability, 0) + 1
        group_counts[group] = group_counts.get(group, 0) + 1
        decision = row.get("decision")
        if isinstance(decision, Mapping):
            action_state = str(decision.get("portfolio_action_state") or "")
            reliability_state = str(decision.get("reliability_assessment") or "")
            action_counts[action_state] = action_counts.get(action_state, 0) + 1
            reliability_counts[reliability_state] = reliability_counts.get(reliability_state, 0) + 1

    available_count = availability_counts.get("decision_available", 0)
    if rows and available_count == len(rows):
        watch_status = "complete"
    elif available_count:
        watch_status = "partial"
    else:
        watch_status = "unavailable"

    output: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7H",
        "as_of": daily["as_of"],
        "source_snapshot_id": daily["snapshot_id"],
        "position_book": {
            "schema_version": POSITION_BOOK_SCHEMA_VERSION,
            "source_snapshot_id": positions["source_snapshot_id"],
            "as_of": positions["as_of"],
            "position_count": len(positions["positions"]),
        },
        "watch_status": watch_status,
        "rows": rows,
        "presentation": {
            "groups": {
                group: [row["symbol"] for row in rows if row["presentation_group"] == group]
                for group in ("review_now", "waiting_confirmation", "hold_or_no_action", "unavailable")
            },
            "grouping_is_ranking": False,
            "action_or_reliability_ranking_performed": False,
        },
        "summary": {
            "position_count": len(rows),
            "decision_available_count": available_count,
            "unavailable_count": len(rows) - available_count,
            "attention_required_count": sum(bool(row["attention_required"]) for row in rows),
            "availability_counts": dict(sorted(availability_counts.items())),
            "action_counts": dict(sorted(action_counts.items())),
            "reliability_counts": dict(sorted(reliability_counts.items())),
            "presentation_group_counts": dict(sorted(group_counts.items())),
            "unused_bundle_symbols": sorted(set(raw_bundles) - used_bundles),
        },
        "semantics": {
            "daily_research_is_authoritative_snapshot": True,
            "old_freshness_gate_used": False,
            "history_recent_used_as_freshness_gate": False,
            "scanner_scalar_replaced_missing_decision_evidence": False,
            "model_portfolio_used_as_actual_position_source": False,
            "legacy_holdings_used_as_actual_position_source": False,
            "fuzzy_symbol_matching_used": False,
            "universal_stance_recomputed": False,
            "transition_recomputed": False,
            "portfolio_action_changed": False,
            "conflict_resolved": False,
            "numeric_reliability_score_created": False,
            "holdings_ranked": False,
            "position_sizing_computed": False,
            "target_weight_computed": False,
            "broker_order_generated": False,
        },
        "privacy": {
            "real_position_input_is_runtime_only_by_contract": True,
            "public_repository_persistence_default": False,
            "autopilot_public_commit_enabled": False,
        },
        "validation": {
            "research_only": True,
            "integration_empirically_validated": False,
            "future_mature_outcomes_required": True,
            "phase_7i_promotion_review_required": True,
            "productive_integration_enabled": False,
            "execution_allowed": False,
            "promotion_eligible": False,
        },
    }
    output["watch_id"] = _canonical_hash(output)
    return validate_depot_watch(output)


def validate_depot_watch(value: Mapping[str, object]) -> dict[str, object]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise DepotWatchError("unsupported_depot_watch_schema")
    if value.get("phase") != "7H":
        raise DepotWatchError("invalid_depot_watch_phase")
    if value.get("watch_status") not in WATCH_STATUSES:
        raise DepotWatchError("invalid_watch_status")
    _timestamp(value.get("as_of"), "watch_as_of")
    if not str(value.get("source_snapshot_id") or "").strip():
        raise DepotWatchError("watch_source_snapshot_id_required")

    rows = value.get("rows")
    if not isinstance(rows, list):
        raise DepotWatchError("watch_rows_must_be_list")
    seen: set[str] = set()
    available = 0
    for row in rows:
        if not isinstance(row, Mapping):
            raise DepotWatchError("watch_row_must_be_object")
        symbol = str(row.get("symbol") or "")
        if not symbol or symbol in seen:
            raise DepotWatchError("watch_symbol_missing_or_duplicate")
        seen.add(symbol)
        availability = str(row.get("availability") or "")
        group = str(row.get("presentation_group") or "")
        if availability not in AVAILABILITY_STATES:
            raise DepotWatchError(f"invalid_availability_state:{symbol}")
        if group not in PRESENTATION_GROUPS:
            raise DepotWatchError(f"invalid_presentation_group:{symbol}")
        decision = row.get("decision")
        if availability == "decision_available":
            available += 1
            if not isinstance(decision, Mapping):
                raise DepotWatchError(f"available_row_requires_decision:{symbol}")
            action_state = str(decision.get("portfolio_action_state") or "")
            reliability = str(decision.get("reliability_assessment") or "")
            if action_state not in ACTION_STATES:
                raise DepotWatchError(f"available_row_invalid_action:{symbol}")
            if reliability not in RELIABILITY_STATES:
                raise DepotWatchError(f"available_row_invalid_reliability:{symbol}")
            if decision.get("numeric_reliability_score") is not None:
                raise DepotWatchError(f"numeric_reliability_score_forbidden:{symbol}")
            if _group_for_action(action_state) != group:
                raise DepotWatchError(f"action_presentation_group_mismatch:{symbol}")
        elif decision is not None or group != "unavailable":
            raise DepotWatchError(f"unavailable_row_must_not_expose_decision:{symbol}")
        if row.get("execution_allowed") is not False:
            raise DepotWatchError(f"watch_row_execution_must_remain_disabled:{symbol}")

    expected_status = "unavailable"
    if rows and available == len(rows):
        expected_status = "complete"
    elif available:
        expected_status = "partial"
    if value.get("watch_status") != expected_status:
        raise DepotWatchError("watch_status_count_mismatch")

    forbidden = _forbidden_paths(value)
    if forbidden:
        raise DepotWatchError("forbidden_execution_or_sizing_fields:" + ",".join(forbidden))

    semantics = value.get("semantics")
    if not isinstance(semantics, Mapping):
        raise DepotWatchError("watch_semantics_required")
    required_false = (
        "old_freshness_gate_used",
        "history_recent_used_as_freshness_gate",
        "scanner_scalar_replaced_missing_decision_evidence",
        "model_portfolio_used_as_actual_position_source",
        "legacy_holdings_used_as_actual_position_source",
        "fuzzy_symbol_matching_used",
        "universal_stance_recomputed",
        "transition_recomputed",
        "portfolio_action_changed",
        "conflict_resolved",
        "numeric_reliability_score_created",
        "holdings_ranked",
        "position_sizing_computed",
        "target_weight_computed",
        "broker_order_generated",
    )
    if any(semantics.get(key) is not False for key in required_false):
        raise DepotWatchError("watch_semantic_guard_violation")
    if semantics.get("daily_research_is_authoritative_snapshot") is not True:
        raise DepotWatchError("daily_snapshot_authority_guard_missing")

    validation = value.get("validation")
    if not isinstance(validation, Mapping):
        raise DepotWatchError("watch_validation_required")
    if validation.get("research_only") is not True:
        raise DepotWatchError("watch_must_remain_research_only")
    if validation.get("productive_integration_enabled") is not False:
        raise DepotWatchError("productive_watch_integration_must_remain_disabled")
    if validation.get("execution_allowed") is not False:
        raise DepotWatchError("watch_execution_must_remain_disabled")
    if validation.get("promotion_eligible") is not False:
        raise DepotWatchError("watch_promotion_must_remain_closed")

    watch_id = str(value.get("watch_id") or "")
    if not watch_id:
        raise DepotWatchError("watch_id_required")
    unsigned = deepcopy(dict(value))
    unsigned.pop("watch_id", None)
    if watch_id != _canonical_hash(unsigned):
        raise DepotWatchError("watch_id_integrity_failure")
    return deepcopy(dict(value))
