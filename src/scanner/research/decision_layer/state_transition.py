"""Phase-7E research-only hysteresis around consecutive 7D stances.

The raw Phase-7D Universal Stance is never rewritten. 7E maintains a separate
stable directional anchor plus explicit pending/blocking transition states. The
frozen v1 confirmation depth is a research candidate, not an empirically
validated trading rule.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping, Sequence

from scanner.research.decision_layer.universal_stance import (
    STANCE_STATES,
    validate_universal_stance,
)


SCHEMA_VERSION = "decision_state_transition_v1"
DEFAULT_MIN_CONSECUTIVE = 2
CANDIDATE_DEPTHS = (1, 2, 3, 5)
DIRECTIONAL_STATES = frozenset({"positive", "negative"})
TRANSITION_STATUSES = frozenset({
    "bootstrap_pending",
    "bootstrap_confirmed",
    "stable_confirmed",
    "transition_pending",
    "transition_confirmed",
    "blocked_conflict",
    "blocked_insufficient",
})
FORBIDDEN_OUTPUT_KEYS = frozenset({
    "portfolio_action",
    "trade_decision",
    "order_instruction",
    "buy_signal",
    "sell_signal",
    "position_size",
    "target_weight",
})


class StateTransitionError(ValueError):
    """Raised when a 7E stance sequence or transition output is invalid."""


def _parse_timestamp(value: object) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise StateTransitionError("as_of_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise StateTransitionError("invalid_as_of") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _calendar_date(value: object) -> str:
    return _parse_timestamp(value).date().isoformat()


def _utc_timestamp(value: object) -> datetime:
    return _parse_timestamp(value).astimezone(timezone.utc)


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


def _validate_history(history: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    if not history:
        raise StateTransitionError("stance_history_required")
    normalized: list[dict[str, object]] = []
    symbol: str | None = None
    previous_time: datetime | None = None
    snapshot_ids: set[str] = set()
    for index, item in enumerate(history):
        validated = validate_universal_stance(item)
        item_symbol = str(validated.get("symbol") or "").strip()
        if not item_symbol:
            raise StateTransitionError("symbol_required")
        if symbol is None:
            symbol = item_symbol
        elif item_symbol != symbol:
            raise StateTransitionError("mixed_symbols_not_allowed")
        snapshot_id = str(validated.get("source_snapshot_id") or "").strip()
        if not snapshot_id:
            raise StateTransitionError("source_snapshot_id_required")
        if snapshot_id in snapshot_ids:
            raise StateTransitionError("duplicate_source_snapshot_id")
        snapshot_ids.add(snapshot_id)
        instant = _utc_timestamp(validated.get("as_of"))
        if previous_time is not None and instant <= previous_time:
            raise StateTransitionError(f"stance_history_not_strictly_increasing_at:{index}")
        previous_time = instant
        normalized.append(deepcopy(validated))
    return normalized


def _event(
    *,
    stance: Mapping[str, object],
    status: str,
    stable_before: str | None,
    stable_after: str | None,
    pending_direction: str | None,
    pending_count: int,
    min_consecutive: int,
) -> dict[str, object]:
    raw = stance["universal_stance"]
    assert isinstance(raw, Mapping)
    raw_state = str(raw.get("state"))
    return {
        "as_of": stance["as_of"],
        "calendar_date": _calendar_date(stance["as_of"]),
        "source_snapshot_id": stance["source_snapshot_id"],
        "raw_stance_state": raw_state,
        "raw_stance_direction": raw.get("direction"),
        "transition_status": status,
        "stable_anchor_before": stable_before,
        "stable_anchor_after": stable_after,
        "pending_direction": pending_direction,
        "pending_confirmation_count": pending_count,
        "required_confirmation_count": min_consecutive,
        "stable_anchor_is_current_stance": (
            stable_after is not None
            and raw_state in DIRECTIONAL_STATES
            and raw_state == stable_after
            and status not in {"transition_pending", "bootstrap_pending"}
        ),
    }


def _build(history: Sequence[Mapping[str, object]], min_consecutive: int) -> dict[str, object]:
    if min_consecutive < 1:
        raise StateTransitionError("min_consecutive_must_be_positive")
    items = _validate_history(history)
    stable: str | None = None
    pending_direction: str | None = None
    pending_count = 0
    pending_dates: set[str] = set()
    events: list[dict[str, object]] = []

    for stance in items:
        raw = stance["universal_stance"]
        assert isinstance(raw, Mapping)
        raw_state = str(raw.get("state"))
        if raw_state not in STANCE_STATES:
            raise StateTransitionError("invalid_raw_stance_state")
        date = _calendar_date(stance["as_of"])
        before = stable

        if raw_state == "conflicted":
            pending_direction = None
            pending_count = 0
            pending_dates.clear()
            status = "blocked_conflict"
        elif raw_state == "insufficient_evidence":
            pending_direction = None
            pending_count = 0
            pending_dates.clear()
            status = "blocked_insufficient"
        elif raw_state in DIRECTIONAL_STATES:
            if stable == raw_state:
                pending_direction = None
                pending_count = 0
                pending_dates.clear()
                status = "stable_confirmed"
            else:
                if pending_direction != raw_state:
                    pending_direction = raw_state
                    pending_count = 1
                    pending_dates = {date}
                elif date not in pending_dates:
                    pending_dates.add(date)
                    pending_count += 1
                if pending_count >= min_consecutive:
                    stable = raw_state
                    pending_direction = None
                    pending_count = 0
                    pending_dates.clear()
                    status = "bootstrap_confirmed" if before is None else "transition_confirmed"
                else:
                    status = "bootstrap_pending" if stable is None else "transition_pending"
        else:  # pragma: no cover - guarded by 7D validator and STANCE_STATES
            raise StateTransitionError("unsupported_raw_stance_state")

        events.append(
            _event(
                stance=stance,
                status=status,
                stable_before=before,
                stable_after=stable,
                pending_direction=pending_direction,
                pending_count=pending_count,
                min_consecutive=min_consecutive,
            )
        )

    latest = events[-1]
    latest_stance = items[-1]
    partition = str(latest_stance.get("research_partition") or "")
    result = {
        "schema_version": SCHEMA_VERSION,
        "phase": "7E",
        "symbol": latest_stance["symbol"],
        "as_of": latest_stance["as_of"],
        "source_snapshot_id": latest_stance["source_snapshot_id"],
        "raw_stance": deepcopy(latest_stance["universal_stance"]),
        "transition_state": {
            "status": latest["transition_status"],
            "stable_directional_anchor": latest["stable_anchor_after"],
            "pending_direction": latest["pending_direction"],
            "pending_confirmation_count": latest["pending_confirmation_count"],
            "required_confirmation_count": min_consecutive,
            "stable_anchor_is_current_stance": latest["stable_anchor_is_current_stance"],
        },
        "candidate_rule": {
            "rule_id": "minimal_repeat_candidate_v1" if min_consecutive == 2 else f"comparison_depth_{min_consecutive}",
            "min_consecutive_directional_observations": min_consecutive,
            "require_distinct_calendar_dates": True,
            "require_distinct_source_snapshot_ids": True,
            "reset_pending_on_nondirectional_raw_state": True,
            "empirically_validated": False,
            "production_eligible": False,
        },
        "events": events,
        "semantics": {
            "raw_7d_stance_preserved": True,
            "conflict_treated_as_neutral": False,
            "insufficient_treated_as_neutral": False,
            "stable_anchor_replaces_raw_stance": False,
            "weighted_super_score_used": False,
            "portfolio_action_computed": False,
            "order_instruction_computed": False,
            "threshold_optimized_on_spent_data": False,
        },
        "validation": {
            "status": "prospective_unconfirmed" if partition == "prospective_unspent" else "spent_replay_only",
            "latest_research_partition": partition,
            "hysteresis_rule_empirically_validated": False,
            "future_mature_outcomes_required": True,
            "productive_integration_enabled": False,
            "promotion_eligible": False,
        },
    }
    validate_state_transition(result)
    return result


def build_state_transition_history(
    history: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Apply the frozen minimal-repeat candidate to an ordered 7D history."""
    return _build(history, DEFAULT_MIN_CONSECUTIVE)


def compare_confirmation_depths(
    history: Sequence[Mapping[str, object]],
    depths: Sequence[int] = CANDIDATE_DEPTHS,
) -> dict[str, object]:
    """Compare transition mechanics without selecting or promoting a winner."""
    items = _validate_history(history)
    rows: list[dict[str, object]] = []
    for depth in depths:
        result = _build(items, int(depth))
        events = result["events"]
        assert isinstance(events, list)
        rows.append({
            "confirmation_depth": int(depth),
            "event_count": len(events),
            "bootstrap_confirmations": sum(event["transition_status"] == "bootstrap_confirmed" for event in events),
            "confirmed_transitions": sum(event["transition_status"] == "transition_confirmed" for event in events),
            "pending_observations": sum(event["transition_status"] in {"bootstrap_pending", "transition_pending"} for event in events),
            "blocked_observations": sum(event["transition_status"] in {"blocked_conflict", "blocked_insufficient"} for event in events),
            "final_stable_directional_anchor": result["transition_state"]["stable_directional_anchor"],
        })
    return {
        "schema_version": "decision_state_transition_candidate_comparison_v1",
        "phase": "7E",
        "symbol": items[-1]["symbol"],
        "as_of": items[-1]["as_of"],
        "candidates": rows,
        "winner_selected": False,
        "selection_recommendation": None,
        "historical_churn_reduction_is_outcome_validation": False,
        "future_mature_outcomes_required": True,
    }


def validate_state_transition(value: Mapping[str, object]) -> dict[str, object]:
    """Fail closed if a 7E output violates the frozen semantic boundary."""
    if value.get("schema_version") != SCHEMA_VERSION:
        raise StateTransitionError("unsupported_transition_schema")
    raw = value.get("raw_stance")
    if not isinstance(raw, Mapping) or raw.get("state") not in STANCE_STATES:
        raise StateTransitionError("raw_stance_required")
    transition = value.get("transition_state")
    if not isinstance(transition, Mapping):
        raise StateTransitionError("transition_state_required")
    if transition.get("status") not in TRANSITION_STATUSES:
        raise StateTransitionError("invalid_transition_status")
    stable = transition.get("stable_directional_anchor")
    if stable is not None and stable not in DIRECTIONAL_STATES:
        raise StateTransitionError("invalid_stable_directional_anchor")
    if str(raw.get("state")) in {"conflicted", "insufficient_evidence"} and transition.get("stable_anchor_is_current_stance") is not False:
        raise StateTransitionError("blocking_raw_state_cannot_equal_stable_current_stance")
    rule = value.get("candidate_rule")
    if not isinstance(rule, Mapping):
        raise StateTransitionError("candidate_rule_required")
    if rule.get("empirically_validated") is not False or rule.get("production_eligible") is not False:
        raise StateTransitionError("unvalidated_candidate_rule_must_remain_research_only")
    semantics = value.get("semantics")
    if not isinstance(semantics, Mapping):
        raise StateTransitionError("semantics_required")
    required_true = ("raw_7d_stance_preserved",)
    required_false = (
        "conflict_treated_as_neutral",
        "insufficient_treated_as_neutral",
        "stable_anchor_replaces_raw_stance",
        "weighted_super_score_used",
        "portfolio_action_computed",
        "order_instruction_computed",
        "threshold_optimized_on_spent_data",
    )
    if any(semantics.get(key) is not True for key in required_true):
        raise StateTransitionError("raw_stance_preservation_guard_missing")
    if any(semantics.get(key) is not False for key in required_false):
        raise StateTransitionError("transition_semantic_guard_violation")
    forbidden = _forbidden_paths(value)
    if forbidden:
        raise StateTransitionError("forbidden_portfolio_or_action_fields:" + ",".join(forbidden))
    validation = value.get("validation")
    if not isinstance(validation, Mapping):
        raise StateTransitionError("validation_required")
    if validation.get("hysteresis_rule_empirically_validated") is not False:
        raise StateTransitionError("hysteresis_validation_must_remain_false")
    if validation.get("productive_integration_enabled") is not False or validation.get("promotion_eligible") is not False:
        raise StateTransitionError("productive_promotion_must_remain_closed")
    return dict(value)
