"""W8 completion policy for the Phase-7F depot action matrix.

The policy consumes only already-existing 7F output plus the typed W7
state/history context.  It introduces no indicator threshold, no new market
claim and no broker execution.  Its job is to make the previously specified
action matrix explicit, especially the Ferrari regression sequence:

    overextended -> overextension_with_momentum_loss ->
    post_overextension_correction

Profit protection may therefore be reviewed before a correction, but a pure
profit-protection review must not become a delayed sale after that correction
has already happened while the higher-level stance is still positive.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Mapping, Sequence

from .portfolio_action import (
    COST_SENSITIVE_ACTIONS,
    PortfolioActionError,
    validate_portfolio_action,
)


SCHEMA_VERSION = "decision_depot_action_policy_v1"
STATE_HISTORY_SCHEMA_VERSION = "decision_state_history_context_v1"
CONFIRMED_STATUSES = frozenset({
    "bootstrap_confirmed", "stable_confirmed", "transition_confirmed"
})
PENDING_STATUSES = frozenset({"bootstrap_pending", "transition_pending"})
BLOCKING_STATUSES = frozenset({"blocked_conflict", "blocked_insufficient"})
ADD_CONTEXTS = frozenset({"entry_or_add_review", "reentry_or_add_review"})
PROFIT_PROTECTION_CONTEXT = "profit_protection_review"
STRUCTURAL_REDUCE_CONTEXTS = frozenset({
    "partial_reduce_review", "larger_reduce_or_exit_review"
})
W7_STATES = frozenset({
    "never_overextended",
    "overextended",
    "overextension_with_momentum_loss",
    "post_overextension_correction",
})


class DepotActionPolicyError(ValueError):
    """Raised when W8 cannot resolve the action matrix without inference."""


def _contexts(action: Mapping[str, object]) -> set[str]:
    swing = action.get("swing_management")
    if not isinstance(swing, Mapping):
        return set()
    raw = swing.get("review_contexts")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
        return set()
    return {str(item) for item in raw}


def _validate_state_history(
    action: Mapping[str, object], context: Mapping[str, object] | None
) -> dict[str, object] | None:
    if context is None:
        return None
    if context.get("schema_version") != STATE_HISTORY_SCHEMA_VERSION:
        raise DepotActionPolicyError("unsupported_state_history_schema")
    if context.get("research_only") is not True:
        raise DepotActionPolicyError("state_history_must_remain_research_only")
    state = str(context.get("state") or "")
    if state not in W7_STATES:
        raise DepotActionPolicyError("unsupported_state_history_state")
    for key in ("symbol", "as_of", "source_snapshot_id"):
        if str(context.get(key) or "") != str(action.get(key) or ""):
            raise DepotActionPolicyError(f"state_history_action_{key}_mismatch")
    semantics = context.get("semantics")
    if not isinstance(semantics, Mapping):
        raise DepotActionPolicyError("state_history_semantics_required")
    required_false = (
        "new_overextension_threshold_created",
        "new_trading_rule_created",
        "directional_vote_created",
        "universal_stance_changed",
    )
    if any(semantics.get(key) is not False for key in required_false):
        raise DepotActionPolicyError("state_history_semantic_guard_violation")
    return deepcopy(dict(context))


def _mode_for(action_state: str, position_state: str) -> str:
    return {
        "NO_ACTION": "flat_monitor",
        "WAIT_CONFIRMATION": (
            "position_transition_watch" if position_state == "long" else "flat_transition_watch"
        ),
        "ENTER_REVIEW": "flat_entry_review",
        "HOLD": "position_hold",
        "ADD_REVIEW": "position_add_review",
        "REDUCE_REVIEW": "position_reduce_review",
        "EXIT_REVIEW": "position_exit_review",
    }[action_state]


def _refresh_cost_context(out: dict[str, object], action_state: str) -> None:
    cost = out.get("cost_context")
    if not isinstance(cost, dict):
        return
    cost_sensitive = action_state in COST_SENSITIVE_ACTIONS
    cost_bps = cost.get("transaction_cost_bps")
    cost["cost_sensitive_action"] = cost_sensitive
    cost["cost_model_present"] = cost_bps is not None
    cost["net_benefit_claim_made"] = False
    cost["missing_cost_model_blocks_net_benefit_claim"] = (
        cost_sensitive and cost_bps is None
    )


def apply_depot_action_policy(
    action: Mapping[str, object],
    state_history_context: Mapping[str, object] | None,
) -> dict[str, object]:
    """Resolve the frozen W8 action matrix without changing 7D direction."""
    try:
        validated = validate_portfolio_action(action)
    except PortfolioActionError as exc:
        raise DepotActionPolicyError(str(exc)) from exc
    out = deepcopy(validated)
    context = _validate_state_history(out, state_history_context)

    stance = out.get("universal_stance_context")
    transition = out.get("transition_context")
    position = out.get("position_context")
    action_row = out.get("portfolio_action")
    if not all(isinstance(item, Mapping) for item in (
        stance, transition, position, action_row
    )):
        raise DepotActionPolicyError("w8_action_context_required")

    raw_state = str(stance.get("raw_state") or "")
    status = str(transition.get("status") or "")
    position_state = str(position.get("position_state") or "")
    base_action = str(action_row.get("state") or "")
    final_action = base_action
    reason = str(action_row.get("reason_code") or "")
    state = str(context.get("state") or "") if context is not None else None
    contexts = _contexts(out)
    add_context = bool(contexts & ADD_CONTEXTS)
    profit_protection = PROFIT_PROTECTION_CONTEXT in contexts
    structural_reduce = bool(contexts & STRUCTURAL_REDUCE_CONTEXTS)

    policy_case = "base_action_preserved"
    warning_code: str | None = None
    conflict = False
    reassessment_required = False
    state_history_used_for_review_routing = False

    if position_state != "long":
        policy_case = "non_long_position_base_action_preserved"
    elif status in PENDING_STATUSES:
        policy_case = "negative_or_directional_transition_unconfirmed_wait"
    elif status == "blocked_conflict" or raw_state == "conflicted":
        policy_case = "directional_conflict_hold_warning"
        warning_code = "directional_conflict_requires_review"
        conflict = True
    elif status == "blocked_insufficient" or raw_state == "insufficient_evidence":
        policy_case = "insufficient_evidence_hold_without_inference"
        warning_code = "insufficient_evidence_no_action_inference"
    elif status in CONFIRMED_STATUSES and raw_state == "negative":
        # F4: confirmed higher-level negative evidence remains authoritative.
        policy_case = "f4_confirmed_negative_exit_review"
    elif status in CONFIRMED_STATUSES and raw_state == "positive":
        if state is None:
            policy_case = "positive_state_history_missing_preserve_existing_action"
            warning_code = "state_history_missing"
        elif state == "overextension_with_momentum_loss":
            state_history_used_for_review_routing = True
            if add_context or base_action == "ADD_REVIEW":
                # W7 asks for profit protection while W6 asks to add: do not
                # arbitrarily choose one side of the conflict.
                final_action = "HOLD"
                reason = "w8_profit_protection_vs_add_context_conflict"
                policy_case = "profit_protection_add_conflict_hold"
                warning_code = "add_reduce_context_conflict"
                conflict = True
            else:
                # F2: timely profit-protection review before the correction.
                final_action = "REDUCE_REVIEW"
                reason = "w8_f2_overextension_momentum_loss_profit_protection"
                policy_case = "f2_profit_protection_before_correction"
        elif state == "post_overextension_correction":
            reassessment_required = True
            # F3: a pure profit-protection route is too late once the correction
            # has already happened.  Independent structural reduce evidence is
            # still allowed to stand; W8 does not erase fresh W6 structure.
            if (
                base_action == "REDUCE_REVIEW"
                and profit_protection
                and not structural_reduce
            ):
                final_action = "HOLD"
                reason = "w8_f3_post_correction_hold_reassess"
                policy_case = "f3_delayed_profit_protection_suppressed"
                warning_code = "profit_protection_window_already_passed"
                state_history_used_for_review_routing = True
            elif base_action == "HOLD":
                reason = "w8_f3_post_correction_positive_hold_reassess"
                policy_case = "f3_post_correction_positive_hold_reassess"
            elif base_action == "REDUCE_REVIEW" and structural_reduce:
                policy_case = "post_correction_independent_structural_reduce_preserved"
            elif base_action == "ADD_REVIEW":
                policy_case = "post_correction_independent_add_review_preserved"
        elif state == "overextended":
            # F1: overextension alone is not a sell signal.
            if base_action == "HOLD":
                reason = "w8_f1_overextended_but_strong_hold"
                policy_case = "f1_overextended_but_strong_hold"
        elif state == "never_overextended":
            policy_case = "positive_normal_state_existing_swing_policy"

    action_changed = final_action != base_action
    out_action = deepcopy(dict(action_row))
    out_action["state"] = final_action
    out_action["reason_code"] = reason
    out["portfolio_action"] = out_action

    swing = out.get("swing_management")
    if isinstance(swing, dict):
        previous_mode = swing.get("mode")
        final_mode = _mode_for(final_action, position_state)
        if previous_mode != final_mode:
            swing["pre_w8_mode"] = previous_mode
            swing["mode"] = final_mode

    _refresh_cost_context(out, final_action)

    if context is not None:
        context_semantics = context.get("semantics")
        context_semantics = (
            deepcopy(dict(context_semantics)) if isinstance(context_semantics, Mapping) else {}
        )
        context_semantics["w8_action_policy_evaluated"] = True
        context_semantics["portfolio_action_changed"] = action_changed
        context["semantics"] = context_semantics
        out["state_history_context"] = context

    out["depot_action_policy"] = {
        "schema_version": SCHEMA_VERSION,
        "policy_id": "w8_complete_7f_action_matrix_v1",
        "base_action_state": base_action,
        "resolved_action_state": final_action,
        "action_changed": action_changed,
        "policy_case": policy_case,
        "warning_code": warning_code,
        "conflict": conflict,
        "reassessment_required": reassessment_required,
        "state_history_state": state,
        "state_history_used_for_review_routing": state_history_used_for_review_routing,
        "state_history_used_for_stance_direction": False,
        "swing_contexts": sorted(contexts),
        "new_indicator_threshold_created": False,
        "new_market_evidence_created": False,
        "review_only": True,
        "execution_allowed": False,
    }

    semantics = out.get("semantics")
    semantics = deepcopy(dict(semantics)) if isinstance(semantics, Mapping) else {}
    semantics.update({
        "w8_action_matrix_evaluated": True,
        "state_history_is_directional_vote": False,
        "state_history_changed_universal_stance": False,
        "state_history_changed_action_review_state": action_changed,
        "w8_created_indicator_threshold": False,
        "w8_created_market_evidence": False,
        "w8_generated_broker_order": False,
    })
    out["semantics"] = semantics

    validation = out.get("validation")
    validation = deepcopy(dict(validation)) if isinstance(validation, Mapping) else {}
    validation.update({
        "w8_action_matrix_evaluated": True,
        "w8_regression_contract": "ferrari_f1_f4_v1",
        "w8_execution_allowed": False,
    })
    out["validation"] = validation

    try:
        validate_portfolio_action(out)
    except PortfolioActionError as exc:
        raise DepotActionPolicyError(str(exc)) from exc
    if out["universal_stance_context"] != validated["universal_stance_context"]:
        raise DepotActionPolicyError("w8_changed_universal_stance")
    return out
