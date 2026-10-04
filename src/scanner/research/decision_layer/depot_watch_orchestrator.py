"""Canonical research-only orchestration for the private Depot Watch.

The orchestrator consumes validated archived Phase-7A packets, derives the
existing 7D->7G chain, injects private position context only at 7F and delegates
presentation to 7H. W6 contributes review-only Elliott context, W7 contributes
PIT state/history context, and W8 resolves the explicit 7F action matrix without
changing 7D direction or creating broker execution.

Phase-8 external evidence remains outside this canonical path until a separately
approved production-integration change binds it into the Decision Layer.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping, Sequence

from .depot_action_policy import DepotActionPolicyError, apply_depot_action_policy
from .depot_watch import (
    BUNDLE_SCHEMA_VERSION,
    BUNDLE_SET_SCHEMA_VERSION,
    build_depot_watch,
    seal_depot_watch,
    validate_daily_research_snapshot,
    validate_position_book,
)
from .input_contract import validate_input_packet
from .integrated_evidence import PATH_CONTEXT_TYPE
from .phase5_shadow import Phase5ShadowAdapterError, phase5_shadow_from_packet
from .phase6_elliott import (
    Elliott6HAdapterError,
    build_elliott_7f_multidegree_swing_context,
    index_elliott_6h_source,
)
from .phase7_state_history import (
    StateHistoryContextError,
    attach_state_history_to_7f,
    build_state_history_context,
)
from .portfolio_action import compute_portfolio_action
from .reliability_explainability import build_reliability_explanation
from .state_transition import build_state_transition_history
from .universal_stance import compute_universal_stance


class DepotWatchOrchestrationError(ValueError):
    """Raised when a canonical Decision-Layer chain cannot be reconstructed."""


def _utc(value: object, field: str = "as_of") -> datetime:
    text = str(value or "").strip()
    if not text:
        raise DepotWatchOrchestrationError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise DepotWatchOrchestrationError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def decision_bound_daily_snapshot(
    daily: Mapping[str, object],
    decision_as_of: object | None = None,
) -> dict[str, object]:
    """Return a 7H-facing copy bound to the actual Decision evidence time."""
    validated = validate_daily_research_snapshot(daily)
    generated_at = str(validated.get("generated_at") or "").strip()
    base_value = generated_at or validated["as_of"]
    base_time = _utc(base_value, "daily_generated_at" if generated_at else "daily_as_of")
    target_value = str(decision_as_of or base_value)
    target_time = _utc(target_value, "decision_as_of")
    if target_time < base_time:
        raise DepotWatchOrchestrationError("decision_before_daily_publication")

    out = deepcopy(validated)
    out["scanner_as_of_date"] = validated["as_of"]
    out["scanner_generated_at"] = generated_at or None
    out["as_of"] = target_value
    return out


def _decision_time_for_snapshot(
    packets: Sequence[Mapping[str, object]],
    *,
    snapshot_id: str,
    daily: Mapping[str, object],
) -> str:
    times: list[datetime] = []
    for raw in packets:
        packet = validate_input_packet(raw)
        if str(packet.get("source_snapshot_id")) == snapshot_id:
            times.append(_utc(packet.get("as_of"), "packet_as_of"))
    if not times:
        return str(decision_bound_daily_snapshot(daily)["as_of"])
    return max(times).isoformat()


def _current_packet_for_symbol(
    packets: Sequence[Mapping[str, object]],
    *,
    symbol: str,
    snapshot_id: str,
    decision_as_of: str,
) -> dict[str, object] | None:
    matches: list[dict[str, object]] = []
    decision_time = _utc(decision_as_of, "decision_as_of")
    for raw in packets:
        packet = validate_input_packet(raw)
        if str(packet.get("symbol")) != symbol:
            continue
        if str(packet.get("source_snapshot_id")) != snapshot_id:
            continue
        if _utc(packet.get("as_of"), "packet_as_of") != decision_time:
            continue
        matches.append(packet)
    if len(matches) > 1:
        raise DepotWatchOrchestrationError(f"duplicate_current_packet:{symbol}")
    return matches[0] if matches else None


def _stance_history(
    packets: Sequence[Mapping[str, object]],
    *,
    symbol: str,
    current_packet: Mapping[str, object],
) -> list[dict[str, object]]:
    """Use the latest evidence revision of each scanner snapshot exactly once."""
    current_time = _utc(current_packet.get("as_of"), "current_packet_as_of")
    by_snapshot: dict[str, dict[str, object]] = {}
    for raw in packets:
        packet = validate_input_packet(raw)
        if str(packet.get("symbol")) != symbol:
            continue
        packet_time = _utc(packet.get("as_of"), "packet_as_of")
        if packet_time > current_time:
            continue
        snapshot_id = str(packet.get("source_snapshot_id") or "")
        previous = by_snapshot.get(snapshot_id)
        if previous is None:
            by_snapshot[snapshot_id] = packet
            continue
        previous_time = _utc(previous.get("as_of"), "packet_as_of")
        if packet_time == previous_time:
            raise DepotWatchOrchestrationError(
                f"duplicate_symbol_snapshot_revision:{symbol}:{snapshot_id}"
            )
        if packet_time > previous_time:
            by_snapshot[snapshot_id] = packet

    eligible = sorted(by_snapshot.values(), key=lambda packet: _utc(packet["as_of"]))
    if not eligible:
        raise DepotWatchOrchestrationError(f"stance_history_missing:{symbol}")
    latest = eligible[-1]
    if (
        latest.get("source_snapshot_id") != current_packet.get("source_snapshot_id")
        or _utc(latest.get("as_of")) != current_time
    ):
        raise DepotWatchOrchestrationError(
            f"current_packet_not_latest_in_history:{symbol}"
        )
    return [compute_universal_stance(packet) for packet in eligible]


def _path_review(packet: Mapping[str, object]) -> dict[str, object] | None:
    matches: list[dict[str, object]] = []
    evidence = packet.get("evidence")
    if not isinstance(evidence, list):
        return None
    for row in evidence:
        if not isinstance(row, Mapping) or row.get("family") != "risk":
            continue
        payload = row.get("payload")
        if isinstance(payload, Mapping) and payload.get("context_type") == PATH_CONTEXT_TYPE:
            matches.append(deepcopy(dict(payload)))
    if len(matches) > 1:
        raise DepotWatchOrchestrationError(
            f"duplicate_path_context:{packet.get('symbol')}"
        )
    return matches[0] if matches else None


def _phase5_shadow(packet: Mapping[str, object]) -> dict[str, object] | None:
    try:
        return phase5_shadow_from_packet(packet)
    except Phase5ShadowAdapterError as exc:
        raise DepotWatchOrchestrationError(str(exc)) from exc


def build_decision_bundle_set(
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
    elliott_6h_source: Mapping[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    """Build 7A/7D/7E/7F/7G bundles for positions with final current packets."""
    validated_daily = validate_daily_research_snapshot(daily)
    snapshot_id = str(validated_daily["snapshot_id"])
    decision_as_of = _decision_time_for_snapshot(
        archive_packets,
        snapshot_id=snapshot_id,
        daily=validated_daily,
    )
    decision_daily = decision_bound_daily_snapshot(validated_daily, decision_as_of)
    positions = validate_position_book(
        position_book, decision_as_of=decision_daily["as_of"]
    )
    try:
        elliott_index, elliott_source_meta = index_elliott_6h_source(
            elliott_6h_source,
            decision_as_of=decision_as_of,
        )
    except Elliott6HAdapterError as exc:
        raise DepotWatchOrchestrationError(str(exc)) from exc

    daily_symbols = validated_daily.get("symbols")
    assert isinstance(daily_symbols, Mapping)

    bundles: list[dict[str, object]] = []
    missing_symbols: list[str] = []
    path_review_symbols: list[str] = []
    phase5_shadow_symbols: list[str] = []
    elliott_symbols: list[str] = []
    elliott_actionable_symbols: list[str] = []
    state_history_symbols: list[str] = []
    state_history_counts: dict[str, int] = {}
    w8_policy_symbols: list[str] = []
    w8_changed_action_symbols: list[str] = []
    w8_conflict_symbols: list[str] = []
    w8_reassessment_symbols: list[str] = []

    for position in positions["positions"]:
        symbol = str(position["symbol"])
        packet = _current_packet_for_symbol(
            archive_packets,
            symbol=symbol,
            snapshot_id=snapshot_id,
            decision_as_of=decision_as_of,
        )
        if packet is None:
            missing_symbols.append(symbol)
            continue
        daily_symbol = daily_symbols.get(symbol)
        if not isinstance(daily_symbol, Mapping):
            raise DepotWatchOrchestrationError(f"daily_symbol_missing:{symbol}")
        history = _stance_history(
            archive_packets,
            symbol=symbol,
            current_packet=packet,
        )
        stance = history[-1]
        transition = build_state_transition_history(history)

        swing_context: dict[str, object] | None = None
        elliott_outputs = elliott_index.get(symbol)
        if elliott_outputs is not None:
            try:
                swing_context = build_elliott_7f_multidegree_swing_context(
                    elliott_outputs,
                    source_commit=str(elliott_source_meta["source_commit"]),
                    source_available_from=str(elliott_source_meta["available_from"]),
                )
            except Elliott6HAdapterError as exc:
                raise DepotWatchOrchestrationError(str(exc)) from exc
            elliott_symbols.append(symbol)
            if swing_context.get("review_contexts"):
                elliott_actionable_symbols.append(symbol)

        try:
            state_history_context = build_state_history_context(packet, daily_symbol)
        except StateHistoryContextError as exc:
            raise DepotWatchOrchestrationError(str(exc)) from exc
        if state_history_context is not None:
            state_history_symbols.append(symbol)
            state_name = str(state_history_context.get("state") or "")
            state_history_counts[state_name] = state_history_counts.get(state_name, 0) + 1

        # W6 changes review routing only downstream of 7D/7E.
        action = compute_portfolio_action(
            transition, position, swing_context=swing_context
        )
        # W7 transports typed path/history state without changing the action.
        try:
            action = attach_state_history_to_7f(action, state_history_context)
        except StateHistoryContextError as exc:
            raise DepotWatchOrchestrationError(str(exc)) from exc
        # W8 is the explicit action-matrix resolution step. It may change only
        # the 7F review state, never the preserved Universal Stance or execution.
        try:
            action = apply_depot_action_policy(action, state_history_context)
        except DepotActionPolicyError as exc:
            raise DepotWatchOrchestrationError(str(exc)) from exc

        policy = action.get("depot_action_policy")
        if isinstance(policy, Mapping):
            w8_policy_symbols.append(symbol)
            if policy.get("action_changed") is True:
                w8_changed_action_symbols.append(symbol)
            if policy.get("conflict") is True:
                w8_conflict_symbols.append(symbol)
            if policy.get("reassessment_required") is True:
                w8_reassessment_symbols.append(symbol)

        explanation = build_reliability_explanation(packet, stance, transition, action)
        path_context = _path_review(packet)
        phase5_context = _phase5_shadow(packet)
        if path_context and path_context.get("review_state") == "profit_protection_review":
            path_review_symbols.append(symbol)
        if phase5_context is not None:
            phase5_shadow_symbols.append(symbol)

        attached_state_history = action.get("state_history_context")
        bundles.append({
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "packet": packet,
            "stance": stance,
            "transition": transition,
            "action": action,
            "explanation": explanation,
            "path_review": path_context,
            "phase5_confidence_shadow": phase5_context,
            "elliott_swing_context": deepcopy(swing_context),
            "state_history_context": (
                deepcopy(dict(attached_state_history))
                if isinstance(attached_state_history, Mapping)
                else deepcopy(state_history_context)
            ),
            "depot_action_policy": (
                deepcopy(dict(policy)) if isinstance(policy, Mapping) else None
            ),
        })

    bundle_set = {
        "schema_version": BUNDLE_SET_SCHEMA_VERSION,
        "bundles": bundles,
    }
    diagnostics = {
        "schema_version": "decision_depot_watch_orchestration_diagnostics_v1",
        "snapshot_id": snapshot_id,
        "decision_as_of": decision_as_of,
        "scanner_generated_at": decision_daily.get("scanner_generated_at"),
        "position_count": len(positions["positions"]),
        "bundle_count": len(bundles),
        "missing_current_packet_symbols": sorted(missing_symbols),
        "path_review_symbols": sorted(path_review_symbols),
        "phase5_shadow_symbols": sorted(phase5_shadow_symbols),
        "phase5_shadow_changes_portfolio_action": False,
        "elliott_6h_source_status": elliott_source_meta["status"],
        "elliott_6h_source_commit": elliott_source_meta["source_commit"],
        "elliott_6h_source_available_from": elliott_source_meta["available_from"],
        "elliott_6h_source_output_count": elliott_source_meta["output_count"],
        "elliott_6h_source_symbol_count": elliott_source_meta.get("symbol_count", 0),
        "elliott_6h_source_capture_id": elliott_source_meta.get("source_capture_id"),
        "elliott_6h_symbols": sorted(elliott_symbols),
        "elliott_6h_actionable_symbols": sorted(elliott_actionable_symbols),
        "elliott_changed_universal_stance": False,
        "elliott_direction_used_as_vote": False,
        "state_history_symbols": sorted(state_history_symbols),
        "state_history_counts": dict(sorted(state_history_counts.items())),
        "state_history_changes_universal_stance": False,
        "state_history_changes_portfolio_action": bool(w8_changed_action_symbols),
        "state_history_w8_action_policy_evaluated": True,
        "w8_action_policy_symbols": sorted(w8_policy_symbols),
        "w8_changed_action_symbols": sorted(w8_changed_action_symbols),
        "w8_conflict_symbols": sorted(w8_conflict_symbols),
        "w8_reassessment_symbols": sorted(w8_reassessment_symbols),
        "w8_ferrari_regression_contract": "f1_f4_v1",
        "w8_created_indicator_threshold": False,
        "w8_created_market_evidence": False,
        "w8_generated_broker_order": False,
        "phase8_external_evidence_activated": False,
        "selection_direction_inferred": False,
        "scanner_scalar_fallback_used": False,
        "private_position_data_persisted": False,
    }
    return bundle_set, diagnostics


def _attach_state_history_context(
    watch: dict[str, object],
    bundle_set: Mapping[str, object],
) -> dict[str, object]:
    raw_bundles = bundle_set.get("bundles")
    if not isinstance(raw_bundles, list):
        return watch
    context_by_symbol: dict[str, dict[str, object]] = {}
    policy_by_symbol: dict[str, dict[str, object]] = {}
    for bundle in raw_bundles:
        if not isinstance(bundle, Mapping):
            continue
        packet = bundle.get("packet")
        symbol = str(packet.get("symbol") or "") if isinstance(packet, Mapping) else ""
        context = bundle.get("state_history_context")
        policy = bundle.get("depot_action_policy")
        if symbol and isinstance(context, Mapping):
            context_by_symbol[symbol] = deepcopy(dict(context))
        if symbol and isinstance(policy, Mapping):
            policy_by_symbol[symbol] = deepcopy(dict(policy))

    rows = watch.get("rows")
    if not isinstance(rows, list):
        return watch
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        context = context_by_symbol.get(symbol)
        policy = policy_by_symbol.get(symbol)
        if context is not None:
            row["state_history_context"] = deepcopy(context)
        if policy is not None:
            row["depot_action_policy"] = deepcopy(policy)
        decision = row.get("decision")
        if not isinstance(decision, dict):
            continue
        if context is not None:
            decision["state_history_state"] = context.get("state")
            decision["state_history_sequence"] = deepcopy(
                context.get("state_sequence", [])
            )
        if policy is not None:
            decision["state_history_changed_universal_stance"] = False
            decision["state_history_changed_portfolio_action"] = policy.get(
                "action_changed"
            )
            decision["state_history_w8_action_policy_evaluated"] = True
            decision["w8_action_policy_case"] = policy.get("policy_case")
            decision["w8_action_policy_warning_code"] = policy.get("warning_code")
            decision["w8_action_policy_conflict"] = policy.get("conflict")
            decision["w8_reassessment_required"] = policy.get(
                "reassessment_required"
            )
    return watch


def _attach_path_reviews(
    watch: dict[str, object],
    bundle_set: Mapping[str, object],
) -> dict[str, object]:
    raw_bundles = bundle_set.get("bundles")
    if not isinstance(raw_bundles, list):
        return watch
    path_by_symbol: dict[str, dict[str, object]] = {}
    for bundle in raw_bundles:
        if not isinstance(bundle, Mapping):
            continue
        packet = bundle.get("packet")
        symbol = str(packet.get("symbol") or "") if isinstance(packet, Mapping) else ""
        path = bundle.get("path_review")
        if symbol and isinstance(path, Mapping):
            path_by_symbol[symbol] = deepcopy(dict(path))

    rows = watch.get("rows")
    if not isinstance(rows, list):
        return watch
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        path = path_by_symbol.get(symbol)
        if path is None:
            continue
        row["path_review"] = deepcopy(path)
        policy = row.get("depot_action_policy")
        suppressed = (
            isinstance(policy, Mapping)
            and policy.get("policy_case") == "f3_delayed_profit_protection_suppressed"
        )
        if path.get("review_state") == "profit_protection_review" and not suppressed:
            row["attention_required"] = True
        decision = row.get("decision")
        if isinstance(decision, dict):
            decision["path_review_state"] = path.get("review_state")
            decision["path_sequence_state"] = path.get("sequence_state")
            decision["path_review_is_trade_decision"] = False
            decision["path_review_suppressed_by_w8"] = suppressed
    return watch


def _attach_phase5_shadow(
    watch: dict[str, object],
    bundle_set: Mapping[str, object],
) -> dict[str, object]:
    raw_bundles = bundle_set.get("bundles")
    if not isinstance(raw_bundles, list):
        return watch
    context_by_symbol: dict[str, dict[str, object]] = {}
    for bundle in raw_bundles:
        if not isinstance(bundle, Mapping):
            continue
        packet = bundle.get("packet")
        symbol = str(packet.get("symbol") or "") if isinstance(packet, Mapping) else ""
        context = bundle.get("phase5_confidence_shadow")
        if symbol and isinstance(context, Mapping):
            context_by_symbol[symbol] = deepcopy(dict(context))

    rows = watch.get("rows")
    if not isinstance(rows, list):
        return watch
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        context = context_by_symbol.get(symbol)
        if context is None:
            continue
        row["phase5_confidence_shadow"] = deepcopy(context)
        decision = row.get("decision")
        if isinstance(decision, dict):
            payload = context.get("payload")
            payload = payload if isinstance(payload, Mapping) else {}
            decision["phase5_shadow_integration_mode"] = context.get("integration_mode")
            decision["phase5_shadow_status"] = payload.get("status")
            decision["phase5_shadow_insufficient_evidence_horizons"] = deepcopy(
                payload.get("insufficient_evidence_horizons", [])
            )
            decision["phase5_shadow_eligible_review_horizons"] = deepcopy(
                payload.get("eligible_horizons_for_separate_promotion_review", [])
            )
            decision["phase5_shadow_production_change_performed"] = False
            decision["phase5_shadow_changes_portfolio_action"] = False
    return watch


def _attach_elliott_swing_context(
    watch: dict[str, object],
    bundle_set: Mapping[str, object],
) -> dict[str, object]:
    raw_bundles = bundle_set.get("bundles")
    if not isinstance(raw_bundles, list):
        return watch
    context_by_symbol: dict[str, dict[str, object]] = {}
    for bundle in raw_bundles:
        if not isinstance(bundle, Mapping):
            continue
        packet = bundle.get("packet")
        symbol = str(packet.get("symbol") or "") if isinstance(packet, Mapping) else ""
        context = bundle.get("elliott_swing_context")
        if symbol and isinstance(context, Mapping):
            context_by_symbol[symbol] = deepcopy(dict(context))

    rows = watch.get("rows")
    if not isinstance(rows, list):
        return watch
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "")
        context = context_by_symbol.get(symbol)
        if context is None:
            continue
        row["elliott_swing_context"] = deepcopy(context)
        decision = row.get("decision")
        if isinstance(decision, dict):
            decision["elliott_source_output_id"] = context.get("source_output_id")
            decision["elliott_review_contexts"] = deepcopy(
                context.get("review_contexts", [])
            )
            decision["elliott_changed_universal_stance"] = False
            decision["elliott_review_contexts_are_actions"] = False
    return watch


def build_orchestrated_depot_watch(
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
    elliott_6h_source: Mapping[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    """Build the existing 7H Watch from the canonical archived Decision chain."""
    bundle_set, diagnostics = build_decision_bundle_set(
        daily,
        position_book,
        archive_packets,
        elliott_6h_source=elliott_6h_source,
    )
    decision_daily = decision_bound_daily_snapshot(daily, diagnostics["decision_as_of"])
    watch = build_depot_watch(decision_daily, position_book, bundle_set)
    watch = _attach_state_history_context(watch, bundle_set)
    watch = _attach_path_reviews(watch, bundle_set)
    watch = _attach_phase5_shadow(watch, bundle_set)
    watch = _attach_elliott_swing_context(watch, bundle_set)
    watch = seal_depot_watch(watch)
    return watch, diagnostics
