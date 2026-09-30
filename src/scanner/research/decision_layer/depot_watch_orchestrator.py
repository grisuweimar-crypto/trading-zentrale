"""Canonical research-only orchestration for the private Depot Watch.

The orchestrator never invents upstream evidence. It consumes validated archived
Phase-7A packets, derives the existing 7D->7G chain, injects the explicit private
position snapshot only at 7F, and finally delegates presentation to 7H.

A scanner snapshot may have an early conservative 7A packet and a later final
same-snapshot evidence revision after Phase 2/3/4 finish. The archive keeps both
for auditability, while the operational chain uses only the latest revision of
each snapshot so one market observation can never count twice for hysteresis.

Phase-8 external evidence is deliberately absent here until a separately approved
production-integration change binds it into the canonical Decision Layer.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Mapping, Sequence

from .depot_watch import (
    BUNDLE_SCHEMA_VERSION,
    BUNDLE_SET_SCHEMA_VERSION,
    build_depot_watch,
    validate_daily_research_snapshot,
    validate_position_book,
)
from .input_contract import validate_input_packet
from .integrated_evidence import PATH_CONTEXT_TYPE
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
    """Return a 7H-facing copy bound to the actual Decision evidence time.

    The scanner's market date and publication timestamp remain preserved. A final
    Decision packet may legitimately become available later, after Phase 2/3/4
    finish. Binding 7H to that later timestamp avoids backdating evidence while
    retaining the exact scanner snapshot identity.
    """
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
            raise DepotWatchOrchestrationError(f"duplicate_symbol_snapshot_revision:{symbol}:{snapshot_id}")
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
        raise DepotWatchOrchestrationError(f"current_packet_not_latest_in_history:{symbol}")
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
        raise DepotWatchOrchestrationError(f"duplicate_path_context:{packet.get('symbol')}")
    return matches[0] if matches else None


def build_decision_bundle_set(
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
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
    positions = validate_position_book(position_book, decision_as_of=decision_daily["as_of"])

    bundles: list[dict[str, object]] = []
    missing_symbols: list[str] = []
    path_review_symbols: list[str] = []
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
        history = _stance_history(
            archive_packets,
            symbol=symbol,
            current_packet=packet,
        )
        stance = history[-1]
        transition = build_state_transition_history(history)
        # Elliott remains a separate typed context and is never faked from scanner
        # history. The scanner path review is carried alongside the frozen 7F
        # action, not passed off as Elliott and not allowed to change 7D direction.
        action = compute_portfolio_action(transition, position, swing_context=None)
        explanation = build_reliability_explanation(packet, stance, transition, action)
        path_context = _path_review(packet)
        if path_context and path_context.get("review_state") == "profit_protection_review":
            path_review_symbols.append(symbol)
        bundles.append({
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "packet": packet,
            "stance": stance,
            "transition": transition,
            "action": action,
            "explanation": explanation,
            "path_review": path_context,
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
        "phase8_external_evidence_activated": False,
        "selection_direction_inferred": False,
        "scanner_scalar_fallback_used": False,
        "private_position_data_persisted": False,
    }
    return bundle_set, diagnostics


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
        if path.get("review_state") == "profit_protection_review":
            row["attention_required"] = True
        decision = row.get("decision")
        if isinstance(decision, dict):
            decision["path_review_state"] = path.get("review_state")
            decision["path_sequence_state"] = path.get("sequence_state")
            decision["path_review_is_trade_decision"] = False
    return watch


def build_orchestrated_depot_watch(
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    """Build the existing 7H Watch from the canonical archived Decision chain."""
    bundle_set, diagnostics = build_decision_bundle_set(
        daily,
        position_book,
        archive_packets,
    )
    decision_daily = decision_bound_daily_snapshot(daily, diagnostics["decision_as_of"])
    watch = build_depot_watch(decision_daily, position_book, bundle_set)
    watch = _attach_path_reviews(watch, bundle_set)
    return watch, diagnostics
