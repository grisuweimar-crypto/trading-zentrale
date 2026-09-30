"""Canonical research-only orchestration for the private Depot Watch.

The orchestrator never invents upstream evidence. It consumes validated archived
Phase-7A packets, derives the existing 7D->7G chain, injects the explicit private
position snapshot only at 7F, and finally delegates presentation to 7H.

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


def decision_bound_daily_snapshot(daily: Mapping[str, object]) -> dict[str, object]:
    """Return a 7H-facing copy bound to the exact snapshot publication time.

    ``daily_research_v1`` currently exposes a market-date ``as_of`` and an exact
    ``generated_at``. Phase-7A evidence needs an exact time because
    ``available_from`` must not be after packet time. 7H historically compared
    its bundle ``as_of`` as a raw string. Rather than falsifying evidence time or
    mutating the public artifact, orchestration supplies a defensive copy whose
    decision ``as_of`` is the authoritative ``generated_at`` while preserving
    the original market date in ``scanner_as_of_date``.
    """
    validated = validate_daily_research_snapshot(daily)
    generated_at = str(validated.get("generated_at") or "").strip()
    if not generated_at:
        # Backward-compatible path for tests/legacy snapshots that already carry
        # an exact timestamp in as_of.
        _utc(validated["as_of"], "daily_as_of")
        return validated
    _utc(generated_at, "daily_generated_at")
    out = deepcopy(validated)
    out["scanner_as_of_date"] = validated["as_of"]
    out["as_of"] = generated_at
    return out


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
    current_time = _utc(current_packet.get("as_of"), "current_packet_as_of")
    eligible: list[dict[str, object]] = []
    seen_snapshots: set[str] = set()
    for raw in packets:
        packet = validate_input_packet(raw)
        if str(packet.get("symbol")) != symbol:
            continue
        packet_time = _utc(packet.get("as_of"), "packet_as_of")
        if packet_time > current_time:
            continue
        snapshot_id = str(packet.get("source_snapshot_id") or "")
        if snapshot_id in seen_snapshots:
            raise DepotWatchOrchestrationError(f"duplicate_symbol_snapshot_history:{symbol}:{snapshot_id}")
        seen_snapshots.add(snapshot_id)
        eligible.append(packet)
    eligible.sort(key=lambda packet: _utc(packet["as_of"]))
    if not eligible or eligible[-1].get("source_snapshot_id") != current_packet.get("source_snapshot_id"):
        raise DepotWatchOrchestrationError(f"current_packet_not_latest_in_history:{symbol}")
    return [compute_universal_stance(packet) for packet in eligible]


def build_decision_bundle_set(
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    """Build 7A/7D/7E/7F/7G bundles for the positions with current packets."""
    decision_daily = decision_bound_daily_snapshot(daily)
    positions = validate_position_book(position_book, decision_as_of=decision_daily["as_of"])
    snapshot_id = str(decision_daily["snapshot_id"])
    decision_as_of = str(decision_daily["as_of"])

    bundles: list[dict[str, object]] = []
    missing_symbols: list[str] = []
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
        # Elliott is intentionally not reconstructed here. A future typed and
        # promoted 6H/Phase-8 bridge must pass its own explicit context contract.
        action = compute_portfolio_action(transition, position, swing_context=None)
        explanation = build_reliability_explanation(packet, stance, transition, action)
        bundles.append({
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "packet": packet,
            "stance": stance,
            "transition": transition,
            "action": action,
            "explanation": explanation,
        })

    bundle_set = {
        "schema_version": BUNDLE_SET_SCHEMA_VERSION,
        "bundles": bundles,
    }
    diagnostics = {
        "schema_version": "decision_depot_watch_orchestration_diagnostics_v1",
        "snapshot_id": snapshot_id,
        "decision_as_of": decision_as_of,
        "position_count": len(positions["positions"]),
        "bundle_count": len(bundles),
        "missing_current_packet_symbols": sorted(missing_symbols),
        "phase8_external_evidence_activated": False,
        "selection_direction_inferred": False,
        "scanner_scalar_fallback_used": False,
        "private_position_data_persisted": False,
    }
    return bundle_set, diagnostics


def build_orchestrated_depot_watch(
    daily: Mapping[str, object],
    position_book: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    """Build the existing 7H Watch from the canonical archived Decision chain."""
    decision_daily = decision_bound_daily_snapshot(daily)
    bundle_set, diagnostics = build_decision_bundle_set(
        daily,
        position_book,
        archive_packets,
    )
    watch = build_depot_watch(decision_daily, position_book, bundle_set)
    return watch, diagnostics
