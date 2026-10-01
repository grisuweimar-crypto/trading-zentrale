"""Public, portfolio-free long-state reference for pragmatic Depot-Watch transport.

The reference runs the canonical 7D->7H chain for every authoritative scanner
symbol under one explicit hypothetical position assumption: ``position_state``
is ``long`` while quantities, prices, P/L and add capacity remain missing.  It
contains no actual holdings and therefore may be persisted publicly.  A private
Depot Watch whose mapped positions have the same minimal long-only position
context can join against this reference without transferring the large 7A
archive.

The public reference is built symbol-by-symbol from a pre-indexed archive.  This
preserves the canonical per-symbol Decision chain while avoiding the quadratic
full-archive rescans that would otherwise make a 200+ symbol runtime needlessly
slow.
"""
from __future__ import annotations

from copy import deepcopy
import csv
from io import StringIO
from typing import Mapping, Sequence

from .depot_watch import validate_daily_research_snapshot
from .depot_watch_orchestrator import build_orchestrated_depot_watch


SCHEMA_VERSION = "decision_watch_public_long_reference_v1"
POSITION_SCHEMA_VERSION = "decision_position_snapshot_v1"
POSITION_BOOK_SCHEMA_VERSION = "decision_depot_position_book_v1"

DECISION_FIELDS = (
    "universal_stance_state",
    "universal_stance_direction",
    "transition_status",
    "stable_directional_anchor",
    "pending_direction",
    "pending_confirmation_count",
    "required_confirmation_count",
    "portfolio_action_state",
    "portfolio_action_reason_code",
    "reliability_assessment",
    "coverage_admission_state",
    "evidence_gap_count",
    "path_review_state",
    "path_sequence_state",
    "path_review_is_trade_decision",
    "path_review_suppressed_by_w8",
    "state_history_state",
    "state_history_sequence",
    "state_history_changed_universal_stance",
    "state_history_changed_portfolio_action",
    "state_history_w8_action_policy_evaluated",
    "w8_action_policy_case",
    "w8_action_policy_warning_code",
    "w8_action_policy_conflict",
    "w8_reassessment_required",
    "phase5_shadow_integration_mode",
    "phase5_shadow_status",
    "phase5_shadow_insufficient_evidence_horizons",
    "phase5_shadow_eligible_review_horizons",
    "phase5_shadow_production_change_performed",
    "phase5_shadow_changes_portfolio_action",
    "elliott_source_output_id",
    "elliott_review_contexts",
    "elliott_changed_universal_stance",
    "elliott_review_contexts_are_actions",
)
CURRENT_FIELDS = (
    "name", "score", "rank", "rank_percentile", "r_code", "rs3m",
    "trend200", "cycle", "confidence", "confidence_label", "close", "currency",
)
CSV_FIELDS = (
    "symbol", "name", "score", "r_code", "close", "currency",
    "availability", "presentation_group", "attention_required",
    "universal_stance_state", "universal_stance_direction",
    "transition_status", "stable_directional_anchor", "pending_direction",
    "portfolio_action_state", "portfolio_action_reason_code",
    "reliability_assessment", "coverage_admission_state", "evidence_gap_count",
    "state_history_state", "path_review_state", "path_review_suppressed_by_w8",
    "w8_action_policy_case", "w8_action_policy_warning_code",
    "w8_action_policy_conflict", "w8_reassessment_required",
    "phase5_shadow_integration_mode",
)


def _generated_at(daily: Mapping[str, object]) -> str:
    generated_at = str(daily.get("generated_at") or "").strip()
    if not generated_at:
        raise ValueError("daily_generated_at_required_for_public_long_reference")
    return generated_at


def _hypothetical_long_position(
    *,
    snapshot_id: str,
    symbol: str,
    as_of: str,
) -> dict[str, object]:
    source = f"public-hypothetical-long:{snapshot_id}"
    return {
        "schema_version": POSITION_SCHEMA_VERSION,
        "symbol": symbol,
        "source_snapshot_id": f"{source}:{symbol}",
        "as_of": as_of,
        "position_state": "long",
    }


def _single_symbol_inputs(
    validated_daily: Mapping[str, object],
    *,
    symbol: str,
    generated_at: str,
) -> tuple[dict[str, object], dict[str, object]]:
    symbols = validated_daily.get("symbols")
    if not isinstance(symbols, Mapping) or not isinstance(symbols.get(symbol), Mapping):
        raise ValueError(f"daily_symbol_missing_for_public_long_reference:{symbol}")
    snapshot_id = str(validated_daily["snapshot_id"])
    single_daily = deepcopy(dict(validated_daily))
    single_daily["symbols"] = {symbol: deepcopy(dict(symbols[symbol]))}
    single_daily["universe_size"] = 1
    source = f"public-hypothetical-long:{snapshot_id}"
    single_book = {
        "schema_version": POSITION_BOOK_SCHEMA_VERSION,
        "source_snapshot_id": source,
        "as_of": generated_at,
        "positions": [
            _hypothetical_long_position(
                snapshot_id=snapshot_id,
                symbol=symbol,
                as_of=generated_at,
            )
        ],
    }
    return single_daily, single_book


def _public_row(raw: Mapping[str, object]) -> dict[str, object]:
    context = raw.get("daily_scanner_context")
    context = context if isinstance(context, Mapping) else {}
    decision = raw.get("decision")
    decision = decision if isinstance(decision, Mapping) else {}
    return {
        "symbol": str(raw.get("symbol") or ""),
        "availability": raw.get("availability"),
        "presentation_group": raw.get("presentation_group"),
        "attention_required": raw.get("attention_required"),
        "current": {
            field: deepcopy(context.get(field))
            for field in CURRENT_FIELDS
            if field in context
        },
        "decision": {
            field: deepcopy(decision.get(field))
            for field in DECISION_FIELDS
            if field in decision
        },
    }


def _summary(rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    availability_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    reliability_counts: dict[str, int] = {}
    group_counts: dict[str, int] = {}
    attention_required_count = 0
    for row in rows:
        availability = str(row.get("availability") or "")
        group = str(row.get("presentation_group") or "")
        availability_counts[availability] = availability_counts.get(availability, 0) + 1
        group_counts[group] = group_counts.get(group, 0) + 1
        if row.get("attention_required") is True:
            attention_required_count += 1
        decision = row.get("decision")
        if isinstance(decision, Mapping):
            action = str(decision.get("portfolio_action_state") or "")
            reliability = str(decision.get("reliability_assessment") or "")
            if action:
                action_counts[action] = action_counts.get(action, 0) + 1
            if reliability:
                reliability_counts[reliability] = reliability_counts.get(reliability, 0) + 1
    return {
        "position_count": len(rows),
        "decision_available_count": availability_counts.get("decision_available", 0),
        "unavailable_count": len(rows) - availability_counts.get("decision_available", 0),
        "attention_required_count": attention_required_count,
        "availability_counts": dict(sorted(availability_counts.items())),
        "action_counts": dict(sorted(action_counts.items())),
        "reliability_counts": dict(sorted(reliability_counts.items())),
        "presentation_group_counts": dict(sorted(group_counts.items())),
        "unused_bundle_symbols": [],
    }


def build_public_long_reference(
    daily: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Run the canonical Watch under a public hypothetical-long assumption.

    Archive packets are indexed once by symbol.  Each symbol is then evaluated
    through the unchanged canonical orchestrator using only its own historical
    packet sequence and a one-symbol view of the same authoritative daily
    snapshot.  Decision semantics are per-symbol, so this removes redundant
    cross-symbol scans without changing 7D->7H logic.
    """
    validated = validate_daily_research_snapshot(daily)
    generated_at = _generated_at(daily)
    daily_symbols = validated.get("symbols")
    assert isinstance(daily_symbols, Mapping)

    packets_by_symbol: dict[str, list[Mapping[str, object]]] = {}
    for raw in archive_packets:
        if not isinstance(raw, Mapping):
            raise ValueError("archive_packet_must_be_object")
        symbol = str(raw.get("symbol") or "").strip()
        if not symbol:
            raise ValueError("archive_packet_symbol_required")
        packets_by_symbol.setdefault(symbol, []).append(raw)

    rows: list[dict[str, object]] = []
    decision_as_of: str | None = None
    path_review_symbols: list[str] = []
    missing_symbols: list[str] = []
    w8_changed_action_symbols: list[str] = []
    w8_conflict_symbols: list[str] = []
    w8_reassessment_symbols: list[str] = []
    state_history_counts: dict[str, int] = {}

    for symbol in sorted(map(str, daily_symbols.keys())):
        single_daily, single_book = _single_symbol_inputs(
            validated,
            symbol=symbol,
            generated_at=generated_at,
        )
        symbol_packets = packets_by_symbol.get(symbol, [])
        watch, diagnostics = build_orchestrated_depot_watch(
            single_daily,
            single_book,
            symbol_packets,
            elliott_6h_source=None,
        )
        raw_rows = watch.get("rows")
        if not isinstance(raw_rows, list) or len(raw_rows) != 1:
            raise ValueError(f"public_long_reference_row_count_invalid:{symbol}")
        row = _public_row(raw_rows[0])
        rows.append(row)

        current_decision_as_of = str(diagnostics.get("decision_as_of") or "")
        if not current_decision_as_of:
            raise ValueError(f"public_long_reference_decision_as_of_missing:{symbol}")
        if decision_as_of is None:
            decision_as_of = current_decision_as_of
        elif current_decision_as_of != decision_as_of:
            raise ValueError(
                f"public_long_reference_decision_as_of_mismatch:{symbol}:"
                f"{current_decision_as_of}:{decision_as_of}"
            )

        missing_symbols.extend(map(str, diagnostics.get("missing_current_packet_symbols") or []))
        path_review_symbols.extend(map(str, diagnostics.get("path_review_symbols") or []))
        w8_changed_action_symbols.extend(map(str, diagnostics.get("w8_changed_action_symbols") or []))
        w8_conflict_symbols.extend(map(str, diagnostics.get("w8_conflict_symbols") or []))
        w8_reassessment_symbols.extend(map(str, diagnostics.get("w8_reassessment_symbols") or []))
        raw_counts = diagnostics.get("state_history_counts")
        if isinstance(raw_counts, Mapping):
            for state, count in raw_counts.items():
                state_key = str(state)
                state_history_counts[state_key] = state_history_counts.get(state_key, 0) + int(count)
        if diagnostics.get("phase8_external_evidence_activated") is not False:
            raise ValueError("public_long_reference_phase8_effect_forbidden")
        if diagnostics.get("scanner_scalar_fallback_used") is not False:
            raise ValueError("public_long_reference_scalar_fallback_forbidden")
        if diagnostics.get("private_position_data_persisted") is not False:
            raise ValueError("public_long_reference_private_persistence_forbidden")

    if len(rows) != int(validated.get("universe_size") or len(rows)):
        raise ValueError("public_long_reference_universe_mismatch")
    if missing_symbols:
        raise ValueError("public_long_reference_decision_missing:" + ",".join(sorted(set(missing_symbols))))
    if any(row["availability"] != "decision_available" for row in rows):
        missing = [str(row["symbol"]) for row in rows if row["availability"] != "decision_available"]
        raise ValueError("public_long_reference_decision_unavailable:" + ",".join(missing))
    if decision_as_of is None:
        raise ValueError("public_long_reference_empty")

    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": str(validated["snapshot_id"]),
        "decision_as_of": decision_as_of,
        "row_count": len(rows),
        "position_assumption": {
            "position_state": "long",
            "quantity": None,
            "market_value": None,
            "currency": None,
            "average_entry_price": None,
            "current_price": None,
            "can_add": None,
            "remaining_adds": None,
            "hypothetical": True,
            "actual_holdings_included": False,
            "private_position_data_included": False,
        },
        "watch_summary": _summary(rows),
        "diagnostics": {
            "snapshot_id": str(validated["snapshot_id"]),
            "decision_as_of": decision_as_of,
            "bundle_count": len(rows),
            "missing_current_packet_symbols": [],
            "path_review_symbols": sorted(set(path_review_symbols)),
            "state_history_counts": dict(sorted(state_history_counts.items())),
            "w8_changed_action_symbols": sorted(set(w8_changed_action_symbols)),
            "w8_conflict_symbols": sorted(set(w8_conflict_symbols)),
            "w8_reassessment_symbols": sorted(set(w8_reassessment_symbols)),
            "phase8_external_evidence_activated": False,
            "scanner_scalar_fallback_used": False,
            "private_position_data_persisted": False,
        },
        "rows": rows,
        "semantics": {
            "canonical_7d_to_7h_chain_used": True,
            "archive_preindexed_by_symbol": True,
            "cross_symbol_evidence_used": False,
            "actual_holdings_included": False,
            "private_position_data_included": False,
            "decision_logic_changed": False,
            "reference_is_trade_instruction": False,
        },
    }


def public_long_reference_csv(reference: Mapping[str, object]) -> str:
    """Render one compact row per scanner symbol for external consumption."""
    if reference.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported_public_long_reference_schema")
    rows = reference.get("rows")
    if not isinstance(rows, list):
        raise ValueError("public_long_reference_rows_required")
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        current = raw.get("current")
        current = current if isinstance(current, Mapping) else {}
        decision = raw.get("decision")
        decision = decision if isinstance(decision, Mapping) else {}
        writer.writerow({
            "symbol": raw.get("symbol"),
            "name": current.get("name"),
            "score": current.get("score"),
            "r_code": current.get("r_code"),
            "close": current.get("close"),
            "currency": current.get("currency"),
            "availability": raw.get("availability"),
            "presentation_group": raw.get("presentation_group"),
            "attention_required": raw.get("attention_required"),
            **{field: decision.get(field) for field in CSV_FIELDS if field in DECISION_FIELDS},
        })
    return output.getvalue()
