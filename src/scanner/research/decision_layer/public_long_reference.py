"""Public, portfolio-free long-state reference for pragmatic Depot-Watch transport.

The reference runs the canonical 7D->7H chain for every authoritative scanner
symbol under one explicit hypothetical position assumption: ``position_state``
is ``long`` while quantities, prices, P/L and add capacity remain missing.  It
contains no actual holdings and therefore may be persisted publicly.  A private
Depot Watch whose mapped positions have the same minimal long-only position
context can join against this reference without transferring the large 7A
archive.
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


def _hypothetical_long_book(daily: Mapping[str, object]) -> dict[str, object]:
    validated = validate_daily_research_snapshot(daily)
    generated_at = str(daily.get("generated_at") or "").strip()
    if not generated_at:
        raise ValueError("daily_generated_at_required_for_public_long_reference")
    snapshot_id = str(validated["snapshot_id"])
    symbols = validated["symbols"]
    assert isinstance(symbols, Mapping)
    source = f"public-hypothetical-long:{snapshot_id}"
    return {
        "schema_version": POSITION_BOOK_SCHEMA_VERSION,
        "source_snapshot_id": source,
        "as_of": generated_at,
        "positions": [
            {
                "schema_version": POSITION_SCHEMA_VERSION,
                "symbol": str(symbol),
                "source_snapshot_id": f"{source}:{symbol}",
                "as_of": generated_at,
                "position_state": "long",
            }
            for symbol in sorted(map(str, symbols.keys()))
        ],
    }


def build_public_long_reference(
    daily: Mapping[str, object],
    archive_packets: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Run the canonical Watch under a public hypothetical-long assumption."""
    validated = validate_daily_research_snapshot(daily)
    book = _hypothetical_long_book(daily)
    watch, diagnostics = build_orchestrated_depot_watch(
        validated,
        book,
        archive_packets,
        elliott_6h_source=None,
    )
    rows: list[dict[str, object]] = []
    for raw in watch.get("rows", []):
        if not isinstance(raw, Mapping):
            continue
        context = raw.get("daily_scanner_context")
        context = context if isinstance(context, Mapping) else {}
        decision = raw.get("decision")
        decision = decision if isinstance(decision, Mapping) else {}
        rows.append({
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
        })

    if len(rows) != int(validated.get("universe_size") or len(rows)):
        raise ValueError("public_long_reference_universe_mismatch")
    if any(row["availability"] != "decision_available" for row in rows):
        missing = [str(row["symbol"]) for row in rows if row["availability"] != "decision_available"]
        raise ValueError("public_long_reference_decision_missing:" + ",".join(missing))

    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": str(watch["source_snapshot_id"]),
        "decision_as_of": str(watch["as_of"]),
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
        "watch_summary": deepcopy(dict(watch.get("summary") or {})),
        "diagnostics": {
            "snapshot_id": diagnostics.get("snapshot_id"),
            "decision_as_of": diagnostics.get("decision_as_of"),
            "bundle_count": diagnostics.get("bundle_count"),
            "missing_current_packet_symbols": deepcopy(diagnostics.get("missing_current_packet_symbols", [])),
            "path_review_symbols": deepcopy(diagnostics.get("path_review_symbols", [])),
            "state_history_counts": deepcopy(diagnostics.get("state_history_counts", {})),
            "w8_changed_action_symbols": deepcopy(diagnostics.get("w8_changed_action_symbols", [])),
            "w8_conflict_symbols": deepcopy(diagnostics.get("w8_conflict_symbols", [])),
            "w8_reassessment_symbols": deepcopy(diagnostics.get("w8_reassessment_symbols", [])),
            "phase8_external_evidence_activated": diagnostics.get("phase8_external_evidence_activated"),
            "scanner_scalar_fallback_used": diagnostics.get("scanner_scalar_fallback_used"),
            "private_position_data_persisted": False,
        },
        "rows": rows,
        "semantics": {
            "canonical_7d_to_7h_chain_used": True,
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
