"""BA-QM9 executable audit for the canonical Phase-7H Depot Watch.

The audit independently validates the four end-application inputs/outputs:

Research Snapshot + Decision Bundle + Depot Snapshot -> Wertpapierdepot Watch

It does not create a new action.  It checks that the existing 7H output is a
faithful, reproducible presentation of already-computed Decision state and the
explicitly supplied private position state.

No wall-clock Depot freshness threshold is invented.  Structural staleness
(snapshot/as-of mismatch or future data) is fail-closed; otherwise Depot age is
reported UNKNOWN until a separately governed freshness policy exists.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from scanner.research.decision_layer.depot_watch import (
    build_depot_watch,
    validate_bundle_set,
    validate_daily_research_snapshot,
    validate_decision_bundle,
    validate_depot_watch,
    validate_position_book,
)


SCHEMA_VERSION = "ba_qm9_watch_runtime_audit_v1"
EXPECTED_CHECKS = (
    "IDENTITY",
    "TIME",
    "COMPLETENESS",
    "POSITION_STATE",
    "ADD_CAPACITY",
    "ACTION_SEMANTICS",
    "MISSING_EVIDENCE",
    "STALE_DATA",
    "FAIL_CLOSED",
    "EXPLANATION",
    "REPRODUCIBILITY",
)


class BAQM9WatchAuditError(ValueError):
    pass


def _add_capacity(position: Mapping[str, Any]) -> str:
    can_add = position.get("can_add")
    remaining = position.get("remaining_adds")
    if can_add is False or remaining == 0:
        return "blocked"
    if can_add is True or (isinstance(remaining, int) and not isinstance(remaining, bool) and remaining > 0):
        return "available"
    return "unknown"


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise BAQM9WatchAuditError(code)


def _record(
    checks: dict[str, dict[str, Any]],
    check_id: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> None:
    checks[check_id] = {
        "passed": True,
        "details": dict(details or {}),
    }


def audit_depot_watch(
    daily_research: Mapping[str, Any],
    position_book: Mapping[str, Any],
    bundle_set: Mapping[str, Any],
    watch: Mapping[str, Any],
) -> dict[str, Any]:
    daily = validate_daily_research_snapshot(daily_research)
    validated_watch = validate_depot_watch(watch)
    positions = validate_position_book(
        position_book,
        decision_as_of=validated_watch["as_of"],
    )
    bundles = validate_bundle_set(bundle_set)

    _require(
        str(validated_watch.get("source_snapshot_id") or "")
        == str(daily.get("snapshot_id") or ""),
        "ba_qm9_watch_daily_snapshot_mismatch",
    )
    _require(
        str(validated_watch.get("as_of") or "") == str(daily.get("as_of") or ""),
        "ba_qm9_watch_daily_as_of_mismatch",
    )

    raw_rows = validated_watch.get("rows")
    raw_positions = positions.get("positions")
    _require(isinstance(raw_rows, list), "ba_qm9_watch_rows_required")
    _require(isinstance(raw_positions, list), "ba_qm9_positions_required")
    rows = list(raw_rows)
    position_rows = list(raw_positions)

    position_meta = validated_watch.get("position_book")
    _require(isinstance(position_meta, Mapping), "ba_qm9_watch_position_book_metadata_required")
    _require(
        str(position_meta.get("source_snapshot_id") or "")
        == str(positions.get("source_snapshot_id") or ""),
        "ba_qm9_position_book_identity_mismatch",
    )
    _require(
        str(position_meta.get("as_of") or "") == str(positions.get("as_of") or ""),
        "ba_qm9_position_book_as_of_mismatch",
    )
    _require(
        int(position_meta.get("position_count") or 0) == len(position_rows),
        "ba_qm9_position_book_count_mismatch",
    )

    _require(len(rows) == len(position_rows), "ba_qm9_one_watch_row_per_position_required")
    expected_symbols = [str(row.get("symbol") or "") for row in position_rows]
    actual_symbols = [str(row.get("symbol") or "") for row in rows]
    _require(actual_symbols == expected_symbols, "ba_qm9_watch_position_order_or_identity_mismatch")
    _require(len(set(actual_symbols)) == len(actual_symbols), "ba_qm9_duplicate_watch_symbol")

    checks: dict[str, dict[str, Any]] = {}

    _record(
        checks,
        "IDENTITY",
        details={
            "research_snapshot_id": daily["snapshot_id"],
            "watch_snapshot_id": validated_watch["source_snapshot_id"],
            "position_book_snapshot_id": positions["source_snapshot_id"],
            "exact_symbol_identity": True,
            "fuzzy_symbol_matching_used": False,
        },
    )

    _record(
        checks,
        "TIME",
        details={
            "watch_as_of": validated_watch["as_of"],
            "position_book_as_of": positions["as_of"],
            "future_position_data_blocked_by_validator": True,
            "bundle_as_of_requires_authoritative_watch_as_of": True,
        },
    )

    summary = validated_watch.get("summary")
    _require(isinstance(summary, Mapping), "ba_qm9_watch_summary_required")
    _require(
        int(summary.get("position_count") or 0) == len(position_rows),
        "ba_qm9_summary_position_count_mismatch",
    )
    available_count = sum(
        1 for row in rows if isinstance(row, Mapping) and row.get("availability") == "decision_available"
    )
    _require(
        int(summary.get("decision_available_count") or 0) == available_count,
        "ba_qm9_summary_available_count_mismatch",
    )
    _require(
        int(summary.get("unavailable_count") or 0) == len(rows) - available_count,
        "ba_qm9_summary_unavailable_count_mismatch",
    )
    _record(
        checks,
        "COMPLETENESS",
        details={
            "position_count": len(position_rows),
            "watch_row_count": len(rows),
            "decision_available_count": available_count,
            "unavailable_count": len(rows) - available_count,
        },
    )

    capacity_states: dict[str, str] = {}
    structural_stale_symbols: list[str] = []
    missing_decision_symbols: list[str] = []
    explanation_ids: dict[str, str] = {}
    action_states: dict[str, str] = {}

    for position, row in zip(position_rows, rows):
        _require(isinstance(position, Mapping), "ba_qm9_position_row_invalid")
        _require(isinstance(row, Mapping), "ba_qm9_watch_row_invalid")
        symbol = str(position.get("symbol") or "")
        _require(symbol == str(row.get("symbol") or ""), f"ba_qm9_symbol_mismatch:{symbol}")

        expected_capacity = _add_capacity(position)
        capacity_states[symbol] = expected_capacity
        availability = str(row.get("availability") or "")
        decision = row.get("decision")
        bundle_raw = bundles.get(symbol)

        row_position = row.get("position")
        _require(isinstance(row_position, Mapping), f"ba_qm9_row_position_required:{symbol}")
        for field in (
            "source_snapshot_id",
            "as_of",
            "position_state",
            "quantity",
            "market_value",
            "currency",
            "average_entry_price",
            "current_price",
            "remaining_adds",
        ):
            _require(
                row_position.get(field) == position.get(field),
                f"ba_qm9_position_state_mismatch:{symbol}:{field}",
            )

        if availability == "decision_available":
            _require(isinstance(bundle_raw, Mapping), f"ba_qm9_available_without_bundle:{symbol}")
            bundle = validate_decision_bundle(bundle_raw)
            packet = bundle["packet"]
            action = bundle["action"]
            explanation = bundle["explanation"]
            _require(isinstance(packet, Mapping), f"ba_qm9_packet_required:{symbol}")
            _require(isinstance(action, Mapping), f"ba_qm9_action_required:{symbol}")
            _require(isinstance(explanation, Mapping), f"ba_qm9_explanation_required:{symbol}")

            _require(
                str(packet.get("source_snapshot_id") or "") == str(daily["snapshot_id"]),
                f"ba_qm9_bundle_snapshot_mismatch:{symbol}",
            )
            _require(
                str(packet.get("as_of") or "") == str(daily["as_of"]),
                f"ba_qm9_bundle_as_of_mismatch:{symbol}",
            )

            action_position = action.get("position_context")
            _require(
                isinstance(action_position, Mapping),
                f"ba_qm9_action_position_context_required:{symbol}",
            )
            _require(
                action_position.get("add_capacity_state") == expected_capacity,
                f"ba_qm9_add_capacity_mismatch:{symbol}",
            )
            _require(
                row_position.get("add_capacity_state") == expected_capacity,
                f"ba_qm9_watch_add_capacity_mismatch:{symbol}",
            )

            action_row = action.get("portfolio_action")
            _require(isinstance(action_row, Mapping), f"ba_qm9_portfolio_action_required:{symbol}")
            action_state = str(action_row.get("state") or "")
            action_states[symbol] = action_state
            _require(
                isinstance(decision, Mapping),
                f"ba_qm9_available_row_decision_required:{symbol}",
            )
            _require(
                str(decision.get("portfolio_action_state") or "") == action_state,
                f"ba_qm9_watch_changed_action_semantics:{symbol}",
            )
            _require(
                row.get("execution_allowed") is False,
                f"ba_qm9_execution_must_remain_disabled:{symbol}",
            )

            explanation_id = str(explanation.get("explanation_id") or "")
            _require(explanation_id, f"ba_qm9_explanation_id_required:{symbol}")
            _require(
                str(decision.get("explanation_id") or "") == explanation_id,
                f"ba_qm9_explanation_identity_mismatch:{symbol}",
            )
            explanation_ids[symbol] = explanation_id

            explanation_body = explanation.get("explanation")
            gaps = (
                explanation_body.get("missing_or_limited_evidence", [])
                if isinstance(explanation_body, Mapping)
                else []
            )
            _require(isinstance(gaps, list), f"ba_qm9_explanation_gaps_invalid:{symbol}")
            _require(
                int(decision.get("evidence_gap_count") or 0) == len(gaps),
                f"ba_qm9_evidence_gap_count_mismatch:{symbol}",
            )
            _require(
                decision.get("missing_or_limited_evidence") == gaps,
                f"ba_qm9_missing_evidence_visibility_mismatch:{symbol}",
            )
        else:
            missing_decision_symbols.append(symbol)
            _require(decision is None, f"ba_qm9_unavailable_row_exposes_decision:{symbol}")
            _require(
                row.get("presentation_group") == "unavailable",
                f"ba_qm9_unavailable_row_group_invalid:{symbol}",
            )
            if isinstance(bundle_raw, Mapping):
                packet = bundle_raw.get("packet")
                if isinstance(packet, Mapping):
                    if (
                        str(packet.get("source_snapshot_id") or "") != str(daily["snapshot_id"])
                        or str(packet.get("as_of") or "") != str(daily["as_of"])
                    ):
                        structural_stale_symbols.append(symbol)

    semantics = validated_watch.get("semantics")
    _require(isinstance(semantics, Mapping), "ba_qm9_watch_semantics_required")
    for key in (
        "scanner_scalar_replaced_missing_decision_evidence",
        "model_portfolio_used_as_actual_position_source",
        "legacy_holdings_used_as_actual_position_source",
        "fuzzy_symbol_matching_used",
        "universal_stance_recomputed",
        "transition_recomputed",
        "portfolio_action_changed",
        "broker_order_generated",
    ):
        _require(semantics.get(key) is False, f"ba_qm9_semantic_guard_violation:{key}")

    _record(
        checks,
        "POSITION_STATE",
        details={
            "supplied_position_snapshot_preserved": True,
            "model_or_legacy_position_substitution_used": False,
        },
    )
    _record(
        checks,
        "ADD_CAPACITY",
        details={
            "states": dict(sorted(capacity_states.items())),
            "unknown_is_not_available": True,
            "derived_only_from_private_position_input": True,
        },
    )
    _record(
        checks,
        "ACTION_SEMANTICS",
        details={
            "action_states": dict(sorted(action_states.items())),
            "watch_recomputed_action": False,
            "execution_allowed": False,
        },
    )
    _record(
        checks,
        "MISSING_EVIDENCE",
        details={
            "symbols_without_available_decision": sorted(missing_decision_symbols),
            "scanner_scalar_fallback_used": False,
            "missing_decision_neutralized": False,
        },
    )
    _record(
        checks,
        "STALE_DATA",
        details={
            "structural_stale_symbols": sorted(set(structural_stale_symbols)),
            "structural_stale_data_is_blocked": True,
            "depot_wall_clock_freshness_policy": "UNDEFINED",
            "depot_wall_clock_freshness_state": "UNKNOWN_NOT_CLAIMED_FRESH",
            "future_position_data_blocked": True,
        },
    )
    _record(
        checks,
        "FAIL_CLOSED",
        details={
            "unavailable_rows_expose_no_decision": True,
            "invalid_or_mismatched_decision_is_not_substituted": True,
            "execution_allowed": False,
        },
    )
    _record(
        checks,
        "EXPLANATION",
        details={
            "explanation_ids": dict(sorted(explanation_ids.items())),
            "missing_or_limited_evidence_preserved": True,
            "reliability_is_not_success_probability": True,
        },
    )

    rebuilt = build_depot_watch(daily, positions, bundle_set)
    _require(
        rebuilt == validated_watch,
        "ba_qm9_watch_not_deterministically_reproducible",
    )
    _record(
        checks,
        "REPRODUCIBILITY",
        details={
            "watch_id": validated_watch["watch_id"],
            "rebuilt_watch_id": rebuilt["watch_id"],
            "exact_rebuild_equal": True,
        },
    )

    _require(
        tuple(checks) == EXPECTED_CHECKS,
        "ba_qm9_runtime_check_coverage_incomplete",
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "PASSED",
        "source_snapshot_id": validated_watch["source_snapshot_id"],
        "watch_as_of": validated_watch["as_of"],
        "watch_id": validated_watch["watch_id"],
        "check_count": len(checks),
        "checks": checks,
        "all_required_checks_passed": True,
        "old_scanner_heuristic_fallback_used": False,
        "missing_evidence_treated_as_neutral": False,
        "depot_wall_clock_freshness_claimed": False,
        "investment_logic_changed": False,
        "execution_allowed": False,
        "empirical_promotion_performed": False,
    }
