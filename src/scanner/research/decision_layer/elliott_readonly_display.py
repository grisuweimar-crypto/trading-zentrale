"""Read-only Elliott vNext presentation for Decision Watch symbol views.

This module is presentation transport only.  It consumes the prospective
research-only 6H capture created outside the Decision Layer and exposes a
compact per-symbol view without creating a W10 source, changing Universal
Stance, changing Portfolio Action, or translating review contexts into actions.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Mapping, Sequence

from scanner.research.elliott_vnext.output import validate_module_output


CAPTURE_SCHEMA_VERSION = "elliott_vnext_prospective_capture_v1"
DISPLAY_SCHEMA_VERSION = "elliott_vnext_watch_display_v1"
AVAILABLE_STATUS = "available"
MISSING_STATUS = "prospective_capture_not_supplied"
STALE_STATUS = "prospective_capture_not_current_snapshot"
NO_SYMBOL_STATUS = "no_elliott_output_for_symbol"


class ElliottWatchDisplayError(ValueError):
    """Raised when supplied Elliott display evidence violates the read-only boundary."""


def _scenario_summary(value: object) -> dict[str, object] | None:
    if not isinstance(value, Mapping):
        return None
    fields = (
        "scenario_id",
        "family",
        "direction",
        "stage",
        "status",
        "available_from",
        "truncated_fifth",
        "diagonal",
        "correction_family",
    )
    return {field: deepcopy(value.get(field)) for field in fields if field in value}


def _route_summary(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    rows: list[dict[str, object]] = []
    allowed = (
        "trigger",
        "review_context",
        "reason",
        "scenario_id",
        "scenario_role",
        "available_from",
        "source",
        "requires_external_confirmation",
        "final_decision_owned_by_global_layer",
        "actionability",
        "research_only",
    )
    for raw in value:
        if not isinstance(raw, Mapping):
            continue
        rows.append({field: deepcopy(raw.get(field)) for field in allowed if field in raw})
    return rows


def _compact_validated_output(output: Mapping[str, object]) -> dict[str, object]:
    alternatives = output.get("alternative_scenarios")
    alternatives = alternatives if isinstance(alternatives, list) else []
    integration = output.get("integration")
    if not isinstance(integration, Mapping):
        raise ElliottWatchDisplayError("elliott_output_integration_missing")
    if integration.get("productive_integration_enabled") is not False:
        raise ElliottWatchDisplayError("elliott_productive_integration_must_remain_disabled")
    if integration.get("direct_ordering_allowed") is not False:
        raise ElliottWatchDisplayError("elliott_direct_ordering_must_remain_disabled")
    if output.get("routing_is_trade_decision") is not False:
        raise ElliottWatchDisplayError("elliott_routing_must_remain_review_only")
    return {
        "output_id": output.get("output_id"),
        "timeframe": output.get("timeframe"),
        "degree": output.get("degree"),
        "current_wave_stage": output.get("current_wave_stage"),
        "primary_scenario": _scenario_summary(output.get("primary_scenario")),
        "alternative_scenarios": [
            summary
            for summary in (_scenario_summary(item) for item in alternatives)
            if summary is not None
        ],
        "fibonacci": deepcopy(output.get("fibonacci")),
        "projection_zones": deepcopy(output.get("projection_zones", [])),
        "hard_invalidations": deepcopy(output.get("hard_invalidations", [])),
        "routing_triggers": deepcopy(output.get("routing_triggers", [])),
        "swing_routing": _route_summary(output.get("swing_routing")),
        "routing_summary": deepcopy(output.get("routing_summary", {})),
        "historical_expectancy": deepcopy(output.get("historical_expectancy")),
        "validation": deepcopy(output.get("validation", {})),
        "warnings": deepcopy(output.get("warnings", [])),
    }


def _compact_output(raw: Mapping[str, object]) -> dict[str, object]:
    return _compact_validated_output(validate_module_output(raw))


def validate_prospective_capture_for_display(
    value: Mapping[str, object],
) -> dict[str, object]:
    if value.get("schema_version") != CAPTURE_SCHEMA_VERSION:
        raise ElliottWatchDisplayError("unsupported_elliott_capture_schema")
    guards = value.get("guards")
    if not isinstance(guards, Mapping):
        raise ElliottWatchDisplayError("elliott_capture_guards_missing")
    required_false = (
        "productive_integration_enabled",
        "w10_source_emitted",
        "changes_universal_stance",
        "changes_portfolio_action",
        "direct_ordering_allowed",
        "future_rows_used",
        "frozen_elliott_core_modified",
    )
    for key in required_false:
        if guards.get(key) is not False:
            raise ElliottWatchDisplayError(f"elliott_capture_guard_invalid:{key}")
    if guards.get("research_only") is not True:
        raise ElliottWatchDisplayError("elliott_capture_must_be_research_only")
    if guards.get("missing_evidence_not_imputed") is not True:
        raise ElliottWatchDisplayError("elliott_missing_evidence_guard_invalid")

    snapshot_id = str(value.get("snapshot_id") or "").strip()
    as_of = str(value.get("as_of") or "").strip()
    capture_id = str(value.get("capture_id") or "").strip()
    if not snapshot_id or not as_of or not capture_id:
        raise ElliottWatchDisplayError("elliott_capture_identity_incomplete")

    raw_outputs = value.get("outputs")
    if not isinstance(raw_outputs, list):
        raise ElliottWatchDisplayError("elliott_capture_outputs_must_be_list")
    outputs: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw in raw_outputs:
        if not isinstance(raw, Mapping):
            raise ElliottWatchDisplayError("elliott_capture_output_must_be_object")
        output = validate_module_output(raw)
        if str(output.get("as_of") or "") != as_of:
            raise ElliottWatchDisplayError("elliott_capture_output_as_of_mismatch")
        key = (
            str(output.get("symbol") or ""),
            str(output.get("timeframe") or ""),
            str(output.get("degree") or ""),
        )
        if not all(key):
            raise ElliottWatchDisplayError("elliott_capture_output_identity_incomplete")
        if key in seen:
            raise ElliottWatchDisplayError("elliott_capture_duplicate_symbol_timeframe_degree")
        seen.add(key)
        outputs.append(output)

    if int(value.get("output_count") or 0) != len(outputs):
        raise ElliottWatchDisplayError("elliott_capture_output_count_mismatch")

    result = deepcopy(dict(value))
    result["outputs"] = outputs
    return result


def prepare_elliott_watch_display_capture(
    value: Mapping[str, object],
) -> dict[str, object]:
    """Validate and compact one prospective capture exactly once."""
    source = validate_prospective_capture_for_display(value)
    raw_outputs = source.get("outputs")
    assert isinstance(raw_outputs, list)
    outputs_by_symbol: dict[str, list[dict[str, object]]] = {}
    for output in raw_outputs:
        assert isinstance(output, Mapping)
        symbol = str(output.get("symbol") or "")
        outputs_by_symbol.setdefault(symbol, []).append(
            _compact_validated_output(output)
        )
    for symbol in outputs_by_symbol:
        outputs_by_symbol[symbol].sort(
            key=lambda row: (
                str(row.get("timeframe") or ""),
                str(row.get("degree") or ""),
            )
        )
    return {
        "source": source,
        "outputs_by_symbol": outputs_by_symbol,
    }


def build_prepared_elliott_watch_display(
    prepared: Mapping[str, object] | None,
    *,
    symbol: str,
    expected_snapshot_id: str,
    expected_as_of: str,
) -> dict[str, object]:
    """Build one display block from a capture validated once for the whole Watch."""
    symbol = str(symbol or "").strip()
    if not symbol:
        raise ElliottWatchDisplayError("elliott_display_symbol_required")
    if prepared is None:
        return _base(
            symbol=symbol,
            status=MISSING_STATUS,
            reason="no_prospective_elliott_capture_available_for_watch_display",
            source=None,
        )
    source = prepared.get("source")
    outputs_by_symbol = prepared.get("outputs_by_symbol")
    if not isinstance(source, Mapping) or not isinstance(outputs_by_symbol, Mapping):
        raise ElliottWatchDisplayError("elliott_prepared_capture_invalid")
    if (
        str(source.get("snapshot_id") or "") != str(expected_snapshot_id or "")
        or str(source.get("as_of") or "") != str(expected_as_of or "")
    ):
        return _base(
            symbol=symbol,
            status=STALE_STATUS,
            reason="prospective_elliott_capture_does_not_match_current_watch_snapshot",
            source=source,
        )
    outputs = deepcopy(list(outputs_by_symbol.get(symbol, [])))
    if not outputs:
        return _base(
            symbol=symbol,
            status=NO_SYMBOL_STATUS,
            reason="current_capture_contains_no_6h_output_for_symbol",
            source=source,
        )
    result = _base(
        symbol=symbol,
        status=AVAILABLE_STATUS,
        reason="current_prospective_6h_outputs_available_read_only",
        source=source,
    )
    result["outputs"] = outputs
    result["output_count"] = len(outputs)
    return result


def _base(
    *,
    symbol: str,
    status: str,
    reason: str,
    source: Mapping[str, object] | None,
) -> dict[str, object]:
    return {
        "schema_version": DISPLAY_SCHEMA_VERSION,
        "symbol": symbol,
        "status": status,
        "reason": reason,
        "source_capture_id": None if source is None else source.get("capture_id"),
        "source_snapshot_id": None if source is None else source.get("snapshot_id"),
        "source_as_of": None if source is None else source.get("as_of"),
        "source_run_id": None if source is None else source.get("run_id"),
        "source_publication_commit": None if source is None else source.get("source_publication_commit"),
        "scanner_published_at": None if source is None else source.get("scanner_published_at"),
        "captured_at": None if source is None else source.get("captured_at"),
        "validation_partition": None if source is None else source.get("validation_partition"),
        "outputs": [],
        "output_count": 0,
        "research_only": True,
        "read_only_presentation": True,
        "w10_source_emitted": False,
        "changes_universal_stance": False,
        "changes_portfolio_action": False,
        "direct_ordering_allowed": False,
        "review_contexts_are_actions": False,
        "decision_effect": "none",
    }


def build_elliott_watch_display(
    capture: Mapping[str, object] | None,
    *,
    symbol: str,
    expected_snapshot_id: str,
    expected_as_of: str,
) -> dict[str, object]:
    """Backward-compatible one-off wrapper for read-only display callers."""
    prepared = (
        None
        if capture is None
        else prepare_elliott_watch_display_capture(capture)
    )
    return build_prepared_elliott_watch_display(
        prepared,
        symbol=symbol,
        expected_snapshot_id=expected_snapshot_id,
        expected_as_of=expected_as_of,
    )

