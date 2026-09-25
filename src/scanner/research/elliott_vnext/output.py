"""Final research-only module output for Elliott vNext Module 6H.

6H is an integration boundary, not a decision engine.  It assembles the causal
6A-6G research products into the frozen v2 module-output shape, preserves
missing evidence as missing, and recursively removes fields that could be
mistaken for autonomous trading instructions.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime
from hashlib import sha256
import json
import math
from typing import Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


FORBIDDEN_OUTPUT_KEYS = frozenset({"trade_decision", "order_instruction"})
REQUIRED_OUTPUT_FIELDS = (
    "symbol", "as_of", "timeframe", "degree", "primary_scenario",
    "alternative_scenarios", "pivots", "fibonacci", "current_wave_stage",
    "projection_zones", "wave_cycle_map", "hard_invalidations",
    "structural_fit", "confirmation_strength", "historical_expectancy",
    "routing_triggers", "swing_routing", "warnings",
)


class ModuleOutputError(ValueError):
    """Raised when a 6H input/output violates the frozen integration contract."""


def _iso_date(value: object, field: str) -> str:
    text = str(value or "")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ModuleOutputError(f"invalid_{field}") from exc


def _json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat() if value == value.normalize() else value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _strip_forbidden(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_forbidden(item)
            for key, item in value.items()
            if str(key) not in FORBIDDEN_OUTPUT_KEYS
        }
    if isinstance(value, list):
        return [_strip_forbidden(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_forbidden(item) for item in value]
    return value


def _forbidden_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if str(key) in FORBIDDEN_OUTPUT_KEYS:
                found.append(child)
            found.extend(_forbidden_paths(item, child))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{index}]"))
    return found


def _scenario_items(snapshot: Mapping[str, object]) -> list[tuple[str, Mapping[str, object]]]:
    result: list[tuple[str, Mapping[str, object]]] = []
    primary = snapshot.get("primary_scenario")
    if isinstance(primary, Mapping):
        result.append(("primary", primary))
    alternatives = snapshot.get("alternative_scenarios")
    if isinstance(alternatives, list):
        result.extend(
            (f"alternative_{index}", item)
            for index, item in enumerate(alternatives, start=1)
            if isinstance(item, Mapping)
        )
    return result


def _pivot_identity(pivot: Mapping[str, object]) -> tuple[object, ...]:
    return (
        str(pivot.get("pivot_time", "")),
        str(pivot.get("confirmed_time", "")),
        str(pivot.get("kind", "")),
        float(pivot.get("price")),
    )


def _canonical_pivots(
    snapshot: Mapping[str, object],
    source_pivots: Iterable[Mapping[str, object]] | None,
) -> tuple[list[dict[str, object]], bool]:
    """Return all supplied causal pivots, or a scenario-embedded fallback subset."""
    symbol = str(snapshot.get("symbol", ""))
    timeframe = str(snapshot.get("timeframe", ""))
    degree = str(snapshot.get("degree", ""))
    as_of = _iso_date(snapshot.get("as_of"), "as_of")
    candidates: list[Mapping[str, object]] = []
    fallback = source_pivots is None
    if source_pivots is not None:
        for pivot in source_pivots:
            if str(pivot.get("symbol", "")) != symbol:
                continue
            if str(pivot.get("timeframe", "")) != timeframe:
                continue
            if str(pivot.get("degree", "")) != degree:
                continue
            candidates.append(pivot)
    else:
        for _role, scenario in _scenario_items(snapshot):
            pivots = scenario.get("pivots")
            if isinstance(pivots, list):
                candidates.extend(item for item in pivots if isinstance(item, Mapping))

    dedup: dict[tuple[object, ...], dict[str, object]] = {}
    for raw in candidates:
        required = ("pivot_time", "confirmed_time", "price", "kind")
        if any(raw.get(field) in (None, "") for field in required):
            raise ModuleOutputError("pivot_required_fields_missing")
        confirmed = _iso_date(raw.get("confirmed_time"), "pivot_confirmed_time")
        pivot_time = _iso_date(raw.get("pivot_time"), "pivot_time")
        if confirmed > as_of:
            raise ModuleOutputError("future_confirmed_pivot_in_6h_output")
        if pivot_time > confirmed:
            raise ModuleOutputError("pivot_time_after_confirmation")
        kind = str(raw.get("kind"))
        if kind not in {"high", "low"}:
            raise ModuleOutputError("invalid_pivot_kind")
        try:
            price = float(raw.get("price"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ModuleOutputError("invalid_pivot_price") from exc
        if not math.isfinite(price) or price <= 0:
            raise ModuleOutputError("invalid_pivot_price")
        item = {
            "pivot_time": pivot_time,
            "confirmed_time": confirmed,
            "available_from": _iso_date(raw.get("available_from", confirmed), "pivot_available_from"),
            "price": price,
            "kind": kind,
        }
        for field in (
            "atr_at_pivot", "excursion_atr", "price_basis", "sequence_ambiguous",
            "left_bars", "right_bars", "atr_window", "min_excursion_atr",
            "parameter_validated",
        ):
            if field in raw:
                item[field] = _json_safe(raw.get(field))
        dedup[_pivot_identity(item)] = item
    return sorted(dedup.values(), key=lambda p: (p["confirmed_time"], p["pivot_time"], p["kind"])), fallback


def _primary_fibonacci(snapshot: Mapping[str, object]) -> dict[str, object]:
    primary = snapshot.get("primary_scenario")
    primary_id = str(primary.get("scenario_id", "")) if isinstance(primary, Mapping) else ""
    geometries = snapshot.get("fibonacci_geometry")
    if isinstance(geometries, list):
        for geometry in geometries:
            if not isinstance(geometry, Mapping) or str(geometry.get("scenario_id", "")) != primary_id:
                continue
            fib = geometry.get("fibonacci")
            if isinstance(fib, Mapping):
                return {
                    "anchor_start": _json_safe(fib.get("anchor_start", {})),
                    "anchor_end": _json_safe(fib.get("anchor_end", {})),
                    "zones": _json_safe(fib.get("zones", [])),
                    "scenario_id": primary_id or None,
                    "status": "available" if fib.get("anchor_start") and fib.get("anchor_end") else "anchors_unavailable",
                    "anchor_selection_by_fibonacci": bool(fib.get("anchor_selection_by_fibonacci", False)),
                }
    return {
        "anchor_start": {},
        "anchor_end": {},
        "zones": [],
        "scenario_id": primary_id or None,
        "status": "unavailable_for_primary_scenario",
        "anchor_selection_by_fibonacci": False,
    }


def _hard_invalidations(snapshot: Mapping[str, object]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for role, scenario in _scenario_items(snapshot):
        invalidations = scenario.get("hard_invalidations")
        if not isinstance(invalidations, list):
            continue
        for raw in invalidations:
            if not isinstance(raw, Mapping):
                continue
            records.append({
                "scenario_id": scenario.get("scenario_id"),
                "scenario_role": role,
                **dict(_json_safe(raw)),
            })
    return records


def _validate_validation_report(report: Mapping[str, object], as_of: str) -> None:
    if report.get("schema_version") != "elliott_vnext_validation_v1":
        raise ModuleOutputError("unsupported_6g_validation_schema")
    if report.get("research_only") is not True:
        raise ModuleOutputError("6g_validation_must_be_research_only")
    if report.get("automatic_promotion_allowed") is not False:
        raise ModuleOutputError("automatic_validation_promotion_forbidden")
    if report.get("technical_completion_is_empirical_validation") is not False:
        raise ModuleOutputError("technical_completion_must_not_equal_empirical_validation")
    frozen = _iso_date(report.get("rules_frozen_through"), "validation_rules_frozen_through")
    if frozen > as_of:
        raise ModuleOutputError("validation_freeze_after_output_as_of")


def _historical_expectancy(
    snapshot: Mapping[str, object],
    validation_report: Mapping[str, object] | None,
) -> dict[str, object] | None:
    if validation_report is None:
        return None
    as_of = _iso_date(snapshot.get("as_of"), "as_of")
    _validate_validation_report(validation_report, as_of)
    degree = str(snapshot.get("degree", ""))
    primary = snapshot.get("primary_scenario")
    stage = str(primary.get("stage", "")) if isinstance(primary, Mapping) else ""
    expected_wave = {
        "wave_2_complete": "wave_3",
        "wave_3_complete": "wave_4",
        "wave_4_complete": "wave_5",
    }.get(stage)

    projection_rows = [
        dict(_json_safe(row))
        for row in validation_report.get("projection_summary", [])
        if isinstance(row, Mapping)
        and str(row.get("degree", "")) == degree
        and (expected_wave is None or str(row.get("wave_role", "")) == expected_wave)
    ]
    route_rows = [
        dict(_json_safe(row))
        for row in validation_report.get("route_summary", [])
        if isinstance(row, Mapping)
        and str(row.get("degree", "")) == degree
        and str(row.get("wave_stage", "")) == stage
    ]
    formal_rows = sum(
        1 for row in [*projection_rows, *route_rows]
        if row.get("formal_promotion_evidence") is True
    )
    return {
        "status": validation_report.get("promotion_status"),
        "rules_frozen_through": validation_report.get("rules_frozen_through"),
        "current_degree": degree,
        "current_primary_stage": stage,
        "expected_next_wave_role": expected_wave,
        "projection_evidence": projection_rows,
        "route_review_evidence": route_rows,
        "formal_promotion_evidence_rows": formal_rows,
        "legacy_data_can_support_promotion": False,
        "automatic_promotion_allowed": False,
        "round_trip_pnl_evaluated": bool(validation_report.get("round_trip_pnl_evaluated", False)),
        "numeric_w5_levels_promoted": bool(validation_report.get("numeric_w5_levels_promoted", False)),
        "research_only": True,
    }


def _context_rows(value: object, *, as_of: str, symbol: str) -> dict[str, object] | None:
    if value is None:
        return None
    if isinstance(value, pd.DataFrame):
        raw_rows = value.to_dict(orient="records")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        raw_rows = list(value)
    else:
        raise ModuleOutputError("market_context_snapshot_must_be_dataframe_or_sequence")
    rows: list[dict[str, object]] = []
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise ModuleOutputError("invalid_market_context_row")
        row = dict(_json_safe(raw))
        if row.get("asset_symbol") not in (None, "", symbol):
            raise ModuleOutputError("market_context_symbol_mismatch")
        if row.get("as_of") not in (None, "") and _iso_date(row.get("as_of"), "context_as_of") > as_of:
            raise ModuleOutputError("future_market_context_snapshot")
        if row.get("assignment_pit_verified") is False and row.get("usable_as_real_market_evidence") is True:
            raise ModuleOutputError("unverified_context_marked_usable")
        rows.append(row)
    return {
        "status": "available" if rows else "not_available",
        "rows": rows,
        "eligible_real_evidence_count": sum(bool(row.get("usable_as_real_market_evidence")) for row in rows),
        "missing_context_stays_missing": True,
        "scanner_peer_fallback_used": False,
    }


def _relative_strength(value: Mapping[str, object] | None, *, as_of: str) -> dict[str, object] | None:
    if value is None:
        return None
    result = dict(_json_safe(value))
    available = result.get("available_from")
    if available not in (None, "") and _iso_date(available, "relative_strength_available_from") > as_of:
        raise ModuleOutputError("future_relative_strength_evidence")
    result["research_only"] = True
    return result


def validate_module_output(output: Mapping[str, object]) -> dict[str, object]:
    """Fail closed on the invariants required by the frozen v2 module schema."""
    missing = [field for field in REQUIRED_OUTPUT_FIELDS if field not in output]
    if missing:
        raise ModuleOutputError("missing_required_output_fields:" + ",".join(missing))
    forbidden = _forbidden_paths(output)
    if forbidden:
        raise ModuleOutputError("forbidden_output_fields:" + ",".join(forbidden))
    as_of = _iso_date(output.get("as_of"), "as_of")
    if output.get("research_only") is not True:
        raise ModuleOutputError("6h_output_must_be_research_only")
    if output.get("single_true_count_claimed") is not False:
        raise ModuleOutputError("single_true_count_claim_forbidden")
    if output.get("fibonacci_selects_wave_count") is not False:
        raise ModuleOutputError("fibonacci_must_not_select_wave_count")
    if output.get("routing_is_trade_decision") is not False:
        raise ModuleOutputError("routing_must_remain_review_only")
    if str(output.get("timeframe")) not in {"daily", "weekly"}:
        raise ModuleOutputError("unsupported_output_timeframe")
    for pivot in output.get("pivots", []):
        if not isinstance(pivot, Mapping):
            raise ModuleOutputError("invalid_output_pivot")
        if _iso_date(pivot.get("confirmed_time"), "pivot_confirmed_time") > as_of:
            raise ModuleOutputError("future_confirmed_pivot_in_output")
    for zone in output.get("projection_zones", []):
        if not isinstance(zone, Mapping):
            raise ModuleOutputError("invalid_projection_zone")
        if _iso_date(zone.get("available_from"), "projection_available_from") > as_of:
            raise ModuleOutputError("future_projection_in_output")
        low, high = zone.get("price_low"), zone.get("price_high")
        try:
            if not (math.isfinite(float(low)) and math.isfinite(float(high)) and float(low) < float(high)):
                raise ModuleOutputError("invalid_projection_zone_bounds")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ModuleOutputError("invalid_projection_zone_bounds") from exc
    for route in output.get("swing_routing", []):
        if not isinstance(route, Mapping):
            raise ModuleOutputError("invalid_swing_route")
        if route.get("actionability") != "review_only_not_trade_instruction":
            raise ModuleOutputError("swing_route_actionability_guard_failed")
        if _iso_date(route.get("available_from"), "route_available_from") > as_of:
            raise ModuleOutputError("future_route_in_output")
    integration = output.get("integration")
    if not isinstance(integration, Mapping) or integration.get("decision_layer_required") is not True:
        raise ModuleOutputError("global_decision_layer_requirement_missing")
    if integration.get("productive_integration_enabled") is not False:
        raise ModuleOutputError("productive_integration_must_remain_disabled")
    return dict(output)


def build_module_output(
    routed_snapshot: Mapping[str, object],
    *,
    source_pivots: Iterable[Mapping[str, object]] | None = None,
    validation_report: Mapping[str, object] | None = None,
    market_context_snapshot: object = None,
    relative_strength: Mapping[str, object] | None = None,
    cross_system_summary: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Assemble the stable 6H output without adding new predictive logic."""
    if routed_snapshot.get("research_only") is not True:
        raise ModuleOutputError("6h_requires_research_only_6d_snapshot")
    if routed_snapshot.get("fibonacci_used") is not True:
        raise ModuleOutputError("6h_requires_6c_fibonacci_geometry")
    if routed_snapshot.get("routing_is_trade_decision") is not False:
        raise ModuleOutputError("6h_requires_review_only_6d_routing")
    if routed_snapshot.get("single_true_count_claimed") is not False:
        raise ModuleOutputError("6h_requires_multi_scenario_contract")

    safe_snapshot = _strip_forbidden(_json_safe(deepcopy(dict(routed_snapshot))))
    symbol = str(safe_snapshot.get("symbol", "")).strip()
    if not symbol:
        raise ModuleOutputError("symbol_required")
    as_of = _iso_date(safe_snapshot.get("as_of"), "as_of")
    timeframe = str(safe_snapshot.get("timeframe", ""))
    degree = str(safe_snapshot.get("degree", ""))
    if not degree:
        raise ModuleOutputError("degree_required")

    pivots, fallback_pivots = _canonical_pivots(safe_snapshot, source_pivots)
    warnings = list(safe_snapshot.get("warnings", [])) if isinstance(safe_snapshot.get("warnings"), list) else []
    if fallback_pivots:
        warnings.append("top_level_pivots_limited_to_scenario_embedded_subset")

    expectancy = _historical_expectancy(safe_snapshot, validation_report)
    if expectancy is None:
        warnings.append("historical_expectancy_not_supplied")
    context = _context_rows(market_context_snapshot, as_of=as_of, symbol=symbol)
    if context is None:
        warnings.append("external_market_context_not_supplied")
    rs = _relative_strength(relative_strength, as_of=as_of)
    if rs is None:
        warnings.append("relative_strength_not_supplied_no_value_invented")
    warnings.extend((
        "structural_fit_not_calibrated_remains_null",
        "confirmation_strength_not_calibrated_remains_null",
    ))

    primary = safe_snapshot.get("primary_scenario") if isinstance(safe_snapshot.get("primary_scenario"), Mapping) else {}
    alternatives = safe_snapshot.get("alternative_scenarios") if isinstance(safe_snapshot.get("alternative_scenarios"), list) else []
    primary_id = str(primary.get("scenario_id", ""))
    validation_status = validation_report.get("promotion_status") if isinstance(validation_report, Mapping) else "validation_report_not_supplied"

    output: dict[str, object] = {
        "schema_version": "elliott_vnext_output_v2",
        "module": "6H_module_output",
        "symbol": symbol,
        "as_of": as_of,
        "timeframe": timeframe,
        "degree": degree,
        "selection_policy": safe_snapshot.get("selection_policy"),
        "single_true_count_claimed": False,
        "primary_scenario": primary,
        "alternative_scenarios": alternatives,
        "pivots": pivots,
        "fibonacci": _primary_fibonacci(safe_snapshot),
        "fibonacci_geometry": safe_snapshot.get("fibonacci_geometry", []),
        "fibonacci_selects_wave_count": False,
        "current_wave_stage": safe_snapshot.get("current_wave_stage", "uncertain"),
        "projection_zones": safe_snapshot.get("projection_zones", []),
        "wave_cycle_map": safe_snapshot.get("wave_cycle_map", {
            "current_stage": "uncertain", "next_expected_structures": ["uncertain"], "scenario_maps": []
        }),
        "hard_invalidations": _hard_invalidations(safe_snapshot),
        "rule_violations": list(primary.get("rule_violations", [])) if isinstance(primary, Mapping) else [],
        "structural_fit": None,
        "confirmation_strength": None,
        "historical_expectancy": expectancy,
        "routing_triggers": safe_snapshot.get("routing_triggers", []),
        "swing_routing": safe_snapshot.get("swing_routing", []),
        "routing_summary": safe_snapshot.get("routing_summary", {}),
        "routing_is_trade_decision": False,
        "market_context": context,
        "relative_strength": rs,
        "cross_system": dict(_json_safe(cross_system_summary)) if isinstance(cross_system_summary, Mapping) else None,
        "validation": {
            "status": validation_status,
            "technical_completion_is_empirical_validation": False,
            "automatic_promotion_allowed": False,
            "rules_frozen_through": validation_report.get("rules_frozen_through") if isinstance(validation_report, Mapping) else None,
            "evidence_policy": _json_safe(validation_report.get("evidence_policy")) if isinstance(validation_report, Mapping) else None,
        },
        "integration": {
            "contract_version": "elliott_vnext_integration_contract_v1",
            "decision_layer_required": True,
            "decision_authority": "future_global_decision_layer_or_orchestrating_depot_watch",
            "productive_integration_enabled": False,
            "direct_ordering_allowed": False,
            "review_contexts_are_not_actions": True,
            "technical_module_6_complete": True,
            "empirical_promotion_status": validation_status,
            "primary_scenario_id": primary_id or None,
        },
        "warnings": sorted(set(map(str, warnings))),
        "research_only": True,
    }
    output = dict(_strip_forbidden(_json_safe(output)))
    canonical = json.dumps(output, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    output["output_id"] = sha256(canonical.encode("utf-8")).hexdigest()
    validate_module_output(output)
    return output
