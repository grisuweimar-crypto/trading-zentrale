"""W6 adapter from Elliott-vNext 6H output into the frozen Phase-7F interface.

The adapter consumes an already-built 6H research object.  It does not run an
Elliott count, choose a scenario, infer direction, resolve 6D route conflicts or
create an order.  It only transports the already-existing review contexts to
7F, where portfolio-aware review logic already exists.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from typing import Mapping, Sequence


SOURCE_SET_SCHEMA = "decision_elliott_6h_source_v1"
PROSPECTIVE_CAPTURE_SCHEMA = "elliott_vnext_prospective_capture_v1"
ELLIOTT_SCHEMA = "elliott_vnext_output_v2"
ELLIOTT_MODULE = "6H_module_output"
ELLIOTT_CONTRACT = "elliott_vnext_integration_contract_v1"
SOURCE_NAME = "elliott_vnext_6h"

# W6 deliberately exposes only the action-relevant review contexts named in the
# Watch build plan.  6D's hold_review remains a valid Elliott research route but
# has no 7F action effect and is therefore omitted from the transported list.
W6_REVIEW_CONTEXTS = frozenset({
    "entry_or_add_review",
    "reentry_or_add_review",
    "partial_reduce_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
})
SIX_D_REVIEW_CONTEXTS = W6_REVIEW_CONTEXTS | {"hold_review"}
FORBIDDEN_TRADING_KEYS = frozenset({"trade_decision", "order_instruction"})
REQUIRED_6H_FIELDS = frozenset({
    "symbol",
    "as_of",
    "timeframe",
    "degree",
    "primary_scenario",
    "alternative_scenarios",
    "pivots",
    "fibonacci",
    "current_wave_stage",
    "projection_zones",
    "wave_cycle_map",
    "hard_invalidations",
    "structural_fit",
    "confirmation_strength",
    "historical_expectancy",
    "routing_triggers",
    "swing_routing",
    "warnings",
})


class Elliott6HAdapterError(ValueError):
    """Raised when 6H evidence cannot cross the W6 integration boundary."""


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise Elliott6HAdapterError(f"{field}_required")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Elliott6HAdapterError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Elliott6HAdapterError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _date(value: object, field: str) -> date:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise Elliott6HAdapterError(f"invalid_{field}") from exc


def _forbidden_paths(value: object, path: str = "$") -> list[str]:
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            child = f"{path}.{key}"
            if str(key) in FORBIDDEN_TRADING_KEYS:
                found.append(child)
            found.extend(_forbidden_paths(item, child))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{index}]"))
    return found


def validate_elliott_6h_output(value: Mapping[str, object]) -> dict[str, object]:
    """Validate the 6H boundary needed by W6 without re-running Elliott."""
    if value.get("schema_version") != ELLIOTT_SCHEMA:
        raise Elliott6HAdapterError("unsupported_elliott_6h_schema")
    if value.get("module") != ELLIOTT_MODULE:
        raise Elliott6HAdapterError("elliott_6h_module_identity_invalid")
    missing = sorted(REQUIRED_6H_FIELDS - set(map(str, value.keys())))
    if missing:
        raise Elliott6HAdapterError("elliott_6h_required_fields_missing:" + ",".join(missing))
    if value.get("research_only") is not True:
        raise Elliott6HAdapterError("elliott_6h_must_remain_research_only")
    if value.get("routing_is_trade_decision") is not False:
        raise Elliott6HAdapterError("elliott_6h_routing_must_remain_review_only")
    if value.get("single_true_count_claimed") is not False:
        raise Elliott6HAdapterError("elliott_single_true_count_forbidden")
    if value.get("fibonacci_selects_wave_count") is not False:
        raise Elliott6HAdapterError("elliott_fibonacci_count_selection_forbidden")
    if not str(value.get("output_id") or "").strip():
        raise Elliott6HAdapterError("elliott_6h_output_id_required")
    if not str(value.get("symbol") or "").strip():
        raise Elliott6HAdapterError("elliott_6h_symbol_required")
    output_as_of = _date(value.get("as_of"), "elliott_6h_as_of")

    forbidden = _forbidden_paths(value)
    if forbidden:
        raise Elliott6HAdapterError("elliott_6h_forbidden_trading_fields:" + ",".join(forbidden))

    integration = value.get("integration")
    if not isinstance(integration, Mapping):
        raise Elliott6HAdapterError("elliott_6h_integration_required")
    if integration.get("contract_version") != ELLIOTT_CONTRACT:
        raise Elliott6HAdapterError("elliott_6h_integration_contract_invalid")
    if integration.get("decision_layer_required") is not True:
        raise Elliott6HAdapterError("elliott_decision_layer_requirement_missing")
    if integration.get("productive_integration_enabled") is not False:
        raise Elliott6HAdapterError("elliott_productive_integration_must_remain_disabled")
    if integration.get("direct_ordering_allowed") is not False:
        raise Elliott6HAdapterError("elliott_direct_ordering_must_remain_disabled")
    if integration.get("review_contexts_are_not_actions") is not True:
        raise Elliott6HAdapterError("elliott_review_context_guard_missing")

    routes = value.get("swing_routing")
    if not isinstance(routes, list):
        raise Elliott6HAdapterError("elliott_6h_swing_routing_must_be_list")
    for index, route in enumerate(routes):
        if not isinstance(route, Mapping):
            raise Elliott6HAdapterError(f"elliott_6h_invalid_swing_route:{index}")
        context = str(route.get("review_context") or "")
        if context not in SIX_D_REVIEW_CONTEXTS:
            raise Elliott6HAdapterError(f"elliott_6h_unknown_review_context:{context}")
        if route.get("actionability") != "review_only_not_trade_instruction":
            raise Elliott6HAdapterError(f"elliott_6h_route_actionability_invalid:{index}")
        route_date = _date(route.get("available_from"), "elliott_route_available_from")
        if route_date > output_as_of:
            raise Elliott6HAdapterError(f"elliott_6h_future_route:{index}")
        if route.get("final_decision_owned_by_global_layer") is False:
            raise Elliott6HAdapterError(f"elliott_6h_global_decision_guard_invalid:{index}")

    return deepcopy(dict(value))



def build_elliott_6h_source_from_prospective_capture(
    capture: Mapping[str, object],
    *,
    source_commit: str,
    available_from: str,
    expected_snapshot_id: str,
    expected_as_of: str,
    evidence_available_from: str | None = None,
) -> dict[str, object]:
    """Adapt one prospective Stage-1 capture to the existing W6 source envelope.

    This is an integration adapter only.  It does not select a wave degree,
    re-run Elliott, alter scenarios or infer direction.  All validated 6H
    outputs remain present so W6 can preserve cross-degree review conflicts.
    """
    if capture.get("schema_version") != PROSPECTIVE_CAPTURE_SCHEMA:
        raise Elliott6HAdapterError("unsupported_elliott_prospective_capture_schema")
    guards = capture.get("guards")
    if not isinstance(guards, Mapping):
        raise Elliott6HAdapterError("elliott_prospective_capture_guards_required")
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
            raise Elliott6HAdapterError(f"elliott_prospective_capture_guard_invalid:{key}")
    if guards.get("research_only") is not True:
        raise Elliott6HAdapterError("elliott_prospective_capture_must_be_research_only")
    if guards.get("missing_evidence_not_imputed") is not True:
        raise Elliott6HAdapterError("elliott_prospective_missing_evidence_guard_invalid")
    if guards.get("multi_degree_outputs_retained_without_reducer") is not True:
        raise Elliott6HAdapterError("elliott_prospective_multidegree_guard_invalid")

    snapshot_id = str(capture.get("snapshot_id") or "").strip()
    capture_as_of = str(capture.get("as_of") or "").strip()
    capture_id = str(capture.get("capture_id") or "").strip()
    if snapshot_id != str(expected_snapshot_id or "").strip():
        raise Elliott6HAdapterError("elliott_prospective_snapshot_mismatch")
    if capture_as_of != str(expected_as_of or "").strip():
        raise Elliott6HAdapterError("elliott_prospective_as_of_mismatch")
    if not capture_id:
        raise Elliott6HAdapterError("elliott_prospective_capture_id_required")

    commit = str(source_commit or "").strip()
    if len(commit) < 12:
        raise Elliott6HAdapterError("elliott_6h_source_commit_required")
    available = _timestamp(available_from, "elliott_6h_source_available_from")
    evidence_available = (
        _timestamp(evidence_available_from, "elliott_6h_evidence_available_from")
        if evidence_available_from is not None
        else None
    )
    if evidence_available is not None and evidence_available > available:
        raise Elliott6HAdapterError("elliott_source_before_evidence_availability")

    raw_outputs = capture.get("outputs")
    if not isinstance(raw_outputs, list):
        raise Elliott6HAdapterError("elliott_prospective_outputs_must_be_list")
    outputs: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw in raw_outputs:
        if not isinstance(raw, Mapping):
            raise Elliott6HAdapterError("elliott_prospective_output_must_be_object")
        output = validate_elliott_6h_output(raw)
        if str(output.get("as_of") or "") != capture_as_of:
            raise Elliott6HAdapterError("elliott_prospective_output_as_of_mismatch")
        key = (
            str(output.get("symbol") or ""),
            str(output.get("timeframe") or ""),
            str(output.get("degree") or ""),
        )
        if not all(key):
            raise Elliott6HAdapterError("elliott_prospective_output_identity_incomplete")
        if key in seen:
            raise Elliott6HAdapterError(
                "duplicate_elliott_6h_symbol_timeframe_degree:" + ":".join(key)
            )
        seen.add(key)
        outputs.append(output)

    outputs.sort(
        key=lambda row: (
            str(row.get("symbol") or ""),
            str(row.get("timeframe") or ""),
            str(row.get("degree") or ""),
            str(row.get("output_id") or ""),
        )
    )
    source: dict[str, object] = {
        "schema_version": SOURCE_SET_SCHEMA,
        "source_commit": commit,
        "available_from": available.isoformat(),
        "source_capture_id": capture_id,
        "snapshot_id": snapshot_id,
        "as_of": capture_as_of,
        "source_run_id": capture.get("run_id"),
        "source_publication_commit": capture.get("source_publication_commit"),
        "source_capture_available_from": capture.get("captured_at"),
        "source_evidence_available_from": (
            None if evidence_available is None else evidence_available.isoformat()
        ),
        "validation_partition": capture.get("validation_partition"),
        "source_hashes": (
            deepcopy(dict(capture.get("source_hashes")))
            if isinstance(capture.get("source_hashes"), Mapping)
            else None
        ),
        "validation_source": (
            deepcopy(dict(capture.get("validation_source")))
            if isinstance(capture.get("validation_source"), Mapping)
            else None
        ),
        "outputs": outputs,
        "output_count": len(outputs),
        "symbol_count": len({str(row.get("symbol") or "") for row in outputs}),
        "integration": {
            "stage": "stage3_w6_review_context",
            "research_only": True,
            "multi_degree_reducer_used": False,
            "all_available_degrees_retained": True,
            "elliott_direction_used_as_vote": False,
            "changes_universal_stance": False,
            "review_contexts_are_actions": False,
            "direct_ordering_allowed": False,
        },
    }
    return source


def index_elliott_6h_source(
    source: Mapping[str, object] | None,
    *,
    decision_as_of: str,
) -> tuple[dict[str, list[dict[str, object]]], dict[str, object]]:
    """Validate one optional W6 source envelope and group all 6H outputs by symbol."""
    if source is None:
        return {}, {
            "status": "not_supplied",
            "source_commit": None,
            "available_from": None,
            "source_capture_id": None,
            "output_count": 0,
            "symbol_count": 0,
        }
    if source.get("schema_version") != SOURCE_SET_SCHEMA:
        raise Elliott6HAdapterError("unsupported_elliott_6h_source_schema")
    source_commit = str(source.get("source_commit") or "").strip()
    if len(source_commit) < 12:
        raise Elliott6HAdapterError("elliott_6h_source_commit_required")
    available_text = str(source.get("available_from") or "").strip()
    available = _timestamp(available_text, "elliott_6h_source_available_from")
    decision_time = _timestamp(decision_as_of, "decision_as_of")
    if available > decision_time:
        raise Elliott6HAdapterError("future_elliott_6h_source")
    outputs = source.get("outputs")
    if not isinstance(outputs, list):
        raise Elliott6HAdapterError("elliott_6h_outputs_must_be_list")

    expressed_as_of = str(source.get("as_of") or "").strip()
    indexed: dict[str, list[dict[str, object]]] = {}
    identities: set[tuple[str, str, str]] = set()
    for raw in outputs:
        if not isinstance(raw, Mapping):
            raise Elliott6HAdapterError("elliott_6h_output_must_be_object")
        output = validate_elliott_6h_output(raw)
        symbol = str(output["symbol"])
        if _date(output["as_of"], "elliott_6h_as_of") > available.date():
            raise Elliott6HAdapterError(f"elliott_6h_output_after_source_availability:{symbol}")
        if expressed_as_of and str(output.get("as_of") or "") != expressed_as_of:
            raise Elliott6HAdapterError(f"elliott_6h_source_as_of_mismatch:{symbol}")
        identity = (
            symbol,
            str(output.get("timeframe") or ""),
            str(output.get("degree") or ""),
        )
        if identity in identities:
            raise Elliott6HAdapterError(
                "duplicate_elliott_6h_symbol_timeframe_degree:" + ":".join(identity)
            )
        identities.add(identity)
        indexed.setdefault(symbol, []).append(output)

    for symbol in indexed:
        indexed[symbol].sort(
            key=lambda row: (
                str(row.get("timeframe") or ""),
                str(row.get("degree") or ""),
                str(row.get("output_id") or ""),
            )
        )

    return indexed, {
        "status": "available",
        "source_commit": source_commit,
        "available_from": available.isoformat(),
        "source_capture_id": source.get("source_capture_id"),
        "snapshot_id": source.get("snapshot_id"),
        "source_run_id": source.get("source_run_id"),
        "source_publication_commit": source.get("source_publication_commit"),
        "source_capture_available_from": source.get("source_capture_available_from"),
        "source_evidence_available_from": source.get("source_evidence_available_from"),
        "validation_partition": source.get("validation_partition"),
        "source_hashes": (
            deepcopy(dict(source.get("source_hashes")))
            if isinstance(source.get("source_hashes"), Mapping)
            else None
        ),
        "validation_source": (
            deepcopy(dict(source.get("validation_source")))
            if isinstance(source.get("validation_source"), Mapping)
            else None
        ),
        "output_count": len(outputs),
        "symbol_count": len(indexed),
    }

def build_elliott_7f_multidegree_swing_context(
    outputs: Sequence[Mapping[str, object]],
    *,
    source_commit: str,
    source_available_from: str,
    source_provenance: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Aggregate existing review contexts across all available Elliott degrees.

    No degree is selected, weighted or interpreted as more truthful.  The
    existing 6D review contexts are unioned and any add/reduce disagreement is
    deliberately preserved for the frozen 7F conflict handling.
    """
    if not isinstance(outputs, Sequence) or isinstance(outputs, (str, bytes, bytearray)) or not outputs:
        raise Elliott6HAdapterError("elliott_6h_symbol_outputs_required")
    validated = [validate_elliott_6h_output(output) for output in outputs]
    symbols = {str(output.get("symbol") or "") for output in validated}
    as_of_dates = {str(output.get("as_of") or "") for output in validated}
    if len(symbols) != 1:
        raise Elliott6HAdapterError("elliott_multidegree_symbol_mismatch")
    if len(as_of_dates) != 1:
        raise Elliott6HAdapterError("elliott_multidegree_as_of_mismatch")

    output_ids = [str(output["output_id"]) for output in validated]
    if len(set(output_ids)) != len(output_ids):
        raise Elliott6HAdapterError("duplicate_elliott_6h_output_id")
    all_routes: list[Mapping[str, object]] = []
    timeframe_degrees: list[dict[str, object]] = []
    for output in validated:
        routes = output["swing_routing"]
        assert isinstance(routes, list)
        all_routes.extend(route for route in routes if isinstance(route, Mapping))
        feature_payloads = {
            "elliott_structure": {
                "pivots": output.get("pivots"),
                "primary_scenario": output.get("primary_scenario"),
                "alternative_scenarios": output.get("alternative_scenarios"),
                "current_wave_stage": output.get("current_wave_stage"),
                "hard_invalidations": output.get("hard_invalidations"),
            },
            "fibonacci_geometry": {
                "fibonacci": output.get("fibonacci"),
                "fibonacci_geometry": output.get("fibonacci_geometry"),
                "projection_zones": output.get("projection_zones"),
            },
            "swing_routing": {
                "routing_triggers": output.get("routing_triggers"),
                "swing_routing": output.get("swing_routing"),
                "routing_summary": output.get("routing_summary"),
            },
        }
        lineage_features = {
            name: sha256(
                json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    default=str,
                ).encode("utf-8")
            ).hexdigest()
            for name, payload in feature_payloads.items()
        }
        timeframe_degrees.append({
            "output_id": str(output["output_id"]),
            "timeframe": str(output.get("timeframe") or ""),
            "degree": str(output.get("degree") or ""),
            "current_wave_stage": str(output.get("current_wave_stage") or ""),
            "primary_scenario_id": (
                output.get("integration", {}).get("primary_scenario_id")
                if isinstance(output.get("integration"), Mapping)
                else None
            ),
            "lineage_features": lineage_features,
        })

    actionable = sorted({
        str(route["review_context"])
        for route in all_routes
        if str(route.get("review_context")) in W6_REVIEW_CONTEXTS
    })
    hold_count = sum(
        1 for route in all_routes if route.get("review_context") == "hold_review"
    )
    add_present = bool(
        set(actionable) & {"entry_or_add_review", "reentry_or_add_review"}
    )
    reduce_present = bool(
        set(actionable)
        & {
            "partial_reduce_review",
            "profit_protection_review",
            "larger_reduce_or_exit_review",
        }
    )
    canonical_ids = sorted(output_ids)
    aggregate_id = canonical_ids[0]
    if len(canonical_ids) > 1:
        digest = sha256(
            json.dumps(canonical_ids, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        aggregate_id = "w6-multidegree:" + digest

    timeframe_degrees.sort(
        key=lambda row: (
            str(row.get("timeframe") or ""),
            str(row.get("degree") or ""),
            str(row.get("output_id") or ""),
        )
    )
    return {
        "source": SOURCE_NAME,
        "source_output_id": aggregate_id,
        "as_of": _timestamp(
            source_available_from, "elliott_6h_source_available_from"
        ).isoformat(),
        "review_contexts": actionable,
        "routing_is_trade_decision": False,
        "research_only": True,
        "w6": {
            "source_commit": str(source_commit),
            "elliott_output_as_of": next(iter(as_of_dates)),
            "source_output_ids": canonical_ids,
            "output_count": len(validated),
            "timeframe_degrees": timeframe_degrees,
            "route_count": len(all_routes),
            "actionable_route_context_count": len(actionable),
            "hold_review_routes_omitted": hold_count,
            "add_reduce_conflict_preserved": add_present and reduce_present,
            "multi_degree_reducer_used": False,
            "all_available_degrees_aggregated": True,
            "direction_from_elliott_used": False,
            "stance_from_elliott_used": False,
            "review_contexts_are_actions": False,
            "changes_universal_stance": False,
            "source_provenance": (
                deepcopy(dict(source_provenance))
                if isinstance(source_provenance, Mapping)
                else None
            ),
        },
    }


def build_elliott_7f_swing_context(
    output: Mapping[str, object],
    *,
    source_commit: str,
    source_available_from: str,
) -> dict[str, object]:
    """Backward-compatible single-output wrapper around the multi-degree W6 path."""
    context = build_elliott_7f_multidegree_swing_context(
        [output],
        source_commit=source_commit,
        source_available_from=source_available_from,
    )
    validated = validate_elliott_6h_output(output)
    w6 = context["w6"]
    assert isinstance(w6, dict)
    w6["timeframe"] = str(validated.get("timeframe") or "")
    w6["degree"] = str(validated.get("degree") or "")
    return context

