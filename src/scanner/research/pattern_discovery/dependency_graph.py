"""Phase L6 Dependency Graph for Pattern Discovery Lab v2.

L6 measures structural and empirical dependency between immutable L5 PAT
versions. It is descriptive research infrastructure only: it does not alter L4
Discovery Evidence, mutate frozen patterns, confirm hypotheses, assign ratings,
or create directional / portfolio / execution authority.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
from itertools import combinations
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .candidate_registry import verify_freeze_snapshot


SCHEMA_VERSION = "pattern_discovery_l6_dependency_graph_v1"
EVENT_CONTEXT_SCHEMA_VERSION = "pattern_discovery_l6_event_context_v1"
GRAPH_SCHEMA_VERSION = "pattern_discovery_l6_graph_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l6_dependency_graph_v1.json"
)


class DependencyGraphError(ValueError):
    """Raised when an L6 dependency-graph invariant is violated."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise DependencyGraphError(f"value_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise DependencyGraphError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise DependencyGraphError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_dependency_graph_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DependencyGraphError(
            f"dependency_graph_contract_unreadable:{target}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != SCHEMA_VERSION:
        raise DependencyGraphError("dependency_graph_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise DependencyGraphError("dependency_graph_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise DependencyGraphError("dependency_graph_productive_integration_forbidden")
    if payload.get("execution_allowed") is not False:
        raise DependencyGraphError("dependency_graph_execution_forbidden")
    if payload.get("principles", {}).get("directional_authority_created") is not False:
        raise DependencyGraphError("dependency_graph_directional_authority_forbidden")
    return payload


def dependency_graph_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = dict(contract) if contract is not None else load_dependency_graph_contract()
    return _hash(value)


def _node_ref(pattern_id: str, pattern_version: str, pattern_spec_hash: str) -> dict[str, str]:
    return {
        "pattern_id": _text(pattern_id, "pattern_id"),
        "pattern_version": _text(pattern_version, "pattern_version"),
        "pattern_spec_hash": _text(pattern_spec_hash, "pattern_spec_hash"),
    }


def _node_key(ref: Mapping[str, Any]) -> str:
    return (
        f"{_text(ref.get('pattern_id'), 'pattern_id')}::"
        f"{_text(ref.get('pattern_version'), 'pattern_version')}::"
        f"{_text(ref.get('pattern_spec_hash'), 'pattern_spec_hash')}"
    )


def _node_id(ref: Mapping[str, Any]) -> str:
    return f"NODE-{_hash(_node_ref(ref['pattern_id'], ref['pattern_version'], ref['pattern_spec_hash']))[:24].upper()}"


def _normalize_context_value(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        result = value.strip()
        return result if result else None
    raise DependencyGraphError(f"context_value_must_be_string_or_null:{field}")


def _normalize_event(raw: Mapping[str, Any], *, index: int, pit_cutoff: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise DependencyGraphError(f"event_must_be_object:{index}")
    allowed = {"symbol", "as_of", "sector", "regime"}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise DependencyGraphError(
            f"event_unknown_fields:{index}:" + ",".join(unknown)
        )
    symbol = _text(raw.get("symbol"), f"events[{index}].symbol")
    as_of = _timestamp(raw.get("as_of"), f"events[{index}].as_of")
    if as_of > pit_cutoff:
        raise DependencyGraphError(f"event_after_pit_cutoff:{symbol}:{as_of}")
    sector = _normalize_context_value(raw.get("sector"), f"events[{index}].sector")
    regime = _normalize_context_value(raw.get("regime"), f"events[{index}].regime")
    event_id = f"EV-{_hash({'symbol': symbol, 'as_of': as_of})[:24].upper()}"
    return {
        "event_id": event_id,
        "symbol": symbol,
        "as_of": as_of,
        "sector": sector,
        "regime": regime,
    }


def build_event_context(
    pattern_events: Sequence[Mapping[str, Any]],
    *,
    source_id: str,
    pit_cutoff: str,
) -> dict[str, Any]:
    """Build a deterministic, explicit L6 event/context input.

    Each frozen PAT version must be represented by one pattern-event set. A set
    can be COMPLETE (including an explicitly empty event list) or UNAVAILABLE.
    Missing sector/regime values stay null and are never inferred from another
    pattern's event row.
    """
    if isinstance(pattern_events, (str, bytes, bytearray)) or not isinstance(
        pattern_events, Sequence
    ):
        raise DependencyGraphError("pattern_events_sequence_required")
    cutoff = _timestamp(pit_cutoff, "pit_cutoff")
    source = _text(source_id, "source_id")
    normalized: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()
    observed_context: dict[str, dict[str, set[str]]] = {}

    for p_index, raw in enumerate(pattern_events):
        if not isinstance(raw, Mapping):
            raise DependencyGraphError(f"pattern_event_set_must_be_object:{p_index}")
        allowed = {
            "pattern_id",
            "pattern_version",
            "pattern_spec_hash",
            "coverage_status",
            "events",
        }
        unknown = sorted(set(raw) - allowed)
        if unknown:
            raise DependencyGraphError(
                f"pattern_event_set_unknown_fields:{p_index}:" + ",".join(unknown)
            )
        ref = _node_ref(
            raw.get("pattern_id"),
            raw.get("pattern_version"),
            raw.get("pattern_spec_hash"),
        )
        key = _node_key(ref)
        if key in seen_nodes:
            raise DependencyGraphError(f"duplicate_pattern_event_set:{key}")
        seen_nodes.add(key)
        coverage = _text(raw.get("coverage_status"), f"pattern_events[{p_index}].coverage_status")
        if coverage not in {"COMPLETE", "UNAVAILABLE"}:
            raise DependencyGraphError(f"pattern_event_coverage_invalid:{coverage}")
        raw_events = raw.get("events")
        if not isinstance(raw_events, Sequence) or isinstance(raw_events, (str, bytes, bytearray)):
            raise DependencyGraphError(f"events_sequence_required:{key}")
        if coverage == "UNAVAILABLE" and len(raw_events) != 0:
            raise DependencyGraphError(f"unavailable_pattern_events_must_be_empty:{key}")

        events = [
            _normalize_event(event, index=e_index, pit_cutoff=cutoff)
            for e_index, event in enumerate(raw_events)
        ]
        events.sort(key=lambda item: (item["symbol"], item["as_of"], item["event_id"]))
        event_ids = [event["event_id"] for event in events]
        if len(event_ids) != len(set(event_ids)):
            raise DependencyGraphError(f"duplicate_event_in_pattern_set:{key}")

        for event in events:
            ctx = observed_context.setdefault(
                event["event_id"], {"sector": set(), "regime": set()}
            )
            if event["sector"] is not None:
                ctx["sector"].add(str(event["sector"]))
            if event["regime"] is not None:
                ctx["regime"].add(str(event["regime"]))
        normalized.append({**ref, "coverage_status": coverage, "events": events})

    for event_id, ctx in observed_context.items():
        if len(ctx["sector"]) > 1:
            raise DependencyGraphError(f"inconsistent_sector_for_same_event:{event_id}")
        if len(ctx["regime"]) > 1:
            raise DependencyGraphError(f"inconsistent_regime_for_same_event:{event_id}")

    normalized.sort(key=_node_key)
    context: dict[str, Any] = {
        "schema_version": EVENT_CONTEXT_SCHEMA_VERSION,
        "source_id": source,
        "pit_cutoff": cutoff,
        "pattern_event_sets": normalized,
    }
    context["event_context_hash"] = _hash(context)
    PatternDiscoveryBoundary().assert_research_payload(context)
    return context


def verify_event_context(context: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(context, Mapping):
        raise DependencyGraphError("event_context_must_be_object")
    if context.get("schema_version") != EVENT_CONTEXT_SCHEMA_VERSION:
        raise DependencyGraphError("event_context_schema_invalid")
    stored = _text(context.get("event_context_hash"), "event_context_hash")
    body = dict(context)
    body.pop("event_context_hash", None)
    if _hash(body) != stored:
        raise DependencyGraphError("event_context_hash_mismatch")

    rebuilt = build_event_context(
        [
            {
                "pattern_id": row["pattern_id"],
                "pattern_version": row["pattern_version"],
                "pattern_spec_hash": row["pattern_spec_hash"],
                "coverage_status": row["coverage_status"],
                "events": [
                    {
                        "symbol": event["symbol"],
                        "as_of": event["as_of"],
                        "sector": event.get("sector"),
                        "regime": event.get("regime"),
                    }
                    for event in row["events"]
                ],
            }
            for row in context.get("pattern_event_sets", [])
        ],
        source_id=context.get("source_id"),
        pit_cutoff=context.get("pit_cutoff"),
    )
    if rebuilt != dict(context):
        raise DependencyGraphError("event_context_noncanonical")
    return {
        "valid": True,
        "event_context_hash": stored,
        "pattern_event_set_count": len(context.get("pattern_event_sets", [])),
    }


def _condition_signature(condition: Mapping[str, Any]) -> str:
    required = (
        "feature_id",
        "feature_version",
        "feature_version_hash",
        "transformation_id",
        "transformation_version",
        "parameters",
        "state",
    )
    missing = [field for field in required if field not in condition]
    if missing:
        raise DependencyGraphError(
            "pattern_condition_fields_missing:" + ",".join(missing)
        )
    return _canonical_json({field: condition[field] for field in required})


def _forecast_signature(pattern_spec: Mapping[str, Any]) -> str:
    semantics = pattern_spec.get("semantics")
    forecast = pattern_spec.get("forecast")
    if not isinstance(semantics, Mapping) or not isinstance(forecast, Mapping):
        raise DependencyGraphError("pattern_spec_semantics_or_forecast_missing")
    required_forecast = (
        "target_id",
        "expected_direction",
        "horizon_sessions",
        "baseline",
    )
    missing = [field for field in required_forecast if field not in forecast]
    if missing:
        raise DependencyGraphError(
            "pattern_forecast_fields_missing:" + ",".join(missing)
        )
    return _canonical_json(
        {
            "pattern_type": _text(semantics.get("pattern_type"), "pattern_type"),
            **{field: forecast[field] for field in required_forecast},
        }
    )


def _validate_and_collect_nodes(
    snapshots: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    if isinstance(snapshots, (str, bytes, bytearray)) or not isinstance(snapshots, Sequence):
        raise DependencyGraphError("l5_snapshots_sequence_required")
    if not snapshots:
        raise DependencyGraphError("at_least_one_l5_snapshot_required")

    nodes_by_key: dict[str, dict[str, Any]] = {}
    snapshot_hashes: list[str] = []
    for snapshot in snapshots:
        verified = verify_freeze_snapshot(snapshot)
        snapshot_hash = _text(verified["snapshot_hash"], "l5_snapshot_hash")
        snapshot_hashes.append(snapshot_hash)
        for record in snapshot.get("frozen_patterns", []):
            pattern_id = _text(record.get("pattern_id"), "pattern_id")
            pattern_version = _text(record.get("pattern_version"), "pattern_version")
            pattern_spec_hash = _text(record.get("pattern_spec_hash"), "pattern_spec_hash")
            pattern_spec = record.get("pattern_spec")
            if not isinstance(pattern_spec, Mapping):
                raise DependencyGraphError("pattern_spec_missing")
            if _hash(pattern_spec) != pattern_spec_hash:
                raise DependencyGraphError(
                    f"pattern_spec_hash_mismatch:{pattern_id}:{pattern_version}"
                )
            identity = pattern_spec.get("identity")
            if not isinstance(identity, Mapping):
                raise DependencyGraphError("pattern_spec_identity_missing")
            if identity.get("pattern_id") != pattern_id:
                raise DependencyGraphError("pattern_spec_pattern_id_mismatch")
            if identity.get("pattern_version") != pattern_version:
                raise DependencyGraphError("pattern_spec_pattern_version_mismatch")

            semantics = pattern_spec.get("semantics")
            forecast = pattern_spec.get("forecast")
            if not isinstance(semantics, Mapping) or not isinstance(forecast, Mapping):
                raise DependencyGraphError("pattern_spec_semantics_or_forecast_missing")
            conditions = semantics.get("conditions")
            if not isinstance(conditions, Sequence) or isinstance(conditions, (str, bytes, bytearray)):
                raise DependencyGraphError("pattern_conditions_sequence_required")
            condition_signatures = sorted(_condition_signature(c) for c in conditions)
            feature_ids = sorted({_text(c.get("feature_id"), "feature_id") for c in conditions})
            forecast_signature = _forecast_signature(pattern_spec)

            ref = _node_ref(pattern_id, pattern_version, pattern_spec_hash)
            key = _node_key(ref)
            node = {
                "node_id": _node_id(ref),
                **ref,
                "frozen_record_hash": _text(record.get("frozen_record_hash"), "frozen_record_hash"),
                "discovery_run_id": _text(record.get("discovery_run_id"), "discovery_run_id"),
                "feature_library_version": _text(record.get("feature_library_version"), "feature_library_version"),
                "horizon_sessions": int(forecast["horizon_sessions"]),
                "condition_count": len(condition_signatures),
                "feature_ids": feature_ids,
                "condition_signatures": condition_signatures,
                "forecast_signature": forecast_signature,
            }
            previous = nodes_by_key.get(key)
            if previous is not None and previous != node:
                raise DependencyGraphError(f"conflicting_duplicate_pattern_node:{key}")
            nodes_by_key[key] = node

    nodes = [nodes_by_key[key] for key in sorted(nodes_by_key)]
    return nodes, sorted(set(snapshot_hashes))


def _set_overlap(left: set[str], right: set[str], *, status: str = "AVAILABLE") -> dict[str, Any]:
    if status != "AVAILABLE":
        return {
            "status": status,
            "left_count": len(left),
            "right_count": len(right),
            "shared_count": None,
            "jaccard": None,
        }
    union = left | right
    if not union:
        return {
            "status": "UNAVAILABLE",
            "left_count": len(left),
            "right_count": len(right),
            "shared_count": None,
            "jaccard": None,
        }
    shared = left & right
    return {
        "status": "AVAILABLE",
        "left_count": len(left),
        "right_count": len(right),
        "shared_count": len(shared),
        "jaccard": len(shared) / len(union),
    }


def _distribution_overlap(
    left_events: Sequence[Mapping[str, Any]],
    right_events: Sequence[Mapping[str, Any]],
    field: str,
) -> dict[str, Any]:
    left_values = [event.get(field) for event in left_events if event.get(field) is not None]
    right_values = [event.get(field) for event in right_events if event.get(field) is not None]
    left_total = len(left_events)
    right_total = len(right_events)
    left_nonmissing = len(left_values)
    right_nonmissing = len(right_values)
    if left_nonmissing == 0 or right_nonmissing == 0:
        return {
            "status": "UNAVAILABLE",
            "left_event_count": left_total,
            "right_event_count": right_total,
            "left_nonmissing_count": left_nonmissing,
            "right_nonmissing_count": right_nonmissing,
            "shared_categories": None,
            "jaccard": None,
            "distribution_overlap": None,
        }
    left_counts = Counter(str(value) for value in left_values)
    right_counts = Counter(str(value) for value in right_values)
    categories = set(left_counts) | set(right_counts)
    shared = set(left_counts) & set(right_counts)
    overlap = sum(
        min(left_counts[c] / left_nonmissing, right_counts[c] / right_nonmissing)
        for c in categories
    )
    status = (
        "AVAILABLE"
        if left_nonmissing == left_total and right_nonmissing == right_total
        else "PARTIAL"
    )
    return {
        "status": status,
        "left_event_count": left_total,
        "right_event_count": right_total,
        "left_nonmissing_count": left_nonmissing,
        "right_nonmissing_count": right_nonmissing,
        "shared_categories": len(shared),
        "jaccard": len(shared) / len(categories) if categories else None,
        "distribution_overlap": overlap,
    }


def _event_sets_by_node(context: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in context["pattern_event_sets"]:
        key = _node_key(row)
        out[key] = {
            "coverage_status": row["coverage_status"],
            "events": list(row["events"]),
        }
    return out


def _metric_value(component: Mapping[str, Any], field: str = "jaccard") -> float | None:
    value = component.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DependencyGraphError("dependency_metric_must_be_numeric_or_null")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise DependencyGraphError("dependency_metric_must_be_finite")
    return numeric


def _edge_for_pair(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    left_events: Mapping[str, Any],
    right_events: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    left_features = set(left["feature_ids"])
    right_features = set(right["feature_ids"])
    left_conditions = set(left["condition_signatures"])
    right_conditions = set(right["condition_signatures"])

    feature_overlap = _set_overlap(left_features, right_features)
    condition_overlap = _set_overlap(left_conditions, right_conditions)
    same_forecast_scope = left["forecast_signature"] == right["forecast_signature"]
    duplicate = same_forecast_scope and left_conditions == right_conditions
    left_nested = same_forecast_scope and left_conditions < right_conditions
    right_nested = same_forecast_scope and right_conditions < left_conditions

    empirical_available = (
        left_events["coverage_status"] == "COMPLETE"
        and right_events["coverage_status"] == "COMPLETE"
    )
    if empirical_available:
        l_events = list(left_events["events"])
        r_events = list(right_events["events"])
        event_overlap = _set_overlap(
            {str(event["event_id"]) for event in l_events},
            {str(event["event_id"]) for event in r_events},
        )
        temporal_overlap = _set_overlap(
            {str(event["as_of"]) for event in l_events},
            {str(event["as_of"]) for event in r_events},
        )
        symbol_overlap = _set_overlap(
            {str(event["symbol"]) for event in l_events},
            {str(event["symbol"]) for event in r_events},
        )
        sector_overlap = _distribution_overlap(l_events, r_events, "sector")
        regime_overlap = _distribution_overlap(l_events, r_events, "regime")
    else:
        l_events = list(left_events["events"])
        r_events = list(right_events["events"])
        event_overlap = _set_overlap(set(), set(), status="UNAVAILABLE")
        temporal_overlap = _set_overlap(set(), set(), status="UNAVAILABLE")
        symbol_overlap = _set_overlap(set(), set(), status="UNAVAILABLE")
        sector_overlap = {
            "status": "UNAVAILABLE",
            "left_event_count": len(l_events),
            "right_event_count": len(r_events),
            "left_nonmissing_count": 0,
            "right_nonmissing_count": 0,
            "shared_categories": None,
            "jaccard": None,
            "distribution_overlap": None,
        }
        regime_overlap = dict(sector_overlap)

    thresholds = contract["thresholds"]
    related_thresholds = thresholds["related"]
    triggers: list[str] = []
    relation = "NONE"
    nested_direction = None
    if duplicate:
        relation = "DUPLICATE"
        triggers.append("EXACT_STRUCTURAL_DUPLICATE")
    elif left_nested or right_nested:
        relation = "NESTED"
        nested_direction = (
            "SOURCE_CONDITIONS_SUBSET_OF_TARGET"
            if left_nested
            else "TARGET_CONDITIONS_SUBSET_OF_SOURCE"
        )
        triggers.append("STRICT_CONDITION_SUBSET_SAME_FORECAST_SCOPE")
    else:
        candidates = [
            ("FEATURE_OVERLAP", _metric_value(feature_overlap), float(related_thresholds["feature_jaccard_min"])),
            ("CONDITION_OVERLAP", _metric_value(condition_overlap), float(related_thresholds["condition_jaccard_min"])),
            ("EVENT_OVERLAP", _metric_value(event_overlap), float(related_thresholds["event_jaccard_min"])),
            ("TEMPORAL_OVERLAP", _metric_value(temporal_overlap), float(related_thresholds["temporal_jaccard_min"])),
            ("SYMBOL_OVERLAP", _metric_value(symbol_overlap), float(related_thresholds["symbol_jaccard_min"])),
            ("SECTOR_OVERLAP", _metric_value(sector_overlap, "distribution_overlap"), float(related_thresholds["sector_distribution_overlap_min"])),
            ("REGIME_OVERLAP", _metric_value(regime_overlap, "distribution_overlap"), float(related_thresholds["regime_distribution_overlap_min"])),
        ]
        for name, value, threshold in candidates:
            if value is not None and value >= threshold:
                triggers.append(name)
        if triggers:
            relation = "RELATED"

    component_values = [
        value
        for value in [
            _metric_value(feature_overlap),
            _metric_value(condition_overlap),
            _metric_value(event_overlap),
            _metric_value(temporal_overlap),
            _metric_value(symbol_overlap),
            _metric_value(sector_overlap, "distribution_overlap"),
            _metric_value(regime_overlap, "distribution_overlap"),
        ]
        if value is not None
    ]
    max_component = max(component_values) if component_values else 0.0
    if relation == "DUPLICATE":
        severity = "CRITICAL"
    elif relation == "NESTED":
        severity = "HIGH"
    elif relation == "RELATED" and max_component >= float(thresholds["severity"]["high_component_min"]):
        severity = "HIGH"
    elif relation == "RELATED":
        severity = "MEDIUM"
    elif max_component > 0:
        severity = "LOW"
    else:
        severity = "NONE"

    source_id, target_id = left["node_id"], right["node_id"]
    edge_basis = {"source_node_id": source_id, "target_node_id": target_id}
    edge = {
        "edge_id": f"EDGE-{_hash(edge_basis)[:24].upper()}",
        **edge_basis,
        "relationship": relation,
        "dependency_severity": severity,
        "nested_direction": nested_direction,
        "provenance": {
            "structural": {
                "same_forecast_scope": same_forecast_scope,
                "feature_overlap": feature_overlap,
                "condition_overlap": condition_overlap,
                "duplicate_criterion_met": duplicate,
                "nested_criterion_met": left_nested or right_nested,
            },
            "empirical": {
                "event_overlap": event_overlap,
                "temporal_overlap": temporal_overlap,
                "symbol_overlap": symbol_overlap,
                "sector_overlap": sector_overlap,
                "regime_overlap": regime_overlap,
            },
            "threshold_triggers": sorted(triggers),
            "single_similarity_score_used": False,
        },
    }
    return edge


def build_dependency_graph(
    l5_snapshots: Sequence[Mapping[str, Any]],
    event_context: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic pairwise graph over exact frozen PAT versions."""
    spec = dict(contract) if contract is not None else load_dependency_graph_contract()
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise DependencyGraphError("dependency_graph_contract_schema_invalid")
    verify_event_context(event_context)
    nodes, snapshot_hashes = _validate_and_collect_nodes(l5_snapshots)
    event_sets = _event_sets_by_node(event_context)

    node_keys = {_node_key(node) for node in nodes}
    context_keys = set(event_sets)
    missing = sorted(node_keys - context_keys)
    extra = sorted(context_keys - node_keys)
    if missing:
        raise DependencyGraphError("event_context_missing_pattern_nodes:" + ",".join(missing))
    if extra:
        raise DependencyGraphError("event_context_unknown_pattern_nodes:" + ",".join(extra))

    public_nodes = [
        {
            "node_id": node["node_id"],
            "pattern_id": node["pattern_id"],
            "pattern_version": node["pattern_version"],
            "pattern_spec_hash": node["pattern_spec_hash"],
            "frozen_record_hash": node["frozen_record_hash"],
            "discovery_run_id": node["discovery_run_id"],
            "feature_library_version": node["feature_library_version"],
            "horizon_sessions": node["horizon_sessions"],
            "condition_count": node["condition_count"],
        }
        for node in nodes
    ]
    public_nodes.sort(key=lambda item: item["node_id"])

    internal_by_node_id = {node["node_id"]: node for node in nodes}
    edges: list[dict[str, Any]] = []
    for source_public, target_public in combinations(public_nodes, 2):
        source = internal_by_node_id[source_public["node_id"]]
        target = internal_by_node_id[target_public["node_id"]]
        source_key = _node_key(source)
        target_key = _node_key(target)
        edges.append(
            _edge_for_pair(
                source,
                target,
                event_sets[source_key],
                event_sets[target_key],
                spec,
            )
        )
    edges.sort(key=lambda item: (item["source_node_id"], item["target_node_id"]))

    contract_hash = dependency_graph_contract_hash(spec)
    identity_basis = {
        "l6_contract_hash": contract_hash,
        "l5_snapshot_hashes": snapshot_hashes,
        "event_context_hash": event_context["event_context_hash"],
        "node_identities": [
            {
                "pattern_id": node["pattern_id"],
                "pattern_version": node["pattern_version"],
                "pattern_spec_hash": node["pattern_spec_hash"],
            }
            for node in public_nodes
        ],
    }
    graph_id = f"PDG-{_hash(identity_basis)[:24].upper()}"
    graph: dict[str, Any] = {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L6",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "graph_id": graph_id,
        "l6_contract_hash": contract_hash,
        "inputs": {
            "l5_snapshot_hashes": snapshot_hashes,
            "event_context_hash": event_context["event_context_hash"],
            "event_context_source_id": event_context["source_id"],
            "pit_cutoff": event_context["pit_cutoff"],
        },
        "nodes": public_nodes,
        "edges": edges,
        "counts": {
            "node_count": len(public_nodes),
            "evaluated_pair_count": len(edges),
            "duplicate_edge_count": sum(1 for edge in edges if edge["relationship"] == "DUPLICATE"),
            "nested_edge_count": sum(1 for edge in edges if edge["relationship"] == "NESTED"),
            "related_edge_count": sum(1 for edge in edges if edge["relationship"] == "RELATED"),
            "high_or_critical_edge_count": sum(1 for edge in edges if edge["dependency_severity"] in {"HIGH", "CRITICAL"}),
        },
        "boundaries": {
            "directional_authority_created": False,
            "l4_discovery_evidence_mutated": False,
            "l5_pattern_versions_mutated": False,
            "prospective_capture_started": False,
            "confirmation_evaluation_performed": False,
            "rating_assigned": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(graph)
    graph["graph_hash"] = _hash(graph)
    return graph


def verify_dependency_graph(graph: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(graph, Mapping):
        raise DependencyGraphError("dependency_graph_must_be_object")
    if graph.get("schema_version") != GRAPH_SCHEMA_VERSION:
        raise DependencyGraphError("dependency_graph_schema_invalid")
    if graph.get("research_only") is not True:
        raise DependencyGraphError("dependency_graph_research_only_guard_missing")
    if graph.get("productive_integration_enabled") is not False:
        raise DependencyGraphError("dependency_graph_productive_integration_forbidden")
    if graph.get("execution_allowed") is not False:
        raise DependencyGraphError("dependency_graph_execution_forbidden")
    boundaries = graph.get("boundaries")
    if not isinstance(boundaries, Mapping):
        raise DependencyGraphError("dependency_graph_boundaries_missing")
    if boundaries.get("directional_authority_created") is not False:
        raise DependencyGraphError("dependency_graph_directional_authority_forbidden")

    stored = _text(graph.get("graph_hash"), "graph_hash")
    body = dict(graph)
    body.pop("graph_hash", None)
    if _hash(body) != stored:
        raise DependencyGraphError("dependency_graph_hash_mismatch")

    nodes = graph.get("nodes")
    edges = graph.get("edges")
    if not isinstance(nodes, Sequence) or isinstance(nodes, (str, bytes, bytearray)):
        raise DependencyGraphError("dependency_graph_nodes_sequence_required")
    if not isinstance(edges, Sequence) or isinstance(edges, (str, bytes, bytearray)):
        raise DependencyGraphError("dependency_graph_edges_sequence_required")
    node_ids = [str(node.get("node_id")) for node in nodes if isinstance(node, Mapping)]
    if len(node_ids) != len(nodes) or len(node_ids) != len(set(node_ids)):
        raise DependencyGraphError("dependency_graph_node_identity_invalid")
    valid_nodes = set(node_ids)
    expected_pairs = len(nodes) * (len(nodes) - 1) // 2
    if len(edges) != expected_pairs:
        raise DependencyGraphError("dependency_graph_pair_count_invalid")
    seen_pairs: set[tuple[str, str]] = set()
    for edge in edges:
        if not isinstance(edge, Mapping):
            raise DependencyGraphError("dependency_graph_edge_must_be_object")
        source = str(edge.get("source_node_id") or "")
        target = str(edge.get("target_node_id") or "")
        if source not in valid_nodes or target not in valid_nodes or source >= target:
            raise DependencyGraphError("dependency_graph_edge_node_binding_invalid")
        pair = (source, target)
        if pair in seen_pairs:
            raise DependencyGraphError("dependency_graph_duplicate_pair")
        seen_pairs.add(pair)
        if edge.get("relationship") not in {"DUPLICATE", "NESTED", "RELATED", "NONE"}:
            raise DependencyGraphError("dependency_graph_relationship_invalid")
        if edge.get("dependency_severity") not in {"CRITICAL", "HIGH", "MEDIUM", "LOW", "NONE"}:
            raise DependencyGraphError("dependency_graph_severity_invalid")
        provenance = edge.get("provenance")
        if not isinstance(provenance, Mapping) or provenance.get("single_similarity_score_used") is not False:
            raise DependencyGraphError("dependency_graph_provenance_invalid")

    PatternDiscoveryBoundary().assert_research_payload(graph)
    return {
        "valid": True,
        "graph_id": graph.get("graph_id"),
        "graph_hash": stored,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def dependency_graph_repo_path(
    graph_id: str,
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = dict(contract) if contract is not None else load_dependency_graph_contract()
    return str(spec["identity"]["graph_path_template"]).format(
        graph_id=_text(graph_id, "graph_id")
    )


def write_dependency_graph(
    repo_root: str | Path,
    graph: Mapping[str, Any],
    *,
    boundary: PatternDiscoveryBoundary | None = None,
    contract: Mapping[str, Any] | None = None,
) -> Path:
    verify_dependency_graph(graph)
    spec = dict(contract) if contract is not None else load_dependency_graph_contract()
    guard = boundary or PatternDiscoveryBoundary()
    repo_path = dependency_graph_repo_path(str(graph["graph_id"]), contract=spec)
    guard.assert_write_path_allowed(repo_path)

    root = Path(repo_root).resolve()
    target = (root / repo_path).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise DependencyGraphError("dependency_graph_path_outside_repo") from exc
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        try:
            existing = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DependencyGraphError("existing_dependency_graph_unreadable") from exc
        verify_dependency_graph(existing)
        if existing == dict(graph):
            return target
        raise DependencyGraphError(f"dependency_graph_identity_collision:{repo_path}")

    with target.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(
            dict(graph),
            handle,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        handle.write("\n")
    return target
