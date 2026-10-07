"""Phase L11 Pattern Library / Research UI.

Read-only presentation over governed L5/L6/L9/L10 Pattern Discovery artifacts.
No evidence is recomputed, no rating is changed, and no promotion or productive
decision authority is created.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from html import escape
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .boundary import PatternDiscoveryBoundary
from .candidate_registry import verify_freeze_snapshot
from .confirmation_engine import verify_confirmation_look
from .dependency_graph import verify_dependency_graph
from .rating_engine import verify_rating_history

SCHEMA_VERSION = "pattern_discovery_l11_pattern_library_v1"
DEFAULT_CONTRACT_PATH = (
    Path(__file__).resolve().parents[4]
    / "configs"
    / "pattern_discovery"
    / "l11_pattern_library_v1.json"
)


class PatternLibraryError(ValueError):
    """Raised when an L11 invariant is violated."""


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return sha256(_canonical(value).encode("utf-8")).hexdigest()


def _text(value: Any, field: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise PatternLibraryError(f"value_required:{field}")
    return result


def _timestamp(value: Any, field: str) -> str:
    text = _text(value, field)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    if len(normalized) == 10:
        normalized += "T00:00:00+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise PatternLibraryError(f"invalid_timestamp:{field}") from exc
    if parsed.tzinfo is None:
        raise PatternLibraryError(f"timezone_required:{field}")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_pattern_library_contract(
    path: str | Path | None = None,
) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PatternLibraryError(
            f"pattern_library_contract_unreadable:{target}"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise PatternLibraryError("pattern_library_contract_schema_invalid")
    if payload.get("research_only") is not True:
        raise PatternLibraryError("pattern_library_must_be_research_only")
    if payload.get("productive_integration_enabled") is not False:
        raise PatternLibraryError(
            "pattern_library_productive_integration_forbidden"
        )
    if payload.get("execution_allowed") is not False:
        raise PatternLibraryError("pattern_library_execution_forbidden")
    principles = payload.get("principles") or {}
    for field in (
        "presentation_only",
        "discovery_and_prospective_evidence_visually_separate",
        "uncertainty_must_remain_visible",
        "horizons_never_aggregated",
        "missing_remains_missing",
        "rating_not_recomputed",
        "promotion_not_performed",
    ):
        if principles.get(field) is not True:
            raise PatternLibraryError(
                f"pattern_library_principle_missing:{field}"
            )
    return payload


def pattern_library_contract_hash(
    contract: Mapping[str, Any] | None = None,
) -> str:
    value = (
        dict(contract)
        if contract is not None
        else load_pattern_library_contract()
    )
    return _hash(value)


def _key(pattern_id: Any, version: Any, spec_hash: Any) -> str:
    return (
        f"{_text(pattern_id, 'pattern_id')}::"
        f"{_text(version, 'pattern_version')}::"
        f"{_text(spec_hash, 'pattern_spec_hash')}"
    )


def _evidence(
    raw: Mapping[str, Any] | None,
    *,
    discovery: bool,
) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    return {
        "raw_n": raw.get("raw_n"),
        "effective_n": (
            raw.get("effective_n_proxy")
            if discovery
            else raw.get("effective_n")
        ),
        "symbol_count": raw.get("symbol_count"),
        "observation_date_count": raw.get("observation_date_count"),
        "support_region_count": raw.get("support_region_count"),
        "direction_probability": raw.get("direction_probability"),
        "baseline_probability": raw.get("baseline_probability"),
        "probability_advantage_lift": raw.get(
            "probability_advantage_lift"
        ),
        "mean_aligned_outcome": raw.get("mean_aligned_outcome"),
        "median_aligned_outcome": raw.get("median_aligned_outcome"),
        "effect_size_vs_baseline": raw.get("effect_size_vs_baseline"),
        "robust_uncertainty": raw.get("robust_uncertainty"),
        "concentration": raw.get("concentration"),
        "regime_diagnostics": (
            raw.get("diagnostic_splits")
            if discovery
            else raw.get("regime_diagnostics")
        ),
        "context_splits": (
            None if discovery else raw.get("context_splits")
        ),
        "confirmation_period": raw.get("confirmation_period"),
        "open_blockers": list(raw.get("open_blockers") or []),
    }


def _ratings(
    histories: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for raw in histories:
        history = dict(raw)
        verify_rating_history(history)
        key = _key(
            history.get("pattern_id"),
            history.get("pattern_version"),
            history.get("pattern_spec_hash"),
        )
        if key in result:
            raise PatternLibraryError(f"duplicate_rating_history:{key}")
        result[key] = history
    return result


def _confirmations(
    reports: Sequence[Mapping[str, Any]],
) -> tuple[
    dict[str, Mapping[str, Any]],
    dict[str, list[dict[str, Any]]],
]:
    latest: dict[str, tuple[str, Mapping[str, Any]]] = {}
    history: dict[str, list[dict[str, Any]]] = {}
    seen: set[str] = set()

    for raw in reports:
        report = dict(raw)
        verify_confirmation_look(report)
        look_hash = _text(report.get("look_hash"), "l9.look_hash")
        if look_hash in seen:
            raise PatternLibraryError(f"duplicate_l9_look_hash:{look_hash}")
        seen.add(look_hash)
        evaluated = _timestamp(
            report.get("evaluated_at"), "l9.evaluated_at"
        )
        governance = report.get("qm_governance") or {}
        for result in report.get("pattern_results") or []:
            key = _key(
                result.get("pattern_id"),
                result.get("pattern_version"),
                result.get("pattern_spec_hash"),
            )
            item = {
                "confirmation_look_id": report.get(
                    "confirmation_look_id"
                ),
                "look_hash": look_hash,
                "evaluated_at": evaluated,
                "look_id": governance.get("look_id"),
                "monitoring_plan_id": governance.get(
                    "monitoring_plan_id"
                ),
                "monitoring_plan_version": governance.get(
                    "monitoring_plan_version"
                ),
                "is_final_look": governance.get("is_final_look"),
                "result_class": result.get("result_class"),
                "result_reasons": list(
                    result.get("result_reasons") or []
                ),
                "prospective_evidence": result.get(
                    "prospective_evidence"
                ),
            }
            history.setdefault(key, []).append(item)
            previous = latest.get(key)
            if previous is None or evaluated >= previous[0]:
                latest[key] = (evaluated, item)

    for rows in history.values():
        rows.sort(
            key=lambda row: (
                str(row.get("evaluated_at") or ""),
                str(row.get("confirmation_look_id") or ""),
            )
        )
    return (
        {key: pair[1] for key, pair in latest.items()},
        history,
    )


def _dependencies(
    graph: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    if graph is None:
        return {}
    verify_dependency_graph(graph)
    nodes = {
        str(node["node_id"]): node
        for node in graph.get("nodes") or []
    }
    rank = {
        "NONE": 0,
        "LOW": 1,
        "MEDIUM": 2,
        "HIGH": 3,
        "CRITICAL": 4,
    }
    result: dict[str, dict[str, Any]] = {}
    for node in nodes.values():
        key = _key(
            node.get("pattern_id"),
            node.get("pattern_version"),
            node.get("pattern_spec_hash"),
        )
        result[key] = {
            "graph_id": graph.get("graph_id"),
            "graph_hash": graph.get("graph_hash"),
            "highest_severity": "NONE",
            "relationship_count": 0,
            "relationships": [],
        }

    for edge in graph.get("edges") or []:
        source = nodes.get(str(edge.get("source_node_id")))
        target = nodes.get(str(edge.get("target_node_id")))
        if source is None or target is None:
            raise PatternLibraryError(
                "dependency_edge_references_unknown_node"
            )
        severity = str(edge.get("dependency_severity") or "NONE")
        if severity not in rank:
            raise PatternLibraryError(
                f"dependency_severity_unknown:{severity}"
            )
        for own, other in ((source, target), (target, source)):
            key = _key(
                own.get("pattern_id"),
                own.get("pattern_version"),
                own.get("pattern_spec_hash"),
            )
            summary = result[key]
            summary["relationship_count"] += 1
            if rank[severity] > rank[summary["highest_severity"]]:
                summary["highest_severity"] = severity
            summary["relationships"].append(
                {
                    "other_pattern_id": other.get("pattern_id"),
                    "other_pattern_version": other.get(
                        "pattern_version"
                    ),
                    "relationship": edge.get("relationship"),
                    "dependency_severity": severity,
                    "feature_overlap": edge.get("feature_overlap"),
                    "event_overlap": edge.get("event_overlap"),
                    "context_overlap": edge.get("context_overlap"),
                }
            )
    for summary in result.values():
        summary["relationships"].sort(
            key=lambda row: (
                -rank[str(row["dependency_severity"])],
                str(row.get("other_pattern_id") or ""),
            )
        )
    return result


def _last_change(history: Mapping[str, Any]) -> dict[str, Any]:
    transitions = list(history.get("transitions") or [])
    if not transitions:
        raise PatternLibraryError("rating_history_without_transition")
    last = transitions[-1]
    return {
        "observed_at": last.get("observed_at"),
        "previous_rating": last.get("previous_rating"),
        "new_rating": last.get("new_rating"),
        "reason_codes": list(last.get("reason_codes") or []),
        "transition_hash": last.get("transition_hash"),
    }


def _blockers(
    discovery: Mapping[str, Any],
    confirmation: Mapping[str, Any] | None,
) -> list[dict[str, str]]:
    values: dict[tuple[str, str], dict[str, str]] = {}
    for code in discovery.get("open_blockers") or []:
        item = {"code": str(code), "source": "L4_DISCOVERY"}
        values[(item["code"], item["source"])] = item
    if confirmation is None:
        item = {
            "code": "NO_PROSPECTIVE_CONFIRMATION_RESULT",
            "source": "L11_DERIVED",
        }
        values[(item["code"], item["source"])] = item
    else:
        for code in confirmation.get("result_reasons") or []:
            item = {"code": str(code), "source": "L9"}
            values[(item["code"], item["source"])] = item
    return [values[key] for key in sorted(values)]


def build_pattern_library(
    l5_snapshots: Sequence[Mapping[str, Any]],
    *,
    rating_histories: Sequence[Mapping[str, Any]],
    confirmation_reports: Sequence[Mapping[str, Any]] = (),
    dependency_graph: Mapping[str, Any] | None = None,
    generated_at: str,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the deterministic L11 presentation model."""
    spec = (
        dict(contract)
        if contract is not None
        else load_pattern_library_contract()
    )
    if (
        isinstance(l5_snapshots, (str, bytes, bytearray))
        or not isinstance(l5_snapshots, Sequence)
        or not l5_snapshots
    ):
        raise PatternLibraryError("l5_snapshots_required")

    snapshots: list[Mapping[str, Any]] = []
    frozen: list[Mapping[str, Any]] = []
    for snapshot in l5_snapshots:
        verify_freeze_snapshot(snapshot)
        snapshots.append(snapshot)
        frozen.extend(list(snapshot.get("frozen_patterns") or []))

    ratings = _ratings(rating_histories)
    latest_l9, l9_history = _confirmations(confirmation_reports)
    dependencies = _dependencies(dependency_graph)
    generated = _timestamp(generated_at, "generated_at")

    patterns: list[dict[str, Any]] = []
    known: set[str] = set()
    for pattern in frozen:
        key = _key(
            pattern.get("pattern_id"),
            pattern.get("pattern_version"),
            pattern.get("pattern_spec_hash"),
        )
        if key in known:
            raise PatternLibraryError(f"duplicate_frozen_pattern:{key}")
        known.add(key)
        rating_history = ratings.get(key)
        if rating_history is None:
            raise PatternLibraryError(
                f"missing_l10_rating_history:{key}"
            )

        pattern_spec = pattern.get("pattern_spec") or {}
        semantics = pattern_spec.get("semantics") or {}
        forecast = pattern_spec.get("forecast") or {}
        discovery_raw = pattern.get("discovery_evidence") or {}
        latest = latest_l9.get(key)
        prospective_raw = (
            latest.get("prospective_evidence")
            if latest is not None
            else None
        )
        dependency = dependencies.get(
            key,
            {
                "graph_id": None,
                "graph_hash": None,
                "highest_severity": "UNAVAILABLE",
                "relationship_count": 0,
                "relationships": [],
            },
        )
        patterns.append(
            {
                "pattern_id": pattern.get("pattern_id"),
                "pattern_version": pattern.get("pattern_version"),
                "pattern_spec_hash": pattern.get("pattern_spec_hash"),
                "description": (
                    pattern.get("natural_language_description")
                    or semantics.get("natural_language_description")
                ),
                "pattern_type": semantics.get("pattern_type"),
                "direction": forecast.get("expected_direction"),
                "target_id": forecast.get("target_id"),
                "horizon_sessions": forecast.get("horizon_sessions"),
                "baseline_definition": forecast.get("baseline"),
                "rating": rating_history.get("current_rating"),
                "rating_history_hash": rating_history.get("history_hash"),
                "latest_status_change": _last_change(rating_history),
                "discovery_evidence": _evidence(
                    discovery_raw,
                    discovery=True,
                ),
                "prospective_evidence": _evidence(
                    prospective_raw,
                    discovery=False,
                ),
                "latest_confirmation": {
                    "result_class": (
                        latest.get("result_class")
                        if latest is not None
                        else None
                    ),
                    "result_reasons": (
                        list(latest.get("result_reasons") or [])
                        if latest is not None
                        else []
                    ),
                    "evaluated_at": (
                        latest.get("evaluated_at")
                        if latest is not None
                        else None
                    ),
                    "look_id": (
                        latest.get("look_id")
                        if latest is not None
                        else None
                    ),
                    "is_final_look": (
                        latest.get("is_final_look")
                        if latest is not None
                        else None
                    ),
                },
                "confirmation_history": l9_history.get(key, []),
                "dependency": dependency,
                "open_blockers": _blockers(discovery_raw, latest),
                "provenance": {
                    "frozen_record_hash": pattern.get(
                        "frozen_record_hash"
                    ),
                    "l4_evidence_hash": pattern.get("l4_evidence_hash"),
                    "discovery_run_id": pattern.get("discovery_run_id"),
                    "freeze_timestamp": pattern.get("freeze_timestamp"),
                    "data_cutoff": pattern.get("data_cutoff"),
                    "code_version": pattern.get("code_version"),
                    "universe_version": pattern.get("universe_version"),
                    "feature_library_version": pattern.get(
                        "feature_library_version"
                    ),
                },
            }
        )

    for source_name, source_keys in (
        ("l10", set(ratings)),
        ("l9", set(latest_l9)),
        ("l6", set(dependencies)),
    ):
        unknown = sorted(source_keys - known)
        if unknown:
            raise PatternLibraryError(
                f"{source_name}_references_unknown_l5_pattern:"
                + ",".join(unknown)
            )

    patterns.sort(
        key=lambda row: (
            str(row.get("pattern_id") or ""),
            str(row.get("pattern_version") or ""),
            int(row.get("horizon_sessions") or 0),
        )
    )
    rating_counts: dict[str, int] = {}
    for pattern in patterns:
        rating = _text(pattern.get("rating"), "rating")
        if rating not in {"D", "C", "B", "A", "U", "F"}:
            raise PatternLibraryError(f"rating_invalid:{rating}")
        rating_counts[rating] = rating_counts.get(rating, 0) + 1

    library: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "module": "pattern_discovery_lab",
        "phase": "L11",
        "research_only": True,
        "productive_integration_enabled": False,
        "execution_allowed": False,
        "generated_at": generated,
        "l11_contract_hash": pattern_library_contract_hash(spec),
        "source_bindings": {
            "l5_snapshot_hashes": sorted(
                str(snapshot.get("snapshot_hash"))
                for snapshot in snapshots
            ),
            "l6_graph_hash": (
                dependency_graph.get("graph_hash")
                if dependency_graph is not None
                else None
            ),
            "l9_look_hashes": sorted(
                str(report.get("look_hash"))
                for report in confirmation_reports
            ),
            "l10_history_hashes": sorted(
                str(history.get("history_hash"))
                for history in rating_histories
            ),
        },
        "counts": {
            "pattern_count": len(patterns),
            "rating_counts": rating_counts,
        },
        "patterns": patterns,
        "boundaries": {
            "source_artifacts_mutated": False,
            "confirmation_recomputed": False,
            "rating_recomputed": False,
            "promotion_performed": False,
            "decision_layer_integration_performed": False,
            "portfolio_or_execution_effect_created": False,
        },
    }
    PatternDiscoveryBoundary().assert_research_payload(library)
    library["library_hash"] = _hash(library)
    return library


def verify_pattern_library(
    library: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_pattern_library_contract()
    )
    if not isinstance(library, Mapping):
        raise PatternLibraryError("pattern_library_must_be_object")
    if library.get("schema_version") != SCHEMA_VERSION:
        raise PatternLibraryError("pattern_library_schema_invalid")
    if library.get("research_only") is not True:
        raise PatternLibraryError(
            "pattern_library_research_only_guard_missing"
        )
    if library.get("productive_integration_enabled") is not False:
        raise PatternLibraryError(
            "pattern_library_productive_integration_forbidden"
        )
    if library.get("execution_allowed") is not False:
        raise PatternLibraryError("pattern_library_execution_forbidden")
    if (
        library.get("l11_contract_hash")
        != pattern_library_contract_hash(spec)
    ):
        raise PatternLibraryError(
            "pattern_library_contract_hash_mismatch"
        )
    patterns = library.get("patterns")
    if not isinstance(patterns, list):
        raise PatternLibraryError("pattern_library_patterns_required")
    if library.get("counts", {}).get("pattern_count") != len(patterns):
        raise PatternLibraryError("pattern_library_count_mismatch")
    seen: set[str] = set()
    for index, pattern in enumerate(patterns):
        if not isinstance(pattern, Mapping):
            raise PatternLibraryError(
                f"pattern_library_item_invalid:{index}"
            )
        key = _key(
            pattern.get("pattern_id"),
            pattern.get("pattern_version"),
            pattern.get("pattern_spec_hash"),
        )
        if key in seen:
            raise PatternLibraryError(
                f"pattern_library_duplicate_pattern:{key}"
            )
        seen.add(key)
        if pattern.get("rating") not in {"D", "C", "B", "A", "U", "F"}:
            raise PatternLibraryError(
                f"pattern_library_rating_invalid:{key}"
            )
        if pattern.get("horizon_sessions") is None:
            raise PatternLibraryError(
                f"pattern_library_horizon_missing:{key}"
            )

    for field in (
        "source_artifacts_mutated",
        "confirmation_recomputed",
        "rating_recomputed",
        "promotion_performed",
        "decision_layer_integration_performed",
        "portfolio_or_execution_effect_created",
    ):
        if library.get("boundaries", {}).get(field) is not False:
            raise PatternLibraryError(
                f"pattern_library_boundary_invalid:{field}"
            )

    stored = _text(library.get("library_hash"), "library_hash")
    body = dict(library)
    body.pop("library_hash", None)
    if _hash(body) != stored:
        raise PatternLibraryError("pattern_library_hash_mismatch")
    PatternDiscoveryBoundary().assert_research_payload(library)
    return {
        "valid": True,
        "pattern_count": len(patterns),
        "library_hash": stored,
    }


def pattern_library_json_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_pattern_library_contract()
    )
    return str(spec["storage"]["library_json_path"])


def pattern_library_html_repo_path(
    *,
    contract: Mapping[str, Any] | None = None,
) -> str:
    spec = (
        dict(contract)
        if contract is not None
        else load_pattern_library_contract()
    )
    return str(spec["storage"]["library_html_path"])


def _fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return str(value)


def _pct(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{100.0 * float(value):.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _interval(raw: Mapping[str, Any] | None, field: str) -> str:
    if not isinstance(raw, Mapping):
        return "—"
    value = raw.get(field)
    if not isinstance(value, list) or len(value) != 2:
        return "—"
    return f"[{_fmt(value[0])}, {_fmt(value[1])}]"


def _kv(rows: Sequence[tuple[str, Any]]) -> str:
    cells = []
    for key, value in rows:
        cells.append(
            f'<div class="k">{escape(str(key))}</div>'
            f'<div class="v">{escape(str(value if value is not None else "—"))}</div>'
        )
    return '<div class="kv">' + "".join(cells) + "</div>"


def _evidence_html(
    evidence: Mapping[str, Any] | None,
    *,
    title: str,
    css_class: str,
) -> str:
    if not isinstance(evidence, Mapping):
        body = '<div class="muted">Keine Evidenz vorhanden.</div>'
    else:
        uncertainty = evidence.get("robust_uncertainty")
        body = _kv(
            (
                ("N", evidence.get("raw_n")),
                ("Effective-N", evidence.get("effective_n")),
                ("Support-Regionen", evidence.get("support_region_count")),
                ("Symbole", evidence.get("symbol_count")),
                (
                    "Direction Probability",
                    _pct(evidence.get("direction_probability")),
                ),
                (
                    "Baseline",
                    _pct(evidence.get("baseline_probability")),
                ),
                (
                    "Lift",
                    _pct(evidence.get("probability_advantage_lift")),
                ),
                (
                    "Mean aligned outcome",
                    _fmt(evidence.get("mean_aligned_outcome")),
                ),
                (
                    "Effect size vs baseline",
                    _fmt(evidence.get("effect_size_vs_baseline")),
                ),
                (
                    "95% Effekt",
                    _interval(uncertainty, "aligned_effect_interval_95"),
                ),
                (
                    "95% Lift",
                    _interval(uncertainty, "probability_lift_interval_95"),
                ),
            )
        )
    return (
        f'<section class="card {css_class}">'
        f"<h3>{escape(title)}</h3>{body}</section>"
    )


def render_pattern_library_html(
    library: Mapping[str, Any],
) -> str:
    """Render a self-contained static L11 research page."""
    verify_pattern_library(library)
    rows: list[str] = []
    details: list[str] = []
    for pattern in library["patterns"]:
        p = pattern.get("prospective_evidence")
        latest = p if isinstance(p, Mapping) else {}
        uncertainty = latest.get("robust_uncertainty")
        dependency = pattern.get("dependency") or {}
        change = pattern.get("latest_status_change") or {}
        key = (
            f"{pattern['pattern_id']}-{pattern['pattern_version']}"
            .replace(" ", "-")
            .replace("/", "-")
        )
        rows.append(
            "<tr>"
            f'<td><a href="#{escape(key)}" class="mono">{escape(str(pattern["pattern_id"]))}</a>'
            f'<br><span class="muted">{escape(str(pattern["pattern_version"]))}</span></td>'
            f'<td class="desc">{escape(str(pattern.get("description") or "—"))}</td>'
            f'<td>{escape(str(pattern.get("pattern_type") or "—"))}</td>'
            f'<td>{escape(str(pattern.get("direction") or "—"))}</td>'
            f'<td class="mono">{escape(str(pattern.get("horizon_sessions")))}T</td>'
            f'<td><span class="rating r{escape(str(pattern["rating"]))}">{escape(str(pattern["rating"]))}</span></td>'
            f'<td class="right mono">{escape(_pct(latest.get("direction_probability")))}</td>'
            f'<td class="right mono">{escape(_pct(latest.get("baseline_probability")))}</td>'
            f'<td class="right mono">{escape(_pct(latest.get("probability_advantage_lift")))}</td>'
            f'<td class="right mono">{escape(_fmt(latest.get("effect_size_vs_baseline") if latest.get("effect_size_vs_baseline") is not None else latest.get("mean_aligned_outcome")))}</td>'
            f'<td class="right mono">{escape(str(latest.get("raw_n") if latest.get("raw_n") is not None else "—"))}</td>'
            f'<td class="right mono">{escape(str(latest.get("effective_n") if latest.get("effective_n") is not None else "—"))} / '
            f'{escape(str(latest.get("support_region_count") if latest.get("support_region_count") is not None else "—"))}</td>'
            f'<td class="mono small">Eff {_interval(uncertainty, "aligned_effect_interval_95")}<br>'
            f'Lift {_interval(uncertainty, "probability_lift_interval_95")}</td>'
            f'<td class="mono">{escape(str(dependency.get("highest_severity") or "UNAVAILABLE"))} '
            f'({escape(str(dependency.get("relationship_count") or 0))})</td>'
            f'<td class="mono small">{escape(str(change.get("observed_at") or "—"))}</td>'
            "</tr>"
        )

        blockers = "".join(
            f'<span class="blocker">{escape(str(item["code"]))} · {escape(str(item["source"]))}</span>'
            for item in pattern.get("open_blockers") or []
        ) or '<span class="muted">Keine offenen Blocker ausgewiesen.</span>'
        confirmation_history = "".join(
            "<li>"
            f'<span class="mono">{escape(str(item.get("evaluated_at") or "—"))}</span> · '
            f'{escape(str(item.get("look_id") or "—"))} · '
            f'<b>{escape(str(item.get("result_class") or "—"))}</b>'
            f'{" · final" if item.get("is_final_look") else ""}'
            "</li>"
            for item in pattern.get("confirmation_history") or []
        ) or "<li>Keine prospektive Confirmation vorhanden.</li>"
        relationships = "".join(
            "<li>"
            f'{escape(str(item.get("other_pattern_id") or "—"))} '
            f'{escape(str(item.get("other_pattern_version") or ""))} · '
            f'{escape(str(item.get("relationship") or "—"))} · '
            f'{escape(str(item.get("dependency_severity") or "—"))}'
            "</li>"
            for item in (dependency.get("relationships") or [])[:12]
        ) or "<li>Keine Dependency-Beziehung verfügbar.</li>"

        details.append(
            f'<article class="pattern" id="{escape(key)}">'
            f'<h2>{escape(str(pattern["pattern_id"]))} · {escape(str(pattern["pattern_version"]))} '
            f'<span class="rating r{escape(str(pattern["rating"]))}">{escape(str(pattern["rating"]))}</span></h2>'
            f'<p>{escape(str(pattern.get("description") or "—"))}</p>'
            '<div class="grid">'
            + _evidence_html(
                pattern.get("discovery_evidence"),
                title="Discovery Evidence",
                css_class="discovery",
            )
            + _evidence_html(
                pattern.get("prospective_evidence"),
                title="Prospective / Confirmed Evidence",
                css_class="prospective",
            )
            + '<section class="card"><h3>Pattern & Lifecycle</h3>'
            + _kv(
                (
                    ("Typ", pattern.get("pattern_type")),
                    ("Richtung", pattern.get("direction")),
                    ("Target", pattern.get("target_id")),
                    ("Horizont", f'{pattern.get("horizon_sessions")}T'),
                    ("Rating", pattern.get("rating")),
                    (
                        "letzte Statusänderung",
                        change.get("observed_at"),
                    ),
                    (
                        "Reason Codes",
                        ", ".join(change.get("reason_codes") or []),
                    ),
                    (
                        "letztes L9 Resultat",
                        pattern.get("latest_confirmation", {}).get(
                            "result_class"
                        ),
                    ),
                )
            )
            + "</section>"
            '<section class="card"><h3>Dependency / Blocker</h3>'
            f'<ul>{relationships}</ul><div>{blockers}</div></section>'
            '<section class="card"><h3>Confirmation History</h3>'
            f'<ul>{confirmation_history}</ul></section>'
            '<section class="card"><h3>Provenienz</h3>'
            + _kv(
                tuple(
                    (key_name, value)
                    for key_name, value in (
                        pattern.get("provenance") or {}
                    ).items()
                )
            )
            + "</section></div></article>"
        )

    if not rows:
        rows.append(
            '<tr><td colspan="15" class="empty">Keine Pattern vorhanden.</td></tr>'
        )
    counts = library.get("counts") or {}
    return """<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scanner_vNext – Pattern Library</title>
<style>
:root{--bg:#0b0f14;--card:#111827;--panel:#0f172a;--border:#263346;--text:#e5e7eb;--muted:#94a3b8;--good:#34d399;--warn:#fbbf24;--bad:#fb7185;--blue:#60a5fa;--violet:#c084fc;--mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;--sans:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(900px 500px at 12% 0,rgba(96,165,250,.12),transparent 55%),var(--bg);color:var(--text);font-family:var(--sans)}
header{position:sticky;top:0;z-index:10;background:rgba(11,15,20,.94);backdrop-filter:blur(10px);border-bottom:1px solid var(--border)}.wrap{max-width:1600px;margin:auto;padding:16px}h1{font-size:20px;margin:0 0 4px}.meta,.muted{color:var(--muted)}.mono{font-family:var(--mono)}
.notice{margin:0 0 14px;padding:10px 12px;border:1px solid rgba(251,191,36,.35);background:rgba(251,191,36,.07);border-radius:12px;font-size:13px}.panel{border:1px solid var(--border);background:rgba(17,24,39,.75);border-radius:14px;overflow:hidden}.tableWrap{overflow:auto;max-height:66vh}
table{border-collapse:collapse;width:max-content;min-width:100%;font-size:12px}th,td{padding:9px 10px;border-bottom:1px solid rgba(38,51,70,.7);vertical-align:top;text-align:left}th{position:sticky;top:0;background:var(--panel);z-index:2;color:#cbd5e1;white-space:nowrap}.desc{max-width:360px;white-space:normal;line-height:1.35}.right{text-align:right}.small{font-size:11px}
a{color:#bfdbfe}.rating{display:inline-flex;padding:2px 8px;border-radius:999px;border:1px solid var(--border);font-family:var(--mono)}.rA{color:#a7f3d0;border-color:rgba(52,211,153,.45)}.rB{color:#bfdbfe;border-color:rgba(96,165,250,.45)}.rC{color:#fde68a;border-color:rgba(251,191,36,.45)}.rD{color:#cbd5e1}.rU{color:#e9d5ff;border-color:rgba(192,132,252,.45)}.rF{color:#fecdd3;border-color:rgba(251,113,133,.45)}
.pattern{margin-top:14px;padding:14px;border:1px solid var(--border);border-radius:14px;background:rgba(17,24,39,.72)}.pattern h2{font-size:16px;margin:0 0 6px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.card{border:1px solid rgba(148,163,184,.16);background:rgba(15,23,42,.48);border-radius:12px;padding:12px;min-width:0}.card h3{font-size:13px;margin:0 0 9px}.discovery{border-color:rgba(148,163,184,.35)}.prospective{border-color:rgba(52,211,153,.32)}
.kv{display:grid;grid-template-columns:180px 1fr;gap:6px 10px;font-size:12px}.k{color:var(--muted)}.v{font-family:var(--mono);overflow-wrap:anywhere}.blocker{display:inline-block;margin:2px 4px 2px 0;padding:2px 6px;border-radius:7px;background:rgba(251,191,36,.08);border:1px solid rgba(251,191,36,.25);font-size:10px}.empty{padding:24px;text-align:center;color:var(--muted)}
@media(max-width:900px){.grid{grid-template-columns:1fr}.kv{grid-template-columns:130px 1fr}}
</style>
</head><body>
<header><div class="wrap"><h1>Pattern Library / Research UI</h1><div class="meta">L11 · research-only · """ + escape(str(counts.get("pattern_count", 0))) + """ Pattern · Stand """ + escape(str(library.get("generated_at"))) + """</div></div></header>
<main class="wrap">
<div class="notice"><b>Research-only.</b> Rating ist kein Performance-Score. Discovery- und prospektive Evidenz werden getrennt gezeigt. B/A ist keine Promotion und erzeugt keine produktive Directional Authority.</div>
<div class="panel"><div class="tableWrap"><table><thead><tr>
<th>ID / Version</th><th>Beschreibung</th><th>Typ</th><th>Richtung</th><th>Horizont</th><th>Rating</th>
<th class="right">P(Direction)</th><th class="right">Baseline</th><th class="right">Lift</th><th class="right">Effekt</th>
<th class="right">N</th><th class="right">N_eff / Support</th><th>95%-Intervalle</th><th>Dependency</th><th>letzte Änderung</th>
</tr></thead><tbody>""" + "".join(rows) + """</tbody></table></div></div>
""" + "".join(details) + """
</main></body></html>"""


def persist_pattern_library(
    repo_root: str | Path,
    library: Mapping[str, Any],
    *,
    contract: Mapping[str, Any] | None = None,
    boundary: PatternDiscoveryBoundary | None = None,
) -> dict[str, Any]:
    spec = (
        dict(contract)
        if contract is not None
        else load_pattern_library_contract()
    )
    verify_pattern_library(library, contract=spec)
    guard = boundary or PatternDiscoveryBoundary()
    root = Path(repo_root).resolve()
    json_repo = str(spec["storage"]["library_json_path"])
    html_repo = str(spec["storage"]["library_html_path"])
    for repo_path in (json_repo, html_repo):
        guard.assert_write_path_allowed(repo_path)

    json_path = (root / json_repo).resolve()
    html_path = (root / html_repo).resolve()
    for path in (json_path, html_path):
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise PatternLibraryError(
                "pattern_library_path_outside_repo"
            ) from exc
        path.parent.mkdir(parents=True, exist_ok=True)

    json_text = json.dumps(
        dict(library),
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"
    html_text = render_pattern_library_html(library)
    json_path.write_text(json_text, encoding="utf-8", newline="\n")
    html_path.write_text(html_text, encoding="utf-8", newline="\n")
    return {
        "valid": True,
        "library_hash": library["library_hash"],
        "pattern_count": library["counts"]["pattern_count"],
        "json_path": json_repo,
        "html_path": html_repo,
        "json_sha256": sha256(json_text.encode("utf-8")).hexdigest(),
        "html_sha256": sha256(html_text.encode("utf-8")).hexdigest(),
    }
