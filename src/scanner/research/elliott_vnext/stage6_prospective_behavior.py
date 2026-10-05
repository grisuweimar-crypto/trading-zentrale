from __future__ import annotations

"""Stage 6 prospective behavior audit for Elliott vNext.

This post-activation stage consumes only already-published prospective 6H
captures.  It does not replay Elliott, inspect future outcomes, choose a wave
degree, alter W6/7F, or promote any rule.  The result is a compact descriptive
audit of stability, review-context frequency and cross-degree conflicts.
"""

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from typing import Any, Iterable, Mapping

from scanner.research.decision_layer.phase6_elliott import (
    Elliott6HAdapterError,
    validate_elliott_6h_output,
)


SCHEMA_VERSION = "elliott_vnext_stage6_prospective_behavior_v1"
STAGE = "STAGE_6_PROSPECTIVE_BEHAVIOR"
CAPTURE_SCHEMA = "elliott_vnext_prospective_capture_v1"
CAPTURE_ENGINE = "prospective_capture_engine_v2_iso_date_replay"

ADD_CONTEXTS = frozenset({"entry_or_add_review", "reentry_or_add_review"})
REDUCE_CONTEXTS = frozenset({
    "partial_reduce_review",
    "profit_protection_review",
    "larger_reduce_or_exit_review",
})
ACTIONABLE_CONTEXTS = ADD_CONTEXTS | REDUCE_CONTEXTS
ALL_REVIEW_CONTEXTS = ACTIONABLE_CONTEXTS | {"hold_review"}


class Stage6ProspectiveBehaviorError(ValueError):
    """Raised when prospective behavior cannot be audited without inference."""


def _canonical_hash(value: object) -> str:
    frozen = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )
    return sha256(frozen.encode("utf-8")).hexdigest()


def _timestamp(value: object, field: str) -> datetime:
    text = str(value or "").strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise Stage6ProspectiveBehaviorError(f"invalid_{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise Stage6ProspectiveBehaviorError(f"{field}_timezone_required")
    return parsed.astimezone(timezone.utc)


def _date(value: object, field: str) -> date:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise Stage6ProspectiveBehaviorError(f"invalid_{field}") from exc


def _git_sha(value: object, field: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 40 or any(char not in "0123456789abcdef" for char in text):
        raise Stage6ProspectiveBehaviorError(f"{field}_git_sha_required")
    return text


def _scenario_id(output: Mapping[str, object]) -> str | None:
    primary = output.get("primary_scenario")
    if not isinstance(primary, Mapping):
        return None
    value = str(primary.get("scenario_id") or "").strip()
    return value or None


def _wave_stage(output: Mapping[str, object]) -> str | None:
    value = str(output.get("current_wave_stage") or "").strip()
    return value or None


def _route_contexts(output: Mapping[str, object]) -> list[str]:
    routes = output.get("swing_routing")
    if not isinstance(routes, list):
        raise Stage6ProspectiveBehaviorError("swing_routing_must_be_list")
    contexts: list[str] = []
    for route in routes:
        if not isinstance(route, Mapping):
            raise Stage6ProspectiveBehaviorError("swing_route_must_be_object")
        context = str(route.get("review_context") or "").strip()
        if context not in ALL_REVIEW_CONTEXTS:
            raise Stage6ProspectiveBehaviorError(f"unknown_review_context:{context}")
        contexts.append(context)
    return contexts


def _validate_capture(raw: Mapping[str, object], index: int) -> dict[str, object]:
    if raw.get("schema_version") != CAPTURE_SCHEMA:
        raise Stage6ProspectiveBehaviorError(f"capture_schema_invalid:{index}")
    if raw.get("capture_engine_version") != CAPTURE_ENGINE:
        raise Stage6ProspectiveBehaviorError(f"capture_engine_invalid:{index}")
    if raw.get("validation_partition") != "prospective_unspent":
        raise Stage6ProspectiveBehaviorError(f"capture_not_prospective_unspent:{index}")

    capture_id = str(raw.get("capture_id") or "").strip()
    snapshot_id = str(raw.get("snapshot_id") or "").strip()
    run_id = str(raw.get("run_id") or "").strip()
    if not capture_id or not snapshot_id or not run_id:
        raise Stage6ProspectiveBehaviorError(f"capture_identity_incomplete:{index}")

    source_commit = _git_sha(
        raw.get("source_publication_commit"),
        f"capture_source_publication_commit:{index}",
    )
    scanner_published_at = _timestamp(
        raw.get("scanner_published_at"),
        f"scanner_published_at:{index}",
    )
    captured_at = _timestamp(raw.get("captured_at"), f"captured_at:{index}")
    as_of = _date(raw.get("as_of"), f"capture_as_of:{index}")
    if captured_at < scanner_published_at:
        raise Stage6ProspectiveBehaviorError(f"capture_before_scanner_publication:{index}")
    if as_of > captured_at.date():
        raise Stage6ProspectiveBehaviorError(f"future_capture_as_of:{index}")

    guards = raw.get("guards")
    if not isinstance(guards, Mapping):
        raise Stage6ProspectiveBehaviorError(f"capture_guards_required:{index}")
    required_true = (
        "research_only",
        "missing_evidence_not_imputed",
        "multi_degree_outputs_retained_without_reducer",
    )
    required_false = (
        "productive_integration_enabled",
        "w10_source_emitted",
        "changes_universal_stance",
        "changes_portfolio_action",
        "direct_ordering_allowed",
        "future_rows_used",
        "frozen_elliott_core_modified",
    )
    for field in required_true:
        if guards.get(field) is not True:
            raise Stage6ProspectiveBehaviorError(f"capture_guard_true_required:{index}:{field}")
    for field in required_false:
        if guards.get(field) is not False:
            raise Stage6ProspectiveBehaviorError(f"capture_guard_false_required:{index}:{field}")

    outputs = raw.get("outputs")
    if not isinstance(outputs, list):
        raise Stage6ProspectiveBehaviorError(f"capture_outputs_must_be_list:{index}")
    if int(raw.get("output_count") or 0) != len(outputs):
        raise Stage6ProspectiveBehaviorError(f"capture_output_count_mismatch:{index}")

    normalized_outputs: list[dict[str, object]] = []
    identities: set[tuple[str, str, str]] = set()
    for output_index, item in enumerate(outputs):
        if not isinstance(item, Mapping):
            raise Stage6ProspectiveBehaviorError(
                f"capture_output_must_be_object:{index}:{output_index}"
            )
        try:
            output = validate_elliott_6h_output(item)
        except Elliott6HAdapterError as exc:
            raise Stage6ProspectiveBehaviorError(
                f"invalid_6h_output:{index}:{output_index}:{exc}"
            ) from exc
        if _date(output.get("as_of"), "output_as_of") != as_of:
            raise Stage6ProspectiveBehaviorError(
                f"capture_output_as_of_mismatch:{index}:{output_index}"
            )
        identity = (
            str(output.get("symbol") or ""),
            str(output.get("timeframe") or ""),
            str(output.get("degree") or ""),
        )
        if not all(identity):
            raise Stage6ProspectiveBehaviorError(
                f"capture_output_identity_incomplete:{index}:{output_index}"
            )
        if identity in identities:
            raise Stage6ProspectiveBehaviorError(
                "duplicate_symbol_timeframe_degree:" + ":".join(identity)
            )
        identities.add(identity)
        normalized_outputs.append(output)

    symbols = {str(output.get("symbol") or "") for output in normalized_outputs}
    if int(raw.get("symbols_with_outputs") or 0) != len(symbols):
        raise Stage6ProspectiveBehaviorError(f"capture_symbol_count_mismatch:{index}")

    return {
        "capture_id": capture_id,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "source_publication_commit": source_commit,
        "scanner_published_at": scanner_published_at,
        "captured_at": captured_at,
        "as_of": as_of.isoformat(),
        "outputs": normalized_outputs,
    }


def _stability_summary(
    timelines: Mapping[tuple[str, str, str], list[dict[str, object]]],
    field: str,
) -> dict[str, object]:
    counts = Counter({"STABLE": 0, "CHANGED": 0, "INSUFFICIENT_EVIDENCE": 0})
    identities_with_observations = 0
    identities_with_single_output = 0
    for rows in timelines.values():
        ordered = sorted(rows, key=lambda row: (row["captured_at"], row["capture_id"]))
        if len(ordered) < 2:
            identities_with_single_output += 1
            continue
        identities_with_observations += 1
        for prior, current in zip(ordered, ordered[1:]):
            left = prior.get(field)
            right = current.get(field)
            if left is None or right is None:
                counts["INSUFFICIENT_EVIDENCE"] += 1
            elif left == right:
                counts["STABLE"] += 1
            else:
                counts["CHANGED"] += 1
    total = sum(counts.values())
    return {
        "comparison": f"strict_consecutive_available_{field}_equality",
        "observation_count": total,
        "counts": dict(counts),
        "identities_with_observations": identities_with_observations,
        "identities_with_single_output_only": identities_with_single_output,
        "threshold_used": False,
        "smoothing_used": False,
        "missing_is_not_neutral": True,
    }


def build_stage6_prospective_behavior(
    captures: Iterable[Mapping[str, object]],
) -> dict[str, object]:
    """Reduce the append-only prospective archive to a compact behavior audit."""
    timelines: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    route_occurrences: Counter[str] = Counter()
    output_context_presence: Counter[str] = Counter()
    symbol_capture_context_presence: Counter[str] = Counter()
    warning_occurrences: Counter[str] = Counter()
    capture_summaries: list[dict[str, object]] = []

    seen_capture_ids: set[str] = set()
    seen_run_ids: set[str] = set()
    seen_snapshot_ids: set[str] = set()
    excluded_legacy_capture_count = 0
    eligible_capture_count = 0
    validated_output_count = 0
    symbol_capture_observations = 0
    add_reduce_conflict_observations = 0
    actionable_symbol_capture_observations = 0
    outputs_without_routes = 0

    for index, raw in enumerate(captures):
        if not isinstance(raw, Mapping):
            raise Stage6ProspectiveBehaviorError(f"capture_must_be_object:{index}")
        if raw.get("schema_version") != CAPTURE_SCHEMA:
            raise Stage6ProspectiveBehaviorError(f"capture_schema_invalid:{index}")
        if raw.get("capture_engine_version") != CAPTURE_ENGINE:
            excluded_legacy_capture_count += 1
            continue

        capture = _validate_capture(raw, index)
        capture_id = str(capture["capture_id"])
        run_id = str(capture["run_id"])
        snapshot_id = str(capture["snapshot_id"])
        if capture_id in seen_capture_ids:
            raise Stage6ProspectiveBehaviorError(f"duplicate_capture_id:{capture_id}")
        if run_id in seen_run_ids:
            raise Stage6ProspectiveBehaviorError(f"duplicate_current_engine_run_id:{run_id}")
        if snapshot_id in seen_snapshot_ids:
            raise Stage6ProspectiveBehaviorError(
                f"duplicate_current_engine_snapshot_id:{snapshot_id}"
            )
        seen_capture_ids.add(capture_id)
        seen_run_ids.add(run_id)
        seen_snapshot_ids.add(snapshot_id)
        eligible_capture_count += 1

        per_symbol_contexts: dict[str, set[str]] = defaultdict(set)
        per_symbol_output_count: Counter[str] = Counter()
        per_capture_route_occurrences: Counter[str] = Counter()

        for output in capture["outputs"]:
            assert isinstance(output, Mapping)
            validated_output_count += 1
            symbol = str(output.get("symbol") or "")
            timeframe = str(output.get("timeframe") or "")
            degree = str(output.get("degree") or "")
            identity = (symbol, timeframe, degree)
            contexts = _route_contexts(output)
            if not contexts:
                outputs_without_routes += 1
            route_occurrences.update(contexts)
            per_capture_route_occurrences.update(contexts)
            output_context_presence.update(set(contexts))
            per_symbol_contexts[symbol].update(contexts)
            per_symbol_output_count[symbol] += 1

            warnings = output.get("warnings")
            if isinstance(warnings, list):
                warning_occurrences.update(
                    str(item).strip() for item in warnings if str(item).strip()
                )

            timelines[identity].append({
                "capture_id": capture_id,
                "captured_at": capture["captured_at"],
                "scenario_id": _scenario_id(output),
                "wave_stage": _wave_stage(output),
            })

        conflict_symbols: list[str] = []
        actionable_symbols: list[str] = []
        for symbol, contexts in per_symbol_contexts.items():
            symbol_capture_observations += 1
            symbol_capture_context_presence.update(contexts)
            has_add = bool(contexts & ADD_CONTEXTS)
            has_reduce = bool(contexts & REDUCE_CONTEXTS)
            if contexts & ACTIONABLE_CONTEXTS:
                actionable_symbol_capture_observations += 1
                actionable_symbols.append(symbol)
            if has_add and has_reduce:
                add_reduce_conflict_observations += 1
                conflict_symbols.append(symbol)

        capture_summaries.append({
            "capture_id": capture_id,
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "as_of": capture["as_of"],
            "captured_at": capture["captured_at"].isoformat(),
            "source_publication_commit": capture["source_publication_commit"],
            "output_count": len(capture["outputs"]),
            "symbol_count": len(per_symbol_output_count),
            "route_occurrences": dict(sorted(per_capture_route_occurrences.items())),
            "actionable_symbol_count": len(actionable_symbols),
            "add_reduce_conflict_symbol_count": len(conflict_symbols),
            "add_reduce_conflict_symbols": sorted(conflict_symbols),
        })

    capture_summaries.sort(key=lambda row: (row["captured_at"], row["capture_id"]))
    scenario_stability = _stability_summary(timelines, "scenario_id")
    wave_stage_stability = _stability_summary(timelines, "wave_stage")

    first_capture = capture_summaries[0] if capture_summaries else None
    last_capture = capture_summaries[-1] if capture_summaries else None
    observation_days = None
    if first_capture and last_capture:
        first_day = _timestamp(first_capture["captured_at"], "first_capture").date()
        last_day = _timestamp(last_capture["captured_at"], "last_capture").date()
        observation_days = (last_day - first_day).days

    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "technical_stage_status": "COMPLETE",
        "prospective_observation_status": (
            "AVAILABLE" if eligible_capture_count else "NO_ELIGIBLE_CAPTURE"
        ),
        "empirical_promotion_status": "NOT_PROMOTED",
        "source": {
            "capture_schema": CAPTURE_SCHEMA,
            "capture_engine_version": CAPTURE_ENGINE,
            "eligible_capture_count": eligible_capture_count,
            "excluded_legacy_capture_count": excluded_legacy_capture_count,
            "first_capture_id": first_capture["capture_id"] if first_capture else None,
            "last_capture_id": last_capture["capture_id"] if last_capture else None,
            "first_as_of": first_capture["as_of"] if first_capture else None,
            "last_as_of": last_capture["as_of"] if last_capture else None,
            "observation_span_calendar_days": observation_days,
        },
        "coverage": {
            "validated_6h_output_count": validated_output_count,
            "identity_count": len(timelines),
            "symbol_capture_observations": symbol_capture_observations,
            "outputs_without_routes": outputs_without_routes,
        },
        "scenario_stability": scenario_stability,
        "wave_stage_stability": wave_stage_stability,
        "review_contexts": {
            "route_occurrences": dict(sorted(route_occurrences.items())),
            "output_context_presence": dict(sorted(output_context_presence.items())),
            "symbol_capture_context_presence": dict(
                sorted(symbol_capture_context_presence.items())
            ),
            "actionable_symbol_capture_observations": actionable_symbol_capture_observations,
            "add_reduce_conflict_observations": add_reduce_conflict_observations,
            "symbol_capture_observations": symbol_capture_observations,
        },
        "warnings": {
            "occurrences": dict(sorted(warning_occurrences.items())),
            "distinct_warning_count": len(warning_occurrences),
        },
        "capture_summaries": capture_summaries,
        "interpretation": {
            "descriptive_only": True,
            "prospective_behavior_not_forward_performance": True,
            "scenario_stability_semantics": "strict primary_scenario.scenario_id equality",
            "wave_stage_stability_semantics": "strict current_wave_stage equality",
            "no_minimum_sample_threshold_invented": True,
            "small_sample_does_not_imply_empirical_validation": True,
            "technical_completion_is_empirical_validation": False,
        },
        "boundaries": {
            "research_only": True,
            "future_outcomes_used": False,
            "elliott_replayed": False,
            "elliott_core_modified": False,
            "degree_reducer_used": False,
            "elliott_direction_used_as_vote": False,
            "scanner_thresholds_reoptimized": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "review_contexts_are_actions": False,
            "automatic_promotion_allowed": False,
            "trade_decision": None,
            "order_instruction": None,
        },
    }
    report["result_hash"] = _canonical_hash(report)
    return report
