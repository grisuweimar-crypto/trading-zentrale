from __future__ import annotations

"""Post-activation Stage 4: historical Elliott-vNext validation.

Stage 4 executes the already frozen 6G validation architecture on the real
project OHLCV history.  It is descriptive / research-only.  It does not alter
6A-6H, select a degree, fit Elliott rules to outcomes, change Decision-Layer
state, or perform promotion.
"""

from collections import Counter
from hashlib import sha256
import json
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from .validation import (
    ValidationConfig,
    build_validation_report,
    summarize_projection_outcomes,
    summarize_route_outcomes,
)
from .validation_replay import replay_universe_states


SCHEMA_VERSION = "elliott_vnext_stage4_historical_validation_v1"
STAGE = "STAGE_4_HISTORICAL_VALIDATION"


class Stage4HistoricalValidationError(ValueError):
    pass


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )
    return sha256(payload.encode("utf-8")).hexdigest()


def _counter_rows(counter: Counter[tuple[Any, ...]], fields: Sequence[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(counter, key=lambda item: tuple("" if value is None else str(value) for value in item)):
        values = key if isinstance(key, tuple) else (key,)
        row = {field: values[index] for index, field in enumerate(fields)}
        row["N"] = int(counter[key])
        rows.append(row)
    return rows


def _structure_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    resolution = Counter(str(row.get("structure_resolution") or "missing") for row in rows)
    by_stage = Counter(
        (
            row.get("partition"),
            row.get("wave_stage"),
            row.get("degree"),
            row.get("scenario_role"),
            row.get("structure_resolution"),
        )
        for row in rows
    )
    fit_values = [
        float(row["structural_fit"])
        for row in rows
        if row.get("structural_fit") is not None
    ]
    return {
        "claim_count": len(rows),
        "resolution_counts": dict(sorted(resolution.items())),
        "resolved_structural_fit_mean": (
            float(sum(fit_values) / len(fit_values)) if fit_values else None
        ),
        "by_partition_stage_degree_role_resolution": _counter_rows(
            by_stage,
            (
                "partition",
                "wave_stage",
                "degree",
                "scenario_role",
                "structure_resolution",
            ),
        ),
    }


def _replay_guard_review(
    snapshots: Iterable[Mapping[str, Any]],
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    violations: list[str] = []
    for index, snapshot in enumerate(snapshots):
        replay = snapshot.get("validation_replay")
        if not isinstance(replay, Mapping):
            violations.append(f"snapshot[{index}]:validation_replay_missing")
            continue
        if replay.get("mode") != "prefix_only":
            violations.append(f"snapshot[{index}]:not_prefix_only")
        if replay.get("future_rows_used") is not False:
            violations.append(f"snapshot[{index}]:future_rows_used_not_false")
        if replay.get("performance_used_for_state") is not False:
            violations.append(f"snapshot[{index}]:performance_used_for_state_not_false")
        if snapshot.get("research_only") is not True:
            violations.append(f"snapshot[{index}]:research_only_not_true")
        if "trade_decision" in snapshot:
            violations.append(f"snapshot[{index}]:trade_decision_present")
        if "order_instruction" in snapshot:
            violations.append(f"snapshot[{index}]:order_instruction_present")

    details = coverage.get("details")
    replay_errors = 0
    coverage_violations: list[str] = []
    requested = int(coverage.get("symbols_requested") or 0)
    snapshot_count = int(coverage.get("snapshots") or 0)

    if not isinstance(details, list):
        coverage_violations.append("coverage_details_missing")
    else:
        if len(details) != requested:
            coverage_violations.append(
                f"coverage_details_count_mismatch:{len(details)}:{requested}"
            )
        for item in details:
            if not isinstance(item, Mapping):
                coverage_violations.append("coverage_detail_not_mapping")
                continue
            errors = item.get("errors") or []
            replay_errors += len(errors)
            if errors:
                coverage_violations.append(
                    f"replay_errors_present:{item.get('symbol')}:{len(errors)}"
                )

    if requested <= 0:
        coverage_violations.append("symbols_requested_zero")
    if snapshot_count <= 0:
        coverage_violations.append("snapshot_count_zero")

    all_violations = [*violations, *coverage_violations]
    return {
        "valid": not all_violations,
        "violation_count": len(all_violations),
        "violations": all_violations[:100],
        "coverage_complete": not coverage_violations,
        "coverage_violation_count": len(coverage_violations),
        "replay_error_count": int(replay_errors),
        "replay_errors_are_missing_evidence_not_imputed": True,
    }


def _prepare_stage4_inputs(
    prices: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    price_source_sha256: str,
    source_commit: str,
) -> tuple[pd.DataFrame, str, str]:
    frame = prices.copy() if isinstance(prices, pd.DataFrame) else pd.DataFrame(list(prices))
    if frame.empty:
        raise Stage4HistoricalValidationError("price_history_empty")
    required = {"date", "symbol", "close", "adj_close", "high", "low"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise Stage4HistoricalValidationError(
            "price_history_columns_missing:" + ",".join(missing)
        )
    commit = str(source_commit or "").strip().lower()
    if len(commit) != 40 or any(ch not in "0123456789abcdef" for ch in commit):
        raise Stage4HistoricalValidationError("source_commit_must_be_git_sha")
    source_hash = str(price_source_sha256 or "").strip().lower()
    if len(source_hash) != 64 or any(ch not in "0123456789abcdef" for ch in source_hash):
        raise Stage4HistoricalValidationError("price_source_sha256_required")
    return frame, commit, source_hash


def build_stage4_historical_validation_from_replay(
    prices: pd.DataFrame | Iterable[Mapping[str, Any]],
    routed_snapshots: Sequence[Mapping[str, Any]] | Iterable[Mapping[str, Any]],
    replay_coverage: Mapping[str, Any],
    *,
    price_source_sha256: str,
    source_commit: str,
    replay_chunk_count: int = 1,
    config: ValidationConfig = ValidationConfig(),
) -> dict[str, Any]:
    """Build the single Stage-4 / 6G result from already completed PIT replay."""

    frame, commit, source_hash = _prepare_stage4_inputs(
        prices,
        price_source_sha256=price_source_sha256,
        source_commit=source_commit,
    )
    # Full Stage-4 replay artifacts are multi-gigabyte when expanded.  Keep
    # reusable/sized snapshot collections lazy so aggregation can replay the
    # frozen 6G passes without materialising every routed snapshot at once.
    # One-shot iterators are still materialised because 6G intentionally makes
    # several independent passes over the same causal snapshots.
    iterator = iter(routed_snapshots)
    if iterator is routed_snapshots or not hasattr(routed_snapshots, "__len__"):
        routed: Sequence[Mapping[str, Any]] | Iterable[Mapping[str, Any]] = [
            dict(item) for item in iterator
        ]
    else:
        routed = routed_snapshots

    guards = _replay_guard_review(routed, replay_coverage)
    if not guards["valid"]:
        raise Stage4HistoricalValidationError(
            "historical_replay_guard_violation:" + ";".join(guards["violations"])
        )

    report = build_validation_report(
        routed,
        frame,
        config=config,
    )
    if report.get("schema_version") != "elliott_vnext_validation_v1":
        raise Stage4HistoricalValidationError("unexpected_6g_report_schema")
    if report.get("research_only") is not True:
        raise Stage4HistoricalValidationError("6g_report_not_research_only")
    if report.get("automatic_promotion_allowed") is not False:
        raise Stage4HistoricalValidationError("automatic_promotion_must_remain_disabled")
    if report.get("trade_decision") is not None or report.get("order_instruction") is not None:
        raise Stage4HistoricalValidationError("6g_may_not_emit_trade_or_order")

    source_dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    source_symbols = sorted(frame["symbol"].dropna().astype(str).unique().tolist())
    requested = int(replay_coverage.get("symbols_requested") or 0)
    if requested != len(source_symbols):
        raise Stage4HistoricalValidationError(
            f"replay_universe_incomplete:{requested}:{len(source_symbols)}"
        )

    structure_rows = report.get("structure_validation")
    if not isinstance(structure_rows, list):
        raise Stage4HistoricalValidationError("structure_validation_missing")
    evidence_policy = report.get("evidence_policy")
    if not isinstance(evidence_policy, Mapping):
        raise Stage4HistoricalValidationError("evidence_policy_missing")

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "technical_stage_status": "COMPLETE",
        "empirical_promotion_status": "NOT_PROMOTED",
        "source": {
            "source_commit": commit,
            "price_source_sha256": source_hash,
            "price_row_count": int(len(frame)),
            "price_symbol_count": len(source_symbols),
            "price_first_date": source_dates.min().date().isoformat() if not source_dates.empty else None,
            "price_last_date": source_dates.max().date().isoformat() if not source_dates.empty else None,
            "stable_start": config.stable_start,
            "rules_frozen_through": config.rules_frozen_through,
        },
        "replay": {
            "execution_mode": "parallel_chunks" if int(replay_chunk_count) > 1 else "single_process",
            "chunk_count": int(replay_chunk_count),
            "symbols_requested": requested,
            "symbols_with_snapshots": int(replay_coverage.get("symbols_with_snapshots") or 0),
            "snapshot_count": int(replay_coverage.get("snapshots") or 0),
            "failures_are_missing_evidence_not_imputed": (
                replay_coverage.get("failures_are_missing_evidence_not_imputed") is True
            ),
            "guard_review": guards,
        },
        "coverage": dict(report.get("coverage") or {}),
        "structure_summary": _structure_summary(structure_rows),
        "projection_summary": report.get("projection_summary") or [],
        "route_summary": report.get("route_summary") or [],
        "evidence_policy": dict(evidence_policy),
        "promotion_status_from_6g": report.get("promotion_status"),
        "boundaries": {
            "historical_results_are_descriptive_for_pre_freeze_claims": True,
            "legacy_data_can_support_promotion": False,
            "future_rows_used_for_replay": False,
            "performance_used_to_build_elliott_state": False,
            "raw_close_fallback_for_performance": False,
            "same_session_projection_hit_allowed": False,
            "round_trip_pnl_invented": False,
            "numeric_w5_level_promoted": False,
            "degree_reducer_used": False,
            "elliott_core_modified": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "automatic_promotion_allowed": False,
            "research_only": True,
        },
        "stage4_scope": {
            "historical_prefix_replay": True,
            "structure_progression_validation": True,
            "projection_zone_validation": True,
            "forward_outcomes_5_10_20_40_60_sessions": True,
            "review_routing_directional_validation": True,
            "cross_system_incremental_value_deferred_to_stage5": True,
            "swing_execution_round_trip_deferred_until_frozen_execution_policy": True,
            "prospective_confirmation_continues_in_parallel": True,
        },
    }
    result["stage4_result_hash"] = _canonical_hash(result)
    return result



def build_stage4_historical_validation_from_components(
    prices: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    structure_validation: Sequence[Mapping[str, Any]],
    projection_outcomes: Sequence[Mapping[str, Any]],
    route_outcomes: Sequence[Mapping[str, Any]],
    replay_coverage: Mapping[str, Any],
    replay_guard_review: Mapping[str, Any],
    projection_claim_count: int,
    route_claim_count: int,
    price_source_sha256: str,
    source_commit: str,
    replay_chunk_count: int,
    source_workflow_run_id: int | None = None,
    config: ValidationConfig = ValidationConfig(),
) -> dict[str, Any]:
    """Build Stage 4 from per-chunk 6G validation components.

    This path is semantically equivalent to the full replay wrapper after the
    causal replay has already been completed and each chunk has independently
    extracted structure/projection/route evidence.  It exists to keep the final
    aggregation bounded in memory and runtime.
    """

    frame, commit, source_hash = _prepare_stage4_inputs(
        prices,
        price_source_sha256=price_source_sha256,
        source_commit=source_commit,
    )
    guards = dict(replay_guard_review)
    if guards.get("valid") is not True:
        raise Stage4HistoricalValidationError("component_replay_guard_invalid")
    if int(guards.get("replay_error_count") or 0) != 0:
        raise Stage4HistoricalValidationError("component_replay_errors_present")
    if guards.get("coverage_complete") is not True:
        raise Stage4HistoricalValidationError("component_replay_coverage_incomplete")

    source_dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    source_symbols = sorted(frame["symbol"].dropna().astype(str).unique().tolist())
    requested = int(replay_coverage.get("symbols_requested") or 0)
    if requested != len(source_symbols):
        raise Stage4HistoricalValidationError(
            f"replay_universe_incomplete:{requested}:{len(source_symbols)}"
        )

    structure_rows = [dict(row) for row in structure_validation]
    projection_rows = [dict(row) for row in projection_outcomes]
    route_rows = [dict(row) for row in route_outcomes]
    prospective_mature = sum(
        1
        for row in [*projection_rows, *route_rows]
        if row.get("partition") == "prospective_unspent"
        and row.get("outcome_available")
    )
    prospective_structure = sum(
        1
        for row in structure_rows
        if row.get("partition") == "prospective_unspent"
        and row.get("structure_resolution") != "unresolved"
    )
    evidence_policy = {
        "legacy_data_can_support_promotion": False,
        "formal_claims_require_available_from_after_freeze": True,
        "prospective_unspent_mature_outcomes": int(prospective_mature),
        "prospective_unspent_resolved_structure_claims": int(prospective_structure),
    }
    coverage = {
        "routed_snapshots": int(replay_coverage.get("snapshots") or 0),
        "structure_claims": len(structure_rows),
        "projection_claims": int(projection_claim_count),
        "route_review_claims": int(route_claim_count),
        "projection_outcome_rows": len(projection_rows),
        "route_outcome_rows": len(route_rows),
        "cross_system_rows": 0,
        "context_alpha_rows": 0,
    }
    promotion_status = (
        "prospective_evidence_accumulating_no_automatic_promotion"
        if prospective_mature + prospective_structure > 0
        else "awaiting_unspent_prospective_evidence"
    )

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "technical_stage_status": "COMPLETE",
        "empirical_promotion_status": "NOT_PROMOTED",
        "source": {
            "source_commit": commit,
            "price_source_sha256": source_hash,
            "price_row_count": int(len(frame)),
            "price_symbol_count": len(source_symbols),
            "price_first_date": (
                source_dates.min().date().isoformat() if not source_dates.empty else None
            ),
            "price_last_date": (
                source_dates.max().date().isoformat() if not source_dates.empty else None
            ),
            "stable_start": config.stable_start,
            "rules_frozen_through": config.rules_frozen_through,
        },
        "replay": {
            "execution_mode": "parallel_chunks",
            "aggregation_mode": "distilled_validation_components",
            "chunk_count": int(replay_chunk_count),
            "symbols_requested": requested,
            "symbols_with_snapshots": int(
                replay_coverage.get("symbols_with_snapshots") or 0
            ),
            "snapshot_count": int(replay_coverage.get("snapshots") or 0),
            "failures_are_missing_evidence_not_imputed": (
                replay_coverage.get("failures_are_missing_evidence_not_imputed")
                is True
            ),
            "guard_review": guards,
            "source_workflow_run_id": (
                int(source_workflow_run_id)
                if source_workflow_run_id is not None
                else None
            ),
        },
        "coverage": coverage,
        "structure_summary": _structure_summary(structure_rows),
        "projection_summary": summarize_projection_outcomes(
            projection_rows,
            config,
        ),
        "route_summary": summarize_route_outcomes(
            route_rows,
            config,
        ),
        "evidence_policy": evidence_policy,
        "promotion_status_from_6g": promotion_status,
        "boundaries": {
            "historical_results_are_descriptive_for_pre_freeze_claims": True,
            "legacy_data_can_support_promotion": False,
            "future_rows_used_for_replay": False,
            "performance_used_to_build_elliott_state": False,
            "raw_close_fallback_for_performance": False,
            "same_session_projection_hit_allowed": False,
            "round_trip_pnl_invented": False,
            "numeric_w5_level_promoted": False,
            "degree_reducer_used": False,
            "elliott_core_modified": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "direct_ordering_allowed": False,
            "automatic_promotion_allowed": False,
            "research_only": True,
        },
        "stage4_scope": {
            "historical_prefix_replay": True,
            "structure_progression_validation": True,
            "projection_zone_validation": True,
            "forward_outcomes_5_10_20_40_60_sessions": True,
            "review_routing_directional_validation": True,
            "cross_system_incremental_value_deferred_to_stage5": True,
            "swing_execution_round_trip_deferred_until_frozen_execution_policy": True,
            "prospective_confirmation_continues_in_parallel": True,
        },
    }
    result["stage4_result_hash"] = _canonical_hash(result)
    return result


def adapt_stage4_validation_for_6h(
    stage4_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose the completed Stage-4 aggregate through the frozen 6G contract.

    Stage 4 is a compact wrapper around the already-computed 6G summaries.
    This adapter performs no replay and no re-estimation.  It only validates
    the Stage-4 research/no-promotion boundaries and maps the stored summaries
    back into the existing elliott_vnext_validation_v1 shape consumed by 6H.
    """

    if stage4_report.get("schema_version") != SCHEMA_VERSION:
        raise Stage4HistoricalValidationError("stage4_adapter_schema_invalid")
    if stage4_report.get("stage") != STAGE:
        raise Stage4HistoricalValidationError("stage4_adapter_stage_invalid")
    if stage4_report.get("technical_stage_status") != "COMPLETE":
        raise Stage4HistoricalValidationError("stage4_adapter_not_complete")
    if stage4_report.get("empirical_promotion_status") != "NOT_PROMOTED":
        raise Stage4HistoricalValidationError("stage4_adapter_promotion_state_invalid")

    source = stage4_report.get("source")
    evidence_policy = stage4_report.get("evidence_policy")
    coverage = stage4_report.get("coverage")
    boundaries = stage4_report.get("boundaries")
    projection_summary = stage4_report.get("projection_summary")
    route_summary = stage4_report.get("route_summary")
    if not isinstance(source, Mapping):
        raise Stage4HistoricalValidationError("stage4_adapter_source_missing")
    if not isinstance(evidence_policy, Mapping):
        raise Stage4HistoricalValidationError("stage4_adapter_evidence_policy_missing")
    if not isinstance(coverage, Mapping):
        raise Stage4HistoricalValidationError("stage4_adapter_coverage_missing")
    if not isinstance(boundaries, Mapping):
        raise Stage4HistoricalValidationError("stage4_adapter_boundaries_missing")
    if not isinstance(projection_summary, list) or not isinstance(route_summary, list):
        raise Stage4HistoricalValidationError("stage4_adapter_summaries_missing")

    required_false = (
        "future_rows_used_for_replay",
        "performance_used_to_build_elliott_state",
        "raw_close_fallback_for_performance",
        "same_session_projection_hit_allowed",
        "round_trip_pnl_invented",
        "numeric_w5_level_promoted",
        "degree_reducer_used",
        "elliott_core_modified",
        "changes_universal_stance",
        "changes_portfolio_action",
        "direct_ordering_allowed",
        "automatic_promotion_allowed",
    )
    for field in required_false:
        if boundaries.get(field) is not False:
            raise Stage4HistoricalValidationError(
                f"stage4_adapter_boundary_invalid:{field}"
            )
    if boundaries.get("research_only") is not True:
        raise Stage4HistoricalValidationError("stage4_adapter_must_remain_research_only")
    if evidence_policy.get("legacy_data_can_support_promotion") is not False:
        raise Stage4HistoricalValidationError("stage4_adapter_legacy_promotion_forbidden")

    rules_frozen_through = str(source.get("rules_frozen_through") or "").strip()
    if not rules_frozen_through:
        raise Stage4HistoricalValidationError("stage4_adapter_rules_freeze_missing")
    promotion_status = str(stage4_report.get("promotion_status_from_6g") or "").strip()
    if promotion_status not in {
        "awaiting_unspent_prospective_evidence",
        "prospective_evidence_accumulating_no_automatic_promotion",
    }:
        raise Stage4HistoricalValidationError("stage4_adapter_6g_status_invalid")

    return {
        "schema_version": "elliott_vnext_validation_v1",
        "module": "6G_historical_validation",
        "rules_frozen_through": rules_frozen_through,
        "horizons_sessions": [5, 10, 20, 40, 60],
        "evidence_policy": dict(evidence_policy),
        "coverage": dict(coverage),
        "structure_validation": [],
        "structure_summary": stage4_report.get("structure_summary"),
        "projection_summary": [dict(row) for row in projection_summary],
        "route_summary": [dict(row) for row in route_summary],
        "cross_system_validation": [],
        "market_context_validation": {
            "status": "not_supplied_missing_context_stays_missing",
            "alpha_rows": [],
            "scanner_peer_fallback_used": False,
        },
        "promotion_status": promotion_status,
        "automatic_promotion_allowed": False,
        "technical_completion_is_empirical_validation": False,
        "round_trip_pnl_evaluated": False,
        "numeric_w5_levels_promoted": False,
        "trade_decision": None,
        "order_instruction": None,
        "research_only": True,
        "source": {
            "adapter": "stage4_compact_aggregate_to_frozen_6g_v1",
            "stage4_result_hash": stage4_report.get("stage4_result_hash"),
            "source_commit": source.get("source_commit"),
            "price_source_sha256": source.get("price_source_sha256"),
        },
    }


def build_stage4_historical_validation(
    prices: pd.DataFrame | Iterable[Mapping[str, Any]],
    *,
    symbols: Sequence[str] | None = None,
    price_source_sha256: str,
    source_commit: str,
    config: ValidationConfig = ValidationConfig(),
) -> dict[str, Any]:
    """Run the full frozen 6A->6D replay and 6G historical validation."""

    frame, commit, source_hash = _prepare_stage4_inputs(
        prices,
        price_source_sha256=price_source_sha256,
        source_commit=source_commit,
    )
    routed, replay_coverage = replay_universe_states(
        frame,
        symbols=symbols,
        config=config,
        keep_unchanged=False,
    )
    # Explicit symbol subsets are integration-test/debug paths.  The canonical
    # full Stage-4 artifact always uses the complete source universe.
    if symbols is not None:
        selected = sorted({str(symbol) for symbol in symbols})
        selected_frame = frame.loc[frame["symbol"].astype(str).isin(selected)].copy()
        return build_stage4_historical_validation_from_replay(
            selected_frame,
            routed,
            replay_coverage,
            price_source_sha256=source_hash,
            source_commit=commit,
            replay_chunk_count=1,
            config=config,
        )
    return build_stage4_historical_validation_from_replay(
        frame,
        routed,
        replay_coverage,
        price_source_sha256=source_hash,
        source_commit=commit,
        replay_chunk_count=1,
        config=config,
    )
