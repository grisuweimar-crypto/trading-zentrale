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

from .validation import ValidationConfig, build_validation_report
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
    snapshots: Sequence[Mapping[str, Any]],
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
    if isinstance(details, list):
        replay_errors = sum(
            len(item.get("errors") or [])
            for item in details
            if isinstance(item, Mapping)
        )
    return {
        "valid": not violations,
        "violation_count": len(violations),
        "violations": violations[:100],
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
    routed_snapshots: Sequence[Mapping[str, Any]],
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
    routed = [dict(item) for item in routed_snapshots]
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
