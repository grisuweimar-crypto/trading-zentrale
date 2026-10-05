#!/usr/bin/env python3
from __future__ import annotations

"""Aggregate exact Stage-4 sufficient statistics into the canonical artifact."""

import argparse
from collections import Counter, defaultdict
import csv
import gzip
from hashlib import sha256
import json
from math import ceil
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from scanner.research.elliott_vnext.validation import ValidationConfig


SCHEMA_VERSION = "elliott_vnext_stage4_historical_validation_v1"
STAGE = "STAGE_4_HISTORICAL_VALIDATION"
DEFAULT_PRICES = "artifacts/market_data/yahoo_ohlcv.csv"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_stage4_historical_validation.json"


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _sha256_stream(path: Path) -> str:
    h = sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(1024 * 1024)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


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


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


def _price_metadata(path: Path) -> dict[str, Any]:
    row_count = 0
    symbols: set[str] = set()
    first_date: str | None = None
    last_date: str | None = None
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"date", "symbol", "close", "adj_close", "high", "low"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError("price_history_columns_missing:" + ",".join(sorted(missing)))
        for row in reader:
            row_count += 1
            symbol = str(row.get("symbol") or "").strip()
            if symbol:
                symbols.add(symbol)
            date = str(row.get("date") or "").strip()
            if date:
                first_date = date if first_date is None or date < first_date else first_date
                last_date = date if last_date is None or date > last_date else last_date
    return {
        "row_count": row_count,
        "symbols": symbols,
        "first_date": first_date,
        "last_date": last_date,
    }


def _sort_key(values: tuple[object, ...]) -> tuple[str, ...]:
    return tuple("" if value is None else str(value) for value in values)


def _bootstrap_daily(
    daily: Mapping[str, tuple[float, int]],
    *,
    metric: str,
    horizon: int,
    seed_key: tuple[object, ...],
    config: ValidationConfig,
) -> dict[str, Any]:
    dates = sorted(date for date, (_, count) in daily.items() if count > 0)
    block_length = max(1, 2 * int(horizon))
    total_count = int(sum(daily[date][1] for date in dates))
    total_sum = float(sum(daily[date][0] for date in dates))
    point = total_sum / total_count if total_count else None
    support_regions = int(ceil(len(dates) / block_length)) if dates else 0
    result = {
        "N": total_count,
        "date_count": len(dates),
        "block_count": len(dates),
        "block_length": block_length,
        "support_regions": support_regions,
        "mean": float(point) if point is not None else None,
        "mean_95": None,
        "robust_interval_available": False,
    }
    if (
        not dates
        or config.bootstrap_reps <= 0
        or len(dates) < 2
        or support_regions < config.min_support_regions
    ):
        return result

    n = len(dates)
    span = min(block_length, n)
    draws_per_rep = int(np.ceil(n / block_length))
    rng = np.random.default_rng(
        _stable_seed(config.random_seed, *seed_key, metric, horizon)
    )
    estimates: list[float] = []
    for _ in range(config.bootstrap_reps):
        chosen = rng.integers(0, n, size=draws_per_rep)
        sampled: list[str] = []
        for idx in chosen:
            start = int(idx)
            sampled.extend(dates[(start + offset) % n] for offset in range(span))
        sampled = sampled[:n]
        sample_sum = 0.0
        sample_count = 0
        for date in sampled:
            day_sum, day_count = daily[date]
            sample_sum += day_sum
            sample_count += day_count
        if sample_count:
            estimates.append(sample_sum / sample_count)
    if estimates:
        lo, hi = np.quantile(np.asarray(estimates, dtype=float), [0.025, 0.975])
        result["mean_95"] = [float(lo), float(hi)]
        result["robust_interval_available"] = True
    return result


def _summary_rows(
    *,
    group_fields: tuple[str, ...],
    groups: set[tuple[object, ...]],
    daily_acc: Mapping[tuple[object, ...], tuple[float, int]],
    kind: str,
    config: ValidationConfig,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for group in sorted(groups, key=_sort_key):
        values = {field: group[idx] for idx, field in enumerate(group_fields)}
        horizon = int(values["horizon_sessions"])

        if kind == "projection":
            specs = (
                (
                    "projection_hit_numeric",
                    ("projection", values["partition"], values["wave_role"], values["projection_type"], values["degree"]),
                ),
                (
                    "signed_forward_return",
                    ("projection_return", values["partition"], values["wave_role"], values["projection_type"], values["degree"]),
                ),
            )
        else:
            specs = (
                (
                    "review_correct_numeric",
                    ("route", values["partition"], values["review_context"], values["wave_stage"], values["degree"]),
                ),
                (
                    "signed_forward_return",
                    ("route_return", values["partition"], values["review_context"], values["wave_stage"], values["degree"]),
                ),
            )

        stats: dict[str, dict[str, Any]] = {}
        for metric, seed_key in specs:
            by_day: dict[str, tuple[float, int]] = {}
            prefix = (*group, metric)
            for key, aggregate in daily_acc.items():
                if key[:-1] == prefix:
                    by_day[str(key[-1])] = aggregate
            stats[metric] = _bootstrap_daily(
                by_day,
                metric=metric,
                horizon=horizon,
                seed_key=seed_key,
                config=config,
            )

        if kind == "projection":
            result.append({
                **values,
                "zone_hit": stats["projection_hit_numeric"],
                "signed_return": stats["signed_forward_return"],
                "formal_promotion_evidence": values["partition"] == "prospective_unspent",
                "numeric_level_promoted": False,
                "research_only": True,
            })
        else:
            result.append({
                **values,
                "directional_review_correctness": stats["review_correct_numeric"],
                "signed_return": stats["signed_forward_return"],
                "round_trip_pnl_evaluated": False,
                "reason_round_trip_not_evaluated": "no_frozen_execution_fraction_or_reentry_policy",
                "formal_promotion_evidence": values["partition"] == "prospective_unspent",
                "research_only": True,
            })
    return result


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--prices", default=DEFAULT_PRICES)
    parser.add_argument("--stats-dir", required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-workflow-run-id", type=int, required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    args = parser.parse_args()

    root = args.root.resolve()
    prices_path = _resolve(root, args.prices)
    stats_dir = _resolve(root, args.stats_dir)
    output_path = _resolve(root, args.output)
    config = ValidationConfig(
        bootstrap_reps=args.bootstrap_reps,
        replay_price_basis="raw",
    )

    price_hash = _sha256_stream(prices_path)
    price_meta = _price_metadata(prices_path)
    paths = sorted(stats_dir.glob("stage4-sufficient-stats-*.json.gz"))
    if len(paths) != args.chunk_count:
        raise ValueError(
            f"stage4_sufficient_stats_count_mismatch:{len(paths)}:{args.chunk_count}"
        )

    seen_indices: set[int] = set()
    seen_symbols: set[str] = set()
    coverage_details: list[dict[str, Any]] = []
    symbols_requested = 0
    symbols_with_snapshots = 0
    snapshot_count = 0
    replay_error_count = 0
    guard_violations: list[str] = []
    coverage_complete = True

    structure_claim_count = 0
    structure_resolution = Counter()
    structure_grouped = Counter()
    structure_fit_sum = 0.0
    structure_fit_count = 0
    prospective_structure = 0

    projection_claim_count = 0
    route_claim_count = 0
    projection_outcome_rows = 0
    route_outcome_rows = 0
    prospective_mature = 0

    projection_fields = (
        "partition",
        "horizon_sessions",
        "wave_role",
        "projection_type",
        "degree",
    )
    route_fields = (
        "partition",
        "horizon_sessions",
        "review_context",
        "wave_stage",
        "degree",
    )
    projection_groups: set[tuple[object, ...]] = set()
    route_groups: set[tuple[object, ...]] = set()
    projection_daily: dict[tuple[object, ...], list[float]] = defaultdict(lambda: [0.0, 0.0])
    route_daily: dict[tuple[object, ...], list[float]] = defaultdict(lambda: [0.0, 0.0])

    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("schema_version") != "elliott_vnext_stage4_sufficient_stats_v1":
            raise ValueError(f"stage4_sufficient_stats_schema_invalid:{path.name}")
        if payload.get("source_commit") != args.source_commit:
            raise ValueError(f"stage4_sufficient_stats_source_commit_mismatch:{path.name}")
        if payload.get("price_source_sha256") != price_hash:
            raise ValueError(f"stage4_sufficient_stats_price_hash_mismatch:{path.name}")
        if int(payload.get("chunk_count") or -1) != args.chunk_count:
            raise ValueError(f"stage4_sufficient_stats_partition_mismatch:{path.name}")

        index = int(payload.get("chunk_index"))
        if index in seen_indices:
            raise ValueError(f"stage4_sufficient_stats_duplicate_index:{index}")
        seen_indices.add(index)

        symbols = [str(value) for value in payload.get("symbols", [])]
        overlap = seen_symbols.intersection(symbols)
        if overlap:
            raise ValueError("stage4_sufficient_stats_symbol_overlap:" + ",".join(sorted(overlap)))
        seen_symbols.update(symbols)

        coverage = payload.get("replay_coverage")
        guard = payload.get("replay_guard_review")
        if not isinstance(coverage, dict) or not isinstance(guard, dict):
            raise ValueError(f"stage4_sufficient_stats_replay_metadata_invalid:{index}")
        if guard.get("valid") is not True:
            guard_violations.extend(str(value) for value in (guard.get("violations") or []))
        replay_error_count += int(guard.get("replay_error_count") or 0)
        coverage_complete = coverage_complete and guard.get("coverage_complete") is True
        details = coverage.get("details")
        if isinstance(details, list):
            coverage_details.extend(item for item in details if isinstance(item, dict))
        else:
            coverage_complete = False
        symbols_requested += int(coverage.get("symbols_requested") or 0)
        symbols_with_snapshots += int(coverage.get("symbols_with_snapshots") or 0)
        snapshot_count += int(coverage.get("snapshots") or 0)

        structure = payload.get("structure_stats") or {}
        structure_claim_count += int(structure.get("claim_count") or 0)
        structure_fit_sum += float(structure.get("fit_sum") or 0.0)
        structure_fit_count += int(structure.get("fit_count") or 0)
        prospective_structure += int(structure.get("prospective_resolved_count") or 0)
        for key, value in (structure.get("resolution_counts") or {}).items():
            structure_resolution[str(key)] += int(value)
        for row in structure.get("by_partition_stage_degree_role_resolution") or []:
            key = (
                row.get("partition"),
                row.get("wave_stage"),
                row.get("degree"),
                row.get("scenario_role"),
                row.get("structure_resolution"),
            )
            structure_grouped[key] += int(row.get("N") or 0)

        projection_claim_count += int(payload.get("projection_claim_count") or 0)
        route_claim_count += int(payload.get("route_claim_count") or 0)
        projection_outcome_rows += int(payload.get("projection_outcome_rows") or 0)
        route_outcome_rows += int(payload.get("route_outcome_rows") or 0)
        prospective_mature += int(payload.get("prospective_mature_outcome_count") or 0)

        for row in payload.get("projection_groups") or []:
            projection_groups.add(tuple(row.get(field) for field in projection_fields))
        for row in payload.get("route_groups") or []:
            route_groups.add(tuple(row.get(field) for field in route_fields))

        for row in payload.get("projection_daily") or []:
            key = tuple(row.get(field) for field in projection_fields) + (
                row.get("metric"),
                str(row.get("date")),
            )
            projection_daily[key][0] += float(row.get("sum") or 0.0)
            projection_daily[key][1] += int(row.get("count") or 0)
        for row in payload.get("route_daily") or []:
            key = tuple(row.get(field) for field in route_fields) + (
                row.get("metric"),
                str(row.get("date")),
            )
            route_daily[key][0] += float(row.get("sum") or 0.0)
            route_daily[key][1] += int(row.get("count") or 0)

    if seen_indices != set(range(args.chunk_count)):
        raise ValueError("stage4_sufficient_stats_index_set_incomplete")
    if seen_symbols != price_meta["symbols"]:
        missing = sorted(price_meta["symbols"] - seen_symbols)
        extra = sorted(seen_symbols - price_meta["symbols"])
        raise ValueError(
            f"stage4_sufficient_stats_universe_mismatch:missing={missing[:20]}:extra={extra[:20]}"
        )
    if symbols_requested != len(price_meta["symbols"]):
        raise ValueError(
            f"replay_universe_incomplete:{symbols_requested}:{len(price_meta['symbols'])}"
        )

    guard_valid = not guard_violations and replay_error_count == 0 and coverage_complete
    if not guard_valid:
        raise ValueError(
            "historical_replay_guard_violation:"
            + ";".join(guard_violations[:100])
        )

    structure_group_rows = []
    fields = (
        "partition",
        "wave_stage",
        "degree",
        "scenario_role",
        "structure_resolution",
    )
    for key in sorted(structure_grouped, key=_sort_key):
        row = {field: key[idx] for idx, field in enumerate(fields)}
        row["N"] = int(structure_grouped[key])
        structure_group_rows.append(row)

    projection_summary = _summary_rows(
        group_fields=projection_fields,
        groups=projection_groups,
        daily_acc={key: (float(value[0]), int(value[1])) for key, value in projection_daily.items()},
        kind="projection",
        config=config,
    )
    route_summary = _summary_rows(
        group_fields=route_fields,
        groups=route_groups,
        daily_acc={key: (float(value[0]), int(value[1])) for key, value in route_daily.items()},
        kind="route",
        config=config,
    )

    evidence_policy = {
        "legacy_data_can_support_promotion": False,
        "formal_claims_require_available_from_after_freeze": True,
        "prospective_unspent_mature_outcomes": int(prospective_mature),
        "prospective_unspent_resolved_structure_claims": int(prospective_structure),
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
            "source_commit": args.source_commit,
            "price_source_sha256": price_hash,
            "price_row_count": int(price_meta["row_count"]),
            "price_symbol_count": len(price_meta["symbols"]),
            "price_first_date": price_meta["first_date"],
            "price_last_date": price_meta["last_date"],
            "stable_start": config.stable_start,
            "rules_frozen_through": config.rules_frozen_through,
        },
        "replay": {
            "execution_mode": "parallel_chunks",
            "aggregation_mode": "exact_daily_sufficient_statistics",
            "chunk_count": int(args.chunk_count),
            "symbols_requested": int(symbols_requested),
            "symbols_with_snapshots": int(symbols_with_snapshots),
            "snapshot_count": int(snapshot_count),
            "failures_are_missing_evidence_not_imputed": True,
            "guard_review": {
                "valid": True,
                "violation_count": 0,
                "violations": [],
                "coverage_complete": True,
                "coverage_violation_count": 0,
                "replay_error_count": 0,
                "replay_errors_are_missing_evidence_not_imputed": True,
                "snapshot_guard_verified_per_chunk": True,
            },
            "source_workflow_run_id": int(args.source_workflow_run_id),
        },
        "coverage": {
            "routed_snapshots": int(snapshot_count),
            "structure_claims": int(structure_claim_count),
            "projection_claims": int(projection_claim_count),
            "route_review_claims": int(route_claim_count),
            "projection_outcome_rows": int(projection_outcome_rows),
            "route_outcome_rows": int(route_outcome_rows),
            "cross_system_rows": 0,
            "context_alpha_rows": 0,
        },
        "structure_summary": {
            "claim_count": int(structure_claim_count),
            "resolution_counts": dict(sorted(structure_resolution.items())),
            "resolved_structural_fit_mean": (
                float(structure_fit_sum / structure_fit_count)
                if structure_fit_count
                else None
            ),
            "by_partition_stage_degree_role_resolution": structure_group_rows,
        },
        "projection_summary": projection_summary,
        "route_summary": route_summary,
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
    _atomic_json(output_path, result)

    print(json.dumps({
        "technical_stage_status": result["technical_stage_status"],
        "empirical_promotion_status": result["empirical_promotion_status"],
        "source": result["source"],
        "replay": result["replay"],
        "coverage": result["coverage"],
        "structure_resolution_counts": result["structure_summary"]["resolution_counts"],
        "projection_summary_rows": len(result["projection_summary"]),
        "route_summary_rows": len(result["route_summary"]),
        "output": str(output_path),
    }, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
