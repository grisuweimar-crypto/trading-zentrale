#!/usr/bin/env python3
from __future__ import annotations

"""Aggregate compact Stage-5 chunk statistics into the canonical Stage-5 report."""

import argparse
from collections import Counter, defaultdict
from hashlib import sha256
import gzip
import json
from math import ceil
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from scanner.research.elliott_vnext.validation import effective_block_length


SCHEMA_VERSION = "elliott_vnext_stage5_incremental_v1"
STAGE = "STAGE_5_INCREMENTAL_CROSS_SYSTEM"
DEFAULT_OUTPUT = "artifacts/research/elliott_vnext_stage5_incremental.json"
TRANSITION_FIELDS = (
    "transition",
    "partition",
    "horizon_sessions",
    "review_orientation",
    "wave_stage",
    "degree",
    "elliott_direction",
)
LEAD_LAG_FIELDS = (
    "partition",
    "transition",
    "wave_stage",
    "degree",
    "elliott_direction",
    "review_orientation",
)


def _stable_seed(base: int, *parts: object) -> int:
    digest = sha256("|".join(map(str, parts)).encode("utf-8")).digest()
    return int((base + int.from_bytes(digest[:4], "big")) % (2**32 - 1))


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


def _sort_key(values: Sequence[object]) -> tuple[str, ...]:
    return tuple("" if value is None else str(value) for value in values)


def _mean_from_daily(daily: Mapping[str, tuple[float, int]]) -> tuple[int, float | None]:
    n = int(sum(count for _, count in daily.values()))
    if n <= 0:
        return 0, None
    total = float(sum(total for total, _ in daily.values()))
    return n, total / n


def _support_regions(
    values: Mapping[str, tuple[float, int]],
    baseline_dates: Sequence[str],
    block_length: int,
) -> int:
    positions = {day: idx for idx, day in enumerate(baseline_dates)}
    observed = sorted(
        positions[day]
        for day, (_, count) in values.items()
        if count > 0 and day in positions
    )
    count = 0
    last: int | None = None
    for position in observed:
        if last is None or position - last >= block_length:
            count += 1
            last = position
    return count


def bootstrap_difference_daily(
    values: Mapping[str, tuple[float, int]],
    baseline: Mapping[str, tuple[float, int]],
    *,
    metric: str,
    horizon: int,
    bootstrap_reps: int = 1000,
    random_seed: int = 20260925,
    min_support_regions: int = 2,
    seed_key: Sequence[object] = (),
) -> dict[str, object]:
    """Exact daily-statistics equivalent of 6G block_bootstrap_difference.

    The original implementation materializes every sampled observation date for
    every bootstrap replicate.  Here the same circular blocks are represented by
    precomputed block sums/counts, so one replicate needs only a handful of array
    lookups.  RNG seeds, block starts, truncation of the final block and weighted
    observation means remain unchanged.
    """

    v_n, v_mean = _mean_from_daily(values)
    b_n, b_mean = _mean_from_daily(baseline)
    dates = sorted(day for day, (_, count) in baseline.items() if count > 0)
    block_length = effective_block_length(horizon)
    regions = _support_regions(values, dates, block_length)
    result: dict[str, object] = {
        "N": v_n,
        "baseline_N": b_n,
        "difference": (
            float(v_mean - b_mean)
            if v_mean is not None and b_mean is not None
            else None
        ),
        "difference_95": None,
        "block_length": block_length,
        "support_regions": regions,
        "robust_interval_available": False,
    }
    if (
        v_n <= 0
        or b_n <= 0
        or bootstrap_reps <= 0
        or len(dates) < 2
        or regions < min_support_regions
    ):
        return result

    n = len(dates)
    span = min(block_length, n)
    draws_per_rep = int(ceil(n / block_length))
    full_blocks = n // span
    remainder = n - full_blocks * span
    if full_blocks == draws_per_rep and remainder == 0:
        full_draws = draws_per_rep
    else:
        full_draws = max(0, draws_per_rep - 1)

    v_sum = np.asarray([float(values.get(day, (0.0, 0))[0]) for day in dates], dtype=float)
    v_count = np.asarray([int(values.get(day, (0.0, 0))[1]) for day in dates], dtype=np.int64)
    b_sum = np.asarray([float(baseline.get(day, (0.0, 0))[0]) for day in dates], dtype=float)
    b_count = np.asarray([int(baseline.get(day, (0.0, 0))[1]) for day in dates], dtype=np.int64)

    offsets = np.arange(span, dtype=np.int64)
    block_indices = (np.arange(n, dtype=np.int64)[:, None] + offsets[None, :]) % n
    v_block_sum = v_sum[block_indices].sum(axis=1)
    v_block_count = v_count[block_indices].sum(axis=1)
    b_block_sum = b_sum[block_indices].sum(axis=1)
    b_block_count = b_count[block_indices].sum(axis=1)

    if remainder:
        partial_indices = block_indices[:, :remainder]
        v_partial_sum = v_sum[partial_indices].sum(axis=1)
        v_partial_count = v_count[partial_indices].sum(axis=1)
        b_partial_sum = b_sum[partial_indices].sum(axis=1)
        b_partial_count = b_count[partial_indices].sum(axis=1)
    else:
        v_partial_sum = v_block_sum
        v_partial_count = v_block_count
        b_partial_sum = b_block_sum
        b_partial_count = b_block_count

    rng = np.random.default_rng(
        _stable_seed(random_seed, *seed_key, "diff", metric, horizon)
    )
    chosen = rng.integers(0, n, size=(bootstrap_reps, draws_per_rep))

    if full_draws:
        full = chosen[:, :full_draws]
        rep_v_sum = v_block_sum[full].sum(axis=1)
        rep_v_count = v_block_count[full].sum(axis=1)
        rep_b_sum = b_block_sum[full].sum(axis=1)
        rep_b_count = b_block_count[full].sum(axis=1)
    else:
        rep_v_sum = np.zeros(bootstrap_reps, dtype=float)
        rep_v_count = np.zeros(bootstrap_reps, dtype=np.int64)
        rep_b_sum = np.zeros(bootstrap_reps, dtype=float)
        rep_b_count = np.zeros(bootstrap_reps, dtype=np.int64)

    if remainder:
        last = chosen[:, full_draws]
        rep_v_sum = rep_v_sum + v_partial_sum[last]
        rep_v_count = rep_v_count + v_partial_count[last]
        rep_b_sum = rep_b_sum + b_partial_sum[last]
        rep_b_count = rep_b_count + b_partial_count[last]

    valid = (rep_v_count > 0) & (rep_b_count > 0)
    if np.any(valid):
        estimates = (
            rep_v_sum[valid] / rep_v_count[valid]
            - rep_b_sum[valid] / rep_b_count[valid]
        )
        lo, hi = np.quantile(estimates.astype(float), [0.025, 0.975])
        result["difference_95"] = [float(lo), float(hi)]
        result["robust_interval_available"] = True
    return result


def _lead_lag_summary(
    counts: Mapping[tuple[object, ...], int],
) -> list[dict[str, object]]:
    grouped: dict[tuple[object, ...], list[tuple[int, int]]] = defaultdict(list)
    for key, count in counts.items():
        base = key[:-1]
        offset = int(key[-1])
        grouped[base].append((offset, int(count)))

    rows: list[dict[str, object]] = []
    for base in sorted(grouped, key=_sort_key):
        weighted: list[int] = []
        scanner_leads = 0
        same = 0
        elliott_leads = 0
        total = 0
        for offset, count in grouped[base]:
            weighted.extend([offset] * count)
            total += count
            scanner_leads += count if offset < 0 else 0
            same += count if offset == 0 else 0
            elliott_leads += count if offset > 0 else 0
        row = {
            field: base[index]
            for index, field in enumerate(LEAD_LAG_FIELDS)
        }
        row.update({
            "N": total,
            "median_relative_session": (
                float(np.median(np.asarray(weighted, dtype=float)))
                if weighted
                else None
            ),
            "scanner_leads_N": scanner_leads,
            "same_session_N": same,
            "elliott_leads_N": elliott_leads,
            "post_event_performance_use_allowed": False,
            "research_only": True,
        })
        rows.append(row)
    return rows


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-dir", required=True)
    parser.add_argument("--chunk-count", type=int, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-workflow-run-id", type=int, required=True)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--bootstrap-reps", type=int, default=1000)
    args = parser.parse_args()

    chunks_dir = Path(args.chunks_dir)
    paths = sorted(chunks_dir.glob("stage5-chunk-*.json.gz"))
    if len(paths) != args.chunk_count:
        raise ValueError(f"stage5_chunk_count_mismatch:{len(paths)}:{args.chunk_count}")

    seen_indices: set[int] = set()
    seen_symbols: set[str] = set()
    history_hashes: set[str] = set()
    price_hashes: set[str] = set()
    scanner_rows = 0
    elliott_events = 0
    route_claims = 0
    route_outcomes = 0
    mature_outcomes = 0
    prospective_mature_outcomes = 0
    events_without_scanner_anchor = 0
    lead_lag_rows = 0

    # Indexed once while reading chunks.  Key:
    # class tuple -> metric -> arm -> date -> [sum, count].
    # This avoids the previous O(number_of_classes × all_daily_rows) rescan.
    daily_index: dict[
        tuple[object, ...],
        dict[str, dict[str, dict[str, list[float]]]],
    ] = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))
        )
    )
    lead_lag_counts: Counter[tuple[object, ...]] = Counter()

    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("schema_version") != "elliott_vnext_stage5_chunk_v1":
            raise ValueError(f"stage5_chunk_schema_invalid:{path.name}")
        if payload.get("source_commit") != args.source_commit:
            raise ValueError(f"stage5_chunk_source_commit_mismatch:{path.name}")
        if int(payload.get("source_workflow_run_id") or -1) != args.source_workflow_run_id:
            raise ValueError(f"stage5_chunk_source_run_mismatch:{path.name}")
        if int(payload.get("chunk_count") or -1) != args.chunk_count:
            raise ValueError(f"stage5_chunk_partition_mismatch:{path.name}")
        if payload.get("research_only") is not True:
            raise ValueError(f"stage5_chunk_not_research_only:{path.name}")

        index = int(payload.get("chunk_index"))
        if index in seen_indices:
            raise ValueError(f"stage5_chunk_duplicate_index:{index}")
        seen_indices.add(index)

        symbols = [str(value) for value in payload.get("symbols", [])]
        overlap = seen_symbols.intersection(symbols)
        if overlap:
            raise ValueError("stage5_chunk_symbol_overlap:" + ",".join(sorted(overlap)))
        seen_symbols.update(symbols)

        history_hashes.add(str(payload.get("history_sha256") or ""))
        price_hashes.add(str(payload.get("price_sha256") or ""))
        scanner_rows += int((payload.get("scanner_coverage") or {}).get("rows") or 0)
        elliott_events += int(payload.get("elliott_event_count") or 0)
        route_claims += int(payload.get("route_claim_count") or 0)
        route_outcomes += int(payload.get("route_outcome_rows") or 0)
        mature_outcomes += int(payload.get("mature_route_outcomes") or 0)
        prospective_mature_outcomes += int(
            payload.get("prospective_unspent_mature_route_outcomes") or 0
        )
        transition_coverage = payload.get("transition_coverage") or {}
        events_without_scanner_anchor += int(
            transition_coverage.get("events_without_scanner_anchor") or 0
        )
        lead_lag_rows += int(transition_coverage.get("lead_lag_rows") or 0)

        for row in payload.get("transition_daily_sufficient_stats") or []:
            base = tuple(row.get(field) for field in TRANSITION_FIELDS)
            metric = str(row.get("metric"))
            arm = str(row.get("arm"))
            day = str(row.get("event_date"))
            aggregate = daily_index[base][metric][arm][day]
            aggregate[0] += float(row.get("sum") or 0.0)
            aggregate[1] += int(row.get("count") or 0)

        for row in payload.get("lead_lag_sufficient_stats") or []:
            key = tuple(row.get(field) for field in LEAD_LAG_FIELDS) + (
                int(row.get("relative_session")),
            )
            lead_lag_counts[key] += int(row.get("N") or 0)

    if seen_indices != set(range(args.chunk_count)):
        raise ValueError("stage5_chunk_index_set_incomplete")
    if len(history_hashes) != 1 or "" in history_hashes:
        raise ValueError("stage5_history_hash_not_unique")
    if len(price_hashes) != 1 or "" in price_hashes:
        raise ValueError("stage5_price_hash_not_unique")

    validation_rows: list[dict[str, object]] = []
    for base in sorted(daily_index, key=_sort_key):
        values = {
            field: base[index]
            for index, field in enumerate(TRANSITION_FIELDS)
        }
        transition = str(values["transition"])
        horizon = int(values["horizon_sessions"])
        metrics = (
            "signed_forward_return",
            "review_correct_numeric",
        )
        stats: dict[str, dict[str, object]] = {}
        valid_comparison = True
        for metric in metrics:
            metric_arms = daily_index[base].get(metric, {})
            with_daily = {
                day: (float(aggregate[0]), int(aggregate[1]))
                for day, aggregate in metric_arms.get("with_transition", {}).items()
            }
            without_daily = {
                day: (float(aggregate[0]), int(aggregate[1]))
                for day, aggregate in metric_arms.get("without_transition", {}).items()
            }

            if not with_daily or not without_daily:
                valid_comparison = False
                break
            stats[metric] = bootstrap_difference_daily(
                with_daily,
                without_daily,
                metric=metric,
                horizon=horizon,
                bootstrap_reps=args.bootstrap_reps,
                seed_key=(
                    "stage5_transition_" + (
                        "signed_return"
                        if metric == "signed_forward_return"
                        else "review_correct"
                    ),
                    transition,
                    values["partition"],
                    values["review_orientation"],
                    values["wave_stage"],
                    values["degree"],
                    values["elliott_direction"],
                ),
            )
        if not valid_comparison:
            continue

        validation_rows.append({
            **values,
            "event_time_transition_window": [-20, 0],
            "signed_return_difference_vs_same_class_without_transition": stats["signed_forward_return"],
            "review_correctness_difference_vs_same_class_without_transition": stats["review_correct_numeric"],
            "formal_promotion_evidence": values["partition"] == "prospective_unspent",
            "rule_selected_on_this_partition": False,
            "research_only": True,
        })

    result: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "stage": STAGE,
        "technical_stage_status": "COMPLETE",
        "empirical_promotion_status": "NOT_PROMOTED",
        "source": {
            "source_commit": args.source_commit,
            "source_workflow_run_id": args.source_workflow_run_id,
            "history_sha256": next(iter(history_hashes)),
            "price_sha256": next(iter(price_hashes)),
            "chunk_count": args.chunk_count,
            "symbol_count": len(seen_symbols),
            "rules_frozen_through": "2026-09-25",
        },
        "coverage": {
            "scanner_rows": scanner_rows,
            "elliott_events": elliott_events,
            "route_claims": route_claims,
            "route_outcome_rows": route_outcomes,
            "mature_route_outcomes": mature_outcomes,
            "prospective_unspent_mature_route_outcomes": prospective_mature_outcomes,
            "events_without_scanner_anchor": events_without_scanner_anchor,
            "lead_lag_rows": lead_lag_rows,
            "transition_incremental_rows": len(validation_rows),
        },
        "lead_lag_summary": _lead_lag_summary(lead_lag_counts),
        "transition_incremental_validation": validation_rows,
        "model_relation_validation": {
            "status": "not_testable_no_historical_frozen_directional_model_claims_supplied",
            "claim_count": 0,
            "relation_count": 0,
            "relation_counts": {},
            "validation_rows": [],
            "claims_retrojected": False,
            "research_only": True,
        },
        "boundaries": {
            "present_day_model_claims_retrojected": False,
            "post_event_scanner_rows_used_for_incremental_test": False,
            "scanner_thresholds_reoptimized": False,
            "elliott_recounted": False,
            "legacy_data_can_support_promotion": False,
            "automatic_promotion_allowed": False,
            "changes_universal_stance": False,
            "changes_portfolio_action": False,
            "trade_decision": None,
            "order_instruction": None,
            "research_only": True,
        },
        "interpretation": {
            "historical_transition_results_are_descriptive": True,
            "prospective_unspent_results_may_accumulate_evidence": True,
            "confirmation_redundancy_rescue_conflict_require_explicit_asof_model_claims": True,
            "absence_of_model_claims_is_not_scanner_disagreement": True,
            "overlapping_forward_windows_are_iid": False,
            "baseline_rule": "same_partition_horizon_review_orientation_wave_stage_degree_direction_without_same_transition",
            "lead_lag_positive_offsets_are_descriptive_only": True,
        },
    }
    result["stage5_result_hash"] = _canonical_hash(result)
    _atomic_json(Path(args.output), result)
    print(json.dumps({
        "technical_stage_status": result["technical_stage_status"],
        "empirical_promotion_status": result["empirical_promotion_status"],
        "source": result["source"],
        "coverage": result["coverage"],
        "lead_lag_summary_rows": len(result["lead_lag_summary"]),
        "transition_incremental_rows": len(validation_rows),
        "output": args.output,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
