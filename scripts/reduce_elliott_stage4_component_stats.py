#!/usr/bin/env python3
from __future__ import annotations

"""Reduce one Stage-4 validation component to exact sufficient statistics."""

import argparse
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


def _norm(value: object) -> object:
    return value if value is not None else None


def _daily_metric_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_fields: tuple[str, ...],
    metric_specs: tuple[tuple[str, str], ...],
) -> list[dict[str, Any]]:
    acc: dict[tuple[object, ...], list[float]] = defaultdict(lambda: [0.0, 0.0])
    for row in rows:
        date = row.get("available_from")
        if not date:
            continue
        group = tuple(_norm(row.get(field)) for field in group_fields)
        for output_metric, source_metric in metric_specs:
            raw = row.get(source_metric)
            if output_metric in {"projection_hit_numeric", "review_correct_numeric"}:
                if raw is True:
                    value = 1.0
                elif raw is False:
                    value = 0.0
                else:
                    continue
            else:
                if raw is None:
                    continue
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    continue
            key = (*group, output_metric, str(date))
            acc[key][0] += value
            acc[key][1] += 1.0

    result: list[dict[str, Any]] = []
    for key in sorted(acc, key=lambda item: tuple("" if value is None else str(value) for value in item)):
        *group, metric, date = key
        total, count = acc[key]
        record = {field: group[idx] for idx, field in enumerate(group_fields)}
        record.update({
            "metric": metric,
            "date": date,
            "sum": float(total),
            "count": int(count),
        })
        result.append(record)
    return result


def _group_rows(rows: Iterable[Mapping[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    keys = {
        tuple(_norm(row.get(field)) for field in fields)
        for row in rows
    }
    result: list[dict[str, Any]] = []
    for key in sorted(keys, key=lambda item: tuple("" if value is None else str(value) for value in item)):
        result.append({field: key[idx] for idx, field in enumerate(fields)})
    return result


def _structure_stats(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    resolution = Counter()
    grouped = Counter()
    fit_sum = 0.0
    fit_count = 0
    prospective_resolved = 0
    claim_count = 0

    for row in rows:
        claim_count += 1
        res = str(row.get("structure_resolution") or "missing")
        resolution[res] += 1
        grouped[(
            row.get("partition"),
            row.get("wave_stage"),
            row.get("degree"),
            row.get("scenario_role"),
            row.get("structure_resolution"),
        )] += 1
        if row.get("structural_fit") is not None:
            fit_sum += float(row["structural_fit"])
            fit_count += 1
        if (
            row.get("partition") == "prospective_unspent"
            and row.get("structure_resolution") != "unresolved"
        ):
            prospective_resolved += 1

    grouped_rows = []
    fields = (
        "partition",
        "wave_stage",
        "degree",
        "scenario_role",
        "structure_resolution",
    )
    for key in sorted(grouped, key=lambda item: tuple("" if value is None else str(value) for value in item)):
        record = {field: key[idx] for idx, field in enumerate(fields)}
        record["N"] = int(grouped[key])
        grouped_rows.append(record)

    return {
        "claim_count": int(claim_count),
        "resolution_counts": dict(sorted(resolution.items())),
        "fit_sum": float(fit_sum),
        "fit_count": int(fit_count),
        "prospective_resolved_count": int(prospective_resolved),
        "by_partition_stage_degree_role_resolution": grouped_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    with gzip.open(input_path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema_version") != "elliott_vnext_stage4_validation_component_v1":
        raise ValueError("stage4_validation_component_schema_invalid")

    structure = payload.get("structure_validation")
    projection = payload.get("projection_outcomes")
    routes = payload.get("route_outcomes")
    coverage = payload.get("replay_coverage")
    guard = payload.get("replay_guard_review")
    if not isinstance(structure, list) or not isinstance(projection, list) or not isinstance(routes, list):
        raise ValueError("stage4_validation_component_rows_invalid")
    if not isinstance(coverage, dict) or not isinstance(guard, dict):
        raise ValueError("stage4_validation_component_guard_invalid")

    projection_group = (
        "partition",
        "horizon_sessions",
        "wave_role",
        "projection_type",
        "degree",
    )
    route_group = (
        "partition",
        "horizon_sessions",
        "review_context",
        "wave_stage",
        "degree",
    )

    result = {
        "schema_version": "elliott_vnext_stage4_sufficient_stats_v1",
        "source_commit": payload.get("source_commit"),
        "price_source_sha256": payload.get("price_source_sha256"),
        "chunk_index": payload.get("chunk_index"),
        "chunk_count": payload.get("chunk_count"),
        "symbols": payload.get("symbols") or [],
        "replay_coverage": coverage,
        "replay_guard_review": guard,
        "structure_stats": _structure_stats(structure),
        "projection_claim_count": int(payload.get("projection_claim_count") or 0),
        "route_claim_count": int(payload.get("route_claim_count") or 0),
        "projection_outcome_rows": len(projection),
        "route_outcome_rows": len(routes),
        "prospective_mature_outcome_count": sum(
            1
            for row in [*projection, *routes]
            if row.get("partition") == "prospective_unspent"
            and row.get("outcome_available") is True
        ),
        "projection_groups": _group_rows(projection, projection_group),
        "route_groups": _group_rows(routes, route_group),
        "projection_daily": _daily_metric_rows(
            projection,
            group_fields=projection_group,
            metric_specs=(
                ("projection_hit_numeric", "projection_hit"),
                ("signed_forward_return", "signed_forward_return"),
            ),
        ),
        "route_daily": _daily_metric_rows(
            routes,
            group_fields=route_group,
            metric_specs=(
                ("review_correct_numeric", "review_correct"),
                ("signed_forward_return", "signed_forward_return"),
            ),
        ),
        "research_only": True,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, "wt", encoding="utf-8", compresslevel=6) as handle:
        json.dump(result, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    print(json.dumps({
        "chunk_index": result["chunk_index"],
        "structure_claims": result["structure_stats"]["claim_count"],
        "projection_daily_rows": len(result["projection_daily"]),
        "route_daily_rows": len(result["route_daily"]),
        "output": str(output_path),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
