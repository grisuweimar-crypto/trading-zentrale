from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping

from .fundamental_change import is_pit_usable, metric_for_row


def measure_concept_coverage(
    rows: Iterable[Mapping[str, Any]],
    *,
    metric_families: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Measure mapping coverage without using market outcomes.

    Coverage is descriptive only. It never selects concepts, thresholds, or domains.
    Custom-taxonomy facts remain visible as unmapped rather than being guessed into
    a standard metric family.
    """
    materialized = [dict(row) for row in rows]
    usable = [row for row in materialized if is_pit_usable(row)]
    us_gaap = [row for row in usable if str(row.get("taxonomy") or "").lower() == "us-gaap"]

    mapped_rows: list[tuple[dict[str, Any], str]] = []
    unmapped_us_gaap = []
    custom_taxonomy = []
    for row in usable:
        metric = metric_for_row(row, metric_families)
        if metric is not None:
            mapped_rows.append((row, metric))
        elif str(row.get("taxonomy") or "").lower() == "us-gaap":
            unmapped_us_gaap.append(row)
        else:
            custom_taxonomy.append(row)

    entities = {str(row.get("cik")) for row in usable if row.get("cik")}
    metric_entities: dict[str, set[str]] = defaultdict(set)
    metric_concepts: dict[str, Counter[str]] = defaultdict(Counter)
    metric_rows = Counter()
    for row, metric in mapped_rows:
        metric_rows[metric] += 1
        if row.get("cik"):
            metric_entities[metric].add(str(row.get("cik")))
        metric_concepts[metric][str(row.get("concept") or "")] += 1

    total_usable = len(usable)
    total_us_gaap = len(us_gaap)
    mapped_count = len(mapped_rows)
    return {
        "status": "KNOWN",
        "outcome_research": "NOT_RUN",
        "total_input_rows": len(materialized),
        "pit_usable_rows": total_usable,
        "entity_count": len(entities),
        "us_gaap_usable_rows": total_us_gaap,
        "mapped_rows": mapped_count,
        "mapped_share_of_usable": (mapped_count / total_usable) if total_usable else None,
        "mapped_share_of_us_gaap": (mapped_count / total_us_gaap) if total_us_gaap else None,
        "unmapped_us_gaap_rows": len(unmapped_us_gaap),
        "custom_taxonomy_unmapped_rows": len(custom_taxonomy),
        "metric_row_counts": dict(sorted(metric_rows.items())),
        "metric_entity_counts": {
            metric: len(metric_entities.get(metric, set()))
            for metric in sorted(metric_families)
        },
        "metric_concept_counts": {
            metric: dict(sorted(metric_concepts.get(metric, Counter()).items()))
            for metric in sorted(metric_families)
        },
        "unmapped_us_gaap_concepts": dict(
            sorted(Counter(str(row.get("concept") or "") for row in unmapped_us_gaap).items())
        ),
        "custom_taxonomies": dict(
            sorted(Counter(str(row.get("taxonomy") or "") for row in custom_taxonomy).items())
        ),
        "direction": "UNASSIGNED",
        "reason_codes": [],
    }
