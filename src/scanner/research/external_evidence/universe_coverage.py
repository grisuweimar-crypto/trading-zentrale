from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping


def build_sec_ticker_map(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Build an exact current SEC ticker lookup.

    This helper deliberately performs no fuzzy company-name matching and no suffix
    stripping. An unmatched scanner symbol remains unresolved rather than being
    attached to a potentially different security/ADR.
    """
    mapping: dict[str, dict[str, Any]] = {}
    for raw in payload.values():
        if not isinstance(raw, Mapping):
            continue
        ticker = str(raw.get("ticker") or "").strip().upper()
        cik = raw.get("cik_str")
        if not ticker or cik is None:
            continue
        record = {
            "ticker": ticker,
            "cik": str(cik).zfill(10),
            "sec_title": raw.get("title"),
        }
        existing = mapping.get(ticker)
        if existing is not None and existing != record:
            raise ValueError(f"Conflicting SEC ticker mapping for {ticker}")
        mapping[ticker] = record
    return mapping


def exact_sec_match(symbol: str, ticker_map: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    normalized = str(symbol or "").strip().upper()
    match = ticker_map.get(normalized)
    if match is None:
        return {
            "symbol": normalized,
            "sec_match_status": "UNKNOWN",
            "reason_codes": ["NO_EXACT_SEC_TICKER_MATCH"],
        }
    return {
        "symbol": normalized,
        "sec_match_status": "KNOWN",
        "cik": match.get("cik"),
        "sec_title": match.get("sec_title"),
        "reason_codes": [],
    }


def aggregate_company_coverage(company_rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in company_rows]
    match_status = Counter(str(row.get("sec_match_status") or "UNKNOWN") for row in rows)
    processing_status = Counter(str(row.get("processing_status") or "NOT_RUN") for row in rows)
    metric_entity_counts: Counter[str] = Counter()
    metric_row_counts: Counter[str] = Counter()
    feature_entity_counts: Counter[str] = Counter()
    feature_row_counts: Counter[str] = Counter()
    unmapped_concepts: Counter[str] = Counter()
    custom_taxonomies: Counter[str] = Counter()

    total_usable = 0
    total_mapped = 0
    total_us_gaap = 0
    total_unmapped_us_gaap = 0
    total_custom = 0
    research_ready = 0

    for row in rows:
        concept = row.get("concept_coverage") or {}
        if isinstance(concept, Mapping):
            total_usable += int(concept.get("pit_usable_rows") or 0)
            total_mapped += int(concept.get("mapped_rows") or 0)
            total_us_gaap += int(concept.get("us_gaap_usable_rows") or 0)
            total_unmapped_us_gaap += int(concept.get("unmapped_us_gaap_rows") or 0)
            total_custom += int(concept.get("custom_taxonomy_unmapped_rows") or 0)
            for metric, count in (concept.get("metric_row_counts") or {}).items():
                metric_row_counts[str(metric)] += int(count or 0)
                if int(count or 0) > 0:
                    metric_entity_counts[str(metric)] += 1
            for name, count in (concept.get("unmapped_us_gaap_concepts") or {}).items():
                unmapped_concepts[str(name)] += int(count or 0)
            for name, count in (concept.get("custom_taxonomies") or {}).items():
                custom_taxonomies[str(name)] += int(count or 0)

        features = row.get("feature_counts") or {}
        if isinstance(features, Mapping):
            for feature, count in features.items():
                feature_row_counts[str(feature)] += int(count or 0)
                if int(count or 0) > 0:
                    feature_entity_counts[str(feature)] += 1

        if row.get("research_ready") is True:
            research_ready += 1

    return {
        "company_count": len(rows),
        "sec_match_status_counts": dict(sorted(match_status.items())),
        "processing_status_counts": dict(sorted(processing_status.items())),
        "research_ready_company_count": research_ready,
        "pit_usable_fact_rows": total_usable,
        "mapped_fact_rows": total_mapped,
        "us_gaap_usable_fact_rows": total_us_gaap,
        "unmapped_us_gaap_rows": total_unmapped_us_gaap,
        "custom_taxonomy_unmapped_rows": total_custom,
        "mapped_share_of_usable": (total_mapped / total_usable) if total_usable else None,
        "mapped_share_of_us_gaap": (total_mapped / total_us_gaap) if total_us_gaap else None,
        "metric_entity_counts": dict(sorted(metric_entity_counts.items())),
        "metric_row_counts": dict(sorted(metric_row_counts.items())),
        "feature_entity_counts": dict(sorted(feature_entity_counts.items())),
        "feature_row_counts": dict(sorted(feature_row_counts.items())),
        "top_unmapped_us_gaap_concepts": dict(unmapped_concepts.most_common(50)),
        "custom_taxonomies": dict(custom_taxonomies.most_common(50)),
        "outcome_research": "NOT_RUN",
        "direction": "UNASSIGNED",
    }
