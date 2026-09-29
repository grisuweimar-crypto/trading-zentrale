"""QM-B historical listing-metadata source assessment.

Research-only. The module validates the listing-metadata evidence contract and derives
field-level readiness without fetching or promoting any external data. It reuses the
Phase-8 source-governance concepts (coverage, PIT, access/license, publication
semantics) while keeping listing state, venue assignment, market tradability and
project investability distinct.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "qm_b_listing_metadata_sources_v1"
DEFAULT_CONTRACT_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_listing_metadata_sources_v1.json"


class ListingMetadataEvidenceError(ValueError):
    """Raised when the QM-B listing-metadata evidence contract is inconsistent."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _require(value: Any, *, field: str) -> str:
    result = _clean(value)
    if not result:
        raise ListingMetadataEvidenceError(f"value_required:{field}")
    return result


def load_listing_metadata_contract(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_CONTRACT_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ListingMetadataEvidenceError(f"contract_unreadable:{target}") from exc
    validate_listing_metadata_contract(payload)
    return payload


def validate_listing_metadata_contract(payload: Mapping[str, Any]) -> None:
    if not isinstance(payload, Mapping):
        raise ListingMetadataEvidenceError("contract_must_be_object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ListingMetadataEvidenceError("contract_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ListingMetadataEvidenceError("contract_scope_invalid")

    field_model = payload.get("field_model")
    if not isinstance(field_model, Mapping):
        raise ListingMetadataEvidenceError("field_model_missing")
    required_fields = {
        "stable_instrument_identity",
        "venue_master",
        "venue_assignment",
        "listing_start",
        "listing_end",
        "symbol_change",
        "market_tradability",
        "project_investability",
    }
    if set(field_model) != required_fields:
        missing = sorted(required_fields - set(field_model))
        extra = sorted(set(field_model) - required_fields)
        raise ListingMetadataEvidenceError(
            "field_model_invalid:missing=" + ",".join(missing) + ":extra=" + ",".join(extra)
        )

    capability_values = set(payload.get("capability_status_values") or [])
    pit_values = set(payload.get("pit_status_values") or [])
    coverage_values = set(payload.get("coverage_status_values") or [])
    access_values = set(payload.get("access_status_values") or [])
    license_values = set(payload.get("license_status_values") or [])
    source_class_values = set(payload.get("source_class_values") or [])
    if not all((capability_values, pit_values, coverage_values, access_values, license_values, source_class_values)):
        raise ListingMetadataEvidenceError("status_value_sets_missing")

    rule = payload.get("strict_promotion_rule")
    if not isinstance(rule, Mapping):
        raise ListingMetadataEvidenceError("strict_promotion_rule_missing")
    if rule.get("current_only_mapping_is_sufficient") is not False:
        raise ListingMetadataEvidenceError("current_only_must_not_be_sufficient")
    if rule.get("event_date_without_publication_vintage_is_sufficient") is not False:
        raise ListingMetadataEvidenceError("event_date_without_vintage_must_not_be_sufficient")
    if rule.get("requires_explicit_publication_or_availability_semantics") is not True:
        raise ListingMetadataEvidenceError("publication_semantics_must_be_required")

    sources = payload.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ListingMetadataEvidenceError("sources_required")
    seen: set[str] = set()
    capability_fields = required_fields - {"stable_instrument_identity"}
    for index, source in enumerate(sources):
        if not isinstance(source, Mapping):
            raise ListingMetadataEvidenceError(f"source_must_be_object:{index}")
        source_id = _require(source.get("source_id"), field=f"sources[{index}].source_id")
        if source_id in seen:
            raise ListingMetadataEvidenceError(f"duplicate_source_id:{source_id}")
        seen.add(source_id)
        if source.get("source_class") not in source_class_values:
            raise ListingMetadataEvidenceError(f"source_class_invalid:{source_id}")
        if source.get("coverage_status") not in coverage_values:
            raise ListingMetadataEvidenceError(f"coverage_status_invalid:{source_id}")
        if source.get("pit_status") not in pit_values:
            raise ListingMetadataEvidenceError(f"pit_status_invalid:{source_id}")
        if source.get("access_status") not in access_values:
            raise ListingMetadataEvidenceError(f"access_status_invalid:{source_id}")
        if source.get("license_status") not in license_values:
            raise ListingMetadataEvidenceError(f"license_status_invalid:{source_id}")
        _require(source.get("provider"), field=f"sources[{index}].provider")
        _require(source.get("documentation_url"), field=f"sources[{index}].documentation_url")
        _require(source.get("publication_semantics"), field=f"sources[{index}].publication_semantics")
        capabilities = source.get("capabilities")
        if not isinstance(capabilities, Mapping) or set(capabilities) != capability_fields:
            raise ListingMetadataEvidenceError(f"capabilities_invalid:{source_id}")
        for field, status in capabilities.items():
            if status not in capability_values:
                raise ListingMetadataEvidenceError(f"capability_status_invalid:{source_id}:{field}:{status}")

        blockers = source.get("promotion_blockers")
        if not isinstance(blockers, list):
            raise ListingMetadataEvidenceError(f"promotion_blockers_must_be_list:{source_id}")
        if source.get("promotion_eligible") is True and blockers:
            raise ListingMetadataEvidenceError(f"promotion_eligible_with_blockers:{source_id}")

        # Fail closed: a source cannot claim field-level PIT safety while the source
        # itself is explicitly UNSAFE or UNKNOWN.
        if source.get("pit_status") in {"UNSAFE", "UNKNOWN"} and "PIT_SAFE" in set(capabilities.values()):
            raise ListingMetadataEvidenceError(f"pit_safe_capability_on_unsafe_source:{source_id}")

    assessment = payload.get("assessment")
    if not isinstance(assessment, Mapping):
        raise ListingMetadataEvidenceError("assessment_missing")
    if assessment.get("global_strict_listing_source_available_and_cleared") is not False:
        raise ListingMetadataEvidenceError("global_source_availability_claim_must_be_false")
    if int(assessment.get("globally_promotion_ready_source_count", -1)) != 0:
        raise ListingMetadataEvidenceError("global_promotion_ready_count_must_be_zero")
    if assessment.get("strict_listing_ledger_status") != "BLOCKED_SOURCE_GAP":
        raise ListingMetadataEvidenceError("strict_listing_ledger_status_invalid")


def _source_field_strict_ready(source: Mapping[str, Any], field: str, contract: Mapping[str, Any]) -> bool:
    rule = contract["strict_promotion_rule"]
    capabilities = source["capabilities"]
    return (
        capabilities.get(field) == rule["required_capability_status"]
        and source.get("pit_status") == rule["required_pit_status"]
        and source.get("access_status") in set(rule["allowed_access_status"])
        and source.get("license_status") in set(rule["allowed_license_status"])
        and source.get("promotion_eligible") is True
        and not source.get("promotion_blockers")
    )


def assess_listing_metadata_sources(
    contract: Mapping[str, Any] | None = None,
    *,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    payload = dict(contract) if contract is not None else load_listing_metadata_contract(contract_path)
    validate_listing_metadata_contract(payload)

    sources = [dict(source) for source in payload["sources"]]
    fields = list(payload["sources"][0]["capabilities"].keys())
    capability_counts: dict[str, Counter[str]] = {field: Counter() for field in fields}
    strict_ready_by_field: dict[str, list[str]] = defaultdict(list)
    source_rows: list[dict[str, Any]] = []

    for source in sources:
        source_id = str(source["source_id"])
        strict_ready_fields: list[str] = []
        for field in fields:
            status = str(source["capabilities"][field])
            capability_counts[field][status] += 1
            if _source_field_strict_ready(source, field, payload):
                strict_ready_fields.append(field)
                strict_ready_by_field[field].append(source_id)
        source_rows.append(
            {
                "source_id": source_id,
                "provider": source["provider"],
                "source_class": source["source_class"],
                "coverage_status": source["coverage_status"],
                "pit_status": source["pit_status"],
                "access_status": source["access_status"],
                "license_status": source["license_status"],
                "promotion_eligible": source["promotion_eligible"],
                "promotion_blockers": list(source["promotion_blockers"]),
                "strict_ready_fields": sorted(strict_ready_fields),
                "capabilities": dict(source["capabilities"]),
            }
        )

    required_for_strict_listing = ["venue_assignment", "listing_start", "listing_end"]
    blocked_required_fields = [
        field for field in required_for_strict_listing if not strict_ready_by_field.get(field)
    ]

    prospective_sources = sorted(
        source["source_id"]
        for source in sources
        if "PROSPECTIVE_ONLY" in set(source["capabilities"].values())
    )
    event_date_challengers = sorted(
        source["source_id"]
        for source in sources
        if "EVENT_DATE_NO_PUBLICATION_VINTAGE" in set(source["capabilities"].values())
    )

    return {
        "schema_version": "qm_b_listing_metadata_source_assessment_v1",
        "research_only": True,
        "productive_integration_enabled": False,
        "assessment_date": payload["assessment_date"],
        "source_count": len(sources),
        "source_class_counts": dict(sorted(Counter(str(source["source_class"]) for source in sources).items())),
        "pit_status_counts": dict(sorted(Counter(str(source["pit_status"]) for source in sources).items())),
        "access_status_counts": dict(sorted(Counter(str(source["access_status"]) for source in sources).items())),
        "license_status_counts": dict(sorted(Counter(str(source["license_status"]) for source in sources).items())),
        "capability_counts": {
            field: dict(sorted(counter.items())) for field, counter in sorted(capability_counts.items())
        },
        "strict_ready_sources_by_field": {
            field: sorted(strict_ready_by_field.get(field, [])) for field in sorted(fields)
        },
        "required_for_strict_listing": required_for_strict_listing,
        "blocked_required_fields": blocked_required_fields,
        "strict_listing_ledger_ready": not blocked_required_fields,
        "global_strict_listing_source_available_and_cleared": False,
        "prospective_collection_sources": prospective_sources,
        "event_date_challenger_sources": event_date_challengers,
        "project_investability_requires_separate_project_rule": True,
        "source_rows": source_rows,
        "next_step": payload["assessment"]["next_step"],
    }
