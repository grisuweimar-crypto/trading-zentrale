from __future__ import annotations

from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8f_source_candidates_v1"


class SourceCandidates8FError(ValueError):
    pass


def validate_source_candidates(
    config: Mapping[str, Any],
    *,
    allowed_factor_ids: set[str],
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise SourceCandidates8FError("unexpected Phase 8F source-candidate schema_version")

    rules = config.get("rules") or {}
    required_rules = {
        "single_provider_required": False,
        "historical_current_value_retrojection_forbidden": True,
        "historical_original_vintage_or_release_archive_required": True,
        "prospective_snapshot_from_actual_ingestion_allowed": True,
        "source_terms_and_series_rights_must_be_cleared": True,
        "blocked_source_may_not_be_enabled": True,
    }
    wrong = [key for key, expected in required_rules.items() if rules.get(key) is not expected]
    if wrong:
        raise SourceCandidates8FError("invalid Phase 8F source-routing rules: " + ", ".join(sorted(wrong)))

    sources = config.get("sources") or []
    source_by_id: dict[str, Mapping[str, Any]] = {}
    for source in sources:
        source_id = str(source.get("source_id") or "").strip()
        if not source_id:
            raise SourceCandidates8FError("every source candidate requires source_id")
        if source_id in source_by_id:
            raise SourceCandidates8FError(f"duplicate source_id: {source_id}")
        factors = {str(value) for value in source.get("factors") or []}
        unknown_factors = factors - allowed_factor_ids
        if unknown_factors:
            raise SourceCandidates8FError(
                f"source {source_id} references unknown factors: {sorted(unknown_factors)}"
            )
        for field in ("provider", "frequency", "pit_status", "license_status", "build_status"):
            if not str(source.get(field) or "").strip():
                raise SourceCandidates8FError(f"source {source_id} missing {field}")
        if not source.get("source_urls"):
            raise SourceCandidates8FError(f"source {source_id} requires source_urls")
        source_by_id[source_id] = source

    blocked = config.get("blocked_sources") or []
    blocked_ids = set()
    for item in blocked:
        source_id = str(item.get("source_id") or "").strip()
        reason = str(item.get("reason") or "").strip()
        if not source_id or not reason:
            raise SourceCandidates8FError("blocked source requires source_id and reason")
        blocked_ids.add(source_id)

    if "fred_alfred_realtime" not in blocked_ids:
        raise SourceCandidates8FError("FRED/ALFRED must remain explicitly blocked under current terms")
    if "fred_alfred_realtime" in source_by_id:
        raise SourceCandidates8FError("blocked FRED/ALFRED may not appear as an enabled source candidate")

    routing = config.get("factor_routing") or {}
    gaps = config.get("explicit_source_gaps") or {}
    if set(routing) != allowed_factor_ids:
        missing = sorted(allowed_factor_ids - set(routing))
        extra = sorted(set(routing) - allowed_factor_ids)
        raise SourceCandidates8FError(f"factor routing mismatch missing={missing} extra={extra}")

    for factor_id, routed_ids_raw in routing.items():
        routed_ids = [str(value) for value in routed_ids_raw or []]
        if len(routed_ids) != len(set(routed_ids)):
            raise SourceCandidates8FError(f"duplicate routing source for factor {factor_id}")
        if not routed_ids and factor_id not in gaps:
            raise SourceCandidates8FError(
                f"factor {factor_id} has no candidate source and no explicit source gap"
            )
        for source_id in routed_ids:
            if source_id in blocked_ids:
                raise SourceCandidates8FError(
                    f"blocked source {source_id} may not be routed to factor {factor_id}"
                )
            source = source_by_id.get(source_id)
            if source is None:
                raise SourceCandidates8FError(
                    f"factor {factor_id} routes to unknown source {source_id}"
                )
            if factor_id not in set(source.get("factors") or []):
                raise SourceCandidates8FError(
                    f"source {source_id} does not declare routed factor {factor_id}"
                )

    return {
        "schema_version": "external_evidence_8f_source_candidates_gate_v1",
        "status": "PASS_SOURCE_ROUTING_CONTRACT",
        "source_candidate_count": len(source_by_id),
        "blocked_source_count": len(blocked_ids),
        "factor_count": len(routing),
        "explicit_gap_factors": sorted(gaps),
        "fred_alfred_enabled": False,
    }
