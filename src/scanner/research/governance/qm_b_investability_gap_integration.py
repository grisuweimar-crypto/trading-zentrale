"""QM-B current as-of investability gap integration.

Combines already-versioned project membership/identity evidence with the separately
versioned internal project-restriction policy.  Listing, market tradability and
execution-channel evidence remain separate and fail closed when missing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from scanner.research.governance.qm_b_project_investability import (
    ProjectInvestabilityError,
    _membership_identity_evidence,
    evaluate_project_investability,
    load_investability_contract,
)


SCHEMA_VERSION = "qm_b_investability_gap_integration_v1"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ProjectInvestabilityError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProjectInvestabilityError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProjectInvestabilityError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def audit_investability_as_of(
    membership_snapshot: Mapping[str, Any],
    *,
    as_of: str,
    additional_evidence: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    contract = load_investability_contract()
    if membership_snapshot.get("schema_version") != "qm_b_prospective_membership_snapshot_v1":
        raise ProjectInvestabilityError("membership_snapshot_schema_invalid")
    if membership_snapshot.get("historical_retrojection_permitted") is not False:
        raise ProjectInvestabilityError("membership_snapshot_retrojection_guard_missing")
    claims = membership_snapshot.get("claims")
    if not isinstance(claims, list):
        raise ProjectInvestabilityError("membership_claims_must_be_list")

    membership_time = _utc(membership_snapshot.get("membership_valid_from"), field="membership_valid_from")
    audit_time = _utc(as_of, field="as_of")
    if audit_time < membership_time:
        raise ProjectInvestabilityError("as_of_precedes_membership_snapshot")
    source_id = _clean(membership_snapshot.get("membership_snapshot_id"))
    if not source_id:
        raise ProjectInvestabilityError("membership_snapshot_id_required")

    results: list[dict[str, Any]] = []
    gap_counts = {dimension: 0 for dimension in contract["required_dimensions"]}
    status_counts: dict[str, int] = {}
    for claim in claims:
        if not isinstance(claim, Mapping):
            raise ProjectInvestabilityError("membership_claim_not_object")
        instrument_id = _clean(claim.get("instrument_id"))
        if not instrument_id:
            raise ProjectInvestabilityError("membership_claim_instrument_id_required")
        evidence = _membership_identity_evidence(
            claim,
            snapshot_time=membership_time.isoformat(),
            source_id=source_id,
        )
        evidence.extend(dict(row) for row in additional_evidence if _clean(row.get("instrument_id")) == instrument_id)
        result = evaluate_project_investability(
            instrument_id=instrument_id,
            as_of=audit_time.isoformat(),
            evidence_records=evidence,
        )
        status = result["project_investability_status"]
        status_counts[status] = status_counts.get(status, 0) + 1
        for dimension, row in result["dimensions"].items():
            if row["resolved_status"] in {"UNKNOWN", "PARTIAL", "CONFLICTING", "CONFLICTING_EVIDENCE"}:
                gap_counts[dimension] += 1
        results.append(result)

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": source_id,
        "membership_valid_from": membership_time.isoformat(),
        "as_of": audit_time.isoformat(),
        "instrument_count": len(results),
        "status_counts": dict(sorted(status_counts.items())),
        "evidence_gap_counts": gap_counts,
        "strict_universe_promotion_ready_count": sum(1 for row in results if row["strict_universe_promotion_ready"]),
        "historical_retrojection_permitted": False,
        "results": results,
    }
