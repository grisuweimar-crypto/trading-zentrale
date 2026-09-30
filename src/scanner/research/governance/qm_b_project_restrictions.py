"""QM-B project-restriction evidence.

This module evaluates only internal scanner/research policy.  It makes no claim about
listing, market tradability, execution-channel availability, legal eligibility or
regulatory restrictions.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping


SCHEMA_VERSION = "qm_b_project_restriction_evidence_v1"
DEFAULT_POLICY_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_project_restrictions_v1.json"


class ProjectRestrictionError(ValueError):
    """Raised when project-restriction evidence cannot be generated safely."""


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _utc(value: Any, *, field: str) -> datetime:
    text = _clean(value)
    if not text:
        raise ProjectRestrictionError(f"timestamp_required:{field}")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ProjectRestrictionError(f"timestamp_invalid:{field}:{value}") from exc
    if parsed.tzinfo is None:
        raise ProjectRestrictionError(f"timestamp_timezone_required:{field}")
    return parsed.astimezone(timezone.utc)


def load_project_restriction_policy(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_POLICY_PATH
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectRestrictionError(f"policy_unreadable:{target}") from exc
    if payload.get("schema_version") != "qm_b_project_restrictions_v1":
        raise ProjectRestrictionError("policy_schema_invalid")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ProjectRestrictionError("policy_scope_invalid")
    if payload.get("policy_scope") != "scanner_project_policy_only":
        raise ProjectRestrictionError("policy_scope_must_be_internal_only")
    pit = payload.get("pit_rules")
    if not isinstance(pit, Mapping) or pit.get("historical_retrojection_permitted") is not False:
        raise ProjectRestrictionError("policy_retrojection_guard_missing")
    if pit.get("policy_commit_sha_required") is not True:
        raise ProjectRestrictionError("policy_commit_sha_rule_missing")
    if set(payload.get("blocked_instrument_ids") or []) & set(payload.get("restricted_instrument_ids") or []):
        raise ProjectRestrictionError("policy_id_cannot_be_blocked_and_restricted")
    return payload


def _valid_commit_sha(value: Any) -> str:
    sha = _clean(value).lower()
    if len(sha) != 40 or any(ch not in "0123456789abcdef" for ch in sha):
        raise ProjectRestrictionError("policy_commit_sha_invalid")
    return sha


def _asset_types_from_claim(claim: Mapping[str, Any]) -> list[str]:
    rows = claim.get("source_rows")
    if not isinstance(rows, list):
        raise ProjectRestrictionError("membership_source_rows_must_be_list")
    return sorted({_clean(row.get("asset_type")).lower() for row in rows if isinstance(row, Mapping) and _clean(row.get("asset_type"))})


def build_project_restriction_evidence(
    membership_snapshot: Mapping[str, Any],
    *,
    policy_commit_sha: str,
    policy_observed_at: str,
    policy_path: str | Path | None = None,
) -> dict[str, Any]:
    policy = load_project_restriction_policy(policy_path)
    if membership_snapshot.get("schema_version") != "qm_b_prospective_membership_snapshot_v1":
        raise ProjectRestrictionError("membership_snapshot_schema_invalid")
    if membership_snapshot.get("historical_retrojection_permitted") is not False:
        raise ProjectRestrictionError("membership_retrojection_guard_missing")
    membership_time = _utc(membership_snapshot.get("membership_valid_from"), field="membership_valid_from")
    policy_time = _utc(policy_observed_at, field="policy_observed_at")
    sha = _valid_commit_sha(policy_commit_sha)
    valid_from = max(membership_time, policy_time).isoformat()
    claims = membership_snapshot.get("claims")
    if not isinstance(claims, list):
        raise ProjectRestrictionError("membership_claims_must_be_list")

    allowed = {str(value).lower() for value in policy.get("allowed_asset_types") or []}
    blocked = set(policy.get("blocked_instrument_ids") or [])
    restricted = set(policy.get("restricted_instrument_ids") or [])
    evidence: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    for claim in claims:
        if not isinstance(claim, Mapping):
            raise ProjectRestrictionError("membership_claim_not_object")
        instrument_id = _clean(claim.get("instrument_id"))
        if not instrument_id:
            raise ProjectRestrictionError("membership_claim_instrument_id_required")
        asset_types = _asset_types_from_claim(claim)
        if instrument_id in blocked:
            status = "BLOCKED"
            reason = "EXPLICIT_PROJECT_BLOCKLIST"
        elif instrument_id in restricted:
            status = "RESTRICTED"
            reason = "EXPLICIT_PROJECT_RESTRICTION"
        elif not asset_types:
            status = "UNKNOWN"
            reason = "ASSET_TYPE_MISSING"
        elif len(asset_types) != 1:
            status = "UNKNOWN"
            reason = "MIXED_ASSET_TYPES_SAME_INSTRUMENT"
        elif asset_types[0] not in allowed:
            status = "BLOCKED"
            reason = "ASSET_TYPE_NOT_ALLOWED_BY_PROJECT_POLICY"
        else:
            status = "CLEAR"
            reason = "ALLOWED_ASSET_TYPE_NO_EXPLICIT_PROJECT_RESTRICTION"
        counts[status] = counts.get(status, 0) + 1
        evidence.append({
            "instrument_id": instrument_id,
            "dimension": "project_restrictions",
            "status": status,
            "valid_from": valid_from,
            "pit_verified": True,
            "source_id": f"git:{sha}:configs/qm_b_project_restrictions_v1.json",
            "reason_codes": [reason],
            "asset_types": asset_types,
        })

    return {
        "schema_version": SCHEMA_VERSION,
        "research_only": True,
        "productive_integration_enabled": False,
        "membership_snapshot_id": _clean(membership_snapshot.get("membership_snapshot_id")),
        "membership_valid_from": membership_time.isoformat(),
        "policy_commit_sha": sha,
        "policy_observed_at": policy_time.isoformat(),
        "evidence_valid_from": valid_from,
        "instrument_count": len(evidence),
        "status_counts": dict(sorted(counts.items())),
        "historical_retrojection_permitted": False,
        "legal_eligibility_evaluated": False,
        "broker_availability_evaluated": False,
        "market_tradability_evaluated": False,
        "listing_state_evaluated": False,
        "evidence": evidence,
    }
