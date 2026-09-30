"""QM-B governance gate for durable persistence of third-party listing data.

This is a project-control decision layer, not a legal opinion.  Public availability,
research usefulness and public-repository redistribution are treated as separate
questions.  Public-repository persistence fails closed unless explicitly cleared by
project policy.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


DEFAULT_POLICY_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_listing_persistence_policy_v1.json"
DEFAULT_SOURCE_PATH = Path(__file__).resolve().parents[4] / "configs" / "qm_b_listing_metadata_sources_v1.json"


class ListingPersistenceGateError(ValueError):
    """Raised when listing persistence is not explicitly permitted."""


def _load_json(path: Path, *, expected_schema: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ListingPersistenceGateError(f"config_unreadable:{path}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != expected_schema:
        raise ListingPersistenceGateError(f"config_schema_invalid:{path}")
    return payload


def load_persistence_policy(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_POLICY_PATH
    payload = _load_json(target, expected_schema="qm_b_listing_persistence_policy_v1")
    if payload.get("research_only") is not True or payload.get("productive_integration_enabled") is not False:
        raise ListingPersistenceGateError("persistence_policy_scope_invalid")
    public = payload.get("public_repository")
    local = payload.get("local_ephemeral")
    if not isinstance(public, Mapping) or not isinstance(local, Mapping):
        raise ListingPersistenceGateError("persistence_policy_sections_missing")
    if public.get("default_allowed") is not False:
        raise ListingPersistenceGateError("public_repository_default_must_fail_closed")
    if public.get("requires_explicit_redistribution_clearance") is not True:
        raise ListingPersistenceGateError("public_repository_clearance_requirement_missing")
    if local.get("implies_license_clearance") is not False:
        raise ListingPersistenceGateError("local_ephemeral_must_not_imply_license_clearance")
    return payload


def load_source_assessment(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else DEFAULT_SOURCE_PATH
    payload = _load_json(target, expected_schema="qm_b_listing_metadata_sources_v1")
    sources = payload.get("sources")
    if not isinstance(sources, list):
        raise ListingPersistenceGateError("source_assessment_sources_missing")
    return payload


def assess_persistence(
    source_id: str,
    *,
    persistence_scope: str,
    policy_path: str | Path | None = None,
    source_assessment_path: str | Path | None = None,
) -> dict[str, Any]:
    policy = load_persistence_policy(policy_path)
    assessment = load_source_assessment(source_assessment_path)
    source_id = str(source_id or "").strip()
    scope = str(persistence_scope or "").strip()
    if not source_id:
        raise ListingPersistenceGateError("source_id_required")
    if scope not in set(policy.get("persistence_scopes", [])):
        raise ListingPersistenceGateError(f"persistence_scope_invalid:{scope}")

    source = next((row for row in assessment["sources"] if row.get("source_id") == source_id), None)
    if not isinstance(source, Mapping):
        raise ListingPersistenceGateError(f"source_not_assessed:{source_id}")

    if scope == "local_ephemeral":
        allowed = policy["local_ephemeral"].get("allowed_for_qm_test_execution") is True
        return {
            "schema_version": "qm_b_listing_persistence_gate_result_v1",
            "source_id": source_id,
            "persistence_scope": scope,
            "allowed": allowed,
            "gate_status": "ALLOWED_QM_TEST_ONLY" if allowed else "BLOCKED",
            "license_status": source.get("license_status"),
            "access_status": source.get("access_status"),
            "explicit_redistribution_clearance": False,
            "license_clearance_inferred": False,
            "may_be_committed_or_redistributed": False,
            "reason_codes": ["LOCAL_EPHEMERAL_DOES_NOT_IMPLY_LICENSE_CLEARANCE"],
        }

    public = policy["public_repository"]
    cleared = source_id in set(public.get("explicitly_cleared_source_ids", []))
    required_license = str(public.get("required_source_license_status") or "")
    license_ok = source.get("license_status") == required_license
    reasons: list[str] = []
    if not license_ok:
        reasons.append(f"LICENSE_STATUS_NOT_{required_license}")
    if not cleared:
        reasons.append("NO_EXPLICIT_REDISTRIBUTION_CLEARANCE")
    allowed = bool(license_ok and cleared)
    return {
        "schema_version": "qm_b_listing_persistence_gate_result_v1",
        "source_id": source_id,
        "persistence_scope": scope,
        "allowed": allowed,
        "gate_status": "ALLOWED" if allowed else "BLOCKED",
        "license_status": source.get("license_status"),
        "access_status": source.get("access_status"),
        "explicit_redistribution_clearance": cleared,
        "license_clearance_inferred": False,
        "may_be_committed_or_redistributed": allowed,
        "reason_codes": reasons,
    }


def require_persistence_allowed(
    source_id: str,
    *,
    persistence_scope: str,
    policy_path: str | Path | None = None,
    source_assessment_path: str | Path | None = None,
) -> dict[str, Any]:
    result = assess_persistence(
        source_id,
        persistence_scope=persistence_scope,
        policy_path=policy_path,
        source_assessment_path=source_assessment_path,
    )
    if result["allowed"] is not True:
        reasons = ",".join(result.get("reason_codes", [])) or "UNSPECIFIED"
        raise ListingPersistenceGateError(
            f"persistence_blocked:{source_id}:{persistence_scope}:{reasons}"
        )
    return result
