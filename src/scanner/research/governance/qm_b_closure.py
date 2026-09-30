"""Fail-closed validator for the final QM-B closure manifest."""
from __future__ import annotations

import json
from pathlib import Path


EXPECTED_DISPLAY_STATUS = "QM-B COMPLETE — STRICT PROMOTION BLOCKED BY EXTERNAL EVIDENCE"
EXPECTED_EXTERNAL_BLOCKERS = {
    "LISTING_EVIDENCE",
    "MARKET_TRADABILITY_EVIDENCE",
    "EXECUTION_CHANNEL_EVIDENCE",
}
REQUIRED_COMPLETED_BLOCKS = {
    "strict_asof_universe_integrity_foundation",
    "observed_membership_staging_and_crypto_namespace_diagnostics",
    "historical_noncrypto_stable_identity_reconciliation",
    "alias_pit_boundaries_and_legacy_watchlist_evidence",
    "listing_metadata_source_assessment",
    "prospective_listing_snapshot_archive",
    "durable_prospective_membership_universe_capture",
    "identity_membership_listing_evidence_fusion",
    "listing_persistence_and_license_gate",
    "investability_contract",
    "project_restrictions",
    "integrated_investability_gap_audit",
    "execution_channel_evidence_contract_and_source_gap",
    "market_tradability_evidence_contract_and_source_gap",
    "survivorship_and_historical_sample_audit",
    "provider_coverage_and_outcome_availability",
    "historical_taxonomy_and_classification_audit",
    "crypto_stable_object_semantics",
    "evidence_impact_audit",
    "historical_matcher_session_alignment",
}


def load_closure_manifest(path="configs/qm_b_closure_v1.json"):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def validate_closure_manifest(payload):
    errors = []
    if payload.get("qm_axis") != "QM-B":
        errors.append("qm_axis must be QM-B")
    if payload.get("research_governance_status") != "COMPLETE":
        errors.append("research_governance_status must be COMPLETE")
    if payload.get("strict_historical_promotion_status") != "BLOCKED_EXTERNAL_EVIDENCE":
        errors.append("strict historical promotion must remain blocked by external evidence")
    if payload.get("display_status") != EXPECTED_DISPLAY_STATUS:
        errors.append("display_status mismatch")
    if payload.get("historical_retrojection_permitted") is not False:
        errors.append("historical retrojection must be forbidden")
    if payload.get("missing_is_neutral") is not False:
        errors.append("missing evidence must not be neutral")
    if payload.get("unknown_is_promotable") is not False:
        errors.append("UNKNOWN must not be promotable")
    if payload.get("productive_strict_asof_investable_universe_promoted") is not False:
        errors.append("strict as-of investable universe cannot be promoted while blockers remain")

    completed = set(payload.get("completed_blocks") or [])
    missing_blocks = sorted(REQUIRED_COMPLETED_BLOCKS - completed)
    if missing_blocks:
        errors.append("missing completed blocks: " + ", ".join(missing_blocks))

    blockers = payload.get("external_blockers") or []
    blocker_ids = {row.get("id") for row in blockers}
    if blocker_ids != EXPECTED_EXTERNAL_BLOCKERS:
        errors.append("external blocker set mismatch")
    for row in blockers:
        if row.get("classification") != "EXTERNAL_EVIDENCE_GAP":
            errors.append(f"{row.get('id')}: blocker classification mismatch")
        if row.get("state") != "UNKNOWN_FAIL_CLOSED":
            errors.append(f"{row.get('id')}: blocker must remain UNKNOWN_FAIL_CLOSED")
        if row.get("remaining_qm_b_engineering_task") is not False:
            errors.append(f"{row.get('id')}: external blocker must not be an open QM-B engineering task")

    rules = payload.get("closure_rules") or {}
    for field in (
        "external_blockers_do_not_reopen_qm_b_engineering",
        "strict_promotion_must_remain_blocked_while_external_blockers_exist",
        "unknown_must_remain_unknown",
        "absence_must_not_be_interpreted_as_negative_evidence",
        "current_state_must_not_be_back_projected",
    ):
        if rules.get(field) is not True:
            errors.append(f"closure rule must be true: {field}")

    findings = {row.get("finding_id"): row for row in payload.get("resolved_internal_findings") or []}
    if (findings.get("QM-B-POA-001") or {}).get("status") != "RESOLVED":
        errors.append("QM-B-POA-001 must be explicitly resolved before closure")

    if errors:
        raise ValueError("QM-B closure validation failed: " + "; ".join(errors))
    return payload


def validate_closure_file(path="configs/qm_b_closure_v1.json"):
    return validate_closure_manifest(load_closure_manifest(path))
