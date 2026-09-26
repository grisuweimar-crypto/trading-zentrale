from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping


SCHEMA_VERSION = "external_evidence_8f_mapping_candidates_v1"


class ExposureMappingCandidates8FError(ValueError):
    pass


def _hash_candidate(row: Mapping[str, Any]) -> str:
    payload = {
        key: row.get(key)
        for key in (
            "candidate_id",
            "subject_id",
            "factor_id",
            "relationship_class",
            "evidence_type",
            "evidence_reference",
            "evidence_summary",
        )
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_mapping_candidates(
    config: Mapping[str, Any],
    *,
    allowed_factor_ids: set[str],
    allowed_subject_ids: set[str],
) -> dict[str, Any]:
    if config.get("schema_version") != SCHEMA_VERSION:
        raise ExposureMappingCandidates8FError("unexpected mapping-candidate schema_version")

    rules = config.get("promotion_rules") or {}
    required_false = {
        "candidate_may_enter_exposure_map_without_human_review",
        "candidate_may_be_backdated_before_review",
        "sector_or_name_alone_is_sufficient_evidence",
        "market_outcomes_may_be_used",
        "direction_or_signed_exposure_may_be_assigned",
        "weight_or_threshold_may_be_selected",
    }
    required_true = {
        "reviewer_must_verify_source_and_relationship_class",
        "approved_mapping_valid_from_must_be_at_or_after_reviewed_at",
    }
    bad_false = sorted(key for key in required_false if rules.get(key) is not False)
    bad_true = sorted(key for key in required_true if rules.get(key) is not True)
    if bad_false or bad_true:
        raise ExposureMappingCandidates8FError(
            f"invalid candidate promotion rules false={bad_false} true={bad_true}"
        )

    candidates = config.get("candidates") or []
    if not isinstance(candidates, list) or not candidates:
        raise ExposureMappingCandidates8FError("mapping candidate batch must contain candidates")

    ids: set[str] = set()
    pairs: set[tuple[str, str]] = set()
    factor_counts: dict[str, int] = {}
    for row in candidates:
        candidate_id = str(row.get("candidate_id") or "").strip()
        subject_id = str(row.get("subject_id") or "").strip()
        factor_id = str(row.get("factor_id") or "").strip()
        relationship_class = str(row.get("relationship_class") or "").strip().upper()
        evidence_reference = str(row.get("evidence_reference") or "").strip()
        evidence_summary = str(row.get("evidence_summary") or "").strip()
        evidence_sha = str(row.get("evidence_record_sha256") or "").strip().lower()

        if not candidate_id or candidate_id in ids:
            raise ExposureMappingCandidates8FError(f"duplicate or empty candidate_id: {candidate_id!r}")
        ids.add(candidate_id)
        if subject_id not in allowed_subject_ids:
            raise ExposureMappingCandidates8FError(f"candidate subject outside frozen domain: {subject_id}")
        if factor_id not in allowed_factor_ids:
            raise ExposureMappingCandidates8FError(f"unsupported candidate factor: {factor_id}")
        if (subject_id, factor_id) in pairs:
            raise ExposureMappingCandidates8FError(f"duplicate subject/factor candidate: {subject_id}/{factor_id}")
        pairs.add((subject_id, factor_id))
        if relationship_class not in {
            "REVENUE_LINK",
            "INPUT_COST_LINK",
            "FINANCING_SENSITIVITY",
            "CURRENCY_TRANSLATION",
            "BALANCE_SHEET_LINK",
            "OTHER_DOCUMENTED",
        }:
            raise ExposureMappingCandidates8FError(f"unsupported relationship_class: {relationship_class}")
        if not evidence_reference.startswith("https://") or not evidence_summary:
            raise ExposureMappingCandidates8FError(f"candidate lacks documentary evidence: {candidate_id}")
        if len(evidence_sha) != 64 or any(ch not in "0123456789abcdef" for ch in evidence_sha):
            raise ExposureMappingCandidates8FError(f"invalid evidence_record_sha256: {candidate_id}")
        if _hash_candidate(row) != evidence_sha:
            raise ExposureMappingCandidates8FError(f"candidate evidence fingerprint mismatch: {candidate_id}")
        if row.get("candidate_status") != "PENDING_HUMAN_REVIEW":
            raise ExposureMappingCandidates8FError(f"candidate must remain pending human review: {candidate_id}")
        if row.get("human_reviewed") is not False or row.get("promotion_allowed") is not False:
            raise ExposureMappingCandidates8FError(f"candidate may not claim review/promotion: {candidate_id}")
        factor_counts[factor_id] = factor_counts.get(factor_id, 0) + 1

    guards = config.get("guards") or {}
    if guards.get("market_outcomes_read") is not False:
        raise ExposureMappingCandidates8FError("candidate batch must remain outcome-blind")
    if guards.get("active_exposure_map_modified") is not False:
        raise ExposureMappingCandidates8FError("candidate batch may not modify active exposure map")
    if guards.get("human_review_claimed") is not False:
        raise ExposureMappingCandidates8FError("candidate batch may not claim human review")
    if int(guards.get("candidate_count") or -1) != len(candidates):
        raise ExposureMappingCandidates8FError("candidate_count guard mismatch")

    return {
        "schema_version": "external_evidence_8f_mapping_candidate_gate_v1",
        "status": "PASS_DOCUMENTARY_CANDIDATE_GATE",
        "candidate_count": len(candidates),
        "factor_counts": dict(sorted(factor_counts.items())),
        "promotion_allowed": False,
        "human_review_claimed": False,
        "market_outcomes_read": False,
    }
