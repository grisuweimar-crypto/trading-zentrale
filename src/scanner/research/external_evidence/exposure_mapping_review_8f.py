from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any, Mapping


DECISION_SCHEMA = "external_evidence_8f_mapping_review_decisions_v1"
CANDIDATE_SCHEMA = "external_evidence_8f_mapping_candidates_v1"
EXPOSURE_SCHEMA = "external_evidence_8f_exposure_map_v1"


class ExposureMappingReview8FError(ValueError):
    pass


def _aware_datetime(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExposureMappingReview8FError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExposureMappingReview8FError(f"{field} must be timezone-aware")
    return parsed


def _same_mapping(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    keys = (
        "mapping_id",
        "map_version",
        "subject_id",
        "factor_id",
        "relationship_class",
        "evidence_type",
        "evidence_reference",
        "evidence_sha256",
        "evidence_valid_from",
        "human_reviewed",
        "reviewed_at",
        "review_status",
        "valid_from",
        "valid_to",
    )
    return all(left.get(key) == right.get(key) for key in keys)


def apply_human_mapping_review(
    *,
    candidate_config: Mapping[str, Any],
    review_config: Mapping[str, Any],
    exposure_map: Mapping[str, Any],
) -> dict[str, Any]:
    if candidate_config.get("schema_version") != CANDIDATE_SCHEMA:
        raise ExposureMappingReview8FError("unexpected candidate schema_version")
    if review_config.get("schema_version") != DECISION_SCHEMA:
        raise ExposureMappingReview8FError("unexpected review-decision schema_version")
    if exposure_map.get("schema_version") != EXPOSURE_SCHEMA:
        raise ExposureMappingReview8FError("unexpected exposure-map schema_version")
    if review_config.get("candidate_batch_id") != candidate_config.get("candidate_batch_id"):
        raise ExposureMappingReview8FError("review candidate_batch_id does not match candidate batch")

    guards = review_config.get("guards") or {}
    for key in (
        "market_outcomes_read",
        "automatic_approval_allowed",
        "backdating_before_review_allowed",
        "direction_or_signed_exposure_assigned",
        "weights_or_thresholds_selected",
    ):
        if guards.get(key) is not False:
            raise ExposureMappingReview8FError(f"review guard {key} must remain false")

    decisions = review_config.get("decisions") or []
    if not decisions:
        return {
            "status": "AWAITING_HUMAN_REVIEW",
            "approved_count": 0,
            "already_applied_count": 0,
            "rejected_count": 0,
            "deferred_count": 0,
            "exposure_map": deepcopy(exposure_map),
        }

    reviewed_at = _aware_datetime(review_config.get("reviewed_at"), field="reviewed_at")
    if str(review_config.get("reviewer_role") or "") != "HUMAN_REVIEWER":
        raise ExposureMappingReview8FError("reviewer_role must be HUMAN_REVIEWER")

    by_id = {str(row.get("candidate_id")): row for row in candidate_config.get("candidates") or []}
    if len(by_id) != len(candidate_config.get("candidates") or []):
        raise ExposureMappingReview8FError("candidate ids must be unique")

    seen: set[str] = set()
    approved: list[dict[str, Any]] = []
    rejected = 0
    deferred = 0
    for decision in decisions:
        candidate_id = str(decision.get("candidate_id") or "").strip()
        if not candidate_id or candidate_id in seen:
            raise ExposureMappingReview8FError(f"duplicate or empty review candidate_id: {candidate_id!r}")
        seen.add(candidate_id)
        candidate = by_id.get(candidate_id)
        if candidate is None:
            raise ExposureMappingReview8FError(f"review references unknown candidate: {candidate_id}")

        action = str(decision.get("decision") or "").upper()
        if action not in {"APPROVE", "REJECT", "DEFER"}:
            raise ExposureMappingReview8FError(f"unsupported review decision: {action}")
        if action == "REJECT":
            rejected += 1
            continue
        if action == "DEFER":
            deferred += 1
            continue

        if decision.get("source_verified") is not True:
            raise ExposureMappingReview8FError(f"APPROVE requires source_verified=true: {candidate_id}")
        if decision.get("relationship_class_confirmed") is not True:
            raise ExposureMappingReview8FError(
                f"APPROVE requires relationship_class_confirmed=true: {candidate_id}"
            )
        if candidate.get("human_reviewed") is not False or candidate.get("promotion_allowed") is not False:
            raise ExposureMappingReview8FError("candidate source artifact must remain pending before review")

        approved.append(
            {
                "mapping_id": candidate_id.replace("MAPCAND:", "MAP:", 1),
                "map_version": str(exposure_map.get("map_version") or ""),
                "subject_id": candidate["subject_id"],
                "factor_id": candidate["factor_id"],
                "relationship_class": candidate["relationship_class"],
                "evidence_type": candidate["evidence_type"],
                "evidence_reference": candidate["evidence_reference"],
                "evidence_sha256": candidate["evidence_record_sha256"],
                "evidence_valid_from": reviewed_at.isoformat(),
                "human_reviewed": True,
                "reviewed_at": reviewed_at.isoformat(),
                "review_status": "ACTIVE",
                "valid_from": reviewed_at.isoformat(),
                "valid_to": None,
            }
        )

    updated = deepcopy(exposure_map)
    existing = list(updated.get("mappings") or [])
    by_existing_id = {str(row.get("mapping_id") or ""): row for row in existing}
    existing_pairs = {
        (str(row.get("subject_id") or ""), str(row.get("factor_id") or "")): row
        for row in existing
        if str(row.get("review_status") or "").upper() == "ACTIVE"
    }
    applied: list[dict[str, Any]] = []
    already_applied: list[dict[str, Any]] = []
    for mapping in approved:
        existing_by_id = by_existing_id.get(mapping["mapping_id"])
        if existing_by_id is not None:
            if _same_mapping(existing_by_id, mapping):
                already_applied.append(mapping)
                continue
            raise ExposureMappingReview8FError(
                f"mapping id already exists with different content: {mapping['mapping_id']}"
            )

        pair = (mapping["subject_id"], mapping["factor_id"])
        existing_by_pair = existing_pairs.get(pair)
        if existing_by_pair is not None:
            if _same_mapping(existing_by_pair, mapping):
                already_applied.append(mapping)
                continue
            raise ExposureMappingReview8FError(
                f"active mapping already exists with different content for subject/factor: {pair[0]}/{pair[1]}"
            )

        existing.append(mapping)
        by_existing_id[mapping["mapping_id"]] = mapping
        existing_pairs[pair] = mapping
        applied.append(mapping)

    updated["mappings"] = existing
    if approved:
        updated["status"] = "DOCUMENTARY_HUMAN_REVIEWED_MAPPINGS_ACTIVE"

    return {
        "status": "HUMAN_REVIEW_APPLIED" if applied else "HUMAN_REVIEW_ALREADY_APPLIED",
        "reviewed_at": reviewed_at.isoformat(),
        "approved_count": len(applied),
        "already_applied_count": len(already_applied),
        "rejected_count": rejected,
        "deferred_count": deferred,
        "approved_mapping_ids": [row["mapping_id"] for row in applied],
        "already_applied_mapping_ids": [row["mapping_id"] for row in already_applied],
        "exposure_map": updated,
        "guards": {
            "market_outcomes_read": False,
            "automatic_approval_used": False,
            "mapping_backdated_before_review": False,
            "direction_assigned": False,
            "weights_or_thresholds_selected": False,
        },
    }
