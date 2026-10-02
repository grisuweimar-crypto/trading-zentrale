from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review


REGISTRY_SCHEMA = "external_evidence_8f_exposure_overlay_registry_v1"
LEGACY_CORRECTIONS_SCHEMA = "external_evidence_8f_overlay_corrections_v1"
RESOLUTIONS_SCHEMA = "external_evidence_8f_overlay_resolutions_v2"
ACTIVE_OVERLAY_STATUS = "ACTIVE_HUMAN_REVIEWED"
LEGACY_CORRECTIONS_STATUS = "ACTIVE_APPEND_ONLY_REDUNDANT_REVIEW_CORRECTIONS"
ACTIVE_RESOLUTIONS_STATUS = "ACTIVE_APPEND_ONLY_REVIEW_RESOLUTIONS"
REDUNDANT_ACTION = "TREAT_AS_ALREADY_MATERIALIZED"
SUPERSESSION_ACTION = "SUPERSEDE_WITH_HUMAN_REVIEWED_MAPPING"


class ExposureMapStore8FError(ValueError):
    pass


def _safe_relative_path(value: Any, *, field: str) -> Path:
    text = str(value or "").strip()
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise ExposureMapStore8FError(f"invalid {field}: {value!r}")
    return path


def _aware_datetime(value: Any, *, field: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ExposureMapStore8FError(f"invalid {field}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ExposureMapStore8FError(f"{field} must be timezone-aware")
    return parsed


def _candidate_id_for_mapping(mapping_id: str) -> str:
    if not mapping_id.startswith("MAP:"):
        raise ExposureMapStore8FError(f"invalid mapping_id: {mapping_id!r}")
    return mapping_id.replace("MAP:", "MAPCAND:", 1)


def _mapping_id_for_candidate(candidate_id: str) -> str:
    if not candidate_id.startswith("MAPCAND:"):
        raise ExposureMapStore8FError(f"invalid candidate_id: {candidate_id!r}")
    return candidate_id.replace("MAPCAND:", "MAP:", 1)


def validate_overlay_corrections(corrections: Mapping[str, Any]) -> dict[str, Any]:
    schema = corrections.get("schema_version")
    if schema not in {LEGACY_CORRECTIONS_SCHEMA, RESOLUTIONS_SCHEMA}:
        raise ExposureMapStore8FError("unexpected exposure-overlay correction schema_version")
    if corrections.get("phase") != "8F_macro_exposure_context":
        raise ExposureMapStore8FError("unexpected exposure-overlay correction phase")

    legacy = schema == LEGACY_CORRECTIONS_SCHEMA
    expected_status = LEGACY_CORRECTIONS_STATUS if legacy else ACTIVE_RESOLUTIONS_STATUS
    if corrections.get("status") != expected_status:
        raise ExposureMapStore8FError("unexpected exposure-overlay correction status")

    guards = corrections.get("guards") or {}
    required_false = {
        "market_outcomes_read",
        "automatic_conflict_suppression_allowed",
        "correction_may_mutate_existing_mapping",
        "correction_may_backdate",
    }
    required_true = {"correction_requires_explicit_mapping_id"}
    if not legacy:
        required_false.add("correction_may_mutate_source_artifact")
        required_true.update(
            {
                "supersession_requires_completed_human_review",
                "supersession_preserves_prior_interval",
            }
        )
    bad_false = sorted(key for key in required_false if guards.get(key) is not False)
    bad_true = sorted(key for key in required_true if guards.get(key) is not True)
    if bad_false or bad_true:
        raise ExposureMapStore8FError(
            f"invalid exposure-overlay correction guards false={bad_false} true={bad_true}"
        )

    rows = corrections.get("corrections" if legacy else "resolutions") or []
    if not isinstance(rows, list):
        raise ExposureMapStore8FError("overlay corrections/resolutions must be a list")

    seen: set[tuple[str, str]] = set()
    normalized: list[dict[str, str]] = []
    redundant_by_batch: dict[str, set[str]] = {}
    supersessions_by_batch: dict[str, list[dict[str, str]]] = {}
    all_batches: set[str] = set()

    for row in rows:
        batch_id = str(row.get("batch_id") or "").strip()
        mapping_id = str(row.get("mapping_id") or "").strip()
        subject_id = str(row.get("subject_id") or "").strip()
        factor_id = str(row.get("factor_id") or "").strip()
        action = str(row.get("action") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if not batch_id or not mapping_id or not subject_id or not factor_id or not reason:
            raise ExposureMapStore8FError(
                "overlay correction requires batch_id, mapping_id, subject_id, factor_id and reason"
            )
        key = (batch_id, mapping_id)
        if key in seen:
            raise ExposureMapStore8FError(f"duplicate overlay correction: {batch_id}/{mapping_id}")
        seen.add(key)
        all_batches.add(batch_id)

        common = {
            "batch_id": batch_id,
            "mapping_id": mapping_id,
            "subject_id": subject_id,
            "factor_id": factor_id,
            "action": action,
            "reason": reason,
        }

        if action == REDUNDANT_ACTION:
            normalized.append(common)
            redundant_by_batch.setdefault(batch_id, set()).add(mapping_id)
            continue

        if legacy or action != SUPERSESSION_ACTION:
            raise ExposureMapStore8FError(f"unsupported overlay correction action: {action!r}")

        new_mapping_id = str(row.get("new_mapping_id") or "").strip()
        prior_class = str(row.get("expected_prior_relationship_class") or "").strip()
        approved_class = str(row.get("approved_relationship_class") or "").strip()
        if not new_mapping_id or not prior_class or not approved_class:
            raise ExposureMapStore8FError(
                "supersession requires new_mapping_id, expected_prior_relationship_class and approved_relationship_class"
            )
        if new_mapping_id == mapping_id:
            raise ExposureMapStore8FError("supersession new_mapping_id must differ from prior mapping_id")
        if prior_class == approved_class:
            raise ExposureMapStore8FError("supersession must represent a relationship-class change")

        normalized_row = {
            **common,
            "new_mapping_id": new_mapping_id,
            "expected_prior_relationship_class": prior_class,
            "approved_relationship_class": approved_class,
        }
        normalized.append(normalized_row)
        supersessions_by_batch.setdefault(batch_id, []).append(normalized_row)

    return {
        "corrections": normalized,
        "by_batch": redundant_by_batch,
        "redundant_by_batch": redundant_by_batch,
        "supersessions_by_batch": supersessions_by_batch,
        "resolution_batches": all_batches,
        "correction_count": len(normalized),
        "redundant_review_count": sum(len(values) for values in redundant_by_batch.values()),
        "supersession_count": sum(len(values) for values in supersessions_by_batch.values()),
    }


def validate_overlay_registry(registry: Mapping[str, Any]) -> dict[str, Any]:
    if registry.get("schema_version") != REGISTRY_SCHEMA:
        raise ExposureMapStore8FError("unexpected exposure-overlay registry schema_version")
    if registry.get("phase") != "8F_macro_exposure_context":
        raise ExposureMapStore8FError("unexpected exposure-overlay registry phase")

    rules = registry.get("rules") or {}
    required_false = {
        "market_outcomes_may_be_read",
        "automatic_review_or_promotion_allowed",
        "overlay_may_backdate_before_review",
        "base_map_is_rewritten_per_batch",
    }
    required_true = {
        "overlay_order_is_append_only",
        "final_8f_freeze_requires_materialized_effective_map",
    }
    bad_false = sorted(key for key in required_false if rules.get(key) is not False)
    bad_true = sorted(key for key in required_true if rules.get(key) is not True)
    if bad_false or bad_true:
        raise ExposureMapStore8FError(
            f"invalid exposure-overlay rules false={bad_false} true={bad_true}"
        )

    base_map_path = _safe_relative_path(registry.get("base_map_path"), field="base_map_path")
    corrections_path = _safe_relative_path(registry.get("corrections_path"), field="corrections_path")
    overlays = registry.get("overlays") or []
    if not isinstance(overlays, list):
        raise ExposureMapStore8FError("overlays must be a list")

    normalized: list[dict[str, Any]] = []
    seen_orders: set[int] = set()
    seen_batches: set[str] = set()
    previous_order: int | None = None
    for row in overlays:
        order = int(row.get("order"))
        batch_id = str(row.get("batch_id") or "").strip()
        status = str(row.get("status") or "").strip()
        if order in seen_orders or (previous_order is not None and order <= previous_order):
            raise ExposureMapStore8FError("overlay order must be unique and strictly increasing")
        if not batch_id or batch_id in seen_batches:
            raise ExposureMapStore8FError(f"duplicate or empty overlay batch_id: {batch_id!r}")
        if status != ACTIVE_OVERLAY_STATUS:
            raise ExposureMapStore8FError(f"unsupported overlay status: {status!r}")
        candidate_path = _safe_relative_path(row.get("candidates_path"), field="candidates_path")
        review_path = _safe_relative_path(row.get("review_path"), field="review_path")
        normalized.append(
            {
                "order": order,
                "batch_id": batch_id,
                "status": status,
                "candidates_path": candidate_path.as_posix(),
                "review_path": review_path.as_posix(),
            }
        )
        seen_orders.add(order)
        seen_batches.add(batch_id)
        previous_order = order

    return {
        "base_map_path": base_map_path.as_posix(),
        "corrections_path": corrections_path.as_posix(),
        "overlays": normalized,
        "overlay_count": len(normalized),
    }


def _load_registry_and_corrections(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    registry_path = registry_path or Path("configs/external_evidence_8f_exposure_overlays_v1.json")
    registry_file = registry_path if registry_path.is_absolute() else root / registry_path
    registry = json.loads(registry_file.read_text(encoding="utf-8"))
    normalized_registry = validate_overlay_registry(registry)
    corrections_file = root / normalized_registry["corrections_path"]
    corrections = json.loads(corrections_file.read_text(encoding="utf-8"))
    normalized_corrections = validate_overlay_corrections(corrections)

    overlay_batches = {row["batch_id"] for row in normalized_registry["overlays"]}
    unknown_batches = normalized_corrections["resolution_batches"] - overlay_batches
    if unknown_batches:
        raise ExposureMapStore8FError(
            "overlay corrections reference unknown batches: " + ", ".join(sorted(unknown_batches))
        )
    return normalized_registry, normalized_corrections


def load_registered_redundant_mapping_ids(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> dict[str, set[str]]:
    _, corrections = _load_registry_and_corrections(root=root, registry_path=registry_path)
    return {
        batch_id: set(mapping_ids)
        for batch_id, mapping_ids in corrections["redundant_by_batch"].items()
    }


def _prepare_versioned_supersessions(
    *,
    exposure_map: Mapping[str, Any],
    candidate_config: Mapping[str, Any],
    review_config: Mapping[str, Any],
    supersessions: list[Mapping[str, str]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    updated_map = deepcopy(exposure_map)
    updated_candidates = deepcopy(candidate_config)
    updated_review = deepcopy(review_config)
    if not supersessions:
        return updated_map, updated_candidates, updated_review

    if review_config.get("status") != "HUMAN_REVIEW_COMPLETED":
        raise ExposureMapStore8FError("supersession requires HUMAN_REVIEW_COMPLETED review")
    if str(review_config.get("reviewer_role") or "") != "HUMAN_REVIEWER":
        raise ExposureMapStore8FError("supersession requires HUMAN_REVIEWER")
    reviewed_at = _aware_datetime(review_config.get("reviewed_at"), field="reviewed_at")
    reviewed_at_text = reviewed_at.isoformat()

    candidate_rows = updated_candidates.get("candidates") or []
    decision_rows = updated_review.get("decisions") or []
    mappings = updated_map.get("mappings") or []
    candidate_by_id = {str(row.get("candidate_id") or ""): row for row in candidate_rows}
    decision_by_id = {str(row.get("candidate_id") or ""): row for row in decision_rows}
    mapping_by_id = {str(row.get("mapping_id") or ""): row for row in mappings}

    for resolution in supersessions:
        old_mapping_id = str(resolution["mapping_id"])
        new_mapping_id = str(resolution["new_mapping_id"])
        old_candidate_id = _candidate_id_for_mapping(old_mapping_id)
        new_candidate_id = _candidate_id_for_mapping(new_mapping_id)
        subject_id = str(resolution["subject_id"])
        factor_id = str(resolution["factor_id"])
        prior_class = str(resolution["expected_prior_relationship_class"])
        approved_class = str(resolution["approved_relationship_class"])

        old_mapping = mapping_by_id.get(old_mapping_id)
        candidate = candidate_by_id.get(old_candidate_id)
        decision = decision_by_id.get(old_candidate_id)
        if old_mapping is None:
            raise ExposureMapStore8FError(f"supersession prior mapping not found: {old_mapping_id}")
        if candidate is None or decision is None:
            raise ExposureMapStore8FError(
                f"supersession candidate/review not found for {old_mapping_id}"
            )
        if new_mapping_id in mapping_by_id:
            raise ExposureMapStore8FError(f"supersession new mapping already exists: {new_mapping_id}")

        if old_mapping.get("subject_id") != subject_id or old_mapping.get("factor_id") != factor_id:
            raise ExposureMapStore8FError(f"supersession prior mapping identity mismatch: {old_mapping_id}")
        if str(old_mapping.get("relationship_class") or "") != prior_class:
            raise ExposureMapStore8FError(f"supersession prior relationship class mismatch: {old_mapping_id}")
        if str(old_mapping.get("review_status") or "").upper() != "ACTIVE":
            raise ExposureMapStore8FError(f"supersession prior mapping must be ACTIVE: {old_mapping_id}")
        if old_mapping.get("valid_to") is not None:
            raise ExposureMapStore8FError(f"supersession prior mapping already closed: {old_mapping_id}")
        if old_mapping.get("human_reviewed") is not True:
            raise ExposureMapStore8FError(f"supersession prior mapping must be human reviewed: {old_mapping_id}")

        if candidate.get("subject_id") != subject_id or candidate.get("factor_id") != factor_id:
            raise ExposureMapStore8FError(f"supersession candidate identity mismatch: {old_candidate_id}")
        if str(candidate.get("relationship_class") or "") != approved_class:
            raise ExposureMapStore8FError(f"supersession approved relationship class mismatch: {old_candidate_id}")
        if str(decision.get("decision") or "").upper() != "APPROVE":
            raise ExposureMapStore8FError(f"supersession decision must be APPROVE: {old_candidate_id}")
        if decision.get("source_verified") is not True or decision.get("relationship_class_confirmed") is not True:
            raise ExposureMapStore8FError(
                f"supersession decision must verify source and relationship class: {old_candidate_id}"
            )

        prior_valid_from = _aware_datetime(old_mapping.get("valid_from"), field=f"{old_mapping_id}.valid_from")
        if reviewed_at < prior_valid_from:
            raise ExposureMapStore8FError(f"supersession may not backdate before prior interval: {old_mapping_id}")

        old_mapping["valid_to"] = reviewed_at_text
        old_mapping["review_status"] = "SUPERSEDED"
        candidate["candidate_id"] = new_candidate_id
        decision["candidate_id"] = new_candidate_id
        candidate_by_id.pop(old_candidate_id)
        decision_by_id.pop(old_candidate_id)
        candidate_by_id[new_candidate_id] = candidate
        decision_by_id[new_candidate_id] = decision

    updated_map["mappings"] = mappings
    return updated_map, updated_candidates, updated_review


def compose_effective_exposure_map(
    *,
    base_map: Mapping[str, Any],
    overlay_pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]],
    redundant_mapping_ids_by_batch: Mapping[str, set[str]] | None = None,
    supersession_rows_by_batch: Mapping[str, list[Mapping[str, str]]] | None = None,
) -> dict[str, Any]:
    current = deepcopy(base_map)
    redundant = redundant_mapping_ids_by_batch or {}
    supersessions = supersession_rows_by_batch or {}
    exercised_batches: set[str] = set()

    for candidate_config, review_config in overlay_pairs:
        if review_config.get("status") != "HUMAN_REVIEW_COMPLETED":
            raise ExposureMapStore8FError("active overlay must have HUMAN_REVIEW_COMPLETED status")
        batch_id = str(candidate_config.get("candidate_batch_id") or "")
        registered_ids = set(redundant.get(batch_id, set()))
        batch_supersessions = list(supersessions.get(batch_id, []))
        superseded_ids = {str(row["mapping_id"]) for row in batch_supersessions}

        if registered_ids or superseded_ids:
            candidate_mapping_ids = {
                _mapping_id_for_candidate(str(row.get("candidate_id") or ""))
                for row in candidate_config.get("candidates") or []
            }
            missing = (registered_ids | superseded_ids) - candidate_mapping_ids
            if missing:
                raise ExposureMapStore8FError(
                    f"overlay corrections reference mappings absent from candidate batch {batch_id}: {sorted(missing)}"
                )
            exercised_batches.add(batch_id)

        prepared_map, prepared_candidates, prepared_review = _prepare_versioned_supersessions(
            exposure_map=current,
            candidate_config=candidate_config,
            review_config=review_config,
            supersessions=batch_supersessions,
        )

        result = apply_human_mapping_review(
            candidate_config=prepared_candidates,
            review_config=prepared_review,
            exposure_map=prepared_map,
            redundant_mapping_ids=registered_ids,
        )
        if result.get("status") not in {"HUMAN_REVIEW_APPLIED", "HUMAN_REVIEW_ALREADY_APPLIED"}:
            raise ExposureMapStore8FError(
                f"active overlay did not resolve to reviewed mappings: {result.get('status')}"
            )
        if int(result.get("approved_count") or 0) + int(result.get("already_applied_count") or 0) <= 0:
            raise ExposureMapStore8FError("active overlay must account for at least one approved mapping")
        current = result["exposure_map"]

        for resolution in batch_supersessions:
            old_id = str(resolution["mapping_id"])
            new_id = str(resolution["new_mapping_id"])
            by_id = {str(row.get("mapping_id") or ""): row for row in current.get("mappings") or []}
            old_row = by_id.get(old_id)
            new_row = by_id.get(new_id)
            if old_row is None or new_row is None:
                raise ExposureMapStore8FError(f"supersession was not materialized: {old_id} -> {new_id}")
            if str(old_row.get("review_status") or "").upper() != "SUPERSEDED" or old_row.get("valid_to") is None:
                raise ExposureMapStore8FError(f"supersession prior interval was not closed: {old_id}")
            if str(new_row.get("review_status") or "").upper() != "ACTIVE" or new_row.get("valid_to") is not None:
                raise ExposureMapStore8FError(f"supersession replacement is not active: {new_id}")

    all_resolution_batches = set(redundant) | set(supersessions)
    unused_correction_batches = all_resolution_batches - exercised_batches
    if unused_correction_batches:
        raise ExposureMapStore8FError(
            "overlay corrections were not exercised for batches: " + ", ".join(sorted(unused_correction_batches))
        )
    return current


def load_effective_exposure_map(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    normalized, corrections = _load_registry_and_corrections(root=root, registry_path=registry_path)

    base_map = json.loads((root / normalized["base_map_path"]).read_text(encoding="utf-8"))
    overlay_pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    for row in normalized["overlays"]:
        candidates = json.loads((root / row["candidates_path"]).read_text(encoding="utf-8"))
        review = json.loads((root / row["review_path"]).read_text(encoding="utf-8"))
        if review.get("candidate_batch_id") != row["batch_id"]:
            raise ExposureMapStore8FError(
                f"overlay review batch mismatch: expected {row['batch_id']} got {review.get('candidate_batch_id')}"
            )
        if candidates.get("candidate_batch_id") != row["batch_id"]:
            raise ExposureMapStore8FError(
                f"overlay candidate batch mismatch: expected {row['batch_id']} got {candidates.get('candidate_batch_id')}"
            )
        overlay_pairs.append((candidates, review))

    return compose_effective_exposure_map(
        base_map=base_map,
        overlay_pairs=overlay_pairs,
        redundant_mapping_ids_by_batch=corrections["redundant_by_batch"],
        supersession_rows_by_batch=corrections["supersessions_by_batch"],
    )


def build_effective_exposure_map_audit(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    normalized, corrections = _load_registry_and_corrections(root=root, registry_path=registry_path)
    base_map = json.loads((root / normalized["base_map_path"]).read_text(encoding="utf-8"))
    effective = load_effective_exposure_map(root=root, registry_path=registry_path)
    mappings = list(effective.get("mappings") or [])
    active = [row for row in mappings if str(row.get("review_status") or "").upper() == "ACTIVE"]
    superseded = [row for row in mappings if str(row.get("review_status") or "").upper() == "SUPERSEDED"]
    active_subjects = {str(row.get("subject_id") or "") for row in active}
    return {
        "schema_version": "external_evidence_8f_effective_exposure_map_audit_v2",
        "status": "PASS_EFFECTIVE_EXPOSURE_MAP_COMPOSITION",
        "base_mapping_count": len(base_map.get("mappings") or []),
        "overlay_count": normalized["overlay_count"],
        "review_resolution_count": corrections["correction_count"],
        "redundant_review_correction_count": corrections["redundant_review_count"],
        "supersession_count": corrections["supersession_count"],
        "effective_mapping_interval_count": len(mappings),
        "effective_mapping_count": len(active),
        "active_subject_count": len(active_subjects),
        "superseded_mapping_count": len(superseded),
        "market_outcomes_read": False,
        "automatic_promotion_used": False,
        "final_freeze_materialization_required": True,
    }
