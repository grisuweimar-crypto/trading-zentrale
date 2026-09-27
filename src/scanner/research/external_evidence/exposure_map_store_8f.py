from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review


REGISTRY_SCHEMA = "external_evidence_8f_exposure_overlay_registry_v1"
CORRECTIONS_SCHEMA = "external_evidence_8f_overlay_corrections_v1"
ACTIVE_OVERLAY_STATUS = "ACTIVE_HUMAN_REVIEWED"
ACTIVE_CORRECTIONS_STATUS = "ACTIVE_APPEND_ONLY_REDUNDANT_REVIEW_CORRECTIONS"
REDUNDANT_ACTION = "TREAT_AS_ALREADY_MATERIALIZED"


class ExposureMapStore8FError(ValueError):
    pass


def _safe_relative_path(value: Any, *, field: str) -> Path:
    text = str(value or "").strip()
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise ExposureMapStore8FError(f"invalid {field}: {value!r}")
    return path


def validate_overlay_corrections(corrections: Mapping[str, Any]) -> dict[str, Any]:
    if corrections.get("schema_version") != CORRECTIONS_SCHEMA:
        raise ExposureMapStore8FError("unexpected exposure-overlay correction schema_version")
    if corrections.get("phase") != "8F_macro_exposure_context":
        raise ExposureMapStore8FError("unexpected exposure-overlay correction phase")
    if corrections.get("status") != ACTIVE_CORRECTIONS_STATUS:
        raise ExposureMapStore8FError("unexpected exposure-overlay correction status")

    guards = corrections.get("guards") or {}
    required_false = {
        "market_outcomes_read",
        "automatic_conflict_suppression_allowed",
        "correction_may_mutate_existing_mapping",
        "correction_may_backdate",
    }
    required_true = {"correction_requires_explicit_mapping_id"}
    bad_false = sorted(key for key in required_false if guards.get(key) is not False)
    bad_true = sorted(key for key in required_true if guards.get(key) is not True)
    if bad_false or bad_true:
        raise ExposureMapStore8FError(
            f"invalid exposure-overlay correction guards false={bad_false} true={bad_true}"
        )

    rows = corrections.get("corrections") or []
    if not isinstance(rows, list):
        raise ExposureMapStore8FError("corrections must be a list")

    seen: set[tuple[str, str]] = set()
    normalized: list[dict[str, str]] = []
    by_batch: dict[str, set[str]] = {}
    for row in rows:
        batch_id = str(row.get("batch_id") or "").strip()
        mapping_id = str(row.get("mapping_id") or "").strip()
        subject_id = str(row.get("subject_id") or "").strip()
        factor_id = str(row.get("factor_id") or "").strip()
        action = str(row.get("action") or "").strip()
        reason = str(row.get("reason") or "").strip()
        if not batch_id or not mapping_id or not subject_id or not factor_id or not reason:
            raise ExposureMapStore8FError("overlay correction requires batch_id, mapping_id, subject_id, factor_id and reason")
        if action != REDUNDANT_ACTION:
            raise ExposureMapStore8FError(f"unsupported overlay correction action: {action!r}")
        key = (batch_id, mapping_id)
        if key in seen:
            raise ExposureMapStore8FError(f"duplicate overlay correction: {batch_id}/{mapping_id}")
        seen.add(key)
        normalized.append(
            {
                "batch_id": batch_id,
                "mapping_id": mapping_id,
                "subject_id": subject_id,
                "factor_id": factor_id,
                "action": action,
                "reason": reason,
            }
        )
        by_batch.setdefault(batch_id, set()).add(mapping_id)

    return {
        "corrections": normalized,
        "by_batch": by_batch,
        "correction_count": len(normalized),
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
    unknown_batches = set(normalized_corrections["by_batch"]) - overlay_batches
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
    return {batch_id: set(mapping_ids) for batch_id, mapping_ids in corrections["by_batch"].items()}


def compose_effective_exposure_map(
    *,
    base_map: Mapping[str, Any],
    overlay_pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]],
    redundant_mapping_ids_by_batch: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    current = deepcopy(base_map)
    corrections = redundant_mapping_ids_by_batch or {}
    exercised_batches: set[str] = set()

    for candidate_config, review_config in overlay_pairs:
        if review_config.get("status") != "HUMAN_REVIEW_COMPLETED":
            raise ExposureMapStore8FError("active overlay must have HUMAN_REVIEW_COMPLETED status")
        batch_id = str(candidate_config.get("candidate_batch_id") or "")
        registered_ids = set(corrections.get(batch_id, set()))
        if registered_ids:
            candidate_mapping_ids = {
                str(row.get("candidate_id") or "").replace("MAPCAND:", "MAP:", 1)
                for row in candidate_config.get("candidates") or []
            }
            missing = registered_ids - candidate_mapping_ids
            if missing:
                raise ExposureMapStore8FError(
                    f"overlay corrections reference mappings absent from candidate batch {batch_id}: {sorted(missing)}"
                )
            exercised_batches.add(batch_id)

        result = apply_human_mapping_review(
            candidate_config=candidate_config,
            review_config=review_config,
            exposure_map=current,
            redundant_mapping_ids=registered_ids,
        )
        if result.get("status") not in {"HUMAN_REVIEW_APPLIED", "HUMAN_REVIEW_ALREADY_APPLIED"}:
            raise ExposureMapStore8FError(
                f"active overlay did not resolve to reviewed mappings: {result.get('status')}"
            )
        if int(result.get("approved_count") or 0) + int(result.get("already_applied_count") or 0) <= 0:
            raise ExposureMapStore8FError("active overlay must account for at least one approved mapping")
        current = result["exposure_map"]

    unused_correction_batches = set(corrections) - exercised_batches
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
        redundant_mapping_ids_by_batch=corrections["by_batch"],
    )


def build_effective_exposure_map_audit(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    normalized, corrections = _load_registry_and_corrections(root=root, registry_path=registry_path)
    base_map = json.loads((root / normalized["base_map_path"]).read_text(encoding="utf-8"))
    effective = load_effective_exposure_map(root=root, registry_path=registry_path)
    return {
        "schema_version": "external_evidence_8f_effective_exposure_map_audit_v1",
        "status": "PASS_EFFECTIVE_EXPOSURE_MAP_COMPOSITION",
        "base_mapping_count": len(base_map.get("mappings") or []),
        "overlay_count": normalized["overlay_count"],
        "redundant_review_correction_count": corrections["correction_count"],
        "effective_mapping_count": len(effective.get("mappings") or []),
        "market_outcomes_read": False,
        "automatic_promotion_used": False,
        "final_freeze_materialization_required": True,
    }
