from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review


REGISTRY_SCHEMA = "external_evidence_8f_exposure_overlay_registry_v1"
ACTIVE_OVERLAY_STATUS = "ACTIVE_HUMAN_REVIEWED"


class ExposureMapStore8FError(ValueError):
    pass


def _safe_relative_path(value: Any, *, field: str) -> Path:
    text = str(value or "").strip()
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise ExposureMapStore8FError(f"invalid {field}: {value!r}")
    return path


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
        "overlays": normalized,
        "overlay_count": len(normalized),
    }


def compose_effective_exposure_map(
    *,
    base_map: Mapping[str, Any],
    overlay_pairs: list[tuple[Mapping[str, Any], Mapping[str, Any]]],
) -> dict[str, Any]:
    current = deepcopy(base_map)
    for candidate_config, review_config in overlay_pairs:
        if review_config.get("status") != "HUMAN_REVIEW_COMPLETED":
            raise ExposureMapStore8FError("active overlay must have HUMAN_REVIEW_COMPLETED status")
        result = apply_human_mapping_review(
            candidate_config=candidate_config,
            review_config=review_config,
            exposure_map=current,
        )
        if result.get("status") not in {"HUMAN_REVIEW_APPLIED", "HUMAN_REVIEW_ALREADY_APPLIED"}:
            raise ExposureMapStore8FError(
                f"active overlay did not resolve to reviewed mappings: {result.get('status')}"
            )
        if int(result.get("approved_count") or 0) + int(result.get("already_applied_count") or 0) <= 0:
            raise ExposureMapStore8FError("active overlay must account for at least one approved mapping")
        current = result["exposure_map"]
    return current


def load_effective_exposure_map(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    registry_path = registry_path or Path("configs/external_evidence_8f_exposure_overlays_v1.json")
    if registry_path.is_absolute():
        registry_file = registry_path
    else:
        registry_file = root / registry_path
    registry = json.loads(registry_file.read_text(encoding="utf-8"))
    normalized = validate_overlay_registry(registry)

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

    return compose_effective_exposure_map(base_map=base_map, overlay_pairs=overlay_pairs)


def build_effective_exposure_map_audit(
    *,
    root: Path,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    registry_path = registry_path or Path("configs/external_evidence_8f_exposure_overlays_v1.json")
    registry_file = registry_path if registry_path.is_absolute() else root / registry_path
    registry = json.loads(registry_file.read_text(encoding="utf-8"))
    normalized = validate_overlay_registry(registry)
    base_map = json.loads((root / normalized["base_map_path"]).read_text(encoding="utf-8"))
    effective = load_effective_exposure_map(root=root, registry_path=registry_path)
    return {
        "schema_version": "external_evidence_8f_effective_exposure_map_audit_v1",
        "status": "PASS_EFFECTIVE_EXPOSURE_MAP_COMPOSITION",
        "base_mapping_count": len(base_map.get("mappings") or []),
        "overlay_count": normalized["overlay_count"],
        "effective_mapping_count": len(effective.get("mappings") or []),
        "market_outcomes_read": False,
        "automatic_promotion_used": False,
        "final_freeze_materialization_required": True,
    }
