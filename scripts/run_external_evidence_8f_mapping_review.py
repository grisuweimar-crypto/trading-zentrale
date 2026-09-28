from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from scanner.research.external_evidence.exposure_map_store_8f import (
    load_effective_exposure_map,
    load_registered_redundant_mapping_ids,
    validate_overlay_corrections,
    validate_overlay_registry,
)
from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANDIDATES = ROOT / "configs/external_evidence_8f_mapping_candidates_v1.json"
DEFAULT_REVIEW = ROOT / "configs/external_evidence_8f_mapping_review_decisions_v1.json"
DEFAULT_EXPOSURE = ROOT / "configs/external_evidence_8f_exposure_map_v1.json"
DEFAULT_OVERLAY_REGISTRY = ROOT / "configs/external_evidence_8f_exposure_overlays_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_exposure(path: Path, overlay_registry: Path) -> dict:
    if path.resolve() == DEFAULT_EXPOSURE.resolve():
        return load_effective_exposure_map(root=ROOT, registry_path=overlay_registry)
    return _load(path)


def _load_supersession_registry(overlay_registry: Path) -> tuple[dict[str, list[dict]], dict[str, dict]]:
    registry = _load(overlay_registry)
    normalized_registry = validate_overlay_registry(registry)
    corrections = _load(ROOT / normalized_registry["corrections_path"])
    normalized_corrections = validate_overlay_corrections(corrections)
    supersessions_by_batch = normalized_corrections["supersessions_by_batch"]
    supersessions_by_prior_mapping_id: dict[str, dict] = {}
    for rows in supersessions_by_batch.values():
        for row in rows:
            prior_mapping_id = str(row["mapping_id"])
            if prior_mapping_id in supersessions_by_prior_mapping_id:
                raise RuntimeError(f"duplicate supersession prior mapping_id: {prior_mapping_id}")
            supersessions_by_prior_mapping_id[prior_mapping_id] = row
    return supersessions_by_batch, supersessions_by_prior_mapping_id


def _candidate_id(mapping_id: str) -> str:
    if not mapping_id.startswith("MAP:"):
        raise ValueError(f"invalid supersession mapping_id: {mapping_id}")
    return mapping_id.replace("MAP:", "MAPCAND:", 1)


def _mapping_id(candidate_id: str) -> str:
    if not candidate_id.startswith("MAPCAND:"):
        raise ValueError(f"invalid candidate_id: {candidate_id}")
    return candidate_id.replace("MAPCAND:", "MAP:", 1)


def _normalized_timestamp(value: object) -> str:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RuntimeError(f"invalid review timestamp: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeError("review timestamp must be timezone-aware")
    return parsed.isoformat()


def _empty_replay_result(review_config: dict, exposure_map: dict) -> dict:
    return {
        "status": "HUMAN_REVIEW_ALREADY_APPLIED",
        "reviewed_at": review_config.get("reviewed_at"),
        "approved_count": 0,
        "already_applied_count": 0,
        "redundant_already_applied_count": 0,
        "rejected_count": 0,
        "deferred_count": 0,
        "approved_mapping_ids": [],
        "already_applied_mapping_ids": [],
        "redundant_already_applied_mapping_ids": [],
        "exposure_map": deepcopy(exposure_map),
        "guards": {
            "market_outcomes_read": False,
            "automatic_approval_used": False,
            "mapping_backdated_before_review": False,
            "direction_assigned": False,
            "weights_or_thresholds_selected": False,
            "redundant_review_resolution_used_only_when_registered": True,
        },
    }


def _validate_historical_superseded_prior(*, candidate: dict, old_row: dict, mapping_id: str) -> None:
    if str(old_row.get("review_status") or "").upper() != "SUPERSEDED":
        raise RuntimeError(f"registered historical supersession prior interval is not SUPERSEDED: {mapping_id}")
    if old_row.get("valid_to") is None:
        raise RuntimeError(f"registered historical supersession prior interval is not closed: {mapping_id}")
    for field in ("subject_id", "factor_id", "relationship_class", "evidence_reference"):
        if old_row.get(field) != candidate.get(field):
            raise RuntimeError(f"registered historical supersession prior content mismatch for {field}: {mapping_id}")


def _replay_supersession_aware(
    *,
    candidate_config: dict,
    review_config: dict,
    exposure_map: dict,
    redundant_mapping_ids: set[str],
    supersessions: list[dict],
    supersessions_by_prior_mapping_id: dict[str, dict],
) -> dict:
    original_candidates = {
        str(row.get("candidate_id") or ""): row
        for row in candidate_config.get("candidates") or []
    }
    current_supersession_candidate_ids = {_candidate_id(str(row["mapping_id"])) for row in supersessions}

    historical_superseded_candidate_ids: set[str] = set()
    for candidate_id in original_candidates:
        mapping_id = _mapping_id(candidate_id)
        if mapping_id in supersessions_by_prior_mapping_id and candidate_id not in current_supersession_candidate_ids:
            historical_superseded_candidate_ids.add(candidate_id)

    filtered_candidate_ids = current_supersession_candidate_ids | historical_superseded_candidate_ids
    replay_candidates = deepcopy(candidate_config)
    replay_review = deepcopy(review_config)
    replay_candidates["candidates"] = [
        row for row in replay_candidates.get("candidates") or []
        if str(row.get("candidate_id") or "") not in filtered_candidate_ids
    ]
    replay_review["decisions"] = [
        row for row in replay_review.get("decisions") or []
        if str(row.get("candidate_id") or "") not in filtered_candidate_ids
    ]

    if replay_review["decisions"]:
        result = apply_human_mapping_review(
            candidate_config=replay_candidates,
            review_config=replay_review,
            exposure_map=exposure_map,
            redundant_mapping_ids=redundant_mapping_ids,
        )
    else:
        result = _empty_replay_result(review_config, exposure_map)

    reviewed_at = _normalized_timestamp(review_config.get("reviewed_at"))
    by_mapping_id = {
        str(row.get("mapping_id") or ""): row
        for row in exposure_map.get("mappings") or []
    }

    historical_replayed_ids: list[str] = []
    for candidate_id in sorted(historical_superseded_candidate_ids):
        candidate = original_candidates[candidate_id]
        mapping_id = _mapping_id(candidate_id)
        old_row = by_mapping_id.get(mapping_id)
        if old_row is None:
            raise RuntimeError(f"registered historical supersession prior mapping missing for replay: {mapping_id}")
        _validate_historical_superseded_prior(candidate=candidate, old_row=old_row, mapping_id=mapping_id)
        historical_replayed_ids.append(mapping_id)

    replayed_new_ids: list[str] = []
    for resolution in supersessions:
        old_id = str(resolution["mapping_id"])
        new_id = str(resolution["new_mapping_id"])
        candidate = original_candidates.get(_candidate_id(old_id))
        old_row = by_mapping_id.get(old_id)
        new_row = by_mapping_id.get(new_id)
        if candidate is None or old_row is None or new_row is None:
            raise RuntimeError(f"registered supersession is not materialized for replay: {old_id} -> {new_id}")
        if str(old_row.get("review_status") or "").upper() != "SUPERSEDED":
            raise RuntimeError(f"registered supersession prior interval is not SUPERSEDED: {old_id}")
        if old_row.get("valid_to") != reviewed_at:
            raise RuntimeError(f"registered supersession prior interval closes at wrong time: {old_id}")
        if str(new_row.get("review_status") or "").upper() != "ACTIVE" or new_row.get("valid_to") is not None:
            raise RuntimeError(f"registered supersession replacement is not ACTIVE: {new_id}")
        if new_row.get("reviewed_at") != reviewed_at or new_row.get("valid_from") != reviewed_at:
            raise RuntimeError(f"registered supersession replacement has wrong review interval: {new_id}")
        if new_row.get("subject_id") != candidate.get("subject_id") or new_row.get("factor_id") != candidate.get("factor_id"):
            raise RuntimeError(f"registered supersession replacement identity mismatch: {new_id}")
        if new_row.get("relationship_class") != candidate.get("relationship_class"):
            raise RuntimeError(f"registered supersession replacement class mismatch: {new_id}")
        if new_row.get("evidence_reference") != candidate.get("evidence_reference"):
            raise RuntimeError(f"registered supersession replacement evidence mismatch: {new_id}")
        replayed_new_ids.append(new_id)

    resolved_count = len(historical_replayed_ids) + len(replayed_new_ids)
    result["already_applied_count"] = int(result.get("already_applied_count") or 0) + resolved_count
    result["historical_superseded_already_applied_count"] = len(historical_replayed_ids)
    result["historical_superseded_already_applied_mapping_ids"] = historical_replayed_ids
    result["supersession_already_applied_count"] = len(replayed_new_ids)
    result["supersession_already_applied_mapping_ids"] = replayed_new_ids
    result.setdefault("already_applied_mapping_ids", []).extend(historical_replayed_ids)
    result.setdefault("already_applied_mapping_ids", []).extend(replayed_new_ids)
    result.setdefault("guards", {})["registered_historical_supersession_replay_used_only_when_registered"] = True
    result.setdefault("guards", {})["registered_supersession_replay_used_only_when_registered"] = True
    if int(result.get("approved_count") or 0) == 0:
        result["status"] = "HUMAN_REVIEW_ALREADY_APPLIED"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply explicit human review decisions to Phase 8F mapping candidates.")
    parser.add_argument("--candidates", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--review", type=Path, default=DEFAULT_REVIEW)
    parser.add_argument("--exposure-map", type=Path, default=DEFAULT_EXPOSURE)
    parser.add_argument("--overlay-registry", type=Path, default=DEFAULT_OVERLAY_REGISTRY)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--write-exposure-map", action="store_true")
    args = parser.parse_args()

    candidate_config = _load(args.candidates)
    review_config = _load(args.review)
    exposure_map = _load_exposure(args.exposure_map, args.overlay_registry)
    redundant_by_batch = load_registered_redundant_mapping_ids(root=ROOT, registry_path=args.overlay_registry)
    supersessions_by_batch, supersessions_by_prior_mapping_id = _load_supersession_registry(args.overlay_registry)
    batch_id = str(candidate_config.get("candidate_batch_id") or "")

    result = _replay_supersession_aware(
        candidate_config=candidate_config,
        review_config=review_config,
        exposure_map=exposure_map,
        redundant_mapping_ids=set(redundant_by_batch.get(batch_id, set())),
        supersessions=list(supersessions_by_batch.get(batch_id, [])),
        supersessions_by_prior_mapping_id=supersessions_by_prior_mapping_id,
    )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")

    if args.write_exposure_map:
        args.exposure_map.write_text(
            json.dumps(result["exposure_map"], indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
