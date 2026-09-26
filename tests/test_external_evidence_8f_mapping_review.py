from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.exposure_mapping_review_8f import (
    ExposureMappingReview8FError,
    apply_human_mapping_review,
)


ROOT = Path(__file__).resolve().parents[1]


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _empty_exposure() -> dict:
    exposure = copy.deepcopy(_load("configs/external_evidence_8f_exposure_map_v1.json"))
    exposure["status"] = "FOUNDATION_EMPTY_MAP_NO_ASSET_EXPOSURES_PROMOTED"
    exposure["mappings"] = []
    return exposure


def _empty_review() -> dict:
    review = copy.deepcopy(_load("configs/external_evidence_8f_mapping_review_decisions_v1.json"))
    review["status"] = "AWAITING_HUMAN_REVIEW"
    review["reviewed_at"] = None
    review["decisions"] = []
    return review


def _approved_review(candidate_id: str = "MAPCAND:CVX:oil:1") -> dict:
    return {
        "schema_version": "external_evidence_8f_mapping_review_decisions_v1",
        "phase": "8F_macro_exposure_context",
        "candidate_batch_id": "8F_MAPPING_CANDIDATES_2026-09-26_B1",
        "status": "HUMAN_REVIEW_COMPLETED",
        "reviewer_role": "HUMAN_REVIEWER",
        "reviewed_at": "2026-09-26T20:00:00+00:00",
        "decisions": [
            {
                "candidate_id": candidate_id,
                "decision": "APPROVE",
                "source_verified": True,
                "relationship_class_confirmed": True,
                "notes": "reviewed test candidate",
            }
        ],
        "guards": {
            "market_outcomes_read": False,
            "automatic_approval_allowed": False,
            "backdating_before_review_allowed": False,
            "direction_or_signed_exposure_assigned": False,
            "weights_or_thresholds_selected": False,
        },
    }


def test_empty_review_decisions_do_not_modify_exposure_map():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    review = _empty_review()
    exposure = _empty_exposure()
    result = apply_human_mapping_review(
        candidate_config=candidates,
        review_config=review,
        exposure_map=exposure,
    )
    assert result["status"] == "AWAITING_HUMAN_REVIEW"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 0
    assert result["exposure_map"]["mappings"] == []


def test_committed_b1_review_contains_ten_explicit_human_approvals():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_v1.json")
    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert len(review["decisions"]) == 10
    assert all(item["decision"] == "APPROVE" for item in review["decisions"])
    assert all(item["source_verified"] is True for item in review["decisions"])
    assert all(item["relationship_class_confirmed"] is True for item in review["decisions"])


def test_committed_b1_review_replay_is_idempotent():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_v1.json")
    exposure = _load("configs/external_evidence_8f_exposure_map_v1.json")
    result = apply_human_mapping_review(
        candidate_config=candidates,
        review_config=review,
        exposure_map=exposure,
    )
    assert result["status"] == "HUMAN_REVIEW_ALREADY_APPLIED"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 10
    assert len(result["exposure_map"]["mappings"]) == 10


def test_explicit_human_approval_promotes_only_reviewed_candidate_at_review_time():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    exposure = _empty_exposure()
    result = apply_human_mapping_review(
        candidate_config=candidates,
        review_config=_approved_review(),
        exposure_map=exposure,
    )
    assert result["status"] == "HUMAN_REVIEW_APPLIED"
    assert result["approved_count"] == 1
    assert result["already_applied_count"] == 0
    mapping = result["exposure_map"]["mappings"][0]
    assert mapping["mapping_id"] == "MAP:CVX:oil:1"
    assert mapping["human_reviewed"] is True
    assert mapping["reviewed_at"] == "2026-09-26T20:00:00+00:00"
    assert mapping["valid_from"] == mapping["reviewed_at"]
    assert mapping["evidence_valid_from"] == mapping["reviewed_at"]
    assert result["guards"]["mapping_backdated_before_review"] is False


def test_approval_requires_source_verification():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    exposure = _empty_exposure()
    review = _approved_review()
    review["decisions"][0]["source_verified"] = False
    with pytest.raises(ExposureMappingReview8FError, match="source_verified=true"):
        apply_human_mapping_review(
            candidate_config=candidates,
            review_config=review,
            exposure_map=exposure,
        )


def test_reject_and_defer_never_promote():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    exposure = _empty_exposure()
    review = _approved_review()
    review["decisions"] = [
        {"candidate_id": "MAPCAND:CVX:oil:1", "decision": "REJECT"},
        {"candidate_id": "MAPCAND:CCJ:uranium:1", "decision": "DEFER"},
    ]
    result = apply_human_mapping_review(
        candidate_config=candidates,
        review_config=review,
        exposure_map=exposure,
    )
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 0
    assert result["rejected_count"] == 1
    assert result["deferred_count"] == 1
    assert result["exposure_map"]["mappings"] == []


def test_review_cannot_enable_automatic_approval_guard():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    exposure = _empty_exposure()
    review = _approved_review()
    review["guards"]["automatic_approval_allowed"] = True
    with pytest.raises(ExposureMappingReview8FError, match="must remain false"):
        apply_human_mapping_review(
            candidate_config=candidates,
            review_config=review,
            exposure_map=exposure,
        )
