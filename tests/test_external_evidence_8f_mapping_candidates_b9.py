from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.exposure_map_store_8f import load_effective_exposure_map
from scanner.research.external_evidence.exposure_mapping_candidates_8f import validate_mapping_candidates
from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b9_documentary_candidate_batch_passes_gate_and_has_twelve_distinct_subjects():
    b9 = _load("configs/external_evidence_8f_mapping_candidates_b9_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b9, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 12
    assert result["factor_counts"] == {"fx": 4, "gold": 1, "inflation": 1, "rates_policy": 2, "silver": 4}
    assert len({row["subject_id"] for row in b9["candidates"]}) == 12
    assert all(row["human_reviewed"] is False for row in b9["candidates"])
    assert all(row["promotion_allowed"] is False for row in b9["candidates"])


def test_b9_subjects_are_present_in_current_effective_reviewed_map_after_approval():
    b9 = _load("configs/external_evidence_8f_mapping_candidates_b9_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    b9_subjects = {row["subject_id"] for row in b9["candidates"]}
    assert b9_subjects.issubset(active_subjects)


def test_b9_review_artifact_is_human_review_completed():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b9_v1.json")
    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert review["reviewed_at"] == "2026-09-27T07:55:08+02:00"
    assert len(review["decisions"]) == 12
    assert all(item["decision"] == "APPROVE" for item in review["decisions"])
    assert all(item["source_verified"] is True for item in review["decisions"])
    assert all(item["relationship_class_confirmed"] is True for item in review["decisions"])
    assert all(value is False for value in review["guards"].values())


def test_b9_review_replay_is_idempotent_against_current_effective_map():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b9_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b9_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    before_count = len(effective["mappings"])
    result = apply_human_mapping_review(candidate_config=candidates, review_config=review, exposure_map=effective)
    assert result["status"] == "HUMAN_REVIEW_ALREADY_APPLIED"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 12
    assert len(result["exposure_map"]["mappings"]) == before_count
