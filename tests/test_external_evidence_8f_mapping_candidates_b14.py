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


def test_b14_documentary_candidate_batch_passes_gate_and_has_two_distinct_subjects():
    b14 = _load("configs/external_evidence_8f_mapping_candidates_b14_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b14, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 2
    assert result["factor_counts"] == {"rates_policy": 2}
    assert {row["subject_id"] for row in b14["candidates"]} == {"PATH", "9988.HK"}
    assert all(row["human_reviewed"] is False for row in b14["candidates"])
    assert all(row["promotion_allowed"] is False for row in b14["candidates"])


def test_b14_adds_only_subjects_not_already_covered_by_effective_b1_through_b13_map():
    b14 = _load("configs/external_evidence_8f_mapping_candidates_b14_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    b14_subjects = {row["subject_id"] for row in b14["candidates"]}
    assert len(effective["mappings"]) == 169
    assert active_subjects.isdisjoint(b14_subjects)


def test_b14_review_artifact_is_explicitly_pending():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b14_v1.json")
    assert review["status"] == "AWAITING_HUMAN_REVIEW"
    assert review["reviewed_at"] is None
    assert review["decisions"] == []
    assert all(value is False for value in review["guards"].values())


def test_pending_b14_review_cannot_modify_effective_169_mapping_exposure_map():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b14_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b14_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    assert len(effective["mappings"]) == 169
    result = apply_human_mapping_review(candidate_config=candidates, review_config=review, exposure_map=effective)
    assert result["status"] == "AWAITING_HUMAN_REVIEW"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 0
    assert len(result["exposure_map"]["mappings"]) == 169
