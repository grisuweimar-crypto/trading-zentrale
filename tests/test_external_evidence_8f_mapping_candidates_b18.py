from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.exposure_map_store_8f import load_effective_exposure_map
from scanner.research.external_evidence.exposure_mapping_candidates_8f import validate_mapping_candidates
from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]
B18_MAPPED_SUBJECTS = {"6861.T", "FANUY", "9888.HK", "BIDU", "DVLT", "JD"}
B18_EXPLICIT_UNMAPPED_SUBJECTS = {"8035.T"}


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b18_mapping_candidates_pass_gate_and_cover_six_of_seven_remaining_subjects():
    b18 = _load("configs/external_evidence_8f_mapping_candidates_b18_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b18, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 6
    assert result["factor_counts"] == {"fx": 2, "rates_policy": 4}
    assert {row["subject_id"] for row in b18["candidates"]} == B18_MAPPED_SUBJECTS
    assert all(row["human_reviewed"] is False for row in b18["candidates"])
    assert all(row["promotion_allowed"] is False for row in b18["candidates"])


def test_b18_mapping_candidates_are_unaccounted_and_pending_review_is_non_mutating():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b18_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b18_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    assert len(effective["mappings"]) == 200
    assert active_subjects.isdisjoint(B18_MAPPED_SUBJECTS)
    result = apply_human_mapping_review(candidate_config=candidates, review_config=review, exposure_map=effective)
    assert result["status"] == "AWAITING_HUMAN_REVIEW"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 0
    assert result["exposure_map"] == effective


def test_b18_mapping_review_artifact_is_pending_and_empty():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b18_v1.json")
    assert review["status"] == "PENDING_HUMAN_REVIEW"
    assert review["reviewed_at"] is None
    assert review["reviewer_role"] is None
    assert review["decisions"] == []
    assert all(value is False for value in review["guards"].values())


def test_b18_explicit_unmapped_proposal_is_single_pending_tokyo_electron_subject():
    proposal = _load("configs/external_evidence_8f_explicit_unmapped_candidates_b18_v1.json")
    review = _load("configs/external_evidence_8f_explicit_unmapped_review_decisions_b18_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    assert proposal["status"] == "PENDING_HUMAN_REVIEW"
    assert proposal["guards"]["proposal_count"] == 1
    assert {row["subject_id"] for row in proposal["proposals"]} == B18_EXPLICIT_UNMAPPED_SUBJECTS
    assert proposal["proposals"][0]["proposal"] == "EXPLICIT_UNMAPPED"
    assert proposal["proposals"][0]["human_reviewed"] is False
    assert proposal["proposals"][0]["promotion_allowed"] is False
    assert review["status"] == "PENDING_HUMAN_REVIEW"
    assert review["reviewed_at"] is None
    assert review["decisions"] == []
    assert domain["explicit_unmapped"] == []


def test_b18_full_approval_would_complete_207_subject_accounting_without_forcing_8035_mapping():
    effective = load_effective_exposure_map(root=ROOT)
    assert len(effective["mappings"]) == 200
    assert len(effective["mappings"]) + len(B18_MAPPED_SUBJECTS) + len(B18_EXPLICIT_UNMAPPED_SUBJECTS) == 207
    assert B18_MAPPED_SUBJECTS.isdisjoint(B18_EXPLICIT_UNMAPPED_SUBJECTS)
