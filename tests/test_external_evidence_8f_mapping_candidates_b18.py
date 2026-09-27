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
REVIEWED_AT = "2026-09-27T17:42+02:00"


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b18_mapping_candidates_pass_gate_and_cover_six_final_mapped_subjects():
    b18 = _load("configs/external_evidence_8f_mapping_candidates_b18_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b18, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 6
    assert result["factor_counts"] == {"fx": 2, "rates_policy": 4}
    assert {row["subject_id"] for row in b18["candidates"]} == B18_MAPPED_SUBJECTS
    assert all(row["human_reviewed"] is False for row in b18["candidates"])
    assert all(row["promotion_allowed"] is False for row in b18["candidates"])


def test_b18_mapping_subjects_are_active_with_review_timestamp():
    effective = load_effective_exposure_map(root=ROOT)
    by_subject = {row["subject_id"]: row for row in effective["mappings"]}
    assert len(effective["mappings"]) == 206
    assert B18_MAPPED_SUBJECTS <= set(by_subject)
    assert by_subject["6861.T"]["factor_id"] == "fx"
    assert by_subject["6861.T"]["relationship_class"] == "OTHER_DOCUMENTED"
    assert by_subject["FANUY"]["factor_id"] == "fx"
    assert by_subject["FANUY"]["relationship_class"] == "OTHER_DOCUMENTED"
    assert by_subject["9888.HK"]["factor_id"] == "rates_policy"
    assert by_subject["9888.HK"]["relationship_class"] == "OTHER_DOCUMENTED"
    assert by_subject["BIDU"]["factor_id"] == "rates_policy"
    assert by_subject["BIDU"]["relationship_class"] == "OTHER_DOCUMENTED"
    assert by_subject["DVLT"]["factor_id"] == "rates_policy"
    assert by_subject["DVLT"]["relationship_class"] == "REVENUE_LINK"
    assert by_subject["JD"]["factor_id"] == "rates_policy"
    assert by_subject["JD"]["relationship_class"] == "FINANCING_SENSITIVITY"
    assert all(by_subject[subject]["reviewed_at"] == REVIEWED_AT for subject in B18_MAPPED_SUBJECTS)
    assert all(by_subject[subject]["valid_from"] == REVIEWED_AT for subject in B18_MAPPED_SUBJECTS)


def test_b18_mapping_review_artifact_is_completed_and_idempotent():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b18_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b18_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert review["reviewer_role"] == "HUMAN_REVIEWER"
    assert review["reviewed_at"] == REVIEWED_AT
    assert len(review["decisions"]) == 6
    assert all(item["decision"] == "APPROVE" for item in review["decisions"])
    assert all(item["source_verified"] is True for item in review["decisions"])
    assert all(item["relationship_class_confirmed"] is True for item in review["decisions"])
    assert all(value is False for value in review["guards"].values())
    result = apply_human_mapping_review(candidate_config=candidates, review_config=review, exposure_map=effective)
    assert result["status"] == "HUMAN_REVIEW_ALREADY_APPLIED"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 6
    assert result["redundant_already_applied_count"] == 0
    assert len(result["exposure_map"]["mappings"]) == 206


def test_b18_explicit_unmapped_tokyo_electron_is_human_reviewed_and_active_in_domain():
    proposal = _load("configs/external_evidence_8f_explicit_unmapped_candidates_b18_v1.json")
    review = _load("configs/external_evidence_8f_explicit_unmapped_review_decisions_b18_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    assert proposal["guards"]["proposal_count"] == 1
    assert {row["subject_id"] for row in proposal["proposals"]} == B18_EXPLICIT_UNMAPPED_SUBJECTS
    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert review["reviewer_role"] == "HUMAN_REVIEWER"
    assert review["reviewed_at"] == REVIEWED_AT
    assert len(review["decisions"]) == 1
    assert review["decisions"][0]["subject_id"] == "8035.T"
    assert review["decisions"][0]["decision"] == "APPROVE_EXPLICIT_UNMAPPED"
    assert review["decisions"][0]["source_verified"] is True
    assert review["decisions"][0]["reason_confirmed"] is True
    assert all(value is False for value in review["guards"].values())
    assert {row["subject_id"] for row in domain["explicit_unmapped"]} == B18_EXPLICIT_UNMAPPED_SUBJECTS
    assert domain["explicit_unmapped"][0]["reviewed_at"] == REVIEWED_AT


def test_b18_completes_207_subject_accounting_without_forcing_8035_mapping():
    effective = load_effective_exposure_map(root=ROOT)
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    assert "8035.T" not in active_subjects
    assert len(effective["mappings"]) == 206
    assert len(domain["explicit_unmapped"]) == 1
    assert len(effective["mappings"]) + len(domain["explicit_unmapped"]) == 207
