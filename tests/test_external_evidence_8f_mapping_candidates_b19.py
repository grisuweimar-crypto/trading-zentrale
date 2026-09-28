from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.exposure_domain_8f import build_exposure_domain_audit
from scanner.research.external_evidence.exposure_map_store_8f import load_effective_exposure_map
from scanner.research.external_evidence.exposure_mapping_candidates_8f import validate_mapping_candidates
from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]
B19_MAPPING_SUBJECTS = {"GOT.V", "SGM.AX", "TME", "GRAB", "OCGN", "NPN.JO"}
B19_EXPLICIT_UNMAPPED_SUBJECTS = {
    "LGO",
    "LC0A.MU",
    "MNSO",
    "NOVO-B.CO",
    "RCAT",
    "ACB.TO",
    "SMX",
    "SE",
    "SPCX",
    "DRO.AX",
    "1211.HK",
}
B19_ALL_SUBJECTS = B19_MAPPING_SUBJECTS | B19_EXPLICIT_UNMAPPED_SUBJECTS


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b19_documentary_mapping_candidates_pass_gate_but_remain_pending():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b19_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(candidates, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 6
    assert result["factor_counts"] == {"copper": 1, "fx": 1, "gold": 1, "rates_policy": 3}
    assert {row["subject_id"] for row in candidates["candidates"]} == B19_MAPPING_SUBJECTS
    assert candidates["status"] == "DOCUMENTARY_CANDIDATES_PENDING_HUMAN_REVIEW"
    assert all(row["human_reviewed"] is False for row in candidates["candidates"])
    assert all(row["promotion_allowed"] is False for row in candidates["candidates"])


def test_b19_mapping_review_is_pending_and_replay_is_non_mutating():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b19_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b19_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    assert review["status"] == "PENDING_HUMAN_REVIEW"
    assert review["reviewer_role"] is None
    assert review["reviewed_at"] is None
    assert review["decisions"] == []
    assert all(value is False for value in review["guards"].values())
    result = apply_human_mapping_review(candidate_config=candidates, review_config=review, exposure_map=effective)
    assert result["status"] == "AWAITING_HUMAN_REVIEW"
    assert result["approved_count"] == 0
    assert result["exposure_map"] == effective


def test_b19_explicit_unmapped_proposals_are_pending_and_partition_the_true_remainder():
    proposal = _load("configs/external_evidence_8f_explicit_unmapped_candidates_b19_v1.json")
    review = _load("configs/external_evidence_8f_explicit_unmapped_review_decisions_b19_v1.json")
    assert proposal["status"] == "PENDING_HUMAN_REVIEW"
    assert proposal["guards"]["proposal_count"] == 11
    assert {row["subject_id"] for row in proposal["proposals"]} == B19_EXPLICIT_UNMAPPED_SUBJECTS
    assert all(row["human_reviewed"] is False for row in proposal["proposals"])
    assert all(row["promotion_allowed"] is False for row in proposal["proposals"])
    assert review["status"] == "PENDING_HUMAN_REVIEW"
    assert review["reviewer_role"] is None
    assert review["reviewed_at"] is None
    assert review["decisions"] == []
    assert all(value is False for value in review["guards"].values())

    effective = load_effective_exposure_map(root=ROOT)
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe_text = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    audit = build_exposure_domain_audit(domain_config=domain, exposure_map=effective, universe_csv_text=universe_text)
    assert set(audit["unaccounted_subject_ids"]) == B19_ALL_SUBJECTS
    assert audit["unaccounted_subject_count"] == 17


def test_b19_pending_state_does_not_falsely_complete_frozen_domain():
    effective = load_effective_exposure_map(root=ROOT)
    active_rows = [row for row in effective["mappings"] if row["review_status"] == "ACTIVE"]
    active_subjects = {row["subject_id"] for row in active_rows}
    assert len(effective["mappings"]) == 195
    assert len(active_rows) == 189
    assert B19_MAPPING_SUBJECTS.isdisjoint(active_subjects)

    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe_text = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    audit = build_exposure_domain_audit(domain_config=domain, exposure_map=effective, universe_csv_text=universe_text)
    assert audit["mapped_subject_count"] == 189
    assert audit["explicit_unmapped_subject_count"] == 1
    assert audit["unaccounted_subject_count"] == 17
    assert audit["mapped_subject_count"] + audit["explicit_unmapped_subject_count"] + audit["unaccounted_subject_count"] == 207


def test_b19_full_human_approval_would_account_for_all_207_without_claiming_activation_now():
    assert len(B19_MAPPING_SUBJECTS) == 6
    assert len(B19_EXPLICIT_UNMAPPED_SUBJECTS) == 11
    assert len(B19_ALL_SUBJECTS) == 17
    assert 189 + len(B19_MAPPING_SUBJECTS) == 195
    assert 1 + len(B19_EXPLICIT_UNMAPPED_SUBJECTS) == 12
    assert 195 + 12 == 207
