from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.exposure_map_store_8f import load_effective_exposure_map
from scanner.research.external_evidence.exposure_mapping_candidates_8f import validate_mapping_candidates
from scanner.research.external_evidence.exposure_mapping_review_8f import apply_human_mapping_review
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]
B15_SUBJECTS = {"SYM", "USAR", "NESN.SW", "TOM.OL", "UMI.BR"}


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b15_documentary_candidate_batch_passes_gate_and_has_five_distinct_subjects():
    b15 = _load("configs/external_evidence_8f_mapping_candidates_b15_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b15, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 5
    assert result["factor_counts"] == {"fx": 2, "rates_policy": 3}
    assert {row["subject_id"] for row in b15["candidates"]} == B15_SUBJECTS
    assert all(row["human_reviewed"] is False for row in b15["candidates"])
    assert all(row["promotion_allowed"] is False for row in b15["candidates"])


def test_b15_candidates_are_currently_unaccounted_and_do_not_modify_effective_map():
    effective = load_effective_exposure_map(root=ROOT)
    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    completion = _load("configs/external_evidence_8f_completion_v1.json")
    assert len(effective["mappings"]) == 171
    assert active_subjects.isdisjoint(B15_SUBJECTS)
    assert completion["expected_mapped_subject_count"] == 171
    assert completion["expected_unaccounted_subject_count"] == 36


def test_b15_review_artifact_is_explicitly_pending_and_empty():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b15_v1.json")
    assert review["status"] == "PENDING_HUMAN_REVIEW"
    assert review["reviewed_at"] is None
    assert review["reviewer_role"] is None
    assert review["decisions"] == []
    assert all(value is False for value in review["guards"].values())


def test_b15_pending_review_replay_is_idempotent_and_keeps_171_active_mappings():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b15_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b15_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    result = apply_human_mapping_review(candidate_config=candidates, review_config=review, exposure_map=effective)
    assert result["status"] == "AWAITING_HUMAN_REVIEW"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 0
    assert result["redundant_already_applied_count"] == 0
    assert result["rejected_count"] == 0
    assert result["deferred_count"] == 0
    assert result["exposure_map"] == effective
    assert len(result["exposure_map"]["mappings"]) == 171
