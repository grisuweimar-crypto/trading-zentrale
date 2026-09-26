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
    return (
        {row["factor_id"] for row in macro["factor_catalog"]},
        {row["subject_id"] for row in queue["subjects"]},
    )


def test_b6_documentary_candidate_batch_passes_gate_and_has_fourteen_distinct_subjects():
    b6 = _load("configs/external_evidence_8f_mapping_candidates_b6_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(
        b6,
        allowed_factor_ids=factor_ids,
        allowed_subject_ids=subject_ids,
    )
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 14
    assert result["factor_counts"] == {"fx": 13, "uranium": 1}
    assert len({row["subject_id"] for row in b6["candidates"]}) == 14
    assert all(row["human_reviewed"] is False for row in b6["candidates"])
    assert all(row["promotion_allowed"] is False for row in b6["candidates"])


def test_b6_is_active_in_effective_map_and_review_is_explicit_human_approval():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b6_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b6_v1.json")
    effective = load_effective_exposure_map(root=ROOT)

    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert review["reviewed_at"] == "2026-09-26T23:28:24+02:00"
    assert len(review["decisions"]) == 14
    assert all(row["decision"] == "APPROVE" for row in review["decisions"])
    assert all(row["source_verified"] is True for row in review["decisions"])
    assert all(row["relationship_class_confirmed"] is True for row in review["decisions"])

    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    b6_subjects = {row["subject_id"] for row in candidates["candidates"]}
    assert b6_subjects.issubset(active_subjects)
    assert len(effective["mappings"]) == 94


def test_b6_replay_is_idempotent_against_effective_ninety_four_mapping_map():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b6_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b6_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    result = apply_human_mapping_review(
        candidate_config=candidates,
        review_config=review,
        exposure_map=effective,
    )
    assert result["status"] == "HUMAN_REVIEW_ALREADY_APPLIED"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 14
    assert len(result["exposure_map"]["mappings"]) == 94
