from __future__ import annotations

import json
from pathlib import Path

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


def test_b3_documentary_candidate_batch_passes_gate_and_has_twenty_distinct_subjects():
    b3 = _load("configs/external_evidence_8f_mapping_candidates_b3_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(
        b3,
        allowed_factor_ids=factor_ids,
        allowed_subject_ids=subject_ids,
    )
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 20
    assert result["factor_counts"] == {
        "fx": 10,
        "gas": 1,
        "gold": 2,
        "rates_policy": 6,
        "silver": 1,
    }
    assert len({row["subject_id"] for row in b3["candidates"]}) == 20
    assert all(row["human_reviewed"] is False for row in b3["candidates"])
    assert all(row["promotion_allowed"] is False for row in b3["candidates"])


def test_b3_pairs_are_disjoint_from_b1_and_b2():
    b1 = _load("configs/external_evidence_8f_mapping_candidates_v1.json")
    b2 = _load("configs/external_evidence_8f_mapping_candidates_b2_v1.json")
    b3 = _load("configs/external_evidence_8f_mapping_candidates_b3_v1.json")

    def pairs(config: dict) -> set[tuple[str, str]]:
        return {(row["subject_id"], row["factor_id"]) for row in config["candidates"]}

    assert pairs(b1).isdisjoint(pairs(b3))
    assert pairs(b2).isdisjoint(pairs(b3))


def test_empty_b3_human_review_cannot_modify_active_twenty_mapping_exposure_map():
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b3_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b3_v1.json")
    exposure = _load("configs/external_evidence_8f_exposure_map_v1.json")
    assert len(exposure["mappings"]) == 20
    result = apply_human_mapping_review(
        candidate_config=candidates,
        review_config=review,
        exposure_map=exposure,
    )
    assert result["status"] == "AWAITING_HUMAN_REVIEW"
    assert result["approved_count"] == 0
    assert result["already_applied_count"] == 0
    assert len(result["exposure_map"]["mappings"]) == 20
