from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.exposure_mapping_candidates_8f import (
    ExposureMappingCandidates8FError,
    validate_mapping_candidates,
)
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]


def _inputs(path: str = "configs/external_evidence_8f_mapping_candidates_v1.json") -> tuple[dict, set[str], set[str]]:
    config = json.loads((ROOT / path).read_text(encoding="utf-8"))
    macro = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )
    domain = json.loads(
        (ROOT / "configs/external_evidence_8f_research_domain_v1.json").read_text(encoding="utf-8")
    )
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    subject_ids = {row["subject_id"] for row in queue["subjects"]}
    factor_ids = {row["factor_id"] for row in macro["factor_catalog"]}
    return config, factor_ids, subject_ids


def test_first_documentary_candidate_batch_passes_fail_closed_gate():
    config, factor_ids, subject_ids = _inputs()
    result = validate_mapping_candidates(
        config,
        allowed_factor_ids=factor_ids,
        allowed_subject_ids=subject_ids,
    )
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 10
    assert result["promotion_allowed"] is False
    assert result["human_review_claimed"] is False
    assert result["market_outcomes_read"] is False
    assert result["factor_counts"] == {
        "copper": 1,
        "gold": 1,
        "lithium": 1,
        "oil": 3,
        "silver": 1,
        "uranium": 3,
    }


def test_second_documentary_candidate_batch_passes_and_is_disjoint_from_b1():
    b1, factor_ids, subject_ids = _inputs()
    b2, _, _ = _inputs("configs/external_evidence_8f_mapping_candidates_b2_v1.json")
    result = validate_mapping_candidates(
        b2,
        allowed_factor_ids=factor_ids,
        allowed_subject_ids=subject_ids,
    )
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 10
    assert result["factor_counts"] == {"gold": 5, "oil": 1, "silver": 4}
    b1_pairs = {(row["subject_id"], row["factor_id"]) for row in b1["candidates"]}
    b2_pairs = {(row["subject_id"], row["factor_id"]) for row in b2["candidates"]}
    assert b1_pairs.isdisjoint(b2_pairs)


def test_candidate_cannot_claim_human_review_or_promotion():
    config, factor_ids, subject_ids = _inputs()
    broken = copy.deepcopy(config)
    broken["candidates"][0]["human_reviewed"] = True
    broken["candidates"][0]["promotion_allowed"] = True
    with pytest.raises(ExposureMappingCandidates8FError, match="may not claim review/promotion"):
        validate_mapping_candidates(broken, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)


def test_sector_or_name_shortcut_cannot_be_enabled():
    config, factor_ids, subject_ids = _inputs()
    broken = copy.deepcopy(config)
    broken["promotion_rules"]["sector_or_name_alone_is_sufficient_evidence"] = True
    with pytest.raises(ExposureMappingCandidates8FError, match="invalid candidate promotion rules"):
        validate_mapping_candidates(broken, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)


def test_candidate_must_belong_to_frozen_pre8f_domain():
    config, factor_ids, subject_ids = _inputs()
    broken = copy.deepcopy(config)
    broken["candidates"][0]["subject_id"] = "NOT_IN_FROZEN_DOMAIN"
    with pytest.raises(ExposureMappingCandidates8FError, match="outside frozen domain"):
        validate_mapping_candidates(broken, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)


def test_evidence_fingerprint_detects_candidate_mutation():
    config, factor_ids, subject_ids = _inputs()
    broken = copy.deepcopy(config)
    broken["candidates"][0]["evidence_summary"] += " changed"
    with pytest.raises(ExposureMappingCandidates8FError, match="fingerprint mismatch"):
        validate_mapping_candidates(broken, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
