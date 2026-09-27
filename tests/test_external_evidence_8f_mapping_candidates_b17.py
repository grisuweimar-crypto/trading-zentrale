from __future__ import annotations

import json
from pathlib import Path

from scanner.research.external_evidence.exposure_map_store_8f import (
    load_effective_exposure_map,
    load_registered_redundant_mapping_ids,
)
from scanner.research.external_evidence.exposure_mapping_candidates_8f import validate_mapping_candidates
from scanner.research.external_evidence.exposure_review_queue_8f import build_exposure_review_queue


ROOT = Path(__file__).resolve().parents[1]
REVIEWED_AT = "2026-09-27T16:38+02:00"
B17_SUBJECTS = {
    "1810.HK", "9880.HK", "CGNX", "000660.KS", "005930.KS",
    "0700.HK", "IFX.DE", "SAP.DE", "YASKY", "6506.T",
}
B17_NEW_SUBJECTS = {"1810.HK", "9880.HK", "000660.KS", "005930.KS", "0700.HK", "YASKY", "6506.T"}
B17_RECLASSIFIED_SUBJECTS = {"CGNX", "IFX.DE"}
B17_AUDIT_ONLY_SUBJECTS = {"SAP.DE"}


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b17_documentary_candidate_batch_passes_gate_and_has_ten_distinct_subjects():
    b17 = _load("configs/external_evidence_8f_mapping_candidates_b17_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b17, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 10
    assert result["factor_counts"] == {"fx": 6, "rates_policy": 4}
    assert {row["subject_id"] for row in b17["candidates"]} == B17_SUBJECTS
    assert all(row["human_reviewed"] is False for row in b17["candidates"])
    assert all(row["promotion_allowed"] is False for row in b17["candidates"])


def test_b17_effective_state_distinguishes_new_audit_only_and_reclassified_subjects():
    effective = load_effective_exposure_map(root=ROOT)
    active_rows = [row for row in effective["mappings"] if row["review_status"] == "ACTIVE"]
    active_by_subject = {row["subject_id"]: row for row in active_rows}
    assert len(effective["mappings"]) == 195
    assert len(active_rows) == 189
    assert B17_SUBJECTS <= set(active_by_subject)

    for subject in {"1810.HK", "9880.HK", "000660.KS", "0700.HK"}:
        assert active_by_subject[subject]["factor_id"] == "rates_policy"
        assert active_by_subject[subject]["relationship_class"] == "FINANCING_SENSITIVITY"
    assert active_by_subject["CGNX"]["factor_id"] == "fx"
    assert active_by_subject["CGNX"]["relationship_class"] == "CURRENCY_TRANSLATION"
    assert active_by_subject["SAP.DE"]["factor_id"] == "fx"
    assert active_by_subject["SAP.DE"]["relationship_class"] == "CURRENCY_TRANSLATION"
    for subject in {"005930.KS", "IFX.DE", "YASKY", "6506.T"}:
        assert active_by_subject[subject]["factor_id"] == "fx"
        assert active_by_subject[subject]["relationship_class"] == "OTHER_DOCUMENTED"

    assert all(active_by_subject[subject]["reviewed_at"] == REVIEWED_AT for subject in B17_NEW_SUBJECTS | B17_RECLASSIFIED_SUBJECTS)
    assert all(active_by_subject[subject]["valid_from"] == REVIEWED_AT for subject in B17_NEW_SUBJECTS | B17_RECLASSIFIED_SUBJECTS)
    assert active_by_subject["SAP.DE"]["reviewed_at"] != REVIEWED_AT


def test_b17_review_artifact_is_explicit_human_review():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b17_v1.json")
    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert review["reviewer_role"] == "HUMAN_REVIEWER"
    assert review["reviewed_at"] == REVIEWED_AT
    assert len(review["decisions"]) == 10
    assert all(item["decision"] == "APPROVE" for item in review["decisions"])
    assert all(item["source_verified"] is True for item in review["decisions"])
    assert all(item["relationship_class_confirmed"] is True for item in review["decisions"])
    assert all(value is False for value in review["guards"].values())


def test_b17_resolution_ledger_registers_one_audit_only_and_two_supersessions():
    registered = load_registered_redundant_mapping_ids(root=ROOT)
    assert registered["8F_MAPPING_CANDIDATES_2026-09-27_B17"] == {"MAP:SAP.DE:fx:1"}

    resolutions = _load("configs/external_evidence_8f_overlay_resolutions_v2.json")["resolutions"]
    rows = [
        row for row in resolutions
        if row["batch_id"] == "8F_MAPPING_CANDIDATES_2026-09-27_B17"
        and row["action"] == "SUPERSEDE_WITH_HUMAN_REVIEWED_MAPPING"
    ]
    assert {row["subject_id"] for row in rows} == B17_RECLASSIFIED_SUBJECTS
    assert {row["new_mapping_id"] for row in rows} == {"MAP:CGNX:fx:2", "MAP:IFX.DE:fx:2"}
