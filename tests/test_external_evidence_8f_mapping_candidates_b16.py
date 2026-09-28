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
REVIEWED_AT = "2026-09-27T14:34:54+02:00"
B16_SUBJECTS = {
    "ABT", "ISRG", "ROK", "SYK", "TER", "AMAT", "AMZN", "ASML",
    "GOOGL", "META", "MSFT", "MU", "ORCL", "PLTR",
}
B16_AUDIT_ONLY_SUBJECTS = {"ABT", "ISRG", "ROK", "TER", "AMAT", "AMZN", "ASML", "MU", "ORCL", "PLTR"}
B16_RECLASSIFIED_SUBJECTS = {"SYK", "GOOGL", "META", "MSFT"}


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _allowed() -> tuple[set[str], set[str]]:
    macro = _load("configs/external_evidence_8f_macro_exposure_v1.json")
    domain = _load("configs/external_evidence_8f_research_domain_v1.json")
    universe = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    queue = build_exposure_review_queue(domain_config=domain, universe_csv=universe)
    return ({row["factor_id"] for row in macro["factor_catalog"]}, {row["subject_id"] for row in queue["subjects"]})


def test_b16_documentary_candidate_batch_passes_gate_and_has_fourteen_distinct_subjects():
    b16 = _load("configs/external_evidence_8f_mapping_candidates_b16_v1.json")
    factor_ids, subject_ids = _allowed()
    result = validate_mapping_candidates(b16, allowed_factor_ids=factor_ids, allowed_subject_ids=subject_ids)
    assert result["status"] == "PASS_DOCUMENTARY_CANDIDATE_GATE"
    assert result["candidate_count"] == 14
    assert result["factor_counts"] == {"fx": 14}
    assert {row["subject_id"] for row in b16["candidates"]} == B16_SUBJECTS
    assert all(row["human_reviewed"] is False for row in b16["candidates"])
    assert all(row["promotion_allowed"] is False for row in b16["candidates"])


def test_b16_effective_state_distinguishes_audit_only_from_reclassification():
    effective = load_effective_exposure_map(root=ROOT)
    active_rows = [row for row in effective["mappings"] if row["review_status"] == "ACTIVE"]
    active_by_subject = {row["subject_id"]: row for row in active_rows}
    assert len(effective["mappings"]) == 201
    assert len(active_rows) == 195
    assert B16_SUBJECTS <= set(active_by_subject)

    expected_classes = {
        "ABT": "CURRENCY_TRANSLATION", "ISRG": "OTHER_DOCUMENTED", "ROK": "CURRENCY_TRANSLATION",
        "SYK": "OTHER_DOCUMENTED", "TER": "OTHER_DOCUMENTED", "AMAT": "OTHER_DOCUMENTED",
        "AMZN": "CURRENCY_TRANSLATION", "ASML": "CURRENCY_TRANSLATION", "GOOGL": "OTHER_DOCUMENTED",
        "META": "OTHER_DOCUMENTED", "MSFT": "OTHER_DOCUMENTED", "MU": "OTHER_DOCUMENTED",
        "ORCL": "CURRENCY_TRANSLATION", "PLTR": "OTHER_DOCUMENTED",
    }
    for subject, relationship_class in expected_classes.items():
        assert active_by_subject[subject]["factor_id"] == "fx"
        assert active_by_subject[subject]["relationship_class"] == relationship_class

    assert all(active_by_subject[subject]["reviewed_at"] == REVIEWED_AT for subject in B16_RECLASSIFIED_SUBJECTS)
    assert all(active_by_subject[subject]["valid_from"] == REVIEWED_AT for subject in B16_RECLASSIFIED_SUBJECTS)
    assert all(active_by_subject[subject]["reviewed_at"] != REVIEWED_AT for subject in B16_AUDIT_ONLY_SUBJECTS)


def test_b16_review_artifact_is_explicit_human_review():
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b16_v1.json")
    assert review["status"] == "HUMAN_REVIEW_COMPLETED"
    assert review["reviewer_role"] == "HUMAN_REVIEWER"
    assert review["reviewed_at"] == REVIEWED_AT
    assert len(review["decisions"]) == 14
    assert all(item["decision"] == "APPROVE" for item in review["decisions"])
    assert all(item["source_verified"] is True for item in review["decisions"])
    assert all(item["relationship_class_confirmed"] is True for item in review["decisions"])
    assert all(value is False for value in review["guards"].values())


def test_b16_resolution_ledger_registers_ten_audit_only_and_four_supersessions():
    registered = load_registered_redundant_mapping_ids(root=ROOT)
    assert len(registered["8F_MAPPING_CANDIDATES_2026-09-27_B16"]) == 10
    assert {mapping_id.split(":")[1] for mapping_id in registered["8F_MAPPING_CANDIDATES_2026-09-27_B16"]} == B16_AUDIT_ONLY_SUBJECTS

    resolutions = _load("configs/external_evidence_8f_overlay_resolutions_v2.json")["resolutions"]
    rows = [
        row for row in resolutions
        if row["batch_id"] == "8F_MAPPING_CANDIDATES_2026-09-27_B16"
        and row["action"] == "SUPERSEDE_WITH_HUMAN_REVIEWED_MAPPING"
    ]
    assert {row["subject_id"] for row in rows} == B16_RECLASSIFIED_SUBJECTS
    assert {row["new_mapping_id"] for row in rows} == {f"MAP:{subject}:fx:2" for subject in B16_RECLASSIFIED_SUBJECTS}
