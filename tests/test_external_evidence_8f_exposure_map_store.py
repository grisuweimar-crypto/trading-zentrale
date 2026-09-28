from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.exposure_map_store_8f import (
    ExposureMapStore8FError,
    build_effective_exposure_map_audit,
    compose_effective_exposure_map,
    load_effective_exposure_map,
    load_registered_redundant_mapping_ids,
    validate_overlay_corrections,
    validate_overlay_registry,
)
from scanner.research.external_evidence.exposure_mapping_review_8f import ExposureMappingReview8FError


ROOT = Path(__file__).resolve().parents[1]


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def test_committed_overlay_registry_materializes_repaired_b5_through_b19_state():
    base = _load("configs/external_evidence_8f_exposure_map_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    audit = build_effective_exposure_map_audit(root=ROOT)

    mappings = effective["mappings"]
    active = [row for row in mappings if row["review_status"] == "ACTIVE"]
    superseded = [row for row in mappings if row["review_status"] == "SUPERSEDED"]

    assert len(base["mappings"]) == 60
    assert len(mappings) == 201
    assert len(active) == 195
    assert len(superseded) == 6

    assert audit["status"] == "PASS_EFFECTIVE_EXPOSURE_MAP_COMPOSITION"
    assert audit["base_mapping_count"] == 60
    assert audit["overlay_count"] == 15
    assert audit["review_resolution_count"] == 24
    assert audit["redundant_review_correction_count"] == 18
    assert audit["supersession_count"] == 6
    assert audit["effective_mapping_interval_count"] == 201
    assert audit["effective_mapping_count"] == 195
    assert audit["active_subject_count"] == 195
    assert audit["superseded_mapping_count"] == 6
    assert audit["market_outcomes_read"] is False
    assert audit["automatic_promotion_used"] is False
    assert audit["final_freeze_materialization_required"] is True


def test_effective_map_has_unique_active_subject_factor_pairs_after_supersession():
    effective = load_effective_exposure_map(root=ROOT)
    pairs = [
        (row["subject_id"], row["factor_id"])
        for row in effective["mappings"]
        if row["review_status"] == "ACTIVE"
    ]
    assert len(pairs) == len(set(pairs)) == 195


def test_six_conflicting_rereviews_are_versioned_not_retroactively_overwritten():
    effective = load_effective_exposure_map(root=ROOT)
    by_id = {row["mapping_id"]: row for row in effective["mappings"]}

    expected = {
        "MAP:SYK:fx:1": ("MAP:SYK:fx:2", "CURRENCY_TRANSLATION", "OTHER_DOCUMENTED"),
        "MAP:GOOGL:fx:1": ("MAP:GOOGL:fx:2", "CURRENCY_TRANSLATION", "OTHER_DOCUMENTED"),
        "MAP:META:fx:1": ("MAP:META:fx:2", "CURRENCY_TRANSLATION", "OTHER_DOCUMENTED"),
        "MAP:MSFT:fx:1": ("MAP:MSFT:fx:2", "CURRENCY_TRANSLATION", "OTHER_DOCUMENTED"),
        "MAP:CGNX:fx:1": ("MAP:CGNX:fx:2", "OTHER_DOCUMENTED", "CURRENCY_TRANSLATION"),
        "MAP:IFX.DE:fx:1": ("MAP:IFX.DE:fx:2", "CURRENCY_TRANSLATION", "OTHER_DOCUMENTED"),
    }

    for old_id, (new_id, old_class, new_class) in expected.items():
        old = by_id[old_id]
        new = by_id[new_id]
        assert old["review_status"] == "SUPERSEDED"
        assert old["relationship_class"] == old_class
        assert old["valid_to"] == new["valid_from"] == new["reviewed_at"]
        assert new["review_status"] == "ACTIVE"
        assert new["relationship_class"] == new_class
        assert new["valid_to"] is None


def test_overlay_registry_guards_fail_closed():
    registry = _load("configs/external_evidence_8f_exposure_overlays_v1.json")
    registry = copy.deepcopy(registry)
    registry["rules"]["automatic_review_or_promotion_allowed"] = True
    with pytest.raises(ExposureMapStore8FError, match="invalid exposure-overlay rules"):
        validate_overlay_registry(registry)


def test_overlay_resolution_guards_fail_closed():
    resolutions = _load("configs/external_evidence_8f_overlay_resolutions_v2.json")
    resolutions = copy.deepcopy(resolutions)
    resolutions["guards"]["automatic_conflict_suppression_allowed"] = True
    with pytest.raises(ExposureMapStore8FError, match="invalid exposure-overlay correction guards"):
        validate_overlay_corrections(resolutions)


def test_registered_redundant_reviews_are_explicit_and_audit_only():
    registered = load_registered_redundant_mapping_ids(root=ROOT)
    assert registered["8F_MAPPING_CANDIDATES_2026-09-27_B8"] == {
        "MAP:ABX.TO:gold:1",
        "MAP:BTO.TO:gold:1",
        "MAP:EDR.TO:silver:1",
        "MAP:EQX.TO:gold:1",
        "MAP:NEM.AX:gold:1",
        "MAP:PAAS.TO:silver:1",
    }
    assert registered["8F_MAPPING_CANDIDATES_2026-09-27_B10"] == {"MAP:SHOP:fx:1"}
    assert registered["8F_MAPPING_CANDIDATES_2026-09-27_B16"] == {
        "MAP:ABT:fx:1",
        "MAP:ISRG:fx:1",
        "MAP:ROK:fx:1",
        "MAP:TER:fx:1",
        "MAP:AMAT:fx:1",
        "MAP:AMZN:fx:1",
        "MAP:ASML:fx:1",
        "MAP:MU:fx:1",
        "MAP:ORCL:fx:1",
        "MAP:PLTR:fx:1",
    }
    assert registered["8F_MAPPING_CANDIDATES_2026-09-27_B17"] == {"MAP:SAP.DE:fx:1"}
    assert sum(len(values) for values in registered.values()) == 18


def test_b8_redundant_reviews_require_explicit_registered_resolutions():
    base = _load("configs/external_evidence_8f_exposure_map_v1.json")
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b8_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b8_v1.json")

    with pytest.raises(ExposureMappingReview8FError, match="mapping id already exists with different content"):
        compose_effective_exposure_map(base_map=base, overlay_pairs=[(candidates, review)])

    registered = load_registered_redundant_mapping_ids(root=ROOT)
    effective_after_b8 = compose_effective_exposure_map(
        base_map=base,
        overlay_pairs=[(candidates, review)],
        redundant_mapping_ids_by_batch={
            "8F_MAPPING_CANDIDATES_2026-09-27_B8": registered["8F_MAPPING_CANDIDATES_2026-09-27_B8"]
        },
    )
    assert len(effective_after_b8["mappings"]) == 69


def test_active_overlay_must_be_human_review_completed():
    base = _load("configs/external_evidence_8f_exposure_map_v1.json")
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b5_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b5_v1.json")
    review = copy.deepcopy(review)
    review["status"] = "AWAITING_HUMAN_REVIEW"
    with pytest.raises(ExposureMapStore8FError, match="HUMAN_REVIEW_COMPLETED"):
        compose_effective_exposure_map(base_map=base, overlay_pairs=[(candidates, review)])
