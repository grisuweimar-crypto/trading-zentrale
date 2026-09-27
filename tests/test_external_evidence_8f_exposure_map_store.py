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
    validate_overlay_registry,
)


ROOT = Path(__file__).resolve().parents[1]


def _load(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def test_committed_overlay_registry_materializes_b5_through_b11_to_one_hundred_sixty_effective_mappings():
    base = _load("configs/external_evidence_8f_exposure_map_v1.json")
    effective = load_effective_exposure_map(root=ROOT)
    audit = build_effective_exposure_map_audit(root=ROOT)

    assert len(base["mappings"]) == 60
    assert len(effective["mappings"]) == 160
    assert audit["status"] == "PASS_EFFECTIVE_EXPOSURE_MAP_COMPOSITION"
    assert audit["base_mapping_count"] == 60
    assert audit["overlay_count"] == 7
    assert audit["effective_mapping_count"] == 160
    assert audit["market_outcomes_read"] is False
    assert audit["automatic_promotion_used"] is False
    assert audit["final_freeze_materialization_required"] is True

    active_subjects = {row["subject_id"] for row in effective["mappings"]}
    for subject in {
        "KLAC", "LRCX", "SNOW", "FSLR", "GMED", "SNN", "TSLA", "NFLX", "SPOT", "ADBE",
        "FISV", "PFE", "PEP", "PG", "NKE", "GE", "KO", "JNJ", "UBER", "STLA",
        "SN.L", "BAYN.DE", "FME.DE", "NXP", "PM", "RACE", "ETN", "ADS.DE", "VOW3.DE",
        "BAS.DE", "ABBN.SW", "UUUU", "ENR.DE", "RI.PA",
        "3750.HK", "XSDG.F", "SU.PA", "SOLB.BR", "MC.PA", "UAA", "CSTM", "AUTO.OL",
        "6503.T", "ASX", "TSMN.MX", "NDA.DE", "RIGD.IL", "2899.HK",
        "ABX.TO", "BTO.TO", "EDR.TO", "EQX.TO", "NEM.AX", "PAAS.TO", "DSV.TO", "VALE",
        "LUMN", "KLAR", "PGY", "FIGR", "BLK", "CPB", "INGR",
        "AAPL", "AMKR", "ON", "OGN", "WM", "CNC", "UNH", "SLVR.V", "PPTA", "MGMA.V", "DV.V", "AAGFF",
        "APLD", "MBLY", "ZETA", "TTAN", "QS", "CRWV", "MRNA", "SGL.DE", "VZLA.TO", "NBIS", "SHOP", "INOD", "AVAV", "FLNC",
        "APP", "IREN", "MELI", "ENSG", "HIMS", "ROL", "NRDS", "XYZ", "NIO", "XPEV", "TTK.DE",
    }:
        assert subject in active_subjects


def test_effective_map_has_unique_active_subject_factor_pairs():
    effective = load_effective_exposure_map(root=ROOT)
    pairs = [(row["subject_id"], row["factor_id"]) for row in effective["mappings"] if row["review_status"] == "ACTIVE"]
    assert len(pairs) == len(set(pairs)) == 160


def test_overlay_registry_guards_fail_closed():
    registry = _load("configs/external_evidence_8f_exposure_overlays_v1.json")
    registry = copy.deepcopy(registry)
    registry["rules"]["automatic_review_or_promotion_allowed"] = True
    with pytest.raises(ExposureMapStore8FError, match="invalid exposure-overlay rules"):
        validate_overlay_registry(registry)


def test_active_overlay_must_be_human_review_completed():
    base = _load("configs/external_evidence_8f_exposure_map_v1.json")
    candidates = _load("configs/external_evidence_8f_mapping_candidates_b5_v1.json")
    review = _load("configs/external_evidence_8f_mapping_review_decisions_b5_v1.json")
    review = copy.deepcopy(review)
    review["status"] = "AWAITING_HUMAN_REVIEW"
    with pytest.raises(ExposureMapStore8FError, match="HUMAN_REVIEW_COMPLETED"):
        compose_effective_exposure_map(base_map=base, overlay_pairs=[(candidates, review)])
