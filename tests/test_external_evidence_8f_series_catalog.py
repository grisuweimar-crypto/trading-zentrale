from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.series_catalog_8f import (
    SeriesCatalog8FError,
    validate_series_catalog,
)


ROOT = Path(__file__).resolve().parents[1]


def _load():
    macro = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )
    sources = json.loads(
        (ROOT / "configs/external_evidence_8f_source_candidates_v1.json").read_text(encoding="utf-8")
    )
    return macro, sources


def test_series_catalog_matches_implemented_adapters_and_source_routing():
    macro, sources = _load()
    result = validate_series_catalog(macro, sources)
    assert result["status"] == "PASS_SERIES_CATALOG_CONTRACT"
    assert result["series_count"] == 9
    assert result["implemented_adapter_sources"] == [
        "bls_cpi_archived_releases",
        "ecb_data_portal",
        "eia_open_data_energy",
        "federal_reserve_board_h15",
    ]


def test_series_catalog_cannot_use_blocked_fred_source():
    macro, sources = _load()
    broken = copy.deepcopy(macro)
    broken["series_catalog"][0]["source_id"] = "fred_alfred_realtime"
    with pytest.raises(SeriesCatalog8FError, match="blocked source"):
        validate_series_catalog(broken, sources)


def test_series_factor_must_be_declared_by_source():
    macro, sources = _load()
    broken = copy.deepcopy(macro)
    broken["series_catalog"][0]["factor_id"] = "oil"
    with pytest.raises(SeriesCatalog8FError, match="not declared by source"):
        validate_series_catalog(broken, sources)


def test_adapter_source_declaration_must_match_implemented_catalog():
    macro, sources = _load()
    broken = copy.deepcopy(macro)
    broken["source_contract"]["adapter_source_ids"] = ["bls_cpi_archived_releases"]
    with pytest.raises(SeriesCatalog8FError, match="adapter source mismatch"):
        validate_series_catalog(broken, sources)
