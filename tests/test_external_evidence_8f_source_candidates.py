from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.source_candidates_8f import (
    SourceCandidates8FError,
    validate_source_candidates,
)


ROOT = Path(__file__).resolve().parents[1]


def _load():
    config = json.loads(
        (ROOT / "configs/external_evidence_8f_source_candidates_v1.json").read_text(encoding="utf-8")
    )
    macro = json.loads(
        (ROOT / "configs/external_evidence_8f_macro_exposure_v1.json").read_text(encoding="utf-8")
    )
    factor_ids = {item["factor_id"] for item in macro["factor_catalog"]}
    return config, factor_ids


def test_source_routing_contract_passes_and_fred_remains_blocked():
    config, factor_ids = _load()
    result = validate_source_candidates(config, allowed_factor_ids=factor_ids)
    assert result["status"] == "PASS_SOURCE_ROUTING_CONTRACT"
    assert result["factor_count"] == 11
    assert result["fred_alfred_enabled"] is False
    assert "lithium" in result["explicit_gap_factors"]
    assert "uranium" in result["explicit_gap_factors"]


def test_blocked_fred_cannot_be_routed():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    broken["factor_routing"]["inflation"] = ["fred_alfred_realtime"]
    with pytest.raises(SourceCandidates8FError, match="blocked source"):
        validate_source_candidates(broken, allowed_factor_ids=factor_ids)


def test_unknown_routed_source_fails_closed():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    broken["factor_routing"]["fx"] = ["invented_macro_vendor"]
    with pytest.raises(SourceCandidates8FError, match="unknown source"):
        validate_source_candidates(broken, allowed_factor_ids=factor_ids)


def test_empty_factor_route_requires_explicit_gap():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    broken["explicit_source_gaps"].pop("lithium")
    with pytest.raises(SourceCandidates8FError, match="no candidate source and no explicit source gap"):
        validate_source_candidates(broken, allowed_factor_ids=factor_ids)


def test_source_factor_declaration_must_match_routing():
    config, factor_ids = _load()
    broken = copy.deepcopy(config)
    source = next(item for item in broken["sources"] if item["source_id"] == "ecb_data_portal")
    source["factors"] = ["rates_policy", "yield_curve"]
    with pytest.raises(SourceCandidates8FError, match="does not declare routed factor fx"):
        validate_source_candidates(broken, allowed_factor_ids=factor_ids)
