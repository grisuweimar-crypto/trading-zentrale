from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scanner.research.external_evidence.exposure_domain_8f import (
    ExposureDomain8FError,
    build_exposure_domain_audit,
)


ROOT = Path(__file__).resolve().parents[1]


def _mapping(subject_id: str = "AAA") -> dict:
    return {
        "mapping_id": f"MAP:{subject_id}:oil:1",
        "map_version": "8F_EXPOSURE_MAP_2026-09-26_V1",
        "subject_id": subject_id,
        "factor_id": "oil",
        "relationship_class": "INPUT_COST_LINK",
        "evidence_type": "ISSUER_FILING",
        "evidence_reference": f"issuer-filing-{subject_id}",
        "evidence_sha256": "a" * 64,
        "evidence_valid_from": "2026-09-26T10:00:00+00:00",
        "human_reviewed": True,
        "reviewed_at": "2026-09-26T11:00:00+00:00",
        "review_status": "ACTIVE",
        "valid_from": "2026-09-26T11:00:00+00:00",
        "valid_to": None,
    }


def _domain() -> dict:
    return {
        "schema_version": "external_evidence_8f_research_domain_v1",
        "domain_version": "TEST_DOMAIN_V1",
        "status": "FROZEN_TEST_DOMAIN",
        "subject_ids": ["AAA", "BBB"],
        "explicit_unmapped": [
            {"subject_id": "BBB", "reason": "No documentary macro exposure accepted"}
        ],
        "rules": {
            "domain_must_be_defined_before_8f_freeze": True,
            "subject_may_not_enter_domain_because_mapping_exists": True,
            "mapped_and_unmapped_subjects_both_remain_in_denominator": True,
            "explicit_unmapped_requires_reason": True,
            "market_outcomes_may_be_used_to_define_domain": False,
            "current_mapping_coverage_may_be_used_to_select_domain": False,
        },
    }


def _empty_exposure() -> dict:
    return json.loads(
        (ROOT / "configs/external_evidence_8f_exposure_map_v1.json").read_text(encoding="utf-8")
    )


def test_domain_audit_keeps_mapped_and_unmapped_in_denominator():
    exposure = copy.deepcopy(_empty_exposure())
    exposure["mappings"] = [_mapping()]
    result = build_exposure_domain_audit(domain_config=_domain(), exposure_map=exposure)
    assert result["domain_subject_count"] == 2
    assert result["mapped_subject_count"] == 1
    assert result["explicit_unmapped_subject_count"] == 1
    assert result["unaccounted_subject_count"] == 0
    assert result["guards"]["unmapped_subjects_silently_dropped"] is False


def test_unaccounted_subject_remains_visible_not_dropped():
    exposure = copy.deepcopy(_empty_exposure())
    exposure["mappings"] = [_mapping()]
    domain = _domain()
    domain["explicit_unmapped"] = []
    result = build_exposure_domain_audit(domain_config=domain, exposure_map=exposure)
    assert result["unaccounted_subject_ids"] == ["BBB"]
    assert result["unaccounted_subject_count"] == 1


def test_subject_cannot_be_both_mapped_and_explicit_unmapped():
    exposure = copy.deepcopy(_empty_exposure())
    exposure["mappings"] = [_mapping("AAA")]
    domain = _domain()
    domain["explicit_unmapped"].append(
        {"subject_id": "AAA", "reason": "contradictory test"}
    )
    with pytest.raises(ExposureDomain8FError, match="both mapped and explicit_unmapped"):
        build_exposure_domain_audit(domain_config=domain, exposure_map=exposure)


def test_domain_rule_cannot_select_subjects_from_mapping_coverage():
    domain = _domain()
    domain["rules"]["current_mapping_coverage_may_be_used_to_select_domain"] = True
    with pytest.raises(ExposureDomain8FError, match="invalid research-domain rules"):
        build_exposure_domain_audit(domain_config=domain, exposure_map=_empty_exposure())


def test_committed_domain_is_pinned_to_pre8f_universe_blob_and_nonempty():
    domain = json.loads(
        (ROOT / "configs/external_evidence_8f_research_domain_v1.json").read_text(encoding="utf-8")
    )
    universe_text = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8")
    result = build_exposure_domain_audit(
        domain_config=domain,
        exposure_map=_empty_exposure(),
        universe_csv_text=universe_text,
    )
    assert result["domain_status"] == "FROZEN_OUTCOME_BLIND_PRE8F_ACTIVE_STOCK_UNIVERSE"
    assert result["domain_subject_count"] > 100
    assert result["unaccounted_subject_count"] == result["domain_subject_count"]
    assert result["source_snapshot"]["source_ref"] == "feef4e572283613739b2f24cf42b837ff68a1508"
    assert result["source_snapshot"]["git_blob_sha"] == "2c6efec912ea7478096f77dc8f88d7eed2a0581b"
    assert result["guards"]["domain_pinned_to_pre8f_source_blob"] is True


def test_pinned_domain_fails_if_universe_blob_changes():
    domain = json.loads(
        (ROOT / "configs/external_evidence_8f_research_domain_v1.json").read_text(encoding="utf-8")
    )
    universe_text = (ROOT / "data/inputs/universe_master.csv").read_text(encoding="utf-8") + "\n"
    with pytest.raises(ExposureDomain8FError, match="source blob mismatch"):
        build_exposure_domain_audit(
            domain_config=domain,
            exposure_map=_empty_exposure(),
            universe_csv_text=universe_text,
        )
