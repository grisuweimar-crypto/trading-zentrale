from __future__ import annotations

import copy
import json
from pathlib import Path

from scanner.research.external_evidence.completion_8f import evaluate_phase8f_completion


ROOT = Path(__file__).resolve().parents[1]


def _load_json(name: str) -> dict:
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def _mapping() -> dict:
    return {
        "mapping_id": "MAP:TEST:oil:1",
        "map_version": "8F_EXPOSURE_MAP_2026-09-26_V1",
        "subject_id": "TEST",
        "factor_id": "oil",
        "relationship_class": "INPUT_COST_LINK",
        "evidence_type": "ISSUER_FILING",
        "evidence_reference": "issuer-filing-test",
        "evidence_sha256": "b" * 64,
        "evidence_valid_from": "2026-09-26T10:00:00+00:00",
        "human_reviewed": True,
        "reviewed_at": "2026-09-26T11:00:00+00:00",
        "review_status": "ACTIVE",
        "valid_from": "2026-09-26T11:00:00+00:00",
        "valid_to": None,
    }


def _ledger() -> dict:
    return {
        "schema_version": "external_evidence_8f_macro_ledger_v1",
        "status": "OUTCOME_BLIND_APPEND_ONLY_MACRO_LEDGER",
        "knowable_row_count": 2,
        "coverage": {"series_count": 1},
        "guards": {
            "market_outcomes_read": False,
            "later_revision_overwrites_original": False,
        },
    }


def _domain_audit() -> dict:
    return {
        "schema_version": "external_evidence_8f_exposure_domain_audit_v1",
        "domain_subject_count": 2,
        "mapped_subject_count": 1,
        "explicit_unmapped_subject_count": 1,
        "market_outcomes_read": False,
        "guards": {
            "domain_pinned_to_pre8f_source_blob": True,
            "market_outcomes_read": False,
        },
    }


def test_current_phase8f_state_is_blocked_not_falsely_frozen():
    config = _load_json("configs/external_evidence_8f_completion_v1.json")
    macro = _load_json("configs/external_evidence_8f_macro_exposure_v1.json")
    exposure = _load_json("configs/external_evidence_8f_exposure_map_v1.json")

    result = evaluate_phase8f_completion(
        completion_config=config,
        macro_config=macro,
        exposure_map=exposure,
        macro_ledger=None,
        exposure_domain_audit=None,
    )
    assert result["status"] == "BLOCKED_8F_COMPLETION"
    assert result["freeze_allowed"] is False
    assert "real_macro_ledger:missing" in result["blockers"]
    assert "exposure_domain_audit:missing" in result["blockers"]
    assert not any(value.startswith("reviewed_exposure_mappings:") for value in result["blockers"])
    assert result["metrics"]["active_reviewed_mapping_count"] == 10


def test_completion_passes_only_when_real_requirements_are_represented():
    config = _load_json("configs/external_evidence_8f_completion_v1.json")
    macro = _load_json("configs/external_evidence_8f_macro_exposure_v1.json")
    exposure = _load_json("configs/external_evidence_8f_exposure_map_v1.json")
    exposure = copy.deepcopy(exposure)
    exposure["mappings"] = [_mapping()]

    result = evaluate_phase8f_completion(
        completion_config=config,
        macro_config=macro,
        exposure_map=exposure,
        macro_ledger=_ledger(),
        exposure_domain_audit=_domain_audit(),
    )
    assert result["status"] == "PASS_8F_COMPLETION"
    assert result["freeze_allowed"] is True
    assert result["blockers"] == []
    assert result["metrics"]["domain_pinned_to_pre8f_source_blob"] is True
    assert result["guards"]["market_outcomes_read"] is False


def test_domain_accounting_must_be_complete():
    config = _load_json("configs/external_evidence_8f_completion_v1.json")
    macro = _load_json("configs/external_evidence_8f_macro_exposure_v1.json")
    exposure = _load_json("configs/external_evidence_8f_exposure_map_v1.json")
    exposure = copy.deepcopy(exposure)
    exposure["mappings"] = [_mapping()]
    audit = _domain_audit()
    audit["explicit_unmapped_subject_count"] = 0

    result = evaluate_phase8f_completion(
        completion_config=config,
        macro_config=macro,
        exposure_map=exposure,
        macro_ledger=_ledger(),
        exposure_domain_audit=audit,
    )
    assert result["status"] == "BLOCKED_8F_COMPLETION"
    assert "domain_accounting:1!=2" in result["blockers"]


def test_domain_must_be_pinned_to_pre8f_source_blob():
    config = _load_json("configs/external_evidence_8f_completion_v1.json")
    macro = _load_json("configs/external_evidence_8f_macro_exposure_v1.json")
    exposure = copy.deepcopy(_load_json("configs/external_evidence_8f_exposure_map_v1.json"))
    exposure["mappings"] = [_mapping()]
    audit = _domain_audit()
    audit["guards"]["domain_pinned_to_pre8f_source_blob"] = False

    result = evaluate_phase8f_completion(
        completion_config=config,
        macro_config=macro,
        exposure_map=exposure,
        macro_ledger=_ledger(),
        exposure_domain_audit=audit,
    )
    assert result["status"] == "BLOCKED_8F_COMPLETION"
    assert "research_domain:not_pinned_to_pre8f_source_blob" in result["blockers"]
