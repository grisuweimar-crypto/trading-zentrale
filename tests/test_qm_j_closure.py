from __future__ import annotations

import copy
import json

import pytest

from scanner.research.governance.qm_g_closure import validate_closure_file as validate_ba_qm6_closure_file
from scanner.research.governance.qm_h_capa import CapaLedger
from scanner.research.governance.qm_j_closure import (
    QM_DIR,
    validate_closure_file,
    validate_closure_manifest,
)
from scanner.research.governance.qm_j_negative_controls import load_qm_j_contract
from scanner.research.governance.qm_j_selection_freshness_gate import load_gate


def _read(name: str) -> dict:
    return json.loads((QM_DIR / name).read_text(encoding="utf-8"))


def _dependencies() -> dict:
    ledger = CapaLedger(QM_DIR / "qm_h_capa_ledger.jsonl")
    assert ledger.verify_integrity()["valid"] is True
    return {
        "ba_qm6_closure": validate_ba_qm6_closure_file(),
        "qm_j_contract": load_qm_j_contract(),
        "feature_result": _read("qm_j_phase1a_selection_falsification.json"),
        "data_research_result": _read("qm_j_phase1a_data_research_falsification.json"),
        "decision_e2e_result": _read("qm_j_decision_e2e_falsification.json"),
        "investigation_result": _read("qm_j_phase1a_lag1_investigation.json"),
        "freshness_gate": load_gate(),
        "capa_finding": ledger.get_finding("QM-H-QMJ-PHASE1A-LAG1-001"),
    }


def test_ba_qm7_closure_is_complete_without_claiming_validation_or_closing_capa() -> None:
    result = validate_closure_file()
    assert result["engineering_status"] == "COMPLETE"
    assert result["negative_control_coverage_status"] == "COMPLETE"
    assert result["empirical_validation_status"] == "NOT_ESTABLISHED"
    assert result["all_negative_controls_passed"] is False
    assert set(result["control_levels"]) == {"DATA", "FEATURE", "RESEARCH", "DECISION_LAYER", "END_TO_END"}
    assert result["open_capa"]["status"] == "IMPLEMENTED"
    assert result["open_capa"]["evidence_impact"] == "PROMOTION_BLOCKED"
    assert result["open_capa"]["effectiveness_verification"] == "PENDING_PROSPECTIVE_UNSPENT_EVIDENCE"
    assert result["open_capa"]["closure_does_not_close_capa"] is True
    assert result["next_mandatory_work_package"] == "BA-QM8 – End-to-End Scanner Audit"


def test_closure_rejects_rewriting_trigger_as_all_controls_passed() -> None:
    result = validate_closure_file()
    changed = copy.deepcopy(result)
    changed["all_negative_controls_passed"] = True
    with pytest.raises(ValueError, match="ba_qm7_must_preserve_triggered_control"):
        validate_closure_manifest(changed, **_dependencies())


def test_closure_rejects_dropping_triggered_control() -> None:
    result = validate_closure_file()
    changed = copy.deepcopy(result)
    changed["triggered_negative_controls"] = []
    with pytest.raises(ValueError, match="ba_qm7_triggered_control_set_invalid"):
        validate_closure_manifest(changed, **_dependencies())


def test_closure_rejects_missing_negative_control_level() -> None:
    result = validate_closure_file()
    changed = copy.deepcopy(result)
    del changed["control_levels"]["END_TO_END"]
    with pytest.raises(ValueError, match="ba_qm7_control_level_coverage_invalid"):
        validate_closure_manifest(changed, **_dependencies())


def test_closure_rejects_automatic_capa_release() -> None:
    result = validate_closure_file()
    changed = copy.deepcopy(result)
    changed["open_capa"]["automatic_release_allowed"] = True
    with pytest.raises(ValueError, match="ba_qm7_open_capa_release_guard_invalid"):
        validate_closure_manifest(changed, **_dependencies())


def test_closure_rejects_empirical_validation_or_promotion_claim() -> None:
    result = validate_closure_file()
    changed = copy.deepcopy(result)
    changed["empirical_validation_status"] = "ESTABLISHED"
    with pytest.raises(ValueError, match="ba_qm7_must_not_claim_empirical_validation"):
        validate_closure_manifest(changed, **_dependencies())

    changed = copy.deepcopy(result)
    changed["boundaries"]["empirical_promotion_performed"] = True
    with pytest.raises(ValueError, match="ba_qm7_boundary_guards_invalid"):
        validate_closure_manifest(changed, **_dependencies())
