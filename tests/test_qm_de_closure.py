import copy

import pytest

from scanner.research.governance.qm_d_dependence import load_qm_de_contract
from scanner.research.governance.qm_de_closure import (
    EXPECTED_BA_STATUS,
    EXPECTED_QM_D_STATUS,
    EXPECTED_QM_E_STATUS,
    validate_closure_file,
    validate_closure_manifest,
)
from scanner.research.governance.qm_i_closure import validate_closure_file as validate_ba_qm3_closure_file


def test_ba_qm4_closure_validates_against_actual_ba_qm3_and_contract():
    result = validate_closure_file()
    assert result["display_status_qm_d"] == EXPECTED_QM_D_STATUS
    assert result["display_status_qm_e"] == EXPECTED_QM_E_STATUS
    assert result["business_area_status"] == EXPECTED_BA_STATUS
    assert result["engineering_status"] == "COMPLETE"
    assert result["empirical_promotion_claimed"] is False
    assert result["next_mandatory_work_package"] == "BA-QM5 / QM-F — Decision Ablation"


def test_closure_preserves_qm_b_external_blockers_and_no_reopen():
    result = validate_closure_file()
    qm_b = result["qm_b_constraints"]
    assert qm_b["strict_historical_promotion_status"] == "BLOCKED_EXTERNAL_EVIDENCE"
    assert set(qm_b["external_blockers"]) == {
        "LISTING_EVIDENCE",
        "MARKET_TRADABILITY_EVIDENCE",
        "EXECUTION_CHANNEL_EVIDENCE",
    }
    assert qm_b["blockers_overridden"] is False
    assert qm_b["qm_b_engineering_reopened"] is False


def test_closure_rejects_relabelling_phase2_aggregate_report_as_row_level_calibration():
    good = validate_closure_file()
    bad = copy.deepcopy(good)
    bad["integration_contracts"]["phase2_probability"]["aggregate_report_is_row_level_calibration_data"] = True
    with pytest.raises(ValueError, match="ba_qm4_phase2_aggregate_relabel_forbidden"):
        validate_closure_manifest(
            bad,
            ba_qm3_closure=validate_ba_qm3_closure_file(),
            qm_de_contract=load_qm_de_contract(),
        )


def test_closure_rejects_single_universal_effective_n_claim():
    good = validate_closure_file()
    bad = copy.deepcopy(good)
    bad["dependence_scope"]["single_universal_effective_n_selected"] = True
    with pytest.raises(ValueError, match="ba_qm4_universal_effective_n_forbidden"):
        validate_closure_manifest(
            bad,
            ba_qm3_closure=validate_ba_qm3_closure_file(),
            qm_de_contract=load_qm_de_contract(),
        )


def test_closure_rejects_productive_boundary_change():
    good = validate_closure_file()
    bad = copy.deepcopy(good)
    bad["boundaries"]["productive_calibration_changed"] = True
    with pytest.raises(ValueError, match="ba_qm4_unsafe_boundary_enabled:productive_calibration_changed"):
        validate_closure_manifest(
            bad,
            ba_qm3_closure=validate_ba_qm3_closure_file(),
            qm_de_contract=load_qm_de_contract(),
        )
