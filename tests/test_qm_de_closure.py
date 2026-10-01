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


def _validate(payload):
    return validate_closure_manifest(payload, ba_qm3_closure=validate_ba_qm3_closure_file(), qm_de_contract=load_qm_de_contract())


def test_ba_qm4_closure_validates_against_actual_ba_qm3_and_contract():
    result = validate_closure_file()
    assert result["display_status_qm_d"] == EXPECTED_QM_D_STATUS
    assert result["display_status_qm_e"] == EXPECTED_QM_E_STATUS
    assert result["business_area_status"] == EXPECTED_BA_STATUS
    assert result["engineering_status"] == "COMPLETE"
    assert result["empirical_promotion_claimed"] is False
    assert result["next_mandatory_work_package"] == "BA-QM5 / QM-F — Decision Ablation"


def test_closure_preserves_qm_b_external_blockers_and_no_reopen():
    qm_b = validate_closure_file()["qm_b_constraints"]
    assert qm_b["strict_historical_promotion_status"] == "BLOCKED_EXTERNAL_EVIDENCE"
    assert set(qm_b["external_blockers"]) == {"LISTING_EVIDENCE", "MARKET_TRADABILITY_EVIDENCE", "EXECUTION_CHANNEL_EVIDENCE"}
    assert qm_b["blockers_overridden"] is False
    assert qm_b["qm_b_engineering_reopened"] is False


def test_closure_rejects_relabelling_phase2_aggregate_report_as_row_level_calibration():
    bad = copy.deepcopy(validate_closure_file())
    bad["integration_contracts"]["phase2_probability"]["aggregate_report_is_row_level_calibration_data"] = True
    with pytest.raises(ValueError, match="ba_qm4_phase2_bridge_invalid"):
        _validate(bad)


def test_closure_rejects_single_universal_effective_n_claim():
    bad = copy.deepcopy(validate_closure_file())
    bad["dependence_scope"]["single_universal_effective_n_selected"] = True
    with pytest.raises(ValueError, match="ba_qm4_dependence_boundary_violation"):
        _validate(bad)


def test_closure_rejects_productive_boundary_change():
    bad = copy.deepcopy(validate_closure_file())
    bad["boundaries"]["productive_calibration_changed"] = True
    with pytest.raises(ValueError, match="ba_qm4_boundary_values_must_be_false"):
        _validate(bad)


def test_closure_rejects_missing_or_non_boolean_boundary_keys():
    bad = copy.deepcopy(validate_closure_file())
    bad["boundaries"].pop("orders_generated")
    with pytest.raises(ValueError, match="ba_qm4_boundary_keys_mismatch"):
        _validate(bad)

    bad = copy.deepcopy(validate_closure_file())
    bad["boundaries"]["orders_generated"] = None
    with pytest.raises(ValueError, match="ba_qm4_boundary_values_must_be_false"):
        _validate(bad)


def test_closure_rejects_disabled_qm_c_hash_reuse_guards():
    for field in ("analysis_plan_hash_reused", "result_hash_reused"):
        bad = copy.deepcopy(validate_closure_file())
        bad["integration_contracts"]["qm_c"][field] = False
        with pytest.raises(ValueError, match="ba_qm4_qm_c_integration_invalid"):
            _validate(bad)


def test_closure_rejects_disabled_qm_i_lineage_node_guard():
    bad = copy.deepcopy(validate_closure_file())
    bad["integration_contracts"]["qm_i"]["observation_and_prediction_lineage_nodes_must_exist"] = False
    with pytest.raises(ValueError, match="ba_qm4_qm_i_integration_invalid"):
        _validate(bad)
