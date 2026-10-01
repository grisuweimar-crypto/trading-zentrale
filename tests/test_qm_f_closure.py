import copy

import pytest

from scanner.research.governance.qm_f_closure import (
    EXPECTED_BA_STATUS,
    EXPECTED_QM_F_STATUS,
    validate_closure_file,
    validate_closure_manifest,
)
from scanner.research.governance.qm_f_decision_ablation import load_qm_f_contract
from scanner.research.governance.qm_de_closure import validate_closure_file as validate_ba_qm4_closure_file


def _validate(payload):
    return validate_closure_manifest(
        payload,
        ba_qm4_closure=validate_ba_qm4_closure_file(),
        qm_f_contract=load_qm_f_contract(),
    )


def test_ba_qm5_closure_validates_against_current_ba_qm4():
    result = validate_closure_file()
    assert result["display_status_qm_f"] == EXPECTED_QM_F_STATUS
    assert result["business_area_status"] == EXPECTED_BA_STATUS
    assert result["engineering_status"] == "COMPLETE"
    assert result["next_mandatory_work_package"] == "QM-G — Elliott Challenger Registry"


def test_closure_rejects_productive_or_phase7i_relabelling_changes():
    bad = copy.deepcopy(validate_closure_file())
    bad["boundaries"]["phase7i_validation_relabelled"] = True
    with pytest.raises(ValueError, match="ba_qm5_boundary_values_must_be_false"):
        _validate(bad)

    bad = copy.deepcopy(validate_closure_file())
    bad["boundaries"]["portfolio_action_changed"] = True
    with pytest.raises(ValueError, match="ba_qm5_boundary_values_must_be_false"):
        _validate(bad)


def test_closure_rejects_missing_lineage_or_predecessor_guards():
    bad = copy.deepcopy(validate_closure_file())
    bad["integration_contracts"]["qm_i_lineage_required"] = False
    with pytest.raises(ValueError, match="ba_qm5_integration_guard_invalid"):
        _validate(bad)

    bad = copy.deepcopy(validate_closure_file())
    bad["completed_capabilities"].remove("b5_b6_qm_i_core_lineage_equivalence")
    with pytest.raises(ValueError, match="ba_qm5_capabilities_missing"):
        _validate(bad)
