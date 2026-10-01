from __future__ import annotations

from copy import deepcopy
import json

import pytest

from scanner.research.governance.qm_f_closure import (
    validate_closure_file as validate_ba_qm5_closure_file,
)
from scanner.research.governance.qm_g_challenger_evaluation import (
    load_challenger_evaluation_contract,
)
from scanner.research.governance.qm_g_closure import (
    DEFAULT_MANIFEST_PATH,
    validate_closure_file,
    validate_closure_manifest,
)
from scanner.research.governance.qm_g_elliott_challengers import load_qm_g_contract
from scanner.research.governance.qm_g_scenario_stability import (
    load_scenario_stability_contract,
)


def _fixture():
    payload = json.loads(DEFAULT_MANIFEST_PATH.read_text(encoding="utf-8"))
    return (
        payload,
        validate_ba_qm5_closure_file(),
        load_qm_g_contract(),
        load_scenario_stability_contract(),
        load_challenger_evaluation_contract(),
    )


def _validate(payload, ba_qm5, registry, scenario, evaluation):
    return validate_closure_manifest(
        payload,
        ba_qm5_closure=ba_qm5,
        registry_contract=registry,
        scenario_contract=scenario,
        evaluation_contract=evaluation,
    )


def test_ba_qm6_closure_file_is_valid_and_truthful():
    result = validate_closure_file()
    assert result["engineering_status"] == "COMPLETE"
    assert result["empirical_validation_status"] == "NOT_ESTABLISHED"
    assert result["executable_challengers"] == ["Scenario Stability"]
    assert len(result["remaining_documented_candidates"]) == 11
    assert result["promoted_challengers"] == []
    assert result["empirical_promotion_claimed"] is False
    assert result["next_mandatory_work_package"].startswith("BA-QM7 / QM-J")


def test_closure_cannot_upgrade_empirical_status_or_claim_promotion():
    payload, predecessor, registry, scenario, evaluation = _fixture()
    bad = deepcopy(payload)
    bad["empirical_validation_status"] = "ESTABLISHED"
    with pytest.raises(ValueError, match="empirical_status"):
        _validate(bad, predecessor, registry, scenario, evaluation)
    bad = deepcopy(payload)
    bad["promoted_challengers"] = ["Scenario Stability"]
    with pytest.raises(ValueError, match="promoted_challengers"):
        _validate(bad, predecessor, registry, scenario, evaluation)


def test_closure_candidate_partition_must_match_masterplan_catalog():
    payload, predecessor, registry, scenario, evaluation = _fixture()
    bad = deepcopy(payload)
    bad["executable_challengers"].append("Channel Fit")
    bad["remaining_documented_candidates"].remove("Channel Fit")
    with pytest.raises(ValueError, match="executable_challenger_set"):
        _validate(bad, predecessor, registry, scenario, evaluation)
    bad = deepcopy(payload)
    bad["remaining_documented_candidates"].remove("Alternation")
    with pytest.raises(ValueError, match="candidate_partition"):
        _validate(bad, predecessor, registry, scenario, evaluation)


def test_closure_requires_strict_scenario_stability_contract():
    payload, predecessor, registry, scenario, evaluation = _fixture()
    bad_scenario = deepcopy(scenario)
    bad_scenario["feature"]["threshold_allowed"] = True
    with pytest.raises(ValueError, match="fitted_rule_forbidden"):
        _validate(payload, predecessor, registry, bad_scenario, evaluation)
    bad_scenario = deepcopy(scenario)
    bad_scenario["feature"]["missing_state"] = "NEUTRAL"
    with pytest.raises(ValueError, match="missing_semantics"):
        _validate(payload, predecessor, registry, bad_scenario, evaluation)


def test_closure_requires_stateful_no_winner_evaluation_guards():
    payload, predecessor, registry, scenario, evaluation = _fixture()
    bad_eval = deepcopy(evaluation)
    bad_eval["principles"]["winner_not_declared_by_engine"] = False
    with pytest.raises(ValueError, match="principle_guard_missing"):
        _validate(payload, predecessor, registry, scenario, bad_eval)
    bad_eval = deepcopy(evaluation)
    bad_eval["principles"]["same_execution_cost_model_required"] = False
    with pytest.raises(ValueError, match="principle_guard_missing"):
        _validate(payload, predecessor, registry, scenario, bad_eval)


def test_closure_preserves_frozen_boundaries_and_predecessor_handoff():
    payload, predecessor, registry, scenario, evaluation = _fixture()
    bad = deepcopy(payload)
    bad["boundaries"]["w8_action_matrix_changed"] = True
    with pytest.raises(ValueError, match="boundary_values"):
        _validate(bad, predecessor, registry, scenario, evaluation)
    bad_predecessor = deepcopy(predecessor)
    bad_predecessor["next_mandatory_work_package"] = "something else"
    with pytest.raises(ValueError, match="handoff_mismatch"):
        _validate(payload, bad_predecessor, registry, scenario, evaluation)


def test_closure_next_work_package_is_qm_j_negative_controls():
    payload, predecessor, registry, scenario, evaluation = _fixture()
    bad = deepcopy(payload)
    bad["next_mandatory_work_package"] = "BA-QM8"
    with pytest.raises(ValueError, match="next_work_package"):
        _validate(bad, predecessor, registry, scenario, evaluation)
