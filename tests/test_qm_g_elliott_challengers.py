from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from scanner.research.governance.qm_g_elliott_challengers import (
    ElliottChallengerError,
    ElliottChallengerRegistry,
)


HASH = "a" * 64
CORE_HASH = "b" * 64
CHALLENGER_HASH = "c" * 64


def _record():
    return {
        "challenger_id": "qm-g:w5-momentum-divergence",
        "challenger_version": "v1",
        "hypothesis_id": "H-ELLIOTT-W5-MOM-DIV",
        "hypothesis_version": "v1",
        "hypothesis_version_hash": HASH,
        "hypothesis_family_id": "F-ELLIOTT-CHALLENGERS-W5",
        "analysis_plan_id": "AP-ELLIOTT-W5-MOM-DIV",
        "analysis_plan_version": "v1",
        "analysis_plan_hash": "d" * 64,
        "control_plan_id": "CP-ELLIOTT-W5",
        "control_plan_version": "v1",
        "control_plan_hash": "e" * 64,
        "qm_a_analysis_id": "A-ELLIOTT-W5-MOM-DIV",
        "qm_a_version_id": "v1",
        "evidence_state": "FROZEN_FOR_CONFIRMATION",
        "feature_definition": {
            "feature_id": "w5_momentum_divergence_v1",
            "definition": "Predeclared momentum divergence feature evaluated as sidecar evidence.",
            "inputs": ["price", "frozen_momentum_indicator"],
            "missing_policy": "MISSING_REMAINS_MISSING"
        },
        "pit_contract": {
            "as_of_field": "as_of",
            "available_from_field": "available_from",
            "future_data_forbidden": True,
            "outcome_data_excluded_from_feature_build": True,
            "retroactive_reclassification_forbidden": True,
            "missing_policy": "FAIL_CLOSED"
        },
        "core_binding": {
            "source_commit": "1c32230b6e4be6869f59ca4c322e57e65170e906",
            "module": "6H_module_output",
            "schema_version": "elliott_vnext_output_v2",
            "integration_contract": "elliott_vnext_integration_contract_v1",
            "core_content_hash": CORE_HASH,
            "core_frozen": True,
            "hard_rules_overridden": False
        },
        "count_scenario_freeze": {
            "freeze_id": "freeze-w5-001",
            "frozen_at": "2026-10-01T08:00:00+00:00",
            "count_or_scenario_id": "scenario-001",
            "frozen_output_hash": "f" * 64,
            "outcome_visibility_at_freeze": "NONE",
            "retrofit_after_outcome_forbidden": True
        },
        "decision_layer_evaluation": {
            "comparison": "B5_VS_B6",
            "stateful_policy_required": True,
            "same_starting_state_required": True,
            "same_observation_grid_required": True,
            "same_eligibility_required": True,
            "same_tradeability_required": True,
            "same_action_availability_required": True,
            "same_cost_model_required": True,
            "qm_f_incremental_ablation_required": True
        },
        "lineage_binding": {
            "core": {"node_id": "elliott-core", "version_id": "v1", "content_hash": CORE_HASH},
            "challenger": {"node_id": "elliott-challenger", "version_id": "v1", "content_hash": CHALLENGER_HASH},
            "independent_confirmation_claimed": False
        },
        "boundaries": {
            "sidecar_evidence_only": True,
            "elliott_core_changed": False,
            "hard_rules_changed": False,
            "universal_stance_changed": False,
            "w6_interface_replaced": False,
            "w8_action_matrix_replaced": False,
            "portfolio_action_directly_changed": False,
            "order_or_execution_generated": False,
            "productive_promotion_performed": False
        }
    }


class _Hypotheses:
    def get_hypothesis(self, hypothesis_id, version):
        return {
            "hypothesis_id": hypothesis_id,
            "hypothesis_version": version,
            "hypothesis_version_hash": HASH,
            "hypothesis_family_id": "F-ELLIOTT-CHALLENGERS-W5",
            "qm_a_analysis_id": "A-ELLIOTT-W5-MOM-DIV",
            "research_mode": "CONFIRMATION",
            "state": "FROZEN_FOR_CONFIRMATION",
        }


class _Plans:
    def get_plan(self, plan_id, version):
        return {
            "analysis_plan_id": plan_id,
            "analysis_plan_version": version,
            "analysis_plan_hash": "d" * 64,
            "hypothesis_id": "H-ELLIOTT-W5-MOM-DIV",
            "hypothesis_version": "v1",
            "hypothesis_version_hash": HASH,
            "research_mode": "CONFIRMATION",
            "state": "FROZEN_FOR_CONFIRMATION",
        }

    def validate_confirmation_ready(self, **_):
        return {"valid": True}


class _Multiplicity:
    def validate_multiplicity_ready(self, **_):
        return {
            "control_plan_hash": "e" * 64,
            "hypothesis_family_id": "F-ELLIOTT-CHALLENGERS-W5",
            "family_members": [{
                "hypothesis_id": "H-ELLIOTT-W5-MOM-DIV",
                "hypothesis_version": "v1",
                "analysis_plan_id": "AP-ELLIOTT-W5-MOM-DIV",
                "analysis_plan_version": "v1",
            }],
        }


class _QmA:
    def get_analysis(self, *_):
        return {"state": "FROZEN_FOR_CONFIRMATION"}


class _Lineage:
    def get_node(self, node_id, version_id):
        return {
            "node_id": node_id,
            "version_id": version_id,
            "content_hash": CORE_HASH if node_id == "elliott-core" else CHALLENGER_HASH,
            "lineage_complete": True,
        }

    def double_counting_review(self, **_):
        return {
            "status": "REVIEW_REQUIRED",
            "triggers": [{"trigger": "COMMON_ANCESTRY_REVIEW_REQUIRED"}],
        }


def _registry(tmp_path: Path):
    return ElliottChallengerRegistry(tmp_path / "qm_g.jsonl")


def test_registers_immutable_sidecar_challenger(tmp_path):
    registry = _registry(tmp_path)
    registry.register_challenger(record=_record(), actor_id="researcher", actor_role="RESEARCHER")
    stored = registry.get_challenger("qm-g:w5-momentum-divergence", "v1")
    assert stored["boundaries"]["sidecar_evidence_only"] is True
    assert stored["boundaries"]["elliott_core_changed"] is False
    assert stored["challenger_version_hash"]
    assert registry.verify_integrity()["challenger_version_count"] == 1


def test_forbids_retroactive_count_fit_or_outcome_visible_freeze(tmp_path):
    registry = _registry(tmp_path)
    bad = _record()
    bad["count_scenario_freeze"]["outcome_visibility_at_freeze"] = "ROW_LEVEL"
    with pytest.raises(ElliottChallengerError, match="must_precede_outcome_visibility"):
        registry.register_challenger(record=bad, actor_id="r", actor_role="R")

    bad = _record()
    bad["count_scenario_freeze"]["retrofit_after_outcome_forbidden"] = False
    with pytest.raises(ElliottChallengerError, match="retroactive_count_fit"):
        registry.register_challenger(record=bad, actor_id="r", actor_role="R")


def test_forbids_core_or_decision_layer_takeover(tmp_path):
    registry = _registry(tmp_path)
    for field in ("elliott_core_changed", "universal_stance_changed", "w6_interface_replaced", "w8_action_matrix_replaced", "portfolio_action_directly_changed", "order_or_execution_generated"):
        bad = _record()
        bad["boundaries"][field] = True
        with pytest.raises(ElliottChallengerError, match="challenger_boundary_must_be_false"):
            registry.register_challenger(record=bad, actor_id="r", actor_role="R")


def test_rejects_wrong_frozen_core_contract_identity(tmp_path):
    registry = _registry(tmp_path)
    bad = _record()
    bad["core_binding"]["module"] = "other_module"
    with pytest.raises(ElliottChallengerError, match="elliott_core_binding_mismatch:module"):
        registry.register_challenger(record=bad, actor_id="r", actor_role="R")


def test_requires_existing_qm_f_b5_b6_comparability_guards(tmp_path):
    registry = _registry(tmp_path)
    bad = _record()
    bad["decision_layer_evaluation"]["same_cost_model_required"] = False
    with pytest.raises(ElliottChallengerError, match="qm_f_guard_required"):
        registry.register_challenger(record=bad, actor_id="r", actor_role="R")


def test_evaluation_ready_binds_qm_c_qm_a_qm_i_and_freeze(tmp_path):
    registry = _registry(tmp_path)
    registry.register_challenger(record=_record(), actor_id="researcher", actor_role="RESEARCHER")
    result = registry.validate_evaluation_ready(
        challenger_id="qm-g:w5-momentum-divergence",
        challenger_version="v1",
        hypothesis_registry=_Hypotheses(),
        analysis_plan_registry=_Plans(),
        multiplicity_registry=_Multiplicity(),
        qm_a_ledger=_QmA(),
        qm_b_closure={"schema_version": "qm_b_closure_v1"},
        lineage_registry=_Lineage(),
        evaluation_started_at="2026-10-01T09:00:00+00:00",
    )
    assert result["valid"] is True
    assert result["qm_c_confirmation_ready"] is True
    assert result["qm_a_evidence_state"] == "FROZEN_FOR_CONFIRMATION"
    assert result["promotion_performed"] is False
    assert result["universal_stance_changed"] is False
    assert result["w6_interface_changed"] is False
    assert result["w8_action_matrix_changed"] is False


def test_evaluation_fails_when_count_was_frozen_after_outcomes_started(tmp_path):
    registry = _registry(tmp_path)
    registry.register_challenger(record=_record(), actor_id="researcher", actor_role="RESEARCHER")
    with pytest.raises(ElliottChallengerError, match="freeze_after_evaluation_start"):
        registry.validate_evaluation_ready(
            challenger_id="qm-g:w5-momentum-divergence",
            challenger_version="v1",
            hypothesis_registry=_Hypotheses(),
            analysis_plan_registry=_Plans(),
            multiplicity_registry=_Multiplicity(),
            qm_a_ledger=_QmA(),
            qm_b_closure={"schema_version": "qm_b_closure_v1"},
            lineage_registry=_Lineage(),
            evaluation_started_at="2026-10-01T07:59:59+00:00",
        )


def test_independent_confirmation_claim_is_forbidden_when_same_information_chain(tmp_path):
    registry = _registry(tmp_path)
    bad = deepcopy(_record())
    bad["lineage_binding"]["independent_confirmation_claimed"] = True
    with pytest.raises(ElliottChallengerError, match="may_not_claim_independent_confirmation"):
        registry.register_challenger(record=bad, actor_id="r", actor_role="R")
