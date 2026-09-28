from __future__ import annotations

import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8i_decision_extension_contract_v1.json"
CONFLICT_MATRIX_PATH = ROOT / "configs" / "external_conflict_matrix_v1.json"
PROMOTION_8G_PATH = ROOT / "configs" / "external_evidence_8g_promotion_review_v1.json"
PROMOTION_8H_PATH = ROOT / "configs" / "external_evidence_8h_interaction_promotion_review_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: str) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", f"HEAD:{path}"],
        cwd=ROOT,
        text=True,
    ).strip()


def test_8i_a_is_outcome_blind_research_only_shadow_contract() -> None:
    contract = _load(CONTRACT_PATH)

    assert contract["schema_version"] == "external_evidence_8i_decision_extension_contract_v1"
    assert contract["phase"] == "8I-A"
    assert contract["status"] == "FROZEN_OUTCOME_BLIND_WAITING_FOR_PROMOTED_EXTERNAL_EVIDENCE"
    assert contract["research_only"] is True
    assert contract["shadow_mode"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["phase7_mutation_enabled"] is False
    assert contract["external_state_engine_enabled"] is False
    assert contract["extended_reliability_enabled"] is False
    assert contract["extended_stance_enabled"] is False
    assert contract["portfolio_action_change_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False

    scope = contract["scope"]
    assert scope["8i_a_is_contract_only"] is True
    assert scope["real_decision_outcome_read_allowed"] is False
    assert scope["runtime_component_binding_in_8i_a"] is False
    assert scope["runtime_state_engine_in_8i_a"] is False
    assert scope["reliability_recompute_in_8i_a"] is False
    assert scope["stance_recompute_in_8i_a"] is False
    assert scope["portfolio_action_recompute_in_8i_a"] is False


def test_parent_freeze_binds_exact_completed_8h_head_and_upstream_blobs() -> None:
    contract = _load(CONTRACT_PATH)
    freeze = contract["parent_freeze"]

    assert freeze["base_branch"] == "phase8h-cross-factor-interaction"
    assert freeze["base_commit_sha"] == "e31990b826ab093bbfdc5d7426c58d657bffc51c"

    frozen = {item["path"]: item["git_blob_sha"] for item in freeze["upstream_files"]}
    required = {
        "configs/decision_layer_input_contract_v1.json",
        "configs/decision_universal_stance_v1.json",
        "configs/decision_reliability_explainability_v1.json",
        "configs/decision_state_transition_v1.json",
        "configs/decision_portfolio_action_v1.json",
        "configs/decision_validation_promotion_v1.json",
        "configs/decision_research_dataset_v1.json",
        "configs/external_evidence_8_contract_v1.json",
        "configs/external_conflict_matrix_v1.json",
        "configs/external_evidence_8g_research_contract_v1.json",
        "configs/external_evidence_8g_promotion_review_v1.json",
        "configs/external_evidence_8h_interaction_research_contract_v1.json",
        "configs/external_evidence_8h_interaction_promotion_review_v1.json",
    }
    assert set(frozen) == required

    for path, expected_sha in frozen.items():
        assert _git_blob_sha(path) == expected_sha, path


def test_existing_8g_scope_is_not_silently_reinterpreted_as_8i_authority() -> None:
    contract = _load(CONTRACT_PATH)
    promotion_8g = _load(PROMOTION_8G_PATH)

    scope_8g = promotion_8g["approval_scope"]
    assert scope_8g["approved_state"] == "APPROVED_FOR_8H_RESEARCH_ONLY"
    assert scope_8g["authorizes_8i_decision_layer_integration"] is False
    assert scope_8g["authorizes_phase7_mutation"] is False
    assert scope_8g["authorizes_orders_or_trades"] is False

    rule = contract["input_eligibility"]["8g_main_effects"]
    assert rule["upstream_approval_state"] == scope_8g["approved_state"]
    assert rule["upstream_authorizes_8i_decision_layer_integration"] is False
    assert rule["eligible_for_8i_decision_use_with_existing_receipt"] is False
    assert rule["requires_separate_8i_research_authorization"] is True
    assert rule["implicit_transitive_promotion_forbidden"] is True


def test_existing_8h_scope_is_research_gate_not_decision_integration_authority() -> None:
    contract = _load(CONTRACT_PATH)
    promotion_8h = _load(PROMOTION_8H_PATH)

    scope_8h = promotion_8h["approval_scope"]
    assert scope_8h["maximum_state"] == "APPROVED_FOR_8I_RESEARCH_ONLY"
    assert scope_8h["authorizes_8i_research_design"] is True
    assert scope_8h["authorizes_8i_integration"] is False
    assert scope_8h["authorizes_phase7_mutation"] is False
    assert scope_8h["authorizes_orders_or_trades"] is False
    assert scope_8h["requires_separate_8i_contract_before_any_decision_layer_use"] is True

    rule = contract["input_eligibility"]["8h_interactions"]
    assert rule["required_upstream_state"] == scope_8h["maximum_state"]
    assert rule["upstream_authorizes_8i_research_design_only"] is True
    assert rule["upstream_authorizes_8i_integration"] is False
    assert rule["requires_8i_contract_and_future_binding_before_decision_use"] is True
    assert rule["automatic_activation_forbidden"] is True


def test_external_state_vocabulary_and_relation_mapping_are_inherited_exactly() -> None:
    contract = _load(CONTRACT_PATH)
    matrix = _load(CONFLICT_MATRIX_PATH)

    states = contract["state_model"]
    assert states["external_direction_states"] == matrix["external_states"]
    assert set(states["external_evidence_relation_states"]) == {
        row["relation"] for row in matrix["relations"]
    }
    assert set(states["external_evidence_relation_states"]) == {
        "CONFIRMING",
        "CONFLICTING",
        "EXTERNAL_ONLY",
        "MIXED_EXTERNAL",
        "UNKNOWN",
        "INSUFFICIENT_EXTERNAL",
    }
    assert states["unknown_is_not_insufficient"] is True
    assert states["descriptive_relation_is_not_decision_policy"] is True
    assert matrix["status"] == "descriptive_not_policy"
    assert matrix["policy_boundary"]["relation_may_change_phase7_stance_directly"] is False
    assert matrix["policy_boundary"]["external_only_is_not_a_trade_signal"] is True


def test_no_promoted_external_evidence_is_exact_fail_closed_neutral_state() -> None:
    contract = _load(CONTRACT_PATH)
    current = contract["current_empirical_entry_state"]

    assert current == {
        "state": "NO_PROMOTED_EXTERNAL_EVIDENCE",
        "operational_status": "WAITING_FOR_PROMOTED_EXTERNAL_EVIDENCE",
        "decision_active_8g_main_effect_ids": [],
        "decision_active_8h_interaction_ids": [],
        "external_direction_state": "INSUFFICIENT_EXTERNAL",
        "external_evidence_state": "INSUFFICIENT_EXTERNAL",
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
        "extended_decision_research_enabled": False,
    }

    fallback = contract["fail_closed_rules"]["when_no_valid_external_evidence_remains"]
    assert fallback == {
        "external_direction_state": "INSUFFICIENT_EXTERNAL",
        "external_evidence_state": "INSUFFICIENT_EXTERNAL",
        "decision_effect": "NO_CHANGE_TO_PHASE7_DECISION",
    }
    assert contract["fail_closed_rules"]["technical_external_failure_may_disable_phase7_core"] is False


def test_backward_compatibility_freezes_phase7_when_external_evidence_is_unavailable() -> None:
    contract = _load(CONTRACT_PATH)
    compat = contract["backward_compatibility"]

    assert compat["no_eligible_external_evidence_invariant"] == (
        "NO_PROMOTED_EXTERNAL_EVIDENCE -> INSUFFICIENT_EXTERNAL + NO_CHANGE_TO_PHASE7_DECISION"
    )
    assert compat["phase7_universal_stance_unchanged"] is True
    assert compat["phase7_reliability_unchanged"] is True
    assert compat["phase7_state_transition_unchanged"] is True
    assert compat["phase7_portfolio_action_unchanged"] is True
    assert compat["phase7_swing_management_unchanged"] is True
    assert compat["only_additive_external_block_allowed"] is True
    assert compat["phase7_output_must_remain_reconstructible"] is True
    assert compat["hidden_meta_score_forbidden"] is True


def test_no_direct_trade_path_no_arbitrary_weighting_and_no_double_counting() -> None:
    contract = _load(CONTRACT_PATH)

    order = contract["integration_order"]
    assert order["sequence"] == [
        "frozen_phase7_core",
        "external_evidence_state",
        "future_separately_validated_extended_interpretation",
        "portfolio_action",
    ]
    assert order["universal_stance_remains_depot_independent"] is True
    assert order["portfolio_action_remains_downstream"] is True
    assert order["direct_external_to_portfolio_action_forbidden"] is True
    assert order["direct_external_to_order_or_trade_forbidden"] is True

    guards = contract["aggregation_guards"]
    assert guards["numeric_vote_counting_forbidden"] is True
    assert guards["arbitrary_weights_forbidden"] is True
    assert guards["main_effect_plus_dependent_interaction_independent_vote_forbidden"] is True
    assert guards["interaction_parent_dependency_must_be_preserved"] is True
    assert guards["aggregation_logic_deferred_to_8i_c_and_must_be_outcome_blind"] is True
    assert guards["failed_hypothesis_inversion_forbidden"] is True


def test_provenance_and_evidence_consumption_are_mandatory() -> None:
    contract = _load(CONTRACT_PATH)

    assert set(contract["provenance_required_fields"]) == {
        "source",
        "source_id",
        "factor_id",
        "interaction_id_if_applicable",
        "as_of",
        "valid_from",
        "mapping_version",
        "promotion_receipt",
        "evidence_consumption_status",
        "model_or_spec_version",
        "horizon",
        "pit_status",
        "component_artifact_hash",
    }

    consumption = contract["evidence_consumption"]
    assert consumption["outcome_informed_rule_design_marks_evidence"] == "spent_for_design"
    assert consumption["8i_a_consumes_real_decision_outcomes"] is False
    assert consumption["8g_and_8h_promotion_evidence_may_not_be_claimed_as_independent_8i_confirmation"] is True
    assert consumption["holdout_may_select_rule"] is False
    assert consumption["validation_or_holdout_may_tune_thresholds_weights_signs_or_horizons"] is False
    assert consumption["promotion_by_significance_forbidden"] is True


def test_8i_a_does_not_select_metrics_or_partitions_from_outcomes() -> None:
    contract = _load(CONTRACT_PATH)
    policy = contract["outcome_and_partition_policy"]

    assert policy["upstream_label_catalog"] == "configs/decision_research_dataset_v1.json"
    assert policy["allowed_reference_labels"] == [
        "return",
        "peer_excess",
        "adverse_excursion",
        "path_max_drawdown",
    ]
    assert policy["8i_primary_decision_metric"] == "NOT_YET_DEFINED_NO_OUTCOME_READ"
    assert policy["metric_selection_after_8i_outcome_read_forbidden"] is True
    assert policy["exact_8i_discovery_validation_holdout_or_prospective_partitions_must_be_frozen_before_outcome_read"] is True
    assert policy["phase7_or_8g_or_8h_consumed_evidence_is_not_fresh_8i_confirmation"] is True
    assert policy["overlapping_forward_windows_are_iid"] is False
    assert policy["effect_size_and_uncertainty_reported_separately"] is True
    assert policy["outcome_read_in_8i_a"] is False


def test_reliability_is_researched_before_any_separate_stance_extension() -> None:
    contract = _load(CONTRACT_PATH)
    layer = contract["reliability_vs_stance"]

    assert layer["first_future_research_layer"] == "EXTENDED_RELIABILITY_SEPARATE_FROM_PHASE7_RELIABILITY"
    assert layer["phase7_reliability_must_remain_reconstructible"] is True
    assert layer["external_state_may_not_change_phase7_reliability_without_later_empirical_gate"] is True
    assert layer["stance_extension_requires_separate_later_preregistration_and_validation"] is True
    assert layer["stance_extension_enabled_now"] is False


def test_parked_18_september_audit_remains_out_of_scope() -> None:
    contract = _load(CONTRACT_PATH)
    parked = contract["parked_audit"]

    assert parked["possible_earlier_prospective_collection_start"] == "2026-09-18"
    assert parked["status"] == "PARKED_OUT_OF_SCOPE"
    assert parked["may_be_changed_in_8i_a"] is False
    assert parked["requires_separate_pit_outcome_blind_audit"] is True


def test_8i_a_completion_is_technical_only_and_does_not_start_8i_b() -> None:
    contract = _load(CONTRACT_PATH)
    gate = contract["completion_gate"]
    production = contract["production_gate"]

    assert gate["technical_completion_requires_contract_tests_docs_and_ci"] is True
    assert gate["empirical_promotion_granted_by_8i_a"] is False
    assert gate["8i_a_complete_does_not_enable_external_decision_influence"] is True
    assert gate["next_subblock_after_manual_start"] == "8I-B_PROMOTION_AND_PROVENANCE_BINDING"

    assert production["state"] == "CLOSED"
    assert production["research_only_shadow_until_manual_final_review"] is True
    assert production["may_overwrite_phase7_output"] is False
    assert production["may_change_portfolio_action"] is False
    assert production["may_generate_orders"] is False
    assert production["automatic_future_8g_or_8h_promotion_activation"] is False
