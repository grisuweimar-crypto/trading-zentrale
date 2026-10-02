from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs" / "external_evidence_8h_interaction_research_contract_v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _git_blob_sha(path: Path) -> str:
    data = path.read_bytes()
    payload = f"blob {len(data)}\0".encode("ascii") + data
    return hashlib.sha1(payload).hexdigest()


def test_8h_a_contract_is_research_only_and_empirically_closed() -> None:
    contract = _load(CONTRACT_PATH)

    assert contract["schema_version"] == "external_evidence_8h_interaction_research_contract_v1"
    assert contract["phase"] == "8H-A"
    assert contract["status"] == "PRE_REGISTERED_OUTCOME_BLIND_NO_PROMOTED_FACTORS"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["automatic_promotion_enabled"] is False
    assert contract["phase8i_integration_enabled"] is False
    assert contract["orders_or_trades_enabled"] is False

    state = contract["current_empirical_entry_state"]
    assert state["state"] == "NO_PROMOTED_FACTORS"
    assert state["promoted_factor_horizon_ids"] == []
    assert state["empirical_interaction_research_enabled"] is False
    assert state["hard_rule"] == "NO_PROMOTED_FACTORS -> NO_EMPIRICAL_INTERACTION_RESEARCH"
    assert state["synthetic_contract_tests_allowed"] is True
    assert state["synthetic_results_are_empirical_evidence"] is False


def test_8h_a_parent_contracts_are_bound_to_exact_frozen_blobs() -> None:
    contract = _load(CONTRACT_PATH)
    parent = contract["parent_freeze"]

    assert parent["verified_parent_commit"] == "e461405ae1e0595db94a10bc20578bf9108b99fc"

    bound = {
        "phase8_contract": "phase8_contract_git_blob_sha",
        "8g_research_contract": "8g_research_contract_git_blob_sha",
        "8g_split_plan": "8g_split_plan_git_blob_sha",
        "8g_promotion_review": "8g_promotion_review_git_blob_sha",
        "source_registry": "source_registry_git_blob_sha",
    }
    for path_key, sha_key in bound.items():
        path = ROOT / parent[path_key]
        assert path.exists(), parent[path_key]
        assert _git_blob_sha(path) == parent[sha_key]


def test_8h_a_does_not_move_the_frozen_prospective_start() -> None:
    contract = _load(CONTRACT_PATH)
    evidence = contract["prospective_and_evidence_consumption"]
    split_plan = _load(ROOT / contract["parent_freeze"]["8g_split_plan"])

    expected = "2026-09-28T05:24:30.406650+00:00"
    assert evidence["8g_prospective_lower_bound_unchanged"] == expected
    assert split_plan["prospective_cohort_not_before"] == expected
    assert evidence["retroactive_start_change_allowed_in_8h_a"] is False
    assert evidence["separate_18_september_audit_remains_out_of_scope"] is True


def test_8h_a_requires_exact_factor_horizon_upstream_approval() -> None:
    contract = _load(CONTRACT_PATH)
    eligibility = contract["upstream_eligibility"]
    promotion = _load(ROOT / contract["parent_freeze"]["8g_promotion_review"])

    assert eligibility["external_approval_unit"] == "factor_id+horizon_sessions"
    assert eligibility["required_upstream_state"] == "APPROVED_FOR_8H_RESEARCH_ONLY"
    assert eligibility["approval_must_match_exact_factor_id"] is True
    assert eligibility["approval_must_match_exact_horizon_sessions"] is True
    assert eligibility["approval_at_one_horizon_authorizes_other_horizons"] is False
    assert eligibility["all_10_phase8_promotion_criteria_must_pass"] is True
    assert eligibility["governance_blockers_must_be_empty"] is True
    assert eligibility["source_registry_identity_may_be_manually_waived"] is False
    assert eligibility["defer_or_reject_is_eligible"] is False
    assert eligibility["rejection_authorizes_inverse_signal"] is False

    assert promotion["approval_scope"]["approved_state"] == eligibility["required_upstream_state"]
    assert promotion["manual_review"]["approval_requires_all_10_criteria_pass"] is True
    assert promotion["manual_review"]["approval_requires_no_governance_blockers"] is True
    assert promotion["source_governance"]["all_frozen_source_ids_must_resolve_in_source_registry"] is True


def test_8h_a_freezes_governance_not_concrete_interaction_candidates() -> None:
    contract = _load(CONTRACT_PATH)
    scope = contract["interaction_scope"]
    guards = contract["pre_registration_guards"]

    assert scope["allowed_interaction_classes"] == ["CORE_X_EXTERNAL", "EXTERNAL_X_EXTERNAL"]
    assert scope["concrete_interaction_specs_frozen_in_this_block"] == []
    assert scope["concrete_candidate_selection_stage"] == (
        "8H-C_AFTER_8H-B_ELIGIBILITY_BINDING_BEFORE_ANY_INTERACTION_OUTCOME_ACCESS"
    )
    assert scope["external_component_must_have_exact_upstream_approval"] is True
    assert scope["external_x_external_requires_both_exact_upstream_approvals"] is True
    assert scope["unpromoted_external_component_allowed"] is False
    assert scope["proxy_substitution_for_unpromoted_component_allowed"] is False
    assert scope["automatic_cartesian_product_generation_allowed"] is False
    assert scope["candidate_ranking_before_spec_freeze_allowed"] is False
    assert scope["interaction_variants_per_frozen_hypothesis"] == 1

    assert guards["real_interaction_outcomes_read_before_contract_freeze"] is False
    assert guards["interaction_candidates_selected_from_outcomes"] is False
    assert guards["economic_theory_ranking_used_to_select_candidates"] is False
    assert guards["outcome_derived_direction_or_sign_allowed"] is False
    assert guards["outcome_derived_thresholds_allowed"] is False
    assert guards["arbitrary_feature_cross_product_search_allowed"] is False


def test_8h_a_comparison_is_incremental_beyond_permitted_main_effects() -> None:
    contract = _load(CONTRACT_PATH)
    model = contract["model_comparison"]

    assert model["baseline_model"] == "FROZEN_PERMITTED_MAIN_EFFECTS"
    assert model["challenger_model"] == (
        "FROZEN_PERMITTED_MAIN_EFFECTS_PLUS_EXACTLY_ONE_PREREGISTERED_INTERACTION"
    )
    assert model["underlying_main_effects_must_remain_in_both_models"] is True
    assert model["challenger_may_remove_main_effects"] is False
    assert model["same_rows_required"] is True
    assert model["same_snapshot_identity_required"] is True
    assert model["same_pit_identity_required"] is True
    assert model["same_matured_labels_required"] is True
    assert model["interaction_may_claim_main_effect_gain_as_its_own"] is False
    assert model["estimator"] == "Ridge Linear Regression"
    assert model["alpha"] == 1.0
    assert model["fit_intercept"] is True
    assert model["hyperparameter_tuning"] is False
    assert model["validation_or_holdout_refit"] is False


def test_8h_a_outcomes_statistics_and_dependence_are_frozen_without_results() -> None:
    contract = _load(CONTRACT_PATH)
    outcomes = contract["outcomes"]
    stats = contract["dependence_and_statistics"]

    assert outcomes["horizons_sessions"] == [5, 20, 40, 60]
    assert outcomes["primary_outcome"] == "peer_excess"
    assert outcomes["forward_outcomes_are_labels_never_features"] is True
    assert stats["overlapping_forward_windows_are_iid"] is False
    assert stats["comparison_loss"] == (
        "squared_error_main_effects_minus_squared_error_interaction_challenger"
    )
    assert stats["primary_uncertainty_method"] == "moving_block_bootstrap_by_observation_date"
    assert stats["block_length_sessions_rule"] == "2 * evaluated_horizon_sessions"
    assert stats["bootstrap_repetitions"] == 1000
    assert stats["bootstrap_seed"] == 20260928
    assert stats["confidence_level"] == 0.95
    assert stats["non_overlap_sensitivity_required"] is True
    assert stats["minimum_paired_n_per_interaction_horizon_split"] == 30
    assert stats["minimum_temporal_support_regions"] == 2


def test_8h_a_requires_fresh_confirmation_and_tracks_spent_evidence() -> None:
    contract = _load(CONTRACT_PATH)
    evidence = contract["prospective_and_evidence_consumption"]

    assert evidence["8g_validation_or_holdout_used_for_main_effect_promotion_is_fresh_8h_confirmation"] is False
    assert evidence["fresh_8h_confirmatory_evidence_required"] is True
    assert evidence["fresh_confirmatory_row_not_before_interaction_spec_freeze"] is True
    assert evidence["fresh_confirmatory_row_not_before_all_required_upstream_approvals"] is True
    assert evidence["spent_pit_valid_rows_may_support_documented_discovery_diagnostics_only"] is True
    assert evidence["outcome_inspection_causing_design_change_marks_evidence_spent_for_design"] is True
    assert evidence["human_outcome_inspection_log_required"] is True


def test_8h_a_multiplicity_family_is_empty_now_and_frozen_before_future_outcomes() -> None:
    contract = _load(CONTRACT_PATH)
    multiplicity = contract["multiplicity_control"]

    assert multiplicity["current_confirmatory_family"] == []
    assert multiplicity["family_membership_frozen_before_first_interaction_outcome_join"] is True
    assert multiplicity["family_membership_may_shrink_after_outcome_inspection"] is False
    assert multiplicity["method"] == "Holm"
    assert multiplicity["family_wise_alpha"] == 0.05
    assert multiplicity["secondary_outcomes_in_confirmatory_family"] is False
    assert multiplicity["interaction_results_may_not_decide_upstream_eligibility"] is True


def test_8h_a_keeps_decision_layer_and_production_closed() -> None:
    contract = _load(CONTRACT_PATH)
    boundary = contract["cost_and_decision_boundary"]
    promotion = contract["future_interaction_promotion_boundary"]
    forbidden = set(contract["forbidden"])

    assert boundary["8h_primary_scope"] == "prediction_and_evidence_quality_only"
    assert boundary["interaction_directly_changes_orders_or_trades"] is False
    assert boundary["interaction_directly_changes_phase7_stance_or_action"] is False
    assert boundary["decision_layer_effect_belongs_to"] == "8I"
    assert promotion["maximum_approval_scope"] == "APPROVED_FOR_8I_RESEARCH_ONLY"
    assert promotion["authorizes_production"] is False
    assert promotion["authorizes_phase7_mutation"] is False
    assert promotion["authorizes_orders_or_trades"] is False
    assert promotion["separate_manual_review_required"] is True

    required_forbidden = {
        "unpromoted_external_factor_injection",
        "cross_horizon_promotion_leakage",
        "arbitrary_feature_pair_search",
        "automatic_cartesian_interaction_search",
        "outcome_based_sign_choice",
        "outcome_based_threshold_choice",
        "validation_or_holdout_hyperparameter_tuning",
        "post_hoc_confirmatory_family_reduction",
        "failed_hypothesis_inversion",
        "main_effect_removal_from_interaction_model",
        "interaction_credit_for_existing_main_effect",
        "reuse_8g_confirmatory_evidence_as_fresh_8h_confirmation",
        "phase7_mutation",
        "elliott_vnext_proxy_validation",
        "direct_order_or_trade_decision",
    }
    assert required_forbidden <= forbidden


def test_8h_a_completion_gate_stops_before_8h_b() -> None:
    contract = _load(CONTRACT_PATH)
    gate = contract["8h_a_completion_gate"]

    assert gate["contract_file_committed"] is True
    assert gate["contract_tests_required"] is True
    assert gate["ci_required"] is True
    assert gate["real_interaction_outcomes_may_be_read_before_gate_passes"] is False
    assert gate["current_empirical_interaction_research_must_remain_closed_while_no_promotions_exist"] is True
    assert gate["next_step_after_pass"] == "8H-B_INTERACTION_ELIGIBILITY_PROMOTION_BINDING"
