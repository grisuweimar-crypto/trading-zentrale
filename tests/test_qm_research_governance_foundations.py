import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "qm_research_governance_v1.json").read_text(encoding="utf-8"))


def test_qm_foundation_is_not_productive():
    c = load_contract()
    assert c["status"] == "foundation_only_not_productive"
    assert c["activation_condition"]["phase8_must_be_complete"] is True
    assert c["activation_condition"]["productive_scanner_or_decision_logic_may_change_in_foundation"] is False


def test_evidence_state_machine_is_fail_closed_and_spent_cannot_return_unspent():
    c = load_contract()
    s = c["evidence_state_machine"]
    assert s["required"] is True
    assert s["forbidden_transition_must_fail_closed"] is True
    assert s["evaluated_confirmatory_evidence_cannot_return_to_unspent"] is True
    assert s["policy_or_hypothesis_change_after_outcome_inspection_requires_new_version"] is True
    assert {"FROZEN_FOR_CONFIRMATION", "CONFIRMATORY_EVALUATED", "PROMOTION_ELIGIBLE", "INVALIDATED"}.issubset(set(s["states"]))


def test_confirmatory_identity_binds_code_data_labels_universe_and_analysis_plan():
    c = load_contract()
    i = c["immutable_analysis_identity"]
    assert i["required_for_confirmatory_and_promotion_states"] is True
    required = set(i["required_fields"])
    assert {"hypothesis_version_hash", "analysis_plan_hash", "code_or_commit_hash", "dataset_snapshot_hash", "universe_ledger_version", "label_definition_hash"}.issubset(required)
    assert i["mutable_aliases_may_not_substitute_for_hashes"] is True


def test_evidence_consumption_handles_indirect_views_and_uncertain_equivalence_safely():
    c = load_contract()
    e = c["evidence_consumption"]
    assert e["aggregate_or_indirect_performance_views_can_consume_evidence"] is True
    assert e["default_if_equivalence_uncertain"] == "PREVENTIVE_QA_NEW_VERSION"
    assert e["outcome_driven_change_marks_inspected_evidence_spent_for_design"] is True
    assert e["spent_evidence_may_not_confirm_redesigned_rule"] is True
    assert e["registry_entry_mutation_in_place_forbidden"] is True
    assert {"access_mode", "outcome_visibility_level", "artifact_hash", "analysis_plan_version_id", "evidence_effect"}.issubset(set(e["required_log_fields"]))


def test_as_of_universe_includes_investability_and_stable_instrument_identity():
    c = load_contract()
    u = c["as_of_universe_ledger"]
    assert u["required"] is True
    assert u["historical_universe_must_be_reconstructable_without_present_day_metadata"] is True
    assert u["ticker_reuse_and_symbol_changes_require_stable_instrument_mapping"] is True
    assert u["delistings_and_suspensions_must_remain_observable_when_known"] is True
    assert "tradability_status" in u["required_fields"]
    assert "instrument_master_version" in u["required_fields"]


def test_outcome_availability_does_not_silently_drop_delistings_or_censoring():
    c = load_contract()
    o = c["outcome_availability_ledger"]
    assert o["required"] is True
    assert o["known_delisting_is_not_generic_missing"] is True
    assert o["censored_outcome_may_not_be_silently_dropped"] is True


def test_hypothesis_registry_freezes_full_analysis_plan_and_sequential_monitoring():
    c = load_contract()
    h = c["hypothesis_registry"]
    assert h["required"] is True
    assert h["confirmatory_status_requires_pre_outcome_freeze"] is True
    assert h["full_family_results_must_be_retained"] is True
    assert h["abandoned_and_negative_hypotheses_must_be_retained"] is True
    assert h["manual_cherry_picking_forbidden"] is True
    assert h["near_duplicate_successor_must_link_to_parent"] is True
    assert h["sequential_looks_require_pre_registered_schedule_or_alpha_spending_or_equivalent_control"] is True
    required = set(h["required_fields"])
    assert {"primary_estimand", "primary_inference_method", "promotion_rule", "planned_sensitivity_menu", "sequential_monitoring_plan", "analysis_plan_hash"}.issubset(required)


def test_research_derivation_graph_preserves_parentage():
    c = load_contract()
    g = c["research_derivation_graph"]
    assert g["required"] is True
    assert g["superseding_version_must_preserve_parent_link"] is True
    assert g["canonical_path"][0] == "RESEARCH_QUESTION"
    assert g["canonical_path"][-1] == "DECISION_OR_PROMOTION_OUTCOME"


def test_dependence_is_multi_axis_and_sensitivity_cannot_be_method_shopped():
    c = load_contract()
    d = c["dependence_and_robustness"]
    assert d["raw_event_count_may_not_be_presented_as_independent_n"] is True
    assert d["single_scalar_effective_n_is_not_required_and_may_not_replace_diagnostics"] is True
    assert {"TEMPORAL", "CROSS_SECTIONAL_DATE", "ENTITY_SYMBOL", "GROUP_SECTOR_DOMAIN_OR_MARKET_FACTOR"}.issubset(set(d["dependence_axes"]))
    assert d["alternative_bootstrap_cluster_or_block_methods_are_sensitivity_not_model_selection"] is True
    assert d["all_pre_registered_sensitivity_variants_must_be_reported"] is True


def test_probability_calibration_is_diagnostic_and_recalibration_is_new_model():
    c = load_contract()
    p = c["probability_calibration_audit"]
    assert p["research_only_extension"] is True
    assert p["diagnostic_before_retraining"] is True
    assert p["primary_calibration_population_must_be_defined"] is True
    assert p["selected_claim_population_and_all_eligible_population_are_distinct_estimands"] is True
    assert p["advantage_probability_is_not_equivalent_to_calibration"] is True
    assert p["recalibrator_is_new_model_version_and_requires_new_training_validation_promotion_contract"] is True
    assert p["automatic_isotonic_platt_or_beta_repair_forbidden"] is True


def test_decision_ablation_is_stateful_and_b5_b6_require_lineage_equality():
    c = load_contract()
    a = c["decision_layer_ablation"]
    assert a["must_not_relabel_phase7i_validation"] is True
    assert a["stateful_policy_evaluation_required_for_portfolio_actions"] is True
    assert a["claim_level_pairing_alone_is_insufficient_after_policy_paths_diverge"] is True
    assert a["elliott_incremental_value_primary_pair"] == ["B5", "B6"]
    assert a["b5_b6_lineage_equivalence_required_before_interpretation"] is True
    assert a["path_divergence_must_be_modeled_not_ignored"] is True
    assert {"B0_FLAT", "B0_LONG"}.issubset(set(a["candidate_ladder"].keys()))


def test_elliott_challengers_freeze_count_and_stay_conditional_on_core():
    c = load_contract()
    e = c["elliott_challenger_registry"]
    assert e["frozen_hard_rules_may_not_be_rewritten_by_challengers"] is True
    assert e["challengers_start_as_soft_sidecar_evidence"] is True
    assert e["count_or_scenario_freeze_must_precede_challenger_outcome_evaluation"] is True
    assert e["challenger_family_multiplicity_required"] is True
    assert e["incremental_value_must_be_evaluated_conditional_on_frozen_core"] is True


def test_qm_h_separates_incidents_from_methodology_findings_and_requires_verified_closure():
    c = load_contract()
    q = c["incident_and_capa"]
    assert q["continuous_across_future_development"] is True
    assert {"DEFECT", "NEAR_MISS", "INCONSISTENCY", "DEVIATION", "DATA_QUALITY_EVENT"}.issubset(set(q["incident_categories"]))
    assert {"METHODOLOGY_RISK", "OBSERVATION"}.issubset(set(q["finding_categories_not_incidents"]))
    assert q["evidence_impact_assessment_required"] is True
    assert q["closure_requires_verified_regression_or_reproduction_check"] is True
    assert q["repeated_recurrence_family_triggers_system_level_capa_review"] is True


def test_qm_i_requires_typed_lineage_independence_claims_and_external_evidence_ancestry():
    c = load_contract()
    q = c["evidence_lineage"]
    assert q["typed_provenance_graph_required"] is True
    assert q["common_ancestry_is_review_trigger_not_automatic_invalidation"] is True
    assert q["statistical_correlation_alone_does_not_prove_double_counting"] is True
    assert q["independence_claim_registry_required"] is True
    assert q["material_decision_inputs_must_trace_to_raw_source"] is True
    assert q["phase8_external_evidence_must_enter_same_lineage_graph"] is True
    assert q["no_cycles"] is True
    assert q["conditional_mutual_information_is_optional_research_not_gate"] is True


def test_qm_j_negative_controls_are_preregistered_isolated_and_have_pre_frozen_failure_rule():
    c = load_contract()
    q = c["negative_controls"]
    assert q["pre_registration_required"] is True
    assert q["isolated_artifacts_required"] is True
    assert q["canonical_data_mutation_forbidden"] is True
    assert q["reproducible_seed_or_deterministic_transform_required"] is True
    assert q["failure_rule_must_be_frozen_before_control_results_are_visible"] is True
    assert q["wrong_entity_mapping_is_pipeline_integrity_test_not_primary_statistical_negative_control"] is True
    assert q["single_expected_false_positive_is_not_automatically_systematic_failure"] is True
    assert q["systematic_placebo_signal_blocks_confirmatory_promotion_pending_investigation"] is True
    assert q["controls_may_not_be_retuned_after_results_to_make_pipeline_pass"] is True


def test_qm_dependencies_make_a_h_continuous_and_i_blocking_before_f_g():
    c = load_contract()
    d = c["implementation_dependencies"]
    assert d["continuous_from_qm_start"] == ["QM-A", "QM-H"]
    assert set(d["foundational_parallel"]) == {"QM-B", "QM-C"}
    assert d["qm_i_starts_when_b_c_schemas_exist"] is True
    assert d["qm_i_must_be_blocking_before_qm_f_or_qm_g_interpretation"] is True


def test_dynamic_portfolio_correlation_is_deferred_to_portfolio_risk_overlay():
    c = load_contract()
    assert c["deferred_outside_qm_core"]["dynamic_portfolio_correlation"] == "later_portfolio_risk_overlay"
