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


def test_spent_evidence_cannot_be_relabelled_unspent():
    c = load_contract()
    assert c["principles"]["spent_evidence_may_not_be_relabelled_unspent"] is True
    o = c["evidence_consumption"]["outcome_driven_research_change"]
    assert o["marks_inspected_evidence_spent_for_design"] is True
    assert o["spent_evidence_may_not_confirm_redesigned_rule"] is True


def test_preventive_qa_is_separate_from_outcome_driven_change():
    c = load_contract()
    assert c["principles"]["preventive_qa_is_distinct_from_outcome_driven_redesign"] is True
    assert c["evidence_consumption"]["preventive_qa_change"]["log_required"] is True
    assert c["evidence_consumption"]["outcome_driven_research_change"]["log_required"] is True


def test_as_of_universe_guards_survivorship():
    c = load_contract()
    u = c["as_of_universe_ledger"]
    assert u["required"] is True
    assert u["current_membership_may_not_define_historical_membership"] is True
    assert u["delistings_and_suspensions_must_remain_observable_when_known"] is True
    assert u["missing_target_is_distinct_from_missing_feature"] is True


def test_hypothesis_registry_requires_pre_outcome_freeze_for_confirmatory_claims():
    c = load_contract()
    h = c["hypothesis_registry"]
    assert h["required"] is True
    assert h["confirmatory_status_requires_pre_outcome_freeze"] is True
    assert h["full_family_results_must_be_retained"] is True
    assert h["manual_cherry_picking_forbidden"] is True


def test_dependence_diagnostics_do_not_choose_best_bootstrap_result():
    c = load_contract()
    d = c["dependence_and_robustness"]
    assert d["raw_event_count_may_not_be_presented_as_independent_n"] is True
    assert d["date_concentration_report_required"] is True
    assert d["symbol_concentration_report_required"] is True
    assert d["alternative_bootstrap_or_block_lengths_are_sensitivity_not_model_selection"] is True
    assert "choose_bootstrap_or_block_length_by_best_result" in c["forbidden_shortcuts"]


def test_probability_advantage_is_not_calibration_proof():
    c = load_contract()
    p = c["probability_calibration_audit"]
    assert p["research_only_extension"] is True
    assert p["advantage_probability_is_not_equivalent_to_calibration"] is True
    required = set(p["required_diagnostics_when_sample_permits"])
    assert {"reliability_curve", "brier_score", "log_loss", "calibration_intercept", "calibration_slope"}.issubset(required)


def test_decision_ablation_preserves_phase7i_and_pairs_b5_b6():
    c = load_contract()
    a = c["decision_layer_ablation"]
    assert a["must_not_relabel_phase7i_validation"] is True
    assert a["paired_same_claim_comparison_required"] is True
    assert a["elliott_incremental_value_primary_pair"] == ["B5", "B6"]
    assert a["execution_must_remain_disabled_until_separate_promotion"] is True


def test_elliott_challengers_do_not_rewrite_frozen_hard_rules():
    c = load_contract()
    e = c["elliott_challenger_registry"]
    assert e["frozen_hard_rules_may_not_be_rewritten_by_challengers"] is True
    assert e["challengers_start_as_soft_sidecar_evidence"] is True
    assert e["scenario_stability_is_not_assumed_to_equal_correctness"] is True
    assert e["w5_divergence_is_not_a_hard_rule"] is True


def test_dynamic_portfolio_correlation_is_deferred_to_portfolio_risk_overlay():
    c = load_contract()
    assert c["deferred_outside_qm_core"]["dynamic_portfolio_correlation"] == "later_portfolio_risk_overlay"
