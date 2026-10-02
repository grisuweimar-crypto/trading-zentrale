import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "decision_layer_7_contract_v1.json").read_text(encoding="utf-8"))


def load_output_schema():
    return json.loads((ROOT / "configs" / "decision_layer_7_output_schema_v1.json").read_text(encoding="utf-8"))


def load_state_catalog():
    return json.loads((ROOT / "configs" / "decision_state_catalog_v1.json").read_text(encoding="utf-8"))


def symbol_schema():
    return load_output_schema()["properties"]["symbols"]["additionalProperties"]


def test_probability_is_calibration_not_vote():
    c = load_contract()
    assert c["upstream_semantics"]["probability"] == "calibration_of_selection_and_timing_not_independent_vote"
    assert c["fusion_policy"]["probability_independent_directional_vote_allowed"] is False


def test_risk_and_confidence_keep_semantics():
    c = load_contract()
    assert c["upstream_semantics"]["risk"] == "downside_and_failure_risk_not_return_vote"
    assert c["upstream_semantics"]["confidence"] == "reliability_of_existing_claims_not_directional_vote"
    assert c["fusion_policy"]["risk_independent_buy_vote_allowed"] is False
    assert c["fusion_policy"]["risk_may_invert_direction_by_itself"] is False
    assert c["fusion_policy"]["low_confidence_is_bearish_allowed"] is False


def test_portfolio_is_after_universal_stance_and_not_training_input():
    c = load_contract()
    p = c["decision_layers"]["portfolio_action"]
    assert p["computed_after_universal_stance"] is True
    assert p["portfolio_data_may_train_universal_stance"] is False
    assert "encode_portfolio_constraint_as_universal_hold" in c["forbidden_shortcuts"]


def test_hold_semantics_do_not_include_portfolio_constraints():
    c = load_contract()
    u = c["decision_layers"]["universal_stance"]
    assert set(u["hold_details"]) == {"HOLD_CONSTRUCTIVE", "HOLD_NEUTRAL", "HOLD_UNRESOLVED"}
    assert u["portfolio_constrained_hold_detail_forbidden"] is True
    assert "HOLD_PORTFOLIO_CONSTRAINED" not in u["hold_details"]


def test_missing_evidence_fails_closed_and_has_reason_taxonomy():
    c = load_contract()
    u = c["decision_layers"]["universal_stance"]
    ie = c["insufficient_evidence"]
    assert u["fail_closed_state"] == "INSUFFICIENT_EVIDENCE"
    assert ie["must_not_be_mapped_to_hold"] is True
    assert set(ie["reason_codes"]) >= {
        "INSUFFICIENT_DATA",
        "INSUFFICIENT_MODEL_COVERAGE",
        "INSUFFICIENT_CONSENSUS",
        "INSUFFICIENT_VALIDATION",
        "OUTSIDE_VALIDATED_DOMAIN",
        "STALE_OR_INCOMPATIBLE_INPUT",
        "INPUT_CONTRACT_VIOLATION",
    }


def test_manual_super_score_is_forbidden():
    c = load_contract()
    assert c["fusion_policy"]["fixed_manual_weighted_super_score_allowed"] is False
    assert "invent_manual_module_weights" in c["forbidden_shortcuts"]


def test_conflict_taxonomy_separates_risk_constraint_from_direction():
    c = load_contract()
    x = c["conflict_confirmation"]
    assert "RISK_CONSTRAINT_NOT_DIRECTIONAL_CONFLICT" in x["conflict_types"]
    assert "SELECTION_TIMING_CONFLICT" in x["conflict_types"]
    assert x["risk_constraint_must_not_automatically_relabel_buy_as_sell"] is True
    assert x["must_be_resolved_before_universal_stance_policy"] is True


def test_decision_reliability_is_structured_and_non_directional():
    c = load_contract()
    d = c["decision_reliability"]
    assert set(d["levels"]) == {"HIGH", "MEDIUM", "LOW", "INSUFFICIENT"}
    assert set(d["structured_components"]) >= {
        "coverage", "data_quality", "module_agreement", "conflict_severity", "walk_forward_support", "reason_codes"
    }
    assert d["directional_interpretation_forbidden"] is True
    assert d["internal_numeric_score_is_user_facing_truth"] is False


def test_elliott_stage_alone_cannot_decide_trade():
    c = load_contract()
    assert c["swing_management"]["elliott_stage_alone_can_decide_action"] is False
    assert c["fusion_policy"]["elliott_target_zone_alone_is_trade_decision_allowed"] is False


def test_swing_must_be_compared_with_no_swing_and_costs():
    c = load_contract()
    s = c["swing_management"]
    assert s["must_compare_against_no_swing_baseline"] is True
    assert s["transaction_costs_required"] is True
    assert s["reentry_costs_required"] is True
    assert s["missed_rebound_cost_required"] is True
    assert s["reduced_exposure_opportunity_cost_required"] is True


def test_hysteresis_has_three_research_mechanisms_and_hard_override():
    c = load_contract()
    t = c["transition_policy"]
    assert set(t["candidate_mechanisms"]) == {"confirmation_window", "evidence_margin", "exception_override"}
    assert t["confirmation_window_parameter_frozen"] is False
    assert t["evidence_margin_parameter_frozen"] is False
    assert t["hysteresis_must_not_preserve_hard_invalidated_state"] is True


def test_phase8_is_explicitly_separate():
    c = load_contract()
    p = c["phase8_boundary"]
    assert p["external_factor_phase"] == 8
    assert p["phase7_core_must_work_without_phase8"] is True
    assert p["unhistorized_live_information_may_train_core"] is False


def test_output_schema_keeps_universal_and_portfolio_layers_separate():
    props = symbol_schema()["properties"]
    assert "universal_stance" in props
    assert "portfolio_overlay" in props
    assert set(props["universal_stance"]["enum"]) == {"BUY", "HOLD", "SELL", "INSUFFICIENT_EVIDENCE"}


def test_output_schema_contains_hold_detail_and_insufficient_reasons():
    props = symbol_schema()["properties"]
    assert set(v for v in props["stance_detail"]["enum"] if v is not None) == {
        "HOLD_CONSTRUCTIVE", "HOLD_NEUTRAL", "HOLD_UNRESOLVED"
    }
    insufficient = props["insufficient_evidence_reasons"]["items"]["enum"]
    assert "INSUFFICIENT_DATA" in insufficient
    assert "INPUT_CONTRACT_VIOLATION" in insufficient


def test_output_schema_requires_structured_decision_reliability():
    d = symbol_schema()["properties"]["decision_reliability"]
    assert set(d["required"]) == {
        "level", "coverage", "data_quality", "module_agreement", "conflict_severity", "walk_forward_support", "reason_codes"
    }
    assert set(d["properties"]["level"]["enum"]) == {"HIGH", "MEDIUM", "LOW", "INSUFFICIENT"}


def test_output_requires_reasons_counter_evidence_and_provenance():
    required = set(symbol_schema()["required"])
    assert {"reasons", "counter_evidence", "provenance", "transition"}.issubset(required)


def test_state_catalog_size_ids_and_types():
    catalog = load_state_catalog()
    cases = catalog["cases"]
    c = load_contract()["decision_state_catalog"]
    assert c["minimum_cases"] <= len(cases) <= c["maximum_cases"]
    ids = [case["id"] for case in cases]
    assert len(ids) == len(set(ids))
    assert {case["kind"] for case in cases} == {"contract_deterministic", "research_pending_policy"}


def test_state_catalog_contains_core_semantic_cases():
    cases = {case["id"]: case for case in load_state_catalog()["cases"]}
    assert cases["C02_buy_but_portfolio_constrained"]["expected"]["universal_stance"] == "BUY"
    assert cases["C02_buy_but_portfolio_constrained"]["expected"]["portfolio_action"] == "NO_ACTION"
    assert cases["C06_insufficient_data"]["expected"]["reason"] == "INSUFFICIENT_DATA"
    assert cases["C13_high_risk_does_not_invert_direction"]["expected"]["conflict_type"] == "RISK_CONSTRAINT_NOT_DIRECTIONAL_CONFLICT"
    assert cases["C23_hysteresis_exception_override"]["expected"]["mechanism"] == "exception_override"


def test_contract_deterministic_catalog_cases_do_not_freeze_research_policy():
    for case in load_state_catalog()["cases"]:
        if case["kind"] == "contract_deterministic":
            assert "candidate_stance" not in case["expected"] or case["expected"].get("candidate_stance") == "INSUFFICIENT_EVIDENCE"


def test_holdout_and_upstream_reoptimization_are_forbidden():
    c = load_contract()
    assert c["research_policy"]["discovery_validation_holdout_separation_required"] is True
    assert c["fusion_policy"]["upstream_reoptimization_forbidden"] is True
    assert "use_holdout_for_policy_selection" in c["forbidden_shortcuts"]
