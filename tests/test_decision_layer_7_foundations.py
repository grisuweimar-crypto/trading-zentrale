import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_contract():
    return json.loads((ROOT / "configs" / "decision_layer_7_contract_v1.json").read_text(encoding="utf-8"))


def load_output_schema():
    return json.loads((ROOT / "configs" / "decision_layer_7_output_schema_v1.json").read_text(encoding="utf-8"))


def test_probability_is_calibration_not_vote():
    c = load_contract()
    assert c["upstream_semantics"]["probability"] == "calibration_of_selection_and_timing_not_independent_vote"
    assert c["fusion_policy"]["probability_independent_directional_vote_allowed"] is False


def test_risk_and_confidence_keep_semantics():
    c = load_contract()
    assert c["upstream_semantics"]["risk"] == "downside_and_failure_risk_not_return_vote"
    assert c["upstream_semantics"]["confidence"] == "reliability_of_existing_claims_not_directional_vote"
    assert c["fusion_policy"]["risk_independent_buy_vote_allowed"] is False
    assert c["fusion_policy"]["low_confidence_is_bearish_allowed"] is False


def test_portfolio_is_after_universal_stance_and_not_training_input():
    c = load_contract()
    p = c["decision_layers"]["portfolio_action"]
    assert p["computed_after_universal_stance"] is True
    assert p["portfolio_data_may_train_universal_stance"] is False


def test_missing_evidence_fails_closed():
    c = load_contract()
    u = c["decision_layers"]["universal_stance"]
    assert u["fail_closed_state"] == "INSUFFICIENT_EVIDENCE"
    assert "INSUFFICIENT_EVIDENCE" in u["states"]


def test_manual_super_score_is_forbidden():
    c = load_contract()
    assert c["fusion_policy"]["fixed_manual_weighted_super_score_allowed"] is False
    assert "invent_manual_module_weights" in c["forbidden_shortcuts"]


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


def test_phase8_is_explicitly_separate():
    c = load_contract()
    p = c["phase8_boundary"]
    assert p["external_factor_phase"] == 8
    assert p["phase7_core_must_work_without_phase8"] is True
    assert p["unhistorized_live_information_may_train_core"] is False


def test_output_schema_keeps_universal_and_portfolio_layers_separate():
    schema = load_output_schema()
    symbol = schema["properties"]["symbols"]["additionalProperties"]["properties"]
    assert "universal_stance" in symbol
    assert "portfolio_overlay" in symbol
    assert set(symbol["universal_stance"]["enum"]) == {"BUY", "HOLD", "SELL", "INSUFFICIENT_EVIDENCE"}


def test_output_requires_reasons_counter_evidence_and_provenance():
    schema = load_output_schema()
    required = set(schema["properties"]["symbols"]["additionalProperties"]["required"])
    assert {"reasons", "counter_evidence", "provenance", "transition"}.issubset(required)


def test_holdout_and_upstream_reoptimization_are_forbidden():
    c = load_contract()
    assert c["research_policy"]["discovery_validation_holdout_separation_required"] is True
    assert c["fusion_policy"]["upstream_reoptimization_forbidden"] is True
    assert "use_holdout_for_policy_selection" in c["forbidden_shortcuts"]
