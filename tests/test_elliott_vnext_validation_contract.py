import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load(name):
    return json.loads((ROOT / "configs" / name).read_text(encoding="utf-8"))


def test_validation_freeze_and_unspent_evidence_boundary_are_explicit():
    contract = _load("elliott_vnext_validation_v1.json")
    assert contract["rules_frozen_through"] == "2026-09-25"
    evidence = contract["formal_unspent_evidence"]
    assert evidence["claim_available_after"] == "2026-09-25"
    assert evidence["legacy_data_can_support_promotion"] is False
    assert evidence["prospective_evidence_required_for_promotion"] is True


def test_validation_horizons_and_block_rule_are_frozen():
    contract = _load("elliott_vnext_validation_v1.json")
    assert contract["horizons_sessions"] == [5, 10, 20, 40, 60]
    uncertainty = contract["uncertainty"]
    assert uncertainty["method"] == "circular_moving_observation_date_blocks"
    assert uncertainty["effective_block_length_sessions"] == "2_x_evaluated_horizon"
    assert uncertainty["complete_date_clusters_stay_together"] is True
    assert uncertainty["minimum_time_separated_support_regions"] == 2
    assert uncertainty["iid_intervals_must_not_establish_robust_evidence"] is True


def test_no_raw_return_fallback_and_no_same_session_target_hit():
    contract = _load("elliott_vnext_validation_v1.json")
    price = contract["price_policy"]
    assert price["outcome_return_requires_adjusted_close"] is True
    assert price["raw_close_fallback_for_returns"] is False
    assert price["projection_hit_window_begins_next_session"] is True
    assert price["same_session_pre_signal_high_low_must_not_count_as_future_hit"] is True


def test_no_invented_swing_execution_policy():
    contract = _load("elliott_vnext_validation_v1.json")
    routing = contract["routing_validation"]
    assert routing["review_context_not_trade_instruction"] is True
    assert routing["round_trip_pnl_requires_explicit_execution_policy"] is True
    assert routing["default_execution_policy"] is None
    assert routing["must_not_invent_position_fraction_or_reentry_size"] is True


def test_missing_external_context_cannot_fall_back_to_scanner_peers():
    contract = _load("elliott_vnext_validation_v1.json")
    context = contract["market_context_validation"]
    assert context["consume_only_6f_quality_gated_context"] is True
    assert context["scanner_peer_fallback_for_external_context"] is False
    assert context["missing_verified_context_stays_missing"] is True


def test_output_schema_blocks_automatic_promotion_semantics():
    schema = _load("elliott_vnext_validation_output_schema_v1.json")
    props = schema["properties"]
    assert props["automatic_promotion_allowed"] == {"const": False}
    assert props["technical_completion_is_empirical_validation"] == {"const": False}
    assert props["round_trip_pnl_evaluated"] == {"const": False}
    assert props["numeric_w5_levels_promoted"] == {"const": False}
    assert props["trade_decision"] == {"type": ["null"]}
    assert props["order_instruction"] == {"type": ["null"]}
