import json
from pathlib import Path

import pytest

from scanner.research.decision_layer import (
    DecisionInputError,
    build_input_packet,
    validate_input_packet,
)


ROOT = Path(__file__).resolve().parents[1]
AS_OF = "2026-09-25T18:00:00+00:00"


def _row(family, claim_id, payload, **overrides):
    row = {
        "family": family,
        "claim_id": claim_id,
        "as_of": AS_OF,
        "available_from": AS_OF,
        "source_version": f"test_{family}_v1",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": payload,
    }
    row.update(overrides)
    return row


def _selection():
    return _row(
        "selection",
        "selection:TEST:2026-09-25",
        {
            "score": 71.2,
            "score_percentile": 0.91,
            "quality_band": "B5",
            "r_code": "R5",
            "score_status": "OK",
            "trend_ok": True,
            "liquidity_ok": True,
        },
        integration_mode="production_existing",
        maturity_state="not_applicable",
    )


def _timing():
    return _row(
        "timing",
        "timing:TEST:p1:5T",
        {
            "pattern_id": "p1",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
        },
    )


def _probability():
    return _row(
        "probability",
        "probability:TEST:p1:5T",
        {
            "horizon_sessions": 5,
            "beta_shrunk_probability": 0.61,
            "probability_advantage_pp": 6.0,
            "robust_interval_available": True,
        },
        claim_ref="timing:TEST:p1:5T",
    )


def _risk():
    return _row(
        "risk",
        "risk:TEST:5T",
        {
            "horizon_sessions": 5,
            "validated_factors": ["volatility", "drawdown"],
            "protection_role": True,
        },
    )


def _confidence():
    return _row(
        "confidence",
        "confidence:TEST:p1:5T",
        {
            "horizon_sessions": 5,
            "reliability_state": "robust",
            "data_quality_state": "sufficient",
        },
        claim_ref="timing:TEST:p1:5T",
        integration_mode="shadow_only",
    )


def _elliott():
    return _row(
        "elliott",
        "elliott:TEST:daily:minor",
        {
            "schema_version": "elliott_vnext_output_v2",
            "module": "6H_module_output",
            "research_only": True,
            "routing_is_trade_decision": False,
            "structural_fit": None,
            "confirmation_strength": None,
            "integration": {
                "contract_version": "elliott_vnext_integration_contract_v1",
                "decision_layer_required": True,
                "productive_integration_enabled": False,
                "direct_ordering_allowed": False,
                "review_contexts_are_not_actions": True,
            },
        },
        maturity_state="directional_but_immature",
    )


def test_7a_contract_freezes_semantic_boundaries():
    contract = json.loads((ROOT / "configs/decision_layer_input_contract_v1.json").read_text(encoding="utf-8"))
    assert contract["schema_version"] == "decision_layer_input_contract_v1"
    assert contract["research_only"] is True
    assert contract["productive_integration_enabled"] is False
    assert contract["evidence_families"]["selection"]["directional_authority"] is True
    assert contract["evidence_families"]["timing"]["directional_authority"] is True
    assert contract["evidence_families"]["probability"]["directional_authority"] is False
    assert contract["evidence_families"]["risk"]["directional_authority"] is False
    assert contract["evidence_families"]["confidence"]["directional_authority"] is False
    assert contract["evidence_families"]["elliott"]["directional_authority"] is False
    assert contract["cross_family_rules"]["missing_family_does_not_become_neutral_evidence"] is True
    assert contract["cross_family_rules"]["portfolio_state_forbidden_in_7a"] is True


def test_complete_typed_packet_is_admitted_without_stance_or_action():
    packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id="snapshot-1",
        evidence=[_selection(), _timing(), _probability(), _risk(), _confidence(), _elliott()],
    )
    coverage = packet["coverage"]
    assert coverage["admission_state"] == "admissible"
    assert coverage["directional_claim_count"] == 2
    assert coverage["missing_is_neutral"] is False
    assert coverage["stance_computed"] is False
    assert coverage["portfolio_action_computed"] is False
    assert "universal_stance" not in packet
    assert "portfolio_action" not in packet


def test_missing_optional_families_stay_explicit_gaps():
    packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id="snapshot-2",
        evidence=[_selection()],
    )
    assert packet["coverage"]["admission_state"] == "admissible_with_gaps"
    assert packet["coverage"]["families"]["elliott"]["present"] is False
    assert packet["coverage"]["families"]["probability"]["present"] is False


def test_no_directional_evidence_is_insufficient_not_neutral():
    packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id="snapshot-3",
        evidence=[_risk()],
    )
    assert packet["coverage"]["admission_state"] == "insufficient_directional_evidence"
    assert packet["coverage"]["directional_claim_count"] == 0
    assert packet["coverage"]["missing_is_neutral"] is False


def test_probability_is_attachment_not_independent_vote():
    timing = _timing()
    probability = _probability()
    probability["payload"]["direction"] = "positive"
    with pytest.raises(DecisionInputError, match="probability_must_not_be_directional_vote"):
        build_input_packet(
            symbol="TEST",
            as_of=AS_OF,
            source_snapshot_id="snapshot-4",
            evidence=[timing, probability],
        )


def test_probability_claim_reference_must_resolve_to_directional_claim():
    probability = _probability()
    probability["claim_ref"] = "missing-claim"
    with pytest.raises(DecisionInputError, match="probability_claim_ref_unresolved"):
        build_input_packet(
            symbol="TEST",
            as_of=AS_OF,
            source_snapshot_id="snapshot-5",
            evidence=[_selection(), probability],
        )


def test_risk_cannot_invert_or_vote_on_direction():
    risk = _risk()
    risk["payload"]["stance"] = "sell"
    with pytest.raises(DecisionInputError, match="risk_must_not_be_directional_vote"):
        build_input_packet(
            symbol="TEST",
            as_of=AS_OF,
            source_snapshot_id="snapshot-6",
            evidence=[_selection(), risk],
        )


def test_confidence_cannot_encode_attractiveness():
    confidence = _confidence()
    confidence["payload"]["attractiveness"] = 0.9
    with pytest.raises(DecisionInputError, match="confidence_must_not_encode_attractiveness_or_direction"):
        build_input_packet(
            symbol="TEST",
            as_of=AS_OF,
            source_snapshot_id="snapshot-7",
            evidence=[_timing(), confidence],
        )


def test_future_evidence_fails_closed():
    selection = _selection()
    selection["available_from"] = "2026-09-26T00:00:00+00:00"
    with pytest.raises(DecisionInputError, match="future_evidence_available_from"):
        build_input_packet(
            symbol="TEST",
            as_of=AS_OF,
            source_snapshot_id="snapshot-8",
            evidence=[selection],
        )


def test_elliott_must_preserve_6h_research_and_ordering_guards():
    elliott = _elliott()
    elliott["payload"]["integration"]["direct_ordering_allowed"] = True
    with pytest.raises(DecisionInputError, match="elliott_direct_ordering_must_remain_disabled"):
        build_input_packet(
            symbol="TEST",
            as_of=AS_OF,
            source_snapshot_id="snapshot-9",
            evidence=[_selection(), elliott],
        )


def test_action_and_portfolio_fields_are_recursively_forbidden_in_7a():
    packet = {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": "TEST",
        "as_of": AS_OF,
        "source_snapshot_id": "snapshot-10",
        "evidence": [_selection()],
        "portfolio_action": "ADD",
    }
    with pytest.raises(DecisionInputError, match="forbidden_decision_or_portfolio_fields"):
        validate_input_packet(packet)
