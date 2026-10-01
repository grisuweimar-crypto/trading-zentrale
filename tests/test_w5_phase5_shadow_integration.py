from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.integrated_evidence import (
    IntegratedDecisionEvidenceError,
    integrate_current_packet_set,
)
from scanner.research.decision_layer.portfolio_action import compute_portfolio_action
from scanner.research.decision_layer.state_transition import build_state_transition_history
from scanner.research.decision_layer.universal_stance import compute_universal_stance


SNAPSHOT = "w5-snapshot"
SCANNER_TIME = "2026-09-30T20:00:00+00:00"
PHASE5_TIME = "2026-09-30T20:30:00+00:00"
FINAL_TIME = "2026-09-30T21:00:00+00:00"
PHASE5_COMMIT = "1234567890abcdef1234567890abcdef12345678"


def _selection() -> dict[str, object]:
    return {
        "family": "selection",
        "claim_id": f"selection:RACE:{SNAPSHOT}",
        "as_of": SCANNER_TIME,
        "available_from": SCANNER_TIME,
        "source_version": "scanner:test",
        "coverage_state": "available",
        "maturity_state": "not_applicable",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": {"score": 30.0, "quality_band": "B5", "direction": "positive"},
    }


def _timing() -> dict[str, object]:
    return {
        "family": "timing",
        "claim_id": "timing:RACE:w5-positive:5T",
        "as_of": SCANNER_TIME,
        "available_from": SCANNER_TIME,
        "source_version": "phase1b:test",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": "w5-positive",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
            "direction": "positive",
        },
    }


def _base_packet_set() -> dict[str, object]:
    packet = build_input_packet(
        symbol="RACE",
        as_of=SCANNER_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[_selection(), _timing()],
    )
    return {
        "schema_version": "decision_current_packet_set_7a_v1",
        "phase": "7A-current-orchestration",
        "snapshot_id": SNAPSHOT,
        "as_of": SCANNER_TIME,
        "daily_as_of": "2026-09-30",
        "packet_count": 1,
        "packets": [packet],
        "semantics": {},
        "validation": {"research_only": True},
    }


def _daily() -> dict[str, object]:
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": SNAPSHOT,
        "source_snapshot_id": SNAPSHOT,
        "as_of": "2026-09-30",
        "generated_at": SCANNER_TIME,
        "universe_size": 1,
        "symbols": {
            "RACE": {
                "current": {
                    "name": "Ferrari N.V.",
                    "score": 30.0,
                    "r_code": "R3",
                    "rs3m": 0.02,
                    "trend200": 0.08,
                    "close": 393.27,
                    "currency": "USD",
                },
                "dynamics": {},
            }
        },
    }


def _phase4() -> dict[str, object]:
    return {
        "phase": "4_confidence_vnext_empirical_research",
        "semantics": {
            "research_only": True,
            "production_confidence_changed": False,
            "scalar_confidence_mapping_created": False,
            "confidence_thresholds_created": False,
        },
        "config": {"evidence_version": "phase4_confidence_research_v1"},
        "current": {
            "snapshot_id": SNAPSHOT,
            "as_of": "2026-09-30",
            "generated_at": SCANNER_TIME,
            "rows": [{
                "as_of": "2026-09-30",
                "symbol": "RACE",
                "horizon_sessions": 5,
                "selection": {"state": "robust", "direction": "positive"},
                "timing": {"state": "robust_claim", "direction": "positive"},
                "risk": {"state": "middle"},
                "data_quality": {},
                "model_agreement": {"state": "agreement"},
                "regime": {"state": "unknown_unvalidated"},
            }],
        },
    }


def _phase5(*, eligible_horizon: int | None = None, production_change: bool = False) -> dict[str, object]:
    horizons: dict[str, object] = {}
    for horizon in (5, 20, 40, 60):
        eligible = horizon == eligible_horizon
        horizons[str(horizon)] = {
            "finalized_phase5d_epochs": 0,
            "epoch_assessments": [],
            "promotion": {
                "status": "eligible_for_separate_promotion_review" if eligible else "insufficient_evidence",
                "gates": {
                    "multiple_walkforward_evaluations": eligible,
                    "pit_leakage_audit_passed": eligible,
                    "reproducible_versions": eligible,
                    "robust_uncertainty_available": eligible,
                    "concentration_check_passed": eligible,
                    "temporal_stability_check_passed": eligible,
                    "baseline_advantage_demonstrated": eligible,
                    "minimum_robust_epochs_pre_registered": eligible,
                },
                "robust_promotion_epochs": 2 if eligible else 0,
                "production_change_performed": production_change,
            },
        }
    return {
        "phase": "5E_adaptive_shadow_and_promotion",
        "schema_version": "phase5e_adaptive_shadow_v1",
        "status": (
            "promotion_review_eligible_for_at_least_one_horizon"
            if eligible_horizon is not None
            else "phase5_engineering_complete_collecting_evidence"
        ),
        "technical_phase5_complete": True,
        "claims": 6604,
        "mature_outcomes": 0,
        "phase5c_versions": 0,
        "phase5d_finalized_evaluations": 0,
        "policy": {
            "version": "phase5e_state_reliability_follow_flip_v1",
            "sha256": "a" * 64,
        },
        "horizons": horizons,
        "semantics": {
            "research_only": True,
            "adaptive_shadow_inference_active": True,
            "promotion_is_horizon_specific": True,
            "production_confidence_changed": False,
            "adaptive_production_weights_created": False,
            "portfolio_or_depot_watch_changed": False,
            "production_change_performed": production_change,
            "eligible_status_requires_separate_promotion_review": True,
        },
    }


def _integrated(phase5: dict[str, object] | None) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    if phase5 is not None:
        kwargs.update({
            "phase5_report": phase5,
            "phase5_source_commit": PHASE5_COMMIT,
            "phase5_available_from": PHASE5_TIME,
        })
    return integrate_current_packet_set(
        packet_set=_base_packet_set(),
        daily=_daily(),
        history=pd.DataFrame(columns=["date", "symbol", "rs3m", "observation_type"]),
        phase4_report=_phase4(),
        finalized_at=FINAL_TIME,
        **kwargs,
    )


def _position() -> dict[str, object]:
    return {
        "schema_version": "decision_position_snapshot_v1",
        "symbol": "RACE",
        "source_snapshot_id": "position-race",
        "as_of": "2026-09-30T20:45:00+00:00",
        "position_state": "long",
        "quantity": 1,
        "average_entry_price": 350.0,
        "current_price": 393.27,
        "currency": "USD",
    }


def test_w5_insufficient_evidence_is_shadow_only_and_cannot_change_stance_or_action():
    without = _integrated(None)["packets"][0]
    with_shadow_set = _integrated(_phase5())
    with_shadow = with_shadow_set["packets"][0]

    stance_without = compute_universal_stance(without)
    stance_with = compute_universal_stance(with_shadow)
    assert stance_with["universal_stance"] == stance_without["universal_stance"]
    assert stance_with["evidence_structure"]["known_directional_claim_ids"] == stance_without["evidence_structure"]["known_directional_claim_ids"]

    transition_without = build_state_transition_history([stance_without])
    transition_with = build_state_transition_history([stance_with])
    action_without = compute_portfolio_action(transition_without, _position())
    action_with = compute_portfolio_action(transition_with, _position())
    assert action_with["portfolio_action"] == action_without["portfolio_action"]
    assert action_with["swing_management"] == action_without["swing_management"]

    phase5_rows = [
        row for row in with_shadow["evidence"]
        if row["family"] == "confidence"
        and row["payload"].get("context_type") == "phase5_confidence_shadow_governance_v1"
    ]
    assert len(phase5_rows) == 1
    row = phase5_rows[0]
    assert row["integration_mode"] == "shadow_only"
    assert row["coverage_state"] == "insufficient"
    assert row["maturity_state"] == "insufficient_evidence"
    assert row["payload"]["insufficient_evidence_horizons"] == [5, 20, 40, 60]
    assert row["payload"]["production_change_performed"] is False
    assert row["payload"]["current_symbol_shadow_policy_evaluated"] is False
    assert row["payload"]["changes_universal_stance"] is False
    assert row["payload"]["changes_portfolio_action"] is False
    serialized = json.dumps(row["payload"], sort_keys=True)
    for forbidden in ('"direction"', '"stance"', '"vote"', '"attractiveness"'):
        assert forbidden not in serialized

    assert with_shadow_set["semantics"]["phase5_shadow_context_attached"] is True
    assert with_shadow_set["semantics"]["phase5_shadow_integration_mode"] == "shadow_only"
    assert with_shadow_set["semantics"]["phase5_current_symbol_policy_evaluated"] is False
    assert with_shadow_set["semantics"]["phase5_production_change_performed"] is False
    assert with_shadow_set["semantics"]["phase5_unpromoted_changes_decision"] is False


def test_w5_watch_surfaces_shadow_state_without_changing_action_or_attention():
    integrated = _integrated(_phase5())
    position_book = {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-book",
        "as_of": "2026-09-30T20:45:00+00:00",
        "positions": [_position()],
    }
    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(), position_book, integrated["packets"]
    )
    row = watch["rows"][0]
    assert row["decision"]["phase5_shadow_integration_mode"] == "shadow_only"
    assert row["decision"]["phase5_shadow_insufficient_evidence_horizons"] == [5, 20, 40, 60]
    assert row["decision"]["phase5_shadow_eligible_review_horizons"] == []
    assert row["decision"]["phase5_shadow_production_change_performed"] is False
    assert row["decision"]["phase5_shadow_changes_portfolio_action"] is False
    assert row["phase5_confidence_shadow"]["payload"]["current_symbol_shadow_policy_evaluated"] is False
    assert row["decision"]["portfolio_action_state"] == "WAIT_CONFIRMATION"
    assert row["attention_required"] is True  # caused by 7F WAIT_CONFIRMATION, not by Phase 5.
    assert diagnostics["phase5_shadow_symbols"] == ["RACE"]
    assert diagnostics["phase5_shadow_changes_portfolio_action"] is False


def test_w5_future_review_eligibility_uses_existing_mode_without_watch_rewrite():
    integrated = _integrated(_phase5(eligible_horizon=20))
    row = next(
        item for item in integrated["packets"][0]["evidence"]
        if item["family"] == "confidence"
        and item["payload"].get("context_type") == "phase5_confidence_shadow_governance_v1"
    )
    assert row["integration_mode"] == "eligible_after_promotion_review"
    assert row["payload"]["eligible_horizons_for_separate_promotion_review"] == [20]
    assert row["payload"]["insufficient_evidence_horizons"] == [5, 40, 60]
    assert row["payload"]["production_change_performed"] is False
    assert row["payload"]["changes_portfolio_action"] is False


def test_w5_refuses_to_silently_treat_a_production_change_as_shadow():
    with pytest.raises(IntegratedDecisionEvidenceError, match="phase5_unpromoted_guard_invalid"):
        _integrated(_phase5(production_change=True))
