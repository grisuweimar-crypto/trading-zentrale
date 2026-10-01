from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.integrated_evidence import (
    INTEGRATED_STAGE,
    PATH_CONTEXT_TYPE,
    IntegratedDecisionEvidenceError,
    build_scanner_path_state,
    integrate_current_packet_set,
)
from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.universal_stance import compute_universal_stance


SNAPSHOT = "snapshot-current"
SCANNER_TIME = "2026-09-30T20:00:00+00:00"
FINAL_TIME = "2026-09-30T21:00:00+00:00"


def _selection(symbol: str, snapshot: str, as_of: str) -> dict[str, object]:
    return {
        "family": "selection",
        "claim_id": f"selection:{symbol}:{snapshot}",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "scanner:v1:research_views_v1",
        "coverage_state": "available",
        "maturity_state": "not_applicable",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": {"score": 30.0, "score_percentile": 0.1, "quality_band": "B5"},
    }


def _timing(symbol: str, as_of: str, suffix: str = "positive") -> dict[str, object]:
    return {
        "family": "timing",
        "claim_id": f"timing:{symbol}:{suffix}:5T",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "phase1b_frozen_patterns_v1:test",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": suffix,
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
            "direction": "positive",
        },
    }


def _probability(symbol: str, selection_claim_id: str, as_of: str) -> dict[str, object]:
    return {
        "family": "probability",
        "claim_id": f"probability:{symbol}:selection:5T",
        "claim_ref": selection_claim_id,
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "phase2_probability_calibration:test",
        "coverage_state": "available",
        "maturity_state": "robust",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {"horizon_sessions": 5, "state": "robust", "probability": 0.62},
    }


def _phase3_risk(symbol: str, snapshot: str, as_of: str) -> dict[str, object]:
    return {
        "family": "risk",
        "claim_id": f"risk:{symbol}:phase3:{snapshot}",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": "phase3_risk_vnext_current:v1:research_views_v1",
        "coverage_state": "available",
        "maturity_state": "not_yet_mature",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": {"aggregate_risk": 41.0, "risk_is_directional_vote": False},
    }


def _path_row(symbol: str, as_of: str) -> dict[str, object]:
    return {
        "family": "risk",
        "claim_id": f"risk:{symbol}:scanner-path:2026-09-30",
        "as_of": as_of,
        "available_from": as_of,
        "source_version": PATH_CONTEXT_TYPE,
        "coverage_state": "available",
        "maturity_state": "not_yet_mature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "context_type": PATH_CONTEXT_TYPE,
            "review_state": "profit_protection_review",
            "sequence_state": "post_overextension_decay",
            "review_is_trade_decision": False,
            "execution_allowed": False,
        },
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
                    "score": 24.74,
                    "rank": 77.0,
                    "r_code": "R3",
                    "rs3m": -0.00675,
                    "trend200": 0.0769,
                    "close": 393.27,
                    "currency": "USD",
                },
                "dynamics": {
                    "score_delta_5d": -1.63,
                    "rank_delta_5d": 21.0,
                    "rs3m_delta_5d": -0.1326,
                    "trend200_delta_5d": -0.0556,
                    "r_code_previous": "R4",
                },
            }
        },
    }


def _phase4(snapshot: str = SNAPSHOT) -> dict[str, object]:
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
            "snapshot_id": snapshot,
            "as_of": "2026-09-30",
            "generated_at": SCANNER_TIME,
            "rows": [{
                "as_of": "2026-09-30",
                "symbol": "RACE",
                "horizon_sessions": 5,
                "selection": {"state": "robust", "direction": "positive", "N": 100},
                "timing": {"state": "robust_claim", "direction": "positive"},
                "risk": {
                    "state": "elevated",
                    "features": [{
                        "feature": "drawdown",
                        "level": "high",
                        "direction": "higher_is_riskier",
                    }],
                },
                "data_quality": {"selection": {"state": "proxy_complete"}},
                "model_agreement": {
                    "state": "conflict",
                    "conflicts": ["positive_return_claim_vs_elevated_downside_risk"],
                },
                "regime": {"state": "unknown_unvalidated", "counted_as_model_vote": False},
            }],
        },
    }


def _base_packet_set() -> dict[str, object]:
    selection = _selection("RACE", SNAPSHOT, SCANNER_TIME)
    packet = build_input_packet(
        symbol="RACE",
        as_of=SCANNER_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[
            selection,
            _timing("RACE", SCANNER_TIME),
            _probability("RACE", str(selection["claim_id"]), SCANNER_TIME),
            _phase3_risk("RACE", SNAPSHOT, SCANNER_TIME),
        ],
    )
    return {
        "schema_version": "decision_current_packet_set_7a_v1",
        "phase": "7A-current-orchestration",
        "snapshot_id": SNAPSHOT,
        "as_of": SCANNER_TIME,
        "daily_as_of": "2026-09-30",
        "packet_count": 1,
        "timing_claim_count": 1,
        "symbols_with_timing_claims": 1,
        "probability_claim_count": 1,
        "symbols_with_probability_claims": 1,
        "risk_claim_count": 1,
        "symbols_with_risk_claims": 1,
        "packets": [packet],
        "semantics": {},
        "validation": {"research_only": True},
    }


def _history() -> pd.DataFrame:
    return pd.DataFrame([
        {"date": "2026-09-22", "symbol": "RACE", "rs3m": 0.18, "observation_type": "observed_scanner"},
        {"date": "2026-09-29", "symbol": "RACE", "rs3m": 0.01, "observation_type": "observed_scanner"},
    ])


def test_ferrari_like_path_memory_survives_after_current_overextension_flag_clears():
    history = pd.DataFrame([
        {"date": "2026-09-18", "symbol": "RACE", "rs3m": 0.12, "observation_type": "observed_scanner"},
        {"date": "2026-09-21", "symbol": "RACE", "rs3m": 0.18, "observation_type": "observed_scanner"},
        {"date": "2026-09-22", "symbol": "RACE", "rs3m": 0.17, "observation_type": "observed_scanner"},
        {"date": "2026-09-23", "symbol": "RACE", "rs3m": 0.14, "observation_type": "observed_scanner"},
        {"date": "2026-09-24", "symbol": "RACE", "rs3m": 0.10, "observation_type": "observed_scanner"},
        {"date": "2026-09-25", "symbol": "RACE", "rs3m": 0.07, "observation_type": "observed_scanner"},
        {"date": "2026-09-28", "symbol": "RACE", "rs3m": 0.04, "observation_type": "observed_scanner"},
        {"date": "2026-09-29", "symbol": "RACE", "rs3m": 0.01, "observation_type": "observed_scanner"},
    ])
    state = build_scanner_path_state(
        symbol="RACE",
        daily_symbol=_daily()["symbols"]["RACE"],
        history=history,
        current_date="2026-09-30",
    )
    assert state["overextension_active"] is False
    assert state["recent_overextension"] is True
    assert state["last_overextension_date"] == "2026-09-22"
    assert state["sequence_state"] == "post_overextension_decay"
    assert state["review_state"] == "profit_protection_review"
    assert state["review_is_trade_decision"] is False
    assert state["deterioration"]["score_falling_5t"] is True
    assert state["deterioration"]["rank_worsening_5t"] is True
    assert state["deterioration"]["rs3m_falling_5t"] is True
    assert state["deterioration"]["trend200_falling_5t"] is True
    assert state["deterioration"]["r_code_downgrade"] is True


def test_w4_integrated_packet_preserves_w2_w3_and_adds_confidence_only():
    base = _base_packet_set()
    before = base["packets"][0]
    base_stance = compute_universal_stance(before)

    integrated = integrate_current_packet_set(
        packet_set=base,
        daily=_daily(),
        history=_history(),
        phase4_report=_phase4(),
        finalized_at=FINAL_TIME,
    )
    packet = integrated["packets"][0]
    assert packet["as_of"] == FINAL_TIME
    assert packet["orchestration_stage"] == INTEGRATED_STAGE

    families = [row["family"] for row in packet["evidence"]]
    assert families.count("probability") == 1
    assert families.count("risk") == 2  # Phase 3 + scanner path state; no Phase-4 replacement risk.
    assert families.count("confidence") == 1

    probability = next(row for row in packet["evidence"] if row["family"] == "probability")
    assert probability["source_version"] == "phase2_probability_calibration:test"
    phase3 = next(row for row in packet["evidence"] if row["claim_id"].startswith("risk:RACE:phase3:"))
    assert phase3["source_version"].startswith("phase3_risk_vnext_current")

    confidence = next(row for row in packet["evidence"] if row["family"] == "confidence")
    assert confidence["claim_ref"] == "selection:RACE:snapshot-current"
    assert confidence["payload"]["phase4_role"] == "ordinal_reliability_context_not_directional_vote"
    serialized = json.dumps(confidence["payload"], sort_keys=True)
    for forbidden in ('"direction"', '"stance"', '"vote"', '"attractiveness"'):
        assert forbidden not in serialized

    with_confidence = compute_universal_stance(packet)
    assert with_confidence["universal_stance"] == base_stance["universal_stance"]
    assert with_confidence["evidence_structure"]["known_directional_claim_ids"] == base_stance["evidence_structure"]["known_directional_claim_ids"]
    assert with_confidence["evidence_structure"]["annotation_count"] == base_stance["evidence_structure"]["annotation_count"] + 1
    assert with_confidence["semantics"]["probability_and_confidence_count_as_votes"] is False

    assert integrated["phase4_confidence_claim_count"] == 1
    assert integrated["validation"]["phase4_same_snapshot_verified"] is True
    assert integrated["semantics"]["phase2_probability_source_preserved"] is True
    assert integrated["semantics"]["phase3_risk_source_preserved"] is True
    assert integrated["semantics"]["confidence_is_directional_vote"] is False
    assert integrated["semantics"]["confidence_encodes_attractiveness"] is False


def test_w4_rejects_phase4_from_another_snapshot():
    with pytest.raises(IntegratedDecisionEvidenceError, match="phase4_snapshot_mismatch"):
        integrate_current_packet_set(
            packet_set=_base_packet_set(),
            daily=_daily(),
            history=_history(),
            phase4_report=_phase4(snapshot="stale-snapshot"),
            finalized_at=FINAL_TIME,
        )


def test_orchestrator_uses_final_same_snapshot_revision_once_and_surfaces_path_review():
    old_time = "2026-09-29T20:00:00+00:00"
    old = build_input_packet(
        symbol="RACE",
        as_of=old_time,
        source_snapshot_id="snapshot-old",
        evidence=[_timing("RACE", old_time, "old-positive")],
    )
    preliminary = build_input_packet(
        symbol="RACE",
        as_of=SCANNER_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[_timing("RACE", SCANNER_TIME, "preliminary-positive")],
    )
    final = build_input_packet(
        symbol="RACE",
        as_of=FINAL_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[_timing("RACE", SCANNER_TIME, "final-positive"), _path_row("RACE", SCANNER_TIME)],
    )
    final["orchestration_stage"] = INTEGRATED_STAGE

    position_book = {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-book",
        "as_of": "2026-09-30T20:30:00+00:00",
        "positions": [{
            "schema_version": "decision_position_snapshot_v1",
            "symbol": "RACE",
            "source_snapshot_id": "private-race",
            "as_of": "2026-09-30T20:30:00+00:00",
            "position_state": "long",
            "quantity": 1,
            "average_entry_price": 350.0,
            "current_price": 393.27,
            "currency": "USD",
        }],
    }

    watch, diagnostics = build_orchestrated_depot_watch(
        _daily(),
        position_book,
        [old, preliminary, final],
    )
    assert diagnostics["decision_as_of"] == "2026-09-30T21:00:00+00:00"
    assert diagnostics["path_review_symbols"] == ["RACE"]
    row = watch["rows"][0]
    assert row["availability"] == "decision_available"
    assert row["decision"]["transition_status"] == "bootstrap_confirmed"
    assert row["decision"]["portfolio_action_state"] == "HOLD"
    assert row["attention_required"] is True
    assert row["path_review"]["review_state"] == "profit_protection_review"
    assert row["decision"]["path_sequence_state"] == "post_overextension_decay"
    assert row["decision"]["path_review_is_trade_decision"] is False
