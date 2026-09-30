from __future__ import annotations

import json

import pandas as pd

from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.integrated_evidence import (
    INTEGRATED_STAGE,
    PATH_CONTEXT_TYPE,
    build_scanner_path_state,
    integrate_current_packet_set,
)
from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch


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


def test_integrated_packet_adds_phase4_context_without_new_directional_votes():
    base_packet = build_input_packet(
        symbol="RACE",
        as_of=SCANNER_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[_selection("RACE", SNAPSHOT, SCANNER_TIME), _timing("RACE", SCANNER_TIME)],
    )
    base = {
        "schema_version": "decision_current_packet_set_7a_v1",
        "phase": "7A-current-orchestration",
        "snapshot_id": SNAPSHOT,
        "as_of": SCANNER_TIME,
        "daily_as_of": "2026-09-30",
        "packet_count": 1,
        "timing_claim_count": 1,
        "symbols_with_timing_claims": 1,
        "packets": [base_packet],
        "semantics": {},
        "validation": {"research_only": True},
    }
    history = pd.DataFrame([
        {"date": "2026-09-22", "symbol": "RACE", "rs3m": 0.18, "observation_type": "observed_scanner"},
        {"date": "2026-09-29", "symbol": "RACE", "rs3m": 0.01, "observation_type": "observed_scanner"},
    ])
    phase4 = {
        "phase": "4_confidence_vnext_empirical_research",
        "config": {"evidence_version": "phase4_confidence_research_v1"},
        "current": {
            "rows": [{
                "symbol": "RACE",
                "horizon_sessions": 5,
                "selection": {
                    "state": "robust",
                    "direction": "positive",
                    "N": 100,
                    "probability_advantage_vs_baseline": 0.04,
                },
                "timing": {"state": "robust_claim", "direction": "positive"},
                "risk": {
                    "state": "elevated",
                    "features": [{"feature": "drawdown", "level": "high", "direction": "higher_is_riskier"}],
                },
                "data_quality": {"selection": {"state": "proxy_complete"}},
                "model_agreement": {"state": "conflict", "conflicts": ["positive_return_claim_vs_elevated_downside_risk"]},
                "regime": {"state": "unknown_unvalidated", "counted_as_model_vote": False},
            }]
        },
    }
    integrated = integrate_current_packet_set(
        packet_set=base,
        daily=_daily(),
        history=history,
        phase4_report=phase4,
        finalized_at=FINAL_TIME,
    )
    packet = integrated["packets"][0]
    assert packet["as_of"] == FINAL_TIME
    assert packet["orchestration_stage"] == INTEGRATED_STAGE
    families = [row["family"] for row in packet["evidence"]]
    assert "probability" in families
    assert families.count("risk") == 2
    assert "confidence" in families
    assert integrated["semantics"]["phase2_phase3_phase4_outputs_consumed"] is True
    assert integrated["semantics"]["scanner_path_state_is_directional_vote"] is False

    for row in packet["evidence"]:
        if row["family"] in {"probability", "risk", "confidence"}:
            serialized = json.dumps(row["payload"], sort_keys=True)
            assert '"direction"' not in serialized
            assert '"stance"' not in serialized
            assert '"vote"' not in serialized


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
