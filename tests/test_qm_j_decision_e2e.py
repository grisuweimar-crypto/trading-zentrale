from __future__ import annotations

from copy import deepcopy

import pytest

from scanner.research.governance import qm_j_decision_e2e as qmj

SNAPSHOT = "snapshot-qmj-e2e"
DECISION = "2026-10-01T10:30:00+00:00"


def _daily():
    symbols = {}
    for symbol in ("AAA", "BBB"):
        symbols[symbol] = {
            "current": {
                "name": symbol,
                "score": 30.0,
                "r_code": "R4",
                "close": 100.0,
                "currency": "USD",
            }
        }
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": SNAPSHOT,
        "source_snapshot_id": SNAPSHOT,
        "as_of": "2026-10-01",
        "generated_at": DECISION,
        "universe_size": 2,
        "symbols": symbols,
    }


def _packet(symbol: str, direction: str):
    return {
        "schema_version": "decision_layer_input_contract_v1",
        "symbol": symbol,
        "as_of": DECISION,
        "source_snapshot_id": SNAPSHOT,
        "evidence": [{
            "family": "selection",
            "claim_id": f"selection:{symbol}:{SNAPSHOT}",
            "as_of": DECISION,
            "available_from": DECISION,
            "source_version": "test-selection-v1",
            "coverage_state": "available",
            "maturity_state": "not_applicable",
            "pit_state": "verified",
            "integration_mode": "production_existing",
            "payload": {"direction": direction, "score": 30.0},
        }],
        "research_only": True,
        "productive_integration_enabled": False,
    }


def _packets():
    return [_packet("AAA", "positive"), _packet("BBB", "negative")]


def _manifest():
    available = "2026-10-01T10:29:00+00:00"
    common = {
        "status": "available",
        "available_from": available,
        "availability_source": "test",
        "snapshot_identity_state": "verified",
    }
    return {
        "schema_version": "decision_snapshot_orchestration_w10_v1",
        "snapshot_id": SNAPSHOT,
        "snapshot_as_of": "2026-10-01",
        "status": "sealed",
        "started_at": available,
        "pre_7a_frozen_at": DECISION,
        "sealed_at": "2026-10-01T10:31:00+00:00",
        "stages": {
            "scanner_daily_research": deepcopy(common),
            "phase2_probability": deepcopy(common),
            "phase3_risk": deepcopy(common),
            "phase4_confidence": deepcopy(common),
            "phase5_governance": deepcopy(common),
            "phase6_elliott": {
                "status": "not_supplied",
                "resolved_at": DECISION,
                "availability_source": None,
                "snapshot_identity_state": "not_applicable",
                "decision_effect": "none",
                "missing_is_neutral_evidence": False,
            },
            "final_7a": {**deepcopy(common), "available_from": DECISION},
            "phase7a_archive": {**deepcopy(common), "available_from": "2026-10-01T10:31:00+00:00"},
        },
        "guards": {
            "final_7a_requires_all_upstream_stages_resolved": True,
            "runtime_stage_available_from_is_recorded_not_supplied": True,
            "later_evidence_backdating_allowed": False,
            "phase6_missing_is_neutral_evidence": False,
            "private_position_data_persisted": False,
        },
        "downstream_contract": {
            "private_runner": "scripts/run_depot_watch_orchestrated.py",
            "chain": ["7D", "7E", "7F", "7G", "7H"],
            "requires_same_snapshot_id": True,
            "phase6_effect_boundary": "7F_review_context_only",
            "private_position_data_persisted": False,
        },
    }


def _plan():
    return qmj.DecisionE2EPlan(
        frozen_against_main_commit="2" * 40,
        placebo_seed="placebo-seed",
        destroyed_seed="destroy-seed",
        trigger_status="PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED",
        nontrigger_status="NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG",
    )


def test_synthetic_position_book_is_private_free_and_snapshot_complete():
    book = qmj.build_synthetic_long_position_book(_daily(), _packets())
    assert book["source_snapshot_id"].startswith("qm-j-synthetic-long:")
    assert [row["symbol"] for row in book["positions"]] == ["AAA", "BBB"]
    assert all(row["position_state"] == "long" for row in book["positions"])
    assert all("current_price" not in row for row in book["positions"])


def test_placebo_sidecar_has_zero_effect_and_destroyed_direction_changes_stance():
    result = qmj.run_falsification(
        daily=_daily(),
        archive_packets=_packets(),
        manifest=_manifest(),
        plan=_plan(),
    )
    assert result["baseline"]["w11_status"] == "passed"
    assert result["placebo_sidecar"]["zero_decision_effect"] is True
    assert result["placebo_sidecar"]["triggered"] is False
    destroyed = result["destroyed_information"]
    assert destroyed["effective"] is True
    assert destroyed["changed_directional_claims"] == 2
    assert destroyed["affected_universal_stance_change_count"] == 2
    assert destroyed["w11_status"] == "passed"
    assert destroyed["triggered"] is False
    assert result["status"] == "NEGATIVE_CONTROLS_NOT_SIMILARLY_STRONG"
    assert result["promotion_performed"] is False


def test_effective_destruction_with_zero_stance_response_triggers_falsification(monkeypatch):
    original = qmj._destroy_current_directions

    def fake_destroy(packets, *, snapshot_id, seed):
        unchanged = [deepcopy(dict(packet)) for packet in packets]
        return unchanged, {
            "effective": True,
            "reason": None,
            "eligible_directional_claims": 2,
            "changed_directional_claims": 2,
            "changed_directional_symbols": ["AAA", "BBB"],
            "control_artifact_hash": "a" * 64,
            "source_content_hash": "b" * 64,
            "control_content_hash": "c" * 64,
        }

    monkeypatch.setattr(qmj, "_destroy_current_directions", fake_destroy)
    result = qmj.run_falsification(
        daily=_daily(), archive_packets=_packets(), manifest=_manifest(), plan=_plan()
    )
    assert result["destroyed_information"]["triggered"] is True
    assert result["status"] == "PROMOTION_STOP_INVESTIGATION_CAPA_REQUIRED"
    assert result["promotion_blocked_by_qm_j"] is True
    assert result["capa_required"] is True
    monkeypatch.setattr(qmj, "_destroy_current_directions", original)


def test_homogeneous_directions_block_control_instead_of_claiming_pass():
    packets = [_packet("AAA", "positive"), _packet("BBB", "positive")]
    result = qmj.run_falsification(
        daily=_daily(), archive_packets=packets, manifest=_manifest(), plan=_plan()
    )
    assert result["placebo_sidecar"]["zero_decision_effect"] is True
    assert result["destroyed_information"]["effective"] is False
    assert result["status"] == "BLOCKED_CONTROL_INEFFECTIVE"
    assert result["promotion_blocked_by_qm_j"] is False


def test_packet_grid_change_fails_closed():
    with pytest.raises(qmj.NegativeControlError, match="comparison_symbol_grid_changed"):
        qmj._changed_symbols(
            {"AAA": {"stance_state": "positive"}},
            {"BBB": {"stance_state": "negative"}},
            ["stance_state"],
        )
