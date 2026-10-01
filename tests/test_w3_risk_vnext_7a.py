from __future__ import annotations

from pathlib import Path

import pandas as pd

from scanner.research.decision_layer.input_contract import build_input_packet, validate_input_packet
from scanner.research.decision_layer.phase3_risk import build_phase3_risk_row
from scanner.research.decision_layer.universal_stance import compute_universal_stance


ROOT = Path(__file__).resolve().parents[1]
AS_OF = "2026-10-01T18:00:00+02:00"
SNAPSHOT = "snap-w3"


def _base_row(**extra):
    row = {
        "symbol": "TEST",
        "snapshot_id": SNAPSHOT,
        "generated_at": AS_OF,
        "scoring_version": "v1",
        "schema_version": "research_views_v1",
    }
    row.update(extra)
    return row


def _selection():
    return {
        "family": "selection",
        "claim_id": "selection:TEST:snap-w3",
        "as_of": AS_OF,
        "available_from": AS_OF,
        "source_version": "scanner:v1:research_views_v1",
        "coverage_state": "available",
        "maturity_state": "not_applicable",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": {"score": 50.0, "score_percentile": 0.5, "quality_band": "B3"},
    }


def _timing():
    return {
        "family": "timing",
        "claim_id": "timing:TEST:w3:5T",
        "as_of": AS_OF,
        "available_from": AS_OF,
        "source_version": "phase1b:test",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": "w3",
            "pattern": "score_d5_up",
            "direction": "positive",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
        },
    }


def _contains_forbidden_vote(value):
    if isinstance(value, dict):
        return any(
            key in {"direction", "stance", "vote"} or _contains_forbidden_vote(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_forbidden_vote(item) for item in value)
    return False


def test_w3_risk_row_transports_existing_values_and_preserves_volatility_lock():
    row = _base_row(
        risk=41.65,
        volatility=0.409,
        drawdown=0.616,
        debt_ratio=10.389,
        atr_pct=0.071,
        persistent_strong_volatility=True,
        risk_class="HIGH",
        volatility_lock="BLOCK",
        volatility_gate="LOCKED",
        risk_quality_status="VALID",
    )
    risk = build_phase3_risk_row(row=row, expected_snapshot_id=SNAPSHOT)
    assert risk is not None
    payload = risk["payload"]

    assert risk["family"] == "risk"
    assert risk["integration_mode"] == "production_existing"
    assert payload["aggregate_risk"] == 41.65
    assert payload["volatility"] == 0.409
    assert payload["drawdown"] == 0.616
    assert payload["debt_ratio"] == 10.389
    assert payload["atr_pct"] == 0.071
    assert payload["persistent_strong_volatility"] is True
    assert payload["risk_class"] == "HIGH"
    assert payload["volatility_lock"] == "BLOCK"
    assert payload["volatility_gate"] == "LOCKED"
    assert payload["risk_quality_status"] == "VALID"
    assert payload["missing_values_backfilled"] is False
    assert payload["risk_is_directional_vote"] is False
    assert not _contains_forbidden_vote(payload)


def test_w3_missing_risk_values_are_not_synthesized():
    assert build_phase3_risk_row(
        row=_base_row(),
        expected_snapshot_id=SNAPSHOT,
    ) is None

    risk = build_phase3_risk_row(
        row=_base_row(risk=37.5),
        expected_snapshot_id=SNAPSHOT,
    )
    assert risk is not None
    payload = risk["payload"]
    assert payload["aggregate_risk"] == 37.5
    assert "volatility" not in payload
    assert "drawdown" not in payload
    assert "atr_pct" not in payload
    assert "risk_class" not in payload
    assert risk["coverage_state"] == "available"


def test_w3_risk_family_is_accepted_by_existing_7a_validator():
    risk = build_phase3_risk_row(
        row=_base_row(risk=44.0, volatility=0.30, drawdown=0.20),
        expected_snapshot_id=SNAPSHOT,
    )
    assert risk is not None
    packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id=SNAPSHOT,
        evidence=[_selection(), _timing(), risk],
    )
    validated = validate_input_packet(packet)
    families = [row["family"] for row in validated["evidence"]]
    assert families == ["selection", "timing", "risk"]


def test_w3_risk_does_not_change_7d_directional_stance():
    base_packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id=SNAPSHOT,
        evidence=[_selection(), _timing()],
    )
    risk = build_phase3_risk_row(
        row=_base_row(risk=99.0, volatility=0.99, volatility_lock="BLOCK"),
        expected_snapshot_id=SNAPSHOT,
    )
    assert risk is not None
    risk_packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id=SNAPSHOT,
        evidence=[_selection(), _timing(), risk],
    )

    base = compute_universal_stance(base_packet)
    with_risk = compute_universal_stance(risk_packet)
    assert with_risk["universal_stance"] == base["universal_stance"]
    assert (
        with_risk["evidence_structure"]["known_directional_claim_ids"]
        == base["evidence_structure"]["known_directional_claim_ids"]
    )
    assert with_risk["evidence_structure"]["context_count"] == base["evidence_structure"]["context_count"] + 1
    assert with_risk["semantics"]["risk_and_elliott_count_as_votes"] is False


def test_w3_real_current_snapshot_uses_only_fields_that_exist_upstream():
    latest = pd.read_csv(ROOT / "artifacts/research/latest_scanner.csv")
    candidate = latest.loc[latest["risk"].notna()].iloc[0].to_dict()
    risk = build_phase3_risk_row(
        row=candidate,
        expected_snapshot_id=str(candidate["snapshot_id"]),
    )
    assert risk is not None
    payload = risk["payload"]

    assert payload["aggregate_risk"] == candidate["risk"]
    if pd.notna(candidate.get("volatility")):
        assert payload["volatility"] == candidate["volatility"]
    if pd.notna(candidate.get("drawdown")):
        assert payload["drawdown"] == candidate["drawdown"]
    if pd.notna(candidate.get("debt_ratio")):
        assert payload["debt_ratio"] == candidate["debt_ratio"]

    # The current authoritative snapshot does not publish these planned W3
    # fields. Their absence must remain explicit rather than being reconstructed.
    assert "atr_pct" not in latest.columns
    assert "risk_class" not in latest.columns
    assert "atr_pct" not in payload
    assert "risk_class" not in payload
