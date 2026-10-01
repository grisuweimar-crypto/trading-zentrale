from scanner.reports.selection_timing import HORIZONS
from scanner.research.decision_layer.input_contract import build_input_packet, validate_input_packet
from scanner.research.decision_layer.phase2_probability import build_phase2_probability_rows


AS_OF = "2026-10-01T18:00:00+02:00"


def _stats(probability=0.61):
    return {
        "N": 42,
        "shrunk_positive_peer_excess_probability": probability,
        "baseline_positive_peer_excess_rate": 0.50,
        "probability_advantage_vs_baseline": probability - 0.50,
    }


def _calibration(*, timing_validation=True):
    horizons = {}
    for horizon in HORIZONS:
        patterns = []
        if horizon == 5:
            patterns = [{
                "pattern": "score_d5_up & rs3m_d5_up",
                "conditions": ["score_d5_up", "rs3m_d5_up"],
                "discovery_direction": "positive",
                "discovery": _stats(0.58),
                "validation": _stats(0.63) if timing_validation else None,
                "validation_sufficient": bool(timing_validation),
                "strong_validation": bool(timing_validation),
            }]
        horizons[str(horizon)] = {
            "selection": {
                "discovery": {"B3": _stats(0.55)},
                "validation": {"B3": _stats(0.60)},
            },
            "timing_patterns": {"patterns": patterns},
        }
    return {
        "phase": "2_probability_calibration",
        "semantics": {
            "research_only": True,
            "score_is_trade_signal": False,
            "r_code_is_trade_signal": False,
            "selection_and_timing_kept_separate": True,
            "timing_candidates_are_frozen_from_phase1b": True,
            "probability_target": "future peer_excess > 0 versus peer benchmark",
        },
        "horizons": horizons,
    }


def _claims():
    selection = {
        "family": "selection",
        "claim_id": "selection:TEST:snap-1",
        "as_of": AS_OF,
        "available_from": AS_OF,
        "source_version": "scanner:v1:schema1",
        "coverage_state": "available",
        "maturity_state": "not_applicable",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": {"score": 55.0, "score_percentile": 0.50, "quality_band": "B3"},
    }
    timing = {
        "family": "timing",
        "claim_id": "timing:TEST:abc123:5T",
        "as_of": AS_OF,
        "available_from": AS_OF,
        "source_version": "phase1b:test",
        "coverage_state": "available",
        "maturity_state": "directional_but_immature",
        "pit_state": "verified",
        "integration_mode": "research_only",
        "payload": {
            "pattern_id": "abc123",
            "pattern": "score_d5_up & rs3m_d5_up",
            "direction": "positive",
            "horizon_sessions": 5,
            "pattern_frozen": True,
            "match_from_pit_features": True,
        },
    }
    return selection, timing


def _forbidden_probability_key(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"direction", "stance", "vote"}:
                return True
            if _forbidden_probability_key(item):
                return True
    elif isinstance(value, list):
        return any(_forbidden_probability_key(item) for item in value)
    return False


def test_w2_valid_7a_packet_contains_selection_timing_and_probability_annotations():
    selection, timing = _claims()
    probability = build_phase2_probability_rows(
        symbol="TEST",
        selection=selection,
        timing=[timing],
        calibration=_calibration(),
        packet_as_of=AS_OF,
    )
    packet = build_input_packet(
        symbol="TEST",
        as_of=AS_OF,
        source_snapshot_id="snap-1",
        evidence=[selection, timing, *probability],
    )
    validated = validate_input_packet(packet)

    families = [row["family"] for row in validated["evidence"]]
    assert "selection" in families
    assert "timing" in families
    assert "probability" in families

    probability_rows = [row for row in validated["evidence"] if row["family"] == "probability"]
    selection_probability = [row for row in probability_rows if "phase2-selection" in row["claim_id"]]
    timing_probability = [row for row in probability_rows if "phase2-timing" in row["claim_id"]]
    assert len(selection_probability) == len(HORIZONS)
    assert len(timing_probability) == 1
    assert all(row["claim_ref"] == selection["claim_id"] for row in selection_probability)
    assert timing_probability[0]["claim_ref"] == timing["claim_id"]
    assert timing_probability[0]["payload"]["pattern"] == timing["payload"]["pattern"]
    assert all(not _forbidden_probability_key(row["payload"]) for row in probability_rows)


def test_w2_unvalidated_timing_probability_stays_limited_and_non_directional():
    selection, timing = _claims()
    probability = build_phase2_probability_rows(
        symbol="TEST",
        selection=selection,
        timing=[timing],
        calibration=_calibration(timing_validation=False),
        packet_as_of=AS_OF,
    )
    row = next(row for row in probability if "phase2-timing" in row["claim_id"])
    assert row["claim_ref"] == timing["claim_id"]
    assert row["coverage_state"] == "limited"
    assert row["maturity_state"] == "not_yet_mature"
    assert row["payload"]["calibration_split"] == "discovery_only"
    assert row["payload"]["annotation_is_directional_vote"] is False
    assert not _forbidden_probability_key(row["payload"])
