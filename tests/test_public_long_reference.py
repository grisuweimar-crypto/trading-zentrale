from __future__ import annotations

from scanner.research.decision_layer.depot_watch_orchestrator import build_orchestrated_depot_watch
from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.public_long_reference import (
    DECISION_FIELDS,
    build_public_long_reference,
    public_long_reference_csv,
)


CURRENT_SNAPSHOT = "snapshot-current"
CURRENT_TIME = "2026-10-01T10:30:00+00:00"


def _daily():
    return {
        "schema_version": "daily_research_v1",
        "snapshot_id": CURRENT_SNAPSHOT,
        "source_snapshot_id": CURRENT_SNAPSHOT,
        "as_of": "2026-10-01",
        "generated_at": "2026-10-01T10:20:00+00:00",
        "universe_size": 1,
        "symbols": {
            "TEST": {
                "current": {
                    "name": "Test Corp",
                    "score": 30.0,
                    "r_code": "R4",
                    "close": 100.0,
                    "currency": "USD",
                }
            }
        },
    }


def _packet(snapshot: str, as_of: str):
    return build_input_packet(
        symbol="TEST",
        as_of=as_of,
        source_snapshot_id=snapshot,
        evidence=[
            {
                "family": "selection",
                "claim_id": f"selection:TEST:{snapshot}",
                "as_of": as_of,
                "available_from": as_of,
                "source_version": "scanner:v1:research_views_v1",
                "coverage_state": "available",
                "maturity_state": "not_applicable",
                "pit_state": "verified",
                "integration_mode": "production_existing",
                "payload": {"score": 30.0, "quality_band": "R4"},
            },
            {
                "family": "timing",
                "claim_id": f"timing:TEST:{snapshot}:5T",
                "as_of": as_of,
                "available_from": as_of,
                "source_version": "phase1b_frozen_patterns_v1:test",
                "coverage_state": "available",
                "maturity_state": "directional_but_immature",
                "pit_state": "verified",
                "integration_mode": "research_only",
                "payload": {
                    "pattern_id": f"positive-{snapshot}",
                    "horizon_sessions": 5,
                    "pattern_frozen": True,
                    "match_from_pit_features": True,
                    "direction": "positive",
                },
            },
        ],
    )


def _packets():
    return [
        _packet("snapshot-old", "2026-09-30T10:30:00+00:00"),
        _packet(CURRENT_SNAPSHOT, CURRENT_TIME),
    ]


def _private_minimal_long_book():
    return {
        "schema_version": "decision_depot_position_book_v1",
        "source_snapshot_id": "private-book",
        "as_of": "2026-09-30T14:00:00+00:00",
        "positions": [{
            "schema_version": "decision_position_snapshot_v1",
            "symbol": "TEST",
            "source_snapshot_id": "private-position",
            "as_of": "2026-09-30T14:00:00+00:00",
            "position_state": "long",
            "holding_label": "Private Test Holding",
            "scanner_mapping_state": "exact",
        }],
    }


def test_public_long_reference_matches_private_minimal_long_decision_fields():
    daily = _daily()
    packets = _packets()
    reference = build_public_long_reference(daily, packets)
    private_watch, _ = build_orchestrated_depot_watch(
        daily, _private_minimal_long_book(), packets
    )

    assert reference["snapshot_id"] == CURRENT_SNAPSHOT
    assert reference["row_count"] == 1
    assert reference["position_assumption"]["actual_holdings_included"] is False
    assert reference["position_assumption"]["private_position_data_included"] is False
    assert reference["semantics"]["decision_logic_changed"] is False

    public_row = reference["rows"][0]
    private_row = private_watch["rows"][0]
    assert public_row["availability"] == private_row["availability"] == "decision_available"
    assert public_row["presentation_group"] == private_row["presentation_group"]
    assert public_row["attention_required"] == private_row["attention_required"]
    for field in DECISION_FIELDS:
        if field in public_row["decision"]:
            assert public_row["decision"][field] == private_row["decision"].get(field)


def test_public_long_reference_csv_is_compact_and_actionable_without_private_data():
    reference = build_public_long_reference(_daily(), _packets())
    text = public_long_reference_csv(reference)
    assert text.startswith("symbol,name,score,r_code,close,currency,")
    assert "TEST,Test Corp,30.0,R4,100.0,USD" in text
    assert "HOLD" in text
    assert "Private Test Holding" not in text
    assert "private-book" not in text
