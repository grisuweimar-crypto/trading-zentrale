from __future__ import annotations

import pandas as pd
import pytest

from scanner.research.decision_layer.input_contract import build_input_packet
from scanner.research.decision_layer.integrated_evidence import (
    IntegratedDecisionEvidenceError,
    integrate_current_packet_set,
)


SNAPSHOT = "1596e19c-5d01-47e0-b4f9-c85e54d1de7b"
SCANNER_TIME = "2026-09-30T19:58:04.926083+00:00"
DAILY_PUBLISH_TIME = "2026-09-30T19:59:21.494424+00:00"
FINAL_TIME = "2026-10-01T10:00:00+00:00"


def _packet_set() -> dict[str, object]:
    selection = {
        "family": "selection",
        "claim_id": f"selection:RACE:{SNAPSHOT}",
        "as_of": SCANNER_TIME,
        "available_from": SCANNER_TIME,
        "source_version": "scanner:v1:research_views_v1",
        "coverage_state": "available",
        "maturity_state": "not_applicable",
        "pit_state": "verified",
        "integration_mode": "production_existing",
        "payload": {"score": 24.74, "score_percentile": 0.5, "quality_band": "R3"},
    }
    packet = build_input_packet(
        symbol="RACE",
        as_of=DAILY_PUBLISH_TIME,
        source_snapshot_id=SNAPSHOT,
        evidence=[selection],
    )
    return {
        "schema_version": "decision_current_packet_set_7a_v1",
        "phase": "7A-current-orchestration",
        "snapshot_id": SNAPSHOT,
        "as_of": DAILY_PUBLISH_TIME,
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
        "generated_at": DAILY_PUBLISH_TIME,
        "symbols": {
            "RACE": {
                "current": {"rs3m": 0.01, "r_code": "R3"},
                "dynamics": {},
            }
        },
    }


def _phase4(generated_at: str = SCANNER_TIME) -> dict[str, object]:
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
            "generated_at": generated_at,
            "rows": [
                {
                    "as_of": "2026-09-30",
                    "symbol": "RACE",
                    "horizon_sessions": 5,
                    "selection": {"state": "robust"},
                    "timing": {"state": "unknown"},
                    "risk": {"state": "unknown"},
                    "data_quality": {},
                    "model_agreement": {},
                    "regime": {},
                }
            ],
        },
    }


def test_w10_distinguishes_raw_scanner_time_from_later_daily_publication() -> None:
    integrated = integrate_current_packet_set(
        packet_set=_packet_set(),
        daily=_daily(),
        history=pd.DataFrame(),
        phase4_report=_phase4(),
        finalized_at=FINAL_TIME,
    )

    assert integrated["validation"]["scanner_generated_at_utc"] == SCANNER_TIME
    assert integrated["validation"]["daily_research_generated_at_utc"] == DAILY_PUBLISH_TIME
    packet = integrated["packets"][0]
    assert packet["scanner_generated_at"] == SCANNER_TIME
    assert packet["daily_research_generated_at"] == DAILY_PUBLISH_TIME
    confidence = next(row for row in packet["evidence"] if row["family"] == "confidence")
    assert confidence["as_of"] == SCANNER_TIME
    assert confidence["available_from"] == FINAL_TIME


def test_w10_keeps_strict_phase4_raw_scanner_identity_guard() -> None:
    with pytest.raises(IntegratedDecisionEvidenceError, match="phase4_scanner_generation_mismatch"):
        integrate_current_packet_set(
            packet_set=_packet_set(),
            daily=_daily(),
            history=pd.DataFrame(),
            phase4_report=_phase4(generated_at=DAILY_PUBLISH_TIME),
            finalized_at=FINAL_TIME,
        )
