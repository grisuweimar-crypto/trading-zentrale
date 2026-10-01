import json
from pathlib import Path

from scanner.reports.selection_timing import HORIZONS
from scanner.research.decision_layer.phase2_probability import (
    build_phase2_probability_rows,
    load_probability_calibration,
)


ROOT = Path(__file__).resolve().parents[1]
AS_OF = "2026-10-01T18:00:00+02:00"


def test_real_phase2_artifact_covers_frozen_phase1b_pattern_identities():
    calibration = load_probability_calibration(ROOT / "artifacts/research/probability_calibration_2.json")
    catalog = json.loads((ROOT / "artifacts/research/timing_patterns_1b_frozen.json").read_text(encoding="utf-8"))

    for horizon in HORIZONS:
        frozen = {
            row["pattern"]
            for row in catalog["horizons"][str(horizon)]["frozen_patterns"]
        }
        calibrated = {
            row["pattern"]
            for row in calibration["horizons"][str(horizon)]["timing_patterns"]["patterns"]
        }
        assert frozen <= calibrated


def test_real_phase2_artifact_builds_validator_ready_selection_and_timing_annotations():
    calibration = load_probability_calibration(ROOT / "artifacts/research/probability_calibration_2.json")
    catalog = json.loads((ROOT / "artifacts/research/timing_patterns_1b_frozen.json").read_text(encoding="utf-8"))

    horizon = next(h for h in HORIZONS if catalog["horizons"][str(h)]["frozen_patterns"])
    frozen = catalog["horizons"][str(horizon)]["frozen_patterns"][0]
    selection_cal = calibration["horizons"][str(horizon)]["selection"]
    available_bands = set(selection_cal["validation"]) | set(selection_cal["discovery"])
    band = sorted(available_bands)[0]

    selection = {
        "family": "selection",
        "claim_id": "selection:REAL:snap-real",
        "payload": {"quality_band": band},
    }
    timing = {
        "family": "timing",
        "claim_id": f"timing:REAL:realpattern:{horizon}T",
        "payload": {
            "pattern_id": "realpattern",
            "pattern": frozen["pattern"],
            "horizon_sessions": horizon,
        },
    }
    rows = build_phase2_probability_rows(
        symbol="REAL",
        selection=selection,
        timing=[timing],
        calibration=calibration,
        packet_as_of=AS_OF,
    )

    assert any(row["claim_ref"] == selection["claim_id"] for row in rows)
    assert any(row["claim_ref"] == timing["claim_id"] for row in rows)
    assert all(row["integration_mode"] == "research_only" for row in rows)
