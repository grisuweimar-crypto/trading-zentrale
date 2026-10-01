from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.reports.confidence_snapshot_identity import stamp_current_snapshot_identity


def test_w4_stamps_exact_snapshot_identity_without_changing_confidence_rows(tmp_path):
    latest = tmp_path / "latest.csv"
    pd.DataFrame([
        {
            "symbol": "RACE",
            "as_of": "2026-09-30",
            "snapshot_id": "snap-exact",
            "generated_at": "2026-09-30T20:00:00+00:00",
        },
        {
            "symbol": "DIS",
            "as_of": "2026-09-30",
            "snapshot_id": "snap-exact",
            "generated_at": "2026-09-30T20:00:00+00:00",
        },
    ]).to_csv(latest, index=False)
    output = tmp_path / "phase4.json"
    rows = [{"as_of": "2026-09-30", "symbol": "RACE", "horizon_sessions": 5}]
    result = {"current": {"as_of": "2026-09-30", "rows": rows.copy()}}

    stamp_current_snapshot_identity(result, latest, output)

    assert result["current"]["snapshot_id"] == "snap-exact"
    assert result["current"]["generated_at"] == "2026-09-30T20:00:00+00:00"
    assert result["current"]["rows"] == rows
    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted["current"]["snapshot_id"] == "snap-exact"


def test_w4_refuses_mixed_snapshot_identity(tmp_path):
    latest = tmp_path / "latest.csv"
    pd.DataFrame([
        {
            "symbol": "RACE",
            "as_of": "2026-09-30",
            "snapshot_id": "snap-a",
            "generated_at": "2026-09-30T20:00:00+00:00",
        },
        {
            "symbol": "DIS",
            "as_of": "2026-09-30",
            "snapshot_id": "snap-b",
            "generated_at": "2026-09-30T20:00:00+00:00",
        },
    ]).to_csv(latest, index=False)
    output = tmp_path / "phase4.json"
    result = {"current": {"as_of": "2026-09-30", "rows": []}}

    with pytest.raises(ValueError, match="phase4_snapshot_id_not_unique"):
        stamp_current_snapshot_identity(result, latest, output)
