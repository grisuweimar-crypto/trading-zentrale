from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

from scanner.research.elliott_vnext.validation import (
    ValidationConfig,
    block_bootstrap_mean,
)


def _load_script(name: str, filename: str):
    path = Path(__file__).resolve().parents[1] / "scripts" / filename
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


aggregate = _load_script(
    "aggregate_stage4_sufficient_stats",
    "aggregate_elliott_stage4_sufficient_stats.py",
)


def test_daily_sufficient_stats_exactly_match_block_bootstrap_mean() -> None:
    rows = []
    for day_index, day in enumerate(pd.date_range("2026-01-01", periods=35, freq="D")):
        for copy_index in range(1 + (day_index % 3)):
            rows.append(
                {
                    "event_date": day,
                    "metric": (day_index - 10) * 0.01 + copy_index * 0.002,
                }
            )
    frame = pd.DataFrame(rows)
    config = ValidationConfig(bootstrap_reps=250, random_seed=2026)

    expected = block_bootstrap_mean(
        frame,
        metric="metric",
        date_col="event_date",
        horizon=10,
        config=config,
        seed_key=("projection_return", "legacy", "w3", "extension", "minor"),
    )

    daily = {}
    for day, group in frame.groupby("event_date", sort=True):
        daily[pd.Timestamp(day).date().isoformat()] = (
            float(group["metric"].sum()),
            int(len(group)),
        )

    actual = aggregate._bootstrap_daily(
        daily,
        metric="metric",
        horizon=10,
        seed_key=("projection_return", "legacy", "w3", "extension", "minor"),
        config=config,
    )

    expected_interval = expected.pop("mean_95")
    actual_interval = actual.pop("mean_95")
    assert actual == expected
    assert actual_interval is not None
    assert expected_interval is not None
    assert np.allclose(
        np.asarray(actual_interval, dtype=float),
        np.asarray(expected_interval, dtype=float),
        rtol=0.0,
        atol=1e-15,
    )


def test_daily_sufficient_stats_preserve_empty_metric_diagnostics() -> None:
    config = ValidationConfig(bootstrap_reps=100)
    actual = aggregate._bootstrap_daily(
        {},
        metric="signed_forward_return",
        horizon=20,
        seed_key=("route_return", "legacy", "defensive_review", "wave_5_complete", "minor"),
        config=config,
    )

    assert actual["N"] == 0
    assert actual["date_count"] == 0
    assert actual["block_count"] == 0
    assert actual["block_length"] == 40
    assert actual["support_regions"] == 0
    assert actual["mean"] is None
    assert actual["mean_95"] is None
    assert actual["robust_interval_available"] is False
