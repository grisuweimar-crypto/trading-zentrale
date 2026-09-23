from __future__ import annotations

import json

import pandas as pd
import pytest

from scanner.reports import probability_calibration as module
from scanner.reports.probability_calibration import (
    Phase2Config,
    _beta_shrinkage,
    _probability_stats,
    _wilson_interval,
    analyze,
)


def _synthetic():
    dates = pd.bdate_range("2026-04-15", periods=150)
    scanner = []
    prices = []
    specs = (("A", 75.0, 0.8), ("B", 50.0, 0.3), ("C", 25.0, -0.1), ("D", 10.0, -0.25))
    for symbol, base, slope in specs:
        for i, day in enumerate(dates[:115]):
            scanner.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "name": symbol,
                    "score": base + 0.02 * i,
                    "opportunity": 35.0 + 0.15 * i + base / 10.0,
                    "risk": 55.0 - 0.08 * i - base / 20.0,
                    "rs3m": -0.2 + i * 0.008 + slope / 10.0,
                    "trend200": -0.1 + i * 0.004 + slope / 20.0,
                    "cycle": float((i * 3 + int(base)) % 100),
                    "currency": "USD",
                    "r_code": "R3" if i < 55 else "R4",
                    "observation_type": "observed_scanner",
                }
            )
        for i, day in enumerate(dates):
            value = 100.0 + slope * i
            prices.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "close": value,
                    "adj_close": value,
                    "observation_type": "price_backfill",
                }
            )
    return pd.DataFrame(scanner), pd.DataFrame(prices)


def _frozen():
    return {
        "schema_version": "phase1b_frozen_patterns_v1",
        "source_phase": "1B_timing_patterns",
        "source_events": 1,
        "horizons": {
            str(h): {
                "discovery_window": ["2026-04-15", "2026-07-31"],
                "discovery_candidate_count": 10,
                "frozen_patterns": [
                    {
                        "pattern": "score_d1_up",
                        "conditions": ["score_d1_up"],
                        "discovery_direction": "positive",
                    },
                    {
                        "pattern": "score_d1_down",
                        "conditions": ["score_d1_down"],
                        "discovery_direction": "negative",
                    },
                ],
            }
            for h in (5, 20, 40, 60)
        },
    }


def test_wilson_interval_matches_standard_formula():
    low, high = _wilson_interval(5, 10)
    assert low == pytest.approx(0.2366, abs=0.001)
    assert high == pytest.approx(0.7634, abs=0.001)


def test_beta_shrinkage_pulls_small_sample_toward_baseline():
    out = _beta_shrinkage(4, 5, 0.50, 20.0)
    assert out["posterior_mean"] < 0.80
    assert out["posterior_mean"] > 0.50
    assert out["posterior_interval_95"][0] < out["posterior_mean"] < out["posterior_interval_95"][1]


def test_beta_shrinkage_uses_proper_prior_at_boundary_rates():
    low = _beta_shrinkage(0, 5, 0.0, 20.0)
    high = _beta_shrinkage(5, 5, 1.0, 20.0)
    assert low["prior_alpha"] > 0 and low["prior_beta"] > 0
    assert high["prior_alpha"] > 0 and high["prior_beta"] > 0
    assert low["posterior_interval_95"][1] > 0
    assert high["posterior_interval_95"][0] < 1
    for invalid in (0.0, float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError):
            _beta_shrinkage(1, 2, 0.5, invalid)


def test_probability_stats_reports_raw_shrunk_and_cluster_uncertainty():
    days = pd.to_datetime(["2026-05-01", "2026-05-01", "2026-05-08", "2026-05-08"])
    baseline = pd.DataFrame(
        {
            "obs_date": list(days) * 2,
            "symbol": list("ABCDEFGH"),
            "peer_excess_5t": [-0.2, 0.1, -0.1, 0.2, -0.05, 0.05, -0.3, 0.3],
        }
    )
    occurrences = baseline.iloc[[1, 3, 5, 7]].copy()
    out = _probability_stats(
        occurrences,
        baseline,
        "peer_excess_5t",
        Phase2Config(cluster_bootstrap_reps=50),
        ("test",),
        comparisons=4,
    )
    assert out is not None
    assert out["raw_positive_peer_excess_rate"] == 1.0
    assert out["shrunk_positive_peer_excess_probability"] < 1.0
    assert out["probability_advantage_vs_baseline"] > 0
    assert len(out["cluster_bootstrap_mean_peer_excess_95"]) == 2
    assert out["bonferroni_adjusted_p"] == pytest.approx(min(1.0, out["approx_binomial_p"] * 4))


def test_phase2_keeps_selection_and_timing_separate_and_uses_frozen_candidates():
    history, prices = _synthetic()
    result = analyze(
        history,
        prices,
        _frozen(),
        Phase2Config(
            min_pattern_discovery_n=5,
            min_pattern_validation_n=3,
            cluster_bootstrap_reps=20,
        ),
    )
    assert result["semantics"]["selection_and_timing_kept_separate"] is True
    assert result["semantics"]["timing_candidates_are_frozen_from_phase1b"] is True
    assert set(result["horizons"]["5"]) == {"validation_maturity", "selection", "timing_patterns"}
    timing = result["horizons"]["5"]["timing_patterns"]
    assert timing["candidate_source"] == "frozen_phase1b"
    assert timing["candidate_selection_uses_validation"] is False
    assert [row["pattern"] for row in timing["patterns"]] == ["score_d1_up", "score_d1_down"]
    for row in timing["patterns"]:
        assert row["selection_was_discovery_only"] is True
        assert "alpha_direction_confirmed" in row
        assert "probability_direction_confirmed" in row
        assert "strong_validation" in row
        validation = row["validation"]
        if validation and validation["approx_binomial_p"] is not None:
            assert validation["bonferroni_adjusted_p"] == pytest.approx(
                min(1.0, validation["approx_binomial_p"] * 2)
            )


def test_analyze_rejects_nonfinite_prior_strength_before_research():
    history, prices = _synthetic()
    for invalid in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(ValueError):
            analyze(history, prices, _frozen(), Phase2Config(prior_strength=invalid, cluster_bootstrap_reps=0))


def test_frozen_catalog_window_must_match_phase2_config():
    history, prices = _synthetic()
    frozen = _frozen()
    frozen["horizons"]["5"]["discovery_window"] = ["2026-05-01", "2026-07-31"]
    with pytest.raises(ValueError):
        analyze(history, prices, frozen, Phase2Config(cluster_bootstrap_reps=0))


def test_writer_rejects_nan_json(tmp_path, monkeypatch):
    history = tmp_path / "history.csv"
    prices = tmp_path / "prices.csv"
    frozen = tmp_path / "frozen.json"
    output = tmp_path / "report.json"
    pd.DataFrame({"x": [1]}).to_csv(history, index=False)
    pd.DataFrame({"x": [1]}).to_csv(prices, index=False)
    frozen.write_text(json.dumps(_frozen()), encoding="utf-8")
    monkeypatch.setattr(module, "analyze", lambda *_args, **_kwargs: {"bad": float("nan")})
    with pytest.raises(ValueError):
        module.run(history, prices, output, frozen_patterns_path=frozen, metadata_path=None)
