from __future__ import annotations

import pandas as pd

from scanner.reports.timing_patterns import (
    Phase1BConfig,
    _add_peer_excess,
    _cooldown_matching,
    _strict_windows,
    build_timing_events,
    continuous_feature_summary,
    discover_patterns,
    feature_rows,
)


def _synthetic():
    dates = pd.bdate_range("2026-04-15", periods=150)
    scanner = []
    prices = []
    for symbol, base, slope in (("A", 20.0, 1.0), ("B", 10.0, 0.4), ("C", 5.0, -0.2)):
        for i, day in enumerate(dates[:115]):
            scanner.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "name": symbol,
                    "score": base + 0.01 * i * i,
                    "opportunity": 40 + i * 0.2,
                    "risk": 50 - i * 0.1,
                    "rs3m": -0.2 + i * 0.01,
                    "trend200": -0.1 + i * 0.005,
                    "cycle": float((i * 3) % 100),
                    "currency": "USD",
                    "r_code": "R3" if i < 50 else "R4",
                    "observation_type": "observed_scanner",
                }
            )
        for i, day in enumerate(dates):
            prices.append(
                {
                    "date": day.date().isoformat(),
                    "symbol": symbol,
                    "close": 100 + slope * i,
                    "observation_type": "price_backfill",
                }
            )
    return pd.DataFrame(scanner), pd.DataFrame(prices)


def test_feature_rows_create_point_in_time_deltas_and_r_changes():
    history, _ = _synthetic()
    frame, coverage = feature_rows(history)
    assert "score_d5" in frame.columns
    assert "rs3m_d10" in frame.columns
    assert "r_d1" in frame.columns
    assert coverage["sources"]["rs3m"] == "rs3m"
    assert frame["r_upgrade"].any()


def test_elliott_is_reported_missing_instead_of_invented():
    history, _ = _synthetic()
    _, coverage = feature_rows(history)
    assert coverage["sources"]["elliott"] is None
    assert coverage["non_null"]["elliott_state"] == 0


def test_elliott_transition_requires_prior_known_state():
    rows = []
    for day, signal in zip(
        pd.bdate_range("2026-04-15", periods=3),
        (None, "BUY", "SELL"),
    ):
        rows.append(
            {
                "date": day.date().isoformat(),
                "symbol": "A",
                "name": "A",
                "score": 10.0,
                "opportunity": 50.0,
                "risk": 30.0,
                "rs3m": 0.1,
                "trend200": 0.1,
                "cycle": 50.0,
                "currency": "USD",
                "r_code": "R3",
                "elliott_signal": signal,
                "observation_type": "observed_scanner",
            }
        )
    frame, _ = feature_rows(pd.DataFrame(rows))
    assert not bool(frame.iloc[1]["elliott_to_buy"])
    assert bool(frame.iloc[2]["elliott_to_sell"])


def test_build_timing_events_preserves_future_horizons():
    history, prices = _synthetic()
    events, coverage = build_timing_events(history, prices)
    assert len(events) > 0
    assert "score_d5" in events.columns
    assert events["return_20t"].notna().any()
    assert coverage["rows"] > 0


def test_continuous_summary_is_split_between_discovery_and_validation():
    history, prices = _synthetic()
    events, _ = build_timing_events(history, prices)
    out = continuous_feature_summary(
        events,
        prices,
        5,
        Phase1BConfig(min_single_n=5),
    )
    assert set(out) == {"discovery", "validation"}
    assert "score_d5" in out["discovery"]
    assert "score" not in out["discovery"]


def test_same_day_rerun_uses_latest_row_via_phase1a_loader():
    history, _ = _synthetic()
    extra = history.iloc[[0]].copy()
    extra["score"] = 99.0
    history = pd.concat([history, extra], ignore_index=True)
    frame, _ = feature_rows(history)
    first_day = pd.Timestamp(history.iloc[0]["date"])
    row = frame[(frame["symbol"] == "A") & (frame["date"] == first_day)].iloc[0]
    assert row["score"] == 99.0


def test_discovery_excludes_outcomes_that_end_after_cutoff():
    work = pd.DataFrame(
        {
            "obs_date": pd.to_datetime(["2026-06-01", "2026-07-27", "2026-08-03"]),
            "end_date_60t": pd.to_datetime(["2026-07-30", "2026-10-19", "2026-10-26"]),
        }
    )
    discovery, validation = _strict_windows(work, 60, Phase1BConfig())
    assert discovery["obs_date"].tolist() == [pd.Timestamp("2026-06-01")]
    assert validation["obs_date"].tolist() == [pd.Timestamp("2026-08-03")]


def test_peer_median_is_computed_before_sampling_and_singletons_fall_back():
    day = pd.Timestamp("2026-05-01")
    events = pd.DataFrame(
        {
            "obs_date": [day] * 4,
            "currency": ["USD", "USD", "EUR", "JPY"],
            "return_5t": [0.10, 0.20, 0.30, 0.40],
        }
    )
    out = _add_peer_excess(events, 5)
    assert round(float(out.iloc[0]["peer_excess_5t"]), 8) == -0.05
    assert round(float(out.iloc[1]["peer_excess_5t"]), 8) == 0.05
    # EUR/JPY are singleton currency cohorts, so they use the global 0.25 median.
    assert round(float(out.iloc[2]["peer_excess_5t"]), 8) == 0.05
    assert round(float(out.iloc[3]["peer_excess_5t"]), 8) == 0.15


def test_pattern_cooldown_is_applied_after_matching_occurrences():
    dates = pd.bdate_range("2026-05-01", periods=15)
    prices = pd.DataFrame(
        {"date": dates, "symbol": "A", "close": range(100, 115)}
    )
    work = pd.DataFrame(
        {
            "symbol": ["A", "A", "A"],
            "start_market_date": [dates[3], dates[4], dates[10]],
            "match": [True, True, True],
        }
    )
    kept = _cooldown_matching(work, prices, work["match"], 5)
    assert kept["start_market_date"].tolist() == [dates[3], dates[10]]


def test_validation_cannot_select_or_remove_discovery_candidate():
    discovery_days = pd.bdate_range("2026-05-01", periods=8)
    validation_days = pd.bdate_range("2026-08-03", periods=6)
    rows = []
    price_rows = []
    all_days = list(discovery_days) + list(validation_days)
    for day in all_days:
        is_discovery = day <= pd.Timestamp("2026-07-31")
        end_day = day + pd.offsets.BDay(5)
        for symbol in ("A", "B"):
            target = symbol == "A"
            ret = (0.10 if is_discovery else -0.10) if target else 0.0
            rows.append(
                {
                    "obs_date": day,
                    "symbol": symbol,
                    "currency": "USD",
                    "start_market_date": day,
                    "end_date_5t": end_day,
                    "return_5t": ret,
                    "atom": target,
                }
            )
            price_rows.append({"date": day, "symbol": symbol, "close": 100.0})
    events = pd.DataFrame(rows)
    prices = pd.DataFrame(price_rows)
    config = Phase1BConfig(
        min_pattern_discovery_n=5,
        min_pattern_validation_n=3,
        cooldown_sessions=0,
        max_combo_size=1,
        max_atoms_for_combos=4,
        top_patterns=2,
    )
    out = discover_patterns(events, prices, 5, {"atomic_patterns": ["atom"]}, config)
    assert len(out["frozen_positive"]) == 1
    candidate = out["frozen_positive"][0]
    assert candidate["pattern"] == "atom"
    assert candidate["discovery"]["mean_peer_excess"] > 0
    assert candidate["validation"]["mean_peer_excess"] < 0
    assert candidate["validation_sufficient"]
    assert not candidate["direction_confirmed"]
