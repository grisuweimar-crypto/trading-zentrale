from datetime import date, timedelta

import pytest

from scanner.reports.danelfin_compare import (
    build_comparison_events,
    build_price_series,
    select_symbols,
    summarize_events,
)


def _price_row(day, symbol, close, observation_type="market_data"):
    return {
        "date": day.isoformat(),
        "symbol": symbol,
        "close": str(close),
        "observation_type": observation_type,
    }


def _scanner_row(day, symbol, score, percentile="0.1", currency="USD"):
    return {
        "date": day.isoformat(),
        "symbol": symbol,
        "score": str(score),
        "rank_percentile": percentile,
        "currency": currency,
        "observation_type": "observed_scanner",
    }


def test_price_backfill_rows_are_canonical_price_observations():
    start = date(2026, 1, 1)
    rows = []
    for i in range(70):
        day = start + timedelta(days=i)
        rows.append(_price_row(day, "AAA", 100 + i, "price_backfill"))
        if i < 6:
            rows.append(_scanner_row(day, "AAA", 20 + i))

    prices = build_price_series(rows)
    assert len(prices["AAA"].dates) == 70

    selected = select_symbols(rows, limit=10, markets=("us",))
    assert [item["symbol"] for item in selected] == ["AAA"]


def test_unknown_scanner_rank_is_not_treated_as_negative_signal():
    events = [
        {
            "symbol": "A",
            "scanner_score": 20,
            "rank_percentile": None,
            "aiscore": 9,
            "alpha_5t": 0.10,
        },
        {
            "symbol": "B",
            "scanner_score": 30,
            "rank_percentile": 0.50,
            "aiscore": 9,
            "alpha_5t": -0.05,
        },
        {
            "symbol": "C",
            "scanner_score": 40,
            "rank_percentile": 0.10,
            "aiscore": 9,
            "alpha_5t": 0.20,
        },
    ]

    groups = summarize_events(events)["horizons"]["5"]
    assert groups["agreement_groups"]["scanner_rank_unknown"]["N"] == 1
    assert groups["agreement_groups"]["danelfin_only"]["N"] == 1
    assert groups["agreement_groups"]["both_positive"]["N"] == 1
    assert groups["agreement_rank_known_N"] == 2
    assert groups["agreement_rank_unknown_N"] == 1


def test_benchmark_return_uses_assets_exact_horizon_end_date():
    start = date(2026, 1, 1)
    rows = []
    days = [start + timedelta(days=i) for i in range(70)]

    for i, day in enumerate(days):
        rows.append(_price_row(day, "AAA", 100 + 2 * i))
        # Deliberately omit one SPY session before the asset's 5-session target.
        # A session-count-based SPY horizon would therefore end one day too late.
        if i != 2:
            rows.append(_price_row(day, "SPY", 200 + i))

    rows.append(_scanner_row(days[0], "AAA", 30))
    danelfin = {
        "AAA": [{"date": days[0].isoformat(), "aiscore": 8}]
    }

    event = build_comparison_events(
        rows,
        danelfin,
        benchmark_symbol="SPY",
        cooldown_sessions=5,
        max_scanner_staleness_days=3,
    )[0]

    assert event["target_date_5t"] == days[5].isoformat()
    assert event["return_5t"] == pytest.approx((110 / 100) - 1)
    assert event["benchmark_return_5t"] == pytest.approx((205 / 200) - 1)
    assert event["alpha_5t"] == pytest.approx(
        ((110 / 100) - 1) - ((205 / 200) - 1)
    )


def test_alpha_is_missing_when_benchmark_lacks_exact_target_endpoint():
    start = date(2026, 1, 1)
    rows = []
    days = [start + timedelta(days=i) for i in range(70)]

    for i, day in enumerate(days):
        rows.append(_price_row(day, "AAA", 100 + i))
        if i != 5:
            rows.append(_price_row(day, "SPY", 200 + i))

    rows.append(_scanner_row(days[0], "AAA", 30))
    danelfin = {"AAA": [{"date": days[0].isoformat(), "aiscore": 8}]}

    event = build_comparison_events(rows, danelfin, benchmark_symbol="SPY")[0]
    assert event["target_date_5t"] == days[5].isoformat()
    assert event["benchmark_return_5t"] is None
    assert event["alpha_5t"] is None
