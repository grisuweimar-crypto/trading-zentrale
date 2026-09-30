from copy import deepcopy
from datetime import date

from scanner.reports.historical_matches import HistoricalMatcher, MatchPolicy, PriceSessions


CURRENT = {
    "symbol": "AAA",
    "date": "2026-09-30",
    "rank_percentile": "0.05",
    "r_code": "R4",
    "rs3m": "-0.10",
    "trend200": "0.10",
}


def event(day, symbol="BBB", **changes):
    row = dict(CURRENT, symbol=symbol, date=day)
    row.update(changes)
    return row


def prices(symbol, rows):
    return [{"symbol": symbol, "date": day, "close": str(close)} for day, close in rows]


def test_resolver_uses_exact_or_previous_session_never_future_and_caps_lookback():
    sessions = PriceSessions(prices("BBB", [
        ("2026-09-04", 100),
        ("2026-09-08", 101),
    ]))
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 4)) == date(2026, 9, 4)
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 5)) == date(2026, 9, 4)  # Saturday
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 6)) == date(2026, 9, 4)  # Sunday
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 7)) == date(2026, 9, 4)  # holiday/no session
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 3)) is None  # never future
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 16)) is None  # > 7 calendar days


def test_invalid_prior_price_does_not_become_a_session_or_use_future_price():
    sessions = PriceSessions(prices("BBB", [
        ("2026-09-04", "NaN"),
        ("2026-09-08", 101),
    ]))
    assert sessions.resolve_on_or_before("BBB", date(2026, 9, 6)) is None


def test_weekend_reruns_resolving_to_same_session_keep_latest_scanner_observation():
    history = [
        event("2026-09-04", r_code="R2"),
        event("2026-09-05", r_code="R3"),
        event("2026-09-06", r_code="R4"),
    ]
    original = deepcopy(history)
    market = prices("BBB", [("2026-09-04", 100)])
    matcher = HistoricalMatcher(history, market, MatchPolicy(min_matches=1, cooldown_trading_days=0))

    assert len(matcher.events) == 1
    raw_day, session, symbol, state = matcher.events[0]
    assert raw_day == date(2026, 9, 6)
    assert session == date(2026, 9, 4)
    assert symbol == "BBB"
    assert state["r_code"] == "R4"
    assert history == original  # raw MarketDate and scanner rows are never rewritten

    result = matcher.summary(CURRENT)
    assert result["filter_id"] == "level_1"
    assert result["N"] == 1


def test_weekend_event_uses_previous_session_close_for_forward_return():
    market = prices("BBB", [
        ("2026-09-04", 100),
        ("2026-09-08", 101),
        ("2026-09-09", 102),
        ("2026-09-10", 103),
        ("2026-09-11", 104),
        ("2026-09-14", 150),
    ])
    result = HistoricalMatcher(
        [event("2026-09-06")], market, MatchPolicy(min_matches=1, cooldown_trading_days=0)
    ).summary(CURRENT)
    assert result["N"] == 1
    assert result["forward_5t"]["N"] == 1
    assert result["forward_5t"]["median_return"] == 0.5


def test_gap_beyond_seven_days_remains_unresolved_and_cannot_produce_outcome():
    market = prices("BBB", [
        ("2026-09-01", 100),
        ("2026-09-21", 150),
        ("2026-09-22", 151),
        ("2026-09-23", 152),
        ("2026-09-24", 153),
        ("2026-09-25", 154),
    ])
    matcher = HistoricalMatcher(
        [event("2026-09-15")], market, MatchPolicy(min_matches=1, cooldown_trading_days=0)
    )
    assert matcher.events[0][1] is None
    result = matcher.summary(CURRENT)
    assert result["N"] == 1
    assert result["forward_5t"] == {
        "N": 0,
        "median_return": None,
        "positive_count": 0,
        "positive_rate": None,
    }
