from datetime import date, timedelta

import pytest

from scanner.reports.danelfin_compare import (
    DanelfinClient,
    build_comparison_events,
    infer_danelfin_market,
    parse_ranking_history,
    select_symbols,
    summarize_events,
)


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


def test_market_inference_is_conservative():
    assert infer_danelfin_market("AAPL", "USD") == "us"
    assert infer_danelfin_market("SAP.DE", "EUR") == "europe"
    assert infer_danelfin_market("9988.HK", "HKD") is None
    assert infer_danelfin_market("6503.T", "JPY") is None
    assert infer_danelfin_market("BTC-USD", "USD") is None


def test_parse_ranking_history_orders_dates_and_validates_scores():
    payload = {
        "2026-09-21": {"aiscore": 8, "technical": 9, "sentiment": 7},
        "meta": {"ignored": True},
        "2026-09-19": {"aiscore": 6, "fundamental": 4, "low_risk": 11},
    }
    rows = parse_ranking_history(payload, ticker="AAPL", market="us")
    assert [row["date"] for row in rows] == ["2026-09-19", "2026-09-21"]
    assert rows[0]["aiscore"] == 6
    assert rows[0]["low_risk"] is None


def test_client_sends_key_only_as_header():
    session = FakeSession({"2026-09-21": {"aiscore": 8}})
    client = DanelfinClient("secret-key", session=session)
    rows = client.ranking_history("AAPL")
    assert rows[0]["aiscore"] == 8
    url, kwargs = session.calls[0]
    assert "secret-key" not in url
    assert kwargs["params"] == {"ticker": "AAPL"}
    assert kwargs["headers"]["x-api-key"] == "secret-key"


def _price_row(day, symbol, close):
    return {
        "date": day.isoformat(),
        "symbol": symbol,
        "close": str(close),
        "observation_type": "market_data",
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


def test_point_in_time_alignment_cooldown_and_alpha():
    start = date(2026, 1, 1)
    days = [start + timedelta(days=i) for i in range(70)]
    rows = []
    for i, day in enumerate(days):
        rows.append(_price_row(day, "AAA", 100 + 2 * i))
        rows.append(_price_row(day, "SPY", 100 + i))
        if i % 2 == 0:
            rows.append(_scanner_row(day, "AAA", 20 + i))

    danelfin = {
        "AAA": [
            {
                "date": day.isoformat(),
                "aiscore": 5 + (i % 5),
                "fundamental": 6,
                "technical": 7,
                "sentiment": 8,
                "low_risk": 5,
            }
            for i, day in enumerate(days[:20])
        ]
    }
    events = build_comparison_events(
        rows, danelfin, benchmark_symbol="SPY",
        cooldown_sessions=5, max_scanner_staleness_days=3,
    )
    assert [event["date"] for event in events] == [
        days[0].isoformat(), days[5].isoformat(), days[10].isoformat(), days[15].isoformat()
    ]
    assert events[1]["scanner_date"] == days[4].isoformat()
    assert events[1]["scanner_staleness_days"] == 1
    assert events[0]["alpha_5t"] == pytest.approx(
        ((110 / 100) - 1) - ((105 / 100) - 1)
    )


def test_select_symbols_uses_supported_markets_and_history_depth():
    start = date(2026, 1, 1)
    rows = []
    for symbol, currency in (("AAA", "USD"), ("SAP.DE", "EUR"), ("9988.HK", "HKD")):
        for i in range(70):
            day = start + timedelta(days=i)
            rows.append(_price_row(day, symbol, 100 + i))
            if i < 6:
                rows.append(_scanner_row(day, symbol, 20 + i, currency=currency))
    selected = select_symbols(rows, limit=10, markets=("us", "europe"))
    assert {item["symbol"] for item in selected} == {"AAA", "SAP.DE"}


def test_summary_compares_scanner_danelfin_and_subscores():
    events = [
        {
            "symbol": "A", "scanner_score": 10, "rank_percentile": 0.9,
            "aiscore": 1, "fundamental": 1, "technical": 1, "sentiment": 1, "low_risk": 1,
            "alpha_5t": -0.2,
        },
        {
            "symbol": "B", "scanner_score": 20, "rank_percentile": 0.5,
            "aiscore": 5, "fundamental": 5, "technical": 5, "sentiment": 5, "low_risk": 5,
            "alpha_5t": 0.0,
        },
        {
            "symbol": "C", "scanner_score": 30, "rank_percentile": 0.1,
            "aiscore": 10, "fundamental": 10, "technical": 10, "sentiment": 10, "low_risk": 10,
            "alpha_5t": 0.2,
        },
    ]
    summary = summarize_events(events)
    assert summary["horizons"]["5"]["outcome"] == "alpha_5t"
    assert summary["horizons"]["5"]["scanner_spearman"] == pytest.approx(1.0)
    assert summary["horizons"]["5"]["danelfin_ai_spearman"] == pytest.approx(1.0)
