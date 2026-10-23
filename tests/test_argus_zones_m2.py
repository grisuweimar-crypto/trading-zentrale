"""ARGUS M2 research-only invariants, hand-checks and causality."""
from datetime import date, timedelta
import math
import pytest

from scanner.research.zones.engine import build_zones, ZoneInputError, make_zone


def sample(n=165):
    start = date(2026, 1, 5)
    out = []
    for i in range(n):
        d = start + timedelta(days=i)
        close = round(100 + 11 * math.sin(i * math.pi / 9) + 0.03 * i, 6)
        out.append({
            "date": d.isoformat(), "symbol": "TEST", "currency": "USD",
            "open": str(close), "high": str(close + 1.2),
            "low": str(close - 1.2), "close": str(close),
            "adj_close": str(close), "volume": "1000",
            "retrieved_at": "2026-10-22T00:00:00Z",
        })
    return out


def build(rows=None, at=135, **kw):
    data = sample() if rows is None else rows
    day = data[at]["date"]
    return build_zones(
        data, symbol="TEST", exchange="XNAS",
        asset_id="XNAS:TEST:US", currency="USD", asof=day,
        bar_end_utc=day + "T20:00:00Z",
        published_at="2026-10-22T23:00:00Z",
        **kw,
    )


def test_all_three_families_present_without_video_zones():
    obj = build()
    counts = obj["diagnostics"]["family_count"]
    assert counts["horizontal"] > 0
    assert counts["fibonacci_retracement"] > 0
    assert counts["swing_abc_projection"] > 0
    assert counts["swing_origin_invalidation"] > 0
    assert not obj["no_qualifying_zone"]
    assert obj["scenarios"] == []
    assert obj["origin"] == "argus_zone_engine"
    assert all(z["empirical_probability"] is None for z in obj["zones"])
    assert all(z["evidence_status"] == "not_validated" for z in obj["zones"])
    assert all(z["lower"] <= z["upper"] for z in obj["zones"])
    assert all(z["drivers"] for z in obj["zones"])


def test_replay_does_not_use_appended_future_bars():
    all_rows = sample()
    prior = build(all_rows[:136])
    later = build(all_rows)
    assert prior == later


def test_replay_stability_and_confirmation():
    result = build()
    assert result == build()
    asof = result["asof"]
    for zone in result["zones"]:
        for driver in zone["drivers"]:
            anchors = driver["anchors"]
            for key in ("A", "B", "C", "pivot"):
                if key in anchors:
                    assert anchors[key]["confirmed_time"] <= asof
                    assert anchors[key]["pivot_time"] <= anchors[key]["confirmed_time"]
        assert zone["known_at"] == result["published_at"]


def test_ratio_hand_calculation():
    atr = 2.0
    zone = make_zone(
        "support", "bull", 61.8, atr,
        "fibonacci_retracement", "minor",
        {"A": {"price": 0}, "B": {"price": 100}, "ratio": 0.382},
        "2026-10-22T23:00:00Z", 110, 109, 111,
    )
    # A=0/B=100, 38.2% retracement gives 61.8, ATR 2 gives half width .6
    assert zone["lower"] == pytest.approx(61.2)
    assert zone["upper"] == pytest.approx(62.4)
    assert zone["role"] == "support"


def test_missing_completed_day_rejected():
    rows = sample()
    rows.pop(135)
    with pytest.raises(ZoneInputError, match="asof_daily_candle_missing"):
        build(rows)


def test_partial_adjustment_fails_closed():
    rows = sample()
    rows[5]["adj_close"] = ""
    with pytest.raises(ValueError, match="partial_adjusted_close_coverage"):
        build(rows)


def test_time_and_identity_gates():
    rows = sample()
    asof = rows[135]["date"]
    with pytest.raises(ZoneInputError, match="bar_completion_or_publication_gate"):
        build_zones(
            rows, symbol="TEST", exchange="XNAS", asset_id="XNAS:TEST:US",
            currency="USD", asof=asof,
            bar_end_utc=asof + "T20:00:00Z",
            published_at="2026-01-06T23:00:00Z",
        )
    with pytest.raises(ZoneInputError, match="history_currency_mismatch"):
        build(rows, currency="EUR")
    with pytest.raises(ZoneInputError, match="asset_identity_missing"):
        build(rows, asset_id="")


def test_incomplete_and_duplicate_rows_fail_closed():
    rows = sample()
    with pytest.raises(ZoneInputError, match="history_invalid_or_duplicate"):
        build(rows + [rows[10]])
    rows = sample()
    rows[12]["high"] = ""
    with pytest.raises(ZoneInputError, match="history_invalid_or_duplicate"):
        build(rows)


def test_raw_basis_is_degraded_and_not_market_validated():
    rows = sample()
    for row in rows:
        row["adj_close"] = ""
    result = build(rows)
    assert result["data_quality"] == "degraded"
    assert result["price_basis"] == "raw_unadjusted"
