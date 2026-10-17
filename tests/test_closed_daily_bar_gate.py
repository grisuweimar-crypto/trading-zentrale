from datetime import datetime, timezone
import pandas as pd

from scanner.data.enrich.yahoo_prices import _completed_daily_series


def test_closed_bar_gate_drops_current_utc_day_and_future():
    close = pd.Series(
        [10.0, 12.0, 14.0],
        index=pd.to_datetime(["2026-10-15", "2026-10-16", "2026-10-17"]),
    )
    volume = pd.Series([1.0, 2.0, 3.0], index=close.index)
    now = datetime(2026, 10, 17, 15, 0, tzinfo=timezone.utc)
    filtered, vol = _completed_daily_series(close, volume, as_of=now)
    assert list(filtered.values) == [10.0, 12.0]
    assert list(vol.values) == [1.0, 2.0]


def test_closed_bar_gate_conservatively_defers_already_closed_asia_day():
    close = pd.Series([10.0, 11.0], index=pd.to_datetime(["2026-10-16", "2026-10-17"]))
    filtered, _ = _completed_daily_series(
        close, None, as_of=datetime(2026, 10, 17, 21, 0, tzinfo=timezone.utc)
    )
    assert len(filtered) == 1
    assert filtered.index[-1] == pd.Timestamp("2026-10-16")


def test_closed_bar_gate_does_not_fabricate_missing_sessions():
    close = pd.Series([10.0], index=pd.to_datetime(["2026-10-14"]))
    filtered, vol = _completed_daily_series(
        close, None, as_of=datetime(2026, 10, 17, tzinfo=timezone.utc)
    )
    assert len(filtered) == 1
    assert vol is None
