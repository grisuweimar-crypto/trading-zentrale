"""CY02-B01: verified September 2026 equity holidays, never synthetic market bars."""
from datetime import datetime, timezone

import pandas as pd
import pytest

from scanner.data.enrich.cycle_exchange_calendar import (
    POLICY_VERSION, authorized_extended_equity_gap,
)
from scanner.data.enrich.cycle_oscillator import calculate_cycle
from scanner.reports.cycle_history import lag_mask
from test_cycle_history_cy03 import row as history_row


AS_OF = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("symbol,currency,closures", [
    ("6503.T", "JPY", ["2026-09-21", "2026-09-22", "2026-09-23"]),
    ("6861.T", "JPY", ["2026-09-21", "2026-09-22", "2026-09-23"]),
    ("8035.T", "JPY", ["2026-09-21", "2026-09-22", "2026-09-23"]),
    ("6506.T", "JPY", ["2026-09-21", "2026-09-22", "2026-09-23"]),
    ("000660.KS", "KRW", ["2026-09-24", "2026-09-25"]),
    ("005930.KS", "KRW", ["2026-09-24", "2026-09-25"]),
])
def test_six_cash_equity_venue_gaps_are_allowed_only_for_audited_days(symbol, currency, closures):
    dates = pd.bdate_range(end="2026-10-09", periods=90)
    values = pd.Series([100 + x * 0.12 + 4 * __import__("math").sin(x / 5)
                        for x in range(90)], index=dates)
    clean = values.drop(pd.to_datetime(closures))
    result = calculate_cycle(clean, symbol=symbol, currency=currency,
                             is_crypto=False, as_of=AS_OF)
    assert result["cycle_quality"] == "VALID"
    assert result["cycle_quality_reason"] == "COMPLETED_DAILY_BARS_" + POLICY_VERSION
    assert len(result["_cycle_input_bars"]) == 60
    assert result["cycle_price_sha256"]

    # One unexplained session adjoining an official holiday is NOT excused.
    damaged = clean.drop(pd.Timestamp("2026-09-24" if symbol.endswith(".T") else "2026-09-23"))
    assert calculate_cycle(damaged, symbol=symbol, currency=currency,
                           is_crypto=False, as_of=AS_OF)["cycle_quality_reason"] == "GAP_IN_DAILY_BARS"
    # Wrong quote-currency or stock listing cannot borrow that venue's closures.
    wrong = calculate_cycle(clean, symbol=symbol, currency="USD" if currency == "JPY" else "JPY",
                            is_crypto=False, as_of=AS_OF)
    assert wrong["cycle_quality_reason"] == "GAP_IN_DAILY_BARS"


def test_unlisted_dates_suffixes_and_crypto_are_never_released_by_calendar():
    from datetime import date
    assert authorized_extended_equity_gap(
        date(2026, 9, 18), date(2026, 9, 24), symbol="6503.T", currency="JPY"
    )
    assert authorized_extended_equity_gap(
        date(2026, 9, 23), date(2026, 9, 28), symbol="000660.KS", currency="KRW"
    )
    for sym, cur in (("6503.T", "USD"), ("6503.TO", "JPY"),
                     ("000660.KQ", "KRW"), ("BTC-USD", "USD")):
        assert not authorized_extended_equity_gap(
            date(2026, 9, 18), date(2026, 9, 24), symbol=sym, currency=cur
        )
    assert not authorized_extended_equity_gap(
        date(2026, 10, 1), date(2026, 10, 7), symbol="6503.T", currency="JPY"
    )


def test_calendar_policy_change_is_a_hard_cy03_lag_identity_boundary():
    first = history_row("2026-10-10", asset="6503.T")
    second = history_row("2026-10-11", asset="6503.T")
    second["reason"] = "COMPLETED_DAILY_BARS_" + POLICY_VERSION
    mask = lag_mask([first, second])
    assert mask[1]["lag_1obs"] == "IDENTITY_OR_FORMULA_CHANGED"
    assert mask[1]["research_status"] == "BLOCKED_EXTERNAL_VERIFICATION_269"
