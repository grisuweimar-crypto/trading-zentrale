"""CY-02-B01: narrowly reviewed 2026 JPX/KRX *equity* closure exceptions.

Source-bound, calendar-specific gap checks only. Does not certify historical
Yahoo publication times, quote currency, listing identity or adjusted-bar PIT.
Dates have independent primary-source evidence. Every unlisted date and venue
remains on the original fail-closed quality rule.
"""
from __future__ import annotations

from datetime import date, timedelta


POLICY_VERSION = "cycle_equity_calendar_closures_2026_v1"
OFFICIAL_EQUITY_CLOSURES = {
    # Official JPX equity market holidays (not OSE futures holiday trading).
    # https://www.jpx.co.jp/english/corporate/about-jpx/calendar/index.html
    ".T": {
        "currency": "JPY",
        "days": frozenset({date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)}),
        "venue": "JPX_CASH_EQUITY",
    },
    # Official Bank of Korea Chuseok 2026 holiday calendar + KRX KOSPI
    # cash equity market holiday rule for government public holidays.
    # https://www.bok.or.kr/eng/main/contents.do?menuNo=400373
    # https://global.krx.co.kr/contents/GLB/06/0602/0602010201/GLB0602010201T1.jsp
    ".KS": {
        "currency": "KRW",
        "days": frozenset({date(2026, 9, 24), date(2026, 9, 25)}),
        "venue": "KRX_KOSPI_CASH_EQUITY",
    },
}


def authorized_extended_equity_gap(
    previous: date, following: date, *, symbol: str, currency: str,
) -> bool:
    """Permit only an extended gap containing *solely* verified closures.

    Normal 0/1-missing-weekday gaps remain governed by the original rule.
    No mixing official holidays with unexplained absent trading sessions.
    Symbols and currencies must identify the same cash-equity venue.
    """
    if previous >= following:
        return False
    matched = [
        data for suffix, data in OFFICIAL_EQUITY_CLOSURES.items()
        if symbol.upper().endswith(suffix)
        and currency.upper() == data["currency"]
    ]
    if len(matched) != 1:
        return False
    missing = []
    cursor = previous + timedelta(days=1)
    while cursor < following:
        if cursor.weekday() < 5:
            missing.append(cursor)
        cursor += timedelta(days=1)
    return len(missing) >= 2 and set(missing).issubset(matched[0]["days"])
