"""CY02-B01 read-only Yahoo gap audit for six Japan/Korea exceptions.

Not a live-publication claim. Does NOT weaken the oscillator's gap validation.
JPX dates below are from the official 2026 equity market holiday calendar;
KRX dates are preliminary third-party calendar references, NOT primary-verified.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import pandas as pd

from scanner.data.enrich.cycle_oscillator import calculate_cycle
from scanner.data.enrich.yahoo_prices import _series_from_download

ASSETS = {
    "6503.T": "JPY", "6861.T": "JPY", "8035.T": "JPY", "6506.T": "JPY",
    "000660.KS": "KRW", "005930.KS": "KRW",
}
# Stock exchange closures. Never silently substitute public holidays for a
# market-calendar source. KRX items are specifically marked unverified.
JPX_VERIFIED_2026 = {"2026-09-21", "2026-09-22", "2026-09-23"}
KRX_REFERENCED_2026 = {"2026-09-24", "2026-09-25",
                       "2026-10-05", "2026-10-09"}


def dates_between(a: date, b: date):
    d = a + timedelta(days=1)
    while d < b:
        if d.weekday() < 5:
            yield d.isoformat()
        d += timedelta(days=1)


def audit_frame(frame: pd.DataFrame, *, as_of: datetime):
    results = []
    for symbol, currency in ASSETS.items():
        close, _ = _series_from_download(frame, symbol)
        if close is None or close.empty:
            results.append({"symbol": symbol, "currency": currency,
                            "status": "PROVIDER_UNAVAILABLE", "source": "fresh Yahoo 1d",
                            "coverage": 0, "gaps": []})
            continue
        series = close[close.index.date < as_of.date()].sort_index().tail(60)
        observed = [d.date() for d in series.index]
        gaps = []
        known = JPX_VERIFIED_2026 if symbol.endswith(".T") else KRX_REFERENCED_2026
        authority = "JPX_OFFICIAL" if symbol.endswith(".T") else "KRX_UNVERIFIED_REFERENCE"
        for a, bb in zip(observed, observed[1:]):
            missing = list(dates_between(a, bb))
            if len(missing) >= 2:
                gaps.append({"from":a.isoformat(),"to":bb.isoformat(),
                    "missing_weekdays":missing,
                    "known_market_closed":sorted(set(missing)&known),
                    "unexplained_days":sorted(set(missing)-known),
                    "calendar_source_strength":authority})
        calc = calculate_cycle(close,symbol=symbol,currency=currency,
                               is_crypto=False,as_of=as_of)
        results.append({"symbol":symbol,"currency":currency,"status":calc["cycle_quality"],
            "quality_reason":calc["cycle_quality_reason"],
            "covered_input_bars":len(series),
            "last_session":observed[-1].isoformat() if observed else None,
            "gaps":gaps,"calendar_source_strength":authority,
            "research_approved":False})
    return results


def main():
    import yfinance as yf
    as_of = datetime(2026, 10, 10, 8, 0, tzinfo=timezone.utc)
    frame = yf.download(tickers=sorted(ASSETS),period="1y",interval="1d",
                        auto_adjust=True,group_by="column",threads=True,progress=False)
    result = audit_frame(frame,as_of=as_of)
    print(json.dumps({"audit_as_of":as_of.isoformat(),
                      "captured_now":datetime.now(timezone.utc).isoformat(),
                      "provider":"fresh Yahoo 1d, NOT 2026-10-10 published payload",
                      "assets":result},indent=2))
    if len(result)!=6:
        raise SystemExit("wrong asset cardinality")
    # Offline/provider problems remain explicit; do not claim gate success.
    if any(r["status"]=="PROVIDER_UNAVAILABLE" for r in result):
        raise SystemExit("CY02-B01 GAP DIAGNOSTIC INCOMPLETE: provider unavailable")
    return 0


if __name__=="__main__":
    raise SystemExit(main())
