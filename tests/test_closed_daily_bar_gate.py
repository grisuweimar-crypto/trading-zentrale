from datetime import datetime, timezone
import pandas as pd
from scanner.data.enrich.yahoo_prices import _completed_daily_series_for_venue

def test_us_uses_previous_trading_day_even_after_close():
    c=pd.Series([10.,11.,12.],index=pd.to_datetime(["2026-10-15","2026-10-16","2026-10-19"]))
    v=pd.Series([100.,110.,120.],index=c.index)
    now=datetime(2026,10,19,23,tzinfo=timezone.utc)
    x,y=_completed_daily_series_for_venue(c,v,as_of=now,symbol="UEC")
    assert x.index[-1]==pd.Timestamp("2026-10-16")
    assert y.iloc[-1]==110.

def test_hong_kong_uses_previous_local_day_not_utc():
    c=pd.Series([1.,2.],index=pd.to_datetime(["2026-10-16","2026-10-19"]))
    now=datetime(2026,10,18,23,tzinfo=timezone.utc)  # Mon 07 HK
    x,_=_completed_daily_series_for_venue(c,None,as_of=now,symbol="0700.HK")
    assert x.index[-1]==pd.Timestamp("2026-10-16")

def test_crypto_utc_daily_boundary():
    c=pd.Series([1.,2.],index=pd.to_datetime(["2026-10-16","2026-10-17"]))
    x,_=_completed_daily_series_for_venue(c,None,as_of=datetime(2026,10,17,23,tzinfo=timezone.utc),symbol="BTC-USD")
    assert len(x)==1

def test_unsupported_exchange_is_not_guessed():
    c=pd.Series([1.,2.],index=pd.to_datetime(["2026-10-16","2026-10-17"]))
    x,_=_completed_daily_series_for_venue(c,None,as_of=datetime(2026,10,17,23,tzinfo=timezone.utc),symbol="XYZ.UNKNOWN")
    assert x.empty
