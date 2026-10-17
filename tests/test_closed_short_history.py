from datetime import datetime,timezone
import pandas as pd
from scanner.data.enrich.yahoo_prices import enrich_watchlist_with_yahoo

def test_newly_listed_security_does_not_keep_stale_sma200(monkeypatch):
    monkeypatch.setenv("SCANNER_CLOSED_DAILY_ONLY","1")
    # 87 genuine observations, deliberately fewer than 200
    dates=pd.bdate_range("2026-06-01",periods=87)
    dates=dates[dates<pd.Timestamp("2026-10-17")]
    frame=pd.DataFrame({("Close","SPCX"):range(1,len(dates)+1),
                        ("Volume","SPCX"):[1_000_000]*len(dates),
                        ("Close","SPY"):range(100,100+len(dates)),
                        ("Volume","SPY"):[5_000_000]*len(dates),
                        ("Close","BTC-USD"):range(1000,1000+len(dates)),
                        ("Volume","BTC-USD"):[8_000]*len(dates)},index=dates)
    frame.columns=pd.MultiIndex.from_tuples(frame.columns)
    raw=pd.DataFrame([{"YahooSymbol":"SPCX","SMA200":999,"Trend200":0.99,
                       "RS3M":0.5,"Akt. Kurs":999}])
    out,_=enrich_watchlist_with_yahoo(raw,enabled=True,
                                      as_of=datetime(2026,10,17,18,tzinfo=timezone.utc),
                                      downloaded_frame=frame)
    assert out.loc[0,"price_bar_status"]=="CLOSED_PREVIOUS_SESSION"
    assert pd.isna(out.loc[0,"SMA200"])
    assert pd.isna(out.loc[0,"Trend200"])
    assert out.loc[0,"Akt. Kurs"]!=999
