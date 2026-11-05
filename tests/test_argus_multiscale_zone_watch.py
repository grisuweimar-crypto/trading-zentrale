"""Offline acceptance tests: monthly macro Elliott and completed 4-hour intraday."""
from datetime import date,datetime,timedelta,timezone
import csv
import json
import math
from pathlib import Path

import pandas as pd

from scanner.research.zone_timeframes import COLUMNS, analyze
from scripts.prefetch_argus_timeframes import _monthly, _four_hour, update
from scripts.generate_argus_zone_watch import calculate,write_outputs

ASOF="2026-11-05"


def months(symbol="UEC"):
    rows=[]
    for i in range(84):
        year=2020+i//12
        mon=i%12+1
        yr2=year+1 if mon==12 else year
        m2=1 if mon==12 else mon+1
        price=15 + i*.05 + 3*math.sin(i/3)
        rows.append({"symbol":symbol,"currency":"USD","timeframe":"monthly",
                     "bar_start":f"{year:04}-{mon:02}-01",
                     "confirmed_at":f"{yr2:04}-{m2:02}-01",
                     "open":price-.3,"high":price+1,"low":price-1,
                     "close":price,"adj_close":price,"volume":5000,
                     "source":"test","retrieved_at":"2026-11-05T20:00:00+00:00",
                     "session_tail":"0"})
    return rows


def hours(symbol="UEC"):
    rows=[]
    for i in range(50):
        d=date(2026,9,1)+timedelta(days=i)
        value=12 + .03*i + 1.4*math.sin(i/2)
        rows.append({"symbol":symbol,"currency":"USD","timeframe":"four_hour",
                     "bar_start":f"{d.isoformat()}T09:30:00-04:00",
                     "confirmed_at":d.isoformat(),
                     "open":value-.15,"high":value+.5,"low":value-.5,
                     "close":value,"adj_close":value,"volume":1000,
                     "source":"test","retrieved_at":"2026-11-05T20:00:00+00:00",
                     "session_tail":"0"})
    return rows


def test_monthly_and_four_hour_research_from_completed_bars():
    m=analyze(months(),"UEC","monthly",asof=ASOF)
    h=analyze(hours(),"UEC","four_hour",asof=ASOF)
    assert m["status"]=="RESEARCH_ONLY"
    assert m["bars"]==82+1  # latest completed month = 2026-11 boundary: Jan 2020 + 82 = Nov 2026 (excluded)
    assert m["zones"] and all(z["known_at"]<=ASOF for z in m["zones"])
    assert len(m["price_history"])>=70
    assert h["status"]=="RESEARCH_ONLY" and h["zones"]
    assert h["currency"]=="USD"
    assert all(z["empirical_probability"] is None for z in h["zones"])
    assert all(w["empirical_probability"] is None for w in h["waves"])


def test_no_future_months_or_bar_lookahead():
    records=months()
    cutoff="2026-02-15"
    before=analyze(records,"UEC","monthly",asof=cutoff)
    extra=records+[dict(records[-1],bar_start="2026-11-01",confirmed_at="2026-12-01",
                        close=900,open=900,low=899,high=901,adj_close=900)]
    after=analyze(extra,"UEC","monthly",asof=cutoff)
    assert before==after
    assert all(z["known_at"]<=cutoff for z in before["zones"])


def test_future_four_hour_bars_cannot_create_pivots():
    before=analyze(hours(),"UEC","four_hour",asof="2026-10-03")
    after=analyze(hours()+[dict(hours()[-1],bar_start="2026-12-10T09:30:00-05:00",
                                confirmed_at="2026-12-10",close=40,open=40,
                                low=39,high=41,adj_close=40)],"UEC","four_hour",asof="2026-10-03")
    assert before==after


def test_invalid_duplicate_and_mixed_currencies_fail_closed():
    invalid=months()+[dict(months()[-1])]
    a=analyze(invalid,"UEC","monthly",asof=ASOF)
    assert a["status"]=="UNAVAILABLE"
    assert a["zones"]==[]
    mixed=months()
    mixed[5]["currency"]="EUR"
    b=analyze(mixed,"UEC","monthly",asof=ASOF)
    assert b["status"]=="UNAVAILABLE" and b["waves"]==[]


def test_true_four_hour_aggregation_and_session_tail():
    from zoneinfo import ZoneInfo
    tz=ZoneInfo("America/New_York")
    index=pd.date_range("2026-11-04 09:30",periods=7,freq="h",tz=tz)
    frame=pd.DataFrame({"Open":[10+i*.1 for i in range(7)],
                        "High":[10.15+i*.1 for i in range(7)],
                        "Low":[9.95+i*.1 for i in range(7)],
                        "Close":[10.10+i*.1 for i in range(7)],
                        "Adj Close":[10.10+i*.1 for i in range(7)],
                        "Volume":[100]*7},index=index)
    daily={"2026-11-04":{"close":"10.7","high":"10.75","low":"9.95"}}
    result=_four_hour(frame,symbol="XPEV",currency="USD",asof="2026-11-04",
                      retrieved="test",daily=daily)
    assert len(result)==2
    assert [r["session_tail"] for r in result]==["0","1"]
    assert result[0]["bar_start"].startswith("2026-11-04T09:30")
    assert result[0]["high"]==10.45
    assert result[1]["close"]==10.7
    assert not _four_hour(frame,symbol="XPEV",currency="USD",asof="2026-11-03",
                          retrieved="test",daily=daily)
    assert not _four_hour(frame,symbol="XPEV",currency="USD",asof="2026-11-04",
                          retrieved="test",daily={"2026-11-04":{**daily["2026-11-04"],"close":"12.0"}})


def test_monthly_current_incomplete_month_suppressed():
    index=pd.to_datetime(["2026-09-01","2026-10-01","2026-11-01"]).tz_localize("America/New_York")
    frame=pd.DataFrame({"Open":[10,12,14],"High":[11,13,15],
                        "Low":[9,11,13],"Close":[10,12,14],
                        "Adj Close":[10,12,14],"Volume":[100]*3},index=index)
    out=_monthly(frame,symbol="UEC",currency="USD",asof=ASOF,retrieved="test")
    assert [r["bar_start"] for r in out]==["2026-09-01","2026-10-01"]
    assert out[-1]["confirmed_at"]=="2026-11-01"


def test_update_cache_and_readonly_universe_using_mock(tmp_path):
    sym="UEC"; asof="2026-11-05"
    watch=tmp_path/"watch.csv"
    with watch.open("w",newline="") as f:
        wr=csv.DictWriter(f,fieldnames=["yahoo_symbol","name","asset_id","currency","score",
                                        "score_status","price_bar_date","price_bar_status"])
        wr.writeheader()
        wr.writerow(dict(yahoo_symbol=sym,name=sym,asset_id=sym,currency="USD",
                         score=12,score_status="OK",price_bar_date=asof,
                         price_bar_status="CLOSED_PREVIOUS_SESSION"))
    daily=tmp_path/"daily.csv"
    with daily.open("w",newline="") as f:
        fields=["symbol","date","currency","open","high","low","close","adj_close","volume"]
        wr=csv.DictWriter(f,fieldnames=fields)
        wr.writeheader()
        wr.writerow(dict(symbol=sym,date=asof,currency="USD",open=12,high=13,
                         low=11,close=12,adj_close=12,volume=1000))
    cached=tmp_path/"tf.csv";receipt=tmp_path/"receipt.json"
    def provider(symbol,**kwargs):
        assert kwargs["daily"][asof]["close"]=="12"
        assert kwargs["currency"]=="USD"
        return months()+hours()
    state=update(watchlist=watch,daily_market=daily,output=cached,
                 receipt=receipt,provider=provider,workers=1,
                 now=datetime(2026,11,5,22,tzinfo=timezone.utc))
    assert state["successful"]==1
    assets=calculate({sym:{"symbol":sym,"name":sym,"scanner_asset_id":sym,
                      "scanner_score":12,"scanner_status":"OK","asof":asof,
                      "closed_bar_status":"CLOSED_PREVIOUS_SESSION",
                      "declared_currency":"USD"}},daily,cached)
    assert assets[0]["multiscale"]["monthly"]["status"]=="RESEARCH_ONLY"
    assert assets[0]["multiscale"]["four_hour"]["status"]=="RESEARCH_ONLY"
    out=write_outputs(assets,out_json=tmp_path/"argus.json",out_html=tmp_path/"zones.html",
                      history_dir=tmp_path/"history")
    assert out["monthly_available"]==1 and out["four_hour_available"]==1
    assert "4-Stunden-Elliott" in (tmp_path/"zones.html").read_text()
