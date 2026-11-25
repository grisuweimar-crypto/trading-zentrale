from datetime import date, timedelta
import pandas as pd
import pytest
from scripts.prefetch_market_history import capture_full_window_pilot


def test_opt_in_capture_is_evidence_only():
    calls=[]
    attempts=[]
    end=date(2026,11,25)
    def fetch(symbols, request, *, retries, witness_attempts):
        calls.append((symbols,request,retries))
        witness_attempts.append({"test":"attempt"})
        rows=[{"symbol":s,"date":(end-timedelta(days=i+1)).isoformat(),"adj_close":"10"}
              for s in symbols for i in range(60)]
        rows.append({"symbol":"UEC","date":end.isoformat(),"adj_close":"11"})
        return pd.DataFrame(rows), {}
    assert capture_full_window_pilot([],end,witness_attempts=attempts,fetcher=fetch)=={}
    report=capture_full_window_pilot(["UEC","TTD"],end,witness_attempts=attempts,fetcher=fetch)
    assert report["at_least_60_sessions"]=={"UEC":True,"TTD":True}
    assert calls==[(["UEC","TTD"],{"period":"1y","start":None,"end":"2026-11-25"},0)]
    assert report["attempts_recorded"]==1
    assert not report["independent_pit_verified"]
    with pytest.raises(ValueError,match="unregistered"):
        capture_full_window_pilot(["OTHER"],end,witness_attempts=attempts,fetcher=fetch)


def test_failed_pilot_does_not_replace_market_cache():
    def fail(*args,**kwargs):
        raise RuntimeError("provider error")
    result=capture_full_window_pilot(["UEC"],date(2026,11,25),
                                     witness_attempts=[],fetcher=fail)
    assert result["status"]=="PILOT_FAILED"

def test_expected_session_excludes_future_and_detects_provider_omission():
    from datetime import datetime, timezone
    from scripts.prefetch_market_history import expected_pilot_session, missing_pilot_sessions
    end=date(2026,11,25)
    now=datetime(2026,11,25,1,36,tzinfo=timezone.utc)
    assert expected_pilot_session("UEC",end=end,now=now)=="2026-11-24"
    assert expected_pilot_session("TTD",end=end,now=now)=="2026-11-24"
    existing=[{"symbol":"UEC","date":"2026-11-23","adj_close":"5.84"},
              {"symbol":"TTD","date":"2026-11-23","adj_close":"14.06"}]
    assert missing_pilot_sessions(("UEC","TTD"),end=end,now=now,existing=existing,incoming=[])=={
        "UEC":"2026-11-24","TTD":"2026-11-24"}
    incoming=[{"symbol":"UEC","date":"2026-11-24","adj_close":"5.57"}]
    assert missing_pilot_sessions(("UEC","TTD"),end=end,now=now,existing=existing,incoming=incoming)=={
        "TTD":"2026-11-24"}
    before_close=datetime(2026,11,24,18,tzinfo=timezone.utc)
    assert expected_pilot_session("TTD",end=end,now=before_close)=="2026-11-23"
