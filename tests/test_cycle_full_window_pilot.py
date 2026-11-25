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
