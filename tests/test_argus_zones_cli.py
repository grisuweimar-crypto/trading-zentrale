"""Offline M1/M2 as-of CLI contracts; isolated research, never Orders."""
from datetime import datetime, timezone
import csv
import math
from pathlib import Path
import tempfile

import pandas as pd
import pytest

from scripts.argus_zones import PILOT_IDENTITIES, analyze, run, verify_completed


def sample(symbol="AMD"):
    # 160 distinct completed weekday sessions, monotonic clock, several
    # objectively identifiable swings. No vendor, video, watch, or score.
    days=pd.bdate_range(end="2026-10-23", periods=160)
    out=[]
    for i,d in enumerate(days):
        close=100+0.07*i+5*math.sin(i*0.34)
        op=close-0.2*math.cos(i*.5)
        out.append({
            "symbol":symbol,"currency":"USD","date":d.date().isoformat(),
            "open":str(op),"high":str(max(op,close)+1.1),
            "low":str(min(op,close)-1.1),"close":str(close),
            "adj_close":str(close),"volume":str(500_000+i*70)})
    return out


def now():
    return datetime(2026,10,26,20,tzinfo=timezone.utc)


def test_offline_identified_symbol_outputs_actual_zones_and_scenarios():
    rows=sample("AMD")
    with tempfile.TemporaryDirectory() as root:
        p=Path(root)/"input.csv"
        with p.open("w",newline="") as f:
            writer=csv.DictWriter(f,fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        destination,report=run("AMD","2026-10-23",root,
                               input_file=p,now=now())
        assert destination.exists()
        assert report["symbol"]=="AMD"
        assert report["exchange"]=="XNAS"
        assert report["asset_id"]=="XNAS:AMD:US"
        assert report["last_completed_bar"]=="2026-10-23"
        assert report["data_quality"]=="degraded"
        assert len(report["zones"])>3
        assert len(report["zone_clusters"])>0
        assert report["research_cli_receipt"]["source_retrospective_pit_certified"] is False
        assert report["research_cli_receipt"]["first_seen_prospective"] is False
        assert report["research_cli_receipt"]["can_trade"] is False
        assert all(z["empirical_probability"] is None for z in report["zones"])
        assert all(z["lower"]<=z["upper"] for z in report["zones"])
        assert len(list(Path(root).glob("*zones*.csv")))>0
        assert len(list(Path(root).glob("*zones*.md")))>0


def test_lookahead_rows_do_not_change_causal_forecast():
    history=sample()
    extended=history+[{"date":"2026-10-26","open":"10000",
                       "high":"10010","low":"9990",
                       "close":"10000","adj_close":"10000",
                       "volume":"5000000","symbol":"AMD","currency":"USD"}]
    a=analyze("AMD","2026-10-23",history,source_kind="csv")
    b=analyze("AMD","2026-10-23",extended,source_kind="csv")
    assert a["data_hash"]==b["data_hash"]
    assert a["zones"]==b["zones"]
    assert a["zone_clusters"]==b["zone_clusters"]
    assert a["research_cli_receipt"]==b["research_cli_receipt"]


def test_unknown_identity_bad_bar_duplicate_and_currency_fail_closed():
    rows=sample()
    with pytest.raises(ValueError, match="instrument_identity_unverified"):
        analyze("XYZ","2026-10-23",rows,source_kind="csv")
    wrong=[dict(row) for row in rows]
    wrong[5]["currency"]="EUR"
    with pytest.raises(ValueError,match="security_or_currency_mismatch"):
        analyze("AMD","2026-10-23",wrong,source_kind="csv")
    duplicate=rows+[dict(rows[10])]
    with pytest.raises(ValueError,match="duplicate_sessions"):
        analyze("AMD","2026-10-23",duplicate,source_kind="csv")
    malformed=[dict(row) for row in rows]
    malformed[7]["low"]="99999"
    with pytest.raises(ValueError,match="inconsistent_ohlc"):
        analyze("AMD","2026-10-23",malformed,source_kind="csv")


def test_requires_truly_closed_us_daily_candle():
    verify_completed("2026-10-23",now())
    with pytest.raises(ValueError,match="session_not_yet_closed"):
        verify_completed("2026-10-23",datetime(2026,10,23,18,tzinfo=timezone.utc))
    with pytest.raises(ValueError,match="not_a_us_stock_market_session"):
        verify_completed("2026-10-25",now())
