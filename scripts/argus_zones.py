"""Research-only, reproducible one-symbol ARGUS zone CLI (M1–M3).

No Watch/Decision/Order code path. Source is either explicit local OHLCV
or the existing read-only Yahoo historical adapter, never video price levels.
A retrospective run cannot be claimed as originally first-seen on its asof.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path

from scanner.research.zones_v01 import build_zones, read_rows
from scanner.research.zones_m3 import write_readable_tables

# Instrument identity is pinned to the same pilot mapping as the private M5
# experiment. This is NOT a generic ticker-string-to-security resolver.
PILOT_IDENTITIES = {
    "AMD": ("XNAS", "XNAS:AMD:US", "USD"),
    "INTC": ("XNAS", "XNAS:INTC:US", "USD"),
    "ARM": ("XNAS", "XNAS:ARM:US", "USD"),
    "SMH": ("XNAS", "XNAS:SMH:US", "USD"),
    "NOW": ("XNYS", "XNYS:NOW:US", "USD"),
}
COLUMNS=("date","symbol","currency","open","high","low","close","adj_close","volume")


def fetch_provider(symbol, asof):
    # Lazy import: offline CSV replays require no network adapter.
    from scripts.prefetch_market_history import _download
    end = (date.fromisoformat(asof)+timedelta(days=1)).isoformat()
    start = (date.fromisoformat(asof)-timedelta(days=930)).isoformat()
    frame=_download([symbol],start=start,end=end,threads=1)
    if frame.attrs.get("errors"):
        raise ValueError("provider_fetch_errors_for_selected_instrument")
    rows=[]
    for item in frame.to_dict("records"):
        if str(item.get("symbol"))!=symbol:
            raise ValueError("provider_returned_different_instrument")
        if str(item.get("date",""))[:10]>asof:continue
        row={"symbol":symbol,"currency":PILOT_IDENTITIES[symbol][2],
             "date":str(item["date"])[:10]}
        for k in ("open","high","low","close","adj_close","volume"):
            value=float(item[k])
            if not math.isfinite(value) or value<0 or (k!="volume" and value<=0):
                raise ValueError("provider_invalid_"+k)
            row[k]=str(value)
        rows.append(row)
    return rows


def verify_completed(asof, now=None):
    """Prevent unfinished NYSE/NASDAQ candles being used as complete."""
    import pandas_market_calendars as mcal
    now=now or datetime.now(timezone.utc)
    session=mcal.get_calendar("NYSE").schedule(
        start_date=(date.fromisoformat(asof)-timedelta(days=2)).isoformat(),
        end_date=asof)
    if asof not in {x.date().isoformat() for x in session.index}:
        raise ValueError("not_a_us_stock_market_session")
    bar=session.loc[asof,"market_close"].to_pydatetime()
    if bar+timedelta(minutes=90)>now:
        raise ValueError("session_not_yet_closed_plus_safety_margin")


def analyze(symbol,asof,rows,*,source_kind):
    symbol=symbol.upper()
    if symbol not in PILOT_IDENTITIES:
        raise ValueError("instrument_identity_unverified:"+symbol)
    date.fromisoformat(asof)
    exchange, asset_id, currency=PILOT_IDENTITIES[symbol]
    sample=sorted((r for r in rows if r.get("date","")<=asof),key=lambda r:r["date"])
    if len(sample)<80 or sample[-1]["date"]!=asof:
        raise ValueError("closed_asof_bar_or_minimum_history_missing:"+symbol)
    if len({r["date"] for r in sample})!=len(sample):
        raise ValueError("duplicate_sessions")
    for r in sample:
        if r["symbol"]!=symbol or r["currency"]!=currency:
            raise ValueError("security_or_currency_mismatch")
        vals={}
        for k in ("open","high","low","close","adj_close"):
            try: vals[k]=float(r[k])
            except (KeyError,TypeError,ValueError):
                raise ValueError("invalid_market_price:"+k) from None
            if not math.isfinite(vals[k]) or vals[k]<=0:
                raise ValueError("nonpositive_or_nonfinite_market_price:"+k)
        if vals["low"]>min(vals["open"],vals["close"]) or vals["high"]<max(vals["open"],vals["close"]):
            raise ValueError("inconsistent_ohlc:"+r["date"])
    report=build_zones(sample,symbol,asof=asof,exchange=exchange,
                       asset_id=asset_id,source_sha=None)
    if report.get("data_quality")=="blocked" or report.get("last_completed_bar")!=asof:
        raise ValueError("zone_engine_data_quality_blocked")
    report["research_cli_receipt"]={
        "source_kind":source_kind,
        "source_rows_sha256":hashlib.sha256(json.dumps(
            sample,sort_keys=True,separators=(",",":"),allow_nan=False).encode()).hexdigest(),
        "source_sessions":len(sample),
        "source_asof":asof,
        "source_retrospective_pit_certified":False,
        "first_seen_prospective":False,
        "market_identity":"fixed_five_pilot_security_registry",
        "can_trade":False,
        "empirical_probability":None,
    }
    return report


def run(symbol,asof,output_dir,*,input_file=None,downloader=fetch_provider,now=None):
    symbol=symbol.upper()
    if symbol not in PILOT_IDENTITIES:
        raise ValueError("unknown_instrument_id_not_allowed")
    verify_completed(asof,now=now)
    if input_file:
        data=read_rows(Path(input_file),{symbol})
        records=data[symbol]
        source="explicit_historical_ohlcv_csv"
    else:
        records=downloader(symbol,asof)
        source="read_only_yahoo_provider_adapter"
    report=analyze(symbol,asof,records,source_kind=source)
    output=Path(output_dir)
    output.mkdir(parents=True,exist_ok=True)
    dest=output/f"{symbol}_{asof}_zones.json"
    dest.write_text(json.dumps(report,indent=2,sort_keys=True,ensure_ascii=False,allow_nan=False)+"\n")
    write_readable_tables(report,dest)
    return dest,report


def main():
    parser=argparse.ArgumentParser(description="ARGUS independent research zone analysis for five identity-verified pilots")
    parser.add_argument("--symbol",required=True)
    parser.add_argument("--asof",required=True)
    parser.add_argument("--input",type=Path,default=None,
                        help="Optional OHLCV CSV for reproducible offline replay")
    parser.add_argument("--output-dir",required=True,type=Path)
    args=parser.parse_args()
    path,report=run(args.symbol,args.asof,args.output_dir,input_file=args.input)
    print(json.dumps({
        "symbol":report["symbol"],"asof":report["asof"],
        "last_completed_bar":report["last_completed_bar"],
        "asset_id":report["asset_id"],"quality":report["data_quality"],
        "zones":len(report["zones"]),"clusters":len(report["zone_clusters"]),
        "plans":len(report["scenario_plans"]),"output":str(path),
        "research_only":True},sort_keys=True))


if __name__=="__main__":
    main()
