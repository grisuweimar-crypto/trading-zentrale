#!/usr/bin/env python3
"""Optional, independent long-term / intraday provider input for ARGUS research.

Yahoo has no native 4h candle. Completed regular-session 60m observations are
assembled into consecutive groups of up to four hourly bars, with the final
shorter *session tail explicitly marked*. A session is accepted only when its
last hourly close matches the already completed canonical daily close.
Monthly candles are accepted only from months strictly earlier than asof.
No data is ever substituted for missing or unavailable provider history.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import argparse
import csv
import json
import math
from pathlib import Path

from scanner.research.zone_timeframes import COLUMNS
from scanner.research.zones_v01 import read_rows
from scripts.generate_argus_zone_watch import _load_universe

DEFAULT_CACHE=Path("artifacts/market_data/argus_multiscale.csv")
DEFAULT_RECEIPT=Path("artifacts/reports/argus_multiscale_fetch.json")


def _value(x, *, volume=False):
    v=float(x)
    if not math.isfinite(v) or (v<0 if volume else v<=0):
        raise ValueError("nonfinite_or_nonpositive_provider_price")
    return v


def _fields(item):
    result={}
    for key in ("open","high","low","close"):
        result[key]=_value(item[key.title()])
    result["volume"]=_value(item.get("Volume",0),volume=True)
    if result["high"] < max(result["open"],result["close"]) - 1e-7 or (
            result["low"] > min(result["open"],result["close"]) + 1e-7):
        raise ValueError("provider_inconsistent_ohlc")
    adj=item.get("Adj Close")
    result["adj_close"]=_value(adj) if adj is not None and not _isna(adj) else result["close"]
    return result


def _isna(x):
    import pandas as pd
    return bool(pd.isna(x))


def _monthly(frame, *, symbol, currency, asof, retrieved):
    out=[]
    if frame is None or frame.empty:
        return out
    cutoff=date.fromisoformat(asof)
    for ts, item in frame.sort_index().iterrows():
        local=ts.date()
        first=local.replace(day=1)
        next_month=date(first.year+1,1,1) if first.month==12 else date(first.year,first.month+1,1)
        if next_month>cutoff:
            continue
        row=_fields(item)
        out.append(dict(symbol=symbol,currency=currency,timeframe="monthly",
                        bar_start=first.isoformat(),confirmed_at=next_month.isoformat(),
                        source="yahoo_1mo_completed",retrieved_at=retrieved,
                        session_tail="0",**row))
    return out


def _four_hour(frame, *, symbol, currency, asof, retrieved, daily):
    """Build RTH 4-hour buckets from consecutive LOCAL hourly starts.

    Unlike date-rounded groupers this preserves 30-minute NYSE opening offsets,
    market timezone/DST, and excludes out-of-hours quotes (prepost=False).
    Incomplete sessions, gaps and unsupported daily-close differences fail closed.
    """
    if frame is None or frame.empty:
        return []
    grouped=defaultdict(list)
    cutoff=date.fromisoformat(asof)
    for ts,item in frame.sort_index().iterrows():
        if ts.tzinfo is None:
            raise ValueError("hourly_timestamp_not_timezone_aware")
        if ts.date()>cutoff:
            continue
        grouped[ts.date().isoformat()].append((ts,item))
    out=[]
    for session, events in sorted(grouped.items()):
        if session not in daily or len(events)<4:
            continue
        events.sort(key=lambda p:p[0])
        # Venue lunch gaps, broken provider coverage or duplicate intervals
        # cannot be mistaken for regular 4-hour candles.
        if any((events[i][0]-events[i-1][0]).total_seconds()!=3600
               for i in range(1,len(events))):
            continue
        try:
            hourly=[_fields(item) for _,item in events]
            daily_close=_value(daily[session]["close"])
            hourly_close=hourly[-1]["close"]
            # Provider sessions must reconcile to the canonical *raw* daily bar.
            if abs(hourly_close-daily_close)/daily_close > 0.003:
                continue
            # Require basic daily high/low consistency as additional guard.
            hi=max(x["high"] for x in hourly)
            lo=min(x["low"] for x in hourly)
            d_hi=_value(daily[session]["high"])
            d_lo=_value(daily[session]["low"])
            if (abs(hi-d_hi)/d_hi>0.01 or abs(lo-d_lo)/d_lo>0.01):
                continue
        except (ValueError,TypeError,KeyError,OverflowError):
            continue
        for start in range(0,len(events),4):
            chunk=events[start:start+4]
            if len(chunk)<2:
                continue
            data=hourly[start:start+len(chunk)]
            out.append(dict(symbol=symbol,currency=currency,timeframe="four_hour",
                            bar_start=chunk[0][0].isoformat(),
                            # All these bars are only released after full daily
                            # session confirmation, not while intraday forming.
                            confirmed_at=session,source="yahoo_60m_rth_aggregated",
                            retrieved_at=retrieved,session_tail="1" if len(chunk)<4 else "0",
                            open=data[0]["open"],high=max(x["high"] for x in data),
                            low=min(x["low"] for x in data),close=data[-1]["close"],
                            # Hourly adjusted-price series should not invent
                            # an adjusted history; close is the provider raw basis.
                            adj_close=data[-1]["adj_close"],
                            volume=sum(x["volume"] for x in data)))
    return out


def _provider(symbol, *, currency, asof, daily, retrieved):
    import yfinance as yf
    ticker=yf.Ticker(symbol)
    metadata=ticker.get_history_metadata() or {}
    quote_currency=str(metadata.get("currency") or "").upper()
    if quote_currency!=currency.upper():
        raise ValueError("provider_currency_mismatch_or_unverified")
    if str(metadata.get("instrumentType") or "").upper() not in {"EQUITY","ETF"}:
        raise ValueError("unsupported_or_unverified_instrument_type")
    monthly=ticker.history(period="10y",interval="1mo",auto_adjust=False,
                           prepost=False,repair=False,raise_errors=True)
    # Yahoo's supported period values do not include "60d". Hourly bars
    # may have a provider-specific history window; use 3mo with 1mo fallback.
    # Preserve the independent monthly analysis if hourly fetching fails.
    try:
        hourly=ticker.history(period="3mo",interval="60m",auto_adjust=False,
                              prepost=False,repair=False,raise_errors=True)
    except Exception:
        try:
            hourly=ticker.history(period="1mo",interval="60m",auto_adjust=False,
                                  prepost=False,repair=False,raise_errors=True)
        except Exception:
            hourly=None
    mo=_monthly(monthly,symbol=symbol,currency=currency,asof=asof,retrieved=retrieved)
    h4=_four_hour(hourly,symbol=symbol,currency=currency,asof=asof,
                  retrieved=retrieved,daily=daily)
    return mo+h4


def _read_existing(path):
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8-sig",newline="") as fh:
        reader=csv.DictReader(fh)
        if not set(COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError("cached_multiscale_schema_invalid")
        result={}
        for r in reader:
            key=(r["symbol"],r["timeframe"],r["bar_start"])
            if key in result:
                raise ValueError("duplicate_cached_multiscale_bar")
            result[key]=r
    return result


def update(*, watchlist, daily_market, output=DEFAULT_CACHE, receipt=DEFAULT_RECEIPT,
           provider=_provider, workers=6, now=None):
    """Refresh every current Scanner equity; preserve cached evidence on failure."""
    now=now or datetime.now(timezone.utc)
    stamp=now.isoformat()
    universe=_load_universe(Path(watchlist))
    raw_daily=read_rows(Path(daily_market),set(universe))
    cached=_read_existing(Path(output))
    previous={}
    if Path(receipt).is_file():
        previous=json.loads(Path(receipt).read_text(encoding="utf-8")).get("assets",{})
    attempted={}
    tasks=[]
    for symbol,info in sorted(universe.items()):
        if not info["asof"]:
            attempted[symbol]={"status":"blocked","reason":"scanner_closed_bar_missing"}
            continue
        rows={r["date"]:r for r in raw_daily.get(symbol,[])
              if r["date"]<=info["asof"] and r.get("currency")==info["declared_currency"]}
        if info["asof"] not in rows:
            attempted[symbol]={"status":"blocked","reason":"canonical_daily_bar_missing"}
            continue
        last=previous.get(symbol,{})
        if (last.get("status")=="ok" and last.get("asof")==info["asof"]
                and last.get("attempted_at") and
                (now-datetime.fromisoformat(last["attempted_at"])).total_seconds()<12*3600):
            attempted[symbol]=last
            continue
        tasks.append((symbol,info,rows))
    def run(job):
        symbol,info,rows=job
        try:
            data=provider(symbol,currency=info["declared_currency"],
                          asof=info["asof"],daily=rows,retrieved=stamp)
            if not data:
                return symbol,[],{"status":"unavailable","reason":"provider_returned_no_completed_bars"}
            return symbol,data,{"status":"ok","reason":None}
        except Exception as exc:
            return symbol,[],{"status":"unavailable","reason":type(exc).__name__+":"+str(exc)[:120]}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for symbol,data,state in executor.map(run,tasks):
            asof=universe[symbol]["asof"]
            revision_count=0
            for r in data:
                key=(r["symbol"],r["timeframe"],r["bar_start"])
                previous_row=cached.get(key)
                if previous_row and any(
                    str(previous_row.get(f))!=str(r[f]) for f in
                    ("open","high","low","close","volume","adj_close")):
                    revision_count+=1
                cached[key]={k:str(r.get(k,"")) for k in COLUMNS}
            attempted[symbol]={**state,"attempted_at":stamp,
                               "asof":asof,"new_or_refreshed_bars":len(data),
                               "revised_provider_rows":revision_count,
                               "monthly_bars":sum(r["timeframe"]=="monthly" for r in data),
                               "four_hour_bars":sum(r["timeframe"]=="four_hour" for r in data)}
    # Preserve a bounded multi-year window while retaining asof-relevant records.
    current=set(universe)
    relevant={k:r for k,r in cached.items() if k[0] in current and (
        k[1]=="monthly" or k[1]=="four_hour" and
        str(r["confirmed_at"])>=(now.date()-timedelta(days=90)).isoformat())}
    output=Path(output);receipt=Path(receipt)
    output.parent.mkdir(parents=True,exist_ok=True)
    receipt.parent.mkdir(parents=True,exist_ok=True)
    from tempfile import NamedTemporaryFile
    with NamedTemporaryFile("w",newline="",encoding="utf-8",dir=output.parent,delete=False) as f:
        writer=csv.DictWriter(f,fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(relevant[k] for k in sorted(relevant))
        temporary=Path(f.name)
    temporary.replace(output)
    state={"version":"argus-multiscale-provider-v1",
           "updated_at":stamp,"assets":attempted,"total":len(universe),
           "successful":sum(x["status"]=="ok" for x in attempted.values()),
           "provider_verified_point_in_time":False,
           "no_automatic_orders":True}
    receipt.write_text(json.dumps(state,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    return state


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watchlist",type=Path,default=Path("artifacts/watchlist/watchlist_full.csv"))
    parser.add_argument("--daily-market",type=Path,default=Path("artifacts/market_data/yahoo_ohlcv.csv"))
    parser.add_argument("--output",type=Path,default=DEFAULT_CACHE)
    parser.add_argument("--receipt",type=Path,default=DEFAULT_RECEIPT)
    parser.add_argument("--workers",type=int,default=6)
    args=parser.parse_args(argv)
    if not 1<=args.workers<=12:
        parser.error("--workers must be in 1..12")
    state=update(watchlist=args.watchlist,daily_market=args.daily_market,
                 output=args.output,receipt=args.receipt,workers=args.workers)
    print(json.dumps({k:state[k] for k in ("version","total","successful")}))
    # Partial provider failure is explicitly represented, not hidden as success.
    return 0


if __name__=="__main__":
    raise SystemExit(main())
