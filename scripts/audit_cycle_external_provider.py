"""Retrospective cross-provider robustness pilot. NOT historical Yahoo PIT.

Fetch only public, unauthenticated endpoints. No orders, cost, or source
substitution. Store response digests/status and research aggregates only.
An optional licensed CSV can be supplied locally; no automatic purchases.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timezone
import csv
import hashlib
import io
import json
import math
from pathlib import Path

import requests
from scripts.audit_cycle_client_vintage import load_windows
from scripts.audit_cycle_vintage_robustness import calc, band, direction, summary
from scanner.reports.cycle_history import ROOT, lag_mask, read_ledger
from scanner.research.pattern_discovery.cycle_cy05_adapter import inspect_cycle_archive

PILOT=("UEC","TTD")
START=date(2026,6,1)
END=date(2026,11,24)
PROVIDERS=("NASDAQ","STOOQ")


def parse_prices(raw, provider):
    if provider=="NASDAQ":
        obj=json.loads(raw)
        if obj.get("status",{}).get("rCode") not in (None,200):
            raise ValueError("provider_reported_error")
        table=obj["data"]["tradesTable"]["rows"]
        values=[(datetime.strptime(r["date"],"%m/%d/%Y").date(),r["close"]) for r in table]
    elif provider=="STOOQ":
        reader=csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
        if not {"Date","Close"}.issubset(reader.fieldnames or []):
            raise ValueError("no_daily_csv")
        values=[(date.fromisoformat(r["Date"]),r["Close"]) for r in reader]
    else:
        raise ValueError("unsupported_provider")
    output={}
    for d,v in values:
        if d<START or d>END:
            continue
        p=float(str(v).replace("$","").replace(",","").strip())
        if not math.isfinite(p) or p<=0 or d.isoformat() in output:
            raise ValueError("invalid_or_duplicate_price")
        output[d.isoformat()]=p
    if not output:
        raise ValueError("no_prices_in_registered_interval")
    return output


def fetch(symbol, provider, session):
    if symbol not in PILOT:
        raise ValueError("unregistered_symbol")
    if provider=="NASDAQ":
        url=f"https://api.nasdaq.com/api/quote/{symbol}/historical"
        params={"assetclass":"stocks","fromdate":START.isoformat(),"todate":END.isoformat(),"limit":500}
    elif provider=="STOOQ":
        url="https://stooq.com/q/d/l/"
        params={"s":symbol.lower()+".us","i":"d","d1":START.strftime("%Y%m%d"),"d2":END.strftime("%Y%m%d")}
    else:
        raise ValueError("unsupported_provider")
    record={"provider":provider,"symbol":symbol,"request_url":url,"params":params,
            "requested_at_utc":datetime.now(timezone.utc).isoformat(),
            "price_basis":"PUBLISHED_CLOSE_ADJUSTMENT_UNVERIFIED",
            "historical_vintage_verified":False}
    try:
        response=session.get(url,params=params,timeout=25,
                             headers={"User-Agent":"ARGUS research comparison/1.0",
                                      "Accept":"application/json,text/csv,text/plain,*/*"})
        record.update(http_status=response.status_code,
                      response_sha256=hashlib.sha256(response.content).hexdigest())
        response.raise_for_status()
        prices=parse_prices(response.content,provider)
        record.update(status="DATA_ACQUIRED",rows=len(prices),first=min(prices),last=max(prices))
    except (requests.RequestException,ValueError,KeyError,TypeError) as exc:
        record.update(status="DATA_UNAVAILABLE",error_type=type(exc).__name__,detail=str(exc)[:180])
        prices={}
    record["finished_at_utc"]=datetime.now(timezone.utc).isoformat()
    return record,prices


def study(root, series):
    meta=inspect_cycle_archive(root)
    ledger=read_ledger(root/ROOT/"observations.csv")
    selected=[r for r in ledger if r["asset_id"] in PILOT and r["availability"]=="PROVISIONAL_REPLAYED"
              and r["as_of"]<=END.isoformat()[:8]+"25"]
    result=[]
    for provider in PROVIDERS:
        for symbol in PILOT:
            prices=series.get((provider,symbol),{})
            for row in selected:
                if row["asset_id"]!=symbol or row["price_symbol"]!=symbol or row["currency"]!="USD":
                    continue
                bars=sorted(load_windows(root,row["snapshot_id"])[(symbol,row["currency"],row["price_sha256"])],
                            key=lambda b:b["session_date"])
                days=[b["session_date"] for b in bars]
                if len(days)!=60 or len(set(days))!=60:
                    raise ValueError("bad_archived_window")
                if not all(d in prices for d in days):
                    continue
                original=[float(b["close"]) for b in bars]
                new=[prices[d] for d in days]
                if calc(row,days,original)!=float(row["cycle"]):
                    raise ValueError("baseline_replay_failed")
                compared=calc(row,days,new)
                result.append({"provider":provider,"symbol":symbol,"snapshot_id":row["snapshot_id"],
                               "as_of":row["as_of"],"old":float(row["cycle"]),"new":compared,
                               "price_diff_bps":summary([abs(b/a-1)*10000 for a,b in zip(original,new)])})
    bykey={(r["provider"],r["symbol"],r["snapshot_id"]):r for r in result}
    canonical={}
    for r in ledger:
        key=(r["generated_at"],r["snapshot_id"])
        if r["as_of"] not in canonical or key>canonical[r["as_of"]]:
            canonical[r["as_of"]]=key
    dates=sorted(canonical)
    index={d:i for i,d in enumerate(dates)}
    masks=lag_mask(ledger)
    reports={}
    for provider in PROVIDERS:
        reports[provider]={}
        for symbol in PILOT:
            rows=[r for r in result if r["provider"]==provider and r["symbol"]==symbol]
            valid=[r for r in rows if r["new"] is not None]
            stats={"complete_windows":len(rows),"valid_comparisons":len(valid),
                   "cycle_delta_points":summary([abs(r["old"]-r["new"]) for r in valid]),
                   "band_changes":sum(band(r["old"])!=band(r["new"]) for r in valid),
                   "lt25_changes":sum((r["old"]<25)!=(r["new"]<25) for r in valid),
                   "lag_comparisons":{}}
            for n in (1,5,10):
                counts=Counter()
                for m in masks:
                    if m["asset_id"]!=symbol or m[f"lag_{n}obs"]!="PROVISIONAL_CHAIN":
                        continue
                    i=index[m["as_of"]]
                    chain=[bykey.get((provider,symbol,canonical[d][1])) for d in dates[i-n:i+1]]
                    if len(chain)!=n+1 or any(x is None or x["new"] is None for x in chain):
                        continue
                    a,b=chain[-1],chain[0]
                    old=direction(a["old"],b["old"])
                    new=direction(a["new"],b["new"])
                    counts["chains"]+=1
                    counts["direction_changes"]+=old!=new
                    counts["primary_changes"]+=((a["old"]<25 and old=="UP")!=(a["new"]<25 and new=="UP"))
                stats["lag_comparisons"][str(n)]={k:counts[k] for k in ("chains","direction_changes","primary_changes")}
            reports[provider][symbol]=stats
    return {"archive_latest_snapshot_id":meta["latest_snapshot_id"],"source_archive_manifest_hashes":meta["manifest_file_hashes"],
            "comparison_type":"RETROSPECTIVE_PROVIDER_CLOSE_ONLY","independent_historical_pit_verified":False,
            "research_released":False,"outcomes_inspected":False,"results":reports,"rows":result}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root",type=Path,default=Path("."))
    p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    receipts=[]
    series={}
    with requests.Session() as session:
        for provider in PROVIDERS:
            for symbol in PILOT:
                receipt,prices=fetch(symbol,provider,session)
                receipts.append(receipt)
                series[provider,symbol]=prices
    report=study(args.repo_root,series)
    report["receipts"]=receipts
    args.output.write_text(json.dumps(report,indent=2,allow_nan=False)+"\n")
    print(json.dumps({k:v for k,v in report.items() if k!="rows"},indent=2))
    if not any(x["status"]=="DATA_ACQUIRED" for x in receipts):
        print("NO_EXTERNAL_DATA_ACQUIRED: scientific comparison remains blocked")


if __name__=="__main__":
    main()
