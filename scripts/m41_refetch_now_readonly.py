"""Read-only M4.1 input isolation: refetch historical NOW daily sessions.

The old canonical archive (which has missing adj_close in its early rows) is
NEVER edited. Returns an independent later-retrieved provider history proxy
for M4.1 only; does not certify what could be known on historic as-of dates.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

from scripts.prefetch_market_history import _download

FIELDS = ["date", "symbol", "currency", "open", "high", "low",
          "close", "adj_close", "volume", "retrieved_at"]


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input",type=Path,required=True)
    ap.add_argument("--output",type=Path,required=True)
    ap.add_argument("--asof",default="2026-10-21")
    args=ap.parse_args()
    end=(date.fromisoformat(args.asof)+timedelta(days=1)).isoformat()
    source_sha=hashlib.sha256(args.input.read_bytes()).hexdigest()
    with args.input.open(newline="",encoding="utf-8-sig") as f:
        reader=csv.DictReader(f)
        if not set(FIELDS).issubset(reader.fieldnames or []):
            raise ValueError("missing_canonical_price_fields")
        others=[x for x in reader if x["symbol"]!="NOW"]
    frame=_download(["NOW"],start="2024-06-01",end=end,threads=1)
    if frame.attrs.get("errors",{}).get("NOW"):
        raise ValueError("now_provider_error:"+str(frame.attrs["errors"]["NOW"]))
    at=datetime.now(timezone.utc).isoformat()
    now=[]
    for row in frame.to_dict("records"):
        if row.get("symbol")!="NOW" or not str(row.get("date",""))[:10]<=args.asof:
            continue
        if any(row.get(k) is None or not __import__("math").isfinite(float(row[k]))
               for k in ("open","high","low","close","adj_close")):
            raise ValueError("NOW_provider_missing_adjusted_daily_price")
        now.append({"date":str(row["date"])[:10],"symbol":"NOW","currency":"USD",
                    **{k:str(row[k]) for k in ("open","high","low","close","adj_close","volume")},
                    "retrieved_at":at})
    if len(now)<300 or len({x["date"] for x in now})!=len(now):
        raise ValueError("NOW_provider_history_short_or_duplicated")
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open("w",encoding="utf-8",newline="") as fh:
        w=csv.DictWriter(fh,fieldnames=FIELDS)
        w.writeheader()
        w.writerows({k:r.get(k,"") for k in FIELDS} for r in others+now)
    receipt={"reason":"NOW prior archive adjusted_close incomplete",
             "source_sha256":source_sha,
             "new_input_sha256":hashlib.sha256(args.output.read_bytes()).hexdigest(),
             "fresh_now_session_count":len(now),
             "now_first":now[0]["date"],"now_last":now[-1]["date"],
             "provider_retrieved_at":at,
             "historical_market_point_in_time_proof":False,
             "product_archive_modified":False}
    args.output.with_suffix(".receipt.json").write_text(
        json.dumps(receipt,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(json.dumps(receipt,sort_keys=True))


if __name__=="__main__":
    main()
