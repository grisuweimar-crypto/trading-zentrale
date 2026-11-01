#!/usr/bin/env python3
"""Exploratory, fail-closed joined cycle × causal-pivot Fibonacci outcome study.

NOT a validated Elliott wave count. All features are fixed before forward outcomes.
No production changes, no orders, no retroactive promotion.
"""
import csv
import json
import math
import statistics
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path

ROOT = Path("artifacts")
OUT = Path("artifacts/research/cycle_fib_joint_exploratory")
HORIZONS = (5, 10, 20)
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
FIB = 0.618
NEAR_PCT = 0.05

def load_cycle(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def load_market(path, symbols):
    result = defaultdict(dict)
    with path.open(newline="", encoding="utf-8-sig") as f:
        rd = csv.DictReader(f)
        needed = {"symbol", "date", "currency", "open", "high", "low", "close", "adj_close"}
        if not needed.issubset(rd.fieldnames or []):
            raise ValueError("market_schema_mismatch")
        for row in rd:
            if row["symbol"] not in symbols or (row.get("observation_type") and row["observation_type"] != "price_backfill"):
                continue
            try:
                vals = {k: float(row[k]) for k in ("open", "high", "low", "close", "adj_close")}
            except (ValueError, TypeError):
                continue
            if not all(math.isfinite(v) and v > 0 for v in vals.values()):
                continue
            # Only internally coherent unadjusted OHLC; no retroactive adjusted-price mixing.
            key = row["date"]
            if key in result[row["symbol"]]:
                result[row["symbol"]][key] = None  # ambiguous provider rows: exclude date
            else:
                result[row["symbol"]][key] = {"date":key, "currency":row["currency"], **vals}
    return {s: [r for d,r in sorted(rows.items()) if r is not None] for s,rows in result.items()}

def causal_fib(bars, idx):
    """Pivots confirmed using only bars up to idx, excluding current candidate as a pivot."""
    if idx < 65:
        return None
    lows, highs = [], []
    for i in range(PIVOT_LEFT, idx-PIVOT_RIGHT+1):
        left, right = i-PIVOT_LEFT, i+PIVOT_RIGHT+1
        hi,lo = bars[i]["high"],bars[i]["low"]
        if hi > max(bars[j]["high"] for j in range(left,right) if j != i):
            highs.append((i,hi))
        if lo < min(bars[j]["low"] for j in range(left,right) if j != i):
            lows.append((i,lo))
    if not lows or not highs:
        return None
    # Last confirmed alternating low->high upward swing (support retracement).
    high_i,high = highs[-1]
    prior = [(i,v) for i,v in lows if i < high_i]
    if not prior:
        return None
    low_i,low = prior[-1]
    if high <= low:
        return None
    level = high - FIB*(high-low)
    close = bars[idx]["close"]
    if close <= 0:
        return None
    distance = (close-level)/close
    return {"fib_level":level, "distance":distance,
            "near_support": 0 <= distance <= NEAR_PCT,
            "swing_low_date":bars[low_i]["date"],"swing_high_date":bars[high_i]["date"],
            "pivot_confirmed_date":bars[high_i+PIVOT_RIGHT]["date"]}

def main():
    cycle = load_cycle(ROOT/"cycle_history/observations.csv")
    symbols = {r.get("price_symbol","") for r in cycle if r.get("price_symbol")}
    market = load_market(ROOT/"market_data/yahoo_ohlcv.csv",symbols)
    by_asset = defaultdict(list)
    for r in cycle:
        by_asset[r["asset_id"]].append(r)
    rows, exclusions = [], Counter()
    for asset, recs in by_asset.items():
        # Canonical latest run per scan date, never treat same-day reruns as independent.
        byday = {}
        for r in recs:
            d = r["as_of"]
            if d not in byday or (r["generated_at"],r["snapshot_id"]) > (byday[d]["generated_at"],byday[d]["snapshot_id"]):
                byday[d] = r
        previous = None
        for d,r in sorted(byday.items()):
            try:
                value = float(r["cycle"])
            except (ValueError,TypeError):
                exclusions["invalid_cycle"] += 1
                previous = None
                continue
            if r.get("availability") != "PROVISIONAL_REPLAYED" or not 0<=value<=100:
                exclusions["unavailable_cycle"] += 1
                previous = None
                continue
            sym = r["price_symbol"]
            bars = market.get(sym,[])
            # Do not assume the scan date is the feature availability time.
            try:
                generated = datetime.fromisoformat(r["generated_at"].replace("Z","+00:00")).date().isoformat()
            except ValueError:
                exclusions["invalid_generated_at"] += 1
                previous = r
                continue
            eligible = [i for i,b in enumerate(bars) if b["date"] < generated]
            if not eligible:
                exclusions["no_pre_signal_price"] += 1
                previous = r
                continue
            idx = eligible[-1]
            if bars[idx]["currency"] != r.get("currency"):
                exclusions["currency_mismatch"] += 1
                previous = r
                continue
            if r.get("price_basis") not in ("raw","unadjusted","raw_close","RAW","RAW_OHLC","adjusted","adj_close","auto_adjusted","ADJUSTED"):
                # Historical cycle source may use adjusted data: report incompatibility, never silently combine.
                exclusions["price_basis_not_raw"] += 1
                previous = r
                continue
            if r.get("price_basis") in ("adjusted","adj_close","auto_adjusted","ADJUSTED"):
                bars = [{**b, **{k:b[k]*(b["adj_close"]/b["close"]) for k in ("open","high","low","close")}} for b in bars]
            fib = causal_fib(bars,idx)
            if fib is None:
                exclusions["insufficient_causal_pivots"] += 1
                previous = r
                continue
            direction = None
            if previous is not None and previous["price_symbol"]==sym and previous["currency"]==r["currency"]:
                try:
                    old = float(previous["cycle"])
                    direction = "UP" if value>old else ("DOWN" if value<old else "FLAT")
                except (TypeError,ValueError):
                    pass
            for h in HORIZONS:
                # Only future closes strictly after generated date; never count prior or same-day bars.
                future = [b for b in bars[idx+1:] if b["date"] > generated]
                if len(future)<h:
                    exclusions["right_censored_"+str(h)] += 1
                    continue
                ret = future[h-1]["close"]/bars[idx]["close"]-1
                rows.append({"asset_id":asset,"symbol":sym,"as_of":d,"generated_date":generated,
                    "reference_bar":bars[idx]["date"],"horizon":h,"cycle":value,
                    "cycle_direction":direction or "UNKNOWN","cycle_low":value<25,
                    "fib_near_support":fib["near_support"],"fib_distance":round(fib["distance"],6),
                    "fib_level":round(fib["fib_level"],6),"pivot_confirmed_date":fib["pivot_confirmed_date"],
                    "return":ret})
            previous = r
    OUT.mkdir(parents=True,exist_ok=True)
    with (OUT/"joined_events.csv").open("w",newline="") as f:
        if rows:
            w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    comparisons=[]
    for h in HORIZONS:
        sample=[r for r in rows if r["horizon"]==h]
        baseline=[r["return"] for r in sample if r["fib_near_support"]]
        combo=[r["return"] for r in sample if r["fib_near_support"] and r["cycle_low"] and r["cycle_direction"]=="UP"]
        # Observational cohort contrast, NOT a matched ablation or causal uplift.
        comparisons.append({"horizon":h,"all_n":len(sample),"fib_n":len(baseline),
          "fib_plus_cycle_n":len(combo),"fib_mean":statistics.mean(baseline) if baseline else None,
          "fib_plus_cycle_mean":statistics.mean(combo) if combo else None,
          "descriptive_difference":statistics.mean(combo)-statistics.mean(baseline) if combo and baseline else None})
    result={"status":"EXPLORATORY_NOT_PIT_VALIDATED","method":"confirmed_3x3_pivots_618_upward_retracement_raw_close",
      "note":"Not full Elliott counting. Cycle+Fib cohorts are nested, not matched randomized groups. No independent OOS proof.",
      "cycle_rows":len(cycle),"joined_event_rows":len(rows),"exclusions":dict(exclusions),
      "comparisons":comparisons}
    (OUT/"summary.json").write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n")
    print(json.dumps(result,indent=2))
    if not rows:
        print("NO_JOINED_EVENTS: diagnostic generated; no predictive conclusion")
if __name__=="__main__":
    main()
