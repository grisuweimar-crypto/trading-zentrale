"""Causal, research-only ARGUS zones from independent 10-year monthly and 4h bars.

Never select pivots using Fibonacci or imply that technical geometry is a
validated trading strategy. Use only completed provider bars with an explicit
available-from date. Existing Scanner scores are unaffected.
"""
from __future__ import annotations

from datetime import date
import hashlib
import json
import math

from scanner.research.elliott_vnext.scenarios import evaluate_motive_window, evaluate_correction_window

VERSION = "argus-multiscale-v1-research"
COLUMNS = ("symbol","currency","timeframe","bar_start","confirmed_at",
           "open","high","low","close","adj_close","volume","source","retrieved_at","session_tail")
RETRACEMENTS = (0.382, 0.500, 0.618, 0.786, 0.887)
EXTENSIONS = (1.000, 1.272, 1.618)
SPECS = {
    "monthly": {"minimum": 36, "max_stale_days": 75,
                "degree": "primary", "left": 2, "right": 2, "atr": 10, "excursion": 0.70},
    "four_hour": {"minimum": 40, "max_stale_days": 14,
                  "degree": "minor", "left": 3, "right": 3, "atr": 14, "excursion": 0.65},
}


def _float(value, *, positive=True):
    try:
        v = float(value)
    except (ValueError, TypeError, OverflowError):
        raise ValueError("invalid_numeric_ohlcv") from None
    if not math.isfinite(v) or (v <= 0 if positive else v < 0):
        raise ValueError("invalid_numeric_ohlcv")
    return v


def _id(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:20]


def read_bars(path, symbols):
    """Streaming adapter: never silently merge duplicate provider observations."""
    import csv
    wanted = set(symbols)
    out = {(symbol, tf): [] for symbol in wanted for tf in SPECS}
    if not path.is_file():
        return out
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not set(COLUMNS).issubset(reader.fieldnames or []):
            raise ValueError("multiscale_cache_schema_mismatch")
        for row in reader:
            key = (row["symbol"], row["timeframe"])
            if key in out:
                out[key].append(row)
    return out


def _validated(rows, symbol, timeframe, asof):
    cutoff = date.fromisoformat(asof)
    selected, seen = [], set()
    for original in sorted(rows, key=lambda r: r.get("bar_start","")):
        if original.get("symbol") != symbol or original.get("timeframe") != timeframe:
            raise ValueError("multi_timeframe_identity_mismatch")
        start = str(original.get("bar_start", ""))
        confirmed = date.fromisoformat(str(original.get("confirmed_at", "")))
        if confirmed > cutoff:
            continue
        if start in seen:
            raise ValueError("duplicate_multiscale_bars")
        seen.add(start)
        if timeframe == "monthly":
            dt = date.fromisoformat(start)
            if dt.day != 1 or not dt < confirmed:
                raise ValueError("invalid_monthly_causality")
        else:
            from datetime import datetime
            dt = datetime.fromisoformat(start)
            if dt.tzinfo is None or dt.date() > confirmed:
                raise ValueError("invalid_four_hour_timestamp")
        item = dict(original)
        for field in ("open","high","low","close","volume"):
            item[field] = _float(original.get(field), positive=(field != "volume"))
        adj = original.get("adj_close")
        item["adj_close"] = _float(adj) if adj not in ("",None) else None
        if item["low"] > min(item["open"],item["close"]) + 1e-7:
            raise ValueError("inconsistent_low")
        if item["high"] < max(item["open"],item["close"]) - 1e-7:
            raise ValueError("inconsistent_high")
        if item["high"] < item["low"]:
            raise ValueError("reversed_high_low")
        item["confirmed_at"] = confirmed.isoformat()
        selected.append(item)
    if selected and len({r["currency"] for r in selected}) != 1:
        raise ValueError("mixed_currencies")
    return selected


def _adjust_to_current_raw(rows):
    """Use one consistent split-adjusted price scale, anchored to latest raw."""
    if any(r["adj_close"] is None for r in rows):
        raise ValueError("adjusted_history_incomplete")
    last = rows[-1]
    factor = last["close"] / last["adj_close"]
    out = []
    for row in rows:
        adj_ratio = row["adj_close"] / row["close"]
        r = dict(row)
        for c in ("open","high","low","close"):
            r[c] = row[c] * adj_ratio * factor
        out.append(r)
    return out


def _pivots(rows, conf):
    left,right,atr_len=conf["left"],conf["right"],conf["atr"]
    tr = []
    for i,r in enumerate(rows):
        prev=rows[i-1]["close"] if i else r["close"]
        tr.append(max(r["high"]-r["low"],abs(r["high"]-prev),abs(r["low"]-prev)))
    avgs = [None]*len(rows)
    for i in range(atr_len-1,len(rows)):
        avgs[i] = sum(tr[i-atr_len+1:i+1])/atr_len
    pivots=[]
    for i in range(max(left,atr_len-1),len(rows)-right):
        w = rows[i-left:i+right+1]
        p = rows[i]
        atr=avgs[i]
        if atr is None or atr<=0:
            continue
        hi = p["high"] > max(q["high"] for q in w if q is not p)
        lo = p["low"] < min(q["low"] for q in w if q is not p)
        # If the same bar spans both extrema, intrabar order is unknowable.
        if hi and lo:
            continue
        if hi and (p["high"] - min(q["low"] for q in rows[i-left:i+1])) >= atr*conf["excursion"]:
            pivots.append({"kind":"high","price":p["high"],
                           "pivot_start":p["bar_start"],"known_at":rows[i+right]["confirmed_at"],
                           "degree":conf["degree"],"atr":atr})
        if lo and (max(q["high"] for q in rows[i-left:i+1])-p["low"]) >= atr*conf["excursion"]:
            pivots.append({"kind":"low","price":p["low"],
                           "pivot_start":p["bar_start"],"known_at":rows[i+right]["confirmed_at"],
                           "degree":conf["degree"],"atr":atr})
    selected=[]
    for p in pivots:
        if selected and selected[-1]["kind"]==p["kind"]:
            old=selected[-1]
            if (p["price"]>old["price"] if p["kind"]=="high" else p["price"]<old["price"]):
                selected[-1]=p
        else:
            selected.append(p)
    return selected


def _zone(kind, direction, value, source, rows, atr, *, level=None):
    if not math.isfinite(value) or value<=0:
        return None
    # Parameter frozen as an unvalidated research convention, not a confidence score.
    width=min(max(0.30*atr, 0.003*value), 0.025*value)
    lo=value-width
    if lo<=0:
        return None
    known=max(p["known_at"] for p in source)
    current=rows[-1]["close"]
    status=("broken" if direction=="support" and current < lo else
            "broken" if direction=="resistance" and current > value+width else
            "watch")
    contacts=0
    touched=False
    for r in rows:
        if r["confirmed_at"] <= known or r is rows[-1]:
            continue
        hit = r["low"] <= value+width and r["high"] >= lo
        if hit and not touched:
            contacts += 1
        touched=hit
    if status=="watch" and rows[-1]["low"] <= value+width and rows[-1]["high"] >= lo and known < rows[-1]["confirmed_at"]:
        status="touched"
    source_anchors=[{"start":p["pivot_start"],"confirmed_at":p["known_at"],
                     "kind":p["kind"],"price":round(p["price"],6)} for p in source]
    identity={"kind":kind,"direction":direction,"price":round(value,8),
              "anchors":source_anchors,"level":level}
    return {"zone_id":"AM-"+_id(identity),"family":kind,"role":direction,
            "lower":round(lo,6),"upper":round(value+width,6),
            "center":round(value,6),"status":status,"known_at":known,
            "level":level,"anchors":source_anchors,"previous_contacts":contacts,
            "empirical_probability":None,"evidence_status":"not_validated"}


def _wave_hypotheses(pivots, limit=4):
    out=[]
    for i in range(max(0,len(pivots)-12),len(pivots)-5):
        window=pivots[i:i+6]
        if len(window)!=6:
            continue
        compatible=[{"kind":p["kind"],"price":p["price"]} for p in window]
        r=evaluate_motive_window(compatible)
        if not r.get("valid") or r.get("stage")!="wave_5_complete":
            continue
        out.append({"direction":r["direction"],"stage":r["stage"],
                    "pattern":"possible_impulse","truncated_fifth":r["truncated_fifth"],
                    "pivots":[{"start":p["pivot_start"],"price":round(p["price"],6),
                               "confirmed_at":p["known_at"]} for p in window],
                    "wave_lengths":{k:round(v,6) for k,v in r["wave_lengths"].items()},
                    "evidence_status":"unvalidated_structural_candidate",
                    "empirical_probability":None})
    return out[-limit:]


def analyze(rows, symbol, timeframe, *, asof):
    """Safe research result; no fabricated level if unavailable/stale/unverified."""
    if timeframe not in SPECS:
        raise ValueError("unsupported_timeframe")
    conf=SPECS[timeframe]
    result={"symbol":symbol,"timeframe":timeframe,"asof":asof,
            "status":"UNAVAILABLE","reason":None,"bars":0,"last_confirmed":None,
            "currency":None,"last_close":None,"session_tail_bars":0,"zones":[],"waves":[],
            "price_history":[],"research_only":True,"empirical_probability":None,
            "version":VERSION}
    try:
        complete=_validated(rows,symbol,timeframe,asof)
        result["bars"]=len(complete)
        if len(complete)<conf["minimum"]:
            result["reason"]="insufficient_completed_bars"
            return result
        if (date.fromisoformat(asof)-date.fromisoformat(complete[-1]["confirmed_at"])).days>conf["max_stale_days"]:
            result["reason"]="stale_market_bars"
            return result
        adjusted=_adjust_to_current_raw(complete)
        anchors=_pivots(adjusted,conf)
        last=adjusted[-1]
        result.update({"status":"RESEARCH_ONLY","currency":last["currency"],
                       "last_close":round(last["close"],6),
                       "last_confirmed":last["confirmed_at"]})
        result["session_tail_bars"]=sum(r.get("session_tail")=="1" for r in adjusted)
        result["price_history"]=[{"date":r["bar_start"],"close":round(r["close"],6)}
                                 for r in adjusted[-160:]]
        atrsum=[]
        for i,r in enumerate(adjusted[-14:]):
            prev=adjusted[-15+i]["close"] if len(adjusted)>=15 else r["close"]
            atrsum.append(max(r["high"]-r["low"],abs(r["high"]-prev),abs(r["low"]-prev)))
        atr=sum(atrsum)/len(atrsum)
        zones=[]
        for p in anchors[-12:]:
            zones.append(_zone("horizontal","support" if p["kind"]=="low" else "resistance",
                               p["price"],[p],adjusted,atr))
        for a,b in list(zip(anchors,anchors[1:]))[-3:]:
            if a["kind"]==b["kind"]:
                continue
            length=abs(b["price"]-a["price"])
            if length<0.5*atr:
                continue
            upwards=a["kind"]=="low"
            sign=1 if upwards else -1
            for f in RETRACEMENTS:
                center=b["price"]-sign*f*length
                zones.append(_zone("fib_retracement","support" if upwards else "resistance",
                                   center,[a,b],adjusted,atr,level=f))
        for a,b,c in list(zip(anchors,anchors[1:],anchors[2:]))[-3:]:
            if a["kind"]!=c["kind"] or a["kind"]==b["kind"]:
                continue
            sign=1 if a["kind"]=="low" else -1
            length=abs(b["price"]-a["price"])
            if length<0.5*atr:
                continue
            for f in EXTENSIONS:
                zones.append(_zone("abc_projection","target",c["price"]+sign*f*length,
                                   [a,b,c],adjusted,atr,level=f))
        result["zones"]=sorted((z for z in zones if z),key=lambda z:(
            abs(z["center"]-last["close"]),z["zone_id"]))[:30]
        result["waves"]=_wave_hypotheses(anchors)
        result["pivot_count"]=len(anchors)
    except (ValueError, TypeError, KeyError, IndexError, OverflowError, ArithmeticError) as exc:
        result.update({"status":"UNAVAILABLE","reason":f"input_or_model_blocked:{type(exc).__name__}:{exc}",
                       "zones":[],"waves":[],"price_history":[]})
    return result
