"""Outcome-blind robustness of actual archived cycle inputs to earlier captures.

No independent PIT certification. Three distinct comparisons:
SINGLE_RESPONSE: all 60 dates from one earlier successful response.
MULTI_RESPONSE: latest captured value for each date, all 60 present.
PARTIAL_HYBRID: only observed replacements, other dates left unchanged.
The latter two are constructed sensitivity scenarios, not provider vintages.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date
import json
from pathlib import Path
import numpy as np
import pandas as pd

from scripts.audit_cycle_client_vintage import capture_events, load_windows
from scanner.data.enrich.cycle_oscillator import calculate_cycle
from scanner.data.enrich.yahoo_prices import _looks_like_crypto_pair
from scanner.reports.cycle_history import ROOT, lag_mask, read_ledger, utc
from scanner.reports.cycle_prefetch_evidence import ARCHIVE_DIR, inspect_archived_witnesses
from scanner.research.pattern_discovery.cycle_cy05_adapter import inspect_cycle_archive

MODES = ("SINGLE_RESPONSE", "MULTI_RESPONSE", "PARTIAL_HYBRID")


def band(v):
    return sum(v >= x for x in (25, 50, 75))


def direction(a, b):
    return "UP" if a > b else "DOWN" if a < b else "FLAT"


def summary(values):
    if not values:
        return {"n": 0}
    a = np.asarray(values, dtype=float)
    return {"n": len(a), "median": float(np.median(a)),
            "p90": float(np.quantile(a, .9)), "p99": float(np.quantile(a, .99)),
            "max": float(np.max(a))}


def calc(row, days, values):
    result = calculate_cycle(pd.Series(values, index=pd.to_datetime(days)),
                             symbol=row["price_symbol"], currency=row["currency"],
                             is_crypto=_looks_like_crypto_pair(row["price_symbol"]),
                             as_of=utc(row["cycle_as_of"]))
    if result["cycle_quality"] != "VALID":
        return None
    return float(result["Zyklus %"])


def compare(row, days, old, observed, mode):
    """Never fill missing dates in the two full-coverage comparison modes."""
    n = sum(d in observed for d in days)
    if (mode != "PARTIAL_HYBRID" and n != 60) or not n:
        return None
    if mode == "PARTIAL_HYBRID" and n == 60:
        return None
    new = [observed.get(d, v) for d,v in zip(days, old)]
    ratios = [observed[d]/v for d,v in zip(days, old) if d in observed]
    bps = [abs(x-1)*10000 for x in ratios if x != 1]
    spread = (max(ratios)/min(ratios)-1)*10000
    base = float(row["cycle"])
    changed = calc(row, days, new)
    return {"mode": mode, "captured": n, "changed_bars": len(bps),
            "abs_diff_bps": summary(bps), "ratio_spread_bps": spread,
            "uniform_scale_1bp_on_captured_subset": spread <= 1,
            "old_cycle": base, "new_cycle": changed,
            "abs_cycle_delta": None if changed is None else abs(changed-base),
            "band_changed": None if changed is None else band(base) != band(changed),
            "lt25_changed": None if changed is None else (base < 25) != (changed < 25)}


def audit(root):
    root = Path(root)
    meta = inspect_cycle_archive(root)
    ledger = read_ledger(root / ROOT / "observations.csv")
    inspect_archived_witnesses(root, ledger)
    # Keep each actual response separate, even if its finish timestamp coincides.
    events = []
    for p in sorted((root / ARCHIVE_DIR).glob("*.json")):
        doc = json.loads(p.read_text())
        for i, attempt in enumerate(doc["witness"]["attempts"]):
            sub = dict(doc, witness={"attempts":[attempt]})
            groups = defaultdict(dict)
            bad = set()
            for t,s,d,v in capture_events(sub):
                if d in groups[s] and groups[s][d] != v:
                    bad.add(s)
                groups[s][d] = v
            for s, vals in groups.items():
                if s not in bad:
                    events.append((utc(attempt["finished_at_utc"]),p.stem,i,s,vals))
    events.sort(key=lambda x:x[:4])
    ordered = sorted((r for r in ledger if r["availability"]=="PROVISIONAL_REPLAYED"),
                     key=lambda r:(utc(r["cycle_as_of"]),r["snapshot_id"],r["asset_id"]))
    pos=0
    latest=defaultdict(dict)
    responses=defaultdict(list)
    prior=None
    groups={}
    result=[]
    discrepancies=[]
    bins=Counter()
    for row in ordered:
        cutoff=utc(row["cycle_as_of"])
        while pos<len(events) and events[pos][0]<=cutoff:
            t,sid,i,s,vals=events[pos]
            latest[s].update(vals)
            responses[s].append((t,sid,i,vals))
            pos+=1
        sid=row["snapshot_id"]
        if sid!=prior:
            groups=load_windows(root,sid)
            prior=sid
        symbol=row["price_symbol"]
        bars=sorted(groups[(symbol,row["currency"],row["price_sha256"])],
                    key=lambda b:b["session_date"])
        days=[b["session_date"] for b in bars]
        old=[float(b["close"]) for b in bars]
        if (len(days)!=60 or len(set(days))!=60 or
            any(b["as_of"]!=row["cycle_as_of"] or b["formula_version"]!=row["formula"] for b in bars)
            or calc(row,days,old)!=float(row["cycle"])):
            raise ValueError("archived_cycle_replay_mismatch:"+sid+":"+row["asset_id"])
        # Latest prior value, not a hindsight-selected nearest match.
        for d,v in zip(days,old):
            if d not in latest[symbol]:
                bins["not_captured"]+=1
                continue
            bps=abs(latest[symbol][d]/v-1)*10000
            if latest[symbol][d]==v:
                bins["exact"]+=1
            else:
                discrepancies.append(bps)
                bins["le_0_1bp" if bps<=.1 else "le_1bp" if bps<=1 else
                     "le_10bp" if bps<=10 else "le_100bp" if bps<=100 else "gt_100bp"]+=1
        entry={"snapshot_id":sid,"asset_id":row["asset_id"],"as_of":row["as_of"],
               "comparisons":{}}
        complete=next((x for x in reversed(responses[symbol])
                       if all(d in x[3] for d in days)),None)
        if complete:
            c=compare(row,days,old,complete[3],"SINGLE_RESPONSE")
            c.update(response_finished_at=complete[0].isoformat(),
                     response_snapshot_id=complete[1],response_attempt_index=complete[2])
            entry["comparisons"]["SINGLE_RESPONSE"]=c
        mode="MULTI_RESPONSE" if all(d in latest[symbol] for d in days) else "PARTIAL_HYBRID"
        c=compare(row,days,old,latest[symbol],mode)
        if c:
            entry["comparisons"][mode]=c
        result.append(entry)
    # Direction comparisons only across existing canonical provisional chains.
    # For FULL modes every intermediate observation must have a valid comparison
    # of that same mode. Partial scenarios are not used as direction evidence.
    bykey={(x["snapshot_id"],x["asset_id"]):x for x in result}
    canonical={}
    for row in ledger:
        key=(utc(row["generated_at"]),row["snapshot_id"])
        day=row["as_of"]
        if day not in canonical or key>canonical[day]:
            canonical[day]=key
    dates=sorted(canonical)
    idx={d:i for i,d in enumerate(dates)}
    masks=lag_mask(ledger)
    lag_results={}
    for mode in MODES[:2]:
        lag_results[mode]={}
        for n in (1,5,10):
            counts=Counter()
            for mask in masks:
                if mask[f"lag_{n}obs"]!="PROVISIONAL_CHAIN":
                    continue
                i=idx[mask["as_of"]]
                chain=[bykey.get((canonical[d][1],mask["asset_id"]),{}).get("comparisons",{}).get(mode)
                       for d in dates[i-n:i+1]]
                if len(chain)!=n+1 or any(c is None or c["new_cycle"] is None for c in chain):
                    continue
                a,b=chain[-1],chain[0]
                olddir=direction(a["old_cycle"],b["old_cycle"])
                newdir=direction(a["new_cycle"],b["new_cycle"])
                counts["chains"]+=1
                counts["direction_changed"]+=olddir!=newdir
                counts["lt25_up_changed"]+=((a["old_cycle"]<25 and olddir=="UP")!=
                                            (a["new_cycle"]<25 and newdir=="UP"))
            lag_results[mode][str(n)]={k:counts[k] for k in
                                      ("chains","direction_changed","lt25_up_changed")}
    mode_stats={}
    for mode in MODES:
        cs=[e["comparisons"][mode] for e in result if mode in e["comparisons"]]
        good=[c for c in cs if c["new_cycle"] is not None]
        mode_stats[mode]={"windows":len(cs),"recomputed":len(good),
                          "invalid_recomputation":len(cs)-len(good),
                          "windows_with_changed_prices":sum(c["changed_bars"]>0 for c in cs),
                          "uniform_scale_1bp_subset":sum(c["uniform_scale_1bp_on_captured_subset"] for c in cs),
                          "cycle_delta_points":summary([c["abs_cycle_delta"] for c in good]),
                          "band_changes":sum(c["band_changed"] for c in good),
                          "lt25_changes":sum(c["lt25_changed"] for c in good)}
    return {"schema_version":"cycle_vintage_robustness_v1",
            "archive_latest_snapshot_id":meta["latest_snapshot_id"],
            "manifest_hashes":meta["manifest_file_hashes"],
            "archived_windows_replayed":len(result),"latest_capture_bar_bins":dict(bins),
            "latest_capture_discrepancy_bps":summary(discrepancies),
            "comparison_modes":mode_stats,"direction_comparisons":lag_results,
            "research_released":False,"independent_pit_verified":False,
            "outcomes_inspected":False,"rows":result}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo-root",type=Path,default=Path("."))
    p.add_argument("--output",type=Path,required=True)
    args=p.parse_args()
    result=audit(args.repo_root)
    args.output.write_text(json.dumps(result,indent=2,sort_keys=True,allow_nan=False)+"\n")
    print(json.dumps({k:v for k,v in result.items() if k!="rows"},indent=2))


if __name__=="__main__":
    main()
