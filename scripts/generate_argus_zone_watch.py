#!/usr/bin/env python3
"""Publish read-only ARGUS zone research for the entire Scanner universe.

Uses the *same* archived completed daily OHLCV as Scanner_vNext. Never changes
scores, orders, Depot-Watch decisions, or the causal Elliott research modules.
A missing/incomplete security remains explicitly unavailable (fail closed).
"""
from __future__ import annotations

import argparse
import csv
from datetime import date
import hashlib
import html
import json
import math
from pathlib import Path
import sys

from scanner.research.zones_v01 import build_zones, read_rows

VERSION = "argus-zone-watch-v1"
MIN_BARS = 80
MAX_DISPLAY_ZONES = 24


def _number(v):
    try:
        value = float(v)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) and value > 0 else None


def _load_universe(path: Path):
    """Use Scanner identity and documented last *completed* bar, not today's clock."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"price_bar_date", "price_bar_status"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError("scanner_closed_bar_receipt_columns_missing")
        result = {}
        for row in reader:
            symbol = (row.get("yahoo_symbol") or row.get("YahooSymbol")
                      or row.get("ticker") or "").strip()
            if not symbol:
                continue
            if symbol in result:
                raise ValueError("duplicate_scanner_symbol:" + symbol)
            completed = (row.get("price_bar_status") or "").strip().upper()
            asof = (row.get("price_bar_date") or "").strip()
            try:
                if asof:
                    date.fromisoformat(asof)
            except ValueError:
                asof = ""
            result[symbol] = {
                "symbol": symbol,
                "name": (row.get("name") or row.get("Name") or symbol).strip(),
                "scanner_asset_id": (row.get("asset_id") or symbol).strip(),
                "scanner_score": _number_or_zero(row.get("score")),
                "scanner_status": (row.get("score_status") or "").strip(),
                "asof": asof if completed.startswith("CLOSED") else "",
                "closed_bar_status": completed or "UNAVAILABLE",
                "declared_currency": (row.get("currency") or row.get("Currency") or "").strip(),
            }
        if not result:
            raise ValueError("scanner_universe_empty")
        return result


def _number_or_zero(v):
    if v in (None, ""):
        return None
    try:
        value = float(v)
    except (ValueError, TypeError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _prior_contacts(zone, bars, asof):
    """Count distinct completed touch episodes strictly AFTER zone became known.

    Consecutive touched sessions count as a single contact. Never look ahead
    and never count the current as-of session as a previous contact.
    """
    lower, upper = float(zone["lower"]), float(zone["upper"])
    known = zone["known_at"]
    n, touching, last = 0, False, None
    for bar in bars:
        d = bar["date"]
        if d <= known or d >= asof:
            continue
        low, high = _number(bar.get("low")), _number(bar.get("high"))
        hit = low is not None and high is not None and low <= upper and high >= lower
        if hit and not touching:
            n += 1
            last = d
        touching = hit
    return {"episodes": n, "most_recent": last, "confirmed_after": known}


def _wave_counts(zones):
    """Strict, descriptive five-leg candidates from already confirmed pivots.

    Fibonacci does NOT choose pivots. A candidate is not an independently
    certified Elliott count; unknown structures are left unknown.
    """
    by_degree = {}
    for z in zones:
        for driver in z.get("drivers", []):
            if driver.get("family") != "horizontal":
                continue
            for a in driver.get("anchors", []):
                degree = str(a["degree"])
                key = (a["pivot_time"], a["kind"], a["price"])
                by_degree.setdefault(degree, {})[key] = a
    out = []
    for degree, entries in by_degree.items():
        ordered = sorted(entries.values(), key=lambda x: (x["pivot_time"], x["known_at"]))
        for i in range(max(0, len(ordered) - 7), len(ordered) - 5):
            p = ordered[i:i+6]
            if len(p) != 6:
                continue
            k = [x["kind"] for x in p]
            values = [float(x["price"]) for x in p]
            direction = ("down" if k == ["high","low","high","low","high","low"]
                         else "up" if k == ["low","high","low","high","low","high"]
                         else None)
            if direction is None:
                continue
            a,b,c,d,e,f = values
            if direction == "down":
                valid = b < a and b < c < a and d < b and d < e < b and f < d
                w1,w3,w5 = a-b, c-d, e-f
            else:
                valid = b > a and a < c < b and d > b and b < e < d and f > d
                w1,w3,w5 = b-a, d-c, f-e
            if not valid or min(w1,w3,w5) <= 0 or w3 < min(w1,w5)-1e-9:
                continue
            out.append({
                "degree": degree, "direction": direction,
                "status": "confirmed_pivot_geometry_candidate",
                "pivots": [{"date": x["pivot_time"], "known_at": x["known_at"],
                            "kind": x["kind"], "price": round(float(x["price"]), 6)} for x in p],
                "leg_lengths": [round(w1, 6), round(w3, 6), round(w5, 6)],
                "wave2_retracement": round(abs(b-c)/w1, 5),
                "method_note": "hard Elliott overlap/length checks only; not a certified full count",
                "validated_probability": None,
            })
    out.sort(key=lambda w: (w["pivots"][-1]["date"], w["degree"]), reverse=True)
    return out[:4]


def _zone_rows(items, bars, asof, last_close, limit=MAX_DISPLAY_ZONES):
    scored = []
    for z in items:
        row = {k: z.get(k) for k in
               ("zone_id","role","side","timeframe","lower","upper","center",
                "status","known_at","families","independent_anchor_groups")}
        if row["center"] is None:
            row["center"] = (float(row["lower"]) + float(row["upper"])) / 2
        if z.get("drivers"):
            row["families"] = sorted({d["family"] for d in z["drivers"]})
            row["anchors"] = [a for d in z["drivers"] for a in d.get("anchors", [])]
        row["contacts"] = _prior_contacts(z, bars, asof)
        row["distance_pct"] = round(100 * abs(float(row["center"])-last_close) / last_close, 2)
        row["empirical_probability"] = None
        scored.append(row)
    scored.sort(key=lambda z: (z["distance_pct"], z["zone_id"]))
    return scored[:limit]


def calculate(universe, market_csv: Path):
    raw = read_rows(market_csv, set(universe))
    results = []
    for symbol, entry in sorted(universe.items()):
        item = {**entry, "status": "UNAVAILABLE", "reason": None, "currency": None,
                "last_close": None, "data_quality": "blocked", "bar_count": 0,
                "zones": [], "weekly_zones": [], "clusters": [], "plans": [],
                "wave_counts": [], "model_version": None,
                "research_only": True, "empirical_probability": None}
        asof = entry["asof"]
        if not asof:
            item["reason"] = "closed_daily_bar_not_proven"
            results.append(item)
            continue
        bars = sorted((b for b in raw.get(symbol, []) if b["date"] <= asof),
                      key=lambda b: b["date"])
        item["bar_count"] = len(bars)
        if not bars or bars[-1]["date"] != asof:
            item["reason"] = "exact_scanner_closed_session_missing_in_archive"
            results.append(item)
            continue
        if len(bars) < MIN_BARS:
            item["reason"] = "insufficient_archived_history"
            results.append(item)
            continue
        if entry["declared_currency"] and any(
                b.get("currency") != entry["declared_currency"] for b in bars[-450:]):
            item["reason"] = "currency_mismatch"
            results.append(item)
            continue
        try:
            # This is the existing M1–M3 research engine; do not create an
            # alternative score or a second independent price download.
            report = build_zones(bars, symbol, asof=asof,
                                 exchange="unknown", asset_id=entry["scanner_asset_id"])
        except (ValueError, KeyError, TypeError, IndexError, ArithmeticError) as exc:
            item["reason"] = "engine_failed:" + type(exc).__name__ + ":" + str(exc)[:120]
            results.append(item)
            continue
        item["data_quality"] = report["data_quality"]
        item["model_version"] = report["model_version"]
        item["reason"] = (report["warnings"][0] if report["warnings"] else None)
        if report["data_quality"] == "blocked":
            results.append(item)
            continue
        close = _number(report.get("asof_raw_close"))
        if close is None:
            item["reason"] = "nonpositive_last_close"
            results.append(item)
            continue
        item["status"] = "RESEARCH_ONLY"
        item["currency"] = report["currency"]
        item["last_close"] = close
        item["zones"] = _zone_rows(report["zones"], bars, asof, close)
        item["weekly_zones"] = _zone_rows(report.get("weekly_zones", []), bars, asof, close, 12)
        item["clusters"] = [{k: c.get(k) for k in
                             ("cluster_id","role","side","lower","upper","status",
                              "families","independent_anchor_groups","known_at")}
                            for c in report.get("zone_clusters", [])][:30]
        item["plans"] = [{k: p.get(k) for k in
                          ("scenario_id","side","state","activation_rule",
                           "entry_cluster_id","invalidation_cluster_id","target_cluster_ids")}
                         for p in report.get("scenario_plans", [])][:10]
        item["wave_counts"] = _wave_counts(report["zones"])
        item["asof"] = report["last_completed_bar"]
        item["source_hash"] = report["data_hash"]
        results.append(item)
    return results


PAGE = r"""<!doctype html>
<html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ARGUS Wellen &amp; Zonen</title>
<style>
:root{color-scheme:dark;font-family:system-ui,Segoe UI,sans-serif;background:#0c111a;color:#e7edf5}
body{margin:0}header{padding:16px 20px;border-bottom:1px solid #2a3644}
h1{font-size:22px;margin:4px 0}a{color:#9cc7ff}.muted,small{color:#9daec0}
main{display:grid;grid-template-columns:minmax(235px,300px) 1fr;max-width:1320px;margin:auto;gap:20px;padding:20px}
aside,article{background:#141d29;border:1px solid #2b394a;border-radius:12px;padding:16px}
input{background:#09111c;color:#fff;border:1px solid #3c4d61;border-radius:8px;padding:12px;width:100%;box-sizing:border-box;font:inherit}
#items{max-height:75vh;overflow:auto;margin-top:10px}
.item{width:100%;border:0;background:transparent;text-align:left;padding:12px;border-bottom:1px solid #263345;color:inherit;cursor:pointer}
.item:hover,.item.active{background:#20324a}.item span{display:block;color:#a9b8ca;font-size:12px}
h2{font-size:20px}h3{font-size:15px;margin-top:22px}table{width:100%;border-collapse:collapse;font-size:13px}
td,th{border-bottom:1px solid #2b394a;text-align:left;padding:9px 7px;vertical-align:top}
th{color:#a9b8ca}td.num{text-align:right;font-variant-numeric:tabular-nums}
.tag{color:#bad1e8;background:#263545;border-radius:5px;padding:3px 7px;font-size:12px;display:inline-block;margin:3px 0}
.notice{border-left:3px solid #b48c50;padding:10px 13px;background:#2a2530;color:#f5dcae}
.empty{padding:15px;color:#aab9ca}.cards{display:flex;gap:14px;flex-wrap:wrap}
.kpi{border:1px solid #304357;border-radius:9px;padding:11px;min-width:135px}.kpi strong{display:block;font-size:20px}
.wide{overflow:auto}p{line-height:1.5}
@media(max-width:760px){main{display:block;padding:12px}aside{margin-bottom:14px}#items{max-height:220px}}
</style></head><body>
<header><a href="index.html">← Scanner_vNext</a><h1>ARGUS · Elliott &amp; Fibonacci-Zonen</h1>
<div class="muted" id="headline"></div></header>
<main><aside><input id="search" type="search" placeholder="Aktie suchen (UEC, XPEV, WPM…)">
<div id="items"></div></aside><article id="detail" aria-live="polite"></article></main>
<script id="ARGUS_DATA" type="application/json">__ZONES_DATA__</script>
<script>
"use strict";
const data=JSON.parse(document.getElementById("ARGUS_DATA").textContent);
const rows=data.assets||[];let selected=rows.find(x=>x.symbol==="UEC")?.symbol||rows[0]?.symbol;
const esc=x=>String(x??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmt=x=>Number.isFinite(Number(x))&&x!==null&&x!==""?Number(x).toLocaleString("de-DE",{maximumFractionDigits:3}):"–";
const txt=x=>esc(x??"–");
function table(head,records){
  if(!records.length)return '<p class="muted">Keine belastbare Zone aus den verfügbaren, bestätigten Daten.</p>';
  return '<div class="wide"><table><thead><tr>'+head.map(t=>'<th>'+esc(t)+'</th>').join("")+'</tr></thead><tbody>'+
    records.join("")+'</tbody></table></div>';
}
function drawList(){
  const q=document.getElementById("search").value.toLowerCase().trim();
  document.getElementById("items").innerHTML=rows.filter(x=>(x.symbol+" "+x.name).toLowerCase().includes(q))
     .map(x=>'<button class="item '+(selected===x.symbol?"active":"")+'" data-symbol="'+esc(x.symbol)+'">'+
       '<b>'+esc(x.symbol)+'</b> · '+esc(x.name)+'<span>'+esc(x.status)+' · '+esc(x.asof||"kein abgeschlossener Bar")+'</span></button>').join("");
}
function drawDetail(){
  const x=rows.find(x=>x.symbol===selected);
  if(!x)return;
  const tag=(x.status==="RESEARCH_ONLY")?"Forschungszonen, nicht validiert":"Keine zuverlässigen Zonen";
  let out='<h2>'+esc(x.name)+' ('+esc(x.symbol)+')</h2><div class="notice"><b>'+tag+
      '</b><p>Keine Handelssignale, keine kalibrierte Trefferwahrscheinlichkeit. Zonen basieren auf bestätigten Kurs-Pivots, nicht auf nachträglich gewählten Screenshot-Punkten. Angezeigter Scanner-Score bleibt unabhängig.</p></div>';
  out+='<div class="cards"><div class="kpi"><small>Letzter abgeschlossener Bar</small><strong>'+txt(x.asof)+'</strong></div>'+
    '<div class="kpi"><small>Schlusskurs</small><strong>'+fmt(x.last_close)+' '+txt(x.currency)+'</strong></div>'+
    '<div class="kpi"><small>Historische Bars</small><strong>'+fmt(x.bar_count)+'</strong></div>'+
    '<div class="kpi"><small>Scanner-Score (unverändert)</small><strong>'+fmt(x.scanner_score)+'</strong></div></div>';
  if(x.status!=="RESEARCH_ONLY"){
    out+='<p class="notice">Berechnung blockiert: '+txt(x.reason)+'</p>';
  }else{
    out+='<p class="muted">Datenqualität: '+txt(x.data_quality)+' · Modell: '+txt(x.model_version)+
    '. Historische Anpassungen sind nur eine rekonstruierte Forschungsbasis und kein vollständiger historischer Point-in-Time-Nachweis.</p>';
    const zoneLine=z=>'<tr><td>'+txt(z.role)+' / '+txt(z.side)+'</td><td class="num">'+fmt(z.lower)+'–'+fmt(z.upper)+'</td>'+
       '<td>'+txt(z.status)+'</td><td class="num">'+fmt(z.distance_pct)+' %</td><td>'+fmt(z.contacts?.episodes)+'</td>'+
       '<td>'+txt((z.families||[]).join(", "))+'</td></tr>';
    const cols=["Art","Preiszone","Zustand","Distanz","Frühere Kontakte","Herleitung"];
    out+='<h3>Tageszonen · nächstliegende zuerst</h3>'+table(cols,(x.zones||[]).map(zoneLine));
    out+='<h3>Wochenzonen · bestätigte Wochen</h3>'+table(cols,(x.weekly_zones||[]).map(zoneLine));
    out+='<h3>Regelkonforme Elliott-Fünferkandidaten</h3>';
    if(!x.wave_counts?.length)out+='<p class="muted">Kein vollständiger fünfteiliger Kandidat anhand der bestätigten Struktur-Pivots. Keine Wellenzählung erzwungen.</p>';
    else for(const w of x.wave_counts){
      out+='<p><span class="tag">'+txt(w.degree)+' · '+txt(w.direction)+'</span> W1/W3/W5: '+
           (w.leg_lengths||[]).map(fmt).join(" / ")+' · W2-Rücklauf: '+fmt(100*w.wave2_retracement)+
           ' %<br><small>'+w.pivots.map(p=>txt(p.date)+": "+fmt(p.price)).join(" → ")+'</small></p>';
    }
    out+='<h3>Bedingte Szenarien · nur zukünftige Bestätigungen</h3>';
    out+=table(["Richtung","Status","Bestätigungsregel"],(x.plans||[]).map(p=>
      '<tr><td>'+txt(p.side)+'</td><td>'+txt(p.state)+'</td><td>'+txt(p.activation_rule)+'</td></tr>'));
  }
  out+='<p class="muted">Piekarski-ähnliche W2-Fibonacci-Rückläufe (38,2–61,8 %) sind nur als Forschungshypothese gekennzeichnet; kein belegter proprietärer Signalabgleich. Für Langfristzählungen über 450 Tageskerzen und Intraday-Wellen fehlen in diesem Modul noch die entsprechenden Daten-/Modellfreigaben.</p>';
  document.getElementById("detail").innerHTML=out;
}
document.getElementById("headline").textContent=data.version+" · "+data.count+" Titel · "+data.available+" mit Forschungszonen · zuletzt abgeschlossene Tageskerzen";
document.getElementById("search").addEventListener("input",drawList);
document.getElementById("items").addEventListener("click",e=>{const b=e.target.closest("button[data-symbol]");if(b){selected=b.dataset.symbol;drawList();drawDetail();}});
drawList();drawDetail();
</script></body></html>"""


def write_outputs(assets, *, out_json: Path, out_html: Path, history_dir: Path):
    output = {"version": VERSION, "count": len(assets),
              "available": sum(a["status"] == "RESEARCH_ONLY" for a in assets),
              "research_only": True, "score_modified": False,
              "assets": assets}
    encoded = json.dumps(output, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False)
    content = encoded + "\n"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)
    out_json.write_text(content, encoding="utf-8")
    # Escape '<' so a company name cannot terminate the inline script element.
    safe_json = encoded.replace("<", "\\u003c").replace("&", "\\u0026")
    out_html.write_text(PAGE.replace("__ZONES_DATA__", safe_json), encoding="utf-8")
    latest_asof = max((x.get("asof") or "0000-00-00" for x in assets))
    fingerprint = hashlib.sha256(content.encode()).hexdigest()[:12]
    historical = history_dir / f"{latest_asof}_{fingerprint}.json"
    if not historical.exists():
        historical.write_text(content, encoding="utf-8")
    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build complete read-only ARGUS multi-symbol zone dashboard")
    parser.add_argument("--watchlist", type=Path, default=Path("artifacts/watchlist/watchlist_ALL.csv"))
    parser.add_argument("--market", type=Path, default=Path("artifacts/market_data/yahoo_ohlcv.csv"))
    parser.add_argument("--json", type=Path, default=Path("artifacts/reports/argus_zones_all.json"))
    parser.add_argument("--html", type=Path, default=Path("artifacts/ui/zones.html"))
    parser.add_argument("--history", type=Path, default=Path("artifacts/zones/history"))
    args = parser.parse_args(argv)
    universe = _load_universe(args.watchlist)
    results = calculate(universe, args.market)
    published = write_outputs(results, out_json=args.json, out_html=args.html,
                              history_dir=args.history)
    print(json.dumps({"version": VERSION, "count": published["count"],
                      "research_zone_available": published["available"],
                      "unavailable": published["count"]-published["available"],
                      "html": str(args.html), "json": str(args.json)},
                     sort_keys=True))


if __name__ == "__main__":
    main()
