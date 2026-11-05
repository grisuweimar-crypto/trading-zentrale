#!/usr/bin/env python3
"""Read-only, point-in-time-conscious comparison of public Radar candidates and ARGUS.

Radar discovery must remain separate from the approved scanner universe.
The bridge neither adds assets nor changes weights, scores, probabilities, orders
or Decision/Depot-Watch semantics. Records unanswered questions for a future,
pre-registered out-of-sample effectiveness test, not a fabricated success rate.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date
from hashlib import sha256
import json
from pathlib import Path

VERSION="radar_zone_bridge_v1"

PAGE=r"""<!doctype html><html lang="de"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ARGUS · Kandidatenradar gegen Scanner</title>
<style>
:root{color-scheme:dark;font-family:system-ui,Segoe UI,Arial;background:#0d1420;color:#e5eff7}body{margin:0}
header{border-bottom:1px solid #344252;padding:16px 22px}a{color:#a1cbff}main{padding:18px;max-width:1250px;margin:auto}
h1{font-size:22px}h2{font-size:18px;margin-top:22px}input,select{background:#152232;color:#e5eff7;border:1px solid #3a4d62;border-radius:7px;padding:10px;max-width:100%;font:inherit}
p{line-height:1.5}.muted{color:#a8bbcb}.stats{display:flex;gap:12px;flex-wrap:wrap}
.stat{border:1px solid #334455;background:#152232;padding:13px;border-radius:9px;min-width:130px}
.stat strong{font-size:22px;display:block}.note{border-left:3px solid #b09b68;padding:12px;background:#20252f}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:10px 7px;border-bottom:1px solid #304153;vertical-align:top}
th{color:#aabbd0}.wide{overflow:auto}.tag{background:#203044;padding:3px 7px;border-radius:6px;display:inline-block}
</style></head><body>
<header><a href="index.html">← Scanner</a> · <a href="zones.html">Wellen &amp; Zonen</a><h1>ARGUS · Kandidatenradar &amp; Scanner</h1>
<span class="muted">Forschungsvergleich – keine Aufnahme, keine Kaufempfehlung</span></header>
<main><div class="stats" id="stats"></div><p class="note">Das Radar entdeckt externe Ideen, der Scanner bewertet zugelassene Werte, die Zonenanalyse prüft mögliche Kursbereiche. Ein Radar-Titel wird durch diesen Vergleich nicht automatisch zur Watchlist hinzugefügt. Ein Quellenrang ist keine Kauf-/Verkaufsprognose.</p>
<h2>Abgleich der dokumentierten Kandidaten</h2>
<label><input id="query" type="search" placeholder="Ticker oder Name durchsuchen…"></label>
<label><select id="filter"><option value="all">Alle Kandidaten</option><option value="inside">Im Scanner</option><option value="outside">Nur Radar</option><option value="review">Ungeklärte Identität / Quelle</option></select></label>
<div class="wide"><table><thead><tr><th>Titel</th><th>Abdeckung</th><th>Quelle</th><th>ARGUS-Zonen</th><th>Prüfbedarf</th></tr></thead><tbody id="rows"></tbody></table></div>
<h2>Wie beurteilen wir künftig den Nutzen?</h2>
<p>Vergleich der Stichproben <b>Radar allein</b>, <b>Scanner allein</b> und <b>beide</b> mit identischen Kursdaten und festgelegten Folgehorizonten (5, 20 und 60 Handelstage), einschließlich Transaktionskosten, Drawdown, Preiszonen-Treffern, Out-of-Sample und Negativkontrollen. Bewertungen erst nach ausgereiften Beobachtungen und geprüften Zeitstempeln. Bis dahin keine behauptete Erfolgsquote.</p>
<p class="muted">Ein identischer Ticker beweist nur eine Zeichenübereinstimmung; Identität, Primärnotierung, Währung und Prospektiv-Zeitpunkt müssen gesondert geprüft werden. Datenstatus pro Eintrag beachten.</p>
</main><script id="RADAR_DATA" type="application/json">__RADAR_DATA__</script><script>
"use strict";
const d=JSON.parse(document.getElementById("RADAR_DATA").textContent);
const esc=x=>String(x??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const cells=x=>'<td>'+esc(x)+'</td>';
document.getElementById('stats').innerHTML=[
['Radartitel',d.count],['Im Scanner',d.in_scanner],['Nur Radar',d.radar_only],['Mit Tageszonen',d.daily_zones_available],['Mit Monatszonen',d.monthly_zones_available],['Mit 4-Stunden-Zonen',d.four_hour_zones_available]
].map(([k,v])=>'<div class="stat"><span class="muted">'+esc(k)+'</span><strong>'+v+'</strong></div>').join("");
function draw(){
 const term=document.getElementById('query').value.toLowerCase().trim(),fil=document.getElementById('filter').value;
 document.getElementById('rows').innerHTML=d.items.filter(x=>(x.symbol+' '+x.name).toLowerCase().includes(term)&&
  (fil==='all'||fil==='inside'&&x.membership==='SCANNER'||fil==='outside'&&x.membership==='RADAR_ONLY'||fil==='review'&&x.review_required))
  .map(x=>'<tr><td><b>'+esc(x.symbol)+'</b><div class="muted">'+esc(x.name)+'</div></td>'+
  cells(x.membership)+( '<td>'+esc(x.source_family)+'<div class="muted">'+esc(x.source_date||'Quellenzeit unbekannt')+
  ' · '+esc(x.source_evidence_status)+'</div></td>')+
  '<td><span class="tag">'+esc(x.daily_zone_status)+'</span><div class="muted">Monat: '+esc(x.monthly_zone_status)+
  ' · 4h: '+esc(x.four_hour_zone_status)+'</div></td>'+
  cells(x.review_reason||'Prospektiver Nutzen noch offen')+'</tr>').join('');
}
document.getElementById('query').addEventListener('input',draw);
document.getElementById('filter').addEventListener('change',draw);
draw();
</script></body></html>"""


def _read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _load_watch(path):
    with Path(path).open(encoding="utf-8-sig",newline="") as f:
        rows=list(csv.DictReader(f))
    if not rows or not {"price_bar_date","price_bar_status"}.issubset(rows[0]):
        raise ValueError("full_scanner_watch_receipt_missing")
    symbols={}
    for r in rows:
        symbol=(r.get("yahoo_symbol") or r.get("YahooSymbol") or r.get("Ticker") or "").strip()
        if not symbol:
            raise ValueError("scanner_symbol_missing")
        if symbol in symbols:
            raise ValueError("scanner_symbol_duplicated")
        symbols[symbol]=r
    return symbols


def build(radar, watch, zone_report, *, source_notes=None):
    if radar.get("schema_version")!="argus_external_thesis_public_candidates_v1":
        raise ValueError("radar_registry_schema_invalid")
    items=radar.get("items")
    if not isinstance(items,list) or not items:
        raise ValueError("radar_empty_or_invalid")
    if zone_report.get("research_only") is not True or zone_report.get("score_modified") is not False:
        raise ValueError("scanner_zone_report_governance_mismatch")
    zones=zone_report.get("assets")
    if not isinstance(zones,list):
        raise ValueError("scanner_zone_assets_missing")
    zone_map={}
    for item in zones:
        symbol=item.get("symbol")
        if not symbol or symbol in zone_map:
            raise ValueError("duplicate_zone_identity")
        zone_map[symbol]=item
    notes_by_symbol={}
    if source_notes is not None:
        if source_notes.get("schema_version")!="argus_external_thesis_source_notes_v1":
            raise ValueError("radar_notes_schema_invalid")
        for n in source_notes.get("items",[]):
            notes_by_symbol.setdefault(n.get("symbol"),[]).append(n)
    results=[]
    seen=set()
    for candidate in items:
        symbol=str(candidate.get("symbol") or "").strip()
        if not symbol or symbol in seen:
            raise ValueError("duplicate_or_empty_radar_symbol")
        seen.add(symbol)
        if candidate.get("research_only") is not True or candidate.get("scanner_admission_status")!="not_requested":
            raise ValueError("radar_unapproved_membership_change")
        found=watch.get(symbol)
        z=zone_map.get(symbol)
        member="SCANNER" if found is not None else "RADAR_ONLY"
        identity_state="NOT_IN_SCANNER"
        if found is not None:
            given=str(candidate.get("isin") or "").strip().upper()
            canonical=str(found.get("isin") or found.get("ISIN") or "").strip().upper()
            identity_state=("ISIN_CONFLICT" if given and canonical and given!=canonical
                            else "EXACT_SYMBOL_AND_ISIN_MATCH" if given and canonical and given==canonical
                            else "EXACT_SYMBOL_ONLY_REVIEW")
        asof=(found or {}).get("price_bar_date")
        expected_complete=str((found or {}).get("price_bar_status") or "").startswith("CLOSED")
        status="UNAVAILABLE"
        if found is not None and z is not None and expected_complete and str(z.get("asof"))==str(asof) and identity_state!="ISIN_CONFLICT":
            status=z.get("status") or "UNAVAILABLE"
        result={"symbol":symbol,"name":str(candidate.get("name") or symbol),
                "membership":member,"identity_state":identity_state,
                "source_family":candidate.get("source_family"),
                "source_date":candidate.get("source_date"),
                "source_evidence_status":candidate.get("source_evidence_status"),
                "research_priority_source":candidate.get("research_priority"),
                "source_notes_count":len(notes_by_symbol.get(symbol,[])),
                "research_only":True,"scanner_admission":"NOT_AUTHORIZED",
                "daily_zone_status":status,
                "monthly_zone_status":"UNAVAILABLE",
                "four_hour_zone_status":"UNAVAILABLE",
                "daily_zones_count":0,"monthly_zones_count":0,"four_hour_zones_count":0,
                "scanner_score_used_as_admission":False,
                "review_required":True,"review_reason":None,
                "sample_eligibility":"PENDING_PIT_SOURCE_AND_OUTCOME_REVIEW",
                "empirical_effect":None}
        if found is None:
            result["review_reason"]="Extern – Börsenidentität, Originalwährung und Kursdaten prüfen; nicht automatisch aufnehmen"
        elif identity_state=="ISIN_CONFLICT":
            result["review_reason"]="ISIN-KONFLIKT – Symbolgleichheit reicht nicht"
        elif z is None or str(z.get("asof"))!=str(asof):
            result["review_reason"]="Kein zeitgleicher bestätigter ARGUS-Zonenbericht"
        else:
            result["review_reason"]="Kurszonen sind Forschungsdaten; Quelle und zukünftigen Prognosewert prüfen"
            if status=="RESEARCH_ONLY":
                result["daily_zones_count"]=len(z.get("zones") or [])
                for key,outkey in [("monthly","monthly"),("four_hour","four_hour")]:
                    tf=(z.get("multiscale") or {}).get(key) or {}
                    result[outkey+"_zone_status"]=tf.get("status") or "UNAVAILABLE"
                    if tf.get("status")=="RESEARCH_ONLY":
                        result[outkey+"_zones_count"]=len(tf.get("zones") or [])
        source_date=candidate.get("source_date")
        if source_date is not None:
            try:
                date.fromisoformat(source_date)
            except ValueError:
                raise ValueError("invalid_external_source_date")
            if asof and source_date>asof:
                result["sample_eligibility"]="SOURCE_DATE_AFTER_SCANNER_SNAPSHOT"
        results.append(result)
    return {"version":VERSION,"count":len(results),
            "in_scanner":sum(r["membership"]=="SCANNER" for r in results),
            "radar_only":sum(r["membership"]=="RADAR_ONLY" for r in results),
            "daily_zones_available":sum(r["daily_zone_status"]=="RESEARCH_ONLY" for r in results),
            "monthly_zones_available":sum(r["monthly_zone_status"]=="RESEARCH_ONLY" for r in results),
            "four_hour_zones_available":sum(r["four_hour_zone_status"]=="RESEARCH_ONLY" for r in results),
            "scanner_universe_modified":False,
            "scanner_scores_modified":False,"orders_allowed":False,
            "auto_admission_allowed":False,"research_only":True,
            "prospective_outcomes_confirmed":False,
            "redundancy_conclusion":"UNDETERMINED_PENDING_OUT_OF_SAMPLE_COMPARISON",
            "comparison_arms":["RADAR_ONLY","SCANNER_ONLY","INTERSECTION"],
            "comparative_horizons_sessions":[5,20,60],
            "items":results}


def write(report, *, json_output, html_output, history_dir):
    out=Path(json_output);page=Path(html_output);history=Path(history_dir)
    out.parent.mkdir(parents=True,exist_ok=True)
    page.parent.mkdir(parents=True,exist_ok=True)
    history.mkdir(parents=True,exist_ok=True)
    encoded=json.dumps(report,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False)
    out.write_text(encoded+"\n",encoding="utf-8")
    safe=encoded.replace("<","\\u003c").replace("&","\\u0026")
    page.write_text(PAGE.replace("__RADAR_DATA__",safe),encoding="utf-8")
    key=sha256(encoded.encode()).hexdigest()[:16]
    snapshot=history / (key+".json")
    if not snapshot.exists():
        snapshot.write_text(encoded+"\n",encoding="utf-8")
    return report


def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--radar",type=Path,default=Path("argus-preview/observations/external_thesis_watch_candidates.json"))
    ap.add_argument("--notes",type=Path,default=Path("argus-preview/observations/external_thesis_source_notes.json"))
    ap.add_argument("--watch",type=Path,default=Path("artifacts/watchlist/watchlist_full.csv"))
    ap.add_argument("--zones",type=Path,default=Path("artifacts/reports/argus_zones_all.json"))
    ap.add_argument("--json",type=Path,default=Path("artifacts/reports/radar_zone_bridge.json"))
    ap.add_argument("--html",type=Path,default=Path("artifacts/ui/radar-zones.html"))
    ap.add_argument("--history",type=Path,default=Path("artifacts/research/radar_zone_bridge_history"))
    args=ap.parse_args(argv)
    report=build(_read(args.radar),_load_watch(args.watch),_read(args.zones),source_notes=_read(args.notes))
    write(report,json_output=args.json,html_output=args.html,history_dir=args.history)
    print(json.dumps({k:report[k] for k in ("version","count","in_scanner","radar_only","daily_zones_available","monthly_zones_available","four_hour_zones_available","redundancy_conclusion")},sort_keys=True))


if __name__=="__main__":
    main()
