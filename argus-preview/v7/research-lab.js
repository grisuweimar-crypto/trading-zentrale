/* ARGUS V7 / Wissenschaftsmonitor. READ ONLY. Forschungsautorität: Scanner_vNext. */
(()=>{
'use strict';
const ROOT='https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/main/';
const GH='https://github.com/grisuweimar-crypto/trading-zentrale/blob/main/';
const M='artifacts/research/history_metadata.json';
const P='artifacts/research/pattern_discovery/operations/latest.json';
const section=document.getElementById('science');if(!section)return;
const css=String.raw`
/* V7 namespace is intentionally separate from the V4–V6 design system */
#v7Monitor{margin:28px 0 18px;color:#e6edf3}
#v7Monitor .v7head{display:flex;justify-content:space-between;align-items:center;gap:15px;flex-wrap:wrap;border-bottom:1px solid #3b5060;padding-bottom:15px}
#v7Monitor .v7over{font-size:11px;letter-spacing:.13em;color:#d8b77b;text-transform:uppercase}
#v7Monitor h3{font-family:Georgia,serif;font-size:26px;line-height:1.18;margin:8px 0;color:#f5e5c0}
#v7Monitor h4{font-size:17px;margin:4px 0 13px;color:#e9deca}
#v7Monitor p{line-height:1.65}#v7Monitor button{border:1px solid #bca067;border-radius:7px;padding:9px 14px;color:#f5e4bb;background:#133045;cursor:pointer;font:inherit}
#v7Monitor button:disabled{opacity:.55;cursor:wait}
#v7Monitor .v7status{margin:14px 0;background:#10283d;border:1px solid #35526b;border-left:3px solid #d3af6e;padding:12px 14px;font-size:12px;line-height:1.65}
#v7Monitor .v7grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:12px;margin:15px 0 20px}
#v7Monitor .v7metric{background:#102a3e;border:1px solid #365368;padding:15px 12px;min-width:0;border-radius:8px}
#v7Monitor .v7metric label{display:block;color:#aebfcb;font-size:11px;min-height:32px}
#v7Monitor .v7metric strong{display:block;font-size:29px;color:#f6e3bb;margin:5px 0}
#v7Monitor .v7metric small{font-size:10px;color:#aec0cb;line-height:1.4;display:block}
#v7Monitor .v7cols{display:grid;grid-template-columns:minmax(0,1.1fr) minmax(0,1fr);gap:14px}
#v7Monitor .v7card{border:1px solid #344f62;background:rgba(9,33,51,.75);border-radius:9px;padding:19px;margin-bottom:14px;min-width:0}
#v7Monitor .v7stage{padding:12px 0;border-top:1px solid #365269}
#v7Monitor .v7stage:first-child{border-top:0;padding-top:0}
#v7Monitor .v7stageHead{display:flex;justify-content:space-between;align-items:center;gap:9px;flex-wrap:wrap}
#v7Monitor .v7stage b{font-size:12px}#v7Monitor .v7stage code{color:#f0cb84;background:#17374a;border-radius:4px;padding:3px 7px;font-size:11px}
#v7Monitor .v7stage p{font-size:11px;color:#bed0dc;margin:7px 0 0}
#v7Monitor .v7statrow{display:flex;justify-content:space-between;gap:12px;border-top:1px solid #334b5c;padding:10px 0;font-size:12px}
#v7Monitor .v7statrow span{color:#b7c9d4}#v7Monitor .v7statrow b{text-align:right}
#v7Monitor .v7empty{padding:13px;border:1px solid #42576a;border-left:2px solid #caa969;background:#10293b;font-size:12px}
#v7Monitor .v7foot{font-size:11px;color:#afc1cd;line-height:1.7}
#v7Monitor a{color:#ebca91;text-decoration:underline;text-underline-offset:2px;overflow-wrap:anywhere}
#v7Monitor code{overflow-wrap:anywhere}#v7Monitor .v7sources p{font-size:12px}
@media(max-width:1000px){#v7Monitor .v7grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:700px){#v7Monitor .v7grid{grid-template-columns:repeat(2,minmax(0,1fr))}#v7Monitor .v7cols{grid-template-columns:1fr}#v7Monitor .v7card{padding:13px}#v7Monitor h3{font-size:22px}}
`;
const st=document.createElement('style');st.id='v7Styles';st.textContent=css;document.head.append(st);
section.insertAdjacentHTML('afterbegin',
'<div id="v7Monitor"><div class="v7head"><div><div class="v7over">ARGUS / SCIENTIA · Version 7</div>'+
'<h3>Wissenschaftlicher Prüfstand</h3><p class="v7foot">Originalstatus aus den veröffentlichten L14-Forschungsartefakten. Keine Dashboard-Berechnung oder Bewertung.</p></div>'+
'<button id="v7Reload" type="button">↻ Forschungsstand laden</button></div>'+
'<div class="v7status" id="v7Notice" role="status">Veröffentlichte Forschungsdaten werden geprüft …</div>'+
'<div class="v7grid" id="v7Counts"></div><div class="v7cols">'+
'<div><article class="v7card"><h4>L14 · Arbeitsstationen</h4><div id="v7Stages"></div></article>'+
'<article class="v7card"><h4>Einzelmuster · L5 bis L10</h4><div id="v7PatternState"></div></article></div>'+
'<div><article class="v7card"><h4>Forschungsgrenzen und Register</h4><div id="v7Lab"></div></article>'+
'<article class="v7card"><h4>Qualitätsnachweis im L14-Bericht</h4><div id="v7QM"></div></article>'+
'<article class="v7card v7sources"><h4>Herkunft &amp; Datenstand</h4><div id="v7Sources"></div></article></div></div></div>');
const el=id=>document.getElementById(id);
const esc=x=>String(x??'').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));
const count=x=>Number.isSafeInteger(x)&&x>=0?new Intl.NumberFormat('de-DE').format(x):'Nicht verfügbar';
const date=x=>x&&!Number.isNaN(Date.parse(x))?new Intl.DateTimeFormat('de-DE',{dateStyle:'medium',timeStyle:'short',timeZone:'Europe/Berlin'}).format(new Date(x))+' (Berlin)':'Nicht verfügbar';
const link=(path,label)=>'<a target="_blank" rel="noopener noreferrer" href="'+GH+encodeURI(path)+'">'+esc(label)+' ↗</a>';
const stageNames={DISCOVERY_TRIGGER:'Discovery-Trigger',PROSPECTIVE_CAPTURE:'L7 · Prospective Capture',OUTCOME_MATURATION:'L8 · Outcome Maturation',SEQUENTIAL_CONFIRMATION:'L9 · Confirmation',RATING_UPDATE:'L10 · Rating',DEPENDENCY_REFRESH:'L6 · Dependency Graph',NEGATIVE_RESULT_AUDIT:'QM-C5 · Negative Results',PATTERN_DECAY_AUDIT:'Pattern Decay',QM_REGRESSION:'Laufende QM-Regression'};
const lbl=(k,v)=>'<div class="v7statrow"><span>'+esc(k)+'</span><b>'+esc(v)+'</b></div>';
const summary=(k,n,d)=>'<div class="v7metric"><label>'+esc(k)+'</label><strong>'+count(n)+'</strong><small>'+esc(d)+'</small></div>';
async function fetchJson(path){
const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),16000);
try{const r=await fetch(ROOT+path,{cache:'no-store',signal:controller.signal});if(!r.ok)throw new Error('HTTP '+r.status+': '+path);return await r.json()}
finally{clearTimeout(timeout)}
}
let running=false;
function fail(msg){
  el('v7Counts').innerHTML='';el('v7Stages').innerHTML='';el('v7PatternState').innerHTML='';
  el('v7Lab').innerHTML='';el('v7QM').innerHTML='';el('v7Sources').innerHTML='';
  el('v7Notice').textContent='Forschungsdaten nicht verfügbar oder nicht konsistent: '+msg+'. Kein alter Ergebnisstand wird als aktuell gezeigt.';
}
async function load(){
 if(running)return;running=true;el('v7Reload').disabled=true;
 el('v7Notice').textContent='Publikationsmarker, Forschungszeiger und L14-Zyklus werden geladen …';
 try{
   const [meta,ptr]=await Promise.all([fetchJson(M),fetchJson(P)]);
   if(meta?.validation?.status!=='ok'||meta.latest_run_complete!==true||typeof meta.snapshot_id!=='string')throw new Error('Scanner-Publikationsmarker nicht gültig');
   const h=ptr?.cycle_hash;
   if(ptr?.schema_version!=='pattern_discovery_l14_latest_pointer_v1'||typeof h!=='string'||!/^[a-f0-9]{64}$/.test(h))throw new Error('L14-Zeiger unzulässig');
   const cyclePath='artifacts/research/pattern_discovery/operations/cycles/'+h+'.json';
   if(ptr.cycle_path!==cyclePath)throw new Error('L14-Pfad widersprüchlich');
   const c=await fetchJson(cyclePath);
   if(c?.schema_version!=='pattern_discovery_l14_operations_cycle_v1'||c.cycle_hash!==h||
      c.cycle_status!==ptr.cycle_status||c.evaluated_at!==ptr.evaluated_at||c.module!=='pattern_discovery_lab'||c.phase!=='L14'||
      c.research_only!==true||!Array.isArray(c.work_items)||!c.laboratory_state)throw new Error('L14-Zyklus/Schemaprüfung fehlgeschlagen');
   const same=c.current_scanner?.snapshot_id===meta.snapshot_id&&c.source_heads?.scanner_snapshot_id===meta.snapshot_id;
   el('v7Notice').innerHTML=same?
     'Quelle: <b>L14, '+esc(date(c.evaluated_at))+'</b> · Statuscode <code>'+esc(c.cycle_status)+'</code> · Snapshot <code>'+esc(meta.snapshot_id)+'</code>. Dies sind Registerdaten, keine Vorhersage.':
     '<b>Unterschiedliche Snapshot-IDs:</b> L14: <code>'+esc(c.current_scanner?.snapshot_id)+'</code>, aktueller Scanner: <code>'+esc(meta.snapshot_id)+'</code>. Die Anzeigen stehen nebeneinander; keine zeitliche Gleichsetzung.';
   const q=c.laboratory_state;
   el('v7Counts').innerHTML=summary('L5 · Eingefrorene Muster',q.frozen_pattern_count,'aus L14 laboratory_state')+
      summary('L7 · Claims',q.claim_count,'aus L14 laboratory_state')+
      summary('L8 · Reife Outcomes',q.matured_outcome_count,'aus L14 laboratory_state')+
      summary('L9 · Confirmation Looks',q.confirmation_look_count,'aus L14 laboratory_state')+
      summary('L10 · Rating-Ereignisse',q.rating_event_count,'aus L14 laboratory_state');
   el('v7Stages').innerHTML=c.work_items.length?c.work_items.map(s=>
      '<div class="v7stage"><div class="v7stageHead"><b>'+esc(stageNames[s.stage]||s.stage||'Unbekannte Phase')+'</b><code>'+esc(s.status??'UNAVAILABLE')+
      '</code></div><p>Originalstatus: '+esc(s.status??'Nicht verfügbar')+'</p></div>').join(''):
      '<div class="v7empty">Keine Stationen im verifizierten Zyklus enthalten.</div>';
   const trg=c.discovery?.trigger??{}, cand=c.discovery?.candidate_state??{}, head=c.source_heads??{};
   el('v7Lab').innerHTML=lbl('Discovery-Trigger-Status',trg.status??'Nicht verfügbar')+
      lbl('Discovery-Modus',trg.mode??'Nicht verfügbar')+
      lbl('L1-Vorregistrierung nötig',trg.new_l1_preregistration_required===true?'true':trg.new_l1_preregistration_required===false?'false':'Nicht verfügbar')+
      lbl('Automatischer Suchstart erlaubt',trg.automatic_search_start_allowed===true?'true':trg.automatic_search_start_allowed===false?'false':'Nicht verfügbar')+
      lbl('Discovery-Manifeste',count(c.discovery?.manifest_count))+
      lbl('Freeze-Snapshot vorhanden',cand.freeze_snapshot_present===true?'true':cand.freeze_snapshot_present===false?'false':'Nicht verfügbar')+
      '<p class="v7foot">Diese Angaben stammen aus L14. Die Anzeige ändert weder Vorregistrierung noch Forschungsstatus. L5-Set-Hash: <code>'+esc(head.l5_pattern_set_hash??'Nicht verfügbar')+'</code>.</p>';
   el('v7PatternState').innerHTML=Number.isSafeInteger(q.frozen_pattern_count)&&q.frozen_pattern_count===0?
      '<div class="v7empty"><b>Keine L5-Muster registriert.</b><p>Der veröffentlichte L14-Zyklus nennt 0 eingefrorene Muster. Deshalb gibt es hier keine fiktiven Einzelmusterkarten, Quoten oder Ratings.</p></div>':
      '<div class="v7empty">Eine wissenschaftliche Einzelmusterakte benötigt ein veröffentlichtes, schema-geprüftes L5–L10-Register. Der L14-Operationszyklus allein enthält dafür keine vollständigen Pattern-Datensätze.</div>';
   const qm=c.qm_audit??{},negative=qm.negative_result_registry??{};
   el('v7QM').innerHTML=lbl('L14-QM-Auditstatus',qm.status??'Nicht verfügbar')+
      lbl('QM-C5 Registry valid',negative.valid===true?'true':negative.valid===false?'false':'Nicht verfügbar')+
      lbl('QM-C5 negative Events',count(negative.event_count))+
      lbl('Produktive Integration enabled',c.productive_integration_enabled===true?'true':c.productive_integration_enabled===false?'false':'Nicht verfügbar')+
      lbl('Execution allowed',c.execution_allowed===true?'true':c.execution_allowed===false?'false':'Nicht verfügbar')+
      '<p class="v7foot">Nur Statusfelder des aktuellen L14-Zyklus. Keine eigenständige QM-Freigabe und kein statistischer Wirksamkeitsnachweis.</p>';
   el('v7Sources').innerHTML='<p><b>L14 bewertet:</b> '+esc(date(c.evaluated_at))+'</p>'+
      '<p><b>L14-Scannerbezug:</b> '+esc(c.current_scanner?.as_of??'Nicht verfügbar')+' · '+count(c.current_scanner?.observed_symbol_count)+
      ' / '+count(c.current_scanner?.required_symbol_count)+' Titel.</p>'+
      '<p><b>Scanner-Publikationsmarker:</b> '+esc(meta.as_of)+' · '+esc(meta.snapshot_id)+'</p>'+
      '<p>'+link(P,'L14 Latest Pointer')+'</p><p>'+link(cyclePath,'L14 Cycle')+'</p><p>'+link(M,'Scanner Metadata')+'</p>'+
      '<p>'+link('docs/pattern_discovery/l14_continuous_operations.md','L14-Datenvertrag')+'</p>'+
      '<p class="v7foot">Lesende Darstellung. Die Übereinstimmung von Zeiger und Zyklus wurde anhand der veröffentlichten Metadaten geprüft; keine zusätzliche kryptographische Archivverifikation behauptet.</p>';
 }catch(e){fail(e.name==='AbortError'?'Zeitüberschreitung':(e.message||'Unbekannter Fehler'))}
 finally{running=false;el('v7Reload').disabled=false}
}
el('v7Reload').addEventListener('click',load);
load();
})();