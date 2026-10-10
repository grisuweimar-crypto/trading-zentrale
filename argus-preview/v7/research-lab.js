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
#v7Monitor .v7stage p{font-size:11px;color:#bed0dc;margin:7px 0 0}#v7Monitor .v7stage .v7stageMeaning{color:#e4ebee}#v7Monitor .v7stage .v7stageCode{color:#b8ccda}
#v7Monitor .v7statrow{display:flex;justify-content:space-between;gap:12px;border-top:1px solid #334b5c;padding:10px 0;font-size:12px}
#v7Monitor .v7statrow span{color:#b7c9d4}#v7Monitor .v7statrow b{text-align:right}
#v7Monitor .v7empty{padding:13px;border:1px solid #42576a;border-left:2px solid #caa969;background:#10293b;font-size:12px}
#v7Monitor .v7foot{font-size:11px;color:#afc1cd;line-height:1.7}
#v7Monitor a{color:#ebca91;text-decoration:underline;text-underline-offset:2px;overflow-wrap:anywhere}
#v7Monitor code{overflow-wrap:anywhere}#v7Monitor .v7sources p{font-size:12px}
@media(max-width:1000px){#v7Monitor .v7grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:700px){#v7Monitor .v7grid{grid-template-columns:repeat(2,minmax(0,1fr))}#v7Monitor .v7cols{grid-template-columns:1fr}#v7Monitor .v7card{padding:13px}#v7Monitor h3{font-size:22px}}

#v7Monitor .v7story{margin:18px 0 24px;background:linear-gradient(120deg,#16364b,#091f31 76%);border:1px solid #577086;border-radius:15px;padding:24px;box-shadow:0 16px 50px rgba(0,0,0,.13)}
#v7Monitor .v7storyIntro h4{font-family:Georgia,serif;font-size:29px;font-weight:500;margin:9px 0 7px;color:#f5dfae}
#v7Monitor .v7storyIntro p{font-size:13px;color:#d5e3eb;max-width:820px;line-height:1.7}
#v7Monitor .v7cycle{display:flex;flex-wrap:wrap;align-items:center;gap:9px;font-size:12px;padding:10px 0;margin:2px 0 17px;color:#bcd1dc}
#v7Monitor .v7cycle b{color:#f2dba7;font-size:13px}#v7Monitor .v7cycle code{background:#1c4055;color:#c9d9e5;padding:4px 8px;border-radius:5px;font-size:10px}
#v7Monitor .v7journey{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
#v7Monitor .v7step{display:flex;align-items:start;gap:12px;background:rgba(4,21,33,.67);border:1px solid #3e5b6f;border-radius:11px;padding:15px 11px;min-width:0}
#v7Monitor .v7stepNum{font:22px Georgia,serif;color:#ddb978;background:#17384e;border:1px solid #6b715f;border-radius:50%;width:41px;height:41px;display:grid;place-items:center;flex:none}
#v7Monitor .v7stepText{min-width:0}#v7Monitor .v7stepText b{font-size:14px;color:#f8e7c8}
#v7Monitor .v7stepText p{font-size:11px;margin:7px 0 14px;line-height:1.5;color:#b9d0df}
#v7Monitor .v7stepStatus{display:block;color:#f1d09a;font-size:12px;font-weight:600}
#v7Monitor .v7stepText small{display:block;color:#90adbc;font-size:9px;margin-top:6px;line-height:1.3;overflow-wrap:anywhere}
#v7Monitor .v7guideFoot{font-size:10px;color:#a7bdca;margin:12px 0 0}
#v7Monitor .v7sectionHead{display:flex;align-items:end;justify-content:space-between;gap:12px;flex-wrap:wrap;margin:23px 0 5px}
#v7Monitor .v7sectionHead h4{font-family:Georgia,serif;font-size:23px;margin:5px 0;font-weight:500}
#v7Monitor .v7sectionHead p{font-size:11px;color:#a4bfcd;margin:0 0 7px}
#v7Monitor .v7grid{gap:9px;margin:10px 0 18px}
#v7Monitor .v7metric{background:linear-gradient(155deg,#123148,#0c2335);border:1px solid #405c71;border-radius:11px;min-height:135px}
#v7Monitor .v7metric strong{font:37px Georgia,serif;line-height:1.3;color:#f4dba6}
#v7Monitor .v7ops{padding:0}
#v7Monitor .v7ops details>summary{cursor:pointer;list-style:none;padding:17px 19px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
#v7Monitor .v7ops details>summary::-webkit-details-marker{display:none}
#v7Monitor .v7ops details>summary:focus-visible{outline:2px solid #d4b47e}
#v7Monitor .v7ops details>summary strong{font-size:13px;color:#f4dcb0;flex:1}
#v7Monitor .v7ops details>summary span{color:#afc6d3;font-size:11px}
#v7Monitor .v7ops #v7Stages{padding:0 18px 12px;border-top:1px solid #354e61}
@media(max-width:1100px){#v7Monitor .v7journey{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:650px){#v7Monitor .v7story{padding:17px 13px}#v7Monitor .v7storyIntro h4{font-size:25px}#v7Monitor .v7journey{grid-template-columns:1fr}#v7Monitor .v7step{padding:12px}#v7Monitor .v7metric{min-height:120px}#v7Monitor .v7sectionHead h4{font-size:22px}}


.v7legacy{margin:20px 0 26px;padding:3px 15px 14px;background:#0b263a;border:1px solid #3c5970;border-radius:11px}
.v7legacy>summary{cursor:pointer;padding:14px 0;font-size:12px;font-weight:600;color:#e6cb9d;list-style:none}
.v7legacy>summary::-webkit-details-marker{display:none}
.v7legacy>summary:focus-visible{outline:2px solid #e5c68c}
`;
const st=document.createElement('style');st.id='v7Styles';st.textContent=css;document.head.append(st);
section.insertAdjacentHTML('afterbegin',
'<div id="v7Monitor"><div class="v7head"><div><div class="v7over">ARGUS / SCIENTIA · Version 7</div>'+
'<h3>Wissenschaftlicher Prüfstand</h3><p class="v7foot">Originalstatus aus den veröffentlichten L14-Forschungsartefakten. Keine Dashboard-Berechnung oder Bewertung.</p></div>'+
'<button id="v7Reload" type="button">↻ Forschungsstand laden</button></div>'+
'<div class="v7status" id="v7Notice" role="status">Veröffentlichte Forschungsdaten werden geprüft …</div>'+
'<div class="v7story" id="v7Story"></div>'+
'<div class="v7sectionHead"><div><span class="v7over">FORSCHUNGSREGISTER</span><h4>Was ist bereits dokumentiert?</h4></div><p>Diese Zahlen kommen unverändert aus dem Musterlabor L14.</p></div>'+
'<div class="v7grid" id="v7Counts"></div><div class="v7cols">'+
'<div><article class="v7card v7ops"><details><summary><strong>Technischer Prüfablauf</strong><span id="v7StageCount">Originalstatus der Arbeitsstationen</span><span aria-hidden="true">▾</span></summary><div id="v7Stages"></div></details></article>'+
'<article class="v7card"><h4>Einzelmuster · L5 bis L10</h4><div id="v7PatternState"></div></article></div>'+
'<div><article class="v7card"><h4>Forschungsgrenzen und Register</h4><div id="v7Lab"></div></article>'+
'<article class="v7card"><h4>Qualitätsnachweis im L14-Bericht</h4><div id="v7QM"></div></article>'+
'<article class="v7card v7sources"><h4>Herkunft &amp; Datenstand</h4><div id="v7Sources"></div></article></div></div></div>');
/* Bestehende V6-Forschungserläuterung erhalten, zur besseren Lesbarkeit zusammenklappen. */
const legacy=[...section.children].filter(node=>node.id!=='v7Monitor');
if(legacy.length){
 const details=document.createElement('details');details.className='v7legacy';details.id='v7LegacyPanel';
 const summary=document.createElement('summary');summary.textContent='Methodische Hintergründe und Forschungsgrenzen anzeigen';
 details.appendChild(summary);legacy.forEach(node=>details.appendChild(node));section.appendChild(details);
}
const el=id=>document.getElementById(id);
const esc=x=>String(x??'').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));
const count=x=>Number.isSafeInteger(x)&&x>=0?new Intl.NumberFormat('de-DE').format(x):'Nicht verfügbar';
const date=x=>x&&!Number.isNaN(Date.parse(x))?new Intl.DateTimeFormat('de-DE',{dateStyle:'medium',timeStyle:'short',timeZone:'Europe/Berlin'}).format(new Date(x))+' (Berlin)':'Nicht verfügbar';
const link=(path,label)=>'<a target="_blank" rel="noopener noreferrer" href="'+GH+encodeURI(path)+'">'+esc(label)+' ↗</a>';
const stageNames={DISCOVERY_TRIGGER:'Discovery-Trigger',PROSPECTIVE_CAPTURE:'L7 · Prospective Capture',OUTCOME_MATURATION:'L8 · Outcome Maturation',SEQUENTIAL_CONFIRMATION:'L9 · Confirmation',RATING_UPDATE:'L10 · Rating',DEPENDENCY_REFRESH:'L6 · Dependency Graph',NEGATIVE_RESULT_AUDIT:'QM-C5 · Negative Results',PATTERN_DECAY_AUDIT:'Pattern Decay',QM_REGRESSION:'Laufende QM-Regression'};
/* Erläuterungen folgen den L14-Modulverträgen; kein eigener Forschungsstatus. */
const stageHelp={
 DISCOVERY_TRIGGER:'Prüft, ob eine neue Discovery-Runde gemäß L14 zur Vorregistrierung ansteht. L14 startet keine Mustersuche eigenständig.',
 PROSPECTIVE_CAPTURE:'L7 erfasst neue Prospective-Claims nur für eingefrorene Muster und einen gültigen, noch nicht erfassten Snapshot.',
 OUTCOME_MATURATION:'L8 prüft ausstehende Beobachtungshorizonte. Ob ein Outcome reif ist, entscheidet ausschließlich L8.',
 SEQUENTIAL_CONFIRMATION:'L9 und QM-C4 steuern vorab festgelegte Bestätigungsprüfungen. L14 legt keine zusätzlichen Prüfzeitpunkte fest.',
 RATING_UPDATE:'L10 verarbeitet neue, gültige L9-Ergebnisse. ARGUS vergibt selbst keine Ratings.',
 DEPENDENCY_REFRESH:'L6 prüft Abhängigkeiten bei relevanter Änderung des eingefrorenen Musterbestands.',
 NEGATIVE_RESULT_AUDIT:'Prüft die Integrität des QM-C5-Negativergebnisregisters, ohne neue Ergebnisse zu erzeugen.',
 PATTERN_DECAY_AUDIT:'Prüft dokumentierte Hinweise auf nachlassende Musterstabilität. L14 ändert dadurch keine Ratings.',
 QM_REGRESSION:'Bindet den bestehenden BA-QM12-Prüfstatus ein; ein PASS ist keine Validierung der Handelsprognose.'
};
const statusHelp={
 DUE_PREREGISTRATION:'Vor einer Mustersuche ist eine neue, unveränderliche L1-Vorregistrierung erforderlich.',
 NOT_DUE:'Dieser Arbeitsschritt ist laut dem veröffentlichten L14-Zyklus derzeit nicht fällig.',
 PASS:'Die jeweilige technische oder QM-Kontrolle meldet PASS; das ist keine Bestätigung einer Renditeprognose.',
 BLOCKED_FAIL_CLOSED:'Der Forschungsbetrieb ist laut Register fail-closed gesperrt; keine stillschweigende Freigabe.',
 READY_WITH_WORK:'Der L14-Betriebscheck ist bereit; die ausgewiesenen Aufgaben sind noch zu bearbeiten.'
};
const help=(stage,status)=>'<p class="v7stageMeaning">'+esc(stageHelp[stage]??'Für diese Station liegt keine freigegebene Kurzbeschreibung vor.')+'</p>'+
 '<p class="v7stageCode">'+esc(statusHelp[status]??'Für diesen Status liegt keine freigegebene Kurzerläuterung vor. Maßgeblich bleibt der Originalcode.')+'</p>';
const lbl=(k,v)=>'<div class="v7statrow"><span>'+esc(k)+'</span><b>'+esc(v)+'</b></div>';
const summary=(k,n,d)=>'<div class="v7metric"><label>'+esc(k)+'</label><strong>'+count(n)+'</strong><small>'+esc(d)+'</small></div>';
async function fetchJson(path){
const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),16000);
try{const r=await fetch(ROOT+path,{cache:'no-store',signal:controller.signal});if(!r.ok)throw new Error('HTTP '+r.status+': '+path);return await r.json()}
finally{clearTimeout(timeout)}
}
let running=false;
function fail(msg){
  el('v7Counts').innerHTML='';el('v7Stages').innerHTML='';el('v7Story').innerHTML='';el('v7PatternState').innerHTML='';
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
   const statusNames={
     DUE_PREREGISTRATION:'Vorregistrierung erforderlich',
     NOT_DUE:'Derzeit nicht fällig',
     PASS:'Prüfung bestanden',
     BLOCKED_FAIL_CLOSED:'Gesperrt',
     READY_WITH_WORK:'Betrieb bereit · Aufgaben offen'
   };
   const work=stage=>c.work_items.find(x=>x.stage===stage)?.status??null;
   const step=(num,title,subtitle,code)=>'<div class="v7step"><div class="v7stepNum">'+esc(num)+'</div>'+
     '<div class="v7stepText"><b>'+esc(title)+'</b><p>'+esc(subtitle)+'</p>'+
     '<span class="v7stepStatus">'+esc(code===null?'Nicht verfügbar':statusNames[code]??'Status siehe Original')+
     '</span><small>Originalstatus: <code>'+esc(code??'Nicht verfügbar')+'</code></small></div></div>';
   const zero=Number.isSafeInteger(q.frozen_pattern_count)&&q.frozen_pattern_count===0;
   el('v7Story').innerHTML='<div class="v7storyIntro"><span class="v7over">FORSCHUNG AUF EINEN BLICK</span>'+
      '<h4>Wie weit sind unsere Musterprüfungen?</h4>'+
      '<p>'+ (zero?
       'Im veröffentlichten L14-Musterlabor sind noch keine Muster eingefroren. Die Betriebs- und Qualitätskontrollen laufen; ihre erfolgreichen Tests sind keine statistische Bestätigung eines Handelssignals.':
       'Hier siehst du die Arbeitsstationen aus dem veröffentlichten L14-Zyklus. Wissenschaftliche Freigaben und Ergebnisse bleiben ausschließlich in den Scanner_vNext-Registern.')+
      '</p><div class="v7cycle">L14-Betriebsstatus: <b>'+esc(statusNames[c.cycle_status]??c.cycle_status)+'</b>'+
      '<code>'+esc(c.cycle_status)+'</code></div></div>'+
      '<div class="v7journey">'+
       step('01','Entdecken','Forschungsfrage vorregistrieren und Muster einfrieren',work('DISCOVERY_TRIGGER'))+
       step('02','Beobachten','Eingefrorene Muster prospektiv erfassen',work('PROSPECTIVE_CAPTURE'))+
       step('03','Überprüfen','Ergebnisse nach Ablauf der festgelegten Horizonte beurteilen',work('OUTCOME_MATURATION'))+
       step('04','Einordnen','Kontrollierte Bestätigung und anschließend Rating',work('RATING_UPDATE'))+
      '</div><p class="v7guideFoot">Die vier Schritte sind eine Navigationshilfe. Die angezeigten Stati stammen ausschließlich aus L14; ARGUS leitet daraus keine neue Forschungseinstufung ab.</p>';
   el('v7StageCount').textContent=c.work_items.length+' Arbeitsstationen · Originaldaten';
   el('v7Counts').innerHTML=summary('Muster eingefroren',q.frozen_pattern_count,'aus L14 laboratory_state')+
      summary('Prospektive Beobachtungen',q.claim_count,'aus L14 laboratory_state')+
      summary('Abgeschlossene Horizonte',q.matured_outcome_count,'aus L14 laboratory_state')+
      summary('Bestätigungsprüfungen',q.confirmation_look_count,'aus L14 laboratory_state')+
      summary('Rating-Ereignisse',q.rating_event_count,'aus L14 laboratory_state');
   el('v7Stages').innerHTML=c.work_items.length?c.work_items.map(s=>
      '<div class="v7stage"><div class="v7stageHead"><b>'+esc(stageNames[s.stage]||s.stage||'Unbekannte Phase')+'</b><code>'+esc(s.status??'UNAVAILABLE')+
      '</code></div>'+help(s.stage,s.status)+'</div>').join(''):
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