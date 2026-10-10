/* ARGUS V7-B: read-only presentation of original QM evidence. */
(()=>{'use strict';
const parent=document.getElementById('v7Monitor');if(!parent)return;
const BASE='https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/main/';
const GH='https://github.com/grisuweimar-crypto/trading-zentrale/blob/main/';
const LEDGER='artifacts/research/qm/qm_h_capa_ledger.jsonl';
const REPORT='artifacts/research/qm/qm_j_decision_e2e_falsification.json';
const esc=v=>String(v??'').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));
const link=(path,label)=>'<a target="_blank" rel="noopener noreferrer" href="'+GH+encodeURI(path)+'">'+esc(label)+' ↗</a>';
const clock=v=>v&&!Number.isNaN(Date.parse(v))?new Intl.DateTimeFormat('de-DE',{dateStyle:'medium',timeStyle:'short',timeZone:'Europe/Berlin'}).format(new Date(v))+' (Berlin)':'Unbekannter Zeitpunkt';
const categories={DEFECT:'Technischer Fehler',NEAR_MISS:'Beinahefehler',METHODOLOGY_FINDING:'Methodischer Befund',EXTERNAL_EVIDENCE_GAP:'Externe Evidenzlücke'};
const states={OPEN:'Erfasst',TRIAGED:'Eingeordnet',ROOT_CAUSE_IDENTIFIED:'Ursache identifiziert',ACTION_PLANNED:'Maßnahme geplant',IMPLEMENTED:'Maßnahme umgesetzt',EFFECTIVENESS_VERIFIED:'Wirksamkeit geprüft',CLOSED:'Formell abgeschlossen',INVALIDATED:'Formal invalidiert'};
const css=[
'#v7Quality{margin-top:18px;border-top:1px solid #46617a;padding-top:22px}',
'#v7Quality .v7qmTop{display:flex;justify-content:space-between;gap:15px;align-items:start;flex-wrap:wrap}',
'#v7Quality h4{font-size:20px;margin:0 0 6px;color:#ecdab7}',
'#v7Quality h5{font-size:14px;margin:0 0 8px;color:#f3debb}',
'#v7Quality p{font-size:12px;line-height:1.65;color:#cfdee6;margin:8px 0}',
'#v7Quality .v7qmState{margin:13px 0;padding:12px 14px;background:#0f283c;border:1px solid #3a556d;border-left:2px solid #d1ae71;font-size:12px;line-height:1.65}',
'#v7Quality .v7qmFinding{background:rgba(12,39,56,.9);border:1px solid #365368;border-radius:8px;margin:9px 0;min-width:0;overflow-wrap:anywhere}',
'#v7Quality .v7qmFinding[open]{border-color:#6f6c5d}',
'#v7Quality .v7qmFinding summary{cursor:pointer;padding:13px 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}',
'#v7Quality .v7qmFinding summary:focus-visible{outline:2px solid #d1ae71;outline-offset:1px}',
'#v7Quality .v7qmFinding summary .v7qmTitle{font-weight:650;font-size:12px;flex:1;min-width:170px;line-height:1.5}',
'#v7Quality .v7qmFinding summary .v7qmCode{font:11px ui-monospace,monospace;color:#e8cc97;border:1px solid #5a6b76;border-radius:4px;padding:5px 7px;white-space:nowrap}',
'#v7Quality .v7qmBody{border-top:1px solid #344e61;padding:12px 14px}',
'#v7Quality .v7qmBody strong{color:#f5ddba}',
'#v7Quality .v7qmGrid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:14px}',
'#v7Quality .v7qmPanel{background:#0e293c;border:1px solid #344d61;padding:16px;border-radius:9px;min-width:0}',
'#v7Quality code{font-size:11px;overflow-wrap:anywhere}',
'#v7Quality .v7qmFoot{color:#a9c0cb;font-size:11px}',
'#v7Quality a{color:#efce96;text-decoration:underline;text-underline-offset:2px;overflow-wrap:anywhere}',
'@media(max-width:750px){#v7Quality .v7qmGrid{grid-template-columns:1fr}#v7Quality .v7qmFinding summary .v7qmTitle{min-width:130px}#v7Quality .v7qmPanel{padding:12px}}'
].join('\n');
const style=document.createElement('style');style.id='v7QMStyles';style.textContent=css;document.head.append(style);
parent.insertAdjacentHTML('beforeend',
'<section id="v7Quality" aria-label="Veröffentlichte QM-Befunde">'+
'<div class="v7qmTop"><div><div class="v7over">ARGUS / V7-B · QUELLENGEBUNDENES QUALITÄTSMANAGEMENT</div>'+
'<h4>QM-H · Feststellungen und Maßnahmen</h4>'+
'<p>Dokumentierte Befunde mit ihren Originalstatuscodes. Dies ist keine neue QM-Bewertung.</p></div>'+
'<button type="button" id="v7QMReload">↻ QM-Register aktualisieren</button></div>'+
'<div class="v7qmState" id="v7QMState" role="status">QM-H-Register wird geladen …</div>'+
'<div id="v7QMFindings"></div><div class="v7qmGrid">'+
'<div class="v7qmPanel"><h5>Unabhängige QM-J-Einzelprüfung</h5><div id="v7QMJ">Prüfbericht wird geladen …</div></div>'+
'<div class="v7qmPanel"><h5>Abgrenzung und Originalquellen</h5>'+
'<p>QM-H dokumentiert Befunde und Korrekturmaßnahmen. Es ersetzt weder QM-A-Forschungsfreigaben noch QM-B-External-Evidence-Sperren.</p>'+
'<p>Der L14-QM-Status weiter oben gehört zum jeweiligen L14-Zyklus; das QM-H-Ledger hat seinen eigenen Datenstand.</p>'+
'<p>'+link(LEDGER,'QM-H-Originalregister')+'</p>'+
'<p>'+link('docs/qm_h_defect_near_miss_capa.md','QM-H-Vertrag und Lebenszyklus')+'</p>'+
'<p>'+link(REPORT,'QM-J-Einzelbericht')+'</p></div></div></section>');
const el=id=>document.getElementById(id);
async function getText(path,maxBytes=1500000){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),16000);
 try{
  const r=await fetch(BASE+path,{cache:'no-store',signal:controller.signal});
  if(!r.ok)throw Error('HTTP '+r.status);
  const length=r.headers.get('Content-Length');if(length!==null&&Number(length)>maxBytes)throw Error('Quelle zu groß');
  const data=await r.text();if(data.length>maxBytes)throw Error('Quelle zu groß');return data;
 }finally{clearTimeout(timer)}
}
function replayLedger(source){
 if(!source.trim())throw Error('QM-H-Register leer');
 const lines=source.trim().split(/\r?\n/);
 if(lines.length>5000)throw Error('Unerwarteter Registerumfang');
 const found=new Map(),ids=new Set();let prev=null,seq=0,lastTime=null;
 for(const line of lines){
  const e=JSON.parse(line);seq++;
  if(e?.schema_version!=='qm_h_ledger_event_v1'||e.sequence!==seq||
     !/^[a-f0-9]{64}$/.test(e.entry_hash??'')||e.previous_event_hash!==prev||
     typeof e.event_id!=='string'||ids.has(e.event_id)||
     typeof e.finding_id!=='string'||!e.finding_id||!e.payload)throw Error('QM-H-Ereigniskette inkonsistent');
  ids.add(e.event_id);prev=e.entry_hash;lastTime=e.recorded_at;
  if(e.event_type==='FINDING_REGISTERED'){
   if(found.has(e.finding_id)||typeof e.payload.title!=='string'||!e.payload.title)throw Error('Ungültige Befundregistrierung');
   found.set(e.finding_id,{id:e.finding_id,title:e.payload.title,description:e.payload.description,
     category:e.payload.category,initialImpact:e.payload.evidence_impact,
     initialReference:e.payload.source_reference,status:'OPEN',changed_at:e.recorded_at,
     lastDetails:null,eventSequence:e.sequence});
  }else if(e.event_type==='FINDING_TRANSITION'){
   const f=found.get(e.finding_id),p=e.payload;
   if(!f||typeof p.from_status!=='string'||typeof p.to_status!=='string'||p.from_status!==f.status)throw Error('QM-H-Statusfolge inkonsistent');
   f.status=p.to_status;f.changed_at=e.recorded_at;f.lastDetails=p.details??null;f.eventSequence=e.sequence;
  }else throw Error('Unbekannter Ereignistyp');
 }
 return{findings:[...found.values()].sort((a,b)=>b.eventSequence-a.eventSequence),lastTime,events:seq,head:prev};
}
function findingCard(f){
 const details=f.lastDetails??{};
 const rationale=details.closure_rationale??details.implementation_summary??details.corrective_action??null;
 return '<details class="v7qmFinding"'+(f.status==='IMPLEMENTED'?' open':'')+'>'+
 '<summary><span class="v7qmTitle">'+esc(f.title)+'</span><span class="v7qmCode">'+esc(f.status)+'</span></summary>'+
 '<div class="v7qmBody"><p><strong>Status im QM-H-Register:</strong> '+esc(states[f.status]??'Nicht übersetzt')+' (<code>'+esc(f.status)+'</code>)</p>'+
 '<p><strong>Finding-ID:</strong> <code>'+esc(f.id)+'</code></p>'+
 '<p><strong>Kategorie:</strong> '+esc(categories[f.category]??'Nicht übersetzt')+' (<code>'+esc(f.category??'—')+'</code>)</p>'+
 '<p><strong>Letzter protokollierter Übergang:</strong> '+esc(clock(f.changed_at))+'</p>'+
 '<p><strong>Evidenzwirkung bei Registrierung:</strong> <code>'+esc(f.initialImpact??'Nicht verfügbar')+'</code> – nicht automatisch aktueller Sperrstatus.</p>'+
 (f.description?'<p><strong>Ursprünglicher Befund:</strong> '+esc(f.description)+'</p>':'')+
 (rationale?'<p><strong>Letzte dokumentierte Maßnahme oder Abschlussbegründung:</strong> '+esc(rationale)+'</p>':'')+
 (f.initialReference?'<p class="v7qmFoot"><strong>Referenz bei Registrierung:</strong> <code>'+esc(f.initialReference)+'</code></p>':'')+
 '<p class="v7qmFoot">Ereignis #'+esc(f.eventSequence)+'. '+link(LEDGER,'Originalregister öffnen')+'</p></div></details>';
}
let busy=false;
async function loadLedger(){
 if(busy)return;busy=true;el('v7QMReload').disabled=true;
 el('v7QMState').textContent='QM-H-Originalregister wird geladen und strukturell geprüft …';
 el('v7QMFindings').replaceChildren();
 try{
  const data=replayLedger(await getText(LEDGER));
  el('v7QMState').innerHTML='<b>QM-H-Registerstand:</b> '+esc(clock(data.lastTime))+
   ' · '+esc(data.events)+' protokollierte Ereignisse · '+esc(data.findings.length)+' registrierte Feststellungen.'+
   '<br>Sequenz, Statusübergänge und Hash-Verweise wurden strukturell geprüft. Das ist keine kryptographische SHA-256-Verifikation und keine wissenschaftliche Gesamtfreigabe.'+
   '<br><b>Wichtig:</b> Der Registerstand ist nicht der aktuelle Gesamtstatus sämtlicher QM-Sperren.';
  el('v7QMFindings').innerHTML=data.findings.length?
   data.findings.map(findingCard).join(''):'<p>Keine registrierten Feststellungen im veröffentlichten Ledger.</p>';
 }catch(e){
  el('v7QMState').textContent='QM-H nicht verfügbar oder inkonsistent ('+
   (e.name==='AbortError'?'Zeitüberschreitung':e.message)+'). Keine alten Findings werden als aktuell angezeigt.';
  el('v7QMFindings').replaceChildren();
 }finally{busy=false;el('v7QMReload').disabled=false}
}
async function loadJ(){
 el('v7QMJ').textContent='QM-J-Einzelbericht wird geladen …';
 try{
  const d=JSON.parse(await getText(REPORT,250000));
  if(d?.schema_version!=='qm_j_decision_e2e_falsification_result_v1'||d.module!=='QM-J'||typeof d.status!=='string')throw Error('Unbekanntes QM-J-Schema');
  el('v7QMJ').innerHTML='<p><b>Originaler Prüfstatus:</b><br><code>'+esc(d.status)+'</code></p>'+
  '<p><b>Ausdrückliche Ergebnisgrenze:</b><br><code>'+esc(d.w11_acceptance_interpretation??'Nicht verfügbar')+'</code></p>'+
  '<p><b>Snapshot-ID des Berichts:</b><br><code>'+esc(d.snapshot_id??'Nicht verfügbar')+'</code></p>'+
  '<p class="v7qmFoot">Separater historischer Einzelbericht; keine Aussage über den heutigen Scannerlauf, keine Renditevalidierung und kein globales QM-PASS.</p>';
 }catch(e){el('v7QMJ').textContent='QM-J-Einzelbericht nicht verfügbar oder Schema unbekannt. Kein Ersatzwert.'}
}
el('v7QMReload').addEventListener('click',()=>{loadLedger();loadJ()});
loadLedger();loadJ();
})();