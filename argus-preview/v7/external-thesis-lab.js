/* ARGUS V7 external thesis research: read-only, isolated from L14/QM. */
(()=>{
'use strict';
const box=document.getElementById('external-theses');if(!box)return;
const SRC='https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/external-thesis-research-data/artifacts/research/external_thesis/latest.json';
const esc=v=>String(v??'').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[x]));
const css=[
'#external-theses{margin:30px 0;padding:24px;border:1px solid #4a6679;border-radius:14px;background:linear-gradient(130deg,#103147,#081e30)}',
'#external-theses h2{font:normal clamp(24px,3vw,32px) Georgia,serif;color:#f2ddb8;margin:6px 0 12px}',
'#external-theses p{font-size:13px;line-height:1.7;color:#cbdde7;margin:10px 0}',
'#external-theses .head{font-size:11px;letter-spacing:.12em;color:#c4a86f;text-transform:uppercase}',
'#external-theses .meta{padding:13px;border-left:3px solid #d0b178;background:#0b293e;margin:15px 0;line-height:1.7;font-size:12px}',
'#external-theses .rows{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:14px}',
'#external-theses .row{background:#09283b;border:1px solid #36566a;border-radius:9px;padding:14px;min-width:0}',
'#external-theses .row h3{font:normal 19px Georgia,serif;color:#f5e1b8;margin:0 0 8px}',
'#external-theses .row p{font-size:12px;margin:7px 0}',
'#external-theses .values{display:flex;flex-wrap:wrap;gap:5px}',
'#external-theses .values span{background:#143448;border:1px solid #3d586c;border-radius:6px;padding:7px;font-size:11px;color:#d6e6eb}',
'#external-theses .values b{color:#f0d2a0}',
'#external-theses a{color:#f0d09e;text-underline-offset:3px}',
'@media(max-width:690px){#external-theses{padding:17px 13px}#external-theses .rows{grid-template-columns:1fr}}'
].join('\n');
const st=document.createElement('style');st.textContent=css;document.head.append(st);
box.innerHTML='<span class="head">Unabhängiger Vergleich · externe Thesen</span>'+
'<h2>Was wurde behauptet – und was passierte danach?</h2>'+
'<p>Hier verfolgen wir datierte Trading-Ideen anderer Anbieter getrennt von unserer eigenen Musterforschung. Die Beobachtung nach 1, 5, 10, 20 und 40 Handelstagen erfolgt anhand historischer Kurse. Ohne verbindliche Prognosefrist berechnen wir keine vermeintliche Trefferquote des Anbieters.</p>'+
'<div class="meta" id="externalState">Die Kursentwicklung wird geladen …</div>'+
'<div class="rows" id="externalRows"></div>'+
'<p><a href="../#observations">Zu den Begründungen in der Kandidatenbeobachtung ↗</a></p>';
const state=document.getElementById('externalState'),rows=document.getElementById('externalRows');
const fmt=n=>new Intl.NumberFormat('de-DE',{minimumFractionDigits:1,maximumFractionDigits:2}).format(n);
async function load(){
 const ctrl=new AbortController(),timer=setTimeout(()=>ctrl.abort(),15000);
 try{
  const r=await fetch(SRC,{signal:ctrl.signal,cache:'no-store'});
  if(r.status===404){state.textContent='Die Quellen sind dokumentiert. Der erste eigenständige Kursvergleich wurde noch nicht veröffentlicht.';return}
  if(!r.ok)throw Error('Datenabruf: HTTP '+r.status);
  const d=await r.json();
  if(d.schema_version!=='argus_external_thesis_followup_v1'||d.research_only!==true||
     d.scanner_changed!==false||d.qm_changed!==false||d.provider_score_produced!==false||
     !Array.isArray(d.claims)||d.claim_count!==d.claims.length)throw Error('Unzulässiger Forschungsstand');
  const observed=d.claims.reduce((n,c)=>n+Object.values(c.windows||{}).filter(w=>w.state==='OBSERVED').length,0);
  const pending=d.claims.reduce((n,c)=>n+Object.values(c.windows||{}).filter(w=>w.state==='NOT_YET_DUE').length,0);
  state.textContent='Datenstand '+d.as_of+' · '+d.claim_count+' datierte Thesen · '+observed+
    ' abgeschlossene Vergleichsfenster · '+pending+' noch offene Fenster. Diese Zahlen sind keine Anbieter-Trefferquote.';
  rows.innerHTML=d.claims.map(c=>{
   const windows=c.windows||{};
   const list=[1,5,10,20,40].map(h=>{
    const w=windows[String(h)]||{};
    const value=w.state==='OBSERVED'&&Number.isFinite(w.return_pct_adjusted)?
       (w.return_pct_adjusted>0?'+':'')+fmt(w.return_pct_adjusted)+' %':'offen';
    return '<span>+'+h+' Tage: <b>'+esc(value)+'</b></span>';
   }).join('');
   const prior=c.scanner_comparison;
   const compare=prior&&prior.state==='PRIOR_SNAPSHOT_OBSERVED'?
     'Vorausgehender Scanner-Snapshot vom '+esc(prior.scanner_as_of)+
     ': Score '+esc(prior.score)+' · Risiko '+esc(prior.risk)+
     ' · Confidence '+esc(prior.confidence)+'. Kein eigener Prognosebeweis.':
     'Historischer Scannervergleich: kein vor der These nachweisbar veröffentlichter, passender Scannerstand.';
   return '<article class="row"><h3>'+esc(c.symbol)+'</h3><p>Veröffentlichung: '+esc(c.publication_date)+
     ' · Ausgangskurs: '+(c.baseline?esc(fmt(c.baseline.close))+' '+esc(c.currency):'nicht verfügbar')+'</p>'+
     (c.state==='PRICE_SOURCE_UNAVAILABLE'?'<p>Kursquelle nicht nachprüfbar.</p>':
      '<div class="values">'+list+'</div>')+
     '<p>'+compare+'</p></article>';
  }).join('');
 }catch(e){state.textContent='Externe Vergleichsdaten konnten nicht gültig geladen werden: '+(e.name==='AbortError'?'Zeitüberschreitung':e.message);rows.replaceChildren()}
 finally{clearTimeout(timer)}
}
load();
})();
