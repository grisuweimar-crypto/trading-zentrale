/* ARGUS External Thesis Watch – Presentation only, never an admission/scoring engine. */
(()=>{'use strict';
// Load the common explanation aid on the main ARGUS preview, without changing navigation.
const aid=document.createElement('script');
aid.src=new URL('argus-help.js',document.currentScript?.src||new URL('observations/',document.baseURI)).href;
aid.defer=true;document.head.appendChild(aid);
const host=document.getElementById('observations');if(!host)return;
const RAW='https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/main/';
const REF='argus-preview/observations/external_thesis_watch_reference.json';
const PUBLIC='argus-preview/observations/external_thesis_watch_candidates.json';
const ELLIOTT='https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/external-candidate-elliott-data/artifacts/research/candidate_elliott_current.json';
const META='artifacts/research/history_metadata.json';
const SCAN='artifacts/research/latest_scanner.csv';
const KEY='argus_external_thesis_private_v1';
const esc=x=>String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const safeLink=x=>typeof x==='string'&&x.startsWith('https://mail.google.com/mail/u/')?x:null;
const formatDate=x=>/^\d{4}-\d{2}-\d{2}$/.test(x||'')?x.split('-').reverse().join('.'):'Nicht dokumentiert';
const css=[
'#etw{margin:6px 0 28px;background:linear-gradient(125deg,#0e2e43,#071e31 85%);border:1px solid #506980;border-radius:14px;padding:23px;box-shadow:0 10px 35px rgba(0,0,0,.14)}',
'#etw .etwTitle{display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:12px}',
'#etw .etwTitle h3{font:normal 30px Georgia,serif;color:#f5dfb0;line-height:1.1;margin:7px 0}',
'#etw .etwTitle p{font-size:13px;line-height:1.7;color:#c2d4e0;max-width:740px;margin:8px 0 4px}',
'#etw .etwGold{font-size:11px;letter-spacing:.15em;color:#d8b77b;text-transform:uppercase}',
'#etw .etwTools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:9px}',
'#etw button{border:1px solid #78817b;border-radius:8px;padding:10px 12px;color:#f6e4bf;background:#15374c;font-size:12px;cursor:pointer}',
'#etw button:focus-visible,#etw input:focus-visible{outline:2px solid #e0bc77}',
'#etw button:disabled{opacity:.5;cursor:wait}',
'#etw .etwMetrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:20px 0 11px}',
'#etw .etwMetric{padding:13px;background:#0e2a40;border:1px solid #3e5e73;border-radius:11px;min-width:0}',
'#etw .etwMetric span{font-size:11px;color:#bad0dc;display:block;line-height:1.4;min-height:32px}',
'#etw .etwMetric b{display:block;font:normal 33px Georgia,serif;color:#f5dfa9;margin-top:4px}',
'#etw .etwGate{background:#102b3e;border-left:3px solid #cca864;color:#c1d2df;padding:12px 14px;font-size:11px;line-height:1.6;border-radius:0 8px 8px 0;margin:13px 0}',
'#etw .etwFilters{display:flex;align-items:center;gap:7px;flex-wrap:wrap;margin-top:17px}',
'#etw .etwFilters button{border-color:#3c586c;background:#102b3f;color:#bfcfda}',
'#etw .etwFilters button[aria-pressed="true"]{color:#ffe5b3;border-color:#caa76a;background:#224158}',
'#etw #etwQuery{min-width:150px;flex:1;padding:11px;background:#071d2e;border:1px solid #466175;border-radius:8px;color:#f3e3c3;font-size:12px}',
'#etw .etwList{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px;margin-top:12px}',
'#etw .etwCard{background:#092236;border:1px solid #355267;border-radius:11px;padding:15px;min-width:0;line-height:1.5}',
'#etw .etwTop{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;flex-wrap:wrap}',
'#etw .etwSym{font:normal 23px Georgia,serif;color:#f0d9a7}',
'#etw .etwName{font-size:12px;color:#d8e4ea;line-height:1.6}',
'#etw .etwPill{display:inline-block;font-size:10px;line-height:1.25;padding:5px 8px;border-radius:99px;background:#193b4c;border:1px solid #4f6575;color:#cbd8df}',
'#etw .etwPill.etwShadow{border-color:#98845d;color:#efd5a3;background:#302c2a}',
'#etw .etwTag{font-size:11px;color:#bdd0db;margin:13px 0 8px}',
'#etw .etwTag strong{color:#efd19c}',
'#etw .etwRationale{border-left:2px solid #b79c67;padding:7px 10px;background:#102c41;border-radius:0 7px 7px 0;margin-top:10px}',
'#etw .etwRationale p{font-size:11px;color:#e2eaf0;margin:6px 0}',
'#etw .etwRationale small{font-size:10px;color:#adc3d1}',
'#etw .etwElliott{font-size:11px;color:#c9deea;background:#102d42;border-left:2px solid #caab72;padding:9px 11px;margin-top:10px}',
'#etw .etwElliott details{margin-top:6px}#etw .etwElliott summary{cursor:pointer;color:#f2d5a0}',
'#etw .etwCard a{color:#f5cf91;text-decoration:underline;font-size:11px;overflow-wrap:anywhere}',
'#etw .etwNote{font-size:11px;color:#acbeca;line-height:1.7;margin:12px 0}',
'#etw .etwSources{margin-top:15px;border-top:1px solid #365268;padding-top:15px}',
'#etw .etwSources summary{cursor:pointer;font-size:12px;color:#f3d5a4}',
'#etw .etwSources p{font-size:11px;line-height:1.7;color:#bcd0dc}',
'#etw .etwEmpty{padding:13px;border:1px solid #496273;border-radius:8px;font-size:12px;color:#d7e3e8}',
'@media(max-width:900px){#etw .etwMetrics{grid-template-columns:repeat(2,minmax(0,1fr))}}',
'@media(max-width:680px){#etw{padding:14px}#etw .etwTitle h3{font-size:26px}#etw .etwList{grid-template-columns:1fr}#etw .etwTools button{font-size:11px;padding:9px}}'
].join('\n');
const st=document.createElement('style');st.textContent=css+"\n#etw .etwExplainer{background:linear-gradient(130deg,#18394c,#0b2537);border:1px solid #4d687b;border-radius:10px;padding:14px 16px;margin:17px 0 9px;font-size:12px;line-height:1.7;color:#d9e4eb}\n#etw .etwExplainer b{color:#f2d9a7}\n#etw .etwTech{margin:8px 0;border-top:1px solid #355267;padding-top:8px}\n#etw .etwTech summary{color:#efd4a4;cursor:pointer;font-size:11px}\n#etw .etwTech summary:focus-visible{outline:2px solid #e0bc77}\n#etw .etwElliott p{margin:8px 0;line-height:1.6}\n#etw .etwMetric small{color:#a9c4d2;line-height:1.4;font-size:10px}"+"\n#etw .etwWave{font-size:11px;border-top:1px solid #355267;padding:8px 0;line-height:1.6}";st.id='etwStyles';document.head.appendChild(st);
host.insertAdjacentHTML('afterbegin',
'<section id="etw" aria-label="Externe Aufnahmekandidaten">'+
'<div class="etwTitle"><div><div class="etwGold">ARGUS · EXTERNAL THESIS WATCH</div>'+
'<h3>Kandidatenradar</h3><p>Hier sammeln wir Aktienideen aus externen Quellen, auch wenn sie noch nicht im Scanner stehen. Eine Erwähnung ist weder eine Kaufempfehlung noch eine Entscheidung zur Aufnahme.</p></div>'+
'<div class="etwTools"><button type="button" id="etwImportBtn">↑ Private Notizen öffnen</button><input type="file" id="etwFile" accept="application/json,.json" style="display:none" aria-label="Private Kandidaten-Datei auswählen">'+
'<button type="button" id="etwBackupBtn">↓ Eigene Notizen sichern</button><button type="button" id="etwRefresh">↻ Beobachtungen aktualisieren</button></div></div>'+
'<div class="etwExplainer"><b>Was gehört hierher?</b> Externe <b>Aktienkandidaten</b>, die wir beobachten möchten. Die <b>Musterkandidaten</b> im Forschungsstand sind etwas anderes: Dort werden statistische Zusammenhänge geprüft. <b>Im Scanner</b> bedeutet nur, dass die Aktie im aktuellen Scannerlauf vorkommt – nicht, dass sie gekauft werden sollte.</div>'+ 
'<div id="etwMetrics" class="etwMetrics"></div><div class="etwGate" id="etwGate" role="status">Externe Referenz und Scannerstand werden geladen …</div>'+
'<div class="etwFilters"><button type="button" data-etw-filter="all" aria-pressed="true">Alle Fälle</button><button type="button" data-etw-filter="shadow" aria-pressed="false">Außerhalb Scanner</button>'+
'<button type="button" data-etw-filter="scanner" aria-pressed="false">Im Scanner enthalten</button><button type="button" data-etw-filter="new" aria-pressed="false">Neue Beobachtungen</button>'+
'<input id="etwQuery" aria-label="Kandidaten durchsuchen" placeholder="Aktie oder Kürzel suchen …"></div>'+
'<div class="etwList" id="etwList"></div>'+
'<p class="etwNote">Eigene Notizen bleiben ausschließlich in diesem Browser, sofern sie nicht ausdrücklich als Sicherungsdatei exportiert werden. ARGUS entscheidet nicht über die Aufnahme in den Scanner. Der beobachtete Kursverlauf beweist allein noch nicht die Qualität einer externen These.</p>'+
'<details class="etwSources"><summary>Quellen und Grenzen dieser Darstellung</summary><p id="etwSourcesText">Datenstände werden nach dem Laden angezeigt.</p></details>'+
'</section>');
const el=id=>document.getElementById(id);
let baseline=[],publicCandidates=[],candidateElliott=null,priv={schema_version:'argus_external_thesis_private_v1',new_leads:[],source_updates:[],newsletters:[]};
let scanSymbols=null,scanMeta=null,filter='all',query='',running=false;
try{const saved=localStorage.getItem(KEY);if(saved){const parsed=JSON.parse(saved);if(parsed?.schema_version==='argus_external_thesis_private_v1'&&Array.isArray(parsed.new_leads)&&Array.isArray(parsed.source_updates))priv=parsed}}catch(_){}
async function get(path,format){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),18000);
 try{const r=await fetch(RAW+path,{signal:controller.signal,cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status+' '+path);return format==='json'?r.json():r.text()}
 finally{clearTimeout(timer)}
}
async function getCandidateElliott(){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),18000);
 try{
  const r=await fetch(ELLIOTT,{signal:controller.signal,cache:'no-store'});
  if(r.status===404)return null;
  if(!r.ok)throw Error('HTTP '+r.status);
  const d=await r.json();
  if(d?.schema_version!=='external_candidate_elliott_shadow_v1'||d?.research_only!==true||
   d?.scanner_universe_unchanged!==true||d?.symbol!=='LULU'||!/^\d{4}-\d{2}-\d{2}$/.test(d.as_of||'')||
   !Array.isArray(d.outputs)||d.output_count!==d.outputs.length||
   !d.outputs.every(o=>o.symbol==='LULU'&&o.research_only===true))throw Error('Ungültiger Elliott-Schattenstand');
  return d;
 }finally{clearTimeout(timer)}
}
function csvRows(text){
 const rows=[],max=20000;let row=[],field='',q=false;
 for(let i=0;i<text.length;i++){
  const c=text[i];
  if(c==='"'){if(q&&text[i+1]==='"'){field+='"';i++}else q=!q}
  else if(c===','&&!q){row.push(field);field=''}
  else if((c==='\n'||c==='\r')&&!q){if(c==='\r'&&text[i+1]==='\n')i++;row.push(field);field='';if(row.length>1)rows.push(row);row=[];if(rows.length>max)throw Error('Scannerarchiv zu groß')}
  else field+=c;
 }
 if(q)throw Error('CSV-Anführungszeichen nicht geschlossen');
 row.push(field);if(row.length>1)rows.push(row);
 return rows;
}
async function refresh(){
 if(running)return;running=true;el('etwRefresh').disabled=true;
 el('etwGate').textContent='Referenz und Scanner-Mitgliedschaft werden neu geprüft …';
 const errors=[];
 try{
  const d=await get(REF,'json');
  if(d?.schema_version!=='argus_external_thesis_reference_v1'||!Array.isArray(d.items)||d.items.length!==10||new Set(d.items.map(x=>x.symbol)).size!==10)throw Error('Referenzstruktur unvollständig');
  baseline=d.items;
 }catch(e){baseline=[];errors.push('Referenz: '+e.message)}
 try{
  const d=await get(PUBLIC,'json');
  if(d?.schema_version!=='argus_external_thesis_public_candidates_v1'||!Array.isArray(d.items)||
   !d.items.every(x=>x&&/^[A-Z0-9.^-]{1,20}$/.test(x.symbol||'')&&typeof x.name==='string'&&x.research_only===true)||
   new Set(d.items.map(x=>x.symbol)).size!==d.items.length||
   d.items.some(x=>baseline.some(b=>b.symbol===x.symbol)))throw Error('Kandidatenreferenz ungültig');
  publicCandidates=d.items;
 }catch(e){publicCandidates=[];errors.push('Kandidaten: '+e.message)}
 try{candidateElliott=await getCandidateElliott()}
 catch(e){candidateElliott=null;errors.push('Elliott-Kandidatenstand: '+e.message)}
 try{
  const meta=await get(META,'json');
  if(meta?.latest_run_complete!==true||meta?.validation?.status!=='ok'||!meta.snapshot_id)throw Error('Scanner-Publikation nicht vollständig');
  const rows=csvRows(await get(SCAN,'text'));const h=rows.shift();const ix={};
  for(let i=0;i<h.length;i++)ix[h[i]]=i;
  for(const key of ['symbol','snapshot_id','observation_type','data_source'])if(ix[key]===undefined)throw Error('Scanner-Spalte '+key+' fehlt');
  const symbols=new Set();
  for(const row of rows){
   if(row[ix.snapshot_id]!==meta.snapshot_id||row[ix.observation_type]!=='observed_scanner'||row[ix.data_source]!=='scanner_run'||!row[ix.symbol]||symbols.has(row[ix.symbol]))throw Error('Snapshot oder Scanner-Zeile inkonsistent');
   symbols.add(row[ix.symbol]);
  }
  if(meta.latest_scanner?.row_count!==undefined&&meta.latest_scanner.row_count!==rows.length)throw Error('Scanner-Zeilenzahl stimmt nicht');
  if(meta.latest_scanner?.symbol_count!==undefined&&meta.latest_scanner.symbol_count!==symbols.size)throw Error('Scanner-Titelzahl stimmt nicht');
  scanSymbols=symbols;scanMeta={as_of:meta.as_of,snapshot_id:meta.snapshot_id,count:symbols.size};
 }catch(e){scanSymbols=null;scanMeta=null;errors.push('Scannerabgleich: '+e.message)}
 el('etwGate').textContent=errors.length?'Eingeschränkter Datenstand: '+errors.join(' | ')+'. Nicht überprüfbare Scanner-Zugehörigkeit bleibt unbekannt.':
  'Scanner-Snapshot vom '+scanMeta.as_of+' · '+scanMeta.count+' echte Scanner-Titel · Snapshot-ID '+scanMeta.snapshot_id+'. Die externe Referenz ist davon fachlich getrennt.';
 running=false;el('etwRefresh').disabled=false;render();
}
function merge(){
 const m=new Map();
 for(const item of baseline){m.set(item.symbol,{symbol:item.symbol,name:item.name,historical:item.historical_label,oldGroup:item.original_group,isNew:false,notes:[]})}
 for(const s of publicCandidates){
  m.set(s.symbol,{symbol:s.symbol,name:s.name,historical:null,oldGroup:'EXTERNAL_WATCH',
   isNew:true,notes:[s],externalResearch:true});
 }
 for(const s of Array.isArray(priv.new_leads)?priv.new_leads:[]){
  if(!s||typeof s.symbol!=='string'||!s.symbol.match(/^[A-Z0-9.^-]{1,20}$/))continue;
  const item=m.get(s.symbol)??{symbol:s.symbol,name:s.name??s.symbol,historical:null,oldGroup:null,isNew:true,notes:[]};
  item.notes.push(s);m.set(s.symbol,item)
 }
 for(const s of Array.isArray(priv.source_updates)?priv.source_updates:[]){
  if(!s||!m.has(s.symbol))continue;m.get(s.symbol).notes.push(s)
 }
 return [...m.values()];
}
const inScan=s=>scanSymbols===null?null:scanSymbols.has(s);
const readableTimeframe=x=>({daily:'Tageschart',weekly:'Wochenchart'})[x]||'Zeitraum nicht erläutert';
const readableDegree=x=>({minor:'kleiner Wellengrad',intermediate:'mittlerer Wellengrad',major:'größerer Wellengrad',primary:'übergeordneter Wellengrad'})[x]||'Wellengrad nicht erläutert';
const readablePattern=x=>({impulse:'Impulsfolge',zigzag:'Zickzack-Korrektur',flat:'Flache Korrektur',triangle:'Dreiecks-Korrektur',double_three:'Mehrteilige Korrektur (W–X–Y)',triple_three:'Komplexe Korrektur (W–X–Y–X–Z)',leading_diagonal:'Einleitende Diagonale',ending_diagonal:'Abschlussdiagonale'})[x]||'Wellenstruktur nicht näher eingeordnet';
const readableStage=x=>({wave_2_complete:'zweite Welle strukturell abgeschlossen',wave_3_complete:'dritte Welle strukturell abgeschlossen',wave_4_complete:'vierte Welle strukturell abgeschlossen',wave_5_complete:'fünfte Welle strukturell abgeschlossen'})[x]||'Wellenphase nicht allgemein übersetzt';
const metric=(name,value,hint)=>'<div class="etwMetric"><span>'+esc(name)+'</span><b>'+esc(value)+'</b><small>'+esc(hint)+'</small></div>';
function card(item){
 const state=inScan(item.symbol);
 const status=state===null?'Zugehörigkeit ungeklärt':state?'Im aktuellen Scanner':'Außerhalb des Scanners';
 const manual=(item.oldGroup==='SHADOW')?'Früher separat beobachtet. Ob die Aktie heute im Scanner steht, zeigt nur der aktuelle Abgleich.':'';
 const label=item.historical?'Ältere externe Beobachtung · damalige Kennzeichnung, keine heutige Freigabe':'Externe Aktienidee · noch nicht als Handelssignal bestätigt';
 const oldCode=item.historical?'<details class="etwTech"><summary>Historischen Originalvermerk anzeigen</summary><code>'+esc(item.historical)+'</code> · Nur die damalige Bezeichnung, keine neue Aufnahmeentscheidung.</details>':'';
 const notes=item.notes.slice().sort((a,b)=>String(b.source_date??'').localeCompare(String(a.source_date??'')));
 const ew=candidateElliott&&candidateElliott.symbol===item.symbol?candidateElliott:null;
 const ewHtml=item.externalResearch?(!ew?'<div class="etwElliott">Eigene Elliott-Forschung: Es liegt noch keine auswertbare Beobachtung für diesen Kandidaten vor.</div>':
 '<div class="etwElliott"><b>Eigene Elliott-Wellenbeobachtung</b> · Kursdatenstand '+esc(formatDate(ew.as_of))+' · '+esc(ew.output_count)+' dokumentierte Analysevarianten. Das sind keine Kauf- oder Verkaufssignale und keine Bestätigung der externen Chartthese.'+
 (ew.outputs.length?'<details><summary>Die einzelnen Wellenmodelle verstehen</summary><p>Mehrere Wellengrade und Szenarien können gleichzeitig vorliegen. Die Reihenfolge ist keine Rangliste nach Gewinnaussicht.</p>'+ew.outputs.map(o=>{
  const p=o.primary_scenario||{};
  return '<div class="etwWave"><b>'+esc(readableTimeframe(o.timeframe))+' · '+esc(readableDegree(o.degree))+'</b>: '+esc(readablePattern(p.pattern_class))+
   ' · '+esc(readableStage(p.stage))+' · '+esc((o.alternative_scenarios||[]).length)+' weitere mögliche Deutungen'+
   '<details class="etwTech"><summary>Originale Wellenkennungen</summary><code>'+esc(o.timeframe)+' / '+esc(o.degree)+' · '+esc(p.pattern_class??'unavailable')+' · '+esc(p.stage??'unavailable')+'</code></details></div>';
 }).join('')+'</details>':'<p>Die Berechnung hat noch keine ausgabefähige Wellenstruktur erfasst. Ein fehlender Zustand ist kein neutrales Signal.</p>')+'</div>'):'';
 const noteHTML=notes.length?notes.map(n=>
 '<div class="etwRationale"><small>'+esc(n.source_family??'Externe Quelle')+' · '+esc(formatDate(n.source_date))+'</small>'+
 '<p>'+esc(n.source_rationale??'Keine Quellenbegründung erfasst')+'</p>'+
 (safeLink(n.source_link)?'<a href="'+esc(safeLink(n.source_link))+'" target="_blank" rel="noopener noreferrer">Private Original-Mail öffnen ↗</a>':'')+'</div>').join(''):
 '<p class="etwNote">Die damalige externe E-Mail-Begründung ist hier noch nicht hinterlegt.</p>';
 return '<article class="etwCard"><div class="etwTop"><div><div class="etwSym">'+esc(item.symbol)+'</div><div class="etwName">'+esc(item.name)+'</div></div>'+
 '<span class="etwPill'+(state===false?' etwShadow':'')+'">'+esc(status)+'</span></div>'+
 '<p class="etwTag">'+esc(label)+'</p>'+oldCode+(manual?'<p class="etwNote">'+esc(manual)+'</p>':'')+
 noteHTML+ewHtml+'</article>';
}
function render(){
 const all=merge();const newOnly=all.filter(x=>x.isNew);
 const known=all.filter(x=>inScan(x.symbol)===true).length;
 const unknown=all.filter(x=>inScan(x.symbol)===null).length;
 el('etwMetrics').innerHTML=metric('Ältere Beobachtungen',baseline.length,'historische Fallakten, keine heutigen Bewertungen')+
  metric('Weitere Aktienideen',newOnly.length,'neue Kandidaten und eigene Hinweise')+
  metric('Im aktuellen Scanner',unknown?'—':known,'Mitgliedschaft, kein Kaufsignal')+
  metric('Noch außerhalb',unknown?'—':all.length-known,'zur Beobachtung, nicht aufgenommen');
 const visible=all.filter(x=>(filter==='all'||(filter==='new'&&x.isNew)||(filter==='scanner'&&inScan(x.symbol)===true)||
 (filter==='shadow'&&inScan(x.symbol)===false))&&
 (!query||[x.symbol,x.name,...x.notes.map(n=>n.source_rationale||'')].join(' ').toLowerCase().includes(query)));
 el('etwList').innerHTML=visible.length?visible.map(card).join(''):'<div class="etwEmpty">Keine entsprechenden Fälle im aktuellen Anzeigebestand. Prüfe gegebenenfalls den Filter oder prüfe die verfügbaren Quellen.</div>';
 const sources=Array.isArray(priv.newsletters)?priv.newsletters:[];
 el('etwSourcesText').innerHTML=
  'Öffentliche Basis: '+esc(REF)+'; '+esc(PUBLIC)+'. Elliott-Kandidatenstand: isolierter Research-Schattenzweig. Scannerzugehörigkeit: '+esc(META)+' und '+esc(SCAN)+'. '+
  'Private Ausgangsthesen werden weder an den Scanner noch an GitHub übertragen. Originalstatuscodes aus der historischen Referenz sind keine neuen Entscheidungen.'+
  (sources.length?'<br><br><b>Geprüfte Newsletter-Typen:</b> '+sources.map(n=>'<p><strong>'+esc(n.family)+'</strong>: '+esc(n.suitability)+'</p>').join(''):'');
}
function checkInput(d){
 if(d?.schema_version!=='argus_external_thesis_private_v1'||!Array.isArray(d.new_leads)||!Array.isArray(d.source_updates))throw Error('Unbekanntes Importformat');
 if(d.new_leads.length>200||d.source_updates.length>500)throw Error('Zu viele Einträge');
 const valid=x=>x&&typeof x.symbol==='string'&&/^[A-Z0-9.^-]{1,20}$/.test(x.symbol)&&(!x.source_rationale||typeof x.source_rationale==='string'&&x.source_rationale.length<=4000);
 if(!d.new_leads.every(valid)||!d.source_updates.every(valid))throw Error('Ungültige Titel- oder Quellentexte');
 return{schema_version:d.schema_version,new_leads:d.new_leads,source_updates:d.source_updates,newsletters:Array.isArray(d.newsletters)?d.newsletters.slice(0,20):[]};
}
el('etwImportBtn').addEventListener('click',()=>el('etwFile').click());
el('etwFile').addEventListener('change',async e=>{
 const file=e.target.files?.[0];if(!file)return;
 if(file.size>2e6){el('etwGate').textContent='Die gewählte Importdatei ist zu groß.';return}
 try{const newData=checkInput(JSON.parse(await file.text()));localStorage.setItem(KEY,JSON.stringify(newData));priv=newData;
  el('etwGate').textContent='Private Hinweise wurden lokal geladen: '+priv.new_leads.length+' neue Hinweise, '+priv.source_updates.length+' bestehende Fälle ergänzt. Scanneraufnahme unverändert.';
  render();
 }catch(e){el('etwGate').textContent='Import abgebrochen: '+e.message}
 e.target.value='';
});
el('etwBackupBtn').addEventListener('click',()=>{
 const blob=new Blob([JSON.stringify(priv,null,2)+'\n'],{type:'application/json;charset=utf-8'});
 const url=URL.createObjectURL(blob),a=document.createElement('a');
 a.href=url;a.download='ARGUS_External_Thesis_Private_Sicherung.json';a.click();URL.revokeObjectURL(url);
});
el('etwRefresh').addEventListener('click',refresh);
el('etwQuery').addEventListener('input',e=>{query=e.target.value.toLowerCase().trim();render()});
for(const b of host.querySelectorAll('[data-etw-filter]'))b.addEventListener('click',()=>{
 filter=b.dataset.etwFilter;for(const x of host.querySelectorAll('[data-etw-filter]'))x.setAttribute('aria-pressed',String(x===b));render()
});
render();refresh();
})();