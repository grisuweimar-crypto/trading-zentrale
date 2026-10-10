/* ARGUS External Thesis Watch – Presentation only, never an admission/scoring engine. */
(()=>{'use strict';
const host=document.getElementById('observations');if(!host)return;
const RAW='https://raw.githubusercontent.com/grisuweimar-crypto/trading-zentrale/refs/heads/main/';
const REF='argus-preview/observations/external_thesis_watch_reference.json';
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
'#etw .etwCard a{color:#f5cf91;text-decoration:underline;font-size:11px;overflow-wrap:anywhere}',
'#etw .etwNote{font-size:11px;color:#acbeca;line-height:1.7;margin:12px 0}',
'#etw .etwSources{margin-top:15px;border-top:1px solid #365268;padding-top:15px}',
'#etw .etwSources summary{cursor:pointer;font-size:12px;color:#f3d5a4}',
'#etw .etwSources p{font-size:11px;line-height:1.7;color:#bcd0dc}',
'#etw .etwEmpty{padding:13px;border:1px solid #496273;border-radius:8px;font-size:12px;color:#d7e3e8}',
'@media(max-width:900px){#etw .etwMetrics{grid-template-columns:repeat(2,minmax(0,1fr))}}',
'@media(max-width:680px){#etw{padding:14px}#etw .etwTitle h3{font-size:26px}#etw .etwList{grid-template-columns:1fr}#etw .etwTools button{font-size:11px;padding:9px}}'
].join('\n');
const st=document.createElement('style');st.textContent=css;st.id='etwStyles';document.head.appendChild(st);
host.insertAdjacentHTML('afterbegin',
'<section id="etw" aria-label="Externe Aufnahmekandidaten">'+
'<div class="etwTitle"><div><div class="etwGold">ARGUS · EXTERNAL THESIS WATCH</div>'+
'<h3>Kandidatenradar</h3><p>Externe Aktienideen getrennt vom regulären Scanner beobachten. Hier siehst du vorhandene Fallakten, neue Hinweise und die tatsächliche Scannerzugehörigkeit – keine automatische Aufnahme und keine Kaufempfehlung.</p></div>'+
'<div class="etwTools"><button type="button" id="etwImportBtn">↑ Private Hinweise laden</button><input type="file" id="etwFile" accept="application/json,.json" style="display:none" aria-label="Private Kandidaten-Datei auswählen">'+
'<button type="button" id="etwBackupBtn">↓ Private Sicherung</button><button type="button" id="etwRefresh">↻ Daten prüfen</button></div></div>'+
'<div id="etwMetrics" class="etwMetrics"></div><div class="etwGate" id="etwGate" role="status">Externe Referenz und Scannerstand werden geladen …</div>'+
'<div class="etwFilters"><button type="button" data-etw-filter="all" aria-pressed="true">Alle Fälle</button><button type="button" data-etw-filter="shadow" aria-pressed="false">Nicht im Scanner</button>'+
'<button type="button" data-etw-filter="scanner" aria-pressed="false">Im Scanner</button><button type="button" data-etw-filter="new" aria-pressed="false">Neue Hinweise</button>'+
'<input id="etwQuery" aria-label="Kandidaten durchsuchen" placeholder="Aktie oder Kürzel suchen …"></div>'+
'<div class="etwList" id="etwList"></div>'+
'<p class="etwNote">Private Newsletter-Begründungen werden nur nach einem lokalen JSON-Import angezeigt und ausschließlich in diesem Browser gespeichert. Sie werden nicht an GitHub übermittelt oder zwischen Geräten synchronisiert. Die originale Scanneraufnahme erfolgt ausschließlich außerhalb von ARGUS.</p>'+
'<details class="etwSources"><summary>Quellen und Grenzen dieser Darstellung</summary><p id="etwSourcesText">Datenstände werden nach dem Laden angezeigt.</p></details>'+
'</section>');
const el=id=>document.getElementById(id);
let baseline=[],priv={schema_version:'argus_external_thesis_private_v1',new_leads:[],source_updates:[],newsletters:[]};
let scanSymbols=null,scanMeta=null,filter='all',query='',running=false;
try{const saved=localStorage.getItem(KEY);if(saved){const parsed=JSON.parse(saved);if(parsed?.schema_version==='argus_external_thesis_private_v1'&&Array.isArray(parsed.new_leads)&&Array.isArray(parsed.source_updates))priv=parsed}}catch(_){}
async function get(path,format){
 const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),18000);
 try{const r=await fetch(RAW+path,{signal:controller.signal,cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status+' '+path);return format==='json'?r.json():r.text()}
 finally{clearTimeout(timer)}
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
const metric=(name,value,hint)=>'<div class="etwMetric"><span>'+esc(name)+'</span><b>'+esc(value)+'</b><small>'+esc(hint)+'</small></div>';
function card(item){
 const state=inScan(item.symbol);
 const status=state===null?'Scannerabgleich fehlt':state?'Im Scannerlauf':'Nicht im Scannerlauf';
 const manual=(item.oldGroup==='SHADOW')?'Ursprünglich separat geprüft · kein Universe-Eintrag':'';
 const label=item.historical?'Historischer Prüfvermerk: '+item.historical:'Neuer Hinweis · ungeprüft';
 const notes=item.notes.slice().sort((a,b)=>String(b.source_date??'').localeCompare(String(a.source_date??'')));
 const noteHTML=notes.length?notes.map(n=>
 '<div class="etwRationale"><small>'+esc(n.source_family??'Externe Quelle')+' · '+esc(formatDate(n.source_date))+'</small>'+
 '<p>'+esc(n.source_rationale??'Keine Quellenbegründung erfasst')+'</p>'+
 (safeLink(n.source_link)?'<a href="'+esc(safeLink(n.source_link))+'" target="_blank" rel="noopener noreferrer">Private Original-Mail öffnen ↗</a>':'')+'</div>').join(''):
 '<p class="etwNote">Die damalige externe E-Mail-Begründung ist hier noch nicht hinterlegt.</p>';
 return '<article class="etwCard"><div class="etwTop"><div><div class="etwSym">'+esc(item.symbol)+'</div><div class="etwName">'+esc(item.name)+'</div></div>'+
 '<span class="etwPill'+(state===false?' etwShadow':'')+'">'+esc(status)+'</span></div>'+
 '<p class="etwTag">'+esc(label)+'</p>'+(manual?'<p class="etwNote">'+esc(manual)+'</p>':'')+
 noteHTML+'</article>';
}
function render(){
 const all=merge();const newOnly=all.filter(x=>x.isNew);
 const known=all.filter(x=>inScan(x.symbol)===true).length;
 const unknown=all.filter(x=>inScan(x.symbol)===null).length;
 el('etwMetrics').innerHTML=metric('Frühere Fallakten',baseline.length,'getrennte externe Beobachtung')+
  metric('Neue Hinweise',newOnly.length,'nur nach privatem Import')+
  metric('Im Scannerlauf',unknown?'—':known,'aus aktueller Scannerpublikation')+
  metric('Außerhalb Scanner',unknown?'—':all.length-known,'nicht zur Aufnahme freigegeben');
 const visible=all.filter(x=>(filter==='all'||(filter==='new'&&x.isNew)||(filter==='scanner'&&inScan(x.symbol)===true)||
 (filter==='shadow'&&inScan(x.symbol)===false))&&
 (!query||[x.symbol,x.name,...x.notes.map(n=>n.source_rationale||'')].join(' ').toLowerCase().includes(query)));
 el('etwList').innerHTML=visible.length?visible.map(card).join(''):'<div class="etwEmpty">Keine entsprechenden Fälle im aktuellen Anzeigebestand. Prüfe gegebenenfalls den Filter oder importiere die private Kandidatendatei.</div>';
 const sources=Array.isArray(priv.newsletters)?priv.newsletters:[];
 el('etwSourcesText').innerHTML=
  'Öffentliche Basis: '+esc(REF)+'. Scannerzugehörigkeit: '+esc(META)+' und '+esc(SCAN)+'. '+
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