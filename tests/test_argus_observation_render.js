/* Browser-like rendering regression for historic external-thesis cards.
 * Uses actual released reference JSON; no research or trading logic is mocked. */
'use strict';
const fs=require('fs');
const path=require('path');
const vm=require('vm');
const assert=require('node:assert/strict');

const root=path.resolve(__dirname,'..');
const code=fs.readFileSync(path.join(root,'argus-preview/observations/external-thesis-watch.js'),'utf8');
const historical=JSON.parse(fs.readFileSync(path.join(root,'argus-preview/observations/external_thesis_watch_reference.json'),'utf8'));
const candidates=JSON.parse(fs.readFileSync(path.join(root,'argus-preview/observations/external_thesis_watch_candidates.json'),'utf8'));
const notes=JSON.parse(fs.readFileSync(path.join(root,'argus-preview/observations/external_thesis_source_notes.json'),'utf8'));

async function render(privateNotes=null,followup=null){
 const els=new Map();
 const makeEl=()=>({innerHTML:'',textContent:'',disabled:false,addEventListener(){},appendChild(){},querySelectorAll(){return[];}});
 const host=makeEl();host.insertAdjacentHTML=()=>{};
 const getElementById=id=>{
  if(id==='observations')return host;
  if(!els.has(id))els.set(id,makeEl());
  return els.get(id);
 };
 const scannerCSV='symbol,snapshot_id,observation_type,data_source\nIFX.DE,snapshot-test,observed_scanner,scanner_run\n';
 const metadata={latest_run_complete:true,validation:{status:'ok'},snapshot_id:'snapshot-test',
  as_of:'2026-10-09',latest_scanner:{row_count:1,symbol_count:1}};
 const fetch=async url=>{
  let value;
  if(url.includes('external_thesis_watch_reference.json'))value=historical;
  else if(url.includes('external_thesis_watch_candidates.json'))value=candidates;
  else if(url.includes('external_thesis_source_notes.json'))value=notes;
  else if(url.includes('external_thesis/latest.json')){if(!followup)return {ok:false,status:404};value=followup;}
  else if(url.includes('candidate_elliott_current.json'))return {ok:false,status:404};
  else if(url.includes('history_metadata.json'))value=metadata;
  else if(url.includes('latest_scanner.csv'))value=scannerCSV;
  else throw Error('Unexpected URL '+url);
  return {ok:true,status:200,json:async()=>value,text:async()=>typeof value==='string'?value:JSON.stringify(value)};
 };
 const document={
  currentScript:{src:'https://example.org/argus-preview/observations/external-thesis-watch.js'},
  createElement:()=>makeEl(),head:{appendChild(){}},getElementById
 };
 const localStorage={getItem:()=>privateNotes?JSON.stringify(privateNotes):null};
 vm.runInNewContext(code,{document,localStorage,fetch,URL,AbortController,setTimeout,clearTimeout,console},
  {filename:'external-thesis-watch.js',timeout:3000});
 // refresh() fetches four resources sequentially; wait for the actual rendered state.
 for(let i=0;i<40&&!els.get('etwList')?.innerHTML.includes('Infineon');i++){
   await new Promise(resolve=>setTimeout(resolve,5));
 }
 assert.ok(els.get('etwList')?.innerHTML.includes('Infineon'),'Loaded candidate list missing');
 return els.get('etwList').innerHTML;
}

(async()=>{
 const html=await render();
 const ix=html.indexOf('Infineon');
 const near=html.slice(ix,ix+3600);
 assert.match(near,/Im aktuellen Scanner/,'Infineon should reflect scanner membership only');
 assert.match(near,/Damals uneinheitlich beurteilt/,'Historic MIXED must be clearly separated from source idea');
 assert.match(near,/nicht aus einem Rating des Newsletter-Anbieters/);
 assert.match(near,/Warum wurde die Aktie vorgeschlagen\?/);
 assert.match(near,/Eaton/);
 assert.match(near,/Gewinnmitnahmen nach starkem Kursanstieg/);
 assert.match(near,/65 EUR/);
 assert.match(near,/Originalnewsletter derzeit nicht erneut geprüft/);
 assert.match(near,/<summary>Originalen Archivvermerk anzeigen<\/summary><code>MIXED<\/code>/);
 assert.doesNotMatch(near,/Historischer Prüfvermerk: MIXED/);
 assert.doesNotMatch(html,/Die damalige externe E-Mail-Begründung ist hier noch nicht hinterlegt/);
 const future=html.slice(html.indexOf('Berkshire Hathaway B'),html.indexOf('Berkshire Hathaway B')+2600);
 assert.match(future,/Früherer Vergleichsvermerk/);
 assert.match(future,/Greg Abel/);
 assert.match(future,/<code>PROMOTE_READY<\/code>/);
 assert.doesNotMatch(future,/Freigegeben zum Kauf/);

 const privateNotes={schema_version:'argus_external_thesis_private_v1',new_leads:[],
  source_updates:[{symbol:'IFX.DE',source_family:'Privater Hinweis',source_date:'2026-10-09',
   source_rationale:'Datierter, separat dokumentierter Nachtrag.'}],newsletters:[]};
 const withPrivate=await render(privateNotes);
 assert.match(withPrivate,/Datierter, separat dokumentierter Nachtrag/);
 assert.match(withPrivate,/Damals uneinheitlich beurteilt/);
 assert.match(withPrivate,/Eaton/);
 assert.match(html,/Siemens/);
 assert.match(html,/Lululemon Athletica/);
 const dated={schema_version:'argus_external_thesis_followup_v1',research_only:true,scanner_changed:false,provider_score_produced:false,as_of:'2026-10-10',claim_count:1,claims:[{symbol:'IFX.DE',publication_date:'2026-10-08',currency:'EUR',external_target:65,baseline:{date:'2026-10-07',close:60},windows:{'1':{state:'OBSERVED',date:'2026-10-09',return_pct_adjusted:2.5,target_touch:{seen:false,first_date:null,provider_success:null}},'5':{state:'NOT_YET_DUE'}}}]};
 const tested=await render(null,dated);
 assert.match(tested,/Was ist aus der fremden These geworden\?/);
 assert.match(tested,/\+2,5 %/);
 assert.match(tested,/Noch offen/);
 assert.match(tested,/keine<\/b> bestätigte Trefferquote/);
 dated.claims[0].scanner_comparison={
   state:'PRIOR_SNAPSHOT_OBSERVED',scanner_as_of:'2026-10-07',
   published_at:'2026-10-07T19:00:00+00:00',snapshot_id:'historic-tested',
   score:'62.5',risk:'31.3',confidence:'68.1'
 };
 const withPIT=await render(null,dated);
 assert.match(withPIT,/Damals gespeicherter Scannerstand/);
 assert.match(withPIT,/Score 62.5/);
 assert.doesNotMatch(withPIT,/heutige Werte/);
 console.log('ARGUS cards: newsletter reasons, verified preview dates, prior analyses and private notes remain separate.');
})().catch(err=>{console.error(err);process.exitCode=1;});
