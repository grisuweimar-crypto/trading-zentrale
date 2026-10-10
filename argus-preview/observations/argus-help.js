/* ARGUS PANOPTES – sitewide plain-language glossary; pure presentation. */
(()=>{
'use strict';
if(document.getElementById('argusClarityGlossary'))return;
const entries=[
 ['Grundlagen',[
  ['Scanner','Erfasst den veröffentlichten Zustand der Wertpapiere im Scanner_vNext. Ein Eintrag ist noch keine Kaufempfehlung.'],
  ['Scanner-Universum','Die Aktien und weiteren Instrumente, die der produktive Scanner grundsätzlich berücksichtigt. Ein externer Kandidat kann außerhalb dieses Universums beobachtet werden.'],
  ['Snapshot / Datenaufnahme','Ein festgehaltener Stand zu einem bestimmten Zeitpunkt. Aussagen aus verschiedenen Ständen dürfen nicht unbemerkt vermischt werden.'],
  ['Score','Zusammenfassende Scannerkennzahl. Nicht mit einer Gewinnwahrscheinlichkeit oder einer Kaufanweisung gleichsetzen.'],
  ['Risiko / Risk','Hinweise auf Risiken aus der jeweils ausgewiesenen Datenbasis. Keine Garantie für mögliche Verluste.'],
  ['Confidence','Einschätzung der Belastbarkeit oder Unsicherheit der jeweils geprüften Evidenz. Keine Gewinngarantie.'],
  ['Decision / Handlungseinordnung','Ein gesonderter Interpretationsschritt auf Grundlage veröffentlichter Evidenz. Ein Forschungs- oder Beobachtungsvermerk ist nicht automatisch eine Handlungsempfehlung.']
 ]],
 ['Beobachtungen',[
  ['Aktienkandidat','Eine extern erwähnte Aktie, die separat angesehen wird. Das bedeutet weder Scanneraufnahme noch Kaufempfehlung.'],
  ['Historischer Prüfvermerk','Die damals gespeicherte Einordnung eines Falls. Sie ist nicht automatisch die aktuelle Bewertung.'],
  ['Im aktuellen Scanner','Die Aktie erscheint im nachgeprüften jüngsten Scannerstand. Daraus folgt keine positive Kaufentscheidung.'],
  ['Elliott vNext','Das Forschungsmodul für mehrere mögliche Elliott-Wellenstrukturen und Fibonacci-Bereiche. Es ist kein automatischer Handelssignalgeber.'],
  ['Wellenzählung','Eine mögliche Deutung von Kursbewegungen. Andere plausible Deutungen können gleichzeitig bestehen.']
 ]],
 ['Forschungsstand',[
  ['Musterkandidat','Ein statistischer Zusammenhang, dessen mögliche Wiederholung untersucht wird. Das ist kein Aktien-Aufnahmekandidat.'],
  ['Vorregistrierung','Die Prüfidee und Regeln werden festgehalten, bevor die späteren Ergebnisse bekannt sind.'],
  ['Prospektiv','Eine Beobachtung wird im Voraus dokumentiert und erst später an echten Marktdaten gemessen.'],
  ['Punkt-in-Zeit / Point-in-Time','Zur Bewertung dürfen nur Daten verwendet werden, die am jeweiligen historischen Zeitpunkt schon verfügbar waren.'],
  ['Forschungsfreigabe','Eine gesonderte, nachgewiesene Entscheidung im Research-System. Eine grüne technische Prüfung ersetzt sie nicht.'],
  ['QM / Qualitätsmanagement','Prüft die Dokumentation, Fehlerbehandlung und Zuverlässigkeit der Prozesse. Ein erfolgreicher QM-Test beweist nicht, dass eine Kursprognose stimmt.'],
  ['L14','Betriebs- und Kontrollübersicht für die vorhandene Musterforschung. ARGUS zeigt den veröffentlichten Stand und erzeugt dort selbst keine Forschungsbewertung.']
 ]]
];
const esc=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[c]));
const styles=[
 '#argusClarityGlossary{position:fixed;right:14px;bottom:14px;z-index:4000;max-width:min(430px,calc(100vw - 20px));color:#ecf1f3;font-family:Inter,system-ui,sans-serif}',
 '#argusClarityGlossary>summary{display:block;margin-left:auto;list-style:none;width:max-content;max-width:100%;cursor:pointer;background:#18374b;border:1px solid #bd9e6a;border-radius:10px;padding:11px 16px;box-shadow:0 8px 25px #0007;color:#f7ddb1;font-size:13px;font-weight:650;line-height:1.4}',
 '#argusClarityGlossary>summary::-webkit-details-marker{display:none}',
 '#argusClarityGlossary>summary:focus-visible{outline:2px solid #e7c588;outline-offset:2px}',
 '#argusClarityGlossary .argusClarityBody{max-height:min(66vh,650px);width:min(430px,calc(100vw - 20px));overflow:auto;overscroll-behavior:contain;margin-top:8px;padding:18px;background:#091e30;border:1px solid #5b6f7d;border-radius:13px;box-shadow:0 18px 52px #000b}',
 '#argusClarityGlossary h3{font:normal 23px Georgia,serif;color:#f5deb3;margin:0 0 8px}',
 '#argusClarityGlossary h4{font-size:13px;color:#e7cca0;letter-spacing:.02em;margin:18px 0 7px}',
 '#argusClarityGlossary p{font-size:12px;line-height:1.65;color:#cfdee8;margin:6px 0}',
 '#argusClarityGlossary dl{margin:0}',
 '#argusClarityGlossary dt{font-size:12px;font-weight:650;color:#f0debf;margin-top:12px}',
 '#argusClarityGlossary dd{font-size:12px;line-height:1.6;color:#c5d5e0;margin:4px 0 0}',
 '#argusClarityGlossary .argusClarityFoot{border-top:1px solid #344d60;padding-top:12px;margin-top:17px;color:#a9bfcc}',
 '@media(max-width:500px){#argusClarityGlossary{right:10px;bottom:10px}#argusClarityGlossary>summary{padding:9px 12px;font-size:12px}#argusClarityGlossary .argusClarityBody{padding:14px}}'
].join('\n');
const style=document.createElement('style');style.id='argusClarityStyles';style.textContent=styles;document.head.appendChild(style);
const box=document.createElement('details');
box.id='argusClarityGlossary';
box.setAttribute('aria-label','Begriffe auf ARGUS verständlich erklärt');
box.innerHTML='<summary>ⓘ Begriffe verstehen</summary><div class="argusClarityBody" role="region" aria-label="ARGUS-Begriffslexikon">'+
 '<h3>Was bedeutet das?</h3><p>Eine kurze Lesehilfe für die gesamte ARGUS-Seite. Sie erklärt Begriffe, verändert aber keine Scannerwerte.</p>'+
 entries.map(([title,items])=>'<section><h4>'+esc(title)+'</h4><dl>'+
 items.map(([word,desc])=>'<dt>'+esc(word)+'</dt><dd>'+esc(desc)+'</dd>').join('')+
 '</dl></section>').join('')+
 '<p class="argusClarityFoot">Die einzelnen Originalquellen und Forschungsgrenzen bleiben maßgeblich. ARGUS ist eine Darstellung, keine Handelsplattform.</p></div>';
document.body.appendChild(box);
document.addEventListener('keydown',event=>{if(event.key==='Escape'&&box.open){box.open=false;box.querySelector('summary')?.focus()}});
})();
