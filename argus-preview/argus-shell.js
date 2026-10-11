/* ARGUS PANOPTES – UI-only theme, responsive navigation and accessibility.
 * Does not read/change scanner decisions, research data or quality states. */
(()=>{
'use strict';
const KEY='argus_ui_theme_preference_v1';
const allowed=new Set(['dark','light','system']);
const media=window.matchMedia?.('(prefers-color-scheme: dark)');
let stored='dark';
try{const raw=localStorage.getItem(KEY);if(allowed.has(raw))stored=raw}catch(_){}
const choices=[['dark','☾ Dunkel'],['light','☀ Hell'],['system','◐ System']];
const nativeTheme=document.createElement('div');
nativeTheme.className='argusThemeControl';
const lbl=document.createElement('label');
lbl.className='argusThemeLabel';lbl.htmlFor='argusThemeChoice';lbl.textContent='Darstellung';
const sel=document.createElement('select');
sel.id='argusThemeChoice';sel.setAttribute('aria-label','Farbdarstellung wählen');
for(const [value,label] of choices){const op=document.createElement('option');op.value=value;op.textContent=label;sel.append(op)}
nativeTheme.append(lbl,sel);
const target=document.querySelector('.nav-right')||document.querySelector('.navigation')||document.querySelector('.topbar');
if(target){
  const refresh=document.getElementById('reload');
  if(refresh)target.insertBefore(nativeTheme,refresh);
  else target.appendChild(nativeTheme);
}
function effective(pref){return pref==='system'?(media?.matches?'dark':'light'):pref}
function apply(pref){
  const theme=effective(pref);
  document.documentElement.setAttribute('data-argus-theme',theme);
  document.documentElement.setAttribute('data-argus-theme-preference',pref);
  document.documentElement.style.colorScheme=theme;
  const m=document.querySelector('meta[name="color-scheme"]');
  if(m)m.setAttribute('content',theme);
  sel.value=pref;
}
sel.addEventListener('change',()=>{
 const chosen=allowed.has(sel.value)?sel.value:'dark';
 try{localStorage.setItem(KEY,chosen)}catch(_){}
 apply(chosen);
});
if(media){
 const change=()=>{if(sel.value==='system')apply('system')};
 if(media.addEventListener)media.addEventListener('change',change);
 else if(media.addListener)media.addListener(change);
}
apply(stored);

// The approved Owl Eye appears as original embedded artwork in About's static
// title block. No generated or dynamically redrawn substitute is permitted.
const approved=document.querySelector('#about .argusApprovedAbout img.argusApprovedEye');
if(!approved)console.warn('ARGUS: original approved About emblem missing');
const footerCount=document.querySelectorAll('body > footer.footer').length;
if(footerCount!==1)console.warn('ARGUS: expected one global footer, got '+footerCount);

// Keep active navigation entries fully visible in the horizontal mobile menu.
const nav=document.querySelector('nav.nav');
if(nav){
 const labels={observations:'Beobachtungen',science:'Forschungsstand',about:'Über ARGUS'};
 const smallLabels={observations:'Beobachten',science:'Forschung',about:'Über'};
 function responsiveNames(){
  const compact=window.matchMedia?.('(max-width:600px)').matches;
  for(const [view,full] of Object.entries(labels)){
   const tab=nav.querySelector('button[data-view="'+view+'"]');
   if(tab){tab.textContent=compact?smallLabels[view]:full;tab.setAttribute('aria-label',full);tab.title=full;}
  }
 }
 responsiveNames();
 window.addEventListener('resize',responsiveNames,{passive:true});
 const focusCurrent=()=>{
  const item=nav.querySelector('button.active');
  if(item&&nav.scrollWidth>nav.clientWidth+4)
   item.scrollIntoView({behavior:'smooth',block:'nearest',inline:'center'});
 };
 nav.addEventListener('click',e=>{
  const button=e.target.closest?.('button[data-view]');
  if(button)requestAnimationFrame(focusCurrent);
 });
 window.addEventListener('resize',()=>requestAnimationFrame(focusCurrent),{passive:true});
 requestAnimationFrame(focusCurrent);
}
const refresh=document.getElementById('reload');
if(refresh){
 refresh.setAttribute('aria-label','Scanner-Daten aktualisieren');
 refresh.innerHTML='<span aria-hidden="true">↻</span><span class="argusRefreshText">Aktualisieren</span>';
}
// Defensive DOM assertion for the removed duplicate footer: no injected footer.
})();