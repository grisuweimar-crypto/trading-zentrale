/* ARGUS PANOPTES – dark-only UI shell.
 * Approved branding remains untouched. No color-mode switch or theme persistence.
 * Does not read or modify scanner data, decisions, research or QM.
 */
(()=>{'use strict';
document.documentElement.removeAttribute('data-argus-theme');
document.documentElement.removeAttribute('data-argus-theme-preference');
document.documentElement.style.colorScheme='dark';
const nav=document.querySelector('nav.nav');
if(nav){
 const full={observations:'Beobachtungen',science:'Forschungsstand',about:'Über ARGUS'};
 const compact={observations:'Beobachten',science:'Forschung',about:'Über'};
 const updateLabels=()=>{
  const mobile=window.matchMedia?.('(max-width:600px)').matches;
  for(const view of Object.keys(full)){
   const b=nav.querySelector('button[data-view="'+view+'"]');
   if(b){b.textContent=mobile?compact[view]:full[view];b.setAttribute('aria-label',full[view]);b.title=full[view]}
  }
 };
 updateLabels();
 window.addEventListener('resize',updateLabels,{passive:true});
 const reveal=()=>{
  const active=nav.querySelector('button.active');
  if(active&&nav.scrollWidth>nav.clientWidth+4)active.scrollIntoView({behavior:'auto',block:'nearest',inline:'center'});
 };
 nav.addEventListener('click',e=>{if(e.target.closest?.('button[data-view]'))requestAnimationFrame(reveal)});
 window.addEventListener('resize',()=>requestAnimationFrame(reveal),{passive:true});
 requestAnimationFrame(reveal);
}
const reload=document.getElementById('reload');
if(reload){
 reload.setAttribute('aria-label','Scanner-Daten aktualisieren');
 reload.innerHTML='<span aria-hidden="true">↻</span><span class="argusRefreshText">Aktualisieren</span>';
}
})();
