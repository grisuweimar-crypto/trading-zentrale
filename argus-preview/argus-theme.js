/* ARGUS PANOPTES — visitor appearance preference, presentation only.
 * Dark is the default; light and OS-following are optional. No tracking.
 */
(()=>{'use strict';
const KEY='argus_theme_preference_v1';
const root=document.documentElement;
const allowed=new Set(['dark','light','system']);
const os=typeof matchMedia==='function'?matchMedia('(prefers-color-scheme: light)'):null;
let choice='dark';
try{const saved=localStorage.getItem(KEY);if(allowed.has(saved))choice=saved;}catch(_){}
function resolved(){return choice==='system'?(os?.matches?'light':'dark'):choice}
function apply(){
 const current=resolved();
 root.setAttribute('data-argus-theme',current);
 root.style.colorScheme=current;
 const select=document.getElementById('argusThemeChoice');
 if(select){select.value=choice;select.setAttribute('aria-label','Darstellung: Dunkel, Hell oder System');}
 const meta=document.querySelector('meta[name="theme-color"]');
 if(meta)meta.content=current==='light'?'#f6f7f5':'#061321';
}
apply();
function mount(){
 const select=document.getElementById('argusThemeChoice');
 if(!select)return;
 select.value=choice;
 select.addEventListener('change',()=>{
  if(!allowed.has(select.value))return;
  choice=select.value;
  try{localStorage.setItem(KEY,choice)}catch(_){}
  apply();
 });
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',mount,{once:true});else mount();
if(os?.addEventListener)os.addEventListener('change',()=>{if(choice==='system')apply()});
else if(os?.addListener)os.addListener(()=>{if(choice==='system')apply()});
})();