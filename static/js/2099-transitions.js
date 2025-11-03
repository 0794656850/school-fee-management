// 2099 Neo Page Transitions
(function(){
  const DIR_KEY = 'neo_dir'; // 'forward' | 'back'
  const html = `
    <div class="neo-overlay" id="neoOverlay" aria-hidden="true">
      <div class="neo-stage">
        <div class="neo-pane"></div>
        <div class="neo-grid"></div>
        <div class="neo-glow"></div>
        <div class="neo-scan"></div>
      </div>
    </div>`;

  function ensureOverlay(){
    if (document.getElementById('neoOverlay')) return document.getElementById('neoOverlay');
    const tpl = document.createElement('div');
    tpl.innerHTML = html;
    const el = tpl.firstElementChild;
    document.body.appendChild(el);
    return el;
  }

  function applyDirClass(overlay){
    try {
      const dir = sessionStorage.getItem(DIR_KEY) || 'forward';
      overlay.classList.toggle('dir-back', dir === 'back');
      overlay.classList.toggle('dir-forward', dir !== 'back');
    } catch(_) {}
  }

  function showEnter(delayMs){
    const overlay = ensureOverlay();
    applyDirClass(overlay);
    overlay.classList.remove('is-revealing');
    overlay.classList.add('is-visible','is-entering');
    document.body.classList.add('neo-transitioning');
    return new Promise(resolve => setTimeout(resolve, delayMs || 360));
  }

  function showReveal(){
    const overlay = ensureOverlay();
    applyDirClass(overlay);
    overlay.classList.remove('is-entering');
    overlay.classList.add('is-visible','is-revealing');
    setTimeout(hideOverlay, 680);
  }

  function hideOverlay(){
    const overlay = document.getElementById('neoOverlay');
    if (!overlay) return;
    overlay.classList.remove('is-entering','is-revealing');
    overlay.classList.remove('is-visible');
    document.body.classList.remove('neo-transitioning');
  }

  // Intercept navigations for cover-in → navigate
  function shouldEnhanceLink(a){
    if (!a) return false;
    const href = a.getAttribute('href')||'';
    const target = a.getAttribute('target');
    if (a.hasAttribute('download')) return false;
    if (a.getAttribute('rel') === 'external') return false;
    if (a.dataset.noTransition === 'true') return false;
    if (!href || href.startsWith('#') || href.startsWith('mailto:') || href.startsWith('tel:')) return false;
    try {
      const url = new URL(href, window.location.href);
      if (url.origin !== window.location.origin) return false;
    } catch(_) { return false; }
    return true;
  }

  function setDirection(dir){
    try { sessionStorage.setItem(DIR_KEY, dir); } catch(_) {}
  }

  function onClick(e){
    const a = e.target.closest ? e.target.closest('a') : null;
    if (!a || !shouldEnhanceLink(a)) return;
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
    e.preventDefault();
    const href = a.href;
    setDirection('forward');
    showEnter(380).then(() => { window.location.href = href; });
  }

  // Back helper for explicit back buttons
  window.neoBack = function(){
    setDirection('back');
    showEnter(300).then(() => { window.history.back(); });
  };

  // Page load: reveal (slide away) the cover
  function onLoad(){
    // If page restored from bfcache, avoid double animations
    if (performance && performance.getEntriesByType) {
      try {
        const nav = performance.getEntriesByType('navigation')[0];
        if (nav && nav.type === 'back_forward') {
          // Subtle, faster reveal for bfcache
          ensureOverlay();
          showReveal();
          return;
        }
      } catch(_){}
    }
    ensureOverlay();
    showReveal();
  }

  // Before unload: try to cover for non-click navigations
  function onBeforeUnload(){
    const overlay = ensureOverlay();
    overlay.classList.add('is-visible','is-entering');
  }

  // Install
  document.addEventListener('click', onClick, { capture: true });
  document.addEventListener('submit', function(){ try{ showEnter(220); }catch(_){} }, { capture: true });
  window.addEventListener('pageshow', onLoad);
  window.addEventListener('beforeunload', onBeforeUnload);

})();
