(() => {
  'use strict';
  const item = document.querySelector('[data-media-watch]');
  if (!item) return;
  const url = new URL(item.dataset.mediaWatch, window.location.origin);
  if (url.origin !== window.location.origin) return;
  let errors = 0;
  async function check() {
    try {
      const response = await fetch(url, {cache: 'no-store', redirect: 'error', credentials: 'same-origin'});
      if ([401,403,404].includes(response.status)) return;
      if (!response.ok) throw new Error('unavailable');
      const state = await response.json();
      if (state.state !== item.dataset.state || (state.error || '') !== item.dataset.error) {
        window.location.reload(); return;
      }
      errors = 0;
    } catch (_) { if (++errors >= 3) return; }
    setTimeout(check, 12000);
  }
  setTimeout(check, 4000);
})();
