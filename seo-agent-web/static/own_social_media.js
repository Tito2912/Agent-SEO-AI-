(() => {
  'use strict';
  // UTC remains the usable fallback without JavaScript or Intl support.
  document.querySelectorAll('[data-media-local-time]').forEach(time => {
    try {
      const date = new Date(time.dateTime);
      if (!Number.isFinite(date.getTime())) return;
      time.textContent = new Intl.DateTimeFormat(document.documentElement.lang || 'fr', {
        year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
        timeZoneName: 'short'
      }).format(date);
    } catch (_) { /* Keep the explicit server-rendered UTC date. */ }
  });
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
