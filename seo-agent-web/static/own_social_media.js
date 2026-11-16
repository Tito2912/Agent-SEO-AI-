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
  const monitor = item.querySelector('[data-media-monitor]');
  const label = item.querySelector('[data-media-monitor-label]');
  const checked = item.querySelector('[data-media-checked]');
  const retry = item.querySelector('[data-media-monitor-retry]');
  const title = item.querySelector('#media-progress-title');
  if (!monitor || !label || !checked || !retry) return;
  const phaseTitle = title ? title.textContent : '';
  const states = new Set(['proposal','queued','running','review','approved','stopped','uncertain','cancelled']);
  const phases = new Set(['proposal','queued','starting','voice','montage','montage_queued','review','approved','unconfirmed','expired','stopped','uncertain','cancelled']);
  let errors = 0;
  monitor.hidden = false;
  label.textContent = monitor.dataset.checking;
  function tracking(status, message) {
    item.dataset.monitorStatus = status;
    if (title) title.textContent = status === 'failed' ? monitor.dataset.failedTitle :
      status === 'access' ? monitor.dataset.accessTitle : phaseTitle;
    if (label.textContent !== message) label.textContent = message;
    retry.hidden = !['failed','access'].includes(status);
    checked.hidden = status !== 'ok';
  }
  async function check() {
    if (document.hidden) { setTimeout(check, 12000); return; }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch(url, {cache:'no-store', redirect:'error', credentials:'same-origin', signal:controller.signal});
      if ([401,403,404].includes(response.status)) {
        tracking('access', monitor.dataset.access); return;
      }
      if (!response.ok) throw new Error('unavailable');
      const state = await response.json();
      if (!state || !states.has(state.state) || !phases.has(state.phase) ||
          (state.error !== null && typeof state.error !== 'string')) throw new Error('invalid-status');
      if (state.state !== item.dataset.state || (state.error || '') !== item.dataset.error ||
          state.phase !== item.dataset.mediaPhase) {
        window.location.reload(); return;
      }
      errors = 0;
      const at = new Date();
      checked.dateTime = at.toISOString();
      try {
        checked.textContent = new Intl.DateTimeFormat(document.documentElement.lang || 'fr', {
          hour:'2-digit', minute:'2-digit', second:'2-digit'
        }).format(at);
      } catch (_) { checked.textContent = at.toISOString().slice(11,19) + ' UTC'; }
      tracking('ok', monitor.dataset.ok);
    } catch (_) {
      if (++errors >= 3) { tracking('failed', monitor.dataset.failed); return; }
      tracking('retrying', monitor.dataset.retrying);
    } finally { clearTimeout(timeout); }
    setTimeout(check, 12000);
  }
  setTimeout(check, 4000);
})();
