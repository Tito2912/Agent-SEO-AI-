/* Read-only status checks. Never submit a form, grant consent or repeat a job. */
(() => {
  const root = document.querySelector('[data-journey-version]');
  if (!root || root.dataset.journeyWatch !== 'yes') return;
  let edited = false;
  let attempts = 0;
  root.addEventListener('input', () => { edited = true; });
  const check = async () => {
    if (document.visibilityState === 'hidden') return schedule();
    try {
      const response = await fetch(window.location.href, {
        credentials: 'same-origin', cache: 'no-store', redirect: 'error',
        headers: { Accept: 'text/html' }
      });
      if (!response.ok) return; // Includes logout and unavailable state.
      const next = new DOMParser().parseFromString(await response.text(), 'text/html')
        .querySelector('[data-journey-version]');
      if (!next) return;
      if (next.dataset.journeyVersion !== root.dataset.journeyVersion) {
        const notice = root.querySelector('.own-update');
        if (edited || root.querySelector('details[open]') || root.contains(document.activeElement)) {
          if (notice) notice.hidden = false;
        } else {
          window.location.reload();
        }
        return;
      }
    } catch (_) { /* A later read may recover; no write or automatic login. */ }
    schedule();
  };
  const schedule = () => {
    if (++attempts <= 90) window.setTimeout(check, 10000);
  };
  schedule();
})();
