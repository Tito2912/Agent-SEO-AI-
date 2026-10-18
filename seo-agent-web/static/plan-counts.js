(() => {
  const root = document.querySelector('[data-plan-counts]');
  if (!root) return;
  const cards = new Map([...root.querySelectorAll('[data-plan-key]')].map(card => [card.dataset.planKey, card]));
  const button = root.querySelector('[data-plan-refresh]');
  const error = root.querySelector('[data-plan-error]');
  const clock = root.querySelector('[data-plan-time]');
  const total = root.querySelector('[data-plan-total]');
  const number = new Intl.NumberFormat('fr-FR');
  const date = new Intl.DateTimeFormat('fr-FR', { dateStyle: 'short', timeStyle: 'medium', timeZone: 'Europe/Paris' });
  let timer = null;
  let controller = null;
  let busy = false;
  let paused = false;
  let denied = false;
  let hasReading = Boolean(clock.getAttribute('datetime'));

  function setText(element, value) {
    if (element.textContent !== value) element.textContent = value;
  }

  function validate(data) {
    if (data.ok !== true || !Array.isArray(data.plans) || data.plans.length !== cards.size ||
        !Number.isSafeInteger(data.total) || data.total < 0 || typeof data.generated_at !== 'string') throw new Error('invalid');
    const seen = new Set();
    let sum = 0;
    for (const plan of data.plans) {
      if (!cards.has(plan.key) || seen.has(plan.key) || typeof plan.label !== 'string' ||
          !['count', 'active', 'trialing'].every(key => Number.isSafeInteger(plan[key]) && plan[key] >= 0) ||
          plan.active + plan.trialing > plan.count) throw new Error('invalid');
      seen.add(plan.key);
      sum += plan.count;
    }
    if (sum !== data.total || Number.isNaN(Date.parse(data.generated_at))) throw new Error('invalid');
    return data;
  }

  async function refresh() {
    clearTimeout(timer);
    if (busy || document.hidden || paused) return;
    busy = true;
    button.disabled = true;
    controller = new AbortController();
    const timeout = setTimeout(() => controller?.abort(), 8000);
    try {
      const response = await fetch(root.dataset.endpoint, {
        credentials: 'same-origin', cache: 'no-store', redirect: 'error',
        headers: { Accept: 'application/json' }, signal: controller.signal,
      });
      if (!response.ok) {
        denied = response.status === 401 || response.status === 403;
        throw new Error('unavailable');
      }
      const data = validate(await response.json());
      // A late response must not replace the last reading after leaving this tab.
      if (paused || document.hidden) return;
      denied = false;
      for (const plan of data.plans) {
        const card = cards.get(plan.key);
        setText(card.querySelector('[data-plan-label]'), plan.label);
        const count = card.querySelector('[data-plan-count]');
        count.setAttribute('aria-label', `${plan.label} : ${number.format(plan.count)} comptes`);
        setText(count, number.format(plan.count));
        setText(card.querySelector('[data-plan-detail]'), plan.key === 'free' ? 'Accès gratuit' :
          `${number.format(plan.active)} actif${plan.active === 1 ? '' : 's'} · ${number.format(plan.trialing)} essai${plan.trialing === 1 ? '' : 's'}`);
      }
      setText(total, number.format(data.total));
      clock.dateTime = data.generated_at;
      hasReading = true;
      setText(clock, date.format(new Date(data.generated_at)));
      error.hidden = true;
      setText(error, '');
    } catch (_) {
      if (!paused && !document.hidden) {
        error.hidden = false;
        setText(error, denied ? 'Accès refusé. Reconnectez-vous pour actualiser les plans.' : hasReading ?
          'Actualisation indisponible. Les dernières valeurs reçues restent affichées ; nouvel essai automatique.' :
          'Les données des plans sont indisponibles. Nouvel essai automatique.');
      }
    } finally {
      clearTimeout(timeout);
      controller = null;
      busy = false;
      button.disabled = false;
      if (!paused && !document.hidden && !denied) timer = setTimeout(refresh, 10000);
    }
  }

  root.querySelector('[data-plan-refresh-form]').addEventListener('submit', event => {
    event.preventDefault();
    refresh();
  });
  document.addEventListener('visibilitychange', () => {
    clearTimeout(timer);
    if (document.hidden) controller?.abort();
    else refresh();
  });
  window.addEventListener('pagehide', () => {
    paused = true;
    clearTimeout(timer);
    controller?.abort();
  });
  window.addEventListener('pageshow', () => {
    paused = false;
    refresh();
  });
  refresh();
})();
