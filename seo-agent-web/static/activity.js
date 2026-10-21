/* Poll the existing owner-scoped API, without overlapping reads or replaying an action. */
const noyaruT = window.noyaruT || ((text) => text);
(() => {
  const root = document.querySelector('[data-activity-list], [data-activity-detail]');
  if (!root) return;
  const detail = root.hasAttribute('data-activity-detail');
  const feedback = document.getElementById('activity-live-feedback');
  const refresh = root.querySelector('[data-refresh-activity]');
  if (!refresh) return;
  const active = status => ['queued', 'running', 'cancel_requested'].includes(status);
  const labels = {queued:noyaruT('En attente'),running:noyaruT('En cours'),cancel_requested:noyaruT('Arrêt demandé'),done:noyaruT('Terminé'),failed:noyaruT('En échec'),canceled:noyaruT('Annulé')};
  const rows = () => detail ? [root] : Array.from(root.querySelectorAll('tr[data-job-id]'));
  let busy = false, timer = null, denied = false, interrupted = false;
  const form = root.querySelector('[data-activity-filters]');
  function applyFilters(writeUrl = false) {
    if (!form) return;
    const values = Object.fromEntries(new FormData(form));
    let visible = 0;
    rows().forEach(row => {
      const status = row.dataset.status;
      row.hidden = !!((values.site && values.site !== row.dataset.site) ||
        (values.kind && (values.kind === 'other' ? ['crawl','autopilot'].includes(row.dataset.kind) : values.kind !== row.dataset.kind)) ||
        (values.status && (values.status === 'active' ? !active(status) : values.status !== status)));
      if (!row.hidden) visible++;
    });
    const count = root.querySelector('[data-activity-count]');
    count.textContent = noyaruT('{n} traitement(s) correspondant(s) sur les {total} affichés. Périmètre : traitements récents et encore actifs ; cet écran n’est pas un historique exhaustif.').replace('{n}', visible).replace('{total}', rows().length);
    root.querySelector('[data-filter-empty]').hidden = visible !== 0;
    const url = new URL(location.href);
    ['site','kind','status'].forEach(key => { if (values[key]) url.searchParams.set(key, values[key]); else url.searchParams.delete(key); });
    if (writeUrl) history.replaceState(null, '', url.pathname + url.search);
    const visibleRows = rows().filter(row => !row.hidden);
    const counts = {'total': visibleRows.length, 'active': visibleRows.filter(row => active(row.dataset.status)).length,
      'done': visibleRows.filter(row => row.dataset.status === 'done').length,
      'stopped': visibleRows.filter(row => ['failed','canceled'].includes(row.dataset.status)).length};
    Object.entries(counts).forEach(([key, value]) => { const counter = root.querySelector('[data-count-' + key + ']'); if (counter) counter.textContent = value; });
    const returnTo = url.pathname + url.search;
    root.querySelectorAll('[data-job-link]').forEach(link => {
      const target = new URL(link.href, location.origin); target.searchParams.set('return_to', returnTo); link.href = target.pathname + target.search;
    });
  }
  if (form) {
    const params = new URLSearchParams(location.search);
    ['site','kind','status'].forEach(key => { if (Array.from(form.elements[key].options).some(option => option.value === params.get(key))) form.elements[key].value = params.get(key); });
    form.addEventListener('submit', event => event.preventDefault());
    form.addEventListener('change', () => applyFilters(true));
    form.addEventListener('reset', () => setTimeout(() => applyFilters(true), 0));
    root.querySelector('[data-clear-activity]').addEventListener('click', () => form.reset());
    applyFilters();
  }
  function progress(row, data) {
    const value = data.progress || {}, current = Number(value.current), total = Number(value.total);
    const bar = detail ? document.getElementById('progress-bar') : row.querySelector('[data-role="progress-bar"]');
    const fill = detail ? document.getElementById('progress-fill') : row.querySelector('[data-role="progress-fill"]');
    const text = detail ? document.getElementById('progress-count') : row.querySelector('[data-role="progress-text"]');
    const wrap = detail ? document.getElementById('job-progress') : bar;
    if (!Number.isFinite(current) || !Number.isFinite(total) || total <= 0) { wrap.hidden = true; text.textContent = noyaruT('Avancement non communiqué'); return; }
    const finalizing = active(data.status) && current >= total;
    const percent = Math.max(0, Math.min(finalizing ? 99 : 100, Math.round(current / total * 100)));
    if (!active(data.status)) {
      wrap.hidden = true;
      if (!detail) text.textContent = current + ' ' + (value.type === 'crawl' ? noyaruT('pages visitées') : value.type === 'autopilot' ? noyaruT('sites traités') : noyaruT('étapes traitées'));
      return;
    }
    const label = current + '/' + total + (value.type === 'crawl' ? ' ' + noyaruT('pages') : '') + ' · ' + (finalizing ? noyaruT('finalisation…') : percent + '%');
    if (text.textContent !== label) text.textContent = label;
    fill.style.setProperty('--p', percent); bar.setAttribute('aria-valuenow', percent); wrap.hidden = false;
  }
  function render(row, data) {
    row.dataset.status = data.status; row.dataset.live = active(data.status) ? '1' : '0';
    const pill = detail ? document.getElementById('job-status-pill') : row.querySelector('[data-role="status"]');
    const label = labels[data.status] || noyaruT('État non reconnu');
    if (pill.textContent !== label) pill.textContent = label;
    pill.className = 'pill ' + (labels[data.status] ? data.status : 'unknown');
    progress(row, data);
    if (detail) {
      const view = data.activity;
      if (view) {
        document.getElementById('activity-headline').textContent = view.headline;
        document.getElementById('activity-message').textContent = view.message;
        const result = document.getElementById('job-result-link');
        result.hidden = !view.report_ready;
        result.href = view.report_ready ? view.report_url : '#';
        const steps = root.querySelector('.correction-steps');
        if (steps) {
          steps.hidden = !active(data.status) && data.status !== 'done';
          steps.querySelectorAll('li').forEach((step, index) => {
            const n = index + 1; step.className = n < view.stage ? 'done' : n === view.stage ? 'current' : '';
            step.querySelector('span').textContent = n < view.stage ? '✓' : String(n);
            if (n === view.stage) step.setAttribute('aria-current','step'); else step.removeAttribute('aria-current');
          });
        }
        if (new URLSearchParams(location.search).get('autoredirect') === '1' && data.status === 'done') {
          const target = view.report_ready ? view.report_url : view.kind === 'autopilot' ? '/' : '';
          if (target && target.startsWith('/') && !target.startsWith('//')) { location.assign(target); return; }
        }
      }
      root.querySelector('[data-job-cancel]').hidden = !active(data.status);
      root.querySelector('[data-job-cancel] button').disabled = data.status === 'cancel_requested';
      root.querySelector('[data-job-retry]').hidden = !['failed','canceled'].includes(data.status);
      ['stdout','stderr'].forEach(key => { if (typeof data[key] === 'string') document.getElementById('job-' + key).textContent = data[key] || noyaruT('Aucune sortie enregistrée.'); });
      document.getElementById('job-returncode').hidden = data.returncode === null || data.returncode === undefined;
      document.getElementById('job-returncode-val').textContent = data.returncode ?? '';
    }
  }
  function expired() {
    denied = true;
    const link = document.createElement('a'); link.href = '/auth/login?next=' + encodeURIComponent(location.pathname + location.search); link.textContent = noyaruT('Me reconnecter');
    feedback.replaceChildren(document.createTextNode(noyaruT('Votre session a expiré.') + ' '), link);
  }
  async function pollRow(row) {
    const controller = new AbortController(); const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const response = await fetch('/api/jobs/' + encodeURIComponent(row.dataset.jobId) + '?tail=' + (detail ? '20000' : '1'), {cache:'no-store',headers:{Accept:'application/json'},signal:controller.signal});
      if (response.status === 401 || (response.redirected && new URL(response.url).pathname === '/auth/login')) { expired(); return false; }
      if (response.status === 404 || response.status === 403) {
        row.dataset.live = '0'; row.dataset.status = 'unavailable';
        if (detail) denied = true;
        else {
          row.querySelector('[data-role="status"]').textContent = noyaruT('Suivi indisponible');
          row.querySelector('[data-role="progress-bar"]').hidden = true;
          row.querySelector('[data-role="progress-text"]').textContent = noyaruT('État inaccessible');
        }
        if (detail || !denied) feedback.textContent = noyaruT('Un suivi n’est plus accessible. Retrouvez votre activité ou reconnectez-vous avec le compte concerné.');
        return false;
      }
      if (!response.ok || !(response.headers.get('content-type') || '').includes('application/json')) throw new Error('unavailable');
      render(row, await response.json()); return true;
    } catch (_) { return false; }
    finally { clearTimeout(timeout); }
  }
  async function poll(manual = false) {
    if (busy || denied) return;
    clearTimeout(timer); busy = true; refresh.disabled = true;
    let success = true;
    const targets = rows().filter(row => manual || (detail ? active(row.dataset.status) : row.dataset.live === '1'));
    for (let start = 0; start < targets.length && !denied; start += 4) {
      const results = await Promise.all(targets.slice(start, start + 4).map(pollRow));
      if (results.some(value => !value)) success = false;
    }
    if (!denied) {
      if (!success) { interrupted = true; feedback.textContent = noyaruT('La mise à jour est interrompue. L’état affiché reste le dernier reçu ; actualisez avant de demander une autre action.'); }
      else if (manual || interrupted) { feedback.textContent = noyaruT('Suivi actualisé.'); interrupted = false; }
    }
    applyFilters();
    if (detail) {
      root.querySelector('[data-job-retry] button').disabled = denied || !success;
      root.querySelector('[data-job-cancel] button').disabled = denied || !success || root.dataset.status === 'cancel_requested';
    }
    busy = false; refresh.disabled = denied;
    if (!denied && rows().some(row => active(row.dataset.status))) timer = setTimeout(() => poll(), success ? 3000 : 6000);
  }
  root.querySelectorAll('[data-job-cancel], [data-job-retry]').forEach(form => form.addEventListener('submit', event => {
    if (form.dataset.submitting) { event.preventDefault(); return; }
    form.dataset.submitting = '1';
    const button = form.querySelector('button'); button.disabled = true; button.textContent = noyaruT('Envoi de la demande…');
  }));
  root.querySelector('[data-reload-activity]')?.addEventListener('click', () => location.reload());
  refresh.addEventListener('click', () => poll(true));
  if (rows().some(row => active(row.dataset.status))) poll();
})();
