document.addEventListener('DOMContentLoaded', () => {
  function revealAnchor() {
    const target = document.getElementById(window.location.hash.slice(1));
    if (target?.matches('details')) target.open = true;
  }
  revealAnchor();
  window.addEventListener('hashchange', revealAnchor);
  document.querySelectorAll('.cms-page form').forEach(form => {
    form.addEventListener('submit', event => {
      if (event.defaultPrevented) return;
      const button = event.submitter || form.querySelector('button[type="submit"]');
      if (!button || !button.dataset.busyLabel) return;
      button.disabled = true;
      button.textContent = button.dataset.busyLabel;
      form.setAttribute('aria-busy', 'true');
    });
  });
  const page = document.querySelector('[data-workflow-status-url]');
  if (!page) return;
  const message = page.querySelector('[data-workflow-poll-message]');
  async function poll() {
    if (document.visibilityState === 'hidden') {
      window.setTimeout(poll, 4000);
      return;
    }
    try {
      const response = await fetch(page.dataset.workflowStatusUrl, {
        headers: {Accept: 'application/json'}, cache: 'no-store',
        signal: AbortSignal.timeout(10000),
      });
      if (!response.ok || !response.headers.get('content-type')?.includes('application/json')) throw new Error('unavailable');
      const state = await response.json();
      if (state.state !== page.dataset.workflowState || !state.poll) {
        window.location.reload();
        return;
      }
      if (message) message.textContent = 'Suivi actualisé automatiquement.';
    } catch (_) {
      if (message) message.textContent = 'Le suivi est momentanément indisponible. Nouvelle tentative dans quelques secondes, ou utilisez « Actualiser le suivi ».';
    }
    window.setTimeout(poll, 4000);
  }
  window.setTimeout(poll, 2500);
});
