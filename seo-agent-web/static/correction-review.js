/* One preparation/review boundary for GitHub corrections. No automatic confirmation. */
(() => {
  'use strict';
  const dialog = document.getElementById('correction-review');
  const context = document.getElementById('correction-review-context');
  if (!dialog || !context) return;
  const cfg = JSON.parse(context.textContent);
  const el = name => dialog.querySelector('[data-review-' + name + ']');
  let selection, proposal, busy = false, opener, poll;

  async function receipts(target) {
    const response = await fetch('/api/projects/' + encodeURIComponent(target.slug) + '/tasks', {cache: 'no-store'});
    if (!response.ok || response.redirected) throw new Error('Suivi indisponible');
    const tasks = await response.json();
    if (!Array.isArray(tasks)) throw new Error('Suivi indisponible');
    return tasks.flatMap(task => {
      try {
        const note = JSON.parse(task.note || '{}');
        return note.pr_url && (target.kind === 'bulk' ||
          (task.issue_key === target.issue && (!target.url || task.url === target.url))) ? [note] : [];
      } catch (_) { return []; }
    });
  }
  function stopPolling() { clearInterval(poll); poll = undefined; }
  async function baseline(target) {
    try { return new Set((await receipts(target)).map(note => note.pr_url)); }
    catch (_) { return null; }
  }
  function recover(target, before) {
    if (!before) return;
    stopPolling(); let tries = 0;
    poll = setInterval(async () => {
      if (++tries > 30 || selection !== target) { stopPolling(); return; }
      try {
        const found = (await receipts(target)).find(note => !before.has(note.pr_url));
        if (found && selection === target) { stopPolling(); result(found); }
      } catch (_) { /* Keep the read-only recovery bounded; the follow-up link stays available. */ }
    }, 5000);
  }

  function message(text, state = '') {
    el('message').textContent = text;
    el('message').dataset.state = state;
  }
  function step(stage) {
    dialog.querySelectorAll('.correction-steps li').forEach((item, index) => {
      item.className = index + 1 < stage ? 'done' : index + 1 === stage ? 'current' : '';
      item.querySelector('span').textContent = index + 1 < stage ? '✓' : String(index + 1);
      if (index + 1 === stage) item.setAttribute('aria-current', 'step');
      else item.removeAttribute('aria-current');
    });
  }
  function setBusy(value) {
    busy = value;
    el('prepare').disabled = value;
    el('confirm').disabled = value || !el('consent').checked;
  }
  function recovery(href, label) {
    el('recovery').href = href;
    el('recovery').textContent = label;
    el('recovery').hidden = false;
  }
  function prLink(url) {
    try {
      const parsed = new URL(url);
      if (parsed.protocol === 'https:' && parsed.hostname === 'github.com') {
        el('result').href = parsed.href;
        el('result').hidden = false;
      }
    } catch (_) { /* A malformed link never becomes navigation. */ }
  }
  async function request(body, route) {
    let response;
    try {
      response = await fetch(route, {
        method: 'POST', headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
        body: JSON.stringify(body)
      });
    } catch (_) { throw new Error('La réponse est indisponible. Le traitement peut continuer ; retrouvez le suivi avant de réessayer.'); }
    if (response.status === 401 || (response.redirected && new URL(response.url).pathname.startsWith('/auth/'))) {
      recovery('/auth/login?next=' + encodeURIComponent(location.pathname + location.search), 'Me reconnecter');
      throw new Error('Votre session a expiré. Reconnectez-vous pour reprendre cette correction.');
    }
    let data;
    try { data = await response.json(); }
    catch (_) { throw new Error('La réponse du serveur n’a pas pu être lue. Retrouvez le suivi avant de réessayer.'); }
    if (!response.ok || !data.ok) {
      if (data.pr_url) prLink(data.pr_url);
      if (data.billing_url) recovery('/billing', 'Voir mon abonnement');
      if (data.needs_setup) recovery('/projects/' + encodeURIComponent(selection.slug) + '/settings/crawl#github-section', 'Vérifier la connexion GitHub');
      const error = new Error(data.error || 'La demande n’a pas abouti. Retrouvez le suivi pour poursuivre.');
      error.known = true;
      throw error;
    }
    return data;
  }
  function result(data) {
    step(3);
    document.getElementById('review-title').textContent = 'Proposition GitHub créée';
    el('preview').hidden = true;
    el('prepare').hidden = true;
    el('confirm').hidden = true;
    el('cancel').textContent = 'Fermer';
    prLink(data.pr_url);
    el('result').textContent = data.verification ? 'Voir le brouillon GitHub' : 'Relire sur GitHub';
    let text = data.verification
      ? 'Votre proposition est créée en brouillon. Les vérifications du dépôt sont en cours avant sa relecture et sa fusion.'
      : 'Votre proposition GitHub est créée. Relisez-la avant de la fusionner.';
    text += ' Le résultat SEO reste à vérifier après publication sur votre site.';
    if (data.partial) text += ' Certaines corrections seulement ont été préparées.';
    if (data.not_attempted_count) text += ' ' + data.not_attempted_count + ' fichier(s) n’ont pas été traités.';
    message(text, 'success');
  }
  function open(button) {
    if (dialog.open || busy) return;
    stopPolling();
    opener = button;
    const single = button.matches('[data-single-fix], [data-correction-single-fix]') ||
      (button.matches('.gh-fix-btn') && button.dataset.fixRoute !== 'deep-fix');
    const bulk = button.matches('[data-bulk-fix], [data-correction-bulk-fix]');
    selection = {
      slug: button.dataset.slug || cfg.slug, kind: bulk ? 'bulk' : single ? 'single' : 'deep',
      issue: button.dataset.issue || '', url: button.dataset.url || '', crawl_ts: button.dataset.crawl || cfg.crawl || ''
    };
    proposal = null;
    for (const name of ['preview', 'confirm', 'result', 'recovery', 'followup']) el(name).hidden = true;
    el('consent').checked = false;
    el('prepare').hidden = false;
    el('cancel').textContent = 'Annuler';
    el('followup').href = '/projects/' + encodeURIComponent(selection.slug) + '/corrections';
    el('followup').hidden = false;
    el('target').textContent = bulk ? 'Les problèmes traitables du dernier bilan de ce site' : selection.url || 'Les pages concernées par ce problème';
    el('destination').textContent = cfg.repo ? 'Dépôt : ' + cfg.repo + ' · branche de référence : ' + cfg.branch : 'La connexion GitHub sera vérifiée avant la préparation.';
    document.getElementById('review-title').textContent = single ? 'Préparer une correction' : 'Préparer une proposition GitHub';
    el('prepare').textContent = single ? 'Préparer l’aperçu' : 'Préparer le brouillon GitHub';
    document.getElementById('review-intro').textContent = single
      ? 'Noyaru prépare un texte que vous pourrez relire ici avant de créer une proposition sur GitHub. Cette préparation compte pour une correction IA de votre forfait.'
      : 'Noyaru préparera un brouillon contenant les fichiers traités, dans les limites de votre forfait. Les fichiers rédigés par l’IA sont décomptés de votre quota. Relisez les changements sur GitHub avant de les fusionner.';
    message('La proposition devra être relue et fusionnée par une personne, y compris si le projet autorise habituellement la fusion automatique.');
    step(1); setBusy(false); dialog.showModal();
    el('prepare').focus();
  }
  function close() { dialog.close(); }
  dialog.addEventListener('close', () => { if (opener?.isConnected) opener.focus(); });
  dialog.addEventListener('cancel', event => {
    if (busy) { event.preventDefault(); message('Le traitement est en cours. Retrouvez le suivi si vous quittez cet écran.'); }
  });
  el('close').addEventListener('click', close);
  el('cancel').addEventListener('click', close);
  el('consent').addEventListener('change', () => { el('confirm').disabled = busy || !el('consent').checked; });

  el('prepare').addEventListener('click', async () => {
    if (busy) return;
    setBusy(true); el('recovery').hidden = true; el('result').hidden = true;
    message('Préparation en cours. Vous pourrez retrouver la proposition dans le suivi des corrections.');
    const base = '/api/projects/' + encodeURIComponent(selection.slug);
    const route = selection.kind === 'bulk' ? base + '/github/bulk-fix' : base + '/issues/' + encodeURIComponent(selection.issue) + '/' + (selection.kind === 'single' ? 'github-fix' : 'deep-fix');
    const target = selection;
    const before = target.kind === 'single' ? null : await baseline(target);
    try {
      const body = selection.kind === 'bulk' ? {review_only: true} : {url: selection.url, crawl_ts: selection.crawl_ts};
      if (selection.kind === 'single') body.confirm = false;
      else body.review_only = true;
      const data = await request(body, route);
      if (data.pr_url) result(data);
      else {
        if (!data.file || !data.patched_content || data.diff === '') throw new Error('Cet aperçu ne contient pas de changement complet à relire. Aucune proposition n’a été créée.');
        proposal = data;
        document.getElementById('review-title').textContent = 'Relire la proposition';
        document.getElementById('review-intro').textContent = 'La création de cette proposition ne consomme pas de nouvelle correction IA. Vous devrez ensuite la relire et la fusionner sur GitHub.';
        el('file').textContent = data.file;
        el('description').textContent = data.description || '';
        el('diff').textContent = data.diff || data.patched_content;
        el('preview-title').textContent = data.diff ? 'Changements proposés' : 'Version proposée complète · ancien aperçu';
        el('diff-help').textContent = data.diff
          ? 'Les lignes « − » seront retirées, les lignes « + » seront ajoutées. Les changements au-delà des premières lignes du fichier sont inclus.'
          : 'Cet ancien aperçu ne contient pas de comparaison complète. Relisez la version proposée, puis comparez les changements sur GitHub avant toute fusion. Sa reprise ne relance pas la rédaction IA.';
        el('preview').hidden = false; el('prepare').hidden = true; el('confirm').hidden = false;
        step(2); message('Relisez les différences, puis confirmez la création de la proposition. Votre site n’a pas encore été modifié.');
        el('diff').focus();
      }
    } catch (error) {
      message(error.message || 'La réponse est indisponible. Retrouvez le suivi avant de réessayer.', 'error');
      // A dropped write response must not become an automatic second write.
      if (selection.kind !== 'single') el('prepare').hidden = true;
      if (!error.known && target.kind !== 'single') recover(target, before);
    } finally { setBusy(false); }
  });
  el('confirm').addEventListener('click', async () => {
    if (busy || !proposal || !el('consent').checked) return;
    setBusy(true); message('Création de la proposition sur GitHub…');
    const target = selection;
    const before = await baseline(target);
    try {
      result(await request({url: selection.url, crawl_ts: selection.crawl_ts, confirm: true,
        file_path: proposal.file, patched_content: proposal.patched_content},
      '/api/projects/' + encodeURIComponent(selection.slug) + '/issues/' + encodeURIComponent(selection.issue) + '/github-fix'));
    } catch (error) {
      el('confirm').hidden = true;
      message(error.message + ' Consultez le suivi avant de lancer une nouvelle préparation.', 'error');
      if (!error.known) recover(target, before);
    } finally { setBusy(false); }
  });
  document.addEventListener('click', event => {
    const button = event.target.closest('[data-single-fix], [data-bulk-fix], [data-correction-single-fix], [data-correction-bulk-fix], [data-correction-deep-fix], .gh-fix-btn');
    if (!button) return;
    event.preventDefault(); open(button);
  });
})();
