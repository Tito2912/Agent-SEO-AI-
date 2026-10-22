/* Project repository settings: show only confirmed state; every mutation is explicit. */
const noyaruT = window.noyaruT || ((text) => text);
(() => {
  const root = document.getElementById('github-repo');
  if (!root) return;
  const api = '/api/projects/' + encodeURIComponent(root.dataset.projectSlug) + '/github/';
  const repo = document.getElementById('gh-repo-input');
  const branch = document.getElementById('gh-branch-input');
  const save = document.getElementById('gh-connect-btn');
  const detach = document.getElementById('gh-disconnect-btn');
  const pick = document.getElementById('gh-pick-repo-btn');
  const list = document.getElementById('gh-repo-list');
  const status = document.getElementById('gh-repo-status');
  const feedback = document.getElementById('gh-connect-msg');
  const badge = document.getElementById('gh-repo-badge');
  const consent = document.getElementById('gh-auto-confirm');
  let mode = 'review', busy = false, picking = false, known = false, uncertain = false, touched = false;
  function changeTarget() { touched = true; consent.checked = false; }
  repo.addEventListener('input', changeTarget);
  branch.addEventListener('input', changeTarget);
  const read = window.noyaruSettingsJson;
  function chooseMode(value) {
    mode = value === 'auto' ? 'auto' : 'review';
    document.querySelectorAll('.gh-mode-btn').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.mode === mode)));
    document.getElementById('gh-auto-warning').style.display = mode === 'auto' ? '' : 'none';
    document.getElementById('gh-mode-hint').textContent = mode === 'auto'
      ? noyaruT('Full Access : autorisations des traitements automatiques de ce site.')
      : noyaruT('Vérification : préparez puis relisez chaque proposition avant de l’appliquer.');
  }
  document.querySelectorAll('.gh-mode-btn').forEach(button => button.addEventListener('click', () => { touched = true; consent.checked = false; chooseMode(button.dataset.mode); }));
  function controls() {
    const locked = busy || picking || !known || uncertain;
    save.disabled = detach.disabled = repo.disabled = branch.disabled = consent.disabled = locked;
    document.querySelectorAll('.gh-mode-btn, #gh-repo-list button').forEach(button => { button.disabled = locked; });
    pick.disabled = busy || picking || !known || uncertain;
    refreshButton.disabled = busy || picking;
  }
  function validState(data) {
    return data && data.ok === true && typeof data.repo === 'string'
      && (data.repo === '' || typeof data.branch === 'string') && ['review', 'auto'].includes(data.mode);
  }
  function confirmed(data, replaceChoices = true) {
    const connected = !!data.repo;
    badge.textContent = connected ? noyaruT('Dépôt sélectionné') : noyaruT('Aucun dépôt sélectionné');
    detach.style.display = connected ? '' : 'none';
    status.textContent = connected ? noyaruT('Dépôt enregistré : {repo}. Les corrections restent dans leur parcours de préparation et de validation.').replace('{repo}', data.repo) : noyaruT('Choisissez un dépôt pour préparer des corrections dans le code de ce site.');
    if (replaceChoices) { repo.value = data.repo || ''; branch.value = data.branch || 'main'; chooseMode(data.mode); }
  }
  async function refresh() {
    if (busy || picking) return;
    busy = true; controls(); status.textContent = noyaruT('Lecture de la configuration…');
    try {
      const data = await read(await fetch(api + 'status', {cache:'no-store', headers:{Accept:'application/json'}}));
      if (!validState(data)) throw new Error(data.error);
      const replaceChoices = !touched || uncertain;
      confirmed(data, replaceChoices); known = true; uncertain = false;
      if (replaceChoices) { touched = false; consent.checked = false; }
      feedback.textContent = '';
    } catch (error) { known = false; badge.textContent = noyaruT('État indisponible'); window.noyaruSettingsError(status, error); }
    finally { busy = false; controls(); }
  }
  const refreshButton = document.createElement('button'); refreshButton.type = 'button'; refreshButton.className = 'btn'; refreshButton.textContent = noyaruT('Actualiser l’état'); refreshButton.addEventListener('click', refresh); status.after(refreshButton);
  async function mutate(body) {
    if (busy || picking || !known || uncertain) return;
    busy = true; controls(); feedback.textContent = noyaruT('Enregistrement…');
    try {
      const response = await fetch(api + 'connect', {method:'POST', headers:{'Content-Type':'application/json',Accept:'application/json'}, body:JSON.stringify(body)});
      const data = await read(response);
      if (response.status >= 500 || response.status === 408) throw new Error('Unconfirmed settings');
      if (!response.ok || !data.ok) { feedback.textContent = data.error || noyaruT('La demande a été refusée.'); return; }
      if (!validState(data)) throw new Error('Invalid settings response');
      confirmed(data); feedback.textContent = body.disconnect ? noyaruT('Dépôt retiré de ce site. Votre connexion GitHub personnelle reste disponible.') : noyaruT('Dépôt enregistré pour ce site.'); touched = false;
    } catch (error) {
      uncertain = true; badge.textContent = noyaruT('À confirmer');
      status.textContent = noyaruT('État à confirmer');
      window.noyaruSettingsError(feedback, error, true);
    } finally { busy = false; controls(); }
  }
  save.addEventListener('click', () => {
    if (!repo.value.trim().match(/^[^/\s]+\/[^/\s]+$/)) { feedback.textContent = noyaruT('Saisissez un dépôt sous la forme compte/nom-du-dépôt.'); repo.focus(); return; }
    if (mode === 'auto' && !consent.checked) { feedback.textContent = noyaruT('Confirmez l’autorisation pour le mode Full Access.'); consent.focus(); return; }
    mutate({repo:repo.value.trim(),branch:branch.value.trim() || 'main',mode,consent:consent.checked});
  });
  detach.addEventListener('click', () => {
    if (confirm(noyaruT('Retirer ce dépôt du site ? Les propositions déjà créées sur GitHub restent disponibles.'))) mutate({repo:'',disconnect:true});
  });
  pick.addEventListener('click', async () => {
    if (pick.disabled) return;
    picking = true; controls(); feedback.textContent = noyaruT('Lecture des dépôts…'); list.replaceChildren();
    try {
      const data = await read(await fetch('/api/github/repos?slug=' + encodeURIComponent(root.dataset.projectSlug),{cache:'no-store',headers:{Accept:'application/json'}}));
      if (!data.ok) { feedback.textContent = data.error || noyaruT('Les dépôts ne sont pas disponibles.'); return; }
      (data.repos || []).forEach(item => {
        const button = document.createElement('button'); button.type = 'button'; button.className = 'btn'; button.textContent = item.full_name + (item.private ? ' · ' + noyaruT('privé') : '');
        button.addEventListener('click', () => { if (busy || picking || !known || uncertain) return; repo.value = item.full_name; branch.value = item.default_branch || 'main'; changeTarget(); list.style.display = 'none'; repo.focus(); }); list.append(button);
      });
      feedback.textContent = data.repos?.length ? noyaruT('Choisissez le dépôt de ce site.') : noyaruT('Aucun dépôt accessible avec cette connexion.');
      list.style.display = 'grid';
    } catch (error) { window.noyaruSettingsError(feedback, error); }
    finally { picking = false; controls(); }
  });
  chooseMode('review'); refresh();
})();
