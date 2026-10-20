/* Project repository settings: show only confirmed state; every mutation is explicit. */
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
  let mode = 'review', busy = false, known = false, uncertain = false, touched = false;
  repo.addEventListener('input', () => { touched = true; });
  branch.addEventListener('input', () => { touched = true; });
  const read = window.noyaruSettingsJson;
  function chooseMode(value) {
    mode = value === 'auto' ? 'auto' : 'review';
    document.querySelectorAll('.gh-mode-btn').forEach(button => button.setAttribute('aria-pressed', String(button.dataset.mode === mode)));
    document.getElementById('gh-auto-warning').style.display = mode === 'auto' ? '' : 'none';
    document.getElementById('gh-mode-hint').textContent = mode === 'auto'
      ? 'Full Access : autorisations des traitements automatiques de ce site.'
      : 'Vérification : préparez puis relisez chaque proposition avant de l’appliquer.';
  }
  document.querySelectorAll('.gh-mode-btn').forEach(button => button.addEventListener('click', () => { touched = true; consent.checked = false; chooseMode(button.dataset.mode); }));
  function controls() {
    save.disabled = busy || !known || uncertain;
    detach.disabled = busy || !known || uncertain;
    repo.disabled = branch.disabled = busy;
    document.querySelectorAll('.gh-mode-btn').forEach(button => { button.disabled = busy; });
    consent.disabled = busy;
  }
  function confirmed(data, replaceChoices = true) {
    const connected = !!data.repo;
    badge.textContent = connected ? 'Dépôt sélectionné' : 'Aucun dépôt sélectionné';
    detach.style.display = connected ? '' : 'none';
    status.textContent = connected ? 'Dépôt enregistré : ' + data.repo + '. Les corrections restent dans leur parcours de préparation et de validation.' : 'Choisissez un dépôt pour préparer des corrections dans le code de ce site.';
    if (replaceChoices) { repo.value = data.repo || ''; branch.value = data.branch || 'main'; chooseMode(data.mode); }
  }
  async function refresh() {
    if (busy) return;
    busy = true; controls(); status.textContent = 'Lecture de la configuration…';
    try {
      const data = await read(await fetch(api + 'status', {cache:'no-store', headers:{Accept:'application/json'}}));
      if (!data.ok) throw new Error(data.error);
      confirmed(data, !touched || uncertain); known = true; uncertain = false; feedback.textContent = '';
    } catch (error) { known = false; badge.textContent = 'État indisponible'; window.noyaruSettingsError(status, error); }
    finally { busy = false; controls(); }
  }
  const refreshButton = document.createElement('button'); refreshButton.type = 'button'; refreshButton.className = 'btn'; refreshButton.textContent = 'Actualiser l’état'; refreshButton.addEventListener('click', refresh); status.after(refreshButton);
  async function mutate(body) {
    if (busy || !known || uncertain) return;
    busy = true; controls(); feedback.textContent = 'Enregistrement…';
    try {
      const response = await fetch(api + 'connect', {method:'POST', headers:{'Content-Type':'application/json',Accept:'application/json'}, body:JSON.stringify(body)});
      const data = await read(response);
      if (!response.ok || !data.ok) { feedback.textContent = data.error || 'La demande a été refusée.'; return; }
      confirmed(data); feedback.textContent = body.disconnect ? 'Dépôt retiré de ce site. Votre connexion GitHub personnelle reste disponible.' : 'Dépôt enregistré pour ce site.'; touched = false;
    } catch (error) {
      uncertain = true; window.noyaruSettingsError(feedback, error, true);
    } finally { busy = false; controls(); }
  }
  save.addEventListener('click', () => {
    if (!repo.value.trim().match(/^[^/\s]+\/[^/\s]+$/)) { feedback.textContent = 'Saisissez un dépôt sous la forme compte/nom-du-dépôt.'; repo.focus(); return; }
    if (mode === 'auto' && !consent.checked) { feedback.textContent = 'Confirmez l’autorisation pour le mode Full Access.'; consent.focus(); return; }
    mutate({repo:repo.value.trim(),branch:branch.value.trim() || 'main',mode});
  });
  detach.addEventListener('click', () => {
    if (confirm('Retirer ce dépôt du site ? Les propositions déjà créées sur GitHub restent disponibles.')) mutate({repo:'',disconnect:true});
  });
  pick.addEventListener('click', async () => {
    if (pick.disabled) return;
    pick.disabled = true; feedback.textContent = 'Lecture des dépôts…'; list.replaceChildren();
    try {
      const data = await read(await fetch('/api/github/repos',{cache:'no-store',headers:{Accept:'application/json'}}));
      if (!data.ok) { feedback.textContent = data.error || 'Les dépôts ne sont pas disponibles.'; return; }
      (data.repos || []).forEach(item => {
        const button = document.createElement('button'); button.type = 'button'; button.className = 'btn'; button.textContent = item.full_name + (item.private ? ' · privé' : '');
        button.addEventListener('click', () => { repo.value = item.full_name; branch.value = item.default_branch || 'main'; touched = true; list.style.display = 'none'; repo.focus(); }); list.append(button);
      });
      feedback.textContent = data.repos?.length ? 'Choisissez le dépôt de ce site.' : 'Aucun dépôt accessible avec cette connexion.';
      list.style.display = 'grid';
    } catch (error) { window.noyaruSettingsError(feedback, error); }
    finally { pick.disabled = false; }
  });
  chooseMode('review'); refresh();
})();
