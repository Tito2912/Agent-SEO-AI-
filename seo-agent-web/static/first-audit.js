const noyaruT = window.noyaruT || ((text) => text);
(() => {
  const forms = document.querySelectorAll('[data-audit-submit]');
  for (const form of forms) {
    const button = form.querySelector('button[type="submit"]');
    if (!button) continue;
    const label = button.textContent;
    const disabled = button.disabled;
    form.addEventListener('submit', (event) => {
      if (form.dataset.submitting === 'yes') { event.preventDefault(); return; }
      form.dataset.submitting = 'yes';
      form.setAttribute('aria-busy', 'true');
      button.disabled = true;
      // Le texte traduit vient du gabarit (data-audit-submit) ; le francais reste le repli.
      button.textContent = form.dataset.auditSubmit || noyaruT('Démarrage de l’analyse…');
    });
    window.addEventListener('pageshow', () => {
      delete form.dataset.submitting;
      form.removeAttribute('aria-busy');
      button.disabled = disabled;
      button.textContent = label;
    });
  }
})();
