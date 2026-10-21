/* Success remains available until the customer dismisses it. */
(() => {
  const noyaruT = window.noyaruT || ((text) => text);
  document.querySelectorAll('.alert.ok').forEach((message) => {
    if (message.querySelector('[data-dismiss-feedback]')) return;
    if (!message.hasAttribute('role')) message.setAttribute('role', 'status');
    const dismiss = document.createElement('button');
    dismiss.type = 'button';
    dismiss.className = 'btn small feedback-dismiss';
    dismiss.dataset.dismissFeedback = '';
    dismiss.textContent = noyaruT("Fermer");
    dismiss.setAttribute('aria-label', noyaruT("Fermer ce message de confirmation"));
    dismiss.addEventListener('click', () => {
      const target = message.nextElementSibling || document.getElementById('main-content');
      message.remove();
      if (target) { target.setAttribute('tabindex', '-1'); target.focus({preventScroll:true}); }
    });
    message.append(dismiss);
  });
})();
