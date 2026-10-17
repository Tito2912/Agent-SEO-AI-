/* Keyboard support for the existing mobile drawer. */
(() => {
  const toggle = document.getElementById('nav-mobile');
  const drawer = document.getElementById('barre-laterale');
  const opener = document.querySelector('.nav-mobile-bouton');
  const closer = document.getElementById('nav-mobile-close');
  const mobile = window.matchMedia('(max-width: 980px)');
  if (!toggle || !drawer || !opener || !closer) return;
  const background = [document.querySelector('.main'), document.getElementById('assistant-dock')].filter(Boolean);
  let wasOpen = false;
  function sync() {
    const open = mobile.matches && toggle.checked;
    opener.setAttribute('aria-expanded', String(open));
    background.forEach(el => { el.inert = open; });
    if (open) {
      drawer.setAttribute('role', 'dialog');
      drawer.setAttribute('aria-modal', 'true');
      closer.focus();
    } else {
      drawer.removeAttribute('role');
      drawer.removeAttribute('aria-modal');
      if (wasOpen && mobile.matches) opener.focus();
    }
    wasOpen = open;
  }
  toggle.addEventListener('change', sync);
  mobile.addEventListener('change', () => { toggle.checked = false; sync(); });
  document.addEventListener('keydown', event => {
    if (!mobile.matches || !toggle.checked) return;
    if (event.key === 'Escape') {
      event.preventDefault(); toggle.checked = false; sync();
    } else if (event.key === 'Tab') {
      const items = Array.from(drawer.querySelectorAll('a[href], button, summary, [tabindex="0"]'))
        .filter(el => !el.disabled && el.getClientRects().length);
      const first = items[0], last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    }
  });
  drawer.addEventListener('click', event => {
    if (event.target.closest('a[href]') && mobile.matches) { toggle.checked = false; sync(); }
  });
  sync();
})();
