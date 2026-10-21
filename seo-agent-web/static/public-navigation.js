/* Essential-cookie information remains readable without scripts or browser storage. */
(() => {
  const notice = document.getElementById('cookie-banner');
  const close = document.getElementById('cookie-accept');
  if (!notice || !close) return;
  try {
    if (localStorage.getItem('cookie_consent') === '1') {
      notice.remove();
      return;
    }
  } catch { /* Storage is optional; closing the information still works. */ }
  notice.classList.add('cookie-notice--floating');
  close.hidden = false;
  close.addEventListener('click', () => {
    try { localStorage.setItem('cookie_consent', '1'); } catch { /* This page only. */ }
    const restoreFocus = notice.contains(document.activeElement);
    notice.remove();
    if (restoreFocus) document.getElementById('main-content')?.focus({preventScroll: true});
  });
})();
