/* Filter only the public cards already present. Queries never leave this page. */
(() => {
  const root = document.querySelector('[data-help-directory]');
  if (!root) return;
  const controls = root.querySelector('[data-help-search]');
  const input = controls?.querySelector('input[type="search"]');
  if (!input) return;
  const noyaruT = window.noyaruT || (text => text);
  const normalize = value => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const cards = Array.from(root.querySelectorAll('[data-help-card]')).map(element => ({element, text: normalize(element.textContent)}));
  const sections = Array.from(root.querySelectorAll('[data-help-section]'));
  const links = Array.from(root.querySelectorAll('[data-help-section-link]'));
  const count = root.querySelector('[data-help-count]');
  const empty = root.querySelector('[data-help-empty]');
  let timer;
  const pageCount = n => (n === 1 ? noyaruT("{n} page") : noyaruT("{n} pages")).replace('{n}', String(n));
  function update() {
    clearTimeout(timer);
    const words = normalize(input.value.trim()).split(/\s+/).filter(Boolean);
    let visible = 0;
    cards.forEach(({element, text}) => { element.hidden = !words.every(word => text.includes(word)); if (!element.hidden) visible++; });
    sections.forEach(section => {
      const matches = Array.from(section.querySelectorAll('[data-help-card]')).filter(card => !card.hidden).length;
      section.hidden = matches === 0;
      section.querySelector('[data-help-section-count]').textContent = pageCount(matches);
      links.filter(link => link.dataset.helpSectionLink === section.id).forEach(link => { link.hidden = section.hidden; });
    });
    root.querySelectorAll('[data-help-shortcuts]').forEach(element => { element.hidden = words.length > 0; });
    root.querySelector('[data-help-contents]').hidden = visible === 0;
    count.textContent = pageCount(visible);
    empty.hidden = visible !== 0;
  }
  function reset() { input.value = ''; update(); input.focus(); }
  controls.hidden = false;
  root.querySelectorAll('[data-help-reset]').forEach(button => button.addEventListener('click', reset));
  input.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(update, 120); });
  input.addEventListener('keydown', event => { if (event.key === 'Escape') { event.preventDefault(); reset(); } });
  window.addEventListener('pageshow', update);
  update();
})();
