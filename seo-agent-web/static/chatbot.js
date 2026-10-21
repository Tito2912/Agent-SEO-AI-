(() => {
  const noyaruT = window.noyaruT || (text => text);
  const byId = id => document.getElementById(id);
  const dock = byId('assistant-dock'),
    toggle = byId('assistant-toggle'),
    panel = byId('assistant-panel');
  const closeBtn = byId('assistant-close'),
    clearBtn = byId('assistant-clear');
  const form = byId('assistant-form'),
    input = byId('assistant-input'),
    sendBtn = byId('assistant-send');
  const messagesEl = byId('assistant-messages'),
    metaEl = byId('assistant-meta'),
    dotEl = byId('assistant-dot'),
    footEl = byId('assistant-foot');
  if (![dock, toggle, panel, form, input, sendBtn, messagesEl, metaEl, dotEl, footEl].every(Boolean)) return;

  const style = document.createElement('link');
  style.rel = 'stylesheet';
  style.href = '/static/assistant-journey.css?v=1';
  document.head.appendChild(style);
  dock.classList.add('assistant-journey');
  const ctx = window.SEO_AUDIT_ASSISTANT_CONTEXT || {};
  const projectSlug = String(ctx.project && ctx.project.slug || '');
  const projectName = String(ctx.project && ctx.project.site_name || '');
  const returnPath = projectSlug ? '/projects/' + encodeURIComponent(projectSlug) : '/';
  const loginPath = '/auth/login?next=' + encodeURIComponent(returnPath);
  const errorPrefix = noyaruT("Erreur:");
  const statusEl = document.createElement('p'),
    actionsEl = document.createElement('div'),
    linksEl = document.createElement('nav');
  statusEl.className = 'assistant-status';
  statusEl.setAttribute('role', 'status');
  actionsEl.className = 'assistant-recovery';
  linksEl.className = 'assistant-links';
  linksEl.setAttribute('aria-label', noyaruT("Aide"));

  function link(label, href) {
    const a = document.createElement('a');
    a.textContent = label;
    a.href = href;
    return a;
  }
  if (projectSlug) linksEl.appendChild(link(noyaruT("Voir mon bilan"), returnPath));
  linksEl.appendChild(link(noyaruT("Aide"), document.documentElement.lang === 'en' ? '/en/docs' : '/docs'));
  footEl.replaceChildren(statusEl, actionsEl, linksEl);
  messagesEl.setAttribute('role', 'log');
  messagesEl.setAttribute('aria-relevant', 'additions text');
  messagesEl.setAttribute('aria-atomic', 'false');

  let scope = '',
    history = [],
    ready = false,
    configured = false,
    isOpen = false,
    interacted = false,
    busy = false;
  let generation = 0,
    metaPromise = null,
    needsReload = false,
    cooldownUntil = 0,
    cooldownTimer = null,
    deferredDraft = null;
  let pendingRow = null,
    typingRow = null,
    statusKind = 'checking';

  function historyKey() {
    return 'seo-audit-assistant:v2:history:' + scope;
  }

  function openKey() {
    return 'seo-audit-assistant:v2:open:' + scope;
  }

  function setStatus(text, kind = '', action = '') {
    statusKind = kind;
    statusEl.textContent = text;
    statusEl.hidden = !text;
    statusEl.setAttribute('role', kind === 'error' ? 'alert' : 'status');
    actionsEl.replaceChildren();
    dotEl.classList.toggle('err', kind === 'error');
    dotEl.classList.remove('ok');
    if (action === 'login') actionsEl.appendChild(link(noyaruT("Connexion"), loginPath));
    if (action === 'reload') actionsEl.appendChild(link(noyaruT("Actualiser"), returnPath));
    if (action === 'billing') actionsEl.appendChild(link(noyaruT("Consulter mon abonnement"), '/billing'));
    if (action === 'sites') actionsEl.appendChild(link(noyaruT("Retrouver mes sites"), '/'));
    if (action === 'refresh') {
      const button = document.createElement('button');
      button.type = 'button';
      button.textContent = noyaruT("Actualiser");
      button.addEventListener('click', () => refreshMeta(true));
      actionsEl.appendChild(button);
    }
  }

  function syncControls() {
    const blocked = busy || !ready || !configured || needsReload || Date.now() < cooldownUntil;
    input.disabled = busy || !ready || needsReload;
    sendBtn.disabled = blocked || !input.value.trim();
    if (clearBtn) clearBtn.disabled = busy || !ready || history.length === 0;
    messagesEl.querySelectorAll('.assistant-suggestion').forEach(button => {
      button.disabled = blocked;
    });
    actionsEl.querySelectorAll('button').forEach(button => {
      button.disabled = busy || Boolean(metaPromise);
    });
    messagesEl.setAttribute('aria-busy', busy ? 'true' : 'false');
  }

  function loadHistory() {
    try {
      const values = JSON.parse(localStorage.getItem(historyKey()) || '[]');
      if (!Array.isArray(values)) return [];
      return values.slice(-24).filter(m => m && ['user', 'assistant'].includes(m.role) && typeof m.content === 'string')
        .map(m => ({
          role: m.role,
          content: m.content.trim().slice(0, 4000)
        })).filter(m => m.content);
    } catch {
      return [];
    }
  }

  function saveHistory() {
    if (!ready || !scope || needsReload) return;
    try {
      localStorage.setItem(historyKey(), JSON.stringify(history.slice(-24)));
    } catch {
      /* Conversation still works in memory. */ }
  }

  function clearPending() {
    if (pendingRow) pendingRow.remove();
    if (typingRow) typingRow.remove();
    pendingRow = typingRow = null;
  }

  function renderMessage(role, content) {
    const row = document.createElement('div');
    row.className = 'assistant-msg ' + role;
    const bubble = document.createElement('div');
    bubble.className = 'assistant-bubble';
    if (role === 'assistant') bubble.innerHTML = buildBubbleHtml(content);
    else bubble.textContent = content;
    row.appendChild(bubble);
    return row;
  }

  function renderHistory() {
    clearPending();
    messagesEl.replaceChildren();
    if (!history.length) {
      const empty = document.createElement('div');
      empty.className = 'assistant-empty';
      const title = document.createElement('p');
      title.className = 'assistant-empty-title';
      title.textContent = noyaruT("Comment puis-je vous aider ?");
      const description = document.createElement('p');
      description.className = 'assistant-empty-sub';
      description.textContent = noyaruT("Posez une question sur vos données SEO ou choisissez une suggestion.");
      const choices = document.createElement('div');
      choices.className = 'assistant-suggestions';
      for (const question of [noyaruT("Par quoi devrais-je commencer pour améliorer le SEO ?"), noyaruT("Quelle est la différence entre une erreur et un avertissement ?"), noyaruT("Comment connecter Google Search Console ?")]) {
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'assistant-suggestion';
        button.textContent = question;
        button.addEventListener('click', () => sendMessage(question));
        choices.appendChild(button);
      }
      empty.append(title, description, choices);
      messagesEl.appendChild(empty);
    } else history.forEach(m => messagesEl.appendChild(renderMessage(m.role, m.content)));
    messagesEl.scrollTop = messagesEl.scrollHeight;
    syncControls();
  }

  function setOpen(next, {
    focus = true,
    restoreFocus = true
  } = {}) {
    const hadFocus = panel.contains(document.activeElement);
    isOpen = Boolean(next);
    toggle.setAttribute('aria-expanded', String(isOpen));
    panel.inert = !isOpen;
    panel.hidden = !isOpen;
    dock.classList.toggle('assistant-mounted', isOpen);
    dock.classList.toggle('assistant-open', isOpen);
    if (ready && scope) {
      try {
        localStorage.setItem(openKey(), isOpen ? '1' : '0');
      } catch {
        /* Optional storage. */ }
    }
    if (isOpen) {
      if (focus) input.focus();
      refreshMeta();
    } else if (restoreFocus && hadFocus) toggle.focus();
  }
  async function readJson(url, options = {}, timeout = 15000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);
    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
        cache: 'no-store',
        credentials: 'same-origin'
      });
      let data = null;
      try {
        data = await response.json();
      } catch {
        /* A non-JSON response is a recoverable failure. */ }
      return {
        response,
        data
      };
    } finally {
      clearTimeout(timer);
    }
  }

  function invalidate(message, action = 'refresh') {
    if (scope && input.value) deferredDraft = {
      scope,
      message: input.value
    };
    input.value = '';
    ready = false;
    configured = false;
    generation++;
    history = [];
    renderHistory();
    metaEl.textContent = noyaruT("Assistant indisponible");
    setStatus(message, 'error', action);
    syncControls();
  }

  function changedIdentity() {
    needsReload = true;
    deferredDraft = null;
    input.value = '';
    invalidate(noyaruT("Votre session n’est plus disponible. Reconnectez-vous avant de reprendre votre travail."), 'reload');
  }
  async function refreshMeta(explicit = false) {
    if (needsReload) return false;
    if (metaPromise) return metaPromise;
    const returnFocus = explicit && actionsEl.contains(document.activeElement);
    if (explicit || !scope) {
      metaEl.textContent = noyaruT("Chargement…");
      setStatus(noyaruT("Chargement…"), 'checking');
    }
    metaPromise = (async () => {
      try {
        const {
          response,
          data
        } = await readJson('/api/assistant/meta' + (projectSlug ? '?slug=' + encodeURIComponent(projectSlug) : ''), {
          headers: {
            Accept: 'application/json'
          }
        });
        if (response.status === 401) {
          invalidate(noyaruT("Votre session a expiré. Reconnectez-vous pour reprendre cet écran."), 'login');
          return false;
        }
        if (response.status === 403 || response.status === 404) {
          invalidate(noyaruT("Votre choix n’est pas disponible pour le moment. Réessayez dans quelques instants."), 'sites');
          return false;
        }
        if (!response.ok || !data || data.ok !== true || !/^[a-f0-9]{64}$/.test(data.history_scope || '')) throw new Error('metadata');
        if (scope && scope !== data.history_scope) {
          changedIdentity();
          return false;
        }
        const first = !scope,
          restore = !ready;
        scope = data.history_scope;
        ready = true;
        configured = data.configured === true;
        if (restore) {
          history = loadHistory();
          renderHistory();
        }
        if (deferredDraft && deferredDraft.scope === scope && !input.value) {
          input.value = deferredDraft.message;
          deferredDraft = null;
        }
        metaEl.textContent = projectName || noyaruT("Posez une question");
        if (!configured) setStatus(noyaruT("Assistant temporairement indisponible."), 'error', 'refresh');
        else if (explicit || ['checking', 'unavailable'].includes(statusKind)) setStatus('');
        if (first && !interacted) {
          try {
            if (localStorage.getItem(openKey()) === '1') setOpen(true, {
              focus: false
            });
          } catch {
            /* Optional storage. */ }
        }
        syncControls();
        return true;
      } catch {
        invalidate(noyaruT("Le service ne répond pas comme prévu. Réessayez dans quelques instants."));
        return false;
      }
    })();
    try {
      return await metaPromise;
    } finally {
      metaPromise = null;
      syncControls();
      if (returnFocus && isOpen && !busy && document.activeElement === document.body) {
        const target = ready && configured ? input : actionsEl.querySelector('a,button');
        if (target) target.focus();
      }
    }
  }

  function startCooldown(response) {
    const seconds = Number(response.headers.get('Retry-After'));
    if (!Number.isFinite(seconds) || seconds <= 0 || seconds > 3600) return;
    cooldownUntil = Date.now() + seconds * 1000;
    if (cooldownTimer) clearInterval(cooldownTimer);
    const tick = () => {
      const remaining = Math.max(0, Math.ceil((cooldownUntil - Date.now()) / 1000));
      sendBtn.setAttribute('aria-label', noyaruT("Envoyer") + (remaining ? ' · ' + remaining + ' s' : ''));
      sendBtn.title = remaining ? noyaruT("Réessayer") + ' · ' + remaining + ' s' : noyaruT("Envoyer");
      syncControls();
      if (!remaining) {
        clearInterval(cooldownTimer);
        cooldownTimer = null;
      }
    };
    tick();
    cooldownTimer = setInterval(tick, 1000);
  }
  async function sendMessage(text) {
    const message = String(text || '').trim().slice(0, 2000);
    if (!message || busy || needsReload || Date.now() < cooldownUntil) return;
    busy = true;
    syncControls();
    let submittedScope = '',
      started = 0;
    try {
      if (!await refreshMeta() || !configured) return;
      submittedScope = scope;
      started = generation;
      setStatus(noyaruT("Chargement…"), 'pending');
      messagesEl.querySelector('.assistant-empty')?.remove();
      pendingRow = renderMessage('user', message);
      typingRow = renderMessage('assistant', noyaruT("Chargement…"));
      typingRow.classList.add('assistant-pending');
      typingRow.setAttribute('aria-hidden', 'true');
      messagesEl.append(pendingRow, typingRow);
      messagesEl.scrollTop = messagesEl.scrollHeight;
      input.value = '';
      const {
        response,
        data
      } = await readJson('/api/assistant/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Accept: 'application/json'
        },
        body: JSON.stringify({
          message,
          history: history.slice(-20),
          context: ctx,
          history_scope: submittedScope
        })
      }, 120000);
      if (response.status === 409 && data && data.error_code === 'context_changed') {
        changedIdentity();
        return;
      }
      if (!await refreshMeta() || generation !== started || scope !== submittedScope) {
        if (!needsReload) deferredDraft = {
          scope: submittedScope,
          message
        };
        return;
      }
      clearPending();
      if (response.status === 401) {
        deferredDraft = {
          scope: submittedScope,
          message
        };
        invalidate(noyaruT("Votre session a expiré. Reconnectez-vous pour reprendre cet écran."), 'login');
        return;
      }
      if (response.status === 402) {
        setStatus(noyaruT("Une limite de votre formule est atteinte"), 'error', 'billing');
        input.value = message;
        return;
      }
      if (response.status === 429) {
        startCooldown(response);
        setStatus(noyaruT("Votre choix n’est pas disponible pour le moment. Réessayez dans quelques instants."), 'error');
        input.value = message;
        return;
      }
      if (response.status === 403) {
        setStatus(noyaruT("Votre session n’est plus disponible. Reconnectez-vous avant de reprendre votre travail."), 'error', 'reload');
        input.value = message;
        return;
      }
      if (!response.ok || !data || data.ok !== true || typeof data.reply !== 'string' || !data.reply.trim()) throw new Error('reply');
      const answer = data.reply.trim().slice(0, 4000);
      history.push({
        role: 'user',
        content: message
      }, {
        role: 'assistant',
        content: answer
      });
      history = history.slice(-24);
      saveHistory();
      messagesEl.append(renderMessage('user', message), renderMessage('assistant', answer));
      setStatus('');
      messagesEl.scrollTop = messagesEl.scrollHeight;
    } catch {
      clearPending();
      if (submittedScope && !needsReload && await refreshMeta() && scope === submittedScope && generation === started) {
        input.value = message;
        setStatus(errorPrefix + ' ' + noyaruT("Le service ne répond pas comme prévu. Réessayez dans quelques instants."), 'error', 'refresh');
      } else if (submittedScope && !needsReload) deferredDraft = {
        scope: submittedScope,
        message
      };
    } finally {
      clearPending();
      busy = false;
      syncControls();
      const canReturnFocus = [document.body, sendBtn, input].includes(document.activeElement);
      if (isOpen && ready && !needsReload && !input.disabled && canReturnFocus) input.focus();
    }
  }

  toggle.addEventListener('click', () => {
    interacted = true;
    setOpen(!isOpen);
  });
  if (closeBtn) closeBtn.addEventListener('click', () => {
    interacted = true;
    setOpen(false);
  });
  if (clearBtn) clearBtn.addEventListener('click', () => {
    if (busy || !ready || !window.confirm(noyaruT("Effacer la conversation ?"))) return;
    history = [];
    deferredDraft = null;
    try {
      localStorage.removeItem(historyKey());
    } catch {
      /* Memory only. */ }
    renderHistory();
    setStatus('');
    if (isOpen) input.focus();
  });
  input.addEventListener('input', syncControls);
  form.addEventListener('submit', event => {
    event.preventDefault();
    sendMessage(input.value);
  });
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape' && isOpen) {
      event.preventDefault();
      setOpen(false);
    }
  });
  document.addEventListener('click', event => {
    if (isOpen && !event.composedPath().includes(dock)) setOpen(false, {
      restoreFocus: false
    });
  });
  window.addEventListener('focus', () => {
    if (isOpen) refreshMeta();
  });
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && isOpen) refreshMeta();
  });
  setOpen(false, {
    focus: false,
    restoreFocus: false
  });
  renderHistory();
  refreshMeta();

  function escapeHtml(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function inlineFormat(s) {
    return escapeHtml(s)
      .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
      .replace(/`([^`]+)`/g, "<code>$1</code>");
  }

  // ── Markdown renderer for assistant bubbles ───────────────────────────────
  function buildBubbleHtml(text) {
    const lines = String(text || "").split("\n");
    let html = "";
    let listTag = "";

    const closeList = () => {
      if (listTag) {
        html += `</${listTag}>`;
        listTag = "";
      }
    };

    for (const raw of lines) {
      const bulletM = raw.match(/^[-*]\s+(.*)/);
      const orderedM = raw.match(/^\d+\.\s+(.*)/);
      const blank = raw.trim() === "";

      if (bulletM) {
        if (listTag !== "ul") {
          closeList();
          html += "<ul>";
          listTag = "ul";
        }
        html += `<li>${inlineFormat(bulletM[1])}</li>`;
      } else if (orderedM) {
        if (listTag !== "ol") {
          closeList();
          html += "<ol>";
          listTag = "ol";
        }
        html += `<li>${inlineFormat(orderedM[1])}</li>`;
      } else {
        closeList();
        if (blank) {
          html += "<br>";
        } else {
          html += `<p>${inlineFormat(raw)}</p>`;
        }
      }
    }
    closeList();
    // Remove leading <br>
    return html.replace(/^(<br>)+/, "");
  }


})();
