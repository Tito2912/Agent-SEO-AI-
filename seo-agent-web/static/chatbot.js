(() => {
  const dock = document.getElementById("assistant-dock");
  const toggle = document.getElementById("assistant-toggle");
  const panel = document.getElementById("assistant-panel");
  const closeBtn = document.getElementById("assistant-close");
  const clearBtn = document.getElementById("assistant-clear");
  const form = document.getElementById("assistant-form");
  const input = document.getElementById("assistant-input");
  const sendBtn = document.getElementById("assistant-send");
  const messagesEl = document.getElementById("assistant-messages");
  const metaEl = document.getElementById("assistant-meta");
  const dotEl = document.getElementById("assistant-dot");
  const footEl = document.getElementById("assistant-foot");

  if (!dock || !toggle || !panel || !form || !input || !sendBtn || !messagesEl || !metaEl || !dotEl || !footEl) {
    return;
  }

  const ctx = (window.SEO_AUDIT_ASSISTANT_CONTEXT && typeof window.SEO_AUDIT_ASSISTANT_CONTEXT === "object")
    ? window.SEO_AUDIT_ASSISTANT_CONTEXT
    : {};

  const projectSlug = (ctx.project && typeof ctx.project === "object" && ctx.project.slug) ? String(ctx.project.slug) : "";
  const historyKey = `seo-audit-assistant:history:${projectSlug || "global"}`;
  const openKey = `seo-audit-assistant:open`;

  let meta = null;
  let history = loadHistory();
  let isOpen = false;
  let closeTimer = null;

  function normalizeAssistantText(content) {
    let s = String(content || "");
    s = s.trim();
    s = s.replace(/^Erreur:\s*(?:RuntimeError|Exception|Error):\s*/i, "Erreur: ");
    s = s.replace(/^RuntimeError:\s*/i, "");
    return s.slice(0, 2000);
  }

  function loadHistory() {
    try {
      const raw = localStorage.getItem(historyKey);
      const parsed = raw ? JSON.parse(raw) : null;
      if (!Array.isArray(parsed)) return [];
      let changed = false;
      const out = parsed
        .filter((m) => m && typeof m === "object")
        .map((m) => {
          const role = (m.role === "assistant" ? "assistant" : "user");
          const rawContent = String(m.content || "").trim().slice(0, 2000);
          const content = (role === "assistant" && /^(Erreur:|RuntimeError:)/i.test(rawContent))
            ? normalizeAssistantText(rawContent)
            : rawContent;
          if (content !== rawContent) changed = true;
          return { role, content };
        })
        .filter((m) => m.content);
      if (changed) {
        try {
          localStorage.setItem(historyKey, JSON.stringify(out.slice(-20)));
        } catch {
          // ignore
        }
      }
      return out;
    } catch {
      return [];
    }
  }

  function saveHistory() {
    try {
      const trimmed = history.slice(-20);
      localStorage.setItem(historyKey, JSON.stringify(trimmed));
    } catch {
      // ignore
    }
  }

  function setOpen(next, opts) {
    const instant = Boolean(opts && opts.instant);
    isOpen = Boolean(next);
    toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");

    if (closeTimer) {
      clearTimeout(closeTimer);
      closeTimer = null;
    }

    try {
      localStorage.setItem(openKey, isOpen ? "1" : "0");
    } catch {
      // ignore
    }

    if (isOpen) {
      panel.hidden = false;
      dock.classList.add("assistant-mounted");
      if (instant) {
        dock.classList.add("assistant-open");
      } else {
        requestAnimationFrame(() => dock.classList.add("assistant-open"));
      }
      render();
      input.focus();
      input.select();
      return;
    }

    dock.classList.remove("assistant-open");
    if (instant) {
      panel.hidden = true;
      dock.classList.remove("assistant-mounted");
      return;
    }

    closeTimer = window.setTimeout(() => {
      panel.hidden = true;
      dock.classList.remove("assistant-mounted");
      closeTimer = null;
    }, 190);
  }

  function escapeHtml(s) {
    return String(s)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll("\"", "&quot;")
      .replaceAll("'", "&#39;");
  }

  function renderMessage(role, content) {
    const row = document.createElement("div");
    row.className = `assistant-msg ${role}`;
    if (role === "assistant" && String(content || "").trim().toLowerCase().startsWith("erreur:")) {
      row.classList.add("error");
    }

    const roleEl = document.createElement("div");
    roleEl.className = "assistant-role";
    roleEl.textContent = (role === "user") ? "Vous" : "Assistant";

    const bubble = document.createElement("div");
    bubble.className = "assistant-bubble";
    bubble.textContent = content;

    row.appendChild(roleEl);
    row.appendChild(bubble);
    return row;
  }

  function render() {
    messagesEl.innerHTML = "";
    for (const m of history) {
      messagesEl.appendChild(renderMessage(m.role, m.content));
    }
    messagesEl.scrollTop = messagesEl.scrollHeight;

    if (meta && meta.configured === false) {
      const settingsUrl = meta.settings_url || "/settings/accounts";
      footEl.innerHTML = `Non configuré. Configure une clé dans <a href="${escapeHtml(settingsUrl)}">Comptes &amp; tokens</a>.`;
    } else {
      footEl.textContent = "";
    }
  }

  function setDot(status) {
    dotEl.classList.remove("ok", "err");
    if (status === "ok") dotEl.classList.add("ok");
    if (status === "err") dotEl.classList.add("err");
  }

  async function refreshMeta() {
    metaEl.textContent = "…";
    setDot();
    try {
      const resp = await fetch("/api/assistant/meta", { headers: { "Accept": "application/json" } });
      const data = await resp.json();
      meta = data && typeof data === "object" ? data : null;
    } catch {
      meta = null;
    }

    if (!meta || meta.ok !== true) {
      metaEl.textContent = "Assistant indisponible";
      setDot("err");
      return;
    }

    const p = meta.effective_provider || "—";
    const model = (meta.providers && meta.providers[p] && meta.providers[p].model) ? meta.providers[p].model : "";
    const configured = Boolean(meta.configured);

    metaEl.textContent = configured ? `${p} · ${model || "model"}` : `${p} · à configurer`;
    setDot(configured ? "ok" : "err");
    if (isOpen) render();
  }

  async function sendMessage(text) {
    const message = String(text || "").trim();
    if (!message) return;

    const historyForServer = history.slice(-20);
    history.push({ role: "user", content: message });
    history.push({ role: "assistant", content: "…" });
    saveHistory();
    render();

    input.value = "";
    input.focus();
    input.disabled = true;
    sendBtn.disabled = true;

    try {
      const resp = await fetch("/api/assistant/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Accept": "application/json" },
        body: JSON.stringify({
          message,
          history: historyForServer,
          context: ctx,
        }),
      });
      const data = await resp.json();
      if (!data || data.ok !== true) {
        throw new Error((data && data.error) ? data.error : "Erreur inconnue");
      }

      history[history.length - 1] = { role: "assistant", content: String(data.reply || "").trim() || "—" };
      saveHistory();
      await refreshMeta();
      render();
    } catch (e) {
      const msg = (e && e.message) ? e.message : String(e || "Erreur");
      history[history.length - 1] = { role: "assistant", content: normalizeAssistantText(`Erreur: ${msg}`) };
      saveHistory();
      setDot("err");
      render();
    } finally {
      input.disabled = false;
      sendBtn.disabled = false;
      input.focus();
    }
  }

  toggle.addEventListener("click", () => setOpen(!isOpen));
  if (closeBtn) {
    closeBtn.addEventListener("click", () => setOpen(false));
  }
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      const ok = window.confirm("Effacer la conversation ?");
      if (!ok) return;
      history = [];
      saveHistory();
      render();
      input.focus();
    });
  }

  form.addEventListener("submit", (ev) => {
    ev.preventDefault();
    sendMessage(input.value);
  });

  document.addEventListener("keydown", (ev) => {
    if (ev.key === "Escape" && isOpen) setOpen(false);
  });

  document.addEventListener("click", (ev) => {
    if (!isOpen) return;
    const t = ev.target;
    if (!(t instanceof Node)) return;
    if (dock.contains(t)) return;
    setOpen(false);
  });

  try {
    isOpen = localStorage.getItem(openKey) === "1";
  } catch {
    isOpen = false;
  }

  setOpen(isOpen, { instant: true });
  refreshMeta();
})();
