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
  const projectName = (ctx.project && ctx.project.site_name) ? String(ctx.project.site_name) : "";
  const historyKey = `seo-audit-assistant:history:${projectSlug || "global"}`;
  const openKey = `seo-audit-assistant:open`;

  let meta = null;
  let history = loadHistory();
  let isOpen = false;
  let closeTimer = null;
  let liveTypingEl = null;

  // ── Context-aware suggestions ─────────────────────────────────────────────
  function getSuggestions() {
    if (projectSlug) {
      return [
        "Quels sont les problèmes critiques sur ce site ?",
        "Par quoi devrais-je commencer pour améliorer le SEO ?",
        "Résume les anomalies les plus importantes",
      ];
    }
    return [
      "Comment fonctionne le crawl SEO ?",
      "Quelle est la différence entre une erreur et un avertissement ?",
      "Comment connecter Google Search Console ?",
    ];
  }

  // ── Helpers ───────────────────────────────────────────────────────────────
  function normalizeAssistantText(content) {
    let s = String(content || "").trim();
    s = s.replace(/^Erreur:\s*(?:RuntimeError|Exception|Error):\s*/i, "Erreur: ");
    s = s.replace(/^RuntimeError:\s*/i, "");
    return s.slice(0, 4000);
  }

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
      if (listTag) { html += `</${listTag}>`;  listTag = ""; }
    };

    for (const raw of lines) {
      const bulletM = raw.match(/^[-*]\s+(.*)/);
      const orderedM = raw.match(/^\d+\.\s+(.*)/);
      const blank = raw.trim() === "";

      if (bulletM) {
        if (listTag !== "ul") { closeList(); html += "<ul>"; listTag = "ul"; }
        html += `<li>${inlineFormat(bulletM[1])}</li>`;
      } else if (orderedM) {
        if (listTag !== "ol") { closeList(); html += "<ol>"; listTag = "ol"; }
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

  // ── History ───────────────────────────────────────────────────────────────
  function loadHistory() {
    try {
      const raw = localStorage.getItem(historyKey);
      const parsed = raw ? JSON.parse(raw) : null;
      if (!Array.isArray(parsed)) return [];
      return parsed
        .filter((m) => m && typeof m === "object")
        .map((m) => ({
          role: m.role === "assistant" ? "assistant" : "user",
          content: String(m.content || "").trim().slice(0, 4000),
        }))
        .filter((m) => m.content);
    } catch {
      return [];
    }
  }

  function saveHistory() {
    try {
      localStorage.setItem(historyKey, JSON.stringify(history.slice(-24)));
    } catch { /* ignore */ }
  }

  // ── Open / close ──────────────────────────────────────────────────────────
  function setOpen(next, opts) {
    const instant = Boolean(opts && opts.instant);
    isOpen = Boolean(next);
    toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");

    if (closeTimer) { clearTimeout(closeTimer); closeTimer = null; }

    try { localStorage.setItem(openKey, isOpen ? "1" : "0"); } catch { /* ignore */ }

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

  // ── Render ────────────────────────────────────────────────────────────────
  function renderMessage(role, content) {
    const row = document.createElement("div");
    row.className = `assistant-msg ${role}`;
    const isError = role === "assistant" && String(content || "").trim().toLowerCase().startsWith("erreur:");
    if (isError) row.classList.add("error");

    const bubble = document.createElement("div");
    bubble.className = "assistant-bubble";

    if (role === "assistant" && !isError) {
      bubble.innerHTML = buildBubbleHtml(content);
    } else {
      bubble.textContent = content;
    }

    row.appendChild(bubble);
    return row;
  }

  function renderTypingIndicator() {
    const row = document.createElement("div");
    row.className = "assistant-msg assistant";
    const bubble = document.createElement("div");
    bubble.className = "assistant-bubble assistant-typing";
    bubble.innerHTML = "<span></span><span></span><span></span>";
    row.appendChild(bubble);
    return row;
  }

  function renderEmptyState() {
    const el = document.createElement("div");
    el.className = "assistant-empty";

    const iconBox = document.createElement("div");
    iconBox.className = "assistant-empty-icon";
    const iconImg = document.createElement("img");
    iconImg.src = "/static/assistant-icon.svg";
    iconImg.alt = "";
    iconImg.width = 38;
    iconImg.height = 38;
    iconBox.appendChild(iconImg);
    el.appendChild(iconBox);

    const title = document.createElement("div");
    title.className = "assistant-empty-title";
    title.textContent = projectName ? `Assistant — ${projectName}` : "Comment puis-je vous aider ?";

    const sub = document.createElement("div");
    sub.className = "assistant-empty-sub";
    sub.textContent = "Posez une question sur vos données SEO ou choisissez une suggestion.";

    const chips = document.createElement("div");
    chips.className = "assistant-suggestions";

    for (const q of getSuggestions()) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "assistant-suggestion";
      btn.textContent = q;
      btn.addEventListener("click", () => sendMessage(q));
      chips.appendChild(btn);
    }

    el.appendChild(title);
    el.appendChild(sub);
    el.appendChild(chips);
    return el;
  }

  function render() {
    messagesEl.innerHTML = "";
    liveTypingEl = null;

    if (history.length === 0) {
      messagesEl.appendChild(renderEmptyState());
    } else {
      for (const m of history) {
        messagesEl.appendChild(renderMessage(m.role, m.content));
      }
    }

    messagesEl.scrollTop = messagesEl.scrollHeight;

    if (meta && meta.configured === false) {
      footEl.textContent = "Assistant temporairement indisponible.";
    } else {
      footEl.textContent = "";
    }
  }

  // ── Status dot ────────────────────────────────────────────────────────────
  function setDot(status) {
    dotEl.classList.remove("ok", "err");
    if (status === "ok") dotEl.classList.add("ok");
    if (status === "err") dotEl.classList.add("err");
  }

  // ── Meta ──────────────────────────────────────────────────────────────────
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

    const configured = Boolean(meta.configured);

    metaEl.textContent = configured ? "En ligne" : "Indisponible";
    setDot(configured ? "ok" : "err");
    if (isOpen) render();
  }

  // ── Send ──────────────────────────────────────────────────────────────────
  async function sendMessage(text) {
    const message = String(text || "").trim();
    if (!message) return;

    const historyForServer = history.slice(-20);
    history.push({ role: "user", content: message });
    saveHistory();
    render();

    // Live typing indicator (not stored in history)
    liveTypingEl = renderTypingIndicator();
    messagesEl.appendChild(liveTypingEl);
    messagesEl.scrollTop = messagesEl.scrollHeight;

    input.value = "";
    input.focus();
    input.disabled = true;
    sendBtn.disabled = true;

    try {
      const resp = await fetch("/api/assistant/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json", "Accept": "application/json" },
        body: JSON.stringify({ message, history: historyForServer, context: ctx }),
      });
      const data = await resp.json();
      if (!data || data.ok !== true) {
        throw new Error((data && data.error) ? data.error : "Erreur inconnue");
      }
      history.push({ role: "assistant", content: String(data.reply || "").trim() || "—" });
      saveHistory();
      await refreshMeta();
      render();
    } catch (e) {
      const msg = (e && e.message) ? e.message : String(e || "Erreur");
      history.push({ role: "assistant", content: normalizeAssistantText(`Erreur: ${msg}`) });
      saveHistory();
      setDot("err");
      render();
    } finally {
      liveTypingEl = null;
      input.disabled = false;
      sendBtn.disabled = false;
      input.focus();
    }
  }

  // ── Events ────────────────────────────────────────────────────────────────
  toggle.addEventListener("click", () => setOpen(!isOpen));
  if (closeBtn) closeBtn.addEventListener("click", () => setOpen(false));
  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      if (!window.confirm("Effacer la conversation ?")) return;
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

  try { isOpen = localStorage.getItem(openKey) === "1"; } catch { isOpen = false; }
  setOpen(isOpen, { instant: true });
  refreshMeta();
})();
