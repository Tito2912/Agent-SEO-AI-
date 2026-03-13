(() => {
  const widgetStates = new WeakMap();

  function initPeriodSelects() {
    const selects = document.querySelectorAll(".js-period");
    for (const el of selects) {
      el.addEventListener("change", () => {
        const target = el.getAttribute("data-target") || "";
        const param = el.getAttribute("data-param") || "";
        const anchor = el.getAttribute("data-anchor") || "";
        if (!target || !param) return;
        const val = el.value;
        const url = new URL(target, window.location.origin);
        url.searchParams.set(param, String(val));
        window.location.href = url.toString() + (anchor ? "#" + anchor : "");
      });
    }
  }

  function cssVar(name, fallback) {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return v || fallback;
  }

  function hexToRgb(hex) {
    const v = String(hex || "").trim();
    if (!v.startsWith("#")) return null;
    const h = v.slice(1);
    if (h.length === 3) {
      return {
        r: parseInt(h[0] + h[0], 16),
        g: parseInt(h[1] + h[1], 16),
        b: parseInt(h[2] + h[2], 16),
      };
    }
    if (h.length === 6) {
      return {
        r: parseInt(h.slice(0, 2), 16),
        g: parseInt(h.slice(2, 4), 16),
        b: parseInt(h.slice(4, 6), 16),
      };
    }
    return null;
  }

  function withAlpha(color, alpha) {
    const rgb = hexToRgb(color);
    if (!rgb) return color;
    return `rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${alpha})`;
  }

  function formatDateLabel(isoDate) {
    const parts = String(isoDate || "").split("-");
    if (parts.length !== 3) return String(isoDate || "");
    return `${parts[2]}/${parts[1]}`;
  }

  function formatY(metric, value) {
    if (!Number.isFinite(value)) return "0";
    if (metric === "ctr") return `${(value * 100).toFixed(1)}%`;
    if (metric === "position") return value.toFixed(1);
    return String(Math.round(value));
  }

  function parseSeries(raw) {
    if (!Array.isArray(raw)) return [];
    return raw
      .filter((r) => r && typeof r === "object")
      .map((r) => ({
        date: String(r.date || ""),
        clicks: Number(r.clicks || 0),
        impressions: Number(r.impressions || 0),
        ctr: Number(r.ctr || 0),
        position: Number(r.position || 0),
      }))
      .filter((p) => p.date);
  }

  function readInlineSeries(widget) {
    const id = widget.getAttribute("data-series-id") || "";
    const dataEl = id ? document.getElementById(id) : null;
    if (!dataEl) return [];
    try {
      return parseSeries(JSON.parse(dataEl.textContent || "[]"));
    } catch {
      return [];
    }
  }

  function setWidgetMeta(widget, text) {
    const meta = widget.querySelector("[data-ts-meta]");
    if (meta) meta.textContent = String(text || "");
  }

  function setWidgetEmpty(widget, message) {
    const state = widgetStates.get(widget);
    if (!state) return;
    const empty = widget.querySelector(".ts-empty");
    if (empty) {
      empty.textContent = String(message || "Aucune donnée");
      empty.hidden = false;
    }
    state.canvas.hidden = true;
  }

  function setWidgetReady(widget) {
    const state = widgetStates.get(widget);
    if (!state) return;
    const empty = widget.querySelector(".ts-empty");
    if (empty) empty.hidden = true;
    state.canvas.hidden = false;
  }

  function drawLineChart(canvas, points, metric) {
    const w = canvas.clientWidth;
    const h = canvas.clientHeight;
    if (!w || !h) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(w * dpr);
    canvas.height = Math.floor(h * dpr);

    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);

    const padL = 44;
    const padR = 12;
    const padT = 12;
    const padB = 22;
    const innerW = Math.max(1, w - padL - padR);
    const innerH = Math.max(1, h - padT - padB);

    const rawValues = points.map((p) => Number(p[metric] ?? 0));
    const finite = rawValues.filter((x) => Number.isFinite(x));
    let vMin = finite.length ? Math.min(...finite) : 0;
    let vMax = finite.length ? Math.max(...finite) : 1;

    if (metric === "ctr") {
      vMin = 0;
      vMax = Math.min(1, Math.max(0.05, vMax * 1.1));
    } else if (metric === "clicks" || metric === "impressions") {
      vMin = 0;
      vMax = Math.max(1, vMax * 1.1);
    } else {
      vMin = Math.min(0, vMin);
      vMax = vMax === vMin ? vMin + 1 : vMax * 1.1;
    }
    if (vMax <= vMin) vMax = vMin + 1;

    const chartMuted = cssVar("--chart-muted", "rgba(255,255,255,.09)");
    const border = cssVar("--border", "rgba(255,255,255,.08)");
    const textMuted = cssVar("--muted", "rgba(255,255,255,.64)");

    const colors = {
      clicks: cssVar("--accent", "#d6b26a"),
      impressions: cssVar("--accent-2", "#f1d18a"),
      ctr: cssVar("--ok", "#4fa38a"),
      position: cssVar("--warn", "#c3a25a"),
    };
    const lineColor = colors[metric] || colors.clicks;

    ctx.strokeStyle = chartMuted;
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = padT + (innerH * i) / 4;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(w - padR, y);
      ctx.stroke();
    }

    ctx.strokeStyle = border;
    ctx.lineWidth = 1;
    ctx.strokeRect(padL, padT, innerW, innerH);

    const xAt = (i) => padL + (innerW * i) / Math.max(1, points.length - 1);
    const yAt = (v) => padT + innerH * (1 - (v - vMin) / (vMax - vMin));

    const grad = ctx.createLinearGradient(0, padT, 0, padT + innerH);
    grad.addColorStop(0, withAlpha(lineColor, 0.22));
    grad.addColorStop(1, withAlpha(lineColor, 0.02));
    ctx.fillStyle = grad;
    ctx.beginPath();
    for (let i = 0; i < points.length; i++) {
      const v = Number(points[i][metric] ?? 0);
      const x = xAt(i);
      const y = yAt(Number.isFinite(v) ? v : 0);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.lineTo(xAt(points.length - 1), padT + innerH);
    ctx.lineTo(xAt(0), padT + innerH);
    ctx.closePath();
    ctx.fill();

    ctx.strokeStyle = lineColor;
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (let i = 0; i < points.length; i++) {
      const v = Number(points[i][metric] ?? 0);
      const x = xAt(i);
      const y = yAt(Number.isFinite(v) ? v : 0);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();

    ctx.fillStyle = textMuted;
    ctx.font = "11px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace";
    ctx.textBaseline = "middle";
    ctx.textAlign = "right";
    ctx.fillText(formatY(metric, vMax), padL - 8, padT + 10);
    ctx.fillText(formatY(metric, vMin), padL - 8, padT + innerH);

    const startLabel = formatDateLabel(points[0].date);
    const endLabel = formatDateLabel(points[points.length - 1].date);
    ctx.textBaseline = "alphabetic";
    ctx.textAlign = "left";
    ctx.fillText(startLabel, padL, h - 6);
    ctx.textAlign = "right";
    ctx.fillText(endLabel, w - padR, h - 6);
  }

  function renderWidget(widget) {
    const state = widgetStates.get(widget);
    if (!state) return;
    const points = state.points;
    if (!Array.isArray(points) || points.length < 2) {
      setWidgetEmpty(widget, state.emptyMessage || "Aucune donnée disponible");
      return;
    }
    setWidgetReady(widget);
    drawLineChart(state.canvas, points, state.metric);
  }

  function setActiveMetric(widget, metric) {
    const state = widgetStates.get(widget);
    if (!state) return;
    state.metric = metric;
    for (const button of state.buttons) {
      button.classList.toggle("active", String(button.dataset.metric || "") === metric);
    }
    renderWidget(widget);
  }

  function buildLiveUrl(widget, days) {
    const raw = widget.getAttribute("data-live-url") || "";
    if (!raw) return "";
    const url = new URL(raw, window.location.origin);
    if (days) url.searchParams.set("days", String(days));
    return url.toString();
  }

  function liveMetaText(data) {
    const parts = [];
    if (data.source === "gsc" && data.property) parts.push(`Live · ${data.property}`);
    if (data.source === "bing" && data.site_url) parts.push(`Live · ${data.site_url}`);
    if (data.start_date && data.end_date) {
      const days = Number(data.days || 0);
      parts.push(`${data.start_date} → ${data.end_date}${days > 0 ? ` (${days}j)` : ""}`);
    }
    if (data.data_delay_hint) parts.push(String(data.data_delay_hint));
    return parts.join(" · ");
  }

  function errorLabel(data) {
    const reason = String((data && (data.error || data.reason)) || "").trim();
    const source = String((data && data.source) || "").trim();
    if (reason === "missing_credentials") return "Google Search Console non connecté pour ce projet.";
    if (reason === "missing_api_key") return "Clé Bing Webmaster API manquante.";
    if (reason === "site_not_found") return "Site Bing introuvable dans le compte connecté.";
    if (reason === "no_data") return source === "bing" ? "Aucune donnée Bing disponible sur la période." : "Aucune donnée GSC disponible sur la période.";
    if (!reason) return "Données indisponibles.";
    return reason;
  }

  async function fetchLiveSeries(widget, days) {
    const state = widgetStates.get(widget);
    if (!state) return;

    const liveUrl = buildLiveUrl(widget, days);
    if (!liveUrl) return;

    const requestId = (state.requestId || 0) + 1;
    state.requestId = requestId;

    if (!state.points.length) setWidgetEmpty(widget, "Chargement du graphique live…");
    setWidgetMeta(widget, "Chargement des données live…");

    let response;
    let data = null;
    try {
      response = await fetch(liveUrl, { headers: { Accept: "application/json" }, cache: "no-store" });
      data = await response.json();
    } catch (error) {
      if (state.requestId !== requestId) return;
      if (!state.points.length) setWidgetEmpty(widget, "Erreur de chargement du graphique.");
      setWidgetMeta(widget, `Erreur live · ${error instanceof Error ? error.message : "request_failed"}`);
      return;
    }

    if (state.requestId !== requestId) return;

    const nextPoints = parseSeries(data && data.daily);
    if (response.ok && nextPoints.length >= 2) {
      state.points = nextPoints;
      state.emptyMessage = "";
      const summaryBlock = widget.closest(".card")?.querySelector("[data-summary-block][data-summary-fallback='true']");
      if (summaryBlock) summaryBlock.hidden = true;
      setWidgetMeta(widget, liveMetaText(data || {}));
      renderWidget(widget);
      return;
    }

    if (!state.points.length) setWidgetEmpty(widget, errorLabel(data || {}));
    setWidgetMeta(widget, `Live indisponible · ${errorLabel(data || {})}`);
  }

  function initTimeseriesWidgets() {
    const widgets = document.querySelectorAll(".ts-widget");
    for (const widget of widgets) {
      const canvas = widget.querySelector("canvas.ts-canvas");
      if (!canvas) continue;

      const buttons = Array.from(widget.querySelectorAll("button.seg[data-metric]"));
      const defaultMetric = (widget.getAttribute("data-default-metric") || "").trim() || (buttons[0] ? String(buttons[0].dataset.metric || "") : "clicks");
      const inlinePoints = readInlineSeries(widget);

      const state = {
        canvas,
        buttons,
        metric: defaultMetric || "clicks",
        points: inlinePoints,
        emptyMessage: inlinePoints.length ? "" : "Chargement du graphique…",
        requestId: 0,
      };
      widgetStates.set(widget, state);

      for (const button of buttons) {
        button.addEventListener("click", () => {
          const metric = String(button.dataset.metric || "").trim();
          if (!metric) return;
          setActiveMetric(widget, metric);
        });
      }

      let raf = 0;
      const schedule = () => {
        if (raf) cancelAnimationFrame(raf);
        raf = requestAnimationFrame(() => renderWidget(widget));
      };

      window.addEventListener("resize", schedule);
      if (typeof ResizeObserver !== "undefined") {
        const ro = new ResizeObserver(schedule);
        ro.observe(widget);
      }

      if (inlinePoints.length >= 2) {
        setWidgetMeta(widget, "Dernier crawl enregistré.");
      }

      setActiveMetric(widget, state.metric);

      if (widget.getAttribute("data-live-url")) {
        fetchLiveSeries(widget);
      }
    }
  }

  function initTimeseriesPeriodSelects() {
    const selects = document.querySelectorAll(".js-ts-days");
    for (const select of selects) {
      select.addEventListener("change", () => {
        const widgetId = select.getAttribute("data-widget-id") || "";
        if (!widgetId) return;
        const widget = document.getElementById(widgetId);
        if (!widget) return;
        fetchLiveSeries(widget, select.value);
      });
    }
  }

  function init() {
    initPeriodSelects();
    initTimeseriesWidgets();
    initTimeseriesPeriodSelects();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
