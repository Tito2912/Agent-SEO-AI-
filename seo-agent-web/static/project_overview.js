(() => {
  const widgetStates = new WeakMap();
  const miniStates = new WeakMap();
  const METRIC_CONFIG = {
    clicks: { colorVar: "--ts-clicks", fallback: "#6f8ff2" },
    impressions: { colorVar: "--ts-impressions", fallback: "#7352d9" },
    ctr: { colorVar: "--ts-ctr", fallback: "#5fa99a" },
    position: { colorVar: "--ts-position", fallback: "#b89454" },
  };

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

  function formatMetricValue(metric, value) {
    if (!Number.isFinite(value)) return "—";
    if (metric === "ctr") return `${(value * 100).toFixed(1)}%`;
    if (metric === "position") return value > 0 ? value.toFixed(1) : "—";
    return new Intl.NumberFormat("fr-FR", {
      notation: value >= 1000 ? "compact" : "standard",
      maximumFractionDigits: value >= 1000 ? 1 : 0,
    }).format(Math.round(value));
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatCompactInt(value) {
    if (!Number.isFinite(value)) return "—";
    const v = Math.round(value);
    return new Intl.NumberFormat("fr-FR", {
      notation: v >= 1000 ? "compact" : "standard",
      maximumFractionDigits: v >= 1000 ? 1 : 0,
    }).format(v);
  }

  function formatCtrFraction(value) {
    if (!Number.isFinite(value)) return "—";
    return `${(value * 100).toFixed(1)}%`;
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

  function computeTotals(points) {
    if (!Array.isArray(points) || !points.length) {
      return { clicks: NaN, impressions: NaN, ctr: NaN, position: NaN };
    }
    let clicks = 0;
    let impressions = 0;
    let weightedPosition = 0;
    let weightedPositionImpressions = 0;
    let fallbackPosition = 0;
    let fallbackPositionCount = 0;

    for (const point of points) {
      const pointClicks = Number(point.clicks || 0);
      const pointImpressions = Number(point.impressions || 0);
      const pointPosition = Number(point.position || 0);
      if (Number.isFinite(pointClicks)) clicks += pointClicks;
      if (Number.isFinite(pointImpressions)) impressions += pointImpressions;
      if (Number.isFinite(pointPosition) && pointPosition > 0) {
        if (Number.isFinite(pointImpressions) && pointImpressions > 0) {
          weightedPosition += pointPosition * pointImpressions;
          weightedPositionImpressions += pointImpressions;
        } else {
          fallbackPosition += pointPosition;
          fallbackPositionCount += 1;
        }
      }
    }

    return {
      clicks,
      impressions,
      ctr: impressions > 0 ? clicks / impressions : 0,
      position: weightedPositionImpressions > 0
        ? weightedPosition / weightedPositionImpressions
        : (fallbackPositionCount > 0 ? fallbackPosition / fallbackPositionCount : NaN),
    };
  }

  function updateMetricCards(widget, totals) {
    const cards = widget.querySelectorAll(".ts-metric[data-metric]");
    for (const card of cards) {
      const metric = String(card.getAttribute("data-metric") || "").trim();
      if (!metric) continue;
      const valueEl = card.querySelector(`[data-value-for="${metric}"]`);
      if (!valueEl) continue;
      valueEl.textContent = formatMetricValue(metric, Number(totals?.[metric]));
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

  function metricColor(metric) {
    const config = METRIC_CONFIG[metric] || METRIC_CONFIG.clicks;
    return cssVar(config.colorVar, config.fallback);
  }

  function computeMetricScale(metric, points) {
    const rawValues = points.map((point) => Number(point[metric] ?? 0));
    const finite = rawValues.filter((value) => Number.isFinite(value));
    let min = finite.length ? Math.min(...finite) : 0;
    let max = finite.length ? Math.max(...finite) : 1;

    if (metric === "ctr") {
      min = 0;
      max = Math.min(1, Math.max(0.05, max * 1.1));
    } else if (metric === "clicks" || metric === "impressions" || metric === "position") {
      min = 0;
      max = Math.max(1, max * 1.1);
    } else {
      min = Math.min(0, min);
      max = max === min ? min + 1 : max * 1.1;
    }

    if (max <= min) max = min + 1;
    return { min, max };
  }

  function xTicks(points, count) {
    if (!points.length) return [];
    if (points.length <= count) return points.map((point, index) => ({ point, index }));
    const indexes = [];
    for (let i = 0; i < count; i += 1) {
      indexes.push(Math.round((i * (points.length - 1)) / Math.max(1, count - 1)));
    }
    return Array.from(new Set(indexes)).map((index) => ({ point: points[index], index }));
  }

  function drawAxisLabels(ctx, side, metric, scale, width, padL, padR, padT, innerH, textMuted) {
    ctx.fillStyle = textMuted;
    ctx.font = "11px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace";
    ctx.textBaseline = "middle";
    ctx.textAlign = side === "left" ? "right" : "left";
    const x = side === "left" ? padL - 8 : width - padR + 8;
    ctx.fillText(formatY(metric, scale.max), x, padT + 10);
    ctx.fillText(formatY(metric, scale.min), x, padT + innerH);
  }

  function drawMetricLine(ctx, points, metric, xAt, yAt) {
    ctx.strokeStyle = metricColor(metric);
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (let i = 0; i < points.length; i += 1) {
      const value = Number(points[i][metric] ?? 0);
      const x = xAt(i);
      const y = yAt(Number.isFinite(value) ? value : 0);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  function drawLineChart(canvas, points, metrics) {
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    if (!width || !height) return;

    const dpr = window.devicePixelRatio || 1;
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);

    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    const visibleMetrics = (Array.isArray(metrics) ? metrics : [metrics]).filter(Boolean);
    const primaryMetric = visibleMetrics[0] || "clicks";
    const secondaryMetric = visibleMetrics[1] || null;
    const primaryScale = computeMetricScale(primaryMetric, points);
    const secondaryScale = secondaryMetric ? computeMetricScale(secondaryMetric, points) : null;

    const padL = 54;
    const padR = secondaryScale ? 54 : 16;
    const padT = 16;
    const padB = 30;
    const innerW = Math.max(1, width - padL - padR);
    const innerH = Math.max(1, height - padT - padB);

    const chartMuted = cssVar("--chart-muted", "rgba(255,255,255,.09)");
    const border = cssVar("--border", "rgba(255,255,255,.08)");
    const textMuted = cssVar("--muted", "rgba(255,255,255,.64)");

    ctx.strokeStyle = chartMuted;
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i += 1) {
      const y = padT + (innerH * i) / 4;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(width - padR, y);
      ctx.stroke();
    }

    ctx.strokeStyle = border;
    ctx.lineWidth = 1;
    ctx.strokeRect(padL, padT, innerW, innerH);

    const xAt = (index) => padL + (innerW * index) / Math.max(1, points.length - 1);
    const primaryY = (value) => padT + innerH * (1 - (value - primaryScale.min) / (primaryScale.max - primaryScale.min));

    drawMetricLine(ctx, points, primaryMetric, xAt, primaryY);
    drawAxisLabels(ctx, "left", primaryMetric, primaryScale, width, padL, padR, padT, innerH, textMuted);

    if (secondaryMetric && secondaryScale) {
      const secondaryY = (value) => padT + innerH * (1 - (value - secondaryScale.min) / (secondaryScale.max - secondaryScale.min));
      drawMetricLine(ctx, points, secondaryMetric, xAt, secondaryY);
      drawAxisLabels(ctx, "right", secondaryMetric, secondaryScale, width, padL, padR, padT, innerH, textMuted);
    }

    ctx.fillStyle = textMuted;
    ctx.font = "11px ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace";
    ctx.textBaseline = "alphabetic";
    for (const tick of xTicks(points, 6)) {
      const x = xAt(tick.index);
      ctx.textAlign = tick.index === 0 ? "left" : (tick.index === points.length - 1 ? "right" : "center");
      ctx.fillText(formatDateLabel(tick.point.date), x, height - 8);
    }
  }

  function syncMetricButtons(widget) {
    const state = widgetStates.get(widget);
    if (!state) return;
    for (const button of state.buttons) {
      const metric = String(button.dataset.metric || "").trim();
      const active = state.metrics.includes(metric);
      button.classList.toggle("active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    }
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
    drawLineChart(state.canvas, points, state.metrics);
  }

  function toggleMetric(widget, metric) {
    const state = widgetStates.get(widget);
    if (!state || !metric) return;
    const isActive = state.metrics.includes(metric);
    let nextMetrics = [...state.metrics];

    if (isActive) {
      if (nextMetrics.length === 1) return;
      nextMetrics = nextMetrics.filter((item) => item !== metric);
    } else {
      const maxMetrics = Math.max(1, Number(state.maxMetrics || 2));
      const kept = nextMetrics.length >= maxMetrics ? nextMetrics.slice(nextMetrics.length - (maxMetrics - 1)) : nextMetrics;
      nextMetrics = [...kept.filter((item) => item !== metric), metric];
    }

    state.metrics = state.metricOrder.filter((item) => nextMetrics.includes(item));
    syncMetricButtons(widget);
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
    if (reason === "missing_credentials") {
      return source === "bing"
        ? "Bing Webmaster Tools non connecté."
        : "Google Search Console non connecté pour ce projet.";
    }
    if (reason === "oauth_not_configured") return "OAuth Google non configuré (client id/secret).";
    if (reason === "oauth_invalid_client") return "OAuth Google invalide (client id/secret).";
    if (reason === "credentials_file_not_found") return "Credentials GSC introuvables sur le serveur.";
    if (reason === "oauth_invalid_grant") return "Accès Google révoqué. Reconnecte Google.";
    if (reason === "missing_api_key") return "Clé Bing Webmaster API manquante.";
    if (reason === "site_not_found") return "Site Bing introuvable dans le compte connecté.";
    if (reason === "no_data") return source === "bing" ? "Aucune donnée Bing disponible sur la période." : "Aucune donnée GSC disponible sur la période.";
    if (!reason) return "Données indisponibles.";
    return reason;
  }

  function miniDays(widgetId) {
    if (!widgetId) return 28;
    const select = document.querySelector(`.js-ts-days[data-widget-id="${CSS.escape(widgetId)}"]`);
    const days = Number(select && select.value);
    return Number.isFinite(days) && days > 0 ? days : 28;
  }

  function miniUpdateLink(el, dim) {
    const link = el.querySelector('a[href^="/projects/"][href*="/performance"]');
    if (!link) return;
    try {
      const url = new URL(link.getAttribute("href") || "", window.location.origin);
      url.searchParams.set("dim", String(dim || "query"));
      link.setAttribute("href", url.pathname + url.search);
    } catch {
      // ignore
    }
  }

  function miniRender(el, payload, dim) {
    const body = el.querySelector("[data-mini-body]");
    if (!body) return;
    const meta = el.querySelector("[data-mini-meta]");
    if (meta) meta.textContent = payload && payload.ok ? liveMetaText(payload) : `Live indisponible · ${errorLabel(payload || {})}`;

    if (!payload || !payload.ok) {
      body.innerHTML = `<tr><td colspan="5" class="muted">${escapeHtml(errorLabel(payload || {}))}</td></tr>`;
      return;
    }

    const items = Array.isArray(payload.items) ? payload.items : [];
    if (!items.length) {
      body.innerHTML = `<tr><td colspan="5" class="muted">Aucune donnée sur la période.</td></tr>`;
      return;
    }

    const dimKey = String(dim || "query");
    body.innerHTML = items
      .map((it) => {
        const key = String(it.keyword || "");
        const display = key.length > 72 ? `${key.slice(0, 69)}…` : key;
        const keyCell = dimKey === "page" && key.startsWith("http")
          ? `<a href="${escapeHtml(key)}" target="_blank" rel="noopener">${escapeHtml(display)}</a>`
          : `<span class="mono">${escapeHtml(display)}</span>`;
        const clicks = Number(it.clicks || 0);
        const impressions = Number(it.impressions || 0);
        const ctr = Number(it.ctr || 0);
        const position = Number(it.position || 0);
        return `<tr>
          <td>${keyCell}</td>
          <td class="num mono">${escapeHtml(formatCompactInt(clicks))}</td>
          <td class="num mono">${escapeHtml(formatCompactInt(impressions))}</td>
          <td class="num mono">${escapeHtml(formatCtrFraction(ctr))}</td>
          <td class="num mono">${position > 0 ? escapeHtml(position.toFixed(1)) : "—"}</td>
        </tr>`;
      })
      .join("");
  }

  async function miniFetch(el) {
    const state = miniStates.get(el);
    if (!state) return;
    const requestId = (state.requestId || 0) + 1;
    state.requestId = requestId;

    const body = el.querySelector("[data-mini-body]");
    if (body) body.innerHTML = `<tr><td colspan="5" class="muted">Chargement…</td></tr>`;

    const days = miniDays(state.daysWidgetId);
    const url = new URL(`/api/projects/${state.slug}/search-items`, window.location.origin);
    url.searchParams.set("source", state.source);
    url.searchParams.set("dim", state.dim);
    url.searchParams.set("days", String(days));
    url.searchParams.set("limit", String(state.limit));

    let resp;
    let payload;
    try {
      resp = await fetch(url.toString(), { headers: { Accept: "application/json" }, cache: "no-store" });
      payload = await resp.json();
    } catch (error) {
      if (state.requestId !== requestId) return;
      miniRender(el, { ok: false, source: state.source, error: error instanceof Error ? error.message : "request_failed" }, state.dim);
      return;
    }

    if (state.requestId !== requestId) return;
    miniRender(el, payload, state.dim);
  }

  function miniSetDim(el, dim) {
    const state = miniStates.get(el);
    if (!state) return;
    state.dim = dim;
    miniUpdateLink(el, dim);
    for (const btn of el.querySelectorAll("button[data-dim]")) {
      const isActive = String(btn.getAttribute("data-dim") || "") === dim;
      btn.classList.toggle("active", isActive);
      btn.setAttribute("aria-pressed", isActive ? "true" : "false");
    }
    miniFetch(el);
  }

  function miniRefreshForWidgetId(widgetId) {
    if (!widgetId) return;
    const widgets = document.querySelectorAll(`[data-search-mini][data-days-widget="${CSS.escape(widgetId)}"]`);
    for (const el of widgets) miniFetch(el);
  }

  function initMiniTables() {
    const widgets = document.querySelectorAll("[data-search-mini]");
    for (const el of widgets) {
      const source = String(el.getAttribute("data-source") || "").trim();
      const slug = String(el.getAttribute("data-slug") || "").trim();
      const daysWidgetId = String(el.getAttribute("data-days-widget") || "").trim();
      const limit = Math.max(1, Math.min(50, Number(el.getAttribute("data-limit") || 12) || 12));
      if (!source || !slug || !daysWidgetId) continue;

      const state = { source, slug, daysWidgetId, limit, dim: "query", requestId: 0 };
      miniStates.set(el, state);
      miniUpdateLink(el, state.dim);

      for (const btn of el.querySelectorAll("button[data-dim]")) {
        btn.addEventListener("click", () => {
          const nextDim = String(btn.getAttribute("data-dim") || "").trim();
          if (!nextDim) return;
          miniSetDim(el, nextDim);
        });
      }

      miniFetch(el);
    }
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
      state.totals = computeTotals(nextPoints);
      state.emptyMessage = "";
      const summaryBlock = widget.closest(".card")?.querySelector("[data-summary-block][data-summary-fallback='true']");
      if (summaryBlock) summaryBlock.hidden = true;
      updateMetricCards(widget, state.totals);
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

      const buttons = Array.from(widget.querySelectorAll("button[data-metric]"));
      const metricOrder = buttons.map((button) => String(button.dataset.metric || "").trim()).filter(Boolean);
      const maxMetrics = Math.max(1, Number(widget.getAttribute("data-max-metrics") || 2));
      const defaultMetrics = String(widget.getAttribute("data-default-metrics") || "")
        .split(",")
        .map((metric) => metric.trim())
        .filter((metric) => metricOrder.includes(metric))
        .slice(0, maxMetrics);
      const inlinePoints = readInlineSeries(widget);
      const inlineTotals = computeTotals(inlinePoints);

      const state = {
        canvas,
        buttons,
        metricOrder,
        maxMetrics,
        metrics: defaultMetrics.length ? defaultMetrics : metricOrder.slice(0, Math.min(2, metricOrder.length)),
        points: inlinePoints,
        totals: inlineTotals,
        emptyMessage: inlinePoints.length ? "" : "Chargement du graphique…",
        requestId: 0,
      };
      widgetStates.set(widget, state);
      if (inlinePoints.length) updateMetricCards(widget, inlineTotals);

      for (const button of buttons) {
        button.addEventListener("click", () => {
          const metric = String(button.dataset.metric || "").trim();
          if (!metric) return;
          toggleMetric(widget, metric);
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

      syncMetricButtons(widget);
      renderWidget(widget);

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
        miniRefreshForWidgetId(widgetId);
      });
    }
  }

  function init() {
    initPeriodSelects();
    initTimeseriesWidgets();
    initTimeseriesPeriodSelects();
    initMiniTables();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
