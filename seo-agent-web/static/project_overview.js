(() => {
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
      const r = parseInt(h[0] + h[0], 16);
      const g = parseInt(h[1] + h[1], 16);
      const b = parseInt(h[2] + h[2], 16);
      return { r, g, b };
    }
    if (h.length === 6) {
      const r = parseInt(h.slice(0, 2), 16);
      const g = parseInt(h.slice(2, 4), 16);
      const b = parseInt(h.slice(4, 6), 16);
      return { r, g, b };
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

    // Grid
    ctx.strokeStyle = chartMuted;
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
      const y = padT + (innerH * i) / 4;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(w - padR, y);
      ctx.stroke();
    }

    // Frame (subtle)
    ctx.strokeStyle = border;
    ctx.lineWidth = 1;
    ctx.strokeRect(padL, padT, innerW, innerH);

    const xAt = (i) => padL + (innerW * i) / Math.max(1, points.length - 1);
    const yAt = (v) => padT + innerH * (1 - (v - vMin) / (vMax - vMin));

    // Area fill
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

    // Line
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

    // Labels
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

  function initTimeseriesWidgets() {
    const widgets = document.querySelectorAll(".ts-widget");
    for (const widget of widgets) {
      const id = widget.getAttribute("data-series-id") || "";
      const dataEl = id ? document.getElementById(id) : null;
      if (!dataEl) continue;

      let raw = null;
      try {
        raw = JSON.parse(dataEl.textContent || "[]");
      } catch {
        raw = null;
      }
      if (!Array.isArray(raw) || raw.length < 2) continue;

      const points = raw
        .filter((r) => r && typeof r === "object")
        .map((r) => ({
          date: String(r.date || ""),
          clicks: Number(r.clicks || 0),
          impressions: Number(r.impressions || 0),
          ctr: Number(r.ctr || 0),
          position: Number(r.position || 0),
        }))
        .filter((p) => p.date);
      if (points.length < 2) continue;

      const canvas = widget.querySelector("canvas.ts-canvas");
      if (!canvas) continue;

      const buttons = Array.from(widget.querySelectorAll("button.seg[data-metric]"));
      const defaultMetric = (widget.getAttribute("data-default-metric") || "").trim();
      let metric = defaultMetric || (buttons[0] ? String(buttons[0].dataset.metric || "") : "clicks") || "clicks";

      function setActive(next) {
        metric = next;
        for (const b of buttons) b.classList.toggle("active", String(b.dataset.metric || "") === metric);
        drawLineChart(canvas, points, metric);
      }

      for (const b of buttons) {
        b.addEventListener("click", () => {
          const next = String(b.dataset.metric || "").trim();
          if (!next) return;
          setActive(next);
        });
      }

      let raf = 0;
      const schedule = () => {
        if (raf) cancelAnimationFrame(raf);
        raf = requestAnimationFrame(() => drawLineChart(canvas, points, metric));
      };

      window.addEventListener("resize", schedule);
      if (typeof ResizeObserver !== "undefined") {
        const ro = new ResizeObserver(schedule);
        ro.observe(widget);
      }

      setActive(metric);
    }
  }

  function init() {
    initPeriodSelects();
    initTimeseriesWidgets();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

