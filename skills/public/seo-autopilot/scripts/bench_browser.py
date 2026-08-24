#!/usr/bin/env python3
"""Measure what a crawled page actually costs, and how much of it is browser startup.

`_extract_page` in seo_audit.py pays three startups PER PAGE: a fresh asyncio loop
(`asyncio.run`), a fresh Playwright driver process (`async_playwright()` as a context
manager), and a fresh Chromium (`chromium.launch`). Whether that matters depends entirely on
the host: on a fast laptop with a warm page cache the relaunch costs ~6%, which is noise. On
the Render worker a page costs 6.26 s against 1.0 s locally, and that 6x gap is CPU, not
network — so the same relaunch may well dominate there.

Run this ON THE TARGET HOST before changing the crawler:

    python skills/public/seo-autopilot/scripts/bench_browser.py https://example.com/ [n]

It reproduces seo_audit's navigation exactly (domcontentloaded + bounded networkidle wait +
.content()) so the numbers are comparable to a real crawl, and reports three variants:

  relaunch  - what the crawler does today
  browser   - Playwright + Chromium started once, fresh context per page (same isolation:
              no cookie/storage bleed between pages, so Ahrefs parity is unaffected)
  context   - everything reused, page only (fastest, but pages share cookies/storage)
"""

from __future__ import annotations

import asyncio
import os
import statistics
import sys
import time

try:
    from playwright.async_api import async_playwright
except Exception as exc:  # pragma: no cover - diagnostic tool
    print(f"playwright unavailable: {type(exc).__name__}: {exc}")
    raise SystemExit(1)

NETWORKIDLE_MS = int(os.getenv("SEO_AGENT_NETWORKIDLE_MS", "8000"))
NAV_TIMEOUT_MS = 15_000


async def _navigate(pw_page, url: str) -> None:
    """Mirror seo_audit._extract_page's navigation, including the networkidle wait."""
    await pw_page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    if NETWORKIDLE_MS > 0:
        try:
            await pw_page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_MS)
        except Exception:
            pass
    await pw_page.content()


async def _bench_relaunch(url: str, n: int) -> list[float]:
    out: list[float] = []
    for _ in range(n):
        t0 = time.monotonic()
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx = await browser.new_context(ignore_https_errors=True)
            page = await ctx.new_page()
            try:
                await _navigate(page, url)
            finally:
                await page.close()
                await ctx.close()
                await browser.close()
        out.append(time.monotonic() - t0)
    return out


async def _bench_reuse(url: str, n: int, *, share_context: bool) -> list[float]:
    out: list[float] = []
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    shared = await browser.new_context(ignore_https_errors=True) if share_context else None
    try:
        for _ in range(n):
            t0 = time.monotonic()
            ctx = shared or await browser.new_context(ignore_https_errors=True)
            page = await ctx.new_page()
            try:
                await _navigate(page, url)
            finally:
                await page.close()
                if shared is None:
                    await ctx.close()
            out.append(time.monotonic() - t0)
    finally:
        if shared is not None:
            await shared.close()
        await browser.close()
        await pw.stop()
    return out


async def _main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    url = sys.argv[1]
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 6

    print(f"url={url}  n={n}  networkidle={NETWORKIDLE_MS}ms")
    results: dict[str, list[float]] = {}
    for name, coro in (
        ("relaunch", _bench_relaunch(url, n)),
        ("browser ", _bench_reuse(url, n, share_context=False)),
        ("context ", _bench_reuse(url, n, share_context=True)),
    ):
        try:
            times = await coro
        except Exception as exc:
            print(f"{name}  FAILED {type(exc).__name__}: {exc}")
            continue
        results[name.strip()] = times
        # The first page of every variant pays cold DNS/TLS; report it but exclude it from
        # the steady-state figure, which is what a 3000-page crawl actually experiences.
        steady = times[1:] or times
        print(
            f"{name}  mean {statistics.mean(times):5.2f}s  "
            f"steady {statistics.mean(steady):5.2f}s  "
            f"first {times[0]:5.2f}s  {[round(t, 2) for t in times]}"
        )

    base = results.get("relaunch")
    reuse = results.get("browser")
    if base and reuse:
        b = statistics.mean(base[1:] or base)
        r = statistics.mean(reuse[1:] or reuse)
        saved = (b - r) / b * 100 if b else 0.0
        print(f"\nreusing the browser saves {saved:.0f}% per page ({b:.2f}s -> {r:.2f}s)")
        print(f"a 3000-page crawl: {b * 3000 / 3600:.1f}h -> {r * 3000 / 3600:.1f}h")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
