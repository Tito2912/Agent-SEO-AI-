"""Lifecycle of the per-thread browser the crawler now keeps alive between pages.

Reusing a Chromium instead of launching one per page is worth 26% per page on the Render
worker, but it introduces two failure modes the old throwaway browser could not have:
a browser that breaks poisons every later page on that thread, and a browser kept forever
leaks 250-400 MB on a 2 GB box. These tests pin both, plus the invariant that matters for
Ahrefs parity: the browser is shared, the CONTEXT never is.

No Playwright here — the driver is faked, so this runs in CI where Chromium is absent.
"""

from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_browser_tests",
    REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py",
)
assert _SPEC and _SPEC.loader
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_browser_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)


class _FakeBrowser:
    def __init__(self, tracker: "_Tracker") -> None:
        self.tracker = tracker
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        self.tracker.closed += 1


class _Tracker:
    """Counts launches and closes so 'was the browser actually reused' is observable."""

    def __init__(self) -> None:
        self.launched = 0
        self.closed = 0
        self.stopped = 0
        self.browsers: list[_FakeBrowser] = []


class _FakeChromium:
    def __init__(self, tracker: _Tracker) -> None:
        self.tracker = tracker

    async def launch(self, **_kw: Any) -> _FakeBrowser:
        self.tracker.launched += 1
        b = _FakeBrowser(self.tracker)
        self.tracker.browsers.append(b)
        return b


class _FakePlaywright:
    def __init__(self, tracker: _Tracker) -> None:
        self.chromium = _FakeChromium(tracker)
        self.tracker = tracker

    async def stop(self) -> None:
        self.tracker.stopped += 1


@pytest.fixture()
def tracker(monkeypatch: pytest.MonkeyPatch) -> _Tracker:
    t = _Tracker()

    def _fake_ensure(self: Any) -> Any:
        if self.browser is None:
            self.pw = _FakePlaywright(t)
            self.browser = self.loop.run_until_complete(self.pw.chromium.launch())
            self.served = 0
        return self.browser

    monkeypatch.setattr(seo_audit._BrowserSession, "_ensure_browser", _fake_ensure)
    monkeypatch.setattr(seo_audit._BROWSER_TLS, "session", None, raising=False)
    yield t
    session = getattr(seo_audit._BROWSER_TLS, "session", None)
    if session is not None:
        session.close()
        seo_audit._BROWSER_TLS.session = None


def _noop(browser: Any):
    async def _run() -> None:
        return None

    return _run()


def test_the_browser_is_launched_once_and_reused_across_pages(tracker: _Tracker) -> None:
    for _ in range(10):
        seo_audit._run_in_browser(_noop, timeout_s=5)
    assert tracker.launched == 1, "each page launched its own browser — the reuse is not working"
    assert tracker.closed == 0


def test_the_browser_is_recycled_to_bound_memory(
    tracker: _Tracker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(seo_audit, "_BROWSER_RECYCLE_PAGES", 3)
    for _ in range(9):
        seo_audit._run_in_browser(_noop, timeout_s=5)
    assert tracker.launched == 3, "a browser kept forever leaks on a 2 GB worker"
    assert tracker.closed == 3
    assert tracker.stopped == 3, "the Playwright driver process must be stopped too, not just the browser"


def test_a_broken_browser_is_dropped_and_does_not_poison_the_next_page(tracker: _Tracker) -> None:
    def _boom(browser: Any):
        async def _run() -> None:
            raise RuntimeError("Target page, context or browser has been closed")

        return _run()

    with pytest.raises(RuntimeError):
        seo_audit._run_in_browser(_boom, timeout_s=5)
    assert tracker.closed == 1, "the broken browser was kept and would poison every later page"

    seo_audit._run_in_browser(_noop, timeout_s=5)
    assert tracker.launched == 2, "the thread never recovered a working browser"


def test_the_failing_page_still_reports_its_own_error(tracker: _Tracker) -> None:
    # The caller must see the original exception, not whatever the teardown raised: the crawl
    # records the error against this URL and carries on.
    def _boom(browser: Any):
        async def _run() -> None:
            raise ValueError("navigation refusée")

        return _run()

    with pytest.raises(ValueError, match="navigation refusée"):
        seo_audit._run_in_browser(_boom, timeout_s=5)


def test_each_thread_gets_its_own_browser(tracker: _Tracker) -> None:
    # Playwright objects are bound to the loop that created them, so a browser shared across
    # threads would be a use-after-loop crash rather than an optimisation.
    seen: list[int] = []
    lock = threading.Lock()

    def _work() -> None:
        seo_audit._run_in_browser(_noop, timeout_s=5)
        session = seo_audit._browser_session()
        with lock:
            seen.append(id(session))
        session.close()
        seo_audit._BROWSER_TLS.session = None

    threads = [threading.Thread(target=_work) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(seen)) == 3, "threads shared a browser session"


def test_recycling_can_be_disabled(tracker: _Tracker, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(seo_audit, "_BROWSER_RECYCLE_PAGES", 0)
    for _ in range(20):
        seo_audit._run_in_browser(_noop, timeout_s=5)
    assert tracker.launched == 1
    assert tracker.closed == 0
