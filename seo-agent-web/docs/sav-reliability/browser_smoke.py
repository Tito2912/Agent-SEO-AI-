"""Local browser recipe: fresh fictional DB, simulated providers, no production session.

Run from the repository root with the dev dependencies and Playwright Chromium:
  python seo-agent-web/docs/sav-reliability/browser_smoke.py
Optional: CHROMIUM_EXECUTABLE, CHROMIUM_LIBRARY_PATH, SAV_BROWSER_OUTPUT.
"""

import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time

WEB = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(WEB), str(WEB / "tests")]
temporary = tempfile.TemporaryDirectory(prefix="noyaru-sav-browser-")
directory = Path(temporary.name)
os.environ.update(
    DATABASE_URL="sqlite:///" + str(directory / "bootstrap.db"),
    SEO_AGENT_SECRET_KEY="fictional-browser-session-not-a-production-secret",
    SEO_AGENT_DISABLE_WORKER="true",
    SEO_AGENT_DISABLE_SCHEDULERS="true",
    SEO_AGENT_DISABLE_TELEGRAM_RECEIVER="true",
    TELEGRAM_BOT_TOKEN="",
    TELEGRAM_CHAT_ID="",
)

import pytest  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402
import uvicorn  # noqa: E402
from backend import auth  # noqa: E402
from test_customer_activation import lab  # noqa: E402
from test_own_support import sav, c, m, cfg  # noqa: E402
from test_sav_reliability import ignored  # noqa: E402

output = Path(os.environ.get("SAV_BROWSER_OUTPUT", str(directory / "evidence")))
output.mkdir(parents=True, exist_ok=True)
patch = pytest.MonkeyPatch()
laboratory = lab.__wrapped__(directory, patch)
x = sav.__wrapped__(next(laboratory), patch)
patch.setenv("MAIL_API_PROVIDER", "brevo")
ident = ignored(x)
baseline = cfg(x)


@m.app.get("/_sav_fixture/{state}")
def state(state: str):
    if state not in {"ignored", "blocked", "reconnect", "long"}:
        raise HTTPException(404)
    with x["db"].session() as db:
        channel = db.get(c.Channel, c.KEY)
        channel.active = state != "reconnect"
        channel.error_code = (
            "gmail_reconnect_required" if state == "reconnect" else None
        )
        channel.lease_token = channel.lease_until = None
        channel.cursor = baseline.cursor
        channel.pending = {}
        row = db.get(c.Ticket, ident)
        row.state, row.reason = "ignored", "outside_label"
        row.payload_encrypted, row.input_hash = c.seal({}), c.s.digest({})
        row.revision += 1
        if state == "long":
            row.state, row.reason = "received", None
            payload = {
                "subject": "Un titre de demande très long · " + "Exemple" * 20,
                "from": "client-" + "x" * 55 + "@example.invalid",
                "text": "Bonjour, comment lancer une analyse ?",
            }
            row.payload_encrypted, row.input_hash = c.seal(payload), c.s.digest(payload)
        db.commit()
    x["remote"].messages["a1"]["labelIds"] = [
        "Label_SAV",
        "SPAM" if state == "blocked" else "INBOX",
    ]
    return {
        "ai": len(x["remote"].ai),
        "sends": len(x["remote"].sends),
        "reads": len(x["remote"].calls),
    }


with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
base = f"http://127.0.0.1:{port}"
patch.setenv("PUBLIC_BASE_URL", base)
server = uvicorn.Server(
    uvicorn.Config(
        m.app, host="127.0.0.1", port=port, lifespan="off", log_level="error"
    )
)
thread = threading.Thread(target=server.run, daemon=True)
thread.start()
for _ in range(100):
    if server.started:
        break
    time.sleep(0.05)
assert server.started

if os.environ.get("CHROMIUM_LIBRARY_PATH"):
    os.environ["LD_LIBRARY_PATH"] = os.environ["CHROMIUM_LIBRARY_PATH"]
evidence = []
errors = []
try:
    with sync_playwright() as p:
        options = {"headless": True}
        if os.environ.get("CHROMIUM_EXECUTABLE"):
            options["executable_path"] = os.environ["CHROMIUM_EXECUTABLE"]
        browser = p.chromium.launch(**options)
        for lang in ("fr", "en"):
            context = browser.new_context(locale="fr-FR" if lang == "fr" else "en-US")
            context.add_cookies(
                [
                    {
                        "name": auth.SESSION_COOKIE_NAME,
                        "value": auth.make_session_token(
                            user_id=str(x["owner"].id),
                            secret=os.environ["SEO_AGENT_SECRET_KEY"],
                        ),
                        "url": base,
                    }
                ]
            )
            context.route(
                "**/*",
                lambda route: (
                    route.continue_()
                    if route.request.url.startswith(base + "/")
                    else route.abort()
                ),
            )
            page = context.new_page()
            page.on("pageerror", lambda error: errors.append(str(error)))
            for scene in ("ignored", "reconnect", "long"):
                for width, height in (
                    (1440, 1000),
                    (1024, 900),
                    (768, 1024),
                    (390, 844),
                    (360, 800),
                ):
                    context.request.get(base + "/_sav_fixture/" + scene)
                    page.set_viewport_size({"width": width, "height": height})
                    response = page.goto(base + c.PATH + "?lang=" + lang)
                    assert response.status == 200
                    page.wait_for_load_state("networkidle")
                    overflow = page.evaluate(
                        "document.documentElement.scrollWidth > innerWidth + 1"
                    )
                    assert not overflow, (lang, scene, width)
                    assert all(
                        secret not in page.content()
                        for secret in (
                            "simulated-refresh",
                            "simulated-access",
                            "simulated-client-secret",
                        )
                    )
                    if scene == "reconnect":
                        assert page.locator('form[action$="/oauth/start"]').evaluate(
                            "e => e.closest('details').open"
                        )
                        assert (
                            page.locator('form[action$="/channel/activate"]').count()
                            == 0
                        )
                    if width in (1440, 390):
                        page.screenshot(
                            path=str(output / f"{scene}-{lang}-{width}.png"),
                            full_page=True,
                        )
                    evidence.append(
                        {
                            "lang": lang,
                            "scene": scene,
                            "width": width,
                            "overflow": overflow,
                        }
                    )

            for scene in ("blocked", "ignored"):
                context.request.get(base + "/_sav_fixture/" + scene)
                page.set_viewport_size({"width": 390, "height": 844})
                page.goto(base + c.PATH + "?lang=" + lang)
                form = page.locator('form[action$="/recheck"]')
                form.locator("button").click()
                assert form.locator('[name="confirm"]').evaluate(
                    "e => e.validity.valueMissing"
                )
                form.locator('[name="confirm"]').check()
                with page.expect_navigation():
                    form.locator("button").click()
                if scene == "blocked":
                    assert page.locator('[data-support="ignored"]').count() == 1
                    assert "outside_label" in page.url
                else:
                    assert page.locator('[data-support="received"]').count() == 1
                    assert page.locator('form[action$="/recheck"]').count() == 0
                assert not x["remote"].ai and not x["remote"].sends
                evidence.append(
                    {
                        "lang": lang,
                        "flow": scene,
                        "consent_required": True,
                        "no_ai_or_email": True,
                    }
                )
            context.close()
        browser.close()
    assert not errors, errors
    (output / "browser.json").write_text(
        json.dumps(
            {
                "scope": "Local actual Noyaru routes/templates; fictional SQLite and simulated providers",
                "observations": evidence,
                "javascript_errors": errors,
                "real_google_or_telegram_or_email": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {"observations": len(evidence), "errors": errors, "output": str(output)}
        )
    )
finally:
    server.should_exit = True
    thread.join(timeout=5)
    laboratory.close()
    patch.undo()
    temporary.cleanup()
