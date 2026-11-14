"""Local recipe with fictional accounts and blocked external requests.

Run from the repository root with Playwright installed:
  python seo-agent-web/docs/ux-own-seo-binding/browser_smoke.py
Optional: CHROMIUM_EXECUTABLE, CHROMIUM_LIBRARY_PATH, BINDING_BROWSER_OUTPUT.
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
temporary = tempfile.TemporaryDirectory(prefix="noyaru-binding-browser-")
directory = Path(temporary.name)
os.environ.update(
    DATABASE_URL="sqlite:///" + str(directory / "bootstrap.db"),
    SEO_AGENT_SECRET_KEY="fictional-local-binding-session",
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
from backend.models import OwnSeoWorkspace, Project  # noqa: E402
from test_customer_activation import lab  # noqa: E402
from test_own_seo import own, s, m, PATH  # noqa: E402

output = Path(os.environ.get("BINDING_BROWSER_OUTPUT", str(directory / "evidence")))
output.mkdir(parents=True, exist_ok=True)
patch = pytest.MonkeyPatch()
laboratory = lab.__wrapped__(directory, patch)
x = own.__wrapped__(next(laboratory), patch)


@m.app.get("/_binding_fixture/{scene}")
def fixture(scene: str):
    if scene not in {"unbound", "bound", "paused", "long"}:
        raise HTTPException(404)
    with x["db"].session() as db:
        workspace = db.get(OwnSeoWorkspace, s.KEY)
        if workspace:
            db.delete(workspace)
        db.get(Project, x["project"].id).site_name = (
            "Projet Noyaru de démonstration " + "Exemple" * 35
            if scene == "long" else "Noyaru démo"
        )
        db.commit()
    if scene != "unbound":
        token = s.bind(x["owner"].id, x["project"].id, "", confirm=True)
        if scene == "paused":
            s.pause(x["owner"].id, token, True)
    return {"scene": scene}


with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
base = f"http://127.0.0.1:{port}"
patch.setenv("PUBLIC_BASE_URL", base)
server = uvicorn.Server(
    uvicorn.Config(m.app, host="127.0.0.1", port=port, lifespan="off", log_level="error")
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
evidence, errors, external = [], [], []


def no_overflow(page):
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth + 1")


try:
    with sync_playwright() as p:
        options = {"headless": True}
        if os.environ.get("CHROMIUM_EXECUTABLE"):
            options["executable_path"] = os.environ["CHROMIUM_EXECUTABLE"]
        browser = p.chromium.launch(**options)
        for lang in ("fr", "en"):
            context = browser.new_context(locale="fr-FR" if lang == "fr" else "en-US")
            context.add_cookies([{
                "name": auth.SESSION_COOKIE_NAME,
                "value": auth.make_session_token(
                    user_id=str(x["owner"].id), secret=os.environ["SEO_AGENT_SECRET_KEY"]
                ),
                "url": base,
            }])

            def route(request):
                if request.request.url.startswith(base + "/"):
                    request.continue_()
                else:
                    external.append(request.request.url.split("/", 3)[2])
                    request.abort()

            context.route("**/*", route)
            page = context.new_page()
            page.on("pageerror", lambda error: errors.append(str(error)))
            for scene in ("unbound", "bound", "paused", "long"):
                for width, height in ((1440, 1000), (1024, 900), (768, 1024), (390, 844), (360, 800)):
                    context.request.get(base + "/_binding_fixture/" + scene)
                    page.set_viewport_size({"width": width, "height": height})
                    response = page.goto(base + PATH + "?lang=" + lang)
                    assert response.status == 200
                    page.wait_for_load_state("networkidle")
                    no_overflow(page)
                    card = page.locator('section[aria-labelledby="own-seo-binding-title"]')
                    form = card.locator('form[action$="/bind"]')
                    if scene == "unbound":
                        assert form.is_visible()
                        assert card.locator("details").count() == 0
                    else:
                        assert not form.is_visible()
                        assert not card.locator("details").evaluate("e => e.open")
                        status = card.get_by_role("status")
                        assert status.is_visible()
                        assert ("Liaison active" if lang == "fr" else "Project linked") in status.inner_text()
                        card.locator('a[href="#own-seo-plan"]').click()
                        assert page.locator("#own-seo-plan").evaluate("e => e.getBoundingClientRect().top >= 60")
                        card.locator("summary").click()
                        assert form.is_visible()
                        no_overflow(page)
                        assert form.locator('[name="confirm"]').evaluate("e => e.required && !e.checked")
                        if width == 390 and scene == "bound":
                            page.evaluate("document.activeElement?.blur(); window.scrollTo(0, 0)")
                            page.screenshot(path=str(output / f"change-{lang}-{width}.png"), full_page=True)
                        card.locator("summary").click()
                    if scene == "paused":
                        assert page.locator('form[action$="/pause"] input[name="paused"]').input_value() == "false"
                    if width in (1440, 390) and scene in {"unbound", "bound"}:
                        page.evaluate("document.activeElement?.blur(); window.scrollTo(0, 0)")
                        page.screenshot(path=str(output / f"{scene}-{lang}-{width}.png"), full_page=True)
                    evidence.append({"lang": lang, "scene": scene, "width": width, "overflow": False})

            for width in (1440, 390):
                context.request.get(base + "/_binding_fixture/unbound")
                page.set_viewport_size({"width": width, "height": 900})
                page.goto(base + PATH + "?lang=" + lang)
                form = page.locator('form[action$="/bind"]')
                form.locator("button").click()
                assert form.locator('[name="confirm"]').evaluate("e => e.validity.valueMissing")
                form.locator('[name="confirm"]').check()
                with page.expect_navigation():
                    form.locator("button").click()
                assert "msg=bound" in page.url
                assert not page.locator('form[action$="/bind"]').is_visible()
                with x["db"].session() as db:
                    token = db.get(OwnSeoWorkspace, s.KEY).token
                page.reload()
                assert not page.locator('form[action$="/bind"]').is_visible()
                with x["db"].session() as db:
                    assert db.get(OwnSeoWorkspace, s.KEY).token == token
                evidence.append({"lang": lang, "flow": "native-bind-reload", "width": width, "required_confirmation": True, "binding_preserved": True})
            context.close()
        browser.close()
    assert not errors and not external, (errors, external)
    (output / "browser.json").write_text(json.dumps({
        "scope": "Local Noyaru routes and templates, fictional SQLite, all external requests blocked",
        "observations": evidence,
        "javascript_errors": errors,
        "external_requests": external,
        "production_session": False,
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"observations": len(evidence), "errors": errors, "output": str(output)}))
finally:
    server.should_exit = True
    thread.join(timeout=5)
    laboratory.close()
    patch.undo()
    temporary.cleanup()
