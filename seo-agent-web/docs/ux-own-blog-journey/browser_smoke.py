"""Real local pages, fictional owner, simulated AI/GitHub. All external traffic blocked."""

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
temporary = tempfile.TemporaryDirectory(prefix="noyaru-blog-journey-browser-")
root = Path(temporary.name)
os.environ.update(
    DATABASE_URL="sqlite:///" + str(root / "bootstrap.db"),
    SEO_AGENT_SECRET_KEY="fictional-blog-journey-session",
    SEO_AGENT_DISABLE_WORKER="true",
    SEO_AGENT_DISABLE_SCHEDULERS="true",
    SEO_AGENT_DISABLE_TELEGRAM_RECEIVER="true",
    TELEGRAM_BOT_TOKEN="",
    TELEGRAM_CHAT_ID="",
    AWS_EC2_METADATA_DISABLED="true",
)
import pytest  # noqa: E402
import uvicorn  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402
from backend import app as m, auth, own_blog as b, own_writing as w  # noqa: E402
from backend.models import OwnSeoDraft, OwnSeoBrief  # noqa: E402
from test_customer_activation import lab  # noqa: E402
from test_own_seo import own  # noqa: E402
from test_own_writing import writing  # noqa: E402
from test_own_blog_journey import journey, write  # noqa: E402

patch = pytest.MonkeyPatch()
laboratory = lab.__wrapped__(root, patch)
x = journey.__wrapped__(
    writing.__wrapped__(own.__wrapped__(next(laboratory), patch), patch), patch
)
draft_id, ident = write(x)
output = Path(os.environ.get("JOURNEY_BROWSER_OUTPUT", str(root / "proof")))
output.mkdir(parents=True, exist_ok=True)


@m.app.get("/_journey_scene/{scene}")
def scene(scene: str):
    if scene not in {"topic", "draft", "checks", "published", "error"}:
        raise HTTPException(404)
    with x["db"].session() as db:
        draft = db.get(OwnSeoDraft, draft_id)
        publication = db.get(b.Publication, ident)
        db.get(OwnSeoBrief, x["brief"]).status = (
            "prepared" if scene == "topic" else "reviewed"
        )
        draft.status = "cancelled" if scene == "topic" else "draft"
        publication.state = {
            "topic": "cancelled",
            "draft": "proposal",
            "checks": "reviewing",
            "published": "published",
            "error": "needs_review",
        }[scene]
        publication.error_code = "base_changed" if scene == "error" else None
        publication.receipt = (
            {
                "number": 9,
                "url": "https://github.com/example/local/pull/9",
                "observed": [{"url": publication.proposal["files"][0]["url"]}],
                "observed_at": "2026-11-14T16:00:00+00:00",
            }
            if scene in {"checks", "published"}
            else {}
        )
        db.commit()
    return {"scene": scene}


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
observations = []
errors = []
external = []
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, executable_path=os.environ.get("CHROMIUM_EXECUTABLE")
        )
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

            def route(r):
                if r.request.url.startswith(base + "/"):
                    r.continue_()
                else:
                    external.append(r.request.url.split("/", 3)[2])
                    r.abort()

            context.route("**/*", route)
            page = context.new_page()
            page.on("pageerror", lambda e: errors.append(str(e)))
            for scene_name in ("topic", "draft", "checks", "published", "error"):
                context.request.get(base + "/_journey_scene/" + scene_name)
                path = (
                    w.PATH + "?brief=" + x["brief"]
                    if scene_name in {"topic", "draft"}
                    else b.PATH + "?publication=" + ident
                )
                for width, height in (
                    (1440, 1000),
                    (768, 1024),
                    (390, 844),
                    (360, 800),
                ):
                    page.set_viewport_size({"width": width, "height": height})
                    response = page.goto(base + path + "&lang=" + lang)
                    assert response.status == 200
                    page.wait_for_load_state("networkidle")
                    assert page.evaluate(
                        "document.documentElement.scrollWidth<=innerWidth+1"
                    )
                    assert page.locator('.own-steps [aria-current="step"]').count() == 1
                    if scene_name == "draft":
                        page.locator(".own-seo > p > a.primary").click()
                        assert (
                            "publication=" + ident in page.url
                            and "#publication-" + ident in page.url
                        )
                        page.get_by_role(
                            "link",
                            name="Read the article and its sources"
                            if lang == "en"
                            else "Relire l’article et ses sources",
                        ).click()
                        assert (
                            "brief=" + x["brief"] in page.url
                            and "#draft-" + draft_id in page.url
                        )
                    if width in (1440, 390):
                        page.evaluate("scrollTo(0,0)")
                        page.screenshot(
                            path=str(output / f"{scene_name}-{lang}-{width}.png"),
                            full_page=True,
                        )
                    observations.append(
                        {
                            "lang": lang,
                            "scene": scene_name,
                            "width": width,
                            "status": 200,
                            "overflow": False,
                        }
                    )
            # Fast-forward the real read-only polling and observe the new terminal state.
            context.request.get(base + "/_journey_scene/checks")
            page.goto(base + b.PATH + "?publication=" + ident + "&lang=" + lang)
            page.clock.install()
            context.request.get(base + "/_journey_scene/published")
            page.clock.fast_forward(11000)
            page.locator('[data-publication="published"]').wait_for()
            observations.append(
                {"lang": lang, "flow": "read-only-auto-refresh", "published": True}
            )
            context.close()
        browser.close()
    assert not errors and not external, (errors, external)
    (output / "browser-results.json").write_text(
        json.dumps(
            {
                "scope": "Local fictional accounts; real routes and Chromium; no production session or external requests",
                "observations": observations,
                "javascript_errors": errors,
                "external_requests": external,
                "simulated_ai_calls": len(x["calls"]),
                "simulated_github_writes": len(x["remote"].writes),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"observations": len(observations), "output": str(output)}))
finally:
    server.should_exit = True
    thread.join(timeout=5)
    laboratory.close()
    patch.undo()
    temporary.cleanup()
