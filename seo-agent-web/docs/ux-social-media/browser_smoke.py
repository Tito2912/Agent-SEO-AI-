"""Actual private routes/MP4 player, fictional owner and simulated services only."""

import base64
from datetime import timedelta
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import uuid

WEB = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(WEB), str(WEB / "tests")]
temporary = tempfile.TemporaryDirectory(prefix="noyaru-media-browser-")
root = Path(temporary.name)
os.environ.update(
    DATABASE_URL="sqlite:///" + str(root / "bootstrap.db"),
    SEO_AGENT_SECRET_KEY="fictional-media-browser",
    SEO_AGENT_DISABLE_WORKER="true",
    SEO_AGENT_DISABLE_SCHEDULERS="true",
    SEO_AGENT_DISABLE_TELEGRAM_RECEIVER="true",
    AWS_EC2_METADATA_DISABLED="true",
    OPENAI_API_KEY="fictional-key",
)
import pytest  # noqa: E402
import uvicorn  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402
from backend import app, auth, own_social_media as m  # noqa: E402
from test_customer_activation import lab  # noqa: E402
from test_telegram_approvals import bot, activate  # noqa: E402

patch = pytest.MonkeyPatch()
fixture = lab.__wrapped__(root, patch)
x = bot.__wrapped__(next(fixture), patch)
activate(x)
patch.setattr(m.approvals(), "run_result", lambda *args: None)
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
base = f"http://127.0.0.1:{port}"
patch.setenv("PUBLIC_BASE_URL", base)
server = uvicorn.Server(
    uvicorn.Config(
        app.app, host="127.0.0.1", port=port, lifespan="off", log_level="error"
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
output = Path(
    os.environ.get("SOCIAL_BROWSER_OUTPUT", "/tmp/noyaru-media-browser-proof")
)
output.mkdir(parents=True, exist_ok=True)
proof = Path(os.environ.get("SOCIAL_MEDIA_OUTPUT", "/tmp/noyaru-media-proof"))
results = []
errors = []
external = []
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, executable_path=os.environ.get("CHROMIUM_EXECUTABLE")
        )
        for lang in ("fr", "en"):
            context = browser.new_context(
                locale="fr-FR" if lang == "fr" else "en-US", reduced_motion="reduce"
            )
            context.add_cookies(
                [
                    {
                        "name": auth.SESSION_COOKIE_NAME,
                        "value": auth.make_session_token(
                            user_id=x["owner"].id,
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
            ident = m.prepare(
                x["owner"].id, lang, "discovery", "freelance", uuid.uuid4().hex, True
            )
            data = (proof / f"{lang}.mp4").read_bytes()
            for scene in (
                "storyboard",
                "proposal",
                "queued",
                "running",
                "review",
                "approved",
                "stopped",
                "uncertain",
                "cancelled",
                "expired",
            ):
                if scene != "storyboard":
                    with x["db"].session() as db:
                        row = db.get(m.Media, ident)
                        row.state = "proposal" if scene == "expired" else scene
                        row.expires_at = (
                            m.s.clock() + timedelta(hours=24)
                            if scene != "expired"
                            else m.s.clock() - timedelta(seconds=1)
                        )
                        row.error_code = (
                            "voice_uncertain"
                            if scene == "uncertain"
                            else "render_failed"
                            if scene == "stopped"
                            else None
                        )
                        row.result = (
                            {
                                "video": base64.b64encode(data).decode(),
                                "sha256": m.render.sha(data),
                                "bytes": len(data),
                                "duration": 30,
                            }
                            if scene in {"review", "approved"}
                            else {}
                        )
                        db.commit()
                for width, height in (
                    (1440, 1000),
                    (768, 1024),
                    (390, 844),
                    (360, 800),
                ):
                    page.set_viewport_size({"width": width, "height": height})
                    response = page.goto(
                        base
                        + m.PATH
                        + "?lang="
                        + lang
                        + ("" if scene == "storyboard" else "&media=" + ident)
                    )
                    assert response.status == 200, (scene, response.status)
                    page.wait_for_load_state("networkidle")
                    assert page.evaluate(
                        "document.documentElement.scrollWidth <= innerWidth+1"
                    ), (scene, lang, width)
                    assert page.locator("img").evaluate_all(
                        "(els)=>els.every(x=>x.complete&&x.naturalWidth>0)"
                    )
                    if scene in {"review", "approved"}:
                        page.wait_for_function(
                            "document.querySelector('video').readyState >= 1 || document.querySelector('video').error",
                            timeout=12000,
                        )
                        assert page.locator("video").evaluate(
                            "(v)=>v.videoWidth===720&&v.videoHeight===1280&&Math.abs(v.duration-30)<.2"
                        )
                    if width == 390 and scene in {
                        "storyboard",
                        "review",
                        "expired",
                        "uncertain",
                    }:
                        page.screenshot(
                            path=str(output / f"{scene}-{lang}-{width}.png"),
                            full_page=True,
                        )
                    print(lang, scene, width, flush=True)
                    results.append(
                        {
                            "lang": lang,
                            "state": scene,
                            "width": width,
                            "status": 200,
                            "overflow": False,
                        }
                    )
            # Real GET navigation preserves language/context; POST cancellation retains CSRF protection.
            page.goto(
                base
                + "/settings/system/social?lang="
                + lang
                + "&network=tiktok&topic=score"
            )
            page.locator('a[href*="/social/media"]').click()
            assert "lang=" + lang in page.url
            page.locator('form[action*="/prepare"] input[name="confirm"]').check()
            page.locator('form[action*="/prepare"] button').click()
            page.wait_for_load_state("networkidle")
            assert "media=" in page.url
            page.locator("details").last.locator("summary").click()
            page.locator('form[action*="/cancel"] button').click()
            page.wait_for_load_state("networkidle")
            assert page.locator('form[action*="/cancel"]').count() == 0
            context.close()
            context = browser.new_context(
                java_script_enabled=False, viewport={"width": 360, "height": 800}
            )
            context.add_cookies(
                [
                    {
                        "name": auth.SESSION_COOKIE_NAME,
                        "value": auth.make_session_token(
                            user_id=x["owner"].id,
                            secret=os.environ["SEO_AGENT_SECRET_KEY"],
                        ),
                        "url": base,
                    }
                ]
            )
            context.route("**/*", route)
            page = context.new_page()
            assert page.goto(base + m.PATH + "?lang=" + lang).status == 200
            page.locator("details").first.locator("summary").click()
            assert page.locator('form[action*="/prepare"]').count() == 1
            results.append(
                {
                    "lang": lang,
                    "state": "no-javascript",
                    "width": 360,
                    "native_controls": True,
                }
            )
            context.close()
        browser.close()
    assert not errors and not external, (errors, external)
    (output / "browser-results.json").write_text(
        json.dumps(
            {
                "scope": "Local fictional data, simulated Telegram, real MP4 with synthetic silence; no paid voice or production access",
                "observations": results,
                "javascript_errors": errors,
                "external_requests": external,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"observations": len(results), "output": str(output)}))
finally:
    server.should_exit = True
    thread.join(timeout=5)
    fixture.close()
    patch.undo()
    temporary.cleanup()
