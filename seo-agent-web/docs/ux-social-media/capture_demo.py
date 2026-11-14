"""Release captures of real local routes with fictional data. Never production."""

import hashlib
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
temporary = tempfile.TemporaryDirectory(prefix="noyaru-media-capture-")
root = Path(temporary.name)
os.environ.update(
    DATABASE_URL="sqlite:///" + str(root / "bootstrap.db"),
    SEO_AGENT_SECRET_KEY="fictional-local-media-capture",
    SEO_AGENT_DISABLE_WORKER="true",
    SEO_AGENT_DISABLE_SCHEDULERS="true",
    SEO_AGENT_DISABLE_TELEGRAM_RECEIVER="true",
    TELEGRAM_BOT_TOKEN="",
    TELEGRAM_CHAT_ID="",
    AWS_EC2_METADATA_DISABLED="true",
)
import pytest  # noqa: E402
import uvicorn  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402
from backend import app as m, auth  # noqa: E402
from backend.models import Project  # noqa: E402
from test_customer_activation import lab  # noqa: E402

patch = pytest.MonkeyPatch()
fixture = lab.__wrapped__(root, patch)
x = next(fixture)
with x["db"].session() as db:
    db.add(
        Project(
            owner_user_id=x["admin"].id,
            slug="atelier-demo",
            site_name="Atelier Démo",
            base_url="https://atelier.example/",
            settings={},
        )
    )
    db.commit()
audit = (
    root / "runs" / str(x["admin"].id) / "atelier-demo" / "20261114-120000" / "audit"
)
audit.mkdir(parents=True)
report = {
    "meta": {"base_url": "https://atelier.example/"},
    "pages": [
        {
            "url": "https://atelier.example/service-" + str(i) + "/",
            "status_code": 200,
            "content_type": "text/html",
            "title": "Service de démonstration",
            "indexable": True,
        }
        for i in range(100)
    ],
    "issues": {
        "missing_title": {
            "count": 1,
            "examples": ["https://atelier.example/service-0/"],
        }
    },
}
(audit / "report.json").write_text(json.dumps(report), encoding="utf-8")
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
output = WEB / "content/social-demo"
output.mkdir(parents=True, exist_ok=True)
files, external = {}, []
try:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, executable_path=os.environ.get("CHROMIUM_EXECUTABLE")
        )
        for lang in ("fr", "en"):
            context = browser.new_context(
                viewport={"width": 600, "height": 1050},
                device_scale_factor=2,
                locale="en-US" if lang == "en" else "fr-FR",
                reduced_motion="reduce",
            )
            context.add_cookies(
                [
                    {
                        "name": auth.SESSION_COOKIE_NAME,
                        "value": auth.make_session_token(
                            user_id=str(x["admin"].id),
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
            assert page.goto(base + "/projects/atelier-demo?lang=" + lang).status == 200
            page.wait_for_load_state("networkidle")
            page.add_style_tag(
                content="#assistant-dock { visibility: hidden !important; }"
            )
            metric = page.locator(".overview-health")
            assert metric.count() == 1
            metric.screenshot(path=str(output / (lang + "-score.png")))
            metric.locator("summary").click()
            metric.screenshot(path=str(output / (lang + "-explanation.png")))
            actions = page.locator("#overview-priorities-title").locator(
                "xpath=ancestor::section[1]"
            )
            actions.screenshot(path=str(output / (lang + "-issues.png")))
            context.close()
        browser.close()
    assert not external, external
    for path in sorted(output.glob("*.png")):
        files[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    sources = [
        "templates/project_overview.html",
        "templates/layout.html",
        "static/project_overview.css",
        "static/styles.css",
    ]
    sources = [p for p in sources if (WEB / p).is_file()]
    manifest = {
        "captured": "2026-11-14",
        "base": "4caec100",
        "scope": "Real local Noyaru routes, fictional atelier.example project, no external requests",
        "files": files,
        "sources": {
            p: hashlib.sha256((WEB / p).read_bytes()).hexdigest() for p in sources
        },
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"captures": len(files), "external_requests": external}))
finally:
    server.should_exit = True
    thread.join(timeout=5)
    fixture.close()
    patch.undo()
    temporary.cleanup()
