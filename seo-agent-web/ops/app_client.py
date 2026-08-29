#!/usr/bin/env python3
"""Walk the app exactly as a customer's browser does, from a terminal.

Every real defect this project has found came from being a logged-in customer on a real
account: the checkout funnel that had never worked, the plan change that billed a month
later, the scheduled downgrade that left no trace on the page. None of them were visible
from the code or from an admin session, and each round of that walk started by rebuilding
the same small HTTP client from scratch — one lived in a scratch directory and disappeared
with it.

Two details are what make it a rebuild rather than a one-liner, and both are easy to get
wrong twice:

  * the login form carries a hidden `_csrf` field that must be posted back with the
    credentials, and
  * a JSON POST is refused unless the `x-csrf-token` HEADER carries the value of the
    `seo_agent_csrf` COOKIE. The form field is not accepted there, and the failure looks
    like a permissions problem (403) rather than a missing header.

Usage — as a library, which is the point:

    from ops.app_client import AppClient
    c = AppClient("https://noyaru.com")
    c.login("client@exemple.fr", "motdepasse")
    print(c.get("/projects/mon-site/keywords/opportunities").status_code)
    print(c.post_json("/api/projects/mon-site/keywords/rewrite-pr",
                      {"query": "kling ai", "url": "https://site.fr/guide"}).json())

…or from the command line, to check a session is really established:

    python ops/app_client.py https://noyaru.com client@exemple.fr 'motdepasse' /billing

It prints the status, the final URL and the page title, because "200 on /auth/login" is what
a FAILED login looks like: the form is re-rendered with an error, not a redirect.
"""

from __future__ import annotations

import re
import sys
from typing import Any

import requests

_CSRF_COOKIE = "seo_agent_csrf"
_CSRF_HEADER = "x-csrf-token"
_CSRF_FIELD = re.compile(r'name="_csrf"\s+value="([^"]*)"')
_TITLE = re.compile(r"<title>(.*?)</title>", re.S | re.I)


class LoginFailed(RuntimeError):
    pass


class AppClient:
    """A logged-in session against a running instance (local, Render, or noyaru.com)."""

    def __init__(self, base_url: str, *, timeout: int = 60) -> None:
        self.base = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "noyaru-ops-client"

    # ── plumbing ──────────────────────────────────────────────────────────────────────────
    def _url(self, path: str) -> str:
        return path if path.startswith("http") else f"{self.base}{path}"

    def csrf_token(self, page: str = "/") -> str:
        """The token from the cookie, fetching a page first if the session has none yet."""
        token = self.session.cookies.get(_CSRF_COOKIE, "")
        if not token:
            self.get(page)
            token = self.session.cookies.get(_CSRF_COOKIE, "")
        return token

    def get(self, path: str, **kw: Any) -> requests.Response:
        return self.session.get(self._url(path), timeout=self.timeout, **kw)

    def post_form(self, path: str, data: dict[str, Any], *, form_page: str | None = None,
                  **kw: Any) -> requests.Response:
        """POST a form, carrying the hidden `_csrf` field the page itself would carry."""
        token = ""
        if form_page:
            match = _CSRF_FIELD.search(self.get(form_page).text)
            token = match.group(1) if match else ""
        return self.session.post(self._url(path), data={**data, "_csrf": token or self.csrf_token()},
                                 timeout=self.timeout, allow_redirects=False, **kw)

    def post_json(self, path: str, payload: dict[str, Any], **kw: Any) -> requests.Response:
        """POST JSON the way the pages' fetch() does: the token travels in the header."""
        return self.session.post(
            self._url(path), json=payload, timeout=self.timeout,
            headers={_CSRF_HEADER: self.csrf_token(), "Content-Type": "application/json"}, **kw)

    # ── the session ───────────────────────────────────────────────────────────────────────
    def login(self, email: str, password: str) -> None:
        """Log in, and refuse to pretend it worked.

        A failed login answers 200 with the form re-rendered, so checking the status code is
        precisely the wrong test. What proves a session is the session cookie.
        """
        page = self.get("/auth/login")
        match = _CSRF_FIELD.search(page.text)
        if not match:
            raise LoginFailed(f"no CSRF field on /auth/login (status {page.status_code})")
        resp = self.session.post(
            self._url("/auth/login"),
            data={"email": email, "password": password, "next": "/", "_csrf": match.group(1)},
            timeout=self.timeout, allow_redirects=False)
        if not any(c.name.startswith("seo_agent_session") for c in self.session.cookies):
            raise LoginFailed(
                f"login refused (status {resp.status_code}) — no session cookie was set")

    def whoami(self) -> str:
        """The account the session belongs to, read from a page rather than assumed.

        `/settings` prints it; `/projects` does not — checked against the deployed app, because
        an identity check that silently answers "" is worse than no check at all.
        """
        match = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", self.get("/settings").text)
        return match.group(0) if match else ""


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(__doc__)
        return 2
    base, email, password = argv[1], argv[2], argv[3]
    path = argv[4] if len(argv) > 4 else "/projects"
    client = AppClient(base)
    try:
        client.login(email, password)
    except LoginFailed as exc:
        print(f"ECHEC: {exc}")
        return 1
    resp = client.get(path)
    title = _TITLE.search(resp.text)
    print(f"compte      : {client.whoami() or '(introuvable sur /settings)'}")
    print(f"{path:12s}: {resp.status_code} {resp.url}")
    print(f"titre       : {(title.group(1).strip() if title else '')[:120]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
