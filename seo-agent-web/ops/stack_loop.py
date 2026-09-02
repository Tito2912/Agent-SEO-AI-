#!/usr/bin/env python3
"""Drive the full product loop, on a real repository, for one stack at a time.

The claim this exists to settle: "the agent corrects sites on nine stacks". What was proven
before it: the corrector's loop LOCALLY on nine buildable fixtures (inject → build → crawl →
rewrite → rebuild → re-crawl), and the whole path through GitHub — connect, open a pull request,
merge, re-crawl the rebuilt site — on exactly two, because those are the only stacks a real
customer runs (next-app, static-html).

Nothing here is a new test harness. Every step is the product's own endpoint, called the way the
browser calls it, so what it measures is what a customer gets:

    prepare   copy a fixture into a deployable tree (netlify.toml, .gitignore, no build output)
    publish   create the GitHub repository and push that tree            [needs GITHUB_TOKEN]
    run       create the Noyaru project, connect the repo, crawl, open the two pull requests
    merge     merge a pull request                                        [needs GITHUB_TOKEN]
    verify    re-crawl and report what actually changed on the rebuilt site

Two pull requests per stack, on purpose, because they exercise different halves:
  * `canonical_points_to_redirect` — the DETERMINISTIC corrector, which the fixtures already
    carry as their one injected defect. Its success is measurable: the anomaly disappears.
  * the keyword snippet rewrite — the MODEL-WRITTEN family, whose success is not a number the
    crawler reports. What is verified there is the diff: two lines, the right file, the page's
    language kept, both values inside the window.

Deployment is Netlify, one site per stack, because the route map assumes a site served at the
ROOT: a GitHub Pages project site lives under /<repo>/, and that extra path segment would break
URL→file resolution — precisely the thing under test.

Example, one stack end to end:

    export GITHUB_TOKEN=ghp_...
    python ops/stack_loop.py prepare astro --out /tmp/stacks
    python ops/stack_loop.py publish astro --out /tmp/stacks --repo pployeraffiliation-a11y/noyaru-stack-astro
    #   … connect that repo to a Netlify site (UI: Add new site → Import from Git), note the URL
    python ops/stack_loop.py run astro --site https://noyaru-stack-astro.netlify.app
    python ops/stack_loop.py merge astro --pr 1
    python ops/stack_loop.py verify astro
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops.app_client import AppClient, LoginFailed  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"
GITHUB_API = "https://api.github.com"


@dataclass(frozen=True)
class Stack:
    """One stack, its build, and the two pages the loop acts on."""

    fixture: str
    build: str
    publish: str
    # The page carrying the injected canonical defect — the anomaly PR's target.
    defect_path: str = "/blog"
    # A healthy page, and a query it could plausibly rank for: the keyword PR's target. Never
    # the same page as the defect, so the two pull requests cannot collide on one file.
    keyword_path: str = "/a-propos"
    keyword_query: str = "site de test seo"
    env: dict[str, str] = field(default_factory=dict)
    # Directories that are BUILD OUTPUT: committing them would make the deploy a no-op and the
    # re-crawl meaningless, since the site would not be rebuilt from the patched source.
    ignore: tuple[str, ...] = ("node_modules", "dist", "build", "public", "out", ".output",
                               "_site", ".astro", ".svelte-kit", ".cache", ".netlify",
                               # The framework CACHES, which are build output under another
                               # name: measured, `.next` alone put 74 generated files into the
                               # next-pages tree and `.nuxt` 41 into Nuxt's. A repository that
                               # already contains the built site makes the re-crawl prove
                               # nothing — the host would serve what was committed, not what
                               # the merged patch produces.
                               ".next", ".nuxt", ".vercel", ".vite")


STACKS: dict[str, Stack] = {
    "astro": Stack("astro", "npm run build", "dist"),
    # Hugo publishes to public/, which is also its static input dir on other stacks — the ignore
    # list is per stack for exactly this reason.
    "hugo": Stack("hugo", "hugo --gc --minify", "public",
                  env={"HUGO_VERSION": "0.128.0"},
                  ignore=("node_modules", "public", "resources")),
    "jekyll": Stack("jekyll", "bundle exec jekyll build", "_site",
                    env={"RUBY_VERSION": "3.3.0"},
                    ignore=("_site", ".jekyll-cache", "vendor")),
    "next-pages": Stack("next-pages", "npm run build", "out"),
    # npm 10 crashes on Nuxt's peer graph (`edgesOut` of null in arborist) — an npm bug, not a
    # Nuxt one. Netlify runs npm install for us, so the flag has to be passed to it.
    "nuxt": Stack("nuxt", "npm run build", ".output/public",
                  env={"NPM_FLAGS": "--legacy-peer-deps"}),
    "sveltekit": Stack("sveltekit", "npm run build", "build"),
    "gatsby": Stack("gatsby", "npm run build", "public",
                    ignore=("node_modules", "public", ".cache")),
    "static-html": Stack("static-html", "", ".", ignore=()),
    "next-app": Stack("next-app", "npm run build", "out"),
}

NETLIFY_TOML = """# Generated by ops/stack_loop.py — the deploy half of the nine-stack loop.
# Netlify serves at the ROOT of its own subdomain, which is what the repo route map assumes:
# a site under /<repo>/ would break URL→file resolution, the very thing this loop measures.
[build]
{command}publish = "{publish}"
{env}"""


def _stack(name: str) -> Stack:
    if name not in STACKS:
        sys.exit(f"stack inconnu : {name}. Connus : {', '.join(sorted(STACKS))}")
    return STACKS[name]


# ── prepare ───────────────────────────────────────────────────────────────────────────────────

def cmd_prepare(args) -> int:
    stack = _stack(args.stack)
    src = FIXTURES / stack.fixture
    if not src.exists():
        sys.exit(f"fixture absent : {src}")
    dest = Path(args.out).expanduser() / stack.fixture
    if dest.exists():
        # `copy2` carries read-only attributes over from the fixture, and Windows then refuses
        # to remove the tree — clear the bit and retry rather than leave a half-deleted tree.
        def _force(func, path, _exc):
            os.chmod(path, 0o700)
            func(path)

        shutil.rmtree(dest, onexc=_force)
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _skip(dir_path, names):
        return {n for n in names if n in stack.ignore or n == ".git"}

    shutil.copytree(src, dest, ignore=_skip)

    command = f'  command = "{stack.build}"\n' if stack.build else ""
    env_block = ""
    if stack.env:
        env_block = "\n[build.environment]\n" + "".join(
            f'  {k} = "{v}"\n' for k, v in sorted(stack.env.items()))
    (dest / "netlify.toml").write_text(
        NETLIFY_TOML.format(command=command, publish=stack.publish, env=env_block),
        encoding="utf-8")
    (dest / ".gitignore").write_text(
        "# Build output stays OUT of the repository: the point of the loop is that merging the\n"
        "# patch makes the host rebuild. A committed build would hide exactly that.\n"
        + "\n".join(stack.ignore) + "\n", encoding="utf-8")

    rewritten = 0
    if args.site:
        # The fixtures declare absolute canonicals against a local port. Deployed as they are,
        # they would point at 127.0.0.1 — which the crawler flags as a canonical to ANOTHER host,
        # a different anomaly from the trailing-slash one this loop is built to prove. So the
        # host is rewritten at prepare time, and the defect (the trailing slash) is kept intact.
        site = args.site.rstrip("/")
        host_re = re.compile(r"https?://127\.0\.0\.1:\d+")
        for path in dest.rglob("*"):
            if not path.is_file() or path.suffix.lower() in {".png", ".jpg", ".ico", ".woff2"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                continue
            new_text, n = host_re.subn(site, text)
            if n:
                path.write_text(new_text, encoding="utf-8")
                rewritten += n

    files = sum(1 for _ in dest.rglob("*") if _.is_file())
    host_note = f", {rewritten} URL réécrites vers {args.site}" if rewritten else ""
    print(f"{args.stack:<12} prêt dans {dest}  ({files} fichiers, publish={stack.publish}{host_note})")
    return 0


# ── publish ───────────────────────────────────────────────────────────────────────────────────

def _github(method: str, path: str, token: str, payload: dict | None = None) -> dict:
    import requests
    r = requests.request(method, f"{GITHUB_API}{path}", timeout=30, json=payload,
                         headers={"Authorization": f"Bearer {token}",
                                  "Accept": "application/vnd.github+json"})
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub {method} {path} -> {r.status_code} {r.text[:300]}")
    return r.json() if r.content else {}


def cmd_publish(args) -> int:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        sys.exit("GITHUB_TOKEN manquant (scope repo).")
    stack = _stack(args.stack)
    tree = Path(args.out).expanduser() / stack.fixture
    if not tree.exists():
        sys.exit(f"arborescence absente : {tree} — lance `prepare` d'abord.")
    owner, name = args.repo.split("/", 1)

    try:
        _github("GET", f"/repos/{owner}/{name}", token)
        print(f"dépôt {args.repo} déjà présent, on pousse dedans")
    except RuntimeError:
        me = _github("GET", "/user", token).get("login", "")
        endpoint = "/user/repos" if owner.lower() == me.lower() else f"/orgs/{owner}/repos"
        _github("POST", endpoint, token, {"name": name, "private": False,
                                          "description": f"Noyaru — boucle 9 stacks ({stack.fixture})",
                                          "auto_init": False})
        print(f"dépôt {args.repo} créé")

    remote = f"https://x-access-token:{token}@github.com/{owner}/{name}.git"
    run = lambda *a: subprocess.run(a, cwd=tree, check=True, capture_output=True, text=True)  # noqa: E731
    if not (tree / ".git").exists():
        run("git", "init", "-b", "main")
        run("git", "add", "-A")
        run("git", "-c", "user.email=noyaru@example.invalid", "-c", "user.name=Noyaru",
            "commit", "-m", f"fixture {stack.fixture} avec son defaut injecte")
    run("git", "remote", "remove", "origin") if (tree / ".git" / "config").read_text(
        encoding="utf-8").find("[remote \"origin\"]") >= 0 else None
    run("git", "remote", "add", "origin", remote)
    run("git", "push", "-u", "origin", "main")
    print(f"{args.stack:<12} poussé sur https://github.com/{args.repo}")
    print("   → connecte-le maintenant à un site Netlify (Add new site → Import from Git)")
    return 0


# ── run ───────────────────────────────────────────────────────────────────────────────────────

def _client(args) -> AppClient:
    client = AppClient(args.app)
    try:
        client.login(args.email, args.password)
    except LoginFailed as exc:
        sys.exit(f"connexion refusée : {exc}")
    return client


def _wait_for_crawl(client: AppClient, slug: str, timeout_s: int = 900) -> str:
    """Poll until a crawl finishes, and answer with its timestamp."""
    started = time.time()
    while time.time() - started < timeout_s:
        page = client.get(f"/projects/{slug}/crawls").text
        stamps = re.findall(r"(\d{8}-\d{6})", page)
        if stamps:
            return sorted(set(stamps))[-1]
        time.sleep(15)
    return ""


def cmd_run(args) -> int:
    stack = _stack(args.stack)
    client = _client(args)
    slug = args.slug or f"noyaru-stack-{stack.fixture}"

    if client.get(f"/projects/{slug}").status_code == 404:
        client.post_form("/projects/add",
                         {"mode": "url", "url": args.site, "site_name": slug, "next": "/"},
                         form_page="/")
        print(f"projet {slug} créé sur {args.site}")

    connected = client.post_json(f"/api/projects/{slug}/github/connect",
                                 {"repo": args.repo, "branch": "main", "mode": "review"}).json()
    if not connected.get("ok"):
        sys.exit(f"connexion GitHub refusée : {connected.get('error')}")
    print(f"dépôt connecté : {connected['repo']} ({connected['branch']}, {connected['mode']})")

    client.post_form(f"/projects/{slug}/crawl", {}, form_page=f"/projects/{slug}")
    print("crawl lancé, on attend…")
    crawl_ts = _wait_for_crawl(client, slug)
    print(f"crawl : {crawl_ts or 'AUCUN — regarde /jobs'}")

    # 1) the deterministic corrector, on the defect the fixture carries
    page = client.get(f"/projects/{slug}/corrections").text
    candidates = re.findall(r'data-correction-deep-fix data-issue="([^"]+)" data-url="([^"]+)"', page)
    print(f"anomalies corrigeables : {[k for k, _ in candidates] or 'aucune'}")
    for key, url in candidates:
        if "canonical" not in key:
            continue
        out = client.post_json(f"/api/projects/{slug}/issues/{key}/deep-fix",
                               {"url": url, "crawl_ts": crawl_ts}).json()
        print(f"   PR anomalie ({key}) : {out.get('pr_url') or out.get('error')}")
        break

    # 2) the model-written family, on a healthy page
    kw = client.post_json(f"/api/projects/{slug}/keywords/rewrite-pr",
                          {"query": args.query or stack.keyword_query,
                           "url": args.site.rstrip("/") + (args.page or stack.keyword_path)}).json()
    print(f"   PR mots-clés : {kw.get('pr_url') or kw.get('error')}")
    return 0


# ── merge / verify ────────────────────────────────────────────────────────────────────────────

def cmd_merge(args) -> int:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        sys.exit("GITHUB_TOKEN manquant.")
    owner, name = args.repo.split("/", 1)
    out = _github("PUT", f"/repos/{owner}/{name}/pulls/{args.pr}/merge", token,
                  {"merge_method": "squash"})
    print(f"PR #{args.pr} : {'mergée' if out.get('merged') else out}")
    return 0


def cmd_verify(args) -> int:
    """Re-crawl and say what the rebuilt site now reports. The only honest end of the loop."""
    stack = _stack(args.stack)
    client = _client(args)
    slug = args.slug or f"noyaru-stack-{stack.fixture}"
    before = _issue_counts(client, slug)
    client.post_form(f"/projects/{slug}/crawl", {}, form_page=f"/projects/{slug}")
    print("re-crawl lancé, on attend…")
    _wait_for_crawl(client, slug)
    after = _issue_counts(client, slug)
    keys = sorted(set(before) | set(after))
    print(f"\n{'anomalie':<45} avant  après")
    for key in keys:
        b, a = before.get(key, 0), after.get(key, 0)
        if b or a:
            print(f"{key:<45} {b:>5}  {a:>5}{'   ← résolue' if b and not a else ''}")
    return 0


def _issue_counts(client: AppClient, slug: str) -> dict[str, int]:
    page = client.get(f"/projects/{slug}/issues").text
    return {k: int(n) for k, n in re.findall(r'data-issue-key="([^"]+)"[^>]*data-count="(\d+)"', page)}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app", default="https://noyaru.com")
    parser.add_argument("--email", default=os.environ.get("NOYARU_EMAIL", ""))
    parser.add_argument("--password", default=os.environ.get("NOYARU_PASSWORD", ""))
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("prepare"); p.add_argument("stack"); p.add_argument("--out", required=True)
    p.add_argument("--site", help="URL du site déployé : réécrit les canonicals du fixture, "
                                  "qui visent un port local")
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser("publish"); p.add_argument("stack")
    p.add_argument("--out", required=True); p.add_argument("--repo", required=True)
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("run"); p.add_argument("stack")
    p.add_argument("--site", required=True); p.add_argument("--repo", required=True)
    p.add_argument("--slug"); p.add_argument("--query"); p.add_argument("--page")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("merge"); p.add_argument("stack")
    p.add_argument("--repo", required=True); p.add_argument("--pr", type=int, required=True)
    p.set_defaults(func=cmd_merge)

    p = sub.add_parser("verify"); p.add_argument("stack"); p.add_argument("--slug")
    p.set_defaults(func=cmd_verify)

    args = parser.parse_args(argv[1:])
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
