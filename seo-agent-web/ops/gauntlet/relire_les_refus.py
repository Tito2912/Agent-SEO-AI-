# -*- coding: utf-8 -*-
"""Faire relire aux garde-fous de refus tous les fichiers REELS des neuf depots du banc.

La preuve est de construction : un fichier deja present dans le depot, que Netlify construit
et sert, est valide. Si `_refus_de_format` le refuse, c'est un faux refus — pas une opinion,
un fait. Aucun appel de modele, donc aucun cout.

Ce que ca ne prouve pas : qu'un vrai defaut serait vu. Ca se mesure ailleurs, par mutation.
"""
from __future__ import annotations

import base64
import os
import sys

for _flux in (sys.stdout, sys.stderr):
    try:
        _flux.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, "seo-agent-web")
for line in open("seo-agent-web.env", encoding="utf-8", errors="replace"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        if k.strip() != "DATABASE_URL":
            os.environ.setdefault(k.strip(), v.strip())
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402

TOKEN = os.environ["FIXTURE_TOKEN"]
OWNER, BRANCH = "pployeraffiliation-a11y", "main"
STACKS = ["static-html", "next-app", "next-pages", "astro", "nuxt", "gatsby",
          "sveltekit", "hugo", "jekyll"]

# Les extensions que `_refus_de_format` peut refuser, toutes portes confondues.
INTERESSANTES = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".vue", ".svelte", ".astro",
                 ".html", ".htm", ".md", ".markdown", ".mdx")
IGNORES = ("node_modules/", "dist/", "build/", ".next/", ".nuxt/", ".output/", "public/build/")


def arbre(repo: str) -> list[str]:
    d = m._github_api_get(
        m._github_api_path("repos", OWNER, repo, "git", "trees", BRANCH),
        token=TOKEN, params={"recursive": "1"}, timeout_s=30)
    return [n["path"] for n in (d.get("tree") or []) if n.get("type") == "blob"]


def lire(repo: str, chemin: str) -> str:
    fd = m._github_api_get(m._github_content_api_path(OWNER, repo, chemin),
                           token=TOKEN, params={"ref": BRANCH}, timeout_s=30)
    return base64.b64decode(str(fd.get("content") or "").replace("\n", "")).decode(
        "utf-8", errors="replace")


def main() -> None:
    total = refuses = 0
    faux: list[tuple[str, str, str]] = []
    for stack in STACKS:
        repo = "noyaru-stack-" + stack
        chemins = [c for c in arbre(repo)
                   if c.lower().endswith(INTERESSANTES)
                   and not any(c.startswith(g) or ("/" + g) in c for g in IGNORES)]
        vus = 0
        for chemin in chemins:
            try:
                contenu = lire(repo, chemin)
            except Exception as exc:
                print("  %-52s LECTURE : %s" % (chemin, exc))
                continue
            vus += 1
            refus = m._refus_de_format(chemin, contenu)
            if refus:
                refuses += 1
                faux.append((stack, chemin, refus))
        total += vus
        print("%-12s %3d fichiers relus" % (stack, vus))
    print("\n%d fichiers reels, %d refuses" % (total, refuses))
    for stack, chemin, refus in faux:
        print("  FAUX REFUS  %-12s %-46s %s" % (stack, chemin, refus))
    sys.exit(1 if faux else 0)


if __name__ == "__main__":
    main()
