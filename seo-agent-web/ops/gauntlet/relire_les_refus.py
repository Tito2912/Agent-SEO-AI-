# -*- coding: utf-8 -*-
"""Faire relire les fichiers REELS des neuf depots aux garde-fous qui refusent ou modifient.

LA PREUVE EST DE CONSTRUCTION. Un fichier deja present dans le depot, que Netlify construit et
sert, est valide. Si un garde-fou le REFUSE, c'est un faux refus ; s'il le MODIFIE, c'est soit
une reparation que personne n'avait demandee, soit une corruption. Dans les trois cas il faut
aller voir. Aucun appel de modele : cette sonde ne coute rien et peut tourner a chaque fois.

Elle a trouve, en deux passages, deux defauts que 2068 tests ne voyaient pas :
  - `_refus_de_format` refusait `src/layouts/Base.astro` sur une expression parfaitement
    equilibree (25/09/2026) ;
  - `_antislashs_de_trop` retirait un echappement OBLIGATOIRE dans du code pris en sandwich
    entre deux blocs de balisage, cassant le litteral et le build (26/09/2026).

A LANCER APRES TOUT CHANGEMENT a un garde-fou qui refuse ou qui reecrit. Ce que la sonde ne
prouve pas : qu'un vrai defaut serait vu. Ca se mesure par mutation, ailleurs.
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
# La sonde ne lit que GitHub : la pointer sur la base de production ne servirait qu'a risquer
# de l'ecrire.
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402

TOKEN = os.environ["FIXTURE_TOKEN"]
OWNER, BRANCH = "pployeraffiliation-a11y", "main"
STACKS = ["static-html", "next-app", "next-pages", "astro", "nuxt", "gatsby",
          "sveltekit", "hugo", "jekyll"]

# Tout ce qu'un garde-fou peut refuser, toutes portes confondues.
INTERESSANTES = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".vue", ".svelte", ".astro",
                 ".html", ".htm", ".md", ".markdown", ".mdx")
# Les sorties de construction sont des COPIES : les relire mesurerait deux fois la meme source.
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
    total = 0
    anomalies: list[str] = []
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
                anomalies.append("FAUX REFUS   %-12s %-44s %s" % (stack, chemin, refus))

            reecrit, notes = m._antislashs_de_trop(contenu, chemin)
            if reecrit != contenu:
                anomalies.append("MODIFIE      %-12s %-44s %s" % (stack, chemin, notes))
        total += vus
        print("%-12s %3d fichiers relus" % (stack, vus))

    print("\n%d fichiers reels, %d anomalie(s)" % (total, len(anomalies)))
    for ligne in anomalies:
        print("  " + ligne)
    sys.exit(1 if anomalies else 0)


if __name__ == "__main__":
    main()
