# -*- coding: utf-8 -*-
"""Proposer une page dans un depot QUE NOUS N'AVONS PAS ECRIT.

POURQUOI CE BANC EXISTE A COTE DE `run_content.py`. Les neuf fixtures sont engendrees par
`emit_stacks.py` a partir de notre propre catalogue. Elles prouvent beaucoup — placement sur
neuf idiomes, idiome respecte, garde-fous, build vert — mais elles ne peuvent pas prouver une
chose : qu'on rencontre une convention a laquelle personne ici n'a pense. Le meme angle mort
est des deux cotes, sur l'agent ET sur le sujet d'examen.

Ce banc vise donc un depot tiers, avec ce que les fixtures n'ont pas : des centaines ou des
milliers de fichiers, des routes traduites, une collection de contenu a SCHEMA (un champ de
front matter manquant casse le build), du MDX.

DEUX REGLES ABSOLUES, et elles ne sont pas negociables :
  * la PR est ouverte DANS NOTRE FORK, jamais vers le depot d'origine. Ouvrir une PR chez des
    inconnus pour se tester soi-meme, c'est du spam ;
  * rien n'est jamais fusionne. `--fermer` referme sans fusion, comme pour les fixtures.

Usage :
    run_content_depot.py --depot=<proprietaire>/<repo> --base-url=https://exemple.com/ \\
        --sujet="..." [--route=/section/slug] --ouvrir
    run_content_depot.py --depot=... --verdict
    run_content_depot.py --depot=... --fermer
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import uuid

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
        # Jamais la base de PRODUCTION : ce banc ecrit des lignes de projet.
        if k.strip() != "DATABASE_URL":
            os.environ.setdefault(k.strip(), v.strip())
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)
_ATELIER = os.environ.get("GAUNTLET_WORKDIR", ".")
os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(
    _ATELIER, "banc-depot.db").replace("\\", "/")

from backend import app as m  # noqa: E402
from backend import repo_index  # noqa: E402
from backend.models import Project, User  # noqa: E402

TOKEN = os.environ["FIXTURE_TOKEN"]
ETAT = os.path.join(_ATELIER, "contenu-depot.json")


def _arg(nom: str, defaut: str = "") -> str:
    for a in sys.argv[1:]:
        if a.startswith("--%s=" % nom):
            return a.split("=", 1)[1]
    return defaut


class _Proprietaire:
    """Administrateur : ni le debit ni le quota ne sont ce qu'on mesure ici."""

    def __init__(self, uid: str) -> None:
        self.id, self.is_admin, self.plan = uid, True, "business"
        self.email = "banc-depot@exemple.fr"


def _projet_local(depot: str, base_url: str, branche: str) -> tuple[str, str, str]:
    m.DB.create_tables()
    slug = "banc-" + re.sub(r"[^a-z0-9]+", "-", depot.lower()).strip("-")
    with m.DB.session() as db:
        from sqlalchemy import select
        proj = db.scalar(select(Project).where(Project.slug == slug))
        if proj is not None:
            return slug, str(proj.id), str(proj.owner_user_id)
        u = User(email="banc-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash="x" * 60, is_admin=True)
        db.add(u)
        db.commit()
        db.refresh(u)
        p = Project(owner_user_id=str(u.id), slug=slug, site_name=depot.split("/")[-1],
                    base_url=base_url,
                    settings={"github_repo": depot, "github_branch": branche})
        db.add(p)
        db.commit()
        db.refresh(p)
        return slug, str(p.id), str(u.id)


def _section_la_plus_fournie(paths: list[str], prefixe: str = "") -> str:
    """La section qui porte le plus de pages, eventuellement sous un prefixe de route.

    Le prefixe sert aux sites TRADUITS : sans lui, la section la plus fournie est celle de la
    langue majoritaire, et on proposerait une page francaise au milieu de l'anglais.
    """
    idx = repo_index.build_repo_index(paths)
    compte: dict[str, int] = {}
    for route in (idx.get("routes") or {}):
        if route == "/" or (prefixe and not route.startswith(prefixe)):
            continue
        section = route.rsplit("/", 1)[0] or "/"
        compte[section] = compte.get(section, 0) + 1
    return max(compte, key=lambda s: (compte[s], s)) if compte else ""


def ouvrir() -> None:
    depot, base = _arg("depot"), _arg("base-url")
    sujet = _arg("sujet", "Comment verifier les balises canoniques d'un site")
    prefixe = _arg("prefixe")
    if not depot or "/" not in depot or not base:
        raise SystemExit("--depot=proprietaire/repo et --base-url= sont requis")
    owner, repo = depot.split("/", 1)
    info = m._github_api_get(m._github_api_path("repos", owner, repo), token=TOKEN)
    branche = info["default_branch"]
    if not info.get("fork"):
        print("ATTENTION : %s n'est pas un fork. La PR restera interne a ce depot." % depot)
    slug, pid, uid = _projet_local(depot, base, branche)

    tree = m._github_api_get(m._github_api_path("repos", owner, repo, "git", "trees", branche),
                             token=TOKEN, params={"recursive": "1"}, timeout_s=60)
    paths = [b["path"] for b in tree.get("tree", [])
             if b.get("type") == "blob" and m._github_file_path_allowed(str(b.get("path") or ""))]
    print("%s : %d fichiers retenus sur %d" % (depot, len(paths), len(tree.get("tree", []))))

    route = _arg("route")
    if not route:
        section = _section_la_plus_fournie(paths, prefixe)
        route = "%s/%s" % (section.rstrip("/"), m._slug_de_sujet(sujet))
    print("route visee : %s" % route)

    t0 = time.time()
    try:
        out = m._proposer_une_page(
            _Proprietaire(uid), project_id=pid, site_name=repo, slug=slug,
            sujet=sujet, route=route, base_url=base,
            owner=owner, repo_name=repo, branch=branche, token=TOKEN,
            motif="banc_depot_tiers", refuser_si_orpheline=False)
    except Exception as e:
        out = {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}
    duree = time.time() - t0

    if not out.get("ok"):
        print("REFUS (%.1fs) : %s" % (duree, out.get("error") or out.get("refus")))
        json.dump([{"depot": depot, "ok": False, "error": str(out.get("error"))}],
                  open(ETAT, "w", encoding="utf-8"))
        return
    etat = {"depot": depot, "ok": True, "owner": owner, "repo": repo, "branche": branche,
            "pr_number": out.get("pr_number"), "file": out.get("file"),
            "fix_branch": out.get("branch"), "route": route, "base": base,
            "orpheline": out.get("orpheline")}
    json.dump([etat], open(ETAT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print("PR #%s  %s  orpheline=%s  (%.1fs)"
          % (etat["pr_number"], etat["file"], etat["orpheline"], duree))
    print("NE PAS FUSIONNER. `--verdict` puis `--fermer`.")


def verdict() -> None:
    for r in json.load(open(ETAT, encoding="utf-8")):
        if not r.get("ok"):
            print("pas de PR"); continue
        pr = m._github_api_get(m._github_api_path(
            "repos", r["owner"], r["repo"], "pulls", str(r["pr_number"])), token=TOKEN)
        st = m._github_api_get(m._github_api_path(
            "repos", r["owner"], r["repo"], "commits", pr["head"]["sha"], "check-runs"),
            token=TOKEN)
        runs = st.get("check_runs") or []
        if not runs:
            print("#%s : aucune verification (les Actions d'un fork sont desactivees par "
                  "defaut)" % r["pr_number"])
        for c in runs:
            print("#%s  %-34s %s" % (r["pr_number"], c["name"][:34], c.get("conclusion")))


def fermer() -> None:
    import requests
    for r in json.load(open(ETAT, encoding="utf-8")):
        if not r.get("ok"):
            continue
        resp = requests.patch(
            "https://api.github.com/repos/%s/%s/pulls/%s" % (r["owner"], r["repo"],
                                                             r["pr_number"]),
            headers={"Authorization": "Bearer " + TOKEN,
                     "Accept": "application/vnd.github+json"},
            json={"state": "closed"}, timeout=20)
        print("#%s %s" % (r["pr_number"],
                          "fermee sans fusion" if resp.status_code < 400
                          else "FERMETURE ECHOUEE %s" % resp.status_code))


if __name__ == "__main__":
    if "--ouvrir" in sys.argv:
        ouvrir()
    elif "--verdict" in sys.argv:
        verdict()
    elif "--fermer" in sys.argv:
        fermer()
    else:
        print(__doc__)
