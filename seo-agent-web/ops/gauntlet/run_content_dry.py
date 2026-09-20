# -*- coding: utf-8 -*-
"""Ou l'agent poserait une page neuve, sur les neuf idiomes du banc. SANS RIEN ECRIRE.

Premier des deux passages. Celui-ci ne coute rien et ne touche a rien : il lit l'arbre de
chaque depot, laisse `placement_pour_route` designer le fichier, puis LIT l'index de section
pour dire si la page serait liee, orpheline, ou si la liste parait engendree.

C'est le passage qui trouve les defauts de CIBLAGE, et c'est la ou sept des huit defauts du
08/09/2026 se trouvaient deja. Un modele appele sur un mauvais chemin produit un beau fichier
au mauvais endroit : le cout se paie, le defaut reste.

La section n'est pas choisie d'avance. Chaque idiome range ses pages autrement, et une section
ecrite en dur ici mesurerait ma supposition plutot que le depot : on prend celle qui porte le
plus de soeurs.
"""
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
        os.environ.setdefault(k.strip(), v.strip())
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402
from backend import repo_index  # noqa: E402

TOKEN = os.environ["FIXTURE_TOKEN"]
OWNER, BRANCH = "pployeraffiliation-a11y", "main"
STACKS = ["static-html", "next-app", "next-pages", "astro", "nuxt", "gatsby",
          "sveltekit", "hugo", "jekyll"]
SUJET_SLUG = "banc-page-neuve-2026"


def _lire(repo: str, chemin: str) -> str:
    fd = m._github_api_get(m._github_content_api_path(OWNER, repo, chemin),
                           token=TOKEN, params={"ref": BRANCH}, timeout_s=20)
    import base64
    return base64.b64decode(str(fd.get("content") or "").replace("\n", "")).decode(
        "utf-8", errors="replace")


def section_la_plus_fournie(paths: list[str]) -> tuple[str, int]:
    """La section qui porte le plus de pages. Une section a UNE page n'a pas de convention."""
    idx = repo_index.build_repo_index(paths)
    compte: dict[str, int] = {}
    for route in (idx.get("routes") or {}):
        if route == "/":
            continue
        section = route.rsplit("/", 1)[0] or "/"
        compte[section] = compte.get(section, 0) + 1
    if not compte:
        return "", 0
    meilleure = max(compte, key=lambda s: (compte[s], s))
    return meilleure, compte[meilleure]


def main() -> None:
    lignes = []
    for stack in STACKS:
        repo = f"noyaru-stack-{stack}"
        try:
            tree = m._github_api_get(
                m._github_api_path("repos", OWNER, repo, "git", "trees", BRANCH),
                token=TOKEN, params={"recursive": "1"}, timeout_s=30)
        except Exception as e:
            lignes.append((stack, "", "", "", "DEPOT ILLISIBLE : %s" % e))
            continue
        paths = [b["path"] for b in tree.get("tree", [])
                 if b.get("type") == "blob" and m._github_file_path_allowed(str(b.get("path") or ""))]
        section, n = section_la_plus_fournie(paths)
        if not section:
            lignes.append((stack, "", "", "", "aucune route lue dans l'arbre"))
            continue
        route = "%s/%s" % (section.rstrip("/"), SUJET_SLUG)
        p = repo_index.placement_pour_route(paths, route, date="2026-09-20")
        if p["refus"]:
            lignes.append((stack, route, "", "", "REFUS : %s" % p["refus"]))
            continue

        verdict = ""
        if not p["index"]:
            verdict = "ORPHELINE (aucune page de section)"
        else:
            try:
                contenu_index = _lire(repo, p["index"])
            except Exception as e:
                verdict = "ORPHELINE (index illisible : %s)" % e
            else:
                etat = m.lien_a_poser(contenu_index, p["soeur_slug"])
                if not etat["requis"]:
                    verdict = "liste engendree, rien a poser"
                elif etat.get("ambigu"):
                    verdict = "ORPHELINE (%s)" % etat["ambigu"]
                else:
                    sortie, refus = m.ajouter_le_lien(
                        contenu_index, p["index"], soeur_slug=p["soeur_slug"],
                        slug_neuf=SUJET_SLUG, titre_soeur="", titre_neuf="")
                    verdict = "lien posable" if sortie else "ORPHELINE (%s)" % refus
        lignes.append((stack, route, p["fichier"], "%s (%s)" % (p["soeurs"][0], p["soeur_slug"]),
                       verdict))

    largeur = max(len(s) for s, *_ in lignes)
    print()
    for stack, route, fichier, soeur, verdict in lignes:
        print("%-*s  %s" % (largeur, stack, route or "-"))
        print("%-*s    fichier : %s" % (largeur, "", fichier or "-"))
        print("%-*s    soeur   : %s" % (largeur, "", soeur or "-"))
        print("%-*s    lien    : %s" % (largeur, "", verdict))
        print()
    orphelines = [s for s, _r, _f, _so, v in lignes if v.startswith("ORPHELINE")]
    refus = [s for s, _r, _f, _so, v in lignes if v.startswith("REFUS") or "ILLISIBLE" in v]
    print("placables : %d/%d" % (len(lignes) - len(refus), len(lignes)))
    print("orphelines (l'automatique refuserait) : %s" % (", ".join(orphelines) or "aucune"))


if __name__ == "__main__":
    main()
