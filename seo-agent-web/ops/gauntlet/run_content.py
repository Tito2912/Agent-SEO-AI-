# -*- coding: utf-8 -*-
"""La redaction de contenu, jouee pour de vrai sur les neuf idiomes du banc.

Second des deux passages. Le passage a blanc (`run_content_dry.py`) dit ou la page IRAIT ; ici
elle est ecrite, liee, poussee et ouverte en pull request brouillon — puis le depot du client
dit, par sa propre chaine de build, si le fichier tient debout.

CE QU'UN TEST UNITAIRE NE PEUT PAS DIRE, et qui est toute la raison d'etre de ce banc : un
bouchon rend le contenu qu'on lui a ecrit. Ici c'est un vrai modele, sur une vraie page soeur,
et le verdict vient de Netlify. Le 11/09/2026, neuf DEPLOIEMENTS avaient prouve la boucle de
correction ; un test vert ne l'aurait pas fait.

UN PIEGE PROPRE A CE BANC : chaque page soeur y est defectueuse PAR CONSTRUCTION — c'est un
parcours d'obstacles. `canonical-404` porte un canonical vers une 404. Demander au modele
d'imiter sa forme, c'est risquer qu'il en imite aussi le defaut. Sur un vrai site les soeurs
sont saines ; ce banc est donc PLUS dur que la realite, et c'est ce qu'on lui demande.

REGLE ABSOLUE, heritee du parcours : aucune de ces pull requests ne doit etre fusionnee. Le
banc doit garder ses defauts pour resservir. `--fermer` les referme toutes.

    --ouvrir    ecrit, pousse, ouvre les PR brouillons (couteux : 9 appels modele)
    --verdict   relit l'etat des verifications de chaque PR
    --fermer    ferme toutes les PR du banc SANS FUSION
"""
import json
import os
import sys
import time
import uuid

import requests

for _flux in (sys.stdout, sys.stderr):
    try:
        _flux.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

sys.path.insert(0, "seo-agent-web")

# LA BASE DE PRODUCTION N'A RIEN A FAIRE ICI. `seo-agent-web.env` est le dump des variables
# Render : son `DATABASE_URL` designe le Postgres du produit. Le charger ferait ecrire les
# projets et les taches de ce banc dans la base des vrais clients — ici l'hote interne de
# Render n'est pas resolvable depuis un poste, donc l'erreur est franche, mais depuis un
# environnement qui le resoudrait elle ne le serait pas. On l'ECARTE nommement.
_HORS_BANC = {"DATABASE_URL"}
for line in open("seo-agent-web.env", encoding="utf-8", errors="replace"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        if k.strip() in _HORS_BANC:
            continue
        os.environ.setdefault(k.strip(), v.strip())
_SCRATCH = os.environ.get("GAUNTLET_WORKDIR") or "."
os.environ["DATABASE_URL"] = "sqlite:///" + os.path.join(_SCRATCH, "banc-contenu.db").replace("\\", "/")
os.environ.setdefault("SEO_AGENT_DATA_DIR", os.path.join(_SCRATCH, "data"))
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "x" * 20)

from backend import app as m  # noqa: E402
from backend import repo_index  # noqa: E402
from backend.models import Project, User  # noqa: E402

TOKEN = os.environ["FIXTURE_TOKEN"]
OWNER, BRANCH = "pployeraffiliation-a11y", "main"
STACKS = ["static-html", "next-app", "next-pages", "astro", "nuxt", "gatsby",
          "sveltekit", "hugo", "jekyll"]
SUJET = "Comment vérifier les balises canoniques d'un site"
ETAT = os.path.join(os.environ.get("GAUNTLET_WORKDIR", "."), "contenu-banc.json")


def stacks_demandees() -> list[str]:
    """Les piles a jouer. `--stack=next-pages` n'en joue qu'une.

    Un banc complet coute neuf appels de modele. Apres un correctif qui ne visait qu'une pile,
    les huit autres ne mesurent rien de neuf : on paie pour confirmer ce qu'on sait deja.
    """
    for arg in sys.argv[1:]:
        if arg.startswith("--stack="):
            voulues = [s.strip() for s in arg.split("=", 1)[1].split(",") if s.strip()]
            inconnues = [s for s in voulues if s not in STACKS]
            if inconnues:
                raise SystemExit("pile inconnue : %s" % ", ".join(inconnues))
            return voulues
    return list(STACKS)


class _Proprietaire:
    """Administrateur : le debit et la porte ne sont pas ce qu'on mesure ici, et facturer un
    banc a un compte de test brouillerait les compteurs qu'on relit par ailleurs."""

    is_admin = True

    def __init__(self, uid: str) -> None:
        self.id = uid


def _projet_local(stack: str) -> tuple[str, str, str]:
    """Un projet en base locale, pour que la tache et le controle de doublon se comportent
    comme en production. Sans lui, `_proposer_une_page` ecrirait dans le vide."""
    m.DB.create_tables()
    slug = "banc-%s" % stack
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
        p = Project(owner_user_id=str(u.id), slug=slug, site_name="noyaru-stack-%s" % stack,
                    base_url="https://noyaru-stack-%s.netlify.app/" % stack,
                    settings={"github_repo": "%s/noyaru-stack-%s" % (OWNER, stack),
                              "github_branch": BRANCH})
        db.add(p)
        db.commit()
        db.refresh(p)
        return slug, str(p.id), str(u.id)


def _section_la_plus_fournie(paths: list[str]) -> str:
    idx = repo_index.build_repo_index(paths)
    compte: dict[str, int] = {}
    for route in (idx.get("routes") or {}):
        if route == "/":
            continue
        section = route.rsplit("/", 1)[0] or "/"
        compte[section] = compte.get(section, 0) + 1
    return max(compte, key=lambda s: (compte[s], s)) if compte else ""


def ouvrir() -> None:
    resultats = []
    for stack in stacks_demandees():
        repo = "noyaru-stack-%s" % stack
        slug, pid, uid = _projet_local(stack)
        try:
            tree = m._github_api_get(
                m._github_api_path("repos", OWNER, repo, "git", "trees", BRANCH),
                token=TOKEN, params={"recursive": "1"}, timeout_s=30)
        except Exception as e:
            resultats.append({"stack": stack, "ok": False, "error": "arbre illisible : %s" % e})
            print("%-12s ARBRE ILLISIBLE : %s" % (stack, e))
            continue
        paths = [b["path"] for b in tree.get("tree", [])
                 if b.get("type") == "blob"
                 and m._github_file_path_allowed(str(b.get("path") or ""))]
        section = _section_la_plus_fournie(paths)
        route = "%s/%s" % (section.rstrip("/"), m._slug_de_sujet(SUJET))

        t0 = time.time()
        try:
            out = m._proposer_une_page(
                _Proprietaire(uid), project_id=pid, site_name=repo, slug=slug,
                sujet=SUJET, route=route,
                # L'adresse de PRODUCTION, pas celle de l'apercu : le canonical d'une page
                # doit designer le site, pas le deploiement d'essai qui la montre.
                base_url="https://%s.netlify.app/" % repo,
                owner=OWNER, repo_name=repo,
                branch=BRANCH, token=TOKEN, motif="banc_contenu",
                refuser_si_orpheline=False)
        except Exception as e:
            out = {"ok": False, "error": "%s: %s" % (type(e).__name__, e)}
        out["stack"] = stack
        out["repo"] = repo
        out["secondes"] = round(time.time() - t0, 1)
        out["ouvert_le"] = time.time()
        resultats.append(out)
        if out.get("ok"):
            print("%-12s PR #%s  %s  orpheline=%s  (%ss)"
                  % (stack, out.get("pr_number"), out.get("file"),
                     out.get("orpheline"), out["secondes"]))
        else:
            print("%-12s REFUS : %s" % (stack, out.get("error")))

    json.dump(resultats, open(ETAT, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    ouvertes = [r for r in resultats if r.get("ok")]
    print("\n%d/%d pull requests ouvertes — etat dans %s" % (len(ouvertes), len(STACKS), ETAT))
    print("NE FUSIONNER AUCUNE. `--verdict` puis `--fermer`.")


def verdict() -> None:
    resultats = json.load(open(ETAT, encoding="utf-8"))
    for r in resultats:
        if not r.get("ok"):
            print("%-12s (pas de PR) %s" % (r["stack"], r.get("error", "")[:90]))
            continue
        sha = ""
        try:
            pr = m._github_api_get(
                m._github_api_path("repos", OWNER, r["repo"], "pulls", str(r["pr_number"])),
                token=TOKEN, timeout_s=20)
            sha = str((pr.get("head") or {}).get("sha") or "")
        except Exception as e:
            print("%-12s PR illisible : %s" % (r["stack"], e))
            continue
        v = m._verifications_du_commit(owner=OWNER, repo=r["repo"], sha=sha, token=TOKEN)
        age = time.time() - float(r.get("ouvert_le") or 0) if r.get("ouvert_le") else 10_000
        decision, raison = m._decider_de_la_pr(v, age_s=age)
        print("%-12s #%-4s %-11s %s" % (r["stack"], r["pr_number"], decision, raison))


def fermer() -> None:
    """Fermer sans fusionner. La regle absolue du banc : il doit garder ses defauts."""
    resultats = json.load(open(ETAT, encoding="utf-8"))
    for r in resultats:
        if not r.get("ok"):
            continue
        try:
            # L'application n'expose pas de PATCH : lui en ajouter un pour le seul usage du
            # banc ferait grossir le code de production pour un outil d'atelier.
            resp = requests.patch(
                m._github_api_url(m._github_api_path(
                    "repos", OWNER, r["repo"], "pulls", str(r["pr_number"]))),
                headers={"Authorization": "Bearer %s" % TOKEN,
                         "Accept": "application/vnd.github+json",
                         "X-GitHub-Api-Version": "2022-11-28",
                         "User-Agent": "seo-agent-gauntlet"},
                json={"state": "closed"}, timeout=20)
            if resp.status_code >= 400:
                raise RuntimeError("GitHub %s: %s" % (resp.status_code, resp.text[:200]))
            print("%-12s #%s fermee sans fusion" % (r["stack"], r["pr_number"]))
        except Exception as e:
            print("%-12s #%s FERMETURE ECHOUEE : %s" % (r["stack"], r["pr_number"], e))


if __name__ == "__main__":
    if "--ouvrir" in sys.argv:
        ouvrir()
    elif "--verdict" in sys.argv:
        verdict()
    elif "--fermer" in sys.argv:
        fermer()
    else:
        print(__doc__)
