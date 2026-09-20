# -*- coding: utf-8 -*-
"""Le mode automatique : ce qu'il écrit tout seul, et surtout ce qu'il refuse d'écrire.

C'EST LE SEUL ENDROIT DU PRODUIT QUI ÉCRIT CHEZ UN CLIENT SANS QUE PERSONNE N'AIT CLIQUÉ. La
politique anti-spam de Google vise le contenu produit en masse pour le classement, quelle qu'en
soit la fabrication : un mode automatique généreux ferait de cette fonction un moyen de NUIRE
au client qu'elle prétend servir. Chaque restriction testée ici répond à cette crainte-là.

    * il faut l'avoir demandé, projet par projet, et nommer la section ;
    * une page par semaine au plus, sous le plafond mensuel du forfait ;
    * uniquement des sujets qu'un concurrent traite et que le site ne couvre PAS ;
    * un sujet déjà proposé ne revient jamais, même si sa pull request a été fermée ;
    * une page qu'on ne sait pas LIER n'est pas écrite — là où le mode manuel se contente de
      le dire, parce qu'ici personne ne lira la phrase.

LES DEUX MODES PARTAGENT LEUR CŒUR (`_proposer_une_page`) et une seule différence les sépare,
qui est un paramètre. Deux implémentations divergeraient, et c'est celle que personne ne
regarde qui finirait par écrire n'importe quoi chez un client.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import time
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-auto-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import CompetitorSite, IssueTask, Project, User  # noqa: E402

SOEUR = """export const metadata = {
  title: "Premier article",
  description: "Le premier article du blog, et le gabarit de tous les autres.",
};

export default function Page() { return <article>Texte.</article>; }
"""

INDEX_MANUEL = """export default function Blog() {
  return (
    <ul>
      <li><a href="/blog/premier-article">Premier article</a></li>
      <li><a href="/blog/second-article">Second article</a></li>
    </ul>
  );
}
"""

REDIGE = """export const metadata = {
  title: "HeyGen avatars 2026",
  description: "Ce que fait HeyGen en 2026, ce qu'il coûte, et quand il vaut mieux autre chose.",
};

export default function Page() { return <article>Le test.</article>; }
"""

TREE = [
    "package.json", "next.config.js",
    "app/layout.tsx", "app/page.tsx",
    "app/blog/page.tsx",
    "app/blog/premier-article/page.tsx",
    "app/blog/second-article/page.tsx",
]

# Le rival traite deux sujets : l'un que le site couvre déjà, l'autre non.
RIVAL_PAGES = [
    {"url": "https://rival.fr/blog/kling-ai-prix-2026", "status_code": 200,
     "title": "Kling AI prix 2026 : crédits, plans et coût réel", "h1": ["Kling AI prix 2026"]},
    {"url": "https://rival.fr/blog/heygen-avatars-2026", "status_code": 200,
     "title": "HeyGen avatars 2026 : test complet", "h1": ["HeyGen avatars 2026"]},
]
OWN_PAGES = [
    {"url": "https://site.fr/blog/kling-ai-prix-2026", "status_code": 200,
     "title": "Kling AI prix 2026 : crédits et plans", "h1": ["Kling AI prix 2026"]},
    {"url": "https://site.fr/", "status_code": 200, "title": "Accueil", "h1": ["Accueil"]},
]
ROUTE_ATTENDUE = "/blog/heygen-avatars-2026-test-complet"


@pytest.fixture()
def projet(monkeypatch):
    """Un projet Pro, dépôt connecté, un rival analysé, un crawl de son propre site."""
    app_module.DB.create_tables()
    tag = uuid.uuid4().hex[:8]
    slug = f"site-{tag}"
    with app_module.DB.session() as db:
        user = User(email=f"client-{tag}@exemple.fr",
                    password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(user)
        db.commit()
        db.refresh(user)
        uid = str(user.id)
        proj = Project(owner_user_id=uid, slug=slug, site_name="site.fr",
                       base_url="https://site.fr/",
                       settings={"github_repo": "client/site.fr", "github_branch": "main",
                                 "content_auto": {"enabled": True, "section": "/blog",
                                                  "last_run": 0}})
        db.add(proj)
        db.commit()
        db.refresh(proj)
        pid = str(proj.id)
        db.add(CompetitorSite(project_id=pid, user_id=uid, domain="rival.fr",
                              base_url="https://rival.fr", status="ready",
                              pages=RIVAL_PAGES, pages_count=len(RIVAL_PAGES)))
        db.commit()

    # LE BALAYAGE EST GLOBAL — c'est un cron, il passe sur tout le parc. Les projets laissés
    # par les autres fichiers de test (et par les tests précédents de celui-ci) vivent dans la
    # même base SQLite : leurs réglages entreraient dans les compteurs qu'on vérifie ici, et
    # les tests passeraient ou échoueraient selon l'ORDRE d'exécution.
    with app_module.DB.session() as db:
        for autre in db.scalars(select(Project)):
            if str(autre.id) == pid:
                continue
            reglages = dict(autre.settings or {})
            if reglages.get("content_auto"):
                reglages["content_auto"] = {**reglages["content_auto"], "enabled": False}
                autre.settings = reglages
        db.commit()
    monkeypatch.setattr(app_module, "_effective_user_connection_value",
                        lambda **kw: ("ghp_test_token", "user"))
    monkeypatch.setattr(app_module.dash, "list_project_crawls", lambda *a, **kw: ["20260920-101010"])
    monkeypatch.setattr(app_module.dash, "load_report_json", lambda *a, **kw: {"pages": OWN_PAGES})
    return slug, pid, uid


@pytest.fixture()
def plan(monkeypatch):
    etat = {"plan": "pro", "restant": 4, "debits": []}
    monkeypatch.setattr(app_module.billing, "effective_plan_key", lambda *a, **kw: etat["plan"])
    monkeypatch.setattr(app_module.billing, "remaining_quota", lambda *a, **kw: etat["restant"])
    monkeypatch.setattr(app_module.billing, "usage_add", lambda *a, **kw: etat["debits"].append(kw))
    return etat


@pytest.fixture()
def github(monkeypatch):
    calls: dict[str, list] = {"put": [], "post": [], "tree": list(TREE),
                              "fichiers": {"app/blog/premier-article/page.tsx": SOEUR,
                                           "app/blog/second-article/page.tsx": SOEUR,
                                           "app/blog/page.tsx": INDEX_MANUEL}}

    def _get(path, **kw):
        if "/git/trees/" in path:
            return {"tree": [{"path": p, "type": "blob"} for p in calls["tree"]]}
        if "/git/ref/" in path or "/git/refs/heads/" in path:
            return {"object": {"sha": "base-sha"}}
        if "/contents/" in path:
            chemin = path.split("/contents/", 1)[1]
            contenu = calls["fichiers"].get(chemin)
            if contenu is None:
                raise RuntimeError(f"GitHub 404: {chemin}")
            return {"content": base64.b64encode(contenu.encode()).decode(), "sha": "sha-" + chemin}
        raise AssertionError(f"GET inattendu {path}")

    def _post(path, **kw):
        calls["post"].append((path, kw.get("json_body") or {}))
        if path.endswith("/pulls"):
            return {"html_url": "https://github.com/client/site.fr/pull/91", "number": 91,
                    "node_id": "PR_node", "head": {"sha": "head-sha"}}
        return {"ok": True}

    def _put(path, **kw):
        body = kw.get("json_body") or {}
        calls["put"].append((path.split("/contents/", 1)[1],
                             base64.b64decode(body.get("content", "")).decode()))
        return {"content": {"sha": "new-sha"}}

    monkeypatch.setattr(app_module, "_github_api_get", _get)
    monkeypatch.setattr(app_module, "_github_api_post", _post)
    monkeypatch.setattr(app_module, "_github_api_put", _put)
    monkeypatch.setattr(app_module, "_github_pr_is_open", lambda *a, **kw: True)
    return calls


@pytest.fixture()
def modele(monkeypatch):
    vus: list[str] = []

    def _ai(*, system, user_msg, **kw):
        vus.append(user_msg)
        return {"contenu": REDIGE}

    monkeypatch.setattr(app_module, "_correction_ai_json", _ai)
    return vus


def _eteindre(pid: str, **champs) -> None:
    with app_module.DB.session() as db:
        p = db.get(Project, pid)
        reglages = dict(p.settings or {})
        reglages["content_auto"] = {**(reglages.get("content_auto") or {}), **champs}
        p.settings = reglages
        db.commit()


def _auto(pid: str) -> dict:
    with app_module.DB.session() as db:
        return app_module._reglages_contenu_auto(db.get(Project, pid).settings)


# ── ce qu'il écrit ────────────────────────────────────────────────────────────────────────────

def test_un_sujet_NON_COUVERT_devient_une_page(projet, plan, github, modele) -> None:
    slug, pid, _uid = projet
    res = app_module._balayer_contenu_auto()
    assert res["proposees"] == 1, res
    ecrits = dict(github["put"])
    assert "app/blog/heygen-avatars-2026-test-complet/page.tsx" in ecrits, ecrits
    assert "app/blog/page.tsx" in ecrits, "la page neuve n'a pas été liée à sa section"


def test_le_sujet_DEJA_COUVERT_n_est_jamais_repris(projet, plan, github, modele) -> None:
    """Kling AI est traité des deux côtés : écrire une seconde page dessus, c'est se
    cannibaliser — deux pages du site se disputent la requête et aucune ne gagne."""
    slug, pid, _uid = projet
    app_module._balayer_contenu_auto()
    assert not any("kling" in chemin for chemin, _c in github["put"]), github["put"]


def test_la_page_ecrite_TOUTE_SEULE_part_quand_meme_en_brouillon(projet, plan, github, modele) -> None:
    slug, pid, _uid = projet
    app_module._balayer_contenu_auto()
    pulls = [b for p, b in github["post"] if p.endswith("/pulls")]
    assert len(pulls) == 1 and pulls[0].get("draft") is True, pulls


def test_l_article_ecrit_TOUT_SEUL_est_facture_comme_les_autres(projet, plan, github, modele) -> None:
    """Un mode automatique gratuit serait un mode automatique sans plafond."""
    slug, pid, uid = projet
    app_module._balayer_contenu_auto()
    assert [d["metric"] for d in plan["debits"]] == ["ai_articles_month"], plan["debits"]
    assert plan["debits"][0]["amount"] == 1
    assert plan["debits"][0]["meta"]["motif"] == "content_auto", plan["debits"][0]["meta"]


# ── ce qu'il refuse ───────────────────────────────────────────────────────────────────────────

def test_un_projet_qui_n_a_RIEN_DEMANDE_n_est_pas_touche(projet, plan, github, modele) -> None:
    slug, pid, _uid = projet
    _eteindre(pid, enabled=False)
    res = app_module._balayer_contenu_auto()
    assert res["proposees"] == 0 and github["put"] == [] and plan["debits"] == []


def test_un_mode_allume_SANS_SECTION_n_ecrit_rien(projet, plan, github, modele) -> None:
    """« /blog » contre « /guides » n'est pas une chose qui se devine, et personne ne relira
    l'adresse avant la pull request."""
    slug, pid, _uid = projet
    _eteindre(pid, section="")
    res = app_module._balayer_contenu_auto()
    assert res["proposees"] == 0 and github["put"] == []


def _un_second_sujet(pid: str) -> None:
    """Un SECOND sujet non couvert, sans quoi les deux tests ci-dessous sont vides.

    Avec un seul sujet, le second passage ne donne rien — mais pour la mauvaise raison : le
    premier vient d'être proposé, et un sujet proposé ne revient jamais. Une mutation
    désactivant la cadence hebdomadaire y a SURVÉCU. Avec deux sujets, seule la cadence peut
    encore arrêter le second passage, et c'est elle qu'on mesure.
    """
    with app_module.DB.session() as db:
        rival = db.scalar(select(CompetitorSite).where(CompetitorSite.project_id == pid))
        rival.pages = list(RIVAL_PAGES) + [{
            "url": "https://rival.fr/blog/synthesia-2026", "status_code": 200,
            "title": "Synthesia 2026 : avis et tarifs", "h1": ["Synthesia 2026"]}]
        db.commit()


def _pages_neuves(github) -> set[str]:
    """Les fichiers CRÉÉS par le balayage, l'index de section mis à part."""
    return {chemin for chemin, _c in github["put"] if chemin != "app/blog/page.tsx"}


def test_une_page_PAR_SEMAINE_au_plus(projet, plan, github, modele) -> None:
    slug, pid, _uid = projet
    _un_second_sujet(pid)
    assert app_module._balayer_contenu_auto()["proposees"] == 1
    github["put"].clear()
    assert app_module._balayer_contenu_auto()["proposees"] == 0
    assert github["put"] == [], "un second passage a écrit dans la même semaine"


def test_la_semaine_ECOULEE_rouvre_le_passage(projet, plan, github, modele) -> None:
    """Le témoin de l'assertion ci-dessus : sans lui, un mode définitivement bloqué la
    satisferait aussi."""
    slug, pid, _uid = projet
    _un_second_sujet(pid)
    assert app_module._balayer_contenu_auto()["proposees"] == 1
    premiere = _pages_neuves(github)
    github["put"].clear()
    _eteindre(pid, last_run=time.time() - 8 * 86400)
    assert app_module._balayer_contenu_auto()["proposees"] == 1
    seconde = _pages_neuves(github)
    assert seconde and not (seconde & premiere), (premiere, seconde)


def test_un_sujet_DEJA_PROPOSE_ne_revient_pas(projet, plan, github, modele) -> None:
    """Même si sa pull request a été fermée sans être fusionnée : le client a dit non une
    fois, le lui reproposer chaque semaine serait du harcèlement facturé."""
    slug, pid, uid = projet
    with app_module.DB.session() as db:
        db.add(IssueTask(project_id=pid, user_id=uid, issue_key=app_module._CONTENT_PAGE_KEY,
                         issue_label="Page rédigée", crawl_ts="", url=ROUTE_ATTENDUE,
                         status="done", severity="notice", note=json.dumps({})))
        db.commit()
    res = app_module._balayer_contenu_auto()
    assert res["sans_sujet"] == 1 and github["put"] == [], res


def test_un_QUOTA_EPUISE_ne_consomme_pas_la_semaine(projet, plan, github, modele) -> None:
    """Le quota repart au renouvellement ; la semaine, elle, ne revient pas. Poser la date
    ici ferait perdre au client une page qu'il a pourtant payée."""
    slug, pid, _uid = projet
    plan["restant"] = 0
    res = app_module._balayer_contenu_auto()
    assert res["hors_forfait"] == 1 and github["put"] == []
    assert _auto(pid)["last_run"] == 0, "la semaine a été consommée par un refus de quota"


def test_un_plan_sous_PRO_n_ecrit_rien(projet, plan, github, modele) -> None:
    slug, pid, _uid = projet
    plan["plan"] = "solo"
    assert app_module._balayer_contenu_auto()["hors_forfait"] == 1
    assert github["put"] == [] and plan["debits"] == []


def test_une_page_qu_on_ne_sait_pas_LIER_n_est_PAS_ecrite(projet, plan, github, modele) -> None:
    """LA différence entre les deux modes, et la seule. En manuel la pull request s'ouvre en
    disant que la page sera orpheline — une personne relit le brouillon. Ici personne ne lira
    la phrase, et une page que rien ne pointe est exactement ce que le crawler signalerait au
    passage suivant. Rien n'est écrit : pas même une branche.
    """
    slug, pid, _uid = projet
    github["fichiers"]["app/blog/page.tsx"] = INDEX_MANUEL.replace(
        "    </ul>", '      <nav><a href="/blog/premier-article">encore</a></nav>\n    </ul>')
    res = app_module._balayer_contenu_auto()
    assert res["proposees"] == 0 and res["refusees"] == 1, res
    assert github["put"] == [] and plan["debits"] == []
    assert not any(p.endswith("/git/refs") for p, _b in github["post"]), "une branche a été créée"


def test_sans_crawl_de_son_PROPRE_site_rien_n_est_ecrit(projet, plan, github, modele, monkeypatch) -> None:
    """Sans un côté de la comparaison, TOUT sujet paraîtrait non couvert : l'agent écrirait
    des pages que le client possède déjà."""
    slug, pid, _uid = projet
    monkeypatch.setattr(app_module.dash, "list_project_crawls", lambda *a, **kw: [])
    res = app_module._balayer_contenu_auto()
    assert res["sans_sujet"] == 1 and github["put"] == [], res


def test_un_ECHEC_consomme_quand_meme_la_semaine(projet, plan, github, modele) -> None:
    """Sinon un projet dont le placement est refusé est réessayé à chaque passage : le coût
    d'un refus est petit, mais il se paierait à chaque tour et pour toujours."""
    slug, pid, _uid = projet
    _eteindre(pid, section="/section-inexistante")
    res = app_module._balayer_contenu_auto()
    assert res["refusees"] == 1 and github["put"] == [], res
    assert _auto(pid)["last_run"] > 0, "le refus sera rejoué à chaque passage"


# ── l'interrupteur ────────────────────────────────────────────────────────────────────────────

@pytest.fixture()
def client_connecte(projet):
    slug, pid, uid = projet
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, pid


def _regler(client, slug, **body):
    client.get(f"/projects/{slug}/content")
    token = client.cookies.get(app_module._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/content/auto", json=body,
                       headers={app_module._CSRF_HEADER_NAME: token})


def test_allumer_SANS_SECTION_est_refuse(client_connecte, plan) -> None:
    client, slug, pid = client_connecte
    r = _regler(client, slug, enabled=True, section="")
    assert r.status_code == 400, r.text
    assert "section" in r.json()["error"]


def test_l_interrupteur_se_regle_et_se_relit(client_connecte, plan) -> None:
    client, slug, pid = client_connecte
    assert _regler(client, slug, enabled=True, section="guides").status_code == 200
    assert _auto(pid) == {"enabled": True, "section": "/guides", "last_run": 0}
    page = client.get(f"/projects/{slug}/content").text
    assert 'value="/guides"' in page and 'id="c-auto" checked' in page, page[:200]


def test_ETEINDRE_puis_RALLUMER_ne_rend_pas_une_page_de_plus(client_connecte, plan, github, modele) -> None:
    """La date de dernier passage survit à l'extinction. Sinon l'interrupteur devient un
    bouton « écris-moi une page maintenant », gratuit et sans limite de cadence."""
    client, slug, pid = client_connecte
    app_module._balayer_contenu_auto()
    avant = _auto(pid)["last_run"]
    assert avant > 0
    assert _regler(client, slug, enabled=False, section="/blog").status_code == 200
    assert _regler(client, slug, enabled=True, section="/blog").status_code == 200
    assert _auto(pid)["last_run"] == avant, _auto(pid)


def test_un_plan_sous_PRO_ne_peut_pas_ALLUMER(client_connecte, plan) -> None:
    client, slug, pid = client_connecte
    plan["plan"] = "solo"
    assert _regler(client, slug, enabled=True, section="/blog").status_code == 402
    assert _auto(pid)["enabled"] is True, "l'état d'origine a été modifié par un refus"


def test_un_plan_sous_PRO_peut_toujours_ETEINDRE(client_connecte, plan) -> None:
    """Un client qui rétrograde doit pouvoir couper ce qui tourne chez lui. Lui opposer la
    porte du forfait pour ça serait absurde."""
    client, slug, pid = client_connecte
    plan["plan"] = "solo"
    assert _regler(client, slug, enabled=False, section="/blog").status_code == 200
    assert _auto(pid)["enabled"] is False


# ── le cron ───────────────────────────────────────────────────────────────────────────────────

def test_le_cron_REFUSE_sans_le_secret() -> None:
    client = TestClient(app)
    assert client.post("/cron/auto-content").status_code == 401
    assert client.post("/cron/auto-content",
                       headers={"Authorization": "Bearer mauvais"}).status_code == 401


def test_le_cron_est_ATTEIGNABLE_avec_le_secret(projet, plan, github, modele) -> None:
    """La route doit être dans les DEUX listes de dispense — CSRF et authentification. N'être
    que dans une la rend inatteignable sans que rien ne le dise : vécu le 19/09/2026."""
    client = TestClient(app)
    r = client.post("/cron/auto-content",
                    headers={"Authorization": "Bearer %s" % os.environ["CRON_SECRET"]})
    assert r.status_code == 200, r.text
    assert r.json()["resultats"]["proposees"] == 1, r.text


def test_l_AUTOPILOTE_balaie_aussi() -> None:
    """Le filet : oublier une ligne d'ordonnanceur ne doit pas éteindre la fonction en
    silence. Même forme que le balayage des vérifications."""
    import ast as _ast

    source = Path(app_module.__file__).read_text(encoding="utf-8")
    arbre = _ast.parse(source)
    dedans = False
    for n in _ast.walk(arbre):
        if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef)) and n.name == "cron_autopilot":
            dedans = any(isinstance(c, _ast.Call)
                         and _ast.unparse(c.func) == "_balayer_contenu_auto"
                         for c in _ast.walk(n))
    assert dedans, "l'autopilote ne déclenche plus le mode automatique de rédaction"


def test_les_deux_modes_partagent_leur_COEUR() -> None:
    """Deux implémentations du même geste divergeraient, et c'est celle que personne ne
    regarde qui finirait par écrire n'importe quoi chez un client."""
    import ast as _ast

    arbre = _ast.parse(Path(app_module.__file__).read_text(encoding="utf-8"))
    appelants = set()
    portees = [(n.lineno, n.end_lineno or n.lineno, n.name) for n in _ast.walk(arbre)
               if isinstance(n, (_ast.FunctionDef, _ast.AsyncFunctionDef))]
    for n in _ast.walk(arbre):
        if isinstance(n, _ast.Call) and _ast.unparse(n.func) == "_proposer_une_page":
            englobantes = [p for p in portees if p[0] <= n.lineno <= p[1]]
            if englobantes:
                appelants.add(max(englobantes, key=lambda p: p[0])[2])
    assert appelants == {"api_content_draft", "_balayer_contenu_auto"}, appelants


def test_un_projet_SANS_reglage_ne_plante_pas_le_balayage(projet, plan, github, modele) -> None:
    """Les projets créés avant cette fonctionnalité n'ont pas la clé. Une exception ici
    arrêterait le balayage pour tous les autres."""
    slug, pid, uid = projet
    with app_module.DB.session() as db:
        p = db.get(Project, pid)
        p.settings = {k: v for k, v in (p.settings or {}).items() if k != "content_auto"}
        db.commit()
        db.add(Project(owner_user_id=uid, slug=f"vide-{uuid.uuid4().hex[:8]}",
                       site_name="vide", base_url="https://vide.fr/", settings=None))
        db.commit()
    assert app_module._balayer_contenu_auto() == {
        "proposees": 0, "refusees": 0, "sans_sujet": 0, "hors_forfait": 0}


def test_le_balayage_reste_BORNE(projet, plan, github, modele) -> None:
    """`limit` borne le nombre de projets traités par passage : sans elle, un parc qui grandit
    ferait grandir la durée d'un tour de cron sans que personne ne le décide."""
    slug, pid, _uid = projet
    with app_module.DB.session() as db:
        assert db.scalars(select(Project)).all(), "la fixture n'a pas créé de projet"
    res = app_module._balayer_contenu_auto(limit=0)
    assert res["proposees"] == 0 and github["put"] == [], res
