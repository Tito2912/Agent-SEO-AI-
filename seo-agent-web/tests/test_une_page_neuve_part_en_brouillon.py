# -*- coding: utf-8 -*-
"""La route qui assemble les trois briques : où poser, quoi écrire, quoi lier.

C'est le premier endroit du produit qui CRÉE un fichier chez un client au lieu d'en modifier un.
La différence n'est pas de degré : un correcteur borné remplace une valeur dont il a lu
l'ancienne, ici tout le texte vient d'un modèle et rien ne le contredit. Ce que ces tests
défendent est donc surtout ce que la route REFUSE et ce qu'elle DIT :

* un plan en dessous de Pro, ou un quota épuisé, n'atteint jamais GitHub — ni appel, ni branche ;
* une adresse déjà servie par le site n'est pas réécrite : `placement_pour_route` refuse, et le
  refus sort avant la première ligne engendrée ;
* un modèle qui oublie une clé de tête ne fait pas partir de pull request — ce fichier-là
  casserait le build du client, et aucun réécriveur borné ne rattrape cette famille ;
* la page est LIÉE depuis l'index de sa section, et quand elle ne peut pas l'être le corps de
  la PR le dit en toutes lettres plutôt que de laisser une orpheline passer pour un succès ;
* la PR est un BROUILLON et ne fusionne jamais, même sur un projet en « Full Access » ;
* un article vaut UNE unité, pas une par fichier écrit — l'index modifié en passant n'est pas
  un second article.

GitHub et le modèle sont bouchonnés : ce qui est sous test, c'est la décision, pas le transport.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-contenu-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as app_module  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import Project, User  # noqa: E402

ROUTE = "/blog/mon-sujet"
SUJET = "Comment choisir un outil de veille SEO"

SOEUR = """export const metadata = {
  title: "Premier article",
  description: "Le premier article du blog, et le gabarit de tous les autres.",
};

export default function Page() { return <article>Texte.</article>; }
"""

# Une liste écrite A LA MAIN : l'index cite le slug de ses pages, donc la page neuve doit y être
# ajoutée sous peine de rester orpheline.
INDEX_MANUEL = """export default function Blog() {
  return (
    <ul>
      <li><a href="/blog/premier-article">Premier article</a></li>
      <li><a href="/blog/second-article">Second article</a></li>
    </ul>
  );
}
"""

# La même section, mais dont la liste est engendrée en lisant le dossier : rien à ajouter.
INDEX_ENGENDRE = """import { tousLesArticles } from "@/lib/posts";

export default function Blog() {
  return <ul>{tousLesArticles().map((a) => <li key={a.id}>{a.titre}</li>)}</ul>;
}
"""

# LE CANONICAL EST FAUX EXPRES : il perd le segment /blog, exactement comme les deux pages que
# le banc des neuf idiomes a produites le 20/09/2026. La route doit le remettre avant de
# commiter, et c'est ce que `test_le_canonical_faux_est_CORRIGE_avant_le_commit` mesure.
REDIGE = """export const metadata = {
  title: "Choisir un outil de veille SEO",
  description: "Les critères qui comptent vraiment quand on compare des outils de veille SEO.",
  alternates: {
    canonical: "https://site.fr/choisir-un-outil-de-veille-seo",
  },
};

export default function Page() { return <article>Le comparatif.</article>; }
"""

TREE = [
    "package.json", "next.config.js",
    "app/layout.tsx", "app/page.tsx",
    "app/blog/page.tsx",
    "app/blog/premier-article/page.tsx",
    "app/blog/second-article/page.tsx",
]


@pytest.fixture()
def customer(monkeypatch):
    """Un compte Pro, non-admin, avec un dépôt connecté — exactement le cas réel."""
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
                                 "github_mode": "review"})
        db.add(proj)
        db.commit()
        db.refresh(proj)
        pid = str(proj.id)
    monkeypatch.setattr(app_module, "_effective_user_connection_value",
                        lambda **kw: ("ghp_test_token", "user"))
    client = TestClient(app)
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=uid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return client, slug, pid, uid


@pytest.fixture()
def plan(monkeypatch):
    """Le plan et le quota d'articles, réglables par test. Les débits sont enregistrés.

    `plan_rank` et la comparaison à zéro restent le vrai code : ce qui est bouchonné, c'est le
    stockage, pas la décision.

    LES DEUX LECTURES DÉPENDENT DU COMPTE, et ce n'est pas un raffinement gratuit. Une première
    version rendait « pro » à qui le demandait : une mutation faisant lire le plan de la
    personne CONNECTÉE au lieu du compte payeur y a SURVÉCU, parce qu'avec un bouchon aveugle
    les deux comptes donnent la même réponse. Un bouchon qui ne distingue pas ne mesure pas.
    """
    etat = {"plan": "pro", "restant": 4, "plans": {}, "restants": {}, "debits": []}
    monkeypatch.setattr(app_module.billing, "effective_plan_key",
                        lambda *a, **kw: etat["plans"].get(kw.get("user_id"), etat["plan"]))
    monkeypatch.setattr(app_module.billing, "remaining_quota",
                        lambda *a, **kw: etat["restants"].get(kw.get("user_id"), etat["restant"]))
    monkeypatch.setattr(app_module.billing, "usage_add",
                        lambda *a, **kw: etat["debits"].append(kw))
    return etat


@pytest.fixture()
def github(monkeypatch):
    """Bouchonne l'API GitHub et enregistre chaque écriture, pour pouvoir la relire."""
    calls: dict[str, list] = {"put": [], "post": [], "get": [], "tree": list(TREE),
                              "fichiers": {"app/blog/premier-article/page.tsx": SOEUR,
                                           "app/blog/second-article/page.tsx": SOEUR,
                                           "app/blog/page.tsx": INDEX_MANUEL}}

    def _get(path, **kw):
        calls["get"].append(path)
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
            return {"html_url": "https://github.com/client/site.fr/pull/77", "number": 77,
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
    """Le modèle rend une page ; le code est ce qui doit la placer, la vérifier et la lier."""
    vus: list[dict] = []

    def _ai(*, system, user_msg, **kw):
        vus.append({"system": system, "user": user_msg})
        return {"contenu": _ai.sortie}

    _ai.sortie = REDIGE
    monkeypatch.setattr(app_module, "_correction_ai_json", _ai)
    return _ai, vus


def _demander(client, slug, **body):
    """Un POST JSON comme la page le fait : le jeton CSRF voyage dans l'en-tête."""
    client.get(f"/projects/{slug}")
    token = client.cookies.get(app_module._CSRF_COOKIE_NAME, "")
    return client.post(f"/api/projects/{slug}/content/draft", json=body,
                       headers={app_module._CSRF_HEADER_NAME: token})


# ── le chemin nominal ─────────────────────────────────────────────────────────────────────────

def test_la_page_est_ecrite_au_bon_endroit_et_LIEE_depuis_sa_section(customer, plan, github, modele) -> None:
    client, slug, _pid, _uid = customer
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["ok"] and data["pr_url"].endswith("/77")

    ecrits = dict(github["put"])
    assert set(ecrits) == {"app/blog/mon-sujet/page.tsx", "app/blog/page.tsx"}, ecrits
    assert "Choisir un outil de veille SEO" in ecrits["app/blog/mon-sujet/page.tsx"]
    assert data["orpheline"] is False


def test_le_canonical_faux_est_CORRIGE_avant_le_commit(customer, plan, github, modele) -> None:
    """Le bout en bout du garde-fou d'adresse, mesuré sur le fichier réellement commité.

    Un test de structure — « l'appel porte-t-il bien `url_de_la_page=` ? » — ne suffit pas :
    une mutation qui remplace la VALEUR par une chaîne vide y survit, le mot-clé étant toujours
    là. C'est ce qui est arrivé le 20/09/2026. Seule la lecture du fichier poussé le prouve.
    """
    client, slug, _pid, _uid = customer
    assert _demander(client, slug, sujet=SUJET, route=ROUTE).status_code == 200
    ecrit = dict(github["put"])["app/blog/mon-sujet/page.tsx"]
    assert '"https://site.fr/blog/mon-sujet"' in ecrit, ecrit
    assert "https://site.fr/choisir-un-outil-de-veille-seo" not in ecrit, ecrit


def test_la_pr_DIT_que_l_adresse_a_ete_reprise(customer, plan, github, modele) -> None:
    """Un diff muet se relit mal : le relecteur doit savoir que le modèle avait écrit autre
    chose, sinon il croit lire ce que le modèle a produit."""
    client, slug, _pid, _uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    corps = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    assert "Adresses reprises" in corps, corps
    assert "canonical" in corps, corps


def test_l_entree_d_index_est_CLONEE_sur_celle_d_une_soeur(customer, plan, github, modele) -> None:
    """Pas composée depuis un gabarit : recopiée, slug et libellé remplacés."""
    client, slug, _pid, _uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    index = dict(github["put"])["app/blog/page.tsx"]
    assert '<li><a href="/blog/mon-sujet">Choisir un outil de veille SEO</a></li>' in index
    assert '<li><a href="/blog/premier-article">Premier article</a></li>' in index, \
        "l'entrée d'origine est intacte"


def test_la_forme_montree_au_modele_est_celle_d_une_PAGE_du_depot(customer, plan, github, modele) -> None:
    """La méthode du projet : on ne décrit pas un gabarit, on montre la page d'à côté."""
    client, slug, _pid, _uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    _ai, vus = modele
    assert len(vus) == 1, vus
    demande = vus[0]["user"]
    assert "Premier article" in demande, "la sœur elle-même est dans la demande"
    assert SUJET in demande


def test_la_pr_est_un_BROUILLON_meme_en_full_access(customer, plan, github, modele) -> None:
    """Tout le texte vient d'un modèle. Le mode du projet ne rachète pas ça."""
    client, slug, pid, _uid = customer
    with app_module.DB.session() as db:
        proj = db.get(Project, pid)
        proj.settings = {**proj.settings, "github_mode": "auto"}
        db.commit()
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 200, r.text
    pulls = [b for p, b in github["post"] if p.endswith("/pulls")]
    assert len(pulls) == 1 and pulls[0].get("draft") is True, pulls
    assert not any("merge" in p for p, _b in github["post"]), github["post"]


def test_la_pr_dit_que_le_texte_vient_d_un_modele(customer, plan, github, modele) -> None:
    """Un relecteur qui croit lire un correctif mécanique fusionne sans lire."""
    client, slug, _pid, _uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    corps = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    assert "écrit par un modèle" in corps, corps
    assert "app/blog/premier-article/page.tsx" in corps, "la sœur imitée est nommée"


# ── le compteur ───────────────────────────────────────────────────────────────────────────────

def test_un_article_vaut_UNE_unite_et_pas_une_par_fichier(customer, plan, github, modele) -> None:
    """Deux fichiers partent dans cette PR ; l'index modifié en passant n'est pas un article."""
    client, slug, _pid, _uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert len(dict(github["put"])) == 2, "le test ne dit rien si un seul fichier part"
    assert [d["amount"] for d in plan["debits"]] == [1], plan["debits"]


def test_le_debit_passe_par_le_compteur_d_ARTICLES(customer, plan, github, modele) -> None:
    """Décision du propriétaire : un compteur à part. Mélanger les deux rendrait illisibles
    les deux quotas — publier deux articles ne doit pas coûter ses corrections du mois."""
    client, slug, _pid, _uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert plan["debits"][0]["metric"] == "ai_articles_month", plan["debits"]


def test_le_debit_dit_POURQUOI_il_a_eu_lieu(customer, plan, github, modele) -> None:
    """Un débit anonyme ne se rembourse qu'en lisant des heures de journaux."""
    client, slug, _pid, uid = customer
    _demander(client, slug, sujet=SUJET, route=ROUTE)
    meta = plan["debits"][0]["meta"]
    assert meta["slug"] == slug and meta["motif"] == "content_draft" and meta["par"] == uid, meta


def _invite(client, hote: str) -> str:
    """Un consultant rejoint le compte de l'agence, et prend la main sur le client HTTP."""
    from backend.models import AccountMember

    with app_module.DB.session() as db:
        membre = User(email=f"consultant-{uuid.uuid4().hex[:8]}@exemple.fr",
                      password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(membre)
        db.commit()
        db.refresh(membre)
        mid = str(membre.id)
        db.add(AccountMember(owner_user_id=hote, member_user_id=mid))
        db.commit()
    client.cookies.set(auth.SESSION_COOKIE_NAME,
                       auth.make_session_token(user_id=mid, secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return mid


def test_l_article_d_un_MEMBRE_est_paye_par_le_compte_HOTE(customer, plan, github, modele) -> None:
    """Le comportement derrière le nom de variable `payeur` dans la porte et dans le débit.

    Un consultant invité sur le compte d'une agence écrit sur le projet de l'agence : c'est
    l'agence qui paie, et `par` dit qui a cliqué. Sans ce test, renommer une variable suffirait
    à faire taire la garde qui énumère les facturations sans rien garantir.
    """
    client, slug, _pid, hote = customer
    mid = _invite(client, hote)
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 200, r.text
    debit = plan["debits"][0]
    assert debit["user_id"] == hote, "le membre a été débité au lieu du compte hôte"
    assert debit["meta"]["par"] == mid, debit["meta"]


def test_c_est_le_plan_de_l_HOTE_qui_ouvre_la_porte_pas_celui_du_membre(customer, plan, github, modele) -> None:
    """Un consultant au forfait Gratuit travaille sur le projet d'une agence Pro : il passe.

    L'autre moitié compte autant et c'est elle que ce test mesure d'abord — un consultant Pro
    ne doit PAS déverrouiller le projet d'un compte Gratuit, sinon il suffirait d'inviter
    quelqu'un pour s'offrir la fonction.
    """
    client, slug, _pid, hote = customer
    mid = _invite(client, hote)
    plan["plans"] = {hote: "free", mid: "business"}
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 402, r.text
    assert github["get"] == [] and plan["debits"] == []

    plan["plans"] = {hote: "pro", mid: "free"}
    assert _demander(client, slug, sujet=SUJET, route=ROUTE).status_code == 200


def test_c_est_le_quota_de_l_HOTE_qui_est_lu_pas_celui_du_membre(customer, plan, github, modele) -> None:
    """Même raison : le solde qu'on consulte doit être celui qu'on va débiter, sans quoi le
    compte hôte part en négatif sans qu'aucune porte ne se ferme."""
    client, slug, _pid, hote = customer
    mid = _invite(client, hote)
    plan["restants"] = {hote: 0, mid: 99}
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 402, r.text
    assert plan["debits"] == []


# ── ce que la route REFUSE ────────────────────────────────────────────────────────────────────

def test_un_plan_sous_PRO_n_atteint_jamais_github(customer, plan, github, modele) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "solo"
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 402, r.text
    assert github["get"] == [] and github["put"] == [] and plan["debits"] == []


def test_un_quota_EPUISE_n_atteint_jamais_github(customer, plan, github, modele) -> None:
    client, slug, _pid, _uid = customer
    plan["restant"] = 0
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 402, r.text
    assert github["get"] == [] and github["put"] == [] and plan["debits"] == []


def test_une_adresse_DEJA_SERVIE_n_est_pas_reecrite(customer, plan, github, modele) -> None:
    """Le refus vient de `placement_pour_route` et sort AVANT la première ligne engendrée."""
    client, slug, _pid, _uid = customer
    r = _demander(client, slug, sujet=SUJET, route="/blog/premier-article")
    assert r.status_code == 422, r.text
    # Le fichier qui sert déjà cette route est NOMMÉ : « existe déjà » seul se lit aussi dans
    # le refus voisin « le fichier … existe deja sans servir cette route », qui a une tout
    # autre cause.
    assert "app/blog/premier-article/page.tsx" in r.json()["error"], r.text
    _ai, vus = modele
    assert vus == [], "le modèle a été appelé pour une page qui existe déjà"
    assert github["put"] == [] and plan["debits"] == []


def test_une_section_SANS_SOEUR_fait_s_abstenir(customer, plan, github, modele) -> None:
    """Le tout premier article d'un blog : la convention du site n'existe pas encore."""
    client, slug, _pid, _uid = customer
    r = _demander(client, slug, sujet=SUJET, route="/guides/mon-sujet")
    assert r.status_code == 422, r.text
    assert "soeur" in r.json()["error"] or "sœur" in r.json()["error"], r.text
    assert github["put"] == [] and plan["debits"] == []


def test_une_cle_de_tete_MANQUANTE_ne_part_pas_en_pr(customer, plan, github, modele) -> None:
    """Ce fichier casserait le build du client, et aucun réécriveur borné ne rattrape ça.

    Surtout : pas de branche créée et pas de débit. Facturer une page qu'on refuse d'écrire
    serait la pire des deux moitiés.
    """
    client, slug, _pid, _uid = customer
    _ai, _vus = modele
    _ai.sortie = ('export const metadata = {\n  title: "Choisir un outil de veille SEO",\n};\n\n'
                  "export default function Page() { return <article>x</article>; }\n")
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 422, r.text
    assert "description" in r.json()["error"], r.text
    assert github["put"] == [] and plan["debits"] == []
    assert not any(p.endswith("/git/refs") for p, _b in github["post"]), "une branche a été créée"


def test_une_page_qui_ne_se_RELIT_PAS_ne_part_pas_en_pr(customer, plan, github, modele) -> None:
    """Le même jeu de contrôles que les deux chemins qui commitent déjà.

    Le déséquilibre est posé APRÈS le bloc `metadata`, volontairement : l'abîmer aurait fait
    échouer le contrôle des clés de tête à la place, et ce test aurait passé pour la mauvaise
    raison en prétendant mesurer `_refus_de_format`.
    """
    client, slug, _pid, _uid = customer
    _ai, _vus = modele
    _ai.sortie = REDIGE + "\nconst inacheve = {\n"
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 422, r.text
    assert "délimiteur" in r.json()["error"] or "delimiteur" in r.json()["error"], r.text
    assert github["put"] == [] and plan["debits"] == []


def test_une_adresse_d_un_AUTRE_SITE_est_refusee(customer, plan, github, modele) -> None:
    client, slug, _pid, _uid = customer
    r = _demander(client, slug, sujet=SUJET, route="https://concurrent.fr/blog/mon-sujet")
    assert r.status_code == 400, r.text
    assert github["get"] == [] and plan["debits"] == []


def test_une_seconde_demande_pendant_qu_une_PR_est_ouverte_est_refusee(customer, plan, github, modele) -> None:
    """La seconde écrirait le même fichier et ajouterait une seconde entrée au même index."""
    client, slug, _pid, _uid = customer
    assert _demander(client, slug, sujet=SUJET, route=ROUTE).status_code == 200
    github["put"].clear()
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 409, r.text
    assert r.json()["pr_url"].endswith("/77")
    assert github["put"] == [] and len(plan["debits"]) == 1, "la seconde a débité"


# ── l'orpheline : ce qu'on dit quand on ne sait pas lier ──────────────────────────────────────

def test_un_index_ENGENDRE_ne_se_modifie_pas_et_n_est_pas_une_orpheline(customer, plan, github, modele) -> None:
    """Un gabarit qui lit le dossier n'a besoin de rien : créer le fichier suffit."""
    client, slug, _pid, _uid = customer
    github["fichiers"]["app/blog/page.tsx"] = INDEX_ENGENDRE
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 200, r.text
    assert list(dict(github["put"])) == ["app/blog/mon-sujet/page.tsx"], github["put"]
    assert r.json()["orpheline"] is False, r.text


def test_un_index_AMBIGU_ouvre_quand_meme_mais_le_DIT(customer, plan, github, modele) -> None:
    """La sœur citée deux fois — une carte et une entrée de menu — n'a pas UNE forme à cloner.

    On ouvre quand même : la PR est un brouillon qu'une personne relit, et elle est la mieux
    placée pour savoir où l'entrée va. Lui cacher qu'il manque un lien serait pire que de ne
    rien proposer. Le mode automatique, lui, devra refuser : personne n'y lira la phrase.
    """
    client, slug, _pid, _uid = customer
    github["fichiers"]["app/blog/page.tsx"] = INDEX_MANUEL.replace(
        "    </ul>", '      <nav><a href="/blog/premier-article">encore</a></nav>\n    </ul>')
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 200, r.text
    assert r.json()["orpheline"] is True, r.text
    assert list(dict(github["put"])) == ["app/blog/mon-sujet/page.tsx"], github["put"]
    corps = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    assert "n'est liée depuis aucun index" in corps, corps


def test_un_index_ILLISIBLE_ouvre_quand_meme_mais_le_DIT(customer, plan, github, modele) -> None:
    client, slug, _pid, _uid = customer
    del github["fichiers"]["app/blog/page.tsx"]
    r = _demander(client, slug, sujet=SUJET, route=ROUTE)
    assert r.status_code == 200, r.text
    assert r.json()["orpheline"] is True, r.text
    corps = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    assert "illisible" in corps, corps


# ── l'écran : ce que le client voit avant et après ────────────────────────────────────────────

def test_un_plan_sous_PRO_voit_le_forfait_pas_le_formulaire(customer, plan) -> None:
    client, slug, _pid, _uid = customer
    plan["plan"] = "solo"
    page = client.get(f"/projects/{slug}/content").text
    assert "plans Pro et supérieurs" in page
    assert 'id="c-go"' not in page, "le bouton est affiché à qui ne peut pas s'en servir"


def test_sans_DEPOT_connecte_l_ecran_dit_quoi_faire(customer, plan) -> None:
    """« Aucun dépôt GitHub connecté » après trente secondes de rédaction serait un gâchis :
    la page neuve est un fichier, il faut un dépôt où l'écrire."""
    client, slug, pid, _uid = customer
    with app_module.DB.session() as db:
        proj = db.get(Project, pid)
        proj.settings = {k: v for k, v in proj.settings.items() if k != "github_repo"}
        db.commit()
    page = client.get(f"/projects/{slug}/content").text
    assert "dépôt GitHub" in page
    assert 'id="c-go"' not in page


def test_l_ecran_montre_le_formulaire_et_le_QUOTA_restant(customer, plan) -> None:
    client, slug, _pid, _uid = customer
    plan["restant"] = 3
    page = client.get(f"/projects/{slug}/content").text
    assert 'id="c-go"' in page and 'id="c-sujet"' in page and 'id="c-route"' in page
    assert "3 articles restants" in page, "le client ne sait pas ce qu'il lui reste"


def _ligne_du_journal(page: str, route: str) -> str:
    """La ligne de tableau de cette adresse, et rien d'autre.

    Découper sur l'adresse seule attrapait le `placeholder="/blog/mon-sujet"` du formulaire,
    plus haut dans la page : mes deux premières assertions lisaient donc le formulaire en
    croyant lire le journal. C'est la cinquième fois de ce chantier qu'une sous-chaîne trop
    large fait dire à un test autre chose que ce qu'il annonce.
    """
    cellule = '<td class="mono">%s</td>' % route
    assert cellule in page, "aucune ligne de journal pour %s" % route
    return cellule + page.split(cellule, 1)[1].split("</tr>", 1)[0]


def test_l_ecran_JOURNALISE_les_pages_deja_proposees(customer, plan, github, modele) -> None:
    """Sans cette liste, le client ne sait ni ce qu'il a déjà demandé, ni laquelle de ses pull
    requests attend encore un lien."""
    client, slug, _pid, _uid = customer
    assert _demander(client, slug, sujet=SUJET, route=ROUTE).status_code == 200
    ligne = _ligne_du_journal(client.get(f"/projects/{slug}/content").text, ROUTE)
    assert SUJET in ligne, ligne
    assert "https://github.com/client/site.fr/pull/77" in ligne and "#77" in ligne, ligne


def test_une_page_ORPHELINE_est_signalee_dans_le_journal(customer, plan, github, modele) -> None:
    """C'est le seul endroit où l'information se relit après coup : le corps de la PR le dit
    une fois, l'écran le redit tant que la PR n'est pas fusionnée."""
    client, slug, _pid, _uid = customer
    github["fichiers"]["app/blog/page.tsx"] = INDEX_MANUEL.replace(
        "    </ul>", '      <nav><a href="/blog/premier-article">encore</a></nav>\n    </ul>')
    assert _demander(client, slug, sujet=SUJET, route=ROUTE).status_code == 200
    ligne = _ligne_du_journal(client.get(f"/projects/{slug}/content").text, ROUTE)
    assert "à lier à la main" in ligne, ligne


def test_une_page_LIEE_n_est_pas_signalee_comme_orpheline(customer, plan, github, modele) -> None:
    """Le témoin de l'assertion ci-dessus : sans lui, un badge affiché partout la validerait."""
    client, slug, _pid, _uid = customer
    assert _demander(client, slug, sujet=SUJET, route=ROUTE).status_code == 200
    ligne = _ligne_du_journal(client.get(f"/projects/{slug}/content").text, ROUTE)
    assert "à lier à la main" not in ligne and "liée" in ligne, ligne


def test_le_journal_ne_montre_QUE_les_pages_ecrites(customer, plan, github, modele) -> None:
    """Les corrections ont leur écran. Les mélanger ici donnerait un journal où l'on ne
    retrouve plus ce qu'on a demandé."""
    from backend.models import IssueTask

    client, slug, pid, uid = customer
    with app_module.DB.session() as db:
        db.add(IssueTask(
            project_id=pid, user_id=uid, issue_key="missing_meta_description",
            issue_label="Une correction ordinaire", crawl_ts="",
            url="https://site.fr/une-correction", status="in_progress", severity="notice",
            note=json.dumps({"pr_url": "https://github.com/client/site.fr/pull/5",
                             "pr_number": 5}, ensure_ascii=False)))
        db.commit()
    page = client.get(f"/projects/{slug}/content").text
    assert "Une correction ordinaire" not in page and "/pull/5" not in page, \
        "une correction est apparue dans le journal des pages écrites"


def test_c_est_le_plan_de_l_HOTE_qui_ouvre_l_ECRAN(customer, plan) -> None:
    """Même règle que la porte de l'API, mesurée sur l'écran : un consultant au forfait
    Gratuit voit le formulaire sur le projet d'une agence Pro, et pas l'inverse."""
    client, slug, _pid, hote = customer
    mid = _invite(client, hote)
    plan["plans"] = {hote: "pro", mid: "free"}
    assert 'id="c-go"' in client.get(f"/projects/{slug}/content").text

    plan["plans"] = {hote: "free", mid: "business"}
    page = client.get(f"/projects/{slug}/content").text
    assert 'id="c-go"' not in page and "plans Pro et supérieurs" in page


def test_l_ecran_est_ATTEIGNABLE_depuis_la_navigation(customer, plan) -> None:
    """Une route qu'aucun lien n'atteint n'existe pas pour le client."""
    client, slug, _pid, _uid = customer
    page = client.get(f"/projects/{slug}").text
    assert f'href="/projects/{slug}/content"' in page, "aucune entrée de menu vers Contenu"


def test_le_sujet_venu_de_CONCURRENTS_pre_remplit_sans_rien_engendrer(customer, plan, github, modele) -> None:
    """Le bouton de l'écran Concurrents n'appelle pas le modèle : il ouvre ce formulaire.

    Écrire une page coûte un article du quota, et la SECTION est une décision éditoriale que
    nous ne pouvons pas deviner. Engendrer au clic depuis l'autre écran dépenserait un article
    sur une adresse que personne n'a relue.
    """
    client, slug, _pid, _uid = customer
    r = client.get(f"/projects/{slug}/content?sujet=Comparatif+des+outils")
    assert r.status_code == 200
    _ai, vus = modele
    assert vus == [], "le modèle a été appelé par un simple affichage"
    assert github["put"] == [] and plan["debits"] == []
