# -*- coding: utf-8 -*-
"""Les deux chemins qui commitent chez un client passent le MEME jeu de refus.

LE TROU. Deux routes ecrivent dans le depot d'un client : la boucle de correction etendue
(`_deep_patch_issue_files`) et la route par URL en mode « revue », qui commite le contenu
revenu du NAVIGATEUR apres l'apercu. La premiere refusait un litteral TypeScript invalide ;
la seconde ne verifiait que les formats dotes d'un analyseur — JSON, TOML, YAML, front
matter — via `_verifier_la_syntaxe`. La famille JS/TS lui echappait entierement.

Ce n'est pas theorique : un litteral d'objet casse fait echouer le build du client, et c'est
arrive — trois deploiements Netlify en echec pendant que le journal du correcteur affichait
« zero erreur ». La boucle a ete durcie a ce moment-la. L'autre chemin ne l'a jamais ete.

CE QUE CE FICHIER VERROUILLE, et c'est plus que « la route appelle la fonction » : il donne
le MEME contenu fautif aux DEUX chemins et exige que les deux refusent. Une fonction partagee
peut etre contournee par un appelant ; deux chemins qui repondent la meme chose sur les memes
entrees, non. C'est la forme de defaut que cette journee a produit quatre fois — deux endroits
qui decident de la meme question et finissent par diverger.
"""

from __future__ import annotations

import base64
import os
import sys
import tempfile
import uuid
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-refus-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

CHEMIN = "app/page.tsx"

# Chacun a fait tomber un build reel, ou en ferait tomber un.
FAUTIFS: list[tuple[str, str]] = [
    ("bloc imbrique en double",
     "export const metadata = {\n  openGraph: {\n    twitter: {\n      card: 'a',\n    },\n"
     "    twitter: {\n      card: 'b',\n    },\n  },\n};\n"),
    ("conteneur non referme",
     "export const metadata = {\n  title: 'A',\n  openGraph: {\n    url: '/x',\n};\n"),
    # Hors du litteral de TETE : `_object_literal_error` ne le voit pas, seul le controle de
    # conflits l'attrape. Ma premiere liste n'avait que des cas que le premier detecteur
    # prenait, et une mutation qui retirait le second y a survecu — le jeu de refus n'etait
    # pas couvert, seule sa premiere regle l'etait.
    ("cle en double hors du litteral de tete",
     "export const metadata = {\n  title: 'A',\n};\n\n"
     "const schema = {\n  name: {\n    fr: 'x',\n  },\n  name: {\n    fr: 'y',\n  },\n};\n"),
]

# Reparable par la boucle, refuse par la route — et c'est VOULU, voir le test dedie.
SEPARATEUR_MANQUANT = "export const metadata = {\n  title: 'A'\n  description: 'B',\n};\n"

CORRECT = ("export const metadata = {\n  title: 'Une page',\n"
           "  openGraph: { title: 'Une page', url: '/x' },\n};\n")


def _via_la_boucle(monkeypatch, contenu: str) -> tuple[list[str], list[str]]:
    """Le chemin « correction etendue » : rend (patches, refuses)."""
    monkeypatch.setattr(m, "_github_api_put",
                        lambda chemin, **kw: {"content": {"sha": "n"},
                                              "commit": {"sha": "a", "html_url": ""}})
    patched, skipped, _t, _a = m._deep_patch_issue_files(
        owner="o", repo_name="r", branch="main", token="t", fix_branch="fix",
        all_paths=[CHEMIN], issue_key="k", issue_label="L", impacted_urls=[], site_name="s",
        file_state={CHEMIN: {"sha": "v", "content": "export const metadata = {};\n"}},
        max_files=2, targets_override=[CHEMIN], allow_ai_targeting=False,
        link_rewriter=lambda _raw: (contenu, 1), rewriter_ai_fallback=False)
    return patched, skipped


def _via_la_route(contenu: str) -> str | None:
    """Le chemin « revue » : rend le message de refus, ou None si ca passerait."""
    return (m._github_patched_content_error(contenu, CHEMIN)
            or m._refus_de_format(CHEMIN, contenu))


# --- les deux refusent ---------------------------------------------------------------------

@pytest.mark.parametrize("nom, contenu", FAUTIFS)
def test_la_boucle_refuse(monkeypatch, nom, contenu) -> None:
    patched, skipped = _via_la_boucle(monkeypatch, contenu)
    assert patched == [] and skipped == [CHEMIN], "%s : %r / %r" % (nom, patched, skipped)


@pytest.mark.parametrize("nom, contenu", FAUTIFS)
def test_la_route_par_URL_refuse_AUSSI(nom, contenu) -> None:
    """Le trou que ce fichier ferme : ces trois-la passaient par ici."""
    assert _via_la_route(contenu), "%s : la route par URL laisse passer" % nom


@pytest.mark.parametrize("nom, contenu", FAUTIFS)
def test_les_deux_chemins_SONT_D_ACCORD(monkeypatch, nom, contenu) -> None:
    """La propriete qui survit a un refactor : ce n'est pas « la route appelle la fonction »,
    c'est « les deux repondent la meme chose sur la meme entree »."""
    patched, _skipped = _via_la_boucle(monkeypatch, contenu)
    assert (patched == []) == bool(_via_la_route(contenu)), nom


def test_un_contenu_CORRECT_passe_des_deux_cotes(monkeypatch) -> None:
    """Le contre-test sans lequel les precedents ne prouvent rien : un jeu de refus qui refuse
    tout serait vert partout et ne corrigerait personne."""
    patched, skipped = _via_la_boucle(monkeypatch, CORRECT)
    assert patched == [CHEMIN] and skipped == [], (patched, skipped)
    assert _via_la_route(CORRECT) is None


def test_ce_que_la_boucle_REPARE_la_route_le_REFUSE_et_c_est_voulu(monkeypatch) -> None:
    """La seule divergence admise entre les deux chemins, et elle a une raison.

    MON PREMIER TEST LA COMPTAIT COMME UN DEFAUT : j'avais mis le separateur manquant parmi
    les contenus que « les deux doivent refuser », et la boucle ne le refusait pas. Mesure
    faite, elle le REPARE — elle a des reparateurs que la route n'a pas — puis commite un
    fichier valide. C'est mieux qu'un refus, pas moins bien.

    La route, elle, commite un contenu qu'un HUMAIN vient de relire dans un apercu. Le
    reparer apres coup changerait ce qu'il a approuve. Refuser et le renvoyer est le bon
    geste.

    Ce que les deux chemins doivent partager n'est donc pas le TRAITEMENT, c'est la frontiere
    de l'inacceptable : rien ne doit partir chez le client que la boucle jetterait sans
    pouvoir le reparer.
    """
    patched, skipped = _via_la_boucle(monkeypatch, SEPARATEUR_MANQUANT)
    assert patched == [CHEMIN] and skipped == [], (patched, skipped)
    assert _via_la_route(SEPARATEUR_MANQUANT), "la route devrait renvoyer ce contenu au relecteur"


# --- les fins de ligne, des deux cotes aussi -------------------------------------------------

def test_la_route_par_URL_preserve_les_fins_de_ligne(monkeypatch) -> None:
    """Ce qui part REELLEMENT chez GitHub quand la route commite, pas ce que la fonction rend.

    MA PREMIERE VERSION APPELAIT `_respecter_les_fins_de_ligne` DIRECTEMENT, et une mutation
    qui retirait l'appel *dans la route* y a survécu : je mesurais la fonction, pas le chemin
    qui l'utilise. C'est la leçon que cette journée a répétée — tester la boucle, pas la
    fonction. Ici on traverse la route et on décode ce qui est envoyé.

    Le fichier du client est en CRLF, le contenu revenu du navigateur en LF. Sans ce respect,
    un correctif d'une ligne produit un diff de tout le fichier, et un diff illisible est un
    diff qu'on approuve sans le lire.
    """
    import re as _re

    from fastapi.testclient import TestClient

    from backend import auth
    from backend.app import app
    from backend.models import Project, User

    crlf = CORRECT.replace("\n", "\r\n")
    envoye: dict[str, str] = {}

    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="gh-%s@exemple.fr" % uuid.uuid4().hex[:8],
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        uid = str(u.id)
        slug = "site-%s" % uuid.uuid4().hex[:8]
        db.add(Project(owner_user_id=uid, slug=slug, site_name=slug,
                       base_url="https://%s.fr/" % slug,
                       settings={"github_repo": "o/r", "github_branch": "main",
                                 "github_mode": "review"}))
        db.commit()

    monkeypatch.setattr(m, "_effective_user_connection_value",
                        lambda *, user_id, key, **_kw: ("jeton", "user"))
    monkeypatch.setattr(m, "_github_api_get", lambda chemin, **kw: (
        {"object": {"sha": "base"}} if "/git/ref" in chemin
        else {"sha": "v", "content": base64.b64encode(crlf.encode()).decode()}))
    monkeypatch.setattr(m, "_github_api_post", lambda chemin, **kw: {})
    monkeypatch.setattr(m, "_github_api_put", lambda chemin, *, token, json_body: envoye.update(
        c=base64.b64decode(json_body["content"]).decode("utf-8")) or {
            "content": {"sha": "n"}, "commit": {"sha": "abc", "html_url": ""}})
    monkeypatch.setattr(m, "_ouvrir_pull_request",
                        lambda **kw: {"html_url": "https://github.com/o/r/pull/1", "number": 1})

    c = TestClient(app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=uid,
                                          secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    # `/projects` ne porte pas toujours de formulaire ; la page de reglages, si.
    jeton = _re.search(r'name="_csrf"\s+value="([^"]*)"',
                       c.get("/settings/accounts").text)
    assert jeton, "pas de jeton CSRF : le test mesurerait un 403, pas la route"
    rep = c.post(
        "/api/projects/%s/issues/title_too_short/github-fix" % slug,
        json={"url": "https://%s.fr/p" % slug, "confirm": True,
              "file_path": CHEMIN, "patched_content": CORRECT},
        headers={"X-CSRF-Token": jeton.group(1)} if jeton else {})
    assert rep.status_code == 200, rep.text

    assert envoye, "rien n'a été commité : %s" % rep.text
    ecrit = envoye["c"]
    assert "\r\n" in ecrit and ecrit.count("\n") == ecrit.count("\r\n"), (
        "les fins de ligne du client n'ont pas été respectées : %r" % ecrit[:60])


# --- l'enumeration ----------------------------------------------------------------------------

def test_AUCUN_chemin_ne_refait_la_liste_dans_son_coin() -> None:
    """Le garde-fou contre la reapparition du defaut : un troisieme chemin qui recopierait la
    sequence au lieu de l'appeler diverge des la premiere regle ajoutee."""
    import ast

    arbre = ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    fautes: list[str] = []
    for n in ast.walk(arbre):
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        # Exclure la fonction partagee ET les detecteurs eux-memes : `ast.unparse` rend la
        # ligne `def`, donc chacun contient son propre nom et s'accusait tout seul. Un
        # garde-fou qui ne s'exempte pas de sa propre regle n'est pas plus strict, il est
        # inutilisable — deuxieme fois aujourd'hui.
        if n.name in {"_refus_de_format", "_object_key_conflicts", "_unbalanced_delimiters"}:
            continue
        corps = ast.unparse(n)
        if "_object_key_conflicts(" in corps or "_unbalanced_delimiters(" in corps:
            fautes.append("%s (ligne %d)" % (n.name, n.lineno))
    assert not fautes, (
        "ces fonctions refont la liste des refus au lieu d'appeler _refus_de_format :\n  "
        + "\n  ".join(fautes))


def test_les_deux_appelants_existent_ENCORE() -> None:
    """Une énumération qui ne trouve plus ses appelants passe au vert dans le vide."""
    import inspect

    for fn in (m._deep_patch_issue_files, m.api_github_fix):
        assert "_refus_de_format(" in inspect.getsource(fn), fn.__name__
