# -*- coding: utf-8 -*-
"""Un membre connecte SON compte Bing sur un projet de son hote, et ca se lit.

LE BESOIN, formule par le proprietaire du produit le 19/09/2026 : « le client doit connecter
son propre compte Bing ». Le client est un MEMBRE du compte de l'agence, pas son proprietaire.

CE QUI SE SERAIT PASSE SANS CE CHANGEMENT, et c'est le defaut deja corrige le matin meme sur
les travaux : la connexion s'ecrivait sous l'utilisateur CONNECTE (le membre) et se lisait sous
le compte PAYEUR (le proprietaire). Le client se serait connecte, aurait vu « connecte » sur la
page Comptes & connexions, et ses projets auraient continue d'afficher « non connecte ». Rien
n'echoue, rien ne se journalise — le bouton semble simplement ne servir a rien.

LE MODELE RETENU, aligne sur Google Search Console qui est par projet depuis le debut :

  cle        `BING_OAUTH_REFRESH_TOKEN:<slug>`, comme `GSC_OAUTH:<slug>`
  compte     le PAYEUR, pas la personne qui clique — la ou les pages projet lisent deja
  repli      la connexion de COMPTE sert tous les projets qui n'en ont pas de propre

LE REPLI EST LA MIGRATION. Aucune connexion existante ne bouge, et une agence qui gere dix
sites depuis un seul compte Bing ne se reconnecte nulle part. On ne se reconnecte par projet
QUE lorsque le site vit dans un autre compte Bing — le cas du client.

POURQUOI PAS COMME GITHUB. Un jeton GitHub est un identifiant d'AGENCE, et il a ete decide
qu'un membre pousse avec celui de son hote. Une propriete Bing Webmaster appartient au
proprietaire du SITE. Les deux decisions ne se contredisent pas, elles portent sur deux
natures de credential.
"""

from __future__ import annotations

import ast
import os
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-bing-projet-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("BING_OAUTH_CLIENT_ID", "cid-test")
os.environ.setdefault("BING_OAUTH_CLIENT_SECRET", "csecret-test")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import AccountMember, Project, User  # noqa: E402


def _utilisateur(prefixe: str) -> str:
    m.DB.create_tables()
    with m.DB.session() as db:
        u = User(email="%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8]),
                 password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id)


def _projet(owner_id: str) -> str:
    s = "site-%s" % uuid.uuid4().hex[:8]
    with m.DB.session() as db:
        db.add(Project(owner_user_id=owner_id, slug=s, site_name=s,
                       base_url="https://%s.fr/" % s))
        db.commit()
    return s


def _rejoint(owner_id: str, member_id: str) -> None:
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=owner_id, member_user_id=member_id))
        db.commit()


def _client(user_id: str) -> TestClient:
    c = TestClient(app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=user_id,
                                          secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    return c


def _poser_connexion(user_id: str, cle: str, jeton: str = "refresh-xyz") -> None:
    m._upsert_user_connection(user_id=user_id, key=cle, value=jeton,
                              meta={"auth_type": "oauth", "access_token": "acc",
                                    "expires_at": 9_999_999_999.0})


def _connecter_via_oauth(client: TestClient, monkeypatch, slug: str = "") -> None:
    """Le parcours REEL : on suit la redirection vers Bing, on récupère le state signé, et on
    rejoue le rappel. Tester l'écriture sans passer par là laisserait le state hors mesure."""
    monkeypatch.setattr(m, "_bing_oauth_exchange_code",
                        lambda **kw: {"refresh_token": "refresh-du-client",
                                      "access_token": "acc", "expires_in": 3600})
    url = "/oauth/bing/connect" + ("?slug=%s" % slug if slug else "")
    rep = client.get(url, follow_redirects=False)
    assert rep.status_code == 303, rep.status_code
    from urllib.parse import parse_qs, urlparse
    state = parse_qs(urlparse(rep.headers["location"]).query)["state"][0]
    rep2 = client.get("/oauth/bing/callback?code=abc&state=%s" % state, follow_redirects=False)
    assert rep2.status_code == 303, rep2.status_code
    assert "err=" not in rep2.headers["location"], rep2.headers["location"]


# --- le besoin exprimé -----------------------------------------------------------------------

def test_le_membre_connecte_son_bing_et_le_PROJET_le_voit(monkeypatch) -> None:
    """Le test qui décrit le besoin de bout en bout : écriture par le membre, lecture par le
    projet. Deux moitiés correctes reliées par rien, c'était le défaut de ce matin."""
    hote, membre = _utilisateur("hote"), _utilisateur("membre")
    _rejoint(hote, membre)
    slug = _projet(hote)

    _connecter_via_oauth(_client(membre), monkeypatch, slug=slug)

    # Lu comme une page projet le lit : sous le compte PAYEUR, pour CE projet.
    auth_lue = m._effective_bing_connection(user_id=m._compte_payeur(membre, slug), slug=slug)
    assert auth_lue.get("token"), "le projet ne voit pas la connexion que le membre a posée"


def test_la_connexion_est_rangee_sous_le_PAYEUR_pas_sous_le_membre(monkeypatch) -> None:
    """Décision produit du 19/09/2026, assumée : le jeton du client vit sous le compte de
    l'agence. C'est ce qui fait que l'agence garde la connexion du site qu'elle gère si le
    client s'en va — pendant de la décision inverse déjà prise pour GitHub."""
    hote, membre = _utilisateur("hote"), _utilisateur("membre")
    _rejoint(hote, membre)
    slug = _projet(hote)

    _connecter_via_oauth(_client(membre), monkeypatch, slug=slug)

    cle = m._bing_oauth_connection_key(slug)
    assert m._user_connection_row(user_id=hote, key=cle) is not None, "rien sous l'hôte"
    assert m._user_connection_row(user_id=membre, key=cle) is None, "écrit sous le membre"


# --- le repli, qui est la migration -----------------------------------------------------------

def test_une_connexion_de_COMPTE_sert_un_projet_qui_n_en_a_pas() -> None:
    """Aucune connexion existante ne bouge : c'est ce qui rend ce changement sans migration."""
    hote = _utilisateur("hote")
    slug = _projet(hote)
    _poser_connexion(hote, m._BING_OAUTH_CONNECTION_KEY, "jeton-de-compte")
    assert m._effective_bing_connection(user_id=hote, slug=slug).get("token")


def test_la_connexion_du_PROJET_l_emporte_sur_celle_du_compte() -> None:
    """Sans cette priorité, le client connecterait son Bing et l'agence continuerait de lire
    le sien — la connexion serait posée et ignorée."""
    hote = _utilisateur("hote")
    slug = _projet(hote)
    _poser_connexion(hote, m._BING_OAUTH_CONNECTION_KEY, "jeton-de-compte")
    _poser_connexion(hote, m._bing_oauth_connection_key(slug), "jeton-du-projet")

    # On compare le `refresh_token`, pas le `token` : ce dernier est l'access token, que les
    # deux connexions partagent dans ce montage. Comparer la mauvaise clé ferait échouer un
    # code correct.
    assert m._effective_bing_connection(user_id=hote, slug=slug).get(
        "refresh_token") == "jeton-du-projet", "le projet lit le jeton du compte"
    assert m._effective_bing_connection(user_id=hote).get(
        "refresh_token") == "jeton-de-compte", "le compte lit le jeton d'un projet"


def test_la_connexion_d_un_projet_NE_FUIT_PAS_sur_un_autre() -> None:
    """Toute la raison d'être du par-projet : une agence gère dix sites de dix clients."""
    hote = _utilisateur("hote")
    a, b = _projet(hote), _projet(hote)
    _poser_connexion(hote, m._bing_oauth_connection_key(a), "jeton-du-client-a")
    assert m._effective_bing_connection(user_id=hote, slug=a).get("token")
    assert not m._effective_bing_connection(user_id=hote, slug=b).get("token"), (
        "le Bing du client A s'applique au site du client B")


# --- ce qui ne bouge pas ------------------------------------------------------------------------

def test_un_ETRANGER_ne_peut_pas_connecter_sur_un_projet(monkeypatch) -> None:
    hote = _utilisateur("hote")
    slug = _projet(hote)
    rep = _client(_utilisateur("etranger")).get("/oauth/bing/connect?slug=%s" % slug,
                                                follow_redirects=False)
    assert rep.status_code == 404, rep.status_code


def test_un_ETRANGER_ne_peut_pas_DECONNECTER_le_projet_d_autrui() -> None:
    """La barrière doit tenir des deux côtés. Une mutation a montré que mes tests ne
    couvraient que la connexion : détruire la connexion d'un client est au moins aussi grave
    que d'en poser une."""
    hote = _utilisateur("hote")
    slug = _projet(hote)
    _poser_connexion(hote, m._bing_oauth_connection_key(slug), "jeton-du-client")

    etranger = _client(_utilisateur("etranger"))
    # Le jeton CSRF vient de SA page de réglages : sans lui le refus serait un 403 anti-CSRF,
    # et le test passerait sans jamais atteindre le contrôle de propriété qu'il mesure.
    import re as _re
    jeton = _re.search(r'name="_csrf"\s+value="([^"]*)"',
                       etranger.get("/settings/accounts").text)
    assert jeton, "pas de jeton CSRF : le test ne mesurerait pas ce qu'il croit"
    rep = etranger.post("/oauth/bing/disconnect",
                        data={"slug": slug, "_csrf": jeton.group(1)},
                        follow_redirects=False)
    assert rep.status_code == 404, rep.status_code
    assert m._user_connection_row(user_id=hote, key=m._bing_oauth_connection_key(slug)) is not None


def test_sans_projet_la_connexion_reste_au_niveau_du_COMPTE(monkeypatch) -> None:
    """La carte « Comptes & connexions » garde son sens : elle pose le repli du compte."""
    seul = _utilisateur("seul")
    _connecter_via_oauth(_client(seul), monkeypatch)
    assert m._user_connection_row(user_id=seul, key=m._BING_OAUTH_CONNECTION_KEY) is not None


def test_la_deconnexion_vise_EXACTEMENT_ce_que_la_connexion_a_pose(monkeypatch) -> None:
    """Déconnecter ailleurs qu'on n'a connecté laisserait une connexion que plus aucun bouton
    ne peut retirer. Les deux passent donc par `_bing_oauth_cible`."""
    hote, membre = _utilisateur("hote"), _utilisateur("membre")
    _rejoint(hote, membre)
    slug = _projet(hote)
    c = _client(membre)
    _connecter_via_oauth(c, monkeypatch, slug=slug)

    import re as _re
    page = c.get("/projects/%s/settings/crawl" % slug)
    assert page.status_code == 200, page.status_code
    jeton = _re.search(r'name="_csrf"\s+value="([^"]*)"', page.text)
    rep = c.post("/oauth/bing/disconnect",
                 data={"slug": slug, "_csrf": jeton.group(1) if jeton else ""},
                 follow_redirects=False)
    assert rep.status_code == 303, rep.status_code
    assert m._user_connection_row(user_id=hote, key=m._bing_oauth_connection_key(slug)) is None


def test_le_bouton_de_la_page_projet_MENE_QUELQUE_PART(monkeypatch) -> None:
    """Un lien mal écrit ne casse aucun test unitaire — il rend juste le bouton inerte.

    J'ai écrit `/crawl-settings` au lieu de `/projects/<slug>/settings/crawl` dans le gabarit,
    et c'est un test d'un AUTRE sujet qui l'a attrapé par accident. Celui-ci le cherche : il
    prend le lien tel que la page le rend et le SUIT. Un 404 ou un lien sans slug échoue ici.
    """
    hote, membre = _utilisateur("hote"), _utilisateur("membre")
    _rejoint(hote, membre)
    slug = _projet(hote)
    c = _client(membre)

    page = c.get("/projects/%s/settings/crawl" % slug)
    assert page.status_code == 200, page.status_code

    import html as _html
    import re as _re
    trouve = _re.search(r'href="(/oauth/bing/connect[^"]*)"', page.text)
    assert trouve, "la page projet n'offre aucun lien pour connecter Bing"
    lien = _html.unescape(trouve.group(1))
    assert ("slug=%s" % slug) in lien, "le lien ne porte pas le projet : %s" % lien

    monkeypatch.setattr(m, "_bing_oauth_exchange_code", lambda **kw: {})
    rep = c.get(lien, follow_redirects=False)
    assert rep.status_code == 303, "le lien du bouton mène à %d : %s" % (rep.status_code, lien)
    assert "bing.com" in rep.headers["location"], rep.headers["location"]


# --- l'énumération -------------------------------------------------------------------------------

def test_TOUTE_lecture_qui_connait_un_projet_le_TRANSMET() -> None:
    """Le garde-fou qui compte : il couvre la lecture qu'on ajoutera sans y penser.

    Les tests ci-dessus verrouillent les huit lectures d'aujourd'hui. Celui-ci relit le source
    et refuse qu'une fonction disposant d'un `slug` interroge la connexion Bing sans le passer
    — c'est exactement la forme du défaut de ce matin, où sept comparaisons sur sept avaient
    été écrites correctement et une liste élargie en avait oublié six.
    """
    arbre = ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    fautes: list[str] = []
    for n in ast.walk(arbre):
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        a = n.args
        if "slug" not in [x.arg for x in list(a.posonlyargs) + list(a.args) + list(a.kwonlyargs)]:
            continue
        for c in ast.walk(n):
            if not isinstance(c, ast.Call):
                continue
            if getattr(c.func, "id", "") != "_effective_bing_connection":
                continue
            if not any(k.arg == "slug" for k in c.keywords):
                fautes.append("%s (ligne %d)" % (n.name, c.lineno))
    assert not fautes, (
        "ces fonctions connaissent le projet mais lisent la connexion du compte :\n  "
        + "\n  ".join(fautes))


def test_la_cle_par_projet_est_CALQUEE_sur_celle_de_GSC() -> None:
    """Deux conventions de nommage pour la même idée finiraient par diverger."""
    long_slug = "x" * 200
    for court in ("mon-site", "a"):
        assert m._bing_oauth_connection_key(court).endswith(court)
        assert m._gsc_oauth_connection_key(court).endswith(court)
    # Même écrêtage, même empreinte : la forme est la seule chose qui diffère du préfixe.
    assert len(m._bing_oauth_connection_key(long_slug)) == (
        len(m._gsc_oauth_connection_key(long_slug))
        - len("GSC_OAUTH:") + len(m._BING_OAUTH_CONNECTION_KEY + ":"))
