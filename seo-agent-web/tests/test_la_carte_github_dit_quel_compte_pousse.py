# -*- coding: utf-8 -*-
"""Un membre doit savoir QUEL compte GitHub poussera ses corrections.

CE QU'ON A VU LE 19/09/2026. Depuis le compte client (un MEMBRE), la carte GitHub de
« Comptes & connexions » affichait « compte connecté · Tito2912 ». Ce n'etait ni une fuite ni
un bug : la page lit bien les connexions du compte CONNECTE, et OAuth avait autorise le compte
GitHub ouvert dans le navigateur. Tout etait exact.

MAIS LA PAGE LAISSAIT CROIRE CE QUI EST FAUX. Les cinq routes qui poussent lisent le jeton du
PAYEUR :

    _effective_user_connection_value(user_id=_compte_payeur(str(user.id), slug), key="GITHUB_TOKEN")

Sur un projet de l'hote, c'est donc le compte GitHub de l'HOTE qui pousse, pas celui affiche
sur la carte. La divergence est VOULUE — decision prise en connaissance de cause : un client
ne doit pas avoir a donner un acces en ecriture a ses depots pour que l'agence travaille.

C'EST DONC LE SEUL CAS DE LA JOURNEE OU L'ON NE CORRIGE PAS LE CODE. Ecrire a un endroit et
lire a un autre a produit quatre defauts aujourd'hui ; ici c'est le comportement attendu, et
ce qui manquait etait la phrase qui l'explique. Reconnaitre la difference entre les deux est
tout l'exercice.

CE QUI N'A PAS BESOIN DE CETTE PHRASE : Netlify. Son jeton est lu pour le compte CONNECTE
(`netlify_sites` -> `_ensure_hardened_netlify_connection(user_id=str(user.id))`), la ou il est
ecrit. Pas de divergence, donc pas de note — ajouter la meme phrase partout la rendrait fausse
quelque part.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-carte-gh-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from fastapi.testclient import TestClient  # noqa: E402

from backend import app as m  # noqa: E402
from backend import auth  # noqa: E402
from backend.app import app  # noqa: E402
from backend.models import AccountMember, User  # noqa: E402


def _utilisateur(prefixe: str) -> tuple[str, str]:
    m.DB.create_tables()
    mail = "%s-%s@exemple.fr" % (prefixe, uuid.uuid4().hex[:8])
    with m.DB.session() as db:
        u = User(email=mail, password_hash=auth.hash_password("x" * 12), is_admin=False)
        db.add(u)
        db.commit()
        db.refresh(u)
        return str(u.id), mail


def _rejoint(owner_id: str, member_id: str) -> None:
    with m.DB.session() as db:
        db.add(AccountMember(owner_user_id=owner_id, member_user_id=member_id))
        db.commit()


def _page(user_id: str) -> str:
    c = TestClient(app)
    c.cookies.set(auth.SESSION_COOKIE_NAME,
                  auth.make_session_token(user_id=user_id,
                                          secret=os.environ["SEO_AGENT_SECRET_KEY"]))
    r = c.get("/settings/accounts")
    assert r.status_code == 200, r.status_code
    return r.text


# --- ce que le membre lit --------------------------------------------------------------------

def test_le_membre_voit_QUEL_compte_pousse_et_lequel() -> None:
    hote_id, hote_mail = _utilisateur("hote")
    membre_id, _ = _utilisateur("membre")
    _rejoint(hote_id, membre_id)

    page = _page(membre_id)
    assert hote_mail in page, "la page ne nomme pas le compte hôte"
    assert "utilisent le compte GitHub de l'hôte" in page, page[:400]


def test_un_proprietaire_SANS_hote_ne_voit_aucune_phrase() -> None:
    """Une phrase affichée à tout le monde serait fausse pour la plupart : sur ses propres
    projets, c'est bien la connexion de la carte qui pousse."""
    seul_id, _ = _utilisateur("seul")
    assert "utilisent le compte GitHub de l'hôte" not in _page(seul_id)


def test_le_nom_de_l_hote_est_ECHAPPE() -> None:
    """Une adresse vient de la saisie d'un humain et atterrit dans du HTML."""
    hote_id, _ = _utilisateur("hote")
    with m.DB.session() as db:
        db.get(User, hote_id).email = "a<b>c@exemple.fr"
        db.commit()
    membre_id, _ = _utilisateur("membre")
    _rejoint(hote_id, membre_id)

    page = _page(membre_id)
    assert "a<b>c@exemple.fr" not in page, "l'adresse de l'hôte est injectée telle quelle"
    assert "a&lt;b&gt;c@exemple.fr" in page


# --- le lecteur ---------------------------------------------------------------------------------

def test_le_lecteur_rend_l_hote_pour_un_membre() -> None:
    hote_id, hote_mail = _utilisateur("hote")
    membre_id, _ = _utilisateur("membre")
    _rejoint(hote_id, membre_id)
    assert m._compte_hote_de(membre_id) == hote_mail


def test_le_lecteur_rend_le_VIDE_pour_un_proprietaire() -> None:
    seul_id, _ = _utilisateur("seul")
    assert m._compte_hote_de(seul_id) == ""
    assert m._compte_hote_de("") == ""


def test_un_hote_ne_se_designe_pas_LUI_MEME_comme_son_hote() -> None:
    """`_comptes_accessibles` rend TOUJOURS son propre compte en premier. L'oublier ferait
    dire à un propriétaire que ses corrections partent du compte… de lui-même."""
    hote_id, _ = _utilisateur("hote")
    membre_id, _ = _utilisateur("membre")
    _rejoint(hote_id, membre_id)
    assert m._compte_hote_de(hote_id) == ""


# --- la phrase n'est pas recopiee ailleurs -------------------------------------------------------

def test_NETLIFY_ne_porte_PAS_la_meme_phrase() -> None:
    """Netlify lit son jeton pour le compte CONNECTÉ, là où il l'écrit : aucune divergence à
    expliquer. Recopier la phrase partout la rendrait fausse ici — et une phrase fausse coûte
    plus cher que pas de phrase.
    """
    gabarit = (WEB_ROOT / "templates" / "settings_accounts.html").read_text(encoding="utf-8")
    bloc_netlify = gabarit.split("netlify-connect-card", 1)
    assert len(bloc_netlify) == 2, "la carte Netlify a changé d'identifiant"
    assert "compte GitHub de l'hôte" not in bloc_netlify[1].split("</section>", 1)[0]


def test_la_phrase_est_DANS_la_carte_github() -> None:
    gabarit = (WEB_ROOT / "templates" / "settings_accounts.html").read_text(encoding="utf-8")
    apres = gabarit.split('id="github-connect-card"', 1)
    assert len(apres) == 2, "la carte GitHub a changé d'identifiant"
    assert "compte GitHub de l'hôte" in apres[1].split("</section>", 1)[0]


def test_le_comportement_explique_est_TOUJOURS_celui_du_code() -> None:
    """Le piège d'une phrase : elle reste écrite quand le code change.

    Si les routes cessaient de lire le jeton du payeur, ce texte deviendrait un mensonge
    affiché à tous les membres. Ce test relie la phrase à ce qu'elle décrit.
    """
    src = Path(m.__file__).read_text(encoding="utf-8")
    assert src.count(
        '_effective_user_connection_value(user_id=_compte_payeur(str(user.id), slug), '
        'key="GITHUB_TOKEN")') >= 5, (
        "les routes ne lisent plus le jeton GitHub du payeur : la phrase affichée aux "
        "membres ne décrit plus le comportement")
