# -*- coding: utf-8 -*-
"""Quand aucune propriete Search Console ne repond, les NOMMER toutes.

MESURE DU 19/09/2026, projet homegearwise.com. L'ecran affichait :

    GSC API error: HTTP 403 — User does not have sufficient permission
    for site 'http://homegearwise.com/'

Le `http://` sur un site en `https` faisait accuser un defaut de schema dans le produit — on
allait chercher un bug de construction d'URL. En realite `_gsc_property_candidates` essaie
QUATRE formes, parce qu'une propriete Search Console peut etre enregistree de plusieurs
facons :

    sc-domain:homegearwise.com        le domaine entier
    https://homegearwise.com/         prefixe d'URL exact
    https://www.homegearwise.com/     la variante www
    http://homegearwise.com/          le schema oppose, en DERNIER recours

`last_error` n'en gardait qu'une : la derniere. Le message nommait donc le candidat le moins
probable et taisait les trois vrais essais.

LA LECON. Une erreur qui ne dit pas ce qui a ete TENTE envoie chercher au mauvais endroit. Ici
elle designait un bug de code la ou il fallait selectionner une propriete. C'est la meme forme
que les deux autres defauts d'aujourd'hui — un worker muet et une troncature silencieuse : le
systeme sait quelque chose qu'il ne dit pas, et c'est ce silence qui coute le temps.
"""

from __future__ import annotations

import ast
import contextlib
import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-gsc-msg-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402


# --- le message ------------------------------------------------------------------------------

def test_le_message_NOMME_toutes_les_proprietes_essayees() -> None:
    essayes = ["sc-domain:exemple.fr", "https://exemple.fr/", "http://exemple.fr/"]
    txt = m._gsc_echec_lisible(essayes, "HTTP 403 — permission refusée")
    for candidat in essayes:
        assert candidat in txt, "%r manque : %s" % (candidat, txt)
    assert "403" in txt, "la dernière erreur de Google a disparu : %s" % txt


def test_le_message_garde_l_ORDRE_des_essais() -> None:
    """L'ordre dit lequel le produit privilégie : `sc-domain` d'abord, `http` en dernier.
    Le lire mélangé ferait croire que le produit a demandé n'importe quoi."""
    essayes = ["sc-domain:exemple.fr", "https://exemple.fr/", "http://exemple.fr/"]
    txt = m._gsc_echec_lisible(essayes, "boom")
    assert txt.index("sc-domain:") < txt.index("https://") < txt.index("http://exemple")


def test_sans_candidat_on_N_INVENTE_RIEN() -> None:
    """Un message fabriqué autour d'une liste vide affirmerait des essais qui n'ont pas eu
    lieu — c'est exactement le défaut qu'on corrige, dans l'autre sens."""
    assert m._gsc_echec_lisible([], "RuntimeError: boom") == "RuntimeError: boom"
    assert m._gsc_echec_lisible([], "") == "gsc_request_failed"


def test_le_message_DIT_QUOI_FAIRE() -> None:
    """Nommer quatre propriétés sans dire où les changer laisse la personne au même point."""
    txt = m._gsc_echec_lisible(["sc-domain:x.fr"], "403")
    assert "paramètres de crawl" in txt.lower(), txt


# --- la boucle, telle que homegearwise l'a vecue ------------------------------------------------

def _echec_sur_tout(monkeypatch) -> None:
    """Google refuse CHAQUE candidat, en citant celui qu'on lui demande — comme le vrai 403."""

    @contextlib.contextmanager
    def _faux_creds(*, user_id, slug):
        yield (TEST_ROOT / "creds.json", "oauth", "")

    class _FauxFetch:
        @staticmethod
        def fetch_gsc(*, property_url, **kw):
            raise RuntimeError(
                "GSC API error: HTTP 403 - User does not have sufficient permission for "
                "site '%s'." % property_url)

    monkeypatch.setattr(m, "_gsc_live_credentials", _faux_creds)
    monkeypatch.setattr(m, "_load_gsc_fetch_module", lambda: _FauxFetch)


def test_la_serie_live_rend_les_QUATRE_candidats(monkeypatch) -> None:
    """La reproduction du 19/09/2026, mesuree sur la BOUCLE.

    Le test unitaire du message ne dit rien de ce que l'utilisateur verra : c'est la boucle
    qui remplit la liste, et c'est elle qui ne la remplissait pas.
    """
    _echec_sur_tout(monkeypatch)
    out = m._fetch_gsc_live_series(user_id="u", slug="s", base_url="https://homegearwise.com/",
                                  gsc_cfg={"enabled": True}, days=28)
    assert out["ok"] is False and out["reason"] == "request_failed"
    err = out["error"]
    for attendu in ("sc-domain:homegearwise.com", "https://homegearwise.com/",
                    "https://www.homegearwise.com/", "http://homegearwise.com/"):
        assert attendu in err, "%r absent du message :\n%s" % (attendu, err)


def test_les_items_live_le_font_AUSSI(monkeypatch) -> None:
    """Deux boucles jumelles. En corriger une et laisser l'autre est le défaut que cette
    journée a produit trois fois — on les traite ensemble ou pas du tout."""
    _echec_sur_tout(monkeypatch)
    out = m._fetch_gsc_live_items(user_id="u", slug="s", base_url="https://homegearwise.com/",
                                  gsc_cfg={"enabled": True}, days=28, dim="query", limit=5)
    assert out["ok"] is False
    assert "sc-domain:homegearwise.com" in out["error"], out["error"]


def test_un_succes_ne_porte_AUCUN_message_d_echec(monkeypatch) -> None:
    """Le message ne doit pas apparaître quand une propriété a répondu."""

    @contextlib.contextmanager
    def _faux_creds(*, user_id, slug):
        yield (TEST_ROOT / "creds.json", "oauth", "")

    class _FauxFetch:
        @staticmethod
        def fetch_gsc(*, property_url, **kw):
            if not property_url.startswith("sc-domain:"):
                raise RuntimeError("403")
            return []

    monkeypatch.setattr(m, "_gsc_live_credentials", _faux_creds)
    monkeypatch.setattr(m, "_load_gsc_fetch_module", lambda: _FauxFetch)
    out = m._fetch_gsc_live_series(user_id="u", slug="s", base_url="https://homegearwise.com/",
                                   gsc_cfg={"enabled": True}, days=28)
    assert out["ok"] is True and "error" not in out
    assert out["property"] == "sc-domain:homegearwise.com"


# --- le bandeau de la page projet --------------------------------------------------------------

def test_le_bandeau_d_indisponibilite_est_DATE() -> None:
    """Même famille de défaut, autre surface : une mesure présentée hors de son moment.

    Le bandeau lit `meta.gsc_api` / `meta.bing` du RAPPORT du dernier crawl. Un projet
    connecté après ce crawl affichait donc « activé mais indisponible » juste au-dessus d'un
    graphique live qui fonctionnait — mesuré le 19/09/2026 sur homegearwise.com, Bing branché
    et 213 impressions à l'écran sous un bandeau qui annonçait `no_accessible_site`.

    Deux mesures de deux moments présentées comme une seule : le client doute alors de tout
    l'écran, y compris de ce qui marche.
    """
    page = (WEB_ROOT / "templates" / "project_overview.html").read_text(encoding="utf-8")
    for source in ("GSC", "Bing"):
        assert "Au dernier crawl, %s était indisponible" % source in page, (
            "le bandeau %s ne dit pas de QUAND date son verdict" % source)
        assert "%s activé mais indisponible" % source not in page, (
            "l'ancienne formulation non datée subsiste pour %s" % source)


# --- l'enumeration ---------------------------------------------------------------------------

def _fonctions_du_module() -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    arbre = ast.parse(Path(m.__file__).read_text(encoding="utf-8"))
    return [n for n in ast.walk(arbre)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def test_plus_AUCUN_echec_GSC_ne_rend_la_seule_derniere_erreur() -> None:
    """Le garde-fou qui couvre la boucle qu'on écrira demain.

    MA PREMIÈRE VERSION CHERCHAIT LE MOTIF DANS TOUT LE FICHIER et s'accusait elle-même :
    `_gsc_echec_lisible` contient bien ce repli, et c'est sa place — c'est lui qui décide quoi
    dire quand il n'y a rien à lister. Un garde-fou qui ne s'exempte pas de sa propre règle
    n'est pas plus strict, il est inutilisable.
    """
    fautifs = [n.name for n in _fonctions_du_module()
               if n.name != "_gsc_echec_lisible"
               and 'last_error or "gsc_request_failed"' in ast.unparse(n).replace("'", '"')]
    assert not fautifs, (
        "ces fonctions rendent la seule dernière erreur au lieu de passer par "
        "_gsc_echec_lisible :\n  " + "\n  ".join(fautifs))


def test_CHAQUE_boucle_sur_les_candidats_tient_sa_liste() -> None:
    """Mesure la FORME qui a produit le defaut : PARCOURIR les candidats sans noter lesquels.

    MA PREMIÈRE VERSION MESURAIT DE TRAVERS, et elle a quand même servi : elle interdisait
    toute mention de `_gsc_property_candidates` sans carnet, et a signalé
    `gsc_properties_for_project`. Vérification faite, cette route ne les ESSAIE pas — elle
    s'en sert comme d'un ensemble de recommandations pour marquer les propriétés probables
    dans le sélecteur. Rien à noter, donc rien à corriger. Ce qui compte est la boucle, pas
    la mention.
    """
    fautes: list[str] = []
    for n in _fonctions_du_module():
        boucle = any(
            isinstance(c, ast.For) and isinstance(c.iter, ast.Call)
            and getattr(c.iter.func, "id", "") == "_gsc_property_candidates"
            for c in ast.walk(n))
        if boucle and "essayes.append" not in ast.unparse(n):
            fautes.append("%s (ligne %d)" % (n.name, n.lineno))
    assert not fautes, (
        "ces fonctions essaient plusieurs propriétés sans noter lesquelles :\n  "
        + "\n  ".join(fautes))


def test_le_garde_fou_ci_dessus_SAIT_ENCORE_compter() -> None:
    """Une énumération qui ne trouve plus rien à mesurer ne mesure plus rien.

    Si `_gsc_property_candidates` disparaissait ou changeait de nom, les deux tests
    précédents passeraient au vert en ne regardant aucune boucle.
    """
    avec_boucle = [n.name for n in _fonctions_du_module()
                   if any(isinstance(c, ast.For) and isinstance(c.iter, ast.Call)
                          and getattr(c.iter.func, "id", "") == "_gsc_property_candidates"
                          for c in ast.walk(n))]
    assert len(avec_boucle) >= 2, (
        "l'auditeur ne trouve plus les boucles qu'il surveille : %r" % avec_boucle)
