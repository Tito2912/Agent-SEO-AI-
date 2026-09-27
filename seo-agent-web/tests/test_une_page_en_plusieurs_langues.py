# -*- coding: utf-8 -*-
"""Une page neuve en plusieurs langues : N fichiers, N index, UNE pull request.

TROISIEME COUCHE DU CHANTIER MULTILINGUE (decision du 27/09/2026 : le client choisit ses
langues avant la generation, et toutes partent dans une seule PR pour que les hreflang soient
coherents des la fusion).

La fixture reproduit la FORME mesuree sur un vrai site client (`prosperfactory.com`) :

* MDX sous `content/<langue>/`, l'anglais servi A LA RACINE (aucune route `/en/`) ;
* `translationKey: "guides/<slug anglais>"`, egale dans les langues, unique par page ;
* des slugs TRADUITS — `dca-crypto`, `krypto-dca`, `crypto-dca` ;
* une page allemande (`anfangen-...`) qui precede la traduction dans l'alphabet : laisser le
  placement choisir la soeur allemande la prendrait, elle.

Ce que ces tests defendent :

* RIEN n'est ecrit tant que TOUTES les versions ne sont pas pretes — ni branche, ni fichier ;
* chaque version imite la traduction de la MEME soeur, et vit sous un slug tire de SON titre ;
* chaque canonical designe sa propre version, pas la soeur que le modele a clonee ;
* la cle de traduction est la meme partout ET n'est pas celle de la soeur (que le modele
  recopie : c'est le bouchon ci-dessous qui la recopie, comme le ferait le vrai) ;
* une page seule sur un site multilingue recoit elle aussi sa propre cle.
"""
from __future__ import annotations

import base64
import os
import re
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-langues-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

ROUTE = "/fr/guides/calculer-les-interets-composes/"
SUJET = "Comment calculer les intérêts composés"


def _page(titre: str, cle: str, canonical: str, description: str) -> str:
    return (
        "---\n"
        f'title: "{titre}"\n'
        f'description: "{description}"\n'
        'updatedAt: "2026-03-11"\n'
        f'translationKey: "{cle}"\n'
        'type: "guide"\n'
        f'canonical: "{canonical}"\n'
        "---\n\nTexte du guide.\n"
    )


def _index(liens: list[tuple[str, str]]) -> str:
    return ('---\ntitle: "Guides"\ntranslationKey: "guides"\n---\n\n'
            + "".join("- [%s](%s)\n" % (t, u) for t, u in liens))


FICHIERS = {
    "content/fr/guides/dca-crypto.mdx": _page(
        "DCA crypto : guide pratique", "guides/crypto-dca",
        "https://site.fr/fr/guides/dca-crypto/", "Le DCA en crypto, pas a pas."),
    "content/fr/guides/debuter-investissement.mdx": _page(
        "Débuter en investissement", "guides/start-investing",
        "https://site.fr/fr/guides/debuter-investissement/", "Les premiers pas."),
    "content/de/guides/krypto-dca.mdx": _page(
        "Krypto DCA: Praxis-Guide", "guides/crypto-dca",
        "https://site.fr/de/guides/krypto-dca/", "DCA in Krypto."),
    "content/de/guides/anfangen-zu-investieren.mdx": _page(
        "Anfangen zu investieren", "guides/start-investing",
        "https://site.fr/de/guides/anfangen-zu-investieren/", "Die ersten Schritte."),
    "content/en/guides/crypto-dca.mdx": _page(
        "Crypto DCA guide", "guides/crypto-dca",
        "https://site.fr/guides/crypto-dca/", "Dollar-cost averaging, step by step."),
    "content/en/guides/start-investing.mdx": _page(
        "Start investing", "guides/start-investing",
        "https://site.fr/guides/start-investing/", "First steps."),
    "content/fr/guides.mdx": _index([("DCA crypto", "/fr/guides/dca-crypto/"),
                                     ("Débuter", "/fr/guides/debuter-investissement/")]),
    "content/de/guides.mdx": _index([("Anfangen", "/de/guides/anfangen-zu-investieren/"),
                                     ("Krypto DCA", "/de/guides/krypto-dca/")]),
    "content/en/guides.mdx": _index([("Crypto DCA", "/guides/crypto-dca/"),
                                     ("Start investing", "/guides/start-investing/")]),
}

TREE = ["package.json", "next.config.mjs", "app/(site)/layout.tsx",
        "app/(site)/[...slug]/page.tsx", "app/(site)/fr/[...slug]/page.tsx",
        "app/(site)/de/[...slug]/page.tsx", *FICHIERS]

# Ce que le modele rend pour chaque soeur. Il RECOPIE la cle et le canonical de la page qu'on
# lui montre — c'est exactement ce que fait le vrai, et c'est ce que le code doit rattraper.
TITRES = {"fr": "Calculer les intérêts composés",
          "de": "Zinseszins berechnen",
          "en": "How to calculate compound interest"}


def _redige(soeur_contenu: str, titre: str) -> str:
    cle = re.search(r'translationKey: "([^"]+)"', soeur_contenu).group(1)
    canonical = re.search(r'canonical: "([^"]+)"', soeur_contenu).group(1)
    return _page(titre, cle, canonical, "Un guide sur les interets composes.")


@pytest.fixture()
def github(monkeypatch):
    calls: dict = {"put": [], "post": [], "fichiers": dict(FICHIERS), "tree": list(TREE)}

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
            return {"html_url": "https://github.com/client/site/pull/9", "number": 9,
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
    monkeypatch.setattr(app_module, "_open_pr_for_issue", lambda **kw: "")
    return calls


@pytest.fixture()
def modele(monkeypatch):
    """Rend la page de la langue de la soeur montree ; enregistre quelle soeur on lui a montree."""
    vues: list[str] = []
    casse: set[str] = set()
    titres = dict(TITRES)

    def _ai(*, system, user_msg, **kw):
        soeur = re.search(r"pour la forme \(([^)]+)\)", user_msg).group(1)
        vues.append(soeur)
        langue = soeur.split("/")[1]
        if langue in casse:
            return {"contenu": "---\ntitle: \"Sans les autres cles\"\n---\n\nTexte.\n"}
        return {"contenu": _redige(FICHIERS[soeur], titres[langue])}

    monkeypatch.setattr(app_module, "_correction_ai_json", _ai)
    return SimpleNamespace(vues=vues, casse=casse, titres=titres)


@pytest.fixture()
def debits(monkeypatch):
    faits: list[int] = []
    monkeypatch.setattr(app_module, "_article_charge", lambda user, n, **kw: faits.append(n))
    return faits


def _proposer(langues=None, **kw):
    app_module.DB.create_tables()
    return app_module._proposer_une_page(
        SimpleNamespace(id="u-langues"), project_id="p-langues", site_name="site.fr",
        slug="site", sujet=SUJET, route=ROUTE, base_url="https://site.fr",
        owner="client", repo_name="site", branch="main", token="t", langues=langues, **kw)


def _ecrits(github) -> dict[str, str]:
    return dict(github["put"])


# ── le chemin nominal ─────────────────────────────────────────────────────────────────────────

def test_chaque_version_vit_dans_SA_section_sous_un_slug_TRADUIT(github, modele, debits) -> None:
    out = _proposer(["de", ""])
    assert out["ok"], out
    assert set(_ecrits(github)) == {
        "content/fr/guides/calculer-les-interets-composes.mdx", "content/fr/guides.mdx",
        "content/de/guides/zinseszins-berechnen.mdx", "content/de/guides.mdx",
        "content/en/guides/how-to-calculate-compound-interest.mdx", "content/en/guides.mdx",
    }, _ecrits(github)
    assert [p["route"] for p in out["pages"]] == [
        ROUTE, "/de/guides/zinseszins-berechnen/", "/guides/how-to-calculate-compound-interest/"]


def test_la_langue_principale_COCHEE_ne_fait_pas_une_version_de_plus(github, modele, debits) -> None:
    """L'ecran enverra toutes les cases cochees, la langue de la page comprise, et en capitales
    si un gabarit les ecrit ainsi. Elle n'est pas une traduction d'elle-meme."""
    out = _proposer(["FR", " de "])
    assert out["ok"], out
    assert [p["langue"] for p in out["pages"]] == ["fr", "de"]
    assert debits == [2]


def test_UNE_branche_et_UNE_pull_request_pour_toutes_les_langues(github, modele, debits) -> None:
    _proposer(["de", ""])
    refs = [p for p, _b in github["post"] if p.endswith("/git/refs")]
    pulls = [b for p, b in github["post"] if p.endswith("/pulls")]
    assert len(refs) == 1 and len(pulls) == 1, github["post"]
    assert pulls[0]["draft"] is True
    for route in ("/de/guides/zinseszins-berechnen/", "/guides/how-to-calculate-compound-interest/"):
        assert route in pulls[0]["body"], pulls[0]["body"]
    assert 'translationKey: "guides/how-to-calculate-compound-interest"' in pulls[0]["body"]


def test_l_adresse_PROVISOIRE_n_apparait_nulle_part(github, modele, debits) -> None:
    """Avant que le modele ait ecrit, la version allemande n'a qu'une adresse provisoire — la
    section allemande et le slug FRANCAIS. Donnee au garde-fou d'adresse, elle finirait dans une
    note de la PR (« canonical remis a ... ») : une adresse qui n'a jamais existe, citee comme
    une correction."""
    _proposer(["de", ""])
    body = [b for p, b in github["post"] if p.endswith("/pulls")][0]["body"]
    tout = body + "".join(c for _f, c in github["put"])
    assert "/de/guides/calculer-les-interets-composes" not in tout
    assert "/guides/calculer-les-interets-composes" not in tout.replace(
        "/fr/guides/calculer-les-interets-composes", "")


def test_chaque_version_imite_la_traduction_de_la_MEME_soeur(github, modele, debits) -> None:
    """Sans soeur imposee, l'allemand clonerait `anfangen-zu-investieren`, premier de l'alphabet."""
    _proposer(["de", ""])
    assert modele.vues == ["content/fr/guides/dca-crypto.mdx",
                           "content/de/guides/krypto-dca.mdx",
                           "content/en/guides/crypto-dca.mdx"], modele.vues


def test_chaque_canonical_designe_SA_version(github, modele, debits) -> None:
    _proposer(["de", ""])
    ecrits = _ecrits(github)
    assert 'canonical: "https://site.fr/de/guides/zinseszins-berechnen/"' in \
        ecrits["content/de/guides/zinseszins-berechnen.mdx"]
    assert 'canonical: "https://site.fr/guides/how-to-calculate-compound-interest/"' in \
        ecrits["content/en/guides/how-to-calculate-compound-interest.mdx"]


def test_la_cle_de_traduction_est_la_MEME_partout_et_n_est_PAS_celle_de_la_soeur(
        github, modele, debits) -> None:
    _proposer(["de", ""])
    pages = [c for f, c in _ecrits(github).items() if not f.endswith("guides.mdx")]
    cles = {re.search(r'translationKey: "([^"]+)"', c).group(1) for c in pages}
    assert cles == {"guides/how-to-calculate-compound-interest"}, cles


def test_sans_la_langue_du_slug_de_la_cle_on_prend_celui_de_la_langue_PRINCIPALE(
        github, modele, debits) -> None:
    """La cle du site porte le slug ANGLAIS ; sans version anglaise, le site lui-meme a pris le
    slug de la page (`guides/compte-nickel`, page francaise seule)."""
    _proposer(["de"])
    pages = [c for f, c in _ecrits(github).items() if not f.endswith("guides.mdx")]
    cles = {re.search(r'translationKey: "([^"]+)"', c).group(1) for c in pages}
    assert cles == {"guides/calculer-les-interets-composes"}, cles


def test_chaque_index_lie_SA_version(github, modele, debits) -> None:
    _proposer(["de", ""])
    ecrits = _ecrits(github)
    assert "[Zinseszins berechnen](/de/guides/zinseszins-berechnen/)" in ecrits["content/de/guides.mdx"]
    assert ("[How to calculate compound interest](/guides/how-to-calculate-compound-interest/)"
            in ecrits["content/en/guides.mdx"])


def test_l_entree_d_index_suit_celle_de_la_soeur_IMPOSEE(github, modele, debits) -> None:
    """L'entree neuve est clonee sur celle de la soeur et posee a sa suite. Si le slug de la
    soeur n'etait pas celui de la traduction, l'allemand clonerait l'entree d'`anfangen`."""
    _proposer(["de"])
    lignes = _ecrits(github)["content/de/guides.mdx"].splitlines()
    i = next(n for n, l in enumerate(lignes) if "zinseszins-berechnen" in l)
    assert "krypto-dca" in lignes[i - 1], lignes


def test_en_automatique_une_seule_version_orpheline_bloque_TOUT(github, modele, debits) -> None:
    github["tree"].remove("content/de/guides.mdx")
    out = _proposer(["de"], refuser_si_orpheline=True)
    assert not out["ok"] and out.get("orpheline"), out
    assert "Aucune page de section" in out["error"], out
    _rien_ecrit(github)


def test_chaque_version_est_un_article(github, modele, debits) -> None:
    _proposer(["de", ""])
    assert debits == [3]


def test_une_page_SEULE_sur_un_site_multilingue_recoit_sa_propre_cle(github, modele, debits) -> None:
    """Le modele recopie la cle de la soeur : sans correction, la page rejoindrait la famille
    `guides/crypto-dca` et ses hreflang annonceraient le guide DCA comme sa traduction."""
    out = _proposer()
    assert out["ok"], out
    page = _ecrits(github)["content/fr/guides/calculer-les-interets-composes.mdx"]
    assert 'translationKey: "guides/calculer-les-interets-composes"' in page, page
    assert debits == [1]


# ── ce qui ne part PAS ────────────────────────────────────────────────────────────────────────

def _rien_ecrit(github) -> None:
    assert github["put"] == [], github["put"]
    assert github["post"] == [], github["post"]


def test_un_refus_sur_une_langue_n_ecrit_RIEN_meme_les_versions_pretes(github, modele, debits) -> None:
    modele.casse.add("en")
    out = _proposer(["de", ""])
    assert not out["ok"] and out["status"] == 422, out
    assert "version par défaut" in out["error"], out
    _rien_ecrit(github)
    assert debits == []


def test_un_titre_dont_le_slug_EXISTE_DEJA_est_refuse(github, modele, debits) -> None:
    """Le modele intitule la version allemande comme la soeur : son slug designe une page qui
    existe. Ecrire quand meme viserait un fichier que le placement n'a pas su nommer."""
    modele.titres["de"] = "Krypto DCA"
    out = _proposer(["de"])
    assert not out["ok"] and "/de/guides/krypto-dca/" in out["error"], out
    _rien_ecrit(github)


def test_un_titre_sans_lettre_latine_est_refuse_plutot_qu_adresse_au_hasard(
        github, modele, debits) -> None:
    """`_slug_de_sujet` ne garde que l'ASCII : un titre cyrillique donne un slug VIDE, donc
    une page a l'adresse de sa section."""
    modele.titres["de"] = "Сложные проценты"
    out = _proposer(["de"])
    assert not out["ok"] and "aucun slug" in out["error"], out
    _rien_ecrit(github)


def test_une_langue_que_le_site_ne_sert_pas_est_refusee_AVANT_le_modele(github, modele, debits) -> None:
    out = _proposer(["it"])
    assert not out["ok"] and out["status"] == 400 and "it" in out["error"], out
    assert modele.vues == []
    _rien_ecrit(github)


def test_sans_cle_de_traduction_le_multilingue_est_refuse(github, modele, debits) -> None:
    for chemin, contenu in list(github["fichiers"].items()):
        github["fichiers"][chemin] = re.sub(r'translationKey: "[^"]+"\n', "", contenu)
    out = _proposer(["de"])
    assert not out["ok"] and "hreflang" in out["error"], out
    _rien_ecrit(github)


def test_sans_cle_une_page_SEULE_part_comme_avant(github, modele, debits) -> None:
    for chemin, contenu in list(github["fichiers"].items()):
        github["fichiers"][chemin] = re.sub(r'translationKey: "[^"]+"\n', "", contenu)
    assert _proposer()["ok"]


def test_une_langue_ou_la_soeur_n_existe_pas_est_refusee(github, modele, debits) -> None:
    github["tree"].remove("content/de/guides/krypto-dca.mdx")
    out = _proposer(["de"])
    assert not out["ok"] and "n'existe pas en de" in out["error"], out
    _rien_ecrit(github)


def test_une_cle_qui_ne_porte_aucun_slug_est_refusee_plutot_que_recopiee(github, modele, debits) -> None:
    for chemin, contenu in list(github["fichiers"].items()):
        github["fichiers"][chemin] = contenu.replace('"guides/crypto-dca"', '"k-000017"')
    out = _proposer(["de"])
    assert not out["ok"] and "k-000017" in out["error"], out
    _rien_ecrit(github)


# ── les briques ───────────────────────────────────────────────────────────────────────────────

def test_la_transposition_prend_le_slug_le_plus_LONG() -> None:
    """`dca` se lit dans `guides/crypto-dca` sans en etre le slug."""
    assert app_module._valeur_de_traduction(
        "guides/crypto-dca", {"fr": "dca", "": "crypto-dca"},
        {"fr": "neuf-fr", "": "neuf-en"}, "fr") == "guides/neuf-en"


def test_poser_la_valeur_garde_les_guillemets_et_ne_touche_que_la_tete() -> None:
    texte = ("---\ntranslationKey: 'a/b'\r\ntitle: x\n---\n\ntranslationKey: 'a/b'\n")
    out = app_module._poser_valeur_de_tete(texte, "translationKey", "a/c")
    assert out == "---\ntranslationKey: 'a/c'\r\ntitle: x\n---\n\ntranslationKey: 'a/b'\n"
