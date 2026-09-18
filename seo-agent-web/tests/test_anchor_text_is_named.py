"""La moitie correcteur de `links_with_no_anchor_text` : nommer un lien muet sans rien deranger.

Le crawl a mesure la reponse (voir `test_anchor_text_evidence.py`) : la page CIBLE se nomme
elle-meme, par son h1 unique ou son titre. Le correcteur n'arbitre donc rien, il pose ce qui est
nomme — un `aria-label`, jamais un texte visible. Le rendu de la page du client ne bouge pas.

Ce que ces tests fixent, c'est la RETENUE de la pose. Un fichier de depot n'est pas la page
servie : il contient d'autres liens vers la meme cible, des gabarits dont on ignore le rendu, du
JSX ou un attribut porte un `>`. Ecrire au mauvais endroit dans un en-tete partage se verrait sur
toutes les pages du site a la fois, et un attribut mal ferme casse la page au lieu de l'ameliorer.

La pose doit aussi etre REJOUABLE : la boucle de verification repasse sur un depot deja corrige,
et une seconde passe qui ajouterait un second `aria-label` produirait un HTML invalide.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-ancres-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

S = "https://exemple.fr"
CLE = "links_with_no_anchor_text"

# La page d'accueil porte un lien-icone vers /contact. La cible a un h1 unique : « Contactez-nous ».
ITEMS = [{"page": S + "/", "field": "/contact", "value": "Contactez-nous"}]

MUET = '<header>\n  <a href="/contact"><svg class="i" /></a>\n</header>\n'


def _poser(contenu: str, items: list[dict[str, str]] | None = None) -> tuple[str, int]:
    # `items or ITEMS` aurait transforme la liste VIDE en preuve par defaut, et le test du cas
    # « aucune preuve » aurait mesure le cas nominal sans que rien ne le dise.
    return app_module._poser_aria_label_sur_liens_sans_ancre(
        contenu, ITEMS if items is None else items)


# --- ce que la pose ecrit -------------------------------------------------------------------

def test_le_lien_muet_recoit_le_nom_que_la_cible_se_donne() -> None:
    sortie, n = _poser(MUET)
    assert n == 1
    assert '<a href="/contact" aria-label="Contactez-nous">' in sortie, sortie


def test_ce_que_le_lien_ENTOURE_et_son_href_ne_bougent_pas() -> None:
    """La correction est invisible pour l'humain : c'est la condition qui l'a fait accepter.

    Mesure sur un lien que le correcteur TOUCHE vraiment. Une premiere version l'exercait sur un
    lien libelle par une URL ; depuis que ces liens-la ne sont plus corriges, elle passait parce
    que rien n'etait fait — un test vert qui ne prouvait plus rien, et dont le nom continuait de
    promettre le contraire.
    """
    sortie, n = _poser(MUET)
    assert n == 1
    assert '<svg class="i" />' in sortie, sortie
    assert 'href="/contact"' in sortie


def test_un_lien_libelle_par_une_URL_n_est_PLUS_touche() -> None:
    """Ahrefs compte une URL visible comme une ancre valide, et le crawler l'a suivi le
    18/09/2026. Le correcteur ne doit pas rester plus severe que la detection qui l'alimente :
    poser un aria-label la ou aucune anomalie n'est signalee serait agir hors mandat."""
    libelle = '<a href="/contact">https://exemple.fr/contact</a>'
    sortie, n = _poser(libelle)
    assert n == 0 and sortie == libelle


def test_le_guillemet_est_CLONE_de_celui_du_href() -> None:
    """On ne suppose pas l'idiome du fichier : on reprend celui de la ligne voisine."""
    sortie, n = _poser("<a href='/contact'><svg /></a>")
    assert n == 1
    assert "aria-label='Contactez-nous'" in sortie, sortie


def test_un_nom_a_apostrophe_ne_peut_pas_fermer_l_attribut() -> None:
    """Un h1 francais en contient sans arret ; non echappe, il casserait la page."""
    items = [{"page": S + "/", "field": "/a", "value": "L'atelier & l'équipe"}]
    sortie, n = _poser("<a href='/a'><svg /></a>", items)
    assert n == 1
    assert "aria-label='L&#x27;atelier &amp; l&#x27;équipe'" in sortie, sortie
    assert sortie.count("'") % 2 == 0, sortie


def test_tous_les_exemplaires_du_meme_lien_muet_sont_nommes() -> None:
    """Un en-tete partage n'est la page de personne : on vise par le href, jamais par la page."""
    sortie, n = _poser('<a href="/contact"><i /></a>\n<a href="/contact"><b /></a>')
    assert n == 2
    assert sortie.count('aria-label="Contactez-nous"') == 2


# --- ce que la pose REFUSE de toucher --------------------------------------------------------

def test_un_lien_deja_nomme_n_est_pas_touche() -> None:
    deja = '<a href="/contact" aria-label="Nous ecrire"><svg /></a>'
    sortie, n = _poser(deja)
    assert n == 0 and sortie == deja


def test_un_title_existant_vaut_deja_comme_nom() -> None:
    deja = '<a href="/contact" title="Nous ecrire"><svg /></a>'
    sortie, n = _poser(deja)
    assert n == 0 and sortie == deja


def test_repasser_sur_un_fichier_DEJA_corrige_n_ecrit_rien() -> None:
    """La boucle de verification recrawle et rejoue : deux aria-label seraient du HTML invalide."""
    une_fois, n1 = _poser(MUET)
    deux_fois, n2 = _poser(une_fois)
    assert n1 == 1 and n2 == 0
    assert deux_fois == une_fois
    assert deux_fois.count("aria-label") == 1


def test_un_lien_qui_porte_un_VRAI_texte_est_laisse_tranquille() -> None:
    """Le fichier n'est alors pas celui que le crawl a vu, ou il a ete corrige entre-temps."""
    vrai = '<a href="/contact">Contactez-nous</a>'
    sortie, n = _poser(vrai)
    assert n == 0 and sortie == vrai


def test_un_gabarit_dont_on_ignore_le_RENDU_est_laisse_tranquille() -> None:
    """`{t('nav.contact')}` rend peut-etre deja une ancre parfaite : on ne devine pas."""
    gabarit = "<a href=\"/contact\">{t('nav.contact')}</a>"
    sortie, n = _poser(gabarit)
    assert n == 0 and sortie == gabarit


def test_un_lien_vers_une_AUTRE_cible_n_est_jamais_confondu() -> None:
    """On ne resout aucune URL : le href du fichier doit etre litteralement celui du crawl."""
    autre = '<a href="/contacts"><svg /></a>\n<a href="../contact"><svg /></a>'
    sortie, n = _poser(autre)
    assert n == 0 and sortie == autre


def test_une_preuve_sans_nom_ne_touche_rien() -> None:
    sortie, n = _poser(MUET, [{"page": S + "/", "field": "/contact", "value": ""}])
    assert n == 0 and sortie == MUET


def test_une_preuve_vide_ne_touche_rien() -> None:
    sortie, n = _poser(MUET, [])
    assert n == 0 and sortie == MUET


def test_un_attribut_JSX_qui_porte_un_chevron_ne_fait_rien_ecrire() -> None:
    """`onClick={() => f()}` coupe la balise en deux pour une regex ; l'abstention est la bonne
    issue, l'ecriture au jugé ne l'est pas."""
    jsx = '<a href="/contact" onClick={() => go()}><svg /></a>'
    sortie, n = _poser(jsx)
    assert n == 0 and sortie == jsx


# --- le branchement, seul endroit ou les deux moities se rejoignent --------------------------

def _bloc(items: list[dict[str, str]]) -> dict[str, object]:
    return {"count": 1, "examples": [S + "/"],
            "evidence": {"kind": "page_values", "items": list(items)}}


def _prepare(items: list[dict[str, str]]) -> dict[str, object]:
    return app_module._prepare_issue_fix(
        issue_key=CLE, issues={CLE: _bloc(items)},
        impacted=[S + "/"], all_paths=["src/components/Header.astro", "src/pages/index.astro"],
        site_name="exemple.fr", owner="o", repo_name="r", branch="b", token="")


def test_le_BRANCHEMENT_fournit_vraiment_le_reecriveur() -> None:
    """Sans ce test, la famille peut etre declaree, rangee, couverte — et muette en production."""
    out = _prepare(ITEMS)
    assert out.get("refusal") in (None, ""), out.get("refusal")
    assert out.get("evidence") == [S + "/"], out.get("evidence")
    # Aucun repli modele : le nom vient de la cible, deja mesure.
    assert out.get("rewriter_ai_fallback") is False
    rw = out.get("link_rewriter")
    assert callable(rw), out
    sortie, n = rw(MUET)
    assert n == 1, sortie
    assert 'aria-label="Contactez-nous"' in sortie, sortie


def test_le_BRANCHEMENT_refuse_quand_la_preuve_manque() -> None:
    """Sans preuve, la famille doit se taire avec un motif — pas offrir un reecriveur aveugle."""
    out = _prepare([])
    assert out.get("refusal"), out
    assert out.get("link_rewriter") is None, out


def test_la_famille_est_declaree_corrigeable() -> None:
    assert CLE in set(app_module._handled_issue_keys())
    assert app_module._github_issue_auto_fixable(CLE)


def test_la_famille_est_rangee_sur_les_fichiers_de_PAGE() -> None:
    """Un <a> vit dans le corps d'une page ou d'un en-tete, jamais dans un sitemap ni un robots."""
    groupes = {nom: cles for nom, cles, _ in app_module._issue_file_families()}
    assert CLE in groupes["links"]
    assert [nom for nom, cles in groupes.items() if CLE in cles] == ["links"]
