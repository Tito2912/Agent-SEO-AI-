"""Ajouter UNE balise Open Graph a une page qui n'en avait aucune, c'est troquer une anomalie.

Mesure du 17/09/2026, TROIS stacks a la fois — static-html, hugo, gatsby — page `og-missing` :
chargee d'ajouter les balises Open Graph absentes, la correction n'a ecrit que `og:title`. La page
est passee de `open_graph_tags_missing` a `open_graph_tags_incomplete`, et le bilan affichait `ok`.

    avant   (aucune balise og)
    apres   <meta property="og:title" content="…" />        et rien d'autre

La consigne LISTE pourtant les cinq balises exigees depuis toujours. Une consigne se discute ;
c'est donc une verification qu'il fallait — la meme lecon qu'au plancher de longueur et qu'a la
maitresse canonique.

CE QU'ON NE DEVINE PAS : l'idiome. Le modele vient d'ecrire une balise dans ce fichier, et le code
CLONE la ligne qu'il a produite — indentation, guillemets, style de fermeture — en n'y changeant
que la propriete et la valeur. Les valeurs se lisent dans la page, sauf `og:image` que le site
declare ailleurs et qu'on MESURE (`_dominant_site_og_image`), jamais qu'on invente.
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-og-complet-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

IMAGE = "https://exemple.fr/og.png"
TITRE = "Page de test du parcours d'obstacles"
DESC = "Une description de la page, assez longue pour depasser le plancher de cent caracteres sans peine."

SANS_OG = f"""<!doctype html><html lang="fr"><head>
  <title>{TITRE}</title>
  <meta name="description" content="{DESC}" />
  <link rel="canonical" href="https://exemple.fr/gauntlet/og-missing" />
</head><body><h1>Page</h1></body></html>
"""

AVEC_TITRE_SEUL = SANS_OG.replace(
    "</head>", '  <meta property="og:title" content="%s" />\n</head>' % TITRE)


def _og(contenu: str) -> dict[str, str]:
    return {m.group(1): m.group(2) for m in
            re.finditer(r'<meta property="og:([a-z]+)" content="(.*?)"', contenu)}


def test_le_cas_mesure_sur_trois_stacks_est_complete() -> None:
    sortie, notes = app_module._complete_open_graph(AVEC_TITRE_SEUL, SANS_OG, IMAGE)
    tags = _og(sortie)
    assert set(tags) == {"title", "description", "image", "url", "type"}, tags
    assert tags["title"] == TITRE
    assert tags["description"] == DESC
    assert tags["image"] == IMAGE
    assert tags["url"] == "https://exemple.fr/gauntlet/og-missing"
    assert len(notes) == 4, notes


def test_une_page_qui_avait_DEJA_de_l_open_graph_n_est_pas_touchee() -> None:
    """Ce cas appartient a `open_graph_tags_incomplete` et a ses propres preuves.

    Sans cette borne, le garde-fou completerait n'importe quel bloc OG a chaque patch, y compris
    sur des pages qu'aucun crawl n'a signalees.
    """
    sortie, notes = app_module._complete_open_graph(AVEC_TITRE_SEUL, AVEC_TITRE_SEUL, IMAGE)
    assert sortie == AVEC_TITRE_SEUL and notes == [], notes


def test_sans_image_connue_du_site_on_n_en_invente_pas() -> None:
    """Mieux vaut un bloc incomplet qu'un og:image invente : l'image serait fausse sur chaque
    partage social, et personne ne le verrait depuis le rapport."""
    sortie, notes = app_module._complete_open_graph(AVEC_TITRE_SEUL, SANS_OG, "")
    tags = _og(sortie)
    assert "image" not in tags, tags
    assert set(tags) == {"title", "description", "url", "type"}, tags


def test_l_idiome_du_modele_est_CLONE_pas_devine() -> None:
    """Guillemets simples et fermeture sans barre : la ligne ajoutee doit s'y conformer."""
    source = SANS_OG.replace(
        "</head>", "  <meta property='og:title' content='Un titre'>\n</head>")
    sortie, _ = app_module._complete_open_graph(source, SANS_OG, IMAGE)
    ajoutee = next(x for x in sortie.split("\n") if "og:type" in x)
    assert ajoutee.strip() == "<meta property='og:type' content='website'>", ajoutee


def test_les_formes_OBJET_sont_laissees_tranquilles() -> None:
    """nuxt et next-app decrivent l'Open Graph par des objets : savoir ou s'inserer dans une
    structure demanderait de deviner, et se tromper y casse la construction."""
    source = "useHead({ meta: [{ property: 'og:title', content: 'Un titre' }] })"
    sortie, notes = app_module._complete_open_graph(source, "useHead({ meta: [] })", IMAGE)
    assert sortie == source and notes == [], notes


def test_l_image_du_site_se_MESURE_sur_les_pages_crawlees() -> None:
    pages = [{"og_image": IMAGE}, {"og_image": IMAGE}, {"og_image": IMAGE},
             {"og_image": "https://exemple.fr/autre.png"}]
    assert app_module._dominant_site_og_image(pages) == IMAGE


def test_un_site_SANS_image_dominante_ne_rend_rien() -> None:
    """Trois images differentes : le site n'en a pas UNE, donc on ne choisit pas a sa place."""
    pages = [{"og_image": "https://exemple.fr/a.png"}, {"og_image": "https://exemple.fr/b.png"},
             {"og_image": "https://exemple.fr/c.png"}]
    assert app_module._dominant_site_og_image(pages) == ""


def test_deux_pages_ne_font_pas_une_mesure() -> None:
    assert app_module._dominant_site_og_image([{"og_image": IMAGE}, {"og_image": IMAGE}]) == ""
