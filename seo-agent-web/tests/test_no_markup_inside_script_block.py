"""Une balise nue au milieu d'un module JavaScript casse la construction du site.

SIXIEME forme de cassure du modele, mesuree le 17/09/2026 sur sveltekit, page `og-missing` :
charge d'ajouter les balises Open Graph manquantes, il les a posees DANS le bloc `<script>` au
lieu de `<svelte:head>`.

    <script>
      // FAMILLE VISEE : open_graph_tags_missing
      <meta property="og:title" content="…" />      <- erreur de syntaxe JavaScript
    </script>

Deploiement Netlify en echec, preview en 404 sur toutes les pages. PREMIER deploiement rouge en
cinq cycles, et il est passe par le seul trou que les controles existants laissaient : les
delimiteurs restent equilibres — aucune accolade ajoutee —, ce n'est pas du front matter, et
aucun garde-fou n'analyse le JavaScript.

On REFUSE plutot que de deplacer les balises. Savoir ou elles devraient aller serait deviner, et
c'est deviner qui a produit la panne. Un refus coute une correction et se voit ; un build casse
coute le site et ne se voit pas.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-script-markup-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

CHEMIN = "src/routes/gauntlet/og-missing/+page.svelte"

CASSE = """<script>
  // FAMILLE VISEE : open_graph_tags_missing
  <meta property="og:title" content="Page de test" />
  <meta property="og:type" content="website" />
</script>

<svelte:head>
  <title>Page de test</title>
</svelte:head>

<h1>Page</h1>
"""

CORRECT = """<script>
  // FAMILLE VISEE : open_graph_tags_missing
</script>

<svelte:head>
  <title>Page de test</title>
  <meta property="og:title" content="Page de test" />
  <meta property="og:type" content="website" />
</svelte:head>

<h1>Page</h1>
"""


def test_le_cas_mesure_sur_sveltekit_est_refuse() -> None:
    raison = app_module._markup_in_script_error(CASSE, CHEMIN)
    assert raison, "le fichier qui a casse le deploiement passerait encore"
    assert "<meta" in raison, raison


def test_les_balises_AU_BON_ENDROIT_passent() -> None:
    """Sans cette moitie, le garde-fou pourrait refuser toute correction Open Graph."""
    assert app_module._markup_in_script_error(CORRECT, CHEMIN) == ""


def test_une_balise_CITEE_dans_une_chaine_n_est_pas_du_balisage() -> None:
    """`document.write('<meta …>')` est du JavaScript valide : le refus doit rester etroit."""
    source = ("<script>\n"
              "  const t = '<meta name=\"x\" content=\"y\" />';\n"
              "  document.head.insertAdjacentHTML('beforeend', t);\n"
              "</script>\n")
    assert app_module._markup_in_script_error(source, CHEMIN) == ""


def test_le_JSX_n_est_PAS_concerne() -> None:
    """Du balisage dans une fonction est la norme en JSX — y appliquer la regle casserait
    les deux stacks Next et gatsby, ou chaque page est exactement cela."""
    jsx = ("export default function Page() {\n"
           "  return (\n"
           "    <main>\n"
           "      <meta property=\"og:title\" content=\"x\" />\n"
           "    </main>\n"
           "  );\n"
           "}\n")
    assert app_module._markup_in_script_error(jsx, "pages/gauntlet/og-missing.js") == ""


def test_vue_est_couvert_comme_svelte() -> None:
    """Meme mecanique : le bloc `<script>` d'un composant Vue est un module compile."""
    assert app_module._markup_in_script_error(CASSE, "pages/gauntlet/og-missing.vue") != ""


def test_un_fichier_HTML_ordinaire_n_est_pas_concerne() -> None:
    """Dans une page HTML, un `<script>` est du script de navigateur : on n'y touche pas."""
    assert app_module._markup_in_script_error(CASSE, "public/gauntlet/og-missing.html") == ""
