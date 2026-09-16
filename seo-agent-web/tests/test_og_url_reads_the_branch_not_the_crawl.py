"""og:url s'aligne sur le canonical DU FICHIER, pas sur celui qu'un crawl perime avait mesure.

Le correcteur traite les familles une par une, mais la branche ACCUMULE : quand une famille de
canonical est passee avant, le fichier ne porte plus la valeur du rapport. Aligner og:url sur le
rapport ecrit alors une adresse perimee sur une balise qui allait bien.

Mesure du 16/09/2026, cycle complet sur les neuf idiomes :
`open_graph_url_not_matching_canonical` resistait sur les NEUF stacks — seule famille dans ce cas,
ce qui excluait un accident d'idiome et designait une cause commune.

Deux formes reproduites ici :

  canonical-404    `canonical_points_to_4xx` a repare le canonical vers la page elle-meme,
                   og:url recevait l'ancienne cible morte.
  canonical-other  `non_canonical_page_specified_as_canonical_one` a suivi la chaine
                   a -> b -> c jusqu'a son terme, og:url recevait le maillon du milieu.

Et deux abstentions : sans canonical litteral unique dans le fichier, on ne sait pas de quelle
page on parle, donc on garde ce que le crawl disait.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-og-branche-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

S = "https://exemple.fr"
PAGE = f"{S}/gauntlet/canonical-404"
MORTE = f"{S}/page-absente"


def _page_html(canonical: str, og: str) -> str:
    return (
        "<!doctype html><html lang=\"fr\"><head>\n"
        f'  <link rel="canonical" href="{canonical}" />\n'
        f'  <meta property="og:url" content="{og}" />\n'
        '  <meta property="og:title" content="Page du parcours" />\n'
        "</head><body><h1>Page</h1></body></html>\n"
    )


def test_le_canonical_repare_par_une_autre_famille_est_celui_qui_gagne() -> None:
    """Le crawl disait `to = /page-absente` ; la branche porte deja la page elle-meme."""
    # La paire vient du crawl : og:url etait juste, le canonical pointait la 404.
    paires = [{"page": PAGE, "from": PAGE, "to": MORTE}]
    # Mais le fichier a DEJA ete repare par `canonical_points_to_4xx`.
    source = _page_html(canonical=PAGE, og=PAGE)
    sortie, n = app_module._rewrite_og_url(source, paires)
    assert MORTE not in sortie, "og:url a recopie une 404 que la branche avait deja reparee"
    assert sortie == source and n == 0, sortie


def test_la_chaine_suivie_jusqu_au_bout_est_celle_qu_og_url_recoit() -> None:
    """a -> b -> c : le crawl ne connaissait que b, la branche porte deja c."""
    autre, relais, terme = f"{S}/a", f"{S}/b", f"{S}/c"
    paires = [{"page": autre, "from": autre, "to": relais}]
    source = _page_html(canonical=terme, og=autre)
    sortie, n = app_module._rewrite_og_url(source, paires)
    assert n == 1, sortie
    assert f'content="{terme}"' in sortie, sortie
    assert relais not in sortie, "og:url a recu le maillon du milieu de la chaine"


def test_sans_canonical_dans_le_fichier_le_crawl_reste_la_reference() -> None:
    """Un gabarit partage n'a pas de canonical litteral : on ne devine pas, on garde le rapport."""
    autre, cible = f"{S}/a", f"{S}/b"
    paires = [{"page": autre, "from": autre, "to": cible}]
    source = (
        "<head>\n"
        '  <link rel="canonical" href={`${base}${slug}`} />\n'
        f'  <meta property="og:url" content="{autre}" />\n'
        "</head>\n"
    )
    sortie, n = app_module._rewrite_og_url(source, paires)
    assert n == 1 and f'content="{cible}"' in sortie, sortie


def test_deux_canonicals_dans_un_fichier_font_renoncer_a_la_relecture() -> None:
    """Un fichier de donnees porte plusieurs pages : impossible de dire de laquelle on parle."""
    autre, cible = f"{S}/a", f"{S}/b"
    paires = [{"page": autre, "from": autre, "to": cible}]
    source = (
        "<head>\n"
        f'  <link rel="canonical" href="{S}/x" />\n'
        f'  <link rel="canonical" href="{S}/y" />\n'
        f'  <meta property="og:url" content="{autre}" />\n'
        "</head>\n"
    )
    sortie, n = app_module._rewrite_og_url(source, paires)
    assert n == 1 and f'content="{cible}"' in sortie, sortie


def test_une_page_non_signalee_reste_intouchee() -> None:
    """La propriete de securite d'origine : le crawl decide toujours QUOI reecrire.

    Relire le canonical du fichier change la DESTINATION, jamais la liste des emplacements. Sans
    ce test, la relecture pourrait deriver vers « aligne tout og:url sur le canonical », ce qui
    toucherait des pages qu'aucun crawl n'a signalees.
    """
    paires = [{"page": f"{S}/a", "from": f"{S}/a", "to": f"{S}/b"}]
    source = _page_html(canonical=f"{S}/zzz", og=f"{S}/jamais-signalee")
    sortie, n = app_module._rewrite_og_url(source, paires)
    assert n == 0 and sortie == source, sortie
