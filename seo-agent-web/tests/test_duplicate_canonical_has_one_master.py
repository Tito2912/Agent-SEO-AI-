"""Deux jumelles ne doivent JAMAIS se designer l'une l'autre : une boucle vaut moins que rien.

Mesure du 16/09/2026, branche hugo, famille `duplicate_pages_without_canonical` :

    no-canonical-a  ->  canonical vers no-canonical-b
    no-canonical-b  ->  canonical vers no-canonical-a

Chacune designe l'autre. Aucune des deux n'est indexable au profit de l'autre, et AUCUNE famille
du crawler ne signale la boucle — le defaut est muet, ce qui le rend pire.

Cause architecturale, la meme que pour les valeurs dupliquees : les fichiers partent EN PARALLELE
et aucun appel ne sait ce qu'un autre ecrit. Deux jumelles recoivent la meme consigne et repondent
« l'autre », chacune de son cote. Aucun modele ne peut corriger cela ; c'est au code de trancher
AVANT de demander — puis de verifier, parce qu'une consigne se discute.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-maitresse-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from backend import app as app_module  # noqa: E402

S = "https://exemple.fr"
A, B = f"{S}/gauntlet/no-canonical-a", f"{S}/gauntlet/no-canonical-bis"


def _page(url: str, titre: str, description: str = "") -> dict[str, object]:
    return {"url": url, "title": titre, "meta_description": description}


def test_les_deux_jumelles_recoivent_LA_MEME_maitresse() -> None:
    pages = [_page(A, "Un titre partage"), _page(B, "Un titre partage")]
    maitres = app_module._duplicate_canonical_masters([A, B], pages)
    assert set(maitres) == {A, B}, maitres
    assert len(set(maitres.values())) == 1, f"boucle possible : {maitres}"
    # La plus courte gagne, et la maitresse se designe elle-meme.
    assert maitres[A] == A and maitres[B] == A, maitres


def test_deux_groupes_distincts_ne_fusionnent_jamais() -> None:
    """Le danger symetrique : une seule maitresse pour tout desindexerait des pages sans rapport."""
    c, d = f"{S}/tarifs", f"{S}/tarifs-2"
    pages = [_page(A, "Titre un"), _page(B, "Titre un"),
             _page(c, "Titre deux"), _page(d, "Titre deux")]
    maitres = app_module._duplicate_canonical_masters([A, B, c, d], pages)
    assert maitres[A] == maitres[B] == A, maitres
    assert maitres[c] == maitres[d] == c, maitres
    assert maitres[A] != maitres[c], maitres


def test_une_page_seule_de_son_groupe_est_ecartee() -> None:
    """Rien ne dit qui serait sa maitresse, et se designer soi-meme n'apprend rien."""
    pages = [_page(A, "Un titre unique")]
    assert app_module._duplicate_canonical_masters([A], pages) == {}


def test_le_groupement_se_rabat_sur_la_description() -> None:
    """Le crawler groupe par titre OU par description : une page sans titre reste groupable."""
    pages = [_page(A, "", "La meme description partagee par les deux pages."),
             _page(B, "", "La meme description partagee par les deux pages.")]
    maitres = app_module._duplicate_canonical_masters([A, B], pages)
    assert maitres and len(set(maitres.values())) == 1, maitres


def test_le_choix_est_STABLE_d_un_passage_a_l_autre() -> None:
    """C'est la propriete qui interdit la boucle, et qu'aucune reponse de modele ne garantit."""
    pages = [_page(B, "Un titre partage"), _page(A, "Un titre partage")]
    premier = app_module._duplicate_canonical_masters([B, A], pages)
    second = app_module._duplicate_canonical_masters([A, B], list(reversed(pages)))
    assert premier == second, (premier, second)


def _html(canonical: str) -> str:
    return ('<!doctype html><html lang="fr"><head>\n'
            f'  <link rel="canonical" href="{canonical}" />\n'
            "</head><body><h1>Page</h1></body></html>\n")


def test_un_canonical_vers_la_JUMELLE_est_redirige_vers_la_maitresse() -> None:
    """La verification, celle qui ne demande rien au modele.

    Prevenu par la consigne, le modele a quand meme ecrit « l'autre » — c'est ce qui s'est passe.
    Le code connait la bonne valeur, donc il n'y a rien a deviner : il l'ecrit.
    """
    sortie, notes = app_module._keep_canonical_master(_html(B), A)
    assert f'href="{A}"' in sortie, sortie
    assert B not in sortie, sortie
    assert notes and "maitresse" in notes[0], notes


def test_un_canonical_DEJA_juste_n_est_pas_touche() -> None:
    page = _html(A)
    sortie, notes = app_module._keep_canonical_master(page, A)
    assert sortie == page and notes == [], notes


def test_sans_maitresse_connue_on_ne_touche_a_rien() -> None:
    page = _html(B)
    sortie, notes = app_module._keep_canonical_master(page, "")
    assert sortie == page and notes == [], notes


def test_la_PR_annonce_que_le_choix_de_la_maitresse_est_discutable() -> None:
    """Designer la page qui survit est un arbitrage du proprietaire, pas une mesure."""
    note = app_module._fix_premise_note("duplicate_pages_without_canonical")
    assert note and "CONVENTION" in note.upper(), note
