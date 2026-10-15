# -*- coding: utf-8 -*-
"""Le corps d'une PR ne doit rien laisser tomber DANS une citation qui ne le concerne pas.

PR #1 de cityscootlab, 15/10/2026, telle que GitHub l'affichait :

    > ⏭️ Non corriges : 2 fichier(s) sur 2. … resteront signalees au prochain crawl :
    > - app/(en)/page.tsx
    > - content/index.mdx
      ✅ Balise title trop courte — content/de/about.mdx, …      <- DANS la citation

La note finissait sans ligne vide ; la ligne suivante s'y est collee (continuation « paresseuse »
du Markdown). Meme chose pour l'avertissement « A relire », avale par la liste des « Non traites ».
Ces tests lisent le RENDU (CommonMark, comme GitHub), pas la forme du texte.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-corps-pr-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")

from backend import app as m  # noqa: E402

markdown_it = pytest.importorskip("markdown_it")


def _paragraphes(md: str) -> list[tuple[str, bool, bool]]:
    """(texte, dans une citation ?, dans une liste ?) pour chaque bloc de texte rendu."""
    out, citation, liste = [], 0, 0
    for tok in markdown_it.MarkdownIt("commonmark").parse(md):
        if tok.type == "blockquote_open":
            citation += 1
        elif tok.type == "blockquote_close":
            citation -= 1
        elif tok.type in ("bullet_list_open", "ordered_list_open"):
            liste += 1
        elif tok.type in ("bullet_list_close", "ordered_list_close"):
            liste -= 1
        elif tok.type == "inline":
            out.append((tok.content, citation > 0, liste > 0))
    return out


def _ou(md: str, debut: str) -> tuple[bool, bool]:
    trouves = [(c, q, li) for c, q, li in _paragraphes(md) if c.startswith(debut)]
    assert trouves, f"« {debut} » absent du rendu"
    return trouves[0][1], trouves[0][2]


def _famille(label: str, *, ok: bool = True, files: list[str] | None = None, error: str = "",
             notes: str = "", config: list[str] | None = None) -> dict:
    return {"issue_label": label, "ok": ok, "files": files or [], "error": error, "notes": notes,
            "config_fixed": config or []}


def _corps_cityscootlab() -> str:
    non_corriges = m._blocs_markdown(
        m._skipped_note("hreflang_to_redirect_or_broken_page", ["app/(en)/page.tsx", "content/index.mdx"],
                        ["app/(en)/page.tsx", "content/index.mdx"]))
    non_traites = m._blocs_markdown(m._note_de_troncature([f"content/de/p{i}.mdx" for i in range(16)], None))
    return m._corps_pr_groupee(site_name="cityscootlab.com", ts="20261015-032749", files_total=32,
                               ai_written=True, premise_key="", results=[
        _famille("Canonical → redirection", files=["content/de/index.mdx"]),
        _famille("Hreflang vers redirect / URL cassée", ok=False, error="Aucun patch appliqué",
                 notes=non_corriges),
        _famille("Balise title trop courte", files=["content/de/about.mdx"]),
        _famille("Page → lien vers redirection (indexable)", files=["content/fr/blog.mdx"], notes=non_traites),
    ])


def test_la_famille_qui_suit_une_note_reste_hors_de_la_citation() -> None:
    assert _ou(_corps_cityscootlab(), "✅ **Balise title trop courte**") == (False, False)


def test_l_avertissement_a_relire_ne_tombe_pas_dans_la_liste_des_non_traites() -> None:
    dans_citation, dans_liste = _ou(_corps_cityscootlab(), "⚠️ **À relire avant de merger.**")
    assert dans_citation and not dans_liste


def test_les_notes_restent_des_citations_et_leurs_listes_aussi() -> None:
    corps = _corps_cityscootlab()
    assert _ou(corps, "⏭️ **Non corrigés") == (True, False)
    assert _ou(corps, "`content/de/p0.mdx`") == (True, True)
    assert _ou(corps, "Correction générée par") == (False, False)


def test_une_ligne_de_configuration_n_avale_pas_la_famille_suivante() -> None:
    corps = m._corps_pr_groupee(site_name="s", ts="t", files_total=1, ai_written=False, premise_key="", results=[
        _famille("Boucle de redirection", files=["netlify.toml"], config=["redirection /de retiree"]),
        _famille("Balise title trop courte", files=["a.mdx"]),
    ])

    assert _ou(corps, "✅ **Balise title trop courte**") == (False, False)
    assert _ou(corps, "redirection /de retiree") == (False, True)


def test_un_effet_de_bord_sur_une_ligne_nue_sort_de_la_citation() -> None:
    """Certains effets de bord commencent par un simple saut de ligne : colles sous une note,
    ils y entraient aussi — dans la PR groupee comme dans celle d'une seule famille."""
    notes = m._notes_de_famille("k", ["a.html"], ["a.html"], ["b.html"], None,
                                "\nEntrees laissees sans correction faute de destination verifiee : /x")

    assert _ou(notes, "⏭️ **Non corrigés") == (True, False)
    assert _ou(notes, "📄 **Non traités") == (True, False)
    assert _ou(notes, "Entrees laissees") == (False, False)


def test_sans_note_rien_n_est_ajoute() -> None:
    assert m._notes_de_famille("k", [], [], [], None, None) == ""


def test_les_blocs_vides_disparaissent_et_chaque_bloc_est_separe() -> None:
    assert m._blocs_markdown("\n\nA\n", "", "  ", "\nB") == "A\n\nB"
