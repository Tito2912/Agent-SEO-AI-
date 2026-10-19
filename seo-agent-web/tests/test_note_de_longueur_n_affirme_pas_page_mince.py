"""La note « non corrigés » d'une famille de longueur n'affirme pas une cause qu'elle ignore.

Elle ne reçoit que des chemins de fichiers. Elle disait pourtant « c'est le refus attendu sur une
page mince » : la PR #3 de creativeai-tools l'a écrit pour /fr/tools/, qui n'en est pas une. Elle
nomme maintenant les deux cas, et ce qui reste à faire.
"""
from __future__ import annotations

from backend import app as m


def test_la_cause_reste_conditionnelle_et_la_suite_est_dite():
    note = m._skipped_note("meta_description_too_short", ["content/fr/tools/index.mdx"],
                           ["content/fr/tools/index.mdx", "content/fr/blog/index.mdx"])
    assert "Non corrigés : 1 fichier(s) sur 2" in note and "`content/fr/tools/index.mdx`" in note
    assert "Sur une page mince" in note and "refus attendu sur une page mince" not in note
    assert "à rédiger à la main" in note
