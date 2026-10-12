"""Les jumelles du banc sont de VRAIES jumelles ; toutes les autres pages ont un corps propre.

`duplicate_pages_without_canonical` vise des pages DUPLIQUEES. Jusqu'au 12/10/2026 les deux
jumelles du parcours ne partageaient que titre et description, leur h1 et leur texte citant leur
propre nom : le correcteur, qui exige la preuve d'un contenu identique avant de poser un
canonical, les refusait sur les dix stacks — a raison. A l'inverse, depuis le 14/09/2026, deux
pages qui ne sont PAS des jumelles ne doivent pas avoir le meme corps (`duplicate-a`/`-b`).
"""

from ops.gauntlet.catalogue import CATALOGUE
from ops.gauntlet.emit_stacks import SITES, body_bits

SITE = SITES["static-html"]


def test_twins_share_their_whole_body() -> None:
    jumelles = [s for s in CATALOGUE if s.family == "duplicate_pages_without_canonical"]
    assert len(jumelles) == 2 and jumelles[0].jumelle and jumelles[0].jumelle == jumelles[1].jumelle
    assert body_bits(jumelles[0], SITE) == body_bits(jumelles[1], SITE)


def test_every_other_page_keeps_a_body_of_its_own() -> None:
    corps = [tuple(body_bits(s, SITE)) for s in CATALOGUE if not s.jumelle]
    assert len(corps) == len(set(corps))


def test_only_the_designed_pairs_share_a_title_or_a_description() -> None:
    # Jusqu'au 12/10/2026, TITLE et DESC etaient communs a une trentaine de pages par stack : les
    # familles de doublons ne pouvaient jamais se resoudre a un plafond de banc.
    from collections import Counter
    for champ in ("title", "desc"):
        partages = {v: n for v, n in Counter(getattr(s, champ) for s in CATALOGUE if getattr(s, champ)).items() if n > 1}
        assert sorted(partages.values()) == [2, 2], (champ, partages)


def test_default_titles_and_descriptions_stay_inside_the_crawler_windows() -> None:
    # Une page du parcours n'est fautive que sur SON point : ni titre ni description hors limites.
    from ops.gauntlet.catalogue import PROPRE, Spec
    for spec in CATALOGUE:
        defaut = Spec(spec.slug, spec.family, spec.note)
        assert PROPRE not in (defaut.title, defaut.desc)
        assert 15 <= len(defaut.title) <= 70 and 100 <= len(defaut.desc) <= 160, spec.slug
