"""Reading what competitors build pages about, without buying a keyword database.

Search Console only ever describes your own site. Rather than pay for a keyword database, this
reads what the crawler can already see: the subject each competitor page declares about itself.
No search volume — that is a market estimate, and for a topic you already cover the useful
question is whether a competitor answers it better than your page does.

The verdict that matters is `covered`. It decides whether a retargeting PR may be opened at one
of your pages, so a false positive is the expensive direction: it would rewrite a page's title
for a subject the page does not treat, which is keyword stuffing and makes the site worse.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

from backend import competitors as comp  # noqa: E402


def page(url, title="", h1=None, status=200, error=None):
    row = {"url": url, "title": title, "status_code": status}
    if h1 is not None:
        row["h1"] = h1
    if error:
        row["error"] = error
    return row


# ── reading a page's subject ──────────────────────────────────────────────────────────────────

def test_the_subject_comes_from_title_h1_and_slug() -> None:
    terms = comp.page_terms(page("https://x.fr/tarifs-kling-ai", "Prix Kling AI", ["Kling AI 2026"]))
    assert {"tarifs", "kling", "ai", "prix", "2026"} <= terms


def test_stopwords_carry_no_subject() -> None:
    """Both languages, because these sites are routinely multilingual and one list would let
    the other language's filler dominate every comparison."""
    terms = comp.page_terms(page("https://x.fr/", "Le guide de la voix et the best of it"))
    assert "guide" in terms and "voix" in terms
    assert not ({"le", "de", "la", "et", "the", "of", "it"} & terms)


def test_a_year_is_a_subject_but_a_bare_number_is_not() -> None:
    terms = comp.page_terms(page("https://x.fr/p", "Kling AI 2026 en 12 etapes"))
    assert "2026" in terms
    assert "12" not in terms


def test_two_letter_terms_survive() -> None:
    # "ai" is the subject of this entire market; a three-character floor would erase it.
    assert "ai" in comp.page_terms(page("https://x.fr/p", "Kling AI"))


# ── comparing ─────────────────────────────────────────────────────────────────────────────────

OWN = [
    page("https://moi.fr/guide-kling-ai", "Guide Kling AI : demos et fonctionnalites"),
    page("https://moi.fr/contact", "Contact"),
]


def test_a_subject_this_site_treats_is_covered_and_names_the_page() -> None:
    findings = comp.compare(OWN, [page("https://eux.fr/kling-ai-guide", "Kling AI : le guide complet")])
    assert len(findings) == 1
    assert findings[0]["covered"] is True
    assert findings[0]["own_url"] == "https://moi.fr/guide-kling-ai"


def test_a_subject_nobody_here_treats_is_reported_without_a_target(caplog) -> None:
    """The whole point of the split: reported, never acted on. Putting a keyword into a page
    that does not cover the subject is stuffing."""
    findings = comp.compare(OWN, [page("https://eux.fr/comptabilite-freelance", "Comptabilite pour freelance")])
    assert findings[0]["covered"] is False
    assert findings[0]["own_url"] == "", "an uncovered subject must carry no page to rewrite"


def test_a_weak_match_does_not_count_as_covered() -> None:
    # One shared term out of many is not the same subject, and acting on it is the expensive
    # mistake this threshold exists to prevent.
    findings = comp.compare(OWN, [page("https://eux.fr/ai-comptabilite", "AI pour la comptabilite")])
    assert findings[0]["covered"] is False


def test_one_shared_term_is_a_coincidence_of_vocabulary_not_a_subject() -> None:
    """The floor that a ratio alone could not provide.

    Measuring against the SHORTER page makes a two-term competitor page easy to "cover": sharing
    the single term `ai` with a five-term page here scores 0.5 and would have sent a retargeting
    PR at a page about something else.
    """
    findings = comp.compare(OWN, [page("https://eux.fr/ai-comptabilite", "AI pour la comptabilite")])
    assert findings[0]["match_score"] >= 0.5, "the ratio alone would have called this covered"
    assert findings[0]["covered"] is False
    assert findings[0]["shared_terms"] == ["ai"]


def test_the_threshold_is_the_dial_once_the_floor_is_met() -> None:
    """Both guards are real and independent: the floor rejects coincidences of vocabulary, and
    the ratio rejects a wide page that merely happens to touch the subject."""
    own = [page("https://moi.fr/p", "Guide Kling AI demos fonctionnalites tarifs avis workflow")]
    competitor = [page("https://eux.fr/p",
                       "Kling AI guide comptabilite facturation devis paie fiscalite juridique")]
    # Three shared terms clears the floor; 3 of 8 is below the default ratio.
    assert comp.compare(own, competitor)[0]["match_score"] < 0.5
    assert comp.compare(own, competitor)[0]["covered"] is False
    assert comp.compare(own, competitor, threshold=0.3)[0]["covered"] is True


def test_uncovered_subjects_are_listed_first() -> None:
    findings = comp.compare(OWN, [
        page("https://eux.fr/kling-ai-guide", "Kling AI : le guide complet"),
        page("https://eux.fr/comptabilite-freelance", "Comptabilite pour freelance"),
    ])
    assert findings[0]["covered"] is False, "the more interesting finding should lead"


# ── what must never reach a comparison ────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "label, bad",
    [
        ("a page that errored", page("https://eux.fr/x", "Titre", error="timeout")),
        ("a 404", page("https://eux.fr/x", "Titre", status=404)),
        ("a page with no url", page("", "Titre")),
    ],
)
def test_a_page_the_crawl_could_not_read_is_skipped(label, bad) -> None:
    assert comp.compare(OWN, [bad]) == [], label


def test_a_page_that_says_nothing_about_itself_is_skipped() -> None:
    """A one-term page matches things by accident, and an accidental match is a bad PR."""
    assert comp.compare(OWN, [page("https://eux.fr/", "Accueil")]) == []


def test_an_own_page_that_errored_is_not_offered_as_a_target() -> None:
    own = [page("https://moi.fr/guide-kling-ai", "Guide Kling AI", error="timeout")]
    findings = comp.compare(own, [page("https://eux.fr/kling-ai-guide", "Kling AI : le guide complet")])
    assert findings[0]["covered"] is False


def test_the_summary_counts_both_verdicts() -> None:
    findings = comp.compare(OWN, [
        page("https://eux.fr/kling-ai-guide", "Kling AI : le guide complet"),
        page("https://eux.fr/comptabilite-freelance", "Comptabilite pour freelance"),
    ])
    assert comp.summarise(findings) == {"total": 2, "covered": 1, "uncovered": 1}


def test_overlap_is_measured_against_the_shorter_page() -> None:
    """Not Jaccard: a long competitor guide and a short page here can be about exactly the same
    thing, and Jaccard would punish the size difference instead of measuring the subject."""
    short = {"kling", "ai"}
    long = {"kling", "ai", "guide", "demos", "fonctionnalites", "prix", "2026"}
    assert comp.overlap(short, long) == 1.0


# ── the noise real data exposed, each pinned so it cannot come back ────────────────────────────

def test_a_language_prefix_is_not_a_subject() -> None:
    """`blog`, `es` and `de` were scored as shared subject terms, so a tutorial about creating a
    voice matched a page about podcasts on {2026, blog, elevenlabs, ia, voix} — three of five
    carrying no meaning. A language prefix says which audience, never what about."""
    terms = comp.page_terms(page("https://x.fr/de/blog/kling-ai", "Kling AI Guide"))
    assert "kling" in terms and "ai" in terms
    assert not ({"de", "blog"} & terms)


@pytest.mark.parametrize("url, title", [
    ("https://eux.fr/mentions-legales", "Mentions légales"),
    ("https://eux.fr/privacy-policy", "Privacy Policy"),
    ("https://eux.fr/impressum", "Impressum"),
    ("https://eux.fr/p", "Política de privacidad"),
    ("https://eux.fr/contact", "Contact"),
])
def test_a_utility_page_is_never_a_keyword_opportunity(url, title) -> None:
    """They match each other beautifully — "Legal Notice" against "Legal Notice" scored 0.667 —
    and retargeting one would be absurd. Dropped before comparison, not ranked low inside it."""
    own = [page("https://moi.fr/mentions-legales", "Mentions légales")]
    assert comp.compare(own, [page(url, title)]) == []


def test_the_same_subject_stated_on_several_urls_is_one_finding() -> None:
    """A real competitor published the same subject on three URLs, and a URL key still listed it
    three times."""
    rival = [
        page("https://eux.fr/", "ElevenLabs Avis | Voix IA, clonage vocal et guide"),
        page("https://eux.fr/fr", "ElevenLabs Avis | Voix IA, clonage vocal et guide"),
        page("https://eux.fr/fr/", "ElevenLabs Avis | Voix IA, clonage vocal et guide"),
    ]
    assert len(comp.compare(OWN, rival)) == 1


def test_the_home_page_is_never_offered_as_the_page_to_retarget() -> None:
    """It is about the brand, not a subject: it borrows a little of every topic and matched five
    different competitor articles at exactly the floor."""
    own = [page("https://moi.fr/", "Mon site — guides Kling AI, ElevenLabs et Pictory")]
    findings = comp.compare(own, [page("https://eux.fr/kling-ai-guide", "Kling AI : le guide complet")])
    assert findings[0]["covered"] is False
    assert findings[0]["own_url"] == ""


def test_three_shared_terms_is_the_line_the_real_data_drew() -> None:
    # Every sound match on two live sites shared four terms; every spurious one shared two.
    assert comp.MIN_SHARED_TERMS == 3
