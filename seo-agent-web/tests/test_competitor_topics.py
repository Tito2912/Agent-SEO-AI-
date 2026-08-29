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


# ── what the FIRST REAL RUN exposed (2026-08-29, voiceoverstudioai vs elevenlabs-avis) ────────
#
# Both defects are in the PAIRING, not in the subject reading, and neither was visible offline:
# the engine had been driven on page lists, and the screen is what put a retarget button next to
# each match. What the real 20 subjects showed was a German rival page paired with an English
# page here, and the rival's blog index paired with ours.

def _lang_page(url, title, lang=None, h1=None):
    row = page(url, title, h1 or [title])
    if lang:
        row["lang"] = lang
    return row


def test_a_subject_is_stated_in_a_language_and_the_pair_must_agree() -> None:
    """Real pair: "Erstellen Sie eine realistische ElevenLabs AI Voice…" matched
    /blog/elevenlabs-for-podcasts-2026, an ENGLISH page, on {elevenlabs, voice, ai, 2026}.
    Retargeting it would have aimed a German subject at the wrong locale while the German page
    sat one URL away."""
    rival = _lang_page("https://rival.fr/de/elevenlabs-stimme-2026",
                       "Erstellen Sie eine realistische ElevenLabs AI Voice in 5 Minuten", "de")
    english = _lang_page("https://site.fr/blog/elevenlabs-voice-2026",
                         "Build a realistic ElevenLabs AI voice in 5 minutes", "en")
    german = _lang_page("https://site.fr/blog/elevenlabs-stimme-2026-de",
                        "ElevenLabs AI Stimme in 5 Minuten erstellen", "de")

    only_english = comp.compare([english], [rival])
    assert only_english[0]["covered"] is False, "a German subject was covered by an English page"

    with_german = comp.compare([english, german], [rival])
    assert with_german[0]["covered"] is True
    assert with_german[0]["own_url"] == german["url"], "the wrong locale was chosen"


def test_a_page_that_does_not_declare_its_language_still_matches() -> None:
    """Abstention, like every other guard here: Gatsby and Nuxt emit no <html lang> by default,
    and refusing those matches would cost more than the drift it prevents."""
    rival = _lang_page("https://rival.fr/x", "ElevenLabs voice cloning 2026", "en")
    mine = page("https://site.fr/elevenlabs-voice-cloning-2026", "ElevenLabs voice cloning 2026")
    assert comp.compare([mine], [rival])[0]["covered"] is True


def test_the_language_can_come_from_the_url_when_the_page_is_silent() -> None:
    assert comp.page_language({"url": "https://site.fr/blog/guide-kling-ai-fr"}) == "fr"
    assert comp.page_language({"url": "https://site.fr/de/blog/x"}) == "de"
    assert comp.page_language({"url": "https://site.fr/blog/kling-ai-pricing-2026"}) == ""
    assert comp.page_language({"url": "https://site.fr/x", "lang": "de-DE"}) == "de"


def test_a_listing_page_is_never_the_page_to_retarget() -> None:
    """Real pair: the rival's blog index matched /blog here at the floor. A listing is a shelf,
    not a subject — rewriting its title toward one article's subject describes the shelf as if it
    were one book, and moves a page that ranks for the section name."""
    rival = page("https://rival.fr/blog", "ElevenLabs Reviews Blog — AI voice & voiceover guides")
    listing = page("https://site.fr/blog", "Blog — guides voix IA et voiceover ElevenLabs")
    findings = comp.compare([listing], [rival])
    assert findings and findings[0]["covered"] is False
    assert findings[0]["own_url"] == ""


@pytest.mark.parametrize(
    "url, is_listing",
    [
        ("https://site.fr/blog", True),
        ("https://site.fr/en/blog", True),
        ("https://site.fr/fr/actualites", True),
        ("https://site.fr/tags", True),
        # The word has to BE the page, not appear in it — the substring version of this test
        # would drop real articles from retargeting.
        ("https://site.fr/blog/article", False),
        ("https://site.fr/blog/news-du-mois", False),
        ("https://site.fr/blog/elevenlabs-2026", False),
        ("https://site.fr/", False),
    ],
)
def test_the_listing_rule_matches_a_segment_not_a_substring(url, is_listing) -> None:
    assert comp._is_listing_page(url) is is_listing
