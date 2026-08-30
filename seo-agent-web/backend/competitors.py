"""What competitors build pages about, and whether this site answers it.

Search Console only ever describes your own site, so competitor research needs another source.
Rather than buy a keyword database, this reads what the crawler can already see: the subject
each competitor page declares about itself — its title, its H1, its URL slug. A competitor who
built twelve pages about a topic is telling you something about that topic, and that signal is
free, verifiable, and specific to the market you are actually in.

What this does NOT give is search volume. That comes from Google Ads or a paid vendor, and it is
a market estimate rather than a fact about your site. For a topic you already cover — the only
kind this product will retarget — the useful question is not "how many people search this" but
"does a competitor answer it better than my page does", and that is answerable without paying.

Two verdicts, and the difference matters:

  * **covered** — one of your pages is already about this subject, so its title and description
    can be retargeted. Safe: the content already exists.
  * **uncovered** — nothing on your site is about it. Reported, never acted on: putting a
    keyword into a page that does not cover the subject is stuffing, and it makes the site worse.

Pure and network-free: it takes crawled pages in and gives findings out.
"""

from __future__ import annotations

import re
from typing import Any, Iterable

# Words that carry no subject. Both languages, because the sites this product audits are
# routinely multilingual and a single-language list would let "the"/"les" dominate every overlap.
# `ai` is NOT in this list, though French grammar says it should be ("j'ai"). It is the single
# most important subject term in the market these sites compete in, and filtering it made every
# comparison about AI tools blind to what they were comparing. A stopword list is language
# grammar; a subject list is the market. When they disagree, the market wins.
_STOPWORDS = frozenset("""
a à au aux avec ce ces dans de des du elle en et eux il je la le les leur lui ma mais me
même mes moi mon ne nos notre nous on ou par pas pour qu que qui sa se ses son sur ta te tes
toi ton tu un une vos votre vous c d j l m n s t y été être avoir plus tout tous toute toutes
comment pourquoi quand
a an and are as at be but by for from has have how if in is it its of on or that the this to
was what when where which who why with your you
der die das den dem des ein eine einer eines und oder aber mit von zu im am ist sind war
fuer für auf aus bei nach über wie wenn nicht sich auch
el la los las un una unos unas y o pero con de del en por para es son era como cuando no se
tambien también sobre entre hasta
""".split())

# A term shorter than this is noise once stopwords are gone ("ai" survives on purpose — it is a
# subject in this market, and dropping two-letter terms would erase it).
_MIN_TERM = 2

# A ratio alone is not enough. Measuring against the SHORTER page makes a short competitor page
# easy to "cover": two generic terms in common — {elevenlabs, guide} — scored 0.5 against a
# listing page that borrows a little of every topic, and would have sent a retargeting PR at a
# page about something else.
# Set from the real comparison of two live sites: every sound match shared FOUR terms, every
# spurious one shared two. Three is the line between them, with room on both sides.
MIN_SHARED_TERMS = 3

# URL segments that describe a site's STRUCTURE, not a page's subject. Measured on two real
# sites: `blog`, `es` and `de` were being counted as shared subject terms, so a tutorial about
# creating a voice matched a page about podcasts at 0.625 on {2026, blog, elevenlabs, ia, voix}
# — three of the five carrying no meaning. A language prefix says which audience, never what about.
_STRUCTURAL_SLUG_TERMS = frozenset("""
en fr de es it pt nl pl ru ja zh ar
blog blogs article articles post posts page pages category categories tag tags index home
""".split())

# Pages every site has and nobody competes on. They match each other beautifully — "Legal
# Notice" against "Legal Notice" scores 0.667 — and retargeting one would be absurd, so they are
# dropped before the comparison rather than ranked low inside it.
_UTILITY_PAGE_RE = re.compile(
    r"(privacy|privacid|confidentialit|datenschutz|legal|mentions|impressum|aviso|"
    r"terms|conditions|cgu|cgv|contact|kontakt|contacto|cookie|sitemap|404)",
    re.I,
)

_SPLIT_RE = re.compile(r"[^\w]+", re.UNICODE)


def _terms(text: str) -> set[str]:
    """Significant lowercase terms of a piece of text."""
    out: set[str] = set()
    for raw in _SPLIT_RE.split(str(text or "").lower()):
        token = raw.strip()
        if len(token) < _MIN_TERM or token in _STOPWORDS or token.isdigit() and len(token) != 4:
            # A bare number is noise; a four-digit one is almost always a year, which IS a
            # subject in this market ("kling ai pricing 2026").
            continue
        out.add(token)
    return out


def _slug_terms(url: str) -> set[str]:
    """Terms from the URL path. A slug is the one place an author states the subject with no
    marketing on top, so it is worth as much as the title."""
    path = str(url or "")
    if "://" in path:
        path = path.split("://", 1)[1]
        path = path[path.find("/"):] if "/" in path else ""
    path = path.split("?", 1)[0].split("#", 1)[0]
    terms = _terms(path.replace("-", " ").replace("_", " ").replace("/", " "))
    return terms - _STRUCTURAL_SLUG_TERMS


def page_terms(page: dict[str, Any]) -> set[str]:
    """Everything a crawled page says about its own subject."""
    if not isinstance(page, dict):
        return set()
    h1 = page.get("h1")
    if isinstance(h1, list):
        h1_text = " ".join(str(x) for x in h1)
    else:
        h1_text = str(h1 or "")
    return (
        _terms(page.get("title"))
        | _terms(h1_text)
        | _slug_terms(page.get("url"))
    )


def overlap(a: set[str], b: set[str]) -> float:
    """Share of the SMALLER set that the two have in common.

    Not Jaccard: a competitor's long guide and a short page of yours can be about exactly the
    same thing, and Jaccard would punish the size difference rather than measure the subject.
    """
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def _normalised_url(url: str) -> str:
    """`/en` and `/en/` are one page. A crawl reports both, and without this the same competitor
    subject appeared three times in the findings, each inflating the counts."""
    u = str(url or "").strip().split("#", 1)[0].split("?", 1)[0]
    return u[:-1] if len(u) > 1 and u.endswith("/") else u


def _is_home_page(url: str) -> bool:
    """The site root. It is about the brand, not about a subject: measured on two real sites it
    matched five different competitor articles at exactly the floor, because a home page borrows
    a little of every topic. Retargeting it at one article's subject would be wrong."""
    u = _normalised_url(url)
    if "://" not in u:
        return u in ("", "/")
    rest = u.split("://", 1)[1]
    return "/" not in rest or rest.split("/", 1)[1] == ""


def _is_crawlable_page(page: dict[str, Any]) -> bool:
    if not isinstance(page, dict) or page.get("error"):
        return False
    status = page.get("status_code")
    if isinstance(status, int) and status != 200:
        return False
    url = str(page.get("url") or "").strip()
    if not url:
        return False
    # A legal notice is not a keyword opportunity, on either side of the comparison.
    return not _UTILITY_PAGE_RE.search(url + " " + str(page.get("title") or ""))


# Section indexes: a blog listing is not a subject, it is a shelf. Same argument as the home
# page below — measured on the first real run, where the rival's blog index matched
# `/blog` here at the floor and offered it as the page to retarget. Rewriting a listing's title
# toward one article's subject is wrong twice: it describes the shelf as if it were one book,
# and it moves a page that ranks for the section name.
_LISTING_WORDS = frozenset("""
blog blogs article articles actualite actualites news category categories categorie
tag tags archive archives ressources resources guides
""".split())

# `lang` as the crawler read it (`<html lang>`), falling back to what the URL says. Both are
# needed: Gatsby and Nuxt emit no `<html lang>` by default, and plenty of real sites carry the
# locale only in the slug.
_URL_LANGS = frozenset("fr en de es it pt nl pl".split())


def _is_listing_page(url: str) -> bool:
    """`/blog`, `/en/blog`, `/fr/actualites` — a shelf, not a subject.

    The word has to BE the page, not appear in it: `/blog/article` and `/blog/news-du-mois` are
    articles, and excluding them would quietly drop real pages from retargeting. Same reflex as
    every other string test in this project that decides an identity — match the exact segment,
    never a substring.
    """
    u = _normalised_url(url)
    path = u.split("://", 1)[1] if "://" in u else u
    path = "/" + path.split("/", 1)[1] if "/" in path else "/"
    segments = [seg for seg in path.split("/") if seg]
    if not segments or segments[-1].lower() not in _LISTING_WORDS:
        return False
    # Anything before it may only be a locale: /en/blog is a listing, /blog/news is a post.
    return all(len(seg) == 2 and seg.isalpha() for seg in segments[:-1])


def page_language(page: dict[str, Any]) -> str:
    """The page's language, or '' when it does not say.

    Abstains rather than guesses, like every other guard here: an unknown language must not
    exclude a legitimate match.
    """
    if not isinstance(page, dict):
        return ""
    declared = str(page.get("lang") or "").strip().lower()
    if declared:
        return declared.replace("_", "-").split("-", 1)[0]
    path = _normalised_url(page.get("url"))
    path = path.split("://", 1)[1] if "://" in path else path
    segments = [seg for seg in path.split("/")[1:] if seg]
    if not segments:
        return ""
    # A locale in a slug is a SUFFIX by convention (`…-2026-fr`), never a token in the middle.
    # Measured on this customer's real files: `kling-ai-image-en-video-2026-fr` is a FRENCH page
    # whose slug contains the French preposition "en" — an anywhere-match read it as English and
    # would have paired it with the wrong locale, the very thing this function exists to prevent.
    last = segments[-1].rsplit("-", 1)[-1].rsplit("_", 1)[-1].lower()
    if last in _URL_LANGS:
        return last
    # …or a path PREFIX (`/de/blog/x`), the other convention.
    first = segments[0].lower()
    return first if len(first) == 2 and first in _URL_LANGS else ""


def compare(
    own_pages: Iterable[dict[str, Any]],
    competitor_pages: Iterable[dict[str, Any]],
    *,
    threshold: float = 0.5,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """For each competitor page, the closest page on this site — and whether there is one.

    `threshold` is the overlap above which two pages are treated as the same subject. 0.5 means
    half the terms of the shorter one, which is deliberately demanding: a false "covered" sends
    a retargeting PR at a page that is about something else, and that is the one outcome worth
    avoiding.
    """
    own = [
        (p, page_terms(p), page_language(p))
        for p in own_pages or []
        if _is_crawlable_page(p)
        and not _is_home_page(p.get("url"))
        and not _is_listing_page(p.get("url"))
    ]
    findings: list[dict[str, Any]] = []
    seen: set[str] = set()

    for page in competitor_pages or []:
        if not _is_crawlable_page(page):
            continue
        terms_for_key = page_terms(page)
        # Deduplicated by SUBJECT, not by URL: this competitor states the same subject on three
        # different URLs, so a URL key still listed it three times. One subject, one finding.
        key = "|".join(sorted(terms_for_key)) or _normalised_url(page.get("url"))
        if key in seen:
            continue
        seen.add(key)
        terms = terms_for_key
        if len(terms) < 2:
            continue  # nothing said about the subject; a match here would be an accident

        # A subject is stated in a language. Measured on the first real run: a GERMAN rival page
        # matched an ENGLISH page here on {elevenlabs, voice, 2026} and offered it for
        # retargeting — which would have pointed the German subject at the wrong locale while
        # the right page sat one URL away. When both sides say what they are, they must agree;
        # when either abstains, the match stands.
        page_lang = page_language(page)
        best_page, best_score, best_terms = None, 0.0, set()
        for candidate, candidate_terms, candidate_lang in own:
            if page_lang and candidate_lang and page_lang != candidate_lang:
                continue
            score = overlap(terms, candidate_terms)
            if score > best_score:
                best_page, best_score, best_terms = candidate, score, candidate_terms

        shared = len(terms & best_terms) if best_page is not None else 0
        covered = (
            best_page is not None
            and best_score >= threshold
            and shared >= MIN_SHARED_TERMS
        )
        findings.append({
            "competitor_url": str(page.get("url") or ""),
            "competitor_title": str(page.get("title") or ""),
            "terms": sorted(terms),
            "covered": covered,
            # The page to retarget, and only when the subject really is covered: acting on a
            # weak match is how a correction turns into keyword stuffing.
            "own_url": str(best_page.get("url") or "") if covered else "",
            "own_title": str(best_page.get("title") or "") if covered else "",
            "match_score": round(best_score, 3),
            "shared_terms": sorted(terms & best_terms) if best_page is not None else [],
        })

    # Uncovered first: a subject nobody on this site answers is the more interesting finding,
    # even though it is the one the product deliberately will not act on yet.
    findings.sort(key=lambda f: (f["covered"], -f["match_score"]))
    return findings[: max(0, int(limit))]


def summarise(findings: list[dict[str, Any]]) -> dict[str, Any]:
    covered = [f for f in findings if f["covered"]]
    return {
        "total": len(findings),
        "covered": len(covered),
        "uncovered": len(findings) - len(covered),
    }
