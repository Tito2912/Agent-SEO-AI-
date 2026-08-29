"""Keyword opportunities, derived from the customer's OWN Search Console data.

The performance page already shows what a site ranks for. It does not say what to DO about it,
and that is the gap: a query sitting at position 7 with 7 000 impressions and 10 clicks is not a
ranking problem, it is a title-and-description problem — which is exactly what the corrector
repairs. Three rules, each explainable to a customer in one sentence:

  * **seen, never clicked** — page 1 already, real impressions, ZERO clicks. Needs no model and
    no baseline: it is a fact about the site, and the lever is the snippet.
  * **clicked far less than usual** — page 1, real impressions, and a CTR well under what THIS
    SITE achieves when it ranks in the top 3. When the site has no top-3 query to compare
    against, a plain floor stands in: under 1 % on page one, i.e. fewer than one visitor per
    hundred who saw the result.
  * **near page one** — positions 11-20 with real impressions. The lever is the page itself.

Two things are deliberately NOT modelled. There is no published CTR-by-position curve here:
every one differs, none can be checked against a given site, and a number a customer cannot
verify is a number they cannot act on. And the site's MEAN CTR is not used as the baseline —
measured on a real account it was 0.095 %, because the site's whole problem IS its CTR; a
degenerate baseline turns a 7 000-impression opportunity into "+1 click" and buries it. The
site's top-3 rate answers "what do we get when we rank well", which is the honest comparison,
and when there is no top-3 row the estimate is simply withheld.

Pure and network-free, so the rules can be tested without Search Console.
"""

from __future__ import annotations

from typing import Any, Iterable

# A query seen a handful of times says nothing: its position and CTR are noise. A floor on what
# is worth a customer's attention, not a statistical claim.
MIN_IMPRESSIONS = 50

PAGE_ONE_MAX = 10.0      # Google's first page, in average-position terms
NEAR_PAGE_ONE_MAX = 20.0
TOP_POSITIONS = 3.0      # already at the top: nothing to promise

# A CTR is "well under" the site's top-3 rate when it falls below this share of it.
CTR_GAP_RATIO = 0.5

# Fallback for a site with no top-3 query to compare against — measured need: an account showing
# 7 196 impressions at position 6.8 for 10 clicks was invisible to the rules, because it had
# clicks (so not "never clicked") and no baseline (so nothing to be "less than"). Its single
# biggest opportunity was the one being dropped.
# This is a statement of fact rather than a model: on page one, under 1 % means fewer than one
# visitor per hundred people who saw the result. It is a last resort — whenever the site HAS a
# top-3 rate, that comparison is used instead, because it is the customer's own data.
ABSOLUTE_LOW_CTR = 0.01

KIND_NO_CLICKS = "seen_never_clicked"
KIND_LOW_CTR = "clicked_less_than_usual"
KIND_NEAR_PAGE_ONE = "near_page_one"

# Search Console rows reach this module from two places that name the column differently.
_QUERY_FIELDS = ("query", "keyword", "keys")


def _num(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if out == out else default  # NaN guard: GSC has returned them


def _query_of(row: dict[str, Any]) -> str:
    for field in _QUERY_FIELDS:
        value = row.get(field)
        if isinstance(value, list):  # the raw API shape is {"keys": ["ma requête"]}
            value = value[0] if value else ""
        text = str(value or "").strip()
        if text:
            return text
    return ""


def top_position_ctr(rows: Iterable[dict[str, Any]]) -> float | None:
    """What this site gets when it ranks in the top 3. None when it never does.

    Computed on totals, not as a mean of per-row CTRs: averaging ratios lets a query with three
    impressions weigh as much as one with three thousand.
    """
    clicks = impressions = 0.0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        position = _num(row.get("position"))
        if 0 < position <= TOP_POSITIONS:
            clicks += _num(row.get("clicks"))
            impressions += _num(row.get("impressions"))
    return (clicks / impressions) if impressions > 0 else None


def find_opportunities(
    rows: Iterable[dict[str, Any]],
    *,
    min_impressions: int = MIN_IMPRESSIONS,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Rank the queries worth acting on, most impressions first.

    Ordering is by impressions, not by an estimated gain: impressions are a fact of the account,
    an estimate is not, and the two agree on the ordering anyway.
    """
    rows = [r for r in (rows or []) if isinstance(r, dict)]
    baseline = top_position_ctr(rows)
    out: list[dict[str, Any]] = []

    for row in rows:
        query = _query_of(row)
        impressions = _num(row.get("impressions"))
        if not query or impressions < min_impressions:
            continue
        position = _num(row.get("position"))
        if position <= 0:
            continue
        clicks = _num(row.get("clicks"))
        ctr = _num(row.get("ctr"), clicks / impressions if impressions else 0.0)

        kind = ""
        if TOP_POSITIONS < position <= PAGE_ONE_MAX:
            if clicks <= 0:
                kind = KIND_NO_CLICKS
            elif baseline is not None:
                if ctr < baseline * CTR_GAP_RATIO:
                    kind = KIND_LOW_CTR
            elif ctr < ABSOLUTE_LOW_CTR:
                kind = KIND_LOW_CTR
        elif PAGE_ONE_MAX < position <= NEAR_PAGE_ONE_MAX:
            kind = KIND_NEAR_PAGE_ONE
        if not kind:
            continue

        # Offered only when the site has a top-3 rate to compare against, and always framed as
        # "what this site already achieves elsewhere" so the customer can audit it by hand.
        gain = None
        if baseline:
            gain = max(0, int(round(impressions * max(0.0, baseline - ctr))))

        out.append({
            "query": query,
            "kind": kind,
            "clicks": int(clicks),
            "impressions": int(impressions),
            "ctr": ctr,
            "position": position,
            "reference_ctr": baseline,
            "potential_clicks": gain,
            "page": str(row.get("page") or "").strip(),
        })

    out.sort(key=lambda r: (-r["impressions"], r["position"], r["query"]))
    return out[: max(0, int(limit))]


def summarise(opportunities: list[dict[str, Any]]) -> dict[str, Any]:
    """One line a customer can read without a legend."""
    counts = {k: 0 for k in (KIND_NO_CLICKS, KIND_LOW_CTR, KIND_NEAR_PAGE_ONE)}
    impressions = 0
    gains = [o["potential_clicks"] for o in opportunities if o.get("potential_clicks") is not None]
    for o in opportunities:
        counts[o["kind"]] = counts.get(o["kind"], 0) + 1
        impressions += int(o.get("impressions") or 0)
    return {
        "total": len(opportunities),
        "by_kind": counts,
        "impressions": impressions,
        # None, not 0: "no estimate available" and "no gain expected" are different answers.
        "potential_clicks": sum(gains) if gains else None,
    }
