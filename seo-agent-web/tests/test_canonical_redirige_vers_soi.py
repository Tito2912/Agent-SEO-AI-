"""Un canonical qui REDIRIGE vers sa propre page : le cas le plus courant, refuse partout.

`/blog` declare `<link rel="canonical" href="/blog/">`, et `/blog/` repond 301 vers `/blog`. La
destination de la redirection est la page elle-meme : son canonical vaut encore l'ancienne
valeur, celle qu'on repare. La verification des destinations (fb64a49, 04/10/2026) n'admettait
ce cas que pour `canonical_from_https_to_http` ; `canonical_points_to_redirect` etait donc
refusee sur les dix stacks du banc (12/10/2026) — alors que c'est le defaut injecte dans chaque
fixture, resolu 9/9 le 04/09, et celui de la premiere pull request d'un client.
"""

import pytest

from backend import app as m

SITE = "https://site.test"
PAGE, SLASH = SITE + "/blog", SITE + "/blog/"
KEY = "canonical_points_to_redirect"


def row(url, **extra):
    return {"url": url, "final_url": url, "status_code": 200, "content_type": "text/html", "canonical": url, **extra}


def rows(**redirect):
    return [row(PAGE, canonical=SLASH),
            row(SLASH, **({"final_url": PAGE, "canonical": SLASH, "redirect_statuses": [301]} | redirect))]


def prepare(observed):
    pair = {"page": PAGE, "from": SLASH, "to": PAGE}
    block = {"count": 1, "examples": [PAGE], "evidence": {"kind": "url_pairs", "items": [pair]}}
    return m._prepare_issue_fix(issue_key=KEY, issues={KEY: block}, impacted=[PAGE], all_paths=["blog.html"],
                                site_name="site.test", owner="o", repo_name="r", branch="main", token="t",
                                pages=observed), pair


def test_a_canonical_redirecting_to_its_own_page_is_repaired_to_the_final_url() -> None:
    prep, pair = prepare(rows())
    assert not prep["refusal"] and prep["canonical_pairs"] == [pair]


@pytest.mark.parametrize("change", [
    # La redirection n'est pas observee : rien ne prouve que /blog/ mene a /blog.
    {"redirect_statuses": []},
    # Elle mene ailleurs que sur la page.
    {"final_url": SITE + "/autre"},
])
def test_without_an_observed_redirect_back_to_the_page_nothing_is_repaired(change) -> None:
    prep, _ = prepare(rows(**change))
    assert prep["refusal"] and "canonical_pairs" not in prep


def test_the_page_itself_must_still_declare_the_value_being_repaired() -> None:
    observed = rows()
    observed[0]["canonical"] = PAGE        # deja juste : rien a reparer, et pas de faux accord
    prep, _ = prepare(observed)
    assert prep["refusal"]
