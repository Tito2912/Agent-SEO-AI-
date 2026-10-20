# -*- coding: utf-8 -*-
"""Un site que personne n'a pu joindre n'a pas de score : il a un probleme, et on le DIT.

13/10/2026 : le domaine d'un client, mis en attente par son bureau d'enregistrement, ne
resolvait plus. Le crawl rendait « 4 pages, 0 anomalie, sante 0 » — un score sur un site absent,
et rien pour dire pourquoi. Ahrefs faisait de meme (« No data », sante 0).

Le crawler note desormais `site_injoignable` quand AUCUNE page n'a recu de reponse HTTP, avec la
cause (DNS, TLS, connexion, delai) ; l'ecran la dit, et ne calcule pas de score.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location(
    "seo_audit_for_unreachable_tests", REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py")
seo_audit = importlib.util.module_from_spec(_SPEC)
sys.modules["seo_audit_for_unreachable_tests"] = seo_audit
_SPEC.loader.exec_module(seo_audit)


def page(status=None, error=""):
    return SimpleNamespace(url="https://x.test/", status_code=status, error=error)


@pytest.mark.parametrize("erreur, raison", [
    # Mesures reelles : Playwright puis requests, sur un domaine suspendu.
    ("PlaywrightError: Error: Page.goto: net::ERR_NAME_NOT_RESOLVED at https://x.test/", "dns"),
    ("ConnectionError: (Caused by NameResolutionError(\"Failed to resolve 'x.test' ([Errno 11001] getaddrinfo failed)\"))", "dns"),
    ("PlaywrightError: net::ERR_CERT_DATE_INVALID at https://x.test/", "tls"),
    ("PlaywrightError: net::ERR_CONNECTION_REFUSED at https://x.test/", "connexion"),
    ("PlaywrightError: net::ERR_TIMED_OUT at https://x.test/", "delai"),
    ("quelque chose d'autre", "inconnue"),
])
def test_a_crawl_where_nothing_answered_names_its_cause(erreur, raison) -> None:
    injoignable = seo_audit._site_injoignable([page(error=erreur)] * 4)
    assert injoignable["raison"] == raison and injoignable["detail"]


@pytest.mark.parametrize("pages", [
    [page(error="net::ERR_NAME_NOT_RESOLVED"), page(status=200)],   # une page a repondu
    [page(status=503)],                                            # une erreur serveur EST une reponse
    [page(status=403)],                                            # bloque par l'hote : autre cas, autre bandeau
    [],
])
def test_a_single_answer_means_the_site_exists_and_gets_judged(pages) -> None:
    assert seo_audit._site_injoignable(pages) is None


def _report(meta_extra=None):
    return {"meta": {"base_url": "https://x.test/", "pages_crawled": 4, **(meta_extra or {})},
            "pages": [{"url": "https://x.test/", "status_code": None, "error": "ERR_NAME_NOT_RESOLVED"}], "issues": {}}


def test_the_dashboard_carries_it_and_ignores_garbage() -> None:
    from backend import audit_dashboard
    dns = {"raison": "dns", "detail": "net::ERR_NAME_NOT_RESOLVED"}
    assert audit_dashboard.summarize_report(_report({"site_injoignable": dns}))["site_injoignable"] == dns
    assert audit_dashboard.summarize_report(_report({"site_injoignable": "nonsense"}))["site_injoignable"] is None
    joint = _report({"site_injoignable": None})
    joint["pages"][0]["status_code"] = 200
    assert audit_dashboard.summarize_report(joint)["site_injoignable"] is None


def test_a_report_written_before_the_crawler_said_so_is_recognised() -> None:
    # Les crawls rates de prosperfactory, le 13/10/2026, precedent la cle `site_injoignable`.
    from backend import audit_dashboard
    ancien = _report()                                   # aucune cle, aucune page avec statut
    assert audit_dashboard.site_injoignable(ancien)["raison"] == "dns"
    ancien["pages"].append({"url": "https://x.test/b", "status_code": 200})
    assert audit_dashboard.site_injoignable(ancien) is None


def _ecrire(runs, slug, ts, report):
    import json
    d = runs / slug / ts / "audit"
    d.mkdir(parents=True)
    (d / "report.json").write_text(json.dumps(report), encoding="utf-8")
    (runs / slug / ts / "run.json").write_text(json.dumps({"site_name": "x.test", "base_url": "https://x.test/"}), encoding="utf-8")


def _avec_anomalies(n):
    return {"meta": {"base_url": "https://x.test/", "pages_crawled": 1},
            "pages": [{"url": "https://x.test/", "status_code": 200, "content_type": "text/html"}],
            "issues": {"title_too_long_indexable": {"count": n, "examples": ["https://x.test/"]}}}


def test_the_default_comparison_skips_a_crawl_that_saw_nothing(tmp_path) -> None:
    # « +17 » sur chaque ligne : le crawl precedent etait celui du domaine suspendu.
    from backend import audit_dashboard
    _ecrire(tmp_path, "s", "20261013-100000", _avec_anomalies(17))
    _ecrire(tmp_path, "s", "20261013-170000", _report())
    _ecrire(tmp_path, "s", "20261013-180000", _avec_anomalies(17))
    vue = audit_dashboard.project_overview(tmp_path, "s", timestamp=None, compare_to=None)
    assert vue["compare"]["timestamp"] == "20261013-100000"
    ligne = next(r for r in vue["current"]["summary"]["issues"] if r["key"].startswith("title_too_long"))
    assert ligne["change"] == 0
    # Un choix EXPLICITE reste respecte.
    choisi = audit_dashboard.project_overview(tmp_path, "s", timestamp=None, compare_to="20261013-170000")
    assert choisi["compare"]["timestamp"] == "20261013-170000"


def test_the_crawler_compares_with_the_last_crawl_that_saw_the_site(tmp_path) -> None:
    for ts, report in (("20261013-100000", _avec_anomalies(1)), ("20261013-170000", _report())):
        _ecrire(tmp_path, "s", ts, report)
    (tmp_path / "s" / "20261013-180000" / "audit").mkdir(parents=True)
    prev_ts, _ = seo_audit._find_previous_report(str(tmp_path / "s" / "20261013-180000" / "audit"))
    assert prev_ts == "20261013-100000"


def _fragment():
    import jinja2
    source = (REPO_ROOT / "seo-agent-web" / "templates" / "project_overview.html").read_text(encoding="utf-8")
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(REPO_ROOT / "seo-agent-web" / "templates")))
    return env, source


def test_the_overview_says_why_and_shows_no_score() -> None:
    env, source = _fragment()
    banner = env.from_string(source[source.index("{% set blocked ="):source.index("<h1>{{ project.site_name }}</h1>")])
    crawled = {"current": {"timestamp": "20261013-154552"}}
    dns = banner.render(project=crawled, sum={"site_injoignable": {"raison": "dns", "detail": "ERR_NAME_NOT_RESOLVED"}})
    assert "Site injoignable" in dns and "bureau" in dns and "ERR_NAME_NOT_RESOLVED" in dns
    assert "Site injoignable" not in banner.render(project=crawled, sum={})

    score = env.from_string(source[source.index("{% if project.current %}"):source.index("{# ══")] + "[{{ sc }}]")
    assert "[none]" in score.render(project={"current": {"summary": {"health_score": 0,
                                    "site_injoignable": {"raison": "dns"}}}}).lower()
    assert "[87]" in score.render(project={"current": {"summary": {"health_score": 87}}})


def test_the_projects_list_shows_a_badge_not_a_percentage() -> None:
    import jinja2
    source = (REPO_ROOT / "seo-agent-web" / "templates" / "projects.html").read_text(encoding="utf-8")
    start = source.index("{% if p.timestamp and p.site_injoignable %}")
    end = source.index("{% else %}", source.index("{% elif p.timestamp %}"))
    env = jinja2.Environment()
    from backend import i18n
    i18n.add_template_globals(env)  # le fragment est traduisible (tr)
    tpl = env.from_string(source[start:end] + "{% endif %}")
    assert "Site injoignable" in tpl.render(p={"timestamp": "x", "site_injoignable": {"raison": "dns"}, "health_score": 0})
    rendu = tpl.render(p={"timestamp": "x", "site_injoignable": None, "health_score": 87})
    assert "87%" in rendu and "Site injoignable" not in rendu
