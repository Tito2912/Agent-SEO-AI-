"""Consumption shown to a customer has the same owner, month and limits as enforcement."""
from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

import pytest
from bs4 import BeautifulSoup
from sqlalchemy import func, select

from backend import app as m, billing
from backend.models import AccountMember, UsageEvent
from test_current_plan_counts import subscription
from test_customer_activation import client, lab as lab, project, user


@pytest.fixture(autouse=True)
def local_billing(monkeypatch):
    monkeypatch.setattr(billing, "stripe_enabled", lambda: False)
    monkeypatch.setenv("PLAN_CONFIG_JSON", "")


def quota(page, metric):
    return next(row for row in page.context["quota_rows"] if row["metric"] == metric)


def test_six_real_monthly_counters_include_refunds_but_not_other_months_or_accounts(lab, monkeypatch):
    monkeypatch.setattr(billing, "_period_key", lambda dt=None: "2026-10")
    a, b = user(lab["db"]), user(lab["db"])
    subscription(lab["db"], a, "pro")
    with lab["db"].session() as db:
        for i, (metric, _label, _minimum) in enumerate(m.plan_usage.MONTHLY_METRICS, 1):
            db.add_all([
                UsageEvent(user_id=a.id, period="2026-10", metric=metric, amount=i+1),
                UsageEvent(user_id=a.id, period="2026-10", metric=metric, amount=-1),
                UsageEvent(user_id=a.id, period="2026-09", metric=metric, amount=999),
                UsageEvent(user_id=b.id, period="2026-10", metric=metric, amount=888),
            ])
        db.commit()
    page = client(a).get("/billing")
    assert page.status_code == 200 and page.context["quota_period"] == "2026-10"
    assert [row["used"] for row in page.context["quota_rows"]] == list(range(1, 7))
    with lab["db"].session() as db:
        for row in page.context["quota_rows"]:
            assert row["remaining"] == billing.remaining_quota(db, user_id=a.id, metric=row["metric"])
    assert page.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("plan", ["free", "solo", "pro", "business"])
def test_feature_access_stays_closed_below_its_minimum_plan(lab, monkeypatch, plan):
    a=user(lab["db"])
    if plan != "free": subscription(lab["db"], a, plan)
    monkeypatch.setenv("PLAN_CONFIG_JSON", json.dumps({plan:{"limits":{"ai_articles_month":91,"backlink_searches_month":92,"backlink_replies_month":93}}}))
    page=client(a).get("/billing")
    assert (quota(page,"ai_articles_month")["state"] == "finite") == (billing.plan_rank(plan)>=billing.plan_rank("pro"))
    assert (quota(page,"backlink_searches_month")["state"] == "finite") == (billing.plan_rank(plan)>=billing.plan_rank("solo"))
    assert (quota(page,"backlink_replies_month")["state"] == "finite") == (billing.plan_rank(plan)>=billing.plan_rank("solo"))


def test_zero_monthly_limit_does_not_claim_a_closed_feature_when_enforcement_is_unbounded(lab, monkeypatch):
    a=user(lab["db"]);subscription(lab["db"],a,"pro")
    monkeypatch.setenv("PLAN_CONFIG_JSON",'{"pro":{"limits":{"ai_articles_month":0,"ai_corrections_month":0}}}')
    with lab["db"].session() as db:
        billing.usage_add(db,user_id=a.id,metric="ai_articles_month",amount=8)
        assert billing.remaining_quota(db,user_id=a.id,metric="ai_articles_month") is None
    page=client(a).get("/billing")
    assert quota(page,"ai_articles_month")["state"]=="unlimited"
    assert quota(page,"ai_articles_month")["used"]==8
    assert quota(page,"ai_corrections_month")["state"]=="unlimited"
    assert m._article_gate(a)[0] is True
    public=client().get("/en/pricing")
    dom=BeautifulSoup(public.text,"html.parser")
    article_row=next(row for row in dom.select('.comparatif tr') if "AI-written articles per month" in row.get_text())
    assert [v.get_text(strip=True) for v in article_row.select('td')]==["—","—","Unlimited","12"]
    assert "AI-written articles : Unlimited" in dom.get_text()
    assert m._content_tokens("en")["articles_pro"]=="Unlimited"


def test_closed_correction_engine_is_excluded_even_with_a_positive_monthly_balance(lab, monkeypatch):
    a=user(lab["db"])
    monkeypatch.setenv("PLAN_CONFIG_JSON",'{"free":{"correction":{"max_files":0},"limits":{"ai_corrections_month":99}}}')
    page=client(a).get("/billing")
    assert quota(page,"ai_corrections_month")["state"]=="excluded"
    assert m._correction_gate(a)[0] is False
    public=BeautifulSoup(client().get("/pricing").text,"html.parser")
    correction_row=next(row for row in public.select('.comparatif tr') if "Corrections écrites par l'IA, par mois" in row.get_text())
    assert correction_row.select('td')[0].get_text(strip=True)=="—"
    assert m._content_tokens()["corrections_free"]=="—"


def test_exceeded_limit_preserves_actual_use_and_bounds_only_the_named_progress_bar(lab):
    a=user(lab["db"]);subscription(lab["db"],a,"pro")
    with lab["db"].session() as db: billing.usage_add(db,user_id=a.id,metric="ai_articles_month",amount=7)
    page=client(a).get("/billing")
    row=quota(page,"ai_articles_month")
    assert (row["used"],row["limit"],row["remaining"],row["percent"])==(7,4,0,100)
    dom=BeautifulSoup(page.text,"html.parser");li=dom.select_one('[data-quota="ai_articles_month"]')
    assert "7 / 4" in li.get_text(" ",strip=True)
    bar=li.select_one('[role="progressbar"]')
    assert bar["aria-valuenow"]==bar["aria-valuemax"]=="4"
    assert dom.find(id=bar["aria-labelledby"]).get_text(strip=True)
    assert li.select_one('a[href="#billing-plans"]')


def test_shared_site_context_does_not_replace_personal_consumption(lab):
    owner,member=user(lab["db"]),user(lab["db"])
    subscription(lab["db"],owner,"pro");site=project(lab["db"],owner,slug="shared")
    with lab["db"].session() as db:
        db.add(AccountMember(owner_user_id=owner.id,member_user_id=member.id));db.commit()
        billing.usage_add(db,user_id=owner.id,metric="ai_articles_month",amount=3)
        billing.usage_add(db,user_id=member.id,metric="assistant_messages_month",amount=2)
    page=client(member).get("/billing",params={"project":site.slug,"return_to":"/projects/shared/issues?severity=error"})
    assert page.context["billing_context"]["shared"] is True
    assert page.context["billing_context"]["return_url"]=="/projects/shared/issues?severity=error"
    assert quota(page,"ai_articles_month")["used"]==0
    assert quota(page,"assistant_messages_month")["used"]==2
    assert client(user(lab["db"])).get("/billing?project=shared").status_code==404


def test_admin_usage_is_not_misrepresented_as_zero_or_a_monthly_ceiling(lab):
    page=client(lab["admin"]).get("/billing")
    assert all(row["state"]=="admin" for row in page.context["quota_rows"])
    assert not BeautifulSoup(page.text,"html.parser").select('.plan-usage [role="progressbar"]')


@pytest.mark.parametrize("lang",["fr","en"])
def test_common_definitions_and_calendar_month_are_readable_without_buying(lab,lang):
    c=client();path="/en/pricing" if lang=="en" else "/pricing"
    page=c.get(path);dom=BeautifulSoup(page.text,"html.parser")
    assert page.status_code==200 and dom.select_one('html')["lang"]==lang
    assert "WordPress" in dom.select_one('.faq').get_text()
    assert "00:00 UTC" in dom.select_one('.faq').get_text()
    target=("/en/docs/plans-et-quotas" if lang=="en" else "/docs/plans-et-quotas")
    assert dom.select_one('#quota-guide a')["href"]==target
    guide=c.get(target)
    assert guide.status_code==200 and "00:00 UTC" in guide.text
    for a in dom.select('.pricing-jumps a'):
        assert dom.find(id=a["href"][1:]) is not None


def test_repeated_reading_creates_no_usage_or_subscription_and_anonymous_access_stays_guarded(lab):
    a=user(lab["db"]);c=client(a)
    with lab["db"].session() as db: before=db.scalar(select(func.count()).select_from(UsageEvent))
    for _ in range(2): assert c.get("/billing").status_code==200
    with lab["db"].session() as db:
        assert db.scalar(select(func.count()).select_from(UsageEvent))==before
        assert billing.subscription_for_user(db,user_id=a.id) is None
    response=client().get("/billing",follow_redirects=False)
    destination=urlsplit(response.headers["location"])
    assert response.status_code==303 and destination.path=="/auth/login"
    assert parse_qs(destination.query).get("next")==["/billing"]
