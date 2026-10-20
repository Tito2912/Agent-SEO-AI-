"""Customer-visible milestones must match the stored authorization and publication state."""
import json

import pytest
from bs4 import BeautifulSoup

from backend import app as m
from backend.models import BacklinkOpportunity, BillingSubscription, IssueTask, User
import test_first_verified_correction as _first

journey = _first.journey


def subscribe(data, plan="pro"):
    with m.DB.session() as db:
        for user_id in (data["owner"], data["member"]):
            db.get(User, user_id).country = "FR"
        db.add(BillingSubscription(user_id=data["owner"], stripe_customer_id="cus_" + data["owner"],
                                  stripe_subscription_id="sub_" + data["owner"], stripe_price_id="price_local",
                                  plan_key=plan, status="active"))
        db.commit()


def content_task(data, *, approved=False, state="en_attente", status="in_progress", verification=True):
    note = {"sujet": "Entretenir son vélo", "pr_url": "https://github.com/atelier/site/pull/42", "pr_number": 42,
            "pages": [{"route": "/guides/entretien/", "langue": "fr"}]}
    if verification:
        note["verification"] = {"etat": state, "valide_par_client": approved, "raison": "Verdict simulé"}
    with m.DB.session() as db:
        task = IssueTask(project_id=data["project_id"], user_id=data["owner"], created_by=data["member"],
                         issue_key=m._CONTENT_PAGE_KEY, issue_label="Page rédigée", url="/guides/entretien/",
                         status=status, severity="notice", note=json.dumps(note))
        db.add(task)
        db.commit()
        return str(task.id)


@pytest.mark.parametrize("approved,state,status,expected,button", [
    (False, "en_attente", "in_progress", "Relisez le texte", "Autoriser la fusion"),
    (True, "en_attente", "in_progress", "Contenu validé · fusion en attente", None),
    (True, "echouee", "in_progress", "Les vérifications du dépôt ont échoué", "Réessayer la fusion"),
    (True, "fusionnee", "done", "Sa mise en ligne dépend de votre déploiement", None),
    (False, "abandonnee", "in_progress", "Proposition fermée", None),
])
def test_content_review_does_not_confuse_approval_merge_and_deployment(
        journey, monkeypatch, approved, state, status, expected, button):
    data = journey
    subscribe(data)
    task = content_task(data, approved=approved, state=state, status=status)
    monkeypatch.setattr(m, "_reprendre_une_verification", lambda *a: pytest.fail("GET must not merge"))
    response = data["client"].get(f'/projects/{data["slug"]}/content/valider/{task}')
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    assert expected in response.text
    soup = BeautifulSoup(response.text, "html.parser")
    forms = soup.select('form.content-approval')
    if button:
        assert len(forms) == 1
        assert forms[0].find("button").text == button
        assert forms[0].select_one('input[type="checkbox"][required]')
    else:
        assert not forms
    assert "Valider et fusionner" not in response.text
    assert "part en ligne avec ton prochain" not in response.text
    assert data["stranger"].get(f'/projects/{data["slug"]}/content/valider/{task}').status_code == 404


def test_missing_verification_never_offers_a_merge(journey):
    task = content_task(journey, verification=False)
    response = journey["client"].get(f'/projects/{journey["slug"]}/content/valider/{task}')
    assert "Le suivi de cette proposition est incomplet" in response.text
    assert 'class="content-approval"' not in response.text


def test_article_preview_is_isolated_and_only_the_editor_can_embed_it(journey, monkeypatch):
    monkeypatch.delenv("SEO_AGENT_CSP", raising=False)
    monkeypatch.setenv("SEO_AGENT_CSP_ENABLED", "true")
    subscribe(journey)
    root = f'/projects/{journey["slug"]}'
    content = journey["client"].get(root + "/content")
    assert "frame-src 'self'" in content.headers["content-security-policy"]
    soup = BeautifulSoup(content.text, "html.parser")
    assert soup.select_one("iframe#c-a-apercu").get("sandbox") == []
    assert soup.select_one("textarea#c-a-source[readonly]")
    for path in ("/", root, root + "/keywords/opportunities"):
        assert "frame-src 'none'" in journey["client"].get(path).headers["content-security-policy"]
    monkeypatch.setenv("SEO_AGENT_CSP", "default-src 'self'; frame-src 'none'")
    custom = journey["client"].get(root + "/content")
    assert custom.headers["content-security-policy"] == "default-src 'self'; frame-src 'none'"


@pytest.mark.parametrize("plan,content,outreach", [("free", False, False), ("solo", False, True),
                                                   ("pro", True, True), ("business", True, True)])
def test_project_payer_controls_access_for_shared_account_member(journey, plan, content, outreach):
    data = journey
    subscribe(data, plan)
    page = data["client"].get(f'/projects/{data["slug"]}/content')
    assert page.status_code == 200 and page.context["has_access"] is content
    page = data["client"].get(f'/projects/{data["slug"]}/backlinks/opportunities')
    assert page.status_code == 200 and page.context["has_access"] is outreach


def test_outreach_uses_server_balance_and_keeps_every_step_reachable(journey, monkeypatch):
    data = journey
    subscribe(data)
    monkeypatch.setattr(m.billing, "remaining_quota", lambda *a, metric, **kw: 17 if metric == "backlink_searches_month" else 43)
    response = data["client"].get(f'/projects/{data["slug"]}/backlinks/opportunities')
    soup = BeautifulSoup(response.text, "html.parser")
    assert "17 recherches restantes ce mois-ci" in response.text
    assert "43 brouillons restants ce mois-ci" in response.text
    assert "/200" not in response.text
    for anchor in ("outreach-search", "outreach-queue", "outreach-saved"):
        assert soup.find(id=anchor)


def test_explicit_outreach_declaration_is_scoped_and_is_not_a_verified_link(journey):
    data = journey
    subscribe(data)
    with m.DB.session() as db:
        opportunity = BacklinkOpportunity(project_id=data["project_id"], user_id=data["owner"], source="reddit",
                                         title="Une discussion", url="https://discussion.example/sujet/",
                                         reply="Un texte à relire", queue_status="approved", status="new")
        db.add(opportunity)
        db.commit()
        opportunity_id = str(opportunity.id)
    page = data["client"].get(f'/projects/{data["slug"]}/backlinks/opportunities')
    assert "Copier et ouvrir" in page.text and "J’ai envoyé la réponse" in page.text
    with m.DB.session() as db:
        assert db.get(BacklinkOpportunity, opportunity_id).posted_at is None
    path = f'/api/projects/{data["slug"]}/backlinks/queue/{opportunity_id}/mark-sent'
    assert data["client"].post(path, json={}).status_code == 403
    csrf = data["client"].cookies.get(m._CSRF_COOKIE_NAME, "")
    assert data["client"].post(path, json={}, headers={m._CSRF_HEADER_NAME: csrf}).status_code == 200
    with m.DB.session() as db:
        opportunity = db.get(BacklinkOpportunity, opportunity_id)
        assert opportunity.queue_status == "posted" and opportunity.status == "contacted"
        assert opportunity.posted_at is not None
    refreshed = data["client"].get(f'/projects/{data["slug"]}/backlinks/opportunities')
    assert "Envoi enregistré · lien à vérifier" in refreshed.text
    assert "✓ Publié" not in refreshed.text
    stranger = data["stranger"]
    stranger.get("/settings/accounts")
    assert stranger.post(path, json={}, headers={m._CSRF_HEADER_NAME: stranger.cookies.get(m._CSRF_COOKIE_NAME, "")}).status_code == 404
