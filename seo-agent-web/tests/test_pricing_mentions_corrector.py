"""The one thing no competitor does must be visible to someone comparing prices.

`plan_catalog()["features"]` listed Audit / Suggestions IA / Exports / Monitoring / Backlinks —
five lines every SEO tool on the market also sells. The corrector, which opens a pull request
that actually repairs the customer's code, appeared nowhere, while `ai_corrections_month`
(0 / 100 / 300 / 900) is the metric that most differentiates the four plans.

A prospect on /pricing is not logged in and has no account, so these render the real public page
anonymously — the state every prospect is in.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-pricing-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))
os.environ.setdefault("SEO_AGENT_SECRET_KEY", "test-session-secret")
os.environ.setdefault("SEO_AGENT_ENCRYPTION_KEY", "test-encryption-secret")
os.environ.setdefault("SEO_AGENT_DISABLE_WORKER", "true")
os.environ.setdefault("PUBLIC_BASE_URL", "http://testserver")
os.environ.setdefault("CRON_SECRET", "test-cron-secret")

from fastapi.testclient import TestClient  # noqa: E402

from backend import billing  # noqa: E402
from backend.app import app  # noqa: E402

PAID_PLANS = ("solo", "pro", "business")


@pytest.fixture()
def a_prospect_who_has_no_account() -> TestClient:
    return TestClient(app)


def test_every_paid_plan_names_the_corrector(a_prospect_who_has_no_account: TestClient) -> None:
    response = a_prospect_who_has_no_account.get("/pricing")
    assert response.status_code == 200

    catalog = billing.plan_catalog()
    for plan in PAID_PLANS:
        features = " · ".join(catalog[plan]["features"]).lower()
        assert "pull request" in features, (
            f"plan '{plan}' sells nothing a competitor does not: {catalog[plan]['features']}"
        )


def test_the_free_plan_advertises_a_taste_of_the_corrector(
    a_prospect_who_has_no_account: TestClient,
) -> None:
    """DECISION REVERSED 2026-09-18 by the owner. The previous rule was the opposite.

    It read: "Free has ai_corrections_month = 0. Staying silent about it reads as 'included';
    saying it is what gives the paid plans something to be bought for." The reasoning was sound
    and the conclusion was still wrong, for a reason the same catalogue states two lines above
    about PageSpeed: a taste "is what makes the upgrade worth buying". Free got 5 PageSpeed URLs
    on exactly that argument while the corrector — the one thing no competitor does — gave
    nothing at all.

    A prospect who has never seen a Noyaru pull request on their own repository cannot know what
    the paid plans are selling. Two corrections are enough to show the whole gesture (branch,
    diff, PR body) and are fifty times less than Solo.
    """
    catalog = billing.plan_catalog()
    assert catalog["free"]["limits"]["ai_corrections_month"] > 0
    features = " · ".join(catalog["free"]["features"]).lower()
    assert "pull request" in features
    assert "non incluses" not in features, (
        "the free plan still denies what it now offers: %s" % catalog["free"]["features"])


def test_the_correction_quota_appears_on_the_public_page(
    a_prospect_who_has_no_account: TestClient,
) -> None:
    """A number a prospect can compare, next to sites and pages."""
    body = a_prospect_who_has_no_account.get("/pricing").text
    catalog = billing.plan_catalog()
    for plan in PAID_PLANS:
        quota = catalog[plan]["limits"]["ai_corrections_month"]
        assert f"{quota} corrections/mois" in body, (
            f"plan '{plan}' does not show its correction quota on /pricing"
        )


def test_no_plan_advertises_an_empty_correction_allowance(
    a_prospect_who_has_no_account: TestClient,
) -> None:
    """An allowance of zero is stated in words, never advertised as a quantity.

    Anchored on the separator the template emits: a bare "0 corrections/mois" also matches inside
    "100 corrections/mois", which would make this pass for the wrong reason.
    """
    assert "· 0 corrections/mois" not in a_prospect_who_has_no_account.get("/pricing").text


def test_the_free_plan_is_on_the_public_page_WITH_its_taste_of_the_corrector(
    a_prospect_who_has_no_account: TestClient,
) -> None:
    """Free JOINS /pricing — the owner's decision of 29/09/2026 (« ajouter le plan free »).

    Until then Free was deliberately absent: the free taste worked on conversion only, discovered
    inside the app on /billing. This test used to assert that absence and said it would be the
    one to speak up the day Free joined the page. It did; the decision is now the opposite, and
    the question it asked — should Free's correction quota be shown? — is answered yes: the
    taste of the corrector is what the plan is for, so a prospect must see it.
    """
    body = a_prospect_who_has_no_account.get("/pricing").text
    free = billing.plan_catalog()["free"]
    assert free["label"] in body, "Free is not on /pricing"
    quota = free["limits"]["ai_corrections_month"]
    assert quota > 0 and f"{quota} corrections/mois" in body, (
        "Free is shown without its correction quota — the one thing it exists to let people try")
