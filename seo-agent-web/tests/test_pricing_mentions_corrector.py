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


def test_the_free_plan_says_the_corrector_is_not_included(
    a_prospect_who_has_no_account: TestClient,
) -> None:
    # Free has ai_corrections_month = 0. Staying silent about it reads as "included"; saying it
    # is what gives the paid plans something to be bought for.
    catalog = billing.plan_catalog()
    assert catalog["free"]["limits"]["ai_corrections_month"] == 0
    features = " · ".join(catalog["free"]["features"]).lower()
    assert "pull request" in features and "non incluses" in features


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


def test_the_free_plan_shows_no_correction_quota(a_prospect_who_has_no_account: TestClient) -> None:
    # Zero must not render as "0 corrections/mois" — an empty allowance is stated in the feature
    # list, not advertised as a quantity.
    body = a_prospect_who_has_no_account.get("/pricing").text
    # Anchored on the separator the template emits: a bare "0 corrections/mois" also matches
    # inside "100 corrections/mois", which would make this pass for the wrong reason.
    assert "· 0 corrections/mois" not in body
