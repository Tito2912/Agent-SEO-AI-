"""Reading a Stripe response — the single point every billing feature depends on.

A test purchase completed and the plan stayed Free. Three rounds of investigation blamed the
webhook, then an async race, then a missing subscription, because every diagnostic said Stripe
held nothing:

    client: None None cree= None
    sessions du COMPTE: 0
    abonnements du COMPTE: 0

Stripe held everything. `_stripe_to_dict` tried `to_dict_recursive()` and swallowed the
failure — but stripe-python 8 removed that method and StripeObject stopped subclassing dict.
On stripe 15 the helper returned {} for EVERY response, so checkout reconciliation found no
subscription, webhooks decoded to nothing, and invoices came back empty. From the code's point
of view Stripe simply answered with silence, and the silence was indistinguishable from
"this customer has never paid".

These tests run against the INSTALLED SDK on purpose. A version bump that changes the object
API again fails here rather than in production, quietly, months later.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest
from stripe import StripeObject

WEB_ROOT = Path(__file__).resolve().parents[1]
if str(WEB_ROOT) not in sys.path:
    sys.path.insert(0, str(WEB_ROOT))

TEST_ROOT = Path(tempfile.mkdtemp(prefix="seo-agent-stripe-"))
os.environ.setdefault("SEO_AGENT_DATA_DIR", str(TEST_ROOT / "data"))
os.environ.setdefault("SEO_AGENT_RUNS_DIR", str(TEST_ROOT / "runs"))

from backend import billing  # noqa: E402


def _stripe_object(payload: dict) -> StripeObject:
    """A response shaped the way the real SDK builds one."""
    return StripeObject.construct_from(payload, "sk_test_x")


def test_a_real_sdk_object_is_readable() -> None:
    # The assertion that would have caught this on the day stripe was upgraded.
    obj = _stripe_object({"id": "cus_x", "email": "client@exemple.fr"})
    assert billing._stripe_to_dict(obj) == {"id": "cus_x", "email": "client@exemple.fr"}


def test_nested_data_survives_the_conversion() -> None:
    # plan_key comes from items.data[0].price.id — a shallow conversion silently bills
    # everyone as free.
    obj = _stripe_object(
        {"id": "sub_x", "status": "active", "items": {"data": [{"price": {"id": "price_pro"}}]}}
    )
    converted = billing._stripe_to_dict(obj)
    assert converted["items"]["data"][0]["price"]["id"] == "price_pro"


def test_a_checkout_session_exposes_what_reconciliation_needs() -> None:
    obj = _stripe_object({
        "id": "cs_test_x",
        "customer": "cus_x",
        "subscription": "sub_x",
        "metadata": {"user_id": "u1", "plan_key": "pro"},
    })
    session = billing._stripe_to_dict(obj)
    assert billing._stripe_obj_id(session.get("subscription")) == "sub_x"
    assert session["metadata"]["user_id"] == "u1"


def test_a_list_response_exposes_its_rows() -> None:
    obj = _stripe_object({"object": "list", "data": [{"id": "sub_1", "status": "active"}]})
    assert billing._stripe_to_dict(obj)["data"][0]["id"] == "sub_1"


@pytest.mark.parametrize("value,expected", [({"a": 1}, {"a": 1}), (None, {}), ({}, {}), ("", {})])
def test_plain_values_pass_through(value, expected) -> None:
    assert billing._stripe_to_dict(value) == expected


def test_an_unreadable_object_is_reported_not_hidden(caplog: pytest.LogCaptureFixture) -> None:
    """The behaviour change that matters as much as the fix.

    Returning {} silently is what let a whole SDK major go by unnoticed: an empty response is
    indistinguishable from a customer who never paid.
    """
    class _Opaque:
        def __bool__(self) -> bool:
            return True

    with caplog.at_level("ERROR"):
        assert billing._stripe_to_dict(_Opaque()) == {}
    assert any("cannot read" in r.message for r in caplog.records), (
        "an unreadable Stripe response was swallowed without a word"
    )


def test_a_conversion_that_raises_is_logged_and_falls_through(caplog: pytest.LogCaptureFixture) -> None:
    class _Broken:
        def to_dict(self):
            raise RuntimeError("SDK changed again")

    with caplog.at_level("WARNING"):
        assert billing._stripe_to_dict(_Broken()) == {}
    assert any("to_dict" in r.message for r in caplog.records)


def test_an_older_sdk_shape_is_still_accepted() -> None:
    # Pinning stripe back to a 7.x line must not break this again in the other direction.
    class _Legacy:
        def to_dict_recursive(self):
            return {"id": "sub_legacy"}

    assert billing._stripe_to_dict(_Legacy()) == {"id": "sub_legacy"}


# --- storing the payload ------------------------------------------------------------------

import json  # noqa: E402
from decimal import Decimal  # noqa: E402


def test_a_decimal_from_the_sdk_does_not_break_the_write() -> None:
    """The failure that turned a working reconciliation into a 500.

    stripe_data is a JSON column holding the raw response. The SDK returns Decimal for fields
    like unit_amount_decimal, SQLAlchemy could not flush it, and a failed flush POISONS the
    session: every later query in the same request raised PendingRollbackError, so /billing
    answered 500 the moment a subscription was finally readable.
    """
    payload = {
        "id": "sub_x",
        "items": {"data": [{"price": {"id": "price_pro", "unit_amount_decimal": Decimal("4900")}}]},
    }
    safe = billing._json_safe(payload)
    json.dumps(safe)  # the operation that used to raise
    assert safe["items"]["data"][0]["price"]["unit_amount_decimal"] == 4900.0
    assert safe["items"]["data"][0]["price"]["id"] == "price_pro"


def test_an_unknown_type_is_stringified_rather_than_aborting_the_write() -> None:
    # stripe_data is a diagnostic record. Losing a field's exact typing is a much smaller
    # problem than losing the subscription it accompanies.
    class _Exotic:
        def __str__(self) -> str:
            return "exotique"

    safe = billing._json_safe({"weird": _Exotic(), "keep": 1})
    json.dumps(safe)
    assert safe == {"weird": "exotique", "keep": 1}


@pytest.mark.parametrize(
    "payload",
    [
        {"a": [Decimal("1"), {"b": Decimal("2")}]},
        {"nested": {"deep": {"deeper": [Decimal("0.01")]}}},
        {"none": None, "bool": True, "int": 1, "float": 1.5, "str": "x"},
        {},
    ],
)
def test_every_shape_survives_serialisation(payload) -> None:
    json.dumps(billing._json_safe(payload))


def test_keys_are_strings_after_sanitising() -> None:
    # A non-string key is the other way a JSON dump fails.
    safe = billing._json_safe({1: "a", "2": "b"})
    json.dumps(safe)
    assert set(safe) == {"1", "2"}
