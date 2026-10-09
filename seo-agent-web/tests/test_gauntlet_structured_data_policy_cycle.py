"""The refusal witness retains real error counts and stops its owned local servers."""

import os

import pytest

from backend import app as m
from ops.gauntlet import structured_data_policy_cycle as bench


def test_owned_http_crawler_controls_stay_present_without_a_patch(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        pytest.fail("This witness must not touch GitHub, a provider or billing")

    for name in ("_github_api_get", "_github_api_post", "_github_api_put", "_openai_generate_file_patch",
                 "_anthropic_messages_text", "_openai_chat_text", "_correction_charge"):
        monkeypatch.setattr(m, name, forbidden)
    old = os.environ.get("SEO_AGENT_NETWORKIDLE_MS")
    out = bench.cycle(m, tmp_path)
    assert out["schema_counts"] == [2, 2] and out["faq_counts"] == [4, 4]
    assert out["valid_price_negative_controls"] == 2 and len(out["bodies_sha256"]) == 8
    assert out["bodies_unchanged"] and out["local_servers_stopped"] and out["write_attempts"] == 0
    assert not out["automatic_repair_performed"] and not out["external_validator_called"]
    assert not out["production_ssrf_policy_changed"] and not out["public_https_or_universal_certification"]
    assert os.environ.get("SEO_AGENT_NETWORKIDLE_MS") == old


def test_owned_server_is_stopped_even_when_a_witness_fails():
    with pytest.raises(ValueError, match="controlled failure"):
        with bench.fixture() as state:
            raise ValueError("controlled failure")
    assert state["stopped"]
