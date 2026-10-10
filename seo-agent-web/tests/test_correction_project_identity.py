"""Project labels must not define the host allowed to receive a correction."""

import pytest

from backend import app as m
from tests.test_corrector_operational import harness, HTML, URL  # noqa: F401


def setup_viewport(state, monkeypatch):
    state.update(key="viewport_not_set", sources={"index.html": HTML})
    state["report"] = {
        "issues": {"viewport_not_set": {"count": 1, "examples": [URL]}},
        "pages": [{
            "url": URL, "final_url": URL, "status_code": 200,
            "content_type": "text/html", "canonical": URL,
            "meta_viewport": None, "meta_viewport_tag_count": 0,
        }],
    }
    monkeypatch.setattr(m, "_sitemap_https_page", lambda *args: True)


@pytest.mark.parametrize("mode", ["individual", "bulk"])
@pytest.mark.parametrize("label", ["static-html", "Boutique FR", "https://other.test/"])
def test_project_base_url_not_display_label_defines_correction_host(harness, monkeypatch, mode, label):
    state, run = harness
    setup_viewport(state, monkeypatch)
    state["project"].site_name = label
    state["project"].base_url = URL

    result = run(mode)

    tag = '<meta name="viewport" content="width=device-width, initial-scale=1" />\n'
    assert result["status"] == 200
    assert result["written"] == {"index.html": HTML.replace("</head>", tag + "</head>")}
    assert not result["ai_calls"]
    assert state["charges"] == [0]
    assert state["pr_bodies"]


@pytest.mark.parametrize("mode", ["individual", "bulk"])
def test_matching_label_cannot_authorize_a_foreign_project_host(harness, monkeypatch, mode):
    state, run = harness
    setup_viewport(state, monkeypatch)
    state["project"].site_name = "site.test"
    state["project"].base_url = "https://other.test/"

    result = run(mode)

    assert result["status"] != 200
    assert not result["written"]
    assert not result["ai_calls"]
    assert not any(state["charges"])
    assert not state["pr_bodies"]
