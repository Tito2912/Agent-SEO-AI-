"""A page proposal edits that page's source, or stops without charging or writing."""
import base64

import pytest
from sqlalchemy import select

from backend import app as m, correction_journal
from backend.models import CorrectionOperation
from test_corrector_customer_workflow import OLD, customer as customer


FIND_SOURCE = m._github_find_seo_files
TARGET = "https://fixture.test/blog/guide-fr"
SOURCE = "content/blog/guide-fr.mdx"
TEXT = '---\ntitle: Guide\ndescription: "Une description de la page"\n---\n# Guide\n'


def repository(monkeypatch, paths, *, truncated=False, unreadable=False):
    reads = []

    def get(path, **kwargs):
        reads.append((path, kwargs))
        if "/git/trees/" in path:
            return {"tree": [{"type": "blob", "path": p} for p in paths], "truncated": truncated}
        if unreadable:
            raise RuntimeError("unavailable")
        return {"sha": "source-blob", "content": base64.b64encode(TEXT.encode()).decode()}

    def unexpected(*args, **kwargs):
        pytest.fail("Page source selection must not call the model or write to GitHub")

    monkeypatch.setattr(m, "_github_api_get", get)
    for name in ("_ai_pick_repo_files", "_github_api_post", "_github_api_put"):
        monkeypatch.setattr(m, name, unexpected)
    return reads


@pytest.mark.parametrize("url,paths,expected", [
    (TARGET, ["app/layout.tsx", "app/[...slug]/page.tsx", SOURCE], SOURCE),
    (TARGET, ["index.html", "blog/guide-fr.html", "blog/other.html"], "blog/guide-fr.html"),
    (TARGET, ["app/layout.tsx", "app/blog/guide-fr/page.tsx"], "app/blog/guide-fr/page.tsx"),
    ("https://fixture.test/", ["app/layout.tsx", "app/page.tsx"], "app/page.tsx"),
    ("https://fixture.test/blog/guide-fr/?view=1#summary",
     ["app/layout.tsx", "app/[...slug]/page.tsx", SOURCE], SOURCE),
])
def test_target_page_source_replaces_generic_seo_candidates(monkeypatch, url, paths, expected):
    reads = repository(monkeypatch, paths)
    found = FIND_SOURCE("fixture", "site", "main", "local-only", "meta_description_too_long_indexable", url)
    assert [f["path"] for f in found] == [expected]
    assert found[0]["content"] == TEXT
    assert len(reads) == 2 and reads[-1][1]["params"] == {"ref": "main"}


@pytest.mark.parametrize("paths,truncated", [
    (["app/layout.tsx", "app/[slug]/page.tsx"], False),
    (["index.html", "unrelated.html"], False),
    (["app/layout.tsx", "app/[...slug]/page.tsx", SOURCE, "public/blog/guide-fr.html"], False),
    (["app/layout.tsx", "app/[...slug]/page.tsx", SOURCE], True),
])
def test_unknown_or_ambiguous_source_never_falls_back_to_shared_or_unrelated_file(monkeypatch, paths, truncated):
    reads = repository(monkeypatch, paths, truncated=truncated)
    with pytest.raises(m._GithubPreviewSourceUnavailable, match="certitude"):
        FIND_SOURCE("fixture", "site", "main", "local-only", "missing_title", TARGET)
    assert len(reads) == 1  # No content or model request after uncertain targeting.


def test_an_unreadable_page_source_does_not_switch_to_a_layout(monkeypatch):
    reads = repository(monkeypatch, ["app/layout.tsx", "app/[...slug]/page.tsx", SOURCE], unreadable=True)
    with pytest.raises(RuntimeError, match="Impossible de lire"):
        FIND_SOURCE("fixture", "site", "main", "local-only", "missing_meta_description", TARGET)
    assert len(reads) == 2 and "/layout.tsx" not in reads[-1][0]


def test_shared_infrastructure_families_keep_their_existing_candidates(monkeypatch):
    repository(monkeypatch, ["app/layout.tsx", "app/page.tsx"])
    found = FIND_SOURCE("fixture", "site", "main", "local-only", "viewport_not_set", TARGET)
    assert found[0]["path"] == "app/layout.tsx"


@pytest.mark.parametrize("key", ["missing_title", "meta_description_too_long_indexable"])
def test_real_preview_endpoint_passes_the_chosen_url_to_source_selection(customer, monkeypatch, key):
    state, _, used, _, slug = customer
    repository(monkeypatch, ["app/layout.tsx", "app/[...slug]/page.tsx", SOURCE])
    monkeypatch.setattr(m, "_github_find_seo_files", FIND_SOURCE)
    inputs = []

    def model(**kwargs):
        inputs.append(kwargs)
        return {"patched_content": TEXT.replace("description de la page", "nouvelle description de la page")}

    monkeypatch.setattr(m, "_openai_generate_file_patch", model)
    response = state["client"].post(f"/api/projects/{slug}/issues/{key}/github-fix",
        json={"url": TARGET}, headers={m._CSRF_HEADER_NAME: state["client"].cookies.get(m._CSRF_COOKIE_NAME)})
    assert response.status_code == 200, response.text
    assert response.json()["file"] == SOURCE
    assert len(inputs) == 1 and inputs[0]["file_path"] == SOURCE and inputs[0]["url"] == TARGET
    assert inputs[0]["issue_key"] == key
    assert used() == 1 and not state["writes"] and not state["posts"]


def test_unresolved_source_is_actionable_without_model_or_quota(customer, monkeypatch):
    state, send, used, payer, _ = customer
    repository(monkeypatch, ["app/layout.tsx", "app/[slug]/page.tsx"])
    monkeypatch.setattr(m, "_github_find_seo_files", FIND_SOURCE)
    response = send(url=TARGET)
    assert response.status_code == 422, response.text
    assert response.json()["preview_unavailable"] and response.json()["source_unresolved"]
    assert used() == state["models"] == 0 and not state["writes"] and not state["posts"]
    assert correction_journal.pending(m.DB, payer=payer) is None


@pytest.mark.parametrize("patch", [{"no_change": True}, {"patched_content": OLD}])
def test_no_change_cannot_create_a_paid_empty_preview_or_leave_a_pending_operation(customer, monkeypatch, patch):
    state, send, used, payer, slug = customer
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kwargs: patch)
    response = send()
    assert response.status_code == 422, response.text
    data = response.json()
    assert data["preview_unavailable"] and data["no_change"] and "reste à vérifier" in data["error"]
    assert used() == 0 and not state["writes"] and not state["posts"]
    assert correction_journal.pending(m.DB, payer=payer) is None
    with m.DB.session() as db:
        row = db.scalar(select(CorrectionOperation).where(CorrectionOperation.payer_id == payer))
        assert row.state == "abandoned" and not row.charged and row.request_key is None
        assert not row.data.get("preview")
    # The problem is not silently recorded as fixed in the customer's follow-up.
    tasks = state["client"].get(f"/api/projects/{slug}/tasks").json()
    assert tasks == []


def test_a_paid_existing_preview_keeps_its_identity_without_running_the_new_resolver(customer, monkeypatch):
    state, send, used, _, _ = customer
    first = send()

    def unexpected(*args, **kwargs):
        pytest.fail("A saved preview must be recovered without another source/model selection")

    monkeypatch.setattr(m, "_github_find_seo_files", unexpected)
    monkeypatch.setattr(m, "_openai_generate_file_patch", unexpected)
    resumed = send()
    assert first.status_code == resumed.status_code == 200
    assert resumed.json()["cached"] and resumed.json()["patched_content"] == first.json()["patched_content"]
    assert state["models"] == used() == 1 and not state["writes"] and not state["posts"]


def test_no_change_response_is_translated_for_an_english_customer(customer, monkeypatch):
    state, _, used, _, slug = customer
    monkeypatch.setattr(m, "_openai_generate_file_patch", lambda **kwargs: {"no_change": True})
    response = state["client"].post(f"/api/projects/{slug}/issues/missing_title/github-fix?lang=en",
        json={"url": TARGET}, headers={m._CSRF_HEADER_NAME: state["client"].cookies.get(m._CSRF_COOKIE_NAME)})
    assert response.status_code == 422
    assert "No change was proposed" in response.json()["error"]
    assert "No AI correction has been deducted" in response.json()["error"] and used() == 0
