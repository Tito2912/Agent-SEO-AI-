from __future__ import annotations

import base64
from urllib.parse import unquote

import pytest

from backend import app as m, repo_index


@pytest.mark.parametrize("kind", ["title", "description"])
@pytest.mark.parametrize("failure", ["put", "format"])
def test_an_original_value_remains_reserved_when_its_selected_file_cannot_be_written(monkeypatch, kind, failure):
    old_a = "Ancien titre commun au premier groupe" if kind == "title" else "Ancienne description commune au premier groupe. " * 3
    old_b = "Ancien titre commun au second groupe" if kind == "title" else "Ancienne description commune au second groupe. " * 3
    field = "title" if kind == "title" else "meta_description"
    paths = [f"{name}.html" for name in "abcd"]
    values = dict(zip(paths, (old_a, old_b, old_a, old_b)))

    def source(value):
        tag = f"<title>{value}</title>" if kind == "title" else f'<meta name="description" content="{value}" />'
        return '<!doctype html><html><head>' + tag + '</head><body><h1>Controle</h1></body></html>'

    sources = {p: source(v) for p, v in values.items()}
    writes = []
    proposals = {p: ("Titre distinct pour la page " + p if kind == "title" else "Description distincte pour la page " + p + ". " + "Controle des lots de corrections et de leur reprise. " * 2) for p in paths}
    proposals["b.html"] = old_a

    def model(**kw):
        path = kw["file_path"]
        patched = source(proposals[path])
        return {"patched_content": patched}

    def get(path, **kw):
        file = unquote(path.split("/contents/", 1)[1])
        return {"sha": "old", "content": base64.b64encode(sources[file].encode()).decode()}

    def put(path, **kw):
        file = unquote(path.split("/contents/", 1)[1])
        if failure == "put" and file == "a.html":
            raise OSError("fixture write refused")
        writes.append((file, base64.b64decode(kw["json_body"]["content"]).decode()))
        return {"content": {"sha": "new"}}

    monkeypatch.setattr(m, "_openai_generate_file_patch", model)
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    if failure == "format":
        original_guard = m._refus_de_format
        monkeypatch.setattr(m, "_refus_de_format", lambda path, raw: "fixture source guard refused" if path == "a.html" else original_guard(path, raw))
    urls = ["https://fixture.test/" + p.removesuffix(".html") for p in paths]
    pages = [{"url": url, "status_code": 200, "content_type": "text/html", field: values[p]} for url, p in zip(urls, paths)]
    result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused",
        fix_branch="fixture-only", all_paths=paths, issue_key="duplicate_titles" if kind == "title" else "duplicate_meta_descriptions",
        issue_label="Duplicate", impacted_urls=urls, site_name="fixture.test", file_state={}, max_files=4,
        pages=pages, index=repo_index.build_repo_index(paths), targets_override=paths)
    assert "a.html" in result[1] and "b.html" in result[1]
    assert "b.html" not in [p for p, _ in writes]
    assert all(m._find_head_text_value(raw, kind)[1].strip() != old_a.strip() for _, raw in writes)


@pytest.mark.parametrize("kind", ["title", "description"])
def test_fourteen_pages_can_be_corrected_in_three_capped_batches_without_reusing_a_previous_value(monkeypatch, kind):
    old = "Ancien titre commun au groupe de quatorze pages" if kind == "title" else "Description commune a toutes les pages du groupe de validation. " * 2
    paths = [f"page-{n:02}.html" for n in range(14)]
    field = "title" if kind == "title" else "meta_description"

    def source(value):
        tag = f"<title>{value}</title>" if kind == "title" else f'<meta name="description" content="{value}" />'
        return "<!doctype html><html><head>" + tag + "</head><body><h1>Lot de controle</h1></body></html>"

    originals = {p: source(old) for p in paths}
    final = dict(originals)
    calls = []
    expected = {p: ("Titre distinct de validation pour " + p if kind == "title" else "Description distincte de validation pour " + p + ". " + "Verification des lots, des plafonds et de la reprise des corrections. ") for p in paths}

    def model(**kw):
        path = kw["file_path"]
        calls.append(path)
        value = expected[paths[0]] if path == paths[6] and calls.count(path) == 1 else expected[path]
        return {"patched_content": source(value)}

    def get(path, **kw):
        file = unquote(path.partition("/contents/")[2])
        return {"sha": "old", "content": base64.b64encode(originals[file].encode()).decode()}

    def put(path, **kw):
        file = unquote(path.partition("/contents/")[2])
        final[file] = base64.b64decode(kw["json_body"]["content"]).decode()
        return {"content": {"sha": "new"}}

    monkeypatch.setattr(m, "_openai_generate_file_patch", model)
    monkeypatch.setattr(m, "_github_api_get", get)
    monkeypatch.setattr(m, "_github_api_put", put)
    urls = ["https://fixture.test/" + p.removesuffix(".html") for p in paths]
    pages = [{"url": url, "status_code": 200, "content_type": "text/html", field: old} for url in urls]
    state, remaining, sizes = {}, paths[:], []
    for _ in range(3):
        excluded = []
        result = m._deep_patch_issue_files(owner="fixture", repo_name="fixture", branch="main", token="unused",
            fix_branch="fixture-only", all_paths=paths, issue_key="duplicate_titles" if kind == "title" else "duplicate_meta_descriptions",
            issue_label="Duplicate", impacted_urls=urls, site_name="fixture.test", file_state=state, max_files=6,
            pages=pages, index=repo_index.build_repo_index(paths), targets_override=remaining, ecartes=excluded)
        assert result[0] == remaining[:6] and result[1] == [] and excluded == remaining[6:]
        sizes.append(len(result[0]))
        remaining = excluded
    assert sizes == [6, 6, 2] and not remaining and len(calls) == 15
    values = [m._find_head_text_value(final[p], kind)[1].strip() for p in paths]
    assert values == [expected[p].strip() for p in paths] and len(set(values)) == 14
