"""Fixture diagnostics must preserve results and never collect non-fixture code."""

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from ops.gauntlet.source_capture import FixtureSourceCapture


PATH = "pages/gauntlet/mixed-css.vue"
HOST = "noyaru-stack-nuxt.netlify.app"


@pytest.fixture
def backend():
    result = {"patched_content": "useHead({title: 'New title'});\r\n"}
    return SimpleNamespace(_openai_generate_file_patch=lambda **kw: result,
                           _refus_de_format=lambda path, content: "invalid" if content == "bad" else None)


@pytest.mark.parametrize("stack,paths", [
    ("client", {PATH}), ("nuxt", set()), ("nuxt", {"pages/customer.vue"}),
    ("nuxt", {"pages/gauntlet/../private.vue"}), ("nuxt", {"pages/gauntlet/nested/a.vue"}),
    ("nuxt", {"pages/gauntlet/a.txt"}), ("nuxt", {"/pages/gauntlet/a.vue"}),
    ("hugo", {PATH}), ("nuxt", {f"pages/gauntlet/{i}.vue" for i in range(9)}),
])
def test_restricts_capture_to_small_owned_fixture_scope(backend, tmp_path, stack, paths):
    with pytest.raises(ValueError):
        FixtureSourceCapture(backend, tmp_path, stack=stack, paths=paths)
    assert list(tmp_path.iterdir()) == []


def test_results_source_bytes_and_refusals_are_preserved(backend, tmp_path):
    generate, refuse = backend._openai_generate_file_patch, backend._refus_de_format
    original = "useHead({title: 'Old title'});\r\n"
    expected = generate()
    with FixtureSourceCapture(backend, tmp_path, stack="nuxt", paths={PATH}) as capture:
        assert backend._openai_generate_file_patch(
            file_path=PATH, site_name=HOST, file_content=original, occurrences_hint="DO-NOT-LOG") is expected
        assert backend._refus_de_format(PATH, "bad") == "invalid"
        assert backend._refus_de_format("pages/customer.vue", "PRIVATE") is None
    assert backend._openai_generate_file_patch is generate
    assert backend._refus_de_format is refuse
    saved = json.loads((tmp_path / "sources.json").read_text())
    assert saved["sources"] == capture.rows
    assert saved["served_html_certified"] is False
    assert [row["phase"] for row in capture.rows] == ["original", "generated", "final"]
    assert capture.rows[-1]["refusal"] == "invalid"
    for row, source in zip(capture.rows, (original, expected["patched_content"], "bad")):
        assert (tmp_path / row["file"]).read_bytes() == source.encode()
        assert row["source_sha256"] == hashlib.sha256(source.encode()).hexdigest()
    assert all(b"DO-NOT-LOG" not in path.read_bytes() and b"PRIVATE" not in path.read_bytes()
               for path in tmp_path.iterdir())


@pytest.mark.parametrize("path,host", [(PATH, "customer.fr"), ("pages/private.vue", HOST)])
def test_nonfixture_generation_is_blocked_before_provider(backend, tmp_path, path, host):
    calls = []
    backend._openai_generate_file_patch = lambda **kw: calls.append(kw)
    with FixtureSourceCapture(backend, tmp_path, stack="nuxt", paths={PATH}):
        with pytest.raises(ValueError):
            backend._openai_generate_file_patch(file_path=path, site_name=host, file_content="PRIVATE")
    assert not calls
    assert list(tmp_path.iterdir()) == []


def test_restores_functions_when_provider_raises(backend, tmp_path):
    def fail(**kw):
        raise RuntimeError("provider failed")
    backend._openai_generate_file_patch = fail
    refuse = backend._refus_de_format
    with pytest.raises(RuntimeError):
        with FixtureSourceCapture(backend, tmp_path, stack="nuxt", paths={PATH}):
            backend._openai_generate_file_patch(file_path=PATH, site_name=HOST, file_content="original")
    assert backend._openai_generate_file_patch is fail
    assert backend._refus_de_format is refuse
    assert len(json.loads((tmp_path / "sources.json").read_text())["sources"]) == 1


@pytest.mark.parametrize("result", [None, {}, {"no_change": True}, {"patched_content": None}])
def test_no_generated_source_is_invented(backend, tmp_path, result):
    backend._openai_generate_file_patch = lambda **kw: result
    with FixtureSourceCapture(backend, tmp_path, stack="nuxt", paths={PATH}) as capture:
        assert backend._openai_generate_file_patch(file_path=PATH, site_name=HOST, file_content="original") is result
    assert [row["phase"] for row in capture.rows] == ["original"]


def test_parallel_capture_does_not_overwrite_snapshots(backend, tmp_path):
    with FixtureSourceCapture(backend, tmp_path, stack="nuxt", paths={PATH}) as capture:
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda i: backend._refus_de_format(PATH, f"source-{i}"), range(12)))
    assert len(capture.rows) == len({row["file"] for row in capture.rows}) == 12
    assert {path.read_text() for path in tmp_path.glob("*.vue")} == {f"source-{i}" for i in range(12)}


def test_rejected_targeted_and_full_file_sources_are_captured_and_restored(backend, tmp_path):
    targeted = lambda **kw: {"patched_content": "broken targeted source"}
    full = lambda **kw: {"patched_content": "valid full source"}
    backend._patch_via_edits, backend._patch_via_full_file = targeted, full

    def generate(**kw):
        backend._patch_via_edits(**kw)
        return backend._patch_via_full_file(**kw)

    backend._openai_generate_file_patch = generate
    with FixtureSourceCapture(backend, tmp_path, stack="nuxt", paths={PATH}) as capture:
        backend._openai_generate_file_patch(file_path=PATH, site_name=HOST, file_content="original")
    assert [row["phase"] for row in capture.rows] == ["original", "targeted", "full_file", "generated"]
    assert backend._patch_via_edits is targeted
    assert backend._patch_via_full_file is full
