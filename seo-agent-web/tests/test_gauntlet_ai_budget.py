from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.gauntlet.ai_budget import ClaudeBudget
from ops.gauntlet import live_cycle as bench


@pytest.mark.parametrize("limit", [-1, 101])
def test_invalid_limit_is_refused(limit):
    with pytest.raises(ValueError):
        ClaudeBudget(limit)


def test_zero_budget_makes_no_external_call():
    backend = SimpleNamespace(_anthropic_messages_text=lambda **kw: pytest.fail("external call"))
    budget = ClaudeBudget(0)
    budget.install(backend)
    with pytest.raises(RuntimeError, match="exhausted"):
        backend._anthropic_messages_text()
    assert budget.summary() == {"limit": 0, "attempted": 0, "denied": 1, "exhausted": True}


def test_failed_requests_still_consume_the_budget():
    def fail(**kw):
        raise ConnectionError("provider response lost")

    backend = SimpleNamespace(_anthropic_messages_text=fail)
    budget = ClaudeBudget(1)
    budget.install(backend)
    with pytest.raises(ConnectionError):
        backend._anthropic_messages_text()
    with pytest.raises(RuntimeError, match="exhausted"):
        backend._anthropic_messages_text()
    assert budget.summary()["attempted"] == 1


def test_parallel_file_preparation_cannot_exceed_the_limit():
    backend = SimpleNamespace(_anthropic_messages_text=lambda **kw: kw["user_msg"])
    budget = ClaudeBudget(3)
    budget.install(backend)

    def call(index):
        try:
            return backend._anthropic_messages_text(user_msg=str(index))
        except RuntimeError:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(call, range(20)))
    assert sum(value is not None for value in results) == 3
    assert budget.summary() == {"limit": 3, "attempted": 3, "denied": 17, "exhausted": True}


def test_openai_fallback_is_never_called():
    backend = SimpleNamespace(_anthropic_messages_text=lambda **kw: "Claude",
                              _openai_chat_text=lambda **kw: pytest.fail("fallback call"))
    budget = ClaudeBudget(1)
    budget.install(backend)
    with pytest.raises(RuntimeError, match="Claude only"):
        backend._openai_chat_text()
    assert backend._anthropic_messages_text() == "Claude"
    assert budget.summary()["attempted"] == 1


def test_original_content_probe_is_restricted_to_mechanical_families():
    source = Path(__file__).resolve().parents[1] / "ops/gauntlet/run.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    conditions = [node.test for node in ast.walk(tree) if isinstance(node, ast.If)
                  and any(isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
                          and call.func.id == "_mordait_a_l_origine"
                          for call in ast.walk(node.test))]
    assert len(conditions) == 1
    condition = conditions[0]
    assert isinstance(condition, ast.BoolOp) and isinstance(condition.op, ast.And)
    assert any(isinstance(value, ast.Name) and value.id == "mecanique"
               for value in condition.values[:-1])


@pytest.mark.parametrize("families,expected", [("meta_description_too_long", "7"), ("", "0")])
def test_subprocess_gets_a_hard_budget_and_free_pass_gets_zero(tmp_path, monkeypatch, families, expected):
    bench.save(tmp_path / "gauntlet_run.json", {"branch": "gauntlet-hugo-test"})
    calls = []
    monkeypatch.setattr(bench.subprocess, "run", lambda *args, **kwargs: calls.append(kwargs))
    bench.repair("hugo", tmp_path, families=families, max_ai_calls=7)
    assert calls[0]["env"]["GAUNTLET_MAX_AI_CALLS"] == expected
    assert calls[0]["check"] is True
    assert "shell" not in calls[0]
