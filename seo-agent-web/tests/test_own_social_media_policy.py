"""Owner policy edits never charge, erase attempts or widen an old Telegram mandate."""

import os
from pathlib import Path
import subprocess
import sys
from datetime import timedelta

import pytest
from sqlalchemy import select, func, inspect
from backend import own_social_media as m, app
from backend.models import AuditLog, OwnSocialMediaPolicy
from backend.db import Database
from test_customer_activation import lab as lab, client, user
from test_telegram_approvals import bot as bot
from test_own_social_media import media as media, prepare, approve, read, spent


def change(x, value):
    with x["db"].session() as db:
        token = m.policy(db, x["owner"].id)["token"]
    m.set_policy(x["owner"].id, value, token)


def test_default_is_ten_without_inventing_a_setting_or_work(bot):
    with bot["db"].session() as db:
        assert m.policy(db, bot["owner"].id) == {
            "limit": 10,
            "token": "",
            "max": 100,
            "preparations": 20,
        }
        assert db.scalar(select(func.count()).select_from(OwnSocialMediaPolicy)) == 0
        assert db.scalar(select(func.count()).select_from(m.Media)) == 0


def test_raise_keeps_attempts_and_audits_only_the_setting(media):
    x = media
    ids = [spent(x, m.s.clock(), state=state) for state in ("cancelled", "uncertain")]
    change(x, "10")
    with x["db"].session() as db:
        assert m.audio_budget(db, x["owner"].id) == {
            "limit": 10,
            "remaining": 8,
            "retry_at": None,
        }
        assert db.get(OwnSocialMediaPolicy, x["owner"].id).daily_calls == 10
        audit = db.scalars(
            select(AuditLog)
            .where(AuditLog.action == "own_social_media.limit_changed")
            .order_by(AuditLog.created_at)
        ).all()
        assert audit[-1].meta == {"before": 2, "after": 10}
    assert [read(x, i).state for i in ids] == ["cancelled", "uncertain"]
    assert all(read(x, i).provider_calls == 1 for i in ids)
    assert not x["voice_calls"]


@pytest.mark.parametrize(
    "value",
    ["0", "101", "-1", "2.5", "1e2", "", "True", "10\n"],
    ids=[
        "zero",
        "above",
        "negative",
        "decimal",
        "exponent",
        "empty",
        "bool",
        "newline",
    ],
)
def test_invalid_limit_cannot_change_the_policy(media, value):
    with pytest.raises(ValueError, match="^limit_invalid$"):
        change(media, value)
    with media["db"].session() as db:
        assert m.policy(db, media["owner"].id)["limit"] == 2


def test_stale_tabs_and_other_users_cannot_overwrite_policy(media):
    x = media
    with x["db"].session() as db:
        stale = m.policy(db, x["owner"].id)["token"]
    change(x, "100")
    with pytest.raises(ValueError, match="^limit_changed$"):
        m.set_policy(x["owner"].id, "3", stale)
    for admin in (False, True):
        other = user(x["db"], admin=admin)
        with pytest.raises(ValueError, match="^owner_required$"):
            m.set_policy(other.id, "10", "")
    with x["db"].session() as db:
        assert m.policy(db, x["owner"].id)["limit"] == 100
        assert db.scalar(select(func.count()).select_from(OwnSocialMediaPolicy)) == 1


@pytest.mark.parametrize("legacy", [False, True], ids=["snapshot", "legacy"])
def test_increase_never_widens_old_approval_and_new_request_uses_new_limit(
    media, legacy
):
    x = media
    ident = prepare(x)
    if legacy:
        # Reproduce a pre-policy packet. Its original version remains authoritative.
        with x["db"].session() as db:
            row = db.get(m.Media, ident)
            row.packet = {k: v for k, v in row.packet.items() if k != "daily_limit"}
            row.input_hash = m.s.digest(row.packet)
            db.commit()
    approve(x, ident)
    original = m.version(read(x, ident), "social_render")
    for _ in range(2):
        spent(x, m.s.clock())
    change(x, "10")
    assert m.version(read(x, ident), "social_render") == original
    m.run(ident)
    assert read(x, ident).error_code == "budget" and not x["voice_calls"]
    assert read(x, ident).provider_started_at is None
    fresh = prepare(x)
    assert fresh != ident and read(x, fresh).packet["daily_limit"] == 10
    with x["db"].session() as db:
        card = m.approval_document(db, x["owner"].id, fresh, "social_render", "en")
        assert "Maximum 10 requests" in card["effect"]
    m.run(fresh)  # A setting is never an approval.
    assert not x["voice_calls"]
    approve(x, fresh)
    m.run(fresh)
    assert len(x["voice_calls"]) == 1 and read(x, fresh).state == "review"


def test_decrease_rechecks_latest_limit_before_voice_and_reopening(media):
    x = media
    change(x, "10")
    ident = prepare(x)
    approve(x, ident)
    now = m.s.clock()
    spent(x, now - timedelta(hours=20))
    spent(x, now - timedelta(hours=2))
    change(x, "1")
    with x["db"].session() as db:
        assert m.audio_budget(db, x["owner"].id) == {
            "limit": 1,
            "remaining": 0,
            "retry_at": now + timedelta(hours=22),
        }
    m.run(ident)
    assert read(x, ident).error_code == "budget" and not x["voice_calls"]


@pytest.mark.parametrize("lang", ["fr", "en"], ids=["fr", "en"])
def test_route_requires_owner_csrf_and_preserves_context_without_notifications(
    media, lang
):
    x = media
    c = client(x["owner"])
    c.get(m.PATH)
    with x["db"].session() as db:
        token = m.policy(db, x["owner"].id)["token"]
    form = {
        "daily_calls": "10",
        "policy_token": token,
        "goal": "product",
        "audience": "shop",
    }
    assert c.post(m.PATH + "/limits", data=form).status_code == 403
    csrf = c.cookies.get(app._CSRF_COOKIE_NAME)
    for who in (user(x["db"]), user(x["db"], admin=True)):
        unauthorized = client(who)
        unauthorized.get("/projects")
        assert (
            unauthorized.post(
                m.PATH + "/limits",
                data={**form, "_csrf": unauthorized.cookies.get(app._CSRF_COOKIE_NAME)},
            ).status_code
            == 403
        )
    count = len(x["calls"])
    response = c.post(
        m.PATH + "/limits?lang=" + lang,
        data={**form, "_csrf": csrf},
        follow_redirects=False,
    )
    assert (
        response.status_code == 303
        and "msg=limit_saved" in response.headers["location"]
    )
    assert (
        "lang=" + lang in response.headers["location"]
        and "goal=product" in response.headers["location"]
        and "audience=shop" in response.headers["location"]
    )
    assert response.headers["cache-control"] == "no-store"
    assert len(x["calls"]) == count and not x["voice_calls"]
    page = c.get(m.PATH + "?lang=" + lang + "&msg=limit_saved")
    assert (
        page.status_code == 200 and 'value="10"' in page.text and "10 / 10" in page.text
    )


@pytest.mark.parametrize("precreated", [False, True], ids=["migrations", "create-all"])
def test_migration_leaves_attempts_and_mandates_unchanged(
    tmp_path, monkeypatch, precreated
):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + str(tmp_path / "policy.db"))
    database = Database(data_dir=tmp_path / "data")
    web = Path(__file__).resolve().parents[1]

    def migrate(direction, revision):
        r = subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "-c",
                str(web / "alembic.ini"),
                direction,
                revision,
            ],
            cwd=web,
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert r.returncode == 0, r.stderr

    if precreated:
        database.create_tables()
    migrate("upgrade", "20261114_0031")
    owner = user(database, admin=True)
    ident = spent({"db": database, "owner": owner}, m.s.clock(), state="uncertain")
    with database.session() as db:
        before = dict(db.get(m.Media, ident).__dict__)
        before.pop("_sa_instance_state")
    migrate("upgrade", "head")
    migrate("upgrade", "head")
    database.engine.dispose()
    with database.session() as db:
        assert db.scalar(select(func.count()).select_from(OwnSocialMediaPolicy)) == 0
        after = dict(db.get(m.Media, ident).__dict__)
        after.pop("_sa_instance_state")
        assert after == before
        db.add(OwnSocialMediaPolicy(owner_id=owner.id, daily_calls=100, token="a" * 32))
        db.commit()
        assert db.get(OwnSocialMediaPolicy, owner.id).daily_calls == 100
    migrate("downgrade", "20261114_0031")
    database.engine.dispose()
    assert not inspect(database.engine).has_table("own_social_media_policy")
    with database.session() as db:
        assert db.get(m.Media, ident).provider_calls == 1
        assert db.get(m.Media, ident).state == "uncertain"
    migrate("upgrade", "head")
    database.engine.dispose()


def test_capped_old_proposals_cannot_hide_new_notification(media):
    x = media
    original = read(x, prepare(x))
    with x["db"].session() as db:
        for index in range(9):
            db.add(
                m.Media(
                    owner_id=original.owner_id,
                    authorization_key=f"{index:032x}",
                    input_hash=original.input_hash,
                    packet=original.packet,
                    connection_hash=original.connection_hash,
                    binding=original.binding,
                    state="proposal",
                    result={},
                    expires_at=original.expires_at,
                )
            )
        db.commit()
    for _ in range(2):
        spent(x, m.s.clock())
    change(x, "10")
    fresh = prepare(x)
    with x["db"].session() as db:
        assert list(m.candidates(db, x["owner"].id)) == [("social_render", fresh)]
    approve(x, fresh)
    assert read(x, fresh).state == "queued" and not x["voice_calls"]
