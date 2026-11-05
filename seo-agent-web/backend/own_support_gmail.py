"""Separate readonly Gmail grant; only new messages in the confirmed support label."""

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import re
import secrets
from urllib.parse import urlencode
import uuid

from sqlalchemy import select, func

try:
    from . import own_support_common as c, own_support_message as message
except ImportError:
    import own_support_common as c
    import own_support_message as message

API = "https://gmail.googleapis.com/gmail/v1/users/me"
TOKEN = "https://oauth2.googleapis.com/token"
CALLBACK = c.PATH + "/oauth/callback"
COOKIE = "noyaru_support_oauth"
# Full MIME structure without *any* body data; attachments never get a data request.
TREE = "mimeType,filename,headers,body(size,attachmentId),parts(mimeType,filename,body(size,attachmentId),parts(mimeType,filename,body(size,attachmentId)))"
META_FIELDS = "id,threadId,labelIds,internalDate,sizeEstimate,payload(" + TREE + ")"
BODY_FIELDS = "id,threadId,labelIds,internalDate,sizeEstimate,payload(mimeType,filename,headers,body,parts(mimeType,filename,body))"


def configure(owner, mailbox, label, client_id, client_secret, confirm):
    if not confirm:
        raise c.Stop("confirm_required")
    mailbox = message.addresses(mailbox, single=True)[0].casefold()
    if (
        not mailbox.endswith("@gmail.com")
        or not 1 <= len(label.strip()) <= 80
        or any(ord(x) < 32 for x in label)
    ):
        raise c.Stop("configuration_required")
    if (
        not re.fullmatch(r"[A-Za-z0-9_-]{5,}\.apps\.googleusercontent\.com", client_id)
        or not 8 <= len(client_secret) <= 300
        or any(ord(x) < 33 for x in client_secret)
    ):
        raise c.Stop("configuration_required")
    # Isolate the grant from the client-facing Google project, including revocations.
    if client_id == c.module()._google_oauth_client()[0]:
        raise c.Stop("separate_client_required")
    encrypted = c.seal({"client_id": client_id, "client_secret": client_secret})
    with c.module().DB.session() as db:
        c.s.locked(db)
        c.s.actor(db, owner)
        row = db.get(c.Channel, c.KEY)
        if row and row.owner_id != owner:
            raise c.Stop("owner_changed")
        if row and row.active:
            raise c.Stop("pause_first")
        if row and row.lease_until and c.utc(row.lease_until) > c.now():
            raise c.Stop("busy")
        row = row or c.Channel(id=c.KEY, owner_id=owner)
        row.binding = uuid.uuid4().hex
        row.mailbox = mailbox
        row.label_name = label.strip()
        row.label_id = None
        row.active = False
        row.gmail_credentials = encrypted
        row.cursor = None
        row.activated_at = None
        row.pending = {}
        row.oauth_nonce_hash = None
        row.oauth_until = None
        row.error_code = None
        row.read_count = 0
        row.read_window = c.now()
        db.add(row)
        c.audit(db, owner, "configure", meta={"binding": row.binding})
        db.commit()


def start(owner, redirect_uri):
    nonce = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(48)
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = c.channel(db, owner, active=False)
        if cfg.active:
            raise c.Stop("pause_first")
        credentials = c.reveal(cfg.gmail_credentials)
        credentials["verifier"] = verifier
        cfg.gmail_credentials = c.seal(credentials)
        cfg.oauth_nonce_hash = c.s.digest(nonce)
        cfg.oauth_until = c.now() + timedelta(minutes=10)
        payload = {
            "purpose": "own_support",
            "owner": owner,
            "binding": cfg.binding,
            "nonce": nonce,
            "until": int(cfg.oauth_until.timestamp()),
            "redirect": redirect_uri,
        }
        state = c.module()._oauth_state_encode(payload)
        db.commit()
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .decode()
        .rstrip("=")
    )
    params = {
        "client_id": credentials["client_id"],
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": c.SCOPE,
        "access_type": "offline",
        "prompt": "consent select_account",
        "include_granted_scopes": "false",
        "login_hint": cfg.mailbox,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params), nonce


def finish(owner, code, state, cookie, redirect_uri):
    payload = c.module()._oauth_state_decode(state)
    if (
        not payload
        or payload.get("purpose") != "own_support"
        or payload.get("owner") != owner
        or payload.get("nonce") != cookie
        or payload.get("redirect") != redirect_uri
        or not code
        or len(code) > 2500
    ):
        raise c.Stop("oauth_changed")
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = c.channel(db, owner, active=False)
        if (
            cfg.active
            or cfg.binding != payload.get("binding")
            or not cfg.oauth_nonce_hash
            or cfg.oauth_nonce_hash != c.s.digest(cookie)
            or not cfg.oauth_until
            or c.utc(cfg.oauth_until) <= c.now()
        ):
            raise c.Stop("oauth_changed")
        credentials = c.reveal(cfg.gmail_credentials)
        verifier = credentials.pop("verifier", None)
        if not verifier:
            raise c.Stop("oauth_changed")
        cfg.oauth_nonce_hash = None
        cfg.oauth_until = None
        cfg.gmail_credentials = c.seal(credentials)
        binding = cfg.binding
        mailbox = cfg.mailbox
        db.commit()
    # State consumed before the single code exchange. A lost exchange requires fresh consent.
    _, tokens, _ = c.http(
        "POST",
        TOKEN,
        data={
            "code": code,
            "client_id": credentials["client_id"],
            "client_secret": credentials["client_secret"],
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
            "code_verifier": verifier,
        },
    )
    if (
        set(str(tokens.get("scope", "")).split()) != {c.SCOPE}
        or tokens.get("token_type") != "Bearer"
        or not tokens.get("refresh_token")
        or not tokens.get("access_token")
    ):
        raise c.Stop("scope_changed")
    _, profile, _ = c.http(
        "GET",
        API + "/profile",
        headers={"Authorization": "Bearer " + tokens["access_token"]},
        params={"fields": "emailAddress,historyId"},
    )
    if profile.get("emailAddress", "").casefold() != mailbox:
        raise c.Stop("mailbox_changed")
    credentials.update(
        refresh_token=tokens["refresh_token"],
        access_token=tokens["access_token"],
        scope=c.SCOPE,
        expires_at=(
            c.now() + timedelta(seconds=min(3600, int(tokens.get("expires_in", 0))))
        ).isoformat(),
    )
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = c.channel(db, owner, active=False)
        if cfg.binding != binding or cfg.active:
            raise c.Stop("binding_changed")
        cfg.gmail_credentials = c.seal(credentials)
        cfg.error_code = None
        c.audit(db, owner, "connected", meta={"binding": binding})
        db.commit()


class Reader:
    def __init__(self, owner, binding, lease, *, allow_paused=False):
        self.owner, self.binding, self.lease, self.allow_paused = (
            owner,
            binding,
            lease,
            allow_paused,
        )
        self.access = ""
        self.calls = 0

    def guard(self):
        self.calls += 1
        if self.calls > 30:
            raise c.Stop("read_limit")
        with c.module().DB.session() as db:
            c.s.locked(db)
            cfg = c.channel(db, self.owner, active=not self.allow_paused)
            if (
                cfg.binding != self.binding
                or cfg.lease_token != self.lease
                or not cfg.lease_until
                or c.utc(cfg.lease_until) <= c.now()
            ):
                raise c.Stop("lease_ended")
            if (
                not cfg.read_window
                or c.utc(cfg.read_window) + timedelta(hours=24) <= c.now()
            ):
                cfg.read_window = c.now()
                cfg.read_count = 0
            if cfg.read_count >= c.DAILY_READS:
                raise c.Stop("read_limit")
            cfg.read_count += 1
            credentials = c.reveal(cfg.gmail_credentials)
            db.commit()
            return cfg, credentials

    def token(self):
        cfg, credentials = self.guard()
        if credentials.get("scope") != c.SCOPE or not credentials.get("refresh_token"):
            raise c.Stop("connection_required")
        try:
            valid = datetime.fromisoformat(
                credentials["expires_at"]
            ) > c.now() + timedelta(seconds=60)
        except Exception:
            valid = False
        if valid:
            self.access = credentials["access_token"]
            return cfg
        _, value, _ = c.http(
            "POST",
            TOKEN,
            data={
                "grant_type": "refresh_token",
                "refresh_token": credentials["refresh_token"],
                "client_id": credentials["client_id"],
                "client_secret": credentials["client_secret"],
            },
        )
        if (
            value.get("token_type") != "Bearer"
            or not value.get("access_token")
            or set(str(value.get("scope", c.SCOPE)).split()) != {c.SCOPE}
        ):
            raise c.Stop("scope_changed")
        credentials.update(
            access_token=value["access_token"],
            expires_at=(
                c.now() + timedelta(seconds=min(3600, int(value.get("expires_in", 0))))
            ).isoformat(),
        )
        if value.get("refresh_token"):
            credentials["refresh_token"] = value["refresh_token"]
        with c.module().DB.session() as db:
            c.s.locked(db)
            live = c.channel(db, self.owner, active=not self.allow_paused)
            if live.binding != self.binding or live.lease_token != self.lease:
                raise c.Stop("binding_changed")
            live.gmail_credentials = c.seal(credentials)
            db.commit()
        self.access = credentials["access_token"]
        return cfg

    def get(self, path, **params):
        self.guard()
        if path not in {"/profile", "/labels", "/history"} and not re.fullmatch(
            "/messages/[a-f0-9]{1,40}", path
        ):
            raise c.Stop("tool_forbidden")
        return c.http(
            "GET",
            API + path,
            headers={"Authorization": "Bearer " + self.access},
            params=params,
        )[1]

    def verify(self):
        cfg = self.token()
        profile = self.get("/profile", fields="emailAddress,historyId")
        if profile.get("emailAddress", "").casefold() != cfg.mailbox:
            raise c.Stop("mailbox_changed")
        labels = self.get("/labels", fields="labels(id,name,type)").get("labels")
        if not isinstance(labels, list) or len(labels) > 1000:
            raise c.Stop("label_changed")
        match = [
            r
            for r in labels
            if r.get("name") == cfg.label_name and r.get("type") == "user"
        ]
        if len(match) != 1 or not re.fullmatch(
            "[A-Za-z0-9_-]{1,100}", str(match[0].get("id", ""))
        ):
            raise c.Stop("label_changed")
        if cfg.label_id and cfg.label_id != match[0]["id"]:
            raise c.Stop("label_changed")
        return cfg, match[0]["id"], c.history_id(profile.get("historyId"))

    def incoming(self, ident, cfg):
        cfg, _, _ = self.verify()
        ident = c.google_id(ident)
        meta = self.get("/messages/" + ident, format="full", fields=META_FIELDS)
        payload = message.envelope(meta, cfg)
        try:
            received = datetime.fromtimestamp(
                int(meta["internalDate"]) / 1000, tz=timezone.utc
            )
        except Exception:
            raise c.Stop("message_changed") from None
        if (
            not cfg.activated_at
            or received < c.utc(cfg.activated_at)
            or received < c.now() - c.RETENTION
            or received > c.now() + timedelta(minutes=5)
        ):
            raise c.Stop("outside_window")
        if (
            type(meta.get("sizeEstimate")) is not int
            or not 0 < meta["sizeEstimate"] <= 64000
        ):
            return meta, payload, "message_limit", received
        try:
            if not message.admissible(meta.get("payload")):
                raise c.Stop("plain_text_required")
        except c.Stop as exc:
            return meta, payload, str(exc), received
        full = self.get("/messages/" + ident, format="full", fields=BODY_FIELDS)
        if (
            message.envelope(full, cfg) != payload
            or full.get("id") != ident
            or full.get("threadId") != meta.get("threadId")
        ):
            raise c.Stop("message_changed")
        try:
            payload["text"] = message.plain_body(full)
        except c.Stop as exc:
            return meta, payload, str(exc), received
        reason = (
            "sensitive_request" if message.SENSITIVE.search(payload["text"]) else None
        )
        return meta, payload, reason, received


def claim(owner, *, paused=False):
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = c.channel(db, owner, active=not paused)
        if cfg.lease_until and c.utc(cfg.lease_until) > c.now():
            raise c.Stop("busy")
        cfg.lease_token = uuid.uuid4().hex
        cfg.lease_until = c.now() + timedelta(minutes=4)
        cfg.next_poll_at = c.now() + timedelta(minutes=5)
        db.commit()
        return Reader(owner, cfg.binding, cfg.lease_token, allow_paused=paused)


def release(reader, error=None):
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = db.get(c.Channel, c.KEY)
        if not cfg or cfg.binding != reader.binding or cfg.lease_token != reader.lease:
            return
        if error:
            cfg.error_code = error
            if error in {
                "not_found",
                "label_changed",
                "mailbox_changed",
                "connection_denied",
                "connection_required",
                "scope_changed",
                "cursor_changed",
            }:
                cfg.active = False
        else:
            cfg.error_code = None
        cfg.lease_token = None
        cfg.lease_until = None
        db.commit()


def activate(owner, confirm):
    if not confirm:
        raise c.Stop("confirm_required")
    reader = claim(owner, paused=True)
    error = None
    try:
        cfg, label, cursor = reader.verify()
        with c.module().DB.session() as db:
            c.s.locked(db)
            live = c.channel(db, owner, active=False)
            if live.binding != reader.binding or live.lease_token != reader.lease:
                raise c.Stop("binding_changed")
            # Start now after first connection or an expired cursor. No historical import.
            if not live.cursor or live.error_code in {"not_found", "cursor_changed"}:
                live.cursor = cursor
                live.activated_at = c.now()
                live.pending = {}
            live.label_id = label
            live.active = True
            live.next_poll_at = c.now()
            c.audit(db, owner, "activate", meta={"binding": live.binding})
            db.commit()
    except c.Stop as exc:
        error = str(exc)
        raise
    finally:
        release(reader, error)


def pause(owner):
    with c.module().DB.session() as db:
        c.s.locked(db)
        cfg = c.channel(db, owner, active=False)
        cfg.active = False
        cfg.lease_token = None
        cfg.lease_until = None
        c.audit(db, owner, "pause")
        db.commit()


def poll(owner):
    try:
        reader = claim(owner)
    except c.Stop:
        return
    error = None
    try:
        cfg, label, _ = reader.verify()
        if not cfg.pending or cfg.pending.get("page_request"):
            params = {
                "startHistoryId": c.history_id(cfg.cursor),
                "labelId": label,
                "historyTypes": ["messageAdded", "labelAdded"],
                "maxResults": 10,
                "fields": "history(messagesAdded(message(id,threadId)),labelsAdded(message(id,threadId),labelIds)),historyId,nextPageToken",
            }
            if cfg.pending.get("page_request"):
                params["pageToken"] = cfg.pending["page_request"]
            result = reader.get("/history", **params)
            ids = []
            for history in result.get("history", []):
                for change in history.get("messagesAdded", []) + history.get(
                    "labelsAdded", []
                ):
                    ident = c.google_id((change.get("message") or {}).get("id"))
                    if ident not in ids:
                        ids.append(ident)
            if len(ids) > 20:
                raise c.Stop("read_limit")
            pending = {
                "ids": ids,
                "cursor": c.history_id(result.get("historyId")),
                "page": result.get("nextPageToken"),
            }
            with c.module().DB.session() as db:
                c.s.locked(db)
                live = c.channel(db, owner)
                if live.lease_token != reader.lease:
                    raise c.Stop("lease_ended")
                live.pending = pending
                db.commit()
        else:
            pending = cfg.pending
        # Cursor is committed only after all message outcomes. An interruption dedupes IDs.
        for ident in pending["ids"][:5]:
            with c.module().DB.session() as db:
                existing = db.scalar(
                    select(c.Ticket.id).where(
                        c.Ticket.binding == reader.binding, c.Ticket.google_id == ident
                    )
                )
                count = (
                    db.scalar(
                        select(func.count())
                        .select_from(c.Ticket)
                        .where(
                            c.Ticket.owner_id == owner,
                            c.Ticket.created_at >= c.now() - timedelta(hours=24),
                        )
                    )
                    or 0
                )
            if not existing:
                if count >= c.DAILY_IMPORT:
                    raise c.Stop("import_limit")
                try:
                    meta, payload, reason, received = reader.incoming(ident, cfg)
                except c.Stop as exc:
                    if str(exc) not in {
                        "outside_label",
                        "outside_support",
                        "loop_blocked",
                        "sender_unconfirmed",
                        "outside_window",
                        "not_found",
                        "message_limit",
                    }:
                        raise
                    meta = {"threadId": ident}
                    payload = {}
                    reason = str(exc)
                    received = c.now()
                with c.module().DB.session() as db:
                    c.s.locked(db)
                    live = c.channel(db, owner)
                    if (
                        live.binding != reader.binding
                        or live.lease_token != reader.lease
                    ):
                        raise c.Stop("binding_changed")
                    ignored = reason in {
                        "outside_label",
                        "outside_support",
                        "loop_blocked",
                        "outside_window",
                        "not_found",
                        "sender_unconfirmed",
                    }
                    row = c.Ticket(
                        owner_id=owner,
                        channel_id=c.KEY,
                        binding=reader.binding,
                        google_id=ident,
                        thread_id=meta["threadId"],
                        received_at=received,
                        payload_encrypted=c.seal(payload),
                        input_hash=c.s.digest(payload),
                        lang="en"
                        if payload.get("text", "")
                        .lower()
                        .startswith(("hello", "hi ", "how ", "i "))
                        else "fr",
                        state="ignored"
                        if ignored
                        else "review_required"
                        if reason
                        else "received",
                        reason=reason,
                        revision=1,
                        dossier={},
                        receipt={},
                        provider_calls=0,
                        created_at=c.now(),
                    )
                    db.add(row)
                    db.flush()
                    c.audit(db, owner, "receive", row.id, {"state": row.state})
                    db.commit()
            with c.module().DB.session() as db:
                c.s.locked(db)
                live = c.channel(db, owner)
                if live.lease_token != reader.lease:
                    raise c.Stop("lease_ended")
                live.pending = {
                    **live.pending,
                    "ids": [i for i in live.pending["ids"] if i != ident],
                }
                db.commit()
        with c.module().DB.session() as db:
            c.s.locked(db)
            live = c.channel(db, owner)
            if live.lease_token != reader.lease:
                raise c.Stop("lease_ended")
            if not live.pending.get("ids"):
                # Gmail page tokens need the same starting cursor. Keep both until final page.
                if live.pending.get("page"):
                    live.pending = {
                        "ids": [],
                        "page_request": live.pending["page"],
                        "cursor": live.pending["cursor"],
                    }
                else:
                    live.cursor = live.pending["cursor"]
                    live.pending = {}
            live.next_poll_at = c.now() + timedelta(minutes=5)
            db.commit()
    except c.Stop as exc:
        error = str(exc)
    except Exception:
        error = "transport_uncertain"
    finally:
        release(reader, error)
