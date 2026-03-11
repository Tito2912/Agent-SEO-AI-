from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any


SESSION_COOKIE_NAME = "seo_agent_session"
SESSION_TTL_S = 60 * 60 * 24 * 30  # 30 days


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    s = (raw or "").strip()
    if not s:
        return b""
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _hmac_sig(secret: str, msg: bytes) -> str:
    key = (secret or "").encode("utf-8")
    return _b64url_encode(hmac.new(key, msg, hashlib.sha256).digest())


def make_session_token(*, user_id: str, secret: str, ttl_s: int = SESSION_TTL_S) -> str:
    now = int(time.time())
    payload = {"uid": str(user_id), "iat": now, "exp": now + int(ttl_s)}
    body = _b64url_encode(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    sig = _hmac_sig(secret, body.encode("ascii"))
    return f"{body}.{sig}"


def parse_session_token(token: str, *, secret: str) -> dict[str, Any] | None:
    raw = (token or "").strip()
    if not raw or "." not in raw:
        return None
    body, sig = raw.split(".", 1)
    expected = _hmac_sig(secret, body.encode("ascii"))
    if not hmac.compare_digest(sig, expected):
        return None
    try:
        payload = json.loads(_b64url_decode(body).decode("utf-8", errors="replace"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    exp = payload.get("exp")
    try:
        exp_i = int(exp)
    except Exception:
        return None
    if exp_i <= int(time.time()):
        return None
    uid = str(payload.get("uid") or "").strip()
    if not uid:
        return None
    return payload


def hash_password(password: str, *, iterations: int = 310_000) -> str:
    pw = (password or "").encode("utf-8")
    salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw, salt, int(iterations))
    return f"pbkdf2_sha256${int(iterations)}${_b64url_encode(salt)}${_b64url_encode(dk)}"


def verify_password(password: str, password_hash: str) -> bool:
    raw = (password_hash or "").strip()
    if not raw:
        return False
    parts = raw.split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return False
    try:
        iterations = int(parts[1])
        salt = _b64url_decode(parts[2])
        expected = _b64url_decode(parts[3])
    except Exception:
        return False
    dk = hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), salt, iterations)
    return hmac.compare_digest(dk, expected)

