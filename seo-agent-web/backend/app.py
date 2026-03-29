from __future__ import annotations

import base64
import datetime as dt
import io
import hashlib
import hmac
import html
import importlib.util
import ipaddress
import json
import math
import os
import re
import secrets
import shutil
import socket
import smtplib
import subprocess
import sys
import threading
import time
import textwrap
import unicodedata
import uuid
import csv
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, parse_qsl, quote, urlencode, urlsplit, urlunsplit

import requests
import yaml
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:
    BeautifulSoup = None  # type: ignore
try:  # pragma: no cover - optional dependency
    from cryptography.fernet import Fernet, InvalidToken  # type: ignore
except Exception:  # pragma: no cover
    Fernet = None  # type: ignore

    class InvalidToken(Exception):  # type: ignore
        pass
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request as StarletteRequest
from starlette.responses import Response

try:
    # When running as `uvicorn backend.app:app` (recommended).
    from . import audit_dashboard as dash  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import audit_dashboard as dash  # type: ignore

try:
    # When running as `uvicorn backend.app:app` (recommended).
    from . import fix_suggestions as fix_suggestions  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import fix_suggestions  # type: ignore

try:
    # When running as `uvicorn backend.app:app` (recommended).
    from . import fix_pack as fix_pack  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import fix_pack  # type: ignore

try:
    # When running as `uvicorn backend.app:app` (recommended).
    from . import billing as billing  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import billing  # type: ignore

try:
    # When running as `uvicorn backend.app:app` (recommended).
    from . import object_store as object_store  # type: ignore
except ImportError:
    # When running from inside this folder (`uvicorn app:app`) or with `--app-dir seo-agent-web/backend`.
    import object_store  # type: ignore


try:
    from .db import Database  # type: ignore
    from .models import (  # type: ignore
        AuditLog,
        EmailVerificationToken,
        JobRecord,
        OAuthIdentity,
        PasswordResetToken,
        Project,
        User,
        UserConnection,
    )
    from . import auth as auth  # type: ignore
except ImportError:
    from db import Database  # type: ignore
    from models import (  # type: ignore
        AuditLog,
        EmailVerificationToken,
        JobRecord,
        OAuthIdentity,
        PasswordResetToken,
        Project,
        User,
        UserConnection,
    )
    import auth as auth  # type: ignore


REPO_ROOT = Path(__file__).resolve().parents[2]
AUTOPILOT_SCRIPTS_DIR = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts"
_GSC_FETCH_MODULE: Any | None = None

def _env_path(name: str, default: Path) -> Path:
    raw = str(os.environ.get(name) or "").strip().strip('"').strip("'")
    if not raw:
        return default
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (REPO_ROOT / p).resolve()
    return p


DEFAULT_CONFIG = _env_path("SEO_AGENT_CONFIG_PATH", REPO_ROOT / "seo-autopilot.yml")
DEFAULT_RUNS_DIR = _env_path("SEO_AGENT_RUNS_DIR", REPO_ROOT / "seo-runs")

DATA_DIR = _env_path("SEO_AGENT_DATA_DIR", REPO_ROOT / "seo-agent-web" / "data")
JOBS_DIR = DATA_DIR / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)
PROJECTS_PATH = DATA_DIR / "projects.json"
GSC_OAUTH_DIR = DATA_DIR / "gsc-oauth"
GSC_OAUTH_DIR.mkdir(parents=True, exist_ok=True)

DB = Database(data_dir=DATA_DIR)

_PROJECTS_LOCK = threading.Lock()

_USER_CONNECTION_KEYS: set[str] = {
    "GITHUB_TOKEN",
    "NETLIFY_TOKEN",
    "BING_WEBMASTER_API_KEY",
}

_CSRF_COOKIE_NAME = "seo_agent_csrf"
_CSRF_FORM_FIELD = "_csrf"
_CSRF_HEADER_NAME = "x-csrf-token"
_CSRF_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}
_CSRF_EXEMPT_PATHS = {"/healthz", "/stripe/webhook"}

_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = {}


def _runs_dir_for_user(user_id: str) -> Path:
    p = (DEFAULT_RUNS_DIR / str(user_id)).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _runs_dir_for_request(request: Request) -> Path:
    user = getattr(request.state, "user", None)
    if not user:
        return DEFAULT_RUNS_DIR
    return _runs_dir_for_user(str(user.id))


def _run_tree_candidates(path: Path) -> list[Path]:
    root = DEFAULT_RUNS_DIR.resolve()
    try:
        rel = path.resolve().relative_to(root)
    except Exception:
        return []
    parts = rel.parts
    if not parts:
        return []
    candidates: list[Path] = []
    for idx, part in enumerate(parts):
        if _RUN_TS_RE.fullmatch(part):
            candidates.append(root.joinpath(*parts[: idx + 1]))
            if idx > 0:
                candidates.append(root.joinpath(*parts[:idx]))
            break
    if not candidates:
        candidates.append(path.resolve())
    out: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def _ensure_runs_artifact_local(path: Path) -> bool:
    try:
        target = path.resolve()
    except Exception:
        return False
    if target.exists():
        return True
    root = DEFAULT_RUNS_DIR.resolve()
    try:
        target.relative_to(root)
    except Exception:
        return False
    if object_store.restore_runs_file(root, target):
        return True
    if object_store.restore_runs_tree(root, target):
        return True
    for candidate in _run_tree_candidates(target):
        if object_store.restore_runs_tree(root, candidate) and (target.exists() or candidate.exists()):
            return True
    return target.exists()


def _sync_runs_path_to_object_store(path: Path) -> None:
    try:
        object_store.upload_runs_path(DEFAULT_RUNS_DIR, path.resolve())
    except Exception as e:
        print(f"[S3] upload error for {path}: {type(e).__name__}: {e}")


def _delete_runs_path_from_object_store(path: Path, *, recursive: bool = False) -> None:
    try:
        object_store.delete_runs_path(DEFAULT_RUNS_DIR, path.resolve(), recursive=recursive)
    except Exception as e:
        print(f"[S3] delete error for {path}: {type(e).__name__}: {e}")


dash.register_runs_localizer(_ensure_runs_artifact_local)

_JOB_LOCKS_GUARD = threading.Lock()
_JOB_LOCKS: dict[str, threading.Lock] = {}

_ACTIVE_JOBS_LOCK = threading.Lock()
_ACTIVE_JOBS: set[str] = set()


_GOOGLE_OAUTH_SCOPE = "https://www.googleapis.com/auth/webmasters"
_GOOGLE_AUTH_SCOPE = "openid email profile"
_GITHUB_OAUTH_SCOPE = "read:user user:email repo"
_BING_OAUTH_SCOPE = "webmaster.manage offline_access"
_NETLIFY_PAT_EXPIRES_IN_SECONDS = 60 * 60 * 24 * 365

_BING_OAUTH_CONNECTION_KEY = "BING_OAUTH_REFRESH_TOKEN"
_RUN_TS_RE = re.compile(r"\d{8}-\d{6}")


def _mark_job_active(job_id: str, active: bool) -> None:
    if not job_id:
        return
    with _ACTIVE_JOBS_LOCK:
        if active:
            _ACTIVE_JOBS.add(job_id)
        else:
            _ACTIVE_JOBS.discard(job_id)


def _is_job_active(job_id: str) -> bool:
    if not job_id:
        return False
    with _ACTIVE_JOBS_LOCK:
        return job_id in _ACTIVE_JOBS


def _pid_is_alive(pid: int | None) -> bool:
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        # On POSIX, signal 0 is a no-op used for existence checks.
        # On Windows, `os.kill(pid, 0)` is also supported on modern Python.
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _job_lock(job_id: str) -> threading.Lock:
    with _JOB_LOCKS_GUARD:
        lock = _JOB_LOCKS.get(job_id)
        if lock is None:
            lock = threading.RLock()
            _JOB_LOCKS[job_id] = lock
        return lock

_BASE_ENV = os.environ.copy()
_BASE_ENV_KEYS = set(_BASE_ENV.keys())


_ENV_LINE_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")


def _env_unquote(value: str) -> str:
    v = (value or "").strip()
    if len(v) >= 2 and ((v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'"))):
        return v[1:-1]
    return v


def _load_env_file(path: Path, *, override: bool) -> None:
    if not path.exists() or not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE_RE.match(line)
        if not m:
            continue
        key = (m.group(1) or "").strip()
        value = _env_unquote(m.group(2) or "")
        if not key:
            continue
        if override:
            # Do not override OS-provided env vars by default; allow overrides only of values coming from files.
            base_value = str(_BASE_ENV.get(key) or "").strip() if key in _BASE_ENV_KEYS else ""
            if base_value:
                continue
            os.environ[key] = value
        else:
            os.environ.setdefault(key, value)


# Load `.env` files so subprocesses (crawl/autopilot) can access API keys/tokens when the UI launches jobs.
# We keep it additive (`setdefault`) so OS env vars still win.
#
# Notes:
# - `.env` stays in the repo (dev convenience).
# - UI-edited overrides live in `DATA_DIR` so they persist on Render's mounted disk.
_load_env_file(REPO_ROOT / ".env", override=False)
# Backward compatible: repo-root overrides (local dev)
_load_env_file(REPO_ROOT / ".env.gsc", override=True)
_load_env_file(REPO_ROOT / ".env.local", override=True)
# Preferred: persisted overrides (Render disk)
_load_env_file(DATA_DIR / ".env.gsc", override=True)
_load_env_file(DATA_DIR / ".env.local", override=True)


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    out: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE_RE.match(line)
        if not m:
            continue
        key = (m.group(1) or "").strip()
        if not key:
            continue
        out[key] = _env_unquote(m.group(2) or "")
    return out


def _write_env_key(path: Path, key: str, value: str | None) -> None:
    key = (key or "").strip()
    if not key:
        raise ValueError("Missing env key")
    if value is not None:
        value = str(value)
        if "\n" in value or "\r" in value:
            raise ValueError("Invalid env value (newline)")

    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    out: list[str] = []
    replaced = False
    for raw in lines:
        m = _ENV_LINE_RE.match(raw)
        if not m or (m.group(1) or "").strip() != key:
            out.append(raw)
            continue
        replaced = True
        if value is None:
            continue
        out.append(f"{key}={value}")

    if value is not None and not replaced:
        if out and out[-1].strip():
            out.append("")
        out.append(f"{key}={value}")

    # Keep a trailing newline.
    path.write_text("\n".join(out).rstrip("\n") + "\n", encoding="utf-8")


def _env_effective_value(key: str) -> tuple[str | None, str]:
    k = (key or "").strip()
    if not k:
        return None, "none"
    if k in _BASE_ENV_KEYS and k in _BASE_ENV:
        v = str(_BASE_ENV.get(k) or "").strip()
        if v:
            return v, "os"

    env_local = _read_env_file(DATA_DIR / ".env.local")
    if k in env_local and str(env_local.get(k) or "").strip():
        return str(env_local.get(k) or ""), "data/.env.local"
    env_local_repo = _read_env_file(REPO_ROOT / ".env.local")
    if k in env_local_repo and str(env_local_repo.get(k) or "").strip():
        return str(env_local_repo.get(k) or ""), ".env.local"

    env_gsc = _read_env_file(DATA_DIR / ".env.gsc")
    if k in env_gsc and str(env_gsc.get(k) or "").strip():
        return str(env_gsc.get(k) or ""), "data/.env.gsc"
    env_gsc_repo = _read_env_file(REPO_ROOT / ".env.gsc")
    if k in env_gsc_repo and str(env_gsc_repo.get(k) or "").strip():
        return str(env_gsc_repo.get(k) or ""), ".env.gsc"
    env_base = _read_env_file(REPO_ROOT / ".env")
    if k in env_base and str(env_base.get(k) or "").strip():
        return str(env_base.get(k) or ""), ".env"
    return None, "none"


def _safe_env(name: str) -> str:
    return str(os.environ.get(name) or "").strip().strip('"').strip("'")


def _env_bool(name: str) -> bool:
    v = _safe_env(name).lower()
    return v in {"1", "true", "yes", "y", "on"}


def _env_list(name: str) -> list[str]:
    raw = _safe_env(name)
    if not raw:
        return []
    parts = [p.strip() for p in re.split(r"[,\n;]+", raw) if p and p.strip()]
    return [p for p in parts if p]

_SECRET_PREFIX = "enc:"


def _encryption_seeds() -> list[str]:
    """
    Returns encryption seeds in priority order.

    Rotation:
    - Set `SEO_AGENT_ENCRYPTION_KEYS` (comma/newline/semicolon separated) with the *current* key first,
      then previous keys for read/decrypt compatibility.
    - For backward compatibility, falls back to `SEO_AGENT_ENCRYPTION_KEY` then `SEO_AGENT_SECRET_KEY`.
    """
    raw = _safe_env("SEO_AGENT_ENCRYPTION_KEYS")
    if raw:
        parts = [p.strip() for p in re.split(r"[,\n;]+", raw) if p and p.strip()]
        out = [p for p in parts if p]
        legacy = _safe_env("SEO_AGENT_SECRET_KEY").strip()
        if legacy and legacy not in out:
            out.append(legacy)
        return out
    seed = _safe_env("SEO_AGENT_ENCRYPTION_KEY") or _safe_env("SEO_AGENT_SECRET_KEY")
    seed = seed.strip()
    return [seed] if seed else []


def _encryption_ready() -> bool:
    return bool(_encryption_seeds()) and Fernet is not None


@lru_cache(maxsize=1)
def _fernets() -> list[Any]:
    if not _encryption_ready():
        return []

    out: list[Any] = []
    for seed in _encryption_seeds():
        digest = hashlib.sha256(seed.encode("utf-8")).digest()
        key = base64.urlsafe_b64encode(digest)
        try:
            f = Fernet(key)  # type: ignore[misc]
        except Exception:
            continue
        out.append(f)
    return out


def _encrypt_secret(plaintext: str) -> str:
    raw = (plaintext or "").strip()
    if not raw or raw.startswith(_SECRET_PREFIX):
        return raw
    f_list = _fernets()
    if not f_list:
        return raw
    try:
        token = f_list[0].encrypt(raw.encode("utf-8")).decode("ascii")
    except Exception:
        return raw
    return f"{_SECRET_PREFIX}{token}"

def _decrypt_secret_with_rotation(stored: str) -> tuple[str, bool]:
    raw = (stored or "").strip()
    if not raw:
        return "", False
    if not raw.startswith(_SECRET_PREFIX):
        return raw, False
    f_list = _fernets()
    if not f_list:
        raise RuntimeError("encryption_not_configured")
    token = raw[len(_SECRET_PREFIX) :].strip()
    for idx, f in enumerate(f_list):
        try:
            value = f.decrypt(token.encode("ascii")).decode("utf-8")
            return value, idx > 0
        except InvalidToken:
            continue
        except Exception:
            continue
    raise RuntimeError("invalid_encryption_key")


def _decrypt_secret(stored: str) -> str:
    value, _rotated = _decrypt_secret_with_rotation(stored)
    return value


def _project_meta(settings: dict[str, Any] | None) -> dict[str, Any]:
    node = settings if isinstance(settings, dict) else {}
    meta = node.get("_meta")
    return dict(meta) if isinstance(meta, dict) else {}


def _project_visible_in_connections(project: Project) -> bool:
    meta = _project_meta(project.settings if isinstance(project.settings, dict) else {})
    if bool(meta.get("hide_from_connections")):
        return False
    import_source = str(meta.get("import_source") or "").strip().lower()
    return import_source not in {"legacy_registry", "legacy_import"}

def _effective_user_connection_value(*, user_id: str, key: str, db=None) -> tuple[str, str]:
    normalized_key = str(key or "").strip()
    if not normalized_key:
        return "", "none"

    own_session = db is None
    session = db
    try:
        if session is None:
            session_ctx = DB.session()
            session = session_ctx.__enter__()
        else:
            session_ctx = None

        row = session.scalar(
            select(UserConnection).where(
                UserConnection.user_id == str(user_id),
                UserConnection.key == normalized_key,
            )
        )
        stored = str(getattr(row, "secret_value", "") or "").strip()
        if stored:
            rotated = False
            try:
                value, rotated = _decrypt_secret_with_rotation(stored)
            except Exception:
                value = ""
            if value:
                # Lazy-migrate plaintext/old-key secrets to encrypted-at-rest (current key).
                if _encryption_ready() and row is not None and (rotated or not stored.startswith(_SECRET_PREFIX)):
                    try:
                        row.secret_value = _encrypt_secret(value)
                        session.add(row)
                        session.commit()
                    except Exception:
                        session.rollback()
                return value, "user"
        system_value = _safe_env(normalized_key)
        if system_value:
            return system_value, "system"
        return "", "none"
    finally:
        if own_session and session is not None:
            session_ctx.__exit__(None, None, None)


def _upsert_user_connection(*, user_id: str, key: str, value: str, meta: dict[str, Any] | None = None) -> None:
    stored_value = _encrypt_secret(str(value))
    with DB.session() as db:
        row = db.scalar(
            select(UserConnection).where(
                UserConnection.user_id == str(user_id),
                UserConnection.key == str(key),
            )
        )
        if row:
            row.secret_value = stored_value
            if meta is not None:
                row.meta = dict(meta)
            db.add(row)
        else:
            db.add(
                UserConnection(
                    user_id=str(user_id),
                    key=str(key),
                    secret_value=stored_value,
                    meta=(dict(meta) if meta is not None else {}),
                )
            )
        db.commit()


def _delete_user_connection(*, user_id: str, key: str) -> None:
    with DB.session() as db:
        row = db.scalar(
            select(UserConnection).where(
                UserConnection.user_id == str(user_id),
                UserConnection.key == str(key),
            )
        )
        if row:
            db.delete(row)
            db.commit()


def _effective_bing_connection(*, user_id: str, db=None) -> dict[str, Any]:
    own_session = db is None
    session = db
    try:
        if session is None:
            session_ctx = DB.session()
            session = session_ctx.__enter__()
        else:
            session_ctx = None

        oauth_row = _user_connection_row(user_id=str(user_id), key=_BING_OAUTH_CONNECTION_KEY, db=session)
        oauth_meta = _connection_meta(oauth_row)
        refresh_token_stored = str(getattr(oauth_row, "secret_value", "") or "").strip() if oauth_row else ""
        refresh_token = ""
        if refresh_token_stored:
            rotated = False
            try:
                refresh_token, rotated = _decrypt_secret_with_rotation(refresh_token_stored)
            except Exception:
                refresh_token = ""
            if refresh_token and _encryption_ready() and oauth_row is not None and (rotated or not refresh_token_stored.startswith(_SECRET_PREFIX)):
                try:
                    oauth_row.secret_value = _encrypt_secret(refresh_token)
                    session.add(oauth_row)
                    session.commit()
                except Exception:
                    session.rollback()
        if refresh_token:
            access_token = str(oauth_meta.get("access_token") or "").strip()
            expires_at = float(oauth_meta.get("expires_at") or 0.0) if oauth_meta.get("expires_at") else 0.0
            if (not access_token) or expires_at <= (time.time() + 60):
                client_id, client_secret = _bing_oauth_client()
                if client_id and client_secret:
                    token_data = _bing_oauth_refresh_token_data(
                        refresh_token=refresh_token,
                        client_id=client_id,
                        client_secret=client_secret,
                    )
                    access_token = str(token_data.get("access_token") or "").strip()
                    if access_token:
                        expires_in = int(token_data.get("expires_in") or 3600)
                        oauth_meta = {
                            **oauth_meta,
                            "auth_type": "oauth",
                            "access_token": access_token,
                            "expires_at": time.time() + max(60, expires_in),
                            "scope": str(token_data.get("scope") or oauth_meta.get("scope") or "").strip(),
                            "token_type": str(token_data.get("token_type") or oauth_meta.get("token_type") or "Bearer"),
                            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                        }
                        oauth_row.meta = oauth_meta
                        session.add(oauth_row)
                        session.commit()
            if access_token:
                return {
                    "mode": "oauth",
                    "source": "user",
                    "token": access_token,
                    "refresh_token": refresh_token,
                    "meta": oauth_meta,
                    "masked": _mask_secret(access_token),
                    "source_label": "mon compte",
                }

        api_key, source = _effective_user_connection_value(user_id=str(user_id), key="BING_WEBMASTER_API_KEY", db=session)
        if api_key:
            return {
                "mode": "api_key",
                "source": source,
                "token": api_key,
                "refresh_token": "",
                "meta": {},
                "masked": _mask_secret(api_key) if source == "user" else "fourni par la plateforme",
                "source_label": "mon compte" if source == "user" else ("plateforme" if source == "system" else "non configuré"),
            }
        return {
            "mode": "none",
            "source": "none",
            "token": "",
            "refresh_token": "",
            "meta": {},
            "masked": "—",
            "source_label": "non configuré",
        }
    finally:
        if own_session and session is not None:
            session_ctx.__exit__(None, None, None)


def _build_user_connection_item(*, user_id: str, key: str, db) -> dict[str, Any]:
    meta = _SETTINGS_ENV_KEYS.get(key) or {}
    value, source = _effective_user_connection_value(user_id=str(user_id), key=key, db=db)
    configured = bool(value)
    has_user_value = source == "user"
    source_label = {
        "user": "mon compte",
        "system": "plateforme",
        "none": "non configuré",
    }.get(source, "non configuré")
    masked = _mask_secret(value) if has_user_value else ("fourni par la plateforme" if source == "system" else "—")
    return {
        "key": key,
        "label": str(meta.get("label") or key),
        "hint": str(meta.get("hint") or ""),
        "configured": configured,
        "masked": masked,
        "source": source,
        "source_label": source_label,
        "has_user_value": has_user_value,
        "help": meta.get("help"),
        "group": str(meta.get("group") or "Autres"),
    }


def _build_github_connection_state(*, user_id: str, db) -> dict[str, Any]:
    item = _build_user_connection_item(user_id=user_id, key="GITHUB_TOKEN", db=db)
    row = _user_connection_row(user_id=user_id, key="GITHUB_TOKEN", db=db)
    meta = _connection_meta(row)
    auth_type = str(meta.get("auth_type") or ("manual" if item.get("has_user_value") else "")).strip().lower()
    ready = bool(_github_oauth_client()[0] and _github_oauth_client()[1] and _safe_env("SEO_AGENT_SECRET_KEY"))
    account_label = str(meta.get("login") or meta.get("name") or "").strip()
    return {
        **item,
        "ready": ready,
        "auth_type": auth_type,
        "is_oauth": auth_type == "oauth" and item.get("source") == "user",
        "is_manual": auth_type == "manual" and item.get("source") == "user",
        "account_label": account_label,
        "avatar_url": str(meta.get("avatar_url") or "").strip(),
    }


def _build_netlify_connection_state(*, user_id: str, db) -> dict[str, Any]:
    item = _build_user_connection_item(user_id=user_id, key="NETLIFY_TOKEN", db=db)
    row = _user_connection_row(user_id=user_id, key="NETLIFY_TOKEN", db=db)
    meta = _connection_meta(row)
    auth_type = str(meta.get("auth_type") or ("manual" if item.get("has_user_value") else "")).strip().lower()
    ready = bool(_netlify_oauth_client_id() and _safe_env("SEO_AGENT_SECRET_KEY"))
    account_label = str(meta.get("full_name") or meta.get("email") or meta.get("id") or "").strip()
    token_kind = str(meta.get("token_kind") or "").strip().lower()
    return {
        **item,
        "ready": ready,
        "auth_type": auth_type,
        "is_oauth": auth_type == "oauth" and item.get("source") == "user",
        "is_manual": auth_type == "manual" and item.get("source") == "user",
        "account_label": account_label,
        "token_kind": token_kind,
        "is_hardened": token_kind in {"pat", "personal_access_token"},
        "is_oauth_fallback": token_kind == "oauth_access_token",
        "pat_expires_at": str(meta.get("pat_expires_at") or "").strip(),
        "pat_upgrade_error": str(meta.get("pat_upgrade_error") or "").strip(),
    }


def _build_bing_connection_state(*, user_id: str, db) -> dict[str, Any]:
    auth = _effective_bing_connection(user_id=user_id, db=db)
    ready = bool(_bing_oauth_client()[0] and _bing_oauth_client()[1] and _safe_env("SEO_AGENT_SECRET_KEY"))
    account_label = str(auth.get("meta", {}).get("account_name") or auth.get("meta", {}).get("user_id") or "").strip()
    return {
        **auth,
        "ready": ready,
        "is_oauth": auth.get("mode") == "oauth" and auth.get("source") == "user",
        "is_manual": auth.get("mode") == "api_key" and auth.get("source") == "user",
        "account_label": account_label,
        "manual_item": _build_user_connection_item(user_id=user_id, key="BING_WEBMASTER_API_KEY", db=db),
    }


def _build_env_setting_item(key: str) -> dict[str, Any]:
    meta = _SETTINGS_ENV_KEYS.get(key) or {}
    value, src = _env_effective_value(key)
    return {
        "key": key,
        "label": str(meta.get("label") or key),
        "hint": str(meta.get("hint") or ""),
        "configured": bool(value),
        "masked": _mask_secret(value) if key != "GOOGLE_APPLICATION_CREDENTIALS" else (value or ""),
        "source": src,
        "locked": src == "os",
        "editable": bool(meta.get("editable", True)),
        "help": meta.get("help"),
    }


def _google_oauth_client() -> tuple[str, str]:
    return _safe_env("GOOGLE_OAUTH_CLIENT_ID"), _safe_env("GOOGLE_OAUTH_CLIENT_SECRET")


def _public_base_url(request: Request) -> str:
    """
    External/public base URL used to build OAuth redirect URIs.

    Prefer env PUBLIC_BASE_URL in production (reliable behind proxies), otherwise fall back
    to request headers.
    """
    configured = _safe_env("PUBLIC_BASE_URL").rstrip("/")
    if configured:
        return configured

    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc or "").split(",")[0].strip()
    if not host:
        host = request.url.netloc
    return f"{proto}://{host}".rstrip("/")


def _google_oauth_redirect_uri(request: Request) -> str:
    configured = _safe_env("GOOGLE_OAUTH_REDIRECT_URI").rstrip("/")
    if configured:
        return configured
    return f"{_public_base_url(request)}/oauth/google/callback"


def _google_auth_redirect_uri(request: Request) -> str:
    configured = _safe_env("GOOGLE_AUTH_REDIRECT_URI").rstrip("/")
    if configured:
        return configured
    return f"{_public_base_url(request)}/auth/google/callback"


def _provider_oauth_redirect_uri(request: Request, provider: str) -> str:
    key = f"{str(provider or '').strip().upper()}_OAUTH_REDIRECT_URI"
    configured = _safe_env(key).rstrip("/")
    if configured:
        return configured
    return f"{_public_base_url(request)}/oauth/{str(provider or '').strip().lower()}/callback"


def _github_oauth_client() -> tuple[str, str]:
    return _safe_env("GITHUB_OAUTH_CLIENT_ID"), _safe_env("GITHUB_OAUTH_CLIENT_SECRET")


def _netlify_oauth_client_id() -> str:
    return _safe_env("NETLIFY_OAUTH_CLIENT_ID")


def _bing_oauth_client() -> tuple[str, str]:
    return _safe_env("BING_OAUTH_CLIENT_ID"), _safe_env("BING_OAUTH_CLIENT_SECRET")


def _oauth_state_secret() -> bytes:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        raise RuntimeError("SEO_AGENT_SECRET_KEY not set (required for OAuth state signing).")
    return secret.encode("utf-8")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    v = (value or "").strip()
    if not v:
        return b""
    pad = "=" * ((4 - (len(v) % 4)) % 4)
    return base64.urlsafe_b64decode(v + pad)


def _oauth_state_encode(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    body = _b64url_encode(raw)
    sig = hmac.new(_oauth_state_secret(), body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{_b64url_encode(sig)}"


def _oauth_state_decode(state: str) -> dict[str, Any] | None:
    s = (state or "").strip()
    if "." not in s:
        return None
    body, sig = s.split(".", 1)
    if not body or not sig:
        return None
    expected = hmac.new(_oauth_state_secret(), body.encode("ascii"), hashlib.sha256).digest()
    try:
        provided = _b64url_decode(sig)
    except Exception:
        return None
    if not hmac.compare_digest(expected, provided):
        return None
    try:
        payload = json.loads(_b64url_decode(body).decode("utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _user_connection_row(*, user_id: str, key: str, db=None) -> UserConnection | None:
    normalized_key = str(key or "").strip()
    if not normalized_key:
        return None

    own_session = db is None
    session = db
    try:
        if session is None:
            session_ctx = DB.session()
            session = session_ctx.__enter__()
        else:
            session_ctx = None
        return session.scalar(
            select(UserConnection).where(
                UserConnection.user_id == str(user_id),
                UserConnection.key == normalized_key,
            )
        )
    finally:
        if own_session and session is not None:
            session_ctx.__exit__(None, None, None)


def _connection_meta(row: UserConnection | None) -> dict[str, Any]:
    data = getattr(row, "meta", None) if row is not None else None
    return dict(data) if isinstance(data, dict) else {}

def _gsc_oauth_connection_key(slug: str) -> str:
    safe_slug = re.sub(r"[^a-z0-9_.-]+", "-", (slug or "").strip().lower()).strip("-") or "project"
    prefix = "GSC_OAUTH:"
    if len(safe_slug) <= 80:
        return f"{prefix}{safe_slug}"
    digest = hashlib.sha1(safe_slug.encode("utf-8")).hexdigest()[:8]
    return f"{prefix}{safe_slug[:80]}:{digest}"


def _gsc_oauth_token_path(user_id: str, slug: str) -> Path:
    safe_user = re.sub(r"[^a-z0-9_.-]+", "-", (user_id or "").strip().lower()).strip("-") or "user"
    safe_slug = re.sub(r"[^a-z0-9_.-]+", "-", (slug or "").strip().lower()).strip("-") or "project"
    user_dir = (GSC_OAUTH_DIR / safe_user).resolve()
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir / f"{safe_slug}.json"


def _gsc_oauth_load(user_id: str, slug: str) -> dict[str, Any] | None:
    # Prefer DB storage (required for web/worker separation). Fall back to legacy on-disk token file.
    row = _user_connection_row(user_id=str(user_id), key=_gsc_oauth_connection_key(slug))
    if row is not None:
        stored = str(getattr(row, "secret_value", "") or "").strip()
        if stored:
            meta = _connection_meta(row)
            payload: dict[str, Any] = {
                "v": int(meta.get("v") or 1),
                "type": str(meta.get("type") or "google_oauth_refresh_token"),
                "scope": str(meta.get("scope") or "").strip(),
                "refresh_token": stored,
                "updated_at": str(meta.get("updated_at") or "").strip(),
                "_source": "db",
            }
            return payload

    # Legacy on-disk token (historically stored either under `gsc-oauth/<user>/` or directly in `gsc-oauth/`).
    user_path = _gsc_oauth_token_path(user_id, slug)
    root_path = (GSC_OAUTH_DIR / user_path.name).resolve()
    for path in [user_path, root_path]:
        if not path.exists() or not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        data["_source"] = "file"
        return data
    return None


def _gsc_oauth_refresh_token(user_id: str, slug: str) -> str | None:
    data = _gsc_oauth_load(user_id, slug)
    if not isinstance(data, dict):
        return None
    t = data.get("refresh_token")
    if not isinstance(t, str) or not t.strip():
        return None
    stored = t.strip()
    source = str(data.get("_source") or "").strip().lower() or "file"
    rotated = False
    try:
        value, rotated = _decrypt_secret_with_rotation(stored)
    except Exception:
        return None
    if not value:
        return None
    # Lazy-migrate:
    # - legacy on-disk tokens -> DB (needed for web/worker separation)
    # - plaintext/old-key tokens -> encrypted-at-rest with the current key
    needs_save = (source == "file") or (_encryption_ready() and (rotated or not stored.startswith(_SECRET_PREFIX)))
    if needs_save:
        try:
            scope = str(data.get("scope") or _GOOGLE_OAUTH_SCOPE).strip() or _GOOGLE_OAUTH_SCOPE
            _gsc_oauth_save(user_id, slug, refresh_token=value, scope=scope)
        except Exception:
            pass
    return value


def _gsc_oauth_connected(user_id: str, slug: str) -> bool:
    return bool(_gsc_oauth_refresh_token(user_id, slug))


def _gsc_oauth_save(user_id: str, slug: str, *, refresh_token: str, scope: str) -> None:
    meta = {
        "v": 1,
        "type": "google_oauth_refresh_token",
        "scope": str(scope or "").strip() or _GOOGLE_OAUTH_SCOPE,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    _upsert_user_connection(
        user_id=str(user_id),
        key=_gsc_oauth_connection_key(slug),
        value=str(refresh_token),
        meta=meta,
    )
    # Best-effort cleanup of legacy token file (if present).
    try:
        legacy_path = _gsc_oauth_token_path(user_id, slug)
        if legacy_path.exists():
            legacy_path.unlink()
        root_legacy = (GSC_OAUTH_DIR / legacy_path.name).resolve()
        if root_legacy.exists():
            root_legacy.unlink()
    except Exception:
        pass


def _gsc_oauth_clear(user_id: str, slug: str) -> None:
    _delete_user_connection(user_id=str(user_id), key=_gsc_oauth_connection_key(slug))
    path = _gsc_oauth_token_path(user_id, slug)
    root_path = (GSC_OAUTH_DIR / path.name).resolve()
    for p in [path, root_path]:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass


def _google_oauth_exchange_code(
    *,
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: str,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    data = resp.json()
    return data if isinstance(data, dict) else {}


def _google_oauth_refresh_access_token(
    *,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    timeout_s: float = 20.0,
) -> str:
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Invalid token response")
    token = str(data.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Missing access_token in token response")
    return token


def _google_oauth_revoke_token(token: str, *, timeout_s: float = 10.0) -> None:
    t = (token or "").strip()
    if not t:
        return
    try:
        requests.post("https://oauth2.googleapis.com/revoke", params={"token": t}, timeout=timeout_s)
    except Exception:
        return


def _github_oauth_exchange_code(
    *,
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: str,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    resp = requests.post(
        "https://github.com/login/oauth/access_token",
        headers={"Accept": "application/json"},
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        },
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Invalid GitHub token response")
    if data.get("error"):
        raise RuntimeError(str(data.get("error_description") or data.get("error") or "github_oauth_error"))
    return data


def _github_api_get(path: str, *, token: str, params: dict[str, Any] | None = None, timeout_s: float = 30.0) -> Any:
    resp = requests.get(
        f"https://api.github.com{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "seo-agent-web",
        },
        params=params or {},
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"GitHub JSON decode error: {e}") from e


def _netlify_api_get(path: str, *, token: str, params: dict[str, Any] | None = None, timeout_s: float = 30.0) -> Any:
    resp = requests.get(
        f"https://api.netlify.com{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params or {},
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"Netlify JSON decode error: {e}") from e


def _netlify_api_post(
    path: str,
    *,
    token: str,
    json_body: dict[str, Any] | None = None,
    timeout_s: float = 30.0,
) -> Any:
    resp = requests.post(
        f"https://api.netlify.com{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=(json_body or {}),
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(f"Netlify JSON decode error: {e}") from e


def _netlify_pat_name(*, user_id: str) -> str:
    safe_user = re.sub(r"[^a-z0-9]+", "-", str(user_id or "").strip().lower())[:24].strip("-") or "user"
    stamp = datetime.utcnow().strftime("%Y%m%d")
    return f"seo-agent-web-{safe_user}-{stamp}"


def _netlify_create_personal_access_token(*, oauth_token: str, user_id: str) -> tuple[str, dict[str, Any]]:
    payload = _netlify_api_post(
        "/api/v1/oauth/applications/create_token",
        token=oauth_token,
        json_body={
            "administrator_id": None,
            "expires_in": _NETLIFY_PAT_EXPIRES_IN_SECONDS,
            "grant_saml": False,
            "name": _netlify_pat_name(user_id=user_id),
        },
    )
    if not isinstance(payload, dict):
        raise RuntimeError("Réponse Netlify invalide pendant la création du PAT.")
    pat_token = str(payload.get("access_token") or payload.get("token") or payload.get("personal_access_token") or "").strip()
    if not pat_token:
        raise RuntimeError("PAT Netlify manquant dans la réponse OAuth.")
    return pat_token, payload


def _netlify_store_hardened_token(*, user_id: str, oauth_token: str) -> dict[str, Any]:
    pat_token, pat_payload = _netlify_create_personal_access_token(oauth_token=oauth_token, user_id=user_id)
    profile = _netlify_api_get("/api/v1/user", token=pat_token)
    created_at = datetime.utcnow()
    expires_at = created_at + timedelta(seconds=_NETLIFY_PAT_EXPIRES_IN_SECONDS)
    meta = {
        "auth_type": "oauth",
        "token_kind": "personal_access_token",
        "id": str(profile.get("id") or "").strip() if isinstance(profile, dict) else "",
        "full_name": str(profile.get("full_name") or "").strip() if isinstance(profile, dict) else "",
        "email": str(profile.get("email") or "").strip() if isinstance(profile, dict) else "",
        "avatar_url": str(profile.get("avatar_url") or "").strip() if isinstance(profile, dict) else "",
        "connected_at": created_at.isoformat(timespec="seconds") + "Z",
        "pat_name": str(pat_payload.get("name") or _netlify_pat_name(user_id=user_id)).strip(),
        "pat_created_at": created_at.isoformat(timespec="seconds") + "Z",
        "pat_expires_at": expires_at.isoformat(timespec="seconds") + "Z",
        "pat_expires_in": _NETLIFY_PAT_EXPIRES_IN_SECONDS,
    }
    _upsert_user_connection(user_id=str(user_id), key="NETLIFY_TOKEN", value=pat_token, meta=meta)
    return meta


def _netlify_store_oauth_token_fallback(*, user_id: str, oauth_token: str, upgrade_error: str | None = None) -> dict[str, Any]:
    profile = _netlify_api_get("/api/v1/user", token=oauth_token)
    meta = {
        "auth_type": "oauth",
        "token_kind": "oauth_access_token",
        "id": str(profile.get("id") or "").strip() if isinstance(profile, dict) else "",
        "full_name": str(profile.get("full_name") or "").strip() if isinstance(profile, dict) else "",
        "email": str(profile.get("email") or "").strip() if isinstance(profile, dict) else "",
        "avatar_url": str(profile.get("avatar_url") or "").strip() if isinstance(profile, dict) else "",
        "connected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    if upgrade_error:
        meta["pat_upgrade_error"] = str(upgrade_error)[:500]
    _upsert_user_connection(user_id=str(user_id), key="NETLIFY_TOKEN", value=oauth_token, meta=meta)
    return meta


def _ensure_hardened_netlify_connection(*, user_id: str, db=None) -> tuple[str, str]:
    token, source = _effective_user_connection_value(user_id=str(user_id), key="NETLIFY_TOKEN", db=db)
    if not token or source != "user":
        return token, source
    row = _user_connection_row(user_id=str(user_id), key="NETLIFY_TOKEN", db=db)
    meta = _connection_meta(row)
    auth_type = str(meta.get("auth_type") or "").strip().lower()
    token_kind = str(meta.get("token_kind") or "").strip().lower()
    if auth_type == "oauth" and token_kind in {"", "legacy", "oauth"}:
        try:
            _netlify_store_hardened_token(user_id=str(user_id), oauth_token=token)
        except Exception:
            _netlify_store_oauth_token_fallback(user_id=str(user_id), oauth_token=token)
        return _effective_user_connection_value(user_id=str(user_id), key="NETLIFY_TOKEN", db=db)
    return token, source


def _bing_oauth_exchange_code(
    *,
    code: str,
    redirect_uri: str,
    client_id: str,
    client_secret: str,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    resp = requests.post(
        "https://www.bing.com/webmasters/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Invalid Bing token response")
    if data.get("error"):
        raise RuntimeError(str(data.get("error_description") or data.get("error") or "bing_oauth_error"))
    return data


def _bing_oauth_refresh_token_data(
    *,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    resp = requests.post(
        "https://www.bing.com/webmasters/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=timeout_s,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {(resp.text or '').strip()[:400]}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Invalid Bing refresh response")
    if data.get("error"):
        raise RuntimeError(str(data.get("error_description") or data.get("error") or "bing_oauth_refresh_error"))
    return data


def _mask_secret(value: str | None) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if len(v) <= 4:
        return "••••"
    return f"••••{v[-4:]}"


def _mask_email(value: str | None) -> str:
    v = str(value or "").strip()
    if not v:
        return ""
    if "@" not in v:
        return _mask_secret(v)
    local, domain = v.split("@", 1)
    local = local.strip()
    domain = domain.strip()
    if not local or not domain:
        return _mask_secret(v)
    keep = 2 if len(local) >= 2 else 1
    return f"{local[:keep]}•••@{domain}"


def _env_target_path(key: str) -> Path:
    # Keep GSC creds in a dedicated file by default; everything else goes to `.env.local`.
    #
    # IMPORTANT: In production (Render), the repo directory can be ephemeral and/or read-only.
    # Persist UI-edited overrides under `DATA_DIR` (mounted disk) so values survive restarts.
    return (DATA_DIR / ".env.gsc") if key == "GOOGLE_APPLICATION_CREDENTIALS" else (DATA_DIR / ".env.local")


def _apply_effective_env(key: str) -> None:
    value, _src = _env_effective_value(key)
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value


def _resolve_path_under_root(raw: str, root: Path) -> Path:
    p = Path(raw).expanduser()
    if not p.is_absolute():
        p = (REPO_ROOT / p).resolve()
    else:
        p = p.resolve()
    root = root.resolve()
    if not p.is_relative_to(root):
        raise HTTPException(status_code=403, detail="Path not allowed")
    return p


_TITLE_RE = re.compile(r"(<title\b[^>]*>)(.*?)(</title>)", re.IGNORECASE | re.DOTALL)
_HEAD_OPEN_RE = re.compile(r"<head\b[^>]*>", re.IGNORECASE)
_HEAD_CLOSE_RE = re.compile(r"</head\s*>", re.IGNORECASE)
_META_DESC_TAG_RE = re.compile(r"<meta\b[^>]*\bname\s*=\s*(['\"])description\1[^>]*>", re.IGNORECASE)
_META_CONTENT_ATTR_RE = re.compile(r"(\bcontent\s*=\s*)(['\"])(.*?)(\2)", re.IGNORECASE | re.DOTALL)


def _normalize_title_text(value: str | None) -> str:
    v = html.unescape((value or "").strip())
    v = re.sub(r"\s+", " ", v).strip()
    return v


def _normalize_meta_text(value: str | None) -> str:
    v = html.unescape((value or "").strip())
    v = re.sub(r"\s+", " ", v).strip()
    return v


def _client_wants_json(request: Request) -> bool:
    accept = (request.headers.get("accept") or "").lower()
    if "application/json" in accept:
        return True
    xrw = (request.headers.get("x-requested-with") or "").lower()
    return xrw in {"xmlhttprequest", "fetch"}


def _safe_download_filename(value: str, *, fallback: str = "download") -> str:
    v = re.sub(r"[^A-Za-z0-9._-]+", "_", (value or "").strip()).strip("._-")
    return v or fallback


def _download_response(content: bytes, *, media_type: str, filename: str) -> Response:
    resp = Response(content=content, media_type=media_type)
    resp.headers["Content-Disposition"] = f'attachment; filename="{_safe_download_filename(filename)}"'
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _csv_bytes(rows: list[dict[str, Any]], *, fieldnames: list[str]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _pdf_escape_text(value: str) -> bytes:
    # PDF "literal string" escaping.
    s = (value or "").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return s.encode("latin-1", errors="replace")


def _text_to_pdf_bytes(
    text: str,
    *,
    title: str | None = None,
    page_width: float = 595.28,
    page_height: float = 841.89,
    margin_x: float = 48.0,
    margin_y: float = 54.0,
    font_size: int = 11,
    leading: int = 14,
    wrap_width: int = 100,
) -> bytes:
    """
    Minimal dependency-free PDF generator (text-only, Helvetica, WinAnsi).

    Not a full layout engine: wraps lines by character count and paginates.
    """

    lines: list[str] = []
    if title:
        lines.append(str(title))
        lines.append("")

    for raw in (text or "").splitlines():
        if not raw.strip():
            lines.append("")
            continue
        indent = re.match(r"^\s*", raw).group(0) if raw else ""
        wrapped = textwrap.wrap(
            raw.strip("\n"),
            width=max(20, int(wrap_width)),
            subsequent_indent=indent,
            break_long_words=True,
            break_on_hyphens=False,
        )
        lines.extend(wrapped if wrapped else [""])

    usable_height = max(1.0, page_height - (margin_y * 2))
    lines_per_page = max(1, int(usable_height // float(leading)))
    pages: list[list[str]] = []
    for i in range(0, len(lines), lines_per_page):
        pages.append(lines[i : i + lines_per_page])
    if not pages:
        pages = [[""]]

    def content_stream(page_lines: list[str]) -> bytes:
        start_x = margin_x
        start_y = page_height - margin_y - float(font_size)
        out = bytearray()
        out.extend(b"BT\n")
        out.extend(f"/F1 {font_size} Tf\n".encode("ascii"))
        out.extend(f"{leading} TL\n".encode("ascii"))
        out.extend(f"{start_x:.2f} {start_y:.2f} Td\n".encode("ascii"))
        for line in page_lines:
            out.extend(b"(")
            out.extend(_pdf_escape_text(line))
            out.extend(b") Tj\nT*\n")
        out.extend(b"ET\n")
        return bytes(out)

    # Build PDF objects.
    objects: list[bytes] = []

    def add_obj(payload: bytes) -> int:
        objects.append(payload)
        return len(objects)

    # 1) Catalog
    catalog_id = add_obj(b"<< /Type /Catalog /Pages 2 0 R >>")
    assert catalog_id == 1

    # 2) Pages root (filled later)
    pages_id = add_obj(b"")
    assert pages_id == 2

    # 3) Font
    font_id = add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")

    page_ids: list[int] = []
    content_ids: list[int] = []

    # Add page + content objects
    for page_lines in pages:
        stream = content_stream(page_lines)
        content = b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream)
        content_id = add_obj(content)
        content_ids.append(content_id)

        page = (
            b"<< /Type /Page /Parent 2 0 R "
            b"/MediaBox [0 0 %d %d] "
            b"/Resources << /Font << /F1 %d 0 R >> >> "
            b"/Contents %d 0 R >>"
            % (int(page_width), int(page_height), font_id, content_id)
        )
        page_id = add_obj(page)
        page_ids.append(page_id)

    kids = b"[ " + b" ".join(f"{pid} 0 R".encode("ascii") for pid in page_ids) + b" ]"
    pages_obj = b"<< /Type /Pages /Kids %s /Count %d >>" % (kids, len(page_ids))
    objects[pages_id - 1] = pages_obj

    # Assemble file with xref.
    out = bytearray()
    out.extend(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for idx, payload in enumerate(objects, start=1):
        offsets.append(len(out))
        out.extend(f"{idx} 0 obj\n".encode("ascii"))
        out.extend(payload)
        out.extend(b"\nendobj\n")

    xref_start = len(out)
    out.extend(f"xref\n0 {len(objects)+1}\n".encode("ascii"))
    out.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        out.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    out.extend(b"trailer\n")
    out.extend(f"<< /Size {len(objects)+1} /Root 1 0 R >>\n".encode("ascii"))
    out.extend(b"startxref\n")
    out.extend(f"{xref_start}\n".encode("ascii"))
    out.extend(b"%%EOF\n")
    return bytes(out)


_REPORTLAB_AVAILABLE: bool | None = None


def _reportlab_available() -> bool:
    global _REPORTLAB_AVAILABLE
    if _REPORTLAB_AVAILABLE is not None:
        return _REPORTLAB_AVAILABLE
    try:
        import reportlab  # noqa: F401

        _REPORTLAB_AVAILABLE = True
    except Exception:
        _REPORTLAB_AVAILABLE = False
    return _REPORTLAB_AVAILABLE


def _rl_escape(value: str) -> str:
    try:
        from xml.sax.saxutils import escape
    except Exception:
        return (value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return escape(value or "")


def _issue_fix_hint_lines(issue_key: str) -> list[str]:
    key = str(issue_key or "").strip().lower()
    if not key:
        return []

    exact: dict[str, list[str]] = {
        "meta_description_too_long_indexable": [
            "Raccourcir la meta description (≈ 70–160 caractères) en gardant le mot‑clé principal et une promesse claire.",
            "Éviter les répétitions et rendre la description unique par page.",
        ],
        "meta_description_too_short_indexable": [
            "Allonger la meta description (≈ 70–160 caractères) avec bénéfice + mot‑clé + CTA.",
            "Éviter les descriptions génériques (dupliquées).",
        ],
        "missing_meta_description": [
            "Ajouter une meta description unique (≈ 70–160 caractères) qui résume la page.",
        ],
        "title_too_long_indexable": [
            "Raccourcir le title (≈ 20–60 caractères) et placer le mot‑clé principal au début.",
            "Éviter les suffixes inutiles et doublons (marque répétée).",
        ],
        "title_too_short_indexable": [
            "Rendre le title plus descriptif (≈ 20–60 caractères) en incluant l’intention + mot‑clé.",
        ],
        "missing_title": [
            "Ajouter un title unique par page (≈ 20–60 caractères).",
        ],
        "low_word_count": [
            "Augmenter le contenu (objectif ≥ 200 mots utiles) : sections, FAQ, exemples, comparatifs.",
            "Vérifier que la page répond à l’intention de recherche (contenu réellement informatif).",
        ],
        "redirect_3xx": [
            "Mettre à jour les liens internes vers l’URL finale (éviter les 3xx dans le maillage).",
            "Vérifier canonical, sitemap et hreflang pour pointer directement vers la destination finale.",
        ],
        "redirect_chain": [
            "Réduire la chaîne de redirections (idéalement 1 saut max) en pointant vers la destination finale.",
            "Mettre à jour les liens internes/canonical/sitemap vers la destination finale.",
        ],
        "http_to_https_redirect": [
            "Forcer HTTPS (301) et mettre à jour les liens internes/canonical/sitemap en HTTPS.",
        ],
        "image_file_size_too_large": [
            "Compresser les images (WebP/AVIF) et servir la bonne taille (pas d’images surdimensionnées).",
            "Activer cache/CDN, lazy‑load, et définir width/height pour réduire les sauts de mise en page.",
        ],
        "structured_data_schema_org_validation_error": [
            "Corriger le JSON‑LD (champs requis, types/schema) et re‑valider (Schema.org validator).",
            "Vérifier que les valeurs (url, dates, auteur, image) sont au bon format.",
        ],
        "structured_data_google_rich_results_validation_error": [
            "Corriger les données structurées pour être éligible aux résultats enrichis (Rich Results Test).",
            "S’assurer que les propriétés requises sont présentes et cohérentes avec le contenu de la page.",
        ],
        "indexable_page_not_in_sitemap": [
            "Ajouter les pages indexables au sitemap XML et soumettre dans GSC.",
            "Vérifier que la page canonical est bien celle déclarée dans le sitemap.",
        ],
        "canonical_url_has_no_incoming_internal_links": [
            "Ajouter des liens internes vers ces pages (menu, catégories, articles connexes).",
            "Si la page ne doit pas être trouvable : noindex ou retirer des sitemaps/liens.",
        ],
        "orphan_pages": [
            "Créer du maillage interne vers ces pages (sections, navigation, pages connexes).",
            "Si inutiles : noindex / redirection / suppression.",
        ],
        "slow_page": [
            "Identifier le goulot (TTFB, LCP, JS) et optimiser : cache, compression, images, scripts.",
            "Vérifier Core Web Vitals et corriger les ressources lourdes (lazy‑load, code splitting).",
        ],
        "pages_with_poor_cls": [
            "Fixer les CLS : définir width/height, réserver l’espace, éviter l’injection tardive (bannières).",
            "Limiter les polices et chargements qui provoquent des décalages.",
        ],
    }

    if key in exact:
        return exact[key]

    # Heuristics for unseen keys
    if "meta_description" in key:
        return [
            "Rendre la meta description unique et cohérente avec le contenu de la page.",
            "Respecter une longueur raisonnable (≈ 70–160 caractères).",
        ]
    if "title" in key:
        return [
            "Rendre le title unique par page, descriptif et orienté intention.",
            "Respecter une longueur raisonnable (≈ 20–60 caractères).",
        ]
    if "hreflang" in key or "lang" in key:
        return [
            "Vérifier la cohérence hreflang (réciprocité, x-default, URLs canonicals) et corriger les liens.",
        ]
    if "canonical" in key:
        return [
            "Vérifier que le canonical pointe vers une URL 200 indexable et qu’il correspond à la version préférée.",
            "Mettre à jour les liens internes/sitemaps/hreflang pour éviter les incohérences.",
        ]
    if "sitemap" in key:
        return [
            "Mettre à jour le sitemap (uniquement URLs canonicals indexables) et re‑soumettre dans GSC.",
        ]
    if "redirect" in key or "3xx" in key:
        return [
            "Éviter les redirections dans le maillage interne : pointer vers l’URL finale.",
            "Limiter les chaînes/boucles et uniformiser HTTP/HTTPS et www/non‑www.",
        ]
    if "noindex" in key or "nofollow" in key or "robots" in key:
        return [
            "Vérifier les directives robots (meta robots / headers / robots.txt) et corriger si involontaires.",
        ]
    if "image" in key:
        return [
            "Optimiser les images (poids, dimensions, formats WebP/AVIF) et corriger les URLs cassées.",
        ]
    if key.startswith("structured_data"):
        return [
            "Corriger le balisage JSON‑LD et re‑valider (Schema.org + Rich Results).",
        ]

    meta = dash.issue_meta(key)
    if meta.category == "Content":
        return [
            "Optimiser le contenu : title, meta description, H1, et pertinence par rapport à l’intention.",
        ]
    if meta.category == "Redirects":
        return [
            "Corriger les redirections et mettre à jour les liens internes/canonicals/sitemaps.",
        ]
    if meta.category == "Indexability":
        return [
            "Corriger les signaux d’indexabilité (noindex/nofollow/canonical/robots) et re‑tester.",
        ]
    if meta.category == "Links":
        return [
            "Renforcer le maillage interne (liens entrants) et corriger les liens cassés.",
        ]
    if meta.category in {"Usability and performance", "Performance"}:
        return [
            "Analyser les métriques (CWV/PageSpeed) et optimiser les ressources (images/JS/CSS).",
        ]

    return ["Prioriser cette issue et vérifier manuellement les exemples (URLs) pour appliquer la correction adaptée."]


def _issue_fix_hint_text(issue_key: str) -> str:
    lines = _issue_fix_hint_lines(issue_key)
    return " | ".join(lines)


def _extract_urls_from_issue_examples(examples: list[Any], limit: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for ex in examples[: max(0, limit * 3)]:
        url: str | None = None
        if isinstance(ex, str):
            s = ex.strip()
            if "->" in s:
                left = s.split("->", 1)[0].strip()
                if left.startswith(("http://", "https://")):
                    url = left
            if not url:
                m = re.search(r"https?://\\S+", s)
                if m:
                    url = m.group(0).rstrip(").,;")
        elif isinstance(ex, dict):
            src = ex.get("source_url") or ex.get("source") or ex.get("url")
            if isinstance(src, str) and src.startswith(("http://", "https://")):
                url = src.strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
        if len(out) >= limit:
            break
    return out


def _issue_sample_urls_from_report(report: dict[str, Any] | None, issue_key: str, limit: int = 10) -> list[str]:
    if not report:
        return []
    issues = report.get("issues") if isinstance(report.get("issues"), dict) else {}
    block = issues.get(issue_key)
    if isinstance(block, dict) and isinstance(block.get("examples"), list):
        return _extract_urls_from_issue_examples(block.get("examples") or [], limit)

    # Fallback for duplicate groups: compute from pages list.
    pages = report.get("pages") if isinstance(report.get("pages"), list) else []
    if issue_key == "duplicate_titles":
        groups: dict[str, list[str]] = {}
        for p in pages:
            if not isinstance(p, dict):
                continue
            title = p.get("title")
            if not isinstance(title, str) or not title.strip():
                continue
            u = p.get("final_url") or p.get("url")
            if not isinstance(u, str) or not u.startswith(("http://", "https://")):
                continue
            groups.setdefault(title.strip(), []).append(u)
        urls: list[str] = []
        for _t, us in sorted(groups.items(), key=lambda it: len(it[1]), reverse=True):
            if len(us) < 2:
                continue
            for u in us:
                urls.append(u)
                if len(urls) >= limit:
                    return urls
        return urls

    if issue_key == "duplicate_meta_descriptions":
        groups = {}
        for p in pages:
            if not isinstance(p, dict):
                continue
            md = p.get("meta_description")
            if not isinstance(md, str) or not md.strip():
                continue
            u = p.get("final_url") or p.get("url")
            if not isinstance(u, str) or not u.startswith(("http://", "https://")):
                continue
            groups.setdefault(md.strip(), []).append(u)
        urls = []
        for _t, us in sorted(groups.items(), key=lambda it: len(it[1]), reverse=True):
            if len(us) < 2:
                continue
            for u in us:
                urls.append(u)
                if len(urls) >= limit:
                    return urls
        return urls

    return []


def _ai_reports_enabled() -> bool:
    flag = (os.environ.get("SEO_AUDIT_AI_REPORTS") or "").strip().lower()
    enabled = flag in {"1", "true", "yes", "on"}
    return enabled and bool((os.environ.get("OPENAI_API_KEY") or "").strip())


def _assistant_openai_configured() -> bool:
    return bool((os.environ.get("OPENAI_API_KEY") or "").strip())


def _assistant_gemini_configured() -> bool:
    return bool((os.environ.get("GOOGLE_GEMINI_API_KEY") or "").strip())


def _assistant_effective_provider() -> str:
    raw = (os.environ.get("SEO_AUDIT_ASSISTANT_PROVIDER") or "auto").strip().lower()
    if raw in {"openai", "gemini"}:
        return raw
    # auto: pick the first configured provider (Gemini tends to be cheaper).
    if _assistant_gemini_configured():
        return "gemini"
    if _assistant_openai_configured():
        return "openai"
    return "none"


def _assistant_model(provider: str) -> str:
    provider = (provider or "").strip().lower()
    if provider == "gemini":
        # Note: some API keys no longer expose Gemini 1.5 models (HTTP 404).
        # Default to a currently available Flash model; override via env var.
        return (os.environ.get("SEO_AUDIT_ASSISTANT_GEMINI_MODEL") or "gemini-2.0-flash-001").strip()
    if provider == "openai":
        return (
            os.environ.get("SEO_AUDIT_ASSISTANT_OPENAI_MODEL")
            or os.environ.get("OPENAI_CHAT_MODEL")
            or os.environ.get("OPENAI_MODEL")
            or "gpt-5.1-mini"
        ).strip()
    return ""


def _assistant_system_prompt(context: dict[str, Any] | None) -> str:
    ctx = context if isinstance(context, dict) else {}
    path = str(ctx.get("path") or "").strip()
    project = ctx.get("project") if isinstance(ctx.get("project"), dict) else {}
    slug = str(project.get("slug") or "").strip()
    site_name = str(project.get("site_name") or "").strip()
    base_url = str(project.get("base_url") or "").strip()

    extra: list[str] = []
    if path:
        extra.append(f"page={path}")
    if slug:
        extra.append(f"projet={slug}")
    if site_name:
        extra.append(f"site_name={site_name}")
    if base_url:
        extra.append(f"base_url={base_url}")
    extra_s = (" | ".join(extra)) if extra else "—"

    return (
        "Tu es l’assistant IA principal de l’app Agent SEO IA (SEO Audit). "
        "Tu aides l’utilisateur sur l’utilisation du produit (projets, audits, jobs, automation, réglages, exports), "
        "sur le SEO (technique, contenu, netlinking, analytics), ET sur des questions générales si besoin. "
        "Tu n’es pas limité au SEO.\n"
        "Connaissance produit (si pertinent):\n"
        "- Navigation: Projets, Jobs, Automation, Paramètres > Comptes & connexions.\n"
        "- Par projet: Overview, Paramètres crawl, Performance, Backlinks, All issues, Crawl log.\n"
        "- Intégrations possibles: Google Search Console (API), Bing, PageSpeed Insights, Ahrefs.\n"
        "Règles:\n"
        "- Réponds en français.\n"
        "- Ton professionnel, clair, utile.\n"
        "- Réponses courtes et actionnables (listes à puces quand pertinent).\n"
        "- Si la question sort du SEO / de l’app, répond quand même (ne dis pas que tu es “uniquement SEO”).\n"
        "- Si la demande nécessite des données en temps réel (météo, actualités, cours, etc.) ou un accès web, "
        "explique la limite et propose une alternative.\n"
        "- Ne demande jamais de clés API / secrets, et n’en révèle jamais.\n"
        "- Si une info manque, pose 1–2 questions maximum.\n"
        f"Contexte (best-effort): {extra_s}"
    )


def _assistant_clean_history(history: Any, *, max_items: int = 12) -> list[dict[str, str]]:
    if not isinstance(history, list):
        return []
    out: list[dict[str, str]] = []
    for raw in history[-max_items:]:
        if not isinstance(raw, dict):
            continue
        role = str(raw.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = raw.get("content")
        if not isinstance(content, str):
            continue
        content = content.strip()
        if not content:
            continue
        out.append({"role": role, "content": content[:2000]})
    return out


def _assistant_openai_chat(messages: list[dict[str, str]], *, model: str) -> str:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY manquante")

    base = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip().rstrip("/")
    payload: dict[str, Any] = {
        "model": model,
        "temperature": 0.3,
        "messages": messages,
    }

    resp = requests.post(
        f"{base}/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json=payload,
        timeout=90,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI HTTP {resp.status_code}")
    data = resp.json()

    content = None
    if isinstance(data, dict):
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") if isinstance(choices[0], dict) else None
            if isinstance(msg, dict):
                content = msg.get("content")
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("Réponse OpenAI vide")
    return content.strip()


def _assistant_gemini_chat(contents: list[dict[str, str]], *, system: str, model: str) -> str:
    api_key = (os.environ.get("GOOGLE_GEMINI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_GEMINI_API_KEY manquante")

    base = (os.environ.get("GOOGLE_GEMINI_BASE_URL") or "https://generativelanguage.googleapis.com").strip().rstrip("/")
    url = f"{base}/v1beta/models/{model}:generateContent"

    payload: dict[str, Any] = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [{"role": c["role"], "parts": [{"text": c["content"]}]} for c in contents],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 600},
    }

    resp = requests.post(url, params={"key": api_key}, json=payload, timeout=90)
    if resp.status_code != 200:
        msg = None
        try:
            err = resp.json()
            if isinstance(err, dict):
                e = err.get("error")
                if isinstance(e, dict) and isinstance(e.get("message"), str):
                    msg = e["message"].strip()
        except Exception:
            msg = None
        if msg:
            raise RuntimeError(f"Gemini HTTP {resp.status_code}: {msg}")
        raise RuntimeError(f"Gemini HTTP {resp.status_code}")
    data = resp.json()

    text = None
    if isinstance(data, dict):
        candidates = data.get("candidates")
        if isinstance(candidates, list) and candidates:
            content = candidates[0].get("content") if isinstance(candidates[0], dict) else None
            if isinstance(content, dict):
                parts = content.get("parts")
                if isinstance(parts, list) and parts:
                    text = parts[0].get("text") if isinstance(parts[0], dict) else None
    if not isinstance(text, str) or not text.strip():
        raise RuntimeError("Réponse Gemini vide")
    return text.strip()


def _ai_suggestions_path(runs_dir: Path, slug: str, ts: str) -> Path:
    return (runs_dir / slug / ts / "audit" / "ai_suggestions.json").resolve()


def _load_ai_suggestions(runs_dir: Path, slug: str, ts: str) -> dict[str, Any]:
    path = _ai_suggestions_path(runs_dir, slug, ts)
    if not path.exists():
        _ensure_runs_artifact_local(path)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    issues = data.get("issues") if isinstance(data, dict) else None
    return issues if isinstance(issues, dict) else {}


def _save_ai_suggestions(runs_dir: Path, slug: str, ts: str, issues: dict[str, Any], *, model: str) -> None:
    path = _ai_suggestions_path(runs_dir, slug, ts)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "model": model,
            "issues": issues,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _sync_runs_path_to_object_store(path)
    except Exception:
        # Best-effort cache only; never fail PDF generation because of it.
        return


def _fix_suggestions_path(runs_dir: Path, slug: str, ts: str) -> Path:
    return (runs_dir / slug / ts / "audit" / "fix-suggestions.json").resolve()


def _load_fix_suggestions_meta(runs_dir: Path, slug: str, ts: str) -> dict[str, Any] | None:
    path = _fix_suggestions_path(runs_dir, slug, ts)
    if not path.exists():
        _ensure_runs_artifact_local(path)
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    meta = data.get("meta") if isinstance(data, dict) else None
    return meta if isinstance(meta, dict) else None


def _load_fix_suggestion_for_issue(runs_dir: Path, slug: str, ts: str, issue_key: str) -> dict[str, Any] | None:
    path = _fix_suggestions_path(runs_dir, slug, ts)
    if not path.exists():
        _ensure_runs_artifact_local(path)
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    issues = data.get("issues") if isinstance(data, dict) else None
    if not isinstance(issues, dict):
        return None
    node = issues.get(issue_key)
    return node if isinstance(node, dict) else None


def _openai_generate_issue_suggestions(
    *,
    site_name: str,
    base_url: str,
    timestamp: str,
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return {}

    base = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip().rstrip("/")
    model = (os.environ.get("OPENAI_CHAT_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip()

    # Keep payload compact; the report itself holds full details (issue-level exports).
    cleaned: list[dict[str, Any]] = []
    for it in issues:
        cleaned.append(
            {
                "key": str(it.get("key") or ""),
                "label": str(it.get("label") or ""),
                "category": str(it.get("category") or ""),
                "severity": str(it.get("severity") or ""),
                "count": int(it.get("count") or 0),
                "sample_urls": [str(u) for u in (it.get("sample_urls") or [])][:5],
            }
        )

    system = (
        "Tu es un expert SEO technique. Pour chaque issue, propose une correction concrète et priorisée.\n"
        "Réponds STRICTEMENT en JSON, sans texte additionnel.\n"
        "Format attendu: {\"issues\": {\"<issue_key>\": {\"priority\": \"high|medium|low\", \"why\": \"...\", \"fix\": [\"...\"], \"verify\": [\"...\"]}}}.\n"
        "Contraintes: réponses courtes, actionnables, adaptées au contexte fourni, pas de blabla."
    )
    user = {
        "site": {"name": site_name, "base_url": base_url, "timestamp": timestamp},
        "issues": cleaned,
    }

    payload: dict[str, Any] = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(
            f"{base}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
            timeout=90,
        )
    except Exception:
        return {}

    if resp.status_code != 200:
        return {}
    try:
        data = resp.json()
    except Exception:
        return {}

    content = None
    if isinstance(data, dict):
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") if isinstance(choices[0], dict) else None
            if isinstance(msg, dict):
                content = msg.get("content")

    if not isinstance(content, str) or not content.strip():
        return {}

    try:
        parsed = json.loads(content)
    except Exception:
        return {}
    issues_out = parsed.get("issues") if isinstance(parsed, dict) else None
    return issues_out if isinstance(issues_out, dict) else {}


def _ensure_ai_suggestions_for_issues(
    *,
    runs_dir: Path,
    slug: str,
    ts: str,
    site_name: str,
    base_url: str,
    issues: list[dict[str, Any]],
    report: dict[str, Any] | None,
) -> dict[str, Any]:
    # Load cached suggestions and only generate missing ones.
    existing = _load_ai_suggestions(runs_dir, slug, ts) if slug and ts else {}
    if not issues:
        return existing

    missing: list[dict[str, Any]] = []
    for it in issues:
        key = str(it.get("key") or "")
        if not key or key in existing:
            continue
        sample_urls = _issue_sample_urls_from_report(report, key, limit=10)
        missing.append(
            {
                "key": key,
                "label": str(it.get("label") or ""),
                "category": str(it.get("category") or ""),
                "severity": str(it.get("severity") or ""),
                "count": int(it.get("count") or 0),
                "sample_urls": sample_urls,
            }
        )

    if not missing or not _ai_reports_enabled():
        return existing

    model = (os.environ.get("OPENAI_CHAT_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-4o-mini").strip()
    # Chunk to keep prompts small.
    for i in range(0, len(missing), 8):
        batch = missing[i : i + 8]
        generated = _openai_generate_issue_suggestions(
            site_name=site_name,
            base_url=base_url,
            timestamp=ts,
            issues=batch,
        )
        for k, v in generated.items():
            if isinstance(k, str) and isinstance(v, dict):
                existing[k] = v

    _save_ai_suggestions(runs_dir, slug, ts, existing, model=model)
    return existing


def _reportlab_build_pdf(
    story: list[Any],
    *,
    title: str,
    author: str = "SEO Audit",
    subject: str = "SEO report",
) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.platypus import SimpleDocTemplate

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=1.6 * cm,
        rightMargin=1.6 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
        title=title,
        author=author,
    )

    def _on_page(canvas, doc):  # type: ignore[no-redef]
        canvas.saveState()
        canvas.setTitle(title)
        canvas.setAuthor(author)
        canvas.setSubject(subject)

        canvas.setStrokeColor(colors.HexColor("#E5E7EB"))
        canvas.setLineWidth(0.6)
        canvas.line(doc.leftMargin, doc.bottomMargin - 6, doc.pagesize[0] - doc.rightMargin, doc.bottomMargin - 6)

        canvas.setFillColor(colors.HexColor("#6B7280"))
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            doc.pagesize[0] - doc.rightMargin,
            doc.bottomMargin - 18,
            f"Page {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    return buf.getvalue()


def _reportlab_project_report_pdf(runs_dir: Path, data: dict[str, Any]) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import LongTable, Paragraph, Spacer, TableStyle

    cur = data.get("current") if isinstance(data.get("current"), dict) else {}
    summary = cur.get("summary") if isinstance(cur.get("summary"), dict) else {}
    slug = str(data.get("slug") or "")
    site_name = str(data.get("site_name") or data.get("slug") or "")
    base_url = str(data.get("base_url") or summary.get("base_url") or "")
    ts = str(cur.get("timestamp") or "")
    report = dash.load_report_json(runs_dir, slug, ts) if slug and ts else None

    issues_dist = summary.get("issues_distribution") if isinstance(summary.get("issues_distribution"), dict) else {}
    issues_by_category = summary.get("issues_by_category") if isinstance(summary.get("issues_by_category"), dict) else {}
    top_issues = summary.get("top_issues") if isinstance(summary.get("top_issues"), list) else []

    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="ReportTitle",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=26,
            textColor=colors.HexColor("#111827"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="ReportSubtitle",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=colors.HexColor("#6B7280"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="H2",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=18,
            textColor=colors.HexColor("#111827"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="Small",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#374151"),
        )
    )
    styles.add(
        ParagraphStyle(
            name="Badge",
            parent=styles["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=9,
            leading=11,
            textColor=colors.HexColor("#111827"),
        )
    )

    story: list[Any] = []
    story.append(Paragraph("SEO Audit — Rapport", styles["ReportTitle"]))
    subtitle_bits = [site_name]
    if ts:
        subtitle_bits.append(ts)
    if base_url:
        subtitle_bits.append(base_url)
    story.append(Paragraph(" · ".join(_rl_escape(x) for x in subtitle_bits if x), styles["ReportSubtitle"]))
    story.append(Spacer(1, 12))

    # Summary table
    story.append(Paragraph("Résumé", styles["H2"]))
    health = int(summary.get("health_score") or 0)
    summary_rows = [
        ["Santé (Health score)", f"{health}%"],
        ["Pages crawled", str(int(summary.get("pages_crawled") or 0))],
        ["URLs discovered", str(int(summary.get("urls_discovered") or 0))],
        ["URLs uncrawled", str(int(summary.get("urls_uncrawled") or 0))],
        ["URLs with errors", str(int(summary.get("urls_with_errors") or 0))],
        ["Issues total", str(int(summary.get("issues_total") or 0))],
        ["Errors / Warnings / Notices", f"{int(issues_dist.get('error') or 0)} / {int(issues_dist.get('warning') or 0)} / {int(issues_dist.get('notice') or 0)}"],
    ]
    t = LongTable(summary_rows, colWidths=[210, 310])
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#111827")),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.HexColor("#F9FAFB"), colors.white]),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 14))

    # Issues by category
    if issues_by_category:
        story.append(Paragraph("Issues par catégorie", styles["H2"]))
        cat_rows = [["Catégorie", "Issues"]]
        for k, v in sorted(issues_by_category.items(), key=lambda it: int(it[1] or 0), reverse=True):
            cat_rows.append([str(k), str(int(v or 0))])
        ct = LongTable(cat_rows, colWidths=[340, 180], repeatRows=1)
        ct.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 10),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 10),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F9FAFB")]),
                    ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        story.append(ct)
        story.append(Spacer(1, 14))

    # Top issues
    story.append(Paragraph("Top issues", styles["H2"]))
    top_rows = [["Severity", "Catégorie", "Count", "Issue"]]
    sev_colors = {"error": "#B42318", "warning": "#B54708", "notice": "#175CD3"}
    for it in top_issues[:20]:
        sev = str(it.get("severity") or "")
        sev_color = sev_colors.get(sev, "#111827")
        sev_label = Paragraph(f'<font color="{sev_color}"><b>{_rl_escape(sev)}</b></font>', styles["Small"])
        issue_txt = f"{it.get('label') or ''}<br/><font color=\"#6B7280\">{_rl_escape(str(it.get('key') or ''))}</font>"
        top_rows.append(
            [
                sev_label,
                Paragraph(_rl_escape(str(it.get("category") or "")), styles["Small"]),
                Paragraph(f"<b>{int(it.get('count') or 0)}</b>", styles["Small"]),
                Paragraph(issue_txt, styles["Small"]),
            ]
        )
    tt = LongTable(top_rows, colWidths=[70, 140, 60, 250], repeatRows=1)
    tt.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (2, 1), (2, -1), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(tt)
    story.append(Spacer(1, 10))
    story.append(
        Paragraph(
            "Astuce: utilisez “All issues” pour filtrer par catégorie/severity, puis exportez un rapport dédié.",
            ParagraphStyle("Hint", parent=styles["Small"], textColor=colors.HexColor("#6B7280"), alignment=TA_LEFT),
        )
    )

    # Top issues details: URLs + recommendation
    if report and top_issues:
        story.append(Spacer(1, 16))
        story.append(Paragraph("Top issues — détails", styles["H2"]))
        story.append(
            Paragraph(
                "Ci‑dessous: échantillon des URLs impactées + recommandation. Pour le détail complet, exportez l’issue individuellement.",
                styles["ReportSubtitle"],
            )
        )
        story.append(Spacer(1, 10))

        h3 = ParagraphStyle(
            "H3Top",
            parent=styles["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=14,
            textColor=colors.HexColor("#111827"),
        )
        hint = ParagraphStyle(
            "HintTop",
            parent=styles["Small"],
            fontName="Helvetica",
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#374151"),
        )

        for it in top_issues[:8]:
            issue_key = str(it.get("key") or "")
            label = str(it.get("label") or issue_key)
            cat = str(it.get("category") or "")
            sev = str(it.get("severity") or "")
            sev_color = sev_colors.get(sev, "#111827")
            count = int(it.get("count") or 0)

            story.append(Paragraph(f"{_rl_escape(label)} <font color=\"#6B7280\">({count})</font>", h3))
            story.append(
                Paragraph(
                    f"<font color=\"{sev_color}\"><b>{_rl_escape(sev)}</b></font> · {_rl_escape(cat)} · <font color=\"#6B7280\">{_rl_escape(issue_key)}</font>",
                    styles["ReportSubtitle"],
                )
            )

            hint_lines = _issue_fix_hint_lines(issue_key)
            if hint_lines:
                hint_html = "<br/>".join(f"• {_rl_escape(line)}" for line in hint_lines)
                story.append(Paragraph(f"<b>Correction recommandée</b><br/>{hint_html}", hint))

            urls = _issue_sample_urls_from_report(report, issue_key, limit=10)
            if urls:
                url_rows: list[list[Any]] = [["URLs impactées (échantillon)"]]
                for u in urls:
                    cell = Paragraph(f'<link href="{_rl_escape(u)}">{_rl_escape(u)}</link>', styles["Small"])
                    url_rows.append([cell])
                ut = LongTable(url_rows, colWidths=[520], repeatRows=1)
                ut.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                            ("FONTSIZE", (0, 0), (-1, 0), 10),
                            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 8),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                            ("TOPPADDING", (0, 0), (-1, -1), 6),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                        ]
                    )
                )
                story.append(Spacer(1, 6))
                story.append(ut)

            story.append(Spacer(1, 12))

    # Keywords (GSC) - if present for this crawl
    gsc_dir = (runs_dir / slug / ts / "gsc").resolve() if slug and ts else None
    if gsc_dir:
        queries_csv = gsc_dir / "gsc-queries.csv"
        pages_csv = gsc_dir / "gsc-pages.csv"
        if queries_csv.exists() or pages_csv.exists():
            story.append(Spacer(1, 10))
            story.append(Paragraph("Mots-clés & opportunités (GSC)", styles["H2"]))
            story.append(
                Paragraph(
                    "Données réelles Google Search Console (si activé lors du crawl).",
                    styles["ReportSubtitle"],
                )
            )
            story.append(Spacer(1, 8))

            # Top queries
            if queries_csv.exists():
                rows = _read_gsc_csv_rows(queries_csv)
                rows.sort(key=lambda r: int(r.get("clicks") or 0), reverse=True)
                top = rows[:20]
                qtbl_rows: list[list[Any]] = [["Query", "Clicks", "Impr.", "CTR", "Pos."]]
                for r in top:
                    qtbl_rows.append(
                        [
                            Paragraph(_rl_escape(str(r.get("keyword") or "")), styles["Small"]),
                            Paragraph(str(int(r.get("clicks") or 0)), styles["Small"]),
                            Paragraph(str(int(r.get("impressions") or 0)), styles["Small"]),
                            Paragraph(f"{float(r.get('ctr') or 0.0):.2%}", styles["Small"]),
                            Paragraph(f"{float(r.get('position') or 0.0):.1f}", styles["Small"]),
                        ]
                    )
                qtbl = LongTable(qtbl_rows, colWidths=[255, 55, 60, 60, 50], repeatRows=1)
                qtbl.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                            ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                            ("FONTSIZE", (0, 0), (-1, 0), 10),
                            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 8),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                            ("TOPPADDING", (0, 0), (-1, -1), 6),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                        ]
                    )
                )
                story.append(Paragraph("Top queries", styles["Small"]))
                story.append(qtbl)
                story.append(Spacer(1, 10))

            # Pages opportunities
            if pages_csv.exists():
                rows = _read_gsc_csv_rows(pages_csv)
                rows.sort(key=lambda r: int(r.get("impressions") or 0), reverse=True)
                opp = [
                    r
                    for r in rows
                    if int(r.get("impressions") or 0) >= 100
                    and float(r.get("ctr") or 0.0) <= 0.01
                    and float(r.get("position") or 0.0) <= 20.0
                ][:20]
                if opp:
                    ptbl_rows: list[list[Any]] = [["Page", "Clicks", "Impr.", "CTR", "Pos.", "Action"]]
                    for r in opp:
                        page_url = str(r.get("keyword") or "")
                        ptbl_rows.append(
                            [
                                Paragraph(f'<link href="{_rl_escape(page_url)}">{_rl_escape(page_url)}</link>', styles["Small"]),
                                Paragraph(str(int(r.get("clicks") or 0)), styles["Small"]),
                                Paragraph(str(int(r.get("impressions") or 0)), styles["Small"]),
                                Paragraph(f"{float(r.get('ctr') or 0.0):.2%}", styles["Small"]),
                                Paragraph(f"{float(r.get('position') or 0.0):.1f}", styles["Small"]),
                                Paragraph(
                                    _rl_escape("Optimiser title/meta/H1 selon l’intention + enrichir contenu."),
                                    styles["Small"],
                                ),
                            ]
                        )
                    ptbl = LongTable(ptbl_rows, colWidths=[220, 45, 55, 50, 45, 105], repeatRows=1)
                    ptbl.setStyle(
                        TableStyle(
                            [
                                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                                ("FONTSIZE", (0, 0), (-1, 0), 10),
                                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                                ("ALIGN", (1, 1), (4, -1), "RIGHT"),
                                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                                ("TOPPADDING", (0, 0), (-1, -1), 6),
                                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                            ]
                        )
                    )
                    story.append(Paragraph("Pages à optimiser (impressions élevées, CTR faible)", styles["Small"]))
                    story.append(ptbl)
                    story.append(Spacer(1, 10))

            story.append(
                Paragraph(
                    "Recommandation: prioriser les pages à forte impression/CTR faible, puis aligner title/meta/H1 et enrichir le contenu.",
                    styles["ReportSubtitle"],
                )
            )

    # Backlinks (imports) - optional
    if slug and ts:
        run_dir = (runs_dir / slug / ts).resolve()
        imports_dir = run_dir / "backlinks"
        imports_raw = _load_backlinks_imports(imports_dir) if imports_dir.exists() else {}
        if imports_raw:
            story.append(Spacer(1, 10))
            story.append(Paragraph("Backlinks (imports)", styles["H2"]))
            story.append(
                Paragraph(
                    "Imports manuels (CSV) ou API (selon configuration). Analyse “backlinks nocifs” : bientôt disponible.",
                    styles["ReportSubtitle"],
                )
            )
            rows: list[list[Any]] = [["Source", "Domaines", "Pages", "Backlinks"]]
            for key, label in [("gsc", "GSC"), ("bing", "Bing"), ("ahrefs", "Ahrefs")]:
                ds = imports_raw.get(key, {}) if isinstance(imports_raw.get(key), dict) else {}
                domains_node = ds.get("domains", {}) if isinstance(ds.get("domains"), dict) else {}
                pages_node = ds.get("pages", {}) if isinstance(ds.get("pages"), dict) else {}
                backlinks_node = ds.get("backlinks", {}) if isinstance(ds.get("backlinks"), dict) else {}
                domains_rows = [r for r in (domains_node.get("rows") or []) if isinstance(r, dict)]
                pages_rows = [r for r in (pages_node.get("rows") or []) if isinstance(r, dict)]
                backlinks_rows = [r for r in (backlinks_node.get("rows") or []) if isinstance(r, dict)]
                rows.append([label, str(len(domains_rows)), str(len(pages_rows)), str(len(backlinks_rows))])
            bt = LongTable(rows, colWidths=[140, 120, 120, 140], repeatRows=1)
            bt.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ]
                )
            )
            story.append(bt)

    pdf_title = f"SEO Audit Report - {site_name} - {ts}"
    return _reportlab_build_pdf(story, title=pdf_title, subject="SEO Audit report")


def _reportlab_issues_pdf(
    runs_dir: Path,
    data: dict[str, Any],
    issues_filtered: list[dict[str, Any]],
    *,
    severity: str | None,
    category: str | None,
    q: str | None,
) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import LongTable, Paragraph, Spacer, TableStyle

    cur = data.get("current") if isinstance(data.get("current"), dict) else {}
    ts = str(cur.get("timestamp") or "")
    slug = str(data.get("slug") or "")
    site_name = str(data.get("site_name") or data.get("slug") or "")
    base_url = str(data.get("base_url") or "")
    report = dash.load_report_json(runs_dir, slug, ts) if slug and ts else None
    ai_map = _ensure_ai_suggestions_for_issues(
        runs_dir=runs_dir,
        slug=slug,
        ts=ts,
        site_name=site_name,
        base_url=base_url,
        issues=issues_filtered,
        report=report,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title2",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#111827"),
    )
    meta_style = ParagraphStyle(
        "Meta",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#6B7280"),
    )
    cell_style = ParagraphStyle(
        "Cell",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#111827"),
    )
    h2_style = ParagraphStyle(
        "H2Issues",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=16,
        textColor=colors.HexColor("#111827"),
    )
    h3_style = ParagraphStyle(
        "H3Issues",
        parent=styles["Heading3"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#111827"),
    )
    hint_style = ParagraphStyle(
        "HintIssues",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#374151"),
    )

    story: list[Any] = []
    story.append(Paragraph("Issues — Rapport", title_style))
    story.append(Paragraph(_rl_escape(f"{site_name} · {ts} · {base_url}"), meta_style))

    filters: list[str] = []
    if severity:
        filters.append(f"severity={severity}")
    if category:
        filters.append(f"category={category}")
    if q:
        filters.append(f"q={q}")
    if filters:
        story.append(Paragraph("Filtres: " + _rl_escape(", ".join(filters)), meta_style))
    story.append(Spacer(1, 12))

    sev_colors = {"error": "#B42318", "warning": "#B54708", "notice": "#175CD3"}
    rows: list[list[Any]] = [["Severity", "Catégorie", "Count", "Δ", "Issue"]]
    for it in issues_filtered:
        sev = str(it.get("severity") or "")
        sev_color = sev_colors.get(sev, "#111827")
        change = it.get("change")
        change_txt = "—"
        if change is not None:
            try:
                c = int(change)
                change_txt = f"{c:+d}"
            except Exception:
                change_txt = str(change)

        issue_txt = f"{it.get('label') or ''}<br/><font color=\"#6B7280\">{_rl_escape(str(it.get('key') or ''))}</font>"
        rows.append(
            [
                Paragraph(f'<font color="{sev_color}"><b>{_rl_escape(sev)}</b></font>', cell_style),
                Paragraph(_rl_escape(str(it.get("category") or "")), cell_style),
                Paragraph(f"<b>{int(it.get('count') or 0)}</b>", cell_style),
                Paragraph(_rl_escape(change_txt), cell_style),
                Paragraph(issue_txt, cell_style),
            ]
        )

    tbl = LongTable(rows, colWidths=[70, 140, 55, 45, 240], repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (2, 1), (3, -1), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(tbl)

    story.append(Spacer(1, 14))
    story.append(Paragraph("Détails & corrections", h2_style))
    story.append(
        Paragraph(
            "Pour chaque issue : URLs impactées (échantillon) + recommandation. Pour le détail complet, exportez l’issue individuellement.",
            meta_style,
        )
    )
    story.append(Spacer(1, 10))

    sev_colors = {"error": "#B42318", "warning": "#B54708", "notice": "#175CD3"}
    for it in issues_filtered:
        issue_key = str(it.get("key") or "")
        label = str(it.get("label") or issue_key)
        cat = str(it.get("category") or "")
        sev = str(it.get("severity") or "")
        sev_color = sev_colors.get(sev, "#111827")
        count = int(it.get("count") or 0)

        story.append(Paragraph(f"{_rl_escape(label)} <font color=\"#6B7280\">({count})</font>", h3_style))
        story.append(
            Paragraph(
                f"<font color=\"{sev_color}\"><b>{_rl_escape(sev)}</b></font> · {_rl_escape(cat)} · <font color=\"#6B7280\">{_rl_escape(issue_key)}</font>",
                meta_style,
            )
        )

        hint_lines = _issue_fix_hint_lines(issue_key)
        if hint_lines:
            hint_html = "<br/>".join(f"• {_rl_escape(line)}" for line in hint_lines)
            story.append(Paragraph(f"<b>Correction recommandée</b><br/>{hint_html}", hint_style))

        ai = ai_map.get(issue_key) if isinstance(ai_map, dict) else None
        if isinstance(ai, dict):
            why = str(ai.get("why") or "").strip()
            fix = ai.get("fix") if isinstance(ai.get("fix"), list) else []
            verify = ai.get("verify") if isinstance(ai.get("verify"), list) else []
            priority = str(ai.get("priority") or "").strip().lower()
            if why or fix or verify:
                pr = f" · priorité: {priority}" if priority else ""
                story.append(Spacer(1, 4))
                if why:
                    story.append(Paragraph(f"<b>Suggestion IA</b>{_rl_escape(pr)}<br/>{_rl_escape(why)}", hint_style))
                if fix:
                    fix_html = "<br/>".join(f"• {_rl_escape(str(x))}" for x in fix[:6] if str(x).strip())
                    if fix_html:
                        story.append(Paragraph(f"<b>Actions</b><br/>{fix_html}", hint_style))
                if verify:
                    ver_html = "<br/>".join(f"• {_rl_escape(str(x))}" for x in verify[:4] if str(x).strip())
                    if ver_html:
                        story.append(Paragraph(f"<b>Vérification</b><br/>{ver_html}", hint_style))

        urls = _issue_sample_urls_from_report(report, issue_key, limit=12)
        if urls:
            url_rows: list[list[Any]] = [["URLs impactées (échantillon)"]]
            for u in urls:
                cell = Paragraph(f'<link href="{_rl_escape(u)}">{_rl_escape(u)}</link>', cell_style)
                url_rows.append([cell])
            ut = LongTable(url_rows, colWidths=[510], repeatRows=1)
            ut.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ]
                )
            )
            story.append(Spacer(1, 6))
            story.append(ut)

        story.append(Spacer(1, 12))

    pdf_title = f"Issues Report - {site_name} - {ts}"
    return _reportlab_build_pdf(story, title=pdf_title, subject="SEO issues report")


def _split_issue_example(ex: Any) -> tuple[str, str]:
    if isinstance(ex, dict):
        src = ex.get("source_url") or ex.get("source") or ex.get("url") or ""
        details = json.dumps(ex, ensure_ascii=False)
        return (str(src or "").strip(), details)
    if isinstance(ex, list):
        return ("", json.dumps(ex, ensure_ascii=False))
    s = str(ex or "").strip()
    if not s:
        return ("", "")
    if "->" in s:
        left, right = s.split("->", 1)
        return (left.strip(), right.strip())
    if s.startswith(("http://", "https://")) and " " in s:
        url, tail = s.split(" ", 1)
        return (url.strip(), tail.strip())
    if s.startswith(("http://", "https://")):
        return (s, "")
    return ("", s)


def _reportlab_issue_detail_pdf(runs_dir: Path, data: dict[str, Any]) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import LongTable, Paragraph, Spacer, TableStyle

    issue = data.get("issue") if isinstance(data.get("issue"), dict) else {}
    issue_key = str(issue.get("key") or "")
    label = str(issue.get("label") or issue_key)
    category = str(issue.get("category") or "")
    severity = str(issue.get("severity") or "")
    count = int(issue.get("count") or 0)

    slug = str(data.get("slug") or "")
    ts = str(data.get("timestamp") or "")
    run = dash.load_run_json(runs_dir, slug, ts) if slug and ts else {}
    site_name = str(run.get("site_name") or slug)
    base_url = str(run.get("base_url") or "")

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title3",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#111827"),
    )
    meta_style = ParagraphStyle(
        "Meta2",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=colors.HexColor("#6B7280"),
    )
    cell_style = ParagraphStyle(
        "Cell2",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#111827"),
    )

    story: list[Any] = []
    story.append(Paragraph("Détail issue", title_style))
    story.append(Paragraph(_rl_escape(f"{site_name} · {ts} · {base_url}"), meta_style))
    story.append(Spacer(1, 12))

    sev_colors = {"error": "#B42318", "warning": "#B54708", "notice": "#175CD3"}
    sev_color = sev_colors.get(severity, "#111827")
    meta_rows = [
        ["Issue", Paragraph(_rl_escape(label), cell_style)],
        ["Key", Paragraph(f"<font color=\"#6B7280\">{_rl_escape(issue_key)}</font>", cell_style)],
        ["Category", Paragraph(_rl_escape(category), cell_style)],
        ["Severity", Paragraph(f'<font color="{sev_color}"><b>{_rl_escape(severity)}</b></font>', cell_style)],
        ["Count", Paragraph(f"<b>{count}</b>", cell_style)],
    ]
    mt = LongTable(meta_rows, colWidths=[90, 430])
    mt.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.HexColor("#F9FAFB"), colors.white]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    story.append(mt)
    story.append(Spacer(1, 14))

    hint_lines = _issue_fix_hint_lines(issue_key)
    if hint_lines:
        hint_html = "<br/>".join(f"• {_rl_escape(line)}" for line in hint_lines)
        story.append(Paragraph("Correction recommandée", styles["Heading2"]))
        story.append(Paragraph(hint_html, meta_style))
        story.append(Spacer(1, 12))

    story.append(Paragraph("Exemples", styles["Heading2"]))

    cwv = issue.get("cwv") if isinstance(issue.get("cwv"), dict) else None
    if cwv and isinstance(cwv.get("rows"), list):
        metric = str(cwv.get("metric") or "")
        story.append(Paragraph(_rl_escape(f"Core Web Vitals — {metric}"), meta_style))
        story.append(Spacer(1, 8))

        rows: list[list[Any]] = [["URL", "Valeur", "Statut", "Source"]]
        for r in cwv.get("rows") or []:
            if not isinstance(r, dict):
                continue
            url = str(r.get("url") or "")
            value = r.get("value")
            status = str(r.get("category") or "")
            source = str(r.get("source") or "")
            url_cell = Paragraph(f'<link href="{_rl_escape(url)}">{_rl_escape(url)}</link>', cell_style) if url else Paragraph("—", cell_style)
            rows.append([url_cell, Paragraph(_rl_escape(str(value)), cell_style), Paragraph(_rl_escape(status), cell_style), Paragraph(_rl_escape(source), cell_style)])

        tbl = LongTable(rows, colWidths=[270, 70, 70, 110], repeatRows=1)
        tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 10),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        story.append(tbl)
    else:
        examples = issue.get("examples") if isinstance(issue.get("examples"), list) else []
        if not examples:
            story.append(Paragraph("Aucun exemple disponible.", meta_style))
        else:
            rows = [["URL", "Détails"]]
            for ex in examples:
                url, details = _split_issue_example(ex)
                url_cell = Paragraph(f'<link href="{_rl_escape(url)}">{_rl_escape(url)}</link>', cell_style) if url.startswith(("http://", "https://")) else Paragraph(_rl_escape(url), cell_style)
                rows.append([url_cell, Paragraph(_rl_escape(details), cell_style)])
            tbl = LongTable(rows, colWidths=[260, 260], repeatRows=1)
            tbl.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F3F4F6")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111827")),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 10),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("TOPPADDING", (0, 0), (-1, -1), 6),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ]
                )
            )
            story.append(tbl)

    pdf_title = f"Issue Detail - {site_name} - {issue_key} - {ts}"
    return _reportlab_build_pdf(story, title=pdf_title, subject="SEO issue detail")


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")


def _normalize_base_url(value: str) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if not re.match(r"^https?://", raw, re.IGNORECASE):
        raw = "https://" + raw
    parts = urlsplit(raw)
    host = (parts.hostname or "").strip().lower()
    if not host:
        return None
    scheme = (parts.scheme or "https").strip().lower()
    if scheme not in {"http", "https"}:
        scheme = "https"
    netloc = host
    if parts.port:
        netloc = f"{host}:{parts.port}"
    # Use root as crawl base_url (Ahrefs-like).
    return urlunsplit((scheme, netloc, "/", "", ""))


def _root_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _resolve_repo_path(raw: str) -> Path | None:
    value = str(raw or "").strip().strip('"').strip("'")
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path


def _load_gsc_fetch_module() -> Any:
    global _GSC_FETCH_MODULE
    if _GSC_FETCH_MODULE is not None:
        return _GSC_FETCH_MODULE

    module_path = (AUTOPILOT_SCRIPTS_DIR / "gsc_fetch.py").resolve()
    if not module_path.exists():
        raise RuntimeError(f"Module introuvable: {module_path}")

    spec = importlib.util.spec_from_file_location("seo_agent_gsc_fetch", str(module_path))
    if not spec or not spec.loader:
        raise RuntimeError("Impossible de charger gsc_fetch.py")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _GSC_FETCH_MODULE = module
    return module


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _to_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _timeseries_totals(points: list[dict[str, Any]]) -> dict[str, Any]:
    clicks = sum(_to_int(p.get("clicks")) for p in points if isinstance(p, dict))
    impressions = sum(_to_int(p.get("impressions")) for p in points if isinstance(p, dict))
    ctr = (clicks / impressions) if impressions else 0.0

    weighted_positions: list[tuple[float, int]] = []
    fallback_positions: list[float] = []
    for p in points:
        if not isinstance(p, dict):
            continue
        pos = _to_float(p.get("position"))
        if pos <= 0:
            continue
        impr = _to_int(p.get("impressions"))
        if impr > 0:
            weighted_positions.append((pos, impr))
        else:
            fallback_positions.append(pos)

    avg_position = 0.0
    if weighted_positions:
        total_weight = sum(weight for _, weight in weighted_positions)
        if total_weight > 0:
            avg_position = sum(pos * weight for pos, weight in weighted_positions) / total_weight
    elif fallback_positions:
        avg_position = sum(fallback_positions) / len(fallback_positions)

    return {
        "clicks": clicks,
        "impressions": impressions,
        "avg_ctr": ctr,
        "avg_position": avg_position,
    }


def _gsc_property_candidates(base_url: str, configured: str | None) -> list[str]:
    candidates: list[str] = []
    if isinstance(configured, str) and configured.strip():
        candidates.append(configured.strip())

    parts = urlsplit(base_url)
    scheme = (parts.scheme or "https").strip().lower() or "https"
    host = (parts.hostname or "").strip().lower()
    netloc = (parts.netloc or "").strip().lower()
    host_no_www = host[4:] if host.startswith("www.") else host
    host_www = host if host.startswith("www.") else (f"www.{host}" if host else "")
    if host_no_www:
        candidates.append(f"sc-domain:{host_no_www}")

    # URL-prefix properties require an exact match (scheme + host + trailing slash).
    if netloc:
        root = urlunsplit((scheme, netloc, "", "", "")).strip()
        if root:
            candidates.append(root if root.endswith("/") else f"{root}/")

        if host_www and host_no_www and host_www != host_no_www:
            alt_netloc = host_www
            if parts.port:
                alt_netloc = f"{alt_netloc}:{parts.port}"
            alt_root = urlunsplit((scheme, alt_netloc, "", "", "")).strip()
            if alt_root:
                candidates.append(alt_root if alt_root.endswith("/") else f"{alt_root}/")

        # Also try the opposite scheme (some sites are still registered as http:// in GSC).
        alt_scheme = "http" if scheme == "https" else "https"
        alt_root2 = urlunsplit((alt_scheme, netloc, "", "", "")).strip()
        if alt_root2:
            candidates.append(alt_root2 if alt_root2.endswith("/") else f"{alt_root2}/")

    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)
    return out


def _gsc_daily_series(rows: list[dict[str, Any]], *, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys = row.get("keys") if isinstance(row.get("keys"), list) else []
        key = str(keys[0]) if keys else ""
        if key:
            by_date[key] = row

    out: list[dict[str, Any]] = []
    cur = start_date
    while cur <= end_date:
        key = cur.isoformat()
        row = by_date.get(key) or {}
        clicks = _to_int(row.get("clicks"))
        impressions = _to_int(row.get("impressions"))
        out.append(
            {
                "date": key,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": _to_float(row.get("ctr")),
                "position": _to_float(row.get("position")),
            }
        )
        cur = cur + dt.timedelta(days=1)
    return out


def _gsc_live_credentials_path(*, user_id: str, slug: str) -> tuple[Path | None, str]:
    oauth_path = _gsc_oauth_token_path(user_id, slug)
    if oauth_path.exists():
        return oauth_path, "oauth"

    env_creds = _resolve_repo_path(_safe_env("GOOGLE_APPLICATION_CREDENTIALS"))
    if env_creds and env_creds.exists():
        return env_creds, "service_account"

    return None, ""


def _fetch_gsc_live_series(*, user_id: str, slug: str, base_url: str, gsc_cfg: dict[str, Any], days: int) -> dict[str, Any]:
    enabled = bool(gsc_cfg.get("enabled")) if "enabled" in gsc_cfg else True
    if not enabled:
        return {"ok": False, "enabled": False, "reason": "disabled"}

    credentials_path, auth_mode = _gsc_live_credentials_path(user_id=user_id, slug=slug)
    if not credentials_path:
        return {"ok": False, "enabled": True, "reason": "missing_credentials"}

    gsc_fetch = _load_gsc_fetch_module()

    today = dt.datetime.now(dt.timezone.utc).date()
    end_date = today - dt.timedelta(days=3)
    if end_date < dt.date(2000, 1, 1):
        end_date = today
    days = max(1, min(int(days or 28), 365))
    start_date = end_date - dt.timedelta(days=days - 1)
    search_type = str(gsc_cfg.get("search_type") or "web").strip() or "web"

    last_error = ""
    for property_url in _gsc_property_candidates(base_url, str(gsc_cfg.get("property_url") or "").strip()):
        try:
            rows = gsc_fetch.fetch_gsc(
                credentials_path=credentials_path.resolve(),
                property_url=property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=["date"],
                search_type=search_type,
                row_limit=max(500, days + 10),
                timeout_s=30.0,
            )
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            continue

        daily = _gsc_daily_series(rows if isinstance(rows, list) else [], start_date=start_date, end_date=end_date)
        return {
            "ok": True,
            "enabled": True,
            "source": "gsc",
            "live": True,
            "auth_mode": auth_mode,
            "property": property_url,
            "days": days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": daily,
            "totals": _timeseries_totals(daily),
            "data_delay_hint": "GSC a généralement 48–72h de décalage.",
        }

    return {
        "ok": False,
        "enabled": True,
        "source": "gsc",
        "reason": "request_failed",
        "error": last_error or "gsc_request_failed",
    }


def _bing_site_candidates(base_url: str, configured: str | None) -> list[str]:
    candidates: list[str] = []
    if isinstance(configured, str) and configured.strip():
        candidates.append(configured.strip())
    root = _root_url(base_url).strip()
    if root:
        candidates.append(root if root.endswith("/") else f"{root}/")
        candidates.append(root.rstrip("/"))

    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)
    return out


def _bing_extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("d", "Data", "data", "Result", "result", "Results", "results"):
        node = payload.get(key)
        if isinstance(node, list):
            return [row for row in node if isinstance(row, dict)]
        if isinstance(node, dict):
            for value in node.values():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    return [row for row in value if isinstance(row, dict)]
    for value in payload.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [row for row in value if isinstance(row, dict)]
    return []


def _bing_date_iso(value: Any) -> str:
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return ""
        match = re.search(r"Date\((\d+)([+-]\d+)?\)", raw)
        if match:
            try:
                ms = int(match.group(1))
                return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).date().isoformat()
            except Exception:
                return ""
        try:
            return dt.date.fromisoformat(raw).isoformat()
        except Exception:
            return ""
    if isinstance(value, (int, float)) and float(value) > 0:
        try:
            ts = float(value)
            if ts > 1e12:
                ts = ts / 1000.0
            return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).date().isoformat()
        except Exception:
            return ""
    return ""


def _bing_rank_traffic_series(rows: list[dict[str, Any]], *, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        day = _bing_date_iso(row.get("Date") or row.get("date") or "")
        if not day:
            continue
        by_date[day] = {
            "clicks": _to_int(row.get("Clicks") if "Clicks" in row else row.get("clicks")),
            "impressions": _to_int(row.get("Impressions") if "Impressions" in row else row.get("impressions")),
        }

    available_dates: list[dt.date] = []
    for key in by_date.keys():
        try:
            available_dates.append(dt.date.fromisoformat(key))
        except Exception:
            continue

    effective_start = start_date
    effective_end = end_date
    if available_dates:
        effective_start = max(start_date, min(available_dates))
        effective_end = min(end_date, max(available_dates))
    if effective_end < effective_start:
        return []

    out: list[dict[str, Any]] = []
    cur = effective_start
    while cur <= effective_end:
        key = cur.isoformat()
        node = by_date.get(key) or {}
        clicks = _to_int(node.get("clicks"))
        impressions = _to_int(node.get("impressions"))
        out.append(
            {
                "date": key,
                "clicks": clicks,
                "impressions": impressions,
                "ctr": (clicks / impressions) if impressions else 0.0,
                "position": 0.0,
            }
        )
        cur = cur + dt.timedelta(days=1)
    return out


def _bing_call(
    method: str,
    *,
    params: dict[str, Any],
    timeout_s: float,
    api_key: str = "",
    access_token: str = "",
) -> Any:
    request_params = dict(params or {})
    headers: dict[str, str] = {}
    token = str(access_token or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        key = str(api_key or "").strip()
        if not key:
            raise RuntimeError("bing_credentials_missing")
        request_params["apikey"] = key
    response = requests.get(
        f"https://www.bing.com/webmaster/api.svc/json/{method}",
        params=request_params,
        headers=headers,
        timeout=timeout_s,
    )
    content_type = response.headers.get("content-type", "")
    if not content_type.startswith("application/json"):
        raise RuntimeError(f"Non-JSON response for {method} (HTTP {response.status_code})")
    data = response.json()
    if isinstance(data, dict) and isinstance(data.get("ErrorCode"), int) and int(data.get("ErrorCode")) != 0:
        raise RuntimeError(str(data.get("Message") or f"bing_api_error:{data.get('ErrorCode')}"))
    return data


def _bing_pick_site_url(
    *,
    base_url: str,
    timeout_s: float,
    configured: str | None = None,
    api_key: str = "",
    access_token: str = "",
) -> tuple[str | None, list[str], str | None]:
    try:
        payload = _bing_call("GetUserSites", params={}, timeout_s=timeout_s, api_key=api_key, access_token=access_token)
    except Exception as e:
        return None, [], f"{type(e).__name__}: {e}"

    rows = _bing_extract_rows(payload)
    sites: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("Url", "url", "SiteUrl", "siteUrl", "site_url"):
            value = row.get(key)
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                sites.append(value.strip())
                break

    if not sites:
        blob = json.dumps(payload, ensure_ascii=False)
        sites = [site for site in re.findall(r"https?://[^\s\"\\\\]+", blob) if site.startswith(("http://", "https://"))]

    candidates = {candidate.rstrip("/").lower() for candidate in _bing_site_candidates(base_url, configured)}
    host = (urlsplit(base_url).hostname or "").strip().lower()
    host_no_www = host[4:] if host.startswith("www.") else host

    def score(site_url: str) -> tuple[int, int]:
        root = _root_url(site_url).rstrip("/").lower()
        site_host = (urlsplit(site_url).hostname or "").lower()
        points = 0
        if root in candidates:
            points += 3
        if site_host == host:
            points += 2
        if host_no_www and site_host == host_no_www:
            points += 2
        if site_url.endswith("/"):
            points += 1
        return points, len(site_url)

    best = sorted(sites, key=lambda site: (-score(site)[0], score(site)[1]))[0] if sites else None
    return best, sites, None


def _fetch_bing_live_series(*, user_id: str, base_url: str, bing_cfg: dict[str, Any], days: int) -> dict[str, Any]:
    enabled = bool(bing_cfg.get("enabled")) if "enabled" in bing_cfg else False
    if not enabled:
        return {"ok": False, "enabled": False, "reason": "disabled"}

    auth = _effective_bing_connection(user_id=str(user_id))
    token = str(auth.get("token") or "").strip()
    if not token:
        return {"ok": False, "enabled": True, "source": "bing", "reason": "missing_credentials"}

    timeout_s = 20.0
    configured_site_url = str(bing_cfg.get("site_url") or "").strip()
    site_url = configured_site_url or ""
    user_sites: list[str] = []
    if not site_url:
        site_url, user_sites, sites_error = _bing_pick_site_url(
            base_url=base_url,
            timeout_s=timeout_s,
            configured=configured_site_url,
            api_key=(token if auth.get("mode") == "api_key" else ""),
            access_token=(token if auth.get("mode") == "oauth" else ""),
        )
        if not site_url:
            return {
                "ok": False,
                "enabled": True,
                "source": "bing",
                "reason": "site_not_found",
                "error": sites_error or "bing_site_not_found",
                "user_sites": user_sites[:50],
                "auth_mode": str(auth.get("mode") or ""),
            }

    today = dt.datetime.now(dt.timezone.utc).date()
    end_date = today - dt.timedelta(days=3)
    days = max(1, min(int(days or 28), 365))
    start_date = end_date - dt.timedelta(days=days - 1)

    try:
        payload = _bing_call(
            "GetRankAndTrafficStats",
            params={"siteUrl": site_url},
            timeout_s=timeout_s,
            api_key=(token if auth.get("mode") == "api_key" else ""),
            access_token=(token if auth.get("mode") == "oauth" else ""),
        )
        rows = _bing_extract_rows(payload)
    except Exception as e:
        return {
            "ok": False,
            "enabled": True,
            "source": "bing",
            "reason": "request_failed",
            "error": f"{type(e).__name__}: {e}",
            "auth_mode": str(auth.get("mode") or ""),
        }

    daily = _bing_rank_traffic_series(rows, start_date=start_date, end_date=end_date)
    if not daily:
        return {
            "ok": False,
            "enabled": True,
            "source": "bing",
            "reason": "no_data",
            "site_url": site_url,
            "days": days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "auth_mode": str(auth.get("mode") or ""),
        }

    return {
        "ok": True,
        "enabled": True,
        "source": "bing",
        "live": True,
        "auth_source": str(auth.get("source") or ""),
        "auth_mode": str(auth.get("mode") or ""),
        "site_url": site_url,
        "days": days,
        "start_date": daily[0]["date"],
        "end_date": daily[-1]["date"],
        "daily": daily,
        "totals": _timeseries_totals(daily),
        "data_delay_hint": "Bing Webmaster Tools peut avoir un léger décalage.",
        "source_label": str(auth.get("source_label") or "bing"),
    }


def _to_ctr_fraction(value: Any) -> float:
    """
    Normalize CTR values to a 0..1 fraction.
    Bing APIs may return CTR either as fraction or percent (string/number).
    """
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        v = float(value)
        if v < 0:
            return 0.0
        if v <= 1.0:
            return v
        if v <= 100.0:
            return v / 100.0
        return 0.0
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return 0.0
        if s.endswith("%"):
            s = s[:-1].strip()
        try:
            return _to_ctr_fraction(float(s))
        except Exception:
            return 0.0
    return 0.0


def _gsc_rows_to_perf_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        keys = row.get("keys") if isinstance(row.get("keys"), list) else []
        key = str(keys[0]) if keys else ""
        if not key:
            continue
        items.append(
            {
                "keyword": key,
                "clicks": _to_int(row.get("clicks")),
                "impressions": _to_int(row.get("impressions")),
                "ctr": _to_float(row.get("ctr")),
                "position": _to_float(row.get("position")),
            }
        )
    return items


def _bing_rows_to_perf_items(rows: list[dict[str, Any]], *, dim: str) -> list[dict[str, Any]]:
    def pick(row: dict[str, Any], keys: list[str]) -> Any:
        for k in keys:
            if k in row:
                return row.get(k)
        lookup = {str(k).lower(): k for k in row.keys()}
        for k in keys:
            k2 = lookup.get(k.lower())
            if k2 is not None:
                return row.get(k2)
        return None

    items: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if dim == "query":
            key = str(pick(row, ["Query", "query", "Keyword", "keyword"]) or "").strip()
        else:
            key = str(pick(row, ["Page", "page", "Url", "url", "URL"]) or "").strip()
        if not key:
            continue
        clicks = _to_int(pick(row, ["Clicks", "clicks"]))
        impressions = _to_int(pick(row, ["Impressions", "impressions"]))
        ctr_raw = pick(row, ["Ctr", "ctr", "CTR"])
        ctr = _to_ctr_fraction(ctr_raw) if ctr_raw is not None else ((clicks / impressions) if impressions else 0.0)
        position = _to_float(pick(row, ["AvgPosition", "AveragePosition", "Position", "position"]))
        items.append({"keyword": key, "clicks": clicks, "impressions": impressions, "ctr": ctr, "position": position})
    return items


def _fetch_gsc_live_items(
    *,
    user_id: str,
    slug: str,
    base_url: str,
    gsc_cfg: dict[str, Any],
    days: int,
    dim: str,
    limit: int,
) -> dict[str, Any]:
    enabled = bool(gsc_cfg.get("enabled")) if "enabled" in gsc_cfg else True
    if not enabled:
        return {"ok": False, "enabled": False, "source": "gsc", "reason": "disabled"}

    credentials_path, auth_mode = _gsc_live_credentials_path(user_id=user_id, slug=slug)
    if not credentials_path:
        return {"ok": False, "enabled": True, "source": "gsc", "reason": "missing_credentials"}

    dimension = (dim or "query").strip().lower()
    if dimension not in {"query", "page"}:
        dimension = "query"

    today = dt.datetime.now(dt.timezone.utc).date()
    end_date = today - dt.timedelta(days=3)
    if end_date < dt.date(2000, 1, 1):
        end_date = today
    days = max(1, min(int(days or 28), 365))
    start_date = end_date - dt.timedelta(days=days - 1)
    search_type = str(gsc_cfg.get("search_type") or "web").strip() or "web"
    min_impressions = max(0, int(gsc_cfg.get("min_impressions") or 0))

    gsc_fetch = _load_gsc_fetch_module()

    fetch_limit = min(25000, max(500, int(limit or 200) * 10))
    last_error = ""
    for property_url in _gsc_property_candidates(base_url, str(gsc_cfg.get("property_url") or "").strip()):
        try:
            rows = gsc_fetch.fetch_gsc(
                credentials_path=credentials_path.resolve(),
                property_url=property_url,
                start_date=start_date,
                end_date=end_date,
                dimensions=[dimension],
                search_type=search_type,
                row_limit=fetch_limit,
                timeout_s=30.0,
            )
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            continue

        items = _gsc_rows_to_perf_items(rows if isinstance(rows, list) else [])
        if min_impressions:
            items = [it for it in items if _to_int(it.get("impressions")) >= min_impressions]
        items.sort(key=lambda r: (-_to_int(r.get("clicks")), -_to_int(r.get("impressions"))))
        if limit and limit > 0:
            items = items[: int(limit)]
        return {
            "ok": True,
            "enabled": True,
            "source": "gsc",
            "live": True,
            "auth_mode": auth_mode,
            "dim": dimension,
            "property": property_url,
            "days": days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "min_impressions": min_impressions,
            "items": items,
            "totals": _timeseries_totals(items),
            "data_delay_hint": "GSC a généralement 48–72h de décalage.",
        }

    return {
        "ok": False,
        "enabled": True,
        "source": "gsc",
        "reason": "request_failed",
        "error": last_error or "gsc_request_failed",
    }


def _fetch_bing_live_items(
    *,
    user_id: str,
    base_url: str,
    bing_cfg: dict[str, Any],
    days: int,
    dim: str,
    limit: int,
) -> dict[str, Any]:
    enabled = bool(bing_cfg.get("enabled")) if "enabled" in bing_cfg else False
    if not enabled:
        return {"ok": False, "enabled": False, "source": "bing", "reason": "disabled"}

    auth = _effective_bing_connection(user_id=str(user_id))
    token = str(auth.get("token") or "").strip()
    if not token:
        return {"ok": False, "enabled": True, "source": "bing", "reason": "missing_credentials"}

    dimension = (dim or "query").strip().lower()
    if dimension not in {"query", "page"}:
        dimension = "query"

    timeout_s = 20.0
    configured_site_url = str(bing_cfg.get("site_url") or "").strip()
    site_url = configured_site_url or ""
    user_sites: list[str] = []
    if not site_url:
        site_url, user_sites, sites_error = _bing_pick_site_url(
            base_url=base_url,
            timeout_s=timeout_s,
            configured=configured_site_url,
            api_key=(token if auth.get("mode") == "api_key" else ""),
            access_token=(token if auth.get("mode") == "oauth" else ""),
        )
        if not site_url:
            return {
                "ok": False,
                "enabled": True,
                "source": "bing",
                "reason": "site_not_found",
                "error": sites_error or "bing_site_not_found",
                "user_sites": user_sites[:50],
                "auth_mode": str(auth.get("mode") or ""),
            }

    today = dt.datetime.now(dt.timezone.utc).date()
    end_date = today - dt.timedelta(days=3)
    days = max(1, min(int(days or 28), 365))
    start_date = end_date - dt.timedelta(days=days - 1)
    min_impressions = max(0, int(bing_cfg.get("min_impressions") or 0))

    method = "GetQueryStats" if dimension == "query" else "GetPageStats"
    try:
        payload = _bing_call(
            method,
            params={"siteUrl": site_url, "startDate": start_date.isoformat(), "endDate": end_date.isoformat()},
            timeout_s=timeout_s,
            api_key=(token if auth.get("mode") == "api_key" else ""),
            access_token=(token if auth.get("mode") == "oauth" else ""),
        )
        rows = _bing_extract_rows(payload)
    except Exception as e:
        return {
            "ok": False,
            "enabled": True,
            "source": "bing",
            "reason": "request_failed",
            "error": f"{type(e).__name__}: {e}",
            "auth_mode": str(auth.get("mode") or ""),
        }

    items = _bing_rows_to_perf_items(rows, dim=dimension)
    if min_impressions:
        items = [it for it in items if _to_int(it.get("impressions")) >= min_impressions]
    items.sort(key=lambda r: (-_to_int(r.get("clicks")), -_to_int(r.get("impressions"))))
    if limit and limit > 0:
        items = items[: int(limit)]

    return {
        "ok": True,
        "enabled": True,
        "source": "bing",
        "live": True,
        "auth_source": str(auth.get("source") or ""),
        "auth_mode": str(auth.get("mode") or ""),
        "dim": dimension,
        "site_url": site_url,
        "days": days,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "min_impressions": min_impressions,
        "items": items,
        "totals": _timeseries_totals(items),
        "data_delay_hint": "Bing Webmaster Tools peut avoir un léger décalage.",
        "source_label": str(auth.get("source_label") or "bing"),
    }


def _validate_public_crawl_target(base_url: str) -> str | None:
    """
    Guardrail for a public SaaS: refuse obvious SSRF targets.

    - Only http/https (already normalized upstream)
    - Only ports 80/443 (Ahrefs-like)
    - Block localhost, .local and private/reserved IP ranges (including DNS resolving to them)
    """
    allow_private = str(os.environ.get("SEO_AGENT_ALLOW_PRIVATE_HOSTS") or "").strip().lower() in {"1", "true", "yes"}
    parts = urlsplit(base_url or "")
    host = (parts.hostname or "").strip().lower()
    if not host:
        return "URL invalide (host manquant)."

    port = parts.port
    if port and port not in {80, 443}:
        return "Port non autorisé (80/443 uniquement)."

    if host in {"localhost"} or host.endswith(".localhost"):
        return "Host non autorisé (localhost)."
    if host.endswith(".local") or host.endswith(".localdomain"):
        return "Host non autorisé (.local)."

    try:
        ipaddress.ip_address(host)
        return "Host non autorisé (adresse IP)."
    except ValueError:
        pass

    if allow_private:
        return None

    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        # DNS failure => crawl will likely fail anyway, but it's not an SSRF vector.
        return None

    ips: set[str] = set()
    for it in infos:
        try:
            sockaddr = it[4]
            if isinstance(sockaddr, tuple) and sockaddr:
                ips.add(str(sockaddr[0]))
        except Exception:
            continue

    for ip_s in sorted(ips):
        try:
            ip = ipaddress.ip_address(ip_s)
        except ValueError:
            continue
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_multicast
            or ip.is_reserved
            or ip.is_unspecified
        ):
            return f"Host non autorisé (IP non publique: {ip_s})."

    return None


def _slug_from_base_url(base_url: str) -> str | None:
    parts = urlsplit(base_url)
    host = (parts.hostname or "").strip().lower()
    if not host:
        return None
    if host.startswith("www."):
        host = host[4:]
    return _slugify(host) or None


def _load_projects_registry() -> dict[str, dict[str, Any]]:
    if not PROJECTS_PATH.exists():
        return {}
    try:
        data = json.loads(PROJECTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if isinstance(data, dict) and isinstance(data.get("projects"), dict):
        out: dict[str, dict[str, Any]] = {}
        for slug, node in data["projects"].items():
            if not isinstance(slug, str) or not isinstance(node, dict):
                continue
            base_url = str(node.get("base_url") or "").strip()
            site_name = str(node.get("site_name") or "").strip()
            if not base_url:
                continue
            out[slug] = {"base_url": base_url, "site_name": site_name}
            crawl = node.get("crawl")
            if isinstance(crawl, dict):
                out[slug]["crawl"] = crawl
            gsc_api = node.get("gsc_api")
            if isinstance(gsc_api, dict):
                out[slug]["gsc_api"] = gsc_api
            bing = node.get("bing")
            if isinstance(bing, dict):
                out[slug]["bing"] = bing
        return out
    return {}


def _save_projects_registry(projects: dict[str, dict[str, Any]]) -> None:
    PROJECTS_PATH.write_text(
        json.dumps({"projects": projects}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _upsert_project(*, base_url: str, site_name: str | None = None) -> str | None:
    base = _normalize_base_url(base_url)
    if not base:
        return None
    slug = _slug_from_base_url(base)
    if not slug:
        return None
    name = (site_name or "").strip()
    if not name:
        name = urlsplit(base).hostname or slug

    with _PROJECTS_LOCK:
        reg = _load_projects_registry()
        existing = reg.get(slug) if isinstance(reg.get(slug), dict) else {}
        reg[slug] = {**existing, "base_url": base, "site_name": name}
        _save_projects_registry(reg)
    return slug


templates = Jinja2Templates(directory=str(REPO_ROOT / "seo-agent-web" / "templates"))


def _db_project(user_id: str, slug: str) -> Project | None:
    s = (slug or "").strip()
    u = (user_id or "").strip()
    if not s or not u:
        return None
    with DB.session() as db:
        return db.scalar(select(Project).where(Project.owner_user_id == u, Project.slug == s))


def _db_project_lookup_by_base_url(user_id: str) -> dict[str, str]:
    out: dict[str, str] = {}
    u = (user_id or "").strip()
    if not u:
        return out
    with DB.session() as db:
        projects = list(db.scalars(select(Project).where(Project.owner_user_id == u)))
    for project in projects:
        base = _normalize_base_url(str(getattr(project, "base_url", "") or ""))
        slug = str(getattr(project, "slug", "") or "").strip()
        if base and slug:
            out[base] = slug
    return out


def _db_upsert_project(*, user_id: str, base_url: str, site_name: str | None = None) -> str | None:
    base = _normalize_base_url(base_url)
    if not base:
        return None
    slug = _slug_from_base_url(base)
    if not slug:
        return None
    name = (site_name or "").strip()
    if not name:
        name = urlsplit(base).hostname or slug
    with DB.session() as db:
        existing = db.scalar(select(Project).where(Project.owner_user_id == str(user_id), Project.slug == slug))
        if existing:
            existing.base_url = base
            existing.site_name = name
            current_settings = existing.settings if isinstance(existing.settings, dict) else {}
            meta = _project_meta(current_settings)
            if not meta:
                existing.settings = {**current_settings, "_meta": {"created_via": "ui", "hide_from_connections": False}}
            db.add(existing)
            db.commit()
            return slug
        proj = Project(
            owner_user_id=str(user_id),
            slug=slug,
            base_url=base,
            site_name=name,
            settings={"_meta": {"created_via": "ui", "hide_from_connections": False}},
        )
        db.add(proj)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            return None
        return slug


def _import_legacy_projects_for_user(user_id: str) -> int:
    reg = _load_projects_registry()
    if not reg:
        return 0
    imported = 0
    touched_existing = 0
    with DB.session() as db:
        for slug, node in reg.items():
            if not isinstance(slug, str) or not isinstance(node, dict):
                continue
            base = _normalize_base_url(str(node.get("base_url") or ""))
            if not base:
                continue
            slug_final = _slug_from_base_url(base) or _slugify(slug) or ""
            if not slug_final:
                continue
            site_name = str(node.get("site_name") or slug_final).strip() or slug_final

            existing = db.scalar(
                select(Project).where(Project.owner_user_id == str(user_id), Project.slug == slug_final)
            )
            if existing:
                current_settings = existing.settings if isinstance(existing.settings, dict) else {}
                meta = _project_meta(current_settings)
                has_legacy_payload = any(isinstance(node.get(name), dict) for name in ("crawl", "gsc_api", "bing"))
                if (not meta) and has_legacy_payload:
                    existing.settings = {
                        **current_settings,
                        "_meta": {"import_source": "legacy_registry", "hide_from_connections": True},
                    }
                    db.add(existing)
                    touched_existing += 1
                continue

            settings: dict[str, Any] = {}
            crawl = node.get("crawl")
            if isinstance(crawl, dict):
                settings["crawl"] = crawl
            gsc_api = node.get("gsc_api")
            if isinstance(gsc_api, dict):
                settings["gsc_api"] = gsc_api
            bing = node.get("bing")
            if isinstance(bing, dict):
                settings["bing"] = bing
            settings["_meta"] = {"import_source": "legacy_registry", "hide_from_connections": True}

            db.add(
                Project(
                    owner_user_id=str(user_id),
                    slug=slug_final,
                    base_url=base,
                    site_name=site_name,
                    settings=settings,
                )
            )
            imported += 1
        if imported or touched_existing:
            db.commit()
    return imported


def _migrate_legacy_runs_for_user(user_id: str) -> int:
    src_root = DEFAULT_RUNS_DIR
    if not src_root.exists() or not src_root.is_dir():
        return 0
    dst_root = _runs_dir_for_user(user_id)
    uuid_re = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
    moved = 0
    for p in src_root.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if not name or name.startswith("_"):
            continue
        if uuid_re.match(name):
            continue
        dest = dst_root / name
        if dest.exists():
            continue
        try:
            shutil.move(str(p), str(dest))
            moved += 1
        except Exception:
            continue
    return moved


def _migrate_legacy_gsc_oauth_for_user(user_id: str) -> int:
    if not GSC_OAUTH_DIR.exists() or not GSC_OAUTH_DIR.is_dir():
        return 0
    moved = 0
    for p in GSC_OAUTH_DIR.glob("*.json"):
        if not p.is_file():
            continue
        slug = p.stem
        dest = _gsc_oauth_token_path(user_id, slug)
        if dest.exists():
            continue
        try:
            shutil.move(str(p), str(dest))
            moved += 1
        except Exception:
            continue
    return moved


def _db_project_or_404(request: Request, slug: str) -> Project:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    proj = _db_project(user.id, slug)
    if not proj:
        raise HTTPException(status_code=404, detail="project_not_found")
    return proj

def _norm_header(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _inventory_preview(config_path: Path, max_preview: int = 10) -> dict[str, Any] | None:
    try:
        cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(cfg, dict):
        return None

    inv = cfg.get("inventory") if isinstance(cfg.get("inventory"), dict) else None
    if not inv:
        return None

    domains_csv = inv.get("domains_csv")
    if not isinstance(domains_csv, str) or not domains_csv.strip():
        return None

    delimiter = str(inv.get("delimiter") or ";")
    preferred_col = inv.get("domain_column") if isinstance(inv.get("domain_column"), str) else None

    csv_path = Path(domains_csv).expanduser()
    if not csv_path.is_absolute():
        csv_path = (config_path.parent / csv_path).resolve()

    if not csv_path.exists():
        return {"path": str(csv_path), "exists": False, "count": 0, "preview": []}

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            headers = list(reader.fieldnames or [])
            if not headers:
                return {"path": str(csv_path), "exists": True, "count": 0, "preview": [], "error": "CSV has no headers"}

            domain_col = headers[0]
            if preferred_col:
                preferred_norm = _norm_header(preferred_col)
                for h in headers:
                    if _norm_header(h) == preferred_norm:
                        domain_col = h
                        break

            preview: list[str] = []
            count = 0
            for row in reader:
                d = str(row.get(domain_col) or "").strip()
                if not d:
                    continue
                count += 1
                if len(preview) < max_preview:
                    preview.append(d)

        return {"path": str(csv_path), "exists": True, "count": count, "domain_column": domain_col, "preview": preview}
    except Exception as e:
        return {"path": str(csv_path), "exists": True, "count": 0, "preview": [], "error": f"{type(e).__name__}: {e}"}


def _load_latest_global_summary(runs_dir: Path) -> dict[str, Any] | None:
    global_dir = runs_dir / "_global"
    if not global_dir.exists():
        return None
    timestamps = sorted([p.name for p in global_dir.iterdir() if p.is_dir()])
    if not timestamps:
        return None
    latest = global_dir / timestamps[-1]
    md = latest / "sites-summary.md"
    inter_md = latest / "interlinking-plan.md"
    return {
        "timestamp": timestamps[-1],
        "dir": latest,
        "sites_summary_md": md if md.exists() else None,
        "interlinking_md": inter_md if inter_md.exists() else None,
    }


@dataclass
class Job:
    id: str
    status: str  # queued | running | done | failed
    created_at: float
    updated_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    pid: int | None = None
    config_path: str | None = None
    command: list[str] | None = None
    returncode: int | None = None
    stdout: str | None = None
    stderr: str | None = None
    progress: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    attempts: int = 0
    max_attempts: int = 1
    run_after: float | None = None
    worker_id: str | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=2)


def _job_path(job_id: str) -> Path:
    # Legacy (file-based jobs). Still used as a fallback import path for older deployments.
    return JOBS_DIR / f"{job_id}.json"


def _save_job(job: Job) -> None:
    lock = _job_lock(job.id)
    with lock:
        now = time.time()
        if job.updated_at is None:
            job.updated_at = now
        else:
            job.updated_at = now

        result = job.result if isinstance(job.result, dict) else {}
        owner_id = str(result.get("user_id") or "").strip()
        slug = str(result.get("slug") or "").strip()
        kind = str(result.get("type") or "").strip().lower()
        if not kind:
            kind = _job_kind_from_command(job.command) or ""

        with DB.session() as db:
            row = db.get(JobRecord, str(job.id))
            if row is None:
                if not owner_id:
                    # Cannot create a DB job without an owner (FK). Keep it in-memory only.
                    return
                row = JobRecord(
                    id=str(job.id),
                    owner_user_id=owner_id,
                    slug=slug,
                    kind=kind,
                    status=str(job.status),
                    created_at=float(job.created_at),
                    updated_at=float(job.updated_at or now),
                    started_at=job.started_at,
                    finished_at=job.finished_at,
                    pid=job.pid,
                    config_path=job.config_path,
                    command=job.command,
                    returncode=job.returncode,
                    stdout=job.stdout,
                    stderr=job.stderr,
                    progress=job.progress,
                    result=result if isinstance(result, dict) else None,
                    attempts=int(job.attempts or 0),
                    max_attempts=int(job.max_attempts or 1),
                    run_after=job.run_after,
                    worker_id=job.worker_id,
                )
                db.add(row)
                db.commit()
                return

            # Update existing row.
            if owner_id:
                row.owner_user_id = owner_id
            if slug:
                row.slug = slug
            if kind:
                row.kind = kind
            row.status = str(job.status)
            row.updated_at = float(job.updated_at or now)
            row.created_at = float(job.created_at)
            row.started_at = job.started_at
            row.finished_at = job.finished_at
            row.pid = job.pid
            row.config_path = job.config_path
            row.command = job.command
            row.returncode = job.returncode
            row.stdout = job.stdout
            row.stderr = job.stderr
            row.progress = job.progress
            row.result = result if isinstance(result, dict) else None
            row.attempts = int(job.attempts or 0)
            row.max_attempts = int(job.max_attempts or 1)
            row.run_after = job.run_after
            row.worker_id = job.worker_id
            db.add(row)
            db.commit()


def _load_job(job_id: str) -> Job | None:
    jid = str(job_id or "").strip()
    if not jid:
        return None
    with DB.session() as db:
        row = db.get(JobRecord, jid)
        if row:
            return Job(
                id=str(row.id),
                status=str(row.status),
                created_at=float(row.created_at),
                updated_at=float(row.updated_at) if row.updated_at is not None else None,
                started_at=row.started_at,
                finished_at=row.finished_at,
                pid=row.pid,
                config_path=row.config_path,
                command=row.command,
                returncode=row.returncode,
                stdout=row.stdout,
                stderr=row.stderr,
                progress=row.progress,
                result=row.result,
                attempts=int(row.attempts or 0),
                max_attempts=int(row.max_attempts or 1),
                run_after=row.run_after,
                worker_id=row.worker_id,
            )

    # Legacy fallback (older deployments).
    path = _job_path(jid)
    if not path.exists():
        return None
    lock = _job_lock(jid)
    with lock:
        for attempt in range(3):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                job = Job(**data)
                try:
                    _save_job(job)
                except Exception:
                    pass
                return job
            except json.JSONDecodeError:
                if attempt == 2:
                    return None
                time.sleep(0.02 * (attempt + 1))
            except Exception:
                return None


def _list_jobs(limit: int = 25) -> list[Job]:
    with DB.session() as db:
        rows = list(db.scalars(select(JobRecord).order_by(JobRecord.created_at.desc()).limit(int(limit))))

    jobs: list[Job] = []
    for row in rows:
        job = Job(
            id=str(row.id),
            status=str(row.status),
            created_at=float(row.created_at),
            updated_at=float(row.updated_at) if row.updated_at is not None else None,
            started_at=row.started_at,
            finished_at=row.finished_at,
            pid=row.pid,
            config_path=row.config_path,
            command=row.command,
            returncode=row.returncode,
            stdout=row.stdout,
            stderr=row.stderr,
            progress=row.progress,
            result=row.result,
            attempts=int(row.attempts or 0),
            max_attempts=int(row.max_attempts or 1),
            run_after=row.run_after,
            worker_id=row.worker_id,
        )
        _finalize_stale_job(job)
        jobs.append(job)
    return jobs


_WORKER_STOP = threading.Event()
_WORKER_STARTED_GUARD = threading.Lock()
_WORKER_STARTED = False
_WORKER_THREADS: list[threading.Thread] = []


def _worker_enabled() -> bool:
    return not _env_bool("SEO_AGENT_DISABLE_WORKER")


def _worker_concurrency() -> int:
    raw = str(os.environ.get("SEO_AGENT_WORKER_CONCURRENCY") or "").strip()
    try:
        v = int(raw) if raw else 1
    except Exception:
        v = 1
    return max(1, min(4, v))


def _claim_next_job_id(*, worker_id: str) -> str | None:
    now = time.time()
    with DB.session() as db:
        q = (
            select(JobRecord.id)
            .where(JobRecord.status == "queued")
            .where((JobRecord.run_after == None) | (JobRecord.run_after <= now))  # noqa: E711
            .order_by(JobRecord.created_at.asc())
            .limit(1)
        )
        try:
            q = q.with_for_update(skip_locked=True)
        except Exception:
            pass

        jid = db.scalar(q)
        if not jid:
            return None

        res = db.execute(
            update(JobRecord)
            .where(JobRecord.id == str(jid), JobRecord.status == "queued")
            .values(
                status="running",
                started_at=now,
                updated_at=now,
                worker_id=str(worker_id),
                attempts=(JobRecord.attempts + 1),
            )
        )
        if getattr(res, "rowcount", 0) != 1:
            db.rollback()
            return None
        db.commit()
        return str(jid)


def _execute_queued_job(job_id: str) -> None:
    job = _load_job(job_id)
    if not job:
        return
    result = job.result if isinstance(job.result, dict) else {}
    jtype = str(result.get("type") or "").strip().lower()

    if jtype == "crawl":
        user_id = str(result.get("user_id") or "").strip()
        slug = str(result.get("slug") or "").strip()
        cfg = Path(job.config_path).expanduser() if job.config_path else None
        if cfg and not cfg.is_absolute():
            cfg = (REPO_ROOT / cfg).resolve()
        _run_crawl_job(job.id, user_id, slug, cfg)
        return

    if jtype == "autopilot":
        cfg = Path(job.config_path).expanduser() if job.config_path else None
        if not cfg:
            job.status = "failed"
            job.returncode = 2
            job.stderr = (job.stderr or "") + "\n[WORKER] Missing config_path\n"
            job.finished_at = time.time()
            _save_job(job)
            return
        if not cfg.is_absolute():
            cfg = (REPO_ROOT / cfg).resolve()
        extra_args = result.get("extra_args") if isinstance(result, dict) else None
        extra = extra_args if isinstance(extra_args, list) and all(isinstance(x, str) for x in extra_args) else None
        _run_autopilot_job(job.id, cfg, extra)
        return

    job.status = "failed"
    job.returncode = 2
    job.stderr = (job.stderr or "") + f"\n[WORKER] Unknown job type: {jtype or 'unknown'}\n"
    job.finished_at = time.time()
    _save_job(job)


def _job_worker_loop(worker_id: str) -> None:
    while not _WORKER_STOP.is_set():
        try:
            jid = _claim_next_job_id(worker_id=worker_id)
        except Exception as e:
            print(f"[WORKER] claim error: {type(e).__name__}: {e}")
            _WORKER_STOP.wait(1.0)
            continue

        if not jid:
            _WORKER_STOP.wait(1.0)
            continue

        try:
            _execute_queued_job(jid)
        except Exception as e:
            try:
                job = _load_job(jid)
                if job:
                    job.status = "failed"
                    job.returncode = job.returncode if job.returncode is not None else 1
                    job.stderr = _trim_log((job.stderr or "") + f"\n[WORKER] {type(e).__name__}: {e}\n")
                    job.finished_at = time.time()
                    _save_job(job)
            except Exception:
                pass


def _start_job_worker() -> None:
    global _WORKER_STARTED
    if not _worker_enabled():
        return
    with _WORKER_STARTED_GUARD:
        if _WORKER_STARTED:
            return
        _WORKER_STARTED = True
        base = uuid.uuid4().hex[:8]
        n = _worker_concurrency()
        for idx in range(n):
            wid = f"{base}-{idx+1}"
            t = threading.Thread(target=_job_worker_loop, args=(wid,), daemon=True)
            t.start()
            _WORKER_THREADS.append(t)


_RETENTION_STARTED_GUARD = threading.Lock()
_RETENTION_STARTED = False


def _env_int(name: str, default: int = 0) -> int:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _retention_cutoff_s(*, days_env: str) -> float | None:
    days = _env_int(days_env, 0)
    if days <= 0:
        return None
    return time.time() - (float(days) * 86400.0)


def _cleanup_old_jobs() -> None:
    cutoff = _retention_cutoff_s(days_env="SEO_AGENT_JOBS_RETENTION_DAYS")
    if cutoff is None:
        return
    try:
        with DB.session() as db:
            rows = db.execute(
                select(JobRecord.id).where(
                    JobRecord.created_at < float(cutoff),
                    JobRecord.status.in_(["done", "failed", "canceled"]),
                )
            ).all()
            if not rows:
                return
            ids = [str(r[0]) for r in rows if r and r[0]]
            if not ids:
                return
            db.execute(update(JobRecord).where(JobRecord.id.in_(ids)).values(stdout=None, stderr=None, progress=None))
            db.commit()
    except Exception as e:
        print(f"[RETENTION] jobs cleanup error: {type(e).__name__}: {e}")


def _cleanup_old_runs() -> None:
    cutoff = _retention_cutoff_s(days_env="SEO_AGENT_RUNS_RETENTION_DAYS")
    if cutoff is None:
        return
    root = DEFAULT_RUNS_DIR
    if not root.exists() or not root.is_dir():
        return

    cutoff_dt = datetime.fromtimestamp(float(cutoff))

    def _is_old_ts(name: str) -> bool:
        try:
            dt = dash.parse_timestamp(name)
            return bool(dt and dt < cutoff_dt)
        except Exception:
            return False

    removed = 0
    try:
        for user_dir in root.iterdir():
            if not user_dir.is_dir():
                continue
            for slug_dir in user_dir.iterdir():
                if not slug_dir.is_dir():
                    continue
                for run_dir in slug_dir.iterdir():
                    if not run_dir.is_dir():
                        continue
                    if _is_old_ts(run_dir.name):
                        try:
                            _delete_runs_path_from_object_store(run_dir, recursive=True)
                            shutil.rmtree(str(run_dir))
                            removed += 1
                        except Exception:
                            continue
    except Exception as e:
        print(f"[RETENTION] runs cleanup error: {type(e).__name__}: {e}")
        return

    if removed:
        print(f"[RETENTION] removed runs: {removed}")


def _retention_loop() -> None:
    # Run quickly on boot, then every few hours.
    while not _WORKER_STOP.is_set():
        _cleanup_old_jobs()
        _cleanup_old_runs()
        _WORKER_STOP.wait(float(os.getenv("SEO_AGENT_RETENTION_EVERY_SECONDS", "21600")))  # 6h


def _start_retention() -> None:
    global _RETENTION_STARTED
    if _retention_cutoff_s(days_env="SEO_AGENT_JOBS_RETENTION_DAYS") is None and _retention_cutoff_s(
        days_env="SEO_AGENT_RUNS_RETENTION_DAYS"
    ) is None:
        return
    with _RETENTION_STARTED_GUARD:
        if _RETENTION_STARTED:
            return
        _RETENTION_STARTED = True
        t = threading.Thread(target=_retention_loop, daemon=True)
        t.start()


_SENTRY_READY = False


def _init_sentry() -> None:
    global _SENTRY_READY
    if _SENTRY_READY:
        return
    dsn = _safe_env("SENTRY_DSN")
    if not dsn:
        return
    try:
        import sentry_sdk  # type: ignore
        from sentry_sdk.integrations.asgi import SentryAsgiMiddleware  # type: ignore

        raw_rate = str(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.05"))
        try:
            rate = float(raw_rate)
        except Exception:
            rate = 0.05

        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=max(0.0, min(1.0, rate)),
            environment=str(os.getenv("SENTRY_ENVIRONMENT") or os.getenv("RENDER_SERVICE_NAME") or "prod"),
            release=str(os.getenv("RENDER_GIT_COMMIT") or ""),
        )
        app.add_middleware(SentryAsgiMiddleware)  # type: ignore[name-defined]
        _SENTRY_READY = True
    except Exception as e:
        print(f"[SENTRY] init error: {type(e).__name__}: {e}")


_LOG_LIMIT_CHARS = 200_000

_CRAWL_PROGRESS_RE = re.compile(r"\[CRAWL\]\s+(\d+)\s*/\s*(\d+)", re.IGNORECASE)
_CRAWL_DONE_RE = re.compile(r"\[CRAWL\]\s+Done\b.*?\bpages\s*=\s*(\d+)", re.IGNORECASE)
_AUTOPILOT_PROGRESS_RE = re.compile(r"\[AUTOPILOT\]\s+site\s+(\d+)\s*/\s*(\d+)\s*:\s*(.*)", re.IGNORECASE)


def _trim_log(value: str, limit: int = _LOG_LIMIT_CHARS) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]


def _update_job_progress_from_line(job: Job, line: str, job_kind: str) -> None:
    if not isinstance(line, str) or not line:
        return

    if job_kind == "crawl":
        done = _CRAWL_DONE_RE.search(line)
        if done:
            pages = int(done.group(1))
            if pages >= 0:
                job.progress = {"type": "crawl", "current": pages, "total": pages, "done": True}
            return
        m = _CRAWL_PROGRESS_RE.search(line)
        if not m:
            return
        cur = int(m.group(1))
        total = int(m.group(2))
        job.progress = {"type": "crawl", "current": cur, "total": total}
        return

    if job_kind == "autopilot":
        m = _AUTOPILOT_PROGRESS_RE.search(line)
        if not m:
            return
        cur = int(m.group(1))
        total = int(m.group(2))
        name = (m.group(3) or "").strip()
        job.progress = {"type": "autopilot", "current": cur, "total": total, "site": name}


def _job_kind_from_command(command: list[str] | None) -> str | None:
    if not command:
        return None
    lower = [c.lower() for c in command if isinstance(c, str)]
    if any("seo_autopilot.py" in c for c in lower):
        return "autopilot"
    if any("seo_audit.py" in c for c in lower):
        return "crawl"
    return None


def _command_arg(command: list[str] | None, flag: str) -> str | None:
    if not command:
        return None
    try:
        idx = command.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(command):
        return None
    value = command[idx + 1]
    return value if isinstance(value, str) else None


_WIN_ABS_PATH_RE = re.compile(r"^([a-zA-Z]):[\\\\/](.*)$")


def _path_from_any_os(value: str) -> Path:
    """
    Accept Windows or POSIX-style paths and return a usable local Path.

    When running inside WSL/Linux but job artifacts were written with a Windows path,
    this maps e.g. `C:\\Users\\me\\project\\file.json` -> `/mnt/c/Users/me/project/file.json`.
    """
    if not value:
        return Path(value)
    raw = str(value).strip().strip('"')
    m = _WIN_ABS_PATH_RE.match(raw)
    if m:
        drive = (m.group(1) or "").lower()
        rest = (m.group(2) or "").replace("\\", "/")
        return Path(f"/mnt/{drive}/{rest}")
    return Path(raw)


def _normalize_completed_job(job: Job) -> None:
    if job.status not in {"done", "failed"}:
        return
    kind = _job_kind_from_command(job.command)
    if kind != "crawl":
        return

    stdout = job.stdout or ""
    done = _CRAWL_DONE_RE.search(stdout)
    if done:
        pages = int(done.group(1))
        if pages >= 0:
            job.progress = {"type": "crawl", "current": pages, "total": pages, "done": True}
        return

    out_dir = _command_arg(job.command, "--output-dir")
    if not out_dir:
        return
    report_path = _path_from_any_os(out_dir) / "report.json"
    if not report_path.exists():
        return
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(report, dict):
        return
    meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
    pages_crawled = meta.get("pages_crawled")
    if isinstance(pages_crawled, int) and pages_crawled >= 0:
        job.progress = {"type": "crawl", "current": pages_crawled, "total": pages_crawled, "done": True}


def _finalize_crawl_billing_after_stale(job: Job, *, actual_pages_crawled: int | None) -> None:
    """
    Best-effort billing reconciliation for crawl jobs finalized by `_finalize_stale_job`.

    Normal flow:
      - enqueue reserves `quota_reserved_pages` (usage +planned)
      - `_run_crawl_job` refunds/adjusts based on actual pages crawled in finally block

    When a server crashes/restarts mid-run, the finally block may never run. This keeps quotas consistent.
    """
    try:
        result = job.result if isinstance(job.result, dict) else {}
        if not isinstance(result, dict):
            return
        if str(result.get("type") or "").strip().lower() != "crawl":
            return

        if bool(result.get("skip_billing") or False):
            return

        owner_id = str(result.get("user_id") or "").strip()
        if not owner_id:
            return
        slug = str(result.get("slug") or "").strip()

        try:
            reserved_pages = int(result.get("quota_reserved_pages") or 0)
        except Exception:
            reserved_pages = 0

        if job.status == "done":
            if not isinstance(actual_pages_crawled, int) or actual_pages_crawled < 0:
                return
            if reserved_pages > 0:
                delta = int(actual_pages_crawled) - int(reserved_pages)
                if delta != 0:
                    with DB.session() as db:
                        billing.usage_add(
                            db,
                            user_id=owner_id,
                            metric="pages_crawled_month",
                            amount=int(delta),
                            meta={
                                "kind": "crawl_adjust_stale",
                                "job_id": str(job.id),
                                "slug": slug,
                                "reserved_pages": int(reserved_pages),
                                "actual_pages_crawled": int(actual_pages_crawled),
                            },
                        )
            elif actual_pages_crawled > 0:
                with DB.session() as db:
                    billing.usage_add(
                        db,
                        user_id=owner_id,
                        metric="pages_crawled_month",
                        amount=int(actual_pages_crawled),
                        meta={"kind": "crawl_usage_stale", "job_id": str(job.id), "slug": slug},
                    )
            return

        if reserved_pages > 0:
            with DB.session() as db:
                billing.usage_add(
                    db,
                    user_id=owner_id,
                    metric="pages_crawled_month",
                    amount=-int(reserved_pages),
                    meta={"kind": "crawl_refund_stale", "job_id": str(job.id), "slug": slug},
                )
            try:
                result["quota_reserved_pages"] = 0
                job.result = result
            except Exception:
                pass
    except Exception as e:
        print(f"[BILLING] stale billing reconcile error: {type(e).__name__}: {e}")


def _finalize_stale_job(job: Job) -> bool:
    """
    Best-effort: finalize jobs that are marked running/queued but have finished artifacts on disk.

    This happens when the server (uvicorn reload / crash) is restarted while a subprocess keeps running
    or has already completed, leaving the job JSON stuck.
    """
    if job.status not in {"queued", "running", "cancel_requested"}:
        return False
    # Do not interfere with jobs launched by this server process.
    if _is_job_active(job.id):
        return False

    # Prevent double-finalization inside a single process (and avoid duplicate quota reconciliation).
    lock = _job_lock(job.id)
    with lock:
        cur_status = _job_db_status(job.id)
        if cur_status and cur_status not in {"queued", "running", "cancel_requested"}:
            return False
        if cur_status:
            job.status = cur_status

        kind = _job_kind_from_command(job.command)
        if kind == "autopilot":
            started_at = job.started_at or job.created_at or 0.0
            age_s = max(0.0, time.time() - float(started_at))
            stale_after_s = float(os.getenv("SEO_AGENT_STALE_AUTOPILOT_JOB_SECONDS", "3600"))  # 1h
            if _pid_is_alive(job.pid):
                return False

            if job.status == "cancel_requested":
                job.status = "canceled"
                job.returncode = job.returncode if job.returncode is not None else 130
                job.finished_at = job.finished_at if job.finished_at is not None else time.time()
                job.stderr = _trim_log((job.stderr or "") + "\n[STALE] Job annulé après redémarrage.\n")
                _save_job(job)
                return True

            if age_s < stale_after_s:
                return False

            stdout = job.stdout or ""
            stderr = job.stderr or ""

            # If the last run ended with an exception, fail fast.
            if "Traceback" in stdout or "Traceback" in stderr:
                job.status = "failed"
                job.returncode = job.returncode if job.returncode is not None else 1
                job.finished_at = job.finished_at if job.finished_at is not None else time.time()
                if not (job.stderr or "").strip():
                    job.stderr = "[STALE] Autopilot job marqué en échec (Traceback détecté)."
                _save_job(job)
                return True

            progress = job.progress if isinstance(job.progress, dict) else {}
            cur = int(progress.get("current") or 0) if isinstance(progress.get("current"), (int, float, str)) else 0
            total = int(progress.get("total") or 0) if isinstance(progress.get("total"), (int, float, str)) else 0

            # If progress indicates completion, mark as done and attach latest artifacts.
            if total > 0 and cur >= total:
                job.status = "done"
                job.returncode = job.returncode if job.returncode is not None else 0
                job.finished_at = job.finished_at if job.finished_at is not None else time.time()
                latest = _load_latest_global_summary(DEFAULT_RUNS_DIR) if DEFAULT_RUNS_DIR.exists() else None
                job.result = {
                    "type": "autopilot",
                    "automation_url": "/automation",
                    "timestamp": latest.get("timestamp") if latest else None,
                    "sites_summary_md": str(latest["sites_summary_md"]) if latest and latest.get("sites_summary_md") else None,
                    "interlinking_md": str(latest["interlinking_md"]) if latest and latest.get("interlinking_md") else None,
                }
                _save_job(job)
                return True

            # Otherwise: job is stale and incomplete.
            job.status = "failed"
            job.returncode = job.returncode if job.returncode is not None else 1
            job.finished_at = job.finished_at if job.finished_at is not None else time.time()
            if not (job.stderr or "").strip():
                job.stderr = f"[STALE] Autopilot job marqué en échec (âge={int(age_s)}s)."
            _save_job(job)
            return True

        if kind != "crawl":
            return False

        out_dir = _command_arg(job.command, "--output-dir")
        report_path: Path | None = None
        if out_dir:
            report_path = _path_from_any_os(out_dir) / "report.json"
        elif isinstance(job.result, dict) and isinstance(job.result.get("report_json"), str):
            report_path = _path_from_any_os(str(job.result.get("report_json") or ""))
        if not report_path:
            return False

        try:
            report_path = report_path.expanduser()
            if not report_path.is_absolute():
                report_path = (REPO_ROOT / report_path).resolve()
            else:
                report_path = report_path.resolve()
        except Exception:
            return False

        if not report_path.exists() or not report_path.is_file():
            # If the job process is still alive, keep it as running/queued.
            if _pid_is_alive(job.pid):
                return False

            # If the job has been "running" for a long time and there are still no artifacts,
            # treat it as stale to avoid projects being stuck "En cours" forever after a crash/reload.
            started_at = job.started_at or job.created_at or 0.0
            age_s = max(0.0, time.time() - float(started_at))

            # Heuristic: if output dir exists but is empty, it's extremely likely the process never wrote anything.
            out_dir_path = _path_from_any_os(out_dir) if out_dir else report_path.parent
            is_empty_dir = False
            try:
                if out_dir_path.exists() and out_dir_path.is_dir():
                    is_empty_dir = next(out_dir_path.iterdir(), None) is None
            except Exception:
                is_empty_dir = False

            progress = job.progress if isinstance(job.progress, dict) else {}
            progress_done = bool(progress.get("done"))
            if not progress_done:
                try:
                    cur = int(progress.get("current") or 0)
                    total = int(progress.get("total") or 0)
                    progress_done = total > 0 and cur >= total
                except Exception:
                    progress_done = False
            crawl_done_logged = bool(_CRAWL_DONE_RE.search(job.stdout or ""))

            stale_after_s = float(os.getenv("SEO_AGENT_STALE_CRAWL_JOB_SECONDS", "43200"))  # 12h fallback
            empty_after_s = float(os.getenv("SEO_AGENT_STALE_CRAWL_EMPTY_SECONDS", "300"))  # 5m
            done_after_s = float(os.getenv("SEO_AGENT_STALE_CRAWL_DONE_SECONDS", "900"))  # 15m

            # Fast-path: empty dir or crawl completed but no report => likely interrupted.
            if is_empty_dir and age_s < empty_after_s:
                return False
            if (progress_done or crawl_done_logged) and age_s < done_after_s:
                return False
            if (not is_empty_dir) and (not (progress_done or crawl_done_logged)) and age_s < stale_after_s:
                return False

            if job.status == "cancel_requested":
                job.status = "canceled"
                job.returncode = job.returncode if job.returncode is not None else 130
            else:
                job.status = "failed"
                job.returncode = job.returncode if job.returncode is not None else 1
            job.finished_at = job.finished_at if job.finished_at is not None else time.time()
            if not (job.stderr or "").strip():
                reason = "aucun report.json trouvé après redémarrage"
                if is_empty_dir:
                    reason = "dossier de sortie vide (job probablement interrompu)"
                elif progress_done or crawl_done_logged:
                    reason = "crawl terminé mais aucun report.json (job probablement interrompu)"
                if job.status == "canceled":
                    job.stderr = f"[STALE] Job annulé: {reason} (âge={int(age_s)}s)."
                else:
                    job.stderr = f"[STALE] Job marqué en échec: {reason} (âge={int(age_s)}s)."
            _finalize_crawl_billing_after_stale(job, actual_pages_crawled=None)
            _save_job(job)
            return True

        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            return False
        if not isinstance(report, dict):
            return False

        meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
        pages_crawled = meta.get("pages_crawled")
        if not isinstance(pages_crawled, int) or pages_crawled < 0:
            # Backward/forward compatibility: accept a few alternative meta keys.
            for k in ("pages", "pages_seen", "urls_discovered"):
                v = meta.get(k)
                if isinstance(v, int) and v >= 0:
                    pages_crawled = v
                    break
        if not isinstance(pages_crawled, int) or pages_crawled < 0:
            return False

        # Looks complete enough: finalize as done.
        changed = False
        if job.status != "done":
            job.status = "done"
            changed = True
        if job.returncode is None:
            job.returncode = 0
            changed = True
        if job.finished_at is None:
            try:
                job.finished_at = float(report_path.stat().st_mtime)
            except Exception:
                job.finished_at = time.time()
            changed = True
        before_progress = job.progress
        job.progress = {"type": "crawl", "current": pages_crawled, "total": pages_crawled, "done": True}
        if before_progress != job.progress:
            changed = True

        # Ensure result has file pointers for the UI.
        if not isinstance(job.result, dict):
            job.result = {"type": "crawl"}
            changed = True
        if isinstance(job.result, dict):
            if not job.result.get("report_json"):
                job.result["report_json"] = str(report_path)
                changed = True
            md_path = report_path.parent / "report.md"
            if md_path.exists() and not job.result.get("report_md"):
                job.result["report_md"] = str(md_path)
                changed = True

        if changed:
            _finalize_crawl_billing_after_stale(job, actual_pages_crawled=int(pages_crawled))
            _save_job(job)
        return changed


def _job_db_status(job_id: str) -> str:
    jid = str(job_id or "").strip()
    if not jid:
        return ""
    try:
        with DB.session() as db:
            v = db.scalar(select(JobRecord.status).where(JobRecord.id == jid))
            return str(v or "").strip()
    except Exception:
        return ""


def _run_subprocess_streaming(
    job: Job,
    cmd: list[str],
    cwd: Path,
    job_kind: str,
    timeout_s: float | None = None,
    env_extra: dict[str, str] | None = None,
) -> int:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    if isinstance(env_extra, dict):
        for key, value in env_extra.items():
            if str(value or "").strip():
                env[str(key)] = str(value)

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        universal_newlines=True,
        errors="replace",
        env=env,
    )
    job.pid = int(proc.pid) if getattr(proc, "pid", None) is not None else None
    _save_job(job)

    lock = threading.Lock()
    last_save = 0.0

    def maybe_save(force: bool = False) -> None:
        nonlocal last_save
        now = time.monotonic()
        if force or (now - last_save) >= 0.6:
            _save_job(job)
            last_save = now

    def reader(pipe, target: str) -> None:
        try:
            for line in iter(pipe.readline, ""):
                with lock:
                    if target == "stdout":
                        job.stdout = _trim_log((job.stdout or "") + line)
                    else:
                        job.stderr = _trim_log((job.stderr or "") + line)
                    _update_job_progress_from_line(job, line, job_kind=job_kind)
                    maybe_save()
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    threads: list[threading.Thread] = []
    if proc.stdout is not None:
        t = threading.Thread(target=reader, args=(proc.stdout, "stdout"), daemon=True)
        t.start()
        threads.append(t)
    if proc.stderr is not None:
        t = threading.Thread(target=reader, args=(proc.stderr, "stderr"), daemon=True)
        t.start()
        threads.append(t)

    timed_out = False
    canceled = False
    start = time.monotonic()
    poll_s = 0.5
    while True:
        try:
            returncode = proc.wait(timeout=poll_s)
            break
        except subprocess.TimeoutExpired:
            pass

        # Cancellation check (DB). Keep it reasonably cheap.
        if not canceled:
            st = _job_db_status(job.id)
            if st == "cancel_requested":
                canceled = True
                try:
                    proc.terminate()
                except Exception:
                    pass

        # Timeout check.
        if timeout_s and timeout_s > 0 and (time.monotonic() - start) >= float(timeout_s):
            timed_out = True
            try:
                proc.kill()
            except Exception:
                pass
            try:
                returncode = proc.wait(timeout=10)
            except Exception:
                returncode = 124
            break

        if canceled:
            # Give the process a moment to exit gracefully; then force-kill.
            try:
                returncode = proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    returncode = proc.wait(timeout=10)
                except Exception:
                    returncode = 130
            break

    for t in threads:
        t.join(timeout=2)

    with lock:
        job.pid = None
        if timed_out:
            job.stderr = _trim_log((job.stderr or "") + f"\n[TIMEOUT] Timeout après {int(timeout_s or 0)}s.\n")
        if canceled:
            job.status = "canceled"
            job.stderr = _trim_log((job.stderr or "") + "\n[CANCEL] Job annulé.\n")
        maybe_save(force=True)
    try:
        return int(returncode)
    except Exception:
        return 1


def _run_autopilot_job(job_id: str, config_path: Path, extra_args: list[str] | None) -> None:
    _mark_job_active(job_id, True)
    job = _load_job(job_id)
    if not job:
        _mark_job_active(job_id, False)
        return
    job.status = "running"
    job.started_at = time.time()

    script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_autopilot.py"
    cmd = [sys.executable, "-u", str(script), "--config", str(config_path)]
    if extra_args:
        cmd.extend(extra_args)
    job.config_path = str(config_path)
    job.command = cmd
    job.stdout = job.stdout or ""
    job.stderr = job.stderr or ""
    _save_job(job)

    try:
        raw_timeout = str(os.getenv("SEO_AGENT_AUTOPILOT_JOB_TIMEOUT_SECONDS", "10800"))  # 3h
        try:
            timeout_s = float(raw_timeout)
        except Exception:
            timeout_s = 10800.0
        if timeout_s <= 0:
            timeout_s = None

        returncode = _run_subprocess_streaming(job, cmd, cwd=REPO_ROOT, job_kind="autopilot", timeout_s=timeout_s)
        job.returncode = returncode
        job.finished_at = time.time()
        if job.status != "canceled":
            job.status = "done" if returncode == 0 else "failed"
        if returncode == 0 and job.status != "canceled":
            latest = _load_latest_global_summary(DEFAULT_RUNS_DIR) if DEFAULT_RUNS_DIR.exists() else None
            job.result = {
                "type": "autopilot",
                "automation_url": "/automation",
                "timestamp": latest.get("timestamp") if latest else None,
                "sites_summary_md": str(latest["sites_summary_md"]) if latest and latest.get("sites_summary_md") else None,
                "interlinking_md": str(latest["interlinking_md"]) if latest and latest.get("interlinking_md") else None,
            }
        _save_job(job)
    except Exception as e:
        job.returncode = 1
        job.stderr = f"{type(e).__name__}: {e}"
        job.finished_at = time.time()
        job.status = "failed"
        _save_job(job)
    finally:
        _mark_job_active(job_id, False)


def _load_yaml_or_json_safe(path: Path) -> dict[str, Any]:
    """
    Read a config file that can be either YAML or JSON.

    This wrapper is intentionally defensive: some environments reported NameError on
    `_load_yaml_or_json` during hot reload, so we fall back to a minimal local loader.
    """
    fn = globals().get("_load_yaml_or_json")
    if callable(fn):
        try:
            data = fn(path)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    if not path.exists() or not path.is_file():
        return {}
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        if suffix in {".yml", ".yaml"}:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}
    return {}


def _crawl_defaults_from_config(config_path: Path, slug: str) -> dict[str, Any]:
    cfg = _load_yaml_or_json_safe(config_path)
    if not cfg:
        return {}

    defaults = cfg.get("defaults") if isinstance(cfg.get("defaults"), dict) else {}
    crawl_defaults = defaults.get("crawl") if isinstance(defaults.get("crawl"), dict) else {}

    # Optional per-site overrides.
    sites = cfg.get("sites") if isinstance(cfg.get("sites"), list) else []
    crawl_overrides: dict[str, Any] = {}
    for site in sites:
        if not isinstance(site, dict):
            continue
        name = str(site.get("name") or "").strip()
        site_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") if name else ""
        if slug in {name, site_slug}:
            crawl_overrides = site.get("crawl") if isinstance(site.get("crawl"), dict) else {}
            break

    merged = dict(crawl_defaults)
    merged.update({k: v for k, v in crawl_overrides.items() if v is not None})
    return merged


def _gsc_api_defaults_from_config(config_path: Path, slug: str) -> dict[str, Any]:
    cfg = _load_yaml_or_json_safe(config_path)
    if not cfg:
        return {}

    defaults = cfg.get("defaults") if isinstance(cfg.get("defaults"), dict) else {}
    gsc_api_defaults = defaults.get("gsc_api") if isinstance(defaults.get("gsc_api"), dict) else {}

    sites = cfg.get("sites") if isinstance(cfg.get("sites"), list) else []
    gsc_overrides: dict[str, Any] = {}
    for site in sites:
        if not isinstance(site, dict):
            continue
        name = str(site.get("name") or "").strip()
        site_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") if name else ""
        if slug in {name, site_slug}:
            gsc_overrides = site.get("gsc_api") if isinstance(site.get("gsc_api"), dict) else {}
            break

    merged = dict(gsc_api_defaults)
    merged.update({k: v for k, v in gsc_overrides.items() if v is not None})
    return merged


def _project_overrides_from_settings(settings: dict[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    node = settings if isinstance(settings, dict) else {}
    crawl = node.get("crawl") if isinstance(node.get("crawl"), dict) else {}
    gsc_api = node.get("gsc_api") if isinstance(node.get("gsc_api"), dict) else {}
    bing = node.get("bing") if isinstance(node.get("bing"), dict) else {}
    return dict(crawl), dict(gsc_api), dict(bing)


def _int_in_range(value: Any, *, default: int, min_v: int, max_v: int) -> int:
    try:
        n = int(value)
    except Exception:
        n = int(default)
    return max(min_v, min(int(n), max_v))


def _float_in_range(value: Any, *, default: float, min_v: float, max_v: float) -> float:
    try:
        n = float(value)
    except Exception:
        n = float(default)
    return max(min_v, min(float(n), max_v))


def _normalize_crawl_cfg(raw: dict[str, Any]) -> dict[str, Any]:
    pagespeed_strategy = str(raw.get("pagespeed_strategy") or "mobile").strip().lower() or "mobile"
    if pagespeed_strategy not in {"mobile", "desktop"}:
        pagespeed_strategy = "mobile"

    include_regex = raw.get("include_regex")
    include_regex = str(include_regex).strip() if isinstance(include_regex, str) else ""
    exclude_regex = raw.get("exclude_regex")
    exclude_regex = str(exclude_regex).strip() if isinstance(exclude_regex, str) else ""

    user_agent = str(raw.get("user_agent") or "SEOAutopilot/1.0").strip() or "SEOAutopilot/1.0"

    # Default to the Ahrefs-like profile to keep crawl behavior consistent across existing & future sites.
    profile = str(raw.get("profile") or "ahrefs").strip().lower() or "ahrefs"
    if profile not in {"default", "ahrefs"}:
        profile = "ahrefs"

    # Ahrefs tends to surface network issues (timeouts/connection resets) as "Timed out" (HTTP status 0).
    # Use a lower default timeout in the Ahrefs profile to better match that behavior.
    timeout_default = 8.0 if profile == "ahrefs" else 15.0
    raw_timeout = raw.get("timeout_s")
    # Migration: older UI defaults used 60s. Treat it as a legacy/default when in Ahrefs profile.
    if profile == "ahrefs":
        try:
            if raw_timeout is not None and abs(float(raw_timeout) - 60.0) < 1e-9:
                raw_timeout = None
        except Exception:
            pass

    check_resources = bool(raw.get("check_resources")) if "check_resources" in raw else True
    pagespeed = bool(raw.get("pagespeed")) if "pagespeed" in raw else True

    return {
        "max_pages": _int_in_range(raw.get("max_pages"), default=300, min_v=1, max_v=200_000),
        "workers": _int_in_range(raw.get("workers"), default=6, min_v=1, max_v=32),
        "timeout_s": _float_in_range(raw_timeout, default=timeout_default, min_v=1.0, max_v=120.0),
        "profile": profile,
        "ignore_robots": bool(raw.get("ignore_robots") or False),
        "allow_subdomains": bool(raw.get("allow_subdomains")) if "allow_subdomains" in raw else True,
        "include_regex": include_regex,
        "exclude_regex": exclude_regex,
        "user_agent": user_agent,
        "check_resources": check_resources,
        "max_resources": _int_in_range(raw.get("max_resources"), default=250, min_v=0, max_v=20_000),
        "pagespeed": pagespeed,
        "pagespeed_strategy": pagespeed_strategy,
        "pagespeed_max_urls": _int_in_range(raw.get("pagespeed_max_urls"), default=50, min_v=0, max_v=1000),
        "pagespeed_timeout_s": _float_in_range(raw.get("pagespeed_timeout_s"), default=60.0, min_v=1.0, max_v=180.0),
        "pagespeed_workers": _int_in_range(raw.get("pagespeed_workers"), default=2, min_v=1, max_v=10),
        # Feature flags (not all are wired yet, but stored per project).
        "ai_keywords": bool(raw.get("ai_keywords")) if "ai_keywords" in raw else True,
        "backlinks_research": bool(raw.get("backlinks_research")) if "backlinks_research" in raw else True,
    }


def _normalize_gsc_cfg(raw: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(raw.get("enabled")) if "enabled" in raw else True
    search_type = str(raw.get("search_type") or "web").strip().lower() or "web"
    if search_type not in {"web", "image", "video", "news", "discover"}:
        search_type = "web"
    property_url = str(raw.get("property_url") or "").strip()
    return {
        "enabled": enabled,
        "days": _int_in_range(raw.get("days"), default=28, min_v=1, max_v=365),
        "search_type": search_type,
        "property_url": property_url,
        "min_impressions": _int_in_range(raw.get("min_impressions"), default=200, min_v=0, max_v=1_000_000),
        "inspection_enabled": bool(raw.get("inspection_enabled")) if "inspection_enabled" in raw else True,
        "inspection_max_urls": _int_in_range(raw.get("inspection_max_urls"), default=10, min_v=0, max_v=200),
        "inspection_timeout_s": _float_in_range(raw.get("inspection_timeout_s"), default=30.0, min_v=1.0, max_v=120.0),
        "inspection_language": str(raw.get("inspection_language") or "").strip(),
    }


def _bing_defaults_from_config(config_path: Path, slug: str) -> dict[str, Any]:
    cfg = _load_yaml_or_json_safe(config_path)
    defaults = cfg.get("defaults") if isinstance(cfg.get("defaults"), dict) else {}
    bing_defaults = defaults.get("bing") if isinstance(defaults.get("bing"), dict) else {}

    sites = cfg.get("sites") if isinstance(cfg.get("sites"), list) else []
    bing_overrides: dict[str, Any] = {}
    for site in sites:
        if not isinstance(site, dict):
            continue
        name = str(site.get("name") or "").strip()
        site_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") if name else ""
        if slug in {name, site_slug}:
            bing_overrides = site.get("bing") if isinstance(site.get("bing"), dict) else {}
            break

    merged = dict(bing_defaults)
    merged.update({k: v for k, v in bing_overrides.items() if v is not None})
    return merged


def _normalize_bing_cfg(raw: dict[str, Any]) -> dict[str, Any]:
    enabled = bool(raw.get("enabled")) if "enabled" in raw else True
    queries_csv = str(raw.get("queries_csv") or "").strip()
    pages_csv = str(raw.get("pages_csv") or "").strip()
    site_url = str(raw.get("site_url") or "").strip()
    urlinfo_max = _int_in_range(raw.get("urlinfo_max"), default=0, min_v=0, max_v=50)
    fetch_crawl_issues = bool(raw.get("fetch_crawl_issues")) if "fetch_crawl_issues" in raw else True
    fetch_blocked_urls = bool(raw.get("fetch_blocked_urls")) if "fetch_blocked_urls" in raw else True
    fetch_sitemaps = bool(raw.get("fetch_sitemaps")) if "fetch_sitemaps" in raw else True
    return {
        "enabled": enabled,
        "min_impressions": _int_in_range(raw.get("min_impressions"), default=200, min_v=0, max_v=1_000_000),
        "days": _int_in_range(raw.get("days"), default=28, min_v=1, max_v=365),
        "site_url": site_url,
        "queries_csv": queries_csv,
        "pages_csv": pages_csv,
        "urlinfo_max": urlinfo_max,
        "fetch_crawl_issues": fetch_crawl_issues,
        "fetch_blocked_urls": fetch_blocked_urls,
        "fetch_sitemaps": fetch_sitemaps,
    }


def _load_yaml_or_json(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        if suffix in {".yml", ".yaml"}:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}
    return {}


def _effective_project_crawl_settings(
    slug: str, *, config_path: Path | None, project_settings: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    crawl_cfg: dict[str, Any] = {}
    gsc_cfg: dict[str, Any] = {}
    bing_cfg: dict[str, Any] = {}
    if config_path and config_path.exists():
        crawl_cfg = _crawl_defaults_from_config(config_path, slug)
        gsc_cfg = _gsc_api_defaults_from_config(config_path, slug)
        bing_cfg = _bing_defaults_from_config(config_path, slug)

    overrides_crawl, overrides_gsc, overrides_bing = _project_overrides_from_settings(project_settings)
    crawl_cfg.update({k: v for k, v in overrides_crawl.items() if v is not None})
    gsc_cfg.update({k: v for k, v in overrides_gsc.items() if v is not None})
    bing_cfg.update({k: v for k, v in overrides_bing.items() if v is not None})

    return _normalize_crawl_cfg(crawl_cfg), _normalize_gsc_cfg(gsc_cfg), _normalize_bing_cfg(bing_cfg)


def _run_crawl_job(job_id: str, user_id: str, slug: str, config_path: Path | None) -> None:
    _mark_job_active(job_id, True)
    job = _load_job(job_id)
    if not job:
        _mark_job_active(job_id, False)
        return
    initial_result = dict(job.result) if isinstance(job.result, dict) else {}
    reserved_pages = 0
    override_max_pages: int | None = None
    skip_billing = bool(initial_result.get("skip_billing") or False)
    try:
        reserved_pages = int(initial_result.get("quota_reserved_pages") or 0)
    except Exception:
        reserved_pages = 0
    try:
        ov = initial_result.get("override_max_pages")
        override_max_pages = int(ov) if ov is not None else None
    except Exception:
        override_max_pages = None
    actual_pages_crawled: int | None = None

    job.status = "running"
    job.started_at = time.time()

    runs_dir = _runs_dir_for_user(user_id)
    crawls = dash.list_project_crawls(runs_dir, slug)
    latest_ts = crawls[-1] if crawls else None

    base_url = ""
    site_name = slug
    project_settings: dict[str, Any] | None = None
    with DB.session() as db:
        proj = db.scalar(select(Project).where(Project.owner_user_id == str(user_id), Project.slug == slug))
        if proj:
            base_url = str(proj.base_url or "").strip()
            site_name = str(proj.site_name or site_name).strip() or site_name
            project_settings = proj.settings if isinstance(proj.settings, dict) else {}
    if latest_ts:
        run = dash.load_run_json(runs_dir, slug, latest_ts)
        site_name = str(run.get("site_name") or site_name)
        run_base_url = str(run.get("base_url") or "").strip()
        if run_base_url:
            base_url = run_base_url

        if not base_url:
            report = dash.load_report_json(runs_dir, slug, latest_ts) or {}
            meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
            base_url = str(meta.get("base_url") or "").strip()

    if not base_url:
        job.status = "failed"
        job.returncode = 2
        job.stderr = f"Impossible de déterminer base_url pour le projet: {slug}"
        job.finished_at = time.time()
        _save_job(job)
        return

    validation_err = _validate_public_crawl_target(base_url)
    if validation_err:
        job.status = "failed"
        job.returncode = 2
        job.stderr = f"Refus crawl target: {validation_err}"
        job.finished_at = time.time()
        _save_job(job)
        return

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    site_dir = runs_dir / slug / timestamp
    audit_dir = site_dir / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    run_meta = {
        "site_name": site_name,
        "base_url": base_url,
        "timestamp": timestamp,
        "config_path": str(config_path) if config_path else None,
        "runner": "seo_audit",
    }
    (site_dir / "run.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    job.result = {
        "type": "crawl",
        "slug": slug,
        "user_id": str(user_id),
        "timestamp": timestamp,
        "project_url": f"/projects/{slug}?crawl={timestamp}",
        "report_json": str((audit_dir / "report.json").resolve()),
        "report_md": str((audit_dir / "report.md").resolve()),
    }

    crawl_cfg, gsc_cfg, bing_cfg = _effective_project_crawl_settings(
        slug, config_path=config_path, project_settings=project_settings
    )

    run_meta["settings"] = {"crawl": crawl_cfg, "gsc_api": gsc_cfg, "bing": bing_cfg}
    (site_dir / "run.json").write_text(json.dumps(run_meta, ensure_ascii=False, indent=2), encoding="utf-8")

    max_pages = int(crawl_cfg.get("max_pages") or 300)
    if isinstance(override_max_pages, int) and override_max_pages > 0:
        max_pages = min(max_pages, int(override_max_pages))
    workers = int(crawl_cfg.get("workers") or 6)
    timeout_s = float(crawl_cfg.get("timeout_s") or 15)
    ignore_robots = bool(crawl_cfg.get("ignore_robots") or False)
    allow_subdomains = bool(crawl_cfg.get("allow_subdomains") or False)
    include_regex = str(crawl_cfg.get("include_regex") or "").strip() or None
    exclude_regex = str(crawl_cfg.get("exclude_regex") or "").strip() or None
    user_agent = str(crawl_cfg.get("user_agent") or "SEOAutopilot/1.0")
    check_resources = bool(crawl_cfg.get("check_resources")) if "check_resources" in crawl_cfg else True
    max_resources = int(crawl_cfg.get("max_resources") or 250)
    pagespeed = bool(crawl_cfg.get("pagespeed")) if "pagespeed" in crawl_cfg else True
    pagespeed_strategy = str(crawl_cfg.get("pagespeed_strategy") or "mobile")
    pagespeed_max_urls = int(crawl_cfg.get("pagespeed_max_urls") or 50)
    pagespeed_timeout_s = float(crawl_cfg.get("pagespeed_timeout_s") or 60)
    pagespeed_workers = int(crawl_cfg.get("pagespeed_workers") or 2)
    crawl_profile = str(crawl_cfg.get("profile") or "default").strip().lower() or "default"
    gsc_enabled = bool(gsc_cfg.get("enabled")) if "enabled" in gsc_cfg else True
    gsc_days = int(gsc_cfg.get("days") or 28)
    gsc_search_type = str(gsc_cfg.get("search_type") or "web")
    gsc_property = str(gsc_cfg.get("property_url") or "").strip()
    gsc_min_impressions = int(gsc_cfg.get("min_impressions") or 200)
    gsc_inspection_enabled = bool(gsc_cfg.get("inspection_enabled") or False)
    gsc_inspection_max_urls = int(gsc_cfg.get("inspection_max_urls") or 0)
    gsc_inspection_timeout_s = float(gsc_cfg.get("inspection_timeout_s") or 30.0)
    gsc_inspection_language = str(gsc_cfg.get("inspection_language") or "").strip()

    bing_enabled = bool(bing_cfg.get("enabled")) if "enabled" in bing_cfg else False
    bing_min_impressions = int(bing_cfg.get("min_impressions") or 200)
    bing_days = int(bing_cfg.get("days") or 28)
    bing_site_url = str(bing_cfg.get("site_url") or "").strip()
    bing_queries_csv = str(bing_cfg.get("queries_csv") or "").strip()
    bing_pages_csv = str(bing_cfg.get("pages_csv") or "").strip()
    bing_urlinfo_max = int(bing_cfg.get("urlinfo_max") or 0)
    bing_fetch_crawl_issues = bool(bing_cfg.get("fetch_crawl_issues")) if "fetch_crawl_issues" in bing_cfg else True
    bing_fetch_blocked_urls = bool(bing_cfg.get("fetch_blocked_urls")) if "fetch_blocked_urls" in bing_cfg else True
    bing_fetch_sitemaps = bool(bing_cfg.get("fetch_sitemaps")) if "fetch_sitemaps" in bing_cfg else True

    script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
    env_extra: dict[str, str] = {}
    pagespeed_api_key = str(os.environ.get("PAGESPEED_API_KEY") or "").strip()
    if pagespeed_api_key:
        env_extra["PAGESPEED_API_KEY"] = pagespeed_api_key
    bing_auth = _effective_bing_connection(user_id=str(user_id))
    if str(bing_auth.get("token") or "").strip():
        if bing_auth.get("mode") == "oauth":
            env_extra["BING_WEBMASTER_ACCESS_TOKEN"] = str(bing_auth.get("token") or "")
        elif bing_auth.get("mode") == "api_key":
            env_extra["BING_WEBMASTER_API_KEY"] = str(bing_auth.get("token") or "")
    cmd = [
        sys.executable,
        "-u",
        str(script),
        base_url,
        "--profile",
        ("ahrefs" if crawl_profile == "ahrefs" else "default"),
        "--max-pages",
        str(max_pages),
        "--workers",
        str(workers),
        "--timeout",
        str(timeout_s),
        "--output-dir",
        str(audit_dir),
    ]
    # For Ahrefs profile, let `seo_audit.py` choose the Ahrefs UA unless the user explicitly set another UA.
    if not (crawl_profile == "ahrefs" and user_agent.strip() in {"", "SEOAutopilot/1.0"}):
        cmd.extend(["--user-agent", user_agent])
    if ignore_robots:
        cmd.append("--ignore-robots")
    if allow_subdomains:
        cmd.append("--allow-subdomains")
    if isinstance(include_regex, str) and include_regex.strip():
        cmd.extend(["--include", include_regex.strip()])
    if isinstance(exclude_regex, str) and exclude_regex.strip():
        cmd.extend(["--exclude", exclude_regex.strip()])
    if check_resources:
        cmd.append("--check-resources")
        cmd.extend(["--max-resources", str(max(0, max_resources))])
    if pagespeed:
        cmd.append("--pagespeed")
        cmd.extend(["--pagespeed-strategy", pagespeed_strategy.strip().lower() or "mobile"])
        cmd.extend(["--pagespeed-max-urls", str(max(0, pagespeed_max_urls))])
        cmd.extend(["--pagespeed-timeout", str(max(1.0, float(pagespeed_timeout_s)))])
        cmd.extend(["--pagespeed-workers", str(max(1, pagespeed_workers))])
    if gsc_enabled:
        gsc_dir = site_dir / "gsc"
        gsc_dir.mkdir(parents=True, exist_ok=True)
        cmd.append("--gsc-api")
        # Prefer per-project Google OAuth (refresh token) credentials when available.
        oauth_creds = _gsc_oauth_token_path(str(user_id), slug)
        if oauth_creds.exists() and oauth_creds.is_file():
            cmd.extend(["--gsc-credentials", str(oauth_creds)])
        if gsc_property:
            cmd.extend(["--gsc-property", gsc_property])
        cmd.extend(["--gsc-days", str(max(1, gsc_days))])
        cmd.extend(["--gsc-search-type", gsc_search_type.strip().lower() or "web"])
        cmd.extend(["--gsc-min-impressions", str(max(0, gsc_min_impressions))])
        cmd.extend(["--gsc-output-dir", str(gsc_dir)])
        if gsc_inspection_enabled and gsc_inspection_max_urls > 0:
            cmd.append("--gsc-inspection")
            cmd.extend(["--gsc-inspection-max-urls", str(max(0, gsc_inspection_max_urls))])
            cmd.extend(["--gsc-inspection-timeout", str(max(1.0, float(gsc_inspection_timeout_s)))])
            if gsc_inspection_language:
                cmd.extend(["--gsc-inspection-language", gsc_inspection_language])

    if bing_enabled:
        bing_dir = site_dir / "bing"
        bing_dir.mkdir(parents=True, exist_ok=True)
        cmd.append("--bing")
        cmd.extend(["--bing-min-impressions", str(max(0, bing_min_impressions))])
        cmd.extend(["--bing-days", str(max(1, bing_days))])
        if bing_site_url:
            cmd.extend(["--bing-site-url", bing_site_url])
        cmd.extend(["--bing-output-dir", str(bing_dir)])
        if not bing_fetch_crawl_issues:
            cmd.append("--bing-no-crawl-issues")
        if not bing_fetch_blocked_urls:
            cmd.append("--bing-no-blocked-urls")
        if not bing_fetch_sitemaps:
            cmd.append("--bing-no-sitemaps")
        if bing_urlinfo_max > 0:
            cmd.extend(["--bing-urlinfo-max", str(max(0, bing_urlinfo_max))])

        if bing_queries_csv:
            src = Path(bing_queries_csv).expanduser()
            if not src.is_absolute():
                src = (REPO_ROOT / src).resolve()
            else:
                src = src.resolve()
            if src.exists() and src.is_file():
                dst = bing_dir / "bing-queries.csv"
                try:
                    shutil.copyfile(str(src), str(dst))
                    cmd.extend(["--bing-queries-csv", str(dst)])
                except Exception:
                    cmd.extend(["--bing-queries-csv", str(src)])

        if bing_pages_csv:
            src = Path(bing_pages_csv).expanduser()
            if not src.is_absolute():
                src = (REPO_ROOT / src).resolve()
            else:
                src = src.resolve()
            if src.exists() and src.is_file():
                dst = bing_dir / "bing-pages.csv"
                try:
                    shutil.copyfile(str(src), str(dst))
                    cmd.extend(["--bing-pages-csv", str(dst)])
                except Exception:
                    cmd.extend(["--bing-pages-csv", str(src)])

    job.command = cmd
    job.config_path = str(config_path) if config_path else None
    job.stdout = job.stdout or ""
    job.stderr = job.stderr or ""
    _save_job(job)

    try:
        raw_timeout = str(os.getenv("SEO_AGENT_CRAWL_JOB_TIMEOUT_SECONDS", "21600"))  # 6h
        try:
            timeout_s = float(raw_timeout)
        except Exception:
            timeout_s = 21600.0
        if timeout_s <= 0:
            timeout_s = None

        returncode = _run_subprocess_streaming(
            job,
            cmd,
            cwd=REPO_ROOT,
            job_kind="crawl",
            timeout_s=timeout_s,
            env_extra=env_extra,
        )
        job.returncode = returncode
        if returncode == 0:
            report_path = audit_dir / "report.json"
            try:
                report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else None
            except Exception:
                report = None
            if isinstance(report, dict):
                meta = report.get("meta") if isinstance(report.get("meta"), dict) else {}
                pages_crawled = meta.get("pages_crawled")
                if isinstance(pages_crawled, int) and pages_crawled >= 0:
                    actual_pages_crawled = int(pages_crawled)
                    job.progress = {"type": "crawl", "current": pages_crawled, "total": pages_crawled, "done": True}
            job.result = {
                "type": "crawl",
                "slug": slug,
                "user_id": str(user_id),
                "timestamp": timestamp,
                "project_url": f"/projects/{slug}?crawl={timestamp}",
                "report_md": str((audit_dir / "report.md").resolve()),
                "report_json": str((audit_dir / "report.json").resolve()),
            }
        job.finished_at = time.time()
        if job.status != "canceled":
            job.status = "done" if returncode == 0 else "failed"
        _save_job(job)
    except Exception as e:
        job.returncode = 1
        job.stderr = f"{type(e).__name__}: {e}"
        job.finished_at = time.time()
        job.status = "failed"
        _save_job(job)
    finally:
        try:
            if site_dir.exists() and site_dir.is_dir():
                _sync_runs_path_to_object_store(site_dir)
        except Exception as e:
            print(f"[S3] crawl sync error: {type(e).__name__}: {e}")
        try:
            if (not skip_billing) and reserved_pages > 0:
                if job.status == "done" and isinstance(actual_pages_crawled, int) and actual_pages_crawled >= 0:
                    delta = int(actual_pages_crawled) - int(reserved_pages)
                    if delta != 0:
                        with DB.session() as db:
                            billing.usage_add(
                                db,
                                user_id=str(user_id),
                                metric="pages_crawled_month",
                                amount=int(delta),
                                meta={
                                    "kind": "crawl_adjust",
                                    "job_id": job_id,
                                    "slug": slug,
                                    "reserved_pages": int(reserved_pages),
                                    "actual_pages_crawled": int(actual_pages_crawled),
                                },
                        )
                elif job.status != "done":
                    with DB.session() as db:
                        billing.usage_add(
                            db,
                            user_id=str(user_id),
                            metric="pages_crawled_month",
                            amount=-int(reserved_pages),
                            meta={"kind": "crawl_refund", "job_id": job_id, "slug": slug},
                        )
            elif (not skip_billing) and isinstance(actual_pages_crawled, int) and actual_pages_crawled > 0:
                with DB.session() as db:
                    billing.usage_add(
                        db,
                        user_id=str(user_id),
                        metric="pages_crawled_month",
                        amount=int(actual_pages_crawled),
                        meta={"kind": "crawl_usage", "job_id": job_id, "slug": slug},
                    )
        except Exception as e:
            print(f"[BILLING] usage update error: {type(e).__name__}: {e}")
        _mark_job_active(job_id, False)


app = FastAPI(title="SEO Agent")
app.mount("/static", StaticFiles(directory=str(REPO_ROOT / "seo-agent-web" / "static")), name="static")


def _beta_basic_auth_expected() -> tuple[str, str] | None:
    user = str(os.environ.get("BETA_BASIC_AUTH_USER") or "").strip()
    password = str(os.environ.get("BETA_BASIC_AUTH_PASS") or "").strip()
    if not user or not password:
        return None
    return user, password


def _beta_basic_auth_unauthorized() -> Response:
    return Response(
        "Unauthorized",
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="SEO Agent (beta)"'},
    )


@app.middleware("http")
async def beta_basic_auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    expected = _beta_basic_auth_expected()
    if not expected:
        return await call_next(request)

    path = request.url.path
    if path.startswith("/static/") or path in {
        "/healthz",
        "/stripe/webhook",
        "/",
        "/pricing",
        "/terms",
        "/privacy",
        "/support",
        "/status",
    }:
        return await call_next(request)

    auth = str(request.headers.get("authorization") or "")
    if not auth.lower().startswith("basic "):
        return _beta_basic_auth_unauthorized()
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1].strip()).decode("utf-8", errors="replace")
    except Exception:
        return _beta_basic_auth_unauthorized()
    if ":" not in decoded:
        return _beta_basic_auth_unauthorized()
    user, password = decoded.split(":", 1)
    exp_user, exp_pass = expected
    if not hmac.compare_digest(user, exp_user) or not hmac.compare_digest(password, exp_pass):
        return _beta_basic_auth_unauthorized()

    return await call_next(request)


@app.on_event("startup")
def _startup() -> None:
    # In production (Render/Postgres), schema is managed via Alembic migrations (see `seo-agent-web/alembic.ini`).
    # Keep auto-create for local dev (SQLite) and opt-in environments only.
    if (not _safe_env("DATABASE_URL")) or _env_bool("SEO_AGENT_DB_AUTO_CREATE"):
        DB.create_tables()
    _init_sentry()
    _start_job_worker()
    _start_retention()


@app.on_event("shutdown")
def _shutdown() -> None:
    _WORKER_STOP.set()


def _normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def _safe_next_path(next_path: str | None) -> str:
    n = str(next_path or "").strip()
    if not n:
        return "/"
    if not n.startswith("/"):
        return "/"
    if n.startswith("//"):
        return "/"
    return n


def _env_bool_default(name: str, default: bool) -> bool:
    raw = _safe_env(name)
    if raw == "":
        return default
    return raw.lower() in {"1", "true", "yes", "y", "on"}


def _smtp_config() -> dict[str, Any] | None:
    host = _safe_env("SMTP_HOST")
    if not host:
        return None
    try:
        port = int(_safe_env("SMTP_PORT") or "587")
    except Exception:
        port = 587
    username = _safe_env("SMTP_USERNAME") or _safe_env("SMTP_USER")
    password = _safe_env("SMTP_PASSWORD")
    from_addr = _safe_env("SMTP_FROM") or username
    from_name = _safe_env("SMTP_FROM_NAME") or _safe_env("APP_NAME")
    from_name = str(from_name or "").strip()
    if not from_addr:
        return None

    use_ssl = _env_bool_default("SMTP_SSL", False)
    use_starttls = _env_bool_default("SMTP_STARTTLS", not use_ssl)
    timeout_s = 10.0
    raw_timeout = _safe_env("SMTP_TIMEOUT_SECONDS")
    if raw_timeout:
        try:
            timeout_s = float(raw_timeout)
        except Exception:
            pass
    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "from": from_addr,
        "from_name": from_name,
        "ssl": use_ssl,
        "starttls": use_starttls,
        "timeout_s": timeout_s,
    }


def _app_name() -> str:
    return _safe_env("APP_NAME") or "SEO Audit"


def _support_email() -> str:
    raw = _safe_env("SUPPORT_EMAIL") or _safe_env("SMTP_FROM") or "contact@noyaru.com"
    return str(raw or "").strip() or "contact@noyaru.com"


def _public_nav_items() -> list[dict[str, str]]:
    return [
        {"href": "/pricing", "label": "Pricing"},
        {"href": "/terms", "label": "CGU"},
        {"href": "/privacy", "label": "Confidentialité"},
        {"href": "/support", "label": "Support"},
        {"href": "/status", "label": "Statut"},
    ]


def _legal_version() -> str:
    return _safe_env("LEGAL_VERSION") or "0.1"


def _legal_updated_at() -> str:
    return _safe_env("LEGAL_UPDATED_AT") or datetime.utcnow().strftime("%Y-%m-%d")


def _smtp_send_email(*, to_addr: str, subject: str, body: str) -> None:
    cfg = _smtp_config()
    if not cfg:
        raise RuntimeError("smtp_not_configured")

    to_masked = _mask_email(to_addr)
    from_masked = _mask_email(str(cfg.get("from") or ""))
    host = str(cfg.get("host") or "")
    port = int(cfg.get("port") or 0)
    starttls = bool(cfg.get("starttls"))
    ssl = bool(cfg.get("ssl"))

    from_addr = str(cfg["from"])
    from_name = str(cfg.get("from_name") or "").strip()
    msg = EmailMessage()
    msg["From"] = formataddr((from_name, from_addr)) if from_name else from_addr
    msg["To"] = str(to_addr)
    msg["Subject"] = str(subject)
    msg.set_content(str(body))

    try:
        print(
            f"[MAIL] sending to={to_masked} from={from_masked} via={host}:{port} ssl={ssl} starttls={starttls}",
            flush=True,
        )
        if bool(cfg.get("ssl")):
            with smtplib.SMTP_SSL(str(cfg["host"]), int(cfg["port"]), timeout=float(cfg["timeout_s"])) as smtp:
                if cfg.get("username") and cfg.get("password"):
                    smtp.login(str(cfg["username"]), str(cfg["password"]))
                smtp.send_message(msg)
            print(f"[MAIL] sent to={to_masked} via={host}:{port}", flush=True)
            return

        with smtplib.SMTP(str(cfg["host"]), int(cfg["port"]), timeout=float(cfg["timeout_s"])) as smtp:
            smtp.ehlo()
            if bool(cfg.get("starttls")):
                smtp.starttls()
                smtp.ehlo()
            if cfg.get("username") and cfg.get("password"):
                smtp.login(str(cfg["username"]), str(cfg["password"]))
            smtp.send_message(msg)
        print(f"[MAIL] sent to={to_masked} via={host}:{port}", flush=True)
    except Exception as e:
        print(f"[MAIL] send error: {type(e).__name__}: {e}", flush=True)
        raise


def _sendgrid_api_key_from_smtp_cfg(cfg: dict[str, Any]) -> str:
    host = str(cfg.get("host") or "").strip().lower()
    username = str(cfg.get("username") or "").strip().lower()
    password = str(cfg.get("password") or "").strip()
    if host == "smtp.sendgrid.net" and username == "apikey" and password:
        return password
    return ""


def _sendgrid_send_email(
    *, api_key: str, to_addr: str, subject: str, body: str, from_addr: str, from_name: str = ""
) -> None:
    key = str(api_key or "").strip()
    if not key:
        raise RuntimeError("sendgrid_api_key_missing")

    to_masked = _mask_email(to_addr)
    from_masked = _mask_email(from_addr)
    try:
        print(f"[MAIL] sendgrid api sending to={to_masked} from={from_masked}", flush=True)
        from_obj: dict[str, str] = {"email": str(from_addr).strip()}
        if str(from_name or "").strip():
            from_obj["name"] = str(from_name).strip()

        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            json={
                "personalizations": [{"to": [{"email": str(to_addr).strip()}]}],
                "from": from_obj,
                "subject": str(subject),
                "content": [{"type": "text/plain", "value": str(body)}],
            },
            timeout=15.0,
        )
    except Exception as e:
        print(f"[MAIL] sendgrid api error: {type(e).__name__}: {e}", flush=True)
        raise

    if resp.status_code >= 400:
        detail = (resp.text or "").strip().replace("\n", " ")[:500]
        print(f"[MAIL] sendgrid api failed status={resp.status_code} detail={detail}", flush=True)
        raise RuntimeError(f"sendgrid_api_http_{resp.status_code}")

    print(f"[MAIL] sendgrid api accepted status={resp.status_code} to={to_masked}", flush=True)


def _send_email(*, to_addr: str, subject: str, body: str) -> None:
    """
    Prefer SendGrid HTTP API when the current SMTP config matches SendGrid.

    Render/other PaaS environments can block outbound SMTP ports; HTTPS is much more reliable.
    """
    cfg = _smtp_config()
    if not cfg:
        raise RuntimeError("smtp_not_configured")
    sg_key = _sendgrid_api_key_from_smtp_cfg(cfg)
    try:
        print(
            "[MAIL] dispatch "
            f"host={str(cfg.get('host') or '')}:{int(cfg.get('port') or 0)} "
            f"from={_mask_email(str(cfg.get('from') or ''))} "
            f"to={_mask_email(to_addr)} "
            f"sendgrid_api={bool(sg_key)}",
            flush=True,
        )
    except Exception:
        pass
    if sg_key:
        _sendgrid_send_email(
            api_key=sg_key,
            to_addr=to_addr,
            subject=subject,
            body=body,
            from_addr=str(cfg.get("from") or ""),
            from_name=str(cfg.get("from_name") or ""),
        )
        return

    _smtp_send_email(to_addr=to_addr, subject=subject, body=body)


_PASSWORD_RESET_TTL_DEFAULT_S = 60 * 60


def _password_reset_ttl_s() -> int:
    raw = _safe_env("PASSWORD_RESET_TTL_SECONDS")
    if raw:
        try:
            v = int(raw)
            return max(5 * 60, min(24 * 60 * 60, v))
        except Exception:
            pass
    return _PASSWORD_RESET_TTL_DEFAULT_S


def _dt_as_naive_utc(value: datetime | None) -> datetime | None:
    if not value:
        return None
    if getattr(value, "tzinfo", None) is None:
        return value
    try:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    except Exception:
        return value.replace(tzinfo=None)


def _password_reset_token_hash(token: str) -> str:
    raw = str(token or "").strip()
    if not raw:
        return ""
    pepper = _safe_env("SEO_AGENT_SECRET_KEY")
    if not pepper:
        raise RuntimeError("SEO_AGENT_SECRET_KEY missing")
    return hashlib.sha256(f"{pepper}:{raw}".encode("utf-8")).hexdigest()


def _issue_password_reset_token(db, *, user_id: str) -> tuple[str, datetime]:
    uid = str(user_id or "").strip()
    if not uid:
        raise ValueError("Missing user_id")

    ttl_s = _password_reset_ttl_s()
    now = datetime.utcnow()
    expires_at = now + dt.timedelta(seconds=int(ttl_s))

    # Invalidate previous tokens for this user (single active token at a time).
    try:
        db.execute(
            update(PasswordResetToken)
            .where(
                PasswordResetToken.user_id == uid,
                PasswordResetToken.used_at.is_(None),
            )
            .values(used_at=now)
        )
    except Exception:
        pass

    for _ in range(3):
        token = secrets.token_urlsafe(48)
        token_hash = _password_reset_token_hash(token)
        row = PasswordResetToken(user_id=uid, token_hash=token_hash, expires_at=expires_at)
        db.add(row)
        try:
            db.commit()
            return token, expires_at
        except IntegrityError:
            db.rollback()
            continue
    raise RuntimeError("reset_token_create_failed")


def _valid_password_reset_row(db, *, token: str) -> PasswordResetToken | None:
    h = _password_reset_token_hash(token)
    if not h:
        return None
    row = db.scalar(select(PasswordResetToken).where(PasswordResetToken.token_hash == h))
    if not row:
        return None
    if getattr(row, "used_at", None):
        return None
    now = datetime.utcnow()
    exp = _dt_as_naive_utc(getattr(row, "expires_at", None))
    if exp and exp <= now:
        return None
    return row


def _send_password_reset_email(*, to_email: str, reset_url: str, expires_at: datetime) -> None:
    try:
        print(
            f"[MAIL] reset compose to={_mask_email(to_email)} url_host={urlsplit(str(reset_url)).netloc}",
            flush=True,
        )
    except Exception:
        pass
    exp = _dt_as_naive_utc(expires_at) or datetime.utcnow()
    ttl_s = max(60, int(((_dt_as_naive_utc(expires_at) or exp) - datetime.utcnow()).total_seconds()))
    ttl_minutes = max(1, int(math.ceil(float(ttl_s) / 60.0)))

    app_name = _safe_env("APP_NAME") or "SEO Agent"
    subject_tpl = _safe_env("PASSWORD_RESET_EMAIL_SUBJECT")
    if subject_tpl:
        subject = subject_tpl.replace("{app}", app_name).replace("{brand}", app_name).strip()
    else:
        subject = f"Réinitialisation du mot de passe — {app_name}"
    if not subject:
        subject = f"Réinitialisation du mot de passe — {app_name}"
    body = "\n".join(
        [
            "Bonjour,",
            "",
            "Pour réinitialiser votre mot de passe, cliquez sur ce lien :",
            str(reset_url).strip(),
            "",
            f"Ce lien est valable {ttl_minutes} min.",
            "",
            "Si vous n’êtes pas à l’origine de cette demande, ignorez cet email.",
            "",
        ]
    )
    _send_email(to_addr=str(to_email).strip(), subject=subject, body=body)


_EMAIL_VERIFY_TTL_DEFAULT_S = 60 * 60 * 24


def _email_verify_ttl_s() -> int:
    raw = _safe_env("EMAIL_VERIFY_TTL_SECONDS")
    if raw:
        try:
            v = int(raw)
            return max(5 * 60, min(7 * 24 * 60 * 60, v))
        except Exception:
            pass
    return _EMAIL_VERIFY_TTL_DEFAULT_S


def _email_verification_enabled() -> bool:
    if _env_bool("EMAIL_VERIFICATION_DISABLED"):
        return False
    return bool(_smtp_config())


def _email_verify_token_hash(token: str) -> str:
    raw = str(token or "").strip()
    if not raw:
        return ""
    pepper = _safe_env("SEO_AGENT_SECRET_KEY")
    if not pepper:
        raise RuntimeError("SEO_AGENT_SECRET_KEY missing")
    return hashlib.sha256(f"{pepper}:email_verify:{raw}".encode("utf-8")).hexdigest()


def _user_email_verified(db, *, user_id: str) -> bool:
    uid = str(user_id or "").strip()
    if not uid:
        return False
    row = db.scalar(
        select(EmailVerificationToken.id).where(
            EmailVerificationToken.user_id == uid,
            EmailVerificationToken.used_at.is_not(None),
        )
    )
    return bool(row)


def _mark_user_email_verified(db, *, user_id: str) -> None:
    uid = str(user_id or "").strip()
    if not uid:
        return
    if _user_email_verified(db, user_id=uid):
        return
    now = datetime.utcnow()
    for _ in range(3):
        token = secrets.token_urlsafe(48)
        token_hash = _email_verify_token_hash(token)
        row = EmailVerificationToken(user_id=uid, token_hash=token_hash, expires_at=now, used_at=now)
        db.add(row)
        try:
            db.commit()
            return
        except IntegrityError:
            db.rollback()
            continue


def _issue_email_verification_token(db, *, user_id: str) -> tuple[str, datetime]:
    uid = str(user_id or "").strip()
    if not uid:
        raise ValueError("Missing user_id")

    ttl_s = _email_verify_ttl_s()
    now = datetime.utcnow()
    expires_at = now + dt.timedelta(seconds=int(ttl_s))

    for _ in range(3):
        token = secrets.token_urlsafe(48)
        token_hash = _email_verify_token_hash(token)
        row = EmailVerificationToken(user_id=uid, token_hash=token_hash, expires_at=expires_at)
        db.add(row)
        try:
            db.commit()
            return token, expires_at
        except IntegrityError:
            db.rollback()
            continue
    raise RuntimeError("email_verify_token_create_failed")


def _valid_email_verification_row(db, *, token: str) -> EmailVerificationToken | None:
    h = _email_verify_token_hash(token)
    if not h:
        return None
    row = db.scalar(select(EmailVerificationToken).where(EmailVerificationToken.token_hash == h))
    if not row:
        return None
    if getattr(row, "used_at", None):
        return None
    now = datetime.utcnow()
    exp = _dt_as_naive_utc(getattr(row, "expires_at", None))
    if exp and exp <= now:
        return None
    return row


def _send_email_verification_email(*, to_email: str, verify_url: str, expires_at: datetime) -> None:
    exp = _dt_as_naive_utc(expires_at) or datetime.utcnow()
    ttl_s = max(60, int((exp - datetime.utcnow()).total_seconds()))
    ttl_hours = max(1, int(math.ceil(float(ttl_s) / 3600.0)))

    app_name = _safe_env("APP_NAME") or "SEO Agent"
    subject_tpl = _safe_env("EMAIL_VERIFY_EMAIL_SUBJECT")
    if subject_tpl:
        subject = subject_tpl.replace("{app}", app_name).replace("{brand}", app_name).strip()
    else:
        subject = f"Vérifie ton email — {app_name}"
    if not subject:
        subject = f"Vérifie ton email — {app_name}"

    body = "\n".join(
        [
            "Bonjour,",
            "",
            "Pour confirmer ton email, clique sur ce lien :",
            str(verify_url).strip(),
            "",
            f"Ce lien est valable {ttl_hours} h.",
            "",
            "Si tu n’es pas à l’origine de cette demande, ignore cet email.",
            "",
        ]
    )
    _send_email(to_addr=str(to_email).strip(), subject=subject, body=body)


def _request_is_secure(request: Request) -> bool:
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip().lower()
    return proto == "https"


def _set_lax_cookie(
    response: Response,
    *,
    request: Request,
    name: str,
    value: str,
    max_age: int = auth.SESSION_TTL_S,
    httponly: bool = True,
) -> None:
    response.set_cookie(
        name,
        value,
        max_age=max_age,
        httponly=httponly,
        samesite="lax",
        secure=_request_is_secure(request),
        path="/",
    )


def _sanitize_csrf_token(value: str | None) -> str:
    token = str(value or "").strip()
    if not token:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9._~-]{20,256}", token):
        return ""
    return token


def _issue_csrf_token() -> str:
    return secrets.token_urlsafe(32)


def _make_replay_receive(body: bytes) -> Any:
    sent = False

    async def receive() -> dict[str, Any]:
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return receive


async def _buffer_body_for_downstream(request: Request) -> bytes:
    """
    Starlette/FastAPI `@app.middleware('http')` is implemented on top of `BaseHTTPMiddleware`.
    If we read the request body in middleware (ex: `await request.form()`), the downstream
    route handler will see an empty body and all `Form(...)` fields will be empty.

    We buffer the body once and replace `request._receive` with a replayable receive so the
    rest of the app can read it normally.
    """
    body = await request.body()
    request._receive = _make_replay_receive(body)  # type: ignore[attr-defined]
    return body


async def _request_csrf_submission_token(request: Request) -> str:
    header_token = _sanitize_csrf_token(request.headers.get(_CSRF_HEADER_NAME))
    if header_token:
        return header_token
    content_type = (request.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
    if content_type in {"application/x-www-form-urlencoded", "multipart/form-data"}:
        body = await _buffer_body_for_downstream(request)
        if content_type == "application/x-www-form-urlencoded":
            try:
                data = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
            except Exception:
                return ""
            values = data.get(_CSRF_FORM_FIELD) or []
            return _sanitize_csrf_token(values[0] if values else "")

        # multipart/form-data (e.g. file uploads): parse via a cloned Request replaying the same buffered body.
        try:
            clone = StarletteRequest(request.scope, receive=_make_replay_receive(body))
            form = await clone.form()
        except Exception:
            return ""
        return _sanitize_csrf_token(form.get(_CSRF_FORM_FIELD))
    return ""


def _csrf_failure_response(request: Request, *, token_to_set: str = "") -> Response:
    message = "CSRF invalide. Recharge la page puis réessaie."
    if request.url.path.startswith("/api/") or _client_wants_json(request):
        response: Response = JSONResponse({"ok": False, "error": message}, status_code=403)
    else:
        response = HTMLResponse(message, status_code=403)
    response.headers["Cache-Control"] = "no-store"
    if token_to_set:
        _set_lax_cookie(response, request=request, name=_CSRF_COOKIE_NAME, value=token_to_set)
    return response


def _request_client_ip(request: Request) -> str:
    candidates: list[str] = []
    xff = str(request.headers.get("x-forwarded-for") or "").strip()
    if xff:
        candidates.extend(part.strip() for part in xff.split(","))
    xri = str(request.headers.get("x-real-ip") or "").strip()
    if xri:
        candidates.append(xri)
    if request.client and request.client.host:
        candidates.append(str(request.client.host))
    for raw in candidates:
        try:
            return str(ipaddress.ip_address(raw))
        except Exception:
            continue
    return ""


def _rate_limit_retry_after(*, bucket: str, subject: str, limit: int, window_s: int) -> int | None:
    normalized_bucket = str(bucket or "").strip()
    normalized_subject = str(subject or "").strip()
    if not normalized_bucket or not normalized_subject or limit <= 0 or window_s <= 0:
        return None
    key = f"{normalized_bucket}:{normalized_subject}"
    now = time.monotonic()
    with _RATE_LIMIT_LOCK:
        queue = _RATE_LIMIT_BUCKETS.get(key)
        if queue is None:
            queue = deque()
            _RATE_LIMIT_BUCKETS[key] = queue
        cutoff = now - float(window_s)
        while queue and queue[0] <= cutoff:
            queue.popleft()
        if len(queue) >= int(limit):
            retry_after = max(1, int(math.ceil(float(window_s) - (now - queue[0]))))
            return retry_after
        queue.append(now)
    return None


def _format_retry_after(retry_after_s: int) -> str:
    retry_after = max(1, int(retry_after_s))
    if retry_after >= 60:
        minutes = int(math.ceil(retry_after / 60.0))
        return f"{minutes} min"
    return f"{retry_after}s"


def _audit_log(
    request: Request,
    *,
    action: str,
    status: str = "ok",
    user: User | None = None,
    actor_email: str = "",
    target_type: str = "",
    target_id: str = "",
    meta: dict[str, Any] | None = None,
) -> None:
    action_value = str(action or "").strip()
    if not action_value:
        return
    actor = user or getattr(request.state, "user", None)
    actor_id = str(getattr(actor, "id", "") or "").strip() or None
    email_value = str(actor_email or getattr(actor, "email", "") or "").strip().lower() or None
    payload = dict(meta) if isinstance(meta, dict) else {}
    row = AuditLog(
        actor_user_id=actor_id,
        actor_email=email_value,
        action=action_value[:128],
        status=(str(status or "").strip().lower() or "ok")[:32],
        target_type=(str(target_type or "").strip() or None),
        target_id=(str(target_id or "").strip() or None),
        ip_address=_request_client_ip(request) or None,
        user_agent=(str(request.headers.get("user-agent") or "").strip()[:512] or None),
        meta=payload,
    )
    try:
        with DB.session() as db:
            db.add(row)
            db.commit()
    except Exception as e:
        print(f"[AUDIT] {action_value} error: {type(e).__name__}: {e}")


def _path_with_flash(path: str, *, msg: str | None = None, err: str | None = None) -> str:
    target = _safe_next_path(path)
    parts = urlsplit(target)
    params = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k not in {"msg", "err"}]
    if msg:
        params.append(("msg", str(msg)))
    if err:
        params.append(("err", str(err)))
    query = urlencode(params)
    return urlunsplit(("", "", parts.path or "/", query, parts.fragment))


def _load_user_from_session(request: Request) -> User | None:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        return None
    token = request.cookies.get(auth.SESSION_COOKIE_NAME)
    if not token:
        return None
    payload = auth.parse_session_token(token, secret=secret)
    if not payload:
        return None
    uid = str(payload.get("uid") or "").strip()
    if not uid:
        return None
    with DB.session() as db:
        user = db.get(User, uid)
        return user


def _require_admin(request: Request) -> User:
    user = getattr(request.state, "user", None)
    if not user or not bool(getattr(user, "is_admin", False)):
        raise HTTPException(status_code=403, detail="admin_required")
    return user


def _system_settings_owner_email() -> str:
    return _normalize_email(_safe_env("SYSTEM_SETTINGS_OWNER_EMAIL") or _safe_env("BOOTSTRAP_ADMIN_EMAIL"))


def _user_can_access_system_settings(user: User | None) -> bool:
    if not user:
        return False
    owner_email = _system_settings_owner_email()
    user_email = _normalize_email(str(getattr(user, "email", "") or ""))
    if owner_email:
        return user_email == owner_email
    return bool(getattr(user, "is_admin", False))


def _require_system_owner(request: Request) -> User:
    user = getattr(request.state, "user", None)
    if not _user_can_access_system_settings(user):
        raise HTTPException(status_code=403, detail="system_owner_required")
    return user


@app.middleware("http")
async def session_auth_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    request.state.user = _load_user_from_session(request)

    # Enforce email verification (when enabled) by treating unverified sessions as unauthenticated.
    if request.state.user and _email_verification_enabled():
        try:
            with DB.session() as db:
                verified = _user_email_verified(db, user_id=str(getattr(request.state.user, "id", "") or ""))
        except Exception:
            verified = False
        if not verified:
            request.state.user = None

    request.state.can_access_system_settings = _user_can_access_system_settings(request.state.user)

    path = request.url.path
    if path.startswith("/static/") or path in {
        "/healthz",
        "/",
        "/pricing",
        "/terms",
        "/privacy",
        "/support",
        "/status",
        "/auth/login",
        "/auth/signup",
        "/auth/forgot",
        "/auth/reset",
        "/auth/google/start",
        "/auth/google/callback",
        "/auth/verify",
        "/auth/verify/resend",
        "/stripe/webhook",
    }:
        return await call_next(request)

    if not request.state.user:
        if path.startswith("/api/"):
            return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)
        next_url = path + (("?" + request.url.query) if request.url.query else "")
        return RedirectResponse(url=f"/auth/login?next={quote(next_url)}", status_code=303)

    return await call_next(request)


@app.middleware("http")
async def csrf_middleware(request: Request, call_next):  # type: ignore[no-untyped-def]
    cookie_token = _sanitize_csrf_token(request.cookies.get(_CSRF_COOKIE_NAME))
    csrf_token = cookie_token or _issue_csrf_token()
    request.state.csrf_token = csrf_token
    request.state.csrf_cookie_name = _CSRF_COOKIE_NAME
    needs_cookie_set = csrf_token != cookie_token

    if request.method.upper() not in _CSRF_SAFE_METHODS and request.url.path not in _CSRF_EXEMPT_PATHS:
        submitted_token = await _request_csrf_submission_token(request)
        if not cookie_token or not submitted_token or not hmac.compare_digest(cookie_token, submitted_token):
            return _csrf_failure_response(request, token_to_set=(csrf_token if needs_cookie_set else ""))

    response = await call_next(request)
    if needs_cookie_set:
        _set_lax_cookie(response, request=request, name=_CSRF_COOKIE_NAME, value=csrf_token)
    return response


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


_SETTINGS_ENV_KEYS: dict[str, dict[str, Any]] = {
    "GITHUB_TOKEN": {
        "label": "GitHub",
        "hint": "Token d’accès personnel (PAT)",
        "group": "Intégrations",
        "order": 10,
        "editable": True,
        "help": {
            "title": "GitHub — Token d’accès personnel (PAT)",
            "steps": [
                "Ouvre GitHub → Settings → Developer settings → Personal access tokens.",
                "Génère un nouveau token (choisis une expiration).",
                "Copie le token (il n’est affiché qu’une seule fois).",
                "Dans cette page: clique « Configurer », colle la valeur puis « Enregistrer ».",
            ],
            "links": [{"label": "Ouvrir la page des tokens GitHub", "url": "https://github.com/settings/tokens"}],
        },
    },
    "NETLIFY_TOKEN": {
        "label": "Netlify",
        "hint": "Token d’accès personnel",
        "group": "Intégrations",
        "order": 20,
        "editable": True,
        "help": {
            "title": "Netlify — Token d’accès personnel",
            "steps": [
                "Ouvre Netlify → User settings → Applications → Personal access tokens.",
                "Crée un nouveau token et copie-le.",
                "Dans cette page: clique « Configurer », colle la valeur puis « Enregistrer ».",
            ],
            "links": [
                {
                    "label": "Ouvrir Netlify · Personal access tokens",
                    "url": "https://app.netlify.com/user/applications#personal-access-tokens",
                }
            ],
        },
    },
    "BING_WEBMASTER_API_KEY": {
        "label": "Bing",
        "hint": "Bing Webmaster Tools · clé API (optionnel)",
        "group": "Intégrations",
        "order": 25,
        "editable": True,
        "help": {
            "title": "Bing Webmaster Tools — API key",
            "steps": [
                "Ouvre Bing Webmaster Tools et connecte-toi.",
                "Va dans Settings → API Access.",
                "Génère une clé API et copie-la.",
                "Dans cette page: clique « Configurer », colle la valeur puis « Enregistrer ».",
                "Pour les backlinks, Bing ne fournit pas d’API publique : utilise l’export CSV dans l’interface.",
            ],
            "links": [{"label": "Ouvrir Bing Webmaster Tools", "url": "https://www.bing.com/webmasters/"}],
        },
    },
    "GITHUB_OAUTH_CLIENT_ID": {
        "label": "GitHub OAuth — Client ID",
        "hint": "Application OAuth GitHub",
        "group": "GitHub",
        "order": 11,
        "editable": True,
        "help": {
            "title": "GitHub OAuth — Client ID / Client secret",
            "steps": [
                "Dans GitHub, crée une OAuth App.",
                "Ajoute comme Homepage URL: <PUBLIC_BASE_URL>.",
                "Ajoute comme Authorization callback URL: <PUBLIC_BASE_URL>/oauth/github/callback.",
                "Copie le client ID et le client secret ici.",
            ],
            "links": [{"label": "GitHub · OAuth Apps", "url": "https://github.com/settings/developers"}],
        },
    },
    "GITHUB_OAUTH_CLIENT_SECRET": {
        "label": "GitHub OAuth — Client secret",
        "hint": "Secret OAuth GitHub",
        "group": "GitHub",
        "order": 12,
        "editable": True,
    },
    "GITHUB_OAUTH_REDIRECT_URI": {
        "label": "GitHub OAuth redirect URI",
        "hint": "override (optionnel)",
        "group": "GitHub",
        "order": 13,
        "editable": True,
    },
    "NETLIFY_OAUTH_CLIENT_ID": {
        "label": "Netlify OAuth — Client ID",
        "hint": "Application OAuth Netlify",
        "group": "Netlify",
        "order": 21,
        "editable": True,
        "help": {
            "title": "Netlify OAuth — Client ID",
            "steps": [
                "Dans Netlify, crée une application OAuth.",
                "Ajoute comme Redirect URI: <PUBLIC_BASE_URL>/oauth/netlify/callback.",
                "Copie le client ID ici.",
            ],
            "links": [{"label": "Netlify · OAuth applications", "url": "https://app.netlify.com/user/applications"}],
        },
    },
    "NETLIFY_OAUTH_REDIRECT_URI": {
        "label": "Netlify OAuth redirect URI",
        "hint": "override (optionnel)",
        "group": "Netlify",
        "order": 22,
        "editable": True,
    },
    "BING_OAUTH_CLIENT_ID": {
        "label": "Bing OAuth — Client ID",
        "hint": "Application OAuth Bing Webmaster",
        "group": "Bing",
        "order": 26,
        "editable": True,
        "help": {
            "title": "Bing Webmaster OAuth — Client ID / Client secret",
            "steps": [
                "Dans Bing Webmaster Tools, crée une application OAuth.",
                "Ajoute comme Redirect URI: <PUBLIC_BASE_URL>/oauth/bing/callback.",
                "Copie le client ID et le client secret ici.",
            ],
            "links": [{"label": "Bing Webmaster Tools", "url": "https://www.bing.com/webmasters/"}],
        },
    },
    "BING_OAUTH_CLIENT_SECRET": {
        "label": "Bing OAuth — Client secret",
        "hint": "Secret OAuth Bing Webmaster",
        "group": "Bing",
        "order": 27,
        "editable": True,
    },
    "BING_OAUTH_REDIRECT_URI": {
        "label": "Bing OAuth redirect URI",
        "hint": "override (optionnel)",
        "group": "Bing",
        "order": 28,
        "editable": True,
    },
    "PAGESPEED_API_KEY": {
        "label": "PageSpeed",
        "hint": "Google PageSpeed Insights · clé API",
        "group": "Intégrations",
        "order": 30,
        "editable": True,
        "help": {
            "title": "Google PageSpeed Insights — API key",
            "steps": [
                "Dans Google Cloud Console, active l’API « PageSpeed Insights API » sur ton projet.",
                "Va dans APIs & Services → Credentials → Create credentials → API key.",
                "Copie la clé (optionnel: restreins-la si nécessaire).",
                "Dans cette page: clique « Configurer », colle la valeur puis « Enregistrer ».",
            ],
            "links": [{"label": "Ouvrir Google Cloud Console · Credentials", "url": "https://console.cloud.google.com/apis/credentials"}],
        },
    },
    "SEO_AUDIT_ASSISTANT_PROVIDER": {
        "label": "Assistant",
        "hint": "Fournisseur (auto | gemini | openai)",
        "group": "IA",
        "order": 10,
        "editable": True,
        "help": {
            "title": "Assistant — choix du fournisseur",
            "steps": [
                "Valeurs possibles: auto, gemini, openai.",
                "En mode auto: Gemini est utilisé si une clé est configurée, sinon OpenAI.",
            ],
        },
    },
    "OPENAI_API_KEY": {
        "label": "OpenAI",
        "hint": "Clé API",
        "group": "IA",
        "order": 20,
        "editable": True,
        "help": {
            "title": "OpenAI — clé API",
            "steps": [
                "Crée une clé dans ton tableau de bord OpenAI.",
                "Dans cette page: clique « Configurer », colle la valeur puis « Enregistrer ».",
            ],
        },
    },
    "SEO_AUDIT_ASSISTANT_OPENAI_MODEL": {
        "label": "Modèle OpenAI",
        "hint": "ex: gpt-5.1-mini",
        "group": "IA",
        "order": 21,
        "editable": True,
    },
    "GOOGLE_GEMINI_API_KEY": {
        "label": "Gemini",
        "hint": "Google AI Studio · clé API",
        "group": "IA",
        "order": 30,
        "editable": True,
        "help": {
            "title": "Gemini — clé API (Google AI Studio)",
            "steps": [
                "Crée une clé dans Google AI Studio.",
                "Dans cette page: clique « Configurer », colle la valeur puis « Enregistrer ».",
            ],
        },
    },
    "SEO_AUDIT_ASSISTANT_GEMINI_MODEL": {
        "label": "Modèle Gemini",
        "hint": "ex: gemini-1.5-flash",
        "group": "IA",
        "order": 31,
        "editable": True,
    },
    "GOOGLE_APPLICATION_CREDENTIALS": {
        "label": "Google Search Console (GSC)",
        "hint": "Chemin du JSON (service account)",
        "group": "Google",
        "order": 60,
        "editable": True,
        "help": {
            "title": "Google Search Console — service account",
            "steps": [
                "Dans Google Cloud Console, active l’API « Google Search Console API ».",
                "Crée un Service Account puis génère une clé au format JSON (à télécharger).",
                "Place le fichier JSON sur cette machine (ex: racine du projet ou seo-agent-web/data).",
                "Dans GSC, ajoute l’email du service account en tant qu’utilisateur de la propriété.",
                "Ici: clique « Configurer », choisis le fichier, « Enregistrer », puis « Tester ».",
            ],
            "links": [{"label": "Ouvrir Google Cloud Console · Service Accounts", "url": "https://console.cloud.google.com/iam-admin/serviceaccounts"}],
        },
    },
    "GOOGLE_OAUTH_CLIENT_ID": {
        "label": "Google OAuth — Client ID",
        "hint": "Client ID OAuth 2.0 (appli web)",
        "group": "Google",
        "order": 40,
        "editable": True,
        "help": {
            "title": "Google OAuth (GSC) — Client ID / Client secret",
            "steps": [
                "Dans Google Cloud Console, active l’API « Google Search Console API ».",
                "Configure l’écran de consentement OAuth.",
                "Crée un OAuth Client ID (type: Web application).",
                "Ajoute l’URL de callback: <PUBLIC_BASE_URL>/oauth/google/callback (ou définis GOOGLE_OAUTH_REDIRECT_URI).",
                "Copie le client_id et le client_secret et colle-les ici.",
            ],
            "links": [{"label": "Ouvrir Google Cloud Console · Credentials", "url": "https://console.cloud.google.com/apis/credentials"}],
        },
    },
    "GOOGLE_OAUTH_CLIENT_SECRET": {
        "label": "Google OAuth — Client secret",
        "hint": "Client secret OAuth 2.0",
        "group": "Google",
        "order": 41,
        "editable": True,
    },
    "PUBLIC_BASE_URL": {
        "label": "URL publique (PUBLIC_BASE_URL)",
        "hint": "ex: https://app.example.com (pour OAuth)",
        "group": "Google",
        "order": 42,
        "editable": True,
        "help": {
            "title": "PUBLIC_BASE_URL",
            "steps": [
                "C’est l’URL publique de ton SaaS (celle que tes clients utilisent).",
                "Elle sert à construire l’URL OAuth de callback si GOOGLE_OAUTH_REDIRECT_URI n’est pas défini.",
            ],
        },
    },
    "GOOGLE_OAUTH_REDIRECT_URI": {
        "label": "Google OAuth redirect URI",
        "hint": "override (optionnel)",
        "group": "Google",
        "order": 43,
        "editable": True,
        "help": {
            "title": "GOOGLE_OAUTH_REDIRECT_URI",
            "steps": [
                "Optionnel. Si défini, remplace <PUBLIC_BASE_URL>/oauth/google/callback.",
                "Utile si tu es derrière un proxy/CDN et que la détection auto ne convient pas.",
            ],
        },
    },
    "SEO_AGENT_SECRET_KEY": {
        "label": "App secret",
        "hint": "Signature OAuth state (requis)",
        "group": "Google",
        "order": 44,
        "editable": True,
        "help": {
            "title": "SEO_AGENT_SECRET_KEY",
            "steps": [
                "Secret utilisé pour signer le paramètre OAuth « state » (anti-CSRF).",
                "Définis une valeur longue et aléatoire (32+ chars).",
            ],
        },
    },
    "APP_NAME": {
        "label": "App — Nom",
        "hint": "ex: Noyaru",
        "group": "Emails",
        "order": 9,
        "editable": True,
    },
    "SMTP_HOST": {
        "label": "SMTP — Host",
        "hint": "ex: smtp.mailgun.org",
        "group": "Emails",
        "order": 10,
        "editable": True,
    },
    "SMTP_PORT": {
        "label": "SMTP — Port",
        "hint": "ex: 587",
        "group": "Emails",
        "order": 11,
        "editable": True,
    },
    "SMTP_USERNAME": {
        "label": "SMTP — Username",
        "hint": "Identifiant SMTP",
        "group": "Emails",
        "order": 12,
        "editable": True,
    },
    "SMTP_PASSWORD": {
        "label": "SMTP — Password",
        "hint": "Mot de passe SMTP",
        "group": "Emails",
        "order": 13,
        "editable": True,
    },
    "SMTP_FROM": {
        "label": "SMTP — From",
        "hint": "ex: no-reply@ton-domaine.com",
        "group": "Emails",
        "order": 14,
        "editable": True,
    },
    "SMTP_FROM_NAME": {
        "label": "SMTP — From name",
        "hint": "Nom d’expéditeur (optionnel)",
        "group": "Emails",
        "order": 14.5,
        "editable": True,
    },
    "SMTP_STARTTLS": {
        "label": "SMTP — STARTTLS",
        "hint": "true/false",
        "group": "Emails",
        "order": 15,
        "editable": True,
    },
    "SMTP_SSL": {
        "label": "SMTP — SSL",
        "hint": "true/false (port 465)",
        "group": "Emails",
        "order": 16,
        "editable": True,
    },
    "SMTP_TIMEOUT_SECONDS": {
        "label": "SMTP — Timeout",
        "hint": "ex: 10",
        "group": "Emails",
        "order": 17,
        "editable": True,
    },
    "EMAIL_VERIFICATION_DISABLED": {
        "label": "Email verify — Disabled",
        "hint": "true/false",
        "group": "Emails",
        "order": 17.5,
        "editable": True,
    },
    "EMAIL_VERIFY_TTL_SECONDS": {
        "label": "Email verify — TTL",
        "hint": "Durée lien (secondes)",
        "group": "Emails",
        "order": 17.6,
        "editable": True,
    },
    "EMAIL_VERIFY_EMAIL_SUBJECT": {
        "label": "Email verify — Sujet",
        "hint": "ex: Vérifie ton email — {app}",
        "group": "Emails",
        "order": 17.7,
        "editable": True,
    },
    "PASSWORD_RESET_TTL_SECONDS": {
        "label": "Reset password — TTL",
        "hint": "Durée lien (secondes)",
        "group": "Emails",
        "order": 18,
        "editable": True,
    },
    "PASSWORD_RESET_EMAIL_SUBJECT": {
        "label": "Reset password — Sujet",
        "hint": "ex: Réinitialisation du mot de passe — {app}",
        "group": "Emails",
        "order": 19,
        "editable": True,
    },
}

_INTERNAL_SETTINGS_KEYS: set[str] = {
    "SEO_AUDIT_ASSISTANT_PROVIDER",
    "OPENAI_API_KEY",
    "SEO_AUDIT_ASSISTANT_OPENAI_MODEL",
    "GOOGLE_GEMINI_API_KEY",
    "SEO_AUDIT_ASSISTANT_GEMINI_MODEL",
}


@app.get("/auth/forgot", response_class=HTMLResponse)
def auth_forgot(request: Request, next: str | None = None, msg: str | None = None, err: str | None = None) -> Response:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)

    resp = templates.TemplateResponse(
        "auth_forgot.html",
        {
            "request": request,
            "next": n,
            "next_q": quote(n),
            "msg": str(msg or "").strip(),
            "err": str(err or "").strip(),
            "email": "",
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/auth/forgot")
def auth_forgot_submit(
    request: Request,
    email: str = Form(default=""),
    next: str = Form(default="/"),
) -> Response:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)

    e = _normalize_email(email)

    def _forgot_error(message: str, status_code: int = 400) -> Response:
        resp = templates.TemplateResponse(
            "auth_forgot.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "msg": "",
                "err": message,
                "email": e,
            },
            status_code=status_code,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    ip = _request_client_ip(request) or "unknown"
    retry_ip = _rate_limit_retry_after(bucket="auth_forgot_ip", subject=ip, limit=20, window_s=60 * 60)
    retry_email = _rate_limit_retry_after(
        bucket="auth_forgot_email", subject=(e or "missing"), limit=10, window_s=60 * 60
    )
    retry_after = max(v for v in [retry_ip, retry_email] if isinstance(v, int)) if any(
        isinstance(v, int) for v in [retry_ip, retry_email]
    ) else None
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="auth.forgot",
            status="rate_limited",
            actor_email=e,
            meta={"retry_after_s": retry_after},
        )
        return _forgot_error(f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}.", 429)

    if not e or "@" not in e or len(e) > 320:
        return _forgot_error("Email invalide.", 400)

    if not _smtp_config():
        _audit_log(
            request,
            action="auth.forgot",
            status="smtp_not_configured",
            actor_email=e,
        )
        return _forgot_error("Réinitialisation par email non configurée. Contacte le support.", 503)

    print(f"[MAIL] forgot request email={_mask_email(e)} ip={ip}", flush=True)

    public_msg = "Si un compte existe, un email de réinitialisation a été envoyé."
    with DB.session() as db:
        row = db.scalar(select(User).where(User.email == e))
        if row:
            print(f"[MAIL] forgot user_found=1 email={_mask_email(e)}", flush=True)
            try:
                token, expires_at = _issue_password_reset_token(db, user_id=str(row.id))
            except Exception as exc:
                _audit_log(
                    request,
                    action="auth.forgot",
                    status="token_error",
                    actor_email=e,
                    target_type="user",
                    target_id=str(getattr(row, "id", "") or ""),
                    meta={"error": f"{type(exc).__name__}: {str(exc)[:180]}"},
                )
                return _forgot_error("Erreur lors de la génération du lien. Réessaie plus tard.", 500)

            reset_url = f"{_public_base_url(request)}/auth/reset?{urlencode({'token': token, 'next': n})}"
            try:
                _send_password_reset_email(
                    to_email=str(getattr(row, "email", "") or e),
                    reset_url=reset_url,
                    expires_at=expires_at,
                )
            except Exception as exc:
                try:
                    print(f"[MAIL] forgot send_error: {type(exc).__name__}: {str(exc)[:500]}", flush=True)
                except Exception:
                    pass
                _audit_log(
                    request,
                    action="auth.forgot",
                    status="send_error",
                    actor_email=e,
                    target_type="user",
                    target_id=str(getattr(row, "id", "") or ""),
                    meta={"error": f"{type(exc).__name__}: {str(exc)[:180]}"},
                )
                return _forgot_error("Email non envoyé (erreur serveur). Vérifie la config SendGrid/SMTP.", 503)
            _audit_log(
                request,
                action="auth.forgot",
                status="ok",
                actor_email=e,
                target_type="user",
                target_id=str(getattr(row, "id", "") or ""),
            )
        else:
            print(f"[MAIL] forgot user_found=0 email={_mask_email(e)}", flush=True)
            _audit_log(request, action="auth.forgot", status="ok", actor_email=e, meta={"note": "email_not_found"})

    return RedirectResponse(url=_path_with_flash(f"/auth/forgot?next={quote(n)}", msg=public_msg), status_code=303)


@app.get("/auth/reset", response_class=HTMLResponse)
def auth_reset(request: Request, token: str | None = None, next: str | None = None) -> Response:
    t = str(token or "").strip()
    n = _safe_next_path(next)
    err = ""
    ok = False

    if not t:
        err = "Lien invalide."
    else:
        with DB.session() as db:
            ok = _valid_password_reset_row(db, token=t) is not None
        if not ok:
            err = "Lien invalide ou expiré."

    resp = templates.TemplateResponse(
        "auth_reset.html",
        {
            "request": request,
            "next": n,
            "next_q": quote(n),
            "token": t,
            "ok": ok,
            "err": err,
        },
        status_code=(200 if ok else 400),
    )
    resp.headers["Cache-Control"] = "no-store"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp


@app.post("/auth/reset")
def auth_reset_submit(
    request: Request,
    token: str = Form(default=""),
    password: str = Form(default=""),
    password2: str = Form(default=""),
    next: str = Form(default="/"),
) -> Response:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        raise HTTPException(status_code=500, detail="SEO_AGENT_SECRET_KEY missing")

    t = str(token or "").strip()
    n = _safe_next_path(next)

    def _reset_error(message: str, status_code: int = 400, *, ok: bool = True) -> Response:
        resp = templates.TemplateResponse(
            "auth_reset.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "token": t,
                "ok": ok,
                "err": message,
            },
            status_code=status_code,
        )
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    if not t:
        return _reset_error("Lien invalide.", 400, ok=False)
    if len(password or "") < 10:
        return _reset_error("Mot de passe trop court (min 10).", 400, ok=True)
    if password != password2:
        return _reset_error("Les mots de passe ne correspondent pas.", 400, ok=True)

    now = datetime.utcnow()
    uid = ""
    email_out = ""
    with DB.session() as db:
        row = _valid_password_reset_row(db, token=t)
        if not row:
            _audit_log(request, action="auth.reset_password", status="invalid_token")
            return _reset_error("Lien invalide ou expiré.", 400, ok=False)
        user = db.get(User, str(getattr(row, "user_id", "") or ""))
        if not user:
            _audit_log(request, action="auth.reset_password", status="user_missing")
            return _reset_error("Compte introuvable.", 400, ok=False)

        user.password_hash = auth.hash_password(password)
        row.used_at = now
        db.add(user)
        db.add(row)
        db.commit()
        uid = str(getattr(user, "id", "") or "")
        email_out = str(getattr(user, "email", "") or "")

    if not uid:
        return _reset_error("Erreur serveur.", 500, ok=False)

    token_out = auth.make_session_token(user_id=uid, secret=secret)
    resp = RedirectResponse(url=n, status_code=303)
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    secure_cookie = proto == "https"
    resp.set_cookie(
        auth.SESSION_COOKIE_NAME,
        token_out,
        max_age=auth.SESSION_TTL_S,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )
    _audit_log(request, action="auth.reset_password", status="ok", actor_email=email_out, target_type="user", target_id=uid)
    return resp


@app.get("/auth/verify/resend", response_class=HTMLResponse)
def auth_verify_resend(
    request: Request,
    next: str | None = None,
    email: str | None = None,
    msg: str | None = None,
    err: str | None = None,
) -> Response:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)

    e = _normalize_email(email or "")
    resp = templates.TemplateResponse(
        "auth_verify_resend.html",
        {
            "request": request,
            "next": n,
            "next_q": quote(n),
            "msg": str(msg or "").strip(),
            "err": str(err or "").strip(),
            "email": e,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/auth/verify/resend")
def auth_verify_resend_submit(
    request: Request,
    email: str = Form(default=""),
    next: str = Form(default="/"),
) -> Response:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)

    e = _normalize_email(email)

    def _resend_error(message: str, status_code: int = 400) -> Response:
        resp = templates.TemplateResponse(
            "auth_verify_resend.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "msg": "",
                "err": message,
                "email": e,
            },
            status_code=status_code,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    if not e or "@" not in e or len(e) > 320:
        return _resend_error("Email invalide.", 400)

    if not _email_verification_enabled():
        _audit_log(request, action="auth.verify_send", status="smtp_not_configured", actor_email=e)
        return _resend_error("Vérification email non configurée. Contacte le support.", 503)

    ip = _request_client_ip(request) or "unknown"
    retry_ip = _rate_limit_retry_after(bucket="auth_verify_ip", subject=ip, limit=20, window_s=60 * 60)
    retry_email = _rate_limit_retry_after(bucket="auth_verify_email", subject=(e or "missing"), limit=10, window_s=60 * 60)
    retry_after = max(v for v in [retry_ip, retry_email] if isinstance(v, int)) if any(
        isinstance(v, int) for v in [retry_ip, retry_email]
    ) else None
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="auth.verify_send",
            status="rate_limited",
            actor_email=e,
            meta={"retry_after_s": retry_after},
        )
        return _resend_error(f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}.", 429)

    public_msg = "Si un compte existe, un email de vérification a été envoyé."
    with DB.session() as db:
        row = db.scalar(select(User).where(User.email == e))
        if row and not _user_email_verified(db, user_id=str(getattr(row, "id", "") or "")):
            try:
                token_v, expires_at = _issue_email_verification_token(db, user_id=str(row.id))
                verify_url = f"{_public_base_url(request)}/auth/verify?{urlencode({'token': token_v, 'next': n})}"
                _send_email_verification_email(
                    to_email=str(getattr(row, "email", "") or e),
                    verify_url=verify_url,
                    expires_at=expires_at,
                )
            except Exception as exc:
                _audit_log(
                    request,
                    action="auth.verify_send",
                    status="send_error",
                    actor_email=e,
                    target_type="user",
                    target_id=str(getattr(row, "id", "") or ""),
                    meta={"error": f"{type(exc).__name__}: {str(exc)[:180]}"},
                )
                return _resend_error("Email non envoyé (erreur serveur). Réessaie plus tard.", 503)

            _audit_log(
                request,
                action="auth.verify_send",
                status="ok",
                actor_email=e,
                target_type="user",
                target_id=str(getattr(row, "id", "") or ""),
                meta={"reason": "resend"},
            )
        else:
            _audit_log(request, action="auth.verify_send", status="ok", actor_email=e, meta={"note": "noop"})

    return RedirectResponse(
        url=_path_with_flash(f"/auth/verify/resend?next={quote(n)}&email={quote(e)}", msg=public_msg),
        status_code=303,
    )


@app.get("/auth/verify", response_class=HTMLResponse)
def auth_verify(request: Request, token: str | None = None, next: str | None = None) -> Response:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        raise HTTPException(status_code=500, detail="SEO_AGENT_SECRET_KEY missing")

    t = str(token or "").strip()
    n = _safe_next_path(next)
    if not t:
        resp = templates.TemplateResponse(
            "auth_verify.html",
            {"request": request, "next": n, "next_q": quote(n), "ok": False, "err": "Lien invalide."},
            status_code=400,
        )
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    if not _email_verification_enabled():
        resp = templates.TemplateResponse(
            "auth_verify.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "ok": False,
                "err": "Vérification email non configurée.",
            },
            status_code=503,
        )
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    now = datetime.utcnow()
    uid = ""
    email_out = ""
    with DB.session() as db:
        row = _valid_email_verification_row(db, token=t)
        if not row:
            _audit_log(request, action="auth.verify_email", status="invalid_token")
            resp = templates.TemplateResponse(
                "auth_verify.html",
                {"request": request, "next": n, "next_q": quote(n), "ok": False, "err": "Lien invalide ou expiré."},
                status_code=400,
            )
            resp.headers["Cache-Control"] = "no-store"
            resp.headers["Referrer-Policy"] = "no-referrer"
            return resp

        user = db.get(User, str(getattr(row, "user_id", "") or ""))
        if not user:
            _audit_log(request, action="auth.verify_email", status="user_missing")
            resp = templates.TemplateResponse(
                "auth_verify.html",
                {"request": request, "next": n, "next_q": quote(n), "ok": False, "err": "Compte introuvable."},
                status_code=400,
            )
            resp.headers["Cache-Control"] = "no-store"
            resp.headers["Referrer-Policy"] = "no-referrer"
            return resp

        row.used_at = now
        db.add(row)
        db.commit()
        uid = str(getattr(user, "id", "") or "")
        email_out = str(getattr(user, "email", "") or "")

    if not uid:
        resp = templates.TemplateResponse(
            "auth_verify.html",
            {"request": request, "next": n, "next_q": quote(n), "ok": False, "err": "Erreur serveur."},
            status_code=500,
        )
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Referrer-Policy"] = "no-referrer"
        return resp

    token_out = auth.make_session_token(user_id=uid, secret=secret)
    resp = RedirectResponse(url=n, status_code=303)
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    secure_cookie = proto == "https"
    resp.set_cookie(
        auth.SESSION_COOKIE_NAME,
        token_out,
        max_age=auth.SESSION_TTL_S,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )
    _audit_log(request, action="auth.verify_email", status="ok", actor_email=email_out, target_type="user", target_id=uid)
    return resp


@app.post("/auth/google/start")
def auth_google_start(
    request: Request,
    mode: str = Form(default="login"),
    invite_code: str = Form(default=""),
    next: str = Form(default="/"),
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)

    m = str(mode or "login").strip().lower()
    if m not in {"login", "signup"}:
        m = "login"

    client_id, client_secret = _google_oauth_client()
    if not client_id or not client_secret:
        _audit_log(request, action="auth.google.start", status="oauth_not_configured")
        return RedirectResponse(
            url=_path_with_flash(f"/auth/login?next={quote(n)}", err="Google non configuré (client id/secret)."),
            status_code=303,
        )

    signup_target = f"/auth/signup?next={quote(n)}"
    invite_expected = _safe_env("SIGNUP_INVITE_CODE")
    invite_required = bool(invite_expected)
    invite_ok = True
    if m == "signup" and invite_required:
        invite_code_clean = str(invite_code or "").strip()
        if not hmac.compare_digest(invite_code_clean, invite_expected):
            _audit_log(request, action="auth.google.start", status="invite_invalid", meta={"mode": m})
            return RedirectResponse(
                url=_path_with_flash(signup_target, err="Code d’invitation invalide."),
                status_code=303,
            )

    csrf_token = str(getattr(request.state, "csrf_token", "") or "").strip()
    try:
        state = _oauth_state_encode(
            {
                "purpose": "auth_google",
                "mode": m,
                "ts": int(time.time()),
                "nonce": uuid.uuid4().hex,
                "next": n,
                "invite_ok": invite_ok,
                "csrf": csrf_token,
            }
        )
    except Exception as e:
        _audit_log(
            request,
            action="auth.google.start",
            status="state_error",
            meta={"error": f"{type(e).__name__}: {str(e)[:180]}"},
        )
        return RedirectResponse(
            url=_path_with_flash(f"/auth/login?next={quote(n)}", err=(str(e) or "OAuth state error")[:240]),
            status_code=303,
        )

    auth_url = "https://accounts.google.com/o/oauth2/v2/auth"
    params = {
        "client_id": client_id,
        "redirect_uri": _google_auth_redirect_uri(request),
        "response_type": "code",
        "scope": _GOOGLE_AUTH_SCOPE,
        "include_granted_scopes": "true",
        "prompt": "select_account",
        "state": state,
    }
    _audit_log(request, action="auth.google.start", status="ok", meta={"mode": m})
    return RedirectResponse(url=f"{auth_url}?{urlencode(params)}", status_code=303)


@app.get("/auth/google/callback")
def auth_google_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> Response:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        raise HTTPException(status_code=500, detail="SEO_AGENT_SECRET_KEY missing")

    payload = _oauth_state_decode(state or "")
    mode = str(payload.get("mode") if isinstance(payload, dict) else "login" or "login").strip().lower()
    if mode not in {"login", "signup"}:
        mode = "login"

    next_path = _safe_next_path(payload.get("next") if isinstance(payload, dict) else "/")
    login_target = f"/auth/login?next={quote(next_path)}"
    signup_target = f"/auth/signup?next={quote(next_path)}"
    base_target = signup_target if mode == "signup" else login_target

    if not isinstance(payload, dict) or payload.get("purpose") != "auth_google":
        _audit_log(request, action="auth.google.callback", status="invalid_state")
        return RedirectResponse(url=_path_with_flash(base_target, err="OAuth state invalide."), status_code=303)

    cookie_csrf = _sanitize_csrf_token(request.cookies.get(_CSRF_COOKIE_NAME))
    state_csrf = _sanitize_csrf_token(str(payload.get("csrf") or ""))
    if not cookie_csrf or not state_csrf or not hmac.compare_digest(cookie_csrf, state_csrf):
        _audit_log(request, action="auth.google.callback", status="csrf_mismatch")
        return RedirectResponse(url=_path_with_flash(base_target, err="OAuth invalide (CSRF)."), status_code=303)

    ts = payload.get("ts")
    try:
        if isinstance(ts, int) and ts > 0 and (time.time() - ts) > 20 * 60:
            _audit_log(request, action="auth.google.callback", status="expired")
            return RedirectResponse(url=_path_with_flash(base_target, err="OAuth expiré. Réessaie."), status_code=303)
    except Exception:
        pass

    if error:
        details = (error_description or error or "").strip() or "Google OAuth refusé."
        _audit_log(request, action="auth.google.callback", status="provider_error", meta={"error": details[:200]})
        return RedirectResponse(url=_path_with_flash(base_target, err=details[:240]), status_code=303)

    if not code:
        _audit_log(request, action="auth.google.callback", status="missing_code")
        return RedirectResponse(url=_path_with_flash(base_target, err="Code OAuth manquant."), status_code=303)

    client_id, client_secret = _google_oauth_client()
    if not client_id or not client_secret:
        _audit_log(request, action="auth.google.callback", status="oauth_not_configured")
        return RedirectResponse(
            url=_path_with_flash(base_target, err="Google non configuré (client id/secret)."),
            status_code=303,
        )

    try:
        token_data = _google_oauth_exchange_code(
            code=str(code),
            redirect_uri=_google_auth_redirect_uri(request),
            client_id=client_id,
            client_secret=client_secret,
        )
    except Exception as e:
        msg = f"OAuth token exchange failed: {type(e).__name__}: {e}"
        _audit_log(request, action="auth.google.callback", status="exchange_error", meta={"error": msg[:240]})
        return RedirectResponse(url=_path_with_flash(base_target, err="Erreur OAuth Google (token)."), status_code=303)

    raw_id_token = str(token_data.get("id_token") or "").strip()
    if not raw_id_token:
        _audit_log(request, action="auth.google.callback", status="missing_id_token")
        return RedirectResponse(url=_path_with_flash(base_target, err="Erreur OAuth Google (id_token manquant)."), status_code=303)

    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore
        from google.oauth2 import id_token as google_id_token  # type: ignore

        idinfo = google_id_token.verify_oauth2_token(raw_id_token, GoogleAuthRequest(), client_id)
    except Exception as e:
        _audit_log(
            request,
            action="auth.google.callback",
            status="id_token_invalid",
            meta={"error": f"{type(e).__name__}: {str(e)[:180]}"},
        )
        return RedirectResponse(url=_path_with_flash(base_target, err="Erreur OAuth Google (token invalide)."), status_code=303)

    sub = str((idinfo or {}).get("sub") or "").strip()
    email = _normalize_email(str((idinfo or {}).get("email") or ""))
    email_verified = bool((idinfo or {}).get("email_verified") is True)
    if not sub or not email or "@" not in email:
        _audit_log(request, action="auth.google.callback", status="missing_profile", meta={"sub": sub[:12]})
        return RedirectResponse(url=_path_with_flash(base_target, err="Profil Google invalide."), status_code=303)

    if _email_verification_enabled() and not email_verified:
        _audit_log(request, action="auth.google.callback", status="email_not_verified", actor_email=email)
        return RedirectResponse(
            url=_path_with_flash(base_target, err="Ton email Google n’est pas vérifié."),
            status_code=303,
        )

    uid = ""
    is_admin = False
    identity_exists = False
    linked_by_email = False
    with DB.session() as db:
        ident = db.scalar(
            select(OAuthIdentity).where(
                OAuthIdentity.provider == "google",
                OAuthIdentity.provider_user_id == sub,
            )
        )

        if ident:
            identity_exists = True
            user = db.get(User, str(getattr(ident, "user_id", "") or ""))
            if not user:
                _audit_log(request, action="auth.google.callback", status="user_missing", actor_email=email)
                return RedirectResponse(url=_path_with_flash(base_target, err="Compte introuvable."), status_code=303)
            uid = str(getattr(user, "id", "") or "")
            is_admin = bool(getattr(user, "is_admin", False))

            # Keep email in sync when possible (Google account email can change).
            if email and email != _normalize_email(str(getattr(user, "email", "") or "")):
                existing = db.scalar(select(User.id).where(User.email == email))
                if not existing:
                    user.email = email
                    db.add(user)
                    try:
                        db.commit()
                    except Exception:
                        db.rollback()

            if _email_verification_enabled() and email_verified:
                _mark_user_email_verified(db, user_id=uid)

        else:
            # Auto-link: if a user exists with the same email, attach this Google identity to the account.
            existing_user = db.scalar(select(User).where(User.email == email))
            if existing_user:
                existing_google = db.scalar(
                    select(OAuthIdentity).where(
                        OAuthIdentity.provider == "google",
                        OAuthIdentity.user_id == str(getattr(existing_user, "id", "") or ""),
                    )
                )
                if existing_google:
                    _audit_log(
                        request,
                        action="auth.google.callback",
                        status="already_linked",
                        actor_email=email,
                        meta={"mode": mode},
                    )
                    return RedirectResponse(
                        url=_path_with_flash(
                            login_target,
                            err="Ce compte est déjà lié à un autre compte Google. Connecte-toi avec email/mot de passe.",
                        ),
                        status_code=303,
                    )

                ident = OAuthIdentity(
                    user_id=str(getattr(existing_user, "id", "") or ""),
                    provider="google",
                    provider_user_id=sub,
                    email=email,
                )
                db.add(ident)
                try:
                    db.commit()
                except IntegrityError:
                    db.rollback()
                    ident = db.scalar(
                        select(OAuthIdentity).where(
                            OAuthIdentity.provider == "google",
                            OAuthIdentity.provider_user_id == sub,
                        )
                    )
                    if not ident:
                        _audit_log(
                            request,
                            action="auth.google.callback",
                            status="link_race_failed",
                            actor_email=email,
                            meta={"mode": mode},
                        )
                        return RedirectResponse(
                            url=_path_with_flash(base_target, err="Erreur liaison Google. Réessaie."),
                            status_code=303,
                        )
                    existing_user = db.get(User, str(getattr(ident, "user_id", "") or ""))
                    if not existing_user:
                        _audit_log(request, action="auth.google.callback", status="user_missing", actor_email=email)
                        return RedirectResponse(url=_path_with_flash(base_target, err="Compte introuvable."), status_code=303)

                uid = str(getattr(existing_user, "id", "") or "")
                is_admin = bool(getattr(existing_user, "is_admin", False))
                linked_by_email = True

                if _email_verification_enabled() and email_verified:
                    _mark_user_email_verified(db, user_id=uid)

            elif mode == "login":
                _audit_log(request, action="auth.google.callback", status="no_account", actor_email=email, meta={"mode": "login"})
                return RedirectResponse(
                    url=_path_with_flash(signup_target, err="Aucun compte Google associé. Crée un compte."),
                    status_code=303,
                )

            # Signup with Google
            invite_expected = _safe_env("SIGNUP_INVITE_CODE")
            invite_required = bool(invite_expected)
            invite_ok = bool(payload.get("invite_ok"))
            signup_disabled = _env_bool("SIGNUP_DISABLED")
            allow_emails = {_normalize_email(v) for v in _env_list("SIGNUP_ALLOWLIST_EMAILS")}
            allow_domains = {str(v).strip().lower().lstrip("@") for v in _env_list("SIGNUP_ALLOWLIST_DOMAINS")}
            allowlist_configured = bool(allow_emails or allow_domains)
            bootstrap_admin_email = _normalize_email(_safe_env("BOOTSTRAP_ADMIN_EMAIL"))

            existing = db.scalar(select(User.id).where(User.email == email))
            if existing:
                _audit_log(
                    request,
                    action="auth.google.callback",
                    status="email_exists",
                    actor_email=email,
                    meta={"mode": "signup", "note": "existing_user_unexpected"},
                )
                return RedirectResponse(
                    url=_path_with_flash(login_target, err="Ce compte existe déjà. Connecte-toi."),
                    status_code=303,
                )

            users_count = int(db.scalar(select(func.count()).select_from(User)) or 0)
            if users_count == 0 and bootstrap_admin_email and email != bootstrap_admin_email:
                return RedirectResponse(
                    url=_path_with_flash(signup_target, err="Le premier compte doit utiliser BOOTSTRAP_ADMIN_EMAIL."),
                    status_code=303,
                )
            if signup_disabled:
                if users_count != 0:
                    return RedirectResponse(url=_path_with_flash(signup_target, err="Inscriptions fermées."), status_code=303)
                if not bootstrap_admin_email or email != bootstrap_admin_email:
                    return RedirectResponse(url=_path_with_flash(signup_target, err="Inscriptions fermées."), status_code=303)
            if invite_required and not invite_ok:
                return RedirectResponse(url=_path_with_flash(signup_target, err="Code d’invitation invalide."), status_code=303)
            if allowlist_configured:
                domain = email.split("@", 1)[1] if "@" in email else ""
                if (email not in allow_emails) and (domain not in allow_domains):
                    return RedirectResponse(url=_path_with_flash(signup_target, err="Accès bêta: email non autorisé."), status_code=303)

            is_admin = users_count == 0
            user = User(email=email, password_hash=auth.hash_password(secrets.token_urlsafe(32)), is_admin=is_admin)
            db.add(user)
            try:
                db.flush()
                ident = OAuthIdentity(user_id=str(getattr(user, "id", "") or ""), provider="google", provider_user_id=sub, email=email)
                db.add(ident)
                db.commit()
            except IntegrityError:
                db.rollback()
                return RedirectResponse(url=_path_with_flash(signup_target, err="Erreur création compte. Réessaie."), status_code=303)
            db.refresh(user)
            uid = str(getattr(user, "id", "") or "")

            if _email_verification_enabled() and email_verified:
                _mark_user_email_verified(db, user_id=uid)

    if not uid:
        _audit_log(request, action="auth.google.callback", status="missing_uid", actor_email=email)
        return RedirectResponse(url=_path_with_flash(base_target, err="Erreur serveur."), status_code=303)

    if is_admin:
        _import_legacy_projects_for_user(uid)
        _migrate_legacy_runs_for_user(uid)
        _migrate_legacy_gsc_oauth_for_user(uid)

    token_out = auth.make_session_token(user_id=uid, secret=secret)
    resp = RedirectResponse(url=next_path, status_code=303)
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    secure_cookie = proto == "https"
    resp.set_cookie(
        auth.SESSION_COOKIE_NAME,
        token_out,
        max_age=auth.SESSION_TTL_S,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )
    _audit_log(
        request,
        action="auth.google.callback",
        status="ok",
        actor_email=email,
        target_type="user",
        target_id=uid,
        meta={"mode": mode, "existing": identity_exists, "linked": linked_by_email},
    )
    return resp


@app.get("/auth/login", response_class=HTMLResponse)
def auth_login(
    request: Request,
    next: str | None = None,
    email: str | None = None,
    msg: str | None = None,
    err: str | None = None,
) -> Response:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)
    e = _normalize_email(email or "")
    resp = templates.TemplateResponse(
        "auth_login.html",
        {
            "request": request,
            "next": n,
            "next_q": quote(n),
            "msg": str(msg or "").strip(),
            "err": str(err or "").strip(),
            "email": e,
            "email_q": quote(e),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/auth/login")
def auth_login_submit(
    request: Request,
    email: str = Form(default=""),
    password: str = Form(default=""),
    next: str = Form(default="/"),
) -> Response:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        raise HTTPException(status_code=500, detail="SEO_AGENT_SECRET_KEY missing")

    e = _normalize_email(email)
    n = _safe_next_path(next)

    def _login_error(message: str, status_code: int) -> Response:
        resp = templates.TemplateResponse(
            "auth_login.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "msg": "",
                "err": message,
                "email": e,
                "email_q": quote(e),
            },
            status_code=status_code,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    ip = _request_client_ip(request) or "unknown"
    retry_ip = _rate_limit_retry_after(bucket="auth_login_ip", subject=ip, limit=20, window_s=10 * 60)
    retry_email = _rate_limit_retry_after(bucket="auth_login_email", subject=(e or "missing"), limit=10, window_s=10 * 60)
    retry_after = max(v for v in [retry_ip, retry_email] if isinstance(v, int)) if any(
        isinstance(v, int) for v in [retry_ip, retry_email]
    ) else None
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="auth.login",
            status="rate_limited",
            actor_email=e,
            meta={"retry_after_s": retry_after},
        )
        return _login_error(f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}.", 429)

    with DB.session() as db:
        user = db.scalar(select(User).where(User.email == e))
        if not user or not auth.verify_password(password, user.password_hash):
            _audit_log(request, action="auth.login", status="invalid_credentials", actor_email=e)
            return _login_error("Identifiants invalides.", 401)

        if _email_verification_enabled() and not _user_email_verified(db, user_id=str(user.id)):
            retry_verify_ip = _rate_limit_retry_after(bucket="auth_verify_ip", subject=ip, limit=20, window_s=60 * 60)
            retry_verify_email = _rate_limit_retry_after(
                bucket="auth_verify_email", subject=(e or "missing"), limit=10, window_s=60 * 60
            )
            retry_after_verify = max(v for v in [retry_verify_ip, retry_verify_email] if isinstance(v, int)) if any(
                isinstance(v, int) for v in [retry_verify_ip, retry_verify_email]
            ) else None
            if isinstance(retry_after_verify, int):
                _audit_log(
                    request,
                    action="auth.verify_send",
                    status="rate_limited",
                    actor_email=e,
                    target_type="user",
                    target_id=str(getattr(user, "id", "") or ""),
                    meta={"retry_after_s": retry_after_verify},
                )
                return _login_error(
                    f"Email non vérifié. Réessaie dans {_format_retry_after(retry_after_verify)}.",
                    429,
                )

            try:
                token_v, expires_at = _issue_email_verification_token(db, user_id=str(user.id))
                verify_url = f"{_public_base_url(request)}/auth/verify?{urlencode({'token': token_v, 'next': n})}"
                _send_email_verification_email(to_email=str(getattr(user, "email", "") or e), verify_url=verify_url, expires_at=expires_at)
            except Exception as exc:
                _audit_log(
                    request,
                    action="auth.verify_send",
                    status="send_error",
                    actor_email=e,
                    target_type="user",
                    target_id=str(getattr(user, "id", "") or ""),
                    meta={"error": f"{type(exc).__name__}: {str(exc)[:180]}"},
                )
                return _login_error(
                    "Email non vérifié. Impossible d’envoyer l’email de vérification (erreur serveur).",
                    503,
                )

            _audit_log(
                request,
                action="auth.verify_send",
                status="ok",
                actor_email=e,
                target_type="user",
                target_id=str(getattr(user, "id", "") or ""),
            )
            return _login_error("Email non vérifié. Un email de vérification vient d’être envoyé.", 403)

    if bool(getattr(user, "is_admin", False)):
        _import_legacy_projects_for_user(str(user.id))
        _migrate_legacy_runs_for_user(str(user.id))
        _migrate_legacy_gsc_oauth_for_user(str(user.id))

    token = auth.make_session_token(user_id=user.id, secret=secret)
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    secure_cookie = proto == "https"
    resp = RedirectResponse(url=n, status_code=303)
    resp.set_cookie(
        auth.SESSION_COOKIE_NAME,
        token,
        max_age=auth.SESSION_TTL_S,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )
    _audit_log(request, action="auth.login", status="ok", user=user)
    return resp


@app.get("/auth/signup", response_class=HTMLResponse)
def auth_signup(request: Request, next: str | None = None) -> Response:
    user = getattr(request.state, "user", None)
    n = _safe_next_path(next)
    if user:
        return RedirectResponse(url=n, status_code=303)
    invite_required = bool(_safe_env("SIGNUP_INVITE_CODE"))
    signup_disabled = _env_bool("SIGNUP_DISABLED")
    resp = templates.TemplateResponse(
        "auth_signup.html",
        {
            "request": request,
            "next": n,
            "next_q": quote(n),
            "err": "",
            "email": "",
            "invite_required": invite_required,
            "invite_code": "",
            "signup_disabled": signup_disabled,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/auth/signup")
def auth_signup_submit(
    request: Request,
    email: str = Form(default=""),
    password: str = Form(default=""),
    invite_code: str = Form(default=""),
    next: str = Form(default="/"),
) -> Response:
    secret = _safe_env("SEO_AGENT_SECRET_KEY")
    if not secret:
        raise HTTPException(status_code=500, detail="SEO_AGENT_SECRET_KEY missing")

    e = _normalize_email(email)
    invite_expected = _safe_env("SIGNUP_INVITE_CODE")
    invite_required = bool(invite_expected)
    invite_code_clean = str(invite_code or "").strip()
    signup_disabled = _env_bool("SIGNUP_DISABLED")
    allow_emails = {_normalize_email(v) for v in _env_list("SIGNUP_ALLOWLIST_EMAILS")}
    allow_domains = {str(v).strip().lower().lstrip("@") for v in _env_list("SIGNUP_ALLOWLIST_DOMAINS")}
    allowlist_configured = bool(allow_emails or allow_domains)
    bootstrap_admin_email = _normalize_email(_safe_env("BOOTSTRAP_ADMIN_EMAIL"))
    n = _safe_next_path(next)
    ip = _request_client_ip(request) or "unknown"
    retry_ip = _rate_limit_retry_after(bucket="auth_signup_ip", subject=ip, limit=10, window_s=60 * 60)
    retry_email = _rate_limit_retry_after(bucket="auth_signup_email", subject=(e or "missing"), limit=5, window_s=60 * 60)
    retry_after = max(v for v in [retry_ip, retry_email] if isinstance(v, int)) if any(
        isinstance(v, int) for v in [retry_ip, retry_email]
    ) else None
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="auth.signup",
            status="rate_limited",
            actor_email=e,
            meta={"retry_after_s": retry_after},
        )
        resp = templates.TemplateResponse(
            "auth_signup.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "err": f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}.",
                "email": e,
                "invite_required": invite_required,
                "invite_code": invite_code_clean,
                "signup_disabled": signup_disabled,
            },
            status_code=429,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    def _signup_error(msg: str, status_code: int = 400) -> Response:
        _audit_log(
            request,
            action="auth.signup",
            status="rejected",
            actor_email=e,
            meta={"reason": msg, "status_code": status_code},
        )
        resp = templates.TemplateResponse(
            "auth_signup.html",
            {
                "request": request,
                "next": n,
                "next_q": quote(n),
                "err": msg,
                "email": e,
                "invite_required": invite_required,
                "invite_code": invite_code_clean,
                "signup_disabled": signup_disabled,
            },
            status_code=status_code,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    if not e or "@" not in e or len(e) > 320:
        return _signup_error("Email invalide.", 400)
    if len(password or "") < 10:
        return _signup_error("Mot de passe trop court (min 10).", 400)

    with DB.session() as db:
        existing = db.scalar(select(User).where(User.email == e))
        if existing:
            return _signup_error("Ce compte existe déjà.", 400)

        users_count = int(db.scalar(select(func.count()).select_from(User)) or 0)
        if users_count == 0 and bootstrap_admin_email and e != bootstrap_admin_email:
            return _signup_error(
                "Le premier compte doit utiliser l'email configuré dans BOOTSTRAP_ADMIN_EMAIL.",
                403,
            )
        if signup_disabled:
            if users_count != 0:
                return _signup_error("Inscriptions fermées.", 403)
            if not bootstrap_admin_email or e != bootstrap_admin_email:
                return _signup_error("Inscriptions fermées.", 403)
        if invite_required and not hmac.compare_digest(invite_code_clean, invite_expected):
            return _signup_error("Code d’invitation invalide.", 403)
        if allowlist_configured:
            domain = e.split("@", 1)[1] if "@" in e else ""
            if (e not in allow_emails) and (domain not in allow_domains):
                return _signup_error("Accès bêta: email non autorisé.", 403)

        is_admin = users_count == 0
        user = User(email=e, password_hash=auth.hash_password(password), is_admin=is_admin)
        db.add(user)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            return _signup_error("Ce compte existe déjà.", 400)
        db.refresh(user)

    if bool(getattr(user, "is_admin", False)):
        _import_legacy_projects_for_user(str(user.id))
        _migrate_legacy_runs_for_user(str(user.id))
        _migrate_legacy_gsc_oauth_for_user(str(user.id))

    if _email_verification_enabled():
        try:
            with DB.session() as db:
                token_v, expires_at = _issue_email_verification_token(db, user_id=str(user.id))
            verify_url = f"{_public_base_url(request)}/auth/verify?{urlencode({'token': token_v, 'next': n})}"
            _send_email_verification_email(
                to_email=str(getattr(user, "email", "") or e),
                verify_url=verify_url,
                expires_at=expires_at,
            )
        except Exception as exc:
            _audit_log(
                request,
                action="auth.verify_send",
                status="send_error",
                actor_email=e,
                target_type="user",
                target_id=str(getattr(user, "id", "") or ""),
                meta={"error": f"{type(exc).__name__}: {str(exc)[:180]}"},
            )
            return _signup_error("Impossible d’envoyer l’email de vérification. Réessaie plus tard.", 503)

        _audit_log(
            request,
            action="auth.verify_send",
            status="ok",
            actor_email=e,
            target_type="user",
            target_id=str(getattr(user, "id", "") or ""),
            meta={"reason": "signup"},
        )
        _audit_log(request, action="auth.signup", status="ok", user=user)
        return RedirectResponse(
            url=_path_with_flash(
                f"/auth/login?next={quote(n)}&email={quote(e)}",
                msg="Compte créé. Vérifie ton email pour te connecter.",
            ),
            status_code=303,
        )

    token = auth.make_session_token(user_id=user.id, secret=secret)
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
    secure_cookie = proto == "https"
    resp = RedirectResponse(url=n, status_code=303)
    resp.set_cookie(
        auth.SESSION_COOKIE_NAME,
        token,
        max_age=auth.SESSION_TTL_S,
        httponly=True,
        samesite="lax",
        secure=secure_cookie,
        path="/",
    )
    _audit_log(request, action="auth.signup", status="ok", user=user)
    return resp


@app.post("/auth/logout")
def auth_logout(request: Request) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    resp = RedirectResponse(url="/auth/login", status_code=303)
    resp.delete_cookie(auth.SESSION_COOKIE_NAME, path="/")
    _audit_log(request, action="auth.logout", status="ok", user=user)
    return resp


@app.get("/pricing", response_class=HTMLResponse)
def pricing_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "pricing_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.utcnow().year,
            "nav_items": _public_nav_items(),
            "catalog": billing.plan_catalog(),
            "stripe_ok": billing.stripe_enabled(),
        },
    )


@app.get("/terms", response_class=HTMLResponse)
def terms_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "terms_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.utcnow().year,
            "nav_items": _public_nav_items(),
            "legal_version": _legal_version(),
            "legal_updated_at": _legal_updated_at(),
        },
    )


@app.get("/privacy", response_class=HTMLResponse)
def privacy_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "privacy_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.utcnow().year,
            "nav_items": _public_nav_items(),
            "legal_version": _legal_version(),
            "legal_updated_at": _legal_updated_at(),
        },
    )


@app.get("/support", response_class=HTMLResponse)
def support_public(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "support_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.utcnow().year,
            "nav_items": _public_nav_items(),
        },
    )


@app.get("/status", response_class=HTMLResponse)
def status_public(request: Request) -> HTMLResponse:
    db_ok = False
    db_error = ""
    try:
        with DB.session() as db:
            db.execute(select(1))
        db_ok = True
    except Exception as e:
        db_ok = False
        db_error = f"{type(e).__name__}: {str(e)[:180]}"

    smtp_ok = bool(_smtp_config())
    s3_ok = bool(object_store.s3_enabled())
    s3_reason = ""
    if not s3_ok:
        reason = str(object_store.s3_available_reason() or "disabled")
        s3_reason = {
            "missing_bucket": "Non configuré",
            "missing_boto3": "Dépendance manquante",
            "disabled": "Désactivé",
        }.get(reason, reason)

    return templates.TemplateResponse(
        "status_public.html",
        {
            "request": request,
            "app_name": _app_name(),
            "support_email": _support_email(),
            "year": datetime.utcnow().year,
            "nav_items": _public_nav_items(),
            "now": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
            "web_ok": True,
            "db_ok": db_ok,
            "db_error": db_error,
            "smtp_ok": smtp_ok,
            "s3_ok": s3_ok,
            "s3_reason": s3_reason,
            "stripe_ok": billing.stripe_enabled(),
        },
    )


@app.get("/billing", response_class=HTMLResponse)
def billing_page(
    request: Request,
    success: str | None = None,
    canceled: str | None = None,
    session_id: str | None = None,
    msg: str | None = None,
    err: str | None = None,
) -> HTMLResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)  # type: ignore[return-value]

    stripe_ready = billing.stripe_enabled()
    catalog = billing.plan_catalog()
    msg_out = "Paiement confirmé." if success else ("Paiement annulé." if canceled else "")
    if not msg_out and msg:
        msg_out = str(msg).strip()
    err_out = str(err or "").strip()

    with DB.session() as db:
        if stripe_ready and session_id:
            try:
                billing.sync_from_checkout_session(db, session_id=session_id)
            except Exception as e:
                err_out = str(e).strip() or "Erreur sync Stripe"

        plan_key = billing.effective_plan_key(db, user_id=str(user.id))
        limits = billing.plan_limits(db, user_id=str(user.id))
        sub = billing.subscription_for_user(db, user_id=str(user.id))
        sub_active = bool(sub and str(getattr(sub, "status", "") or "").strip().lower() in billing.ACTIVE_SUB_STATUSES)

        used_pages = billing.usage_sum(db, user_id=str(user.id), metric="pages_crawled_month")
        used_ai = billing.usage_sum(db, user_id=str(user.id), metric="assistant_messages_month")
        projects_count = int(
            db.scalar(select(func.count()).select_from(Project).where(Project.owner_user_id == str(user.id))) or 0
        )

    def _limit_label(key: str) -> str:
        v = limits.get(key)
        if not isinstance(v, int) or v <= 0:
            return "—"
        return str(v)

    def _pct(used: int, key: str) -> int:
        v = limits.get(key)
        if not isinstance(v, int) or v <= 0:
            return 0
        try:
            pct = int(round((float(used) / float(v)) * 100))
            return max(0, min(100, pct))
        except Exception:
            return 0

    plan = catalog.get(plan_key, catalog["free"])
    resp = templates.TemplateResponse(
        "billing.html",
        {
            "request": request,
            "stripe_ready": stripe_ready,
            "msg": msg_out,
            "err": err_out,
            "plan_key": plan_key,
            "plan": plan,
            "subscription": sub,
            "subscription_active": sub_active,
            "limits": limits,
            "limits_labels": {
                "projects": _limit_label("projects"),
                "pages_crawled_month": _limit_label("pages_crawled_month"),
                "assistant_messages_month": _limit_label("assistant_messages_month"),
            },
            "usage": {
                "projects": projects_count,
                "pages_crawled_month": used_pages,
                "assistant_messages_month": used_ai,
            },
            "usage_pct": {
                "pages_crawled_month": _pct(used_pages, "pages_crawled_month"),
                "assistant_messages_month": _pct(used_ai, "assistant_messages_month"),
            },
            "catalog": catalog,
            "prices": {
                "solo": billing.price_id_for_plan("solo"),
                "pro": billing.price_id_for_plan("pro"),
                "business": billing.price_id_for_plan("business"),
            },
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/billing/checkout")
def billing_checkout(request: Request, plan_key: str = Form(default="")) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    retry_after = _rate_limit_retry_after(
        bucket="billing_checkout_user", subject=str(getattr(user, "id", "")), limit=10, window_s=10 * 60
    )
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="billing.checkout",
            status="rate_limited",
            user=user,
            meta={"retry_after_s": retry_after},
        )
        return RedirectResponse(
            url=_path_with_flash("/billing", err=f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."),
            status_code=303,
        )
    pk = (plan_key or "").strip().lower()
    if pk not in {"solo", "pro", "business"}:
        _audit_log(request, action="billing.checkout", status="invalid_plan", user=user, meta={"plan_key": pk})
        return RedirectResponse(url="/billing?canceled=1", status_code=303)
    with DB.session() as db:
        current = billing.effective_plan_key(db, user_id=str(user.id))
        sub = billing.subscription_for_user(db, user_id=str(user.id))
        sub_active = bool(sub and str(getattr(sub, "status", "") or "").strip().lower() in billing.ACTIVE_SUB_STATUSES)
        if sub_active and current == pk:
            _audit_log(request, action="billing.checkout", status="noop", user=user, meta={"plan_key": pk, "current": current})
            return RedirectResponse(url="/billing?msg=Tu%20es%20d%C3%A9j%C3%A0%20sur%20ce%20plan.", status_code=303)
        if sub_active and current != "free":
            try:
                if billing.plan_rank(pk) > billing.plan_rank(current):
                    billing.change_plan_now(db, user_id=str(user.id), target_plan_key=pk)
                    _audit_log(
                        request,
                        action="billing.plan_change",
                        status="ok",
                        user=user,
                        meta={"mode": "upgrade_now", "from": current, "to": pk},
                    )
                    return RedirectResponse(url=f"/billing?msg={quote('Plan mis à jour.')}", status_code=303)
                _, effective_at = billing.schedule_plan_change_at_period_end(db, user_id=str(user.id), target_plan_key=pk)
                _audit_log(
                    request,
                    action="billing.plan_change",
                    status="ok",
                    user=user,
                    meta={"mode": "downgrade_period_end", "from": current, "to": pk},
                )
                if effective_at:
                    msg = f"Downgrade planifié pour le {effective_at.strftime('%d/%m/%Y')}."
                else:
                    msg = "Downgrade planifié en fin de période."
                return RedirectResponse(url=f"/billing?msg={quote(msg)}", status_code=303)
            except Exception as e:
                _audit_log(
                    request,
                    action="billing.plan_change",
                    status="error",
                    user=user,
                    meta={"from": current, "to": pk, "error": str(e)[:240]},
                )
                return RedirectResponse(url=f"/billing?err={quote(str(e) or 'Erreur Stripe')}", status_code=303)
    try:
        with DB.session() as db:
            url = billing.create_checkout_session_url(db, user_id=str(user.id), email=str(user.email), plan_key=pk)
        _audit_log(request, action="billing.checkout", status="ok", user=user, meta={"plan_key": pk})
        return RedirectResponse(url=url, status_code=303)
    except Exception as e:
        _audit_log(request, action="billing.checkout", status="error", user=user, meta={"plan_key": pk, "error": str(e)[:240]})
        return RedirectResponse(url=f"/billing?err={quote(str(e) or 'Erreur Stripe')}", status_code=303)


@app.post("/billing/portal")
def billing_portal(request: Request) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    retry_after = _rate_limit_retry_after(
        bucket="billing_portal_user", subject=str(getattr(user, "id", "")), limit=10, window_s=10 * 60
    )
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="billing.portal",
            status="rate_limited",
            user=user,
            meta={"retry_after_s": retry_after},
        )
        return RedirectResponse(
            url=_path_with_flash("/billing", err=f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."),
            status_code=303,
        )
    try:
        with DB.session() as db:
            url = billing.create_billing_portal_url(db, user_id=str(user.id), email=str(user.email))
        _audit_log(request, action="billing.portal", status="ok", user=user)
        return RedirectResponse(url=url, status_code=303)
    except Exception as e:
        _audit_log(request, action="billing.portal", status="error", user=user, meta={"error": str(e)[:240]})
        return RedirectResponse(url=f"/billing?err={quote(str(e) or 'Erreur Stripe')}", status_code=303)


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request) -> JSONResponse:
    payload = await request.body()
    sig = str(request.headers.get("stripe-signature") or "").strip()
    if not sig:
        return JSONResponse({"ok": False, "error": "missing_signature"}, status_code=400)
    try:
        event = billing.construct_webhook_event(payload=payload, sig_header=sig)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e) or "invalid_signature"}, status_code=400)

    with DB.session() as db:
        try:
            billing.handle_stripe_event(db, event=event)
        except Exception as e:
            print(f"[STRIPE] webhook error: {type(e).__name__}: {e}")
            return JSONResponse({"ok": False, "error": "webhook_handler_error"}, status_code=500)

    return JSONResponse({"ok": True})


@app.get("/", response_class=HTMLResponse)
def projects(request: Request, msg: str | None = None, err: str | None = None) -> HTMLResponse:
    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    runs_dir = _runs_dir_for_request(request)

    user = getattr(request.state, "user", None)
    if not user:
        return templates.TemplateResponse(
            "home_public.html",
            {
                "request": request,
                "app_name": _app_name(),
                "support_email": _support_email(),
                "year": datetime.utcnow().year,
                "nav_items": _public_nav_items(),
            },
        )
    with DB.session() as db:
        db_projects = list(
            db.scalars(select(Project).where(Project.owner_user_id == str(user.id)).order_by(Project.site_name))
            if user
            else []
        )

    projects: list[dict[str, Any]] = []
    for p in db_projects:
        slug = str(p.slug or "").strip()
        if not slug:
            continue
        summary = dash.project_latest_summary(runs_dir, slug) if runs_dir.exists() else None
        if summary:
            projects.append(summary)
            continue
        projects.append(
            {
                "slug": slug,
                "site_name": p.site_name or slug,
                "base_url": p.base_url or "",
                "timestamp": "",
                "timestamp_label": "—",
                "pages_crawled": 0,
                "urls_crawled": 0,
                "health_score": 0,
                "urls_with_errors": 0,
                "issues_distribution": {"error": 0, "warning": 0, "notice": 0},
                "is_registry_only": True,
            }
        )

    projects.sort(key=lambda p: (p.get("site_name") or p.get("slug") or "").lower())

    jobs = _list_jobs(limit=100)
    is_admin = bool(getattr(user, "is_admin", False))
    if not is_admin:
        jobs = [
            j
            for j in jobs
            if isinstance(j.result, dict) and str(j.result.get("user_id") or "") == str(getattr(user, "id", ""))
        ]
    live_crawls: dict[str, dict[str, Any]] = {}
    recent_crawl_jobs: dict[str, dict[str, Any]] = {}
    for j in jobs:
        result = j.result if isinstance(j.result, dict) else None
        if not result or result.get("type") != "crawl":
            continue
        slug = str(result.get("slug") or "").strip()
        if not slug:
            continue
        if slug not in recent_crawl_jobs:
            recent_crawl_jobs[slug] = {
                "id": j.id,
                "status": j.status,
                "created_at": j.created_at,
                "progress": j.progress,
                "error": ((str(j.stderr or "").strip().splitlines() or [""])[-1])[:240],
            }
        if j.status not in {"queued", "running", "cancel_requested"}:
            continue
        existing = live_crawls.get(slug)
        if existing and float(existing.get("created_at") or 0) >= float(j.created_at or 0):
            continue
        live_crawls[slug] = {
            "id": j.id,
            "status": j.status,
            "created_at": j.created_at,
            "progress": j.progress,
        }

    resp = templates.TemplateResponse(
        "projects.html",
        {
            "request": request,
            "config_path": str(config_path) if config_path else None,
            "projects": projects,
            "jobs": jobs,
            "live_crawls": live_crawls,
            "recent_crawl_jobs": recent_crawl_jobs,
            "msg": (msg or "").strip(),
            "err": (err or "").strip(),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/settings", response_class=RedirectResponse)
def settings_root() -> RedirectResponse:
    return RedirectResponse(url="/settings/accounts", status_code=303)


@app.get("/settings/accounts", response_class=HTMLResponse)
def settings_accounts(request: Request) -> HTMLResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")

    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    client_id, client_secret = _google_oauth_client()
    oauth_ready = bool(client_id and client_secret and _safe_env("SEO_AGENT_SECRET_KEY"))
    if bool(getattr(user, "is_admin", False)):
        _import_legacy_projects_for_user(str(user.id))
    _ensure_hardened_netlify_connection(user_id=str(user.id))

    with DB.session() as db:
        github_oauth = _build_github_connection_state(user_id=str(user.id), db=db)
        netlify_oauth = _build_netlify_connection_state(user_id=str(user.id), db=db)
        bing_oauth = _build_bing_connection_state(user_id=str(user.id), db=db)
        db_projects = list(
            db.scalars(select(Project).where(Project.owner_user_id == str(user.id)).order_by(Project.site_name))
        )

    visible_projects = [proj for proj in db_projects if _project_visible_in_connections(proj)]
    gsc_return_to = "/settings/accounts#gsc-oauth-card"

    gsc_projects: list[dict[str, Any]] = []
    bing_projects: list[dict[str, Any]] = []
    for proj in visible_projects:
        slug = str(proj.slug or "").strip()
        if not slug:
            continue
        _, effective_gsc, effective_bing = _effective_project_crawl_settings(
            slug,
            config_path=config_path,
            project_settings=(proj.settings if isinstance(proj.settings, dict) else {}),
        )
        gsc_projects.append(
            {
                "slug": slug,
                "site_name": str(proj.site_name or slug),
                "base_url": str(proj.base_url or ""),
                "gsc_enabled": bool(effective_gsc.get("enabled")) if "enabled" in effective_gsc else True,
                "oauth_connected": _gsc_oauth_connected(str(user.id), slug),
                "connect_url": f"/projects/{slug}/gsc/oauth/connect?{urlencode({'next': gsc_return_to})}",
                "crawl_settings_url": f"/projects/{slug}/settings/crawl#gsc",
                "properties_url": f"/api/projects/{slug}/gsc/properties",
            }
        )
        bing_projects.append(
            {
                "slug": slug,
                "site_name": str(proj.site_name or slug),
                "base_url": str(proj.base_url or ""),
                "bing_enabled": bool(effective_bing.get("enabled")) if "enabled" in effective_bing else False,
                "crawl_settings_url": f"/projects/{slug}/settings/crawl#bing",
            }
        )

    resp = templates.TemplateResponse(
        "settings_accounts.html",
        {
            "request": request,
            "project": None,
            "is_admin": bool(getattr(user, "is_admin", False)),
            "can_access_system_settings": _user_can_access_system_settings(user),
            "github_oauth": {
                **github_oauth,
                "connect_url": "/oauth/github/connect?next=/settings/accounts#github-connect-card",
                "disconnect_url": "/oauth/github/disconnect",
                "repos_url": "/api/github/repos",
                "system_url": "/settings/system#github-oauth-system",
            },
            "netlify_oauth": {
                **netlify_oauth,
                "connect_url": "/oauth/netlify/connect?next=/settings/accounts#netlify-connect-card",
                "disconnect_url": "/oauth/netlify/disconnect",
                "sites_url": "/api/netlify/sites",
                "system_url": "/settings/system#netlify-oauth-system",
            },
            "bing_oauth": {
                **bing_oauth,
                "connect_url": "/oauth/bing/connect?next=/settings/accounts#bing-connect-card",
                "disconnect_url": "/oauth/bing/disconnect",
                "sites_url": "/api/bing/sites",
                "system_url": "/settings/system#bing-oauth-system",
            },
            "bing_projects": bing_projects,
            "gsc_oauth": {
                "configured": oauth_ready,
                "projects": gsc_projects,
                "system_url": "/settings/system#gsc-oauth-system",
            },
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/settings/accounts")
def settings_accounts_save(
    request: Request,
    key: str = Form(default=""),
    op: str = Form(default="save"),
    value: str = Form(default=""),
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    key = (key or "").strip()
    op = (op or "").strip().lower()
    if key not in _USER_CONNECTION_KEYS:
        raise HTTPException(status_code=400, detail="Invalid key")
    try:
        if op == "clear":
            _delete_user_connection(user_id=str(user.id), key=key)
            _audit_log(request, action="settings.account_connection", status="cleared", user=user, target_type="connection", target_id=key)
        else:
            v = (value or "").strip()
            if not v:
                return RedirectResponse(url="/settings/accounts", status_code=303)
            if key == "BING_WEBMASTER_API_KEY":
                _delete_user_connection(user_id=str(user.id), key=_BING_OAUTH_CONNECTION_KEY)
            _upsert_user_connection(user_id=str(user.id), key=key, value=v, meta={"auth_type": "manual"})
            _audit_log(request, action="settings.account_connection", status="saved", user=user, target_type="connection", target_id=key)
    except Exception as e:
        _audit_log(
            request,
            action="settings.account_connection",
            status="error",
            user=user,
            target_type="connection",
            target_id=key,
            meta={"error": str(e)[:240], "op": op},
        )
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}") from e

    return RedirectResponse(url="/settings/accounts", status_code=303)


@app.post("/settings/account/delete")
def settings_account_delete(request: Request, confirm: str = Form(default="")) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")

    c = str(confirm or "").strip().upper()
    if c != "DELETE":
        return RedirectResponse(
            url=_path_with_flash("/settings/accounts#danger-zone", err="Tape DELETE pour confirmer."),
            status_code=303,
        )

    uid = str(getattr(user, "id", "") or "").strip()
    email = _normalize_email(str(getattr(user, "email", "") or ""))

    _audit_log(
        request,
        action="account.delete",
        status="requested",
        actor_email=email,
        target_type="user",
        target_id=uid,
    )

    try:
        with DB.session() as db:
            # Scrub audit log PII while keeping the events.
            if uid:
                db.execute(
                    update(AuditLog)
                    .where(AuditLog.actor_user_id == uid)
                    .values(actor_user_id=None, actor_email=None)
                )
            if email:
                db.execute(update(AuditLog).where(AuditLog.actor_email == email).values(actor_email=None))

            row = db.get(User, uid) if uid else None
            if row is not None:
                db.delete(row)
            db.commit()
    except Exception as e:
        _audit_log(
            request,
            action="account.delete",
            status="db_error",
            actor_email=email,
            target_type="user",
            target_id=uid,
            meta={"error": f"{type(e).__name__}: {str(e)[:200]}"},
        )
        return RedirectResponse(
            url=_path_with_flash("/settings/accounts#danger-zone", err="Erreur suppression compte (DB)."),
            status_code=303,
        )

    # Delete on-disk data (best-effort).
    try:
        if uid:
            user_runs_dir = (DEFAULT_RUNS_DIR / uid).resolve()
            if user_runs_dir.exists() and user_runs_dir.is_dir():
                _delete_runs_path_from_object_store(user_runs_dir, recursive=True)
                shutil.rmtree(str(user_runs_dir), ignore_errors=True)
    except Exception:
        pass

    try:
        if uid:
            safe_user = re.sub(r"[^a-z0-9_.-]+", "-", uid.strip().lower()).strip("-") or "user"
            user_gsc_dir = (GSC_OAUTH_DIR / safe_user).resolve()
            if user_gsc_dir.exists() and user_gsc_dir.is_dir():
                shutil.rmtree(str(user_gsc_dir), ignore_errors=True)
    except Exception:
        pass

    resp = RedirectResponse(url=_path_with_flash("/auth/login", msg="Compte supprimé."), status_code=303)
    resp.delete_cookie(auth.SESSION_COOKIE_NAME, path="/")
    _audit_log(
        request,
        action="account.delete",
        status="ok",
        actor_email=email,
        target_type="user",
        target_id=uid,
    )
    return resp


@app.get("/settings/system", response_class=HTMLResponse)
def settings_system(request: Request) -> HTMLResponse:
    _ = _require_system_owner(request)
    msg = str(request.query_params.get("msg") or "").strip()
    err = str(request.query_params.get("err") or "").strip()

    system_sections = [
        {
            "id": "gsc-oauth-system",
            "title": "OAuth Google — plateforme",
            "description": "Configuration interne requise pour autoriser les clientes à connecter Google Search Console.",
            "items": [
                _build_env_setting_item("GOOGLE_OAUTH_CLIENT_ID"),
                _build_env_setting_item("GOOGLE_OAUTH_CLIENT_SECRET"),
                _build_env_setting_item("PUBLIC_BASE_URL"),
                _build_env_setting_item("GOOGLE_OAUTH_REDIRECT_URI"),
                _build_env_setting_item("SEO_AGENT_SECRET_KEY"),
            ],
        },
        {
            "id": "github-oauth-system",
            "title": "OAuth GitHub — plateforme",
            "description": "Configuration interne requise pour connecter les comptes GitHub des utilisatrices.",
            "items": [
                _build_env_setting_item("GITHUB_OAUTH_CLIENT_ID"),
                _build_env_setting_item("GITHUB_OAUTH_CLIENT_SECRET"),
                _build_env_setting_item("PUBLIC_BASE_URL"),
                _build_env_setting_item("GITHUB_OAUTH_REDIRECT_URI"),
                _build_env_setting_item("SEO_AGENT_SECRET_KEY"),
            ],
        },
        {
            "id": "netlify-oauth-system",
            "title": "OAuth Netlify — plateforme",
            "description": "Configuration interne requise pour connecter les comptes Netlify des utilisatrices.",
            "items": [
                _build_env_setting_item("NETLIFY_OAUTH_CLIENT_ID"),
                _build_env_setting_item("PUBLIC_BASE_URL"),
                _build_env_setting_item("NETLIFY_OAUTH_REDIRECT_URI"),
                _build_env_setting_item("SEO_AGENT_SECRET_KEY"),
            ],
        },
        {
            "id": "bing-oauth-system",
            "title": "OAuth Bing — plateforme",
            "description": "Configuration interne requise pour connecter les comptes Bing Webmaster Tools des utilisatrices.",
            "items": [
                _build_env_setting_item("BING_OAUTH_CLIENT_ID"),
                _build_env_setting_item("BING_OAUTH_CLIENT_SECRET"),
                _build_env_setting_item("PUBLIC_BASE_URL"),
                _build_env_setting_item("BING_OAUTH_REDIRECT_URI"),
                _build_env_setting_item("SEO_AGENT_SECRET_KEY"),
            ],
        },
        {
            "id": "gsc-service-account-system",
            "title": "Search Console — fallback technique",
            "description": "Service account utilisé uniquement en secours ou pour les opérations internes.",
            "items": [_build_env_setting_item("GOOGLE_APPLICATION_CREDENTIALS")],
        },
        {
            "id": "emails-system",
            "title": "Emails transactionnels",
            "description": "SMTP utilisé pour envoyer les emails (ex: mot de passe oublié).",
            "items": [
                _build_env_setting_item("PUBLIC_BASE_URL"),
                _build_env_setting_item("APP_NAME"),
                _build_env_setting_item("SMTP_HOST"),
                _build_env_setting_item("SMTP_PORT"),
                _build_env_setting_item("SMTP_USERNAME"),
                _build_env_setting_item("SMTP_PASSWORD"),
                _build_env_setting_item("SMTP_FROM"),
                _build_env_setting_item("SMTP_FROM_NAME"),
                _build_env_setting_item("SMTP_STARTTLS"),
                _build_env_setting_item("SMTP_SSL"),
                _build_env_setting_item("SMTP_TIMEOUT_SECONDS"),
                _build_env_setting_item("EMAIL_VERIFICATION_DISABLED"),
                _build_env_setting_item("EMAIL_VERIFY_TTL_SECONDS"),
                _build_env_setting_item("EMAIL_VERIFY_EMAIL_SUBJECT"),
                _build_env_setting_item("PASSWORD_RESET_TTL_SECONDS"),
                _build_env_setting_item("PASSWORD_RESET_EMAIL_SUBJECT"),
            ],
        },
        {
            "id": "assistant-system",
            "title": "Assistant IA",
            "description": "Réglages internes non exposés aux utilisateurs.",
            "items": [
                _build_env_setting_item("SEO_AUDIT_ASSISTANT_PROVIDER"),
                _build_env_setting_item("OPENAI_API_KEY"),
                _build_env_setting_item("SEO_AUDIT_ASSISTANT_OPENAI_MODEL"),
                _build_env_setting_item("GOOGLE_GEMINI_API_KEY"),
                _build_env_setting_item("SEO_AUDIT_ASSISTANT_GEMINI_MODEL"),
            ],
        },
        {
            "id": "platform-fallbacks-system",
            "title": "Clés plateforme — fallback",
            "description": "Valeurs plateforme utilisées en fallback pour GitHub/Netlify/Bing et en interne pour PageSpeed.",
            "items": [
                _build_env_setting_item("GITHUB_TOKEN"),
                _build_env_setting_item("NETLIFY_TOKEN"),
                _build_env_setting_item("BING_WEBMASTER_API_KEY"),
                _build_env_setting_item("PAGESPEED_API_KEY"),
            ],
        },
    ]

    cred_value, cred_src = _env_effective_value("GOOGLE_APPLICATION_CREDENTIALS")
    cred_path: Path | None = None
    if cred_value:
        p = Path(str(cred_value)).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / p).resolve()
        cred_path = p

    cred_exists = bool(cred_path and cred_path.exists())
    cred_info: dict[str, Any] = {"path": str(cred_path) if cred_path else "", "exists": cred_exists, "source": cred_src}
    if cred_exists and cred_path:
        try:
            data = json.loads(cred_path.read_text(encoding="utf-8"))
        except Exception:
            data = None
        if isinstance(data, dict):
            cred_info["type"] = str(data.get("type") or "")
            cred_info["project_id"] = str(data.get("project_id") or "")
            cred_info["client_email"] = str(data.get("client_email") or "")

    candidates: list[str] = []
    for base in [REPO_ROOT, DATA_DIR]:
        if not base.exists():
            continue
        for p in base.glob("*.json"):
            if not p.is_file():
                continue
            try:
                raw = p.read_text(encoding="utf-8")
            except Exception:
                continue
            if '"type"' not in raw or "service_account" not in raw:
                continue
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            if isinstance(obj, dict) and obj.get("type") == "service_account":
                try:
                    rel = str(p.relative_to(REPO_ROOT))
                except Exception:
                    rel = str(p)
                candidates.append(rel)
    candidates = sorted(set([c for c in candidates if c]))

    resp = templates.TemplateResponse(
        "settings_system.html",
        {
            "request": request,
            "project": None,
            "sections": system_sections,
            "msg": msg,
            "err": err,
            "gsc": {
                "credentials": cred_info,
                "candidates": candidates,
                "help": _SETTINGS_ENV_KEYS.get("GOOGLE_APPLICATION_CREDENTIALS", {}).get("help"),
            },
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/settings/system")
def settings_system_save(
    request: Request,
    key: str = Form(default=""),
    op: str = Form(default="save"),
    value: str = Form(default=""),
) -> RedirectResponse:
    _ = _require_system_owner(request)
    key = (key or "").strip()
    op = (op or "").strip().lower()
    if key not in _SETTINGS_ENV_KEYS:
        return RedirectResponse(url=_path_with_flash("/settings/system", err="Clé de réglage inconnue."), status_code=303)
    if not bool(_SETTINGS_ENV_KEYS.get(key, {}).get("editable", True)):
        return RedirectResponse(url=_path_with_flash("/settings/system", err="Cette variable est en lecture seule."), status_code=303)

    target = _env_target_path(key)
    try:
        if op == "clear":
            _write_env_key(target, key, None)
        else:
            v = (value or "").strip()
            if not v:
                return RedirectResponse(url=_path_with_flash("/settings/system", err="Valeur manquante."), status_code=303)
            _write_env_key(target, key, v)
    except Exception as e:
        _audit_log(
            request,
            action="settings.system",
            status="error",
            user=request.state.user,
            target_type="env",
            target_id=key,
            meta={"op": op, "error": str(e)[:240]},
        )
        return RedirectResponse(
            url=_path_with_flash(
                "/settings/system",
                err=f"{type(e).__name__}: {str(e)[:180]}",
            ),
            status_code=303,
        )

    _apply_effective_env(key)
    _audit_log(
        request,
        action="settings.system",
        status=("cleared" if op == "clear" else "saved"),
        user=request.state.user,
        target_type="env",
        target_id=key,
        meta={"op": op},
    )
    return RedirectResponse(
        url=_path_with_flash("/settings/system", msg=("Valeur supprimée." if op == "clear" else "Valeur enregistrée.")),
        status_code=303,
    )


@app.post("/projects/add")
def add_project(
    request: Request,
    mode: str = Form(default="domain"),
    domain: str = Form(default=""),
    url: str = Form(default=""),
    site_name: str = Form(default=""),
    gsc_urls: list[str] = Form(default=[]),
    bulk_urls: list[str] = Form(default=[]),
    next: str = Form(default="/"),
) -> RedirectResponse:
    mode = (mode or "").strip().lower()
    next_path = _safe_next_path(next)
    created: list[str] = []
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)

    projects_limit: int | None = None
    remaining_new: int | None = None
    if not bool(getattr(user, "is_admin", False)):
        with DB.session() as db:
            limits = billing.plan_limits(db, user_id=str(user.id))
            v = limits.get("projects")
            if isinstance(v, int) and v > 0:
                projects_limit = int(v)
                projects_count = int(
                    db.scalar(select(func.count()).select_from(Project).where(Project.owner_user_id == str(user.id))) or 0
                )
                remaining_new = max(0, projects_limit - projects_count)

    if mode in {"gsc", "bing", "bulk"}:
        skipped = 0
        capped = False
        source_urls = list(gsc_urls or []) + list(bulk_urls or [])
        for raw in source_urls:
            v = str(raw or "").strip()
            if not v:
                continue
            if v.startswith("sc-domain:"):
                host = v.split(":", 1)[1].strip()
                base = _normalize_base_url(host)
            else:
                base = _normalize_base_url(v)
            if not base:
                skipped += 1
                continue
            validation_err = _validate_public_crawl_target(base)
            if validation_err:
                skipped += 1
                continue
            if remaining_new is not None:
                slug_guess = _slug_from_base_url(base) or ""
                exists = bool(slug_guess and _db_project(str(user.id), slug_guess))
                if (not exists) and remaining_new <= 0:
                    capped = True
                    skipped += 1
                    continue
                if not exists:
                    remaining_new -= 1
            slug = _db_upsert_project(user_id=user.id, base_url=base, site_name="")
            if slug:
                created.append(slug)
        msg = f"{len(created)} projet(s) ajouté(s)." if created else "Aucun projet ajouté."
        if capped:
            msg = "Limite de sites atteinte pour ton plan. Va sur Abonnement pour upgrade."
        if skipped and not created:
            msg = "Aucun projet ajouté (certains hôtes sont refusés)."
        _audit_log(
            request,
            action="project.create",
            status=("ok" if created else "noop"),
            user=user,
            target_type="project",
            meta={"mode": mode, "count": len(created), "created": created[:20], "skipped": skipped, "capped": capped},
        )
        return RedirectResponse(url=_path_with_flash(next_path, msg=msg), status_code=303)

    raw = domain if mode == "domain" else url
    base = _normalize_base_url(raw)
    if not base:
        return RedirectResponse(url=_path_with_flash(next_path, err="URL invalide."), status_code=303)

    validation_err = _validate_public_crawl_target(base)
    if validation_err:
        return RedirectResponse(url=_path_with_flash(next_path, err=validation_err), status_code=303)

    if remaining_new is not None:
        slug_guess = _slug_from_base_url(base) or ""
        exists = bool(slug_guess and _db_project(str(user.id), slug_guess))
        if (not exists) and remaining_new <= 0:
            return RedirectResponse(
                url=_path_with_flash(next_path, err="Limite de sites atteinte pour ton plan. Upgrade: Abonnement."),
                status_code=303,
            )
    slug = _db_upsert_project(user_id=user.id, base_url=base, site_name=site_name)
    if slug:
        created.append(slug)
    _audit_log(
        request,
        action="project.create",
        status=("ok" if created else "noop"),
        user=user,
        target_type="project",
        target_id=(created[0] if created else ""),
        meta={"mode": mode, "base_url": base},
    )
    return RedirectResponse(url=_path_with_flash(next_path, msg="Projet ajouté."), status_code=303)


@app.post("/projects/delete")
def delete_projects(request: Request, slugs: list[str] = Form(default=[])) -> RedirectResponse:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in slugs or []:
        s = str(raw or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        normalized.append(s)

    if not normalized:
        return RedirectResponse(url="/", status_code=303)

    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    with DB.session() as db:
        rows = list(
            db.scalars(
                select(Project).where(Project.owner_user_id == str(user.id), Project.slug.in_(normalized))  # type: ignore[arg-type]
            )
        )
        deleted_slugs = [str(p.slug or "").strip() for p in rows if str(p.slug or "").strip()]
        for p in rows:
            db.delete(p)
        db.commit()
    _audit_log(
        request,
        action="project.delete",
        status="ok",
        user=user,
        target_type="project",
        meta={"count": len(deleted_slugs), "slugs": deleted_slugs[:50]},
    )

    return RedirectResponse(url="/", status_code=303)


@app.get("/api/assistant/meta")
def assistant_meta() -> JSONResponse:
    effective = _assistant_effective_provider()
    openai_ok = _assistant_openai_configured()
    gemini_ok = _assistant_gemini_configured()
    configured = (effective == "openai" and openai_ok) or (effective == "gemini" and gemini_ok)
    return JSONResponse(
        {
            "ok": True,
            "effective_provider": effective,
            "configured": configured,
            "providers": {
                "openai": {"configured": openai_ok, "model": _assistant_model("openai")},
                "gemini": {"configured": gemini_ok, "model": _assistant_model("gemini")},
            },
        }
    )


@app.post("/api/assistant/chat")
async def assistant_chat(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid JSON"}, status_code=400)

    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)

    message = payload.get("message") if isinstance(payload, dict) else None
    if not isinstance(message, str) or not message.strip():
        return JSONResponse({"ok": False, "error": "Missing message"}, status_code=400)
    message = message.strip()[:2000]
    retry_after = _rate_limit_retry_after(
        bucket="assistant_chat_user",
        subject=str(getattr(user, "id", "")),
        limit=20,
        window_s=60,
    )
    if isinstance(retry_after, int):
        return JSONResponse(
            {"ok": False, "error": f"Trop de requêtes. Réessaie dans {_format_retry_after(retry_after)}."},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    if not bool(getattr(user, "is_admin", False)):
        with DB.session() as db:
            ok, remaining = billing.ensure_within_quota(
                db,
                user_id=str(getattr(user, "id", "")),
                metric="assistant_messages_month",
                planned_amount=1,
            )
            if not ok:
                msg = "Quota Assistant IA mensuel atteint. Va sur Abonnement pour upgrade."
                return JSONResponse(
                    {"ok": False, "error": msg, "billing_url": "/billing", "remaining": remaining}, status_code=402
                )

    history = _assistant_clean_history(payload.get("history") if isinstance(payload, dict) else None)
    context = payload.get("context") if isinstance(payload, dict) else None
    system = _assistant_system_prompt(context if isinstance(context, dict) else None)

    provider = _assistant_effective_provider()
    if provider == "none":
        return JSONResponse(
            {
                "ok": False,
                "error": "Assistant temporairement indisponible.",
            },
            status_code=400,
        )

    model = _assistant_model(provider)
    try:
        if provider == "openai":
            messages = [{"role": "system", "content": system}, *history, {"role": "user", "content": message}]
            reply = _assistant_openai_chat(messages, model=model)
        else:
            # Gemini: role mapping user/model (assistant -> model)
            contents: list[dict[str, str]] = []
            for h in history:
                role = "model" if h["role"] == "assistant" else "user"
                contents.append({"role": role, "content": h["content"]})
            contents.append({"role": "user", "content": message})
            reply = _assistant_gemini_chat(contents, system=system, model=model)
    except Exception as e:
        # Keep client errors clean (no Python exception class names), but log full details server-side.
        print(f"[ASSISTANT] {provider} error: {type(e).__name__}: {e}")
        err = str(e).strip() or "Erreur assistant"
        return JSONResponse({"ok": False, "error": err, "provider": provider}, status_code=502)

    try:
        with DB.session() as db:
            billing.usage_add(
                db,
                user_id=str(getattr(user, "id", "")),
                metric="assistant_messages_month",
                amount=1,
                meta={"kind": "assistant_chat", "provider": provider, "model": model},
            )
    except Exception as e:
        print(f"[BILLING] assistant usage error: {type(e).__name__}: {e}")

    return JSONResponse({"ok": True, "reply": reply, "provider": provider, "model": model})


@app.get("/projects/{slug}/gsc/oauth/connect")
def project_gsc_oauth_connect(request: Request, slug: str, next: str | None = None) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    client_id, client_secret = _google_oauth_client()
    return_to = _safe_next_path(next)
    if not client_id or not client_secret:
        return RedirectResponse(
            url=_path_with_flash(return_to or f"/projects/{slug}/settings/crawl", err="Google OAuth non configuré (client id/secret)."),
            status_code=303,
        )

    try:
        state = _oauth_state_encode(
            {"slug": slug, "ts": int(time.time()), "nonce": uuid.uuid4().hex, "next": return_to}
        )
    except Exception as e:
        return RedirectResponse(
            url=_path_with_flash(return_to or f"/projects/{slug}/settings/crawl", err=(str(e) or "OAuth state error")),
            status_code=303,
        )

    redirect_uri = _google_oauth_redirect_uri(request)
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth"
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": _GOOGLE_OAUTH_SCOPE,
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
    }
    return RedirectResponse(url=f"{auth_url}?{urlencode(params)}", status_code=303)


@app.get("/oauth/google/callback")
def google_oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> RedirectResponse:
    payload = _oauth_state_decode(state or "")
    slug = str(payload.get("slug") if isinstance(payload, dict) else "" or "").strip()
    return_to = _safe_next_path(payload.get("next") if isinstance(payload, dict) else None)
    fallback_target = return_to or f"/projects/{slug}/settings/crawl"
    if not slug:
        _audit_log(
            request,
            action="oauth.google.callback",
            status="invalid_state",
            target_type="project",
            target_id="missing_slug",
        )
        return RedirectResponse(url=f"/settings/accounts?err={quote('OAuth state invalide.')}", status_code=303)
    user = getattr(request.state, "user", None)
    if not user or not _db_project(str(user.id), slug):
        _audit_log(
            request,
            action="oauth.google.callback",
            status="project_not_found",
            user=user,
            target_type="project",
            target_id=slug,
        )
        return RedirectResponse(url=f"/?err={quote('Projet introuvable (OAuth).')}", status_code=303)

    ts = payload.get("ts") if isinstance(payload, dict) else None
    try:
        if isinstance(ts, int) and ts > 0 and (time.time() - ts) > 20 * 60:
            _audit_log(
                request,
                action="oauth.google.callback",
                status="expired",
                user=user,
                target_type="project",
                target_id=slug,
            )
            return RedirectResponse(
                url=_path_with_flash(fallback_target, err="OAuth expiré. Relance la connexion Google."),
                status_code=303,
            )
    except Exception:
        pass

    if error:
        details = (error_description or error).strip()
        if len(details) > 200:
            details = details[:200] + "…"
        _audit_log(
            request,
            action="oauth.google.callback",
            status="provider_error",
            user=user,
            target_type="project",
            target_id=slug,
            meta={"error": details},
        )
        return RedirectResponse(url=_path_with_flash(fallback_target, err=details), status_code=303)

    if not code:
        _audit_log(
            request,
            action="oauth.google.callback",
            status="missing_code",
            user=user,
            target_type="project",
            target_id=slug,
        )
        return RedirectResponse(
            url=_path_with_flash(fallback_target, err="Code OAuth manquant."),
            status_code=303,
        )

    client_id, client_secret = _google_oauth_client()
    if not client_id or not client_secret:
        _audit_log(
            request,
            action="oauth.google.callback",
            status="oauth_not_configured",
            user=user,
            target_type="project",
            target_id=slug,
        )
        return RedirectResponse(
            url=_path_with_flash(fallback_target, err="Google OAuth non configuré (client id/secret)."),
            status_code=303,
        )

    redirect_uri = _google_oauth_redirect_uri(request)
    try:
        token_data = _google_oauth_exchange_code(
            code=str(code),
            redirect_uri=redirect_uri,
            client_id=client_id,
            client_secret=client_secret,
        )
    except Exception as e:
        msg = f"OAuth token exchange failed: {type(e).__name__}: {e}"
        if len(msg) > 250:
            msg = msg[:250] + "…"
        _audit_log(
            request,
            action="oauth.google.callback",
            status="exchange_error",
            user=user,
            target_type="project",
            target_id=slug,
            meta={"error": msg},
        )
        return RedirectResponse(url=_path_with_flash(fallback_target, err=msg), status_code=303)

    refresh_token = str(token_data.get("refresh_token") or "").strip()
    if not refresh_token:
        missing_msg = "Google n'a pas renvoyé de refresh_token. Réessaie (prompt=consent) ou révoque l'accès puis reconnecte."
        _audit_log(
            request,
            action="oauth.google.callback",
            status="missing_refresh_token",
            user=user,
            target_type="project",
            target_id=slug,
        )
        return RedirectResponse(url=_path_with_flash(fallback_target, err=missing_msg), status_code=303)

    scope = str(token_data.get("scope") or _GOOGLE_OAUTH_SCOPE).strip() or _GOOGLE_OAUTH_SCOPE
    try:
        _gsc_oauth_save(str(user.id), slug, refresh_token=refresh_token, scope=scope)
    except Exception as e:
        msg = f"SaveError: {type(e).__name__}: {e}"
        if len(msg) > 200:
            msg = msg[:200] + "…"
        _audit_log(
            request,
            action="oauth.google.callback",
            status="save_error",
            user=user,
            target_type="project",
            target_id=slug,
            meta={"error": msg},
        )
        return RedirectResponse(url=_path_with_flash(fallback_target, err=msg), status_code=303)

    _audit_log(
        request,
        action="oauth.google.callback",
        status="ok",
        user=user,
        target_type="project",
        target_id=slug,
    )
    return RedirectResponse(url=_path_with_flash(fallback_target, msg="Google connecté (OAuth)."), status_code=303)


@app.post("/projects/{slug}/gsc/oauth/disconnect")
def project_gsc_oauth_disconnect(request: Request, slug: str) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    token = _gsc_oauth_refresh_token(str(getattr(user, "id", "")), slug)
    if token:
        _google_oauth_revoke_token(token)
    _gsc_oauth_clear(str(getattr(user, "id", "")), slug)
    _audit_log(
        request,
        action="oauth.google.disconnect",
        status="ok",
        user=user,
        target_type="project",
        target_id=slug,
    )
    return RedirectResponse(url=f"/projects/{slug}/settings/crawl?msg={quote('Google déconnecté.')}", status_code=303)


@app.get("/oauth/github/connect")
def github_oauth_connect(request: Request, next: str | None = None) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    client_id, client_secret = _github_oauth_client()
    return_to = _safe_next_path(next or "/settings/accounts#github-connect-card")
    if not client_id or not client_secret:
        return RedirectResponse(
            url=_path_with_flash(return_to, err="GitHub OAuth non configuré (client id/secret)."),
            status_code=303,
        )
    try:
        state = _oauth_state_encode(
            {
                "provider": "github",
                "user_id": str(user.id),
                "ts": int(time.time()),
                "nonce": uuid.uuid4().hex,
                "next": return_to,
            }
        )
    except Exception as e:
        return RedirectResponse(url=_path_with_flash(return_to, err=str(e) or "OAuth state error"), status_code=303)

    params = {
        "client_id": client_id,
        "redirect_uri": _provider_oauth_redirect_uri(request, "github"),
        "scope": _GITHUB_OAUTH_SCOPE,
        "state": state,
    }
    return RedirectResponse(url=f"https://github.com/login/oauth/authorize?{urlencode(params)}", status_code=303)


@app.get("/oauth/github/callback")
def github_oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    payload = _oauth_state_decode(state or "")
    return_to = _safe_next_path(payload.get("next") if isinstance(payload, dict) else "/settings/accounts#github-connect-card")
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    if not isinstance(payload, dict) or payload.get("provider") != "github" or str(payload.get("user_id") or "") != str(user.id):
        return RedirectResponse(url=_path_with_flash(return_to, err="OAuth state invalide."), status_code=303)
    if error:
        return RedirectResponse(
            url=_path_with_flash(return_to, err=(error_description or error or "GitHub OAuth refusé")[:240]),
            status_code=303,
        )
    if not code:
        return RedirectResponse(url=_path_with_flash(return_to, err="Code OAuth GitHub manquant."), status_code=303)

    client_id, client_secret = _github_oauth_client()
    if not client_id or not client_secret:
        return RedirectResponse(url=_path_with_flash(return_to, err="GitHub OAuth non configuré."), status_code=303)

    try:
        token_data = _github_oauth_exchange_code(
            code=str(code),
            redirect_uri=_provider_oauth_redirect_uri(request, "github"),
            client_id=client_id,
            client_secret=client_secret,
        )
        access_token = str(token_data.get("access_token") or "").strip()
        if not access_token:
            raise RuntimeError("Missing access_token")
        profile = _github_api_get("/user", token=access_token)
        meta = {
            "auth_type": "oauth",
            "login": str(profile.get("login") or "").strip() if isinstance(profile, dict) else "",
            "name": str(profile.get("name") or "").strip() if isinstance(profile, dict) else "",
            "avatar_url": str(profile.get("avatar_url") or "").strip() if isinstance(profile, dict) else "",
            "html_url": str(profile.get("html_url") or "").strip() if isinstance(profile, dict) else "",
            "connected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "scope": str(token_data.get("scope") or "").strip(),
        }
        _upsert_user_connection(user_id=str(user.id), key="GITHUB_TOKEN", value=access_token, meta=meta)
    except Exception as e:
        _audit_log(
            request,
            action="oauth.github.connect",
            status="error",
            user=user,
            target_type="connection",
            target_id="GITHUB_TOKEN",
            meta={"error": str(e)[:240]},
        )
        return RedirectResponse(url=_path_with_flash(return_to, err=f"GitHub OAuth: {type(e).__name__}: {e}"), status_code=303)
    _audit_log(
        request,
        action="oauth.github.connect",
        status="ok",
        user=user,
        target_type="connection",
        target_id="GITHUB_TOKEN",
        meta={"login": meta.get("login") or ""},
    )
    return RedirectResponse(url=_path_with_flash(return_to, msg="GitHub connecté."), status_code=303)


@app.post("/oauth/github/disconnect")
def github_oauth_disconnect(request: Request, next: str = Form(default="/settings/accounts#github-connect-card")) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    _delete_user_connection(user_id=str(user.id), key="GITHUB_TOKEN")
    _audit_log(request, action="oauth.github.disconnect", status="ok", user=user, target_type="connection", target_id="GITHUB_TOKEN")
    return RedirectResponse(url=_path_with_flash(next, msg="GitHub déconnecté."), status_code=303)


@app.get("/api/github/repos")
def github_repos(request: Request) -> JSONResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)
    token, source = _effective_user_connection_value(user_id=str(user.id), key="GITHUB_TOKEN")
    if not token:
        return JSONResponse({"ok": False, "error": "GitHub non connecté."}, status_code=400)
    if source != "user":
        return JSONResponse({"ok": False, "error": "Connecte ton propre compte GitHub pour lister tes dépôts."}, status_code=400)
    try:
        repos: list[dict[str, Any]] = []
        page = 1
        while page <= 3:
            payload = _github_api_get(
                "/user/repos",
                token=token,
                params={"per_page": 100, "page": page, "sort": "updated", "affiliation": "owner,collaborator,organization_member"},
            )
            if not isinstance(payload, list) or not payload:
                break
            for repo in payload:
                if not isinstance(repo, dict):
                    continue
                repos.append(
                    {
                        "full_name": str(repo.get("full_name") or "").strip(),
                        "name": str(repo.get("name") or "").strip(),
                        "private": bool(repo.get("private")),
                        "default_branch": str(repo.get("default_branch") or "").strip(),
                        "html_url": str(repo.get("html_url") or "").strip(),
                        "homepage": str(repo.get("homepage") or "").strip(),
                    }
                )
            if len(payload) < 100:
                break
            page += 1
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"GitHubError: {type(e).__name__}: {e}"}, status_code=400)
    return JSONResponse({"ok": True, "repos": repos})


@app.get("/oauth/netlify/connect")
def netlify_oauth_connect(request: Request, next: str | None = None) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    client_id = _netlify_oauth_client_id()
    return_to = _safe_next_path(next or "/settings/accounts#netlify-connect-card")
    if not client_id:
        return RedirectResponse(url=_path_with_flash(return_to, err="Netlify OAuth non configuré (client id)."), status_code=303)
    try:
        state = _oauth_state_encode(
            {
                "provider": "netlify",
                "user_id": str(user.id),
                "ts": int(time.time()),
                "nonce": uuid.uuid4().hex,
                "next": return_to,
            }
        )
    except Exception as e:
        return RedirectResponse(url=_path_with_flash(return_to, err=str(e) or "OAuth state error"), status_code=303)

    params = {
        "client_id": client_id,
        "redirect_uri": _provider_oauth_redirect_uri(request, "netlify"),
        "response_type": "token",
        "state": state,
    }
    return RedirectResponse(url=f"https://app.netlify.com/authorize?{urlencode(params)}", status_code=303)


@app.get("/oauth/netlify/callback", response_class=HTMLResponse)
def netlify_oauth_callback_page(request: Request) -> HTMLResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    resp = templates.TemplateResponse(
        "oauth_fragment_callback.html",
        {
            "request": request,
            "provider_label": "Netlify",
            "complete_url": "/oauth/netlify/callback/complete",
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/oauth/netlify/callback/complete")
def netlify_oauth_complete(
    request: Request,
    access_token: str = Form(default=""),
    state: str = Form(default=""),
    error: str = Form(default=""),
    error_description: str = Form(default=""),
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    payload = _oauth_state_decode(state or "")
    return_to = _safe_next_path(payload.get("next") if isinstance(payload, dict) else "/settings/accounts#netlify-connect-card")
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    if not isinstance(payload, dict) or payload.get("provider") != "netlify" or str(payload.get("user_id") or "") != str(user.id):
        return RedirectResponse(url=_path_with_flash(return_to, err="OAuth state invalide."), status_code=303)
    if error:
        return RedirectResponse(
            url=_path_with_flash(return_to, err=(error_description or error or "Netlify OAuth refusé")[:240]),
            status_code=303,
        )
    token = str(access_token or "").strip()
    if not token:
        return RedirectResponse(url=_path_with_flash(return_to, err="Token Netlify manquant."), status_code=303)
    try:
        _netlify_store_hardened_token(user_id=str(user.id), oauth_token=token)
        message = "Netlify connecté (PAT serveur généré)."
    except Exception as e:
        try:
            _netlify_store_oauth_token_fallback(user_id=str(user.id), oauth_token=token, upgrade_error=str(e))
            message = "Netlify connecté (fallback token OAuth ; génération PAT refusée)."
        except Exception as fallback_error:
            _audit_log(
                request,
                action="oauth.netlify.connect",
                status="error",
                user=user,
                target_type="connection",
                target_id="NETLIFY_TOKEN",
                meta={"error": str(fallback_error)[:240]},
            )
            return RedirectResponse(
                url=_path_with_flash(
                    return_to,
                    err=f"Netlify OAuth: {type(fallback_error).__name__}: {fallback_error}",
                ),
                status_code=303,
            )
    _audit_log(
        request,
        action="oauth.netlify.connect",
        status="ok",
        user=user,
        target_type="connection",
        target_id="NETLIFY_TOKEN",
        meta={"mode": ("fallback_oauth" if "fallback" in message.lower() else "pat")},
    )
    return RedirectResponse(url=_path_with_flash(return_to, msg=message), status_code=303)


@app.post("/oauth/netlify/disconnect")
def netlify_oauth_disconnect(request: Request, next: str = Form(default="/settings/accounts#netlify-connect-card")) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    _delete_user_connection(user_id=str(user.id), key="NETLIFY_TOKEN")
    _audit_log(request, action="oauth.netlify.disconnect", status="ok", user=user, target_type="connection", target_id="NETLIFY_TOKEN")
    return RedirectResponse(url=_path_with_flash(next, msg="Netlify déconnecté."), status_code=303)


@app.get("/api/netlify/sites")
def netlify_sites(request: Request) -> JSONResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)
    token, source = _ensure_hardened_netlify_connection(user_id=str(user.id))
    if not token:
        return JSONResponse({"ok": False, "error": "Netlify non connecté."}, status_code=400)
    if source != "user":
        return JSONResponse({"ok": False, "error": "Connecte ton propre compte Netlify pour lister tes sites."}, status_code=400)

    try:
        sites: list[dict[str, Any]] = []
        page = 1
        while page <= 5:
            payload = _netlify_api_get("/api/v1/sites", token=token, params={"per_page": 100, "page": page})
            if not isinstance(payload, list) or not payload:
                break
            for site in payload:
                if not isinstance(site, dict):
                    continue
                custom_domains = [str(v or "").strip() for v in (site.get("custom_domain") and [site.get("custom_domain")] or []) if str(v or "").strip()]
                custom_domains.extend([str(v or "").strip() for v in (site.get("domain_aliases") or []) if str(v or "").strip()])
                primary_url = ""
                for domain in custom_domains:
                    normalized = _normalize_base_url(domain)
                    if normalized:
                        primary_url = normalized
                        break
                if not primary_url:
                    primary_url = _normalize_base_url(str(site.get("ssl_url") or site.get("url") or "")) or ""
                sites.append(
                    {
                        "site_id": str(site.get("id") or "").strip(),
                        "site_url": primary_url,
                        "site_name": str(site.get("name") or primary_url or "").strip(),
                        "admin_url": str(site.get("admin_url") or "").strip(),
                    }
                )
            if len(payload) < 100:
                break
            page += 1
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"NetlifyError: {type(e).__name__}: {e}"}, status_code=400)

    existing_by_base = _db_project_lookup_by_base_url(str(user.id))
    items: list[dict[str, Any]] = []
    for site in sites:
        base = str(site.get("site_url") or "").strip()
        existing_slug = existing_by_base.get(base)
        items.append(
            {
                "site_url": base,
                "import_base_url": base,
                "domain": (urlsplit(base).hostname or "").lower() if base else "",
                "already_imported": bool(existing_slug),
                "project_slug": existing_slug or "",
                "site_name": str(site.get("site_name") or "").strip(),
            }
        )
    return JSONResponse({"ok": True, "sites": items})


@app.get("/oauth/bing/connect")
def bing_oauth_connect(request: Request, next: str | None = None) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    client_id, client_secret = _bing_oauth_client()
    return_to = _safe_next_path(next or "/settings/accounts#bing-connect-card")
    if not client_id or not client_secret:
        return RedirectResponse(url=_path_with_flash(return_to, err="Bing OAuth non configuré (client id/secret)."), status_code=303)
    try:
        state = _oauth_state_encode(
            {
                "provider": "bing",
                "user_id": str(user.id),
                "ts": int(time.time()),
                "nonce": uuid.uuid4().hex,
                "next": return_to,
            }
        )
    except Exception as e:
        return RedirectResponse(url=_path_with_flash(return_to, err=str(e) or "OAuth state error"), status_code=303)

    params = {
        "client_id": client_id,
        "redirect_uri": _provider_oauth_redirect_uri(request, "bing"),
        "response_type": "code",
        "scope": _BING_OAUTH_SCOPE,
        "state": state,
    }
    return RedirectResponse(url=f"https://www.bing.com/webmasters/oauth/authorize?{urlencode(params)}", status_code=303)


@app.get("/oauth/bing/callback")
def bing_oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    payload = _oauth_state_decode(state or "")
    return_to = _safe_next_path(payload.get("next") if isinstance(payload, dict) else "/settings/accounts#bing-connect-card")
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    if not isinstance(payload, dict) or payload.get("provider") != "bing" or str(payload.get("user_id") or "") != str(user.id):
        return RedirectResponse(url=_path_with_flash(return_to, err="OAuth state invalide."), status_code=303)
    if error:
        return RedirectResponse(
            url=_path_with_flash(return_to, err=(error_description or error or "Bing OAuth refusé")[:240]),
            status_code=303,
        )
    if not code:
        return RedirectResponse(url=_path_with_flash(return_to, err="Code OAuth Bing manquant."), status_code=303)

    client_id, client_secret = _bing_oauth_client()
    if not client_id or not client_secret:
        return RedirectResponse(url=_path_with_flash(return_to, err="Bing OAuth non configuré."), status_code=303)
    try:
        token_data = _bing_oauth_exchange_code(
            code=str(code),
            redirect_uri=_provider_oauth_redirect_uri(request, "bing"),
            client_id=client_id,
            client_secret=client_secret,
        )
        refresh_token = str(token_data.get("refresh_token") or "").strip()
        access_token = str(token_data.get("access_token") or "").strip()
        if not refresh_token or not access_token:
            raise RuntimeError("Missing refresh_token/access_token")
        expires_in = int(token_data.get("expires_in") or 3600)
        meta = {
            "auth_type": "oauth",
            "access_token": access_token,
            "expires_at": time.time() + max(60, expires_in),
            "scope": str(token_data.get("scope") or "").strip(),
            "token_type": str(token_data.get("token_type") or "Bearer").strip(),
            "connected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
        _upsert_user_connection(user_id=str(user.id), key=_BING_OAUTH_CONNECTION_KEY, value=refresh_token, meta=meta)
        _delete_user_connection(user_id=str(user.id), key="BING_WEBMASTER_API_KEY")
    except Exception as e:
        _audit_log(
            request,
            action="oauth.bing.connect",
            status="error",
            user=user,
            target_type="connection",
            target_id=_BING_OAUTH_CONNECTION_KEY,
            meta={"error": str(e)[:240]},
        )
        return RedirectResponse(url=_path_with_flash(return_to, err=f"Bing OAuth: {type(e).__name__}: {e}"), status_code=303)
    _audit_log(
        request,
        action="oauth.bing.connect",
        status="ok",
        user=user,
        target_type="connection",
        target_id=_BING_OAUTH_CONNECTION_KEY,
    )
    return RedirectResponse(url=_path_with_flash(return_to, msg="Bing connecté."), status_code=303)


@app.post("/oauth/bing/disconnect")
def bing_oauth_disconnect(request: Request, next: str = Form(default="/settings/accounts#bing-connect-card")) -> RedirectResponse:
    user = getattr(request.state, "user", None)
    if not user:
        raise HTTPException(status_code=401, detail="auth_required")
    _delete_user_connection(user_id=str(user.id), key=_BING_OAUTH_CONNECTION_KEY)
    _audit_log(request, action="oauth.bing.disconnect", status="ok", user=user, target_type="connection", target_id=_BING_OAUTH_CONNECTION_KEY)
    return RedirectResponse(url=_path_with_flash(next, msg="Bing déconnecté."), status_code=303)


@app.get("/api/projects/{slug}/gsc/properties")
def gsc_properties_for_project(request: Request, slug: str) -> JSONResponse:
    _ = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    user_id = str(getattr(user, "id", "") or "")
    token = _gsc_oauth_refresh_token(str(getattr(user, "id", "")), slug)
    if not token:
        return JSONResponse({"ok": False, "error": "Google OAuth non connecté pour ce projet."}, status_code=400)

    client_id, client_secret = _google_oauth_client()
    if not client_id or not client_secret:
        return JSONResponse({"ok": False, "error": "Google OAuth non configuré (client id/secret)."}, status_code=400)

    try:
        access_token = _google_oauth_refresh_access_token(
            refresh_token=token,
            client_id=client_id,
            client_secret=client_secret,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"AuthError: {type(e).__name__}: {e}"}, status_code=400)

    try:
        resp = requests.get(
            "https://searchconsole.googleapis.com/webmasters/v3/sites",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"RequestError: {type(e).__name__}: {e}"}, status_code=400)

    if resp.status_code != 200:
        snippet = (resp.text or "").strip()
        if len(snippet) > 400:
            snippet = snippet[:400] + "…"
        return JSONResponse({"ok": False, "error": f"HTTP {resp.status_code}: {snippet}"}, status_code=400)

    try:
        data = resp.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"JSONDecodeError: {e}"}, status_code=400)

    entries = data.get("siteEntry") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        entries = []

    existing_by_base = _db_project_lookup_by_base_url(user_id)
    props: list[dict[str, Any]] = []
    for it in entries:
        if not isinstance(it, dict):
            continue
        site_url = str(it.get("siteUrl") or "").strip()
        perm = str(it.get("permissionLevel") or "").strip()
        if not site_url:
            continue
        if perm.lower() in {"siteunverifieduser"}:
            continue

        suggested = ""
        domain = ""
        if site_url.startswith("sc-domain:"):
            domain = site_url.split(":", 1)[1].strip()
            suggested = _normalize_base_url(domain) or ""
        elif site_url.startswith(("http://", "https://")):
            suggested = _normalize_base_url(site_url) or ""
            domain = (urlsplit(suggested).hostname or "").lower() if suggested else ""
        else:
            domain = site_url
            suggested = _normalize_base_url(site_url) or ""

        existing_slug = existing_by_base.get(suggested or "")
        props.append(
            {
                "property_url": site_url,
                "permission": perm,
                "domain": domain,
                "suggested_base_url": suggested,
                "already_imported": bool(existing_slug),
                "project_slug": existing_slug or "",
            }
        )

    props.sort(key=lambda p: (p.get("domain") or p.get("property_url") or "").lower())
    return JSONResponse({"ok": True, "properties": props})


@app.get("/api/gsc/properties")
def gsc_properties(request: Request) -> JSONResponse:
    _ = _require_system_owner(request)
    creds = (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()
    if not creds:
        return JSONResponse({"ok": False, "error": "GOOGLE_APPLICATION_CREDENTIALS not set"}, status_code=400)

    cred_path = Path(creds).expanduser()
    if not cred_path.is_absolute():
        cred_path = (REPO_ROOT / cred_path).resolve()
    if not cred_path.exists():
        return JSONResponse({"ok": False, "error": f"Credentials file not found: {cred_path}"}, status_code=400)

    try:
        from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore
        from google.oauth2 import service_account  # type: ignore
    except ModuleNotFoundError:
        return JSONResponse(
            {"ok": False, "error": "Missing dependency: google-auth (pip install google-auth)"},
            status_code=400,
        )

    scope = "https://www.googleapis.com/auth/webmasters.readonly"
    try:
        creds_obj = service_account.Credentials.from_service_account_file(str(cred_path), scopes=[scope])
        creds_obj.refresh(GoogleAuthRequest())
        token = str(getattr(creds_obj, "token", "") or "")
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"AuthError: {type(e).__name__}: {e}"}, status_code=400)

    if not token:
        return JSONResponse({"ok": False, "error": "AuthError: no access token"}, status_code=400)

    try:
        resp = requests.get(
            "https://searchconsole.googleapis.com/webmasters/v3/sites",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"RequestError: {type(e).__name__}: {e}"}, status_code=400)

    if resp.status_code != 200:
        snippet = (resp.text or "").strip()
        if len(snippet) > 400:
            snippet = snippet[:400] + "…"
        return JSONResponse({"ok": False, "error": f"HTTP {resp.status_code}: {snippet}"}, status_code=400)

    try:
        data = resp.json()
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"JSONDecodeError: {e}"}, status_code=400)

    entries = data.get("siteEntry") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        entries = []

    props: list[dict[str, Any]] = []
    for it in entries:
        if not isinstance(it, dict):
            continue
        site_url = str(it.get("siteUrl") or "").strip()
        perm = str(it.get("permissionLevel") or "").strip()
        if not site_url:
            continue
        if perm.lower() in {"siteunverifieduser"}:
            continue

        suggested = ""
        domain = ""
        if site_url.startswith("sc-domain:"):
            domain = site_url.split(":", 1)[1].strip()
            suggested = _normalize_base_url(domain) or ""
        elif site_url.startswith(("http://", "https://")):
            suggested = _normalize_base_url(site_url) or ""
            domain = (urlsplit(suggested).hostname or "").lower() if suggested else ""
        else:
            domain = site_url
            suggested = _normalize_base_url(site_url) or ""

        props.append({"property_url": site_url, "permission": perm, "domain": domain, "suggested_base_url": suggested})

    props.sort(key=lambda p: (p.get("domain") or p.get("property_url") or "").lower())
    return JSONResponse({"ok": True, "properties": props})


@app.get("/api/bing/sites")
def bing_sites(request: Request) -> JSONResponse:
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"ok": False, "error": "auth_required"}, status_code=401)

    user_id = str(getattr(user, "id", "") or "")
    auth = _effective_bing_connection(user_id=user_id)
    if not auth.get("token"):
        return JSONResponse({"ok": False, "error": "Bing non connecté."}, status_code=400)
    if auth.get("source") != "user":
        return JSONResponse(
            {"ok": False, "error": "Connecte ton propre compte Bing pour lister et importer tes sites."},
            status_code=400,
        )

    try:
        params: dict[str, Any] = {}
        headers: dict[str, str] = {}
        if auth.get("mode") == "oauth":
            headers["Authorization"] = f"Bearer {auth.get('token')}"
        else:
            params["apikey"] = str(auth.get("token") or "")
        r = requests.get(
            "https://www.bing.com/webmaster/api.svc/json/GetUserSites",
            params=params,
            headers=headers,
            timeout=20,
        )
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else None
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"BingError: {type(e).__name__}: {e}"}, status_code=400)

    if isinstance(data, dict) and isinstance(data.get("ErrorCode"), int) and int(data.get("ErrorCode")) != 0:
        msg = str(data.get("Message") or "bing_api_error")
        return JSONResponse({"ok": False, "error": f"BingError: {msg}"}, status_code=400)

    # Extract http(s) URLs (simple + robust across payload shapes).
    sites: list[str] = []
    try:
        blob = json.dumps(data, ensure_ascii=False)
        for u in re.findall(r"https?://[^\\s\"\\\\]+", blob):
            if u not in sites:
                sites.append(u)
    except Exception:
        pass

    existing_by_base = _db_project_lookup_by_base_url(user_id)
    items: list[dict[str, Any]] = []
    for site_url in sites[:200]:
        base = _normalize_base_url(site_url) or ""
        existing_slug = existing_by_base.get(base)
        items.append(
            {
                "site_url": site_url,
                "import_base_url": base,
                "domain": (urlsplit(base).hostname or "").lower() if base else "",
                "already_imported": bool(existing_slug),
                "project_slug": existing_slug or "",
            }
        )

    return JSONResponse({"ok": True, "sites": items})


@app.get("/api/projects/{slug}/search-series")
def project_search_series(request: Request, slug: str, source: str, days: int | None = None) -> JSONResponse:
    proj = _db_project_or_404(request, slug)
    _, gsc_cfg, bing_cfg = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj.settings if isinstance(proj.settings, dict) else {}),
    )

    source_key = str(source or "").strip().lower()
    requested_days = max(1, min(int(days or 28), 365))
    user = getattr(request.state, "user", None)

    if source_key == "gsc":
        payload = _fetch_gsc_live_series(
            user_id=str(getattr(user, "id", "")),
            slug=slug,
            base_url=str(proj.base_url or ""),
            gsc_cfg=gsc_cfg,
            days=requested_days,
        )
        status_code = 200 if payload.get("ok") else 400
    elif source_key == "bing":
        payload = _fetch_bing_live_series(
            user_id=str(getattr(user, "id", "")),
            base_url=str(proj.base_url or ""),
            bing_cfg=bing_cfg,
            days=requested_days,
        )
        status_code = 200 if payload.get("ok") else 400
    else:
        payload = {"ok": False, "error": "source must be gsc or bing"}
        status_code = 400

    resp = JSONResponse(payload, status_code=status_code)
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _perf_items_csv(items: list[dict[str, Any]], *, dim: str) -> bytes:
    out = io.StringIO()
    dimension_header = "query" if dim == "query" else "page"
    writer = csv.DictWriter(out, fieldnames=[dimension_header, "clicks", "impressions", "ctr", "position"])
    writer.writeheader()
    for it in items:
        if not isinstance(it, dict):
            continue
        writer.writerow(
            {
                dimension_header: str(it.get("keyword") or ""),
                "clicks": _to_int(it.get("clicks")),
                "impressions": _to_int(it.get("impressions")),
                "ctr": _to_float(it.get("ctr")),
                "position": _to_float(it.get("position")),
            }
        )
    return out.getvalue().encode("utf-8")


@app.get("/api/projects/{slug}/search-items")
def project_search_items(
    request: Request,
    slug: str,
    source: str,
    dim: str,
    days: int | None = None,
    limit: int | None = None,
    format: str | None = None,
) -> Response:
    proj = _db_project_or_404(request, slug)
    _, gsc_cfg, bing_cfg = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj.settings if isinstance(proj.settings, dict) else {}),
    )

    source_key = str(source or "").strip().lower()
    dimension = str(dim or "query").strip().lower()
    if dimension not in {"query", "page"}:
        dimension = "query"

    default_days = int((gsc_cfg.get("days") if source_key == "gsc" else bing_cfg.get("days")) or 28)
    requested_days = max(1, min(int(days or default_days), 365))
    requested_limit = max(1, min(int(limit or 200), 5000))

    user = getattr(request.state, "user", None)
    if source_key == "gsc":
        payload = _fetch_gsc_live_items(
            user_id=str(getattr(user, "id", "")),
            slug=slug,
            base_url=str(proj.base_url or ""),
            gsc_cfg=gsc_cfg,
            days=requested_days,
            dim=dimension,
            limit=requested_limit,
        )
    elif source_key == "bing":
        payload = _fetch_bing_live_items(
            user_id=str(getattr(user, "id", "")),
            base_url=str(proj.base_url or ""),
            bing_cfg=bing_cfg,
            days=requested_days,
            dim=dimension,
            limit=requested_limit,
        )
    else:
        payload = {"ok": False, "error": "source must be gsc or bing"}

    status_code = 200 if payload.get("ok") else 400
    fmt = str(format or "").strip().lower()
    if fmt == "csv":
        if not payload.get("ok"):
            resp = JSONResponse(payload, status_code=status_code)
            resp.headers["Cache-Control"] = "no-store"
            return resp
        csv_bytes = _perf_items_csv(payload.get("items") if isinstance(payload.get("items"), list) else [], dim=dimension)
        filename = f"{slug}-{source_key}-{dimension}-{payload.get('start_date')}-{payload.get('end_date')}.csv"
        resp = Response(content=csv_bytes, media_type="text/csv")
        resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        resp.headers["Cache-Control"] = "no-store"
        return resp

    resp = JSONResponse(payload, status_code=status_code)
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/automation", response_class=HTMLResponse)
def automation(request: Request) -> HTMLResponse:
    _ = _require_admin(request)
    config_path = DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None
    runs_dir = DEFAULT_RUNS_DIR
    latest = _load_latest_global_summary(runs_dir) if runs_dir.exists() else None

    inventory = _inventory_preview(Path(config_path)) if config_path else None
    try:
        cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) if config_path else None
    except Exception:
        cfg = None
    defaults = cfg.get("defaults") if isinstance(cfg, dict) and isinstance(cfg.get("defaults"), dict) else {}
    crawl_defaults = defaults.get("crawl") if isinstance(defaults.get("crawl"), dict) else {}
    gsc_api_defaults = defaults.get("gsc_api") if isinstance(defaults.get("gsc_api"), dict) else {}

    gsc_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    gsc_creds_exists = bool(gsc_creds and Path(gsc_creds).expanduser().exists())

    env_status = {
        "pagespeed_api_key_set": bool(os.environ.get("PAGESPEED_API_KEY")),
        "gsc_credentials_set": bool(gsc_creds),
        "gsc_credentials_exists": gsc_creds_exists,
    }
    config_status = {
        "pagespeed_enabled": bool(crawl_defaults.get("pagespeed") or False),
        "gsc_api_enabled": bool(gsc_api_defaults.get("enabled") or False),
    }

    resp = templates.TemplateResponse(
        "automation.html",
        {
            "request": request,
            "repo_root": str(REPO_ROOT),
            "config_path": str(config_path) if config_path else None,
            "runs_dir": str(runs_dir),
            "latest": latest,
            "jobs": _list_jobs(),
            "inventory": inventory,
            "env_status": env_status,
            "config_status": config_status,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/jobs", response_class=HTMLResponse)
def jobs(request: Request, job: str | None = None) -> HTMLResponse:
    raw_jobs = _list_jobs(limit=100)
    jobs_view: list[dict[str, Any]] = []
    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    for j in raw_jobs:
        result = j.result if isinstance(j.result, dict) else None
        if not is_admin:
            owner_id = str(result.get("user_id") or "").strip() if result else ""
            if not owner_id or owner_id != str(getattr(user, "id", "")):
                continue
        kind = _job_kind_from_command(j.command) or "unknown"
        slug = str(result.get("slug") or "").strip() if result else ""
        run_ts = str(result.get("timestamp") or "").strip() if result else ""
        run_dt = dash.parse_timestamp(run_ts) if run_ts else None
        created_dt = datetime.fromtimestamp(float(j.created_at)) if j.created_at else None
        ts_label = (run_dt.strftime("%d/%m/%y") if run_dt else None) or (
            created_dt.strftime("%d/%m/%y") if created_dt else ""
        )
        jobs_view.append(
            {
                "id": j.id,
                "status": j.status,
                "kind": kind,
                "slug": slug,
                "timestamp": run_ts,
                "timestamp_label": ts_label,
                "created_at": j.created_at,
                "progress": j.progress,
                "result": result,
            }
        )

    resp = templates.TemplateResponse(
        "jobs.html",
        {
            "request": request,
            "jobs": jobs_view,
            "highlight_job_id": (job or "").strip(),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp

@app.get("/file", response_class=HTMLResponse)
def view_file(request: Request, path: str) -> HTMLResponse:
    raw_path = Path(path).expanduser()
    if not raw_path.is_absolute():
        raw_path = (REPO_ROOT / raw_path).resolve()
    else:
        raw_path = raw_path.resolve()

    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    allowed_roots = [DEFAULT_RUNS_DIR.resolve(), DATA_DIR.resolve()] if is_admin else [_runs_dir_for_request(request).resolve()]
    if not any(raw_path.is_relative_to(root) for root in allowed_roots):
        return HTMLResponse("Path not allowed", status_code=403)
    if not raw_path.exists():
        _ensure_runs_artifact_local(raw_path)
    if not raw_path.exists() or not raw_path.is_file():
        return HTMLResponse("File not found", status_code=404)

    content = raw_path.read_text(encoding="utf-8", errors="replace")
    return templates.TemplateResponse(
        "file.html",
        {"request": request, "path": str(raw_path), "content": content},
    )


@app.post("/run")
def run(
    request: Request,
    background_tasks: BackgroundTasks,
    config_path: str = Form(default=str(DEFAULT_CONFIG)),
    run_policy: str = Form(default="verify"),
    confirm_auto: str | None = Form(default=None),
    site: str | None = Form(default=None),
) -> RedirectResponse:
    user = _require_admin(request)
    retry_after = _rate_limit_retry_after(
        bucket="automation_run_user", subject=str(getattr(user, "id", "")), limit=4, window_s=60
    )
    if isinstance(retry_after, int):
        _audit_log(
            request,
            action="automation.run",
            status="rate_limited",
            user=user,
            meta={"retry_after_s": retry_after},
        )
        return RedirectResponse(
            url=_path_with_flash("/automation", err=f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."),
            status_code=303,
        )
    cfg = Path(config_path).expanduser()
    if not cfg.is_absolute():
        cfg = (REPO_ROOT / cfg).resolve()

    extra_args: list[str] = []

    site = (site or "").strip()
    if site:
        extra_args.extend(["--site", site])

    if run_policy == "verify":
        # Keep it safe (no deploy), but allow GSC when configured.
        extra_args.extend(["--mode", "audit-only", "--no-auto-deploy", "--no-backlog"])

    if run_policy == "auto" and confirm_auto:
        # Double-opt-in: (1) user chooses "auto" in UI, (2) user checks confirm box.
        extra_args.extend(["--mode", "execute", "--auto-deploy", "--execute"])

    job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time(), config_path=str(cfg))
    job.result = {
        "type": "autopilot",
        "user_id": str(getattr(user, "id", "")),
        "run_policy": run_policy,
        "site": site or "",
        "extra_args": extra_args,
    }
    # Pre-fill command so the Jobs UI can immediately categorize the job.
    script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_autopilot.py"
    cmd_preview = [sys.executable, "-u", str(script), "--config", str(cfg)]
    if extra_args:
        cmd_preview.extend(extra_args)
    job.command = cmd_preview
    _save_job(job)
    _audit_log(
        request,
        action="automation.run",
        status="queued",
        user=user,
        target_type="job",
        target_id=job.id,
        meta={"run_policy": run_policy, "site": site or ""},
    )
    return RedirectResponse(url=f"/jobs?job={job.id}", status_code=303)


@app.post("/projects/{slug}/crawl")
def crawl_project(
    request: Request,
    slug: str,
    config_path: str = Form(default=str(DEFAULT_CONFIG)),
) -> Response:
    proj = _db_project_or_404(request, slug)
    user = getattr(request.state, "user", None)
    retry_after = _rate_limit_retry_after(
        bucket="crawl_project_user",
        subject=str(getattr(user, "id", "") or _request_client_ip(request) or slug),
        limit=8,
        window_s=60,
    )
    if isinstance(retry_after, int):
        msg = f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."
        _audit_log(
            request,
            action="crawl.start",
            status="rate_limited",
            user=user,
            target_type="project",
            target_id=slug,
            meta={"retry_after_s": retry_after},
        )
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=429, headers={"Retry-After": str(retry_after)})
        return RedirectResponse(url=f"/projects/{slug}?err={quote(msg)}", status_code=303)
    cfg = Path(config_path).expanduser()
    if not cfg.is_absolute():
        cfg = (REPO_ROOT / cfg).resolve()

    project_settings = proj.settings if isinstance(proj.settings, dict) else {}
    crawl_cfg, _, _ = _effective_project_crawl_settings(
        slug, config_path=(cfg if cfg.exists() else None), project_settings=project_settings
    )
    requested_max_pages = int(crawl_cfg.get("max_pages") or 300)
    planned_pages = max(0, requested_max_pages)
    override_max_pages: int | None = None

    job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time(), config_path=str(cfg))
    job.result = {
        "type": "crawl",
        "slug": slug,
        "user_id": str(getattr(user, "id", "")),
        "requested_max_pages": requested_max_pages,
    }
    # Pre-fill command so the Jobs UI can categorize immediately.
    script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
    job.command = [sys.executable, "-u", str(script)]

    if not bool(getattr(user, "is_admin", False)):
        with DB.session() as db:
            ok, remaining = billing.ensure_within_quota(
                db, user_id=str(getattr(user, "id", "")), metric="pages_crawled_month", planned_amount=planned_pages
            )
            if (not ok) and isinstance(remaining, int) and remaining > 0:
                planned_pages = int(remaining)
                override_max_pages = int(remaining)
            elif not ok:
                msg = "Quota crawl mensuel atteint. Va sur Abonnement pour upgrade."
                _audit_log(
                    request,
                    action="crawl.start",
                    status="quota_reached",
                    user=user,
                    target_type="project",
                    target_id=slug,
                    meta={"requested_max_pages": requested_max_pages},
                )
                if _client_wants_json(request):
                    return JSONResponse({"ok": False, "error": msg, "billing_url": "/billing"}, status_code=402)
                return RedirectResponse(url=f"/projects/{slug}?err={quote(msg)}", status_code=303)

            billing.usage_add(
                db,
                user_id=str(getattr(user, "id", "")),
                metric="pages_crawled_month",
                amount=int(planned_pages),
                meta={
                    "kind": "crawl_reserve",
                    "job_id": job.id,
                    "slug": slug,
                    "requested_max_pages": requested_max_pages,
                },
            )

        if override_max_pages:
            job.result["override_max_pages"] = int(override_max_pages)
        job.result["quota_reserved_pages"] = int(planned_pages)
    else:
        job.result["skip_billing"] = True
    _save_job(job)
    _audit_log(
        request,
        action="crawl.start",
        status="queued",
        user=user,
        target_type="project",
        target_id=slug,
        meta={"job_id": job.id, "requested_max_pages": requested_max_pages, "planned_pages": planned_pages},
    )
    if _client_wants_json(request):
        return JSONResponse({"ok": True, "slug": slug, "job_id": job.id, "status": job.status})
    return RedirectResponse(url=f"/projects/{slug}?job={job.id}", status_code=303)


@app.post("/projects/crawl-batch")
def crawl_projects_batch(
    request: Request,
    config_path: str = Form(default=str(DEFAULT_CONFIG)),
    slugs: list[str] = Form(default=[]),
) -> Response:
    user = getattr(request.state, "user", None)
    retry_after = _rate_limit_retry_after(
        bucket="crawl_batch_user",
        subject=str(getattr(user, "id", "") or _request_client_ip(request) or "batch"),
        limit=4,
        window_s=60,
    )
    if isinstance(retry_after, int):
        msg = f"Trop de tentatives. Réessaie dans {_format_retry_after(retry_after)}."
        _audit_log(
            request,
            action="crawl.batch",
            status="rate_limited",
            user=user,
            meta={"retry_after_s": retry_after},
        )
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": msg}, status_code=429, headers={"Retry-After": str(retry_after)})
        return RedirectResponse(url=_path_with_flash("/", err=msg), status_code=303)
    cfg = Path(config_path).expanduser()
    if not cfg.is_absolute():
        cfg = (REPO_ROOT / cfg).resolve()

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in slugs or []:
        s = str(raw or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        normalized.append(s)

    if not normalized:
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": "Aucun projet sélectionné"}, status_code=400)
        return RedirectResponse(url="/", status_code=303)

    allowed = [s for s in normalized if user and _db_project(str(user.id), s)]
    if not allowed:
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": "Aucun projet autorisé"}, status_code=403)
        return RedirectResponse(url="/", status_code=303)

    job_ids: list[str] = []
    jobs: list[dict[str, str]] = []
    is_admin = bool(getattr(user, "is_admin", False))
    capped_any = False
    if is_admin:
        for slug in allowed:
            job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time(), config_path=str(cfg))
            job.result = {"type": "crawl", "slug": slug, "user_id": str(getattr(user, "id", "")), "skip_billing": True}
            script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
            job.command = [sys.executable, "-u", str(script)]
            _save_job(job)
            job_ids.append(job.id)
            jobs.append({"slug": slug, "job_id": job.id, "status": job.status})
    else:
        with DB.session() as db:
            for slug in allowed:
                proj = _db_project(str(getattr(user, "id", "")), slug)
                project_settings = proj.settings if (proj and isinstance(proj.settings, dict)) else {}
                crawl_cfg, _, _ = _effective_project_crawl_settings(
                    slug, config_path=(cfg if cfg.exists() else None), project_settings=project_settings
                )
                requested_max_pages = int(crawl_cfg.get("max_pages") or 300)
                planned_pages = max(0, requested_max_pages)
                override_max_pages: int | None = None

                ok, remaining = billing.ensure_within_quota(
                    db, user_id=str(getattr(user, "id", "")), metric="pages_crawled_month", planned_amount=planned_pages
                )
                if (not ok) and isinstance(remaining, int) and remaining > 0:
                    planned_pages = int(remaining)
                    override_max_pages = int(remaining)
                    capped_any = True
                elif not ok:
                    capped_any = True
                    break

                job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time(), config_path=str(cfg))
                job.result = {
                    "type": "crawl",
                    "slug": slug,
                    "user_id": str(getattr(user, "id", "")),
                    "requested_max_pages": requested_max_pages,
                    "quota_reserved_pages": int(planned_pages),
                }
                if override_max_pages:
                    job.result["override_max_pages"] = int(override_max_pages)
                script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_audit.py"
                job.command = [sys.executable, "-u", str(script)]

                billing.usage_add(
                    db,
                    user_id=str(getattr(user, "id", "")),
                    metric="pages_crawled_month",
                    amount=int(planned_pages),
                    meta={
                        "kind": "crawl_reserve",
                        "job_id": job.id,
                        "slug": slug,
                        "requested_max_pages": requested_max_pages,
                    },
                )

                _save_job(job)
                job_ids.append(job.id)
                jobs.append({"slug": slug, "job_id": job.id, "status": job.status})

    if _client_wants_json(request):
        if not jobs:
            return JSONResponse(
                {"ok": False, "error": "Quota crawl mensuel atteint.", "billing_url": "/billing"}, status_code=402
            )
        return JSONResponse({"ok": True, "jobs": jobs, "capped": capped_any})

    if not jobs:
        _audit_log(
            request,
            action="crawl.batch",
            status="quota_reached",
            user=user,
            meta={"requested_slugs": normalized[:50]},
        )
        return RedirectResponse(url=f"/?err={quote('Quota crawl mensuel atteint. Va sur Abonnement pour upgrade.')}", status_code=303)

    _audit_log(
        request,
        action="crawl.batch",
        status="queued",
        user=user,
        meta={"count": len(jobs), "jobs": jobs[:50], "capped": capped_any},
    )
    if len(jobs) == 1:
        return RedirectResponse(url=f"/projects/{jobs[0]['slug']}?job={job_ids[0]}", status_code=303)
    if capped_any and jobs:
        return RedirectResponse(url=f"/jobs?job={job_ids[0]}&msg={quote('Quota atteint: certains crawls ont été ignorés.')}", status_code=303)
    return RedirectResponse(url=f"/jobs?job={job_ids[0]}", status_code=303)


@app.get("/projects/{slug}", response_class=HTMLResponse)
def project_overview(
    request: Request, slug: str, crawl: str | None = None, compare: str | None = None, job: str | None = None
) -> HTMLResponse:
    proj_row = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=compare)

    live_job: dict[str, Any] | None = None
    job_id = (job or "").strip()
    j = None
    if job_id:
        j = _load_job(job_id)
        if j:
            user = getattr(request.state, "user", None)
            is_admin = bool(getattr(user, "is_admin", False))
            if not is_admin:
                result = j.result if isinstance(j.result, dict) else {}
                owner_id = str(result.get("user_id") or "").strip()
                if owner_id != str(getattr(user, "id", "")):
                    j = None

        if j:
            # Guardrail: only attach the job if it looks like it belongs to this project crawl.
            job_slug = j.result.get("slug") if isinstance(j.result, dict) else None
            if job_slug in {None, "", slug}:
                live_job = {
                    "id": j.id,
                    "status": j.status,
                    "created_at": j.created_at,
                    "started_at": j.started_at,
                    "finished_at": j.finished_at,
                    "progress": j.progress,
                    "result": j.result,
                }
    if not live_job:
        user = getattr(request.state, "user", None)
        is_admin = bool(getattr(user, "is_admin", False))
        for candidate in _list_jobs(limit=100):
            result = candidate.result if isinstance(candidate.result, dict) else {}
            if result.get("type") != "crawl":
                continue
            if str(result.get("slug") or "").strip() != slug:
                continue
            if not is_admin and str(result.get("user_id") or "").strip() != str(getattr(user, "id", "")):
                continue
            live_job = {
                "id": candidate.id,
                "status": candidate.status,
                "created_at": candidate.created_at,
                "started_at": candidate.started_at,
                "finished_at": candidate.finished_at,
                "progress": candidate.progress,
                "result": candidate.result,
            }
            break

    if not data:
        data = {
            "slug": slug,
            "site_name": str(proj_row.site_name or slug),
            "base_url": str(proj_row.base_url or ""),
            "crawls": [],
            "current": None,
            "compare": None,
            "history": [],
        }

    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    _, effective_gsc, effective_bing = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj_row.settings if isinstance(proj_row.settings, dict) else {}),
    )
    gsc_creds_path, gsc_auth_mode = _gsc_live_credentials_path(user_id=str(getattr(user, "id", "")), slug=slug)
    live_series = {
        "gsc": {
            "enabled": bool(effective_gsc.get("enabled")) if "enabled" in effective_gsc else True,
            "days": int(effective_gsc.get("days") or 28),
            "credentials_ready": bool(gsc_creds_path and gsc_creds_path.exists()),
            "auth_mode": gsc_auth_mode,
        },
        "bing": {
            "enabled": bool(effective_bing.get("enabled")) if "enabled" in effective_bing else False,
            "days": int(effective_bing.get("days") or 28),
            "credentials_ready": bool(_effective_bing_connection(user_id=str(getattr(user, "id", ""))).get("token")),
        },
    }
    plan_key = "free"
    if user and not is_admin:
        with DB.session() as db:
            plan_key = billing.effective_plan_key(db, user_id=str(getattr(user, "id", "")))

    fix_pack_unlocked = is_admin or plan_key in {"solo", "pro", "business"}

    top_actions: list[fix_pack.TopAction] = []
    try:
        cur = data.get("current") if isinstance(data.get("current"), dict) else {}
        ts = str(cur.get("timestamp") or "").strip()
        report = dash.load_report_json(runs_dir, slug, ts) if ts else None
        if report:
            top_actions = fix_pack.top_actions(
                report,
                site_name=str(data.get("site_name") or slug),
                base_url=str(data.get("base_url") or ""),
                limit=3,
            )
    except Exception as e:
        print(f"[FIX_PACK] top actions error: {type(e).__name__}: {e}")

    resp = templates.TemplateResponse(
        "project_overview.html",
        {
            "request": request,
            "project": data,
            "slug": slug,
            "live_job": live_job,
            "top_actions": top_actions,
            "fix_pack_unlocked": bool(fix_pack_unlocked),
            "plan_key": plan_key,
            "live_series": live_series,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/projects/{slug}/settings/crawl", response_class=HTMLResponse)
def project_crawl_settings(
    request: Request,
    slug: str,
    msg: str | None = None,
    err: str | None = None,
    prefill_gsc_days: int | None = None,
    prefill_bing_days: int | None = None,
) -> HTMLResponse:
    proj = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    project = dash.project_overview(runs_dir, slug, timestamp=None, compare_to=None)
    if not project:
        project = {
            "slug": slug,
            "site_name": str(proj.site_name or slug),
            "base_url": str(proj.base_url or ""),
            "crawls": [],
            "current": {"timestamp": ""},
        }

    crawl, gsc, bing = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj.settings if isinstance(proj.settings, dict) else {}),
    )
    if isinstance(prefill_gsc_days, int) and prefill_gsc_days > 0:
        gsc = dict(gsc)
        gsc["days"] = int(prefill_gsc_days)
    if isinstance(prefill_bing_days, int) and prefill_bing_days > 0:
        bing = dict(bing)
        bing["days"] = int(prefill_bing_days)

    client_id, client_secret = _google_oauth_client()
    user = getattr(request.state, "user", None)
    gsc_oauth = {
        "configured": bool(client_id and client_secret and _safe_env("SEO_AGENT_SECRET_KEY")),
        "connected": _gsc_oauth_connected(str(getattr(user, "id", "")), slug),
        "redirect_uri": _google_oauth_redirect_uri(request) if (client_id and client_secret) else "",
        "scope": _GOOGLE_OAUTH_SCOPE,
        "settings_url": "/settings/accounts#gsc-oauth-card",
        "system_url": "/settings/system#gsc-oauth-system",
    }
    bing_auth = _effective_bing_connection(user_id=str(getattr(user, "id", "")))
    bing_api_ready = bool(bing_auth.get("token"))
    resp = templates.TemplateResponse(
        "crawl_settings.html",
        {
            "request": request,
            "project": project,
            "slug": slug,
            "msg": (msg or "").strip(),
            "err": (err or "").strip(),
            "crawl": crawl,
            "gsc": gsc,
            "gsc_oauth": gsc_oauth,
            "bing": bing,
            "bing_api_ready": bing_api_ready,
            "bing_auth_mode": str(bing_auth.get("mode") or ""),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/projects/{slug}/settings/crawl")
def project_crawl_settings_save(
    request: Request,
    slug: str,
    max_pages: int = Form(default=300),
    workers: int = Form(default=6),
    timeout_s: float = Form(default=8.0),
    profile: str = Form(default="ahrefs"),
    check_resources: str | None = Form(default=None),
    pagespeed: str | None = Form(default=None),
    gsc_enabled: str | None = Form(default=None),
    gsc_min_impressions: int = Form(default=200),
    gsc_inspection_enabled: str | None = Form(default=None),
    gsc_inspection_max_urls: int = Form(default=0),
    gsc_inspection_timeout_s: float = Form(default=30.0),
    gsc_inspection_language: str = Form(default=""),
    bing_enabled: str | None = Form(default=None),
    bing_min_impressions: int = Form(default=200),
    bing_days: int = Form(default=28),
    bing_site_url: str = Form(default=""),
    bing_urlinfo_max: int = Form(default=0),
    bing_fetch_crawl_issues: str | None = Form(default=None),
    bing_fetch_blocked_urls: str | None = Form(default=None),
    bing_fetch_sitemaps: str | None = Form(default=None),
    bing_queries_csv: str = Form(default=""),
    bing_pages_csv: str = Form(default=""),
    ai_keywords: str | None = Form(default=None),
    backlinks_research: str | None = Form(default=None),
    allow_subdomains: str | None = Form(default=None),
    ignore_robots: str | None = Form(default=None),
    max_resources: int = Form(default=250),
    user_agent: str = Form(default="SEOAutopilot/1.0"),
    include_regex: str = Form(default=""),
    exclude_regex: str = Form(default=""),
    pagespeed_strategy: str = Form(default="mobile"),
    pagespeed_max_urls: int = Form(default=50),
    pagespeed_timeout_s: float = Form(default=60.0),
    pagespeed_workers: int = Form(default=2),
    gsc_days: int = Form(default=28),
    gsc_search_type: str = Form(default="web"),
    gsc_property: str = Form(default=""),
) -> RedirectResponse:
    proj = _db_project_or_404(request, slug)
    base_url = str(proj.base_url or "").strip()
    site_name = str(proj.site_name or slug).strip() or slug

    # Preserve sub-settings when a module is disabled (disabled checkboxes are not submitted by the browser).
    effective_crawl, effective_gsc, effective_bing = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj.settings if isinstance(proj.settings, dict) else {}),
    )
    gsc_on = bool(gsc_enabled)
    bing_on = bool(bing_enabled)

    crawl_raw: dict[str, Any] = {
        "max_pages": max_pages,
        "workers": workers,
        "timeout_s": timeout_s,
        "profile": profile,
        "check_resources": bool(check_resources),
        "pagespeed": bool(pagespeed),
        "ai_keywords": bool(ai_keywords),
        "backlinks_research": bool(backlinks_research),
        "allow_subdomains": bool(allow_subdomains),
        "ignore_robots": bool(ignore_robots),
        "max_resources": max_resources,
        "user_agent": user_agent,
        "include_regex": include_regex,
        "exclude_regex": exclude_regex,
        "pagespeed_strategy": pagespeed_strategy,
        "pagespeed_max_urls": pagespeed_max_urls,
        "pagespeed_timeout_s": pagespeed_timeout_s,
        "pagespeed_workers": pagespeed_workers,
    }
    gsc_raw: dict[str, Any] = {
        "enabled": gsc_on,
        "days": gsc_days,
        "search_type": gsc_search_type,
        "property_url": gsc_property,
        "min_impressions": gsc_min_impressions,
        "inspection_enabled": bool(gsc_inspection_enabled) if gsc_on else bool(effective_gsc.get("inspection_enabled")),
        "inspection_max_urls": gsc_inspection_max_urls,
        "inspection_timeout_s": gsc_inspection_timeout_s,
        "inspection_language": gsc_inspection_language,
    }
    bing_raw: dict[str, Any] = {
        "enabled": bing_on,
        "min_impressions": bing_min_impressions,
        "days": bing_days,
        "site_url": bing_site_url,
        "queries_csv": bing_queries_csv,
        "pages_csv": bing_pages_csv,
        "urlinfo_max": bing_urlinfo_max,
        "fetch_crawl_issues": bool(bing_fetch_crawl_issues) if bing_on else bool(effective_bing.get("fetch_crawl_issues")),
        "fetch_blocked_urls": bool(bing_fetch_blocked_urls) if bing_on else bool(effective_bing.get("fetch_blocked_urls")),
        "fetch_sitemaps": bool(bing_fetch_sitemaps) if bing_on else bool(effective_bing.get("fetch_sitemaps")),
    }

    crawl_cfg = _normalize_crawl_cfg(crawl_raw)
    gsc_cfg = _normalize_gsc_cfg(gsc_raw)
    bing_cfg = _normalize_bing_cfg(bing_raw)

    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/auth/login", status_code=303)
    with DB.session() as db:
        row = db.scalar(select(Project).where(Project.owner_user_id == str(user.id), Project.slug == slug))
        if not row:
            return RedirectResponse(url=f"/projects/{slug}/settings/crawl?err={quote('Projet introuvable')}", status_code=303)
        current_settings = row.settings if isinstance(row.settings, dict) else {}
        row.base_url = base_url
        row.site_name = site_name
        row.settings = {**current_settings, "crawl": crawl_cfg, "gsc_api": gsc_cfg, "bing": bing_cfg}
        db.add(row)
        db.commit()

    return RedirectResponse(url=f"/projects/{slug}/settings/crawl?msg={quote('Paramètres enregistrés')}", status_code=303)


@app.get("/projects/{slug}/issues", response_class=HTMLResponse)
def project_issues(
    request: Request,
    slug: str,
    crawl: str | None = None,
    compare: str | None = None,
    severity: str | None = None,
    category: str | None = None,
    q: str | None = None,
) -> HTMLResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=compare)
    if not data:
        resp = templates.TemplateResponse(
            "issues.html",
            {"request": request, "project": None, "slug": slug},
            status_code=404,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    issues = data["current"]["summary"]["issues"]
    issues_filtered = dash.filter_issues(issues, severity=severity, category=category, query=q)

    categories = sorted({it["category"] for it in issues})

    cur_node = data.get("current") if isinstance(data.get("current"), dict) else {}
    cur_ts = str(cur_node.get("timestamp") or "")
    fix_meta = _load_fix_suggestions_meta(runs_dir, slug, cur_ts) if cur_ts else None
    fix_path = str(_fix_suggestions_path(runs_dir, slug, cur_ts)) if (cur_ts and fix_meta) else ""
    resp = templates.TemplateResponse(
        "issues.html",
        {
            "request": request,
            "project": data,
            "issues": issues_filtered,
            "severity": severity or "",
            "category": category or "",
            "q": q or "",
            "categories": categories,
            "fix_suggestions_meta": fix_meta,
            "fix_suggestions_path": fix_path,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/projects/{slug}/fix-suggestions/generate")
def project_generate_fix_suggestions(request: Request, slug: str, crawl: str | None = Form(default=None)) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=None)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data.get("current") if isinstance(data.get("current"), dict) else {}
    ts = str(cur.get("timestamp") or "").strip()
    if not ts:
        raise HTTPException(status_code=400, detail="Timestamp manquant")

    report = dash.load_report_json(runs_dir, slug, ts)
    if not report:
        raise HTTPException(status_code=404, detail="report.json introuvable")

    site_name = str(data.get("site_name") or slug)
    base_url = str(data.get("base_url") or "")
    payload = fix_suggestions.build_fix_suggestions_payload(
        report=report,
        slug=slug,
        timestamp=ts,
        site_name=site_name,
        base_url=base_url,
    )

    path = _fix_suggestions_path(runs_dir, slug, ts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _sync_runs_path_to_object_store(path)

    return RedirectResponse(url=f"/projects/{slug}/issues?crawl={quote(ts)}", status_code=303)


@app.get("/projects/{slug}/issues/{issue_key}", response_class=HTMLResponse)
def project_issue_detail(
    request: Request,
    slug: str,
    issue_key: str,
    crawl: str | None = None,
    page: int = 1,
    per_page: int = 200,
    q: str | None = None,
) -> HTMLResponse:
    proj_row = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.issue_detail(runs_dir, slug, timestamp=crawl, issue_key=issue_key, page=page, per_page=per_page, q=q)
    if not data:
        resp = templates.TemplateResponse(
            "issue_detail.html",
            {"request": request, "project": None, "slug": slug, "issue_key": issue_key},
            status_code=404,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    ts = str(data.get("timestamp") or "").strip()
    fix_path_obj = _fix_suggestions_path(runs_dir, slug, ts) if ts else None
    if fix_path_obj and not fix_path_obj.exists():
        _ensure_runs_artifact_local(fix_path_obj)
    fix_path = str(fix_path_obj) if (fix_path_obj and fix_path_obj.exists()) else ""
    fix_suggestion = _load_fix_suggestion_for_issue(runs_dir, slug, ts, issue_key) if ts else None
    if not fix_suggestion:
        report = dash.load_report_json(runs_dir, slug, ts) if ts else None
        report = report if isinstance(report, dict) else {}
        issue_node = data.get("issue") if isinstance(data.get("issue"), dict) else {}
        fix_suggestion = fix_suggestions.suggest_issue_fix(
            issue_key=issue_key,
            label=str(issue_node.get("label") or issue_key),
            category=str(issue_node.get("category") or ""),
            severity=str(issue_node.get("severity") or ""),
            count=int(issue_node.get("count") or 0),
            report=report,
            site_name=str(proj_row.site_name or slug),
            base_url=str(proj_row.base_url or ""),
        )
    resp = templates.TemplateResponse(
        "issue_detail.html",
        {
            "request": request,
            "project": data,
            "slug": slug,
            "issue_key": issue_key,
            "page": int(page or 1),
            "per_page": int(per_page or 200),
            "q": (q or ""),
            "fix_suggestion": fix_suggestion,
            "fix_suggestions_path": fix_path,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/projects/{slug}/export/report.csv")
def export_project_report_csv(request: Request, slug: str, crawl: str | None = None, compare: str | None = None) -> Response:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=compare)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data["current"]
    summary = cur["summary"]
    issues_dist = summary.get("issues_distribution") if isinstance(summary.get("issues_distribution"), dict) else {}
    pagespeed = summary.get("pagespeed") if isinstance(summary.get("pagespeed"), dict) else {}
    gsc = summary.get("gsc") if isinstance(summary.get("gsc"), dict) else {}

    row = {
        "slug": slug,
        "site_name": str(data.get("site_name") or slug),
        "base_url": str(data.get("base_url") or ""),
        "timestamp": str(cur.get("timestamp") or ""),
        "health_score": int(summary.get("health_score") or 0),
        "pages_crawled": int(summary.get("pages_crawled") or 0),
        "urls_crawled": int(summary.get("urls_crawled") or 0),
        "urls_discovered": int(summary.get("urls_discovered") or 0),
        "urls_uncrawled": int(summary.get("urls_uncrawled") or 0),
        "urls_with_errors": int(summary.get("urls_with_errors") or 0),
        "issues_total": int(summary.get("issues_total") or 0),
        "issues_error": int(issues_dist.get("error") or 0),
        "issues_warning": int(issues_dist.get("warning") or 0),
        "issues_notice": int(issues_dist.get("notice") or 0),
        "pagespeed_enabled": bool(pagespeed.get("enabled") or False),
        "gsc_enabled": bool(gsc.get("enabled") or False),
    }

    fieldnames = list(row.keys())
    content = _csv_bytes([row], fieldnames=fieldnames)
    filename = f"{slug}-{row['timestamp']}-report.csv"
    return _download_response(content, media_type="text/csv; charset=utf-8", filename=filename)


@app.get("/projects/{slug}/export/report.pdf")
def export_project_report_pdf(request: Request, slug: str, crawl: str | None = None, compare: str | None = None) -> Response:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=compare)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data["current"]
    ts = str(cur.get("timestamp") or "")
    md_path = Path(str(cur.get("report_md") or ""))
    content_text = ""
    filename = f"{slug}-{ts}-report.pdf"
    if _reportlab_available():
        try:
            pdf = _reportlab_project_report_pdf(runs_dir, data)
            return _download_response(pdf, media_type="application/pdf", filename=filename)
        except Exception:
            # Fallback to a simple PDF if ReportLab fails at runtime.
            pass

    if md_path.is_file():
        content_text = md_path.read_text(encoding="utf-8", errors="replace")
    else:
        # Fallback: create a short text report from the JSON summary.
        s = cur.get("summary") if isinstance(cur.get("summary"), dict) else {}
        content_text = "\n".join(
            [
                f"Site: {data.get('site_name') or slug}",
                f"Base URL: {data.get('base_url') or ''}",
                f"Timestamp: {ts}",
                "",
                f"Health score: {int(s.get('health_score') or 0)}",
                f"Pages crawled: {int(s.get('pages_crawled') or 0)}",
                f"Issues total: {int(s.get('issues_total') or 0)}",
            ]
        )

    title = f"Rapport - {data.get('site_name') or slug} - {ts}"
    pdf = _text_to_pdf_bytes(content_text, title=title, wrap_width=110)
    return _download_response(pdf, media_type="application/pdf", filename=filename)


@app.get("/projects/{slug}/export/fix-pack.zip")
def export_project_fix_pack_zip(request: Request, slug: str, crawl: str | None = None) -> Response:
    _ = _db_project_or_404(request, slug)

    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    plan_key = "free"
    if user and not is_admin:
        with DB.session() as db:
            plan_key = billing.effective_plan_key(db, user_id=str(getattr(user, "id", "")))

    fix_pack_unlocked = is_admin or plan_key in {"solo", "pro", "business"}
    if not fix_pack_unlocked:
        msg = "Fix pack disponible à partir de Solo. Va sur Abonnement pour upgrade."
        if _client_wants_json(request):
            return JSONResponse({"ok": False, "error": msg, "billing_url": "/billing"}, status_code=402)
        return RedirectResponse(url=f"/billing?msg={quote(msg)}", status_code=303)

    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=None)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data.get("current") if isinstance(data.get("current"), dict) else {}
    ts = str(cur.get("timestamp") or "").strip()
    if not ts:
        raise HTTPException(status_code=400, detail="Timestamp manquant")

    report = dash.load_report_json(runs_dir, slug, ts)
    if not report:
        raise HTTPException(status_code=404, detail="report.json introuvable")

    content = fix_pack.build_fix_pack_zip_bytes(
        runs_dir=runs_dir,
        slug=slug,
        timestamp=ts,
        site_name=str(data.get("site_name") or slug),
        base_url=str(data.get("base_url") or ""),
        report=report,
    )
    filename = f"{slug}-{ts}-fix-pack.zip"
    return _download_response(content, media_type="application/zip", filename=filename)


@app.get("/projects/{slug}/export/issues.csv")
def export_project_issues_csv(
    request: Request,
    slug: str,
    crawl: str | None = None,
    compare: str | None = None,
    severity: str | None = None,
    category: str | None = None,
    q: str | None = None,
) -> Response:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=compare)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data["current"]
    ts = str(cur.get("timestamp") or "")
    summary = cur["summary"]
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []
    issues_filtered = dash.filter_issues(issues, severity=severity, category=category, query=q)
    report = dash.load_report_json(runs_dir, slug, ts) if ts else None

    rows: list[dict[str, Any]] = []
    for it in issues_filtered:
        issue_key = str(it.get("key") or "")
        sample_urls = _issue_sample_urls_from_report(report, issue_key, limit=10)
        rows.append(
            {
                "slug": slug,
                "site_name": str(data.get("site_name") or slug),
                "base_url": str(data.get("base_url") or ""),
                "timestamp": ts,
                "issue_key": issue_key,
                "issue_label": str(it.get("label") or ""),
                "category": str(it.get("category") or ""),
                "severity": str(it.get("severity") or ""),
                "count": int(it.get("count") or 0),
                "change": "" if it.get("change") is None else int(it.get("change") or 0),
                "sample_urls": " | ".join(sample_urls),
                "recommandation": _issue_fix_hint_text(issue_key),
            }
        )

    fieldnames = [
        "slug",
        "site_name",
        "base_url",
        "timestamp",
        "issue_key",
        "issue_label",
        "category",
        "severity",
        "count",
        "change",
        "sample_urls",
        "recommandation",
    ]
    content = _csv_bytes(rows, fieldnames=fieldnames)

    suffix = []
    if severity:
        suffix.append(str(severity))
    if category:
        suffix.append(str(category))
    filename = f"{slug}-{ts}-issues" + (f"-{'-'.join(suffix)}" if suffix else "") + ".csv"
    return _download_response(content, media_type="text/csv; charset=utf-8", filename=filename)


@app.get("/projects/{slug}/export/issues.pdf")
def export_project_issues_pdf(
    request: Request,
    slug: str,
    crawl: str | None = None,
    compare: str | None = None,
    severity: str | None = None,
    category: str | None = None,
    q: str | None = None,
) -> Response:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=compare)
    if not data:
        raise HTTPException(status_code=404, detail="Projet introuvable")

    cur = data["current"]
    ts = str(cur.get("timestamp") or "")
    summary = cur["summary"]
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []
    issues_filtered = dash.filter_issues(issues, severity=severity, category=category, query=q)

    filename = f"{slug}-{ts}-issues.pdf"
    if _reportlab_available():
        try:
            pdf = _reportlab_issues_pdf(runs_dir, data, issues_filtered, severity=severity, category=category, q=q)
            return _download_response(pdf, media_type="application/pdf", filename=filename)
        except Exception:
            pass

    lines = [
        f"Site: {data.get('site_name') or slug}",
        f"Base URL: {data.get('base_url') or ''}",
        f"Crawl: {ts}",
    ]
    filters = []
    if severity:
        filters.append(f"severity={severity}")
    if category:
        filters.append(f"category={category}")
    if q:
        filters.append(f"q={q}")
    if filters:
        lines.append("Filtres: " + ", ".join(filters))
    lines.append("")
    lines.append(f"Issues: {len(issues_filtered)}")
    lines.append("")
    for it in issues_filtered:
        change = it.get("change")
        change_txt = ""
        if change is not None:
            try:
                c = int(change)
                change_txt = f" (Δ {c:+d})"
            except Exception:
                change_txt = ""
        lines.append(
            f"[{it.get('severity')}] {it.get('category')} · {int(it.get('count') or 0)}{change_txt} — {it.get('label')} ({it.get('key')})"
        )

    title = f"Issues - {data.get('site_name') or slug} - {ts}"
    pdf = _text_to_pdf_bytes("\n".join(lines), title=title, wrap_width=110)
    return _download_response(pdf, media_type="application/pdf", filename=filename)


@app.get("/projects/{slug}/export/issues/{issue_key}.csv")
def export_project_issue_csv(request: Request, slug: str, issue_key: str, crawl: str | None = None) -> Response:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.issue_detail(runs_dir, slug, timestamp=crawl, issue_key=issue_key)
    if not data:
        raise HTTPException(status_code=404, detail="Issue introuvable")

    ts = str(data.get("timestamp") or "")
    run = dash.load_run_json(runs_dir, slug, ts) if ts else {}
    site_name = str(run.get("site_name") or slug)
    base_url = str(run.get("base_url") or "")

    issue = data.get("issue") if isinstance(data.get("issue"), dict) else {}
    label = str(issue.get("label") or issue_key)
    category = str(issue.get("category") or "")
    severity = str(issue.get("severity") or "")

    rows: list[dict[str, Any]] = []

    cwv = issue.get("cwv") if isinstance(issue.get("cwv"), dict) else None
    if cwv and isinstance(cwv.get("rows"), list):
        metric = str(cwv.get("metric") or "")
        for r in cwv.get("rows") or []:
            if not isinstance(r, dict):
                continue
            rows.append(
                {
                    "slug": slug,
                    "site_name": site_name,
                    "base_url": base_url,
                    "timestamp": ts,
                    "issue_key": issue_key,
                    "issue_label": label,
                    "category": category,
                    "severity": severity,
                    "metric": metric,
                    "url": str(r.get("url") or ""),
                    "value": r.get("value"),
                    "unit": str(r.get("unit") or ""),
                    "source": str(r.get("source") or ""),
                    "status": str(r.get("category") or ""),
                }
            )
        fieldnames = [
            "slug",
            "site_name",
            "base_url",
            "timestamp",
            "issue_key",
            "issue_label",
            "category",
            "severity",
            "metric",
            "url",
            "value",
            "unit",
            "source",
            "status",
        ]
    else:
        examples = issue.get("examples") if isinstance(issue.get("examples"), list) else []
        for ex in examples:
            rows.append(
                {
                    "slug": slug,
                    "site_name": site_name,
                    "base_url": base_url,
                    "timestamp": ts,
                    "issue_key": issue_key,
                    "issue_label": label,
                    "category": category,
                    "severity": severity,
                    "example": json.dumps(ex, ensure_ascii=False) if isinstance(ex, (dict, list)) else str(ex or ""),
                }
            )
        fieldnames = [
            "slug",
            "site_name",
            "base_url",
            "timestamp",
            "issue_key",
            "issue_label",
            "category",
            "severity",
            "example",
        ]

    content = _csv_bytes(rows, fieldnames=fieldnames)
    filename = f"{slug}-{ts}-{issue_key}.csv"
    return _download_response(content, media_type="text/csv; charset=utf-8", filename=filename)


@app.get("/projects/{slug}/export/issues/{issue_key}.pdf")
def export_project_issue_pdf(request: Request, slug: str, issue_key: str, crawl: str | None = None) -> Response:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    data = dash.issue_detail(runs_dir, slug, timestamp=crawl, issue_key=issue_key)
    if not data:
        raise HTTPException(status_code=404, detail="Issue introuvable")

    ts = str(data.get("timestamp") or "")
    run = dash.load_run_json(runs_dir, slug, ts) if ts else {}
    site_name = str(run.get("site_name") or slug)
    base_url = str(run.get("base_url") or "")

    issue = data.get("issue") if isinstance(data.get("issue"), dict) else {}
    label = str(issue.get("label") or issue_key)
    category = str(issue.get("category") or "")
    severity = str(issue.get("severity") or "")
    count = int(issue.get("count") or 0)

    lines = [
        f"Site: {site_name}",
        f"Base URL: {base_url}",
        f"Crawl: {ts}",
        "",
        f"Issue: {label}",
        f"Key: {issue_key}",
        f"Category: {category}",
        f"Severity: {severity}",
        f"Count: {count}",
        "",
    ]

    cwv = issue.get("cwv") if isinstance(issue.get("cwv"), dict) else None
    if cwv and isinstance(cwv.get("rows"), list):
        metric = str(cwv.get("metric") or "")
        lines.append(f"Core Web Vitals — metric: {metric}")
        lines.append("")
        for r in cwv.get("rows") or []:
            if not isinstance(r, dict):
                continue
            url = str(r.get("url") or "")
            val = r.get("value")
            src = str(r.get("source") or "")
            status = str(r.get("category") or "")
            lines.append(f"- {url} · {val} · {status} · {src}")
    else:
        examples = issue.get("examples") if isinstance(issue.get("examples"), list) else []
        if not examples:
            lines.append("Aucun exemple.")
        else:
            lines.append("Exemples:")
            for ex in examples:
                if isinstance(ex, (dict, list)):
                    lines.append("- " + json.dumps(ex, ensure_ascii=False))
                else:
                    lines.append("- " + str(ex or ""))

    title = f"Issue - {site_name} - {issue_key} - {ts}"
    filename = f"{slug}-{ts}-{issue_key}.pdf"
    if _reportlab_available():
        try:
            pdf = _reportlab_issue_detail_pdf(runs_dir, data)
            return _download_response(pdf, media_type="application/pdf", filename=filename)
        except Exception:
            pass

    pdf = _text_to_pdf_bytes("\n".join(lines), title=title, wrap_width=110)
    return _download_response(pdf, media_type="application/pdf", filename=filename)


@app.get("/projects/{slug}/crawls", response_class=HTMLResponse)
def project_crawls(request: Request, slug: str) -> HTMLResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    crawls = dash.list_project_crawls(runs_dir, slug)
    resp = templates.TemplateResponse(
        "crawls.html",
        {
            "request": request,
            "project": {"slug": slug},
            "slug": slug,
            "crawls": list(reversed(crawls)),
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _read_gsc_csv_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if not isinstance(r, dict):
                    continue
                norm = {str(k or "").strip().lower(): (str(v or "").strip()) for k, v in r.items()}
                key = norm.get("query") or norm.get("page") or ""
                if not key:
                    continue
                try:
                    clicks = int(float(norm.get("clicks") or "0"))
                except Exception:
                    clicks = 0
                try:
                    impressions = int(float(norm.get("impressions") or "0"))
                except Exception:
                    impressions = 0
                try:
                    ctr = float(norm.get("ctr") or "0")
                except Exception:
                    ctr = 0.0
                try:
                    position = float(norm.get("position") or "0")
                except Exception:
                    position = 0.0

                rows.append(
                    {
                        "keyword": key,
                        "clicks": clicks,
                        "impressions": impressions,
                        "ctr": ctr,
                        "position": position,
                    }
                )
    except Exception:
        return []
    return rows


def _norm_csv_header(value: str) -> str:
    raw = unicodedata.normalize("NFKD", str(value or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    raw = raw.strip().lower()
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def _decode_csv_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _parse_intish(value: str | None) -> int:
    s = str(value or "").strip().replace("\u00a0", " ")
    if not s:
        return 0
    s = re.sub(r"[^\d,\.]", "", s)
    if not s:
        return 0
    try:
        return int(float(s.replace(",", ".")))
    except Exception:
        try:
            return int(re.sub(r"[^\d]", "", s) or "0")
        except Exception:
            return 0


def _host_no_www(url: str) -> str:
    try:
        host = (urlsplit(url).hostname or "").strip().lower()
    except Exception:
        host = ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _maybe_windows_path_to_posix(path: str) -> str:
    """
    Accept Windows-style paths (e.g. C:\\Users\\...) when running under WSL/Linux.
    """
    p = str(path or "").strip()
    if not p:
        return ""
    m = re.match(r"^([A-Za-z]):\\\\(.*)$", p)
    if not m:
        return p
    drive = m.group(1).lower()
    rest = (m.group(2) or "").replace("\\\\", "/")
    return f"/mnt/{drive}/{rest}"


def _parse_backlinks_csv(data: bytes, *, target_host: str | None = None) -> tuple[str, list[dict[str, Any]]]:
    text = _decode_csv_bytes(data)
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    headers = list(reader.fieldnames or [])
    norm_to_orig = {_norm_csv_header(h): h for h in headers if str(h or "").strip()}
    avail = set(norm_to_orig.keys())

    def col(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in avail:
                return norm_to_orig[c]
        return None

    # CSV variants (FR/EN) — best-effort.
    src_col = col(
        [
            "source url",
            "source page",
            # Ahrefs exports
            "referring page url",
            "url from",
            "url_from",
            "referring url",
            "referring page",
            "page source",
            "url source",
            "page de provenance",
            "url de provenance",
            "from url",
            "from",
        ]
    )
    tgt_col = col(
        [
            "target url",
            "target page",
            # Ahrefs exports
            "linked page url",
            "url to",
            "url_to",
            "destination url",
            "destination page",
            "page cible",
            "url cible",
            "to url",
            "to",
        ]
    )
    anchor_col = col(
        [
            "anchor text",
            "anchor",
            "link text",
            "texte d ancrage",
            "texte ancrage",
            "ancre",
            "texte du lien",
        ]
    )

    domain_col = col(
        [
            "domain",
            "domaine",
            "referring domain",
            "referring domains",
            "refdomain",
            "refdomains",
            "domaines referents",
            "linking domain",
            "linking site",
            "site",
            "sites les plus frequents",
            "sites les plus frequents",
        ]
    )
    page_col = col(
        [
            "page",
            "url",
            "linked page",
            "linked page url",
            "top linked pages",
            "pages les plus liees",
            "page cible",
            "url cible",
        ]
    )
    count_col = col(
        [
            "links",
            "liens",
            "backlinks",
            "dofollow backlinks",
            "backlinks dofollow",
            "dofollow links",
            "nombre de liens",
            "total links",
            "total",
            "nb liens",
        ]
    )

    if src_col and tgt_col:
        out: list[dict[str, Any]] = []
        for r in reader:
            if not isinstance(r, dict):
                continue
            src = str(r.get(src_col) or "").strip()
            tgt = str(r.get(tgt_col) or "").strip()
            if not src or not tgt:
                continue
            if target_host:
                th = _host_no_www(tgt)
                if th and th != target_host:
                    continue
            row: dict[str, Any] = {"source_url": src, "target_url": tgt}
            if anchor_col:
                a = str(r.get(anchor_col) or "").strip()
                if a:
                    row["anchor"] = a
            out.append(row)
        return "backlinks", out

    if count_col and domain_col:
        out = []
        for r in reader:
            if not isinstance(r, dict):
                continue
            d = str(r.get(domain_col) or "").strip()
            if not d:
                continue
            d = d.lower()
            if d.startswith("www."):
                d = d[4:]
            out.append({"domain": d, "links": _parse_intish(str(r.get(count_col) or ""))})
        out.sort(key=lambda x: int(x.get("links") or 0), reverse=True)
        return "domains", out

    if count_col and page_col:
        out = []
        for r in reader:
            if not isinstance(r, dict):
                continue
            u = str(r.get(page_col) or "").strip()
            if not u:
                continue
            out.append({"url": u, "links": _parse_intish(str(r.get(count_col) or ""))})
        out.sort(key=lambda x: int(x.get("links") or 0), reverse=True)
        return "pages", out

    if count_col and anchor_col:
        out = []
        for r in reader:
            if not isinstance(r, dict):
                continue
            a = str(r.get(anchor_col) or "").strip()
            if not a:
                continue
            out.append({"anchor": a, "links": _parse_intish(str(r.get(count_col) or ""))})
        out.sort(key=lambda x: int(x.get("links") or 0), reverse=True)
        return "anchors", out

    cols = ", ".join(headers[:12]) if headers else "—"
    raise ValueError(f"CSV non reconnu (colonnes: {cols})")


def _load_backlinks_imports(dir_path: Path) -> dict[str, dict[str, dict[str, Any]]]:
    imports: dict[str, dict[str, dict[str, Any]]] = {}
    if not dir_path.exists() or not dir_path.is_dir():
        return imports
    for p in dir_path.glob("*.json"):
        m = re.match(r"^(gsc|bing|ahrefs)_(domains|pages|anchors|backlinks)\\.json$", p.name)
        if not m:
            continue
        source, kind = m.group(1), m.group(2)
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = obj.get("rows") if isinstance(obj, dict) and isinstance(obj.get("rows"), list) else []
        meta = obj.get("meta") if isinstance(obj, dict) and isinstance(obj.get("meta"), dict) else {}
        imports.setdefault(source, {})[kind] = {"rows": rows, "meta": meta}
    return imports


# Ahrefs API v3 — Site Explorer (API v2 was discontinued on 2025‑11‑01).
_AHREFS_API_BASE_URL = "https://api.ahrefs.com/v3/site-explorer"

_AHREFS_LIMITS_CACHE: dict[str, Any] = {"token_fp": "", "fetched_at": 0.0, "limits": {}}


def _ahrefs_token_fingerprint(token: str) -> str:
    try:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()
    except Exception:
        return str(len(token or ""))


def _ahrefs_limits_and_usage(token: str, *, timeout: float = 20.0) -> dict[str, Any]:
    fp = _ahrefs_token_fingerprint(token)
    now = time.time()
    if (
        _AHREFS_LIMITS_CACHE.get("token_fp") == fp
        and isinstance(_AHREFS_LIMITS_CACHE.get("limits"), dict)
        and (now - float(_AHREFS_LIMITS_CACHE.get("fetched_at") or 0.0)) < 600.0
    ):
        return dict(_AHREFS_LIMITS_CACHE.get("limits") or {})

    url = "https://api.ahrefs.com/v3/subscription-info/limits-and-usage"
    resp = requests.get(
        url,
        timeout=timeout,
        headers={
            "Accept": "application/json",
            "User-Agent": "SEO-Agent-Web/1.0",
            "Authorization": f"Bearer {token}",
        },
    )
    if resp.status_code != 200:
        body = (resp.text or "").strip()
        snippet = (body[:240] + "…") if len(body) > 240 else body
        raise RuntimeError(f"Ahrefs: HTTP {resp.status_code} — {snippet}")

    try:
        data = resp.json()
    except Exception as e:
        snippet = (resp.text or "").strip()
        snippet = (snippet[:240] + "…") if len(snippet) > 240 else snippet
        raise RuntimeError(f"Ahrefs: réponse non-JSON — {snippet}") from e

    node = data.get("limits_and_usage") if isinstance(data, dict) else None
    limits = node if isinstance(node, dict) else {}
    _AHREFS_LIMITS_CACHE.update({"token_fp": fp, "fetched_at": now, "limits": dict(limits)})
    return dict(limits)


def _ahrefs_is_free_test_target(target: str) -> bool:
    t = (target or "").strip().lower()
    if not t:
        return False
    host = _host_no_www(t) if t.startswith(("http://", "https://")) else t
    return host in {"ahrefs.com", "wordcount.com"} or host.endswith(".ahrefs.com") or host.endswith(".wordcount.com")


def _ahrefs_env_token() -> tuple[str, str]:
    # Support a few common env var names + legacy local name.
    for key in ("AHREFS_API_TOKEN", "AHREFS_TOKEN", "AHREFS_API_KEY", "AHREFS_KEY", "cle_api"):
        v = str(os.environ.get(key) or "").strip()
        if v:
            return v, key
    return "", ""


def _ahrefs_api_get(
    endpoint: str,
    *,
    token: str,
    target: str,
    mode: str = "domain",
    limit: int = 1000,
    select: str | None = None,
    timeout: float = 45.0,
) -> dict[str, Any]:
    endpoint = (endpoint or "").strip().lstrip("/")
    if not endpoint:
        raise ValueError("Ahrefs: endpoint manquant.")

    if not token:
        raise ValueError("Ahrefs: token manquant (AHREFS_API_TOKEN).")

    q: dict[str, Any] = {"target": target, "limit": int(limit)}
    if mode:
        q["mode"] = mode
    if select:
        q["select"] = select

    url = f"{_AHREFS_API_BASE_URL}/{endpoint}"
    try:
        resp = requests.get(
            url,
            params=q,
            timeout=timeout,
            headers={
                "Accept": "application/json",
                "User-Agent": "SEO-Agent-Web/1.0",
                "Authorization": f"Bearer {token}",
            },
        )
    except Exception as e:
        raise RuntimeError("Ahrefs: requête impossible.") from e

    if resp.status_code != 200:
        err = ""
        try:
            obj = resp.json()
        except Exception:
            obj = None
        if isinstance(obj, dict):
            err = str(obj.get("error") or obj.get("message") or "").strip()
        if not err:
            body = (resp.text or "").strip()
            err = (body[:240] + "…") if len(body) > 240 else body
        raise RuntimeError(f"Ahrefs: HTTP {resp.status_code} — {err}")

    try:
        data = resp.json()
    except Exception as e:
        snippet = (resp.text or "").strip()
        snippet = (snippet[:240] + "…") if len(snippet) > 240 else snippet
        raise RuntimeError(f"Ahrefs: réponse non-JSON — {snippet}") from e

    if not isinstance(data, dict):
        raise RuntimeError("Ahrefs: réponse invalide.")
    if data.get("error"):
        raise RuntimeError(f"Ahrefs: {data.get('error')}")
    return data


@app.get("/projects/{slug}/performance", response_class=HTMLResponse)
@app.get("/projects/{slug}/keywords", response_class=HTMLResponse)  # backward-compatible alias
def project_performance(
    request: Request,
    slug: str,
    crawl: str | None = None,
    source: str | None = None,
    dim: str | None = None,
    days: int | None = None,
    q: str | None = None,
    sort: str | None = None,
    dir: str | None = None,
    page: int = 1,
) -> HTMLResponse:
    proj_row = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    project = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=None)
    if not project:
        project = {
            "slug": slug,
            "site_name": str(proj_row.site_name or slug),
            "base_url": str(proj_row.base_url or ""),
            "crawls": [],
            "current": {"timestamp": ""},
        }

    src = (source or "gsc").strip().lower()
    if src not in {"gsc", "bing"}:
        src = "gsc"
    dimension = (dim or "query").strip().lower()
    if dimension not in {"query", "page"}:
        dimension = "query"

    sort_key = (sort or "clicks").strip().lower()
    if sort_key not in {"clicks", "impressions", "ctr", "position"}:
        sort_key = "clicks"
    sort_dir = (dir or "desc").strip().lower()
    if sort_dir not in {"asc", "desc"}:
        sort_dir = "desc"

    _, gsc_cfg, bing_cfg = _effective_project_crawl_settings(
        slug,
        config_path=DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None,
        project_settings=(proj_row.settings if isinstance(proj_row.settings, dict) else {}),
    )

    default_days = int((gsc_cfg.get("days") if src == "gsc" else bing_cfg.get("days")) or 28)
    requested_days = max(1, min(int(days or default_days), 365))
    fetch_limit = 5000

    user = getattr(request.state, "user", None)
    live_payload: dict[str, Any]
    if src == "gsc":
        live_payload = _fetch_gsc_live_items(
            user_id=str(getattr(user, "id", "")),
            slug=slug,
            base_url=str(proj_row.base_url or ""),
            gsc_cfg=gsc_cfg,
            days=requested_days,
            dim=dimension,
            limit=fetch_limit,
        )
    else:
        live_payload = _fetch_bing_live_items(
            user_id=str(getattr(user, "id", "")),
            base_url=str(proj_row.base_url or ""),
            bing_cfg=bing_cfg,
            days=requested_days,
            dim=dimension,
            limit=fetch_limit,
        )

    perf_ok = bool(live_payload.get("ok"))
    needle = (q or "").strip().lower()
    all_rows: list[dict[str, Any]] = (
        list(live_payload.get("items") or []) if perf_ok and isinstance(live_payload.get("items"), list) else []
    )

    if needle:
        all_rows = [r for r in all_rows if needle in str(r.get("keyword") or "").lower()]

    reverse = sort_dir == "desc"
    all_rows.sort(key=lambda r: (r.get(sort_key) is None, r.get(sort_key, 0)), reverse=reverse)

    per_page = 200
    total_rows = len(all_rows)
    pages = max(1, int(math.ceil(total_rows / per_page))) if total_rows else 1
    page = max(1, min(int(page or 1), pages))
    start = (page - 1) * per_page
    end = start + per_page
    rows = all_rows[start:end]

    totals = _timeseries_totals(all_rows)

    csv_url = f"/api/projects/{slug}/search-items?{urlencode({'source': src, 'dim': dimension, 'days': requested_days, 'limit': fetch_limit, 'format': 'csv'})}"

    resp = templates.TemplateResponse(
        "performance.html",
        {
            "request": request,
            "project": project,
            "slug": slug,
            "source": src,
            "dim": dimension,
            "days": requested_days,
            "q": q or "",
            "sort": sort_key,
            "dir": sort_dir,
            "page": page,
            "pages": pages,
            "rows": rows,
            "total_rows": total_rows,
            "perf_ok": perf_ok,
            "csv_url": csv_url if perf_ok else "",
            "live": live_payload,
            "totals": totals,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/projects/{slug}/backlinks", response_class=HTMLResponse)
@app.get("/projects/{slug}/netlinking", response_class=HTMLResponse)  # backward-compatible alias
def project_backlinks(
    request: Request, slug: str, crawl: str | None = None, msg: str | None = None, err: str | None = None
) -> HTMLResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    project = dash.project_overview(runs_dir, slug, timestamp=crawl, compare_to=None)
    if not project:
        resp = templates.TemplateResponse(
            "backlinks.html",
            {"request": request, "project": None, "slug": slug},
            status_code=404,
        )
        resp.headers["Cache-Control"] = "no-store"
        return resp

    cur = project["current"]
    ts = str(cur.get("timestamp") or "")
    report = dash.load_report_json(runs_dir, slug, ts)
    report_ok = bool(report)

    summary = {"pages_total": 0, "orphans_total": 0, "external_domains": 0}
    top_incoming: list[dict[str, Any]] = []
    top_external_domains: list[dict[str, Any]] = []

    if report_ok:
        pages = report.get("pages") if isinstance(report.get("pages"), list) else []

        urls: list[str] = []
        for p in pages:
            if not isinstance(p, dict):
                continue
            u = p.get("final_url") or p.get("url")
            if isinstance(u, str) and u.startswith(("http://", "https://")):
                urls.append(u)
        known = set(urls)

        incoming_df: dict[str, int] = {u: 0 for u in known}
        external_domains: dict[str, dict[str, Any]] = {}

        for p in pages:
            if not isinstance(p, dict):
                continue
            source = p.get("final_url") or p.get("url")
            if not isinstance(source, str) or source not in known:
                continue

            internal_df = p.get("internal_links_dofollow") if isinstance(p.get("internal_links_dofollow"), list) else []
            for t in internal_df:
                if isinstance(t, str) and t in known:
                    incoming_df[t] = incoming_df.get(t, 0) + 1

            ext = p.get("external_links") if isinstance(p.get("external_links"), list) else []
            for href in ext:
                if not isinstance(href, str) or not href.startswith(("http://", "https://")):
                    continue
                parts = urlsplit(href)
                host = (parts.hostname or "").strip().lower()
                if not host:
                    continue
                if host.startswith("www."):
                    host = host[4:]
                node = external_domains.get(host)
                if node is None:
                    node = {"domain": host, "links": 0, "pages": set()}
                    external_domains[host] = node
                node["links"] += 1
                node["pages"].add(source)

        orphans_total = sum(1 for u in known if incoming_df.get(u, 0) == 0)
        summary = {"pages_total": len(known), "orphans_total": orphans_total, "external_domains": len(external_domains)}

        top_incoming = sorted(
            [{"url": u, "count": c} for u, c in incoming_df.items()],
            key=lambda r: r["count"],
            reverse=True,
        )[:30]

        top_external_domains = sorted(
            [
                {"domain": v["domain"], "links": int(v["links"]), "pages": len(v["pages"])}
                for v in external_domains.values()
            ],
            key=lambda r: r["links"],
            reverse=True,
        )[:30]

    run_dir = (runs_dir / slug / ts).resolve()
    imports_dir = run_dir / "backlinks"
    imports_raw = _load_backlinks_imports(imports_dir)

    import_sources: list[dict[str, Any]] = []
    for key, label in [("gsc", "Google Search Console"), ("bing", "Bing Webmaster Tools"), ("ahrefs", "Ahrefs")]:
        ds = imports_raw.get(key, {}) if isinstance(imports_raw, dict) else {}

        domains_node = ds.get("domains", {}) if isinstance(ds.get("domains"), dict) else {}
        pages_node = ds.get("pages", {}) if isinstance(ds.get("pages"), dict) else {}
        anchors_node = ds.get("anchors", {}) if isinstance(ds.get("anchors"), dict) else {}
        backlinks_node = ds.get("backlinks", {}) if isinstance(ds.get("backlinks"), dict) else {}

        domains_rows = [r for r in (domains_node.get("rows") or []) if isinstance(r, dict)]
        pages_rows = [r for r in (pages_node.get("rows") or []) if isinstance(r, dict)]
        anchors_rows = [r for r in (anchors_node.get("rows") or []) if isinstance(r, dict)]
        backlinks_rows = [r for r in (backlinks_node.get("rows") or []) if isinstance(r, dict)]

        domains_meta = domains_node.get("meta") if isinstance(domains_node.get("meta"), dict) else {}
        pages_meta = pages_node.get("meta") if isinstance(pages_node.get("meta"), dict) else {}
        anchors_meta = anchors_node.get("meta") if isinstance(anchors_node.get("meta"), dict) else {}
        backlinks_meta = backlinks_node.get("meta") if isinstance(backlinks_node.get("meta"), dict) else {}
        meta_candidates: list[tuple[str, dict[str, Any]]] = []
        for m in (backlinks_meta, pages_meta, domains_meta, anchors_meta):
            ts = m.get("imported_at")
            if isinstance(ts, str) and ts.strip():
                meta_candidates.append((ts.strip(), m))
        meta_candidates.sort(key=lambda x: x[0], reverse=True)
        last_meta = meta_candidates[0][1] if meta_candidates else {}
        last_imported_at = meta_candidates[0][0] if meta_candidates else ""
        imported_via = str(last_meta.get("imported_via") or "").strip().lower()
        if not imported_via:
            imported_via = "csv" if key != "ahrefs" else ("api" if ("token_env_key" in last_meta) else "csv")

        domains_total = len(domains_rows)
        domains_links_total = sum(int(r.get("links") or 0) for r in domains_rows)
        pages_total = len(pages_rows)
        pages_links_total = sum(int(r.get("links") or 0) for r in pages_rows)
        anchors_total = len(anchors_rows)
        backlinks_total = len(backlinks_rows)

        computed_domains: list[dict[str, Any]] = []
        computed_pages: list[dict[str, Any]] = []
        computed_anchors: list[dict[str, Any]] = []
        if backlinks_rows:
            dom_map: dict[str, int] = {}
            page_map: dict[str, int] = {}
            anchor_map: dict[str, int] = {}
            for r in backlinks_rows:
                src = str(r.get("source_url") or "").strip()
                tgt = str(r.get("target_url") or "").strip()
                anc = str(r.get("anchor") or "").strip()
                if src:
                    h = _host_no_www(src)
                    if h:
                        dom_map[h] = dom_map.get(h, 0) + 1
                if tgt:
                    page_map[tgt] = page_map.get(tgt, 0) + 1
                if anc:
                    anchor_map[anc] = anchor_map.get(anc, 0) + 1
            if not domains_rows and dom_map:
                domains_total = len(dom_map)
                domains_links_total = sum(dom_map.values())
                computed_domains = sorted(
                    [{"domain": d, "links": c} for d, c in dom_map.items()], key=lambda x: x["links"], reverse=True
                )[:20]
            if not pages_rows and page_map:
                pages_total = len(page_map)
                pages_links_total = sum(page_map.values())
                computed_pages = sorted(
                    [{"url": u, "links": c} for u, c in page_map.items()], key=lambda x: x["links"], reverse=True
                )[:20]
            if not anchors_rows and anchor_map:
                anchors_total = len(anchor_map)
                computed_anchors = sorted(
                    [{"anchor": a, "links": c} for a, c in anchor_map.items()], key=lambda x: x["links"], reverse=True
                )[:20]

        domains_top = sorted(domains_rows, key=lambda x: int(x.get("links") or 0), reverse=True)[:20] if domains_rows else []
        pages_top = sorted(pages_rows, key=lambda x: int(x.get("links") or 0), reverse=True)[:20] if pages_rows else []
        anchors_top = (
            sorted(anchors_rows, key=lambda x: int(x.get("links") or 0), reverse=True)[:20] if anchors_rows else []
        )

        import_sources.append(
            {
                "key": key,
                "label": label,
                "has": bool(ds),
                "last_imported_at": last_imported_at,
                "imported_via": imported_via,
                "domains": {
                    "total": domains_total,
                    "links_total": domains_links_total,
                    "rows": domains_top or computed_domains,
                    "meta": domains_meta,
                },
                "pages": {
                    "total": pages_total,
                    "links_total": pages_links_total,
                    "rows": pages_top or computed_pages,
                    "meta": pages_meta,
                },
                "anchors": {"total": anchors_total, "rows": anchors_top or computed_anchors, "meta": anchors_meta},
                "backlinks": {"total": backlinks_total, "rows": backlinks_rows[:200], "meta": backlinks_meta},
            }
        )

    resp = templates.TemplateResponse(
        "backlinks.html",
        {
            "request": request,
            "project": project,
            "slug": slug,
            "msg": (msg or "").strip(),
            "err": (err or "").strip(),
            "report_ok": report_ok,
            "summary": summary,
            "top_incoming": top_incoming,
            "top_external_domains": top_external_domains,
            "import_sources": import_sources,
            "ahrefs_configured": False,
            "ahrefs_plan": "",
            "ahrefs_can_sync": False,
        },
    )
    try:
        ahrefs_token, _ahrefs_key = _ahrefs_env_token()
        if ahrefs_token:
            base_url = str(project.get("base_url") or "")
            target = _host_no_www(base_url) if base_url else ""
            resp.context["ahrefs_configured"] = True
            plan = ""
            try:
                limits = _ahrefs_limits_and_usage(ahrefs_token)
                plan = str(limits.get("subscription") or "").strip()
            except Exception:
                plan = ""
            can_sync = ("enterprise" in plan.lower()) or _ahrefs_is_free_test_target(target)
            resp.context["ahrefs_plan"] = plan
            resp.context["ahrefs_can_sync"] = bool(can_sync)
    except Exception:
        # Fail-open: keep the page usable even if Ahrefs endpoints are unreachable.
        pass
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.post("/projects/{slug}/backlinks/import")
async def backlinks_import(
    request: Request,
    slug: str,
    crawl: str = Form(default=""),
    source: str = Form(default="gsc"),
    file: UploadFile = File(...),
) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    source = (source or "").strip().lower()
    if source not in {"gsc", "bing", "ahrefs"}:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Source invalide')}", status_code=303)

    project = dash.project_overview(runs_dir, slug, timestamp=(crawl or None), compare_to=None)
    if not project:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Projet introuvable')}", status_code=303)

    ts = str(project.get("current", {}).get("timestamp") or "")
    if not ts:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Crawl introuvable')}", status_code=303)

    base_url = str(project.get("base_url") or "")
    target_host = _host_no_www(base_url) if base_url else ""

    try:
        content = await file.read()
    except Exception as e:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(str(e))}", status_code=303)

    if not content:
        return RedirectResponse(
            url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote('Fichier vide')}", status_code=303
        )

    try:
        kind, rows = _parse_backlinks_csv(content, target_host=(target_host or None))
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(msg)}", status_code=303)

    run_dir = (runs_dir / slug / ts).resolve()
    backlinks_dir = run_dir / "backlinks"
    backlinks_dir.mkdir(parents=True, exist_ok=True)

    csv_path = backlinks_dir / f"{source}_{kind}.csv"
    json_path = backlinks_dir / f"{source}_{kind}.json"
    try:
        csv_path.write_bytes(content)
        json_path.write_text(
            json.dumps(
                {
                    "meta": {
                        "source": source,
                        "kind": kind,
                        "filename": str(file.filename or ""),
                        "imported_via": "csv",
                        "imported_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                        "rows": len(rows),
                    },
                    "rows": rows,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        _sync_runs_path_to_object_store(csv_path)
        _sync_runs_path_to_object_store(json_path)
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(msg)}", status_code=303)

    return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&msg={quote('Import terminé')}", status_code=303)


@app.post("/projects/{slug}/backlinks/clear")
def backlinks_clear(
    request: Request,
    slug: str,
    crawl: str = Form(default=""),
    source: str = Form(default="gsc"),
    kind: str = Form(default="all"),
) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    source = (source or "").strip().lower()
    kind = (kind or "").strip().lower()
    if source not in {"gsc", "bing", "ahrefs"}:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Source invalide')}", status_code=303)
    if kind not in {"all", "domains", "pages", "anchors", "backlinks"}:
        kind = "all"

    project = dash.project_overview(runs_dir, slug, timestamp=(crawl or None), compare_to=None)
    if not project:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Projet introuvable')}", status_code=303)
    ts = str(project.get("current", {}).get("timestamp") or crawl or "")
    if not ts:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Crawl introuvable')}", status_code=303)

    run_dir = (runs_dir / slug / ts).resolve()
    backlinks_dir = run_dir / "backlinks"
    if backlinks_dir.exists() and backlinks_dir.is_dir():
        pattern = f"{source}_*.*" if kind == "all" else f"{source}_{kind}.*"
        for p in backlinks_dir.glob(pattern):
            try:
                p.unlink()
                _delete_runs_path_from_object_store(p)
            except Exception:
                pass

    return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&msg={quote('Import supprimé')}", status_code=303)


@app.post("/projects/{slug}/backlinks/ahrefs/sync")
def backlinks_ahrefs_sync(
    request: Request,
    slug: str,
    crawl: str = Form(default=""),
    mode: str = Form(default="domain"),
    limit: int = Form(default=1000),
) -> RedirectResponse:
    _ = _db_project_or_404(request, slug)
    runs_dir = _runs_dir_for_request(request)
    token, token_key = _ahrefs_env_token()
    if not token:
        return RedirectResponse(
            url=f"/projects/{slug}/backlinks?err={quote('Ahrefs: token manquant (AHREFS_API_TOKEN)')}", status_code=303
        )

    mode = (mode or "").strip().lower() or "domain"
    if mode not in {"domain", "subdomains", "exact", "prefix"}:
        mode = "domain"

    try:
        limit_n = int(limit)
    except Exception:
        limit_n = 1000
    limit_n = max(1, min(limit_n, 5000))

    project = dash.project_overview(runs_dir, slug, timestamp=(crawl or None), compare_to=None)
    if not project:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Projet introuvable')}", status_code=303)

    ts = str(project.get("current", {}).get("timestamp") or crawl or "")
    if not ts:
        return RedirectResponse(url=f"/projects/{slug}/backlinks?err={quote('Crawl introuvable')}", status_code=303)

    base_url = str(project.get("base_url") or "")
    target = _host_no_www(base_url) if base_url else ""
    if not target:
        return RedirectResponse(
            url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote('Domaine cible invalide')}", status_code=303
        )

    subscription = ""
    try:
        limits = _ahrefs_limits_and_usage(token)
        subscription = str(limits.get("subscription") or "").strip()
    except Exception:
        subscription = ""

    if (not subscription) and (not _ahrefs_is_free_test_target(target)):
        msg = (
            "Ahrefs: impossible de lire le plan (subscription-info). "
            "Vérifie le token / connexion, puis réessaie."
        )
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(msg)}", status_code=303)

    if subscription and ("enterprise" not in subscription.lower()) and (not _ahrefs_is_free_test_target(target)):
        msg = (
            f"Ahrefs: plan \"{subscription}\" — accès API complet réservé à Enterprise "
            "(sur les autres plans: uniquement les free test queries sur ahrefs.com / wordcount.com)."
        )
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(msg)}", status_code=303)

    imported_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    common_meta: dict[str, Any] = {
        "source": "ahrefs",
        "target": target,
        "mode": mode,
        "limit": limit_n,
        "imported_via": "api",
        "imported_at": imported_at,
        "token_env_key": token_key,
        "subscription": subscription,
    }

    try:
        refdomains_data = _ahrefs_api_get(
            "refdomains",
            token=token,
            target=target,
            mode=mode,
            limit=limit_n,
            select="domain,links_to_target",
        )
        anchors_data = _ahrefs_api_get(
            "anchors",
            token=token,
            target=target,
            mode=mode,
            limit=limit_n,
            select="anchor,links_to_target,refdomains",
        )
        backlinks_data = _ahrefs_api_get(
            "all-backlinks",
            token=token,
            target=target,
            mode=mode,
            limit=limit_n,
            select="url_from,url_to,anchor",
        )
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(msg)}", status_code=303)

    domains_rows: list[dict[str, Any]] = []
    refdomains = refdomains_data.get("refdomains") if isinstance(refdomains_data.get("refdomains"), list) else []
    for r in refdomains:
        if not isinstance(r, dict):
            continue
        d = str(
            r.get("refdomain")
            or r.get("ref_domain")
            or r.get("domain")
            or r.get("referring_domain")
            or r.get("referringDomain")
            or ""
        ).strip()
        if not d:
            continue
        d = d.lower()
        if d.startswith("www."):
            d = d[4:]
        links = _parse_intish(str(r.get("links_to_target") or r.get("dofollow_links") or r.get("links") or ""))
        domains_rows.append({"domain": d, "links": links})
    domains_rows.sort(key=lambda x: int(x.get("links") or 0), reverse=True)

    anchors_rows: list[dict[str, Any]] = []
    anchors = anchors_data.get("anchors") if isinstance(anchors_data.get("anchors"), list) else []
    for r in anchors:
        if not isinstance(r, dict):
            continue
        a = str(r.get("anchor") or r.get("anchor_text") or r.get("text") or "").strip()
        if not a:
            continue
        links = _parse_intish(str(r.get("links_to_target") or r.get("dofollow_links") or r.get("links") or ""))
        anchors_rows.append({"anchor": a, "links": links})
    anchors_rows.sort(key=lambda x: int(x.get("links") or 0), reverse=True)

    backlinks_rows: list[dict[str, Any]] = []
    backlinks = backlinks_data.get("backlinks") if isinstance(backlinks_data.get("backlinks"), list) else []
    for r in backlinks:
        if not isinstance(r, dict):
            continue
        src = str(r.get("url_from") or r.get("source_url") or r.get("from") or "").strip()
        tgt = str(r.get("url_to") or r.get("target_url") or r.get("to") or "").strip()
        if not src or not tgt:
            continue
        row: dict[str, Any] = {"source_url": src, "target_url": tgt}
        a = str(r.get("anchor") or r.get("anchor_text") or "").strip()
        if a:
            row["anchor"] = a
        backlinks_rows.append(row)

    run_dir = (runs_dir / slug / ts).resolve()
    backlinks_dir = run_dir / "backlinks"
    backlinks_dir.mkdir(parents=True, exist_ok=True)
    domains_path = backlinks_dir / "ahrefs_domains.json"
    anchors_path = backlinks_dir / "ahrefs_anchors.json"
    backlinks_path = backlinks_dir / "ahrefs_backlinks.json"

    try:
        domains_path.write_text(
            json.dumps(
                {"meta": {**common_meta, "kind": "domains", "rows": len(domains_rows)}, "rows": domains_rows},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        anchors_path.write_text(
            json.dumps(
                {"meta": {**common_meta, "kind": "anchors", "rows": len(anchors_rows)}, "rows": anchors_rows},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        backlinks_path.write_text(
            json.dumps(
                {"meta": {**common_meta, "kind": "backlinks", "rows": len(backlinks_rows)}, "rows": backlinks_rows},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        _sync_runs_path_to_object_store(domains_path)
        _sync_runs_path_to_object_store(anchors_path)
        _sync_runs_path_to_object_store(backlinks_path)
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        return RedirectResponse(url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&err={quote(msg)}", status_code=303)

    return RedirectResponse(
        url=f"/projects/{slug}/backlinks?crawl={quote(ts)}&msg={quote(f'Synchro Ahrefs terminée ({len(backlinks_rows)} backlinks)')}",
        status_code=303,
    )


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: str) -> HTMLResponse:
    job = _load_job(job_id)
    if not job:
        resp = templates.TemplateResponse("job.html", {"request": request, "job": None})
        resp.headers["Cache-Control"] = "no-store"
        return resp
    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    if not is_admin:
        result = job.result if isinstance(job.result, dict) else {}
        owner_id = str(result.get("user_id") or "").strip()
        if owner_id != str(getattr(user, "id", "")):
            resp = templates.TemplateResponse("job.html", {"request": request, "job": None}, status_code=404)
            resp.headers["Cache-Control"] = "no-store"
            return resp

    before_progress = job.progress
    _normalize_completed_job(job)
    if job.progress != before_progress:
        _save_job(job)

    latest = _load_latest_global_summary(DEFAULT_RUNS_DIR) if (is_admin and DEFAULT_RUNS_DIR.exists()) else None

    corrections_plan = None
    corrections_plan_path: str | None = None
    if is_admin and job.config_path and job.finished_at:
        try:
            # This is a bit brittle, relies on knowing the orchestrator's output structure.
            with open(job.config_path, 'r', encoding='utf-8') as f:
                import yaml
                config = yaml.safe_load(f)
            
            # Reconstruct the run path
            # This logic is duplicated from the script, which is not ideal
            site_name = None
            if config.get("sites"):
                site_name = config["sites"][0].get("name") # Assume first site for now
            
            if site_name:
                slug = re.sub(r"[^a-z0-9]+", "-", site_name.strip().lower()).strip("-")
                
                # We don't know the exact timestamp, so we find the latest run for that slug
                run_dirs = sorted([p for p in (DEFAULT_RUNS_DIR / slug).iterdir() if p.is_dir()], reverse=True)
                if run_dirs:
                    latest_run_dir = run_dirs[0]
                    plan_path = latest_run_dir / "corrections-plan.json"
                    if plan_path.exists():
                        corrections_plan = json.loads(plan_path.read_text(encoding="utf-8"))
                        corrections_plan_path = str(plan_path)

        except Exception:
            # Could fail for many reasons, just ignore and don't show the plan
            pass

    resp = templates.TemplateResponse(
        "job.html",
        {
            "request": request,
            "job": job,
            "latest": latest,
            "corrections_plan": corrections_plan,
            "corrections_plan_path": corrections_plan_path,
        },
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _cancel_queued_job(job: Job) -> None:
    """Cancels a queued job and refunds any reserved quota."""
    result = job.result if isinstance(job.result, dict) else {}
    owner_id = str(result.get("user_id") or "").strip()
    try:
        reserved = int(result.get("quota_reserved_pages") or 0)
    except Exception:
        reserved = 0
    skip_billing = bool(result.get("skip_billing") or False)

    if reserved > 0 and (not skip_billing) and owner_id:
        with DB.session() as db:
            billing.usage_add(
                db,
                user_id=owner_id,
                metric="pages_crawled_month",
                amount=-int(reserved),
                meta={"kind": "crawl_cancel_refund", "job_id": job.id, "reserved_pages": int(reserved)},
            )
        if isinstance(job.result, dict):
            job.result["quota_reserved_pages"] = 0

    job.status = "canceled"
    job.returncode = 0
    job.finished_at = time.time()
    _save_job(job)


@app.post("/jobs/{job_id}/cancel")
def job_cancel(request: Request, job_id: str) -> RedirectResponse:
    job = _load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    result = job.result if isinstance(job.result, dict) else {}
    owner_id = str(result.get("user_id") or "").strip()
    if (not is_admin) and owner_id != str(getattr(user, "id", "")):
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in {"done", "failed", "canceled"}:
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)

    if job.status == "queued":
        _cancel_queued_job(job)
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)

    # If running: request cancellation. The subprocess loop polls DB status and will terminate.
    job.status = "cancel_requested"
    _save_job(job)
    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


@app.post("/jobs/{job_id}/retry")
def job_retry(request: Request, job_id: str) -> RedirectResponse:
    job = _load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    result = job.result if isinstance(job.result, dict) else {}
    owner_id = str(result.get("user_id") or "").strip()
    if (not is_admin) and owner_id != str(getattr(user, "id", "")):
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in {"failed", "canceled"}:
        return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)

    jtype = str(result.get("type") or "").strip().lower()
    if jtype == "crawl":
        slug = str(result.get("slug") or "").strip()
        if not slug:
            return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
        # Reuse the standard crawl enqueue path (includes quota checks).
        resp = crawl_project(request, slug, config_path=(job.config_path or str(DEFAULT_CONFIG)))  # type: ignore[misc]
        return resp if isinstance(resp, RedirectResponse) else RedirectResponse(url=f"/projects/{slug}", status_code=303)

    if jtype == "autopilot":
        _ = _require_admin(request)
        cfg = Path(job.config_path or "").expanduser() if job.config_path else None
        if not cfg:
            return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
        if not cfg.is_absolute():
            cfg = (REPO_ROOT / cfg).resolve()
        extra_args = result.get("extra_args") if isinstance(result, dict) else None
        extra = extra_args if isinstance(extra_args, list) and all(isinstance(x, str) for x in extra_args) else []
        new_job = Job(id=str(uuid.uuid4()), status="queued", created_at=time.time(), config_path=str(cfg))
        new_job.result = {"type": "autopilot", "user_id": str(getattr(user, "id", "")), "extra_args": extra}
        script = REPO_ROOT / "skills" / "public" / "seo-autopilot" / "scripts" / "seo_autopilot.py"
        cmd_preview = [sys.executable, "-u", str(script), "--config", str(cfg)]
        if extra:
            cmd_preview.extend(extra)
        new_job.command = cmd_preview
        _save_job(new_job)
        return RedirectResponse(url=f"/jobs?job={new_job.id}", status_code=303)

    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)


@app.get("/api/jobs/{job_id}", response_class=JSONResponse)
def job_api(request: Request, job_id: str, tail: int = 20_000) -> JSONResponse:
    job = _load_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    user = getattr(request.state, "user", None)
    is_admin = bool(getattr(user, "is_admin", False))
    if not is_admin:
        result = job.result if isinstance(job.result, dict) else {}
        owner_id = str(result.get("user_id") or "").strip()
        if owner_id != str(getattr(user, "id", "")):
            raise HTTPException(status_code=404, detail="Job not found")

    before_progress = job.progress
    _normalize_completed_job(job)
    if job.progress != before_progress:
        _save_job(job)

    data = asdict(job)
    tail = int(tail) if isinstance(tail, int) else 20_000
    if tail > 0:
        data["stdout"] = (data.get("stdout") or "")[-tail:]
        data["stderr"] = (data.get("stderr") or "")[-tail:]
    return JSONResponse(content=data, headers={"Cache-Control": "no-store"})


def _apply_corrections_worker(plan_path_str: str):
    """
    Reads a corrections plan and applies the changes to the target files.

    NOTE: This is a blocking operation and should be run in a background thread/process.
    WARNING: This function directly modifies files on the filesystem. This is powerful but
    inherently risky. For a more robust and safer workflow, consider changing this
    to generate a Git branch with the changes and open a pull request. This would
    provide a clear, reviewable audit trail before any changes go live.
    """
    try:
        plan_path = _resolve_path_under_root(plan_path_str, DEFAULT_RUNS_DIR)
    except HTTPException as e:
        print(f"[FIXER] ERROR: {e.detail}")
        return
    if not plan_path.exists() or not plan_path.is_file():
        print(f"[FIXER] Plan file not found: {plan_path}")
        return

    print(f"[FIXER] Applying corrections from: {plan_path}")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(plan, list):
        print("[FIXER] ERROR: Invalid plan format (expected a JSON list).")
        return

    backup_root = (plan_path.parent / f"corrections-backup-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}").resolve()
    backup_root.mkdir(parents=True, exist_ok=True)

    def _backup_original(path: Path, original: str) -> None:
        try:
            rel = path.relative_to(REPO_ROOT)
        except Exception:
            return
        dest = (backup_root / rel).resolve()
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(original, encoding="utf-8")

    def _insert_into_head(content: str, snippet: str) -> str | None:
        m_head = _HEAD_OPEN_RE.search(content)
        if m_head:
            return content[: m_head.end()] + "\n" + snippet + content[m_head.end() :]
        m_close = _HEAD_CLOSE_RE.search(content)
        if m_close:
            return content[: m_close.start()] + snippet + "\n" + content[m_close.start() :]
        return None

    for correction in plan:
        try:
            if not isinstance(correction, dict):
                continue
            file_to_fix = Path(str(correction.get("file_path") or "")).expanduser()
            if not file_to_fix.is_absolute():
                file_to_fix = (REPO_ROOT / file_to_fix).resolve()
            else:
                file_to_fix = file_to_fix.resolve()

            # Guardrail: only allow writing inside the repo, and never inside run/job folders.
            forbidden_roots = [
                (REPO_ROOT / "seo-runs").resolve(),
                (REPO_ROOT / "seo-agent-web" / "data").resolve(),
                DEFAULT_RUNS_DIR.resolve(),
                DATA_DIR.resolve(),
                (REPO_ROOT / "dist").resolve(),
            ]
            if not file_to_fix.is_relative_to(REPO_ROOT) or any(file_to_fix.is_relative_to(r) for r in forbidden_roots):
                print(f"[FIXER] ERROR: Refusing to write outside allowed roots: {file_to_fix}")
                continue

            if file_to_fix.suffix.lower() not in {".html", ".htm"}:
                print(f"[FIXER] ERROR: Refusing to edit non-HTML file: {file_to_fix}")
                continue

            issue_type = str(correction.get("issue_type") or "").strip()
            current_value = str(correction.get("current_value") or "")
            suggested_value = correction.get("suggested_value")
            if suggested_value is None:
                print(f"[FIXER] ERROR: Missing suggested_value for {file_to_fix}")
                continue
            suggested_value_str = str(suggested_value)

            if not file_to_fix.exists():
                print(f"[FIXER] ERROR: File not found, cannot apply fix: {file_to_fix}")
                continue

            content = file_to_fix.read_text(encoding="utf-8")
            updated: str | None = None

            if issue_type in {"duplicate_title", "title_too_long", "title_too_short"}:
                m = _TITLE_RE.search(content)
                if not m:
                    print("[FIXER]  - FAILED: <title> not found.")
                    continue

                existing_title = _normalize_title_text(m.group(2))
                expected_title = _normalize_title_text(str(current_value))
                if existing_title != expected_title:
                    print(f"[FIXER]  - FAILED: Title mismatch (expected {expected_title!r}, got {existing_title!r}).")
                    continue

                print(f"[FIXER] Applying title fix to {file_to_fix}...")
                new_title = html.escape(suggested_value_str, quote=False)
                updated = content[: m.start(2)] + new_title + content[m.end(2) :]

            elif issue_type == "missing_title":
                m = _TITLE_RE.search(content)
                if m:
                    existing_title = _normalize_title_text(m.group(2))
                    if existing_title:
                        print(f"[FIXER]  - SKIP: <title> already present for {file_to_fix}")
                        continue
                    print(f"[FIXER] Applying missing title fix to {file_to_fix} (empty <title>)...")
                    new_title = html.escape(suggested_value_str, quote=False)
                    updated = content[: m.start(2)] + new_title + content[m.end(2) :]
                else:
                    print(f"[FIXER] Applying missing title fix to {file_to_fix} (insert <title>)...")
                    snippet = f"  <title>{html.escape(suggested_value_str, quote=False)}</title>\n"
                    updated = _insert_into_head(content, snippet)
                    if updated is None:
                        print("[FIXER]  - FAILED: <head> not found.")
                        continue

            elif issue_type in {"missing_meta_description", "duplicate_meta_description", "duplicate_meta_descriptions"}:
                mtag = _META_DESC_TAG_RE.search(content)
                if mtag:
                    tag_text = mtag.group(0)
                    mcontent = _META_CONTENT_ATTR_RE.search(tag_text)
                    existing_md = _normalize_meta_text(mcontent.group(3) if mcontent else "")

                    if issue_type.startswith("duplicate_"):
                        expected = _normalize_meta_text(current_value)
                        if existing_md != expected:
                            print(
                                f"[FIXER]  - FAILED: Meta description mismatch (expected {expected!r}, got {existing_md!r})."
                            )
                            continue
                    else:
                        if existing_md:
                            print(f"[FIXER]  - SKIP: meta description already present for {file_to_fix}")
                            continue

                    new_md_attr = html.escape(suggested_value_str, quote=True)
                    if mcontent:
                        def _repl(m: re.Match[str]) -> str:
                            return f"{m.group(1)}{m.group(2)}{new_md_attr}{m.group(2)}"

                        new_tag_text = _META_CONTENT_ATTR_RE.sub(_repl, tag_text, count=1)
                    else:
                        # Insert missing content attribute before the closing bracket.
                        if tag_text.endswith("/>"):
                            new_tag_text = tag_text[:-2] + f' content="{new_md_attr}" />'
                        else:
                            new_tag_text = tag_text[:-1] + f' content="{new_md_attr}">' if tag_text.endswith(">") else tag_text

                    updated = content[: mtag.start()] + new_tag_text + content[mtag.end() :]
                else:
                    if issue_type.startswith("duplicate_"):
                        print("[FIXER]  - FAILED: meta description tag not found.")
                        continue
                    print(f"[FIXER] Applying missing meta description fix to {file_to_fix} (insert <meta>)...")
                    snippet = f'  <meta name="description" content="{html.escape(suggested_value_str, quote=True)}" />\n'
                    updated = _insert_into_head(content, snippet)
                    if updated is None:
                        print("[FIXER]  - FAILED: <head> not found.")
                        continue

            else:
                print(f"[FIXER] INFO: Unsupported issue_type {issue_type!r} (skip)")
                continue

            if updated is None or updated == content:
                continue

            _backup_original(file_to_fix, content)
            file_to_fix.write_text(updated, encoding="utf-8")
            print("[FIXER]  - SUCCESS: Applied correction.")

        except Exception as e:
            print(f"[FIXER] ERROR: Failed to apply correction for {correction.get('file_path')}: {e}")


@app.post("/jobs/{job_id}/apply-corrections")
def apply_corrections(
    job_id: str,
    background_tasks: BackgroundTasks,
    plan_path: str = Form(...),
) -> RedirectResponse:
    # Guardrail: only accept plans inside `seo-runs/` (prevents arbitrary file reads/writes via crafted form input).
    _ = _resolve_path_under_root(plan_path, DEFAULT_RUNS_DIR)

    # For now, running this synchronously.
    # In a real app, you'd use the background_tasks or a proper worker queue.
    # background_tasks.add_task(_apply_corrections_worker, plan_path)
    _apply_corrections_worker(plan_path)

    return RedirectResponse(url=f"/jobs/{job_id}", status_code=303)
